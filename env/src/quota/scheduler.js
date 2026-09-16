// Fair scheduling policy: Deficit Round Robin (DRR) across tenants with strict
// FIFO inside each tenant.
//
// Why DRR: a tenant with a fat budget must not be able to keep draining a
// shared resource (class budget, a burst window) forever ahead of a quieter
// tenant. Each pass a tenant receives quantum = weight * QUANTUM_UNIT grants
// worth of deficit, serves its head while deficit lasts, and unused deficit
// banks (capped) for the next pass. After each run the cursor rotates just
// past the last tenant served, so when a contended resource frees up the
// tenants that did not just consume it are first in line (a heavy and a quiet
// tenant competing for one slot strictly alternate) instead of one tenant
// re-grabbing everything.
//
// This policy is pure over injected eligibility/grant callbacks; durability
// lives in the ledger (ring/deficit are runtime-only and rebuild from the
// durable waiting queue after a restart).

export const QUANTUM_UNIT = 1; // one grant per weight point per pass
const DEFICIT_CAP_FACTOR = 2; // a blocked tenant may bank at most 2 quanta

// Scheduler state:
//   ring:     tenant ids in (stable) activation order
//   cursor:   index visited first by the next run
//   deficit:  banked unused quantum per tenant
export function createSchedulerState() {
  return { ring: [], cursor: 0, deficit: new Map() };
}

function quantumOf(weight) {
  return Math.max(1, Math.floor(weight)) * QUANTUM_UNIT;
}

// Ensure tenant participates in the ring (appended when it first backlogs).
export function activate(state, tenantId) {
  if (!state.ring.includes(tenantId)) state.ring.push(tenantId);
}

export function deactivate(state, tenantId) {
  const idx = state.ring.indexOf(tenantId);
  if (idx === -1) return;
  state.ring.splice(idx, 1);
  if (idx < state.cursor) state.cursor -= 1;
  if (state.ring.length === 0) state.cursor = 0;
  else state.cursor %= state.ring.length;
  state.deficit.delete(tenantId);
}

// Run scheduling passes with a persistent DRR cursor. Starting at the cursor,
// each tenant is visited once in ring order, accrues one quantum, and serves
// its FIFO head(s) while eligible and deficit lasts. After the run the cursor
// sits just past the last tenant served: when a contended resource frees up,
// tenants that did not just consume it are first in line (a heavy and a quiet
// tenant competing for one slot strictly alternate), while tenants with spare
// capacity keep being served on repeated passes until capacity is exhausted
// (maxGrants bounds the work per call).
//   hasWaiter(tenantId) -> tenant still backlogged
//   canGrant(tenantId)  -> tenant's head waiter is currently eligible
//   grant(tenantId)     -> append the grant event
// Returns { grants }.
export function runRound(state, weightsOf, hasWaiter, canGrant, grant, maxGrants = 10000) {
  const { ring, deficit: deficits } = state;
  let grants = 0;

  while (ring.length && grants < maxGrants) {
    let passGrants = 0;
    let lastGrantedStep = -1;
    for (let step = 0; step < ring.length; step += 1) {
      const idx = (state.cursor + step) % ring.length;
      const tenantId = ring[idx];
      if (!hasWaiter(tenantId)) continue;
      if (grants >= maxGrants) break;

      const weight = Math.max(1, weightsOf(tenantId) || 1);
      const quantum = quantumOf(weight);
      const cap = quantum * DEFICIT_CAP_FACTOR;
      let deficit = Math.min(cap, (deficits.get(tenantId) || 0) + quantum);

      while (deficit > 0 && canGrant(tenantId)) {
        grant(tenantId);
        deficit -= 1;
        grants += 1;
        passGrants += 1;
        lastGrantedStep = step;
      }
      deficits.set(tenantId, deficit);
    }
    if (passGrants === 0) break;

    // Persistent cursor: the next run starts right after the last grantee.
    state.cursor = (state.cursor + lastGrantedStep + 1) % ring.length;
  }
  return { grants };
}

// ---------------------------------------------------------------------------
// Estimated eligibility time (an upper bound), shared by status responses.
//
// A waiter can become eligible no earlier than the slowest of:
//   - the tenant-direction bucket having enough tokens for predecessors + self
//   - the class-direction bucket (shared budgets) likewise
//   - a tenant inflight slot freeing (oldest in-flight hold expires)
//   - a class inflight slot freeing likewise
// Pure math over caller-supplied facts; returns ms since epoch, or null when
// nothing is contended.
// ---------------------------------------------------------------------------

function refillFor(bucket, ratePerSec, burst, nowMs) {
  const elapsed = Math.max(0, nowMs - bucket.updatedAtMs);
  return Math.min(burst, bucket.tokens + (elapsed * ratePerSec) / 1000);
}

export function bucketReadyMs({ bucket, ratePerSec, burst, demand, nowMs }) {
  if (demand <= 0) return null;
  const tokens = bucket ? refillFor(bucket, ratePerSec, burst, nowMs) : burst;
  if (tokens >= demand) return null;
  if (ratePerSec <= 0) return null;
  return nowMs + Math.ceil(((demand - tokens) * 1000) / ratePerSec);
}

// slotsInFlight: currently held grants; maxInflight: configured cap.
// expiryQueue: ascending expiry epoch-ms of the currently held grants.
export function slotReadyMs({ slotsInFlight, maxInflight, expiryQueue, nowMs }) {
  if (maxInflight <= 0 || slotsInFlight < maxInflight) return null;
  const freeing = expiryQueue[0];
  if (freeing === undefined) return null;
  // A slot frees no later than the oldest grant's hold deadline.
  return Math.max(nowMs, freeing);
}
