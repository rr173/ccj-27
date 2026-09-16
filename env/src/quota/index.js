// Quota scheduler service: multi-tenant traffic budgets with fair scheduling.
//
// Model
//   - budgets are per tenant, per direction (in/out) and INDEPENDENT each way:
//     ratePerSec, burst, maxInflight; a tenant also has a DRR weight and a
//     bounded waiting area
//   - a business *class* can carry a SHARED budget: a request tagged with the
//     class must pass both its tenant budget and the class budget (both the
//     rate/burst buckets and the inflight caps)
//   - POST /reserve atomically pre-empts: grant (tokens consumed, inflight
//     held) or enqueue behind the tenant's FIFO. Settlement on completion
//     releases the inflight slot (tokens stay consumed); abort additionally
//     refunds tokens
//   - grants carry a hold TTL (reservation expiry). A background reaper,
//     guarded by a single-writer lease, expires dead holds, refunds tokens and
//     frees slots; a late settlement after expiry is rejected, so a dead
//     handler can never release capacity twice
//   - DRR across tenants (src/quota/scheduler.js), strict FIFO within tenant
//   - budget configs carry revisions; updates use optimistic concurrency
//     (expectedRevision); lowers never revoke granted/waiting-admitted holds,
//     raises immediately feed the next scheduling pass
//
// Like the other components, state is a fold over an append-only journal;
// token math in the fold uses the event timestamp, so replays are deterministic
// and a restart rehydrates all still-unexpired holds and the waiting queue.
import { mkdirSync } from 'node:fs';
import { createServer, HttpError } from '../http.js';
import { Ledger } from '../store/ledger.js';
import { LeaderLock } from '../store/leader-lock.js';
import { logger } from '../log.js';
import { uid, nowIso } from '../util.js';
import { createBucket, refill, refund } from './bucket.js';
import { createSchedulerState, activate, deactivate, runRound, bucketReadyMs, slotReadyMs } from './scheduler.js';

const log = logger('quota');

const DIRECTIONS = ['in', 'out'];

export const DEFAULTS = {
  ratePerSec: 100,
  burst: 100,
  maxInflight: 100,
  holdTtlMs: 30_000,
  waitTimeoutMs: 60_000,
  waitCapacity: 1000,
  weight: 1,
  cost: 1,
};

const GLOBAL_WAIT_CAPACITY = Number(process.env.QUOTA_GLOBAL_WAIT_CAPACITY || 100_000);

// Thrown mid scheduling/reaping round when the cross-process fence shows the
// lease was stolen. The losing process must stop appending for this round;
// maintenance ticks will either keep it a follower or recontest later.
class LeaseLostError extends Error {
  constructor() {
    super('lease lost mid-round');
    this.name = 'LeaseLostError';
  }
}

// The on-disk journal advanced past our local view while we believed we were
// leader: another epoch is writing. Re-anchor (read-only) and stop this round.
class SeqFencedError extends Error {
  constructor() {
    super('journal seq fenced by another epoch');
    this.name = 'SeqFencedError';
  }
}

function initial() {
  return {
    tenants: {}, // tenantId -> { revision, spec, buckets: { in, out } }
    classes: {}, // classId  -> { revision, spec, buckets: { in, out } }
    waiters: [], // FIFO across all tenants; each tenant keeps its relative order
    reservations: {}, // reservationId -> record (incl. terminal history)
    byReqId: {}, // client requestId -> { kind, id }
    lease: null, // { ownerEpoch, expiresAtMs }
    counters: { enqSeq: 0 },
    stats: { reserved: 0, enqueued: 0, settled: 0, expired: 0, rejected: 0, timedOut: 0 },
  };
}

function eventMs(event) {
  return Date.parse(event.ts);
}

// Apply a directional budget fragment over a prior spec (with defaults).
function mergeDirSpec(prev, patch) {
  const base = {
    ratePerSec: DEFAULTS.ratePerSec,
    burst: DEFAULTS.burst,
    maxInflight: DEFAULTS.maxInflight,
    ...(prev || {}),
  };
  return { ...base, ...(patch || {}) };
}

function validateDirSpec(s, label) {
  if (typeof s.ratePerSec !== 'number' || s.ratePerSec < 0) throw new Error(`${label}.ratePerSec must be >= 0`);
  if (typeof s.burst !== 'number' || s.burst < 0) throw new Error(`${label}.burst must be >= 0`);
  if (!Number.isInteger(s.maxInflight) || s.maxInflight < 0) throw new Error(`${label}.maxInflight must be a non-negative integer`);
}

function validateTenantSpec(spec) {
  for (const d of DIRECTIONS) validateDirSpec(spec[d], d);
  if (!Number.isInteger(spec.weight) || spec.weight < 1) throw new Error('weight must be an integer >= 1');
  if (!Number.isInteger(spec.waitCapacity) || spec.waitCapacity < 0) throw new Error('waitCapacity must be a non-negative integer');
  if (!Number.isInteger(spec.holdTtlMs) || spec.holdTtlMs < 1) throw new Error('holdTtlMs must be >= 1');
  if (!Number.isInteger(spec.waitTimeoutMs) || spec.waitTimeoutMs < 0) throw new Error('waitTimeoutMs must be >= 0 (0 = wait forever)');
}

function buildTenantSpec(patch, prev) {
  const p = patch || {};
  const spec = {
    weight: p.weight ?? prev?.weight ?? DEFAULTS.weight,
    waitCapacity: p.waitCapacity ?? prev?.waitCapacity ?? DEFAULTS.waitCapacity,
    holdTtlMs: p.holdTtlMs ?? prev?.holdTtlMs ?? DEFAULTS.holdTtlMs,
    waitTimeoutMs: p.waitTimeoutMs ?? prev?.waitTimeoutMs ?? DEFAULTS.waitTimeoutMs,
    in: mergeDirSpec(prev?.in, p.in),
    out: mergeDirSpec(prev?.out, p.out),
  };
  validateTenantSpec(spec);
  return spec;
}

function validateClassSpec(spec) {
  for (const d of DIRECTIONS) validateDirSpec(spec[d], d);
}

function buildClassSpec(patch, prev) {
  const spec = {
    in: mergeDirSpec(prev?.in, patch.in),
    out: mergeDirSpec(prev?.out, patch.out),
  };
  validateClassSpec(spec);
  return spec;
}

// Adjust existing buckets to a new revision: lowered bursts clamp immediately;
// raised rates are picked up lazily on the next refill (no history rewrite).
function rebiasBuckets(buckets, spec, nowMs) {
  const out = {};
  for (const d of DIRECTIONS) {
    const b = buckets[d] ? refill(buckets[d], spec[d].ratePerSec, spec[d].burst, nowMs) : createBucket(spec[d].burst, nowMs);
    out[d] = { tokens: Math.min(b.tokens, spec[d].burst), updatedAtMs: b.updatedAtMs };
  }
  return out;
}

function fold(state, event) {
  const d = event.data || {};
  const now = eventMs(event);
  switch (event.type) {
    case 'SCHEDULER_STARTED':
      break;

    case 'BUDGET_CONFIGURED': {
      if (d.kind === 'tenant') {
        const prev = state.tenants[d.tenantId];
        const buckets = prev ? rebiasBuckets(prev.buckets, d.spec, now)
          : { in: createBucket(d.spec.in.burst, now), out: createBucket(d.spec.out.burst, now) };
        state.tenants[d.tenantId] = { revision: d.revision, spec: d.spec, buckets };
      } else {
        const prev = state.classes[d.classId];
        const buckets = prev ? rebiasBuckets(prev.buckets, d.spec, now)
          : { in: createBucket(d.spec.in.burst, now), out: createBucket(d.spec.out.burst, now) };
        state.classes[d.classId] = { revision: d.revision, spec: d.spec, buckets };
      }
      break;
    }

    case 'WAIT_ENQUEUED': {
      state.counters.enqSeq += 1;
      d.waiter.enqSeq = state.counters.enqSeq;
      state.waiters.push(d.waiter);
      state.byReqId[d.waiter.reqId] = { kind: 'waiter', id: d.waiter.reqId };
      state.stats.enqueued += 1;
      break;
    }

    case 'WAIT_REJECTED':
      state.stats.rejected += 1;
      if (d.reqId) state.byReqId[d.reqId] = { kind: 'rejected', id: d.reqId, reason: d.reason };
      break;

    case 'WAIT_TIMED_OUT': {
      state.waiters = state.waiters.filter((w) => w.reqId !== d.reqId);
      state.byReqId[d.reqId] = { kind: 'timed_out', id: d.reqId };
      state.stats.timedOut += 1;
      break;
    }

    case 'GRANTED': {
      const tenant = state.tenants[d.tenantId];
      if (tenant) {
        const s = tenant.spec[d.direction];
        const b = refill(tenant.buckets[d.direction], s.ratePerSec, s.burst, now);
        tenant.buckets[d.direction] = { tokens: Math.max(0, b.tokens - d.cost), updatedAtMs: now };
      }
      if (d.classId) {
        const cls = state.classes[d.classId];
        if (cls) {
          const s = cls.spec[d.direction];
          const b = refill(cls.buckets[d.direction], s.ratePerSec, s.burst, now);
          cls.buckets[d.direction] = { tokens: Math.max(0, b.tokens - d.cost), updatedAtMs: now };
        }
      }
      if (d.waiterReqId) {
        state.waiters = state.waiters.filter((w) => w.reqId !== d.waiterReqId);
      }
      state.reservations[d.reservationId] = {
        reservationId: d.reservationId,
        reqId: d.reqId,
        tenantId: d.tenantId,
        direction: d.direction,
        classId: d.classId || null,
        cost: d.cost,
        status: 'HELD',
        enqSeq: d.enqSeq ?? null,
        grantedAtMs: now,
        expiresAtMs: d.expiresAtMs,
        ownerEpoch: d.ownerEpoch,
        tenantRevision: d.tenantRevision,
        classRevision: d.classRevision ?? null,
      };
      state.byReqId[d.reqId] = { kind: 'reservation', id: d.reservationId };
      state.stats.reserved += 1;
      break;
    }

    case 'SETTLED': {
      const r = state.reservations[d.reservationId];
      if (!r || r.status !== 'HELD') break; // duplicate or late-after-expiry: no second release
      r.status = d.outcome === 'abort' ? 'ABORTED' : 'COMPLETED';
      r.settledAtMs = now;
      r.outcome = d.outcome;
      if (d.outcome === 'abort') {
        const tenant = state.tenants[r.tenantId];
        if (tenant) {
          const s = tenant.spec[r.direction];
          tenant.buckets[r.direction] = refund(tenant.buckets[r.direction], r.cost, s.ratePerSec, s.burst, now);
        }
        if (r.classId) {
          const cls = state.classes[r.classId];
          if (cls) {
            const s = cls.spec[r.direction];
            cls.buckets[r.direction] = refund(cls.buckets[r.direction], r.cost, s.ratePerSec, s.burst, now);
          }
        }
      }
      state.stats.settled += 1;
      break;
    }

    case 'EXPIRY_RECLAIMED': {
      const r = state.reservations[d.reservationId];
      if (!r || r.status !== 'HELD') break; // exactly one reclaim ever frees a hold
      r.status = 'EXPIRED';
      r.expiredAtMs = now;
      r.reclaimedBy = d.ownerEpoch;
      const tenant = state.tenants[r.tenantId];
      if (tenant) {
        const s = tenant.spec[r.direction];
        tenant.buckets[r.direction] = refund(tenant.buckets[r.direction], r.cost, s.ratePerSec, s.burst, now);
      }
      if (r.classId) {
        const cls = state.classes[r.classId];
        if (cls) {
          const s = cls.spec[r.direction];
          cls.buckets[r.direction] = refund(cls.buckets[r.direction], r.cost, s.ratePerSec, s.burst, now);
        }
      }
      state.stats.expired += 1;
      break;
    }

    case 'LEASE_ACQUIRED':
      state.lease = { ownerEpoch: d.ownerEpoch, expiresAtMs: d.expiresAtMs };
      break;

    default:
      break;
  }
  return state;
}

export class QuotaEngine {
  constructor(ledger, { tickMs = 50, leaseTtlMs = 5000, reaperMs = 200, maxGrantsPerTick = 5000, leaderLock = null } = {}) {
    this.ledger = ledger;
    this.tickMs = tickMs;
    this.leaseTtlMs = leaseTtlMs;
    this.reaperMs = reaperMs;
    this.maxGrantsPerTick = maxGrantsPerTick;
    this.epoch = uid('qrun_');
    this.sched = createSchedulerState();
    this.timers = [];
    this.kickTimer = null;
    this.stopped = false;
    // Cross-process arbiter of who may append. null in unit-style usage keeps
    // the engine able to run against a private ledger; production and takeover
    // tests pass a LeaderLock on the shared data volume.
    this.leaderLock = leaderLock;
    this.hasLed = false;

    // Rebuild the DRR ring from the durable waiting queue (deficit restarts;
    // fairness is a runtime property, ordering/durability come from the log).
    for (const w of ledger.state.waiters) activate(this.sched, w.tenantId);
  }

  get state() {
    return this.ledger.state;
  }

  // Leadership requires BOTH the folded lease view (this epoch, unexpired) and
  // continued ownership of the cross-process lock file. The inode/payload check
  // fences a process the instant a successor steals its expired lock, even if
  // it has not yet replayed the successor's LEASE_ACQUIRED event.
  isLeader(nowMs = Date.now()) {
    const l = this.state.lease;
    if (!l || l.ownerEpoch !== this.epoch || l.expiresAtMs <= nowMs) return false;
    if (this.leaderLock && !this.leaderLock.isOwner()) return false;
    return true;
  }

  start() {
    // Followers append NOTHING on boot: no SCHEDULER_STARTED, no lease event.
    // They poll the lock read-only until they win it (or never do).
    this.tryBecomeLeader('boot');
    const renewEvery = Math.max(50, Math.floor(this.leaseTtlMs / 2));
    this.timers.push(setInterval(() => this.maintainLease(), renewEvery));
    this.timers.push(setInterval(() => this.safeReap(), this.reaperMs));
    this.timers.push(setInterval(() => this.safeSchedule(), this.tickMs));
    // Followers write nothing but still answer queries; re-fold the leader's
    // durable events on a bounded cadence so their read views stay current.
    this.timers.push(setInterval(() => {
      if (!this.stopped && !this.isLeader()) this.ledger.refreshState();
    }, Math.max(100, this.reaperMs * 5)));
    // Immediate pass shortly after boot only has any effect if leadership won.
    this.timers.push(setTimeout(() => this.safeSchedule(), 10));
    log.info('quota_engine_started', {
      epoch: this.epoch, leaseTtlMs: this.leaseTtlMs,
      leader: this.isLeader(),
    });
  }

  async close() {
    this.stopped = true;
    for (const t of this.timers) clearInterval(t);
    for (const t of this.timers) clearTimeout(t);
    this.timers = [];
    if (this.kickTimer) clearTimeout(this.kickTimer);
    // Graceful shutdown frees the lock immediately so a replacement need not
    // wait out the TTL. ungraceful exits leave the file for TTL-based stealing.
    if (this.leaderLock) this.leaderLock.release();
    await this.ledger.close();
  }

  requireLeader() {
    if (!this.isLeader()) throw new HttpError(503, 'not_leader', 'scheduler lease held by another epoch');
  }

  // The ONLY path through which a leader appends. Each append is fenced three
  // ways against a parallel replacement:
  //   1. pre-write: still holding the lock file (inode + payload). This is the
  //      cross-process source of truth and is valid even while we are appending
  //      the very first LEASE_ACQUIRED that makes isLeader() true in our fold
  //   2. seq: the durable journal tail must equal our local seq, so our new
  //      seq is exactly tail+1 and can never duplicate another writer's
  //   3. post-write: re-check the lock; if it was stolen across the append we
  //      reload the authoritative log and self-fence for the rest of the round
  // A process that fails any of these appends NOTHING further this round.
  fencedAppend(type, data = {}) {
    if (this.leaderLock) {
      if (!this.leaderLock.isOwner()) throw new LeaseLostError();
      const tail = this.ledger.tailSeq();
      if (tail !== this.ledger.seq) {
        // Another epoch committed events we have not folded yet. Re-anchor the
        // read view and let the next tick decide whether we still lead.
        this.ledger.reload();
        for (const w of this.state.waiters) activate(this.sched, w.tenantId);
        throw new SeqFencedError();
      }
      const event = this.ledger.append(type, data, { expectedSeq: tail });
      if (!this.leaderLock.isOwner()) {
        log.warn('append_post_fence_lost', { epoch: this.epoch, type, seq: event.seq });
        this.ledger.reload();
        for (const w of this.state.waiters) activate(this.sched, w.tenantId);
        throw new LeaseLostError();
      }
      return event;
    }
    return this.ledger.append(type, data);
  }

  // One election attempt. Winning the file lock is only the first step: the
  // LEASE_ACQUIRED append is seq-fenced as well, so two processes that believe
  // they won (the rename micro-race) serialize at the journal tail — exactly
  // one append lands at tail+1, the loser sees the tail advanced, reloads and
  // steps down. Returns true iff we durably lead.
  tryBecomeLeader(reason) {
    if (this.stopped) return false;
    if (this.leaderLock) {
      let won;
      try {
        won = this.leaderLock.tryAcquire(this.epoch);
      } catch (err) {
        log.warn('leader_lock_acquire_failed', { reason, err: err.message });
        return false;
      }
      if (!won) return false;
      // Re-anchor folded state + seq against the winner's durable log BEFORE
      // our first append, so our seq starts at the real tail.
      this.ledger.reload();
      for (const w of this.state.waiters) activate(this.sched, w.tenantId);
    }
    try {
      if (!this.hasLed) {
        this.fencedAppend('SCHEDULER_STARTED', { epoch: this.epoch, at: nowIso() });
        this.hasLed = true;
      }
      this.fencedAppend('LEASE_ACQUIRED', {
        ownerEpoch: this.epoch,
        expiresAtMs: Date.now() + this.leaseTtlMs,
        leaseTtlMs: this.leaseTtlMs,
        reason,
      });
    } catch (err) {
      if (err instanceof LeaseLostError || err instanceof SeqFencedError) {
        // A peer actually owns the term; stay/step down (state already
        // re-anchored by the fenced append).
        log.warn('election_append_fenced', { epoch: this.epoch, reason, err: err.name });
        return false;
      }
      throw err;
    }
    log.info('lease_acquired', { epoch: this.epoch, reason });
    return true;
  }

  maintainLease() {
    if (this.stopped) return;
    if (this.isLeader()) {
      // Renew the file lock (cross-process fencing) first, then durably record
      // the renewal through the seq-fenced append. Any failure means another
      // epoch owns the term: demote now and append nothing.
      if (this.leaderLock && !this.leaderLock.renew()) {
        log.warn('lease_lost', { epoch: this.epoch });
        return;
      }
      try {
        this.fencedAppend('LEASE_ACQUIRED', {
          ownerEpoch: this.epoch,
          expiresAtMs: Date.now() + this.leaseTtlMs,
        });
      } catch (err) {
        if (err instanceof LeaseLostError || err instanceof SeqFencedError) {
          log.warn('renewal_append_fenced', { epoch: this.epoch, err: err.name });
        } else {
          throw err;
        }
      }
      return;
    }
    // Follower, or a demoted former leader: recontest only when the lease
    // recorded on disk is genuinely gone, never preempt a live leader. The
    // folded state.lease cannot be trusted here — followers append nothing, so
    // their fold never observes the winner's renewals; read the lock file.
    if (this.leaderLock) {
      const onDisk = this.leaderLock.current();
      if (onDisk && onDisk.ownerEpoch !== this.epoch && onDisk.expiresAtMs > Date.now()) return;
    } else {
      const l = this.state.lease;
      if (l && l.ownerEpoch !== this.epoch && l.expiresAtMs > Date.now()) return;
    }
    this.tryBecomeLeader('takeover');
  }

  // ---- live views over folded state ----------------------------------

  liveTenantBucket(tenantId, direction, nowMs = Date.now()) {
    const t = this.state.tenants[tenantId];
    if (!t) return null;
    const s = t.spec[direction];
    return refill(t.buckets[direction], s.ratePerSec, s.burst, nowMs);
  }

  liveClassBucket(classId, direction, nowMs = Date.now()) {
    const c = this.state.classes[classId];
    if (!c) return null;
    const s = c.spec[direction];
    return refill(c.buckets[direction], s.ratePerSec, s.burst, nowMs);
  }

  heldReservations() {
    return Object.values(this.state.reservations).filter((r) => r.status === 'HELD');
  }

  inflight(scope) {
    // scope: { tenantId?, classId?, direction? }
    return this.heldReservations().filter(
      (r) => (!scope.tenantId || r.tenantId === scope.tenantId)
        && (!scope.classId || r.classId === scope.classId)
        && (!scope.direction || r.direction === scope.direction),
    );
  }

  headWaiters(tenantId) {
    // waiters array is append-order; per-tenant per-direction relative order
    // equals arrival order. Budgets are independent per direction, so each
    // direction keeps its own FIFO head.
    const heads = { in: null, out: null };
    for (const w of this.state.waiters) {
      if (w.tenantId !== tenantId) continue;
      if (!heads[w.direction]) heads[w.direction] = w;
    }
    return [heads.in, heads.out].filter(Boolean);
  }

  // True iff the waiter can be granted RIGHT NOW under the current revisions.
  eligible(w, nowMs = Date.now()) {
    const tenant = this.state.tenants[w.tenantId];
    if (!tenant) return false;
    const tspec = tenant.spec[w.direction];
    const tb = refill(tenant.buckets[w.direction], tspec.ratePerSec, tspec.burst, nowMs);
    if (tb.tokens + 1e-9 < w.cost) return false;
    const ti = this.inflight({ tenantId: w.tenantId, direction: w.direction });
    if (ti.length >= tspec.maxInflight) return false;
    if (w.classId) {
      const cls = this.state.classes[w.classId];
      if (!cls) return false;
      const cspec = cls.spec[w.direction];
      const cb = refill(cls.buckets[w.direction], cspec.ratePerSec, cspec.burst, nowMs);
      if (cb.tokens + 1e-9 < w.cost) return false;
      const ci = this.inflight({ classId: w.classId, direction: w.direction });
      if (ci.length >= cspec.maxInflight) return false;
    }
    return true;
  }

  // ---- scheduling ----------------------------------------------------

  schedule() {
    const now = Date.now();
    // Drop waiters whose deadline passed; the reaper also does this, but
    // scheduling must never promote a timed-out waiter.
    for (const w of [...this.state.waiters]) {
      if (w.deadlineMs && w.deadlineMs <= now) {
        if (!this.isLeader()) return 0;
        this.fencedAppend('WAIT_TIMED_OUT', {
          reqId: w.reqId, tenantId: w.tenantId, at: nowIso(), waitedMs: now - w.enqueuedAtMs,
        });
      }
    }
    for (const w of this.state.waiters) activate(this.sched, w.tenantId);

    const weightsOf = (tenantId) => this.state.tenants[tenantId]?.spec.weight || 1;
    const hasWaiter = (tenantId) => this.headWaiters(tenantId).length > 0;
    const canGrant = (tenantId) => this.headWaiters(tenantId).some((w) => this.eligible(w, Date.now()));
    const grant = (tenantId) => {
      // Fence per grant: a lock stolen mid-round must stop this process from
      // appending any further events (the loser cannot keep writing).
      if (!this.isLeader()) throw new LeaseLostError();
      // Serve the earliest enqueued eligible direction head (FIFO across the
      // tenant's independent direction queues).
      const w = this.headWaiters(tenantId)
        .filter((x) => this.eligible(x, Date.now()))
        .sort((a, b) => a.enqSeq - b.enqSeq)[0];
      this.appendGrant(w, { dequeued: true });
    };

    const before = this.ledger.seq;
    runRound(this.sched, weightsOf, hasWaiter, canGrant, grant, this.maxGrantsPerTick);

    // Tenants whose queues drained leave the ring (deficit reset).
    for (const tenantId of [...this.sched.ring]) {
      if (this.headWaiters(tenantId).length === 0) deactivate(this.sched, tenantId);
    }
    return this.ledger.seq - before;
  }

  safeSchedule() {
    if (this.stopped || !this.isLeader()) return;
    try {
      this.schedule();
    } catch (err) {
      if (err instanceof LeaseLostError || err instanceof SeqFencedError) {
        log.warn('schedule_aborted_lease_lost', { epoch: this.epoch, err: err.name });
      } else {
        log.warn('schedule_failed', { err: err.message });
      }
    }
  }

  // Coalesced immediate pass after a capacity-changing event (settle/config).
  kick() {
    if (this.kickTimer) return;
    this.kickTimer = setTimeout(() => {
      this.kickTimer = null;
      this.safeSchedule();
    }, 0);
  }

  appendGrant(w, { dequeued }) {
    if (!this.isLeader()) throw new LeaseLostError();
    const now = Date.now();
    const tenant = this.state.tenants[w.tenantId];
    const cls = w.classId ? this.state.classes[w.classId] : null;
    const holdTtlMs = Math.min(w.holdTtlMs ?? tenant.spec.holdTtlMs, tenant.spec.holdTtlMs);
    const reservationId = uid('rsv_');
    this.fencedAppend('GRANTED', {
      reservationId,
      reqId: w.reqId,
      tenantId: w.tenantId,
      direction: w.direction,
      classId: w.classId || null,
      cost: w.cost,
      waiterReqId: dequeued ? w.reqId : null,
      enqSeq: dequeued ? w.enqSeq : null,
      expiresAtMs: now + holdTtlMs,
      ownerEpoch: this.epoch,
      tenantRevision: tenant.revision,
      classRevision: cls ? cls.revision : null,
    });
    return reservationId;
  }

  // ---- admission -----------------------------------------------------

  reserve(b) {
    this.requireLeader();
    const tenantId = b?.tenantId;
    const direction = b?.direction;
    if (!tenantId) throw new HttpError(400, 'tenant_required');
    if (!DIRECTIONS.includes(direction)) throw new HttpError(400, 'bad_direction', 'use in|out');
    const tenant = this.state.tenants[tenantId];
    if (!tenant) throw new HttpError(404, 'tenant_not_found', `configure tenant ${tenantId} first`);
    const classId = b.class || null;
    if (classId && !this.state.classes[classId]) throw new HttpError(404, 'class_not_found', classId);
    const cost = Number.isFinite(b.cost) ? b.cost : DEFAULTS.cost;
    if (cost <= 0) throw new HttpError(400, 'bad_cost');
    const reqId = b.requestId || uid('req_');
    const existing = this.state.byReqId[reqId];
    if (existing) return this.idempotentResponse(existing);

    const waiter = {
      reqId,
      tenantId,
      direction,
      classId,
      cost,
      enqueuedAtMs: Date.now(),
      holdTtlMs: Number.isFinite(b.holdTtlMs) ? b.holdTtlMs : tenant.spec.holdTtlMs,
      tenantRevision: tenant.revision,
      classRevision: classId ? this.state.classes[classId].revision : null,
    };

    // Fast path: no waiter queued in THIS direction (the tenant may have
    // waiters in the other independent direction) and free capacity grants
    // immediately. This never overtakes anyone, so repeated requests gain
    // nothing from re-polling.
    const sameDirectionHead = this.headWaiters(tenantId).some(
      (x) => x.direction === waiter.direction,
    );
    if (!sameDirectionHead && this.eligible(waiter)) {
      const reservationId = this.appendGrant(waiter, { dequeued: false });
      const r = this.state.reservations[reservationId];
      return {
        result: 'GRANTED',
        reservationId,
        requestId: reqId,
        tenantId,
        direction,
        expiresAt: new Date(r.expiresAtMs).toISOString(),
        expiresAtMs: r.expiresAtMs,
        tenantRevision: r.tenantRevision,
      };
    }

    // Otherwise enter the bounded waiting area behind the tenant's FIFO.
    const tenantWaiting = this.state.waiters.filter((w) => w.tenantId === tenantId).length;
    if (tenantWaiting >= tenant.spec.waitCapacity || this.state.waiters.length >= GLOBAL_WAIT_CAPACITY) {
      this.fencedAppend('WAIT_REJECTED', {
        reqId, tenantId, direction, classId,
        reason: 'WAIT_AREA_FULL',
        tenantWaiting, waitCapacity: tenant.spec.waitCapacity,
        globalWaiting: this.state.waiters.length, globalCapacity: GLOBAL_WAIT_CAPACITY,
      });
      throw new HttpError(429, 'wait_area_full', 'tenant or global waiting area is full; retry later');
    }

    const waitTimeoutMs = Number.isFinite(b.waitTimeoutMs)
      ? Math.min(b.waitTimeoutMs, tenant.spec.waitTimeoutMs || Infinity)
      : tenant.spec.waitTimeoutMs;
    waiter.deadlineMs = waitTimeoutMs > 0 ? waiter.enqueuedAtMs + waitTimeoutMs : null;

    this.fencedAppend('WAIT_ENQUEUED', { waiter });
    activate(this.sched, tenantId);
    const stored = this.state.waiters.find((w) => w.reqId === reqId);
    log.info('wait_enqueued', { reqId, tenantId, direction, enqSeq: stored.enqSeq });
    this.kick();
    return this.waitingResponse(stored);
  }

  idempotentResponse(entry) {
    if (entry.kind === 'reservation') {
      const r = this.state.reservations[entry.id];
      if (r && r.status === 'HELD') {
        return {
          result: 'GRANTED', reservationId: r.reservationId, requestId: r.reqId,
          tenantId: r.tenantId, direction: r.direction,
          expiresAt: new Date(r.expiresAtMs).toISOString(), expiresAtMs: r.expiresAtMs,
          idempotent: true,
        };
      }
      if (r) return { result: r.status, reservationId: r.reservationId, requestId: r.reqId, idempotent: true };
    }
    if (entry.kind === 'waiter') {
      const w = this.state.waiters.find((x) => x.reqId === entry.id);
      if (w) return { ...this.waitingResponse(w), idempotent: true };
    }
    return { result: entry.kind === 'timed_out' ? 'TIMED_OUT' : 'REJECTED', requestId: entry.id, idempotent: true };
  }

  waitingResponse(w) {
    const eta = this.estimateEta(w);
    return {
      result: 'WAITING',
      requestId: w.reqId,
      tenantId: w.tenantId,
      direction: w.direction,
      position: this.positionOf(w),
      enqueuedAt: new Date(w.enqueuedAtMs).toISOString(),
      deadline: w.deadlineMs ? new Date(w.deadlineMs).toISOString() : null,
      estimatedEligibleAt: eta ? new Date(eta).toISOString() : null,
      estimatedEligibleAtMs: eta,
      query: `/requests/${encodeURIComponent(w.reqId)}`,
    };
  }

  // Rank within the tenant's per-direction FIFO (1-based) plus the global
  // position across all waiting requests.
  positionOf(w) {
    let tenant = 0;
    for (const x of this.state.waiters) {
      if (x.tenantId === w.tenantId && x.direction === w.direction) {
        tenant += 1;
        if (x.reqId === w.reqId) break;
      }
    }
    return {
      tenant,
      direction: w.direction,
      global: this.state.waiters.findIndex((x) => x.reqId === w.reqId) + 1,
      totalWaiting: this.state.waiters.length,
    };
  }

  // Upper-bound ETA; see scheduler.js for the reasoning behind each term.
  estimateEta(w, nowMs = Date.now()) {
    const tenant = this.state.tenants[w.tenantId];
    if (!tenant) return null;
    let bound = nowMs;
    let contended = false;

    const addBucketTerm = (bucket, ratePerSec, burst, demand) => {
      const t = bucketReadyMs({ bucket, ratePerSec, burst, demand, nowMs });
      if (t !== null) { contended = true; bound = Math.max(bound, t); }
    };
    const addSlotTerm = (scoped, maxInflight) => {
      if (scoped.length >= maxInflight) {
        contended = true;
        const queue = scoped.map((r) => r.expiresAtMs).sort((a, b) => a - b);
        const t = slotReadyMs({ slotsInFlight: scoped.length, maxInflight, expiryQueue: queue, nowMs });
        if (t !== null) bound = Math.max(bound, t);
      }
    };

    // Predecessors inside the tenant FIFO (same direction) precede this waiter.
    let seenSelf = false;
    let tenantDemand = w.cost;
    const classPredecessors = [];
    for (const x of this.state.waiters) {
      if (x.tenantId !== w.tenantId) {
        if (w.classId && x.classId === w.classId && x.direction === w.direction) {
          classPredecessors.push(x);
        }
        continue;
      }
      if (x.reqId === w.reqId) { seenSelf = true; break; }
      if (x.direction === w.direction) tenantDemand += x.cost;
      if (w.classId && x.classId === w.classId && x.direction === w.direction) classPredecessors.push(x);
    }
    if (!seenSelf) return null;

    const tspec = tenant.spec[w.direction];
    addBucketTerm(tenant.buckets[w.direction], tspec.ratePerSec, tspec.burst, tenantDemand);
    addSlotTerm(this.inflight({ tenantId: w.tenantId, direction: w.direction }), tspec.maxInflight);

    if (w.classId) {
      const cls = this.state.classes[w.classId];
      if (cls) {
        const cspec = cls.spec[w.direction];
        const classDemand = classPredecessors.reduce((s, x) => s + x.cost, 0) + w.cost;
        addBucketTerm(cls.buckets[w.direction], cspec.ratePerSec, cspec.burst, classDemand);
        addSlotTerm(this.inflight({ classId: w.classId, direction: w.direction }), cspec.maxInflight);
      }
    }
    return contended ? bound : null;
  }

  settle(reservationId, b) {
    this.requireLeader();
    const outcome = b?.outcome === 'abort' ? 'abort' : 'complete';
    const r = this.state.reservations[reservationId];
    if (!r) throw new HttpError(404, 'reservation_not_found');
    if (r.status === 'EXPIRED') {
      // Late completion receipt for a hold that was already reclaimed: it must
      // not release anything a second time.
      return {
        result: 'LATE_AFTER_EXPIRY',
        reservationId,
        expiredAt: new Date(r.expiredAtMs).toISOString(),
        reclaimedBy: r.reclaimedBy,
        tokensReleased: 0,
      };
    }
    if (r.status !== 'HELD') {
      return { result: 'DUPLICATE', reservationId, status: r.status, outcome: r.outcome, tokensReleased: 0 };
    }
    this.requireLeader();
    this.fencedAppend('SETTLED', {
      reservationId,
      reqId: r.reqId,
      tenantId: r.tenantId,
      direction: r.direction,
      classId: r.classId,
      cost: r.cost,
      outcome,
      fromEpoch: b.epoch || null,
    });
    this.kick();
    return {
      result: 'SETTLED',
      reservationId,
      outcome,
      // Tokens are consumed permanently on completion; abort refunds them.
      tokensRefunded: outcome === 'abort' ? r.cost : 0,
    };
  }

  // ---- expiry / waiter reaping (leader only) -------------------------

  reap() {
    const now = Date.now();
    let reclaimed = 0;
    for (const r of this.heldReservations()) {
      if (r.expiresAtMs <= now) {
        // Re-check the cross-process fence before EVERY append: even losing
        // leadership partway through this batch halts reclaiming immediately,
        // so a reservation is never reclaimed by two processes (and the
        // HELD-only fold guard makes the durable log exactly-once as well).
        if (!this.isLeader()) throw new LeaseLostError();
        this.fencedAppend('EXPIRY_RECLAIMED', {
          reservationId: r.reservationId,
          reqId: r.reqId,
          tenantId: r.tenantId,
          direction: r.direction,
          classId: r.classId,
          cost: r.cost,
          ownerEpoch: this.epoch,
          grantedAtMs: r.grantedAtMs,
          expiresAtMs: r.expiresAtMs,
        });
        reclaimed += 1;
        if (reclaimed >= 1000) break;
      }
    }
    for (const w of [...this.state.waiters]) {
      if (w.deadlineMs && w.deadlineMs <= now) {
        if (!this.isLeader()) throw new LeaseLostError();
        this.fencedAppend('WAIT_TIMED_OUT', {
          reqId: w.reqId, tenantId: w.tenantId, classId: w.classId,
          at: nowIso(), waitedMs: now - w.enqueuedAtMs,
        });
      }
    }
    if (reclaimed > 0) this.kick();
    return reclaimed;
  }

  safeReap() {
    if (this.stopped || !this.isLeader()) return;
    try {
      this.reap();
    } catch (err) {
      if (err instanceof LeaseLostError || err instanceof SeqFencedError) {
        log.warn('reap_aborted_lease_lost', { epoch: this.epoch, err: err.name });
      } else {
        log.warn('reap_failed', { err: err.message });
      }
    }
  }

  // ---- config --------------------------------------------------------

  configureTenant(b) {
    this.requireLeader();
    const tenantId = b?.tenantId;
    if (!tenantId) throw new HttpError(400, 'tenant_required');
    const prev = this.state.tenants[tenantId];
    if (prev && b.expectedRevision !== undefined && Number(b.expectedRevision) !== prev.revision) {
      throw new HttpError(409, 'revision_conflict',
        `expected revision ${prev.revision}, got ${b.expectedRevision}`);
    }
    let spec;
    try {
      const patch = b.spec || {
        in: b.in, out: b.out, weight: b.weight, waitCapacity: b.waitCapacity,
        holdTtlMs: b.holdTtlMs, waitTimeoutMs: b.waitTimeoutMs,
      };
      spec = buildTenantSpec(patch, prev?.spec);
    } catch (err) {
      throw new HttpError(400, 'bad_spec', err.message);
    }
    const revision = (prev?.revision || 0) + 1;
    this.fencedAppend('BUDGET_CONFIGURED', {
      kind: 'tenant', tenantId, revision, spec,
      previousRevision: prev?.revision || null,
      operator: b.operator || 'admin',
      change: b.change || null,
    });
    log.info('tenant_budget_configured', { tenantId, revision });
    this.kick();
    return { result: 'CONFIGURED', kind: 'tenant', tenantId, revision, spec };
  }

  configureClass(b) {
    this.requireLeader();
    const classId = b?.classId;
    if (!classId) throw new HttpError(400, 'class_required');
    const prev = this.state.classes[classId];
    if (prev && b.expectedRevision !== undefined && Number(b.expectedRevision) !== prev.revision) {
      throw new HttpError(409, 'revision_conflict',
        `expected revision ${prev.revision}, got ${b.expectedRevision}`);
    }
    let spec;
    try {
      spec = buildClassSpec(b.spec || { in: b.in, out: b.out }, prev?.spec);
    } catch (err) {
      throw new HttpError(400, 'bad_spec', err.message);
    }
    const revision = (prev?.revision || 0) + 1;
    this.fencedAppend('BUDGET_CONFIGURED', {
      kind: 'class', classId, revision, spec,
      previousRevision: prev?.revision || null,
      operator: b.operator || 'admin',
      change: b.change || null,
    });
    log.info('class_budget_configured', { classId, revision });
    this.kick();
    return { result: 'CONFIGURED', kind: 'class', classId, revision, spec };
  }

  // ---- queries -------------------------------------------------------

  tenantView(tenantId, nowMs = Date.now()) {
    const t = this.state.tenants[tenantId];
    if (!t) throw new HttpError(404, 'tenant_not_found');
    const held = this.inflight({ tenantId });
    const dirs = {};
    for (const d of DIRECTIONS) {
      const spec = t.spec[d];
      const live = refill(t.buckets[d], spec.ratePerSec, spec.burst, nowMs);
      const heldDir = held.filter((r) => r.direction === d);
      dirs[d] = {
        spec,
        tokens: live.tokens,
        inflight: heldDir.length,
        inflightReservations: heldDir.map((r) => ({
          reservationId: r.reservationId,
          reqId: r.reqId,
          classId: r.classId,
          cost: r.cost,
          grantedAt: new Date(r.grantedAtMs).toISOString(),
          expiresAt: new Date(r.expiresAtMs).toISOString(),
        })),
      };
    }
    const waiting = this.state.waiters
      .filter((w) => w.tenantId === tenantId)
      .map((w) => {
        let rank = 0;
        for (const x of this.state.waiters) {
          if (x.tenantId !== tenantId || x.direction !== w.direction) continue;
          rank += 1;
          if (x.reqId === w.reqId) break;
        }
        const eta = this.estimateEta(w, nowMs);
        return {
          requestId: w.reqId,
          direction: w.direction,
          class: w.classId,
          cost: w.cost,
          position: rank,
          enqueuedAt: new Date(w.enqueuedAtMs).toISOString(),
          deadline: w.deadlineMs ? new Date(w.deadlineMs).toISOString() : null,
          estimatedEligibleAt: eta ? new Date(eta).toISOString() : null,
          estimatedEligibleAtMs: eta,
          enqueuedUnderRevision: w.tenantRevision,
        };
      });
    return {
      tenantId,
      revision: t.revision,
      weight: t.spec.weight,
      waitCapacity: t.spec.waitCapacity,
      holdTtlMs: t.spec.holdTtlMs,
      waitTimeoutMs: t.spec.waitTimeoutMs,
      directions: dirs,
      waiting,
      waitingCount: waiting.length,
    };
  }

  classView(classId, nowMs = Date.now()) {
    const c = this.state.classes[classId];
    if (!c) throw new HttpError(404, 'class_not_found');
    const held = this.inflight({ classId });
    const dirs = {};
    for (const d of DIRECTIONS) {
      const spec = c.spec[d];
      const live = refill(c.buckets[d], spec.ratePerSec, spec.burst, nowMs);
      const heldDir = held.filter((r) => r.direction === d);
      dirs[d] = {
        spec,
        tokens: live.tokens,
        inflight: heldDir.length,
        inflightReservations: heldDir.map((r) => ({
          reservationId: r.reservationId, tenantId: r.tenantId, reqId: r.reqId, cost: r.cost,
          expiresAt: new Date(r.expiresAtMs).toISOString(),
        })),
      };
    }
    const waiting = this.state.waiters.filter((w) => w.classId === classId).map((w) => ({
      requestId: w.reqId, tenantId: w.tenantId, direction: w.direction, cost: w.cost,
    }));
    return {
      classId, revision: c.revision, shared: true,
      directions: dirs, waiting, waitingCount: waiting.length,
    };
  }

  requestView(reqId, nowMs = Date.now()) {
    const entry = this.state.byReqId[reqId];
    if (!entry) throw new HttpError(404, 'request_not_found');
    if (entry.kind === 'reservation') {
      const r = this.state.reservations[entry.id];
      return {
        requestId: reqId, state: r.status,
        reservation: reservationView(r),
      };
    }
    if (entry.kind === 'waiter') {
      const w = this.state.waiters.find((x) => x.reqId === reqId);
      if (w) return { requestId: reqId, state: 'WAITING', ...this.waitingResponse(w) };
    }
    return { requestId: reqId, state: entry.kind === 'timed_out' ? 'TIMED_OUT' : 'REJECTED' };
  }

  records(tenantId, { type, limit = 200 } = {}) {
    const events = this.ledger.readEvents({ limit: 100000 }).filter((e) => {
      const d = e.data || {};
      if (d.tenantId !== tenantId && d.waiter?.tenantId !== tenantId) return false;
      if (type && e.type !== type) return false;
      return true;
    }).slice(-Number(limit));
    return { tenantId, count: events.length, events };
  }
}

function reservationView(r) {
  return {
    reservationId: r.reservationId,
    reqId: r.reqId,
    tenantId: r.tenantId,
    direction: r.direction,
    classId: r.classId,
    cost: r.cost,
    status: r.status,
    grantedAt: new Date(r.grantedAtMs).toISOString(),
    expiresAt: new Date(r.expiresAtMs).toISOString(),
    settledAt: r.settledAtMs ? new Date(r.settledAtMs).toISOString() : null,
    expiredAt: r.expiredAtMs ? new Date(r.expiredAtMs).toISOString() : null,
    outcome: r.outcome || null,
    ownerEpoch: r.ownerEpoch,
    tenantRevision: r.tenantRevision,
    classRevision: r.classRevision,
  };
}

export async function startQuota(port, dataDir, opts = {}) {
  mkdirSync(dataDir, { recursive: true });
  const ledger = new Ledger(dataDir, { fold, initial, snapshotEvery: 100 });
  await ledger.open();
  const leaseTtlMs = opts.leaseTtlMs ?? 5000;
  // The lock file lives on the SAME volume as the journal: all candidate
  // processes competing for that volume arbitrate leadership atomically in the
  // filesystem (O_CREAT|O_EXCL / atomic rename), independent of the in-memory
  // fold each process keeps.
  const leaderLock = opts.leaderLock === undefined
    ? new LeaderLock(dataDir, { ttlMs: leaseTtlMs })
    : opts.leaderLock;
  const engine = new QuotaEngine(ledger, { ...opts, leaseTtlMs, leaderLock });

  // Mutating routes: leadership may be lost between the entry check and the
  // fenced append during a takeover; surface that as the same 503 not_leader a
  // non-leader returns up front.
  const asLeader = (fn) => async (req, res, params, body) => {
    try {
      return await fn(req, res, params, body);
    } catch (err) {
      if (err instanceof LeaseLostError || err instanceof SeqFencedError) {
        throw new HttpError(503, 'not_leader', 'leadership changed during request');
      }
      throw err;
    }
  };

  const routes = [
    { method: 'GET', pattern: '/health', handler: async () => ({
      ok: true, component: 'quota', epoch: engine.epoch, leader: engine.isLeader(),
      lease: ledger.state.lease,
    }) },

    // ---- admission / settlement ------------------------------------
    { method: 'POST', pattern: '/reserve', handler: asLeader(async (req, res, p, b) => engine.reserve(b)) },
    {
      method: 'POST',
      pattern: '/reservations/:id/settle',
      handler: asLeader(async (req, res, params, b) => engine.settle(params.id, b)),
    },

    // ---- admin: budgets --------------------------------------------
    { method: 'POST', pattern: '/admin/tenants', handler: asLeader(async (req, res, p, b) => engine.configureTenant(b)) },
    { method: 'POST', pattern: '/admin/classes', handler: asLeader(async (req, res, p, b) => engine.configureClass(b)) },

    // ---- queries ----------------------------------------------------
    {
      method: 'GET',
      pattern: '/tenants/:id',
      handler: async (req, res, params) => engine.tenantView(params.id),
    },
    {
      method: 'GET',
      pattern: '/tenants',
      handler: async () => {
        const now = Date.now();
        const tenants = Object.keys(ledger.state.tenants).map((id) => ({
          tenantId: id,
          revision: ledger.state.tenants[id].revision,
          weight: ledger.state.tenants[id].spec.weight,
          waiting: ledger.state.waiters.filter((w) => w.tenantId === id).length,
          inflight: engine.inflight({ tenantId: id }).length,
          inTokens: engine.liveTenantBucket(id, 'in', now)?.tokens,
          outTokens: engine.liveTenantBucket(id, 'out', now)?.tokens,
        }));
        return { count: tenants.length, tenants };
      },
    },
    {
      method: 'GET',
      pattern: '/classes/:id',
      handler: async (req, res, params) => engine.classView(params.id),
    },
    {
      method: 'GET',
      pattern: '/classes',
      handler: async () => {
        const now = Date.now();
        const classes = Object.keys(ledger.state.classes).map((id) => ({
          classId: id,
          revision: ledger.state.classes[id].revision,
          shared: true,
          waiting: ledger.state.waiters.filter((w) => w.classId === id).length,
          inflight: engine.inflight({ classId: id }).length,
          inTokens: engine.liveClassBucket(id, 'in', now)?.tokens,
          outTokens: engine.liveClassBucket(id, 'out', now)?.tokens,
        }));
        return { count: classes.length, classes };
      },
    },
    {
      method: 'GET',
      pattern: '/requests/:id',
      handler: async (req, res, params) => engine.requestView(params.id),
    },
    {
      method: 'GET',
      pattern: '/reservations/:id',
      handler: async (req, res, params) => {
        const r = ledger.state.reservations[params.id];
        if (!r) throw new HttpError(404, 'reservation_not_found');
        return reservationView(r);
      },
    },
    {
      method: 'GET',
      pattern: '/tenants/:id/records',
      handler: async (req, res, params) => engine.records(params.id, {
        type: req.query.type, limit: req.query.limit,
      }),
    },
    {
      method: 'GET',
      pattern: '/events',
      handler: async (req) => {
        const { type, since, limit, tenantId } = req.query;
        let events = ledger.readEvents({
          type, since: since ? Number(since) : 0, limit: limit ? Number(limit) : 500,
        });
        if (tenantId) {
          events = events.filter((e) => {
            const d = e.data || {};
            return d.tenantId === tenantId || d.waiter?.tenantId === tenantId;
          });
        }
        return { events };
      },
    },
    {
      method: 'GET',
      pattern: '/waiting',
      handler: async (req) => {
        const { tenantId, direction, cls } = req.query;
        let rows = ledger.state.waiters;
        if (tenantId) rows = rows.filter((w) => w.tenantId === tenantId);
        if (direction) rows = rows.filter((w) => w.direction === direction);
        if (cls) rows = rows.filter((w) => w.classId === cls);
        const now = Date.now();
        return {
          count: rows.length,
          waiting: rows.map((w) => {
            const eta = engine.estimateEta(w, now);
            return {
              requestId: w.reqId, tenantId: w.tenantId, direction: w.direction,
              class: w.classId, cost: w.cost,
              enqueuedAt: new Date(w.enqueuedAtMs).toISOString(),
              estimatedEligibleAtMs: eta,
            };
          }),
        };
      },
    },
    {
      method: 'GET',
      pattern: '/overview',
      handler: async () => ({
        component: 'quota',
        epoch: engine.epoch,
        leader: engine.isLeader(),
        lease: ledger.state.lease,
        tenants: Object.keys(ledger.state.tenants).length,
        classes: Object.keys(ledger.state.classes).length,
        waiting: ledger.state.waiters.length,
        held: engine.heldReservations().length,
        stats: ledger.state.stats,
        lastSeq: ledger.seq,
      }),
    },
  ];

  const server = await createServer(routes, port);
  engine.start();
  return { server, ledger, engine };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.env.PORT || 8701);
  const dir = process.env.DATA_DIR || './data/quota';
  startQuota(port, dir, {
    tickMs: Number(process.env.QUOTA_TICK_MS || 50),
    leaseTtlMs: Number(process.env.QUOTA_LEASE_TTL_MS || 5000),
    reaperMs: Number(process.env.QUOTA_REAPER_MS || 200),
  }).catch((err) => {
    log.error('quota_failed', { err: err.stack || String(err) });
    process.exit(1);
  });
}
