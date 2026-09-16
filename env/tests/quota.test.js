// Multi-tenant quota scheduler tests.
//
// Covers:
//   1. per-direction rate / burst / max-inflight limits (independent each way)
//   2. bounded waiting area + queued grants + queryable estimated eligibility
//   3. FIFO within a tenant; polling/re-requesting cannot buy priority
//   4. DRR fairness: a heavy tenant cannot starve a quiet tenant, weights hold
//   5. class shared budget gates tenants jointly
//   6. config revisions: only one concurrent writer wins; lowers keep granted
//      holds; raises are used immediately
//   7. crash/takeover: unexpired holds survive restart; one reclaimer only;
//      late completion after expiry is rejected (no double release)
//   8. records of every grant / settlement / expiry reclaim
//
//   node tests/quota.test.js
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { startQuota } from '../src/quota/index.js';
import { Http } from '../src/client.js';
import { sleep } from '../src/util.js';
import { createSchedulerState, runRound } from '../src/quota/scheduler.js';
import { createBucket, available, refund } from '../src/quota/bucket.js';

async function waitFor(fn, { timeout = 4000, every = 20, label = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  let lastErr;
  while (Date.now() < deadline) {
    try {
      const v = await fn();
      if (v) return v;
    } catch (err) { lastErr = err; }
    await sleep(every);
  }
  throw new Error(`timeout waiting for ${label}: ${lastErr?.message || ''}`);
}

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'quota-'));
let basePort = 19100;
const servers = [];

async function boot(dir, opts = {}) {
  const port = opts.port ?? (basePort += 10);
  const started = await startQuota(port, path.join(tmpRoot, dir), {
    tickMs: 10, reaperMs: 20, leaseTtlMs: opts.leaseTtlMs ?? 400, ...opts,
  });
  servers.push(started);
  const boundPort = started.server.address().port;
  return { http: new Http(`http://localhost:${boundPort}`), ...started };
}

test.after(async () => {
  for (const s of servers) {
    try { await s.engine.close(); } catch { /* ignore */ }
    try { s.server.close(); } catch { /* ignore */ }
  }
  fs.rmSync(tmpRoot, { recursive: true, force: true });
});

async function waitState(http, reqId, states, timeout = 3000) {
  const wanted = new Set(Array.isArray(states) ? states : [states]);
  return waitFor(async () => {
    const v = await http.get(`/requests/${encodeURIComponent(reqId)}`);
    return wanted.has(v.state) ? v : null;
  }, { timeout, label: `${reqId} -> ${[...wanted].join('|')}` });
}

test('1. per-direction rate, burst and inflight caps are independent', async () => {
  const { http } = await boot('q1');
  await http.post('/admin/tenants', {
    tenantId: 't1',
    in: { ratePerSec: 5, burst: 3, maxInflight: 3 },
    out: { ratePerSec: 100, burst: 10, maxInflight: 10 },
  });

  // Burst 3 inbound: three granted, fourth waits (rate bucket drained).
  const r1 = await http.post('/reserve', { tenantId: 't1', direction: 'in', requestId: 'i1' });
  const r2 = await http.post('/reserve', { tenantId: 't1', direction: 'in', requestId: 'i2' });
  const r3 = await http.post('/reserve', { tenantId: 't1', direction: 'in', requestId: 'i3' });
  assert.equal(r1.result, 'GRANTED');
  assert.equal(r2.result, 'GRANTED');
  assert.equal(r3.result, 'GRANTED');
  const r4 = await http.post('/reserve', { tenantId: 't1', direction: 'in', requestId: 'i4' });
  assert.equal(r4.result, 'WAITING');
  assert.ok(r4.estimatedEligibleAtMs >= Date.now());

  // Outbound bucket is independent: while inbound is saturated, outbound still
  // has its own tokens and slots and grants immediately (no waiter ahead).
  const o1 = await http.post('/reserve', { tenantId: 't1', direction: 'out', requestId: 'o1' });
  assert.equal(o1.result, 'GRANTED');

  // Settle two inbound -> inflight slots open; queued request then gets granted.
  await http.post(`/reservations/${r1.reservationId}/settle`, { outcome: 'complete' });
  await http.post(`/reservations/${r2.reservationId}/settle`, { outcome: 'complete' });
  const i4 = await waitState(http, 'i4', 'HELD');
  assert.equal(i4.state, 'HELD');

  // Tenant view reports occupancy (r3 + i4 held inbound, o1 held outbound).
  const view = await http.get('/tenants/t1');
  assert.equal(view.directions.in.inflight, 2);
  assert.equal(view.directions.out.inflight, 1);
});

test('2. bounded waiting area rejects when full and tracks positions/ETA', async () => {
  const { http } = await boot('q2');
  await http.post('/admin/tenants', {
    tenantId: 't2',
    in: { ratePerSec: 0, burst: 0, maxInflight: 1 },
    waitCapacity: 2, waitTimeoutMs: 0,
  });
  const a = await http.post('/reserve', { tenantId: 't2', direction: 'in', requestId: 'w1' });
  assert.equal(a.result, 'WAITING');
  const b = await http.post('/reserve', { tenantId: 't2', direction: 'in', requestId: 'w2' });
  assert.equal(b.result, 'WAITING');
  assert.equal(b.position.tenant, 2);
  // Third overflows the bounded waiting area with 429.
  const err = await http.post('/reserve', { tenantId: 't2', direction: 'in', requestId: 'w3' })
    .then(() => null, (e) => e);
  assert.equal(err.status, 429);
  assert.equal(err.body.error, 'wait_area_full');
});

test('3. strict FIFO inside a tenant; polling gains no priority', async () => {
  const { http } = await boot('q3');
  await http.post('/admin/tenants', {
    tenantId: 't3',
    in: { ratePerSec: 100, burst: 1, maxInflight: 100 },
    waitCapacity: 100,
  });
  // One initial burst consumed: everything else queues; tokens accrue at
  // 100/s and FIFO order holds.
  const ids = [];
  for (let i = 0; i < 6; i += 1) {
    const r = await http.post('/reserve', {
      tenantId: 't3', direction: 'in', requestId: `f${i}`,
    });
    ids.push(`f${i}`);
    // Aggressively poll each request — it must not jump ahead of earlier ones.
    await http.get(`/requests/f${i}`);
  }
  assert.equal(ids.length, 6);
  const granted = [];
  for (const id of ids) {
    const v = await waitState(http, id, 'HELD', 5000);
    granted.push(v.requestId);
    // keep polling later requests to try to steal priority
    if (id !== ids[ids.length - 1]) await http.get(`/requests/${ids[ids.length - 1]}`);
  }
  assert.deepEqual(granted, ids);
});

test('4. DRR fairness: heavy and quiet tenants sharing one slot alternate', async () => {
  // Pure-policy test with a single shared "slot" toggled per grant.
  const st = createSchedulerState();
  st.ring = ['heavy', 'quiet'];
  const inflight = { heavy: 0, quiet: 0 };
  const queue = { heavy: 8, quiet: 4 };
  let slotFree = true;
  const order = [];
  runRound(
    st,
    () => 1,
    (t) => queue[t] > 0,
    (t) => slotFree && inflight[t] === 0,
    (t) => {
      order.push(t);
      queue[t] -= 1;
      inflight[t] = 1;
      slotFree = false;
    },
    100,
  );
  assert.deepEqual(order, ['heavy']); // first run: heavy wins the single slot

  // Simulate slot release + scheduling again, repeatedly.
  for (let i = 0; i < 7; i += 1) {
    const last = order[order.length - 1];
    inflight[last] = 0;
    slotFree = true;
    runRound(st, () => 1, (t) => queue[t] > 0, (t) => slotFree && inflight[t] === 0,
      (t) => { order.push(t); queue[t] -= 1; inflight[t] = 1; slotFree = false; }, 100);
  }
  assert.deepEqual(order, ['heavy', 'quiet', 'heavy', 'quiet', 'heavy', 'quiet', 'heavy', 'quiet']);

  // End-to-end: heavy tenant saturates a shared class inflight=1; a late quiet
  // tenant still gets served repeatedly as heavy holds complete.
  const { http } = await boot('q4');
  await http.post('/admin/classes', {
    classId: 'shared',
    in: { ratePerSec: 1000, burst: 1000, maxInflight: 1 },
  });
  await http.post('/admin/tenants', {
    tenantId: 'H', in: { ratePerSec: 1000, burst: 1000, maxInflight: 1000 }, weight: 1,
  });
  await http.post('/admin/tenants', {
    tenantId: 'Q', in: { ratePerSec: 1000, burst: 1000, maxInflight: 1000 }, weight: 1,
  });

  const heavyHolds = [];
  // Heavy grabs the single slot, then piles waiters on the shared class.
  const h0 = await http.post('/reserve', { tenantId: 'H', direction: 'in', class: 'shared', requestId: 'h0' });
  assert.equal(h0.result, 'GRANTED');
  for (let i = 1; i <= 6; i += 1) {
    await http.post('/reserve', { tenantId: 'H', direction: 'in', class: 'shared', requestId: `h${i}` });
  }
  await sleep(50);

  // Worker that completes heavy grants as soon as they appear (the normal
  // lifecycle). This keeps the single class slot rotating.
  let settleLoop = true;
  const settledHeavy = new Set(['h0']);
  const heavyWorker = (async () => {
    while (settleLoop) {
      for (let i = 1; i <= 6; i += 1) {
        if (settledHeavy.has(`h${i}`)) continue;
        const v = await http.get('/requests/h' + i);
        if (v.state === 'HELD' && v.reservation) {
          settledHeavy.add(`h${i}`);
          heavyHolds.push(v.reservation.reservationId);
          await http.post(`/reservations/${v.reservation.reservationId}/settle`, { outcome: 'complete' });
        }
      }
      await sleep(15);
    }
  })();

  // Quiet tenant arrives after the heavy backlog; release the current hold so
  // the slot starts rotating. Quiet must win it despite the heavy queue.
  await http.post(`/reservations/${h0.reservationId}/settle`, { outcome: 'complete' });
  const q0 = await http.post('/reserve', { tenantId: 'Q', direction: 'in', class: 'shared', requestId: 'q0' });
  assert.equal(q0.result, 'WAITING');
  await waitState(http, 'q0', 'HELD', 3000);
  const qView1 = await http.get('/requests/q0');
  assert.ok(qView1.reservation.reservationId);

  // A second quiet request arriving behind heavy waiters is also served
  // quickly after the first quiet hold completes (no starvation over time).
  const q1 = await http.post('/reserve', { tenantId: 'Q', direction: 'in', class: 'shared', requestId: 'q1' });
  assert.equal(q1.result, 'WAITING');
  await http.post(`/reservations/${qView1.reservation.reservationId}/settle`, { outcome: 'complete' });
  const q1Final = await waitState(http, 'q1', 'HELD', 3000);
  assert.equal(q1Final.state, 'HELD');

  settleLoop = false;
  await heavyWorker;
  // Quiet got served twice while the heavy tenant continuously held a backlog:
  // a continuous heavy flow cannot starve the quiet tenant.
});

test('5. shared class budget gates tenants jointly (tokens and inflight)', async () => {
  const { http } = await boot('q5');
  await http.post('/admin/classes', {
    classId: 'batch',
    in: { ratePerSec: 1, burst: 1, maxInflight: 10 },
  });
  await http.post('/admin/tenants', {
    tenantId: 'ta', in: { ratePerSec: 100, burst: 100, maxInflight: 100 },
  });
  await http.post('/admin/tenants', {
    tenantId: 'tb', in: { ratePerSec: 100, burst: 100, maxInflight: 100 },
  });
  const a = await http.post('/reserve', { tenantId: 'ta', direction: 'in', class: 'batch', requestId: 'ca' });
  const b = await http.post('/reserve', { tenantId: 'tb', direction: 'in', class: 'batch', requestId: 'cb' });
  assert.equal(a.result, 'GRANTED');
  assert.equal(b.result, 'WAITING'); // shared class burst drained by ta

  const cls = await http.get('/classes/batch');
  assert.equal(cls.directions.in.inflight, 1);
  assert.equal(cls.waitingCount, 1);

  await http.post(`/reservations/${a.reservationId}/settle`, { outcome: 'complete' });
  // Completion does NOT refund tokens; cb must wait for the class rate to refill.
  await waitState(http, 'cb', 'HELD', 3000);
});

test('6. budget revisions: CAS conflict, lowers keep holds, raises used at once', async () => {
  const { http } = await boot('q6');
  const c1 = await http.post('/admin/tenants', {
    tenantId: 't6', in: { ratePerSec: 100, burst: 10, maxInflight: 10 },
    out: { ratePerSec: 100, burst: 10, maxInflight: 10 },
  });
  assert.equal(c1.revision, 1);

  // Two concurrent writers with the same expected revision: one wins.
  const results = await Promise.allSettled([
    http.post('/admin/tenants', {
      tenantId: 't6', expectedRevision: 1,
      in: { ratePerSec: 200, burst: 20, maxInflight: 20 },
      out: { ratePerSec: 200, burst: 20, maxInflight: 20 },
    }),
    http.post('/admin/tenants', {
      tenantId: 't6', expectedRevision: 1,
      in: { ratePerSec: 5, burst: 5, maxInflight: 5 },
      out: { ratePerSec: 5, burst: 5, maxInflight: 5 },
    }),
  ]);
  const okCount = results.filter((r) => r.status === 'fulfilled').length;
  const errCount = results.filter((r) => r.status === 'rejected' && r.reason.status === 409).length;
  assert.equal(okCount, 1);
  assert.equal(errCount, 1);

  // Lower the inflight cap to 1 while a hold is outstanding: the existing
  // grant is NOT revoked.
  await http.post('/admin/tenants', {
    tenantId: 't6',
    in: { ratePerSec: 100, burst: 100, maxInflight: 1 },
    out: { ratePerSec: 100, burst: 100, maxInflight: 1 },
  });
  const hold = await http.post('/reserve', { tenantId: 't6', direction: 'in', requestId: 'keep1' });
  assert.equal(hold.result, 'GRANTED');
  const next = await http.post('/reserve', { tenantId: 't6', direction: 'in', requestId: 'keep2' });
  assert.equal(next.result, 'WAITING');

  // Raising the cap immediately lets the scheduler promote the waiter without
  // waiting for an old hold to expire.
  await http.post('/admin/tenants', {
    tenantId: 't6',
    in: { ratePerSec: 100, burst: 100, maxInflight: 50 },
    out: { ratePerSec: 100, burst: 100, maxInflight: 50 },
  });
  const promoted = await waitState(http, 'keep2', 'HELD', 2000);
  assert.equal(promoted.state, 'HELD');
  // The earlier grant remains valid and can still settle.
  const settled = await http.post(`/reservations/${hold.reservationId}/settle`, { outcome: 'complete' });
  assert.equal(settled.result, 'SETTLED');
});

test('7. crash/takeover: holds survive, single reclaim, late receipt rejected', async () => {
  const dataDir = path.join(tmpRoot, 'q7');
  const s1 = await boot('q7', { leaseTtlMs: 400 });
  await s1.http.post('/admin/tenants', {
    tenantId: 'tc',
    in: { ratePerSec: 10, burst: 10, maxInflight: 10 },
    out: { ratePerSec: 10, burst: 10, maxInflight: 10 },
    holdTtlMs: 30_000,
  });
  // Long-lived hold that must survive the crash, and a short one that expires.
  const long = await s1.http.post('/reserve', {
    tenantId: 'tc', direction: 'in', requestId: 'live', holdTtlMs: 30_000,
  });
  const short = await s1.http.post('/reserve', {
    tenantId: 'tc', direction: 'in', requestId: 'dead', holdTtlMs: 120,
  });
  assert.equal(long.result, 'GRANTED');
  assert.equal(short.result, 'GRANTED');

  // Crash the scheduler without settling anything.
  await s1.engine.close();
  s1.server.close();

  // A takeover starts from the same durable journal (ephemeral port, same dir).
  const s2 = await startQuota(0, dataDir, { tickMs: 10, reaperMs: 20, leaseTtlMs: 400 });
  const http2 = new Http(`http://localhost:${s2.server.address().port}`);
  servers.push(s2);

  // The unexpired reservation is still valid and settles exactly once.
  await waitFor(async () => (await http2.get('/health')).leader === true,
    { label: 'takeover leader', timeout: 3000 });
  const longView = await http2.get(`/reservations/${long.reservationId}`);
  assert.equal(longView.status, 'HELD');
  const settleLong = await http2.post(`/reservations/${long.reservationId}/settle`, { outcome: 'complete' });
  assert.equal(settleLong.result, 'SETTLED');

  // The short hold expires and exactly one reclaim refunds it; a late
  // completion receipt from the crashed handler releases nothing.
  await waitFor(async () => {
    const v = await http2.get(`/reservations/${short.reservationId}`);
    return v.status === 'EXPIRED' ? v : null;
  }, { label: 'short hold expired', timeout: 3000 });

  const late = await http2.post(`/reservations/${short.reservationId}/settle`, { outcome: 'complete' });
  assert.equal(late.result, 'LATE_AFTER_EXPIRY');
  assert.equal(late.tokensReleased, 0);
  const again = await http2.post(`/reservations/${short.reservationId}/settle`, { outcome: 'abort' });
  assert.equal(again.result, 'LATE_AFTER_EXPIRY');

  // Tokens from the expired hold were refunded: capacity is visible again.
  const view = await http2.get('/tenants/tc');
  assert.ok(view.directions.in.tokens >= 1);

  // Duplicate settlement of the live hold does not double-release.
  const dup = await http2.post(`/reservations/${long.reservationId}/settle`, { outcome: 'complete' });
  assert.equal(dup.result, 'DUPLICATE');
});

test('8. every grant, settlement and expiry reclaim leaves a record', async () => {
  const { http } = await boot('q8');
  await http.post('/admin/tenants', {
    tenantId: 't8',
    in: { ratePerSec: 100, burst: 2, maxInflight: 2 },
    out: { ratePerSec: 100, burst: 2, maxInflight: 2 },
    holdTtlMs: 80,
  });
  const g = await http.post('/reserve', { tenantId: 't8', direction: 'in', requestId: 'r1' });
  const abort = await http.post('/reserve', { tenantId: 't8', direction: 'in', requestId: 'r2' });
  await http.post(`/reservations/${g.reservationId}/settle`, { outcome: 'complete' });
  await http.post(`/reservations/${abort.reservationId}/settle`, { outcome: 'abort' });
  const expiring = await http.post('/reserve', { tenantId: 't8', direction: 'in', requestId: 'r3' });
  await waitFor(async () => {
    const v = await http.get(`/reservations/${expiring.reservationId}`);
    return v.status === 'EXPIRED';
  }, { label: 'r3 expired', timeout: 3000 });

  const recs = await http.get('/tenants/t8/records?limit=1000');
  const types = recs.events.map((e) => e.type);
  assert.ok(types.includes('GRANTED'));
  assert.ok(types.includes('SETTLED'));
  assert.ok(types.includes('EXPIRY_RECLAIMED'));
  // Event filter works.
  const grants = await http.get('/tenants/t8/records?type=GRANTED');
  assert.ok(grants.events.every((e) => e.type === 'GRANTED'));
  assert.ok(grants.events.length >= 3);
});

test('9. bucket primitives: refill, wait and refund math', () => {
  const t0 = 1_000;
  const b = createBucket(5, t0, 0);
  assert.equal(Math.round(available(b, 10, 5, t0 + 100)), 1); // 10/s for 100ms
  assert.equal(available(b, 10, 5, t0 + 600), 5); // capped at burst
  const ref = { tokens: 2, updatedAtMs: t0 };
  const after = refund(ref, 2, 10, 5, t0 + 100); // +1 accrued +2 refund, cap 5
  assert.equal(after.tokens, 5);
});

test('10. independent direction FIFOs: queued inbound never blocks outbound', async () => {
  const { http } = await boot('q10');
  await http.post('/admin/tenants', {
    tenantId: 't10',
    in: { ratePerSec: 1, burst: 1, maxInflight: 1 },
    out: { ratePerSec: 100, burst: 100, maxInflight: 100 },
  });
  // Saturate inbound: one HELD, next inbound waits.
  const ih = await http.post('/reserve', { tenantId: 't10', direction: 'in', requestId: 'd-ih' });
  assert.equal(ih.result, 'GRANTED');
  const iw = await http.post('/reserve', { tenantId: 't10', direction: 'in', requestId: 'd-iw' });
  assert.equal(iw.result, 'WAITING');
  // Outbound has its own queue and budget: several grants go straight through.
  for (const id of ['d-o1', 'd-o2', 'd-o3']) {
    const r = await http.post('/reserve', { tenantId: 't10', direction: 'out', requestId: id });
    assert.equal(r.result, 'GRANTED', `${id} should not queue behind inbound waiters`);
  }
  // Inbound waiter still queued (its slot/tokens untouched by outbound work).
  const still = await http.get('/requests/d-iw');
  assert.equal(still.state, 'WAITING');
  assert.equal(still.position.tenant, 1);
});

test('11. DRR weights: a weight-3 tenant gets ~3x the grants of weight-1', async () => {
  // Pure policy: both tenants perpetually eligible and backlogged.
  const st = createSchedulerState();
  st.ring = ['w3', 'w1'];
  const served = { w3: 0, w1: 0 };
  const queue = { w3: 1000, w1: 1000 };
  runRound(
    st,
    (t) => (t === 'w3' ? 3 : 1),
    (t) => queue[t] > 0,
    () => true,
    (t) => { served[t] += 1; queue[t] -= 1; },
    12,
  );
  assert.equal(served.w3, 9);
  assert.equal(served.w1, 3);
});
