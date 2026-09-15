// End-to-end guarantees test. Runs the whole stack in-process with ephemeral
// data dirs and high ports (no external services needed).
//
//   node tests/e2e.test.js
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { startAll } from '../bin/start-all.js';
import { Http } from '../src/client.js';
import { sleep, makeMarker, MARKER_HEADER, BRIDGE_INSTANCE } from '../src/util.js';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'msgbridge-'));
const ports = {
  brokerA: 18101, brokerB: 18201, mapper: 18301,
  deliverer: 18401, connectorA: 18501, connectorB: 18601,
};

const brokerA = new Http(`http://localhost:${ports.brokerA}`);
const brokerB = new Http(`http://localhost:${ports.brokerB}`);
const mapper = new Http(`http://localhost:${ports.mapper}`);
const deliverer = new Http(`http://localhost:${ports.deliverer}`);
const connectorA = new Http(`http://localhost:${ports.connectorA}`);
const connectorB = new Http(`http://localhost:${ports.connectorB}`);

let stack;

async function waitFor(fn, { timeout = 8000, every = 100, label = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  let lastErr;
  while (Date.now() < deadline) {
    try {
      const v = await fn();
      if (v) return v;
    } catch (err) {
      lastErr = err;
    }
    await sleep(every);
  }
  throw new Error(`timeout waiting for ${label}: ${lastErr?.message || ''}`);
}

async function waitMappedCompleted(n) {
  return waitFor(async () => {
    const ov = await mapper.get('/overview');
    return ov.totals.completed >= n ? ov : null;
  }, { label: `${n} completed mappings` });
}

test.before(async () => {
  stack = await startAll(tmpDir, ports);
  await sleep(500);
});

test.after(async () => {
  await sleep(200);
  fs.rmSync(tmpDir, { recursive: true, force: true });
  process.exit(0);
});

test('1. bidirectional A->B delivery: numbering/ack differ, body is translated', async () => {
  // Publish two native-A orders into broker A.
  const p1 = await brokerA.post('/publish', {
    headers: { trace: 't1' },
    body: { id: 'order-1', type: 'order', amount: 100, currency: 'USD', customerRef: 'C1', at: '2026-09-15T10:00:00Z' },
  });
  const p2 = await brokerA.post('/publish', {
    body: { id: 'order-2', type: 'order', amount: 250, currency: 'EUR', customerRef: 'C2', at: '2026-09-15T10:01:00Z' },
  });
  assert.equal(p1.ok, true);

  // They must land on B translated into B's event envelope.
  const bState = await waitFor(async () => {
    const s = await brokerB.get('/admin/state');
    const orders = s.messages.filter((m) => m.body?.event === 'orderCreated');
    return orders.length >= 2 ? s : null;
  }, { label: 'A->B translated messages' });

  const onB = bState.messages.filter((m) => m.body?.event === 'orderCreated');
  assert.equal(onB.length, 2);
  assert.deepEqual(onB[0].body, {
    eventId: 'order-1', event: 'orderCreated', total: 100, ccy: 'USD',
    ref: 'C1', occurredAt: '2026-09-15T10:00:00Z',
  });
  // Header preserved across the bridge.
  assert.equal(onB[0].headers.trace, 't1');
  // Bridge marker present for loop control.
  assert.match(onB[0].headers[MARKER_HEADER], /^v1:bridge-1:A:m_/);

  // B ack is cumulative: its watermark advanced past the bridged messages only
  // after A individually acked its source messages (which already happened).
  const aState = await brokerA.get('/admin/state');
  assert.ok(aState.messages.find((m) => m.seq === p1.seq).done);
  assert.ok(aState.messages.find((m) => m.seq === p2.seq).done);

  // Every message has a queryable chain mapping its A seq to a B seq.
  await waitMappedCompleted(2);
  const ms = await mapper.get('/mappings?side=A');
  assert.equal(ms.count, 2);
  for (const m of ms.mappings) {
    assert.equal(m.originSide, 'A');
    assert.ok(m.peerSeq, 'peer B seq recorded');
    assert.equal(m.egressStatus, 'PUBLISHED');
    assert.equal(m.sourceAcked, true);
    assert.equal(m.completed, true);
    const chain = await mapper.get(`/chain/${m.mappingId}`);
    const types = chain.events.map((e) => e.type);
    assert.ok(types.includes('MAPPING_CREATED'));
    assert.ok(types.includes('EGRESS_ENQUEUED'));
    assert.ok(types.includes('EGRESS_PUBLISHED'));
    assert.ok(types.includes('SOURCE_ACKED'));
  }
});

test('2. reverse B->A direction works and maps back to the A body shape', async () => {
  await brokerB.post('/publish', {
    body: {
      eventId: 'order-9', event: 'orderCreated', total: 42, ccy: 'GBP',
      ref: 'C9', occurredAt: '2026-09-15T11:00:00Z',
    },
  });
  const aState = await waitFor(async () => {
    const s = await brokerA.get('/admin/state');
    const hit = s.messages.find((m) => m.body?.type === 'order' && m.body.id === 'order-9');
    return hit ? s : null;
  }, { label: 'B->A translated message' });
  const hit = aState.messages.find((m) => m.body?.id === 'order-9');
  assert.deepEqual(hit.body, {
    id: 'order-9', type: 'order', amount: 42, currency: 'GBP',
    customerRef: 'C9', at: '2026-09-15T11:00:00Z',
  });
  // B's cumulative watermark moved up.
  const bState = await brokerB.get('/admin/state');
  assert.ok(bState.ackWatermark >= 1);
  await waitMappedCompleted(3);
});

test('3. loop suppression: reflected bridged message is acked, never re-forwarded', async () => {
  // The message B received in test 1 carries our marker. Simulate the loop:
  // publish it back onto broker A exactly as a reflecting peer would.
  const bState0 = await brokerB.get('/admin/state');
  const bridged = bState0.messages.find((m) => m.body?.eventId === 'order-1');
  assert.ok(bridged, 'bridged message exists on B');
  const reflected = await brokerA.post('/publish', {
    headers: { [MARKER_HEADER]: bridged.headers[MARKER_HEADER] },
    body: bridged.body,
  });
  const reflectedSeq = reflected.seq;

  // Connector A must ack (suppress) it and NOT enqueue another delivery.
  await waitFor(async () => {
    const a = await brokerA.get('/admin/state');
    const m = a.messages.find((x) => x.seq === reflectedSeq);
    return m && m.done ? m : null;
  }, { label: 'reflected message acked by ingress' });

  // Give the pump a moment; B must not gain a second copy of order-1.
  await sleep(600);
  const bState1 = await brokerB.get('/admin/state');
  const copies = bState1.messages.filter((m) => m.body?.event === 'orderCreated' && m.body.eventId === 'order-1');
  assert.equal(copies.length, 1, 'exactly one effective delivery despite the loop');

  // The suppression is recorded on the chain for the original mapping.
  const ms = await mapper.get('/mappings?side=A&limit=100');
  const m1 = ms.mappings.find((x) => x.bizKey === 'order-1');
  const chain = await mapper.get(`/chain/${m1.mappingId}`);
  assert.ok(
    chain.events.some((e) => e.type === 'LOOP_SUPPRESSED'),
    'LOOP_SUPPRESSED evidence present on chain',
  );
  const ev = await mapper.get('/events?type=LOOP_SUPPRESSED');
  assert.ok(ev.events.length >= 1);
});

test('4. body mapping failure leaves a queryable failed chain; poison is acked, replay works', async () => {
  const p = await brokerA.post('/publish', {
    body: { id: 'order-bad', type: 'order', amount: 1, currency: 'X', __mapFail: true },
  });
  await waitFor(async () => {
    const ms = await mapper.get('/mappings?status=MAP_FAILED');
    return ms.count >= 1 ? ms : null;
  }, { label: 'MAP_FAILED mapping' });

  const failed = (await mapper.get('/mappings?status=MAP_FAILED')).mappings[0];
  assert.equal(failed.status, 'MAP_FAILED');
  const chain = await mapper.get(`/chain/${failed.mappingId}`);
  assert.ok(chain.events.some((e) => e.type === 'MAP_FAILED'));

  // Poison message was acked on the source so the queue does not spin.
  const a = await brokerA.get('/admin/state');
  assert.ok(a.messages.find((m) => m.seq === p.seq).done);

  // Operator corrects the body and retries -> new effective delivery on B.
  const remap = await mapper.post(`/mappings/${failed.mappingId}/retry`, {
    body: { id: 'order-fixed', type: 'order', amount: 9, currency: 'USD', customerRef: 'CF', at: '2026-09-15T12:00:00Z' },
  });
  assert.equal(remap.result, 'REMAPPED');
  await deliverer.post('/deliver', {
    mappingId: remap.mappingId,
    originSide: remap.originSide,
    targetSide: 'B',
    direction: remap.direction,
    ingressSeq: remap.ingressSeq,
    headers: remap.mappedHeaders,
    body: remap.mappedBody,
  });
  await waitFor(async () => {
    const s = await brokerB.get('/admin/state');
    return s.messages.some((m) => m.body?.eventId === 'order-fixed') ? s : null;
  }, { label: 'replayed corrected delivery' });
});

test('5. partial batch success: failed item retried, others delivered once', async () => {
  // Batch of 3 to the same target (B); the middle one fails its FIRST attempt.
  await brokerA.post('/publish', {
    body: { id: 'pb-1', type: 'order', amount: 1, currency: 'USD', customerRef: 'x', at: 't' },
  });
  await brokerA.post('/publish', {
    body: { id: 'pb-2', type: 'order', amount: 2, currency: 'USD', customerRef: 'x', at: 't', __egressFailOnce: true },
  });
  await brokerA.post('/publish', {
    body: { id: 'pb-3', type: 'order', amount: 3, currency: 'USD', customerRef: 'x', at: 't' },
  });
  await waitFor(async () => {
    const s = await brokerB.get('/admin/state');
    const hit = ['pb-1', 'pb-2', 'pb-3'].every((id) =>
      s.messages.some((m) => m.body?.eventId === id));
    return hit ? s : null;
  }, { timeout: 12000, label: 'all partial-batch items eventually delivered' });

  // The failure and the retry are both in the chain.
  const ms = await mapper.get('/mappings?side=A&limit=200');
  const pb2 = ms.mappings.find((m) => m.bizKey === 'pb-2');
  const chain = await mapper.get(`/chain/${pb2.mappingId}`);
  assert.ok(chain.events.some((e) => e.type === 'EGRESS_FAILED'), 'failure recorded');
  assert.ok(chain.events.some((e) => e.type === 'EGRESS_PUBLISHED'), 'later success recorded');

  // Exactly once: each pb id occurs one time on B.
  const b = await brokerB.get('/admin/state');
  for (const id of ['pb-1', 'pb-2', 'pb-3']) {
    const n = b.messages.filter((m) => m.body?.eventId === id).length;
    assert.equal(n, 1, `${id} delivered exactly once`);
  }
});

test('6. same-sequence conflict freezes the segment; manual skip resolves it', async () => {
  // Publish onto B; hold its egress to A open in a failure window so the source
  // B message stays inflight/unacked, then rewrite the same B seq content.
  // Hold egress open long enough that the conflicting rewrite is observed
  // BEFORE the first successful publish, but short enough that the original
  // delivery still completes after the segment is resolved.
  const holdUntil = Date.now() + 15000;
  const pub = await brokerB.post('/publish', {
    body: { eventId: 'cf-1', event: 'orderCreated', total: 5, ccy: 'USD', ref: 'r', occurredAt: 't', __egressFailUntil: holdUntil },
  });
  const seq = pub.seq;

  // Wait until the connector has ingested the OLD content for this seq.
  await waitFor(async () => {
    const st = await connectorB.get('/state');
    const s = st.seen.find((x) => x.seq === String(seq));
    return s && ['ENQUEUED', 'MAPPED'].includes(s.status) && s.hash ? s : null;
  }, { label: 'cf-1 old content inflight at connector B' });
  const preHash = (await connectorB.get('/state')).seen.find((x) => x.seq === String(seq)).hash;

  // Broker-side rewrite of the SAME sequence => content conflict.
  const rw = await brokerB.post(`/admin/rewrite/${seq}`, {
    body: { eventId: 'cf-1-CHANGED', event: 'orderCreated', total: 6, ccy: 'EUR', ref: 'r2', occurredAt: 't' },
  });
  assert.equal(rw.ok, true);

  const fz = await waitFor(async () => {
    const f = await mapper.get('/freezes?status=FROZEN');
    const mine = f.freezes.find((x) => x.side === 'B' && x.ingressSeq === String(seq));
    return mine || null;
  }, { label: 'freeze opened for conflicting seq' });
  assert.equal(fz.storedHash, preHash);

  // While frozen, A never receives the changed message.
  await sleep(500);
  const aNow = await brokerA.get('/admin/state');
  assert.ok(!aNow.messages.some((m) => m.body?.id === 'cf-1-CHANGED'));

  // Operator chooses SKIP: keep the original local mapping, drop the rewrite,
  // advance the cumulative B ack over the segment.
  const resolve = await mapper.post(`/freeze/${fz.freezeId}/resolve`, { decision: 'skip' });
  assert.equal(resolve.decision, 'skip');
  await connectorB.post('/ingress/release', { seq: String(seq), action: 'skip', mappingId: fz.mappingId });

  await waitFor(async () => {
    const s = await brokerB.get('/admin/state');
    return s.ackWatermark >= seq ? s : null;
  }, { label: 'B watermark advances past resolved segment' });

  const openAfter = await mapper.get('/freezes?status=FROZEN');
  assert.ok(!openAfter.freezes.some((x) => x.freezeId === fz.freezeId));
  const chain = await mapper.get(`/chain/${fz.mappingId || 'none'}`).catch(() => null);
  // resolution event is queryable from freezes list
  const all = await mapper.get('/freezes');
  const mine = all.freezes.find((x) => x.freezeId === fz.freezeId);
  assert.equal(mine.status, 'RESOLVED_SKIP');
});

test('7. duplicate ack is detected and recorded', async () => {
  const ms = await mapper.get('/mappings?side=A&limit=200');
  const m = ms.mappings.find((x) => x.bizKey === 'order-1');
  // Re-ack the same source seq directly: mapper must flag DUPLICATE.
  const r1 = await mapper.post('/ack', { side: 'A', seq: m.ingressSeq, mappingId: m.mappingId, reason: 'test' });
  assert.equal(r1.result, 'DUPLICATE');
  const ev = await mapper.get('/events?type=SOURCE_ACK_DUPLICATE');
  assert.ok(ev.events.length >= 1);
});

test('8. connector restart: no loss, resume from correct seq, no duplicate delivery', async () => {
  const stack2 = stack;
  // Stop connector B entirely (simulates outage / redeploy).
  await stack2.stopB();
  // A-side publish while B is disconnected: mapper/deliverer persist it.
  await brokerA.post('/publish', {
    body: { id: 'restart-1', type: 'order', amount: 7, currency: 'JPY', customerRef: 'R1', at: 't' },
  });
  await sleep(700);
  // Nothing on B yet (no egress connector), and nothing lost.
  const del = await deliverer.get('/deliveries');
  const queued = del.deliveries.find((d) => d.body?.eventId === 'restart-1');
  assert.ok(queued, 'message retained in deliverer outbox while peer is down');
  assert.notEqual(queued.status, 'DONE');

  // Restart connector B from its persisted data dir.
  await stack2.startB();
  const b = await waitFor(async () => {
    const s = await brokerB.get('/admin/state');
    return s.messages.some((m) => m.body?.eventId === 'restart-1') ? s : null;
  }, { timeout: 10000, label: 'restart-1 delivered after connector restart' });
  assert.equal(b.messages.filter((m) => m.body?.eventId === 'restart-1').length, 1);

  // Restart event is part of the queryable chain.
  await waitFor(async () => {
    const e = await mapper.get('/events?type=CONNECTOR_STARTED');
    return e.events.length >= 2 ? e : null;
  }, { label: 'connector restart evidence' });
});

test('10. conflict override accepts the incoming content via a superseding mapping', async () => {
  const holdUntil = Date.now() + 15000;
  const pub = await brokerB.post('/publish', {
    body: { eventId: 'cf-old', event: 'orderCreated', total: 1, ccy: 'USD', ref: 'r', occurredAt: 't', __egressFailUntil: holdUntil },
  });
  const seq = pub.seq;
  await waitFor(async () => {
    const st = await connectorB.get('/state');
    const s = st.seen.find((x) => x.seq === String(seq));
    return s && ['ENQUEUED', 'MAPPED'].includes(s.status) && s.hash ? s : null;
  }, { label: 'cf-old inflight' });

  await brokerB.post(`/admin/rewrite/${seq}`, {
    body: { eventId: 'cf-new', event: 'orderCreated', total: 2, ccy: 'EUR', ref: 'r2', occurredAt: 't' },
  });
  const fz = await waitFor(async () => {
    const f = await mapper.get('/freezes?status=FROZEN');
    return f.freezes.find((x) => x.side === 'B' && x.ingressSeq === String(seq)) || null;
  }, { label: 'override freeze opened' });

  // Resolve as override on the mapper, then release at the connector which
  // pulls the new content, remaps (superseding), and delivers it.
  await mapper.post(`/freeze/${fz.freezeId}/resolve`, { decision: 'override' });
  const rel = await connectorB.post('/ingress/release', {
    seq: String(seq), action: 'override', mappingId: fz.mappingId, freezeId: fz.freezeId,
  });
  assert.equal(rel.action, 'override');
  const newMappingId = rel.mappingId;
  assert.notEqual(newMappingId, fz.mappingId);

  // Peer A receives the NEW body (id cf-new), not the old one.
  await waitFor(async () => {
    const s = await brokerA.get('/admin/state');
    return s.messages.some((m) => m.body?.id === 'cf-new') ? s : null;
  }, { timeout: 10000, label: 'overridden content delivered' });

  // Old mapping is recorded as superseded; chain is queryable both ways.
  const chainOld = await mapper.get(`/chain/${fz.mappingId}`);
  assert.ok(chainOld.events.some((e) => e.type === 'MAPPING_SUPERSEDED'));
  const chainNew = await mapper.get(`/chain/${newMappingId}`);
  assert.ok(chainNew.events.some((e) => e.type === 'MAPPING_CREATED'));
  assert.ok(chainNew.events.some((e) => e.type === 'EGRESS_PUBLISHED'));

  // B watermark advances once the new delivery completes.
  await waitFor(async () => {
    const s = await brokerB.get('/admin/state');
    return s.ackWatermark >= seq ? s : null;
  }, { timeout: 10000, label: 'B watermark advances after override' });
});

test('9. overview chain is coherent at the end', async () => {
  const ov = await mapper.get('/overview');
  assert.ok(ov.totals.mappings >= 8);
  assert.ok(ov.totals.completed >= 6);
  assert.equal(ov.freezes.open, 0);
});
