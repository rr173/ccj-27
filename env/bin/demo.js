// Guided demonstration of every bridge guarantee. Runs the full stack
// in-process and narrates each scenario against the live HTTP APIs.
//
//   npm run demo
import { startAll } from './start-all.js';
import { Http } from '../src/client.js';
import { sleep, MARKER_HEADER } from '../src/util.js';

const ports = {
  brokerA: 18111, brokerB: 18211, mapper: 18311,
  deliverer: 18411, connectorA: 18511, connectorB: 18611,
};

const A = new Http(`http://localhost:${ports.brokerA}`);
const B = new Http(`http://localhost:${ports.brokerB}`);
const M = new Http(`http://localhost:${ports.mapper}`);
const D = new Http(`http://localhost:${ports.deliverer}`);
const CA = new Http(`http://localhost:${ports.connectorA}`);
const CB = new Http(`http://localhost:${ports.connectorB}`);

function h(title) {
  console.log('\n=== ' + title + ' ===');
}
async function overview() {
  const [m, d] = await Promise.all([M.get('/overview'), D.get('/overview')]);
  console.log('mapper   :', JSON.stringify(m.totals), 'freezes', JSON.stringify(m.freezes));
  console.log('deliverer:', JSON.stringify(d.totals), 'pendingCallbacks', d.callbacksPending);
}

const stack = await startAll('./data-demo', ports);
await sleep(600);

h('1) normal A->B delivery (A individual ack, B cumulative; bodies translated)');
await A.post('/publish', { body: { id: 'demo-1', type: 'order', amount: 100, currency: 'USD', customerRef: 'C1', at: '2026-09-15T10:00:00Z' } });
await sleep(700);
let b = await B.get('/admin/state');
console.log('B now holds:', JSON.stringify(b.messages[0].body));

h('2) reverse B->A');
await B.post('/publish', { body: { eventId: 'demo-2', event: 'orderCreated', total: 55, ccy: 'GBP', ref: 'C2', occurredAt: 't' } });
await sleep(700);
const a = await A.get('/admin/state');
console.log('A now holds the reverse-mapped:', JSON.stringify(a.messages.find((m) => m.body.id === 'demo-2').body));

h('3) loop suppression (reflect the bridged message straight back)');
const bridged = (await B.get('/admin/state')).messages[0];
await A.post('/publish', { headers: { [MARKER_HEADER]: bridged.headers[MARKER_HEADER] }, body: bridged.body });
await sleep(700);
console.log('copies of demo-1 on B after reflection:',
  (await B.get('/admin/state')).messages.filter((m) => m.body.eventId === 'demo-1').length,
  '(must be 1)');

h('4) body mapping failure -> queryable failed chain, poison acked, replay');
await A.post('/publish', { body: { id: 'demo-bad', type: 'order', amount: 1, currency: 'X', __mapFail: true } });
await sleep(600);
const failed = (await M.get('/mappings?status=MAP_FAILED')).mappings[0];
console.log('failed mapping:', failed.mappingId);
const remap = await M.post(`/mappings/${failed.mappingId}/retry`, {
  body: { id: 'demo-fixed', type: 'order', amount: 9, currency: 'USD', customerRef: 'CF', at: 't' },
});
await D.post('/deliver', {
  mappingId: remap.mappingId, originSide: 'A', targetSide: 'B', direction: remap.direction,
  ingressSeq: remap.ingressSeq, headers: remap.mappedHeaders, body: remap.mappedBody,
});
await sleep(700);
console.log('after replay, B has demo-fixed:',
  (await B.get('/admin/state')).messages.some((m) => m.body.eventId === 'demo-fixed'));

h('5) partial batch (one item fails its first attempt, then succeeds exactly once)');
for (const [id, once] of [['pb-1', false], ['pb-2', true], ['pb-3', false]]) {
  const body = { id, type: 'order', amount: 1, currency: 'USD', customerRef: 'x', at: 't' };
  if (once) body.__egressFailOnce = true;
  await A.post('/publish', { body });
}
await sleep(3500);
const bs = await B.get('/admin/state');
for (const id of ['pb-1', 'pb-2', 'pb-3']) {
  console.log(`  ${id} copies:`, bs.messages.filter((m) => m.body.eventId === id).length);
}

h('6) same-sequence conflict -> freeze segment -> manual choice');
const pub = await B.post('/publish', { body: { eventId: 'cf', event: 'orderCreated', total: 1, ccy: 'USD', ref: 'r', occurredAt: 't', __egressFailUntil: Date.now() + 15000 } });
await sleep(700);
await B.post(`/admin/rewrite/${pub.seq}`, { body: { eventId: 'cf-CHANGED', event: 'orderCreated', total: 2, ccy: 'EUR', ref: 'r2', occurredAt: 't' } });
await sleep(1200);
const fz = (await M.get('/freezes?status=FROZEN')).freezes[0];
console.log('frozen segment:', fz && { side: fz.side, seq: fz.ingressSeq, stored: fz.storedHash.slice(0, 8), incoming: fz.incomingHash.slice(0, 8) });
console.log('  choose with: node bin/bridgectl.js freeze ' + fz.freezeId + ' resolve skip|override|keep_local');
await M.post(`/freeze/${fz.freezeId}/resolve`, { decision: 'skip' });
await CB.post('/ingress/release', { seq: fz.ingressSeq, action: 'skip', mappingId: fz.mappingId });
await sleep(500);

h('7) inspect a delivery chain');
const first = (await M.get('/mappings?side=A&limit=1')).mappings[0];
const chain = await M.get(`/chain/${first.mappingId}`);
console.log('chain for', first.mappingId, ':');
for (const e of chain.events) console.log('  ', e.ts, e.type);

h('overview');
await overview();
console.log('\nDemo finished. Data is under ./data-demo (safe to delete).');
process.exit(0);
