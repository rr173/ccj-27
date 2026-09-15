// Connector: adapts ONE messaging system (A or B) to the bridge.
//
// One process hosts both loops of a side:
//   ingress : pull -> loop/marker check -> sequence-conflict check ->
//             mapper.map -> deliverer.deliver ; source ack happens only after
//             the peer publish is confirmed (callback /ingress/complete)
//   egress  : receive batches from the deliverer -> publish to local broker
//             (idempotent) -> cumulative/individual commit of outbound seqs
//
// A and B differ here in exactly two ways (everything else is shared):
//   A ack: individual  POST /ack/:seq
//   B ack: cumulative  POST /ack {seq}  (watermark; cannot skip gaps)
import { mkdirSync } from 'node:fs';
import { Ledger } from '../store/ledger.js';
import { createServer, HttpError } from '../http.js';
import { Http } from '../client.js';
import { logger } from '../log.js';
import {
  uid, bodyHash, parseMarker, MARKER_HEADER, BRIDGE_INSTANCE,
  sleep, nowIso,
} from '../util.js';

const log = (side) => logger(`connector-${side.toLowerCase()}`);

// Largest contiguous run of completed inbound seqs starting at watermark+1.
function contiguousDone(state, fromWatermark) {
  let w = fromWatermark;
  const seen = state.seen;
  while (seen[String(w + 1)] && seen[String(w + 1)].done) w += 1;
  // Completion may also fill behind a stale local watermark after restart:
  // re-base from 1 up to the first gap.
  let fromZero = 0;
  while (seen[String(fromZero + 1)] && seen[String(fromZero + 1)].done) fromZero += 1;
  return Math.max(w, fromZero);
}

// Largest contiguous run of outbound peer seqs actually published (B side).
function contiguousPeerSeqs(state) {
  const seqSet = new Set(
    Object.values(state.published).map((r) => Number(r.peerSeq)).filter((n) => Number.isInteger(n)),
  );
  let w = state.outboundCommitWatermark;
  while (seqSet.has(w + 1)) w += 1;
  return w;
}

// Remove bridge-only double-underscore control keys before broker publish.
function stripControlKeys(value) {
  if (Array.isArray(value)) return value.map(stripControlKeys);
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (k.startsWith('__')) continue;
      out[k] = stripControlKeys(v);
    }
    return out;
  }
  return value;
}


function initial() {
  return {
    // ingress: `${seq}` -> { seq, hash, attempt, inflight, mappingId, status, done, marker }
    seen: {},
    // egress: idempotencyKey -> { deliveryId, peerSeq, status }
    published: {},
    // B-side cumulative watermark of INBOUND messages fully completed+acked
    inboundAckWatermark: 0,
    // broker-B's own watermark learned from pull (resume lower bound)
    brokerAckWatermark: 0,
    // B-side contiguous watermark of OUTBOUND messages successfully published
    outboundCommitWatermark: 0,
    runId: null,
  };
}

function fold(state, event) {
  const d = event.data || {};
  switch (event.type) {
    case 'RUN_STARTED':
      state.runId = d.runId;
      break;
    case 'INGRESS_SEEN': {
      const existing = state.seen[String(d.seq)];
      if (existing) {
        existing.attempt = Math.max(existing.attempt, d.attempt || 1);
        existing.inflight = d.inflight;
      } else {
        state.seen[String(d.seq)] = {
          seq: String(d.seq), hash: d.hash, attempt: d.attempt || 1,
          inflight: d.inflight, marker: !!d.marker, status: 'SEEN',
        };
      }
      break;
    }
    case 'INGRESS_MAPPING': {
      const s = state.seen[String(d.seq)];
      if (s) {
        s.mappingId = d.mappingId;
        s.status = d.result === 'NEW' ? 'MAPPED' : d.result;
      }
      break;
    }
    case 'INGRESS_FROZEN': {
      const s = state.seen[String(d.seq)];
      if (s) {
        s.status = 'FROZEN';
        s.freezeId = d.freezeId;
      }
      break;
    }
    case 'INGRESS_MAP_FAILED': {
      const s = state.seen[String(d.seq)];
      if (s) s.status = 'MAP_FAILED';
      break;
    }
    case 'INGRESS_ENQUEUED': {
      const s = state.seen[String(d.seq)];
      if (s) {
        s.deliveryId = d.deliveryId;
        s.status = 'ENQUEUED';
      }
      break;
    }
    case 'INGRESS_DROPPED': {
      const s = state.seen[String(d.seq)];
      if (s) s.status = 'DROPPED';
      break;
    }
    case 'INGRESS_COMPLETED': {
      const s = state.seen[String(d.seq)];
      if (s) {
        s.status = 'COMPLETED';
        s.peerSeq = d.peerSeq;
        s.done = true;
      }
      // Cumulative watermark = largest contiguous run of completed seqs
      // (completions can land out of order: publish n may confirm before n-1).
      if (d.side === 'B' && d.ackResult !== 'DUPLICATE') {
        state.inboundAckWatermark = contiguousDone(state, state.inboundAckWatermark);
      }
      break;
    }
    case 'BROKER_WATERMARK': {
      state.brokerAckWatermark = Math.max(state.brokerAckWatermark, d.watermark || 0);
      break;
    }
    case 'EGRESS_PUBLISHED_LOCAL': {
      state.published[d.idempotencyKey] = {
        deliveryId: d.deliveryId, peerSeq: String(d.peerSeq),
        key: d.idempotencyKey, status: 'PUBLISHED',
      };
      if (d.side === 'B') {
        state.outboundCommitWatermark = contiguousPeerSeqs(state);
      }
      break;
    }
    case 'EGRESS_COMMITTED': {
      const rec = state.published[d.idempotencyKey];
      if (rec) rec.status = 'COMMITTED';
      break;
    }
    default:
      break;
  }
  return state;
}

export async function startConnector(side, port, dataDir, endpoints) {
  if (!['A', 'B'].includes(side)) throw new Error(`bad side ${side}`);
  mkdirSync(dataDir, { recursive: true });
  const lg = log(side);
  const ledger = new Ledger(dataDir, { fold, initial, snapshotEvery: 50 });
  await ledger.open();

  const broker = new Http(endpoints.broker);
  const mapper = new Http(endpoints.mapper);
  const deliverer = new Http(endpoints.deliverer);

  const TICK_MS = Number(process.env.POLL_MS || 200);
  const VIS_MS = Number(process.env.VISIBILITY_MS || 8000);
  const running = { v: true };
  const runId = uid('run_');
  ledger.append('RUN_STARTED', { runId, side });

  // ---- ingress loop -------------------------------------------------------
  async function ingressLoop() {
    // Announce (re)start so the chain records restart evidence + resume point.
    try {
      await mapper.post('/connector/started', {
        side, runId,
        resumedFrom: side === 'B' ? ledger.state.inboundAckWatermark : null,
      }, { retries: 5, retryDelayMs: 500 });
    } catch (err) {
      lg.warn('start_announce_failed', { err: err.message });
    }

    while (running.v) {
      try {
        await ingressTick();
      } catch (err) {
        // Broker (or dependency) offline: other side's already-confirmed
        // messages remain durable in mapper/deliverer ledgers; we simply retry.
        lg.warn('ingress_tick_failed', { err: err.message });
      }
      await sleep(TICK_MS);
    }
  }

  async function ingressTick() {
    const pullBody = side === 'A'
      ? { max: 10, visibilityMs: VIS_MS }
      : { max: 10 };
    const pulled = await broker.post('/pull', pullBody, { timeoutMs: 5000 });
    if (side === 'B' && typeof pulled.ackWatermark === 'number') {
      if (pulled.ackWatermark !== ledger.state.brokerAckWatermark) {
        ledger.append('BROKER_WATERMARK', { watermark: pulled.ackWatermark });
      }
    }

    for (const msg of pulled.messages) {
      await handleIngressMessage(msg);
    }
  }

  async function handleIngressMessage(msg) {
    const seq = String(msg.seq);
    const headers = msg.headers || {};
    const marker = parseMarker(headers[MARKER_HEADER]);
    const hash = bodyHash(msg.body);
    const seen = ledger.state.seen[seq];

    ledger.append('INGRESS_SEEN', {
      seq, hash, attempt: msg.attempt || 1,
      inflight: true, marker: !!marker,
    });

    // --- loop suppression ---------------------------------------------------
    if (marker) {
      if (marker.instance === BRIDGE_INSTANCE) {
        // This is our own message coming straight back. Do NOT map/publish it
        // again (that would be the infinite amplification loop).
        ledger.append('LOOP_SUPPRESSED_LOCAL', {
          seq, mappingId: marker.mappingId, originSide: marker.originSide, ttl: marker.ttl,
        });
        try {
          await deliverer.post('/loop-suppressed', {
            side, originSide: marker.originSide, mappingId: marker.mappingId,
            reason: 'own_bridge_reflection', ttl: marker.ttl,
          }, { retries: 3 });
        } catch { /* chain evidence is best effort */ }
        await ackSource(seq, { marker: true, mappingId: marker.mappingId });
        return;
      }
      if (marker.ttl <= 0) {
        ledger.append('LOOP_SUPPRESSED_LOCAL', { seq, reason: 'ttl_exhausted' });
        await deliverer.post('/loop-suppressed', { side, reason: 'ttl_exhausted' }, { retries: 2 }).catch(() => {});
        await ackSource(seq, { marker: true });
        return;
      }
      // Foreign-bridge loop: forward on with ttl decremented (marker rewritten
      // at publish time by the deliverer, which sets fresh ttl; the egress here
      // decrements as a defensive bound).
    }

    // --- sequence conflict detection ---------------------------------------
    // Same sequence number, different content while the prior copy is not
    // finished and is not a benign broker redelivery -> freeze the segment.
    if (seen && seen.hash !== hash && !seen.done && seen.status !== 'FROZEN') {
      const res = await mapper.post('/freeze', {
        side, seq, storedHash: seen.hash, incomingHash: hash,
        reason: side === 'A' ? 'content_changed_on_redelivery' : 'content_changed_under_same_seq',
        mappingId: seen.mappingId || null,
      }, { retries: 3 });
      ledger.append('INGRESS_FROZEN', { seq, freezeId: res.freezeId });
      lg.warn('segment_frozen', { seq, storedHash: seen.hash, incomingHash: hash });
      return; // do not ack -> B watermark stays below this seq (gap blocks)
    }
    if (seen && seen.status === 'FROZEN') return; // wait for manual resolution

    // --- already handled? (idempotent replay after restart/redelivery) ------
    if (seen && seen.done) {
      // message still visible at broker (A) or re-pulled post watermark (B race)
      const r = await ackSource(seq, { alreadyDone: true, mappingId: seen.mappingId });
      ledger.append('INGRESS_DROPPED', { seq, reason: 'already_done', ackResult: r?.result });
      return;
    }

    // --- map ----------------------------------------------------------------
    let mapRes;
    try {
      mapRes = await mapper.post('/map', {
        side, seq, attempt: msg.attempt || 1, headers, body: msg.body,
      }, { timeoutMs: 5000, retries: 2 });
    } catch (err) {
      lg.warn('map_call_failed', { seq, err: err.message });
      return; // retry next tick; message is not acked
    }

    if (mapRes.result === 'FROZEN') {
      ledger.append('INGRESS_FROZEN', { seq, freezeId: mapRes.freezeId });
      return;
    }
    if (mapRes.result === 'EXISTING') {
      ledger.append('INGRESS_MAPPING', {
        seq, mappingId: mapRes.mappingId, result: 'EXISTING',
        egressStatus: mapRes.egressStatus, sourceAcked: mapRes.sourceAcked,
      });
      if (mapRes.sourceAcked || mapRes.egressStatus === 'PUBLISHED') {
        // Peer delivery already happened; just converge the source ack.
        await ackSource(seq, { mappingId: mapRes.mappingId });
        ledger.append('INGRESS_COMPLETED', {
          side, seq, peerSeq: mapRes.peerSeq, mappingId: mapRes.mappingId, ackResult: 'CONVERGED',
        });
        return;
      }
      // Existing but not yet delivered: ensure it is enqueued (deliverer is
      // idempotent per ingress seq) then wait for callback.
      await enqueueDelivery(mapRes, seen?.hash || hash, msg, headers);
      return;
    }
    if (mapRes.result === 'MAP_FAILED') {
      ledger.append('INGRESS_MAP_FAILED', { seq, mappingId: mapRes.mappingId });
      // Poison message: do not spin forever. Ack it; the chain retains full
      // evidence and operator retry re-enqueues a corrected mapping.
      await ackSource(seq, { mappingId: mapRes.mappingId, poison: true });
      return;
    }

    // NEW mapping -> enqueue
    ledger.append('INGRESS_MAPPING', { seq, mappingId: mapRes.mappingId, result: 'NEW' });
    await enqueueDelivery(mapRes, hash, msg, headers);
  }

  async function enqueueDelivery(mapRes, hash, msg, headers) {
    const targetSide = side === 'A' ? 'B' : 'A';
    let enq;
    try {
      enq = await deliverer.post('/deliver', {
        mappingId: mapRes.mappingId,
        originSide: side,
        targetSide,
        direction: mapRes.direction,
        ingressSeq: mapRes.ingressSeq,
        headers: mapRes.mappedHeaders || headers,
        body: mapRes.mappedBody ?? msg.body,
        idempotencyKey: `idem:${side}:${mapRes.ingressSeq}`,
      }, { timeoutMs: 5000, retries: 3 });
    } catch (err) {
      lg.warn('enqueue_failed', { seq: mapRes.ingressSeq, err: err.message });
      return; // retry next tick; source not acked
    }
    ledger.append('INGRESS_ENQUEUED', {
      seq: mapRes.ingressSeq, mappingId: mapRes.mappingId,
      deliveryId: enq.deliveryId, result: enq.result,
    });
    // Source ack happens asynchronously when deliverer calls /ingress/complete.
  }

  // Ack the local source message according to side semantics.
  async function ackSource(seq, opts = {}) {
    try {
      if (side === 'A') {
        const res = await broker.post(`/ack/${encodeURIComponent(seq)}`, {}, {
          timeoutMs: 5000, retries: 3,
        });
        if (res?.duplicate) {
          await mapper.post('/ack', {
            side, seq, mappingId: opts.mappingId, reason: res.reason || 'duplicate',
          }, { retries: 3 }).catch(() => {});
        }
        return res;
      }
      // B cumulative: acking seq n acks everything <= n. The frozen gap simply
      // isn't acked until resolved, which is the required "freeze the segment".
      const res = await broker.post('/ack', { seq: Number(seq) }, {
        timeoutMs: 5000, retries: 3,
      });
      if (res?.duplicate) {
        await mapper.post('/ack', {
          side, seq, mappingId: opts.mappingId, reason: res.reason || 'below_watermark',
        }, { retries: 3 }).catch(() => {});
      }
      return res;
    } catch (err) {
      lg.warn('ack_failed', { seq, err: err.message });
      throw err;
    }
  }

  // ---- egress: deliverer -> local broker ----------------------------------
  async function handleEgress(body) {
    const items = body.items || [];
    const results = [];
    for (const item of items) {
      try {
        results.push(await publishOne(item));
      } catch (err) {
        // Partial batch success: items before this one may have published;
        // their commits still run. Failed item stays QUEUED in the deliverer.
        results.push({
          deliveryId: item.deliveryId,
          ok: false,
          code: err.code || 'publish_error',
          detail: err.message,
        });
      }
    }
    return { results };
  }

  async function publishOne(item) {
    const idem = item.idempotencyKey || `idem:${item.direction}:${item.ingressSeq}`;
    const prior = ledger.state.published[idem];
    if (prior) {
      // Connector restarted after publish but before commit: same effective
      // publish, same peer seq. This is the duplicate-suppression guarantee.
      return { deliveryId: item.deliveryId, ok: true, peerSeq: prior.peerSeq, duplicate: true };
    }

    const headers = { ...(item.headers || {}) };
    if (item.marker) headers[MARKER_HEADER] = item.marker;

    // Test/demo failure injections (evaluated BEFORE the control keys are
    // stripped, and never forwarded to the real broker):
    //   __egressFailOnce  -> fail exactly once, then succeed (partial batch)
    //   __egressFailUntil -> epoch ms; publish fails while Date.now() < value
    const failOnceKey = `failonce:${idem}`;
    if (item.body && item.body.__egressFailOnce && !globalThis[failOnceKey]) {
      globalThis[failOnceKey] = true;
      throw new EgressError('forced_transient_failure', 'one-shot failure injection (partial batch)');
    }
    if (item.body && Number.isFinite(item.body.__egressFailUntil) &&
        Date.now() < Number(item.body.__egressFailUntil)) {
      throw new EgressError('forced_window_failure', 'egress held in failing window for test');
    }

    // Strip bridge control/injection keys before publishing to the broker.
    const body = stripControlKeys(item.body);

    const res = await broker.post('/publish', {
      headers, body, idempotencyKey: idem,
    }, { timeoutMs: 8000, retries: 0 }); // no blind retry: broker is idempotent on idem

    ledger.append('EGRESS_PUBLISHED_LOCAL', {
      deliveryId: item.deliveryId, idempotencyKey: idem, peerSeq: res.seq, duplicate: !!res.duplicate,
    });
    return { deliveryId: item.deliveryId, ok: true, peerSeq: res.seq, duplicate: !!res.duplicate };
  }

  // ---- HTTP surface -------------------------------------------------------
  const routes = [
    { method: 'GET', pattern: '/health', handler: async () => ({ ok: true, component: `connector-${side}` }) },

    {
      method: 'POST',
      pattern: '/egress',
      handler: async (req, res, p, body) => handleEgress(body || {}),
    },

    // Deliverer phase-2: peer publish confirmed -> ack the source message.
    {
      method: 'POST',
      pattern: '/ingress/complete',
      handler: async (req, res, p, b) => {
        const seq = String(b.seq);
        const seen = ledger.state.seen[seq];

        // Frozen segments must not be completed until resolved. Signal a
        // retriable conflict so the deliverer keeps the callback pending.
        if (seen?.status === 'FROZEN') {
          throw new HttpError(409, 'segment_frozen', seen.freezeId || '');
        }

        const ackRes = await ackSource(seq, { mappingId: b.mappingId });
        ledger.append('INGRESS_COMPLETED', {
          side, seq, mappingId: b.mappingId, deliveryId: b.deliveryId,
          peerSeq: b.peerSeq, ackResult: ackRes?.duplicate ? 'DUPLICATE' : 'ACKED',
          recovered: !!b.recovered,
        });
        return { result: ackRes?.duplicate ? 'DUPLICATE' : 'ACKED', seq };
      },
    },

    // After operator resolves a freeze, release the local segment.
    // action: skip  -> drop incoming copy, advance ack (accept local mapping)
    //         override -> keep incoming; connector re-pulls & re-maps (mapper
    //                     has marked the resolution; a fresh mapping is made)
    {
      method: 'POST',
      pattern: '/ingress/release',
      handler: async (req, res, p, b) => {
        const seq = String(b.seq);
        const s = ledger.state.seen[seq];
        if (b.action === 'skip') {
          // Local mapping stays authoritative; ack the (conflicting) incoming
          // copy away so the sequence can advance.
          const ackRes = await ackSource(seq, { mappingId: b.mappingId });
          if (s) {
            s.status = 'COMPLETED';
            s.done = true;
            s.resolved = 'skip';
          }
          ledger.append('INGRESS_FREEZE_RELEASED', { seq, action: 'skip', ackResult: ackRes?.result });
          return { ok: true, action: 'skip' };
        }
        if (b.action === 'override') {
          // Accept the INCOMING content: fetch the current broker copy under
          // the same seq, ask the mapper for a fresh superseding mapping, then
          // enqueue it for delivery. The old mapping stays on the chain marked
          // SUPERSEDED (no silent rewrite of history).
          const pulled = await broker.post('/pull', side === 'A'
            ? { max: 100, visibilityMs: VIS_MS }
            : { max: 100 }, { timeoutMs: 5000 });
          const fresh = pulled.messages.find((m) => String(m.seq) === seq);
          if (!fresh) throw new HttpError(409, 'incoming_copy_unavailable', 'broker did not return the conflicting seq');

          const remap = await mapper.post('/remap-conflict', {
            side, seq, headers: fresh.headers || {}, body: fresh.body,
            freezeId: b.freezeId || null, oldMappingId: b.mappingId || (s && s.mappingId) || null,
          }, { timeoutMs: 5000, retries: 2 });

          if (s) {
            s.status = 'SEEN';
            s.done = false;
            s.hash = bodyHash(fresh.body);
            s.resolved = 'override';
            s.mappingId = remap.mappingId;
          }
          ledger.append('INGRESS_FREEZE_RELEASED', {
            seq, action: 'override', newMappingId: remap.mappingId,
          });

          if (remap.result === 'NEW') {
            const targetSide = side === 'A' ? 'B' : 'A';
            const enq = await deliverer.post('/deliver', {
              mappingId: remap.mappingId,
              originSide: side,
              targetSide,
              direction: remap.direction,
              ingressSeq: remap.ingressSeq,
              headers: remap.mappedHeaders,
              body: remap.mappedBody,
              idempotencyKey: `idem:${side}:${seq}:v2`,
            }, { timeoutMs: 5000, retries: 3 });
            ledger.append('INGRESS_ENQUEUED', {
              seq, mappingId: remap.mappingId, deliveryId: enq.deliveryId, result: enq.result, remap: true,
            });
          }
          return { ok: true, action: 'override', mappingId: remap.mappingId, result: remap.result };
        }
        throw new HttpError(400, 'bad_action', 'use skip|override');
      },
    },

    {
      method: 'GET',
      pattern: '/state',
      handler: async () => ({
        side,
        runId: ledger.state.runId,
        inboundAckWatermark: ledger.state.inboundAckWatermark,
        outboundCommitWatermark: ledger.state.outboundCommitWatermark,
        seen: Object.values(ledger.state.seen),
        published: Object.values(ledger.state.published),
      }),
    },
    {
      method: 'GET',
      pattern: '/events',
      handler: async (req) => ({
        events: ledger.readEvents({
          type: req.query.type,
          since: req.query.since ? Number(req.query.since) : 0,
          limit: req.query.limit ? Number(req.query.limit) : 500,
        }),
      }),
    },
    {
      method: 'GET',
      pattern: '/overview',
      handler: async () => {
        const seen = Object.values(ledger.state.seen);
        return {
          component: `connector-${side}`,
          runId: ledger.state.runId,
          inbound: {
            seen: seen.length,
            completed: seen.filter((s) => s.status === 'COMPLETED').length,
            frozen: seen.filter((s) => s.status === 'FROZEN').length,
            mapFailed: seen.filter((s) => s.status === 'MAP_FAILED').length,
            enqueued: seen.filter((s) => s.status === 'ENQUEUED').length,
            ackWatermark: side === 'B' ? ledger.state.inboundAckWatermark : null,
          },
          outbound: {
            published: Object.keys(ledger.state.published).length,
            commitWatermark: side === 'B' ? ledger.state.outboundCommitWatermark : null,
          },
        };
      },
    },
  ];

  const server = await createServer(routes, port);
  ingressLoop();

  return {
    server,
    ledger,
    async stop() {
      running.v = false;
      await new Promise((r) => server.close(r));
    },
  };
}

class EgressError extends Error {
  constructor(code, detail) {
    super(detail || code);
    this.code = code;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const side = process.env.SIDE;
  const port = Number(process.env.PORT || (side === 'A' ? 8501 : 8601));
  const dir = process.env.DATA_DIR || `./data/connector-${side.toLowerCase()}`;
  startConnector(side, port, dir, {
    broker: process.env.BROKER_URL || (side === 'A' ? 'http://localhost:8101' : 'http://localhost:8201'),
    mapper: process.env.MAPPER_URL || 'http://localhost:8301',
    deliverer: process.env.DELIVERER_URL || 'http://localhost:8401',
  });
}
