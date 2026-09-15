// Deliverer service.
//
// Outbox + two-phase completion:
//   1. connectors enqueue mapped messages (durable outbox)
//   2. a batched pump pushes ordered groups to the target connector egress
//   3. on successful publish it reports EGRESS_PUBLISHED to the mapper and
//      asks the SOURCE connector to complete (ack the source message)
//   Partial batches: every item has its own status; failed items stay QUEUED
//   and are retried with backoff. Successful items never block on them.
import { mkdirSync } from 'node:fs';
import { createServer, HttpError } from '../http.js';
import { Ledger } from '../store/ledger.js';
import { Http } from '../client.js';
import { logger } from '../log.js';
import { uid, makeMarker, parseMarker, BRIDGE_INSTANCE, sleep, nowIso } from '../util.js';

const log = logger('deliverer');

const BATCH_MAX = Number(process.env.BATCH_MAX || 5);
const TICK_MS = Number(process.env.DELIVERER_TICK_MS || 250);
const PUBLISH_TIMEOUT_MS = Number(process.env.PUBLISH_TIMEOUT_MS || 8000);
const CALLBACK_RETRIES = Number(process.env.CALLBACK_RETRIES || 6);

function initial() {
  return {
    deliveries: {}, // deliveryId -> record
    order: [], // deliveryIds in enqueue order
    byIdem: {}, // idempotencyKey -> deliveryId
    callbacksPending: {}, // deliveryId -> { kind, url, body, tries, nextAt }
  };
}

function fold(state, event) {
  const d = event.data || {};
  switch (event.type) {
    case 'DELIVERY_ENQUEUED': {
      if (state.byIdem[d.idempotencyKey]) break;
      state.deliveries[d.deliveryId] = {
        deliveryId: d.deliveryId,
        idempotencyKey: d.idempotencyKey,
        mappingId: d.mappingId,
        originSide: d.originSide,
        targetSide: d.targetSide,
        direction: d.direction,
        ingressSeq: String(d.ingressSeq),
        headers: d.headers,
        body: d.body,
        status: 'QUEUED',
        attempts: 0,
        enqueuedAt: event.ts,
        updatedAt: event.ts,
      };
      state.order.push(d.deliveryId);
      state.byIdem[d.idempotencyKey] = d.deliveryId;
      break;
    }
    case 'DELIVERY_ATTEMPT': {
      const r = state.deliveries[d.deliveryId];
      if (r) {
        r.attempts = (r.attempts || 0) + 1;
        r.status = 'INFLIGHT';
        r.batchId = d.batchId;
        r.updatedAt = event.ts;
      }
      break;
    }
    case 'DELIVERY_PUBLISHED': {
      const r = state.deliveries[d.deliveryId];
      if (r) {
        r.status = 'PUBLISHED';
        r.peerSeq = String(d.peerSeq);
        r.publishedAt = event.ts;
        r.updatedAt = event.ts;
      }
      break;
    }
    case 'DELIVERY_FAILED': {
      const r = state.deliveries[d.deliveryId];
      if (r) {
        r.status = d.terminal ? 'FAILED' : 'QUEUED';
        r.lastErrorCode = d.code;
        r.lastErrorDetail = d.detail;
        r.nextAttemptAt = d.nextAttemptAt || null;
        r.updatedAt = event.ts;
      }
      break;
    }
    case 'SOURCE_COMPLETED': {
      const r = state.deliveries[d.deliveryId];
      if (r) {
        r.status = 'DONE';
        r.sourceAckResult = d.ackResult;
        r.completedAt = event.ts;
        r.updatedAt = event.ts;
      }
      delete state.callbacksPending[d.deliveryId];
      break;
    }
    case 'CALLBACK_PENDING': {
      state.callbacksPending[d.deliveryId] = {
        kind: d.kind, tries: d.tries, nextAt: d.nextAt, updatedAt: event.ts,
      };
      break;
    }
    case 'CALLBACK_FAILED': {
      const c = state.callbacksPending[d.deliveryId];
      if (c) {
        c.tries = d.tries;
        c.nextAt = d.nextAt;
      }
      break;
    }
    default:
      break;
  }
  return state;
}

export async function startDeliverer(port, dataDir, endpoints) {
  mkdirSync(dataDir, { recursive: true });
  const ledger = new Ledger(dataDir, { fold, initial, snapshotEvery: 50 });
  await ledger.open();
  ledger.append('DELIVERER_STARTED', { at: nowIso() });

  const connectorA = new Http(endpoints.connectorA);
  const connectorB = new Http(endpoints.connectorB);
  const mapper = new Http(endpoints.mapper);
  const clientFor = (side) => (side === 'A' ? connectorA : connectorB);

  let running = true;

  function pump() {
    (async () => {
      while (running) {
        try {
          await tick();
        } catch (err) {
          log.error('tick_failed', { err: err.stack || String(err) });
        }
        await sleep(TICK_MS);
      }
    })();
  }

  // One ordered batch per direction per tick.
  async function tick() {
    const now = Date.now();
    const due = (r) =>
      (r.status === 'QUEUED') &&
      (!r.nextAttemptAt || new Date(r.nextAttemptAt).getTime() <= now);

    for (const targetSide of ['A', 'B']) {
      const batch = ledger.state.order
        .map((id) => ledger.state.deliveries[id])
        .filter((r) => r && r.targetSide === targetSide && due(r))
        .slice(0, BATCH_MAX);
      if (batch.length) await publishBatch(targetSide, batch);
    }
    await driveCallbacks();
  }

  async function publishBatch(targetSide, batch) {
    const batchId = uid('b_');
    const tickNow = Date.now();
    const items = batch.map((r) => {
      // Loop control marker. A message already carrying a marker from a
      // DIFFERENT bridge instance is a multi-hop loop: decrement its ttl so the
      // ring dies after MARKER_TTL_MAX hops. Our own reflection never reaches
      // here (suppressed at ingress), so a fresh marker marks the first hop.
      const incomingMarker = parseMarker(r.headers && r.headers['x-bridge-marker']);
      let marker;
      if (incomingMarker && incomingMarker.instance !== BRIDGE_INSTANCE) {
        marker = makeMarker(
          incomingMarker.instance,
          incomingMarker.originSide,
          incomingMarker.mappingId,
          Math.max(0, incomingMarker.ttl - 1),
        );
      } else {
        marker = makeMarker(BRIDGE_INSTANCE, r.originSide, r.mappingId, 3);
      }
      return {
        deliveryId: r.deliveryId,
        mappingId: r.mappingId,
        ingressSeq: r.ingressSeq,
        direction: r.direction,
        headers: r.headers,
        body: r.body,
        marker,
        idempotencyKey: r.idempotencyKey,
      };
    });
    batch.forEach((r) => ledger.append('DELIVERY_ATTEMPT', { deliveryId: r.deliveryId, batchId }));

    let resp;
    try {
      resp = await clientFor(targetSide).post('/egress', { items }, { timeoutMs: PUBLISH_TIMEOUT_MS });
    } catch (err) {
      // Whole batch unreachable -> retry everything.
      const next = new Date(tickNow + backoff(1)).toISOString();
      batch.forEach((r) =>
        ledger.append('DELIVERY_FAILED', {
          deliveryId: r.deliveryId, code: 'egress_unreachable', detail: err.message, nextAttemptAt: next,
        }));
      return;
    }

    const results = new Map((resp.results || []).map((x) => [x.deliveryId, x]));
    for (const r of batch) {
      const x = results.get(r.deliveryId);
      if (x && x.ok) {
        ledger.append('DELIVERY_PUBLISHED', { deliveryId: r.deliveryId, peerSeq: x.peerSeq });
        await mirrorEvent('EGRESS_PUBLISHED', {
          mappingId: r.mappingId, deliveryId: r.deliveryId, peerSeq: x.peerSeq, targetSide,
        });
        await completeSource(r, x.peerSeq);
      } else {
        const code = x?.code || 'no_result';
        const detail = x?.detail || 'egress returned no ok for item';
        const terminal = x?.terminal === true;
        const next = terminal ? null : new Date(tickNow + backoff(r.attempts || 1)).toISOString();
        ledger.append('DELIVERY_FAILED', {
          deliveryId: r.deliveryId, code, detail, terminal, nextAttemptAt: next,
        });
        mirrorEvent('EGRESS_FAILED', {
          mappingId: r.mappingId, deliveryId: r.deliveryId, code, detail, terminal,
        }).catch(() => {});
        log.warn('item_failed', { deliveryId: r.deliveryId, code, terminal });
      }
    }
  }

  async function completeSource(record, peerSeq) {
    // Phase 2 callback to the SOURCE connector: ack the original message.
    const url = `/ingress/complete`;
    const body = {
      side: record.originSide,
      seq: record.ingressSeq,
      mappingId: record.mappingId,
      deliveryId: record.deliveryId,
      peerSeq: String(peerSeq),
    };
    try {
      const res = await clientFor(record.originSide).post(url, body, {
        timeoutMs: PUBLISH_TIMEOUT_MS, retries: CALLBACK_RETRIES, retryDelayMs: 300,
      });
      ledger.append('SOURCE_COMPLETED', { deliveryId: record.deliveryId, ackResult: res.result || 'ACKED' });
      await mirrorEvent('SOURCE_ACKED', {
        side: record.originSide, seq: record.ingressSeq, mappingId: record.mappingId,
        deliveryId: record.deliveryId, peerSeq, attempt: body.attempt ?? 1,
      });
    } catch (err) {
      // Connector is down; remember to finish the source side after recovery.
      // Published message is safe on the peer (deduplicated by idempotency key),
      // so redelivering it later causes no duplicate effective delivery.
      ledger.append('CALLBACK_PENDING', {
        deliveryId: record.deliveryId, kind: 'source_complete', tries: 1,
        nextAt: new Date(Date.now() + 2000).toISOString(),
      });
      log.warn('source_complete_pending', { deliveryId: record.deliveryId, err: err.message });
    }
  }

  // After a connector restart, finish any callbacks that were left pending.
  async function driveCallbacks() {
    const t = Date.now();
    for (const [deliveryId, c] of Object.entries(ledger.state.callbacksPending)) {
      if (new Date(c.nextAt).getTime() > t) continue;
      const r = ledger.state.deliveries[deliveryId];
      if (!r) continue;
      try {
        const res = await clientFor(r.originSide).post('/ingress/complete', {
          side: r.originSide,
          seq: r.ingressSeq,
          mappingId: r.mappingId,
          deliveryId: r.deliveryId,
          peerSeq: r.peerSeq,
          recovered: true,
        }, { timeoutMs: 5000 });
        ledger.append('SOURCE_COMPLETED', { deliveryId, ackResult: res?.result || 'ACKED', recovered: true });
        await mirrorEvent('SOURCE_ACKED', {
          side: r.originSide, seq: r.ingressSeq, mappingId: r.mappingId, deliveryId, recovered: true,
        });
      } catch (err) {
        const tries = c.tries + 1;
        const back = Math.min(30000, 1000 * 2 ** Math.min(tries, 4));
        ledger.append('CALLBACK_FAILED', {
          deliveryId, tries, nextAt: new Date(Date.now() + back).toISOString(), err: err.message,
        });
      }
    }
  }

  async function mirrorEvent(type, data) {
    // Best-effort chain mirroring; mapper unavailability must not block delivery.
    for (let i = 0; i < 4; i++) {
      try {
        await mapper.post('/event', { type, data }, { timeoutMs: 3000 });
        return;
      } catch {
        await sleep(300 * (i + 1));
      }
    }
    log.warn('mirror_event_given_up', { type, mappingId: data?.mappingId });
  }

  pump();

  const routes = [
    { method: 'GET', pattern: '/health', handler: async () => ({ ok: true, component: 'deliverer' }) },

    {
      method: 'POST',
      pattern: '/deliver',
      handler: async (req, res, p, b) => {
        const required = ['mappingId', 'originSide', 'targetSide', 'direction', 'ingressSeq', 'body'];
        for (const k of required) if (b?.[k] === undefined) throw new HttpError(400, `missing_${k}`);
        const idempotencyKey =
          b.idempotencyKey || `idem:${b.originSide}:${String(b.ingressSeq)}`;
        const existing = ledger.state.byIdem[idempotencyKey];
        if (existing) {
          const r = ledger.state.deliveries[existing];
          return { result: 'EXISTING', deliveryId: existing, status: r.status, peerSeq: r.peerSeq ?? null };
        }
        const deliveryId = uid('d_');
        ledger.append('DELIVERY_ENQUEUED', {
          deliveryId,
          idempotencyKey,
          mappingId: b.mappingId,
          originSide: b.originSide,
          targetSide: b.targetSide,
          direction: b.direction,
          ingressSeq: String(b.ingressSeq),
          headers: b.headers || {},
          body: b.body,
        });
        log.info('enqueued', { deliveryId, mappingId: b.mappingId, direction: b.direction });
        // Mirror enqueue evidence onto the authoritative chain (best effort).
        mirrorEvent('EGRESS_ENQUEUED', {
          deliveryId, mappingId: b.mappingId,
          originSide: b.originSide, targetSide: b.targetSide,
          ingressSeq: String(b.ingressSeq),
        }).catch(() => {});
        return { result: 'ENQUEUED', deliveryId };
      },
    },

    {
      method: 'POST',
      pattern: '/loop-suppressed',
      handler: async (req, res, p, b) => {
        ledger.append('LOOP_SUPPRESSED_DELIVERY', {
          mappingId: b?.mappingId || null,
          originSide: b?.originSide, side: b?.side, reason: b?.reason, ttl: b?.ttl ?? null,
        });
        mirrorEvent('LOOP_SUPPRESSED', {
          mappingId: b?.mappingId || null, side: b?.side, reason: b?.reason,
        }).catch(() => {});
        return { ok: true };
      },
    },

    {
      method: 'GET',
      pattern: '/deliveries',
      handler: async (req) => {
        const { status, limit } = req.query;
        let rows = ledger.state.order.map((id) => ledger.state.deliveries[id]);
        if (status) rows = rows.filter((r) => r.status === status);
        if (limit) rows = rows.slice(-Number(limit));
        return { count: rows.length, deliveries: rows };
      },
    },
    {
      method: 'GET',
      pattern: '/events',
      handler: async (req) => {
        const { type, since, limit } = req.query;
        return { events: ledger.readEvents({ type, since: since ? Number(since) : 0, limit: limit ? Number(limit) : 500 }) };
      },
    },
    {
      method: 'GET',
      pattern: '/overview',
      handler: async () => {
        const rs = Object.values(ledger.state.deliveries);
        return {
          component: 'deliverer',
          totals: {
            enqueued: rs.length,
            queued: rs.filter((r) => r.status === 'QUEUED').length,
            inflight: rs.filter((r) => r.status === 'INFLIGHT').length,
            published: rs.filter((r) => r.status === 'PUBLISHED').length,
            done: rs.filter((r) => r.status === 'DONE').length,
            failed: rs.filter((r) => r.status === 'FAILED').length,
          },
          callbacksPending: Object.keys(ledger.state.callbacksPending).length,
          lastSeq: ledger.seq,
        };
      },
    },
  ];

  const server = await createServer(routes, port);
  return {
    server,
    ledger,
    async stop() {
      running = false;
      await new Promise((r) => server.close(r));
    },
  };
}

function now() {
  return Date.now();
}

function backoff(attempt) {
  const base = Number(process.env.RETRY_BASE_MS || 400);
  const cap = Number(process.env.RETRY_CAP_MS || 15000);
  return Math.min(cap, base * 2 ** Math.min(attempt, 5));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.env.PORT || 8401);
  const dir = process.env.DATA_DIR || './data/deliverer';
  startDeliverer(port, dir, {
    connectorA: process.env.CONNECTOR_A_URL || 'http://localhost:8501',
    connectorB: process.env.CONNECTOR_B_URL || 'http://localhost:8601',
    mapper: process.env.MAPPER_URL || 'http://localhost:8301',
  });
}
