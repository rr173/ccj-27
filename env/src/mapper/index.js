// Mapper service.
//
// Responsibilities:
//   - bidirectional mapping between (side, ingressSeq) <-> mappingId <-> peer
//   - body/envelope transformation (A<->B) with explicit map-failure state
//   - sequence-conflict "frozen segments" + manual resolution (skip/override)
//   - source-ack ledger with duplicate-ack detection
//   - the authoritative, queryable delivery chain (journal + chain API)
import { mkdirSync } from 'node:fs';
import { createServer, HttpError } from '../http.js';
import { Ledger } from '../store/ledger.js';
import { logger } from '../log.js';
import { uid, bodyHash, businessKey, nowIso } from '../util.js';
import { transformBody, peer, directionOf } from '../mapping/transform.js';

const log = logger('mapper');

const MAPPING_EVENTS = new Set([
  'MAPPING_CREATED', 'MAP_FAILED', 'MAPPING_SUPERSEDED',
  'EGRESS_ENQUEUED', 'EGRESS_PUBLISHED', 'EGRESS_FAILED',
  'SOURCE_ACKED', 'SOURCE_ACK_DUPLICATE', 'LOOP_SUPPRESSED',
  'RESOLVED_SKIP', 'RESOLVED_OVERRIDE', 'RESOLVED_KEEP_LOCAL',
  'RETRY_REQUESTED', 'CONNECTOR_STARTED',
]);

function initial() {
  return {
    mappings: {}, // mappingId -> record
    bySideSeq: {}, // `${side}:${seq}` -> mappingId
    byBizKey: {}, // `${direction}:${bizKey}` -> mappingId
    freezes: {}, // freezeId -> record
    freezeBySideSeq: {}, // `${side}:${seq}` -> freezeId
    acks: {}, // `${side}:${seq}` -> {at, mappingId, attempt}
  };
}

function fold(state, event) {
  const d = event.data || {};
  switch (event.type) {
    case 'MAPPING_CREATED': {
      const key = `${d.originSide}:${d.ingressSeq}`;
      const bk = `${d.direction}:${d.bizKey}`;
      state.bySideSeq[key] = d.mappingId;
      if (!state.byBizKey[bk]) state.byBizKey[bk] = d.mappingId;
      state.mappings[d.mappingId] = {
        mappingId: d.mappingId,
        originSide: d.originSide,
        direction: d.direction,
        ingressSeq: String(d.ingressSeq),
        attempt: d.attempt ?? 1,
        bizKey: d.bizKey,
        bodyHash: d.bodyHash,
        status: 'MAPPED',
        egressStatus: 'PENDING',
        sourceAcked: false,
        createdAt: event.ts,
        updatedAt: event.ts,
      };
      break;
    }
    case 'MAP_FAILED': {
      const m = state.mappings[d.mappingId];
      if (m) {
        m.status = 'MAP_FAILED';
        m.errorCode = d.code;
        m.errorDetail = d.detail;
        m.attempts = (m.attempts ?? 0) + 1;
        m.updatedAt = event.ts;
      }
      break;
    }
    case 'LOOP_SUPPRESSED': {
      // Loop markers are not mapped; only record chain-level evidence.
      break;
    }
    case 'EGRESS_ENQUEUED': {
      const m = state.mappings[d.mappingId];
      if (m) {
        m.egressStatus = 'QUEUED';
        m.deliveryId = d.deliveryId;
        m.updatedAt = event.ts;
      }
      break;
    }
    case 'EGRESS_PUBLISHED': {
      const m = state.mappings[d.mappingId];
      if (m) {
        m.egressStatus = 'PUBLISHED';
        m.peerSeq = String(d.peerSeq);
        m.updatedAt = event.ts;
      }
      break;
    }
    case 'EGRESS_FAILED': {
      const m = state.mappings[d.mappingId];
      if (m) {
        m.egressStatus = 'FAILED';
        m.errorCode = d.code;
        m.errorDetail = d.detail;
        m.attempts = (m.attempts ?? 0) + 1;
        m.updatedAt = event.ts;
      }
      break;
    }
    case 'SOURCE_ACKED': {
      const m = state.mappings[d.mappingId];
      if (m) {
        m.sourceAcked = true;
        m.sourceAckedAt = event.ts;
        m.completed = m.egressStatus === 'PUBLISHED';
        m.updatedAt = event.ts;
      }
      state.acks[`${d.side}:${d.seq}`] = { at: event.ts, mappingId: d.mappingId, attempt: d.attempt };
      break;
    }
    case 'SOURCE_ACK_DUPLICATE': {
      break;
    }
    case 'FREEZE_OPENED': {
      state.freezes[d.freezeId] = {
        freezeId: d.freezeId,
        side: d.side,
        ingressSeq: String(d.ingressSeq),
        storedHash: d.storedHash,
        incomingHash: d.incomingHash,
        reason: d.reason,
        status: 'FROZEN',
        mappingId: d.mappingId || null,
        createdAt: event.ts,
        updatedAt: event.ts,
      };
      state.freezeBySideSeq[`${d.side}:${d.ingressSeq}`] = d.freezeId;
      break;
    }
    case 'RESOLVED_SKIP':
    case 'RESOLVED_OVERRIDE':
    case 'RESOLVED_KEEP_LOCAL': {
      const f = state.freezes[d.freezeId];
      if (f) {
        f.status = event.type === 'RESOLVED_SKIP' ? 'RESOLVED_SKIP'
          : event.type === 'RESOLVED_OVERRIDE' ? 'RESOLVED_OVERRIDE' : 'RESOLVED_KEEP_LOCAL';
        f.resolvedAt = event.ts;
        f.updatedAt = event.ts;
        delete state.freezeBySideSeq[`${f.side}:${f.ingressSeq}`];
      }
      if (d.mappingId) {
        const m = state.mappings[d.mappingId];
        if (m) {
          if (event.type === 'RESOLVED_OVERRIDE') m.status = 'REMAPPED';
          m.updatedAt = event.ts;
        }
      }
      break;
    }
    case 'MAPPING_SUPERSEDED': {
      const m = state.mappings[d.mappingId];
      if (m) {
        m.supersededBy = d.newMappingId;
        m.updatedAt = event.ts;
      }
      break;
    }
    case 'RETRY_REQUESTED': {
      const m = state.mappings[d.mappingId];
      if (m) {
        m.status = 'MAPPED';
        m.errorCode = undefined;
        m.errorDetail = undefined;
        m.updatedAt = event.ts;
      }
      break;
    }
    default:
      break;
  }
  return state;
}

export async function startMapper(port, dataDir) {
  mkdirSync(dataDir, { recursive: true });
  const ledger = new Ledger(dataDir, { fold, initial, snapshotEvery: 50 });
  await ledger.open();
  const bridgeInstance = process.env.BRIDGE_INSTANCE || 'bridge-1';
  ledger.append('MAPPER_STARTED', { bridgeInstance, at: nowIso() });

  // Build an EXISTING response. When the prior delivery has not finished we
  // also return a freshly transformed body + direction so the connector can
  // idempotently re-enqueue (e.g. after a connector restart mid-flight).
  function existingResponse(mappingId, side, body, headers) {
    const existing = ledger.state.mappings[mappingId];
    const out = {
      result: 'EXISTING',
      mappingId,
      status: existing.status,
      egressStatus: existing.egressStatus,
      peerSeq: existing.peerSeq ?? null,
      sourceAcked: existing.sourceAcked,
      direction: existing.direction,
      ingressSeq: existing.ingressSeq,
    };
    if (!existing.sourceAcked && existing.egressStatus !== 'PUBLISHED' &&
        existing.status !== 'MAP_FAILED') {
      try {
        out.mappedHeaders = { ...(headers || {}) };
        out.mappedBody = transformBody(existing.direction, body);
      } catch {
        // original body unavailable (hash-only record); replay must be manual
      }
    }
    return out;
  }

  const routes = [
    { method: 'GET', pattern: '/health', handler: async () => ({ ok: true, component: 'mapper' }) },

    // Attempt a mapping. Idempotent on (originSide, ingressSeq):
    //   result: NEW | EXISTING | FROZEN | MAP_FAILED
    {
      method: 'POST',
      pattern: '/map',
      handler: async (req, res, p, b) => {
        const { side, seq, attempt, headers, body } = b || {};
        if (!side || !['A', 'B'].includes(side)) throw new HttpError(400, 'bad_side');
        if (seq === undefined || seq === null) throw new HttpError(400, 'seq_required');
        const sSeq = String(seq);

        const openFreeze = ledger.state.freezeBySideSeq[`${side}:${sSeq}`];
        if (openFreeze) {
          return { result: 'FROZEN', freezeId: openFreeze, freeze: ledger.state.freezes[openFreeze] };
        }

        const existingId = ledger.state.bySideSeq[`${side}:${sSeq}`];
        if (existingId) {
          return existingResponse(existingId, side, body, headers);
        }

        const mappingId = uid('m_');
        const direction = directionOf(side);
        const bh = bodyHash(body);
        const bk = businessKey(body);

        // Same business message seen again (e.g. re-keyed sequence after a
        // broker rebuild): collapse onto the canonical mapping instead of
        // creating a second effective delivery.
        const bizExisting = ledger.state.byBizKey[`${direction}:${bk}`];
        if (bizExisting) {
          ledger.append('MAPPING_ALIAS', {
            mappingId: bizExisting, aliasSide: side, aliasSeq: sSeq, bizKey: bk, reason: 'bizkey_collision',
          });
          ledger.state.bySideSeq[`${side}:${sSeq}`] = bizExisting;
          return { ...existingResponse(bizExisting, side, body, headers), alias: true };
        }

        ledger.append('MAPPING_CREATED', {
          mappingId, originSide: side, direction, ingressSeq: sSeq,
          attempt: attempt ?? 1, bizKey: bk, bodyHash: bh,
        });

        let mapped;
        try {
          mapped = {
            headers: { ...(headers || {}) },
            body: transformBody(direction, body, { fail: b.forceMapFail === true }),
          };
        } catch (err) {
          ledger.append('MAP_FAILED', {
            mappingId, code: err.code || 'map_error', detail: err.message,
          });
          return { result: 'MAP_FAILED', mappingId, error: err.message, code: err.code || 'map_error' };
        }

        log.info('mapped', { mappingId, side, seq: sSeq, direction, bizKey: bk });
        return {
          result: 'NEW',
          mappingId,
          direction,
          originSide: side,
          ingressSeq: sSeq,
          bizKey: bk,
          bodyHash: bh,
          mappedHeaders: mapped.headers,
          mappedBody: mapped.body,
        };
      },
    },

    // Record/acknowledge source-side completion. Detects duplicate acks.
    {
      method: 'POST',
      pattern: '/ack',
      handler: async (req, res, p, b) => {
        const { side, seq, mappingId, attempt, reason } = b || {};
        const sSeq = String(seq);
        const key = `${side}:${sSeq}`;
        const prior = ledger.state.acks[key];
        if (prior) {
          ledger.append('SOURCE_ACK_DUPLICATE', {
            side, seq: sSeq, priorAt: prior.at, mappingId: prior.mappingId,
            attempt: attempt ?? null, reason: reason || 'redelivered_after_ack',
          });
          return { result: 'DUPLICATE', firstAckAt: prior.at };
        }
        ledger.append('SOURCE_ACKED', { side, seq: sSeq, mappingId, attempt: attempt ?? 1 });
        return { result: 'ACKED' };
      },
    },

    // Connector reports a same-sequence content conflict -> freeze the segment.
    {
      method: 'POST',
      pattern: '/freeze',
      handler: async (req, res, p, b) => {
        const { side, seq, storedHash, incomingHash, reason, mappingId } = b || {};
        const sSeq = String(seq);
        const existing = ledger.state.freezeBySideSeq[`${side}:${sSeq}`];
        if (existing) return { result: 'EXISTING', freezeId: existing, freeze: ledger.state.freezes[existing] };
        const freezeId = uid('fz_');
        ledger.append('FREEZE_OPENED', {
          freezeId, side, ingressSeq: sSeq, storedHash, incomingHash,
          reason: reason || 'sequence_content_conflict', mappingId: mappingId || null,
        });
        log.warn('freeze_opened', { freezeId, side, seq: sSeq });
        return { result: 'FROZEN', freezeId };
      },
    },

    // Connector requests a remap after an override resolution. The old
    // (side,seq) binding is superseded by a fresh mapping for the new content.
    {
      method: 'POST',
      pattern: '/remap-conflict',
      handler: async (req, res, p, b) => {
        const { side, seq, headers, body, freezeId, oldMappingId, attempt } = b || {};
        const sSeq = String(seq);
        const mappingId = uid('m_');
        const direction = directionOf(side);
        const bh = bodyHash(body);
        const bk = businessKey(body);
        ledger.append('MAPPING_CREATED', {
          mappingId, originSide: side, direction, ingressSeq: sSeq,
          attempt: attempt ?? 1, bizKey: bk, bodyHash: bh, remapAfter: freezeId || null,
        });
        if (oldMappingId) {
          ledger.append('MAPPING_SUPERSEDED', { mappingId: oldMappingId, newMappingId: mappingId });
        }
        // Rebind the side/seq index to the new authoritative mapping.
        ledger.state.bySideSeq[`${side}:${sSeq}`] = mappingId;
        let mapped;
        try {
          mapped = { headers: { ...(headers || {}) }, body: transformBody(direction, body) };
        } catch (err) {
          ledger.append('MAP_FAILED', { mappingId, code: err.code || 'map_error', detail: err.message });
          return { result: 'MAP_FAILED', mappingId, error: err.message };
        }
        return {
          result: 'NEW', mappingId, direction, originSide: side, ingressSeq: sSeq,
          bizKey: bk, bodyHash: bh, mappedHeaders: mapped.headers, mappedBody: mapped.body,
        };
      },
    },

    // Manual resolution of a frozen segment.
    // decision: skip (drop incoming, advance) | override (accept incoming, remap) | keep_local
    {
      method: 'POST',
      pattern: '/freeze/:id/resolve',
      handler: async (req, res, params, b = {}) => {
        const f = ledger.state.freezes[params.id];
        if (!f) throw new HttpError(404, 'freeze_not_found');
        if (f.status !== 'FROZEN') return { result: 'ALREADY_RESOLVED', freeze: f };
        const decision = b.decision;
        const mappingId = b.mappingId || f.mappingId || null;

        if (decision === 'skip') {
          ledger.append('RESOLVED_SKIP', { freezeId: f.freezeId, mappingId });
        } else if (decision === 'override') {
          ledger.append('RESOLVED_OVERRIDE', {
            freezeId: f.freezeId, mappingId: b.newMappingId || mappingId,
            incomingHash: f.incomingHash,
          });
        } else if (decision === 'keep_local') {
          ledger.append('RESOLVED_KEEP_LOCAL', { freezeId: f.freezeId, mappingId });
        } else {
          throw new HttpError(400, 'bad_decision', 'use skip|override|keep_local');
        }
        return { result: 'RESOLVED', decision, freeze: ledger.state.freezes[f.freezeId] };
      },
    },

    // Retry a MAP_FAILED (optionally with corrected body supplied by operator).
    {
      method: 'POST',
      pattern: '/mappings/:id/retry',
      handler: async (req, res, params, b = {}) => {
        const m = ledger.state.mappings[params.id];
        if (!m) throw new HttpError(404, 'mapping_not_found');
        let mapped;
        try {
          mapped = {
            headers: b.headers || {},
            body: transformBody(m.direction, b.body ?? reconstructHint(m), { fail: false }),
          };
        } catch (err) {
          ledger.append('MAP_FAILED', { mappingId: m.mappingId, code: err.code, detail: err.message });
          return { result: 'MAP_FAILED', error: err.message };
        }
        ledger.append('RETRY_REQUESTED', {
          mappingId: m.mappingId, operator: b.operator || 'manual', bodyOverride: !!b.body,
        });
        return {
          result: 'REMAPPED',
          mappingId: m.mappingId,
          direction: m.direction,
          originSide: m.originSide,
          ingressSeq: m.ingressSeq,
          mappedHeaders: mapped.headers,
          mappedBody: mapped.body,
        };
      },
    },

    // Deliverer mirrors lifecycle events so the mapper remains the chain authority.
    {
      method: 'POST',
      pattern: '/event',
      handler: async (req, res, p, b) => {
        const { type, data } = b || {};
        if (!MAPPING_EVENTS.has(type)) throw new HttpError(400, 'unknown_event_type', type);
        ledger.append(type, data || {});
        return { ok: true };
      },
    },

    // Connector startup evidence (restart traceability).
    {
      method: 'POST',
      pattern: '/connector/started',
      handler: async (req, res, p, b = {}) => {
        ledger.append('CONNECTOR_STARTED', {
          side: b.side, runId: b.runId, resumedFrom: b.resumedFrom ?? null, at: nowIso(),
        });
        return { ok: true };
      },
    },

    // ---- Query API ----------------------------------------------------
    {
      method: 'GET',
      pattern: '/chain/:id',
      handler: async (req, res, params) => {
        const id = params.id;
        const all = ledger.readEvents({});
        // Explicit chain membership by mappingId.
        const events = all.filter((e) => {
          const d = e.data || {};
          return d.mappingId === id || d.id === id;
        });
        const mapping = ledger.state.mappings[id] || null;
        const freeze = Object.values(ledger.state.freezes).find((f) => f.mappingId === id) || null;
        return { mappingId: id, mapping, freeze, events };
      },
    },
    {
      method: 'GET',
      pattern: '/mappings',
      handler: async (req) => {
        const { status, side, egressStatus, limit } = req.query;
        let rows = Object.values(ledger.state.mappings);
        if (side) rows = rows.filter((m) => m.originSide === side);
        if (status) rows = rows.filter((m) => m.status === status);
        if (egressStatus) rows = rows.filter((m) => m.egressStatus === egressStatus);
        rows = rows.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
        if (limit) rows = rows.slice(-Number(limit));
        return { count: rows.length, mappings: rows };
      },
    },
    {
      method: 'GET',
      pattern: '/freezes',
      handler: async (req) => {
        const { status } = req.query;
        let rows = Object.values(ledger.state.freezes);
        if (status) rows = rows.filter((f) => f.status === status);
        return { count: rows.length, freezes: rows };
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
        const ms = Object.values(ledger.state.mappings);
        const fz = Object.values(ledger.state.freezes);
        return {
          component: 'mapper',
          totals: {
            mappings: ms.length,
            mapFailed: ms.filter((m) => m.status === 'MAP_FAILED').length,
            egressQueued: ms.filter((m) => m.egressStatus === 'QUEUED').length,
            egressPublished: ms.filter((m) => m.egressStatus === 'PUBLISHED').length,
            egressFailed: ms.filter((m) => m.egressStatus === 'FAILED').length,
            sourceAcked: ms.filter((m) => m.sourceAcked).length,
            completed: ms.filter((m) => m.completed).length,
          },
          freezes: {
            open: fz.filter((f) => f.status === 'FROZEN').length,
            resolved: fz.filter((f) => f.status !== 'FROZEN').length,
          },
          lastSeq: ledger.seq,
        };
      },
    },
  ];

  const server = await createServer(routes, port);
  return { server, ledger };
}

// Without the original body we cannot remap from hashes alone; operator must
// supply a corrected body (returns undefined -> transform sees {} if needed).
function reconstructHint() {
  return undefined;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.env.PORT || 8301);
  const dir = process.env.DATA_DIR || './data/mapper';
  startMapper(port, dir);
}
