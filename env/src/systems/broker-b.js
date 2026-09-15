// Simulated messaging system B.
//
// Numbering:   monotonically increasing INTEGER sequences starting at 1,
//              contiguous, no gaps ("log" style)
// Ack:         CUMULATIVE only — ack(n) acknowledges every seq <= n
// Redelivery:  pull returns all seq > ackWatermark (a redelivery is the same
//              identical record; B has no visibility timeout concept)
//
// Wire envelope:
//   { seq: 1001, headers: {...}, body: <json> }
import { createServer } from '../http.js';

const state = {
  nextSeq: 1,
  // seq(number) -> { seq, headers, body }
  messages: new Map(),
  ackWatermark: 0,
  idem: new Map(),
};

function publishNow({ headers = {}, body, idempotencyKey }) {
  if (idempotencyKey && state.idem.has(idempotencyKey)) {
    const seq = state.idem.get(idempotencyKey);
    return { seq, duplicate: true, message: state.messages.get(seq) };
  }
  const seq = state.nextSeq++;
  const msg = { seq, headers: { ...headers }, body };
  state.messages.set(seq, msg);
  if (idempotencyKey) state.idem.set(idempotencyKey, seq);
  return { seq, duplicate: false, message: msg };
}

export async function startBrokerB(port) {
  return createServer(
    [
      {
        method: 'POST',
        pattern: '/publish',
        handler: async (req, res, p, body) => {
          if (!body || body.body === undefined) return { error: 'body_required' };
          const r = publishNow(body);
          return { ok: true, seq: r.seq, duplicate: r.duplicate };
        },
      },
      {
        method: 'POST',
        pattern: '/pull',
        handler: async (req, res, p, body = {}) => {
          const max = body.max ?? 10;
          const after = Number.isInteger(body.after) ? body.after : state.ackWatermark;
          const msgs = [];
          for (let s = after + 1; s < state.nextSeq && msgs.length < max; s++) {
            const m = state.messages.get(s);
            if (m) msgs.push({ seq: m.seq, headers: m.headers, body: m.body });
          }
          return { system: 'B', ackWatermark: state.ackWatermark, messages: msgs };
        },
      },
      {
        method: 'POST',
        pattern: '/ack',
        handler: async (req, res, p, body = {}) => {
          const seq = body.seq;
          if (!Number.isInteger(seq)) return { error: 'seq_integer_required' };
          if (seq < state.ackWatermark) {
            return { ok: true, duplicate: true, watermark: state.ackWatermark, reason: 'below_watermark' };
          }
          if (seq >= state.nextSeq) {
            // Cannot ack beyond what exists
            return { ok: false, error: 'seq_out_of_range', watermark: state.ackWatermark };
          }
          const prev = state.ackWatermark;
          state.ackWatermark = seq;
          return {
            ok: true,
            duplicate: prev === seq,
            advanced: prev < seq,
            watermark: state.ackWatermark,
          };
        },
      },
      {
        method: 'GET',
        pattern: '/health',
        handler: async () => ({ ok: true, system: 'B', ackWatermark: state.ackWatermark }),
      },
      {
        method: 'POST',
        pattern: '/admin/rewrite/:seq',
        handler: async (req, res, params, body = {}) => {
          const seq = Number(params.seq);
          const m = state.messages.get(seq);
          if (!m) return { error: 'not_found' };
          if (seq <= state.ackWatermark) return { error: 'already_acked_cumulatively' };
          if (body.body !== undefined) m.body = body.body;
          if (body.headers) m.headers = { ...m.headers, ...body.headers };
          return { ok: true, seq: m.seq, body: m.body };
        },
      },
      {
        method: 'GET',
        pattern: '/admin/state',
        handler: async () => ({
          system: 'B',
          ackWatermark: state.ackWatermark,
          nextSeq: state.nextSeq,
          messages: [...state.messages.values()],
        }),
      },
    ],
    port,
  );
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.argv[2] || process.env.PORT || 8201);
  startBrokerB(port);
}
