// Simulated messaging system A.
//
// Numbering:   per-message string-ish integer sequence ("101", "102", ...)
// Ack:         INDIVIDUAL ack per sequence (gaps allowed)
// Redelivery:  unacked messages become visible again after a visibility timeout
//              (at-most-once-n attempts model; redeliveries carry attempt>1)
//
// Wire envelope:
//   { seq: "101", headers: {...}, body: <json>, attempt: 1 }
import { createServer } from '../http.js';

const state = {
  nextSeq: 1,
  // seq(string) -> { seq, headers, body, attempt, visibleAt, done }
  messages: new Map(),
  // idempotencyKey -> seq
  idem: new Map(),
};

function publishNow({ headers = {}, body, idempotencyKey }) {
  if (idempotencyKey && state.idem.has(idempotencyKey)) {
    const seq = state.idem.get(idempotencyKey);
    return { seq, duplicate: true, message: state.messages.get(seq) };
  }
  const seq = String(state.nextSeq++);
  const msg = { seq, headers: { ...headers }, body, attempt: 1, visibleAt: Date.now(), done: false };
  state.messages.set(seq, msg);
  if (idempotencyKey) state.idem.set(idempotencyKey, seq);
  return { seq, duplicate: false, message: msg };
}

function wire(m) {
  return { seq: m.seq, headers: m.headers, body: m.body, attempt: m.attempt };
}

export async function startBrokerA(port) {
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
          const visMs = body.visibilityMs ?? 5000;
          const now = Date.now();
          const due = [...state.messages.values()]
            .filter((m) => !m.done && m.visibleAt <= now)
            .sort((a, b) => Number(a.seq) - Number(b.seq))
            .slice(0, max);
          const msgs = due.map((m) => {
            m.visibleAt = now + visMs;
            return wire(m);
          });
          return { system: 'A', messages: msgs };
        },
      },
      {
        method: 'POST',
        pattern: '/ack/:seq',
        handler: async (req, res, params) => {
          const m = state.messages.get(params.seq);
          if (!m) return { ok: true, duplicate: true, reason: 'unknown_seq' };
          if (m.done) return { ok: true, duplicate: true, reason: 'already_acked' };
          m.done = true;
          return { ok: true, duplicate: false };
        },
      },
      // Test/admin: rewrite an unacked message body and force immediate
      // redelivery, simulating a sequence-number/content conflict on resume.
      {
        method: 'POST',
        pattern: '/admin/rewrite/:seq',
        handler: async (req, res, params, body = {}) => {
          const m = state.messages.get(params.seq);
          if (!m) return { error: 'not_found' };
          if (m.done) return { error: 'already_acked' };
          if (body.body !== undefined) m.body = body.body;
          if (body.headers) m.headers = { ...m.headers, ...body.headers };
          m.attempt += 1;
          m.visibleAt = Date.now();
          return { ok: true, seq: m.seq, attempt: m.attempt, body: m.body };
        },
      },
      {
        method: 'GET',
        pattern: '/health',
        handler: async () => ({ ok: true, system: 'A' }),
      },
      {
        method: 'GET',
        pattern: '/admin/state',
        handler: async () => ({
          system: 'A',
          messages: [...state.messages.values()].map(({ visibleAt, ...rest }) => ({
            ...rest,
            visibleIn: Math.max(0, Math.round((visibleAt - Date.now()) / 100) / 10),
          })),
        }),
      },
    ],
    port,
  );
}

// Allow running standalone: node src/systems/broker-a.js <port>
if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.argv[2] || process.env.PORT || 8101);
  startBrokerA(port);
}
