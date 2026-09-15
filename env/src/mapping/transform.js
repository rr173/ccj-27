// Envelope + body mapping between systems A and B.
//
// Both sides carry headers + body JSON, but numbering/ack semantics differ.
// The structural envelope translation is symmetric; the business body mapping
// is deliberately explicit (order <-> orderCreated) so failures are real
// failure modes instead of an opaque pass-through.

// Order of body shapes:
//   A-native:  { id, type: 'order', amount, currency, customerRef, at }
//   B-native:  { eventId, event: 'orderCreated', total, ccy, ref, occurredAt }
// Control keys (double-underscore prefixed) are test/demo failure-injection
// switches; they must survive body translation but are never business data.
function carryControlKeys(src, dst) {
  if (src && typeof src === 'object') {
    for (const k of Object.keys(src)) {
      if (k.startsWith('__')) dst[k] = src[k];
    }
  }
  return dst;
}

export function transformBody(direction, body, { fail = false } = {}) {
  if (body && body.__mapFail) {
    // deterministic injection hook for tests/demos
    throw new MapError('forced_map_failure', 'body flagged with __mapFail');
  }
  if (fail) throw new MapError('forced_map_failure', 'mapper configured to fail this attempt');

  if (direction === 'A->B') {
    if (body && body.type === 'order') {
      return carryControlKeys(body, {
        eventId: body.id,
        event: 'orderCreated',
        total: body.amount,
        ccy: body.currency,
        ref: body.customerRef,
        occurredAt: body.at,
      });
    }
    // Pass-through for marker-only / unknown shapes (still canonicalized).
    return carryControlKeys(body, { ...body });
  }

  if (direction === 'B->A') {
    if (body && body.event === 'orderCreated') {
      return carryControlKeys(body, {
        id: body.eventId,
        type: 'order',
        amount: body.total,
        currency: body.ccy,
        customerRef: body.ref,
        at: body.occurredAt,
      });
    }
    return carryControlKeys(body, { ...body });
  }

  throw new MapError('unknown_direction', `no mapping for direction ${direction}`);
}

export class MapError extends Error {
  constructor(code, detail) {
    super(detail || code);
    this.code = code;
  }
}

// Convert a pulled ingress envelope into the peer-side outbound envelope.
// Headers are preserved; bridge markers are injected later by the deliverer.
export function translate(direction, ingress) {
  const mappedBody = transformBody(direction, ingress.body);
  return {
    headers: { ...(ingress.headers || {}) },
    body: mappedBody,
  };
}

export function peer(side) {
  return side === 'A' ? 'B' : 'A';
}

export function directionOf(ingressSide) {
  return `${ingressSide}->${peer(ingressSide)}`;
}
