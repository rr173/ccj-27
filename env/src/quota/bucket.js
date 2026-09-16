// Token bucket primitives for rate limiting. A bucket is a plain serializable
// record { tokens, updatedAtMs } so it lives in the folded ledger state and
// survives snapshots/restarts. All refill math is lazy: we only recompute
// tokens at the moment we look at the bucket, against a caller-supplied clock.
//
// Semantics used by the quota service:
//   - tokens are *consumed* when a reservation is granted (pre-emption)
//   - tokens are *refunded* on abort and on expiry-reclaim, never on completion
//   - ratePerSec / burst come from the current budget revision; after a config
//     change the same bucket continues from its current token level (clamped to
//     the new burst), so rate raises take effect on the very next refill

export function createBucket(burst, nowMs, tokens = burst) {
  return { tokens, updatedAtMs: nowMs };
}

// Lazy refill against the configured rate. Pure: returns a new record.
export function refill(bucket, ratePerSec, burst, nowMs) {
  if (!bucket) return { tokens: burst, updatedAtMs: nowMs };
  const elapsed = Math.max(0, nowMs - bucket.updatedAtMs);
  if (elapsed <= 0 || ratePerSec <= 0) return bucket;
  const tokens = Math.min(burst, bucket.tokens + (elapsed * ratePerSec) / 1000);
  return { tokens, updatedAtMs: nowMs };
}

// Non-mutating peek of the token level at time nowMs.
export function available(bucket, ratePerSec, burst, nowMs) {
  return refill(bucket, ratePerSec, burst, nowMs).tokens;
}

// Milliseconds until the bucket can cover `demand` tokens (0 if already).
export function waitMs(bucket, ratePerSec, burst, demand, nowMs) {
  const tokens = available(bucket, ratePerSec, burst, nowMs);
  if (tokens >= demand) return 0;
  if (ratePerSec <= 0) return Infinity;
  return Math.ceil(((demand - tokens) * 1000) / ratePerSec);
}

// Add tokens back, capped at burst (abort settlement / expiry reclaim). The
// natural rate accrual during the hold is applied first, otherwise aborting a
// long hold would lose the tokens that regenerated while it was outstanding.
export function refund(bucket, amount, ratePerSec, burst, nowMs) {
  const base = refill(bucket, ratePerSec, burst, nowMs);
  return { tokens: Math.min(burst, base.tokens + amount), updatedAtMs: nowMs };
}
