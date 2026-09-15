// Shared helpers: hashing, canonical JSON, time, ids, retries.
import crypto from 'node:crypto';

export function nowIso() {
  return new Date().toISOString();
}

let counter = 0;
export function pid() {
  return process.pid;
}

// Process-unique monotonic-ish id (timestamp + counter + random suffix).
export function uid(prefix = '') {
  counter = (counter + 1) % 1_000_000;
  const t = Date.now().toString(36);
  const c = counter.toString(36).padStart(3, '0');
  const r = crypto.randomBytes(4).toString('hex');
  return `${prefix}${t}${c}${r}`;
}

// Deterministic canonical serialization (object keys sorted).
export function canonical(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return '[' + value.map(canonical).join(',') + ']';
  }
  const keys = Object.keys(value).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
}

// Content hash over the *business body only* (headers/bridge markers excluded).
export function bodyHash(body) {
  return crypto.createHash('sha256').update(canonical(body ?? null)).digest('hex').slice(0, 32);
}

// Stable business key if present, otherwise derived from body hash.
export function businessKey(body) {
  if (body && typeof body === 'object' && typeof body.id === 'string') return body.id;
  if (body && typeof body === 'object' && typeof body.orderId === 'string') return body.orderId;
  return 'bh:' + bodyHash(body);
}

// Header key on a wire envelope that carries bridge loop markers.
export const MARKER_HEADER = 'x-bridge-marker';
export const BRIDGE_INSTANCE = process.env.BRIDGE_INSTANCE || 'bridge-1';
export const MARKER_TTL_MAX = 3;

// marker format: v1:<instanceId>:<originSide>:<mappingId>:<ttl>
export function makeMarker(instance, originSide, mappingId, ttl) {
  return `v1:${instance}:${originSide}:${mappingId}:${ttl}`;
}

export function parseMarker(marker) {
  if (typeof marker !== 'string' || !marker.startsWith('v1:')) return null;
  const parts = marker.split(':');
  if (parts.length !== 5) return null;
  const [, instance, originSide, mappingId, ttlRaw] = parts;
  const ttl = Number(ttlRaw);
  if (!Number.isInteger(ttl)) return null;
  return { instance, originSide, mappingId, ttl };
}

// Sleep helper for retry backoff.
export function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function logLevel() {
  return (process.env.LOG_LEVEL || 'info').toLowerCase();
}
