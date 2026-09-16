// Standalone quota process used by the split-brain takeover regression test.
// Usage:
//   node tests/fixtures/quota-worker.js <dataDir> <port> <leaseTtlMs>
//
// It is deliberately kill -9 friendly: no graceful-shutdown handlers, so a
// SIGKILL leaves the leader.lock file on disk exactly like a real crash and
// the replacements must steal it after TTL expiry.
import { startQuota } from '../../src/quota/index.js';

const [dataDir, portArg, ttlArg] = process.argv.slice(2);
const port = Number(portArg);
const leaseTtlMs = Number(ttlArg);

const started = await startQuota(port, dataDir, {
  tickMs: 10,
  reaperMs: 10,
  leaseTtlMs,
});

// The listening HTTP server already keeps the event loop alive; nothing else
// is needed. No SIGKILL handler is registered on purpose — a hard kill must
// leave leader.lock behind exactly like a real crash.
process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));

// eslint-disable-next-line no-console
console.log(`quota-worker-ready port=${started.server.address().port} epoch=${started.engine.epoch}`);
