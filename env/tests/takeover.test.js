// Split-brain takeover regression test.
//
// Scenario (the reported incident):
//   1. kill -9 the process that granted a reservation (no graceful release)
//   2. wait until the held credential (reservation) has expired
//   3. start TWO replacement processes in parallel against the same volume
//
// Required behavior:
//   - during recovery exactly ONE writer is elected (both health checks must
//     never report leader simultaneously)
//   - the same reservationId produces EXPIRY_RECLAIMED at most once in the
//     durable journal; seq values are contiguous, no duplicates
//   - the losing process must keep running as a read-only follower and append
//     nothing
//
// Run with: node tests/takeover.test.js
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import net from 'node:net';

import { Http } from '../src/client.js';
import { sleep } from '../src/util.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const worker = path.join(here, 'fixtures', 'quota-worker.js');

async function freePort() {
  return await new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

async function waitFor(fn, { timeout = 8000, every = 25, label = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  let lastErr;
  while (Date.now() < deadline) {
    try {
      const v = await fn();
      if (v) return v;
    } catch (err) { lastErr = err; }
    await sleep(every);
  }
  throw new Error(`timeout waiting for ${label}: ${lastErr?.message || ''}`);
}

function spawnWorker(dataDir, port, leaseTtlMs) {
  const child = spawn(process.execPath, [worker, dataDir, String(port), String(leaseTtlMs)], {
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  let ready = '';
  let readyInfo = null;
  child.stdout.on('data', (b) => {
    ready += b.toString();
    const m = ready.match(/quota-worker-ready port=(\d+) epoch=(\S+)/);
    if (m && !readyInfo) readyInfo = { port: Number(m[1]), epoch: m[2] };
  });
  child.waitReady = async () => {
    await waitFor(() => readyInfo, { timeout: 5000, label: `worker ${child.pid} ready` });
    return readyInfo;
  };
  child.kill9 = () => child.kill('SIGKILL');
  return child;
}

function readJournal(dataDir) {
  const raw = fs.readFileSync(path.join(dataDir, 'journal.jsonl'), 'utf8');
  const events = [];
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;
    events.push(JSON.parse(line));
  }
  return events;
}

test('parallel replacements: exactly one writer, one reclaim, no dup seq', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'quota-takeover-'));
  const children = [];
  const cleanup = async () => {
    for (const c of children) {
      try { c.kill('SIGKILL'); } catch { /* ignore */ }
    }
    await sleep(100);
    fs.rmSync(dataDir, { recursive: true, force: true });
  };

  try {
    const LEASE_TTL = 600;
    const HOLD_TTL = 400; // credential expires shortly after the crash

    // --- step 1: original leader grants a credential ---------------------
    const p1 = await freePort();
    const a = spawnWorker(dataDir, p1, LEASE_TTL);
    children.push(a);
    await a.waitReady();
    const http1 = new Http(`http://localhost:${p1}`);
    await waitFor(async () => (await http1.get('/health')).leader === true,
      { label: 'original leader', timeout: 3000 });
    await http1.post('/admin/tenants', {
      tenantId: 'acme',
      in: { ratePerSec: 100, burst: 100, maxInflight: 100 },
      out: { ratePerSec: 100, burst: 100, maxInflight: 100 },
    });
    const grant = await http1.post('/reserve', {
      tenantId: 'acme', direction: 'in', requestId: 'only-hold', holdTtlMs: HOLD_TTL,
    });
    assert.equal(grant.result, 'GRANTED');
    const reservationId = grant.reservationId;

    // Crash it hard: no lock release, no graceful settle.
    a.kill9();
    await waitFor(() => a.exitCode !== null || a.signalCode === 'SIGKILL',
      { label: 'original killed', timeout: 2000 });

    // --- step 2: wait for the held credential AND lease to be expired ----
    await sleep(HOLD_TTL + LEASE_TTL + 300);

    // --- step 3: two replacements boot in parallel -----------------------
    const p2 = await freePort();
    const p3 = await freePort();
    const b = spawnWorker(dataDir, p2, LEASE_TTL);
    const c = spawnWorker(dataDir, p3, LEASE_TTL);
    children.push(b, c);
    const [infoB, infoC] = await Promise.all([b.waitReady(), c.waitReady()]);

    const http2 = new Http(`http://localhost:${p2}`);
    const http3 = new Http(`http://localhost:${p3}`);

    // Poll both health endpoints across the takeover window; they must never
    // simultaneously claim leadership.
    let bothLeaderEver = false;
    let neitherLeaderEver = false;
    const deadline = Date.now() + 4000;
    let leaderEpoch = null;
    while (Date.now() < deadline) {
      const [hb, hc] = await Promise.all([
        http2.get('/health').catch(() => null),
        http3.get('/health').catch(() => null),
      ]);
      const lb = hb?.leader === true;
      const lc = hc?.leader === true;
      if (lb && lc) bothLeaderEver = true;
      if (!lb && !lc) neitherLeaderEver = true;
      if (lb || lc) {
        const e = lb ? hb.epoch : hc.epoch;
        if (leaderEpoch && e !== leaderEpoch) {
          // Leadership may legitimately transfer; but while two processes are
          // up it must never overlap (the old one fences on lock inode check).
          if (lb && lc) bothLeaderEver = true;
        }
        leaderEpoch = e;
      }
      await sleep(15);
    }
    assert.equal(bothLeaderEver, false, 'both replacements reported leader at the same instant');

    // After the window, settle which one won: exactly one of the two.
    const health = async (h) => (await h.get('/health').catch(() => null));
    let hb = await health(http2);
    let hc = await health(http3);
    if (!(hb?.leader || hc?.leader)) {
      await waitFor(async () => {
        [hb, hc] = await Promise.all([health(http2), health(http3)]);
        return hb?.leader || hc?.leader;
      }, { timeout: 3000, label: 'a replacement becomes leader' });
    }
    // Extra observation window: leadership must converge, never overlap.
    for (let i = 0; i < 20; i += 1) {
      await sleep(20);
      const [b2, c2] = await Promise.all([health(http2), health(http3)]);
      assert.ok(!(b2?.leader && c2?.leader), 'both leaders during convergence window');
      if (b2?.leader) hb = b2;
      if (c2?.leader) hc = c2;
    }
    const finalB = hb;
    const finalC = hc;

    const leaders = [finalB, finalC].filter((h) => h?.leader === true);
    assert.equal(leaders.length, 1, `expected exactly one leader, got ${leaders.length}`);
    const leaderHttp = finalB?.leader ? http2 : http3;
    const followerHttp = finalB?.leader ? http3 : http2;

    // --- the expired hold is reclaimed exactly once ----------------------
    await waitFor(async () => {
      const v = await leaderHttp.get(`/reservations/${reservationId}`).catch(() => null);
      return v?.status === 'EXPIRED' ? v : null;
    }, { label: 'reservation expired/reclaimed', timeout: 4000 });

    // Give every late timer tick a chance to wrongly double-reclaim.
    await sleep(LEASE_TTL * 3);

    const events = readJournal(dataDir);
    const reclaims = events.filter(
      (e) => e.type === 'EXPIRY_RECLAIMED' && e.data.reservationId === reservationId,
    );
    assert.equal(reclaims.length, 1,
      `reservation reclaimed ${reclaims.length} times (want exactly 1)`);

    // Every seq is unique and contiguous: two writers continuing to append
    // would both restart from the same boot-time seq.
    const seqs = events.map((e) => e.seq);
    assert.equal(new Set(seqs).size, seqs.length, 'duplicate seq values in journal');
    for (let i = 1; i < seqs.length; i += 1) {
      assert.equal(seqs[i], seqs[i - 1] + 1, `seq gap between ${seqs[i - 1]} and ${seqs[i]}`);
    }

    // Only the winning epoch ever appears as a lease owner after the crash.
    const leaseEpochs = new Set(
      events.filter((e) => e.type === 'LEASE_ACQUIRED').map((e) => e.data.ownerEpoch),
    );
    const postCrashEpochs = new Set([infoB.epoch, infoC.epoch]);
    const leadingAfterCrash = [...leaseEpochs].filter((e) => postCrashEpochs.has(e));
    assert.equal(leadingAfterCrash.length, 1,
      `both epochs acquired a lease: ${leadingAfterCrash.join(',')}`);
    assert.ok(leadingAfterCrash.includes(leaders[0].epoch));

    // --- the loser is a read-only follower: writes are rejected ----------
    const followerHealth = await followerHttp.get('/health');
    assert.equal(followerHealth.leader, false);
    const blocked = await followerHttp.post('/admin/tenants', {
      tenantId: 'other', in: { ratePerSec: 1, burst: 1, maxInflight: 1 },
    }).then(() => null, (err) => err);
    assert.equal(blocked?.status, 503, 'follower accepted a write (503 expected)');
    assert.equal(blocked?.body?.error, 'not_leader');
    // Queries still served by the follower (read-only availability); its view
    // is refreshed by periodic read-only refolds.
    await waitFor(async () => {
      const v = await followerHttp.get(`/reservations/${reservationId}`).catch(() => null);
      return v?.status === 'EXPIRED' ? v : null;
    }, { label: 'follower read view caught up', timeout: 3000 });

    // --- exactly one reclaim visible through the leader's records too ----
    const recs = await leaderHttp.get('/tenants/acme/records?type=EXPIRY_RECLAIMED&limit=1000');
    assert.equal(recs.events.length, 1);

    // Token math stayed consistent: one cost-1 hold refunded exactly once,
    // bucket at full burst again rather than over-refunded.
    const tenant = await leaderHttp.get('/tenants/acme');
    assert.ok(tenant.directions.in.tokens >= 99 && tenant.directions.in.tokens <= 100.0001,
      `tokens ${tenant.directions.in.tokens} suggest double refund or missing refund`);
    assert.equal(tenant.directions.in.inflight, 0);
  } finally {
    await cleanup();
  }
});
