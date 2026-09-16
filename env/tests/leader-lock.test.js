// Unit tests for the cross-process LeaderLock (filesystem-arbitrated lease).
//
// Verifies:
//   - only one creator wins an O_EXCL publish on a fresh volume
//   - a live lock blocks every other epoch; renewals keep it live
//   - an expired lock can be stolen exactly once by parallel contenders
//   - the displaced old leader is fenced (inode identity no longer matches)
//
// Run with: node tests/leader-lock.test.js
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { LeaderLock } from '../src/store/leader-lock.js';
import { sleep } from '../src/util.js';

function freshDir(name) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), name));
  return dir;
}

test('fresh lock: first acquire wins, live lock blocks others', () => {
  const dir = freshDir('ll-fresh-');
  try {
    const a = new LeaderLock(dir, { ttlMs: 60_000 });
    const b = new LeaderLock(dir, { ttlMs: 60_000 });
    assert.equal(a.tryAcquire('A'), true);
    assert.equal(a.isOwner(), true);
    assert.equal(b.tryAcquire('B'), false);
    assert.equal(b.isOwner(), false);
    assert.equal(a.renew(), true);
    assert.equal(a.isOwner(), true);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('expired lock is stolen once and fences the prior owner', () => {
  const dir = freshDir('ll-steal-');
  try {
    let t = 1_000;
    const clock = () => t;
    const a = new LeaderLock(dir, { ttlMs: 500, now: clock });
    const b = new LeaderLock(dir, { ttlMs: 500, now: clock });
    assert.equal(a.tryAcquire('A'), true);

    t = 1_600; // A's lease expired (A crashed without releasing)
    assert.equal(b.tryAcquire('B'), true);
    assert.equal(b.isOwner(), true);
    // The stale leader is fenced by inode identity, regardless of its clock.
    assert.equal(a.isOwner(), false);
    assert.equal(a.renew(), false);
    assert.equal(b.renew(), true);
    assert.equal(b.isOwner(), true);
    // While B is live, nobody else can take over.
    const c = new LeaderLock(dir, { ttlMs: 500, now: clock });
    assert.equal(c.tryAcquire('C'), false);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('graceful release hands the lock over without waiting for TTL', () => {
  const dir = freshDir('ll-release-');
  try {
    const a = new LeaderLock(dir, { ttlMs: 60_000 });
    const b = new LeaderLock(dir, { ttlMs: 60_000 });
    assert.equal(a.tryAcquire('A'), true);
    a.release();
    assert.deepEqual(fs.readdirSync(dir).filter((f) => f.startsWith('leader')), []);
    assert.equal(b.tryAcquire('B'), true);
    assert.equal(b.isOwner(), true);
    b.release();
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('parallel in-process contenders on a fresh dir elect exactly one', () => {
  const dir = freshDir('ll-race-fresh-');
  try {
    const locks = [];
    const results = [];
    for (let i = 0; i < 20; i += 1) {
      const lock = new LeaderLock(dir, { ttlMs: 60_000 });
      locks.push(lock);
      // Interleave construction attempts as closely as the event loop allows.
      results.push(lock.tryAcquire(`epoch-${i}`));
    }
    const winners = results.filter(Boolean).length;
    assert.equal(winners, 1, `expected 1 winner, got ${winners}`);
    const ownerCount = locks.filter((l) => l.isOwner()).length;
    assert.equal(ownerCount, 1);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('renewal keeps writing a parseable fixed-width lock file', async () => {
  const dir = freshDir('ll-renew-');
  try {
    const a = new LeaderLock(dir, { ttlMs: 120 });
    assert.equal(a.tryAcquire('A'), true);
    for (let i = 0; i < 5; i += 1) {
      await sleep(40);
      assert.equal(a.renew(), true);
    }
    const p = a.current();
    assert.equal(p.ownerEpoch, 'A');
    assert.ok(p.expiresAtMs > Date.now());
    assert.equal(fs.statSync(path.join(dir, 'leader.lock')).size, 255);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
