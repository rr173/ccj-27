// Cross-process leader election backed by lock files on the shared data
// volume. The JSONL journal is a single-writer store; this lock makes that
// invariant hold across processes/containers rather than merely trusting each
// process's in-memory view of the LEASE_ACQUIRED events.
//
// Why a file lock and not just the journal event?
//   Two replacements booting in parallel both replay the same log tail and
//   both see the previous lease as expired. Without a kernel-arbited primitive
//   both append LEASE_ACQUIRED and both believe they are leader: the same
//   reservation then gets reclaimed twice and seq values collide.
//
// Atomicity primitives (all POSIX, zero dependencies):
//   - open(2) O_CREAT|O_EXCL  : creation of a path succeeds for exactly one
//                               caller; everyone else gets EEXIST
//   - link(2)                 : creating a NEW directory entry for an existing
//                               file fails with EEXIST if the target exists,
//                               atomically in the kernel — used to publish the
//                               lockfile, so a fresh live lock can never be
//                               clobbered by rename(2)
//   - rename(2)               : only ever replaces a lock we first proved is
//                               the SAME expired inode (steal path)
//
// State:
//   - leader.lock holds {ownerEpoch, expiresAtMs}; the leader keeps the fd of
//     its published file open for its whole term and renews in place (one
//     constant-width write at offset 0 never grows the file, so a reader sees
//     old or new, never a split record)
//   - leader.takeover.<epoch> intent files serialize parallel stealers
//
// Ownership is proven by the held fd's file identity (dev+ino) matching the
// path on disk: if another process replaced the lock, the path points at a
// new inode and the stale leader fences itself immediately.
import fs from 'node:fs';
import path from 'node:path';
import { logger } from '../log.js';

const log = logger('leaderlock');
const LOCK_FILE = 'leader.lock';
// Payload is a fixed-width JSON line so renewal writes never grow the file.
const PAYLOAD_PAD = 256;

function encodePayload(payload) {
  const body = JSON.stringify(payload);
  if (body.length > PAYLOAD_PAD - 1) {
    throw new Error(`leader lock payload exceeds ${PAYLOAD_PAD - 1} bytes`);
  }
  return body.padEnd(PAYLOAD_PAD - 1, ' ');
}

function decodePayload(raw) {
  try {
    const p = JSON.parse(String(raw).trim());
    if (p && p.ownerEpoch && Number.isFinite(p.expiresAtMs)) return p;
  } catch { /* unreadable lock: treated as absent/stealable */ }
  return null;
}

function fid(stat) {
  return `${stat.dev}:${stat.ino}`;
}

function cleanTakeoverIntents(dir, selfPath, staleMs, now) {
  let names = [];
  try {
    names = fs.readdirSync(dir);
  } catch {
    return;
  }
  for (const name of names) {
    if (!name.startsWith('leader.takeover.')) continue;
    const p = path.join(dir, name);
    if (p === selfPath) continue;
    try {
      const ageMs = now() - fs.statSync(p).mtimeMs;
      // A live stealer holds its intent only for the few synchronous calls of
      // an acquisition; anything older than the grace period is from a dead
      // process and safe to sweep.
      if (ageMs > staleMs) fs.unlinkSync(p);
    } catch { /* vanished concurrently */ }
  }
}

export class LeaderLock {
  constructor(dataDir, { ttlMs, lockFile = LOCK_FILE, now = () => Date.now() } = {}) {
    if (!Number.isFinite(ttlMs) || ttlMs < 1) throw new Error('leader lock requires ttlMs >= 1');
    this.dir = dataDir;
    this.path = path.join(dataDir, lockFile);
    this.ttlMs = ttlMs;
    this.now = now;
    this.fd = null;
    this.fid = null;
    this.ownerEpoch = null;
    this.intentPath = null;
    // A takeover intent older than two lease terms belongs to a dead stealer.
    this.intentStaleMs = Math.max(2_000, ttlMs * 2);
  }

  readPayload() {
    try {
      return decodePayload(fs.readFileSync(this.path, 'utf8'));
    } catch {
      return null;
    }
  }

  // Snapshot of the lease currently on disk (or null).
  current() {
    return this.readPayload();
  }

  // Adopt an already-published lock file (verified identical inode + payload)
  // as our own. Callers own the lifecycle of their private temp files.
  #adoptPublished(payload) {
    const fd = fs.openSync(this.path, fs.constants.O_RDWR);
    try {
      const mine = fs.fstatSync(fd);
      const cur = fs.statSync(this.path);
      if (fid(mine) !== fid(cur)) return false;
      const onDisk = this.readPayload();
      if (!onDisk || onDisk.ownerEpoch !== payload.ownerEpoch) return false;
      this.fd = fd;
      this.fid = fid(mine);
      this.ownerEpoch = payload.ownerEpoch;
      return true;
    } catch (err) {
      fs.closeSync(fd);
      throw err;
    }
  }

  // Publish a brand-new lock when no live lock exists. Build the file in a
  // private O_EXCL temp, then LINK it to the lock path: link fails with EEXIST
  // if any lock appears meanwhile, which closes the create/rename race.
  #publishViaLink(payload) {
    const tempPath = path.join(
      this.dir,
      `leader.lock.new.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}`,
    );
    let tfd = fs.openSync(
      tempPath,
      fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_RDWR,
      0o600,
    );
    let adopted = false;
    try {
      fs.writeSync(tfd, encodePayload(payload));
      fs.fsyncSync(tfd);
      fs.closeSync(tfd);
      tfd = -1;
      try {
        fs.linkSync(tempPath, this.path);
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
        return false; // someone published first
      }
      adopted = this.#adoptPublished(payload);
      return adopted;
    } finally {
      if (tfd !== -1) {
        try { fs.closeSync(tfd); } catch { /* ignore */ }
      }
      // After link() the temp name and leader.lock point to ONE file; removing
      // our private temp name leaves the published leader.lock intact.
      try { fs.rmSync(tempPath, { force: true }); } catch { /* ignore */ }
    }
  }

  // Two-phase takeover of an EXPIRED (or unreadable) lock.
  steal(payload, expiredIno) {
    cleanTakeoverIntents(this.dir, this.intentPath, this.intentStaleMs, this.now);

    // Phase 1: O_EXCL intent serializes parallel stealers in the kernel.
    const intentPath = path.join(this.dir, `leader.takeover.${payload.ownerEpoch}`);
    try {
      const ifd = fs.openSync(
        intentPath,
        fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY,
        0o600,
      );
      fs.writeSync(ifd, String(this.now()));
      fs.closeSync(ifd);
      this.intentPath = intentPath;
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      this.intentPath = null;
      return false; // a live peer currently owns the takeover
    }

    try {
      // Under the claim, prove the lock is STILL the same expired inode. If a
      // peer already published a new live lock, stand down.
      let curStat;
      try {
        curStat = fs.statSync(this.path);
      } catch (err2) {
        if (err2.code !== 'ENOENT') throw err2;
        curStat = null;
      }
      const onDisk = this.readPayload();
      if (curStat && (fid(curStat) !== expiredIno
        || (onDisk && onDisk.ownerEpoch !== payload.ownerEpoch && onDisk.expiresAtMs > this.now()))) {
        return false;
      }

      // Build our replacement in a private O_EXCL temp...
      const tempPath = path.join(
        this.dir,
        `leader.lock.steal.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}`,
      );
      const tfd = fs.openSync(
        tempPath,
        fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_RDWR,
        0o600,
      );
      try {
        fs.writeSync(tfd, encodePayload(payload));
        fs.fsyncSync(tfd);
      } finally {
        fs.closeSync(tfd);
      }

      // Phase 2a: try a hard link first. If the expired path vanished (a peer
      // already replaced it), link either succeeds fresh or EEXISTs — both
      // cases resolve through the identity checks below.
      try {
        fs.linkSync(tempPath, this.path);
      } catch (err) {
        if (err.code !== 'EEXIST') {
          try { fs.rmSync(tempPath, { force: true }); } catch { /* ignore */ }
          throw err;
        }
        // Path exists: only the SAME expired inode may be replaced by rename.
        let again;
        try {
          again = fs.statSync(this.path);
        } catch (err2) {
          if (err2.code === 'ENOENT') again = null;
          else throw err2;
        }
        if (!again || fid(again) !== expiredIno) {
          try { fs.rmSync(tempPath, { force: true }); } catch { /* ignore */ }
          return false; // a new (live) lock is already there
        }
        fs.renameSync(tempPath, this.path);
      }

      try {
        return this.#adoptPublished(payload);
      } finally {
        try { fs.rmSync(tempPath, { force: true }); } catch { /* ignore */ }
      }
    } finally {
      if (this.intentPath) {
        try { fs.unlinkSync(this.intentPath); } catch { /* ignore */ }
        this.intentPath = null;
      }
    }
  }

  // Attempt to become leader for ownerEpoch. Returns true on success.
  //   - no lock / corrupt lock  -> atomic link-publish
  //   - expired lock            -> two-phase steal (intent + identity check)
  //   - live lock (other epoch) -> follower (false)
  tryAcquire(ownerEpoch) {
    if (this.fd !== null) return this.isOwner();
    const now = this.now();
    const existing = this.readPayload(); // null: absent or unreadable

    if (existing && existing.expiresAtMs > now && existing.ownerEpoch !== ownerEpoch) {
      return false; // a live leader holds the lock
    }

    const payload = { ownerEpoch, acquiredAtMs: now, expiresAtMs: now + this.ttlMs, ttlMs: this.ttlMs };

    if (!existing) {
      // Absent or unreadable. link-publish serializes parallel creators; if the
      // path turns out to hold an expired-but-well-formed lock, take the steal
      // path instead so its inode identity is checked.
      let pathStat = null;
      try {
        pathStat = fs.statSync(this.path);
      } catch {
        pathStat = null;
      }
      if (!pathStat) {
        for (let attempt = 0; attempt < 3; attempt += 1) {
          if (this.#publishViaLink(payload)) return true;
          const raced = this.readPayload();
          if (raced && raced.expiresAtMs > this.now() && raced.ownerEpoch !== ownerEpoch) return false;
          if (raced) break; // expired lock now present -> steal below
        }
      }
    }

    // Steal path (expired or unreadable). Bounded attempts: the O_EXCL intent
    // is held only for synchronous work, so retries are cheap.
    let expiredIno = null;
    try {
      expiredIno = fid(fs.statSync(this.path));
    } catch {
      expiredIno = null;
    }
    for (let attempt = 0; attempt < 5; attempt += 1) {
      if (expiredIno === null) {
        // The path was unreadable/absent: try link-publish before stealing.
        if (this.#publishViaLink(payload)) {
          log.info('leader_lock_created', { epoch: ownerEpoch, attempt });
          return true;
        }
        try {
          expiredIno = fid(fs.statSync(this.path));
        } catch {
          expiredIno = null;
        }
      }
      const fromEpoch = this.readPayload()?.ownerEpoch;
      if (this.steal(payload, expiredIno)) {
        log.info('leader_lock_stolen', { fromEpoch, epoch: ownerEpoch, attempt });
        return true;
      }
      const onDisk = this.readPayload();
      if (onDisk && onDisk.expiresAtMs > this.now() && onDisk.ownerEpoch !== ownerEpoch) {
        return false;
      }
      try {
        expiredIno = fid(fs.statSync(this.path));
      } catch {
        expiredIno = null;
      }
    }
    return false;
  }

  // True iff this instance still owns the live lock. The inode identity check
  // fences a leader whose expired lock was replaced out from under it even if
  // that process never noticed the LEASE_ACQUIRED event the winner appended.
  isOwner() {
    if (this.fd === null) return false;
    try {
      const onDisk = this.readPayload();
      if (!onDisk || onDisk.ownerEpoch !== this.ownerEpoch) return false;
      const mine = fs.fstatSync(this.fd);
      return fid(mine) === this.fid && fid(mine) === fid(fs.statSync(this.path));
    } catch {
      return false;
    }
  }

  renew() {
    if (!this.isOwner()) return false;
    const now = this.now();
    const payload = { ownerEpoch: this.ownerEpoch, renewedAtMs: now, expiresAtMs: now + this.ttlMs, ttlMs: this.ttlMs };
    try {
      // One write of constant padded size at offset 0 never extends the file;
      // the single local writer means concurrent readers see either the old
      // or the new fixed-width record, never a split one.
      fs.writeSync(this.fd, encodePayload(payload), 0, 'utf8', 0);
      fs.fsyncSync(this.fd);
      return true;
    } catch (err) {
      log.warn('leader_renew_failed', { err: err.message });
      return false;
    }
  }

  release() {
    if (this.intentPath) {
      try { fs.unlinkSync(this.intentPath); } catch { /* ignore */ }
      this.intentPath = null;
    }
    if (this.fd === null) return;
    const fd = this.fd;
    // Capture ownership while the fd is still wired up; isOwner() below reads
    // this.fd, so decide BEFORE clearing it.
    const mine = this.isOwner();
    this.fd = null;
    this.fid = null;
    this.ownerEpoch = null;
    try {
      // Only unlink if the path is still our inode; never remove a successor's
      // lock. Re-stat the held fd against the path directly.
      if (mine) {
        try { fs.unlinkSync(this.path); } catch (err) {
          if (err.code !== 'ENOENT') log.warn('leader_release_unlink_failed', { err: err.message });
        }
      }
    } finally {
      try { fs.closeSync(fd); } catch { /* ignore */ }
    }
  }
}
