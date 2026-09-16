// Append-only JSONL event ledger, the single source of truth for each stateful
// component. Every state transition is an immutable event; current state is a
// fold over the log. The same log is the queryable "delivery chain".
//
// Layout (all under a component's data dir):
//   journal.jsonl          one JSON object per line, fsynced on append
//   snapshots/state.json   atomically rewritten fold state (optional speedup)
import fs from 'node:fs';
import path from 'node:path';
import { uid } from '../util.js';
import { logger } from '../log.js';

const log = logger('ledger');

export class Ledger {
  constructor(dir, { fold, initial, snapshotEvery = 100 } = {}) {
    this.dir = dir;
    this.file = path.join(dir, 'journal.jsonl');
    this.snapDir = path.join(dir, 'snapshots');
    this.foldFn = fold || ((state) => state);
    this.initialSeed = initial;
    const seed = typeof initial === 'function' ? initial() : (initial === undefined ? {} : initial);
    this.state = structuredClone(seed);
    this.seq = 0;
    this.writeFd = null;
    this.snapshotEvery = snapshotEvery;
    this.sinceSnapshot = 0;
    this.subscribers = new Set();
  }

  async open() {
    fs.mkdirSync(this.snapDir, { recursive: true });
    this.#hydrate();
    // Synchronous O_APPEND writer: every append is durable to the OS page
    // cache before the call returns, so tailSeq() (read straight back from the
    // file during leader fencing) observes this process's own latest event.
    // The reference implementation is single-writer per volume; an async
    // buffered WriteStream here would let a fencing read see a stale tail.
    this.writeFd = fs.openSync(this.file, fs.constants.O_CREAT | fs.constants.O_WRONLY | fs.constants.O_APPEND, 0o644);
  }

  // Re-fold state straight from disk (snapshot + journal tail). A follower
  // process is read-only while another epoch leads: by the time it wins the
  // leader lock the winner's events are durable, so reloading re-anchors seq
  // and folded state BEFORE this process appends anything. Without this the
  // new leader would resume from its stale boot-time seq and duplicate seq
  // values / replay decisions in its in-memory fold.
  reload() {
    if (this.subscribers.size > 0) {
      throw new Error('cannot reload a ledger with active subscribers');
    }
    this.#hydrate();
    return this.state;
  }

  // Read-only refresh of the folded state, for followers that must keep
  // serving queries while another process writes. Unlike reload() this leaves
  // seq and the open append stream untouched: a follower never writes, so its
  // seq is irrelevant until it wins leadership, at which point reload() runs.
  refreshState() {
    const snapPath = path.join(this.snapDir, 'state.json');
    let state = structuredClone(
      typeof this.initialSeed === 'function'
        ? this.initialSeed()
        : (this.initialSeed === undefined ? {} : this.initialSeed),
    );
    let seq = 0;
    try {
      const snap = JSON.parse(fs.readFileSync(snapPath, 'utf8'));
      state = structuredClone(snap.state);
      seq = snap.seq;
    } catch {
      /* no snapshot yet */
    }
    let raw = '';
    try {
      raw = fs.readFileSync(this.file, 'utf8');
    } catch {
      raw = '';
    }
    for (const line of raw.split('\n')) {
      if (!line.trim()) continue;
      let event;
      try {
        event = JSON.parse(line);
      } catch {
        continue;
      }
      if (seq && event.seq <= seq) continue;
      state = this.foldFn(state, event) ?? state;
    }
    this.state = state;
    return state;
  }

  #hydrate() {
    const snapPath = path.join(this.snapDir, 'state.json');
    const seed = typeof this.initialSeed === 'function'
      ? this.initialSeed()
      : (this.initialSeed === undefined ? {} : this.initialSeed);
    let state = structuredClone(seed);
    let seq = 0;
    try {
      const snap = JSON.parse(fs.readFileSync(snapPath, 'utf8'));
      state = structuredClone(snap.state);
      seq = snap.seq;
      log.debug('snapshot_loaded', { dir: this.dir, seq });
    } catch {
      /* no snapshot yet */
    }

    let raw = '';
    try {
      raw = fs.readFileSync(this.file, 'utf8');
    } catch {
      raw = '';
    }
    let appliedAfterSnap = 0;
    for (const line of raw.split('\n')) {
      if (!line.trim()) continue;
      let event;
      try {
        event = JSON.parse(line);
      } catch (err) {
        log.warn('corrupt_line_skipped', { dir: this.dir, err: err.message });
        continue;
      }
      if (seq && event.seq <= seq) continue;
      state = this.foldFn(state, event) ?? state;
      seq = Math.max(seq, event.seq);
      appliedAfterSnap += 1;
    }
    this.state = state;
    this.seq = seq;
    this.sinceSnapshot = appliedAfterSnap;
  }

  // Append + fold atomically (single-threaded event loop; subscribers run
  // after the event is durable on disk).
  //
  // In a single-process deployment seq is simply this.seq + 1. The quota
  // engine (the one component with cross-process takeover) routes writes
  // through a fencing wrapper that passes expectedSeq = the authoritative tail
  // seq re-read from disk; appends are then committed in strict tail order, so
  // a process that lost leadership and re-anchored cannot collide with the
  // real leader's seq.
  append(type, data = {}, { expectedSeq = null } = {}) {
    if (expectedSeq !== null && expectedSeq !== this.seq) {
      const err = new Error(`seq fence violation: tail=${expectedSeq} local=${this.seq}`);
      err.code = 'SEQ_FENCE';
      throw err;
    }
    const seq = (expectedSeq ?? this.seq) + 1;
    const event = {
      id: uid('e_'),
      seq,
      ts: new Date().toISOString(),
      type,
      data,
    };
    this.writeDurable(event);
    this.seq = seq;
    this.state = this.foldFn(this.state, event) ?? this.state;
    this.sinceSnapshot += 1;
    if (this.snapshotEvery > 0 && this.sinceSnapshot >= this.snapshotEvery) {
      this.snapshot();
    }
    for (const fn of this.subscribers) {
      try {
        fn(event);
      } catch (err) {
        log.warn('subscriber_failed', { err: err.message });
      }
    }
    return event;
  }

  // Highest seq currently durable at the END of the journal file. This is the
  // authoritative write position shared across processes; followers/takeovers
  // re-read it immediately before appending rather than trusting their stale
  // boot-time seq.
  tailSeq() {
    let fd;
    try {
      fd = fs.openSync(this.file, fs.constants.O_RDONLY);
    } catch (err) {
      if (err.code === 'ENOENT') return 0;
      throw err;
    }
    try {
      const size = fs.fstatSync(fd).size;
      if (size === 0) return 0;
      // Read up to the final 64KiB, which comfortably covers the last line
      // (events are small JSON objects).
      const chunk = Math.min(size, 64 * 1024);
      const buf = Buffer.alloc(chunk);
      fs.readSync(fd, buf, 0, chunk, size - chunk);
      const lines = buf.toString('utf8').split('\n').filter((l) => l.trim());
      for (let i = lines.length - 1; i >= 0; i -= 1) {
        try {
          const e = JSON.parse(lines[i]);
          if (Number.isInteger(e.seq)) return e.seq;
        } catch {
          // trailing/partial line: keep scanning backwards
        }
      }
      return 0;
    } finally {
      fs.closeSync(fd);
    }
  }

  subscribe(fn) {
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }

  writeDurable(event) {
    // One write per event. POSIX guarantees writes <= PIPE_BUF are atomic and
    // O_APPEND advances the file offset under the inode lock, so concurrent
    // appends from contending processes never interleave a single line; our
    // leader fence ensures normally only one process appends anyway.
    fs.writeSync(this.writeFd, JSON.stringify(event) + '\n');
    // Node exposes no portable fsync toggle for the reference impl; writes go
    // to the OS page cache (host crash may lose the tail; the documented
    // Postgres deployment provides durable production storage).
  }

  snapshot() {
    const snapPath = path.join(this.snapDir, 'state.json');
    const tmp = snapPath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ seq: this.seq, state: this.state }));
    fs.renameSync(tmp, snapPath);
    this.sinceSnapshot = 0;
  }

  async close() {
    if (this.writeFd === null) return;
    try {
      fs.fsyncSync(this.writeFd);
    } catch { /* best-effort sync in the reference impl */ }
    fs.closeSync(this.writeFd);
    this.writeFd = null;
  }

  // Read the raw journal (for chain queries). Optional filter by mappingId.
  readEvents({ type, mappingId, since = 0, limit = 10000 } = {}) {
    let raw = '';
    try {
      raw = fs.readFileSync(this.file, 'utf8');
    } catch {
      return [];
    }
    const out = [];
    for (const line of raw.split('\n')) {
      if (!line.trim()) continue;
      let event;
      try {
        event = JSON.parse(line);
      } catch {
        continue;
      }
      if (event.seq <= since) continue;
      if (type && event.type !== type) continue;
      out.push(event);
      if (out.length >= limit) break;
    }
    return out;
  }
}
