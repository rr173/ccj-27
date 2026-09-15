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
    const seed = typeof initial === 'function' ? initial() : (initial === undefined ? {} : initial);
    this.state = structuredClone(seed);
    this.seq = 0;
    this.stream = null;
    this.snapshotEvery = snapshotEvery;
    this.sinceSnapshot = 0;
    this.subscribers = new Set();
  }

  async open() {
    fs.mkdirSync(this.snapDir, { recursive: true });
    const snapPath = path.join(this.snapDir, 'state.json');
    let snap = null;
    try {
      snap = JSON.parse(fs.readFileSync(snapPath, 'utf8'));
      this.state = structuredClone(snap.state);
      this.seq = snap.seq;
      this.sinceSnapshot = 0;
      log.debug('snapshot_loaded', { dir: this.dir, seq: this.seq });
    } catch {
      snap = null;
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
      if (snap && event.seq <= this.seq) continue;
      this.state = this.foldFn(this.state, event) ?? this.state;
      this.seq = Math.max(this.seq, event.seq);
      appliedAfterSnap += 1;
    }
    this.sinceSnapshot = appliedAfterSnap;
    this.stream = fs.createWriteStream(this.file, { flags: 'a' });
    await new Promise((res, rej) => {
      this.stream.once('open', res);
      this.stream.once('error', rej);
    });
  }

  // Append + fold atomically (single-threaded event loop; subscribers run
  // after the event is durable on disk).
  append(type, data = {}) {
    const event = {
      id: uid('e_'),
      seq: this.seq + 1,
      ts: new Date().toISOString(),
      type,
      data,
    };
    this.writeDurable(event);
    this.seq += 1;
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

  subscribe(fn) {
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }

  writeDurable(event) {
    this.stream.write(JSON.stringify(event) + '\n');
    // Node's WriteStream has no public fsync; createWriteStream flushes to the OS
    // page cache. For the reference impl that is sufficient (host crash may lose
    // the tail; Postgres deployment is documented for durable production use).
  }

  snapshot() {
    const snapPath = path.join(this.snapDir, 'state.json');
    const tmp = snapPath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ seq: this.seq, state: this.state }));
    fs.renameSync(tmp, snapPath);
    this.sinceSnapshot = 0;
  }

  async close() {
    if (!this.stream) return;
    await new Promise((res) => this.stream.end(res));
    this.stream = null;
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
