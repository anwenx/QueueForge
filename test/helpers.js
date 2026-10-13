import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openDb } from '../src/db.js';
import { QueueService } from '../src/queue.js';

export function fakeClock(start = 1_700_000_000_000) {
  let t = start;
  const clock = () => t;
  clock.advance = (ms) => { t += ms; };
  return clock;
}

export function makeQueue(opts = {}) {
  const clock = opts.clock ?? fakeClock();
  const db = openDb(':memory:');
  const queue = new QueueService(db, { clock, rng: () => 0, backoffBaseMs: 1000, backoffMaxMs: 60000, leaseMs: 10000, ...opts });
  return { queue, db, clock };
}

export function tempDbPath() {
  const dir = mkdtempSync(join(tmpdir(), 'qf-'));
  return { path: join(dir, 'test.db'), cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

export const silentLogger = { debug() {}, info() {}, warn() {}, error() {}, child() { return silentLogger; } };
