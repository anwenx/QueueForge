import { randomUUID } from 'node:crypto';
import { LeaseError, NotFoundError, StateError } from './errors.js';
import { assertTransition, backoffMs } from './state.js';
import { validateEnqueue } from './validate.js';

const MAX_ERROR_CHARS = 2000;
const MAX_RESULT_BYTES = 64 * 1024;

function inTx(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const out = fn();
    db.exec('COMMIT');
    return out;
  } catch (err) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    throw err;
  }
}

const parseJson = (s) => { if (s == null) return null; try { return JSON.parse(s); } catch { return null; } };

export function toJob(row) {
  if (!row) return null;
  return {
    id: row.id, queue: row.queue, type: row.type, payload: parseJson(row.payload) ?? {},
    state: row.state, priority: row.priority, attempts: row.attempts, maxAttempts: row.max_attempts,
    runAt: row.run_at, leaseOwner: row.lease_owner, leaseExpiresAt: row.lease_expires_at,
    lastError: row.last_error, result: parseJson(row.result), correlationId: row.correlation_id,
    idempotencyKey: row.idempotency_key, createdAt: row.created_at, updatedAt: row.updated_at,
    startedAt: row.started_at, completedAt: row.completed_at,
  };
}

/** Escape LIKE wildcards so user input is matched literally (used with ESCAPE '\'). */
export function escapeLike(s) { return s.replace(/[\\%_]/g, (c) => `\\${c}`); }

export class QueueService {
  constructor(db, { clock = Date.now, rng = Math.random, leaseMs = 30000, backoffBaseMs = 1000, backoffMaxMs = 300000, defaultMaxAttempts = 5, logger } = {}) {
    this.db = db;
    this.clock = clock;
    this.rng = rng;
    this.leaseMs = leaseMs;
    this.backoffBaseMs = backoffBaseMs;
    this.backoffMaxMs = backoffMaxMs;
    this.defaultMaxAttempts = defaultMaxAttempts;
    this.log = logger;
  }

  #event(jobId, event, detail = null) {
    this.db.prepare('INSERT INTO job_events (job_id, event, detail, at) VALUES (?, ?, ?, ?)').run(jobId, event, detail, this.clock());
  }
  #row(id) { return this.db.prepare('SELECT * FROM jobs WHERE id = ?').get(id); }

  /** @returns {{job:object, duplicate:boolean}} */
  enqueue(input) {
    const v = validateEnqueue(input, { defaultMaxAttempts: this.defaultMaxAttempts });
    const now = this.clock();
    return inTx(this.db, () => {
      if (v.idempotencyKey) {
        const existing = this.db.prepare('SELECT * FROM jobs WHERE queue = ? AND idempotency_key = ?').get(v.queue, v.idempotencyKey);
        if (existing) return { job: toJob(existing), duplicate: true };
      }
      const id = randomUUID();
      this.db.prepare(`INSERT INTO jobs (id, queue, type, payload, state, priority, attempts, max_attempts, run_at, correlation_id, idempotency_key, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?, ?, ?, ?)`)
        .run(id, v.queue, v.type, v.payloadJson, v.priority, v.maxAttempts, now + v.delayMs, v.correlationId, v.idempotencyKey, now, now);
      this.#event(id, 'enqueued', v.delayMs ? `delayed ${v.delayMs}ms` : null);
      return { job: toJob(this.#row(id)), duplicate: false };
    });
  }

  get(id) {
    const row = this.#row(id);
    if (!row) throw new NotFoundError(`Job ${id} not found`);
    return toJob(row);
  }

  events(id) {
    this.get(id);
    return this.db.prepare('SELECT id, event, detail, at FROM job_events WHERE job_id = ? ORDER BY id').all(id).map((r) => ({ ...r }));
  }

  /**
   * Atomically lease the next runnable job: highest priority first, then earliest run_at, then FIFO.
   * Increments attempts. Returns null when nothing is runnable.
   */
  claim(queue, workerId, { types, leaseMs } = {}) {
    const now = this.clock();
    return inTx(this.db, () => {
      const params = [queue, now];
      let typeClause = '';
      if (Array.isArray(types) && types.length) {
        typeClause = ` AND type IN (${types.map(() => '?').join(',')})`;
        params.push(...types);
      }
      const row = this.db.prepare(`SELECT id, state FROM jobs WHERE queue = ? AND state = 'queued' AND run_at <= ?${typeClause}
        ORDER BY priority DESC, run_at ASC, created_at ASC, rowid ASC LIMIT 1`).get(...params);
      if (!row) return null;
      assertTransition(row.state, 'running');
      this.db.prepare(`UPDATE jobs SET state = 'running', attempts = attempts + 1, lease_owner = ?, lease_expires_at = ?, started_at = ?, updated_at = ? WHERE id = ?`)
        .run(workerId, now + (leaseMs ?? this.leaseMs), now, now, row.id);
      this.#event(row.id, 'claimed', workerId);
      return toJob(this.#row(row.id));
    });
  }

  #assertLease(row, id, workerId) {
    if (!row) throw new NotFoundError(`Job ${id} not found`);
    if (row.state !== 'running' || row.lease_owner !== workerId) throw new LeaseError();
  }

  heartbeat(id, workerId, leaseMs) {
    const now = this.clock();
    return inTx(this.db, () => {
      const row = this.#row(id);
      this.#assertLease(row, id, workerId);
      this.db.prepare('UPDATE jobs SET lease_expires_at = ?, updated_at = ? WHERE id = ?').run(now + (leaseMs ?? this.leaseMs), now, id);
      return toJob(this.#row(id));
    });
  }

  complete(id, workerId, result = null) {
    const now = this.clock();
    let resultJson = null;
    if (result !== null && result !== undefined) {
      resultJson = JSON.stringify(result);
      if (Buffer.byteLength(resultJson) > MAX_RESULT_BYTES) resultJson = JSON.stringify({ truncated: true });
    }
    return inTx(this.db, () => {
      const row = this.#row(id);
      this.#assertLease(row, id, workerId);
      assertTransition('running', 'succeeded');
      this.db.prepare(`UPDATE jobs SET state = 'succeeded', result = ?, lease_owner = NULL, lease_expires_at = NULL, completed_at = ?, updated_at = ?, last_error = NULL WHERE id = ?`)
        .run(resultJson, now, now, id);
      this.#event(id, 'succeeded');
      return toJob(this.#row(id));
    });
  }

  /** Record a failure: schedule a retry with backoff, or dead-letter when attempts are exhausted / error is permanent. */
  fail(id, workerId, error, { retryable = true } = {}) {
    return inTx(this.db, () => {
      const row = this.#row(id);
      this.#assertLease(row, id, workerId);
      return toJob(this.#applyFailure(row, error, retryable));
    });
  }

  #applyFailure(row, error, retryable) {
    const now = this.clock();
    const msg = String(error instanceof Error ? error.message : error).slice(0, MAX_ERROR_CHARS);
    if (!retryable || row.attempts >= row.max_attempts) {
      assertTransition('running', 'dead');
      this.db.prepare(`UPDATE jobs SET state = 'dead', last_error = ?, lease_owner = NULL, lease_expires_at = NULL, completed_at = ?, updated_at = ? WHERE id = ?`).run(msg, now, now, row.id);
      this.#event(row.id, 'dead', msg);
    } else {
      assertTransition('running', 'queued');
      const delay = backoffMs(row.attempts, { baseMs: this.backoffBaseMs, maxMs: this.backoffMaxMs, rng: this.rng });
      this.db.prepare(`UPDATE jobs SET state = 'queued', last_error = ?, run_at = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ? WHERE id = ?`).run(msg, now + delay, now, row.id);
      this.#event(row.id, 'retry_scheduled', `${msg} (retry in ${delay}ms)`);
    }
    return this.#row(row.id);
  }

  /** Requeue (or dead-letter) jobs whose worker stopped heartbeating. Returns the number of jobs reaped. */
  reapExpired() {
    const now = this.clock();
    return inTx(this.db, () => {
      const rows = this.db.prepare(`SELECT * FROM jobs WHERE state = 'running' AND lease_expires_at < ?`).all(now);
      for (const row of rows) {
        this.#event(row.id, 'lease_expired', row.lease_owner);
        this.#applyFailure(row, 'lease expired (worker stopped heartbeating)', true);
      }
      if (rows.length) this.log?.warn('reaped expired leases', { count: rows.length });
      return rows.length;
    });
  }

  cancel(id) {
    return inTx(this.db, () => {
      const row = this.#row(id);
      if (!row) throw new NotFoundError(`Job ${id} not found`);
      assertTransition(row.state, 'cancelled');
      const now = this.clock();
      this.db.prepare(`UPDATE jobs SET state = 'cancelled', completed_at = ?, updated_at = ? WHERE id = ?`).run(now, now, id);
      this.#event(id, 'cancelled');
      return toJob(this.#row(id));
    });
  }

  /** Manually requeue a dead or cancelled job with a fresh attempt budget. */
  retry(id) {
    return inTx(this.db, () => {
      const row = this.#row(id);
      if (!row) throw new NotFoundError(`Job ${id} not found`);
      assertTransition(row.state, 'queued');
      const now = this.clock();
      this.db.prepare(`UPDATE jobs SET state = 'queued', attempts = 0, run_at = ?, completed_at = NULL, updated_at = ? WHERE id = ?`).run(now, now, id);
      this.#event(id, 'manual_retry', `from ${row.state}`);
      return toJob(this.#row(id));
    });
  }

  list({ state, queue, type, correlationId, limit = 50, offset = 0 } = {}) {
    const where = [];
    const params = [];
    if (state) { where.push('state = ?'); params.push(state); }
    if (queue) { where.push('queue = ?'); params.push(queue); }
    if (type) { where.push('type = ?'); params.push(type); }
    if (correlationId) { where.push(`correlation_id LIKE ? ESCAPE '\\'`); params.push(`${escapeLike(correlationId)}%`); }
    const w = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const total = this.db.prepare(`SELECT COUNT(*) AS n FROM jobs ${w}`).get(...params).n;
    const rows = this.db.prepare(`SELECT * FROM jobs ${w} ORDER BY created_at DESC, rowid DESC LIMIT ? OFFSET ?`).all(...params, limit, offset);
    return { total, limit, offset, jobs: rows.map(toJob) };
  }

  stats() {
    const now = this.clock();
    const rows = this.db.prepare('SELECT queue, state, COUNT(*) AS n FROM jobs GROUP BY queue, state').all();
    const queues = {};
    const totals = { queued: 0, running: 0, succeeded: 0, dead: 0, cancelled: 0 };
    for (const r of rows) {
      queues[r.queue] ??= { queued: 0, running: 0, succeeded: 0, dead: 0, cancelled: 0 };
      queues[r.queue][r.state] = r.n;
      totals[r.state] += r.n;
    }
    const oldest = this.db.prepare(`SELECT MIN(run_at) AS t FROM jobs WHERE state = 'queued' AND run_at <= ?`).get(now).t;
    return { totals, queues, oldestRunnableAgeMs: oldest == null ? 0 : now - oldest };
  }

  /** Delete finished jobs older than the cutoff. Returns the number removed. */
  purge({ olderThanMs, states = ['succeeded', 'cancelled'] }) {
    if (!Number.isInteger(olderThanMs) || olderThanMs < 0) throw new RangeError('olderThanMs must be a non-negative integer');
    const allowed = states.filter((s) => ['succeeded', 'cancelled', 'dead'].includes(s));
    if (!allowed.length) throw new StateError('purge only supports succeeded, cancelled and dead jobs');
    const cutoff = this.clock() - olderThanMs;
    const res = this.db.prepare(`DELETE FROM jobs WHERE state IN (${allowed.map(() => '?').join(',')}) AND completed_at IS NOT NULL AND completed_at < ?`).run(...allowed, cutoff);
    return Number(res.changes);
  }
}
