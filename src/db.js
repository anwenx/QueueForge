import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export const MIGRATIONS = [
  {
    version: 1,
    name: 'initial schema',
    sql: `
      CREATE TABLE jobs (
        id               TEXT PRIMARY KEY,
        queue            TEXT NOT NULL,
        type             TEXT NOT NULL,
        payload          TEXT NOT NULL DEFAULT '{}',
        state            TEXT NOT NULL CHECK (state IN ('queued','running','succeeded','dead','cancelled')),
        priority         INTEGER NOT NULL DEFAULT 0,
        attempts         INTEGER NOT NULL DEFAULT 0,
        max_attempts     INTEGER NOT NULL DEFAULT 5,
        run_at           INTEGER NOT NULL,
        lease_owner      TEXT,
        lease_expires_at INTEGER,
        last_error       TEXT,
        result           TEXT,
        correlation_id   TEXT,
        idempotency_key  TEXT,
        created_at       INTEGER NOT NULL,
        updated_at       INTEGER NOT NULL,
        started_at       INTEGER,
        completed_at     INTEGER
      );
      CREATE INDEX idx_jobs_claim ON jobs (queue, state, priority DESC, run_at, created_at);
      CREATE INDEX idx_jobs_lease ON jobs (state, lease_expires_at);
      CREATE INDEX idx_jobs_corr  ON jobs (correlation_id);
      CREATE UNIQUE INDEX idx_jobs_idem ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;

      CREATE TABLE job_events (
        id     INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        event  TEXT NOT NULL,
        detail TEXT,
        at     INTEGER NOT NULL
      );
      CREATE INDEX idx_events_job ON job_events (job_id, id);
    `,
  },
];

export function migrate(db) {
  db.exec('CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at INTEGER NOT NULL)');
  const applied = new Set(db.prepare('SELECT version FROM schema_migrations').all().map((r) => r.version));
  for (const m of MIGRATIONS) {
    if (applied.has(m.version)) continue;
    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec(m.sql);
      db.prepare('INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)').run(m.version, m.name, Date.now());
      db.exec('COMMIT');
    } catch (err) {
      try { db.exec('ROLLBACK'); } catch { /* ignore */ }
      throw err;
    }
  }
}

export function openDb(path = ':memory:') {
  if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON; PRAGMA synchronous = NORMAL;');
  migrate(db);
  return db;
}
