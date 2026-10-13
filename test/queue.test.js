import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { openDb, MIGRATIONS } from '../src/db.js';
import { QueueService } from '../src/queue.js';
import { LeaseError, NotFoundError, StateError, ValidationError } from '../src/errors.js';
import { makeQueue, tempDbPath, fakeClock } from './helpers.js';

describe('enqueue', () => {
  test('creates a queued job with an event', () => {
    const { queue } = makeQueue();
    const { job, duplicate } = queue.enqueue({ type: 'echo', payload: { a: 1 }, priority: 5 });
    assert.equal(duplicate, false); assert.equal(job.state, 'queued'); assert.deepEqual(job.payload, { a: 1 });
    assert.equal(queue.events(job.id)[0].event, 'enqueued');
  });
  test('idempotency key dedupes per queue', () => {
    const { queue } = makeQueue();
    const a = queue.enqueue({ type: 't', idempotencyKey: 'k1' });
    const b = queue.enqueue({ type: 't', idempotencyKey: 'k1' });
    const c = queue.enqueue({ type: 't', idempotencyKey: 'k1', queue: 'other' });
    assert.equal(b.duplicate, true); assert.equal(b.job.id, a.job.id); assert.notEqual(c.job.id, a.job.id);
    assert.equal(queue.stats().totals.queued, 2);
  });
  test('rejects invalid input', () => {
    const { queue } = makeQueue();
    assert.throws(() => queue.enqueue({}), ValidationError);
  });
  test('stored SQL injection strings stay inert', () => {
    const { queue } = makeQueue();
    queue.enqueue({ type: 't', payload: { s: "'); DROP TABLE jobs;--" } });
    assert.equal(queue.list().total, 1);
  });
});

describe('claim ordering', () => {
  test('priority, then run_at, then FIFO', () => {
    const { queue, clock } = makeQueue();
    const low = queue.enqueue({ type: 't', priority: 0 }).job; clock.advance(1);
    const high = queue.enqueue({ type: 't', priority: 10 }).job; clock.advance(1);
    const low2 = queue.enqueue({ type: 't', priority: 0 }).job;
    assert.equal(queue.claim('default', 'w').id, high.id);
    assert.equal(queue.claim('default', 'w').id, low.id);
    assert.equal(queue.claim('default', 'w').id, low2.id);
    assert.equal(queue.claim('default', 'w'), null);
  });
  test('delayed jobs are not claimable until due', () => {
    const { queue, clock } = makeQueue();
    queue.enqueue({ type: 't', delayMs: 5000 });
    assert.equal(queue.claim('default', 'w'), null);
    clock.advance(4999); assert.equal(queue.claim('default', 'w'), null);
    clock.advance(1); assert.ok(queue.claim('default', 'w'));
  });
  test('claims are scoped to queue and requested types', () => {
    const { queue } = makeQueue();
    queue.enqueue({ type: 'a', queue: 'q1' }); queue.enqueue({ type: 'b', queue: 'q1' });
    assert.equal(queue.claim('q2', 'w'), null);
    assert.equal(queue.claim('q1', 'w', { types: ['b'] }).type, 'b');
  });
  test('claim increments attempts and sets lease', () => {
    const { queue, clock } = makeQueue();
    queue.enqueue({ type: 't' });
    const j = queue.claim('default', 'w1');
    assert.equal(j.attempts, 1); assert.equal(j.leaseOwner, 'w1'); assert.equal(j.leaseExpiresAt, clock() + 10000);
  });
});

describe('completion and failure', () => {
  test('complete stores result', () => {
    const { queue } = makeQueue();
    const { id } = queue.enqueue({ type: 't' }).job; queue.claim('default', 'w');
    const done = queue.complete(id, 'w', { ok: true });
    assert.equal(done.state, 'succeeded'); assert.deepEqual(done.result, { ok: true }); assert.equal(done.leaseOwner, null);
  });
  test('only the lease owner can ack/nack/heartbeat', () => {
    const { queue } = makeQueue();
    const { id } = queue.enqueue({ type: 't' }).job; queue.claim('default', 'w1');
    assert.throws(() => queue.complete(id, 'w2'), LeaseError);
    assert.throws(() => queue.fail(id, 'w2', 'x'), LeaseError);
    assert.throws(() => queue.heartbeat(id, 'w2'), LeaseError);
    assert.throws(() => queue.complete('00000000-0000-0000-0000-000000000000', 'w1'), NotFoundError);
  });
  test('cannot complete twice', () => {
    const { queue } = makeQueue();
    const { id } = queue.enqueue({ type: 't' }).job; queue.claim('default', 'w'); queue.complete(id, 'w');
    assert.throws(() => queue.complete(id, 'w'), LeaseError);
  });
  test('failure schedules exponential retries then dead-letters', () => {
    const { queue, clock } = makeQueue();
    const { id } = queue.enqueue({ type: 't', maxAttempts: 3 }).job;
    queue.claim('default', 'w');
    let j = queue.fail(id, 'w', 'boom');
    assert.equal(j.state, 'queued'); assert.equal(j.runAt, clock() + 1000); assert.equal(j.lastError, 'boom');
    assert.equal(queue.claim('default', 'w'), null); // backoff not elapsed
    clock.advance(1000); queue.claim('default', 'w');
    j = queue.fail(id, 'w', 'boom2'); assert.equal(j.runAt, clock() + 2000);
    clock.advance(2000); queue.claim('default', 'w');
    j = queue.fail(id, 'w', 'boom3');
    assert.equal(j.state, 'dead'); assert.equal(j.attempts, 3);
    assert.deepEqual(queue.events(id).map((e) => e.event), ['enqueued', 'claimed', 'retry_scheduled', 'claimed', 'retry_scheduled', 'claimed', 'dead']);
  });
  test('non-retryable failure goes straight to dead', () => {
    const { queue } = makeQueue();
    const { id } = queue.enqueue({ type: 't', maxAttempts: 5 }).job; queue.claim('default', 'w');
    assert.equal(queue.fail(id, 'w', 'fatal', { retryable: false }).state, 'dead');
  });
  test('long errors are truncated', () => {
    const { queue } = makeQueue();
    const { id } = queue.enqueue({ type: 't' }).job; queue.claim('default', 'w');
    assert.equal(queue.fail(id, 'w', 'x'.repeat(10000)).lastError.length, 2000);
  });
});

describe('leases', () => {
  test('heartbeat extends the lease and prevents reaping', () => {
    const { queue, clock } = makeQueue();
    const { id } = queue.enqueue({ type: 't' }).job; queue.claim('default', 'w');
    clock.advance(9000); queue.heartbeat(id, 'w');
    clock.advance(9000);
    assert.equal(queue.reapExpired(), 0);
  });
  test('expired lease is requeued, counted as an attempt, and the old worker loses ownership', () => {
    const { queue, clock } = makeQueue();
    const { id } = queue.enqueue({ type: 't', maxAttempts: 2 }).job; queue.claim('default', 'w1');
    clock.advance(10001);
    assert.equal(queue.reapExpired(), 1);
    assert.equal(queue.get(id).state, 'queued');
    assert.throws(() => queue.complete(id, 'w1'), LeaseError);
    clock.advance(1000); queue.claim('default', 'w2');
    clock.advance(10001); queue.reapExpired();
    assert.equal(queue.get(id).state, 'dead');
    assert.ok(queue.events(id).some((e) => e.event === 'lease_expired'));
  });
});

describe('cancel / retry / list / stats / purge', () => {
  test('cancel only works on queued jobs', () => {
    const { queue } = makeQueue();
    const a = queue.enqueue({ type: 't' }).job; const b = queue.enqueue({ type: 't' }).job;
    assert.equal(queue.cancel(a.id).state, 'cancelled');
    queue.claim('default', 'w');
    assert.throws(() => queue.cancel(b.id), StateError);
    assert.throws(() => queue.cancel('00000000-0000-0000-0000-000000000000'), NotFoundError);
  });
  test('retry resets attempts for dead and cancelled jobs only', () => {
    const { queue } = makeQueue();
    const { id } = queue.enqueue({ type: 't', maxAttempts: 1 }).job; queue.claim('default', 'w'); queue.fail(id, 'w', 'x');
    assert.equal(queue.get(id).state, 'dead');
    const r = queue.retry(id); assert.equal(r.state, 'queued'); assert.equal(r.attempts, 0);
    assert.throws(() => queue.retry(id), StateError);
  });
  test('list filters, paginates and matches LIKE wildcards literally', () => {
    const { queue } = makeQueue();
    queue.enqueue({ type: 'a', correlationId: 'req-100' });
    queue.enqueue({ type: 'b', correlationId: 'req_200' });
    queue.enqueue({ type: 'a', queue: 'q2' });
    assert.equal(queue.list({ type: 'a' }).total, 2);
    assert.equal(queue.list({ queue: 'q2' }).total, 1);
    assert.equal(queue.list({ limit: 1, offset: 1 }).jobs.length, 1);
    assert.equal(queue.list({ correlationId: 'req-' }).total, 1);
    assert.equal(queue.list({ correlationId: 'req_' }).total, 1); // "_" must not act as a wildcard
    assert.equal(queue.list({ correlationId: '%' }).total, 0);    // "%" must not match everything
    assert.equal(queue.list({ correlationId: 'req_2' }).total, 1);
  });
  test('stats report totals, per-queue counts and age of oldest runnable job', () => {
    const { queue, clock } = makeQueue();
    queue.enqueue({ type: 't' }); queue.enqueue({ type: 't', queue: 'q2' });
    clock.advance(500);
    queue.claim('default', 'w');
    const s = queue.stats();
    assert.equal(s.totals.running, 1); assert.equal(s.totals.queued, 1); assert.equal(s.queues.q2.queued, 1); assert.equal(s.oldestRunnableAgeMs, 500);
  });
  test('purge removes only old finished jobs', () => {
    const { queue, clock } = makeQueue();
    const a = queue.enqueue({ type: 't' }).job; queue.claim('default', 'w'); queue.complete(a.id, 'w');
    const keep = queue.enqueue({ type: 't' }).job;
    clock.advance(3600_000);
    assert.equal(queue.purge({ olderThanMs: 1000 }), 1);
    assert.equal(queue.get(keep.id).state, 'queued');
    assert.throws(() => queue.get(a.id), NotFoundError);
    assert.throws(() => queue.purge({ olderThanMs: -1 }), RangeError);
    assert.throws(() => queue.purge({ olderThanMs: 1, states: ['running'] }), StateError);
  });
});

describe('concurrency and durability (file-backed SQLite)', () => {
  test('two connections never claim the same job', () => {
    const { path, cleanup } = tempDbPath();
    try {
      const a = new QueueService(openDb(path)); const b = new QueueService(openDb(path));
      for (let i = 0; i < 50; i++) a.enqueue({ type: 't' });
      const seen = new Set(); let claimed = 0;
      for (let i = 0; i < 60; i++) {
        const j = (i % 2 ? a : b).claim('default', i % 2 ? 'wa' : 'wb');
        if (j) { claimed++; assert.ok(!seen.has(j.id), 'duplicate claim'); seen.add(j.id); }
      }
      assert.equal(claimed, 50);
    } finally { cleanup(); }
  });
  test('jobs survive reopening the database', () => {
    const { path, cleanup } = tempDbPath();
    try {
      const id = new QueueService(openDb(path)).enqueue({ type: 'persist', payload: { n: 1 } }).job.id;
      assert.deepEqual(new QueueService(openDb(path)).get(id).payload, { n: 1 });
    } finally { cleanup(); }
  });
  test('migrations are idempotent and recorded', () => {
    const { path, cleanup } = tempDbPath();
    try {
      openDb(path).close(); const db = openDb(path);
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM schema_migrations').get().n, MIGRATIONS.length);
    } finally { cleanup(); }
  });
  test('schema CHECK constraint rejects unknown states', () => {
    const db = openDb(':memory:');
    assert.throws(() => db.prepare(`INSERT INTO jobs (id,queue,type,state,run_at,created_at,updated_at) VALUES ('x','q','t','weird',0,0,0)`).run());
  });
});
