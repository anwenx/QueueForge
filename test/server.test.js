import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createApp } from '../src/server.js';
import { loadConfig } from '../src/config.js';
import { makeQueue, silentLogger } from './helpers.js';

const KEY = 'k'.repeat(32);
let app, base, queue;
const call = (path, { method = 'GET', body, headers = {}, raw, auth = true } = {}) => fetch(base + path, {
  method,
  headers: { ...(auth ? { Authorization: `Bearer ${KEY}` } : {}), ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}), ...headers },
  body: raw ?? (body !== undefined ? JSON.stringify(body) : undefined),
});

describe('HTTP API', () => {
  before(async () => {
    ({ queue } = makeQueue());
    const config = loadConfig({ QF_API_KEYS: `test:${KEY}`, QF_RATE_LIMIT_BURST: '1000', QF_MAX_BODY_BYTES: '2048' });
    app = createApp({ queue, config, logger: silentLogger });
    const addr = await app.listen(0, '127.0.0.1');
    base = `http://127.0.0.1:${addr.port}`;
  });
  after(() => app.close());

  test('health endpoints are public; security headers present', async () => {
    const r = await call('/healthz', { auth: false });
    assert.equal(r.status, 200); assert.equal(r.headers.get('x-content-type-options'), 'nosniff');
    assert.ok(r.headers.get('content-security-policy').includes("default-src 'none'"));
    assert.equal((await call('/readyz', { auth: false })).status, 200);
  });
  test('dashboard assets are served without auth', async () => {
    for (const p of ['/', '/app.js', '/style.css']) assert.equal((await call(p, { auth: false })).status, 200, p);
  });
  test('API requires a valid key', async () => {
    assert.equal((await call('/api/v1/stats', { auth: false })).status, 401);
    assert.equal((await call('/api/v1/stats', { auth: false, headers: { Authorization: 'Bearer wrong' } })).status, 401);
    assert.equal((await call('/api/v1/stats')).status, 200);
  });
  test('enqueue, fetch, list, events', async () => {
    const r = await call('/api/v1/jobs', { method: 'POST', body: { type: 'echo', payload: { a: 1 }, idempotencyKey: 'i1' } });
    assert.equal(r.status, 201);
    const { job } = await r.json();
    const dup = await call('/api/v1/jobs', { method: 'POST', body: { type: 'echo', idempotencyKey: 'i1' } });
    assert.equal(dup.status, 200); assert.equal((await dup.json()).job.id, job.id);
    assert.equal((await (await call(`/api/v1/jobs/${job.id}`)).json()).job.type, 'echo');
    assert.ok((await (await call('/api/v1/jobs?state=queued')).json()).total >= 1);
    assert.equal((await (await call(`/api/v1/jobs/${job.id}/events`)).json()).events[0].event, 'enqueued');
  });
  test('validation errors return 400 with details and correlation id', async () => {
    const r = await call('/api/v1/jobs', { method: 'POST', body: { type: 'bad type!' }, headers: { 'X-Correlation-Id': 'abc-123' } });
    assert.equal(r.status, 400); assert.equal(r.headers.get('x-correlation-id'), 'abc-123');
    const e = (await r.json()).error;
    assert.equal(e.code, 'validation_error'); assert.equal(e.correlationId, 'abc-123'); assert.equal(e.details[0].field, 'type');
  });
  test('malformed JSON, wrong content type, non-object bodies', async () => {
    assert.equal((await call('/api/v1/jobs', { method: 'POST', raw: '{nope', headers: { 'Content-Type': 'application/json' } })).status, 400);
    assert.equal((await call('/api/v1/jobs', { method: 'POST', raw: '{}', headers: { 'Content-Type': 'text/plain' } })).status, 400);
    assert.equal((await call('/api/v1/jobs', { method: 'POST', raw: 'null', headers: { 'Content-Type': 'application/json' } })).status, 400);
  });
  test('oversized body is rejected with 413', async () => {
    const r = await call('/api/v1/jobs', { method: 'POST', body: { type: 't', payload: { s: 'x'.repeat(5000) } } });
    assert.equal(r.status, 413);
  });
  test('invalid correlation header is replaced, not echoed', async () => {
    const r = await call('/healthz', { auth: false, headers: { 'X-Correlation-Id': 'bad id with spaces' } });
    assert.notEqual(r.headers.get('x-correlation-id'), 'bad id with spaces');
  });
  test('unknown routes and malformed ids return 404', async () => {
    assert.equal((await call('/api/v1/nope')).status, 404);
    assert.equal((await call('/api/v1/jobs/../../etc')).status, 404);
    assert.equal((await call('/api/v1/jobs/short')).status, 404);
    assert.equal((await call('/api/v1/jobs/00000000-0000-0000-0000-000000000000')).status, 404);
  });
  test('remote worker protocol: claim, heartbeat, nack, ack', async () => {
    await call('/api/v1/jobs', { method: 'POST', body: { type: 'remote', queue: 'rq', maxAttempts: 3 } });
    const claim = await (await call('/api/v1/queues/rq/claim', { method: 'POST', body: { workerId: 'remote-1' } })).json();
    const id = claim.job.id;
    assert.equal((await call(`/api/v1/jobs/${id}/heartbeat`, { method: 'POST', body: { workerId: 'remote-1' } })).status, 200);
    assert.equal((await call(`/api/v1/jobs/${id}/ack`, { method: 'POST', body: { workerId: 'intruder' } })).status, 409);
    assert.equal((await (await call(`/api/v1/jobs/${id}/nack`, { method: 'POST', body: { workerId: 'remote-1', error: 'oops' } })).json()).job.state, 'queued');
    assert.equal((await (await call('/api/v1/queues/rq/claim', { method: 'POST', body: { workerId: 'remote-1' } })).json()).job, null); // backoff pending
  });
  test('cancel and retry endpoints enforce state rules', async () => {
    const { job } = await (await call('/api/v1/jobs', { method: 'POST', body: { type: 'c', queue: 'cq' } })).json();
    assert.equal((await (await call(`/api/v1/jobs/${job.id}/cancel`, { method: 'POST' })).json()).job.state, 'cancelled');
    assert.equal((await call(`/api/v1/jobs/${job.id}/cancel`, { method: 'POST' })).status, 409);
    assert.equal((await (await call(`/api/v1/jobs/${job.id}/retry`, { method: 'POST' })).json()).job.state, 'queued');
  });
  test('metrics expose job gauges', async () => {
    const text = await (await call('/metrics')).text();
    assert.ok(text.includes('queueforge_jobs{queue="default",state="queued"}'));
    assert.equal((await call('/metrics', { auth: false })).status, 401);
  });
  test('rate limiting returns 429 with Retry-After', async () => {
    const { queue: q2 } = makeQueue();
    const config = loadConfig({ QF_API_KEYS: `rl:${KEY}`, QF_RATE_LIMIT_BURST: '2', QF_RATE_LIMIT_PER_SEC: '1' });
    const a2 = createApp({ queue: q2, config, logger: silentLogger });
    const addr = await a2.listen(0, '127.0.0.1');
    try {
      const get = () => fetch(`http://127.0.0.1:${addr.port}/api/v1/stats`, { headers: { Authorization: `Bearer ${KEY}` } });
      assert.equal((await get()).status, 200); assert.equal((await get()).status, 200);
      const r = await get();
      assert.equal(r.status, 429); assert.ok(Number(r.headers.get('retry-after')) >= 1);
    } finally { await a2.close(); }
  });
  test('never leaks internals on unexpected errors', async () => {
    const broken = { stats() { throw new Error('secret db path /var/x'); }, db: { prepare() { return { get() {} }; } } };
    const config = loadConfig({});
    const a3 = createApp({ queue: broken, config, logger: silentLogger });
    const addr = await a3.listen(0, '127.0.0.1');
    try {
      const r = await fetch(`http://127.0.0.1:${addr.port}/api/v1/stats`);
      const text = await r.text();
      assert.equal(r.status, 500); assert.ok(!text.includes('secret'));
    } finally { await a3.close(); }
  });
});
