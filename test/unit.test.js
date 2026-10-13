import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { Writable } from 'node:stream';
import { assertTransition, backoffMs, canTransition } from '../src/state.js';
import { validateEnqueue, validateListQuery, MAX_PAYLOAD_BYTES } from '../src/validate.js';
import { loadConfig, parseApiKeys } from '../src/config.js';
import { createAuthenticator } from '../src/auth.js';
import { RateLimiter } from '../src/ratelimit.js';
import { createLogger, redact } from '../src/logger.js';
import { AuthError, ConfigError, StateError, ValidationError } from '../src/errors.js';
import { escapeLike } from '../src/queue.js';

describe('state machine', () => {
  test('allows documented transitions only', () => {
    assert.ok(canTransition('queued', 'running'));
    assert.ok(canTransition('running', 'succeeded'));
    assert.ok(canTransition('running', 'queued'));
    assert.ok(canTransition('dead', 'queued'));
    assert.ok(!canTransition('succeeded', 'queued'));
    assert.ok(!canTransition('queued', 'succeeded'));
    assert.ok(!canTransition('bogus', 'queued'));
    assert.throws(() => assertTransition('succeeded', 'running'), StateError);
  });
});

describe('backoff', () => {
  test('grows exponentially and is capped', () => {
    const o = { baseMs: 1000, maxMs: 10000, jitter: 0, rng: () => 0 };
    assert.deepEqual([1, 2, 3, 4, 5, 6].map((a) => backoffMs(a, o)), [1000, 2000, 4000, 8000, 10000, 10000]);
  });
  test('jitter stays within bounds', () => {
    assert.equal(backoffMs(1, { baseMs: 1000, jitter: 0.2, rng: () => 1 }), 800);
    assert.equal(backoffMs(1, { baseMs: 1000, jitter: 0.2, rng: () => 0 }), 1000);
  });
  test('huge attempt numbers do not overflow', () => {
    assert.equal(backoffMs(10_000, { baseMs: 1000, maxMs: 5000, jitter: 0 }), 5000);
  });
  test('rejects invalid attempt', () => {
    assert.throws(() => backoffMs(0), RangeError);
    assert.throws(() => backoffMs(1.5), RangeError);
  });
});

describe('validation', () => {
  test('applies defaults', () => {
    const v = validateEnqueue({ type: 'echo' });
    assert.equal(v.queue, 'default'); assert.equal(v.priority, 0); assert.equal(v.maxAttempts, 5); assert.equal(v.payloadJson, '{}');
  });
  test('collects every error', () => {
    try { validateEnqueue({ type: '', priority: 1000, delayMs: -1, extra: 1, payload: [] }); assert.fail('should throw'); }
    catch (e) { assert.ok(e instanceof ValidationError); assert.deepEqual(e.details.map((d) => d.field).sort(), ['delayMs', 'extra', 'payload', 'priority', 'type']); }
  });
  test('rejects non-object bodies', () => {
    for (const bad of [null, 'x', 5, [], undefined]) assert.throws(() => validateEnqueue(bad), ValidationError);
  });
  test('rejects oversized payloads', () => {
    assert.throws(() => validateEnqueue({ type: 't', payload: { s: 'x'.repeat(MAX_PAYLOAD_BYTES) } }), ValidationError);
  });
  test('correlation id and idempotency key formats', () => {
    assert.throws(() => validateEnqueue({ type: 't', correlationId: 'bad%id' }), ValidationError);
    assert.throws(() => validateEnqueue({ type: 't', correlationId: 'a'.repeat(65) }), ValidationError);
    assert.throws(() => validateEnqueue({ type: 't', idempotencyKey: 'has space' }), ValidationError);
    assert.equal(validateEnqueue({ type: 't', correlationId: 'req-1.a_b' }).correlationId, 'req-1.a_b');
  });
  test('queue names reject path-like and injection-like input', () => {
    for (const q of ['../etc', 'a b', "x'; DROP TABLE jobs;--", '-lead', '']) assert.throws(() => validateEnqueue({ type: 't', queue: q }), ValidationError);
  });
  test('list query validation and bounds', () => {
    assert.deepEqual(validateListQuery({}), { limit: 50, offset: 0 });
    assert.throws(() => validateListQuery({ state: 'nope' }), ValidationError);
    assert.throws(() => validateListQuery({ limit: '0' }), ValidationError);
    assert.throws(() => validateListQuery({ limit: '201' }), ValidationError);
    assert.throws(() => validateListQuery({ offset: 'abc' }), ValidationError);
  });
  test('escapeLike escapes wildcards', () => { assert.equal(escapeLike('a%b_c\\d'), 'a\\%b\\_c\\\\d'); });
});

describe('config', () => {
  test('defaults', () => {
    const c = loadConfig({});
    assert.equal(c.port, 8080); assert.equal(c.host, '127.0.0.1'); assert.equal(c.apiKeys.length, 0);
  });
  test('production requires API keys', () => { assert.throws(() => loadConfig({ NODE_ENV: 'production' }), ConfigError); });
  test('rejects invalid numbers and log levels', () => {
    assert.throws(() => loadConfig({ QF_PORT: 'abc' }), ConfigError);
    assert.throws(() => loadConfig({ QF_PORT: '70000' }), ConfigError);
    assert.throws(() => loadConfig({ QF_LOG_LEVEL: 'loud' }), ConfigError);
  });
  test('parses named and bare keys; enforces min length', () => {
    assert.deepEqual(parseApiKeys('svc:abcdefghijklmnopqrst,zzzzzzzzzzzzzzzzzzzz').map((k) => k.name), ['svc', 'key2']);
    assert.throws(() => parseApiKeys('short'), ConfigError);
  });
  test('config is frozen', () => { assert.throws(() => { 'use strict'; loadConfig({}).port = 1; }, TypeError); });
});

describe('auth', () => {
  const keys = [{ name: 'svc', key: 'a'.repeat(32) }];
  test('accepts bearer and x-api-key', () => {
    const a = createAuthenticator(keys);
    assert.equal(a.authenticate({ authorization: `Bearer ${'a'.repeat(32)}` }).name, 'svc');
    assert.equal(a.authenticate({ 'x-api-key': 'a'.repeat(32) }).name, 'svc');
  });
  test('rejects missing, wrong and malformed credentials', () => {
    const a = createAuthenticator(keys);
    assert.throws(() => a.authenticate({}), AuthError);
    assert.throws(() => a.authenticate({ authorization: 'Bearer nope' }), AuthError);
    assert.throws(() => a.authenticate({ authorization: 'Basic abc' }), AuthError);
    assert.throws(() => a.authenticate({ authorization: `Bearer ${'a'.repeat(31)}` }), AuthError);
  });
  test('disabled when no keys configured', () => {
    const a = createAuthenticator([]);
    assert.equal(a.enabled, false); assert.equal(a.authenticate({}).name, 'anonymous');
  });
});

describe('rate limiter', () => {
  test('bursts then refills', () => {
    let now = 0;
    const rl = new RateLimiter({ capacity: 3, refillPerSec: 1, clock: () => now });
    assert.ok(rl.take('k').allowed && rl.take('k').allowed && rl.take('k').allowed);
    const denied = rl.take('k');
    assert.equal(denied.allowed, false); assert.equal(denied.retryAfterSec, 1);
    now += 1000;
    assert.ok(rl.take('k').allowed);
  });
  test('keys are isolated and map is bounded', () => {
    const rl = new RateLimiter({ capacity: 1, refillPerSec: 1, clock: () => 0, maxKeys: 2 });
    assert.ok(rl.take('a').allowed); assert.ok(rl.take('b').allowed); assert.ok(!rl.take('a').allowed);
    rl.take('c');
    assert.ok(rl.buckets.size <= 2);
  });
});

describe('logger', () => {
  const capture = () => { const lines = []; const stream = new Writable({ write(c, _e, cb) { lines.push(c.toString()); cb(); } }); return { lines, stream }; };
  test('emits JSON lines and honours level', () => {
    const { lines, stream } = capture();
    const log = createLogger({ level: 'warn', stream, clock: () => new Date(0) });
    log.info('hidden'); log.warn('shown', { a: 1 });
    assert.equal(lines.length, 1);
    assert.deepEqual(JSON.parse(lines[0]), { time: '1970-01-01T00:00:00.000Z', level: 'warn', msg: 'shown', a: 1 });
  });
  test('redacts secrets, including nested and circular', () => {
    const o = { authorization: 'Bearer x', nested: { apiKey: 'k', fine: 1 }, list: [{ password: 'p' }] };
    o.self = o;
    const r = redact(o);
    assert.equal(r.authorization, '[REDACTED]'); assert.equal(r.nested.apiKey, '[REDACTED]'); assert.equal(r.nested.fine, 1);
    assert.equal(r.list[0].password, '[REDACTED]'); assert.equal(r.self, '[Circular]');
  });
  test('child loggers merge context', () => {
    const { lines, stream } = capture();
    createLogger({ stream }).child({ jobId: 'j1' }).info('x');
    assert.equal(JSON.parse(lines[0]).jobId, 'j1');
  });
});
