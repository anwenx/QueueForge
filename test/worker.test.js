import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { Worker } from '../src/worker.js';
import { PermanentError } from '../src/errors.js';
import { makeQueue, silentLogger } from './helpers.js';

const waitFor = async (fn, ms = 3000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (fn()) return; await new Promise((r) => setTimeout(r, 10)); }
  throw new Error('timed out waiting for condition');
};
const real = (o = {}) => makeQueue({ clock: Date.now, backoffBaseMs: 20, backoffMaxMs: 50, leaseMs: 1000, ...o });

describe('worker runtime', () => {
  test('runs a job to success with its result', async () => {
    const { queue } = real();
    const { id } = queue.enqueue({ type: 'echo', payload: { n: 7 } }).job;
    const w = new Worker(queue, { handlers: { echo: async (j) => ({ got: j.payload.n }) }, pollMs: 10, logger: silentLogger });
    w.start();
    await waitFor(() => queue.get(id).state === 'succeeded');
    assert.deepEqual(queue.get(id).result, { got: 7 });
    await w.stop();
  });
  test('retries transient failures until success', async () => {
    const { queue } = real();
    const { id } = queue.enqueue({ type: 'flaky', maxAttempts: 5 }).job;
    const w = new Worker(queue, { handlers: { flaky: async (j) => { if (j.attempts < 3) throw new Error('nope'); return { attempts: j.attempts }; } }, pollMs: 10, logger: silentLogger });
    w.start();
    await waitFor(() => queue.get(id).state === 'succeeded');
    assert.equal(queue.get(id).attempts, 3);
    await w.stop();
  });
  test('PermanentError dead-letters immediately; exhausted retries dead-letter', async () => {
    const { queue } = real();
    const p = queue.enqueue({ type: 'perm', maxAttempts: 5 }).job;
    const e = queue.enqueue({ type: 'always', maxAttempts: 2 }).job;
    const w = new Worker(queue, { handlers: { perm: async () => { throw new PermanentError('bad input'); }, always: async () => { throw new Error('x'); } }, concurrency: 2, pollMs: 10, logger: silentLogger });
    w.start();
    await waitFor(() => queue.get(p.id).state === 'dead' && queue.get(e.id).state === 'dead');
    assert.equal(queue.get(p.id).attempts, 1); assert.equal(queue.get(e.id).attempts, 2);
    await w.stop();
  });
  test('handler timeout fails the job and aborts the signal', async () => {
    const { queue } = real();
    const { id } = queue.enqueue({ type: 'slow', maxAttempts: 1 }).job;
    let aborted = false;
    const w = new Worker(queue, { handlers: { slow: (_j, { signal }) => new Promise((res) => { signal.addEventListener('abort', () => { aborted = true; res(); }); }) }, handlerTimeoutMs: 50, pollMs: 10, logger: silentLogger });
    w.start();
    await waitFor(() => queue.get(id).state === 'dead');
    assert.match(queue.get(id).lastError, /timed out/); assert.ok(aborted);
    await w.stop();
  });
  test('jobs with no registered handler are never claimed by this worker', async () => {
    const { queue } = real();
    const { id } = queue.enqueue({ type: 'unknown' }).job;
    const w = new Worker(queue, { handlers: { other: async () => 1 }, pollMs: 10, logger: silentLogger });
    w.start(); await new Promise((r) => setTimeout(r, 80)); await w.stop();
    assert.equal(queue.get(id).state, 'queued');
  });
  test('respects concurrency', async () => {
    const { queue } = real();
    for (let i = 0; i < 6; i++) queue.enqueue({ type: 'c' });
    let active = 0; let peak = 0;
    const w = new Worker(queue, { handlers: { c: async () => { active++; peak = Math.max(peak, active); await new Promise((r) => setTimeout(r, 30)); active--; } }, concurrency: 3, pollMs: 5, logger: silentLogger });
    w.start();
    await waitFor(() => queue.stats().totals.succeeded === 6);
    assert.equal(peak, 3);
    await w.stop();
  });
  test('graceful stop waits for in-flight work', async () => {
    const { queue } = real();
    const { id } = queue.enqueue({ type: 'g' }).job;
    const w = new Worker(queue, { handlers: { g: async () => { await new Promise((r) => setTimeout(r, 100)); return 'done'; } }, pollMs: 10, logger: silentLogger });
    w.start();
    await waitFor(() => w.inFlight === 1);
    assert.equal(await w.stop(), 'drained');
    assert.equal(queue.get(id).state, 'succeeded');
  });
  test('heartbeats keep a long job alive past the lease; a crashed worker is recovered by the reaper', async () => {
    const { queue } = real({ leaseMs: 120 });
    const long = queue.enqueue({ type: 'long' }).job;
    const w = new Worker(queue, { handlers: { long: async () => { await new Promise((r) => setTimeout(r, 400)); return 'ok'; } }, leaseMs: 120, heartbeatMs: 30, pollMs: 10, logger: silentLogger });
    w.start();
    const reaper = setInterval(() => queue.reapExpired(), 20);
    await waitFor(() => queue.get(long.id).state === 'succeeded');
    clearInterval(reaper); await w.stop();
    assert.equal(queue.get(long.id).attempts, 1); // never lost its lease

    const crashed = queue.enqueue({ type: 'x' }).job;
    queue.claim('default', 'dead-worker', { leaseMs: 50 }); // claims and never reports back
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(queue.reapExpired(), 1);
    assert.equal(queue.get(crashed.id).state, 'queued');
  });
});
