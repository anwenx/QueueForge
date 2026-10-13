import { randomUUID } from 'node:crypto';
import { LeaseError } from './errors.js';

export class HandlerTimeoutError extends Error {
  constructor(ms) { super(`handler timed out after ${ms}ms`); this.name = 'HandlerTimeoutError'; }
}

/** Periodically requeue jobs whose lease expired. Returns a stop function. */
export function startReaper(queue, intervalMs, logger) {
  if (!intervalMs) return () => {};
  const t = setInterval(() => {
    try { queue.reapExpired(); } catch (err) { logger?.error('reaper failed', err); }
  }, intervalMs);
  t.unref();
  return () => clearInterval(t);
}

/**
 * Polls a queue and runs handlers. A handler is `async (job, { signal, log }) => result`.
 * Throw PermanentError (or any error with `retryable === false`) to dead-letter immediately.
 */
export class Worker {
  constructor(queue, { queueName = 'default', handlers = {}, workerId = `worker-${randomUUID().slice(0, 8)}`, concurrency = 1,
    pollMs = 500, leaseMs = 30000, heartbeatMs, handlerTimeoutMs = 0, logger } = {}) {
    this.queue = queue;
    this.queueName = queueName;
    this.handlers = handlers;
    this.workerId = workerId;
    this.concurrency = concurrency;
    this.pollMs = pollMs;
    this.leaseMs = leaseMs;
    this.heartbeatMs = heartbeatMs ?? Math.max(250, Math.floor(leaseMs / 3));
    this.handlerTimeoutMs = handlerTimeoutMs;
    this.log = logger;
    this.running = false;
    this.loops = [];
    this.wakers = new Set();
    this.inFlight = 0;
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.loops = Array.from({ length: this.concurrency }, (_, i) => this.#loop(i));
    this.log?.info('worker started', { workerId: this.workerId, queue: this.queueName, concurrency: this.concurrency });
  }

  /** Stop polling and wait (up to timeoutMs) for in-flight jobs to finish. Unfinished jobs are recovered by lease expiry. */
  async stop({ timeoutMs = 10000 } = {}) {
    this.running = false;
    for (const w of this.wakers) w();
    let timer;
    const timeout = new Promise((res) => { timer = setTimeout(() => res('timeout'), timeoutMs); timer.unref?.(); });
    const outcome = await Promise.race([Promise.all(this.loops).then(() => 'drained'), timeout]);
    clearTimeout(timer);
    this.log?.info('worker stopped', { workerId: this.workerId, outcome });
    return outcome;
  }

  #sleep(ms) {
    return new Promise((resolve) => {
      const done = () => { clearTimeout(t); this.wakers.delete(done); resolve(); };
      const t = setTimeout(done, ms);
      this.wakers.add(done);
    });
  }

  async #loop() {
    while (this.running) {
      let job = null;
      try {
        job = this.queue.claim(this.queueName, this.workerId, { types: Object.keys(this.handlers), leaseMs: this.leaseMs });
      } catch (err) {
        this.log?.error('claim failed', err);
      }
      if (!job) { await this.#sleep(this.pollMs); continue; }
      await this.#run(job);
    }
  }

  async #run(job) {
    const log = this.log?.child({ jobId: job.id, type: job.type, attempt: job.attempts, correlationId: job.correlationId });
    const handler = this.handlers[job.type];
    this.inFlight++;
    const ac = new AbortController();
    const hb = setInterval(() => {
      try { this.queue.heartbeat(job.id, this.workerId, this.leaseMs); }
      catch (err) { log?.warn('heartbeat failed; aborting handler', err); ac.abort(); }
    }, this.heartbeatMs);
    let timer;
    try {
      if (!handler) throw Object.assign(new Error(`no handler registered for type "${job.type}"`), { retryable: false });
      const run = Promise.resolve().then(() => handler(job, { signal: ac.signal, log }));
      const racers = [run];
      if (this.handlerTimeoutMs > 0) {
        racers.push(new Promise((_, rej) => { timer = setTimeout(() => { ac.abort(); rej(new HandlerTimeoutError(this.handlerTimeoutMs)); }, this.handlerTimeoutMs); }));
      }
      const result = await Promise.race(racers);
      this.queue.complete(job.id, this.workerId, result ?? null);
      log?.info('job succeeded');
    } catch (err) {
      if (err instanceof LeaseError) { log?.warn('lease lost before completion; job will be retried by another worker'); }
      else {
        try {
          const after = this.queue.fail(job.id, this.workerId, err, { retryable: err?.retryable !== false });
          log?.warn('job failed', { error: err?.message, nextState: after.state });
        } catch (e2) { log?.error('could not record failure', e2); }
      }
    } finally {
      clearInterval(hb);
      clearTimeout(timer);
      this.inFlight--;
    }
  }
}
