#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { loadConfig } from '../src/config.js';
import { createLogger } from '../src/logger.js';
import { openDb } from '../src/db.js';
import { QueueService } from '../src/queue.js';
import { createApp } from '../src/server.js';
import { Worker, startReaper } from '../src/worker.js';
import { demoHandlers } from '../src/handlers.js';
import { ConfigError, QueueForgeError } from '../src/errors.js';

const USAGE = `queueforge <command> [options]

Commands:
  serve   [--with-worker] [--queue default] [--handlers file.js]   Start HTTP API + dashboard
  worker  [--queue default] [--concurrency N] [--handlers file.js]  Run a worker
  enqueue --type T [--queue Q] [--payload '{"k":1}'] [--priority N] [--delay MS] [--max-attempts N] [--key K]
  stats                                                            Queue statistics (JSON)
  list    [--state S] [--queue Q] [--limit N]                      List jobs (JSON)
  get <id> | cancel <id> | retry <id>
  reap                                                             Requeue jobs with expired leases
  purge   [--older-than-hours 24] [--include-dead]                 Delete finished jobs

Env: QF_DB_PATH, QF_PORT, QF_HOST, QF_API_KEYS, QF_LOG_LEVEL, QF_LEASE_MS, ... (see .env.example)`;

const OPTIONS = {
  'with-worker': { type: 'boolean' }, queue: { type: 'string' }, concurrency: { type: 'string' }, handlers: { type: 'string' },
  type: { type: 'string' }, payload: { type: 'string' }, priority: { type: 'string' }, delay: { type: 'string' },
  'max-attempts': { type: 'string' }, key: { type: 'string' }, state: { type: 'string' }, limit: { type: 'string' },
  'older-than-hours': { type: 'string' }, 'include-dead': { type: 'boolean' }, help: { type: 'boolean', short: 'h' },
};

const out = (v) => console.log(JSON.stringify(v, null, 2));
const num = (v) => (v === undefined ? undefined : Number(v));

async function loadHandlers(file) {
  if (!file) return demoHandlers;
  const mod = await import(pathToFileURL(resolve(file)).href);
  const h = mod.default ?? mod.handlers;
  if (!h || typeof h !== 'object') throw new ConfigError(`${file} must export default an object of { type: async (job, ctx) => result }`);
  return h;
}

async function main() {
  const { values: v, positionals } = parseArgs({ options: OPTIONS, allowPositionals: true, strict: true });
  const [cmd, arg] = positionals;
  if (!cmd || v.help) { console.log(USAGE); return; }

  const config = loadConfig();
  const logger = createLogger({ level: config.logLevel, stream: process.stderr });
  const db = openDb(config.dbPath);
  const queue = new QueueService(db, { leaseMs: config.leaseMs, backoffBaseMs: config.backoffBaseMs, backoffMaxMs: config.backoffMaxMs, defaultMaxAttempts: config.defaultMaxAttempts, logger });

  switch (cmd) {
    case 'serve':
    case 'worker': {
      const stops = [];
      let app;
      let worker;
      stops.push(startReaper(queue, config.reaperMs, logger));
      if (cmd === 'serve') {
        app = createApp({ queue, config, logger });
        const addr = await app.listen();
        logger.info('server listening', { host: addr.address, port: addr.port, auth: config.apiKeys.length > 0 });
        if (config.apiKeys.length === 0) logger.warn('API authentication is DISABLED (no QF_API_KEYS set); do not expose this server');
      }
      if (cmd === 'worker' || v['with-worker']) {
        worker = new Worker(queue, { queueName: v.queue ?? 'default', handlers: await loadHandlers(v.handlers), concurrency: num(v.concurrency) ?? config.workerConcurrency, leaseMs: config.leaseMs, logger });
        worker.start();
      }
      const shutdown = async (sig) => {
        logger.info('shutting down', { signal: sig });
        const force = setTimeout(() => process.exit(1), 15000); force.unref();
        stops.forEach((s) => s());
        await worker?.stop();
        await app?.close();
        db.close();
        process.exit(0);
      };
      process.on('SIGINT', () => shutdown('SIGINT'));
      process.on('SIGTERM', () => shutdown('SIGTERM'));
      return;
    }
    case 'enqueue': {
      let payload = {};
      if (v.payload) { try { payload = JSON.parse(v.payload); } catch { throw new ConfigError('--payload must be valid JSON'); } }
      out(queue.enqueue({ type: v.type, queue: v.queue, payload, priority: num(v.priority), delayMs: num(v.delay), maxAttempts: num(v['max-attempts']), idempotencyKey: v.key }));
      break;
    }
    case 'stats': out(queue.stats()); break;
    case 'list': out(queue.list({ state: v.state, queue: v.queue, limit: num(v.limit) ?? 50 })); break;
    case 'get': out(queue.get(arg)); break;
    case 'cancel': out(queue.cancel(arg)); break;
    case 'retry': out(queue.retry(arg)); break;
    case 'reap': out({ reaped: queue.reapExpired() }); break;
    case 'purge': out({ purged: queue.purge({ olderThanMs: Math.round((num(v['older-than-hours']) ?? 24) * 3600000), states: v['include-dead'] ? ['succeeded', 'cancelled', 'dead'] : ['succeeded', 'cancelled'] }) }); break;
    default: console.error(`Unknown command: ${cmd}\n\n${USAGE}`); process.exitCode = 2;
  }
  if (!['serve', 'worker'].includes(cmd)) db.close();
}

main().catch((err) => {
  if (err instanceof ConfigError || err instanceof QueueForgeError || err?.code === 'ERR_PARSE_ARGS_UNKNOWN_OPTION') {
    console.error(`Error: ${err.message}`);
    if (err.details) console.error(JSON.stringify(err.details));
    process.exit(err instanceof ConfigError ? 78 : 1);
  }
  console.error(err);
  process.exit(1);
});
