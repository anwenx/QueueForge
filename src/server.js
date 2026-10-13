import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { QueueForgeError, NotFoundError, PayloadTooLargeError, RateLimitError, ValidationError } from './errors.js';
import { isValidCorrelationId, isValidId, isValidName, validateListQuery } from './validate.js';
import { createAuthenticator } from './auth.js';
import { RateLimiter } from './ratelimit.js';

const PUBLIC_DIR = join(dirname(fileURLToPath(import.meta.url)), '..', 'public');
const STATIC = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/index.html': ['index.html', 'text/html; charset=utf-8'],
  '/app.js': ['app.js', 'text/javascript; charset=utf-8'],
  '/style.css': ['style.css', 'text/css; charset=utf-8'],
};
const SECURITY_HEADERS = {
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Referrer-Policy': 'no-referrer',
  'Cross-Origin-Resource-Policy': 'same-origin',
  'Content-Security-Policy': "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
};

async function readBody(req, limit) {
  const declared = Number(req.headers['content-length']);
  if (declared > limit) throw new PayloadTooLargeError(limit);
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > limit) throw new PayloadTooLargeError(limit);
    chunks.push(c);
  }
  if (size === 0) return {};
  const ct = String(req.headers['content-type'] ?? '');
  if (!ct.toLowerCase().startsWith('application/json')) throw new ValidationError('Content-Type must be application/json');
  let parsed;
  try { parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { throw new ValidationError('Malformed JSON body'); }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) throw new ValidationError('Body must be a JSON object');
  return parsed;
}

export function createApp({ queue, config, logger, authenticator = createAuthenticator(config.apiKeys), limiter } = {}) {
  limiter ??= new RateLimiter({ capacity: config.rateLimitBurst, refillPerSec: config.rateLimitPerSec });
  const startedAt = Date.now();
  const counters = new Map();
  const count = (status) => counters.set(status, (counters.get(status) ?? 0) + 1);

  const send = (res, status, body, headers = {}) => {
    const data = typeof body === 'string' ? body : JSON.stringify(body);
    res.writeHead(status, { 'Content-Type': typeof body === 'string' ? 'text/plain; charset=utf-8' : 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(data), 'Cache-Control': 'no-store', ...SECURITY_HEADERS, ...headers });
    res.end(data);
  };

  function metrics() {
    const s = queue.stats();
    const lines = ['# HELP queueforge_jobs Jobs by queue and state', '# TYPE queueforge_jobs gauge'];
    for (const [q, states] of Object.entries(s.queues)) for (const [st, n] of Object.entries(states)) lines.push(`queueforge_jobs{queue="${q}",state="${st}"} ${n}`);
    lines.push('# TYPE queueforge_oldest_runnable_age_ms gauge', `queueforge_oldest_runnable_age_ms ${s.oldestRunnableAgeMs}`);
    lines.push('# TYPE queueforge_http_requests_total counter');
    for (const [st, n] of counters) lines.push(`queueforge_http_requests_total{status="${st}"} ${n}`);
    lines.push('# TYPE queueforge_uptime_seconds gauge', `queueforge_uptime_seconds ${Math.floor((Date.now() - startedAt) / 1000)}`);
    return lines.join('\n') + '\n';
  }

  async function route(req, res, ctx) {
    const url = new URL(req.url, 'http://localhost');
    const { pathname } = url;
    const method = req.method;
    ctx.path = pathname;

    if (method === 'GET' && STATIC[pathname]) {
      const [file, type] = STATIC[pathname];
      const body = await readFile(join(PUBLIC_DIR, file));
      res.writeHead(200, { 'Content-Type': type, 'Content-Length': body.length, 'Cache-Control': 'no-cache', ...SECURITY_HEADERS });
      return res.end(body);
    }
    if (method === 'GET' && pathname === '/healthz') return send(res, 200, { status: 'ok' });
    if (method === 'GET' && pathname === '/readyz') { queue.db.prepare('SELECT 1').get(); return send(res, 200, { status: 'ready' }); }

    const principal = authenticator.authenticate(req.headers);
    ctx.principal = principal.name;
    const rl = limiter.take(principal.name === 'anonymous' ? `ip:${req.socket.remoteAddress}` : `key:${principal.name}`);
    if (!rl.allowed) throw new RateLimitError(rl.retryAfterSec);

    if (method === 'GET' && pathname === '/metrics') return send(res, 200, metrics(), { 'Content-Type': 'text/plain; version=0.0.4' });
    if (!pathname.startsWith('/api/v1/')) throw new NotFoundError();
    const p = pathname.slice('/api/v1'.length);

    if (method === 'GET' && p === '/stats') return send(res, 200, queue.stats());
    if (method === 'GET' && p === '/jobs') return send(res, 200, queue.list(validateListQuery(Object.fromEntries(url.searchParams))));
    if (method === 'POST' && p === '/jobs') {
      const body = await readBody(req, config.maxBodyBytes);
      const cid = body && typeof body === 'object' && !Array.isArray(body) && body.correlationId === undefined && isValidCorrelationId(ctx.correlationId) ? { ...body, correlationId: ctx.correlationId } : body;
      const { job, duplicate } = queue.enqueue(cid);
      return send(res, duplicate ? 200 : 201, { job, duplicate });
    }

    let m = /^\/queues\/([^/]+)\/claim$/.exec(p);
    if (method === 'POST' && m) {
      if (!isValidName(m[1])) throw new ValidationError('Invalid queue name');
      const body = await readBody(req, config.maxBodyBytes);
      if (!isValidName(body.workerId ?? '')) throw new ValidationError('workerId required (valid name)');
      const types = body.types === undefined ? undefined : body.types;
      if (types !== undefined && (!Array.isArray(types) || types.length > 50 || !types.every(isValidName))) throw new ValidationError('types must be an array of valid names');
      const leaseMs = body.leaseMs;
      if (leaseMs !== undefined && (!Number.isInteger(leaseMs) || leaseMs < 1000 || leaseMs > 3600000)) throw new ValidationError('leaseMs must be an integer 1000-3600000');
      return send(res, 200, { job: queue.claim(m[1], body.workerId, { types, leaseMs }) });
    }

    m = /^\/jobs\/([^/]+)(?:\/(events|cancel|retry|ack|nack|heartbeat))?$/.exec(p);
    if (m) {
      const [, id, action] = m;
      if (!isValidId(id)) throw new NotFoundError();
      if (method === 'GET' && !action) return send(res, 200, { job: queue.get(id) });
      if (method === 'GET' && action === 'events') return send(res, 200, { events: queue.events(id) });
      if (method === 'POST' && action === 'cancel') return send(res, 200, { job: queue.cancel(id) });
      if (method === 'POST' && action === 'retry') return send(res, 200, { job: queue.retry(id) });
      if (method === 'POST' && ['ack', 'nack', 'heartbeat'].includes(action)) {
        const body = await readBody(req, config.maxBodyBytes);
        if (!isValidName(body.workerId ?? '')) throw new ValidationError('workerId required (valid name)');
        if (action === 'ack') return send(res, 200, { job: queue.complete(id, body.workerId, body.result ?? null) });
        if (action === 'nack') return send(res, 200, { job: queue.fail(id, body.workerId, String(body.error ?? 'failed'), { retryable: body.retryable !== false }) });
        return send(res, 200, { job: queue.heartbeat(id, body.workerId, body.leaseMs) });
      }
    }
    throw new NotFoundError();
  }

  const server = http.createServer(async (req, res) => {
    const t0 = process.hrtime.bigint();
    const incoming = req.headers['x-correlation-id'] ?? req.headers['x-request-id'];
    const correlationId = isValidCorrelationId(incoming) ? incoming : randomUUID();
    res.setHeader('X-Correlation-Id', correlationId);
    const ctx = { correlationId, path: req.url, principal: null };
    let status = 500;
    try {
      await route(req, res, ctx);
      status = res.statusCode;
    } catch (err) {
      if (err instanceof QueueForgeError) {
        status = err.status;
        const headers = err instanceof RateLimitError ? { 'Retry-After': String(err.retryAfterSec) } : {};
        if (!res.headersSent) send(res, status, { error: { code: err.code, message: err.message, details: err.details, correlationId } }, headers);
      } else {
        status = 500;
        logger?.error('unhandled error', { correlationId, err: { name: err?.name, message: err?.message } });
        if (!res.headersSent) send(res, 500, { error: { code: 'internal_error', message: 'Internal server error', correlationId } });
      }
    } finally {
      count(status);
      logger?.info('request', { correlationId, method: req.method, path: ctx.path.split('?')[0], status, ms: Number(process.hrtime.bigint() - t0) / 1e6, principal: ctx.principal });
    }
  });
  server.requestTimeout = 15000;
  server.headersTimeout = 10000;
  server.keepAliveTimeout = 5000;
  server.maxRequestsPerSocket = 1000;

  return {
    server,
    listen: (port = config.port, host = config.host) => new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, host, () => { server.off('error', reject); resolve(server.address()); });
    }),
    close: () => new Promise((resolve) => { server.close(() => resolve()); server.closeIdleConnections?.(); }),
  };
}
