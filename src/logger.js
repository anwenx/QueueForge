const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };
const SENSITIVE = /^(authorization|api[-_]?key|x-api-key|token|password|secret|cookie)$/i;

/** Recursively redact sensitive keys. Depth-limited and cycle-safe. */
export function redact(value, depth = 0, seen = new WeakSet()) {
  if (value === null || typeof value !== 'object') return value;
  if (seen.has(value)) return '[Circular]';
  if (depth > 5) return '[Truncated]';
  seen.add(value);
  if (Array.isArray(value)) return value.map((v) => redact(v, depth + 1, seen));
  const out = {};
  for (const [k, v] of Object.entries(value)) out[k] = SENSITIVE.test(k) ? '[REDACTED]' : redact(v, depth + 1, seen);
  return out;
}

export function createLogger({ level = 'info', stream = process.stdout, base = {}, clock = () => new Date() } = {}) {
  const threshold = LEVELS[level] ?? LEVELS.info;
  const make = (ctx) => {
    const log = (lvl, msg, fields) => {
      if (LEVELS[lvl] < threshold) return;
      const extra = fields instanceof Error ? { err: { name: fields.name, message: fields.message } } : redact(fields ?? {});
      stream.write(JSON.stringify({ time: clock().toISOString(), level: lvl, msg, ...ctx, ...extra }) + '\n');
    };
    return {
      debug: (m, f) => log('debug', m, f),
      info: (m, f) => log('info', m, f),
      warn: (m, f) => log('warn', m, f),
      error: (m, f) => log('error', m, f),
      child: (extra) => make({ ...ctx, ...extra }),
    };
  };
  return make(base);
}
