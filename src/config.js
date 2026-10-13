import { ConfigError } from './errors.js';

function int(env, name, def, min, max) {
  const raw = env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < min || n > max) throw new ConfigError(`${name} must be an integer between ${min} and ${max} (got "${raw}")`);
  return n;
}

/** Parse "name:key,name2:key2" (or bare keys) into [{name, key}]. */
export function parseApiKeys(raw) {
  if (!raw) return [];
  return raw.split(',').map((s) => s.trim()).filter(Boolean).map((entry, i) => {
    const idx = entry.indexOf(':');
    const name = idx > 0 ? entry.slice(0, idx) : `key${i + 1}`;
    const key = idx > 0 ? entry.slice(idx + 1) : entry;
    if (key.length < 16) throw new ConfigError(`API key "${name}" is too short (minimum 16 characters)`);
    return { name, key };
  });
}

export function loadConfig(env = process.env) {
  const nodeEnv = env.NODE_ENV || 'development';
  const apiKeys = parseApiKeys(env.QF_API_KEYS);
  if (nodeEnv === 'production' && apiKeys.length === 0) {
    throw new ConfigError('QF_API_KEYS is required when NODE_ENV=production');
  }
  const logLevel = env.QF_LOG_LEVEL || 'info';
  if (!['debug', 'info', 'warn', 'error', 'silent'].includes(logLevel)) throw new ConfigError(`QF_LOG_LEVEL invalid: ${logLevel}`);
  return Object.freeze({
    env: nodeEnv,
    host: env.QF_HOST || '127.0.0.1',
    port: int(env, 'QF_PORT', 8080, 0, 65535),
    dbPath: env.QF_DB_PATH || './data/queueforge.db',
    apiKeys,
    logLevel,
    leaseMs: int(env, 'QF_LEASE_MS', 30000, 1000, 3600000),
    backoffBaseMs: int(env, 'QF_BACKOFF_BASE_MS', 1000, 1, 3600000),
    backoffMaxMs: int(env, 'QF_BACKOFF_MAX_MS', 300000, 1, 86400000),
    defaultMaxAttempts: int(env, 'QF_DEFAULT_MAX_ATTEMPTS', 5, 1, 25),
    reaperMs: int(env, 'QF_REAPER_MS', 5000, 0, 3600000),
    rateLimitPerSec: int(env, 'QF_RATE_LIMIT_PER_SEC', 50, 1, 100000),
    rateLimitBurst: int(env, 'QF_RATE_LIMIT_BURST', 100, 1, 100000),
    maxBodyBytes: int(env, 'QF_MAX_BODY_BYTES', 128 * 1024, 1024, 10 * 1024 * 1024),
    workerConcurrency: int(env, 'QF_WORKER_CONCURRENCY', 4, 1, 256),
  });
}
