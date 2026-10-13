import { ValidationError } from './errors.js';
import { STATES } from './state.js';

const NAME_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const CORR_RE = /^[A-Za-z0-9._-]{1,64}$/;
const ID_RE = /^[A-Za-z0-9-]{8,64}$/;
export const MAX_PAYLOAD_BYTES = 64 * 1024;
export const MAX_DELAY_MS = 30 * 24 * 3600 * 1000;

export function isValidName(s) { return typeof s === 'string' && NAME_RE.test(s); }
export function isValidCorrelationId(s) { return typeof s === 'string' && CORR_RE.test(s); }
export function isValidId(s) { return typeof s === 'string' && ID_RE.test(s); }

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

function intIn(errors, field, value, min, max, fallback) {
  if (value === undefined) return fallback;
  if (!Number.isInteger(value) || value < min || value > max) {
    errors.push({ field, message: `must be an integer between ${min} and ${max}` });
    return fallback;
  }
  return value;
}

/** Validate and normalise an enqueue request. Throws ValidationError listing every problem. */
export function validateEnqueue(input, { defaultMaxAttempts = 5 } = {}) {
  if (!isPlainObject(input)) throw new ValidationError('Body must be a JSON object');
  const errors = [];
  const allowed = new Set(['queue', 'type', 'payload', 'priority', 'delayMs', 'maxAttempts', 'correlationId', 'idempotencyKey']);
  for (const k of Object.keys(input)) if (!allowed.has(k)) errors.push({ field: k, message: 'unknown field' });

  const queue = input.queue ?? 'default';
  if (!isValidName(queue)) errors.push({ field: 'queue', message: 'must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}' });
  if (!isValidName(input.type)) errors.push({ field: 'type', message: 'required; must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}' });

  const payload = input.payload ?? {};
  let payloadJson = '{}';
  if (!isPlainObject(payload)) errors.push({ field: 'payload', message: 'must be a JSON object' });
  else {
    payloadJson = JSON.stringify(payload);
    if (Buffer.byteLength(payloadJson) > MAX_PAYLOAD_BYTES) errors.push({ field: 'payload', message: `must be <= ${MAX_PAYLOAD_BYTES} bytes` });
  }

  const priority = intIn(errors, 'priority', input.priority, -100, 100, 0);
  const delayMs = intIn(errors, 'delayMs', input.delayMs, 0, MAX_DELAY_MS, 0);
  const maxAttempts = intIn(errors, 'maxAttempts', input.maxAttempts, 1, 25, defaultMaxAttempts);

  const correlationId = input.correlationId ?? null;
  if (correlationId !== null && !isValidCorrelationId(correlationId)) errors.push({ field: 'correlationId', message: 'must match [A-Za-z0-9._-]{1,64}' });

  const idempotencyKey = input.idempotencyKey ?? null;
  if (idempotencyKey !== null && (typeof idempotencyKey !== 'string' || idempotencyKey.length < 1 || idempotencyKey.length > 128 || !/^[\x21-\x7E]+$/.test(idempotencyKey))) {
    errors.push({ field: 'idempotencyKey', message: 'must be 1-128 printable ASCII characters without spaces' });
  }

  if (errors.length) throw new ValidationError('Invalid job', errors);
  return { queue, type: input.type, payloadJson, priority, delayMs, maxAttempts, correlationId, idempotencyKey };
}

export function validateListQuery(q) {
  const errors = [];
  const out = {};
  if (q.state !== undefined) { if (!STATES.includes(q.state)) errors.push({ field: 'state', message: `must be one of ${STATES.join(', ')}` }); else out.state = q.state; }
  for (const f of ['queue', 'type']) if (q[f] !== undefined) { if (!isValidName(q[f])) errors.push({ field: f, message: 'invalid name' }); else out[f] = q[f]; }
  if (q.correlationId !== undefined) {
    if (typeof q.correlationId !== 'string' || q.correlationId.length < 1 || q.correlationId.length > 64) errors.push({ field: 'correlationId', message: 'must be 1-64 characters' });
    else out.correlationId = q.correlationId;
  }
  const num = (f, min, max, def) => {
    if (q[f] === undefined) return def;
    const n = Number(q[f]);
    if (!Number.isInteger(n) || n < min || n > max) { errors.push({ field: f, message: `must be an integer between ${min} and ${max}` }); return def; }
    return n;
  };
  out.limit = num('limit', 1, 200, 50);
  out.offset = num('offset', 0, 1_000_000, 0);
  if (errors.length) throw new ValidationError('Invalid query', errors);
  return out;
}
