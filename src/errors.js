export class QueueForgeError extends Error {
  constructor(message, { status = 500, code = 'internal_error', details } = {}) {
    super(message);
    this.name = this.constructor.name;
    this.status = status;
    this.code = code;
    this.details = details;
  }
}
export class ValidationError extends QueueForgeError {
  constructor(message, details) { super(message, { status: 400, code: 'validation_error', details }); }
}
export class AuthError extends QueueForgeError {
  constructor(message = 'Missing or invalid API key') { super(message, { status: 401, code: 'unauthorized' }); }
}
export class NotFoundError extends QueueForgeError {
  constructor(message = 'Not found') { super(message, { status: 404, code: 'not_found' }); }
}
export class StateError extends QueueForgeError {
  constructor(message) { super(message, { status: 409, code: 'invalid_state' }); }
}
export class LeaseError extends QueueForgeError {
  constructor(message = 'Lease lost: job is no longer held by this worker') { super(message, { status: 409, code: 'lease_lost' }); }
}
export class RateLimitError extends QueueForgeError {
  constructor(retryAfterSec) { super('Too many requests', { status: 429, code: 'rate_limited', details: { retryAfterSec } }); this.retryAfterSec = retryAfterSec; }
}
export class PayloadTooLargeError extends QueueForgeError {
  constructor(limit) { super(`Request body exceeds ${limit} bytes`, { status: 413, code: 'payload_too_large' }); }
}
export class ConfigError extends Error {
  constructor(message) { super(message); this.name = 'ConfigError'; }
}
/** Throw from a handler to fail a job permanently (straight to the dead-letter state). */
export class PermanentError extends Error {
  constructor(message) { super(message); this.name = 'PermanentError'; this.retryable = false; }
}
