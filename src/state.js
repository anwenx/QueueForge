import { StateError } from './errors.js';

export const STATES = Object.freeze(['queued', 'running', 'succeeded', 'dead', 'cancelled']);

/** Allowed job state transitions. Anything else is a bug or a race and is rejected. */
export const TRANSITIONS = Object.freeze({
  queued: ['running', 'cancelled'],
  running: ['succeeded', 'queued', 'dead'], // queued = scheduled retry or lease-expiry requeue
  succeeded: [],
  dead: ['queued'],      // manual retry
  cancelled: ['queued'], // manual retry
});

export function canTransition(from, to) {
  return (TRANSITIONS[from] ?? []).includes(to);
}

export function assertTransition(from, to) {
  if (!canTransition(from, to)) throw new StateError(`Illegal transition ${from} -> ${to}`);
}

/**
 * Exponential backoff with bounded jitter.
 * attempt is 1-based (the attempt that just failed).
 * delay = min(max, base * 2^(attempt-1)) * (1 - jitter * rng())
 */
export function backoffMs(attempt, { baseMs = 1000, maxMs = 300000, jitter = 0.2, rng = Math.random } = {}) {
  if (!Number.isInteger(attempt) || attempt < 1) throw new RangeError('attempt must be an integer >= 1');
  const exp = Math.min(maxMs, baseMs * 2 ** Math.min(attempt - 1, 30));
  const j = Math.min(1, Math.max(0, jitter));
  return Math.max(0, Math.round(exp * (1 - j * rng())));
}
