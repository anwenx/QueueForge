import { createHash, timingSafeEqual } from 'node:crypto';
import { AuthError } from './errors.js';

const sha = (s) => createHash('sha256').update(s).digest();

export function extractKey(headers) {
  const auth = headers['authorization'];
  if (typeof auth === 'string') {
    const m = /^Bearer\s+(\S+)$/i.exec(auth);
    if (m) return m[1];
  }
  const x = headers['x-api-key'];
  return typeof x === 'string' && x ? x : null;
}

/**
 * Constant-time API key authenticator.
 * With no keys configured (dev only; production config refuses that) every request is the "anonymous" principal.
 */
export function createAuthenticator(keys) {
  const entries = keys.map(({ name, key }) => ({ name, hash: sha(key) }));
  return {
    enabled: entries.length > 0,
    authenticate(headers) {
      if (entries.length === 0) return { name: 'anonymous' };
      const provided = extractKey(headers);
      if (!provided) throw new AuthError();
      const h = sha(provided);
      let match = null;
      for (const e of entries) if (timingSafeEqual(h, e.hash)) match = e; // no early exit
      if (!match) throw new AuthError();
      return { name: match.name };
    },
  };
}
