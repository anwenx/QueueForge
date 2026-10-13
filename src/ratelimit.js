/** In-memory token bucket limiter keyed by caller identity. */
export class RateLimiter {
  constructor({ capacity = 100, refillPerSec = 50, clock = Date.now, maxKeys = 10000 } = {}) {
    this.capacity = capacity;
    this.refillPerSec = refillPerSec;
    this.clock = clock;
    this.maxKeys = maxKeys;
    this.buckets = new Map();
  }
  /** @returns {{allowed:boolean, retryAfterSec:number, remaining:number}} */
  take(key, cost = 1) {
    const now = this.clock();
    let b = this.buckets.get(key);
    if (!b) {
      if (this.buckets.size >= this.maxKeys) this.buckets.delete(this.buckets.keys().next().value); // evict oldest
      b = { tokens: this.capacity, last: now };
    } else {
      b.tokens = Math.min(this.capacity, b.tokens + ((now - b.last) / 1000) * this.refillPerSec);
      b.last = now;
      this.buckets.delete(key); // re-insert to keep Map in recency order
    }
    this.buckets.set(key, b);
    if (b.tokens >= cost) { b.tokens -= cost; return { allowed: true, retryAfterSec: 0, remaining: Math.floor(b.tokens) }; }
    const retryAfterSec = Math.max(1, Math.ceil((cost - b.tokens) / this.refillPerSec));
    return { allowed: false, retryAfterSec, remaining: 0 };
  }
}
