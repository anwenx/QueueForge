# QueueForge architecture

## Components
| Module | Responsibility |
|---|---|
| `src/db.js` | Opens SQLite (WAL, busy timeout), versioned migrations |
| `src/state.js` | Job state machine + exponential backoff with jitter |
| `src/queue.js` | `QueueService`: enqueue, claim/lease, heartbeat, complete, fail, reap, cancel, retry, list, stats, purge |
| `src/worker.js` | `Worker` polling loop(s), heartbeats, handler timeouts, graceful stop; `startReaper` |
| `src/server.js` | Zero-dependency HTTP API, dashboard hosting, auth, rate limiting, metrics |
| `public/` | Dashboard (vanilla JS, CSP-safe, DOM built with `textContent`) |
| `bin/queueforge.js` | CLI: serve, worker, enqueue, stats, list, get, cancel, retry, reap, purge |

## Job lifecycle
```
            enqueue
               v
  cancel <- queued <--------------------+----------- manual retry (dead/cancelled)
               | claim (attempts+1, lease)
               v
            running --heartbeat--> running
             |  |  \
   ack       |  |   \ lease expires (reaper)
     v       |  |    \
 succeeded   |  |     +--> queued (backoff)   if attempts < max
             |  | nack/throw
             |  +--------> queued (backoff)   if retryable and attempts < max
             +-----------> dead               if permanent or attempts exhausted
```

## Guarantees
* **At-least-once delivery.** A job whose worker crashes is requeued when its lease expires, so handlers must be idempotent. Use `idempotencyKey` at enqueue time to dedupe producers.
* **No double claim.** `claim` runs in a `BEGIN IMMEDIATE` transaction; SQLite serialises writers across processes (tested with two connections on one file).
* **Fencing.** `ack`, `nack` and `heartbeat` require the current `lease_owner` and `running` state; a worker that lost its lease gets `409 lease_lost`.
* **Ordering.** Highest priority first, then earliest `run_at`, then FIFO. Not strictly FIFO across retries.
* **Durability.** WAL with `synchronous=NORMAL`: committed transactions survive a process crash; the last few commits may be lost on OS crash/power loss. Set `PRAGMA synchronous=FULL` in `src/db.js` if that matters.

## Scaling limits (read before production use)
SQLite allows one writer at a time and all processes must share the same local filesystem. QueueForge fits a single host (API + workers on one machine or one volume) at roughly hundreds to low thousands of jobs/second. It is **not** a multi-node distributed queue; for that, replace `QueueService` with a PostgreSQL implementation using `SELECT ... FOR UPDATE SKIP LOCKED` (the claim logic maps directly). Do not put the database on NFS/SMB.

## Security model
* API keys (>= 16 chars) compared in constant time against SHA-256 digests; keys are never logged (logger redacts `authorization`, `api-key`, `token`, `password`, `secret`, `cookie`).
* `NODE_ENV=production` refuses to start without `QF_API_KEYS`. Without keys in development the API is open and logs a warning.
* All SQL is parameterised; LIKE input is escaped; names/ids are allow-listed by regex.
* Request body size limit, request/headers timeouts, per-key token-bucket rate limiting, strict JSON-object body validation.
* Dashboard served with a strict CSP (`script-src 'self'`), no inline scripts, `X-Frame-Options: DENY`. The API key is held in `sessionStorage` only.
* No TLS built in: terminate TLS at a reverse proxy.
