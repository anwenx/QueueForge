# QueueForge

A durable job queue for Node.js with leases, retries with exponential backoff, dead-lettering, idempotent enqueue, an HTTP API, a worker runtime and a live dashboard. **Zero runtime dependencies**: it uses Node's built-in `node:sqlite`, `node:http` and `node:test`.

Requires **Node.js >= 22.13** (Node prints an `ExperimentalWarning` for `node:sqlite`; it is harmless).

## Quick start
```bash
npm test                      # 73 tests
npm start                     # API + dashboard + demo worker on http://127.0.0.1:8080
# in another shell
node bin/queueforge.js enqueue --type echo --payload '{"hello":"world"}'
node bin/queueforge.js stats
```
Open http://127.0.0.1:8080 for the dashboard. In development no API key is needed; in production set `QF_API_KEYS`.

## HTTP API (`/api/v1`, `Authorization: Bearer <key>`)
| Method & path | Purpose |
|---|---|
| `POST /jobs` | Enqueue `{queue?, type, payload?, priority?, delayMs?, maxAttempts?, correlationId?, idempotencyKey?}` -> 201 (200 if duplicate) |
| `GET /jobs?state&queue&type&correlationId&limit&offset` | List |
| `GET /jobs/:id`, `GET /jobs/:id/events` | Inspect |
| `POST /jobs/:id/cancel`, `/retry` | Cancel queued job / requeue dead or cancelled job |
| `POST /queues/:queue/claim` `{workerId, types?, leaseMs?}` | Remote worker: lease next job (`job: null` if none) |
| `POST /jobs/:id/ack` `{workerId, result?}`, `/nack` `{workerId, error, retryable?}`, `/heartbeat` `{workerId, leaseMs?}` | Remote worker protocol |
| `GET /stats`, `GET /metrics` (Prometheus) | Observability |
| `GET /healthz`, `GET /readyz` | Probes (no auth) |

Errors: `{"error":{"code","message","details?","correlationId"}}`. Send `X-Correlation-Id` to propagate your own id (echoed in the response and logs).

```bash
curl -s -X POST localhost:8080/api/v1/jobs -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":{"n":1},"idempotencyKey":"order-42"}'
```

## Writing handlers
```js
// handlers.js
import { PermanentError } from './src/errors.js';
export default {
  'send-email': async (job, { signal, log }) => {
    if (!job.payload.to) throw new PermanentError('missing recipient'); // dead-letter, no retries
    // ... do work; honour `signal` for cancellation/timeouts; must be idempotent
    return { sent: true };
  },
};
```
`node bin/queueforge.js worker --handlers ./handlers.js --queue default --concurrency 8`

## Configuration
See `.env.example`. Invalid values fail fast at startup.

## Docker
```bash
cp .env.example .env   # set QF_API_KEYS
docker compose up --build
```
(The Docker files were written but could not be built in the authoring environment: no Docker available there. CI builds the image.)

## Operations
* Graceful shutdown on SIGTERM/SIGINT: stops claiming, drains in-flight jobs (15 s cap), closes DB.
* Back up the SQLite file with `sqlite3 queueforge.db ".backup backup.db"` (not `cp` while running).
* `queueforge purge --older-than-hours 24` deletes old finished jobs; schedule it with cron.
* Metrics: scrape `/metrics`; alert on `queueforge_oldest_runnable_age_ms` and `state="dead"`.

## Limitations
Single-host by design (SQLite). See `docs/ARCHITECTURE.md` for delivery guarantees, security model and the path to PostgreSQL.
