# Event Ingestion Platform

A browser-interaction-first event ingestion platform built in Rust. Producers send events via HTTP. Workers claim, process, and complete them. Stale or failed events are retried automatically and dead-lettered after exhausting retries.

## Architecture

```
Producer ──POST──► API Server ──INSERT──► PostgreSQL
                       ▲
Worker ◄──claim────────┘
  │
  ├── process (1s sleep)
  │
  └── complete ──PATCH──► API Server ──UPDATE──► PostgreSQL
```

| Binary | Role |
|---|---|
| `event-ingestion-platform` | HTTP API server (Axum + sqlx) |
| `worker` | Background worker that claims, processes, and completes events |
| `producer` | Load generator - sends events with random data and idempotent replays |

## Quick Start

```bash
# 1. Start PostgreSQL
docker compose up -d

# 2. Run migrations
sqlx migrate run

# 3. Start the API server
cargo run --bin event-ingestion-platform

# 4. (Optional) Start a producer to generate events
cargo run --bin producer

# 5. (Optional) Start a worker to process events
cargo run --bin worker
```

Environment: copy `.env.example` as `.env` or set `DATABASE_URL=postgres://event_user:event_password@localhost:5432/event_ingestion`.

## Event Lifecycle

```
                  ┌──────────┐
        ┌────────►│ accepted ├◄──────────────┐
        │         └────┬─────┘               │
        │              │ claim               │ requeue
        │         ┌────▼─────┐               │ (attempt < 3)
        │         │processing├───────────────┘
        │         └────┬─────┘
        │              │ complete
        │    ┌─────────┼─────────┐
        │    │                   │
   ┌────▼──┐              ┌─────▼──────┐
   │processed│            │ dead_lettered│ (attempt ≥ 3)
   └─────────┘            └─────────────┘
```

1. Producer `POST`s an event → status `accepted`
2. Worker `POST /claim` → status `processing`, acquires lock
3. Worker `POST /complete` with `processed` → done
4. If worker crashes mid-processing: `POST /requeue` resets to `accepted`, bumps `attempt_count`
5. After `MAX_ATTEMPTS` (3) retries → `dead_lettered` with reason and timestamp

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check |
| `GET` | `/ready` | Readiness check (includes DB) |
| `POST` | `/v1/events` | Submit an event |
| `GET` | `/v1/events?status=<status>` | List events by status (up to 20) |
| `GET` | `/v1/events/stale?older_than_seconds=<n>` | Find stale processing events |
| `POST` | `/v1/events/claim` | Claim next accepted event (with worker_id) |
| `POST` | `/v1/events/{id}/complete` | Complete a claimed event (processed/failed) |
| `POST` | `/v1/events/{id}/requeue` | Requeue a stale event |
| `GET` | `/v1/events/{id}` | Get full event details |
| `GET` | `/v1/events/{id}/status` | Get event status only |
| `PATCH` | `/v1/events/{id}/status` | Manual status transition |

### Submit Event

```bash
curl -X POST http://localhost:3000/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "producer_id": "service-a",
    "idempotency_key": "req-001",
    "event_type": "user.signup",
    "schema_version": 1,
    "message": "user@example.com"
  }'
```

### Claim + Complete (worker flow)

```bash
# Claim the next available event
curl -X POST http://localhost:3000/v1/events/claim \
  -H "Content-Type: application/json" \
  -d '{"worker_id": "worker-1"}'

# Complete it
curl -X POST http://localhost:3000/v1/events/<event_id>/complete \
  -H "Content-Type: application/json" \
  -d '{"worker_id": "worker-1", "status": "processed"}'
```

### Requeue stale events

```bash
curl -X POST http://localhost:3000/v1/events/<event_id>/requeue \
  -H "Content-Type: application/json" \
  -d '{"older_than_seconds": 10}'
```

## Features

- **Idempotent ingestion** - `(producer_id, idempotency_key)` unique constraint prevents duplicates. Same key + same body returns 202 with the original `event_id`. Same key + different body returns 409 Conflict.
- **Worker claim + lock** - `FOR UPDATE SKIP LOCKED` ensures no two workers claim the same event. Lock ownership is validated on completion.
- **Automatic retry** - Stale processing events (worker crashed) are detected via `locked_at < now() - threshold` and requeued. `attempt_count` tracks retries.
- **Dead letter** - After `MAX_ATTEMPTS` (3) retries, events are marked `dead_lettered` with timestamp and reason. They will not be picked up again.
- **Graceful shutdown** - API server drains in-flight requests before exiting. Worker finishes its current event, then stops. Both listen for SIGINT and SIGTERM.

## Statuses

| Status | Meaning |
|---|---|
| `accepted` | Event received, waiting for a worker |
| `processing` | Claimed by a worker, being processed |
| `processed` | Successfully completed |
| `failed` | Worker reported failure (via complete endpoint) |
| `dead_lettered` | Max retries exhausted, permanently parked |

## Running the demo

```bash
# Terminal 1: API server
cargo run --bin event-ingestion-platform

# Terminal 2: Producer (generates events)
cargo run --bin producer

# Terminal 3: Normal worker
WORKER_ID=worker-a cargo run --bin worker

# Terminal 4: Crash-mode worker (claims then dies)
FAIL_AFTER_CLAIM=true WORKER_ID=crash-worker cargo run --bin worker
```

The crash worker will leave events orphaned in `processing`. Use the requeue endpoint or a monitor to retry them. After 3 requeues, events move to `dead_lettered`.
