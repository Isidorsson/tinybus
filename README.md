# tinybus

A durable job queue for Go, backed by Postgres. Single binary, no
broker, crash-safe via `SELECT … FOR UPDATE SKIP LOCKED`.

Built as a focused MVP to demonstrate the SQL-design half of backend
engineering — the sibling project to
[collab-board](https://github.com/Isidorsson/collab-board), which
covers the concurrency / real-time half.

## Quick start

Local stack via Docker Compose (Postgres + migrate + worker + producer):

```bash
docker compose up --build
```

You'll see the producer enqueueing every 2s and the worker logging
each job (with a 10% synthetic failure rate to exercise the retry
path). Visit http://localhost:8080/healthz and
http://localhost:8080/stats.

Tear down with `docker compose down -v` (the `-v` drops the Postgres
volume).

Without Docker:

```bash
export DATABASE_URL=postgres://tinybus:tinybus@localhost:5432/tinybus?sslmode=disable
make migrate
make worker        # in one terminal
make enqueue       # in another, repeatedly
make stats
```

## What's interesting in here

The single design decision worth reading:

### Crash-safe job ownership without a separate broker

A worker claims a job in *one* round-trip:

```sql
WITH next AS (
    SELECT id FROM jobs
    WHERE queue = $1
      AND locked_at IS NULL
      AND dead_at  IS NULL
      AND run_at   <= now()
    ORDER BY run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET    locked_at = now(),
       locked_by = $2,
       attempts  = attempts + 1
WHERE  id = (SELECT id FROM next)
RETURNING id, queue, payload, attempts, max_attempts, created_at, run_at;
```

Three things to notice:

1. **`FOR UPDATE SKIP LOCKED`** is what makes this work under load. Two
   workers competing for the same row: one wins, the other skips and
   takes the next eligible row. No global lock, no broker, no
   coordinator.
2. **CTE + UPDATE in a single statement** closes the race window. A
   `SELECT` followed by a separate `UPDATE` lets two workers see the
   same row before either claims it. The CTE acquires the row lock,
   the outer `UPDATE` mutates the state, both inside the same
   statement and the same round-trip.
3. **`attempts` is incremented at *claim time***, not failure time.
   So a worker crash *after claim* still counts as an attempt — a
   poisoned job that crashes its handler still hits `max_attempts`
   and ends up dead, instead of running forever.

That's the whole game. Postgres' row-level lock is the broker.

### Crash recovery: lock-expiry, not heartbeat

Workers don't heartbeat. Instead, a sweeper goroutine periodically
clears `locked_at` from any in-flight job whose lock is older than the
configured lease:

```sql
UPDATE jobs
SET    locked_at = NULL, locked_by = NULL, last_error = ...
WHERE  locked_at IS NOT NULL
  AND  dead_at   IS NULL
  AND  locked_at < now() - make_interval(secs => $1);
```

This makes tinybus **at-least-once**, not exactly-once. A handler that
runs longer than the lease will be re-run by another worker. Set the
lease above your p99 handler runtime, or split long handlers into
smaller jobs.

The trade-off: heartbeating from inside the handler conflates "is the
worker alive?" with "is the handler making progress?", and adds a
connection per in-flight job. River (Go) and Oban (Elixir) both make
the same lock-expiry choice for the same reason.

### Backoff with equal jitter

Failed jobs retry with exponential backoff plus equal jitter:

```
d    = 1s * 2^(attempts-1), capped at 5m
half = d / 2
out  = half + rand[0, half]
```

Equal jitter (vs full jitter `rand[0, d]`) guarantees at least `d/2`
separation between consecutive retries, while still spreading retries
across the worker pool — avoids the dogpile where all retries fire at
the same instant after a transient outage.

## Architecture

```
producers ──▶ INSERT ──▶ ┌──────────┐ ◀── UPDATE ── workers
                         │   jobs   │     (CTE+SKIP LOCKED)
                         └──────────┘
                              ▲
                              │  UPDATE locked_at = NULL
                              │  WHERE locked_at < now() - lease
                              │
                          sweeper goroutine
                          (one per Process call)
```

See `images/architecture.svg` for the rendered version embedded in the
portfolio entry.

## Schema

| Column         | Type          | Notes |
|----------------|---------------|-------|
| `id`           | `BIGSERIAL`   | PK |
| `queue`        | `TEXT`        | logical queue name |
| `payload`      | `BYTEA`       | opaque, set by producer |
| `attempts`     | `INT`         | incremented at claim time |
| `max_attempts` | `INT`         | default 5 |
| `last_error`   | `TEXT`        | nullable |
| `created_at`   | `TIMESTAMPTZ` | |
| `run_at`       | `TIMESTAMPTZ` | when the job becomes eligible |
| `locked_at`    | `TIMESTAMPTZ` | NULL = ready, set on claim |
| `locked_by`    | `TEXT`        | worker id; useful for forensics |
| `dead_at`      | `TIMESTAMPTZ` | NULL = alive, set on terminal failure |

State is **implicit**, not a status column:

| State      | Predicate |
|------------|-----------|
| ready      | `locked_at IS NULL AND dead_at IS NULL AND run_at <= now()` |
| delayed    | `locked_at IS NULL AND dead_at IS NULL AND run_at >  now()` |
| in-flight  | `locked_at IS NOT NULL AND dead_at IS NULL` |
| dead       | `dead_at IS NOT NULL` |
| completed  | row deleted |

Three partial indexes back the hot paths:

- `idx_jobs_ready` — `(queue, run_at) WHERE locked_at IS NULL AND dead_at IS NULL` — the claim query
- `idx_jobs_dead` — `(queue, dead_at DESC) WHERE dead_at IS NOT NULL` — dead-letter inspection
- `idx_jobs_in_flight` — `(locked_at) WHERE locked_at IS NOT NULL AND dead_at IS NULL` — the sweeper

Partial indexes only contain rows that match the predicate, so even a
table with millions of historical rows keeps the same fetch latency
as one with a hundred ready rows.

## CLI

```
tinybus migrate <up|down>
tinybus enqueue --queue=X --payload=Y [--run-in=DUR] [--max-attempts=N]
tinybus worker  --queue=X [--concurrency=N] [--http-addr=:8080] [--lease=5m] [--poll=1s] [--fail-pct=N]
tinybus producer --queue=X --interval=2s --payload=...
tinybus stats
```

Reads `DATABASE_URL` from the environment. If `PORT` is set and
`--http-addr` is unset, the worker listens on `:$PORT` for `/healthz`
and `/stats` (Railway-friendly).

## Go API

```go
import "github.com/Isidorsson/tinybus/pkg/tinybus"

q, err := tinybus.New(ctx,
    tinybus.WithDSN(os.Getenv("DATABASE_URL")),
    tinybus.WithConcurrency(4),
    tinybus.WithLeaseDuration(2*time.Minute),
)
if err != nil { return err }
defer q.Close()

// Producer
id, err := q.Enqueue(ctx, "email", []byte(`{"to":"a@b.com"}`),
    tinybus.RunIn(30*time.Second),
    tinybus.MaxAttempts(10),
)

// Worker
err = q.Process(ctx, "email", func(ctx context.Context, job tinybus.Job) error {
    return sendEmail(job.Payload)
})
```

## Testing

```bash
go test -race ./...                # unit tests (no Docker)
go test -race -tags integration ./...   # integration tests (need Docker)
```

The integration tests use [testcontainers-go](https://golang.testcontainers.org)
to spin up a real Postgres for each test. Coverage:

- Enqueue → claim → complete (happy path)
- Failed handler → retry with backoff → eventual dead state
- Concurrent workers don't double-process (50 jobs, 4 workers, exactly-once)
- Hung handler → lock-expiry → reclaim
- Migrations up + down

## Deployment

### Railway

The repo includes `railway.json`. Workflow:

1. Create a Railway project, attach the Postgres plugin (it injects
   `DATABASE_URL` automatically).
2. Connect this repo. Railway uses the Dockerfile.
3. The default startCommand is `tinybus worker --queue=default`. The
   worker listens on `$PORT` for `/healthz`, which Railway probes.
4. For schema migrations on first deploy, set the
   `RAILWAY_RUN_UID` predeploy command to `/tinybus migrate up` — or
   run it manually once via `railway run -- /tinybus migrate up`.

For multi-service setups (separate worker and producer services),
duplicate the service in Railway and override `startCommand` with the
desired subcommand.

### Docker (any platform)

```bash
docker build -t tinybus:dev .
docker run --rm -e DATABASE_URL=$DATABASE_URL tinybus:dev migrate up
docker run --rm -e DATABASE_URL=$DATABASE_URL tinybus:dev worker --queue=default
```

## Layout

```
tinybus/
├── cmd/tinybus/                 # CLI entrypoint, dispatches to internal/cli
├── internal/cli/                # subcommand implementations
├── pkg/tinybus/                 # public library
│   ├── tinybus.go               # Queue, Job, Stats, Handler, New, Close
│   ├── options.go               # functional options
│   ├── errors.go                # sentinel errors
│   ├── enqueue.go               # INSERT … RETURNING id
│   ├── claim.go                 # CTE + FOR UPDATE SKIP LOCKED + UPDATE
│   ├── process.go               # worker loop, sweeper, retry/dead
│   ├── recover.go               # lease-expiry sweeper
│   ├── backoff.go               # equal-jitter exponential
│   ├── stats.go                 # GROUP BY with FILTER
│   ├── migrate.go               # embed.FS + ledger-tracked runner
│   └── migrations/              # *.up.sql / *.down.sql
├── Dockerfile                   # multi-stage → distroless:nonroot
├── docker-compose.yml           # postgres + migrate + worker + producer
├── railway.json                 # Railway deploy config
└── Makefile
```

## Dependencies

- `github.com/jackc/pgx/v5` — Postgres driver

That's it. Test-only:

- `github.com/testcontainers/testcontainers-go` — gated behind `//go:build integration`

Everything else is the standard library — `net/http`, `log/slog`,
`embed`, `context`, `sync`, `math/rand/v2`.

## Deliberate non-goals

- **No admin web UI.** The CLI + JSON `/stats` are enough.
- **No multi-tenant isolation.** One ledger, one namespace.
- **No cron / scheduled jobs.** `RunAt` lets you schedule one-off
  delays; recurring schedules are a separate problem.
- **No job priorities.** A `priority` column would be a one-line
  schema change but isn't in v1.
- **No worker pool autoscaling.** Run more workers, or set
  `--concurrency` higher.
- **No tracing.** Could pair with an OTel exporter later — `slog` is
  the only observability layer in v1.
