-- 0001_init.up.sql
-- Schema for tinybus. One table, three indexes, no enums, no triggers.
--
-- Design choices (trade-offs documented in README):
--   1. payload is BYTEA, not JSONB. Opaque to the queue, faster to insert,
--      no parse cost. Producers serialize whatever they want (JSON,
--      msgpack, protobuf). Switch to JSONB if you need queryable payloads.
--   2. State is implicit, derived from nullable timestamps:
--        ready     -> locked_at IS NULL AND dead_at IS NULL AND run_at <= now()
--        in_flight -> locked_at IS NOT NULL AND dead_at IS NULL
--        dead      -> dead_at IS NOT NULL
--        completed -> row deleted (no audit trail in v1; trade-off: smaller
--                     table, cheaper inserts, no "list completed jobs")
--      Adding states later doesn't require ALTER TYPE.
--   3. id is BIGSERIAL. Compact in indexes, monotonic, no UUID generation
--      cost. Producers don't need the id to route work — handlers receive
--      the payload, not the id, in normal operation.
--   4. The fetch query relies on idx_jobs_ready (partial). It is the single
--      most important object in this file; without the partial WHERE clause
--      the planner has to walk locked / dead rows on every claim.

CREATE TABLE IF NOT EXISTS jobs (
    id            BIGSERIAL PRIMARY KEY,
    queue         TEXT        NOT NULL,
    payload       BYTEA       NOT NULL,
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 5
                                CHECK (max_attempts >= 1),
    last_error    TEXT,                       -- NULL until a failure occurs
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at     TIMESTAMPTZ,                -- NULL = ready, set on claim
    locked_by     TEXT,                       -- worker id; useful for debug + heartbeat
    dead_at       TIMESTAMPTZ                 -- NULL = alive, set on terminal failure
);

COMMENT ON TABLE  jobs              IS 'tinybus job queue (single-table design; states are implicit in nullable timestamps).';
COMMENT ON COLUMN jobs.queue        IS 'Logical queue name; workers subscribe to one or more queues.';
COMMENT ON COLUMN jobs.payload      IS 'Opaque bytes. tinybus does not interpret the payload.';
COMMENT ON COLUMN jobs.run_at       IS 'Earliest time the job is eligible to run. Set in the future for delayed jobs.';
COMMENT ON COLUMN jobs.locked_at    IS 'Set by the claim query; cleared on retry; remains set after dead_at to preserve the locking worker for forensics.';
COMMENT ON COLUMN jobs.dead_at      IS 'Set when attempts >= max_attempts; row is retained for inspection.';

-- The fetch index. Partial: only contains rows the claim query will
-- consider. Order matters: queue first (selectivity), then run_at
-- (the ORDER BY in claimNext).
CREATE INDEX IF NOT EXISTS idx_jobs_ready
    ON jobs (queue, run_at)
    WHERE locked_at IS NULL AND dead_at IS NULL;

-- Dead-letter inspection. Tiny table in steady state, but small partial
-- index keeps "list dead jobs in queue X" fast.
CREATE INDEX IF NOT EXISTS idx_jobs_dead
    ON jobs (queue, dead_at DESC)
    WHERE dead_at IS NOT NULL;

-- Crash-recovery scan (used only by the lock-expiry / heartbeat sweeper,
-- depending on which strategy is chosen in step 5).
CREATE INDEX IF NOT EXISTS idx_jobs_in_flight
    ON jobs (locked_at)
    WHERE locked_at IS NOT NULL AND dead_at IS NULL;
