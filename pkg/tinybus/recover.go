package tinybus

import (
	"context"
	"fmt"
)

// reclaimExpired releases jobs whose locks are older than leaseDuration,
// making them eligible for re-claim by another worker.
//
// Strategy: lock-expiry (lease).
// One sweep per pollInterval, no per-job heartbeat goroutines. Any job
// whose handler runs longer than the lease will be re-run by another
// worker — i.e. tinybus is at-least-once, not exactly-once. Set
// leaseDuration above your p99 handler runtime.
//
// Why not heartbeat: heartbeating from inside the handler conflates "is
// the worker alive?" with "is the handler making progress?", and adds a
// connection per in-flight job. Lease-only keeps the worker side simple
// and pushes the responsibility onto handler authors to either finish
// within the lease or split work into smaller jobs. River (Go) and Oban
// (Elixir) both make this choice.
//
// Returns the number of jobs reclaimed.
func (q *Queue) reclaimExpired(ctx context.Context, queue string) (int64, error) {
	const sql = `
		UPDATE jobs
		SET    locked_at  = NULL,
		       locked_by  = NULL,
		       last_error = COALESCE(NULLIF(last_error, ''), '') ||
		                    CASE WHEN last_error IS NOT NULL AND last_error <> '' THEN E'\n' ELSE '' END ||
		                    'lease expired at ' || now()::text
		WHERE  queue      = $1
		  AND  locked_at IS NOT NULL
		  AND  dead_at   IS NULL
		  AND  locked_at  < now() - make_interval(secs => $2)
	`
	cmd, err := q.pool.Exec(ctx, sql, queue, q.leaseDuration.Seconds())
	if err != nil {
		return 0, fmt.Errorf("tinybus: reclaimExpired: %w", err)
	}
	return cmd.RowsAffected(), nil
}
