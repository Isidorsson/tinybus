package tinybus

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// claimNext atomically claims the next eligible job in the named queue
// for this worker, returning ErrNoJobs if the queue is empty.
//
// The CTE+UPDATE+RETURNING pattern is the heart of tinybus:
//
//   - FOR UPDATE SKIP LOCKED gives us "exactly one worker claims a row"
//     without serializing all workers behind a global lock.
//   - The CTE-then-UPDATE in a single statement closes the window between
//     "I see this row is free" and "I claim it." A SELECT-then-UPDATE
//     split lets two workers see the same row.
//   - attempts is incremented at *claim time*, not failure time. A worker
//     crash after claim still counts as an attempt — so a poisoned job
//     that crashes its worker still hits max_attempts and ends up dead,
//     instead of running forever.
func (q *Queue) claimNext(ctx context.Context, queue string) (Job, error) {
	const sql = `
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
		RETURNING id, queue, payload, attempts, max_attempts, created_at, run_at
	`
	var j Job
	err := q.pool.QueryRow(ctx, sql, queue, q.workerID).Scan(
		&j.ID, &j.Queue, &j.Payload, &j.Attempts, &j.MaxAttempts, &j.CreatedAt, &j.RunAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoJobs
	}
	if err != nil {
		return Job{}, fmt.Errorf("tinybus: claimNext: %w", err)
	}
	return j, nil
}
