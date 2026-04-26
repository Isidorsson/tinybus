package tinybus

import (
	"context"
	"fmt"
)

// ClearDead deletes all dead jobs in the named queue, or in every queue
// if queue is the empty string. Returns the number of rows deleted.
//
// Intended for operational tooling and dashboards. tinybus's own
// correctness does not depend on dead jobs being cleared — they are
// retained until something explicitly removes them.
func (q *Queue) ClearDead(ctx context.Context, queue string) (int64, error) {
	if q.closed.Load() {
		return 0, ErrClosed
	}
	const sqlAll = `DELETE FROM jobs WHERE dead_at IS NOT NULL`
	const sqlQ   = `DELETE FROM jobs WHERE dead_at IS NOT NULL AND queue = $1`

	if queue == "" {
		cmd, err := q.pool.Exec(ctx, sqlAll)
		if err != nil {
			return 0, fmt.Errorf("tinybus: ClearDead: %w", err)
		}
		return cmd.RowsAffected(), nil
	}
	cmd, err := q.pool.Exec(ctx, sqlQ, queue)
	if err != nil {
		return 0, fmt.Errorf("tinybus: ClearDead(%q): %w", queue, err)
	}
	return cmd.RowsAffected(), nil
}
