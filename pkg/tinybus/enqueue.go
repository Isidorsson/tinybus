package tinybus

import (
	"context"
	"fmt"
	"log/slog"
)

// enqueue is the implementation half of Enqueue. It assumes its inputs
// have already been validated.
func (q *Queue) enqueue(ctx context.Context, queue string, payload []byte, p enqueueParams) (int64, error) {
	const sql = `
		INSERT INTO jobs (queue, payload, run_at, max_attempts)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`
	var id int64
	if err := q.pool.QueryRow(ctx, sql, queue, payload, p.runAt, p.maxAttempts).Scan(&id); err != nil {
		return 0, fmt.Errorf("tinybus: enqueue: %w", err)
	}
	q.log.Debug("tinybus: enqueued",
		slog.Group("job",
			slog.Int64("id", id),
			slog.String("queue", queue),
			slog.Time("run_at", p.runAt),
		),
	)
	return id, nil
}
