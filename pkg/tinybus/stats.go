package tinybus

import (
	"context"
	"fmt"
)

// stats returns per-queue row counts. Implementation lands in step 7;
// the current stub returns an empty slice so callers can be wired now.
func (q *Queue) stats(ctx context.Context) ([]Stats, error) {
	const sql = `
		SELECT
		    queue,
		    COUNT(*) FILTER (WHERE locked_at IS NULL AND dead_at IS NULL AND run_at <= now()) AS ready,
		    COUNT(*) FILTER (WHERE locked_at IS NULL AND dead_at IS NULL AND run_at >  now()) AS delayed,
		    COUNT(*) FILTER (WHERE locked_at IS NOT NULL AND dead_at IS NULL)                  AS in_flight,
		    COUNT(*) FILTER (WHERE dead_at IS NOT NULL)                                        AS dead
		FROM jobs
		GROUP BY queue
		ORDER BY queue
	`
	rows, err := q.pool.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("tinybus: stats: %w", err)
	}
	defer rows.Close()

	var out []Stats
	for rows.Next() {
		var s Stats
		if err := rows.Scan(&s.Queue, &s.Ready, &s.Delayed, &s.InFlight, &s.Dead); err != nil {
			return nil, fmt.Errorf("tinybus: stats scan: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tinybus: stats rows: %w", err)
	}
	return out, nil
}
