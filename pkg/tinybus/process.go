package tinybus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// runWorkers spawns q.concurrency worker goroutines plus one sweeper
// goroutine. It blocks until ctx is cancelled.
func (q *Queue) runWorkers(ctx context.Context, queue string, handler Handler) error {
	if q.concurrency < 1 {
		return fmt.Errorf("tinybus: Process: concurrency must be >= 1, got %d", q.concurrency)
	}

	var wg sync.WaitGroup

	// Sweeper: reclaim expired locks for this queue every pollInterval.
	// Co-locating it with the worker(s) means a single Process call has
	// no external dependency on a separate sweeper process.
	wg.Add(1)
	go func() {
		defer wg.Done()
		q.runSweeper(ctx, queue)
	}()

	for i := 0; i < q.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.runWorker(ctx, queue, handler)
		}()
	}

	wg.Wait()
	return ctx.Err()
}

// runSweeper periodically reclaims expired locks. It runs at half the
// lease duration so that any expired job is reclaimed within a lease.
func (q *Queue) runSweeper(ctx context.Context, queue string) {
	interval := q.leaseDuration / 2
	if interval < q.pollInterval {
		interval = q.pollInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := q.reclaimExpired(ctx, queue)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				q.log.Warn("tinybus: sweeper failed", slog.String("err", err.Error()), slog.String("queue", queue))
				continue
			}
			if n > 0 {
				q.log.Info("tinybus: reclaimed expired locks",
					slog.Int64("count", n), slog.String("queue", queue))
			}
		}
	}
}

// runWorker is one worker's main loop: claim, run, complete or retry.
func (q *Queue) runWorker(ctx context.Context, queue string, handler Handler) {
	for ctx.Err() == nil {
		job, err := q.claimNext(ctx, queue)
		switch {
		case errors.Is(err, ErrNoJobs):
			q.sleep(ctx, q.pollInterval)
			continue
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return
		case err != nil:
			q.log.Error("tinybus: claim failed",
				slog.String("err", err.Error()), slog.String("queue", queue))
			q.sleep(ctx, q.pollInterval)
			continue
		}
		q.runJob(ctx, job, handler)
	}
}

// runJob invokes the handler under a lease-bounded context, then either
// completes or schedules a retry.
func (q *Queue) runJob(ctx context.Context, job Job, handler Handler) {
	jobCtx, cancel := context.WithTimeout(ctx, q.leaseDuration)
	defer cancel()

	q.log.Debug("tinybus: claimed",
		slog.Int64("id", job.ID),
		slog.String("queue", job.Queue),
		slog.Int("attempts", job.Attempts),
		slog.Int("max_attempts", job.MaxAttempts),
	)

	handlerErr := safeInvoke(jobCtx, handler, job)
	if handlerErr == nil {
		if err := q.complete(ctx, job.ID); err != nil {
			q.log.Error("tinybus: complete failed",
				slog.Int64("id", job.ID), slog.String("err", err.Error()))
		}
		return
	}

	q.log.Warn("tinybus: handler returned error",
		slog.Int64("id", job.ID),
		slog.Int("attempts", job.Attempts),
		slog.String("err", handlerErr.Error()),
	)
	if err := q.retryOrDead(ctx, job, handlerErr); err != nil {
		q.log.Error("tinybus: retry/dead failed",
			slog.Int64("id", job.ID), slog.String("err", err.Error()))
	}
}

// safeInvoke runs the handler, recovering from panics so a misbehaving
// handler doesn't take down the worker.
func safeInvoke(ctx context.Context, handler Handler, job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("tinybus: handler panic: %v", r)
		}
	}()
	return handler(ctx, job)
}

// complete deletes the row. v1 trade-off: no audit trail of completed
// jobs. Add a `completed_at` column and switch to UPDATE if you want
// retention.
func (q *Queue) complete(ctx context.Context, id int64) error {
	const sql = `DELETE FROM jobs WHERE id = $1`
	if _, err := q.pool.Exec(ctx, sql, id); err != nil {
		return fmt.Errorf("tinybus: complete: %w", err)
	}
	return nil
}

// retryOrDead either schedules a retry (with backoff) or marks the job
// dead, based on attempts vs max_attempts.
func (q *Queue) retryOrDead(ctx context.Context, job Job, handlerErr error) error {
	if job.Attempts >= job.MaxAttempts {
		const dead = `UPDATE jobs SET dead_at = now(), last_error = $2 WHERE id = $1`
		if _, err := q.pool.Exec(ctx, dead, job.ID, handlerErr.Error()); err != nil {
			return fmt.Errorf("tinybus: mark dead: %w", err)
		}
		q.log.Warn("tinybus: job moved to dead state",
			slog.Int64("id", job.ID),
			slog.Int("attempts", job.Attempts),
			slog.Int("max_attempts", job.MaxAttempts),
		)
		return nil
	}
	delay := backoff(job.Attempts)
	const retry = `
		UPDATE jobs
		SET    locked_at  = NULL,
		       locked_by  = NULL,
		       run_at     = $2,
		       last_error = $3
		WHERE  id = $1
	`
	if _, err := q.pool.Exec(ctx, retry, job.ID, time.Now().Add(delay), handlerErr.Error()); err != nil {
		return fmt.Errorf("tinybus: schedule retry: %w", err)
	}
	q.log.Info("tinybus: scheduled retry",
		slog.Int64("id", job.ID),
		slog.Int("attempts", job.Attempts),
		slog.Duration("delay", delay),
	)
	return nil
}

// sleep is a context-aware sleep used between poll attempts.
func (q *Queue) sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
