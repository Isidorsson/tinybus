//go:build integration

package tinybus_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Isidorsson/tinybus/pkg/tinybus"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setup spins up Postgres in a container, applies migrations, and
// returns a Queue plus a cleanup func.
func setup(t *testing.T, opts ...tinybus.Option) (*tinybus.Queue, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("tinybus"),
		postgres.WithUsername("tinybus"),
		postgres.WithPassword("tinybus"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := tinybus.Migrate(ctx, pool, tinybus.Up); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	defaults := []tinybus.Option{tinybus.WithPool(pool)}
	q, err := tinybus.New(ctx, append(defaults, opts...)...)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	t.Cleanup(q.Close)
	return q, pool
}

func TestEnqueueClaimComplete(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	q, _ := setup(t)
	id, err := q.Enqueue(ctx, "qa", []byte(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id == 0 {
		t.Fatalf("expected non-zero id")
	}

	var got atomic.Int64
	procCtx, procCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = q.Process(procCtx, "qa", func(_ context.Context, job tinybus.Job) error {
			got.Store(job.ID)
			procCancel()
			return nil
		})
	}()

	<-done
	if got.Load() != id {
		t.Fatalf("expected job %d to run, got %d", id, got.Load())
	}
}

func TestRetryThenDead(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	q, pool := setup(t,
		tinybus.WithPollInterval(50*time.Millisecond),
		tinybus.WithLeaseDuration(2*time.Second),
	)

	id, err := q.Enqueue(ctx, "fails", []byte("x"), tinybus.MaxAttempts(2))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var attempts atomic.Int32
	procCtx, procCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = q.Process(procCtx, "fails", func(_ context.Context, job tinybus.Job) error {
			attempts.Add(1)
			return errors.New("nope")
		})
	}()

	deadline := time.After(40 * time.Second)
	for {
		select {
		case <-deadline:
			procCancel()
			<-done
			t.Fatalf("job %d never reached dead state (attempts=%d)", id, attempts.Load())
		case <-time.After(200 * time.Millisecond):
		}
		var deadAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT dead_at FROM jobs WHERE id=$1`, id).Scan(&deadAt); err != nil {
			procCancel()
			<-done
			t.Fatalf("read dead_at: %v", err)
		}
		if deadAt != nil {
			break
		}
	}
	procCancel()
	<-done

	if a := attempts.Load(); a != 2 {
		t.Fatalf("expected 2 attempts before dead, got %d", a)
	}
}

func TestNoDoubleProcessing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	q, _ := setup(t,
		tinybus.WithPollInterval(20*time.Millisecond),
		tinybus.WithConcurrency(4),
	)

	const N = 50
	for i := 0; i < N; i++ {
		if _, err := q.Enqueue(ctx, "race", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var (
		mu  sync.Mutex
		run = make(map[int64]int) // id -> times run
	)
	var processed atomic.Int32
	procCtx, procCancel := context.WithCancel(ctx)
	defer procCancel()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Process(procCtx, "race", func(_ context.Context, job tinybus.Job) error {
				mu.Lock()
				run[job.ID]++
				mu.Unlock()
				if processed.Add(1) >= int32(N) {
					procCancel()
				}
				return nil
			})
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(run) != N {
		t.Fatalf("expected %d distinct jobs run, got %d", N, len(run))
	}
	for id, n := range run {
		if n != 1 {
			t.Fatalf("job %d ran %d times, expected 1", id, n)
		}
	}
}

func TestLeaseExpiryReclaim(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	q, pool := setup(t, tinybus.WithLeaseDuration(500*time.Millisecond))

	id, err := q.Enqueue(ctx, "lease", []byte("x"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Manually claim by running a worker for one iteration (the simplest
	// way to acquire the lock without coding around private API). We use
	// a handler that blocks past the lease, then never returns until ctx
	// is cancelled — simulating a hung worker.
	procCtx, procCancel := context.WithCancel(ctx)
	hung := make(chan struct{})
	go func() {
		_ = q.Process(procCtx, "lease", func(jobCtx context.Context, _ tinybus.Job) error {
			close(hung)
			<-jobCtx.Done() // block until lease ctx fires
			return jobCtx.Err()
		})
	}()
	select {
	case <-hung:
	case <-time.After(5 * time.Second):
		procCancel()
		t.Fatalf("worker never claimed the job")
	}

	// Sweep should reclaim once the lease elapses. The worker's own
	// sweeper runs every leaseDuration/2, so wait > 1.5x lease.
	deadline := time.After(10 * time.Second)
	for {
		var lockedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT locked_at FROM jobs WHERE id=$1`, id).Scan(&lockedAt); err != nil {
			procCancel()
			t.Fatalf("read locked_at: %v", err)
		}
		if lockedAt == nil {
			break
		}
		select {
		case <-deadline:
			procCancel()
			t.Fatalf("lock never reclaimed (locked_at still set)")
		case <-time.After(200 * time.Millisecond):
		}
	}
	procCancel()
}

func TestMigrationsUpDown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, pool := setup(t)

	// jobs table should exist after setup (which ran Up)
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'jobs'`,
	).Scan(&n); err != nil {
		t.Fatalf("query jobs table: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected jobs table after Up, got %d", n)
	}

	if err := tinybus.Migrate(ctx, pool, tinybus.Down); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'jobs'`,
	).Scan(&n); err != nil {
		t.Fatalf("query jobs table after down: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected jobs table dropped after Down, got %d", n)
	}
}
