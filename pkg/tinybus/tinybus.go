package tinybus

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Default knobs. Tunable via Option.
const (
	defaultPollInterval   = 1 * time.Second
	defaultLeaseDuration  = 5 * time.Minute
	defaultMaxConcurrency = 1
	defaultMaxAttempts    = 5
)

// Job is the unit of work delivered to a Handler. Fields are read-only
// from the handler's perspective; mutating them has no effect on the
// queue.
type Job struct {
	ID          int64     // database row id
	Queue       string    // queue name
	Payload     []byte    // opaque, set by the producer
	Attempts    int       // claim count; >= 1 inside the handler
	MaxAttempts int       // total attempts allowed before the job is moved to dead state
	CreatedAt   time.Time // when Enqueue inserted the row
	RunAt       time.Time // when the job became eligible
}

// Handler is invoked once per claimed job. Returning nil marks the job
// as completed (the row is deleted). Returning a non-nil error causes
// the job to be retried (with backoff) until max_attempts is reached,
// after which the job is moved to the dead state.
//
// Handlers should respect ctx: it is cancelled if the Queue is closed
// or if the lease is about to expire.
type Handler func(ctx context.Context, job Job) error

// Stats reports the row counts in each state for a single queue.
// Returned by Queue.Stats.
type Stats struct {
	Queue    string `json:"queue"`
	Ready    int64  `json:"ready"`     // locked_at IS NULL AND dead_at IS NULL AND run_at <= now()
	Delayed  int64  `json:"delayed"`   // locked_at IS NULL AND dead_at IS NULL AND run_at >  now()
	InFlight int64  `json:"in_flight"` // locked_at IS NOT NULL AND dead_at IS NULL
	Dead     int64  `json:"dead"`      // dead_at IS NOT NULL
}

// Queue is a tinybus client. A single Queue value is safe for concurrent
// use; share one across producers and Process calls.
type Queue struct {
	pool          *pgxpool.Pool
	ownsPool      bool
	log           *slog.Logger
	workerID      string
	pollInterval  time.Duration
	leaseDuration time.Duration
	concurrency   int

	closed atomic.Bool
}

// New constructs a Queue. Either WithDSN or WithPool must be supplied.
// All other options have sensible defaults.
//
// The returned Queue holds a connection pool; call Close when done
// (Close is a no-op for pools supplied via WithPool).
func New(ctx context.Context, opts ...Option) (*Queue, error) {
	c := config{
		log:            slog.Default(),
		pollInterval:   defaultPollInterval,
		leaseDuration:  defaultLeaseDuration,
		maxConcurrency: defaultMaxConcurrency,
	}
	for _, opt := range opts {
		opt(&c)
	}

	q := &Queue{
		log:           c.log,
		workerID:      c.workerID,
		pollInterval:  c.pollInterval,
		leaseDuration: c.leaseDuration,
		concurrency:   c.maxConcurrency,
	}
	if q.workerID == "" {
		q.workerID = defaultWorkerID()
	}

	switch {
	case c.pool != nil:
		q.pool = c.pool
		q.ownsPool = false
	case c.dsn != "":
		pool, err := pgxpool.New(ctx, c.dsn)
		if err != nil {
			return nil, fmt.Errorf("tinybus: open pool: %w", err)
		}
		q.pool = pool
		q.ownsPool = true
	default:
		return nil, fmt.Errorf("tinybus: New: WithDSN or WithPool is required")
	}

	if err := q.pool.Ping(ctx); err != nil {
		if q.ownsPool {
			q.pool.Close()
		}
		return nil, fmt.Errorf("tinybus: ping: %w", err)
	}
	return q, nil
}

// Close releases resources held by the Queue. Safe to call multiple
// times; subsequent calls are no-ops. Close does not wait for in-flight
// Process loops to finish — cancel their context first.
func (q *Queue) Close() {
	if !q.closed.CompareAndSwap(false, true) {
		return
	}
	if q.ownsPool && q.pool != nil {
		q.pool.Close()
	}
}

// Enqueue inserts a new job into the named queue and returns its id.
// Payload is opaque to tinybus.
func (q *Queue) Enqueue(ctx context.Context, queue string, payload []byte, opts ...EnqueueOption) (int64, error) {
	if q.closed.Load() {
		return 0, ErrClosed
	}
	if err := validateQueueName(queue); err != nil {
		return 0, err
	}
	p := enqueueParams{
		runAt:       time.Now(),
		maxAttempts: defaultMaxAttempts,
	}
	for _, opt := range opts {
		opt(&p)
	}
	return q.enqueue(ctx, queue, payload, p)
}

// Process runs handler against jobs from the named queue until ctx is
// cancelled. It claims one job at a time per worker (concurrency is
// configurable via WithConcurrency). Process returns the first
// non-recoverable error it encounters, or ctx.Err() on cancellation.
func (q *Queue) Process(ctx context.Context, queue string, handler Handler) error {
	if q.closed.Load() {
		return ErrClosed
	}
	if err := validateQueueName(queue); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("tinybus: Process: handler must not be nil")
	}
	return q.runWorkers(ctx, queue, handler)
}

// Stats returns one entry per known queue. Cheap: a single GROUP BY
// over the partial indexes.
func (q *Queue) Stats(ctx context.Context) ([]Stats, error) {
	if q.closed.Load() {
		return nil, ErrClosed
	}
	return q.stats(ctx)
}

// validateQueueName rejects empty names. Postgres TEXT can hold
// anything else, but we keep this point of control so we can tighten
// later (e.g. enforce ASCII or a length cap).
func validateQueueName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: name is empty", ErrInvalidQueue)
	}
	return nil
}

// defaultWorkerID returns "<host>-<pid>" when hostname is available,
// otherwise "tinybus-<pid>".
func defaultWorkerID() string {
	pid := os.Getpid()
	host, err := os.Hostname()
	if err != nil || host == "" {
		return fmt.Sprintf("tinybus-%d", pid)
	}
	return fmt.Sprintf("%s-%d", host, pid)
}
