package tinybus

import (
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Option configures a Queue at construction time. Pass any combination
// to New. Later options override earlier ones for the same setting.
type Option func(*config)

type config struct {
	dsn            string
	pool           *pgxpool.Pool
	log            *slog.Logger
	workerID       string
	pollInterval   time.Duration
	leaseDuration  time.Duration
	maxConcurrency int
}

// WithDSN sets the Postgres DSN used to open a new connection pool.
// Mutually exclusive with WithPool; if both are supplied, the pool wins.
func WithDSN(dsn string) Option {
	return func(c *config) { c.dsn = dsn }
}

// WithPool injects an existing pgx pool. Useful for tests and for sharing
// a pool with the rest of an application. The Queue does not take
// ownership: Close will not close a pool supplied this way.
func WithPool(pool *pgxpool.Pool) Option {
	return func(c *config) { c.pool = pool }
}

// WithLogger sets the slog.Logger used for internal logs (job claimed,
// job failed, slow client evicted, etc.). Default: slog.Default().
func WithLogger(log *slog.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithWorkerID sets the identifier written to jobs.locked_by. Useful for
// distinguishing workers in logs and dashboards. Default: a generated
// "<host>-<pid>-<rand>" string.
func WithWorkerID(id string) Option {
	return func(c *config) { c.workerID = id }
}

// WithPollInterval sets the sleep duration between empty fetches. Lower
// values reduce job pickup latency; higher values reduce idle DB load.
// Default: 1s.
func WithPollInterval(d time.Duration) Option {
	return func(c *config) { c.pollInterval = d }
}

// WithLeaseDuration sets how long a claim is considered valid before the
// crash-recovery sweeper reclaims it. A job whose handler runs longer
// than the lease will be re-run by another worker, so set this above the
// p99 handler runtime. Default: 5m.
func WithLeaseDuration(d time.Duration) Option {
	return func(c *config) { c.leaseDuration = d }
}

// WithConcurrency sets the number of in-flight jobs a single Process
// call will run in parallel. Default: 1.
func WithConcurrency(n int) Option {
	return func(c *config) { c.maxConcurrency = n }
}

// EnqueueOption customizes a single Enqueue call.
type EnqueueOption func(*enqueueParams)

type enqueueParams struct {
	runAt       time.Time
	maxAttempts int
}

// RunAt schedules the job to become eligible at the given absolute time.
// Earlier times are clamped to "immediately." Mutually exclusive with
// RunIn (last one wins).
func RunAt(t time.Time) EnqueueOption {
	return func(p *enqueueParams) { p.runAt = t }
}

// RunIn schedules the job to become eligible after the given delay.
// Equivalent to RunAt(time.Now().Add(d)).
func RunIn(d time.Duration) EnqueueOption {
	return func(p *enqueueParams) { p.runAt = time.Now().Add(d) }
}

// MaxAttempts overrides the default max_attempts for this job.
// Must be >= 1.
func MaxAttempts(n int) EnqueueOption {
	return func(p *enqueueParams) { p.maxAttempts = n }
}
