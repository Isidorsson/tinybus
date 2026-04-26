// Package cli holds the tinybus CLI subcommands. Each subcommand is a
// standalone function callable from cmd/tinybus.
package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SignalContext returns a context that cancels on SIGINT/SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// OpenPool returns a pgxpool.Pool from the DATABASE_URL env var.
func OpenPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// Logger returns a JSON slog.Logger writing to stderr.
func Logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// Die prints msg + err to stderr and exits with status 1.
func Die(msg string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "tinybus: %s: %v\n", msg, err)
	} else {
		fmt.Fprintf(os.Stderr, "tinybus: %s\n", msg)
	}
	os.Exit(1)
}
