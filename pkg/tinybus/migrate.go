package tinybus

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Direction selects the migration direction.
type Direction int

const (
	// Up applies all pending migrations in ascending order.
	Up Direction = iota
	// Down rolls back all applied migrations in descending order.
	Down
)

// Migrate applies the embedded SQL migrations to the database via pool.
//
// Migrations live in pkg/tinybus/migrations/ and are tracked in a
// tinybus_migrations ledger table created on first run. Each migration
// runs inside its own transaction; a failed migration leaves the ledger
// untouched, so the run is safely retryable.
func Migrate(ctx context.Context, pool *pgxpool.Pool, dir Direction) error {
	if err := ensureLedger(ctx, pool); err != nil {
		return err
	}
	migs, err := discoverMigrations(dir)
	if err != nil {
		return err
	}
	applied, err := loadApplied(ctx, pool)
	if err != nil {
		return err
	}
	for _, m := range migs {
		_, isApplied := applied[m.version]
		if dir == Up && isApplied {
			continue
		}
		if dir == Down && !isApplied {
			continue
		}
		if err := applyOne(ctx, pool, m, dir); err != nil {
			return fmt.Errorf("tinybus: migrate %s: %w", m.file, err)
		}
	}
	return nil
}

func ensureLedger(ctx context.Context, pool *pgxpool.Pool) error {
	const sql = `
		CREATE TABLE IF NOT EXISTS tinybus_migrations (
			version    TEXT        PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("tinybus: create ledger: %w", err)
	}
	return nil
}

type migration struct {
	version string
	file    string
}

func discoverMigrations(dir Direction) ([]migration, error) {
	suffix := ".up.sql"
	if dir == Down {
		suffix = ".down.sql"
	}
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("tinybus: read migrations dir: %w", err)
	}
	var migs []migration
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		migs = append(migs, migration{
			version: strings.TrimSuffix(name, suffix),
			file:    name,
		})
	}
	sort.Slice(migs, func(i, j int) bool {
		if dir == Down {
			return migs[i].version > migs[j].version
		}
		return migs[i].version < migs[j].version
	})
	return migs, nil
}

func loadApplied(ctx context.Context, pool *pgxpool.Pool) (map[string]struct{}, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM tinybus_migrations`)
	if err != nil {
		return nil, fmt.Errorf("tinybus: read ledger: %w", err)
	}
	defer rows.Close()
	applied := make(map[string]struct{})
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("tinybus: scan ledger: %w", err)
		}
		applied[v] = struct{}{}
	}
	return applied, rows.Err()
}

func applyOne(ctx context.Context, pool *pgxpool.Pool, m migration, dir Direction) error {
	body, err := fs.ReadFile(migrationsFS, "migrations/"+m.file)
	if err != nil {
		return fmt.Errorf("read %s: %w", m.file, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, string(body)); err != nil {
		return fmt.Errorf("apply %s: %w", m.file, err)
	}
	if dir == Up {
		if _, err := tx.Exec(ctx, `INSERT INTO tinybus_migrations (version) VALUES ($1)`, m.version); err != nil {
			return fmt.Errorf("record applied: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `DELETE FROM tinybus_migrations WHERE version = $1`, m.version); err != nil {
			return fmt.Errorf("remove applied: %w", err)
		}
	}
	return tx.Commit(ctx)
}
