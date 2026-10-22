// Package migrate owns the sql transport's Postgres schema. Migrations are
// embedded SQL files named NNNN_description.sql, applied in version order and
// recorded in the async_schema_migrations table. Run applies pending
// migrations; Check verifies a database is at the version the binary expects.
package migrate

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgconn"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	versionTable = "async_schema_migrations"

	// lockKey is the pg_advisory_xact_lock key that serializes concurrent runs.
	lockKey int64 = 0x6173796e6371 // "asyncq"

	createVersionTable = `CREATE TABLE IF NOT EXISTS ` + versionTable + ` (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`
	selectCurrentVersion = `SELECT COALESCE(MAX(version), 0) FROM ` + versionTable
	insertVersion        = `INSERT INTO ` + versionTable + ` (version, name) VALUES ($1, $2)`

	sqlstateUndefinedTable = "42P01"
)

var (
	// ErrSchemaMissing means the database has never been migrated.
	ErrSchemaMissing = errors.New("sql transport schema not initialized: run llm-d-async migrate")
	// ErrSchemaBehind means the database is at an older version than the binary expects.
	ErrSchemaBehind = errors.New("sql transport schema is behind: run llm-d-async migrate")
)

var fileNamePattern = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// Migration is one embedded SQL file.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// Load parses the embedded migrations in version order. Versions must start
// at 1 and be contiguous.
func Load() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		m := fileNamePattern.FindStringSubmatch(entry.Name())
		if m == nil {
			return nil, fmt.Errorf("migration %q does not match NNNN_name.sql", entry.Name())
		}
		version, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", entry.Name(), err)
		}
		body, err := fs.ReadFile(migrationFiles, "migrations/"+entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		migrations = append(migrations, Migration{Version: version, Name: m[2], SQL: string(body)})
	}

	for i, m := range migrations {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migration versions must be contiguous from 1, got %04d_%s at position %d", m.Version, m.Name, i+1)
		}
	}
	return migrations, nil
}

// LatestVersion is the version a binary built from this tree expects.
func LatestVersion() (int, error) {
	migrations, err := Load()
	if err != nil {
		return 0, err
	}
	return len(migrations), nil
}

// Run applies all pending migrations in a single transaction under an
// advisory lock, so concurrent callers serialize and at most one of them
// executes DDL. It returns the migrations it applied.
func Run(ctx context.Context, db *sql.DB) ([]Migration, error) {
	migrations, err := Load()
	if err != nil {
		return nil, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err := tx.ExecContext(ctx, createVersionTable); err != nil {
		return nil, fmt.Errorf("create %s: %w", versionTable, err)
	}

	var current int
	if err := tx.QueryRowContext(ctx, selectCurrentVersion).Scan(&current); err != nil {
		return nil, fmt.Errorf("read schema version: %w", err)
	}

	var applied []Migration
	for _, m := range migrations {
		if m.Version <= current {
			continue
		}
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			return nil, fmt.Errorf("apply migration %04d_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.ExecContext(ctx, insertVersion, m.Version, m.Name); err != nil {
			return nil, fmt.Errorf("record migration %04d_%s: %w", m.Version, m.Name, err)
		}
		applied = append(applied, m)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return applied, nil
}

// Check verifies the database schema is at least the version this binary
// expects. A missing version table returns ErrSchemaMissing and an older
// version returns ErrSchemaBehind. A newer version is logged and accepted so
// that binaries can be rolled back after a migration.
func Check(ctx context.Context, db *sql.DB) error {
	expected, err := LatestVersion()
	if err != nil {
		return err
	}

	var current int
	if err := db.QueryRowContext(ctx, selectCurrentVersion).Scan(&current); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == sqlstateUndefinedTable {
			return ErrSchemaMissing
		}
		return fmt.Errorf("read schema version: %w", err)
	}

	switch {
	case current < expected:
		return fmt.Errorf("%w: database at version %d, binary expects %d", ErrSchemaBehind, current, expected)
	case current > expected:
		logr.FromContextOrDiscard(ctx).Info("sql transport schema is newer than this binary expects",
			"databaseVersion", current, "expectedVersion", expected)
	}
	return nil
}
