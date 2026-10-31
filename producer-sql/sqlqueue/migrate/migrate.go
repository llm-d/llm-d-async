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

	// lockKey is the advisory lock key that serializes concurrent runs.
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
	ErrSchemaMissing = errors.New("sql transport schema not initialized: start an llm-d-async processor against this database")
	// ErrSchemaBehind means the database is at an older version than the binary expects.
	ErrSchemaBehind = errors.New("sql transport schema is behind: upgrade the llm-d-async processor")
)

var fileNamePattern = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

type migration struct {
	version int
	name    string
	sql     string
}

// load parses the embedded migrations in version order. Versions must start
// at 1 and be contiguous.
func load() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	migrations := make([]migration, 0, len(entries))
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
		migrations = append(migrations, migration{version: version, name: m[2], sql: string(body)})
	}

	for i, m := range migrations {
		if m.version != i+1 {
			return nil, fmt.Errorf("migration versions must be contiguous from 1, got %04d_%s at position %d", m.version, m.name, i+1)
		}
	}
	return migrations, nil
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// readVersion returns ErrSchemaMissing when the version table does not exist.
func readVersion(ctx context.Context, q queryer) (int, error) {
	var current int
	if err := q.QueryRowContext(ctx, selectCurrentVersion).Scan(&current); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == sqlstateUndefinedTable {
			return 0, ErrSchemaMissing
		}
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return current, nil
}

func logIfNewer(ctx context.Context, current, expected int) {
	if current > expected {
		logr.FromContextOrDiscard(ctx).Info("sql transport schema is newer than this binary expects",
			"databaseVersion", current, "expectedVersion", expected)
	}
}

// Run applies all pending migrations in a single transaction under an
// advisory lock, so concurrent callers serialize and at most one of them
// executes DDL. A schema that is already current takes no lock.
func Run(ctx context.Context, db *sql.DB) error {
	migrations, err := load()
	if err != nil {
		return err
	}

	current, err := readVersion(ctx, db)
	if err != nil && !errors.Is(err, ErrSchemaMissing) {
		return err
	}
	if err == nil && current >= len(migrations) {
		logIfNewer(ctx, current, len(migrations))
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err := tx.ExecContext(ctx, createVersionTable); err != nil {
		return fmt.Errorf("create %s: %w", versionTable, err)
	}
	if current, err = readVersion(ctx, tx); err != nil {
		return err
	}

	var applied []migration
	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		if _, err := tx.ExecContext(ctx, m.sql); err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", m.version, m.name, err)
		}
		if _, err := tx.ExecContext(ctx, insertVersion, m.version, m.name); err != nil {
			return fmt.Errorf("record migration %04d_%s: %w", m.version, m.name, err)
		}
		applied = append(applied, m)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	logger := logr.FromContextOrDiscard(ctx)
	for _, m := range applied {
		logger.Info("applied sql transport schema migration", "version", m.version, "name", m.name)
	}
	logIfNewer(ctx, current, len(migrations))
	return nil
}

// Check verifies the database schema is at least the version this binary
// expects. A missing version table returns ErrSchemaMissing and an older
// version returns ErrSchemaBehind; before returning either, Check waits for a
// migration in progress to commit and reads again. A newer version is logged
// and accepted so that binaries can be rolled back after a migration.
func Check(ctx context.Context, db *sql.DB) error {
	migrations, err := load()
	if err != nil {
		return err
	}
	expected := len(migrations)

	current, err := readVersion(ctx, db)
	if err != nil && !errors.Is(err, ErrSchemaMissing) {
		return err
	}
	if err == nil && current >= expected {
		logIfNewer(ctx, current, expected)
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock_shared($1)", lockKey); err != nil {
		return fmt.Errorf("wait for migration lock: %w", err)
	}
	if current, err = readVersion(ctx, tx); err != nil {
		return err
	}
	if current < expected {
		return fmt.Errorf("%w: database at version %d, binary expects %d", ErrSchemaBehind, current, expected)
	}
	logIfNewer(ctx, current, expected)
	return nil
}
