package sqlqueue

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

func DialectFromDSN(dsn string) (Dialect, error) {
	switch {
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"):
		return DialectPostgres, nil
	case strings.HasPrefix(dsn, "sqlite://"), strings.HasPrefix(dsn, "sqlite:"), strings.HasPrefix(dsn, "file:"):
		return DialectSQLite, nil
	default:
		return "", fmt.Errorf("sqlqueue: unsupported DSN scheme in %q (want postgres://, postgresql://, sqlite://, or file:)", redactDSN(dsn))
	}
}

func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return dsn
	}
	u.User = url.User("redacted")
	return u.String()
}

func sqliteDSN(dsn string) string {
	path := strings.TrimPrefix(strings.TrimPrefix(dsn, "sqlite://"), "sqlite:")
	if !strings.HasPrefix(path, "file:") {
		path = "file:" + path
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + "_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
}

const postgresDDL = `
CREATE TABLE IF NOT EXISTS async_requests (
	id             TEXT     NOT NULL,
	request_token  TEXT     NOT NULL,
	queue          TEXT     NOT NULL,
	partition_id   INTEGER  NOT NULL,
	deadline       BIGINT   NOT NULL,
	not_before     BIGINT   NOT NULL DEFAULT 0,
	dispatch_epoch BIGINT   NOT NULL DEFAULT 0,
	cancelled      SMALLINT NOT NULL DEFAULT 0,
	payload        TEXT     NOT NULL,
	created_at     BIGINT   NOT NULL,
	PRIMARY KEY (id, request_token)
) WITH (fillfactor = 70);
CREATE INDEX IF NOT EXISTS async_requests_pending ON async_requests (queue, deadline, created_at) WHERE dispatch_epoch = 0;
CREATE INDEX IF NOT EXISTS async_requests_inflight ON async_requests (queue, partition_id) WHERE dispatch_epoch > 0;

CREATE TABLE IF NOT EXISTS async_partitions (
	queue            TEXT     NOT NULL,
	partition_id     INTEGER  NOT NULL,
	owner            TEXT     NOT NULL DEFAULT '',
	epoch            BIGINT   NOT NULL DEFAULT 0,
	draining         SMALLINT NOT NULL DEFAULT 0,
	lease_expires_ms BIGINT   NOT NULL DEFAULT 0,
	PRIMARY KEY (queue, partition_id)
);

CREATE TABLE IF NOT EXISTS async_dispatchers (
	queue      TEXT   NOT NULL,
	owner      TEXT   NOT NULL,
	expires_ms BIGINT NOT NULL,
	PRIMARY KEY (queue, owner)
);

CREATE TABLE IF NOT EXISTS async_results (
	seq           BIGSERIAL PRIMARY KEY,
	route         TEXT   NOT NULL,
	id            TEXT   NOT NULL,
	request_token TEXT   NOT NULL,
	payload       TEXT   NOT NULL,
	expires_at    BIGINT NOT NULL DEFAULT 0,
	created_at    BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS async_results_route_seq ON async_results (route, seq);
CREATE INDEX IF NOT EXISTS async_results_expiry ON async_results (route, expires_at) WHERE expires_at > 0;
`

const sqliteDDL = `
CREATE TABLE IF NOT EXISTS async_requests (
	id             TEXT    NOT NULL,
	request_token  TEXT    NOT NULL,
	queue          TEXT    NOT NULL,
	partition_id   INTEGER NOT NULL,
	deadline       INTEGER NOT NULL,
	not_before     INTEGER NOT NULL DEFAULT 0,
	dispatch_epoch INTEGER NOT NULL DEFAULT 0,
	cancelled      INTEGER NOT NULL DEFAULT 0,
	payload        TEXT    NOT NULL,
	created_at     INTEGER NOT NULL,
	PRIMARY KEY (id, request_token)
);
CREATE INDEX IF NOT EXISTS async_requests_pending ON async_requests (queue, deadline, created_at) WHERE dispatch_epoch = 0;
CREATE INDEX IF NOT EXISTS async_requests_inflight ON async_requests (queue, partition_id) WHERE dispatch_epoch > 0;

CREATE TABLE IF NOT EXISTS async_partitions (
	queue            TEXT    NOT NULL,
	partition_id     INTEGER NOT NULL,
	owner            TEXT    NOT NULL DEFAULT '',
	epoch            INTEGER NOT NULL DEFAULT 0,
	draining         INTEGER NOT NULL DEFAULT 0,
	lease_expires_ms INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (queue, partition_id)
);

CREATE TABLE IF NOT EXISTS async_dispatchers (
	queue      TEXT    NOT NULL,
	owner      TEXT    NOT NULL,
	expires_ms INTEGER NOT NULL,
	PRIMARY KEY (queue, owner)
);

CREATE TABLE IF NOT EXISTS async_results (
	seq           INTEGER PRIMARY KEY AUTOINCREMENT,
	route         TEXT    NOT NULL,
	id            TEXT    NOT NULL,
	request_token TEXT    NOT NULL,
	payload       TEXT    NOT NULL,
	expires_at    INTEGER NOT NULL DEFAULT 0,
	created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS async_results_route_seq ON async_results (route, seq);
CREATE INDEX IF NOT EXISTS async_results_expiry ON async_results (route, expires_at) WHERE expires_at > 0;
`

// Migrate is safe to run concurrently; on Postgres an existing schema takes no DDL locks.
func (s *Store) Migrate(ctx context.Context) error {
	switch s.dialect {
	case DialectPostgres:
		return s.migratePostgres(ctx)
	case DialectSQLite:
		return execDDL(ctx, s.db, sqliteDDL)
	default:
		return fmt.Errorf("sqlqueue: unknown dialect %q", s.dialect)
	}
}

const migrateLockID = 0x6173796e6371 // "asyncq"

func (s *Store) migratePostgres(ctx context.Context) error {
	var present bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass('async_results_expiry') IS NOT NULL`).Scan(&present); err != nil {
		return fmt.Errorf("sqlqueue: migrate: check schema: %w", err)
	}
	if present {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlqueue: migrate: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, migrateLockID); err != nil {
		return fmt.Errorf("sqlqueue: migrate: lock: %w", err)
	}
	if err := execDDL(ctx, tx, postgresDDL); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlqueue: migrate: commit: %w", err)
	}
	return nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func execDDL(ctx context.Context, db execer, ddl string) error {
	for _, stmt := range strings.Split(ddl, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlqueue: migrate: %w", err)
		}
	}
	return nil
}
