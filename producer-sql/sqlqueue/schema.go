package sqlqueue

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
)

func validateDSN(dsn string) error {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return nil
	}
	return fmt.Errorf("sqlqueue: unsupported DSN scheme in %q (want postgres:// or postgresql://)", redactDSN(dsn))
}

func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return dsn
	}
	u.User = url.User("redacted")
	return u.String()
}

// Queue tables churn their whole contents, so autovacuum runs on a fixed row count without
// cost throttling; percentage thresholds let dead tuples pile up at the head of the pending
// index until every dispatch scans them.
const postgresDDL = `
CREATE TABLE IF NOT EXISTS async_requests (
	id             TEXT     NOT NULL,
	request_token  TEXT     NOT NULL,
	queue          TEXT     NOT NULL,
	partition_id   INTEGER  NOT NULL,
	deadline       BIGINT   NOT NULL,
	not_before     BIGINT   NOT NULL DEFAULT 0,
	dispatch_epoch BIGINT   NOT NULL DEFAULT 0,
	dispatch_attempt BIGINT NOT NULL DEFAULT 0,
	cancelled      SMALLINT NOT NULL DEFAULT 0,
	envelope       TEXT     NOT NULL,
	payload        BYTEA    NOT NULL,
	created_at     BIGINT   NOT NULL,
	PRIMARY KEY (id, request_token)
) WITH (
	fillfactor = 70,
	autovacuum_vacuum_scale_factor = 0,
	autovacuum_vacuum_threshold = 10000,
	autovacuum_vacuum_insert_scale_factor = 0,
	autovacuum_vacuum_insert_threshold = 10000,
	autovacuum_analyze_scale_factor = 0.02,
	autovacuum_vacuum_cost_delay = 0
);
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
) WITH (
	autovacuum_vacuum_scale_factor = 0,
	autovacuum_vacuum_threshold = 10000,
	autovacuum_vacuum_insert_scale_factor = 0,
	autovacuum_vacuum_insert_threshold = 10000,
	autovacuum_vacuum_cost_delay = 0
);
CREATE INDEX IF NOT EXISTS async_results_route_seq ON async_results (route, seq);
CREATE INDEX IF NOT EXISTS async_results_expiry ON async_results (route, expires_at) WHERE expires_at > 0;

CREATE SEQUENCE IF NOT EXISTS async_dispatch_attempts;

CREATE TABLE IF NOT EXISTS async_quota_keys (
	key TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS async_quota_holders (
	holder     TEXT   PRIMARY KEY,
	expires_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS async_quota_slots (
	key    TEXT    NOT NULL,
	holder TEXT    NOT NULL,
	used   INTEGER NOT NULL,
	PRIMARY KEY (key, holder)
);
CREATE INDEX IF NOT EXISTS async_quota_slots_holder ON async_quota_slots (holder);

CREATE TABLE IF NOT EXISTS async_quota_windows (
	key       TEXT   NOT NULL,
	window_ms BIGINT NOT NULL,
	used      BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (key, window_ms)
);

CREATE TABLE IF NOT EXISTS async_quota_admits (
	key       TEXT    NOT NULL,
	window_ms BIGINT  NOT NULL,
	at_ms     BIGINT  NOT NULL,
	n         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS async_quota_admits_key ON async_quota_admits (key, window_ms, at_ms);
`

var postgresFunctions = []string{`
CREATE OR REPLACE FUNCTION async_quota_acquire(p_key TEXT, p_holder TEXT, p_n INTEGER, p_limit INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
	v_now   BIGINT;
	v_used  BIGINT;
	v_grant INTEGER;
BEGIN
	INSERT INTO async_quota_keys (key) VALUES (p_key) ON CONFLICT (key) DO NOTHING;
	PERFORM 1 FROM async_quota_keys WHERE key = p_key FOR UPDATE;
	v_now := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
	PERFORM 1 FROM async_quota_holders WHERE holder = p_holder AND expires_ms > v_now;
	IF NOT FOUND THEN
		RETURN -1;
	END IF;
	SELECT coalesce(sum(s.used), 0) INTO v_used
	FROM async_quota_slots s JOIN async_quota_holders h ON h.holder = s.holder
	WHERE s.key = p_key AND h.expires_ms > v_now;
	v_grant := least(p_n, greatest(p_limit - v_used, 0));
	IF v_grant > 0 THEN
		INSERT INTO async_quota_slots (key, holder, used) VALUES (p_key, p_holder, v_grant)
		ON CONFLICT (key, holder) DO UPDATE SET used = async_quota_slots.used + EXCLUDED.used;
	END IF;
	RETURN v_grant;
END $$`, `
CREATE OR REPLACE FUNCTION async_quota_admit(p_key TEXT, p_n INTEGER, p_limit INTEGER, p_window_ms BIGINT)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
	v_now     BIGINT;
	v_used    BIGINT;
	v_expired BIGINT;
	v_grant   INTEGER;
BEGIN
	INSERT INTO async_quota_windows (key, window_ms) VALUES (p_key, p_window_ms) ON CONFLICT (key, window_ms) DO NOTHING;
	SELECT used INTO v_used FROM async_quota_windows WHERE key = p_key AND window_ms = p_window_ms FOR UPDATE;
	v_now := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
	WITH gone AS (
		DELETE FROM async_quota_admits
		WHERE key = p_key AND window_ms = p_window_ms AND at_ms <= v_now - p_window_ms
		RETURNING n
	)
	SELECT coalesce(sum(n), 0) INTO v_expired FROM gone;
	v_used := v_used - v_expired;
	v_grant := least(p_n, greatest(p_limit - v_used, 0));
	IF v_grant > 0 THEN
		INSERT INTO async_quota_admits (key, window_ms, at_ms, n) VALUES (p_key, p_window_ms, v_now, v_grant);
	END IF;
	IF v_grant > 0 OR v_expired > 0 THEN
		UPDATE async_quota_windows SET used = v_used + v_grant WHERE key = p_key AND window_ms = p_window_ms;
	END IF;
	RETURN v_grant;
END $$`}

const migrateLockID = 0x6173796e6371 // "asyncq"

// Migrate is safe to run concurrently; an existing schema takes no DDL locks.
func (s *Store) Migrate(ctx context.Context) error {
	var present bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass('async_quota_windows') IS NOT NULL AND to_regproc('async_quota_admit') IS NOT NULL`).Scan(&present); err != nil {
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
	for _, fn := range postgresFunctions {
		if _, err := tx.ExecContext(ctx, fn); err != nil {
			return fmt.Errorf("sqlqueue: migrate: %w", err)
		}
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
