DROP TABLE IF EXISTS async_schema;

-- Queue tables churn their whole contents, so autovacuum runs on a fixed row count without
-- cost throttling; percentage thresholds let dead tuples pile up at the head of the pending
-- index until every dispatch scans them.
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
CREATE INDEX IF NOT EXISTS async_results_expiry ON async_results (expires_at) WHERE expires_at > 0;

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

-- The dispatch and result picks walk an index in order and stop at the limit. Queue
-- statistics swing between empty and full within seconds, and a planner that believes the
-- table is empty prefers a seq or bitmap scan that sorts every row, so these run with both off.
CREATE OR REPLACE FUNCTION async_dispatch(p_owner TEXT, p_queue TEXT, p_now BIGINT, p_limit INTEGER)
RETURNS SETOF async_requests LANGUAGE sql SET enable_seqscan = off SET enable_bitmapscan = off AS $$
	WITH picked AS MATERIALIZED (
		SELECT ctid
		FROM async_requests
		WHERE queue = p_queue AND dispatch_epoch = 0 AND not_before <= p_now
			AND partition_id = ANY(ARRAY(
				SELECT partition_id FROM async_partitions WHERE queue = p_queue AND owner = p_owner AND draining = 0))
		ORDER BY deadline, created_at
		LIMIT p_limit
		FOR UPDATE SKIP LOCKED
	)
	UPDATE async_requests AS r SET dispatch_epoch = p.epoch, dispatch_attempt = nextval('async_dispatch_attempts')
	FROM async_partitions p
	WHERE r.ctid = ANY(ARRAY(SELECT ctid FROM picked))
		AND p.queue = r.queue AND p.partition_id = r.partition_id AND p.owner = p_owner AND p.draining = 0
	RETURNING r.*
$$;

CREATE OR REPLACE FUNCTION async_pop_results(p_route TEXT, p_limit INTEGER)
RETURNS SETOF async_results LANGUAGE sql SET enable_seqscan = off SET enable_bitmapscan = off AS $$
	DELETE FROM async_results
	WHERE route = p_route AND seq = ANY(ARRAY(
		SELECT seq FROM async_results WHERE route = p_route ORDER BY seq LIMIT p_limit FOR UPDATE SKIP LOCKED))
	RETURNING *
$$;

CREATE OR REPLACE FUNCTION async_quota_acquire(p_key TEXT, p_holder TEXT, p_n INTEGER, p_limit INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
	v_now   BIGINT;
	v_used  BIGINT;
	v_grant INTEGER;
BEGIN
	LOOP
		PERFORM 1 FROM async_quota_keys WHERE key = p_key FOR UPDATE;
		EXIT WHEN FOUND;
		INSERT INTO async_quota_keys (key) VALUES (p_key) ON CONFLICT (key) DO NOTHING;
	END LOOP;
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
END $$;

CREATE OR REPLACE FUNCTION async_quota_admit(p_key TEXT, p_n INTEGER, p_limit INTEGER, p_window_ms BIGINT)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
	v_now     BIGINT;
	v_used    BIGINT;
	v_expired BIGINT;
	v_grant   INTEGER;
BEGIN
	LOOP
		SELECT used INTO v_used FROM async_quota_windows WHERE key = p_key AND window_ms = p_window_ms FOR UPDATE;
		EXIT WHEN FOUND;
		INSERT INTO async_quota_windows (key, window_ms) VALUES (p_key, p_window_ms) ON CONFLICT (key, window_ms) DO NOTHING;
	END LOOP;
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
END $$;

CREATE OR REPLACE FUNCTION async_quota_reap(p_limit INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
	v_now   BIGINT := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
	v_total INTEGER := 0;
	v_rows  INTEGER;
	v_n     BIGINT;
	w       RECORD;
BEGIN
	FOR w IN
		SELECT qw.key, qw.window_ms FROM async_quota_windows qw
		WHERE EXISTS (
			SELECT 1 FROM async_quota_admits a
			WHERE a.key = qw.key AND a.window_ms = qw.window_ms AND a.at_ms <= v_now - qw.window_ms)
		LIMIT p_limit
		FOR UPDATE SKIP LOCKED
	LOOP
		WITH gone AS (
			DELETE FROM async_quota_admits
			WHERE key = w.key AND window_ms = w.window_ms AND at_ms <= v_now - w.window_ms
			RETURNING n
		)
		SELECT coalesce(sum(n), 0) INTO v_n FROM gone;
		UPDATE async_quota_windows SET used = used - v_n WHERE key = w.key AND window_ms = w.window_ms;
		v_total := v_total + 1;
	END LOOP;

	DELETE FROM async_quota_windows WHERE ctid = ANY(ARRAY(
		SELECT qw.ctid FROM async_quota_windows qw
		WHERE qw.used = 0 AND NOT EXISTS (
			SELECT 1 FROM async_quota_admits a WHERE a.key = qw.key AND a.window_ms = qw.window_ms)
		LIMIT p_limit
		FOR UPDATE SKIP LOCKED));
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_total := v_total + v_rows;

	DELETE FROM async_quota_slots WHERE ctid = ANY(ARRAY(
		SELECT ctid FROM async_quota_slots WHERE used = 0 LIMIT p_limit FOR UPDATE SKIP LOCKED));
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_total := v_total + v_rows;

	DELETE FROM async_quota_keys WHERE ctid = ANY(ARRAY(
		SELECT k.ctid FROM async_quota_keys k
		WHERE NOT EXISTS (SELECT 1 FROM async_quota_slots s WHERE s.key = k.key)
		LIMIT p_limit
		FOR UPDATE SKIP LOCKED));
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	RETURN v_total + v_rows;
END $$;
