ALTER TABLE async_results
	ADD COLUMN claim_owner      TEXT   NOT NULL DEFAULT '',
	ADD COLUMN claim_expires_ms BIGINT NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION async_pop_results(p_route TEXT, p_limit INTEGER)
RETURNS SETOF async_results LANGUAGE plpgsql SET enable_seqscan = off SET enable_bitmapscan = off AS $$
DECLARE
	v_now BIGINT := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
BEGIN
	RETURN QUERY
	DELETE FROM async_results
	WHERE route = p_route AND seq = ANY(ARRAY(
		SELECT seq FROM async_results WHERE route = p_route AND claim_expires_ms <= v_now
		ORDER BY seq LIMIT p_limit FOR UPDATE SKIP LOCKED))
	RETURNING *;
END $$;

CREATE FUNCTION async_claim_results(p_route TEXT, p_owner TEXT, p_lease_ms BIGINT, p_limit INTEGER)
RETURNS SETOF async_results LANGUAGE plpgsql SET enable_seqscan = off SET enable_bitmapscan = off AS $$
DECLARE
	v_now BIGINT := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
BEGIN
	RETURN QUERY
	UPDATE async_results SET claim_owner = p_owner, claim_expires_ms = v_now + p_lease_ms
	WHERE route = p_route AND seq = ANY(ARRAY(
		SELECT seq FROM async_results
		WHERE route = p_route AND claim_expires_ms <= v_now AND (expires_at = 0 OR expires_at >= v_now / 1000)
		ORDER BY seq LIMIT p_limit FOR UPDATE SKIP LOCKED))
	RETURNING *;
END $$;

CREATE FUNCTION async_ack_result(p_seq BIGINT, p_owner TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql AS $$
BEGIN
	DELETE FROM async_results WHERE seq = p_seq AND claim_owner = p_owner;
	IF FOUND THEN
		RETURN true;
	END IF;
	PERFORM 1 FROM async_results WHERE seq = p_seq;
	RETURN NOT FOUND;
END $$;
