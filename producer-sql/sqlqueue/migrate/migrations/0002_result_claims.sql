ALTER TABLE async_results
	ADD COLUMN claim_owner      TEXT   NOT NULL DEFAULT '',
	ADD COLUMN claim_expires_ms BIGINT NOT NULL DEFAULT 0;

CREATE INDEX async_results_unclaimed ON async_results (route, seq) WHERE claim_expires_ms = 0;
CREATE INDEX async_results_claimed ON async_results (route, claim_expires_ms, seq) WHERE claim_expires_ms > 0;
DROP INDEX async_results_route_seq;

CREATE FUNCTION async_pick_results(p_route TEXT, p_now_ms BIGINT, p_limit INTEGER)
RETURNS BIGINT[] LANGUAGE plpgsql SET enable_seqscan = off SET enable_bitmapscan = off AS $$
DECLARE
	v_seqs BIGINT[];
BEGIN
	v_seqs := ARRAY(
		SELECT seq FROM async_results
		WHERE route = p_route AND claim_expires_ms > 0 AND claim_expires_ms <= p_now_ms
			AND (expires_at = 0 OR expires_at >= p_now_ms / 1000)
		ORDER BY claim_expires_ms, seq LIMIT p_limit FOR UPDATE SKIP LOCKED);
	IF cardinality(v_seqs) < p_limit THEN
		v_seqs := v_seqs || ARRAY(
			SELECT seq FROM async_results
			WHERE route = p_route AND claim_expires_ms = 0
				AND (expires_at = 0 OR expires_at >= p_now_ms / 1000)
			ORDER BY seq LIMIT p_limit - cardinality(v_seqs) FOR UPDATE SKIP LOCKED);
	END IF;
	RETURN v_seqs;
END $$;

CREATE OR REPLACE FUNCTION async_pop_results(p_route TEXT, p_limit INTEGER)
RETURNS SETOF async_results LANGUAGE plpgsql SET enable_seqscan = off SET enable_bitmapscan = off AS $$
DECLARE
	v_seqs BIGINT[] := async_pick_results(p_route, (extract(epoch FROM clock_timestamp()) * 1000)::bigint, p_limit);
BEGIN
	RETURN QUERY
	DELETE FROM async_results WHERE seq = ANY(v_seqs)
	RETURNING *;
END $$;

CREATE FUNCTION async_claim_results(p_route TEXT, p_owner TEXT, p_lease_ms BIGINT, p_limit INTEGER)
RETURNS SETOF async_results LANGUAGE plpgsql SET enable_seqscan = off SET enable_bitmapscan = off AS $$
DECLARE
	v_now  BIGINT   := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
	v_seqs BIGINT[] := async_pick_results(p_route, v_now, p_limit);
BEGIN
	RETURN QUERY
	UPDATE async_results SET claim_owner = p_owner, claim_expires_ms = v_now + p_lease_ms
	WHERE seq = ANY(v_seqs)
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
