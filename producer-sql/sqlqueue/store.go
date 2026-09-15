// Package sqlqueue is the Postgres queue behind the llm-d-async sql transport.
// Requests hash into Partitions partitions per queue, and each dispatcher
// leases a share of them. A request is pending while its dispatch_epoch is 0;
// dispatch stamps the partition epoch, and a new owner bumps the epoch and
// resets older stamps, which redelivers whatever the previous owner had in
// flight.
package sqlqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const Partitions = 64

const enqueueChunk = 1000

type Store struct {
	db     *sql.DB
	tracer trace.Tracer
}

type openOptions struct {
	tracerProvider trace.TracerProvider
}

// OpenOption configures Open.
type OpenOption func(*openOptions)

// WithTracerProvider records a span per database statement.
func WithTracerProvider(tp trace.TracerProvider) OpenOption {
	return func(o *openOptions) { o.tracerProvider = tp }
}

func Open(ctx context.Context, dsn string, opts ...OpenOption) (*Store, error) {
	var o openOptions
	for _, opt := range opts {
		opt(&o)
	}
	if err := validateDSN(dsn); err != nil {
		return nil, err
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlqueue: parse postgres dsn: %w", err)
	}
	if o.tracerProvider != nil {
		cfg.Tracer = otelpgx.NewTracer(otelpgx.WithTracerProvider(o.tracerProvider))
	}
	db := stdlib.OpenDB(*cfg)
	s := &Store{db: db, tracer: noop.NewTracerProvider().Tracer("")}
	if o.tracerProvider != nil {
		s.tracer = o.tracerProvider.Tracer("github.com/llm-d/llm-d-async/producer-sql/sqlqueue")
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlqueue: ping: %w", err)
	}
	if err := s.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) span(ctx context.Context, op string) (context.Context, trace.Span) {
	return s.tracer.Start(ctx, "sqlqueue."+op)
}

// Now reads the database clock so every dispatcher times leases alike.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var ms int64
	if err := s.db.QueryRowContext(ctx, `SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint`).Scan(&ms); err != nil {
		return time.Time{}, fmt.Errorf("sqlqueue: read database clock: %w", err)
	}
	return time.UnixMilli(ms), nil
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("sqlqueue: ping: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("sqlqueue: close: %w", err)
	}
	return nil
}

type Key struct {
	ID    string
	Token string
}

type Request struct {
	ID        string
	Token     string
	Queue     string
	Partition int
	Epoch     int64
	Deadline  int64
	Payload   string
	Cancelled bool
	createdAt int64
}

type Stamp struct {
	Key
	Epoch int64
}

func (r Request) Key() Key { return Key{ID: r.ID, Token: r.Token} }

func (r Request) Stamp() Stamp { return Stamp{Key: r.Key(), Epoch: r.Epoch} }

func partitionOf(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % Partitions)
}

// Enqueue inserts all requests or none. Equal deadlines dispatch in submission order.
func (s *Store) Enqueue(ctx context.Context, reqs ...Request) error {
	ctx, span := s.span(ctx, "Enqueue")
	defer span.End()
	if len(reqs) == 0 {
		return nil
	}
	createdAt := time.Now().UnixMicro()
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		for start := 0; start < len(reqs); start += enqueueChunk {
			chunk := reqs[start:min(len(reqs), start+enqueueChunk)]
			ids := make([]string, len(chunk))
			tokens := make([]string, len(chunk))
			queues := make([]string, len(chunk))
			parts := make([]int32, len(chunk))
			deadlines := make([]int64, len(chunk))
			payloads := make([]string, len(chunk))
			created := make([]int64, len(chunk))
			for i, r := range chunk {
				ids[i], tokens[i], queues[i] = r.ID, r.Token, r.Queue
				// #nosec G115 -- partitionOf returns 0..Partitions-1
				parts[i], deadlines[i], payloads[i] = int32(partitionOf(r.ID)), r.Deadline, r.Payload
				created[i] = createdAt + int64(start+i)
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO async_requests (id, request_token, queue, partition_id, deadline, payload, created_at)
				SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::bigint[], $6::text[], $7::bigint[])`,
				ids, tokens, queues, parts, deadlines, payloads, created); err != nil {
				return fmt.Errorf("insert chunk: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("sqlqueue: enqueue %d requests: %w", len(reqs), err)
	}
	return nil
}

// Dispatch stamps up to limit pending requests in partitions owner holds and is not draining.
func (s *Store) Dispatch(ctx context.Context, queue, owner string, now time.Time, limit int) ([]Request, error) {
	ctx, span := s.span(ctx, "Dispatch")
	defer span.End()
	rows, err := s.db.QueryContext(ctx, `
		WITH picked AS MATERIALIZED (
			SELECT r.id, r.request_token
			FROM async_requests r
			JOIN async_partitions q ON q.queue = r.queue AND q.partition_id = r.partition_id
			WHERE r.queue = $2 AND r.dispatch_epoch = 0 AND r.not_before <= $3
				AND q.owner = $1 AND q.draining = 0
			ORDER BY r.deadline, r.created_at
			LIMIT $4
		)
		UPDATE async_requests SET dispatch_epoch = p.epoch
		FROM async_partitions p
		WHERE p.queue = async_requests.queue AND p.partition_id = async_requests.partition_id
			AND p.owner = $1 AND p.draining = 0 AND async_requests.dispatch_epoch = 0
			AND (async_requests.id, async_requests.request_token) IN (SELECT id, request_token FROM picked)
		RETURNING async_requests.id, async_requests.request_token, async_requests.queue,
			async_requests.partition_id, async_requests.dispatch_epoch, async_requests.deadline,
			async_requests.payload, async_requests.cancelled, async_requests.created_at`,
		owner, queue, now.Unix(), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlqueue: dispatch %q: %w", queue, err)
	}
	defer func() { _ = rows.Close() }()
	var out []Request
	for rows.Next() {
		var r Request
		var cancelled int
		if err := rows.Scan(&r.ID, &r.Token, &r.Queue, &r.Partition, &r.Epoch, &r.Deadline, &r.Payload, &cancelled, &r.createdAt); err != nil {
			return nil, fmt.Errorf("sqlqueue: dispatch scan: %w", err)
		}
		r.Cancelled = cancelled == 1
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlqueue: dispatch rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Deadline != out[j].Deadline {
			return out[i].Deadline < out[j].Deadline
		}
		return out[i].createdAt < out[j].createdAt
	})
	return out, nil
}

func (s *Store) InFlight(ctx context.Context, queue, owner string) ([]Stamp, error) {
	ctx, span := s.span(ctx, "InFlight")
	defer span.End()
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.id, r.request_token, r.dispatch_epoch
		FROM async_requests r
		JOIN async_partitions p ON p.queue = r.queue AND p.partition_id = r.partition_id
		WHERE r.queue = $1 AND r.dispatch_epoch > 0 AND p.owner = $2 AND r.dispatch_epoch = p.epoch`, queue, owner)
	if err != nil {
		return nil, fmt.Errorf("sqlqueue: in flight %q: %w", queue, err)
	}
	defer func() { _ = rows.Close() }()
	var out []Stamp
	for rows.Next() {
		var st Stamp
		if err := rows.Scan(&st.ID, &st.Token, &st.Epoch); err != nil {
			return nil, fmt.Errorf("sqlqueue: in flight scan: %w", err)
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlqueue: in flight rows: %w", err)
	}
	return out, nil
}

// ResetStale returns requests stamped by an older epoch of owner's partitions to pending,
// limited to the given partitions unless nil. Rows another transaction holds are skipped
// and left for a later call.
func (s *Store) ResetStale(ctx context.Context, queue, owner string, partitions []int) error {
	ctx, span := s.span(ctx, "ResetStale")
	defer span.End()
	args := []any{queue, owner}
	only := ""
	if partitions != nil {
		if len(partitions) == 0 {
			return nil
		}
		in, parts := partitionList(partitions, 3)
		only = " AND r.partition_id IN (" + in + ")"
		args = append(args, parts...)
	}
	// #nosec G202 -- only generated placeholders are concatenated, partition ids bind as parameters
	if _, err := s.db.ExecContext(ctx, `
		WITH stale AS MATERIALIZED (
			SELECT r.id, r.request_token
			FROM async_requests r
			JOIN async_partitions p ON p.queue = r.queue AND p.partition_id = r.partition_id
			WHERE r.queue = $1 AND r.dispatch_epoch > 0 AND p.owner = $2 AND r.dispatch_epoch < p.epoch`+only+`
			FOR UPDATE OF r SKIP LOCKED
		)
		UPDATE async_requests SET dispatch_epoch = 0
		WHERE dispatch_epoch > 0 AND (id, request_token) IN (SELECT id, request_token FROM stale)`, args...); err != nil {
		return fmt.Errorf("sqlqueue: reset stale %q: %w", queue, err)
	}
	return nil
}

// Undispatch and Retry only touch a request still carrying the given stamp in a partition owner holds.
func (s *Store) Undispatch(ctx context.Context, owner string, stamps []Stamp) error {
	ctx, span := s.span(ctx, "Undispatch")
	defer span.End()
	stamps = uniqueStamps(stamps)
	if len(stamps) == 0 {
		return nil
	}
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		ids, tokens, epochs := make([]string, len(stamps)), make([]string, len(stamps)), make([]int64, len(stamps))
		for i, st := range stamps {
			ids[i], tokens[i], epochs[i] = st.ID, st.Token, st.Epoch
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE async_requests SET dispatch_epoch = 0
			FROM unnest($2::text[], $3::text[], $4::bigint[]) AS k(id, request_token, epoch), async_partitions p
			WHERE async_requests.id = k.id AND async_requests.request_token = k.request_token
				AND async_requests.dispatch_epoch = k.epoch
				AND p.queue = async_requests.queue AND p.partition_id = async_requests.partition_id
				AND p.owner = $1`, owner, ids, tokens, epochs); err != nil {
			return fmt.Errorf("update: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("sqlqueue: undispatch %d requests: %w", len(stamps), err)
	}
	return nil
}

func (s *Store) Retry(ctx context.Context, owner string, stamp Stamp, notBefore int64, payload string) (bool, error) {
	ctx, span := s.span(ctx, "Retry")
	defer span.End()
	res, err := s.db.ExecContext(ctx, `
		UPDATE async_requests SET dispatch_epoch = 0, not_before = $4, payload = $5
		WHERE id = $1 AND request_token = $2 AND dispatch_epoch = $6 AND EXISTS (
			SELECT 1 FROM async_partitions p
			WHERE p.queue = async_requests.queue AND p.partition_id = async_requests.partition_id AND p.owner = $3)`,
		stamp.ID, stamp.Token, owner, notBefore, payload, stamp.Epoch)
	if err != nil {
		return false, fmt.Errorf("sqlqueue: retry %q: %w", stamp.ID, err)
	}
	return oneRow(res)
}

func (s *Store) Cancel(ctx context.Context, ids []string) error {
	ctx, span := s.span(ctx, "Cancel")
	defer span.End()
	var live []string
	for _, id := range ids {
		if id != "" {
			live = append(live, id)
		}
	}
	if len(live) == 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE async_requests SET cancelled = 1 WHERE id = ANY($1::text[])`, live); err != nil {
		return fmt.Errorf("sqlqueue: cancel %d requests: %w", len(live), err)
	}
	return nil
}

func (s *Store) IsCancelled(ctx context.Context, key Key) (bool, error) {
	var cancelled int
	err := s.db.QueryRowContext(ctx,
		`SELECT cancelled FROM async_requests WHERE id = $1 AND request_token = $2`, key.ID, key.Token).Scan(&cancelled)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlqueue: is cancelled %q: %w", key.ID, err)
	}
	return cancelled == 1, nil
}

type Completion struct {
	Key
	Route     string
	Payload   string
	ExpiresAt int64
}

// Ack deletes each request and records its result in one transaction. Fenced, missing, and
// duplicate completions report false.
func (s *Store) Ack(ctx context.Context, owner string, completions []Completion) ([]bool, error) {
	ctx, span := s.span(ctx, "Ack")
	defer span.End()
	acked := make([]bool, len(completions))
	if len(completions) == 0 {
		return acked, nil
	}
	first := make(map[Key]int, len(completions))
	var unique []Completion
	for i, c := range completions {
		if _, dup := first[c.Key]; dup {
			continue
		}
		first[c.Key] = i
		unique = append(unique, c)
	}
	createdAt := time.Now().UnixMilli()
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		n := len(unique)
		ids, tokens, routes, payloads := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
		expires := make([]int64, n)
		for i, c := range unique {
			ids[i], tokens[i], routes[i], payloads[i], expires[i] = c.ID, c.Token, c.Route, c.Payload, c.ExpiresAt
		}
		rows, err := tx.QueryContext(ctx, `
			WITH input AS (
				SELECT * FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[])
					AS t(id, request_token, route, payload, expires_at)
			), done AS (
				DELETE FROM async_requests r USING input i, async_partitions p
				WHERE r.id = i.id AND r.request_token = i.request_token
					AND p.queue = r.queue AND p.partition_id = r.partition_id AND p.owner = $1
				RETURNING r.id, r.request_token
			)
			INSERT INTO async_results (route, id, request_token, payload, expires_at, created_at)
			SELECT i.route, i.id, i.request_token, i.payload, i.expires_at, $7::bigint
			FROM done d JOIN input i ON i.id = d.id AND i.request_token = d.request_token
			RETURNING id, request_token`,
			owner, ids, tokens, routes, payloads, expires, createdAt)
		if err != nil {
			return fmt.Errorf("ack statement: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var k Key
			if err := rows.Scan(&k.ID, &k.Token); err != nil {
				return fmt.Errorf("scan acked key: %w", err)
			}
			acked[first[k]] = true
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("acked rows: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sqlqueue: ack %d results: %w", len(unique), err)
	}
	return acked, nil
}

type Result struct {
	Seq     int64
	Route   string
	ID      string
	Token   string
	Payload string
}

// PopResults deletes the results it returns.
func (s *Store) PopResults(ctx context.Context, route string, now int64, limit int) ([]Result, error) {
	ctx, span := s.span(ctx, "PopResults")
	defer span.End()
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM async_results WHERE route = $1 AND expires_at > 0 AND expires_at < $2`, route, now); err != nil {
		return nil, fmt.Errorf("sqlqueue: expire results on %q: %w", route, err)
	}
	rows, err := s.db.QueryContext(ctx, `
		WITH picked AS MATERIALIZED (
			SELECT seq FROM async_results WHERE route = $1 ORDER BY seq LIMIT $2 FOR UPDATE SKIP LOCKED
		)
		DELETE FROM async_results WHERE seq IN (SELECT seq FROM picked)
		RETURNING seq, route, id, request_token, payload`, route, limit)
	if err != nil {
		return nil, fmt.Errorf("sqlqueue: pop results %q: %w", route, err)
	}
	defer func() { _ = rows.Close() }()
	var out []Result
	for rows.Next() {
		var r Result
		if err := rows.Scan(&r.Seq, &r.Route, &r.ID, &r.Token, &r.Payload); err != nil {
			return nil, fmt.Errorf("sqlqueue: pop results scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlqueue: pop results rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (s *Store) HasRequests(ctx context.Context, queue string) (bool, error) {
	var found int
	if err := s.db.QueryRowContext(ctx, `
		SELECT CASE WHEN
			EXISTS (SELECT 1 FROM async_requests WHERE queue = $1 AND dispatch_epoch = 0)
			OR EXISTS (SELECT 1 FROM async_requests WHERE queue = $1 AND dispatch_epoch > 0)
		THEN 1 ELSE 0 END`, queue).Scan(&found); err != nil {
		return false, fmt.Errorf("sqlqueue: has requests %q: %w", queue, err)
	}
	return found == 1, nil
}

func (s *Store) Backlog(ctx context.Context, queue string, now int64, windows []int64) (depth int64, expiring []int64, err error) {
	const count = `
		SELECT (SELECT COUNT(*) FROM async_requests WHERE queue = $1 AND dispatch_epoch = 0 AND deadline <= $2)
			+ (SELECT COUNT(*) FROM async_requests WHERE queue = $1 AND dispatch_epoch > 0 AND deadline <= $2)`
	if err := s.db.QueryRowContext(ctx, count, queue, int64(math.MaxInt64)).Scan(&depth); err != nil {
		return 0, nil, fmt.Errorf("sqlqueue: backlog depth %q: %w", queue, err)
	}
	expiring = make([]int64, len(windows))
	for i, w := range windows {
		if err := s.db.QueryRowContext(ctx, count, queue, now+w).Scan(&expiring[i]); err != nil {
			return 0, nil, fmt.Errorf("sqlqueue: backlog window %q: %w", queue, err)
		}
	}
	return depth, expiring, nil
}

func (s *Store) ResultDepth(ctx context.Context, route string) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM async_results WHERE route = $1`, route).Scan(&n); err != nil {
		return 0, fmt.Errorf("sqlqueue: result depth %q: %w", route, err)
	}
	return n, nil
}

type Lease struct {
	Partition int
	Epoch     int64
	Draining  bool
}

func (s *Store) EnsurePartitions(ctx context.Context, queue string) error {
	values := make([]string, Partitions)
	for i := range values {
		values[i] = "($1, " + strconv.Itoa(i) + ")"
	}
	// #nosec G202 -- only generated placeholders are concatenated, the queue binds as a parameter
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO async_partitions (queue, partition_id) VALUES `+strings.Join(values, ", ")+`
		ON CONFLICT (queue, partition_id) DO NOTHING`, queue); err != nil {
		return fmt.Errorf("sqlqueue: ensure partitions %q: %w", queue, err)
	}
	return nil
}

// Heartbeat renews owner's leases and reports live dispatchers. A non-member renews without
// counting toward the share, so peers take over its partitions while it drains.
func (s *Store) Heartbeat(ctx context.Context, queue, owner string, now time.Time, ttl time.Duration, member bool) ([]Lease, int, error) {
	ctx, span := s.span(ctx, "Heartbeat")
	defer span.End()
	nowMs, expires := now.UnixMilli(), now.Add(ttl).UnixMilli()
	membership := `DELETE FROM async_dispatchers WHERE queue = $1 AND owner = $2`
	args := []any{queue, owner}
	if member {
		membership = `
			INSERT INTO async_dispatchers (queue, owner, expires_ms) VALUES ($1, $2, $3)
			ON CONFLICT (queue, owner) DO UPDATE SET expires_ms = excluded.expires_ms`
		args = append(args, expires)
	}
	if _, err := s.db.ExecContext(ctx, membership, args...); err != nil {
		return nil, 0, fmt.Errorf("sqlqueue: update membership %q: %w", queue, err)
	}
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM async_dispatchers WHERE queue = $1 AND expires_ms < $2`, queue, nowMs); err != nil {
		return nil, 0, fmt.Errorf("sqlqueue: expire dispatchers %q: %w", queue, err)
	}
	var members int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM async_dispatchers WHERE queue = $1`, queue).Scan(&members); err != nil {
		return nil, 0, fmt.Errorf("sqlqueue: count dispatchers %q: %w", queue, err)
	}
	leases, err := s.queryLeases(ctx, `
		UPDATE async_partitions SET lease_expires_ms = $3
		WHERE queue = $1 AND owner = $2
		RETURNING partition_id, epoch, draining`, queue, owner, expires)
	if err != nil {
		return nil, 0, fmt.Errorf("sqlqueue: renew leases %q: %w", queue, err)
	}
	return leases, members, nil
}

// AcquirePartitions takes unowned or lapsed partitions and bumps their epoch.
func (s *Store) AcquirePartitions(ctx context.Context, queue, owner string, now time.Time, ttl time.Duration, limit int) ([]Lease, error) {
	ctx, span := s.span(ctx, "AcquirePartitions")
	defer span.End()
	leases, err := s.queryLeases(ctx, `
		WITH picked AS MATERIALIZED (
			SELECT partition_id FROM async_partitions
			WHERE queue = $1 AND (owner = '' OR lease_expires_ms < $3)
			ORDER BY lease_expires_ms, partition_id
			LIMIT $5 FOR UPDATE SKIP LOCKED
		)
		UPDATE async_partitions SET owner = $2, epoch = epoch + 1, draining = 0, lease_expires_ms = $4
		WHERE queue = $1 AND (owner = '' OR lease_expires_ms < $3) AND partition_id IN (SELECT partition_id FROM picked)
		RETURNING partition_id, epoch, draining`,
		queue, owner, now.UnixMilli(), now.Add(ttl).UnixMilli(), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlqueue: acquire partitions %q: %w", queue, err)
	}
	return leases, nil
}

// A draining partition stays leased for acks but dispatches nothing new.
func (s *Store) SetDraining(ctx context.Context, queue, owner string, partitions []int, draining bool) error {
	if len(partitions) == 0 {
		return nil
	}
	flag := 0
	if draining {
		flag = 1
	}
	in, args := partitionList(partitions, 4)
	// #nosec G202 -- only generated placeholders are concatenated, partition ids bind as parameters
	if _, err := s.db.ExecContext(ctx, `
		UPDATE async_partitions SET draining = $3
		WHERE queue = $1 AND owner = $2 AND partition_id IN (`+in+`)`,
		append([]any{queue, owner, flag}, args...)...); err != nil {
		return fmt.Errorf("sqlqueue: set draining %q: %w", queue, err)
	}
	return nil
}

func (s *Store) ReleasePartitions(ctx context.Context, queue, owner string, partitions []int) error {
	ctx, span := s.span(ctx, "ReleasePartitions")
	defer span.End()
	if len(partitions) == 0 {
		return nil
	}
	in, args := partitionList(partitions, 3)
	// #nosec G202 -- only generated placeholders are concatenated, partition ids bind as parameters
	if _, err := s.db.ExecContext(ctx, `
		UPDATE async_partitions SET owner = '', draining = 0, lease_expires_ms = 0
		WHERE queue = $1 AND owner = $2 AND partition_id IN (`+in+`)`,
		append([]any{queue, owner}, args...)...); err != nil {
		return fmt.Errorf("sqlqueue: release partitions %q: %w", queue, err)
	}
	return nil
}

func (s *Store) Leave(ctx context.Context, queue, owner string) error {
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			UPDATE async_partitions SET owner = '', draining = 0, lease_expires_ms = 0
			WHERE queue = $1 AND owner = $2`, queue, owner); err != nil {
			return fmt.Errorf("release partitions: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM async_dispatchers WHERE queue = $1 AND owner = $2`, queue, owner); err != nil {
			return fmt.Errorf("remove membership: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("sqlqueue: leave %q: %w", queue, err)
	}
	return nil
}

func (s *Store) TruncateForTest(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "TRUNCATE async_requests, async_results, async_partitions, async_dispatchers"); err != nil {
		return fmt.Errorf("sqlqueue: truncate: %w", err)
	}
	return nil
}

func (s *Store) queryLeases(ctx context.Context, query string, args ...any) ([]Lease, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Lease
	for rows.Next() {
		var l Lease
		var draining int
		if err := rows.Scan(&l.Partition, &l.Epoch, &draining); err != nil {
			return nil, fmt.Errorf("scan lease: %w", err)
		}
		l.Draining = draining == 1
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lease rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out, nil
}

func (s *Store) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func partitionList(partitions []int, first int) (string, []any) {
	marks := make([]string, len(partitions))
	args := make([]any, len(partitions))
	for i, p := range partitions {
		marks[i] = "$" + strconv.Itoa(first+i)
		args[i] = p
	}
	return strings.Join(marks, ", "), args
}

func uniqueStamps(stamps []Stamp) []Stamp {
	seen := make(map[Stamp]struct{}, len(stamps))
	out := stamps[:0:0]
	for _, st := range stamps {
		if _, ok := seen[st]; ok {
			continue
		}
		seen[st] = struct{}{}
		out = append(out, st)
	}
	return out
}

func oneRow(res sql.Result) (bool, error) {
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sqlqueue: rows affected: %w", err)
	}
	if n > 1 {
		return false, errors.New("sqlqueue: statement matched more than one row")
	}
	return n == 1, nil
}
