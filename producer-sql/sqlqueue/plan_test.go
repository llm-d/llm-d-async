package sqlqueue

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

// planRecorder collects the plan of every statement a Store runs, sent back by auto_explain as notices.
type planRecorder struct {
	mu    sync.Mutex
	plans []explainPlan
}

type explainPlan struct {
	Query string   `json:"Query Text"`
	Plan  planNode `json:"Plan"`
}

type planNode struct {
	Relation         string     `json:"Relation Name"`
	ActualRows       float64    `json:"Actual Rows"`
	ActualLoops      float64    `json:"Actual Loops"`
	RemovedByFilter  float64    `json:"Rows Removed by Filter"`
	RemovedByRecheck float64    `json:"Rows Removed by Index Recheck"`
	RemovedByJoin    float64    `json:"Rows Removed by Join Filter"`
	Plans            []planNode `json:"Plans"`
}

// queueRows counts the queue-table rows a plan read or discarded, the work that grows with the table.
func (n planNode) queueRows() float64 {
	total := n.RemovedByJoin * n.ActualLoops
	if n.Relation == "async_requests" || n.Relation == "async_results" {
		total += (n.ActualRows + n.RemovedByFilter + n.RemovedByRecheck) * n.ActualLoops
	}
	for _, c := range n.Plans {
		total += c.queueRows()
	}
	return total
}

func (r *planRecorder) take() []explainPlan {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.plans
	r.plans = nil
	return out
}

func openExplainedStore(t *testing.T) (*Store, *planRecorder) {
	t.Helper()
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	cfg, err := pgx.ParseConfig(pgURL)
	require.NoError(t, err)
	rec := &planRecorder{}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		_, body, ok := strings.Cut(n.Message, "plan:\n")
		if !ok {
			return
		}
		var p explainPlan
		if json.Unmarshal([]byte(body), &p) != nil {
			return
		}
		rec.mu.Lock()
		rec.plans = append(rec.plans, p)
		rec.mu.Unlock()
	}
	db := stdlib.OpenDB(*cfg, stdlib.OptionAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `LOAD 'auto_explain';
			SET auto_explain.log_min_duration = 0;
			SET auto_explain.log_analyze = on;
			SET auto_explain.log_timing = off;
			SET auto_explain.log_format = json;
			SET auto_explain.log_nested_statements = on;
			SET auto_explain.log_level = notice`)
		return err
	}))
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{db: db, tracer: noop.NewTracerProvider().Tracer("")}
	ctx := context.Background()
	require.NoError(t, s.Migrate(ctx))
	require.NoError(t, s.TruncateForTest(ctx))
	return s, rec
}

// A queue table that drains is vacuumed or analyzed while every row is dead, leaving
// reltuples = 0 over nonzero relpages. The planner then prices the refilled table as
// empty, so any statement whose plan can walk the table does so once per joined row.
func TestQueueStatementsStayKeyedUnderEmptyStatistics(t *testing.T) {
	s, rec := openExplainedStore(t)
	ctx := context.Background()
	exec := func(q string) {
		t.Helper()
		_, err := s.db.ExecContext(ctx, q)
		require.NoError(t, err)
	}
	exec(`ALTER TABLE async_requests SET (autovacuum_enabled = false)`)
	exec(`ALTER TABLE async_results SET (autovacuum_enabled = false)`)
	t.Cleanup(func() {
		_, _ = s.db.ExecContext(ctx, `ALTER TABLE async_requests RESET (autovacuum_enabled)`)
		_, _ = s.db.ExecContext(ctx, `ALTER TABLE async_results RESET (autovacuum_enabled)`)
	})

	const rows, batch = 20000, 256
	now := time.Now()
	ownAll(t, s, "a", now)
	fill := func(round string) {
		t.Helper()
		reqs := make([]Request, 0, rows)
		for i := range rows {
			reqs = append(reqs, req(round+"-"+strconv.Itoa(i), now.Unix()+int64(i)))
		}
		require.NoError(t, s.Enqueue(ctx, reqs...))
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO async_results (route, id, request_token, payload, created_at)
			SELECT 'route', $1 || g, 't', '{}', 0 FROM generate_series(1, $2) g`, round, rows)
		require.NoError(t, err)
	}
	fill("drained")
	exec(`DELETE FROM async_requests`)
	exec(`DELETE FROM async_results`)
	exec(`ANALYZE async_requests, async_results, async_partitions`)
	var tuples float64
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT reltuples FROM pg_class WHERE relname = 'async_requests'`).Scan(&tuples))
	require.Zero(t, tuples, "statistics describe the drained table")
	fill("live")

	check := func(op string, fn func()) {
		t.Helper()
		rec.take()
		fn()
		plans := rec.take()
		require.NotEmpty(t, plans, op)
		for _, p := range plans {
			assert.LessOrEqual(t, p.Plan.queueRows(), float64(4*batch),
				"%s read queue rows in proportion to the table, not the batch:\n%s", op, p.Query)
		}
	}

	var dispatched []Request
	check("Dispatch", func() {
		var err error
		dispatched, err = s.Dispatch(ctx, "q", "a", now, batch)
		require.NoError(t, err)
		require.Len(t, dispatched, batch)
	})
	keys := make([]Key, batch)
	stamps := make([]Stamp, batch)
	for i, r := range dispatched {
		keys[i], stamps[i] = r.Key(), Stamp{Key: r.Key(), Attempt: r.Attempt}
	}
	check("InFlight", func() {
		inFlight, err := s.InFlight(ctx, "q", "a")
		require.NoError(t, err)
		require.Len(t, inFlight, batch)
	})
	check("CancelledKeys", func() {
		_, err := s.CancelledKeys(ctx, keys)
		require.NoError(t, err)
	})
	check("ResetStale", func() { require.NoError(t, s.ResetStale(ctx, "q", "a", nil)) })
	check("Undispatch", func() { require.NoError(t, s.Undispatch(ctx, "a", stamps[:batch/2])) })
	check("Ack", func() {
		comps := make([]Completion, 0, batch/2)
		for _, st := range stamps[batch/2:] {
			comps = append(comps, completion(st, "acked"))
		}
		acked, err := s.Ack(ctx, "a", comps)
		require.NoError(t, err)
		for _, ok := range acked {
			require.True(t, ok)
		}
	})
	check("PopResults", func() {
		popped, err := s.PopResults(ctx, "route", now.Unix(), batch)
		require.NoError(t, err)
		require.Len(t, popped, batch)
	})
}
