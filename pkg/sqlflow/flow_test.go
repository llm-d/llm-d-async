package sqlflow

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGateReleasesAreScopedToRequestAttempt(t *testing.T) {
	var first, second, redelivery atomic.Int32
	f := &Flow{}
	key1 := sqlqueue.Stamp{Key: sqlqueue.Key{ID: "same-id", Token: "generation-1"}, Attempt: 1}
	key2 := sqlqueue.Stamp{Key: sqlqueue.Key{ID: "same-id", Token: "generation-2"}, Attempt: 1}
	key3 := sqlqueue.Stamp{Key: key1.Key, Attempt: 2}

	f.trackGateReleases(key1, []pipeline.GateReleaseFunc{func() { first.Add(1) }})
	f.trackGateReleases(key2, []pipeline.GateReleaseFunc{func() { second.Add(1) }})
	f.trackGateReleases(key3, []pipeline.GateReleaseFunc{func() { redelivery.Add(1) }})
	assert.Zero(t, first.Load(), "tracking another generation must not release the first")
	assert.Zero(t, second.Load())
	assert.Zero(t, redelivery.Load(), "tracking a redelivery must not release its earlier attempt")

	f.releaseGateReleases(key1)
	assert.EqualValues(t, 1, first.Load())
	assert.Zero(t, second.Load(), "completing the first generation must not release the second")

	f.releaseGateReleases(key2)
	assert.EqualValues(t, 1, second.Load())

	f.releaseGateReleases(key3)
	assert.EqualValues(t, 1, redelivery.Load())
}

func TestRebalanceRunsQueuesConcurrently(t *testing.T) {
	store := cancelTestStore(t)
	ctx := context.Background()
	cfg := Config{LeaseTTLSeconds: 3}
	for _, name := range []string{"q1", "q2", "q3"} {
		cfg.Queues = append(cfg.Queues, QueueConfig{ID: name, QueueName: name, WorkerPoolID: "default"})
	}
	f, err := NewWithStore(context.Background(), store, cfg, []pipeline.WorkerPoolConfig{{ID: "default"}}, nil)
	require.NoError(t, err)
	t.Cleanup(f.cancelChecks.stop)
	t.Cleanup(f.quota.Close)
	require.Equal(t, time.Second, f.storeTimeout())

	db, err := sql.Open("pgx", os.Getenv("TEST_POSTGRES_URL"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(ctx, "LOCK TABLE async_partitions IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	start := time.Now()
	f.rebalance(ctx)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, f.storeTimeout(), "every queue waits on the lock until its store timeout")
	assert.Less(t, elapsed, 2*f.storeTimeout(), "queues share one heartbeat pass instead of queuing behind each other")
}

type refusingGate struct{ reason string }

func (g refusingGate) Budget(context.Context) float64 { return 1 }

func (g refusingGate) Apply(_ context.Context, msg *api.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	msg.SetClassification(api.ClassificationOverflow)
	return pipeline.Refuse().WithReason(g.reason), nil
}

func TestRefusalIsLabelledByTheGatesReason(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		want   string
	}{
		{name: "overflow refused by a later gate", reason: "", want: metrics.ReasonGateClosed},
		{name: "quota refusal", reason: metrics.ReasonQuotaExhausted, want: metrics.ReasonQuotaExhausted},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := cancelTestStore(t)
			ctx := context.Background()
			queue := fmt.Sprintf("refuse-%d", i)
			f, err := NewWithStore(ctx, store, Config{Queues: []QueueConfig{{ID: queue, QueueName: queue, WorkerPoolID: "default"}}}, []pipeline.WorkerPoolConfig{{ID: "default"}}, nil)
			require.NoError(t, err)
			t.Cleanup(f.cancelChecks.stop)
			t.Cleanup(f.quota.Close)
			q := f.queues[0]
			q.gate = refusingGate{reason: tc.reason}
			f.rebalance(ctx)

			deadline := time.Now().Add(time.Hour).Unix()
			envelope, payload, err := api.SplitPayload(api.NewInternalRequest(api.InternalRouting{RequestToken: "t1"},
				&api.RequestMessage{ID: "r1", Deadline: deadline, Payload: []byte("{}")}))
			require.NoError(t, err)
			require.NoError(t, store.Enqueue(ctx, sqlqueue.Request{
				ID: "r1", Token: "t1", Queue: queue, Deadline: deadline, Envelope: string(envelope), Payload: payload,
			}))
			counted := func(reason string) float64 {
				return testutil.ToFloat64(metrics.GateDecisions.WithLabelValues(queue, queue, "default", reason))
			}
			reasons := []string{metrics.ReasonGateClosed, metrics.ReasonQuotaExhausted}
			before := map[string]float64{}
			for _, r := range reasons {
				before[r] = counted(r)
			}
			f.processQueue(ctx, q, testr.New(t))

			for _, r := range reasons {
				want := 0.0
				if r == tc.want {
					want = 1
				}
				assert.Equal(t, want, counted(r)-before[r], "refusals counted under %s", r)
			}
		})
	}
}

func quotaGatedFlow(t *testing.T, rows, limit int) (*Flow, *queueRuntime, *sqlqueue.Store) {
	t.Helper()
	store := cancelTestStore(t)
	ctx := context.Background()
	f, err := NewWithStore(ctx, store, Config{BatchSize: rows, Queues: []QueueConfig{{ID: "q", QueueName: "q", WorkerPoolID: "default"}}}, []pipeline.WorkerPoolConfig{{ID: "default"}}, nil)
	require.NoError(t, err)
	t.Cleanup(f.cancelChecks.stop)
	t.Cleanup(f.quota.Close)
	q := f.queues[0]
	q.gate = flowcontrol.NewQuotaGate(f.quota, "userid", flowcontrol.QuotaModeConcurrency, limit, time.Minute, "quota:")
	f.rebalance(ctx)

	deadline := time.Now().Add(time.Hour).Unix()
	reqs := make([]sqlqueue.Request, 0, rows)
	for i := range rows {
		id := fmt.Sprintf("r%02d", i)
		envelope, payload, err := api.SplitPayload(api.NewInternalRequest(api.InternalRouting{RequestToken: "t"},
			&api.RequestMessage{ID: id, Deadline: deadline, Payload: []byte("{}"), Metadata: map[string]string{"userid": "alice"}}))
		require.NoError(t, err)
		reqs = append(reqs, sqlqueue.Request{ID: id, Token: "t", Queue: "q", Deadline: deadline, Envelope: string(envelope), Payload: payload})
	}
	require.NoError(t, store.Enqueue(ctx, reqs...))
	return f, q, store
}

func TestQueueGateCountsAPollInFewStatements(t *testing.T) {
	const rows, limit = 64, 10
	f, q, store := quotaGatedFlow(t, rows, limit)
	ctx := context.Background()
	refused := func() float64 {
		return testutil.ToFloat64(metrics.GateDecisions.WithLabelValues("q", "q", "default", metrics.ReasonQuotaExhausted))
	}
	before := refused()

	var dispatched []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ir := range q.channel.Channel {
			dispatched = append(dispatched, ir.PublicRequest.ReqID())
		}
	}()
	f.processQueue(ctx, q, testr.New(t))
	close(q.channel.Channel)
	<-done

	assert.LessOrEqual(t, f.quota.slots.statements.Load(), int64(rows/4), "one poll shares statements per key instead of one per request")
	assert.Len(t, dispatched, limit, "exactly the limit is admitted")
	assert.Equal(t, float64(rows-limit), refused()-before)
	depth, _, err := store.Backlog(ctx, "q", time.Now().Unix(), nil)
	require.NoError(t, err)
	assert.EqualValues(t, rows-limit, depth, "refused requests are pending again")
}

func TestCancelledPollReturnsEveryRowAndSlot(t *testing.T) {
	const rows, limit = 16, 4
	f, q, store := quotaGatedFlow(t, rows, limit)
	ctx, cancel := context.WithCancel(context.Background())
	db := quotaDB(t)

	handed := make(chan *api.InternalRequest, 1)
	go func() {
		handed <- <-q.channel.Channel
		cancel()
	}()
	f.processQueue(ctx, q, testr.New(t))
	ir := <-handed

	depth, _, err := store.Backlog(context.Background(), "q", time.Now().Unix(), nil)
	require.NoError(t, err)
	assert.EqualValues(t, rows-1, depth, "every row but the one handed to a worker is pending again")
	eventuallySlots(t, db, "quota:userid:alice", 1)
	f.releaseGateReleases(sqlqueue.Stamp{Key: sqlqueue.Key{ID: ir.PublicRequest.ReqID(), Token: ir.RequestToken}, Attempt: ir.DispatchAttempt})
	eventuallySlots(t, db, "quota:userid:alice", 0)
}

func TestExpireResultsDrainsMoreThanOneBatch(t *testing.T) {
	store := cancelTestStore(t)
	ctx := context.Background()
	f, err := NewWithStore(ctx, store, Config{Queues: []QueueConfig{{ID: "q", QueueName: "q", WorkerPoolID: "default"}}}, []pipeline.WorkerPoolConfig{{ID: "default"}}, nil)
	require.NoError(t, err)
	t.Cleanup(f.cancelChecks.stop)
	t.Cleanup(f.quota.Close)

	db := quotaDB(t)
	const expired = resultExpiryBatch*2 + 7
	_, err = db.ExecContext(ctx, `
		INSERT INTO async_results (route, id, request_token, payload, expires_at, created_at)
		SELECT 'route-' || (g % 5), 'r' || g, 't', '{}', 1, 0 FROM generate_series(1, $1) g`, expired)
	require.NoError(t, err)

	f.expireResults(ctx)
	var left int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM async_results`).Scan(&left))
	assert.Zero(t, left)
}
