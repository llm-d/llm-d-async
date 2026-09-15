//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	randomrobin "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	"github.com/llm-d/llm-d-async/pkg/sqlflow"
	producersql "github.com/llm-d/llm-d-async/producer-sql"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sqlRequestQueue = "sql-requests"
	sqlResultQueue  = "sql-results"
)

func sqlDSNs(t *testing.T) map[string]string {
	t.Helper()
	dsns := map[string]string{"sqlite": "sqlite://" + filepath.Join(t.TempDir(), "queue.db")}
	if pg := os.Getenv("TEST_POSTGRES_URL"); pg != "" {
		truncatePostgres(t, pg)
		dsns["postgres"] = pg
	}
	return dsns
}

func truncatePostgres(t *testing.T, dsn string) {
	t.Helper()
	store, err := sqlqueue.Open(context.Background(), dsn)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.TruncateForTest(context.Background()))
}

func forEachSQLDialect(t *testing.T, fn func(t *testing.T, dsn string)) {
	t.Helper()
	for name, dsn := range sqlDSNs(t) {
		t.Run(name, func(t *testing.T) { fn(t, dsn) })
	}
}

type sqlHarness struct {
	flow     *sqlflow.Flow
	producer *producersql.Producer
	stop     func()
}

func startSQLFlow(t *testing.T, dsn, igwURL string, leaseTTLSeconds int64, poll time.Duration) *sqlHarness {
	t.Helper()
	return startSQLFlowWorkers(t, dsn, igwURL, leaseTTLSeconds, poll, 1)
}

func startSQLFlowWorkers(t *testing.T, dsn, igwURL string, leaseTTLSeconds int64, poll time.Duration, workers int) *sqlHarness {
	t.Helper()
	cfg := sqlflow.Config{
		URL:             dsn,
		ResultQueueName: sqlResultQueue,
		PollIntervalMs:  int(poll.Milliseconds()),
		BatchSize:       10,
		LeaseTTLSeconds: leaseTTLSeconds,
		Queues: []sqlflow.QueueConfig{{
			QueueName:  sqlRequestQueue,
			IGWBaseURL: igwURL,
		}},
	}
	cfg.ApplyDefaults()
	require.NoError(t, cfg.Validate())
	pools := []pipeline.WorkerPoolConfig{{ID: "default", Workers: workers}}
	flow, err := sqlflow.New(context.Background(), cfg, pools, nil)
	require.NoError(t, err)

	poolMap := map[string]pipeline.WorkerPoolConfig{"default": pools[0]}
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flow.RequestChannels(), poolMap)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			asyncworker.WorkerWithGate(workerCtx, workerCtx, flow.Characteristics(),
				asyncworker.NewHTTPInferenceClient(http.DefaultClient), dispatch.Channels["default"],
				flow.RetryChannel(), flow.ResultChannel(), 30*time.Second, nil, nil)
		}()
	}
	flow.Start(context.Background())

	p, err := producersql.New(context.Background(), producersql.Config{
		URL: dsn, RequestQueueName: sqlRequestQueue, ResultQueueName: sqlResultQueue, PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var once sync.Once
	stop := func() {
		once.Do(func() {
			flow.StopConsuming()
			workerCancel()
			wg.Wait()
			flow.Shutdown()
			_ = p.Close()
		})
	}
	t.Cleanup(stop)
	return &sqlHarness{flow: flow, producer: p, stop: stop}
}

func newRequest(id string, deadline time.Time) *api.RequestMessage {
	return &api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: deadline.Unix(),
		Payload:  map[string]any{"model": "test", "prompt": id},
	}
}

func getResult(t *testing.T, p *producersql.Producer, timeout time.Duration) *api.ResultMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := p.GetResult(ctx)
	require.NoError(t, err)
	return res
}

func recordingServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		mu.Lock()
		seen = append(seen, payload["prompt"].(string))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func TestSQLFlow_DispatchesEarliestDeadlineFirst(t *testing.T) {
	forEachSQLDialect(t, func(t *testing.T, dsn string) {
		srv, seen := recordingServer(t)
		p, err := producersql.New(context.Background(), producersql.Config{
			URL: dsn, RequestQueueName: sqlRequestQueue, ResultQueueName: sqlResultQueue,
		})
		require.NoError(t, err)
		defer func() { _ = p.Close() }()
		now := time.Now()
		require.NoError(t, p.SubmitRequest(context.Background(), newRequest("late", now.Add(30*time.Minute))))
		require.NoError(t, p.SubmitRequest(context.Background(), newRequest("early", now.Add(5*time.Minute))))
		require.NoError(t, p.SubmitRequest(context.Background(), newRequest("mid", now.Add(10*time.Minute))))

		h := startSQLFlow(t, dsn, srv.URL, 60, 100*time.Millisecond)
		got := map[string]*api.ResultMessage{}
		for range 3 {
			res := getResult(t, h.producer, 10*time.Second)
			got[res.ID] = res
		}
		for _, id := range []string{"early", "mid", "late"} {
			require.Contains(t, got, id)
			assert.Equal(t, http.StatusOK, got[id].StatusCode)
			assert.JSONEq(t, `{"result":"ok"}`, got[id].Payload)
			assert.Empty(t, got[id].ErrorCode)
		}
		assert.Equal(t, []string{"early", "mid", "late"}, seen())

		store, err := sqlqueue.Open(context.Background(), dsn)
		require.NoError(t, err)
		defer func() { _ = store.Close() }()
		depth, err := store.ResultDepth(context.Background(), sqlResultQueue)
		require.NoError(t, err)
		assert.Zero(t, depth, "results are consumed destructively")
	})
}

func TestSQLFlow_CancelBeforeDispatch(t *testing.T) {
	forEachSQLDialect(t, func(t *testing.T, dsn string) {
		srv, seen := recordingServer(t)
		p, err := producersql.New(context.Background(), producersql.Config{
			URL: dsn, RequestQueueName: sqlRequestQueue, ResultQueueName: sqlResultQueue,
		})
		require.NoError(t, err)
		defer func() { _ = p.Close() }()
		require.NoError(t, p.SubmitRequest(context.Background(), newRequest("doomed", time.Now().Add(time.Hour))))
		require.NoError(t, p.CancelRequests(context.Background(), []string{"doomed", "never-existed"}))

		h := startSQLFlow(t, dsn, srv.URL, 60, 100*time.Millisecond)
		res := getResult(t, h.producer, 10*time.Second)
		assert.Equal(t, "doomed", res.ID)
		assert.Equal(t, api.ErrCodeCancelled, res.ErrorCode)
		assert.Zero(t, res.StatusCode)
		assert.Empty(t, seen(), "cancelled request never reached inference")
	})
}

func TestSQLFlow_ExpiredDeadlineTerminatesWithResult(t *testing.T) {
	forEachSQLDialect(t, func(t *testing.T, dsn string) {
		srv, seen := recordingServer(t)
		p, err := producersql.New(context.Background(), producersql.Config{
			URL: dsn, RequestQueueName: sqlRequestQueue, ResultQueueName: sqlResultQueue,
		})
		require.NoError(t, err)
		defer func() { _ = p.Close() }()
		require.NoError(t, p.SubmitRequest(context.Background(), newRequest("stale", time.Now().Add(time.Second))))
		time.Sleep(1500 * time.Millisecond)

		h := startSQLFlow(t, dsn, srv.URL, 60, 100*time.Millisecond)
		res := getResult(t, h.producer, 10*time.Second)
		assert.Equal(t, "stale", res.ID)
		assert.Equal(t, api.ErrCodeDeadlineExceeded, res.ErrorCode)
		assert.Empty(t, seen())
	})
}

func TestSQLFlow_RetriesTransientFailure(t *testing.T) {
	forEachSQLDialect(t, func(t *testing.T, dsn string) {
		var mu sync.Mutex
		calls := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":"second try"}`))
		}))
		defer srv.Close()

		h := startSQLFlow(t, dsn, srv.URL, 60, 100*time.Millisecond)
		require.NoError(t, h.producer.SubmitRequest(context.Background(), newRequest("flaky", time.Now().Add(time.Hour))))
		res := getResult(t, h.producer, 30*time.Second)
		assert.Equal(t, "flaky", res.ID)
		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.JSONEq(t, `{"result":"second try"}`, res.Payload)
		mu.Lock()
		assert.Equal(t, 2, calls)
		mu.Unlock()
	})
}

func TestSQLFlow_PeerTakesOverLapsedLease(t *testing.T) {
	forEachSQLDialect(t, func(t *testing.T, dsn string) {
		release := make(chan struct{})
		var stuckOnce sync.Once
		stuck := make(chan struct{})
		slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			stuckOnce.Do(func() { close(stuck) })
			<-release
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":"from the dead"}`))
		}))
		defer slow.Close()
		fast, seen := recordingServer(t)

		dead := startSQLFlow(t, dsn, slow.URL, 1, 100*time.Millisecond)
		require.NoError(t, dead.producer.SubmitRequest(context.Background(), newRequest("orphan", time.Now().Add(time.Hour))))
		select {
		case <-stuck:
		case <-time.After(10 * time.Second):
			t.Fatal("first instance never dispatched")
		}
		dead.flow.StopHeartbeatForTest()

		survivor := startSQLFlow(t, dsn, fast.URL, 1, 100*time.Millisecond)
		res := getResult(t, survivor.producer, 15*time.Second)
		assert.Equal(t, "orphan", res.ID)
		assert.JSONEq(t, `{"result":"ok"}`, res.Payload)
		assert.Equal(t, []string{"orphan"}, seen())

		close(release)
		dead.stop()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := survivor.producer.GetResult(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func collectResults(t *testing.T, p *producersql.Producer, n int, timeout time.Duration) map[string]*api.ResultMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	got := map[string]*api.ResultMessage{}
	for len(got) < n {
		batch, err := p.GetResults(ctx, 16)
		require.NoError(t, err, "collected %d of %d results", len(got), n)
		for _, res := range batch {
			require.NotContains(t, got, res.ID, "duplicate result for %s", res.ID)
			got[res.ID] = res
		}
	}
	return got
}

func TestSQLFlow_InstancesSplitPartitionsWithoutDuplicates(t *testing.T) {
	forEachSQLDialect(t, func(t *testing.T, dsn string) {
		srvA, seenA := recordingServer(t)
		srvB, seenB := recordingServer(t)
		a := startSQLFlowWorkers(t, dsn, srvA.URL, 1, 50*time.Millisecond, 4)
		startSQLFlowWorkers(t, dsn, srvB.URL, 1, 50*time.Millisecond, 4)
		time.Sleep(2 * time.Second)

		const n = 300
		reqs := make([]api.Request, 0, n)
		want := make([]string, 0, n)
		for i := range n {
			id := fmt.Sprintf("split-%d", i)
			reqs = append(reqs, newRequest(id, time.Now().Add(time.Hour)))
			want = append(want, id)
		}
		require.NoError(t, a.producer.SubmitRequests(context.Background(), reqs))

		got := collectResults(t, a.producer, n, 30*time.Second)
		for _, id := range want {
			require.Contains(t, got, id)
			assert.Equal(t, http.StatusOK, got[id].StatusCode, id)
		}
		fromA, fromB := seenA(), seenB()
		assert.NotEmpty(t, fromA, "a kept a share")
		assert.NotEmpty(t, fromB, "b took a share")
		assert.ElementsMatch(t, want, append(fromA, fromB...), "every request reached inference exactly once")
	})
}

func TestSQLFlow_StopHandsOffPartitionsAfterInFlightWork(t *testing.T) {
	forEachSQLDialect(t, func(t *testing.T, dsn string) {
		release := make(chan struct{})
		var mu sync.Mutex
		var seenA []string
		srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			var payload map[string]any
			_ = json.Unmarshal(body, &payload)
			mu.Lock()
			seenA = append(seenA, payload["prompt"].(string))
			mu.Unlock()
			<-release
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":"slow"}`))
		}))
		defer srvA.Close()
		srvB, seenB := recordingServer(t)

		a := startSQLFlow(t, dsn, srvA.URL, 1, 50*time.Millisecond)
		require.NoError(t, a.producer.SubmitRequest(context.Background(), newRequest("in-flight", time.Now().Add(time.Hour))))
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(seenA) == 1
		}, 10*time.Second, 20*time.Millisecond, "a never dispatched")

		a.flow.StopConsuming()
		b := startSQLFlowWorkers(t, dsn, srvB.URL, 1, 50*time.Millisecond, 4)
		const n = 40
		reqs := make([]api.Request, 0, n)
		for i := range n {
			reqs = append(reqs, newRequest(fmt.Sprintf("after-stop-%d", i), time.Now().Add(time.Hour)))
		}
		require.NoError(t, b.producer.SubmitRequests(context.Background(), reqs))

		require.Eventually(t, func() bool { return len(seenB()) >= n*3/4 }, 15*time.Second, 50*time.Millisecond)
		close(release)
		got := collectResults(t, b.producer, n+1, 30*time.Second)
		assert.JSONEq(t, `{"result":"slow"}`, got["in-flight"].Payload, "a finished and acked its own in-flight request")
		assert.NotContains(t, seenB(), "in-flight", "the handoff did not redeliver in-flight work")
		assert.Len(t, seenB(), n)
		mu.Lock()
		assert.Equal(t, []string{"in-flight"}, seenA, "a dispatched nothing after it stopped consuming")
		mu.Unlock()
		a.stop()
	})
}
