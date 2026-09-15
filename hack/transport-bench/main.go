// transport-bench runs real dispatcher flows end to end against each transport with an inference backend that answers immediately.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	randomrobin "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	"github.com/llm-d/llm-d-async/pkg/redis"
	"github.com/llm-d/llm-d-async/pkg/sqlflow"
	"github.com/llm-d/llm-d-async/producer"
	producersql "github.com/llm-d/llm-d-async/producer-sql"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
)

const (
	requestQueue = "bench-requests"
	resultQueue  = "bench-results"
)

type flow interface {
	pipeline.Flow
	StopConsuming()
	Shutdown()
}

type client interface {
	submit(ctx context.Context, reqs []api.Request) error
	results(ctx context.Context) ([]*api.ResultMessage, error)
	close()
}

type sqlClient struct{ p *producersql.Producer }

func (c sqlClient) submit(ctx context.Context, reqs []api.Request) error {
	if err := c.p.SubmitRequests(ctx, reqs); err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	return nil
}

func (c sqlClient) results(ctx context.Context) ([]*api.ResultMessage, error) {
	res, err := c.p.GetResults(ctx, 64)
	if err != nil {
		return res, fmt.Errorf("results: %w", err)
	}
	return res, nil
}

func (c sqlClient) close() { _ = c.p.Close() }

type redisClient struct {
	p *producer.RedisSortedSetProducer
}

func (c redisClient) submit(ctx context.Context, reqs []api.Request) error {
	for _, r := range reqs {
		if err := c.p.SubmitRequest(ctx, r); err != nil {
			return fmt.Errorf("submit: %w", err)
		}
	}
	return nil
}

func (c redisClient) results(ctx context.Context) ([]*api.ResultMessage, error) {
	res, err := c.p.GetResult(ctx)
	if err != nil {
		return nil, fmt.Errorf("results: %w", err)
	}
	return []*api.ResultMessage{res}, nil
}

func (c redisClient) close() { _ = c.p.Close() }

func main() {
	transports := flag.String("transports", "sql", "comma-separated transports: sql, redis-sortedset")
	sqlURL := flag.String("sql-url", os.Getenv("SQL_URL"), "postgres:// or sqlite:// URL (default $SQL_URL)")
	redisURL := flag.String("redis-url", os.Getenv("REDIS_URL"), "redis:// URL (default $REDIS_URL)")
	rates := flag.String("rates", "1000", "comma-separated submit rates in requests/s")
	duration := flag.Duration("duration", 30*time.Second, "how long to submit; the run then drains")
	replicas := flag.Int("replicas", 4, "dispatcher flows sharing the queue")
	workers := flag.Int("workers", 256, "inference workers per replica")
	batch := flag.Int("batch", 64, "batch_size per poll")
	pollMs := flag.Int("poll-ms", 10, "poll_interval_ms")
	producers := flag.Int("producers", 16, "submitting goroutines")
	submitBatch := flag.Int("submit-batch", 100, "requests per submit call (sql batches them in one transaction; redis submits one at a time)")
	collectors := flag.Int("collectors", 8, "result collecting goroutines")
	drainTimeout := flag.Duration("drain-timeout", 2*time.Minute, "how long to wait for results after submitting stops")
	csvPath := flag.String("csv", "transport-bench.csv", "append a CSV row per run to this file")
	dumpCSV := flag.Bool("dump-csv", false, "print the CSV file to stdout after the last run")
	flag.Parse()
	log.SetLogger(logr.Discard())
	for _, transport := range strings.Split(*transports, ",") {
		url := *sqlURL
		if transport == "redis-sortedset" {
			url = *redisURL
		}
		for _, r := range strings.Split(*rates, ",") {
			offered, err := strconv.ParseFloat(r, 64)
			if err != nil {
				fmt.Fprintln(os.Stderr, "transport-bench: bad rate:", r)
				os.Exit(2)
			}
			label := fmt.Sprintf("%s-%s", transport, r)
			if err := run(transport, url, offered, *duration, *replicas, *workers, *batch, *pollMs, *producers, *submitBatch, *collectors, *drainTimeout, *csvPath, label); err != nil {
				fmt.Fprintln(os.Stderr, "transport-bench:", label, err)
			}
		}
	}
	if *dumpCSV {
		data, err := os.ReadFile(*csvPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "transport-bench: read csv:", err)
			os.Exit(1)
		}
		fmt.Printf("=== BEGIN CSV ===\n%s=== END CSV ===\n", data)
	}
}

func run(transport, url string, offered float64, duration time.Duration, replicas, workers, batch, pollMs, producers, submitBatch, collectors int, drainTimeout time.Duration, csvPath, label string) error {
	ctx := context.Background()
	if err := reset(ctx, transport, url); err != nil {
		return err
	}

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices":[{"text":"ok"}]}`))
	}))
	defer backend.Close()
	httpClient := &http.Client{Transport: &http.Transport{MaxIdleConns: 4096, MaxIdleConnsPerHost: 4096}}

	type running struct {
		f      flow
		cancel context.CancelFunc
		wg     *sync.WaitGroup
	}
	var flows []running
	for range replicas {
		f, err := newFlow(ctx, transport, url, backend.URL, batch, pollMs, workers)
		if err != nil {
			return err
		}
		pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: workers}}
		dispatch := randomrobin.NewRandomRobinPolicy("bench", randomrobin.Config{}).MergeRequestChannels(f.RequestChannels(), pools)
		wctx, cancel := context.WithCancel(ctx)
		wg := &sync.WaitGroup{}
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				asyncworker.WorkerWithGate(wctx, wctx, f.Characteristics(), asyncworker.NewHTTPInferenceClient(httpClient),
					dispatch.Channels["default"], f.RetryChannel(), f.ResultChannel(), 30*time.Second, nil, nil)
			}()
		}
		f.Start(ctx)
		flows = append(flows, running{f: f, cancel: cancel, wg: wg})
	}
	time.Sleep(3 * time.Second)

	c, err := newClient(ctx, transport, url)
	if err != nil {
		return err
	}
	defer c.close()

	var submitted, completed, dupes atomic.Int64
	var sent sync.Map
	var latMu sync.Mutex
	var latencies []time.Duration

	collectCtx, stopCollect := context.WithCancel(ctx)
	var cwg sync.WaitGroup
	for range collectors {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for collectCtx.Err() == nil {
				res, err := c.results(collectCtx)
				if err != nil && !errors.Is(err, context.Canceled) {
					continue
				}
				now := time.Now()
				for _, r := range res {
					v, ok := sent.LoadAndDelete(r.ID)
					if !ok {
						dupes.Add(1)
						continue
					}
					if t, ok := v.(time.Time); ok {
						latMu.Lock()
						latencies = append(latencies, now.Sub(t))
						latMu.Unlock()
					}
					completed.Add(1)
				}
			}
		}()
	}

	start := time.Now()
	submitCtx, stopSubmit := context.WithTimeout(ctx, duration)
	limiter := rate.NewLimiter(rate.Limit(offered), max(submitBatch, int(offered/10)))
	var submitErrs atomic.Int64
	var pwg sync.WaitGroup
	for p := range producers {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for seq := 0; ; {
				if limiter.WaitN(submitCtx, submitBatch) != nil {
					return
				}
				now := time.Now()
				reqs := make([]api.Request, submitBatch)
				for i := range reqs {
					id := fmt.Sprintf("p%d-%d", p, seq)
					seq++
					reqs[i] = &api.RequestMessage{ID: id, Created: now.Unix(), Deadline: now.Add(time.Hour).Unix(),
						Payload: map[string]any{"model": "bench", "prompt": strings.Repeat("word ", 400), "max_tokens": 1}}
					sent.Store(id, now)
				}
				if err := c.submit(context.Background(), reqs); err != nil {
					submitErrs.Add(1)
					for _, r := range reqs {
						sent.Delete(r.ReqID())
					}
					continue
				}
				submitted.Add(int64(len(reqs)))
			}
		}()
	}
	pwg.Wait()
	stopSubmit()
	submitElapsed := time.Since(start)
	deadline := time.Now().Add(drainTimeout)
	for completed.Load() < submitted.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	total := time.Since(start)
	stopCollect()
	cwg.Wait()
	for _, r := range flows {
		r.f.StopConsuming()
		r.cancel()
		r.wg.Wait()
		r.f.Shutdown()
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	q := func(p float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		return float64(latencies[min(len(latencies)-1, int(float64(len(latencies))*p))].Microseconds()) / 1000
	}
	sub, done := submitted.Load(), completed.Load()
	completion := float64(done) / total.Seconds()
	fmt.Printf("\n=== transport-bench %s ===\n", label)
	fmt.Printf("transport=%s rate=%.0f replicas=%d workers=%d batch=%d poll_ms=%d producers=%d submit_batch=%d\n",
		transport, offered, replicas, workers, batch, pollMs, producers, submitBatch)
	fmt.Printf("submitted=%d completed=%d duplicates=%d submit_errors=%d drained=%v\n", sub, done, dupes.Load(), submitErrs.Load(), sub == done)
	fmt.Printf("achieved submit rate: %.1f req/s over %s\n", float64(sub)/submitElapsed.Seconds(), submitElapsed.Round(time.Millisecond))
	fmt.Printf("achieved completion rate: %.1f req/s over %s (backlog left: %d)\n", completion, total.Round(time.Millisecond), sub-done)
	fmt.Printf("submit_to_result ms p50=%.1f p95=%.1f p99=%.1f\n", q(0.5), q(0.95), q(0.99))

	if csvPath == "" {
		return nil
	}
	f, err := os.OpenFile(csvPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("csv: %w", err)
	}
	var werr error
	if st, err := f.Stat(); err == nil && st.Size() == 0 {
		_, werr = fmt.Fprintln(f, "label,transport,target_rate,replicas,workers,submitted,completed,duplicates,drained,submit_rate,completion_rate,p50_ms,p95_ms,p99_ms")
	}
	if werr == nil {
		_, werr = fmt.Fprintf(f, "%s,%s,%.0f,%d,%d,%d,%d,%d,%v,%.1f,%.1f,%.1f,%.1f,%.1f\n", label, transport, offered, replicas, workers,
			sub, done, dupes.Load(), sub == done, float64(sub)/submitElapsed.Seconds(), completion, q(0.5), q(0.95), q(0.99))
	}
	if err := errors.Join(werr, f.Close()); err != nil {
		return fmt.Errorf("csv: %w", err)
	}
	return nil
}

func reset(ctx context.Context, transport, url string) error {
	switch transport {
	case "sql":
		store, err := sqlqueue.Open(ctx, url)
		if err != nil {
			return fmt.Errorf("open store: %w", err)
		}
		defer func() { _ = store.Close() }()
		if err := store.TruncateForTest(ctx); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}
		return nil
	case "redis-sortedset":
		opts, err := goredis.ParseURL(url)
		if err != nil {
			return fmt.Errorf("parse redis url: %w", err)
		}
		rdb := goredis.NewClient(opts)
		defer func() { _ = rdb.Close() }()
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			return fmt.Errorf("flush redis: %w", err)
		}
		return nil
	}
	return fmt.Errorf("unknown transport %q", transport)
}

func newFlow(ctx context.Context, transport, url, igw string, batch, pollMs, workers int) (flow, error) {
	pools := []pipeline.WorkerPoolConfig{{ID: "default", Workers: workers}}
	raw, err := json.Marshal(map[string]any{
		"url": url, "result_queue_name": resultQueue, "poll_interval_ms": pollMs, "batch_size": batch,
		"queues": []map[string]any{{"queue_name": requestQueue, "igw_base_url": igw}},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	switch transport {
	case "sql":
		cfg, err := sqlflow.LoadConfig(raw)
		if err != nil {
			return nil, fmt.Errorf("sql config: %w", err)
		}
		f, err := sqlflow.New(ctx, *cfg, pools, nil)
		if err != nil {
			return nil, fmt.Errorf("sql flow: %w", err)
		}
		return f, nil
	case "redis-sortedset":
		cfg, err := redis.LoadSortedSetConfig(raw)
		if err != nil {
			return nil, fmt.Errorf("redis config: %w", err)
		}
		f, err := redis.NewRedisSortedSetFlow(*cfg, pools, nil)
		if err != nil {
			return nil, fmt.Errorf("redis flow: %w", err)
		}
		return f, nil
	}
	return nil, fmt.Errorf("unknown transport %q", transport)
}

func newClient(ctx context.Context, transport, url string) (client, error) {
	switch transport {
	case "sql":
		p, err := producersql.New(ctx, producersql.Config{URL: url, RequestQueueName: requestQueue, ResultQueueName: resultQueue, PollInterval: 10 * time.Millisecond})
		if err != nil {
			return nil, fmt.Errorf("sql producer: %w", err)
		}
		return sqlClient{p: p}, nil
	case "redis-sortedset":
		p, err := producer.NewRedisSortedSetProducer(producer.RedisSortedSetConfig{RedisURL: url, RequestQueueName: requestQueue, ResultQueueName: resultQueue})
		if err != nil {
			return nil, fmt.Errorf("redis producer: %w", err)
		}
		return redisClient{p: p}, nil
	}
	return nil, fmt.Errorf("unknown transport %q", transport)
}
