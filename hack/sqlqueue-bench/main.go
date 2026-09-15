// sqlqueue-bench drives the sql transport store with zero-cost inference to find where a database stops keeping up.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"golang.org/x/time/rate"
)

type opStats struct {
	mu      sync.Mutex
	samples []time.Duration
	errs    atomic.Int64
}

func (o *opStats) observe(d time.Duration, err error) {
	o.mu.Lock()
	o.samples = append(o.samples, d)
	o.mu.Unlock()
	if err != nil && !errors.Is(err, context.Canceled) {
		o.errs.Add(1)
	}
}

func (o *opStats) quantiles() (p50, p95, p99 time.Duration, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n = len(o.samples)
	if n == 0 {
		return 0, 0, 0, 0
	}
	sorted := append([]time.Duration(nil), o.samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(q float64) time.Duration { return sorted[min(n-1, int(float64(n)*q))] }
	return at(0.50), at(0.95), at(0.99), n
}

type bench struct {
	store       *sqlqueue.Store
	queue       string
	route       string
	payload     string
	batch       int
	ackBatch    int
	submitBatch int
	popBatch    int
	poll        time.Duration
	leaseTTL    time.Duration

	submitted, dispatched, acked, popped atomic.Int64

	enqueue, dispatch, ack, pop, rebalance, e2e opStats
}

func main() {
	dsn := flag.String("dsn", os.Getenv("SQL_URL"), "postgres:// or sqlite:// DSN (default $SQL_URL)")
	rateFlag := flag.Float64("rate", 200, "submit rate in requests/s")
	duration := flag.Duration("duration", 30*time.Second, "how long to submit; the run then drains")
	producers := flag.Int("producers", 4, "submitting goroutines")
	submitBatch := flag.Int("submit-batch", 100, "requests per enqueue statement")
	consumers := flag.Int("consumers", 4, "simulated dispatcher replicas sharing the queue")
	pollers := flag.Int("pollers", 1, "result poll loops")
	batch := flag.Int("batch", 32, "dispatch limit per poll")
	ackBatch := flag.Int("ack-batch", 32, "results per ack statement")
	popBatch := flag.Int("pop-batch", 64, "results per pop statement")
	poll := flag.Duration("poll", 100*time.Millisecond, "dispatch poll interval when a poll returns fewer than --batch rows")
	leaseTTL := flag.Duration("lease-ttl", 30*time.Second, "partition lease TTL; replicas heartbeat at a third of it")
	payloadBytes := flag.Int("payload-bytes", 2048, "approximate request payload size")
	drainTimeout := flag.Duration("drain-timeout", 2*time.Minute, "how long to wait for the backlog to clear after submitting stops")
	csv := flag.String("csv", "", "append a CSV row to this file")
	label := flag.String("label", "", "free-form label for the CSV row")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "sqlqueue-bench: --dsn or SQL_URL is required")
		os.Exit(2)
	}

	ctx := context.Background()
	store, err := sqlqueue.Open(ctx, *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()
	if err := store.TruncateForTest(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "truncate:", err)
		os.Exit(1)
	}

	b := &bench{
		store:       store,
		queue:       "bench-requests",
		route:       "bench-results",
		payload:     buildPayload(*payloadBytes),
		batch:       *batch,
		ackBatch:    *ackBatch,
		submitBatch: *submitBatch,
		popBatch:    *popBatch,
		poll:        *poll,
		leaseTTL:    *leaseTTL,
	}

	replicas := make([]*sqlqueue.Consumer, *consumers)
	for i := range replicas {
		replicas[i] = sqlqueue.NewConsumer(store, b.queue, fmt.Sprintf("replica-%d", i), b.leaseTTL, time.Minute)
	}
	if err := b.converge(ctx, replicas); err != nil {
		fmt.Fprintln(os.Stderr, "converge:", err)
		os.Exit(1)
	}

	xacts := openXactCounter(ctx, *dsn)
	xactsBefore := xacts.read(ctx)

	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, c := range replicas {
		wg.Add(1)
		go func() { defer wg.Done(); b.replica(runCtx, c) }()
	}
	for range *pollers {
		wg.Add(1)
		go func() { defer wg.Done(); b.poller(runCtx) }()
	}

	start := time.Now()
	submitCtx, stopSubmit := context.WithTimeout(ctx, *duration)
	limiter := rate.NewLimiter(rate.Limit(*rateFlag), max(*submitBatch, int(*rateFlag/10)))
	var pwg sync.WaitGroup
	for i := range *producers {
		pwg.Add(1)
		go func() { defer pwg.Done(); b.producer(submitCtx, i, limiter) }()
	}
	pwg.Wait()
	stopSubmit()
	submitElapsed := time.Since(start)

	drainDeadline := time.Now().Add(*drainTimeout)
	for b.popped.Load() < b.submitted.Load() && time.Now().Before(drainDeadline) {
		time.Sleep(100 * time.Millisecond)
	}
	total := time.Since(start)
	xactsAfter := xacts.read(ctx)
	stop()
	wg.Wait()
	for _, c := range replicas {
		_ = c.Close(ctx)
	}

	submitted, popped := b.submitted.Load(), b.popped.Load()
	drained := submitted == popped
	xactsPerReq := 0.0
	if xactsAfter > xactsBefore && popped > 0 {
		xactsPerReq = float64(xactsAfter-xactsBefore) / float64(popped)
	}
	fmt.Printf("\n=== sqlqueue-bench %s ===\n", *label)
	fmt.Printf("target rate: %.0f req/s  producers=%d submit_batch=%d consumers=%d pollers=%d batch=%d ack_batch=%d pop_batch=%d poll=%s\n",
		*rateFlag, *producers, *submitBatch, *consumers, *pollers, *batch, *ackBatch, *popBatch, *poll)
	fmt.Printf("submitted=%d dispatched=%d acked=%d popped=%d drained=%v db_xacts_per_req=%.3f\n",
		submitted, b.dispatched.Load(), b.acked.Load(), popped, drained, xactsPerReq)
	fmt.Printf("achieved submit rate: %.1f req/s over %s\n", float64(submitted)/submitElapsed.Seconds(), submitElapsed.Round(time.Millisecond))
	fmt.Printf("achieved completion rate: %.1f req/s over %s (backlog left: %d)\n",
		float64(popped)/total.Seconds(), total.Round(time.Millisecond), submitted-popped)
	fmt.Printf("%-13s %8s %8s %8s %8s %6s\n", "op", "p50", "p95", "p99", "n", "errs")
	rows := []struct {
		name string
		st   *opStats
	}{{"enqueue", &b.enqueue}, {"dispatch", &b.dispatch}, {"ack", &b.ack}, {"pop", &b.pop}, {"rebalance", &b.rebalance}, {"submit_to_pop", &b.e2e}}
	var csvCells []string
	calls := 0
	for _, r := range rows {
		p50, p95, p99, n := r.st.quantiles()
		fmt.Printf("%-13s %8s %8s %8s %8d %6d\n", r.name, ms(p50), ms(p95), ms(p99), n, r.st.errs.Load())
		csvCells = append(csvCells, ms(p50), ms(p95), ms(p99))
		if r.st != &b.e2e {
			calls += n
		}
	}
	callsPerReq := 0.0
	if popped > 0 {
		callsPerReq = float64(calls) / float64(popped)
	}
	fmt.Printf("store calls per request: %.3f\n", callsPerReq)
	if *csv != "" {
		f, err := os.OpenFile(*csv, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, "csv:", err)
			os.Exit(1)
		}
		var werr error
		if st, _ := f.Stat(); st.Size() == 0 {
			_, werr = fmt.Fprintln(f, "label,target_rate,consumers,batch,submit_batch,poll_ms,submitted,popped,drained,submit_rate,completion_rate,calls_per_req,xacts_per_req,"+
				"enqueue_p50,enqueue_p95,enqueue_p99,dispatch_p50,dispatch_p95,dispatch_p99,ack_p50,ack_p95,ack_p99,"+
				"pop_p50,pop_p95,pop_p99,rebalance_p50,rebalance_p95,rebalance_p99,e2e_p50,e2e_p95,e2e_p99")
		}
		if werr == nil {
			_, werr = fmt.Fprintf(f, "%s,%.0f,%d,%d,%d,%d,%d,%d,%v,%.1f,%.1f,%.3f,%.3f,%s\n", *label, *rateFlag, *consumers, *batch, *submitBatch, poll.Milliseconds(),
				submitted, popped, drained, float64(submitted)/submitElapsed.Seconds(), float64(popped)/total.Seconds(), callsPerReq, xactsPerReq, strings.Join(csvCells, ","))
		}
		if err := errors.Join(werr, f.Close()); err != nil {
			fmt.Fprintln(os.Stderr, "csv:", err)
			os.Exit(1)
		}
	}
	if !drained {
		os.Exit(3)
	}
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1f", float64(d.Microseconds())/1000) }

func buildPayload(size int) string {
	prompt := strings.Repeat("word ", max(1, size/5))
	envelope := map[string]any{
		"internal":     map[string]any{"request_queue_name": "bench-requests", "result_queue_name": "bench-results"},
		"request_kind": "plain",
		"data": map[string]any{
			"id": "placeholder", "created": 0, "deadline": 0,
			"payload": map[string]any{"model": "bench", "prompt": prompt, "max_tokens": 1},
		},
	}
	out, err := json.Marshal(envelope)
	if err != nil {
		return "{}"
	}
	return string(out)
}

func (b *bench) converge(ctx context.Context, replicas []*sqlqueue.Consumer) error {
	for range 4 {
		for _, c := range replicas {
			if err := c.Rebalance(ctx, time.Now()); err != nil {
				return fmt.Errorf("rebalance: %w", err)
			}
		}
	}
	return nil
}

func (b *bench) producer(ctx context.Context, id int, limiter *rate.Limiter) {
	for seq := 0; ; {
		if err := limiter.WaitN(ctx, b.submitBatch); err != nil {
			return
		}
		now := time.Now()
		reqs := make([]sqlqueue.Request, b.submitBatch)
		for i := range reqs {
			reqs[i] = sqlqueue.Request{
				ID:       fmt.Sprintf("p%d-%d", id, seq),
				Token:    fmt.Sprintf("%d", now.UnixNano()),
				Queue:    b.queue,
				Deadline: now.Unix() + 3600,
				Payload:  b.payload,
			}
			seq++
		}
		t := time.Now()
		err := b.store.Enqueue(context.Background(), reqs...)
		b.enqueue.observe(time.Since(t), err)
		if err == nil {
			b.submitted.Add(int64(len(reqs)))
		}
	}
}

func (b *bench) replica(ctx context.Context, c *sqlqueue.Consumer) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(b.leaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t := time.Now()
				err := c.Rebalance(ctx, time.Now())
				b.rebalance.observe(time.Since(t), err)
			}
		}
	}()

	results := make(chan sqlqueue.Request, 4*b.batch)
	go func() {
		defer wg.Done()
		for {
			var first sqlqueue.Request
			select {
			case <-ctx.Done():
				return
			case first = <-results:
			}
			completions := []sqlqueue.Completion{b.completion(first)}
		fill:
			for len(completions) < b.ackBatch {
				select {
				case r := <-results:
					completions = append(completions, b.completion(r))
				default:
					break fill
				}
			}
			t := time.Now()
			acked, err := c.Ack(ctx, completions)
			b.ack.observe(time.Since(t), err)
			if err != nil {
				c.Abandon(keysOf(completions)...)
				continue
			}
			for _, ok := range acked {
				if ok {
					b.acked.Add(1)
				}
			}
		}
	}()

	for ctx.Err() == nil {
		t := time.Now()
		rows, err := c.Poll(ctx, time.Now(), b.batch)
		b.dispatch.observe(time.Since(t), err)
		b.dispatched.Add(int64(len(rows)))
		for _, r := range rows {
			select {
			case results <- r:
			case <-ctx.Done():
			}
		}
		if err != nil || len(rows) < b.batch {
			sleepCtx(ctx, b.poll)
		}
	}
	wg.Wait()
}

func (b *bench) completion(r sqlqueue.Request) sqlqueue.Completion {
	return sqlqueue.Completion{
		Key:     r.Key(),
		Route:   b.route,
		Payload: fmt.Sprintf(`{"id":%q,"status_code":200,"payload":"{}","request_token":%q,"submitted_ns":%s}`, r.ID, r.Token, r.Token),
	}
}

func keysOf(completions []sqlqueue.Completion) []sqlqueue.Key {
	keys := make([]sqlqueue.Key, len(completions))
	for i, c := range completions {
		keys[i] = c.Key
	}
	return keys
}

func (b *bench) poller(ctx context.Context) {
	for ctx.Err() == nil {
		t := time.Now()
		res, err := b.store.PopResults(ctx, b.route, time.Now().Unix(), b.popBatch)
		b.pop.observe(time.Since(t), err)
		if err != nil || len(res) == 0 {
			sleepCtx(ctx, 50*time.Millisecond)
			continue
		}
		b.popped.Add(int64(len(res)))
		for _, r := range res {
			var env struct {
				SubmittedNS int64 `json:"submitted_ns"`
			}
			if json.Unmarshal([]byte(r.Payload), &env) == nil && env.SubmittedNS > 0 {
				b.e2e.observe(time.Since(time.Unix(0, env.SubmittedNS)), nil)
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

type xactCounter struct{ db *sql.DB }

func openXactCounter(ctx context.Context, dsn string) xactCounter {
	if d, err := sqlqueue.DialectFromDSN(dsn); err != nil || d != sqlqueue.DialectPostgres {
		return xactCounter{}
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil || db.PingContext(ctx) != nil {
		return xactCounter{}
	}
	return xactCounter{db: db}
}

func (x xactCounter) read(ctx context.Context) int64 {
	if x.db == nil {
		return 0
	}
	var n int64
	if _, err := x.db.ExecContext(ctx, `SELECT pg_stat_clear_snapshot()`); err != nil {
		return 0
	}
	if err := x.db.QueryRowContext(ctx, `SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()`).Scan(&n); err != nil {
		return 0
	}
	return n
}
