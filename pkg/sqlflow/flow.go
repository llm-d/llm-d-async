// Package sqlflow is the sql transport flow.
package sqlflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"go.opentelemetry.io/otel"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const (
	resultChannelBuffer = 64
	storeAttempts       = 3
	storeBackoff        = 100 * time.Millisecond
	releaseTimeout      = 5 * time.Second
)

var (
	_ pipeline.Flow                        = (*Flow)(nil)
	_ pipeline.HealthChecker               = (*Flow)(nil)
	_ pipeline.CancellationCheckerProvider = (*Flow)(nil)
	_ pipeline.BacklogReporter             = (*Flow)(nil)
)

type queueRuntime struct {
	config   QueueConfig
	channel  pipeline.RequestChannel
	gate     pipeline.Gate
	consumer *sqlqueue.Consumer
}

type Flow struct {
	store         *sqlqueue.Store
	quota         *QuotaStore
	queues        []*queueRuntime
	queuesByID    map[string]*queueRuntime
	queuesByName  map[string]*queueRuntime
	retryChannel  chan pipeline.RetryMessage
	resultChannel chan api.ResultMessage

	pollInterval           time.Duration
	batchSize              int
	resultBatchSize        int
	leaseTTL               time.Duration
	handoffTimeout         time.Duration
	defaultResultQueueName string

	activeReleases sync.Map
	cancelChecks   *cancelBatcher

	consumeCancel context.CancelFunc
	consumeWg     sync.WaitGroup
	drainCancel   context.CancelFunc
	drainWg       sync.WaitGroup
	hbCancel      context.CancelFunc
	hbWg          sync.WaitGroup
}

func New(ctx context.Context, cfg Config, workerPools []pipeline.WorkerPoolConfig, gateFactory *flowcontrol.GateFactory) (*Flow, error) {
	var opts []sqlqueue.OpenOption
	if cfg.EnableTracing {
		opts = append(opts, sqlqueue.WithTracerProvider(otel.GetTracerProvider()))
	}
	store, err := sqlqueue.Open(ctx, cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("open sql transport store: %w", err)
	}
	f, err := NewWithStore(ctx, store, cfg, workerPools, gateFactory)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return f, nil
}

// NewWithStore registers the flow's quota store with gateFactory before it
// creates any queue gate, so sql-quota gates count in store.
func NewWithStore(ctx context.Context, store *sqlqueue.Store, cfg Config, workerPools []pipeline.WorkerPoolConfig, gateFactory *flowcontrol.GateFactory) (_ *Flow, err error) {
	owner, err := newOwner()
	if err != nil {
		return nil, err
	}
	f := &Flow{
		store:                  store,
		queuesByID:             map[string]*queueRuntime{},
		queuesByName:           map[string]*queueRuntime{},
		retryChannel:           make(chan pipeline.RetryMessage),
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           time.Duration(cfg.PollIntervalMs) * time.Millisecond,
		batchSize:              cfg.BatchSize,
		resultBatchSize:        cfg.ResultBatchSize,
		leaseTTL:               time.Duration(cfg.LeaseTTLSeconds) * time.Second,
		handoffTimeout:         time.Duration(cfg.HandoffTimeoutSeconds) * time.Second,
		defaultResultQueueName: cfg.ResultQueueName,
	}
	if f.pollInterval <= 0 {
		f.pollInterval = time.Second
	}
	if f.batchSize <= 0 {
		f.batchSize = 10
	}
	if f.resultBatchSize <= 0 {
		f.resultBatchSize = 32
	}
	if f.leaseTTL <= 0 {
		f.leaseTTL = 30 * time.Second
	}
	if f.handoffTimeout <= 0 {
		f.handoffTimeout = 15 * time.Minute
	}
	f.cancelChecks = newCancelBatcher(store, cfg.CancelCheckBatchSize, time.Duration(cfg.CancelCheckLingerMs)*time.Millisecond, f.storeTimeout())
	f.quota = NewQuotaStore(store, f.leaseTTL, f.storeTimeout(), log.FromContext(ctx))
	defer func() {
		if err != nil {
			f.cancelChecks.stop()
			f.quota.Close()
		}
	}()
	if gateFactory != nil {
		gateFactory.WithSQLQuota(f.quota)
	}
	for _, q := range cfg.Queues {
		if !poolExists(workerPools, q.WorkerPoolID) {
			return nil, fmt.Errorf("worker pool %q specified in queue config not found in pool configuration", q.WorkerPoolID)
		}
		gate := pipeline.ConstOpenGate()
		if gateFactory != nil && q.GateType != "" {
			gateCfg := q.GateConfig
			gateCfg.Owner = pipeline.GateOwner{QueueID: q.ID, QueueName: q.QueueName, WorkerPoolID: q.WorkerPoolID}
			created, err := gateFactory.CreateGate(gateCfg)
			if err != nil {
				return nil, fmt.Errorf("failed to create gate for queue %q (gate_type=%q): %w", q.QueueName, q.GateType, err)
			}
			gate = created
		}
		rt := &queueRuntime{
			config:   q,
			gate:     gate,
			consumer: sqlqueue.NewConsumer(store, q.QueueName, owner, f.leaseTTL, f.handoffTimeout),
			channel: pipeline.RequestChannel{
				Channel:            make(chan *api.InternalRequest),
				InferenceObjective: q.InferenceObjective,
				RequestPathURL:     q.RequestPathURL,
				IGWBaseURL:         q.IGWBaseURL,
				Gate:               gate,
				WorkerPoolID:       q.WorkerPoolID,
			},
		}
		f.queues = append(f.queues, rt)
		f.queuesByID[q.ID] = rt
		f.queuesByName[q.QueueName] = rt
	}
	return f, nil
}

func poolExists(pools []pipeline.WorkerPoolConfig, id string) bool {
	for _, p := range pools {
		if p.ID == id {
			return true
		}
	}
	return false
}

func newOwner() (string, error) {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate dispatcher owner: %w", err)
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "dispatcher"
	}
	return host + "-" + hex.EncodeToString(buf), nil
}

func (f *Flow) Characteristics() pipeline.Characteristics {
	return pipeline.Characteristics{HasExternalBackoff: false, SupportsMessageLatency: false}
}

func (f *Flow) RequestChannels() []pipeline.RequestChannel {
	out := make([]pipeline.RequestChannel, 0, len(f.queues))
	for _, q := range f.queues {
		out = append(out, q.channel)
	}
	return out
}

func (f *Flow) RetryChannel() chan pipeline.RetryMessage { return f.retryChannel }

func (f *Flow) ResultChannel() chan api.ResultMessage { return f.resultChannel }

func (f *Flow) HealthCheck(ctx context.Context) error {
	if err := f.store.Ping(ctx); err != nil {
		return fmt.Errorf("sql transport health: %w", err)
	}
	return nil
}

func (f *Flow) CancellationChecker() api.CancellationChecker {
	return cancellationChecker{batcher: f.cancelChecks}
}

type cancellationChecker struct{ batcher *cancelBatcher }

func (c cancellationChecker) IsCancelled(ctx context.Context, requestID, requestToken string) (bool, error) {
	if requestID == "" || requestToken == "" {
		return false, nil
	}
	cancelled, err := c.batcher.isCancelled(ctx, sqlqueue.Key{ID: requestID, Token: requestToken})
	if err != nil {
		return false, fmt.Errorf("check cancellation for %q: %w", requestID, err)
	}
	return cancelled, nil
}

func (f *Flow) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	consumeCtx, consumeCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	f.consumeCancel = consumeCancel
	drainCtx, drainCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	f.drainCancel = drainCancel
	hbCtx, hbCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	f.hbCancel = hbCancel

	for _, q := range f.queues {
		f.consumeWg.Add(1)
		go func(q *queueRuntime) {
			defer f.consumeWg.Done()
			f.requestWorker(consumeCtx, q)
		}(q)
	}
	f.drainWg.Add(2)
	go func() { defer f.drainWg.Done(); f.retryWorker(drainCtx) }()
	go func() { defer f.drainWg.Done(); f.resultWorker(drainCtx) }()
	f.hbWg.Add(1)
	go func() { defer f.hbWg.Done(); f.heartbeat(hbCtx) }()
}

func (f *Flow) StopConsuming() {
	if f.consumeCancel != nil {
		f.consumeCancel()
	}
	f.consumeWg.Wait()
	for _, q := range f.queues {
		q.consumer.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	f.rebalance(ctx)
}

func (f *Flow) StopHeartbeatForTest() {
	if f.hbCancel != nil {
		f.hbCancel()
	}
	f.hbWg.Wait()
}

func (f *Flow) Shutdown() {
	if f.drainCancel != nil {
		f.drainCancel()
	}
	f.drainWg.Wait()
	if f.hbCancel != nil {
		f.hbCancel()
	}
	f.hbWg.Wait()
	f.cancelChecks.stop()
	f.quota.Close()
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	for _, q := range f.queues {
		if err := q.consumer.Close(ctx); err != nil {
			log.FromContext(ctx).V(logutil.DEFAULT).Error(err, "Failed to release partitions; peers take over when the leases lapse", "queue", q.config.QueueName)
		}
	}
	_ = f.store.Close()
}

func (f *Flow) heartbeat(ctx context.Context) {
	interval := max(100*time.Millisecond, f.leaseTTL/3)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		f.rebalance(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (f *Flow) rebalance(ctx context.Context) {
	logger := log.FromContext(ctx)
	var wg sync.WaitGroup
	for _, q := range f.queues {
		wg.Add(1)
		go func(q *queueRuntime) {
			defer wg.Done()
			if err := f.rebalanceQueue(ctx, q); err != nil && ctx.Err() == nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to rebalance partitions", "queue", q.config.QueueName)
			}
		}(q)
	}
	wg.Wait()
}

func (f *Flow) rebalanceQueue(ctx context.Context, q *queueRuntime) error {
	ctx, cancel := context.WithTimeout(ctx, f.storeTimeout())
	defer cancel()
	now, err := f.store.Now(ctx)
	if err != nil {
		return fmt.Errorf("read lease clock: %w", err)
	}
	if err := q.consumer.Rebalance(ctx, now); err != nil {
		return fmt.Errorf("rebalance: %w", err)
	}
	return nil
}

func (f *Flow) storeTimeout() time.Duration {
	return max(time.Second, f.leaseTTL/3)
}

func (f *Flow) requestWorker(ctx context.Context, q *queueRuntime) {
	logger := log.FromContext(ctx).WithValues("queue", q.config.QueueName)
	metrics.InitGateDecisions(q.config.ID, q.config.QueueName, q.config.WorkerPoolID)
	ticker := time.NewTicker(f.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.processQueue(ctx, q, logger)
		}
	}
}

func (f *Flow) processQueue(ctx context.Context, q *queueRuntime, logger logr.Logger) {
	cfg := q.config
	budget := q.gate.Budget(ctx)
	metrics.SetDispatchBudget(budget, cfg.ID, cfg.QueueName, cfg.WorkerPoolID)
	batchSize := int(math.Floor(float64(f.batchSize) * budget))
	if batchSize <= 0 {
		hctx, cancel := context.WithTimeout(ctx, f.storeTimeout())
		pending, err := f.store.HasRequests(hctx, cfg.QueueName)
		cancel()
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to check queue for a closed gate")
		} else if pending {
			metrics.RecordGateDecision(metrics.ReasonGateClosed, cfg.ID, cfg.QueueName, cfg.WorkerPoolID)
		}
		return
	}
	pctx, cancel := context.WithTimeout(ctx, f.storeTimeout())
	rows, err := q.consumer.Poll(pctx, time.Now(), batchSize)
	cancel()
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to dispatch from queue")
		return
	}

	var undispatch []sqlqueue.Stamp
	defer func() {
		if len(undispatch) == 0 {
			return
		}
		uctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		defer cancel()
		if err := q.consumer.Undispatch(uctx, undispatch); err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to return requests to the queue; the next rebalance retries", "count", len(undispatch))
		}
	}()

	now := time.Now().Unix()
	for i, row := range rows {
		stop := func() {
			for _, r := range rows[i:] {
				undispatch = append(undispatch, r.Stamp())
			}
		}
		ir, ok := parseRequest(row, logger)
		if !ok {
			ir = &api.InternalRequest{PublicRequest: &api.RequestMessage{ID: row.ID}}
			ir.RequestToken = row.Token
			ir.DispatchAttempt = row.Attempt
			f.stampRouting(ir, cfg)
			if !f.emit(ctx, api.NewErrorResult(ir.PublicRequest, ir.InternalRouting, api.ErrCodeInvalidRequest, "unparsable queued request")) {
				stop()
				return
			}
			continue
		}
		ir.DispatchAttempt = row.Attempt
		f.stampRouting(ir, cfg)
		reqID := ir.PublicRequest.ReqID()

		if row.Deadline < now {
			logger.V(logutil.DEFAULT).Info("Deadline expired", "id", reqID)
			metrics.RecordExceededDeadlineReq(cfg.ID, cfg.QueueName, cfg.WorkerPoolID)
			if !f.emit(ctx, api.NewDeadlineExceededResult(ir.PublicRequest, ir.InternalRouting)) {
				stop()
				return
			}
			continue
		}
		if row.Cancelled {
			if !f.emit(ctx, api.NewCancelledResult(ir.PublicRequest, ir.InternalRouting)) {
				stop()
				return
			}
			continue
		}

		var releases []pipeline.GateReleaseFunc
		verdict, err := q.gate.Apply(ctx, ir, &releases)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Gating failed")
			metrics.RecordGateDecision(metrics.ReasonError, cfg.ID, cfg.QueueName, cfg.WorkerPoolID)
			pipeline.ReleaseGateReleases(releases)
			undispatch = append(undispatch, row.Stamp())
			continue
		}
		switch verdict.Action {
		case pipeline.ActionRefuse:
			reason := metrics.ReasonGateClosed
			if ir.GetClassification() == api.ClassificationOverflow {
				reason = metrics.ReasonQuotaExhausted
			}
			metrics.RecordGateDecision(reason, cfg.ID, cfg.QueueName, cfg.WorkerPoolID)
			pipeline.ReleaseGateReleases(releases)
			undispatch = append(undispatch, row.Stamp())
			continue
		case pipeline.ActionDrop:
			metrics.RecordGateDecision(metrics.ReasonDropped, cfg.ID, cfg.QueueName, cfg.WorkerPoolID)
			result := api.NewGateDroppedResult(ir.PublicRequest, ir.InternalRouting)
			if verdict.Result != nil {
				result = *verdict.Result
				result.Routing = ir.InternalRouting
			}
			pipeline.ReleaseGateReleases(releases)
			if !f.emit(ctx, result) {
				stop()
				return
			}
			continue
		}

		if len(releases) > 0 {
			f.trackGateReleases(sqlqueue.Stamp{Key: sqlqueue.Key{ID: reqID, Token: ir.RequestToken}, Attempt: ir.DispatchAttempt}, releases)
		}
		ir.IngestionTime = time.Now()
		select {
		case q.channel.Channel <- ir:
		case <-ctx.Done():
			f.releaseGateReleases(sqlqueue.Stamp{Key: sqlqueue.Key{ID: reqID, Token: ir.RequestToken}, Attempt: ir.DispatchAttempt})
			stop()
			return
		}
	}
}

func (f *Flow) emit(ctx context.Context, result api.ResultMessage) bool {
	select {
	case f.resultChannel <- result:
		return true
	case <-ctx.Done():
		return false
	}
}

func (f *Flow) stampRouting(ir *api.InternalRequest, cfg QueueConfig) {
	if ir.RequestQueueName == "" {
		ir.RequestQueueName = cfg.QueueName
	}
	if ir.QueueID == "" {
		ir.QueueID = cfg.ID
	}
	if !ir.ResultRoutingResolved {
		if cfg.ResultQueueName != "" {
			ir.ResultQueueName = cfg.ResultQueueName
		}
		ir.ResultTTLSeconds = cfg.ResultTTLSeconds
		ir.ResultRoutingResolved = true
	}
	if len(cfg.Labels) > 0 {
		if ir.Labels == nil {
			ir.Labels = make(map[string]string, len(cfg.Labels))
		}
		for k, v := range cfg.Labels {
			ir.Labels[k] = v
		}
	}
}

func parseRequest(row sqlqueue.Request, logger logr.Logger) (*api.InternalRequest, bool) {
	var ir api.InternalRequest
	if err := json.Unmarshal([]byte(row.Envelope), &ir); err != nil || ir.PublicRequest == nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to parse queued request", "id", row.ID)
		return nil, false
	}
	if err := api.AttachPayload(&ir, row.Payload); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to attach queued request payload", "id", row.ID)
		return nil, false
	}
	if ir.RequestToken == "" {
		ir.RequestToken = row.Token
	}
	return &ir, true
}

func (f *Flow) originQueue(routing api.InternalRouting) (*queueRuntime, bool) {
	if q, ok := f.queuesByID[routing.QueueID]; ok {
		return q, true
	}
	q, ok := f.queuesByName[routing.RequestQueueName]
	return q, ok
}

func (f *Flow) bounded(ctx context.Context, step func(context.Context)) {
	ctx, cancel := context.WithTimeout(ctx, storeAttempts*(f.storeTimeout()+storeBackoff<<storeAttempts))
	defer cancel()
	step(ctx)
}

func (f *Flow) trackGateReleases(stamp sqlqueue.Stamp, releases []pipeline.GateReleaseFunc) {
	if prev, loaded := f.activeReleases.Swap(stamp, releases); loaded {
		if rels, ok := prev.([]pipeline.GateReleaseFunc); ok {
			pipeline.ReleaseGateReleases(rels)
		}
	}
}

func (f *Flow) releaseGateReleases(stamp sqlqueue.Stamp) {
	if val, ok := f.activeReleases.LoadAndDelete(stamp); ok {
		if rels, ok := val.([]pipeline.GateReleaseFunc); ok {
			pipeline.ReleaseGateReleases(rels)
		}
	}
}

func withRetries[T any](ctx context.Context, op func(context.Context) (T, error)) (T, error) {
	var out T
	var err error
	for attempt := range storeAttempts {
		if out, err = op(ctx); err == nil {
			return out, nil
		}
		if attempt == storeAttempts-1 {
			break
		}
		timer := time.NewTimer(storeBackoff << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return out, err
		case <-timer.C:
		}
	}
	return out, err
}

func (f *Flow) retryWorker(ctx context.Context) {
	logger := log.FromContext(ctx)
	process := func(ctx context.Context, msg pipeline.RetryMessage) {
		if msg.InternalRequest == nil || msg.PublicRequest == nil {
			logger.V(logutil.DEFAULT).Error(nil, "Retry message missing request")
			return
		}
		reqID := msg.PublicRequest.ReqID()
		stamp := sqlqueue.Stamp{Key: sqlqueue.Key{ID: reqID, Token: msg.RequestToken}, Attempt: msg.DispatchAttempt}
		f.releaseGateReleases(stamp)
		q, ok := f.originQueue(msg.InternalRouting)
		if !ok {
			logger.V(logutil.DEFAULT).Info("Retry for a request from an unknown queue; dropped", "id", reqID, "queue", msg.RequestQueueName)
			return
		}
		envelope, _, err := api.SplitPayload(msg.InternalRequest)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to marshal retry; returning request to the queue", "id", reqID)
			q.consumer.Abandon(stamp)
			return
		}
		notBefore := time.Now().Unix() + int64(math.Ceil(msg.BackoffDurationSeconds))
		parked, err := withRetries(ctx, func(ctx context.Context) (bool, error) {
			return q.consumer.Retry(ctx, stamp, notBefore, string(envelope))
		})
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to park retry; returning request to the queue", "id", reqID)
			q.consumer.Abandon(stamp)
			return
		}
		if !parked {
			logger.V(logutil.DEFAULT).Info("Retry fenced: another instance owns the request", "id", reqID)
		}
	}
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-f.retryChannel:
					f.bounded(context.Background(), func(ctx context.Context) { process(ctx, msg) })
				default:
					return
				}
			}
		case msg := <-f.retryChannel:
			f.bounded(ctx, func(ctx context.Context) { process(ctx, msg) })
		}
	}
}

func (f *Flow) resultWorker(ctx context.Context) {
	logger := log.FromContext(ctx)
	process := func(ctx context.Context, first api.ResultMessage) {
		batch := drainBatch(first, f.resultChannel, f.resultBatchSize)
		byQueue := map[*queueRuntime][]sqlqueue.Completion{}
		for _, result := range batch {
			stamp := sqlqueue.Stamp{
				Key:     sqlqueue.Key{ID: result.ID, Token: result.Routing.RequestToken},
				Attempt: result.Routing.DispatchAttempt,
			}
			f.releaseGateReleases(stamp)
			q, ok := f.originQueue(result.Routing)
			if !ok {
				logger.V(logutil.DEFAULT).Info("Result for a request from an unknown queue; dropped", "id", result.ID, "queue", result.Routing.RequestQueueName)
				continue
			}
			route := f.defaultResultQueueName
			if result.Routing.ResultQueueName != "" {
				route = result.Routing.ResultQueueName
			}
			var expiresAt int64
			if result.Routing.ResultTTLSeconds > 0 {
				expiresAt = time.Now().Unix() + result.Routing.ResultTTLSeconds
			}
			byQueue[q] = append(byQueue[q], sqlqueue.Completion{
				Key:       stamp.Key,
				Attempt:   stamp.Attempt,
				Route:     route,
				Payload:   marshalResult(result),
				ExpiresAt: expiresAt,
			})
		}
		for q, completions := range byQueue {
			acked, err := withRetries(ctx, func(ctx context.Context) ([]bool, error) {
				return q.consumer.Ack(ctx, completions)
			})
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to write results; returning requests to the queue", "queue", q.config.QueueName, "count", len(completions))
				stamps := make([]sqlqueue.Stamp, len(completions))
				for i, c := range completions {
					stamps[i] = sqlqueue.Stamp{Key: c.Key, Attempt: c.Attempt}
				}
				q.consumer.Abandon(stamps...)
				continue
			}
			for i, ok := range acked {
				if !ok {
					logger.V(logutil.DEFAULT).Info("Result fenced: another instance owns the request", "id", completions[i].ID)
				}
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-f.resultChannel:
					f.bounded(context.Background(), func(ctx context.Context) { process(ctx, msg) })
				default:
					return
				}
			}
		case msg := <-f.resultChannel:
			f.bounded(ctx, func(ctx context.Context) { process(ctx, msg) })
		}
	}
}

func drainBatch[T any](first T, channel <-chan T, limit int) []T {
	batch := make([]T, 1, limit)
	batch[0] = first
	for len(batch) < limit {
		select {
		case item := <-channel:
			batch = append(batch, item)
		default:
			return batch
		}
	}
	return batch
}

func marshalResult(msg api.ResultMessage) string {
	bytes, err := json.Marshal(api.InternalResult{ResultMessage: msg, RequestToken: msg.Routing.RequestToken})
	if err != nil {
		fallback, _ := json.Marshal(map[string]string{"id": msg.ID, "payload": `{"error":"marshal failed"}`})
		return string(fallback)
	}
	return string(bytes)
}

func (f *Flow) QueueBacklog(ctx context.Context) ([]pipeline.QueueBacklogStat, error) {
	buckets := metrics.DeadlineProximityBuckets()
	labels := metrics.DeadlineProximityBucketLabels()
	windows := make([]int64, len(buckets))
	for i, b := range buckets {
		windows[i] = int64(b.Seconds())
	}
	now := time.Now().Unix()
	stats := make([]pipeline.QueueBacklogStat, 0, len(f.queues))
	var firstErr error
	for _, q := range f.queues {
		stat := pipeline.QueueBacklogStat{QueueID: q.config.ID, QueueName: q.config.QueueName, PoolName: q.config.WorkerPoolID}
		depth, expiring, err := f.store.Backlog(ctx, q.config.QueueName, now, windows)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			expiring = make([]int64, len(labels))
			depth = 0
		}
		stat.Depth = depth
		stat.ExpiringCounts = make([]pipeline.ExpiringCount, 0, len(labels))
		for i, l := range labels {
			stat.ExpiringCounts = append(stat.ExpiringCounts, pipeline.ExpiringCount{Window: l, Count: expiring[i]})
		}
		stats = append(stats, stat)
	}
	return stats, firstErr
}
