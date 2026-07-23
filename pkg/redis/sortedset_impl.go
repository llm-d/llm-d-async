package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

// SortedSetQueueConfig defines a single queue entry in the sorted-set transport config.
type SortedSetQueueConfig struct {
	ID              string `json:"id,omitempty"`
	QueueName       string `json:"queue_name,omitempty"`
	ResultQueueName string `json:"result_queue_name,omitempty"`
	// ResultTTLSeconds, when > 0, sets an expiry on the result destination
	// each time results are pushed. Used for per-request result keys
	// (frontend enqueue mode) so unfetched results are cleaned up. Queues
	// without it behave as before (no expiry).
	ResultTTLSeconds   int64  `json:"result_ttl_seconds,omitempty"`
	WorkerPoolID       string `json:"worker_pool_id"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
	IGWBaseURL         string `json:"igw_base_url"`
	pipeline.GateConfig
	Labels map[string]string `json:"labels,omitempty"`
}

type requestChannelData struct {
	channel   pipeline.RequestChannel
	queueName string
	queueID   string
	gate      pipeline.Gate
}

var (
	_ pipeline.Flow                        = (*RedisSortedSetFlow)(nil)
	_ pipeline.HealthChecker               = (*RedisSortedSetFlow)(nil)
	_ pipeline.CancellationCheckerProvider = (*RedisSortedSetFlow)(nil)
)

var cleanupRequestStateScript = redis.NewScript(`
local token = ARGV[1]
if token == "" then
  return 0
end
if redis.call("GET", KEYS[1]) == token then
  redis.call("DEL", KEYS[1])
end
if redis.call("GET", KEYS[2]) == token then
  redis.call("DEL", KEYS[2])
end
return 1
`)

type RedisSortedSetFlow struct {
	rdb                     *redis.Client
	cancellationChecker     api.CancellationChecker
	requestChannels         []requestChannelData
	retryChannel            chan pipeline.RetryMessage
	resultChannel           chan api.ResultMessage
	pollInterval            time.Duration
	batchSize               int
	retryQueueName          string
	activeReleases          sync.Map
	gate                    pipeline.Gate
	gateFactory             pipeline.GateFactory
	configMap               map[string]SortedSetQueueConfig
	defaultRequestQueueName string
	defaultResultQueueName  string
	workerPools             []pipeline.WorkerPoolConfig
	consumeCancel           context.CancelFunc
	consumeWg               sync.WaitGroup
	drainCancel             context.CancelFunc
	drainWg                 sync.WaitGroup
	enableTracing           bool
}

type redisCancellationChecker struct {
	rdb *redis.Client
}

func (c *redisCancellationChecker) IsCancelled(ctx context.Context, requestID, requestToken string) (bool, error) {
	if requestID == "" || requestToken == "" {
		return false, nil
	}
	token, err := c.rdb.Get(ctx, api.RequestCancellationKey(requestID)).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return token == requestToken, nil
}

// NewRedisSortedSetFlow builds a Redis sorted-set flow from a parsed
// SortedSetConfig. The config is expected to have had ApplyDefaults applied
// (LoadSortedSetConfig does this). workerPools resolves the named pool each
// queue routes to; gateFactory, when non-nil, instantiates a per-queue gate for
// any queue that declares a gate_type.
func NewRedisSortedSetFlow(cfg SortedSetConfig, workerPools []pipeline.WorkerPoolConfig, gateFactory pipeline.GateFactory) (*RedisSortedSetFlow, error) {
	redisOpts, err := ParseRedisOptions(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis connection config: %w", err)
	}
	r := &RedisSortedSetFlow{
		rdb:                    redis.NewClient(redisOpts),
		requestChannels:        make([]requestChannelData, 0, len(cfg.Queues)),
		retryChannel:           make(chan pipeline.RetryMessage),
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           time.Duration(cfg.PollIntervalMs) * time.Millisecond,
		batchSize:              cfg.BatchSize,
		retryQueueName:         cfg.RetryQueueName,
		defaultResultQueueName: cfg.ResultQueueName,
		workerPools:            workerPools,
		gateFactory:            gateFactory,
		enableTracing:          cfg.EnableTracing,
	}

	if r.enableTracing {
		if err := redisotel.InstrumentTracing(r.rdb); err != nil {
			_ = r.rdb.Close()
			return nil, fmt.Errorf("failed to instrument Redis tracing: %w", err)
		}
	}

	// Retry messages that lack an explicit RequestQueueName fall back to this
	// name when re-enqueued (flushRetryBatch). Default to the first configured
	// queue so retries land on a real key rather than "" — preserving the
	// previous single-queue fallback behavior.
	if len(cfg.Queues) > 0 {
		r.defaultRequestQueueName = cfg.Queues[0].QueueName
	}

	r.configMap = make(map[string]SortedSetQueueConfig, len(cfg.Queues))
	for _, cfg := range cfg.Queues {
		// Normalize before anything reads it: configMap is the source of the
		// pool_name label on this queue's metrics, and an unset WorkerPoolID
		// there would label them "" while the request channel below — and every
		// pool-keyed series — says "default".
		if cfg.WorkerPoolID == "" {
			cfg.WorkerPoolID = "default"
		}
		workerPoolID := cfg.WorkerPoolID

		var gate pipeline.Gate
		if r.gateFactory != nil && cfg.GateType != "" {
			gateCfg := cfg.GateConfig
			gateCfg.Owner = pipeline.GateOwner{
				QueueID:      cfg.ID,
				QueueName:    cfg.QueueName,
				WorkerPoolID: workerPoolID,
			}
			gate, err = r.gateFactory.CreateGate(gateCfg)
			if err != nil {
				return nil, fmt.Errorf("failed to create gate for queue %q (gate_type=%q): %w", cfg.QueueName, cfg.GateType, err)
			}
		} else if r.gate != nil {
			gate = r.gate
		} else {
			gate = pipeline.ConstOpenGate()
		}

		found := false
		for _, pool := range r.workerPools {
			if pool.ID == workerPoolID {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("worker pool %q specified in queue config not found in pool configuration", workerPoolID)
		}

		if cfg.IGWBaseURL == "" {
			return nil, fmt.Errorf("queue config for queue %q: igw_base_url must be specified", cfg.QueueName)
		}

		reqPath := cfg.RequestPathURL
		if reqPath == "" {
			reqPath = "/v1/completions"
		}

		ch := pipeline.RequestChannel{
			Channel:            make(chan *api.InternalRequest),
			InferenceObjective: cfg.InferenceObjective,
			RequestPathURL:     reqPath,
			IGWBaseURL:         cfg.IGWBaseURL,
			Gate:               gate,
			WorkerPoolID:       workerPoolID,
		}

		r.configMap[cfg.ID] = cfg
		r.requestChannels = append(r.requestChannels, requestChannelData{
			channel:   ch,
			queueName: cfg.QueueName,
			queueID:   cfg.ID,
			gate:      gate,
		})
	}

	if r.gate == nil {
		r.gate = pipeline.ConstOpenGate()
	}

	return r, nil
}

func (r *RedisSortedSetFlow) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	consumeCtx, consumeCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.consumeCancel = consumeCancel

	drainCtx, drainCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.drainCancel = drainCancel

	for _, ch := range r.requestChannels {
		r.consumeWg.Add(1)
		go func(ch requestChannelData) {
			defer r.consumeWg.Done()
			r.requestWorker(consumeCtx, ch.channel.Channel, ch.queueName, ch.queueID)
		}(ch)
	}
	r.consumeWg.Add(1)
	go func() { defer r.consumeWg.Done(); r.retryMover(consumeCtx) }()

	r.drainWg.Add(2)
	go func() { defer r.drainWg.Done(); r.retryWorker(drainCtx) }()  // #nosec G118
	go func() { defer r.drainWg.Done(); r.resultWorker(drainCtx) }() // #nosec G118
}

func (r *RedisSortedSetFlow) StopConsuming() {
	if r.consumeCancel != nil {
		r.consumeCancel()
	}
	r.consumeWg.Wait()
}

func (r *RedisSortedSetFlow) Shutdown() {
	if r.drainCancel != nil {
		r.drainCancel()
	}
	r.drainWg.Wait()
}

func (r *RedisSortedSetFlow) RequestChannels() []pipeline.RequestChannel {
	channels := make([]pipeline.RequestChannel, len(r.requestChannels))
	for i, ch := range r.requestChannels {
		channels[i] = ch.channel
	}
	return channels
}

// QueueBacklog reports the number of pending members in each queue's sorted set.
func (r *RedisSortedSetFlow) QueueBacklog(ctx context.Context) ([]pipeline.QueueBacklogStat, error) {
	stats := make([]pipeline.QueueBacklogStat, 0, len(r.requestChannels))
	var firstErr error
	for _, cd := range r.requestChannels {
		depth, err := r.rdb.ZCard(ctx, cd.queueName).Result()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("ZCard on queue %q: %w", cd.queueName, err)
			}
			// Report 0 rather than skipping so the gauge does not retain a
			// stale value for this queue after a failed poll.
			stats = append(stats, pipeline.QueueBacklogStat{
				QueueID:   cd.queueID,
				QueueName: cd.queueName,
				PoolName:  cd.channel.WorkerPoolID,
			})
			continue
		}
		stats = append(stats, pipeline.QueueBacklogStat{
			QueueID:   cd.queueID,
			QueueName: cd.queueName,
			PoolName:  cd.channel.WorkerPoolID,
			Depth:     depth,
		})
	}
	return stats, firstErr
}

var _ pipeline.BacklogReporter = (*RedisSortedSetFlow)(nil)

func (r *RedisSortedSetFlow) RetryChannel() chan pipeline.RetryMessage {
	return r.retryChannel
}

func (r *RedisSortedSetFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *RedisSortedSetFlow) CancellationChecker() api.CancellationChecker {
	if r.cancellationChecker != nil {
		return r.cancellationChecker
	}
	return &redisCancellationChecker{rdb: r.rdb}
}

func (r *RedisSortedSetFlow) HealthCheck(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

func (r *RedisSortedSetFlow) Characteristics() pipeline.Characteristics {
	return pipeline.Characteristics{HasExternalBackoff: false, SupportsMessageLatency: false}
}

// Polls sorted set and processes messages by deadline priority (earliest first)
func (r *RedisSortedSetFlow) requestWorker(ctx context.Context, msgChannel chan *api.InternalRequest, queueName string, queueID string) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	// Find the gate for this queue
	var gate pipeline.Gate
	for _, ch := range r.requestChannels {
		if ch.queueName == queueName {
			gate = ch.gate
			break
		}
	}
	if gate == nil {
		gate = r.gate
	}

	metrics.InitGateDecisions(queueID, queueName, r.poolNameFor(queueID))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.processMessages(ctx, msgChannel, queueName, queueID, gate, logger)
		}
	}
}

// poolNameFor returns the worker pool a queue routes to, or "" when the queue has
// no config entry (the metric label is then empty rather than wrong).
func (r *RedisSortedSetFlow) poolNameFor(queueID string) string {
	if cfg, ok := r.configMap[queueID]; ok {
		return cfg.WorkerPoolID
	}
	return ""
}

func (r *RedisSortedSetFlow) processMessages(ctx context.Context, msgChannel chan *api.InternalRequest, queueName string, queueID string, gate pipeline.Gate, logger logr.Logger) {
	currentTime := float64(time.Now().Unix())

	budget := gate.Budget(ctx)
	poolName := r.poolNameFor(queueID)
	metrics.SetDispatchBudget(budget, queueID, queueName, poolName)
	batchSize := int(math.Floor(float64(r.batchSize) * budget))
	if batchSize <= 0 {
		// Back-pressure here is applied pre-dequeue: the budget shrank the batch
		// to zero, so no message reaches gate.Apply below — the only other site
		// that records a refusal. Count the throttled poll itself, or gate_closed
		// stays silent exactly while the gate is doing its job (#368). Only count
		// when work is actually waiting; an idle queue was not held back.
		depth, err := r.rdb.ZCard(ctx, queueName).Result()
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to read queue depth for a closed gate", "queue", queueName)
		} else if depth > 0 {
			metrics.RecordGateDecision(metrics.ReasonGateClosed, queueID, queueName, poolName)
		}
		return
	}

	for i := 0; i < batchSize; i++ {
		results, err := r.rdb.ZPopMin(ctx, queueName, 1).Result()
		if err == redis.Nil || len(results) == 0 {
			break
		}
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to pop from sorted set")
			break
		}

		ir, deadline, ok := r.parseMessage(results[0], logger)
		if !ok {
			continue
		}
		if ir == nil {
			continue
		}
		rview := ir.PublicRequest
		if rview == nil {
			continue
		}
		if deadline < currentTime {
			logger.V(logutil.DEFAULT).Info("Deadline expired", "id", rview.ReqID())
			if err := r.cleanupRequestStateByIDAndToken(ctx, rview.ReqID(), ir.RequestToken); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to cleanup expired request state", "id", rview.ReqID())
			}
			continue
		}

		if ir.RequestQueueName == "" {
			ir.RequestQueueName = queueName
		}
		if ir.QueueID == "" {
			ir.QueueID = queueID
		}
		if cfg, ok := r.configMap[queueID]; ok && len(cfg.Labels) > 0 {
			if ir.Labels == nil {
				ir.Labels = make(map[string]string, len(cfg.Labels))
			}
			for k, v := range cfg.Labels {
				ir.Labels[k] = v
			}
		}

		cancelled, err := r.CancellationChecker().IsCancelled(ctx, rview.ReqID(), ir.RequestToken)
		if err != nil {
			// Best-effort at dequeue time only. The worker path performs the
			// authoritative pre-dispatch cancellation check and fails closed.
			logger.V(logutil.DEFAULT).Error(err, "Failed to check request cancellation", "id", rview.ReqID())
		} else if cancelled {
			select {
			case r.resultChannel <- api.NewCancelledResult(rview, ir.InternalRouting):
			case <-ctx.Done():
				return
			}
			continue
		}

		// Apply gate
		var releases []pipeline.GateReleaseFunc
		verdict, err := gate.Apply(ctx, ir, &releases)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Gating failed")
			metrics.RecordGateDecision(metrics.ReasonError, queueID, queueName, poolName)
			// Re-enqueue the message on gating failure
			member, _ := json.Marshal(ir)
			r.rdb.ZAdd(ctx, queueName, redis.Z{Score: deadline, Member: string(member)})
			continue
		}

		if verdict.Action == pipeline.ActionRefuse {
			reason := metrics.ReasonGateClosed
			if ir.GetClassification() == api.ClassificationOverflow {
				reason = metrics.ReasonQuotaExhausted
			}
			metrics.RecordGateDecision(reason, queueID, queueName, poolName)
			// Re-enqueue the message (wait for capacity or quota)
			member, _ := json.Marshal(ir)
			r.rdb.ZAdd(ctx, queueName, redis.Z{Score: deadline, Member: string(member)})
			continue
		}

		if verdict.Action == pipeline.ActionDrop {
			metrics.RecordGateDecision(metrics.ReasonDropped, queueID, queueName, poolName)
			if verdict.Result != nil {
				resultMsg := *verdict.Result
				resultMsg.Routing = ir.InternalRouting
				r.resultChannel <- resultMsg
			} else {
				r.resultChannel <- api.NewGateDroppedResult(rview, ir.InternalRouting)
			}
			continue
		}

		if len(releases) > 0 {
			// Defensive: never orphan a lingering reservation for this id — release
			// any prior closure instead of silently overwriting it (see #311).
			if prev, loaded := r.activeReleases.Swap(rview.ReqID(), releases); loaded {
				if rels, ok := prev.([]pipeline.GateReleaseFunc); ok {
					pipeline.ReleaseGateReleases(rels)
				}
			}
		}

		// Stamp ingestion time as the message enters the in-process buffer so the
		// worker can record queue residence time when it pulls the message.
		ir.IngestionTime = time.Now()

		select {
		case msgChannel <- ir:
		case <-ctx.Done():
			r.activeReleases.Delete(rview.ReqID())
			if err := retryRedisOp(context.Background(), func(ctx context.Context) error {
				return r.rdb.ZAdd(ctx, queueName, redis.Z{
					Score:  results[0].Score,
					Member: results[0].Member,
				}).Err()
			}); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to re-queue message on shutdown", "id", rview.ReqID())
			}
			pipeline.ReleaseGateReleases(releases)
			return
		}
	}
}

func (r *RedisSortedSetFlow) parseMessage(z redis.Z, logger logr.Logger) (*api.InternalRequest, float64, bool) {
	var ir api.InternalRequest
	if err := json.Unmarshal([]byte(z.Member.(string)), &ir); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message")
		return nil, 0, false
	}
	if ir.PublicRequest == nil {
		logger.V(logutil.DEFAULT).Error(nil, "Missing specific request in message", "id", z.Member)
		return nil, 0, false
	}
	deadline := ir.PublicRequest.ReqDeadline()
	if deadline <= 0 {
		logger.V(logutil.DEFAULT).Error(nil, "Invalid deadline", "id", ir.PublicRequest.ReqID())
		return &ir, 0, false
	}

	return &ir, float64(deadline), true
}

// Re-queues failed messages with exponential backoff
func (r *RedisSortedSetFlow) retryWorker(ctx context.Context) {
	processMsg := func(processCtx context.Context, msg pipeline.RetryMessage) {
		batch := drainBatch(msg, r.retryChannel, maxBatchSize)
		// #311: a retried request returns to the queue and is re-gated (and thus
		// re-reserved) on its next dispatch, so its current gate reservation must
		// be released here — otherwise inFlight ratchets up on every retry until
		// Budget() reaches 0 and the queue stops dispatching entirely. Release
		// BEFORE re-enqueue: a re-dispatch can only occur after flushRetryBatch's
		// ZAdd, so releasing first prevents the reservation from being overwritten
		// and orphaned.
		for _, m := range batch {
			if m.InternalRequest == nil || m.PublicRequest == nil {
				continue
			}
			if val, ok := r.activeReleases.LoadAndDelete(m.PublicRequest.ReqID()); ok {
				if rels, ok := val.([]pipeline.GateReleaseFunc); ok {
					pipeline.ReleaseGateReleases(rels)
				}
			}
		}
		r.flushRetryBatch(processCtx, batch)
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-r.retryChannel:
					processMsg(context.Background(), msg)
				default:
					return
				}
			}
		case msg := <-r.retryChannel:
			processMsg(ctx, msg)
		}
	}
}

func (r *RedisSortedSetFlow) flushRetryBatch(ctx context.Context, batch []pipeline.RetryMessage) {
	if len(batch) == 0 {
		return
	}

	logger := log.FromContext(ctx)
	type retryEntry struct {
		queue string
		value redis.Z
	}

	entries := make([]retryEntry, 0, len(batch))
	for _, msg := range batch {
		if msg.InternalRequest == nil {
			logger.V(logutil.DEFAULT).Error(nil, "Retry message missing InternalRequest")
			continue
		}
		queueName := msg.RequestQueueName
		if queueName == "" {
			queueName = r.defaultRequestQueueName
		}
		// Preserve the origin queue in the envelope so the retry mover can
		// re-enter the message into the right queue once it is due.
		msg.RequestQueueName = queueName
		bytes, err := json.Marshal(msg.InternalRequest)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to marshal retry")
			continue
		}

		// Score is the retry-due time. The retry queue is drained by the
		// retry mover strictly at or after this time, so the backoff is
		// enforced. Retries must NOT be ZADDed into the request queue
		// directly: there the score means deadline and ZPopMin would pop a
		// future-scored retry immediately (and ahead of all fresh traffic,
		// since now+backoff sorts below any realistic deadline).
		retryScore := float64(time.Now().Unix()) + msg.BackoffDurationSeconds
		entries = append(entries, retryEntry{
			queue: r.retryQueue(),
			value: redis.Z{Score: retryScore, Member: string(bytes)},
		})
	}

	if err := retryRedisOp(ctx, func(ctx context.Context) error {
		pipe := r.rdb.Pipeline()
		for _, entry := range entries {
			pipe.ZAdd(ctx, entry.queue, entry.value)
		}
		_, err := pipe.Exec(ctx)
		return err
	}); err == nil {
		logger.V(logutil.DEBUG).Info("Pushed retry batch", "batchSize", len(batch))
	}
}

// Pushes results to Redis list (FIFO)
// Routes results to the queue specified in request metadata, or default queue if not specified.
// Batches multiple results into a single Redis pipeline call to reduce round-trips.
func (r *RedisSortedSetFlow) resultWorker(ctx context.Context) {
	processMsg := func(flushCtx context.Context, msg api.ResultMessage) {
		batch := drainBatch(msg, r.resultChannel, maxBatchSize)
		for _, m := range batch {
			if val, ok := r.activeReleases.LoadAndDelete(m.ID); ok {
				if rels, ok := val.([]pipeline.GateReleaseFunc); ok {
					pipeline.ReleaseGateReleases(rels)
				}
			}
		}
		r.flushResultBatch(flushCtx, batch)
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-r.resultChannel:
					processMsg(context.Background(), msg)
				default:
					return
				}
			}
		case msg := <-r.resultChannel:
			processMsg(ctx, msg)
		}
	}
}

func (r *RedisSortedSetFlow) flushResultBatch(ctx context.Context, batch []api.ResultMessage) {
	logger := log.FromContext(ctx)
	defaultQueue := r.defaultResultQueueName
	queued := make(map[string][]string)
	resultTTLs := make(map[string]time.Duration)
	for _, result := range batch {
		resultQueue := defaultQueue
		cfg, hasCfg := r.configMap[result.Routing.QueueID]
		if hasCfg && cfg.ResultQueueName != "" {
			resultQueue = cfg.ResultQueueName
		} else if result.Routing.ResultQueueName != "" {
			resultQueue = result.Routing.ResultQueueName
		}
		queued[resultQueue] = append(queued[resultQueue], r.marshalResult(result))
		if hasCfg && cfg.ResultTTLSeconds > 0 {
			resultTTLs[resultQueue] = time.Duration(cfg.ResultTTLSeconds) * time.Second
		}
	}

	if err := retryRedisOp(ctx, func(ctx context.Context) error {
		pipe := r.rdb.Pipeline()
		for queue, msgs := range queued {
			for _, msgStr := range msgs {
				pipe.LPush(ctx, queue, msgStr)
			}
			if ttl, ok := resultTTLs[queue]; ok {
				pipe.Expire(ctx, queue, ttl)
			}
		}
		_, err := pipe.Exec(ctx)
		return err
	}); err == nil {
		for _, result := range batch {
			if err := r.cleanupRequestState(ctx, result); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to cleanup request state after result flush", "id", result.ID)
			}
		}
		logger.V(logutil.DEBUG).Info("Pushed result batch", "batchSize", len(batch))
	}
}

func (r *RedisSortedSetFlow) cleanupRequestState(ctx context.Context, result api.ResultMessage) error {
	return r.cleanupRequestStateByIDAndToken(ctx, result.ID, result.Routing.RequestToken)
}

func (r *RedisSortedSetFlow) cleanupRequestStateByIDAndToken(ctx context.Context, requestID, requestToken string) error {
	if requestID == "" || requestToken == "" {
		return nil
	}
	_, err := cleanupRequestStateScript.Run(
		ctx,
		r.rdb,
		[]string{api.RequestActiveTokenKey(requestID), api.RequestCancellationKey(requestID)},
		requestToken,
	).Result()
	return err
}

func (r *RedisSortedSetFlow) marshalResult(msg api.ResultMessage) string {
	if bytes, err := json.Marshal(msg); err == nil {
		return string(bytes)
	}
	fallback := map[string]string{"id": msg.ID, "payload": `{"error":"marshal failed"}`}
	fallbackBytes, _ := json.Marshal(fallback)
	return string(fallbackBytes)
}

// retryQueue returns the retry queue name, defaulting for flows constructed
// directly (tests) without ApplyDefaults.
func (r *RedisSortedSetFlow) retryQueue() string {
	if r.retryQueueName == "" {
		return "retry-sortedset"
	}
	return r.retryQueueName
}

// retryMover re-enters due retries into their request queues. Retries wait in
// the retry queue scored by retry-due time; once due, they return to their
// origin queue with the message's original deadline as the score, restoring
// earliest-deadline-first ordering among fresh traffic.
func (r *RedisSortedSetFlow) retryMover(ctx context.Context) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := float64(time.Now().Unix())
			members, err := r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key: r.retryQueue(), ByScore: true,
				Start: "-inf", Stop: fmt.Sprintf("%f", now),
				Count: int64(r.batchSize), Offset: 0,
			}).Result()
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to read due retries", "queue", r.retryQueue())
				continue
			}
			if len(members) == 0 {
				continue
			}
			for _, member := range members {
				// ZRem guards against double-move: only the remover that wins
				// the removal re-enters the message.
				removed, err := r.rdb.ZRem(ctx, r.retryQueue(), member).Result()
				if err != nil || removed == 0 {
					continue
				}
				var ir api.InternalRequest
				if err := json.Unmarshal([]byte(member), &ir); err != nil || ir.PublicRequest == nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to parse due retry, dropping", "member", member[:min(len(member), 120)])
					continue
				}
				queueName := ir.RequestQueueName
				if queueName == "" {
					queueName = r.defaultRequestQueueName
				}
				if err := r.rdb.ZAdd(ctx, queueName, redis.Z{
					Score:  float64(ir.PublicRequest.ReqDeadline()),
					Member: member,
				}).Err(); err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to re-enter due retry", "queue", queueName)
					// Put it back in the retry queue so it is not lost.
					_ = r.rdb.ZAdd(ctx, r.retryQueue(), redis.Z{Score: now, Member: member}).Err()
				}
			}
		}
	}
}
