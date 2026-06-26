package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

// parseGateParams parses a JSON-encoded string (from --redis.ss.gate-params)
// into a map[string]string for gate parameter configuration.
// Used to pass gate parameters from CLI or YAML to the gate factory.
func parseGateParams(s string) map[string]string {
	m := map[string]string{}
	if s == "" || s == "{}" {
		return m
	}
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

// StringMap is a map[string]string that tolerates non-string JSON values
// by converting them to their string representation during unmarshaling.
type StringMap map[string]string

func (m *StringMap) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	result := make(map[string]string, len(raw))
	for k, v := range raw {
		switch val := v.(type) {
		case string:
			result[k] = val
		case float64:
			result[k] = strconv.FormatFloat(val, 'f', -1, 64)
		case bool:
			result[k] = strconv.FormatBool(val)
		case nil:
			result[k] = ""
		default:
			return fmt.Errorf("gate_params key %q: unsupported value type %T (only strings, numbers, and booleans are allowed)", k, v)
		}
	}
	*m = result
	return nil
}

type queueConfig struct {
	ID                 string    `json:"id,omitempty"`
	QueueName          string    `json:"queue_name,omitempty"`
	ResultQueueName    string    `json:"result_queue_name,omitempty"`
	WorkerPoolID       string    `json:"worker_pool_id"`
	InferenceObjective string    `json:"inference_objective"`
	RequestPathURL     string    `json:"request_path_url"`
	IGWBaseURL         string    `json:"igw_base_url"`
	GateType           string    `json:"gate_type"`
	GateParams         StringMap `json:"gate_params,omitempty"`
}

type requestChannelData struct {
	channel   pipeline.RequestChannel
	queueName string
	queueID   string
	gate      pipeline.Gate
}

var (
	_ pipeline.Flow          = (*RedisSortedSetFlow)(nil)
	_ pipeline.HealthChecker = (*RedisSortedSetFlow)(nil)
)

type RedisSortedSetFlow struct {
	rdb                     *redis.Client
	requestChannels         []requestChannelData
	retryChannel            chan pipeline.RetryMessage
	resultChannel           chan api.ResultMessage
	pollInterval            time.Duration
	batchSize               int
	activeReleases          sync.Map
	gate                    pipeline.Gate
	gateFactory             pipeline.GateFactory
	configMap               map[string]queueConfig
	defaultRequestQueueName string
	defaultResultQueueName  string
	workerPools             []pipeline.WorkerPoolConfig
	consumeCancel           context.CancelFunc
	consumeWg               sync.WaitGroup
	drainCancel             context.CancelFunc
	drainWg                 sync.WaitGroup
	enableTracing           bool
}

// SortedSetOption is a functional option for configuring RedisSortedSetFlow
type SortedSetOption func(*RedisSortedSetFlow)

// WithGateFactory sets a GateFactory for per-queue gate instantiation.
// When set, gates are created per queue from config, overriding any global gate.
func WithGateFactory(factory pipeline.GateFactory) SortedSetOption {
	return func(r *RedisSortedSetFlow) {
		r.gateFactory = factory
	}
}

// WithSortedSetRedisTracing enables per-command Redis tracing spans via redisotel.
func WithSortedSetRedisTracing(enable bool) SortedSetOption {
	return func(r *RedisSortedSetFlow) {
		r.enableTracing = enable
	}
}

// WithSortedSetWorkerPools sets the pool configurations to resolve named pools.
func WithSortedSetWorkerPools(workerPools []pipeline.WorkerPoolConfig) SortedSetOption {
	return func(r *RedisSortedSetFlow) {
		r.workerPools = workerPools
	}
}

func NewRedisSortedSetFlow(flowOpts SortedSetFlowOptions, connOpts ConnectionOptions, fns ...SortedSetOption) (*RedisSortedSetFlow, error) {
	configs, err := loadQueueConfigs(flowOpts)
	if err != nil {
		return nil, err
	}
	redisOpts, err := ParseRedisOptions(connOpts.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis connection config: %w", err)
	}
	r := &RedisSortedSetFlow{
		rdb:                     redis.NewClient(redisOpts),
		requestChannels:         make([]requestChannelData, 0, len(configs)),
		retryChannel:            make(chan pipeline.RetryMessage),
		resultChannel:           make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:            time.Duration(flowOpts.PollIntervalMs) * time.Millisecond,
		batchSize:               flowOpts.BatchSize,
		defaultRequestQueueName: flowOpts.RequestQueueName,
		defaultResultQueueName:  flowOpts.ResultQueueName,
	}

	for _, fn := range fns {
		fn(r)
	}

	if r.enableTracing {
		if err := redisotel.InstrumentTracing(r.rdb); err != nil {
			_ = r.rdb.Close()
			return nil, fmt.Errorf("failed to instrument Redis tracing: %w", err)
		}
	}

	r.configMap = make(map[string]queueConfig, len(configs))
	for _, cfg := range configs {
		var gate pipeline.Gate
		if r.gateFactory != nil && cfg.GateType != "" {
			gate, err = r.gateFactory.CreateGate(cfg.GateType, cfg.GateParams)
			if err != nil {
				return nil, fmt.Errorf("failed to create gate for queue %q (gate_type=%q): %w", cfg.QueueName, cfg.GateType, err)
			}
		} else if r.gate != nil {
			gate = r.gate
		} else {
			gate = pipeline.ConstOpenGate()
		}

		workerPoolID := cfg.WorkerPoolID
		if workerPoolID == "" {
			workerPoolID = "default"
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

func parseQueueConfigs(data []byte) ([]queueConfig, error) {
	var configs []queueConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal queues config: %w", err)
	}
	return configs, nil
}

func loadQueueConfigs(opts SortedSetFlowOptions) ([]queueConfig, error) {
	var configs []queueConfig
	if opts.QueuesConfig != "" {
		var err error
		configs, err = parseQueueConfigs([]byte(opts.QueuesConfig))
		if err != nil {
			return nil, err
		}
	} else if opts.QueuesConfigFile != "" {
		data, err := os.ReadFile(opts.QueuesConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
		configs, err = parseQueueConfigs(data)
		if err != nil {
			return nil, err
		}
	} else {
		configs = []queueConfig{{
			QueueName:          opts.RequestQueueName,
			InferenceObjective: opts.InferenceObjective,
			IGWBaseURL:         opts.IGWBaseURL,
			RequestPathURL:     opts.RequestPathURL,
			GateType:           opts.GateType,
			GateParams:         parseGateParams(opts.GateParamsJSON),
			WorkerPoolID:       "default",
		}}
	}
	seenID := make(map[string]bool, len(configs))
	seenQueue := make(map[string]bool, len(configs))
	for i := range configs {
		applyQueueConfigDefaults(&configs[i])
		if seenID[configs[i].ID] {
			return nil, fmt.Errorf("duplicate queue id %q", configs[i].ID)
		}
		seenID[configs[i].ID] = true
		if seenQueue[configs[i].QueueName] {
			return nil, fmt.Errorf("duplicate queue_name %q", configs[i].QueueName)
		}
		seenQueue[configs[i].QueueName] = true
	}
	return configs, nil
}

func applyQueueConfigDefaults(cfg *queueConfig) {
	if cfg.ID == "" {
		cfg.ID = cfg.QueueName
	}
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

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.processMessages(ctx, msgChannel, queueName, queueID, gate, logger)
		}
	}
}

func (r *RedisSortedSetFlow) processMessages(ctx context.Context, msgChannel chan *api.InternalRequest, queueName string, queueID string, gate pipeline.Gate, logger logr.Logger) {
	currentTime := float64(time.Now().Unix())

	budget := gate.Budget(ctx)
	batchSize := int(math.Floor(float64(r.batchSize) * budget))

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
			continue
		}

		if ir.RequestQueueName == "" {
			ir.RequestQueueName = queueName
		}
		if ir.QueueID == "" {
			ir.QueueID = queueID
		}

		// Apply gate
		var releases []pipeline.GateReleaseFunc
		verdict, err := gate.Apply(ctx, ir, &releases)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Gating failed")
			// Re-enqueue the message on gating failure
			member, _ := json.Marshal(ir)
			r.rdb.ZAdd(ctx, queueName, redis.Z{Score: deadline, Member: string(member)})
			continue
		}

		if verdict.Action == pipeline.ActionRefuse {
			// Re-enqueue the message (wait for capacity or quota)
			member, _ := json.Marshal(ir)
			r.rdb.ZAdd(ctx, queueName, redis.Z{Score: deadline, Member: string(member)})
			continue
		}

		if verdict.Action == pipeline.ActionDrop {
			if verdict.Result != nil {
				r.resultChannel <- *verdict.Result
			} else {
				r.resultChannel <- api.ResultMessage{
					ID:      rview.ReqID(),
					Payload: `{"status": "dropped"}`,
				}
			}
			continue
		}

		if len(releases) > 0 {
			r.activeReleases.Store(rview.ReqID(), releases)
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
		bytes, err := json.Marshal(msg.InternalRequest)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to marshal retry")
			continue
		}

		retryScore := float64(time.Now().Unix()) + msg.BackoffDurationSeconds
		entries = append(entries, retryEntry{
			queue: queueName,
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
	for _, result := range batch {
		resultQueue := defaultQueue
		if cfg, ok := r.configMap[result.Routing.QueueID]; ok && cfg.ResultQueueName != "" {
			resultQueue = cfg.ResultQueueName
		} else if result.Routing.ResultQueueName != "" {
			resultQueue = result.Routing.ResultQueueName
		}
		queued[resultQueue] = append(queued[resultQueue], r.marshalResult(result))
	}

	if err := retryRedisOp(ctx, func(ctx context.Context) error {
		pipe := r.rdb.Pipeline()
		for queue, msgs := range queued {
			for _, msgStr := range msgs {
				pipe.LPush(ctx, queue, msgStr)
			}
		}
		_, err := pipe.Exec(ctx)
		return err
	}); err == nil {
		logger.V(logutil.DEBUG).Info("Pushed result batch", "batchSize", len(batch))
	}
}

func (r *RedisSortedSetFlow) marshalResult(msg api.ResultMessage) string {
	if bytes, err := json.Marshal(msg); err == nil {
		return string(bytes)
	}
	fallback := map[string]string{"id": msg.ID, "payload": `{"error":"marshal failed"}`}
	fallbackBytes, _ := json.Marshal(fallback)
	return string(fallbackBytes)
}
