package redis

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"

	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var (
	ssIGWBaseURL         = flag.String("redis.ss.igw-base-url", "", "IGW base URL")
	ssRequestPathURL     = flag.String("redis.ss.request-path-url", "/v1/completions", "Request path URL")
	ssInferenceObjective = flag.String("redis.ss.inference-objective", "", "Inference objective header")
	ssRequestQueueName   = flag.String("redis.ss.request-queue-name", "request-sortedset", "Request sorted set name")
	ssResultQueueName    = flag.String("redis.ss.result-queue-name", "result-list", "Result list name")
	ssQueuesConfigFile   = flag.String("redis.ss.queues-config-file", "", "Multiple queues config file")
	ssPollIntervalMs     = flag.Int("redis.ss.poll-interval-ms", 1000, "Poll interval in milliseconds")
	ssBatchSize          = flag.Int("redis.ss.batch-size", 10, "Number of messages to process per poll")
)

type queueConfig struct {
	QueueName          string            `json:"queue_name"`
	InferenceObjective string            `json:"inference_objective"`
	RequestPathURL     string            `json:"request_path_url"`
	IGWBaseURL         string            `json:"igw_base_url"`
	HTTPHeaders        map[string]string `json:"http_headers,omitempty"`
	ModelNameOverride  string            `json:"model_name_override,omitempty"`
	// Labels is the queue's static label set. The redis sortedset Flow
	// doesn't model pools natively, so each queue is its own singleton
	// pool — the queue name is auto-injected as labels["pool"].
	Labels map[string]string `json:"labels,omitempty"`
}

type requestChannelData struct {
	channel   pipeline.RequestChannel
	queueName string
}

var _ pipeline.Flow = (*RedisSortedSetFlow)(nil)

type RedisSortedSetFlow struct {
	rdb             *redis.Client
	requestChannels []requestChannelData
	retryChannel    chan pipeline.RetryMessage
	resultChannel   chan api.ResultMessage
	pollInterval    time.Duration
	batchSize       int
	gateFactory     pipeline.GateFactory
}

// SortedSetOption is a functional option for configuring RedisSortedSetFlow.
type SortedSetOption func(*RedisSortedSetFlow)

// WithGateFactory sets a GateFactory for subscription/pool gate
// instantiation. Currently retained for forward compatibility; the
// sortedset flow does not yet wire subscription or pool gate chains.
func WithGateFactory(factory pipeline.GateFactory) SortedSetOption {
	return func(r *RedisSortedSetFlow) {
		r.gateFactory = factory
	}
}

func NewRedisSortedSetFlow(opts ...SortedSetOption) (*RedisSortedSetFlow, error) {
	configs, err := loadQueueConfigs()
	if err != nil {
		return nil, err
	}
	redisOpts, err := RedisOptions()
	if err != nil {
		return nil, fmt.Errorf("invalid Redis connection config: %w", err)
	}
	r := &RedisSortedSetFlow{
		rdb:             redis.NewClient(redisOpts),
		requestChannels: make([]requestChannelData, 0, len(configs)),
		retryChannel:    make(chan pipeline.RetryMessage),
		resultChannel:   make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:    time.Duration(*ssPollIntervalMs) * time.Millisecond,
		batchSize:       *ssBatchSize,
	}

	for _, opt := range opts {
		opt(r)
	}

	for _, cfg := range configs {
		headers, err := util.ExpandEnvMapValues(cfg.HTTPHeaders)
		if err != nil {
			panic(fmt.Sprintf("failed to expand http_headers for queue %q: %v", cfg.QueueName, err))
		}

		// Build effective static labels: auto-injected pool ID (the
		// queue name, since the sortedset flow models each queue as
		// its own singleton pool) layered under the queue's
		// declared labels.
		channelLabels := pipeline.Labels{"pool": cfg.QueueName}
		for k, v := range cfg.Labels {
			channelLabels[k] = v
		}

		ch := pipeline.RequestChannel{
			Channel:            make(chan *pipeline.EmbelishedRequestMessage),
			InferenceObjective: cfg.InferenceObjective,
			RequestPathURL:     util.NormalizeURLPath(cfg.RequestPathURL),
			IGWBaseURL:         util.NormalizeBaseURL(cfg.IGWBaseURL),
			HTTPHeaders:        headers,
			ModelNameOverride:  cfg.ModelNameOverride,
			Labels:             channelLabels,
			PoolID:             cfg.QueueName,
		}

		r.requestChannels = append(r.requestChannels, requestChannelData{
			channel:   ch,
			queueName: cfg.QueueName,
		})
	}

	return r, nil
}

func loadQueueConfigs() ([]queueConfig, error) {
	if *ssQueuesConfigFile != "" {
		data, err := os.ReadFile(*ssQueuesConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
		var configs []queueConfig
		if err := json.Unmarshal(data, &configs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config file: %w", err)
		}
		return configs, nil
	}
	return []queueConfig{{
		QueueName:          *ssRequestQueueName,
		InferenceObjective: *ssInferenceObjective,
		RequestPathURL:     *ssRequestPathURL,
		IGWBaseURL:         *ssIGWBaseURL,
	}}, nil
}

func (r *RedisSortedSetFlow) Start(ctx context.Context) {
	for _, ch := range r.requestChannels {
		go r.requestWorker(ctx, ch.channel.Channel, ch.queueName)
	}
	go r.retryWorker(ctx)
	go r.resultWorker(ctx)
}

func (r *RedisSortedSetFlow) RequestChannels() []pipeline.RequestChannel {
	channels := make([]pipeline.RequestChannel, len(r.requestChannels))
	for i, ch := range r.requestChannels {
		channels[i] = ch.channel
	}
	return channels
}

// Pools synthesizes one pool per queue. The Redis sortedset flow
// doesn't model pools natively, so each queue is its own singleton
// pool — preserves existing single-channel behavior under the new
// per-pool topology.
func (r *RedisSortedSetFlow) Pools() []pipeline.Pool {
	out := make([]pipeline.Pool, 0, len(r.requestChannels))
	for _, cd := range r.requestChannels {
		out = append(out, pipeline.Pool{
			ID:         cd.queueName,
			GatewayURL: cd.channel.IGWBaseURL,
		})
	}
	return out
}

func (r *RedisSortedSetFlow) RetryChannel() chan pipeline.RetryMessage {
	return r.retryChannel
}

func (r *RedisSortedSetFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *RedisSortedSetFlow) Characteristics() pipeline.Characteristics {
	return pipeline.Characteristics{HasExternalBackoff: false, SupportsMessageLatency: false}
}

// Polls sorted set and processes messages by deadline priority (earliest first)
func (r *RedisSortedSetFlow) requestWorker(ctx context.Context, msgChannel chan *pipeline.EmbelishedRequestMessage, queueName string) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	var requestChannel pipeline.RequestChannel
	for _, ch := range r.requestChannels {
		if ch.queueName == queueName {
			requestChannel = ch.channel
			break
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.processMessages(ctx, msgChannel, queueName, requestChannel, logger)
		}
	}
}

func (r *RedisSortedSetFlow) processMessages(ctx context.Context, msgChannel chan *pipeline.EmbelishedRequestMessage, queueName string, requestChannel pipeline.RequestChannel, logger logr.Logger) {
	currentTime := float64(time.Now().Unix())

	for i := 0; i < r.batchSize; i++ {
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
		// channelLabels is the operator-defined static label set
		// (auto-injected pool=queue_name + queue's declared labels),
		// already merged at startup. Producer-controlled per-message
		// data rides on body.Metadata, not Labels.
		ir.Labels = requestChannel.Labels.Clone()

		// Build the fully-embellished message so the merge policy and
		// worker receive it with all HTTP dispatch fields set.
		requestURL := requestChannel.IGWBaseURL + requestChannel.RequestPathURL
		if ep := ir.PublicRequest.ReqEndpoint(); ep != "" {
			requestURL = requestChannel.IGWBaseURL + ep
		}
		headers := map[string]string{"Content-Type": "application/json"}
		if requestChannel.InferenceObjective != "" {
			headers["x-gateway-inference-objective"] = requestChannel.InferenceObjective
		}
		for k, v := range requestChannel.HTTPHeaders {
			headers[k] = v
		}
		emb := &pipeline.EmbelishedRequestMessage{
			InternalRequest:   ir,
			HttpHeaders:       headers,
			RequestURL:        requestURL,
			Gate:              requestChannel.Gate,
			ModelNameOverride: requestChannel.ModelNameOverride,
			Labels:            pipeline.Labels(ir.Labels),
			PoolID:            requestChannel.PoolID,
		}

		select {
		case msgChannel <- emb:
		case <-ctx.Done():
			if err := retryRedisOp(context.Background(), func(ctx context.Context) error {
				return r.rdb.ZAdd(ctx, queueName, redis.Z{
					Score:  results[0].Score,
					Member: results[0].Member,
				}).Err()
			}); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to re-queue message on shutdown", "id", rview.ReqID())
			}
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
			queueName = *ssRequestQueueName
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
	defaultQueue := *ssResultQueueName
	queued := make(map[string][]string)
	for _, result := range batch {
		resultQueue := defaultQueue
		if result.Routing.ResultQueueName != "" {
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
