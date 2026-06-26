package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const (
	// resultChannelBuffer decouples inference workers from the result writer.
	// Workers can send results without blocking until the buffer is full.
	resultChannelBuffer = 64
	// maxBatchSize is the maximum number of messages flushed in a single
	// Redis pipeline call.
	maxBatchSize = 32
)

const retryPopBatchSize = 100

// Retry-polling interval bounds for the exponential backoff used by retryWorker.
// When the worker drains no due messages from the retry sorted set, the next
// sleep doubles up to maxRetryPollInterval; on a successful drain it resets to
// baseRetryPollInterval. Declared as var (rather than const) so tests can
// override to small values for fast, deterministic backoff coverage.
var (
	baseRetryPollInterval = 1 * time.Second
	maxRetryPollInterval  = 30 * time.Second
)

// nextRetryPollInterval returns the next polling interval given the current
// interval, base, and max. Doubles current, caps at max. Pure function so the
// backoff math is unit-testable independently of the Redis-backed worker loop.
func nextRetryPollInterval(current, base, max time.Duration) time.Duration {
	if current < base {
		return base
	}
	next := current * 2
	if next > max {
		return max
	}
	return next
}

// popDueRetryMessagesScript atomically fetches due retry entries (score <= now) and removes them.
var popDueRetryMessagesScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local items = redis.call("ZRANGEBYSCORE", key, "-inf", now, "LIMIT", 0, limit)
if #items > 0 then
  -- Chunk ZREM arguments to avoid Lua unpack stack limits if
  -- limit is increased significantly in the future.
  local chunk_size = 1000
  for i = 1, #items, chunk_size do
    local last = math.min(i + chunk_size - 1, #items)
    local chunk = {}
    for j = i, last do
      chunk[#chunk + 1] = items[j]
    end
    redis.call("ZREM", key, unpack(chunk))
  end
end
return items
`)

const maxRetries = 3

func retryRedisOp(ctx context.Context, fn func(ctx context.Context) error) error {
	var lastErr error
	for attempt := range maxRetries {
		execCtx := ctx
		if execCtx.Err() != nil {
			execCtx = context.Background()
		}
		if err := fn(execCtx); err != nil {
			lastErr = err
			if attempt == maxRetries-1 {
				break
			}
			// On shutdown (ctx cancelled), skip backoff and retry immediately
			// to maximize the chance of flushing data before SIGKILL.
			select {
			case <-time.After(time.Duration(1<<attempt) * 100 * time.Millisecond):
			case <-ctx.Done():
			}
		} else {
			return nil
		}
	}
	return lastErr
}

func drainBatch[T any](first T, channel <-chan T, maxBatchSize int) []T {
	batch := make([]T, 1, maxBatchSize)
	batch[0] = first
	for len(batch) < maxBatchSize {
		select {
		case item := <-channel:
			batch = append(batch, item)
		default:
			return batch
		}
	}
	return batch
}

type QueueConfig struct {
	QueueName          string            `json:"queue_name"`
	WorkerPoolID       string            `json:"worker_pool_id"`
	InferenceObjective string            `json:"inference_objective"`
	RequestPathURL     string            `json:"request_path_url"`
	IGWBaseURL         string            `json:"igw_base_url"`
	Labels             map[string]string `json:"labels,omitempty"`
}

type RequestChannelData struct {
	requestChannel pipeline.RequestChannel
	queueName      string
	labels         map[string]string
}

var (
	_ pipeline.Flow          = (*RedisMQFlow)(nil)
	_ pipeline.HealthChecker = (*RedisMQFlow)(nil)
)

type RedisMQFlow struct {
	rdb             *redis.Client
	requestChannels []RequestChannelData
	retryChannel    chan pipeline.RetryMessage
	resultChannel   chan api.ResultMessage
	retryQueueName  string
	resultQueueName string
	workerPools     []pipeline.WorkerPoolConfig
	consumeCancel   context.CancelFunc
	consumeWg       sync.WaitGroup
	drainCancel     context.CancelFunc
	drainWg         sync.WaitGroup
	enableTracing   bool
}

// RedisOption is a functional option for configuring RedisMQFlow.
type RedisOption func(*RedisMQFlow)

// WithRedisTracing enables per-command Redis tracing spans via redisotel.
func WithRedisTracing(enable bool) RedisOption {
	return func(r *RedisMQFlow) {
		r.enableTracing = enable
	}
}

// WithWorkerPools sets the pool configurations to resolve named pools.
func WithWorkerPools(workerPools []pipeline.WorkerPoolConfig) RedisOption {
	return func(r *RedisMQFlow) {
		r.workerPools = workerPools
	}
}

func NewRedisMQFlow(flowOpts PubSubFlowOptions, connOpts ConnectionOptions, fns ...RedisOption) (*RedisMQFlow, error) {
	redisOpts, err := ParseRedisOptions(connOpts.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis connection config: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	var configs []QueueConfig
	if flowOpts.QueuesConfig != "" {
		if err := json.Unmarshal([]byte(flowOpts.QueuesConfig), &configs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal inline queues config: %w", err)
		}
	} else if flowOpts.QueuesConfigFile != "" {
		data, err := os.ReadFile(flowOpts.QueuesConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read queues config file: %w", err)
		}
		if err := json.Unmarshal(data, &configs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal queues config: %w", err)
		}
	} else {
		configs = []QueueConfig{{
			QueueName:          flowOpts.RequestQueueName,
			WorkerPoolID:       "default",
			InferenceObjective: flowOpts.InferenceObjective,
			IGWBaseURL:         flowOpts.IGWBaseURL,
			RequestPathURL:     flowOpts.RequestPathURL,
		}}
	}

	flow := &RedisMQFlow{
		rdb:             rdb,
		retryChannel:    make(chan pipeline.RetryMessage),
		resultChannel:   make(chan api.ResultMessage, resultChannelBuffer),
		retryQueueName:  flowOpts.RetryQueueName,
		resultQueueName: flowOpts.ResultQueueName,
	}

	for _, fn := range fns {
		fn(flow)
	}

	var channels []RequestChannelData

	for _, cfg := range configs {
		ch := make(chan *api.InternalRequest)

		workerPoolID := cfg.WorkerPoolID
		if workerPoolID == "" {
			workerPoolID = "default"
		}

		found := false
		for _, pool := range flow.workerPools {
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

		channels = append(channels, RequestChannelData{
			requestChannel: pipeline.RequestChannel{
				Channel:            ch,
				InferenceObjective: cfg.InferenceObjective,
				RequestPathURL:     reqPath,
				IGWBaseURL:         cfg.IGWBaseURL,
				WorkerPoolID:       workerPoolID,
			},
			queueName: cfg.QueueName,
			labels:    cfg.Labels,
		})
	}
	if flow.enableTracing {
		if err := redisotel.InstrumentTracing(rdb); err != nil {
			_ = rdb.Close()
			return nil, fmt.Errorf("failed to instrument Redis tracing: %w", err)
		}
	}
	flow.requestChannels = channels

	return flow, nil
}

func (r *RedisMQFlow) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	consumeCtx, consumeCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.consumeCancel = consumeCancel

	drainCtx, drainCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.drainCancel = drainCancel

	for _, channelData := range r.requestChannels {
		r.consumeWg.Add(1)
		go func(cd RequestChannelData) {
			defer r.consumeWg.Done()
			requestWorker(consumeCtx, r.rdb, cd.requestChannel.Channel, cd.queueName, cd.labels)
		}(channelData)
	}

	r.drainWg.Add(3)
	go func() { defer r.drainWg.Done(); addMsgToRetryWorker(drainCtx, r.rdb, r.retryChannel, r.retryQueueName) }()
	go func() { defer r.drainWg.Done(); r.retryWorker(drainCtx, r.rdb) }()
	go func() { defer r.drainWg.Done(); r.resultWorker(drainCtx, r.resultQueueName) }() // #nosec G118
}

func (r *RedisMQFlow) StopConsuming() {
	if r.consumeCancel != nil {
		r.consumeCancel()
	}
	r.consumeWg.Wait()
}

func (r *RedisMQFlow) Shutdown() {
	if r.drainCancel != nil {
		r.drainCancel()
	}
	r.drainWg.Wait()
}
func (r *RedisMQFlow) RequestChannels() []pipeline.RequestChannel {

	var channels []pipeline.RequestChannel
	for _, channelData := range r.requestChannels {
		channels = append(channels, channelData.requestChannel)
	}
	return channels

}

func (r *RedisMQFlow) RetryChannel() chan pipeline.RetryMessage {
	return r.retryChannel
}

func (r *RedisMQFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

// Listening on the results channel and responsible for writing results into Redis.
// Batches multiple results into a single Redis pipeline call to reduce round-trips.
func (r *RedisMQFlow) resultWorker(ctx context.Context, resultsQueueName string) {
	processMsg := func(flushCtx context.Context, msg api.ResultMessage) {
		batch := drainBatch(msg, r.resultChannel, maxBatchSize)
		r.flushResultBatch(flushCtx, batch, resultsQueueName)
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

func (r *RedisMQFlow) flushResultBatch(ctx context.Context, batch []api.ResultMessage, resultsQueueName string) {
	logger := log.FromContext(ctx)
	if err := retryRedisOp(ctx, func(ctx context.Context) error {
		pipe := r.rdb.Pipeline()
		for _, result := range batch {
			pipe.Publish(ctx, resultsQueueName, marshalResultMessage(result))
		}
		_, err := pipe.Exec(ctx)
		return err
	}); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to publish result batch to Redis", "batchSize", len(batch))
	}
}

func marshalResultMessage(msg api.ResultMessage) string {
	if bytes, err := json.Marshal(msg); err == nil {
		return string(bytes)
	}
	fallback := map[string]string{"id": msg.ID, "error": "Failed to marshal result to string"}
	fallbackBytes, _ := json.Marshal(fallback)
	return string(fallbackBytes)
}

// pulls from Redis channel and put in the request channel.
const (
	reconnectDelay = 10 * time.Second
)

// Automatically reconnects when the subscription channel closes.
func requestWorker(ctx context.Context, rdb *redis.Client, msgChannel chan *api.InternalRequest, queueName string, labels map[string]string) {
	logger := log.FromContext(ctx)
	for ctx.Err() == nil {
		shouldReconnect := consumeSubscription(ctx, rdb, msgChannel, queueName, labels)
		if ctx.Err() != nil {
			return
		}
		if shouldReconnect {
			logger.V(logutil.DEFAULT).Info("Redis subscription interrupted, reconnecting", "delay", reconnectDelay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
		}
	}
}

func consumeSubscription(ctx context.Context, rdb *redis.Client, msgChannel chan *api.InternalRequest, queueName string, labels map[string]string) bool {
	logger := log.FromContext(ctx)
	sub := rdb.Subscribe(ctx, queueName)
	defer sub.Close() // nolint:errcheck

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return false
		case rmsg, ok := <-ch:
			if !ok {
				return true
			}
			var ir api.InternalRequest
			err := json.Unmarshal([]byte(rmsg.Payload), &ir)
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message from request channel")
				continue // skip this message
			}
			if ir.PublicRequest == nil {
				continue
			}
			ir.RequestQueueName = queueName
			if len(labels) > 0 {
				if ir.Labels == nil {
					ir.Labels = make(map[string]string, len(labels))
				}
				for k, v := range labels {
					ir.Labels[k] = v
				}
			}
			msgChannel <- &ir
		}
	}
}

func (r *RedisMQFlow) HealthCheck(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

func (r *RedisMQFlow) Characteristics() pipeline.Characteristics {
	return pipeline.Characteristics{
		HasExternalBackoff:     false,
		SupportsMessageLatency: false,
	}
}

// Puts msgs from the retry channel into a Redis sorted-set with a duration Score.
func addMsgToRetryWorker(ctx context.Context, rdb *redis.Client, retryChannel chan pipeline.RetryMessage, sortedSetName string) {
	logger := log.FromContext(ctx)
	addRetry := func(msg pipeline.RetryMessage) {
		if msg.InternalRequest == nil {
			return
		}
		score := float64(time.Now().Unix()) + msg.BackoffDurationSeconds
		bytes, err := json.Marshal(msg.InternalRequest)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to marshal message for retry in Redis")
			return
		}
		if err := retryRedisOp(ctx, func(opCtx context.Context) error {
			return rdb.ZAdd(opCtx, sortedSetName, redis.Z{Score: score, Member: string(bytes)}).Err()
		}); err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to add message for retry in Redis")
		}
	}
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-retryChannel:
					addRetry(msg)
				default:
					return
				}
			}

		case msg := <-retryChannel:
			addRetry(msg)
		}
	}

}

// Every second polls the sorted set and publishes the messages that need to be retried into the request queue
func (r *RedisMQFlow) retryWorker(ctx context.Context, rdb *redis.Client) {
	logger := log.FromContext(ctx)
	// create a map of queuename to channel based on requestchannels
	msgChannels := make(map[string]chan *api.InternalRequest)
	for _, channelData := range r.requestChannels {
		msgChannels[channelData.queueName] = channelData.requestChannel.Channel
	}

	// sleepDuration grows exponentially when no due retries are found and resets
	// to baseRetryPollInterval whenever the drain cycle processes at least one
	// message. This avoids hammering Redis with ZRANGEBYSCORE/ZREM at fixed 1Hz
	// while idle. See #100.
	sleepDuration := baseRetryPollInterval

	for {
		select {
		case <-ctx.Done():
			return

		default:
			// Keep one fixed cutoff for this drain cycle so we only process
			// messages due at cycle start, avoiding an ever-expanding window.
			currentTimeSec := time.Now().Unix()
			drainedAny := false

			for {
				results, err := popDueRetryMessages(ctx, rdb, r.retryQueueName, currentTimeSec, retryPopBatchSize)
				if err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to atomically pop due retry messages")
					break
				}
				if len(results) == 0 {
					break
				}
				drainedAny = true

				for i, msg := range results {
					var message api.InternalRequest
					err := json.Unmarshal([]byte(msg), &message)
					if err != nil {
						logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal retry message")
						continue
					}
					if message.PublicRequest == nil {
						continue
					}
					queueName := message.RequestQueueName
					msgChannel, ok := msgChannels[queueName]
					if !ok {
						logger.V(logutil.DEFAULT).Info("Unknown retry queue, dropping message", "queueName", queueName, "messageId", message.PublicRequest.ReqID())
						continue
					}

					select {
					case msgChannel <- &message:
					case <-ctx.Done():
						r.requeueRetryMessages(rdb, results[i:])
						return
					}
				}
			}

			if drainedAny {
				sleepDuration = baseRetryPollInterval
			} else {
				sleepDuration = nextRetryPollInterval(sleepDuration, baseRetryPollInterval, maxRetryPollInterval)
			}

			// Cancellable sleep — exit promptly on shutdown instead of waiting
			// out the remainder of the current interval (which can be up to
			// maxRetryPollInterval under the backoff).
			select {
			case <-time.After(sleepDuration):
			case <-ctx.Done():
				return
			}
		}
	}
}

// requeueRetryMessages puts undelivered messages back into the retry sorted set.
// Called only after ctx is cancelled, so context.Background() is used for Redis calls.
func (r *RedisMQFlow) requeueRetryMessages(rdb *redis.Client, messages []string) {
	if len(messages) == 0 {
		return
	}
	logger := log.FromContext(context.Background())
	score := float64(time.Now().Unix())
	if err := retryRedisOp(context.Background(), func(ctx context.Context) error {
		pipe := rdb.Pipeline()
		for _, msg := range messages {
			pipe.ZAdd(ctx, r.retryQueueName, redis.Z{Score: score, Member: msg})
		}
		_, err := pipe.Exec(ctx)
		return err
	}); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to requeue retry messages on shutdown", "count", len(messages))
	}
}

// popDueRetryMessages atomically pops up to limit retry messages whose score is <= nowUnixSec.
// It returns the raw message payloads removed from the sorted set.
func popDueRetryMessages(ctx context.Context, rdb *redis.Client, key string, nowUnixSec int64, limit int) ([]string, error) {
	raw, err := popDueRetryMessagesScript.Run(ctx, rdb, []string{key}, nowUnixSec, limit).Result()
	if err != nil {
		return nil, err
	}

	entries, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected script result type: %T", raw)
	}

	messages := make([]string, 0, len(entries))
	for _, entry := range entries {
		msg, ok := entry.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected script entry type: %T", entry)
		}
		messages = append(messages, msg)
	}

	return messages, nil
}
