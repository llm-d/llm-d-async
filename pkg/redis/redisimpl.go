package redis

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"
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

var (
	igwBaseURL         = flag.String("redis.igw-base-url", "", "Base URL for IGW. Mutually exclusive with redis.queues-config-file flag.")
	requestPathURL     = flag.String("redis.request-path-url", "/v1/completions", "request path url. Mutually exclusive with redis.queues-config-file flag.")
	inferenceObjective = flag.String("redis.inference-objective", "", "inference objective to use in requests. Mutually exclusive with redis.queues-config-file flag.")
	requestQueueName   = flag.String("redis.request-queue-name", "request-queue", "name of the Redis channel for request messages. Mutually exclusive with redis.queues-config-file flag.")

	retryQueueName  = flag.String("redis.retry-queue-name", "retry-sortedset", "name of the Redis sorted set for retry messages")
	resultQueueName = flag.String("redis.result-queue-name", "result-queue", "name of the Redis channel for result messages")

	queuesConfigFile = flag.String("redis.queues-config-file", "", "Queues Configuration file. Mutually exclusive with redis.igw-base-url, redis.request-queue-name, redis.request-path-url and redis.inference-objective flags. See documentation about syntax")
)

const retryPopBatchSize = 100

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
	QueueName          string `json:"queue_name"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
	IGWBaseURl         string `json:"igw_base_url"`
}

type RequestChannelData struct {
	requestChannel pipeline.RequestChannel
	queueName      string
}

var _ pipeline.Flow = (*RedisMQFlow)(nil)

type RedisMQFlow struct {
	rdb             *redis.Client
	requestChannels []RequestChannelData
	retryChannel    chan pipeline.RetryMessage
	resultChannel   chan api.ResultMessage
}

func NewRedisMQFlow() (*RedisMQFlow, error) {
	opts, err := RedisOptions()
	if err != nil {
		return nil, fmt.Errorf("invalid Redis connection config: %w", err)
	}
	rdb := redis.NewClient(opts)
	var configs []QueueConfig
	if *queuesConfigFile != "" {
		data, err := os.ReadFile(*queuesConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read queues config file: %w", err)
		}

		if err := json.Unmarshal(data, &configs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal queues config: %w", err)
		}
	} else {
		configs = []QueueConfig{{QueueName: *requestQueueName, IGWBaseURl: *igwBaseURL, InferenceObjective: *inferenceObjective, RequestPathURL: *requestPathURL}}
	}

	var channels []RequestChannelData

	for _, cfg := range configs {
		ch := make(chan *api.InternalRequest)

		channels = append(channels, RequestChannelData{pipeline.RequestChannel{
			Channel:            ch,
			InferenceObjective: cfg.InferenceObjective,
			RequestPathURL:     util.NormalizeURLPath(cfg.RequestPathURL),
			IGWBaseURl:         util.NormalizeBaseURL(cfg.IGWBaseURl),
		}, cfg.QueueName})
	}
	return &RedisMQFlow{
		rdb:             rdb,
		requestChannels: channels,
		retryChannel:    make(chan pipeline.RetryMessage),
		resultChannel:   make(chan api.ResultMessage, resultChannelBuffer),
	}, nil
}

func (r *RedisMQFlow) Start(ctx context.Context) {

	for _, channelData := range r.requestChannels {
		go requestWorker(ctx, r.rdb, channelData.requestChannel.Channel, channelData.queueName)
	}

	go addMsgToRetryWorker(ctx, r.rdb, r.retryChannel, *retryQueueName)

	go r.retryWorker(ctx, r.rdb)

	go r.resultWorker(ctx, *resultQueueName)
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

// pulls from Redis channel and put in the request channel
func requestWorker(ctx context.Context, rdb *redis.Client, msgChannel chan *api.InternalRequest, queueName string) {
	logger := log.FromContext(ctx)
	sub := rdb.Subscribe(ctx, queueName)
	defer sub.Close() // nolint:errcheck

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return

		case rmsg := <-ch:
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
			msgChannel <- &ir
		}
	}

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
	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-retryChannel:
			if msg.InternalRequest == nil {
				continue
			}
			score := float64(time.Now().Unix()) + msg.BackoffDurationSeconds
			bytes, err := json.Marshal(msg.InternalRequest)
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to marshal message for retry in Redis")
				continue // skip this message.
			}
			err = rdb.ZAdd(ctx, sortedSetName, redis.Z{
				Score:  score,
				Member: string(bytes),
			}).Err()

			if err != nil {
				// skip this message. We're not going to retry a "preparing to retry" step.
				logger.V(logutil.DEFAULT).Error(err, "Failed to add message for retry in Redis")
			}
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

	for {
		select {
		case <-ctx.Done():
			return

		default:
			// Keep one fixed cutoff for this drain cycle so we only process
			// messages due at cycle start, avoiding an ever-expanding window.
			currentTimeSec := time.Now().Unix()

			for {
				results, err := popDueRetryMessages(ctx, rdb, *retryQueueName, currentTimeSec, retryPopBatchSize)
				if err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to atomically pop due retry messages")
					break
				}
				if len(results) == 0 {
					break
				}

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
			time.Sleep(time.Second)
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
			pipe.ZAdd(ctx, *retryQueueName, redis.Z{Score: score, Member: msg})
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
