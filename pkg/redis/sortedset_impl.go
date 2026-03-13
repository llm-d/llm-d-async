package redis

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const SORTEDSET_QUEUE_NAME_KEY = "queue_name"

var (
	ssRedisAddr          = flag.String("redis.ss.addr", "localhost:6379", "Redis server address")
	ssRequestPathURL     = flag.String("redis.ss.request-path-url", "/v1/completions", "Request path URL")
	ssInferenceObjective = flag.String("redis.ss.inference-objective", "", "Inference objective header")
	ssRequestQueueName   = flag.String("redis.ss.request-queue-name", "request-sortedset", "Request sorted set name")
	ssResultQueueName    = flag.String("redis.ss.result-queue-name", "result-list", "Result list name")
	ssQueuesConfigFile   = flag.String("redis.ss.queues-config-file", "", "Multiple queues config file")
	ssPollIntervalMs     = flag.Int("redis.ss.poll-interval-ms", 1000, "Poll interval in milliseconds")
	ssBatchSize          = flag.Int("redis.ss.batch-size", 10, "Number of messages to process per poll")
)

type queueConfig struct {
	QueueName          string `json:"queue_name"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
}

type requestChannelData struct {
	channel   api.RequestChannel
	queueName string
}

type RedisSortedSetFlow struct {
	rdb             *redis.Client
	requestChannels []requestChannelData
	retryChannel    chan api.RetryMessage
	resultChannel   chan api.ResultMessage
	pollInterval    time.Duration
	batchSize       int
	gate            flowcontrol.DispatchGate
}

// SortedSetOption is a functional option for configuring RedisSortedSetFlow
type SortedSetOption func(*RedisSortedSetFlow)

// WithDispatchGate enables dispatch gating with the provided gate.
// When enabled, the flow checks the "dispatch budget" before processing messages
// and scales the batch size proportionally to available capacity.
// Default: always dispatch all.
func WithDispatchGate(gate flowcontrol.DispatchGate) SortedSetOption {
	return func(r *RedisSortedSetFlow) {
		r.gate = gate
	}
}

func NewRedisSortedSetFlow(opts ...SortedSetOption) *RedisSortedSetFlow {
	configs := loadQueueConfigs()
	channels := make([]requestChannelData, 0, len(configs))

	for _, cfg := range configs {
		channels = append(channels, requestChannelData{
			channel: api.RequestChannel{
				Channel:            make(chan api.RequestMessage),
				InferenceObjective: cfg.InferenceObjective,
				RequestPathURL:     util.NormalizeURLPath(cfg.RequestPathURL),
			},
			queueName: cfg.QueueName,
		})
	}

	r := &RedisSortedSetFlow{
		rdb:             redis.NewClient(&redis.Options{Addr: *ssRedisAddr}),
		requestChannels: channels,
		retryChannel:    make(chan api.RetryMessage),
		resultChannel:   make(chan api.ResultMessage),
		pollInterval:    time.Duration(*ssPollIntervalMs) * time.Millisecond,
		batchSize:       *ssBatchSize,
	}

	for _, opt := range opts {
		opt(r)
	}

	if r.gate == nil {
		r.gate = flowcontrol.ConstOpenGate()
	}

	return r
}

func loadQueueConfigs() []queueConfig {
	if *ssQueuesConfigFile != "" {
		data, err := os.ReadFile(*ssQueuesConfigFile)
		if err != nil {
			panic(fmt.Sprintf("failed to read config file: %v", err))
		}
		var configs []queueConfig
		if err := json.Unmarshal(data, &configs); err != nil {
			panic(fmt.Sprintf("failed to unmarshal config: %v", err))
		}
		return configs
	}
	return []queueConfig{{
		QueueName:          *ssRequestQueueName,
		InferenceObjective: *ssInferenceObjective,
		RequestPathURL:     *ssRequestPathURL,
	}}
}

func (r *RedisSortedSetFlow) Start(ctx context.Context) {
	for _, ch := range r.requestChannels {
		go r.requestWorker(ctx, ch.channel.Channel, ch.queueName)
	}
	go r.retryWorker(ctx)
	go r.resultWorker(ctx)
}

func (r *RedisSortedSetFlow) RequestChannels() []api.RequestChannel {
	channels := make([]api.RequestChannel, len(r.requestChannels))
	for i, ch := range r.requestChannels {
		channels[i] = ch.channel
	}
	return channels
}

func (r *RedisSortedSetFlow) RetryChannel() chan api.RetryMessage {
	return r.retryChannel
}

func (r *RedisSortedSetFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *RedisSortedSetFlow) Characteristics() api.Characteristics {
	return api.Characteristics{HasExternalBackoff: false, SupportsMessageLatency: false}
}

// Polls sorted set and processes messages by deadline priority (earliest first)
func (r *RedisSortedSetFlow) requestWorker(ctx context.Context, msgChannel chan api.RequestMessage, queueName string) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.processMessages(ctx, msgChannel, queueName, logger)
		}
	}
}

func (r *RedisSortedSetFlow) processMessages(ctx context.Context, msgChannel chan api.RequestMessage, queueName string, logger logr.Logger) {
	currentTime := float64(time.Now().Unix())

	budget := r.gate.Budget(ctx)
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

		msg, deadline, ok := r.parseMessage(results[0], logger)
		if !ok {
			continue
		}

		if deadline < currentTime {
			logger.V(logutil.DEFAULT).Info("Deadline expired", "id", msg.Id)
			continue
		}

		if msg.Metadata == nil {
			msg.Metadata = make(map[string]string)
		}
		msg.Metadata[SORTEDSET_QUEUE_NAME_KEY] = queueName

		select {
		case msgChannel <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (r *RedisSortedSetFlow) parseMessage(z redis.Z, logger logr.Logger) (api.RequestMessage, float64, bool) {
	var msg api.RequestMessage
	if err := json.Unmarshal([]byte(z.Member.(string)), &msg); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message")
		return msg, 0, false
	}

	deadline, err := strconv.ParseInt(msg.DeadlineUnixSec, 10, 64)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Invalid deadline", "id", msg.Id)
		return msg, 0, false
	}

	return msg, float64(deadline), true
}

// Re-queues failed messages with exponential backoff
func (r *RedisSortedSetFlow) retryWorker(ctx context.Context) {
	logger := log.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-r.retryChannel:
			queueName := msg.Metadata[SORTEDSET_QUEUE_NAME_KEY]
			if queueName == "" {
				queueName = *ssRequestQueueName
			}

			bytes, err := json.Marshal(msg.RequestMessage)
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to marshal retry")
				continue
			}

			retryScore := float64(time.Now().Unix()) + msg.BackoffDurationSeconds
			if err := r.rdb.ZAdd(ctx, queueName, redis.Z{Score: retryScore, Member: string(bytes)}).Err(); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to add retry")
			}
		}
	}
}

// Pushes results to Redis list (FIFO)
// Routes results to the queue specified in request metadata, or default queue if not specified
func (r *RedisSortedSetFlow) resultWorker(ctx context.Context) {
	logger := log.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-r.resultChannel:
			// Check metadata for custom result queue (set by producer)
			resultQueue := *ssResultQueueName // default queue
			if msg.Metadata != nil {
				if customQueue, ok := msg.Metadata["result_queue"]; ok && customQueue != "" {
					resultQueue = customQueue
				}
			}

			msgStr := r.marshalResult(msg)
			if err := r.rdb.LPush(ctx, resultQueue, msgStr).Err(); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to push result", "queue", resultQueue)
			} else {
				logger.V(logutil.DEBUG).Info("Pushed result to queue", "id", msg.Id, "queue", resultQueue)
			}
		}
	}
}

func (r *RedisSortedSetFlow) marshalResult(msg api.ResultMessage) string {
	if bytes, err := json.Marshal(msg); err == nil {
		return string(bytes)
	}
	return fmt.Sprintf(`{"id":"%s","payload":"{\"error\":\"marshal failed\"}"}`, msg.Id)
}
