package redis

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"
	"github.com/redis/go-redis/v9"

	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const QUEUE_NAME_KEY = "queue_name"

const (
	// resultChannelBuffer decouples inference workers from the result writer.
	// Workers can send results without blocking until the buffer is full.
	resultChannelBuffer = 64
	// maxResultBatchSize is the maximum number of results flushed in a single
	// Redis pipeline call.
	maxResultBatchSize = 32
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

type QueueConfig struct {
	QueueName          string `json:"queue_name"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
	IGWBaseURl         string `json:"igw_base_url"`
}

type RequestChannelData struct {
	requestChannel api.RequestChannel
	queueName      string
}

type RedisMQFlow struct {
	rdb             *redis.Client
	requestChannels []RequestChannelData
	retryChannel    chan api.RetryMessage
	resultChannel   chan api.ResultMessage
}

func NewRedisMQFlow() *RedisMQFlow {
	rdb := redis.NewClient(&redis.Options{
		Addr:     *RedisAddr,
		Username: *RedisUser,
		Password: *RedisPassword,
	})
	var configs []QueueConfig
	if *queuesConfigFile != "" {
		data, err := os.ReadFile(*queuesConfigFile)
		if err != nil {
			panic(fmt.Sprintf("failed to read queues config file: %v", err))
		}

		if err := json.Unmarshal(data, &configs); err != nil {
			panic(fmt.Sprintf("failed to unmarshal queues config: %v", err))
		}
	} else {
		configs = []QueueConfig{{QueueName: *requestQueueName, IGWBaseURl: *igwBaseURL, InferenceObjective: *inferenceObjective, RequestPathURL: *requestPathURL}}
	}

	var channels []RequestChannelData

	for _, cfg := range configs {
		ch := make(chan api.RequestMessage)

		channels = append(channels, RequestChannelData{api.RequestChannel{
			Channel:            ch,
			InferenceObjective: cfg.InferenceObjective,
			RequestPathURL:     util.NormalizeURLPath(cfg.RequestPathURL),
			IGWBaseURl:         util.NormalizeBaseURL(cfg.IGWBaseURl),
		}, cfg.QueueName})
	}
	return &RedisMQFlow{
		rdb:             rdb,
		requestChannels: channels,
		retryChannel:    make(chan api.RetryMessage),
		resultChannel:   make(chan api.ResultMessage, resultChannelBuffer),
	}
}

func (r *RedisMQFlow) Start(ctx context.Context) {

	for _, channelData := range r.requestChannels {
		go requestWorker(ctx, r.rdb, channelData.requestChannel.Channel, channelData.queueName)
	}

	go addMsgToRetryWorker(ctx, r.rdb, r.retryChannel, *retryQueueName)

	go r.retryWorker(ctx, r.rdb)

	go resultWorker(ctx, r.rdb, r.resultChannel, *resultQueueName)
}
func (r *RedisMQFlow) RequestChannels() []api.RequestChannel {

	var channels []api.RequestChannel
	for _, channelData := range r.requestChannels {
		channels = append(channels, channelData.requestChannel)
	}
	return channels

}

func (r *RedisMQFlow) RetryChannel() chan api.RetryMessage {
	return r.retryChannel
}

func (r *RedisMQFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

// Listening on the results channel and responsible for writing results into Redis.
// Batches multiple results into a single Redis pipeline call to reduce round-trips.
func resultWorker(ctx context.Context, rdb *redis.Client, resultChannel chan api.ResultMessage, resultsQueueName string) {
	logger := log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-resultChannel:
			// Collect a batch: start with the message we already received,
			// then drain any additional available messages without blocking.
			batch := make([]api.ResultMessage, 1, maxResultBatchSize)
			batch[0] = msg
			done := false
			for len(batch) < maxResultBatchSize && !done {
				select {
				case result := <-resultChannel:
					batch = append(batch, result)
				default:
					done = true
				}
			}

			pipe := rdb.Pipeline()
			for _, result := range batch {
				pipe.Publish(ctx, resultsQueueName, marshalResultMessage(result))
			}
			if _, err := pipe.Exec(ctx); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to publish result batch to Redis", "batchSize", len(batch))
			}
		}
	}
}

func marshalResultMessage(msg api.ResultMessage) string {
	if bytes, err := json.Marshal(msg); err == nil {
		return string(bytes)
	}
	fallback := map[string]string{"id": msg.Id, "error": "Failed to marshal result to string"}
	fallbackBytes, _ := json.Marshal(fallback)
	return string(fallbackBytes)
}

// pulls from Redis channel and put in the request channel
func requestWorker(ctx context.Context, rdb *redis.Client, msgChannel chan api.RequestMessage, queueName string) {
	logger := log.FromContext(ctx)
	sub := rdb.Subscribe(ctx, queueName)
	defer sub.Close() // nolint:errcheck

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return

		case rmsg := <-ch:
			var msg api.RequestMessage

			err := json.Unmarshal([]byte(rmsg.Payload), &msg)
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message from request channel")
				continue // skip this message

			}
			if msg.Metadata == nil {
				msg.Metadata = make(map[string]string)
			}
			msg.Metadata[QUEUE_NAME_KEY] = queueName
			msgChannel <- msg
		}
	}

}

func (r *RedisMQFlow) Characteristics() api.Characteristics {
	return api.Characteristics{
		HasExternalBackoff:     false,
		SupportsMessageLatency: false,
	}
}

// Puts msgs from the retry channel into a Redis sorted-set with a duration Score.
func addMsgToRetryWorker(ctx context.Context, rdb *redis.Client, retryChannel chan api.RetryMessage, sortedSetName string) {
	logger := log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-retryChannel:
			score := float64(time.Now().Unix()) + msg.BackoffDurationSeconds
			bytes, err := json.Marshal(msg.RequestMessage)
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
	// create a map of queuename to channel based on requestchannels
	msgChannels := make(map[string]chan api.RequestMessage)
	for _, channelData := range r.requestChannels {
		msgChannels[channelData.queueName] = channelData.requestChannel.Channel
	}

	for {
		select {
		case <-ctx.Done():
			return

		default:
			currentTimeSec := float64(time.Now().Unix())

			results, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key:     *retryQueueName,
				Start:   "0",
				Stop:    strconv.FormatFloat(currentTimeSec, 'f', -1, 64),
				ByScore: true,
			}).Result()
			if err != nil {
				panic(err)
			}
			for _, msg := range results {
				var message api.RequestMessage
				err := json.Unmarshal([]byte(msg), &message)
				if err != nil {
					fmt.Println(err)

				}
				err = rdb.ZRem(ctx, *retryQueueName, msg).Err()
				if err != nil {
					fmt.Println(err)

				}
				queueName := message.Metadata[QUEUE_NAME_KEY]

				// TODO: We probably want to write here back to the request queue/channel in Redis. Adding the msg to the
				// golang channel directly is not that wise as this might be blocking.
				msgChannels[queueName] <- message
			}
			time.Sleep(time.Second)
		}
	}

}

