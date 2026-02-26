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

const SORTEDSET_QUEUE_NAME_KEY = "queue_name"

var (
	// Sorted Set implementation flags
	ssRedisAddr = flag.String("redis.ss.addr", "localhost:6379", "address of the Redis server for sorted set implementation")

	ssRequestPathURL     = flag.String("redis.ss.request-path-url", "/v1/completions", "request path url. Mutually exclusive with redis.ss.queues-config-file flag.")
	ssInferenceObjective = flag.String("redis.ss.inference-objective", "", "inference objective to use in requests. Mutually exclusive with redis.ss.queues-config-file flag.")
	ssRequestQueueName   = flag.String("redis.ss.request-queue-name", "request-sortedset", "name of the Redis sorted set for request messages. Mutually exclusive with redis.ss.queues-config-file flag.")

	ssResultQueueName = flag.String("redis.ss.result-queue-name", "result-list", "name of the Redis list for result messages")

	ssQueuesConfigFile = flag.String("redis.ss.queues-config-file", "", "Queues Configuration file. Mutually exclusive with redis.ss.request-queue-name, redis.ss.request-path-url and redis.ss.inference-objective flags.")

	// Polling interval for checking sorted set
	ssPollIntervalMs = flag.Int("redis.ss.poll-interval-ms", 1000, "polling interval in milliseconds for checking sorted set")
)

type SortedSetQueueConfig struct {
	QueueName          string `json:"queue_name"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
}

type SortedSetRequestChannelData struct {
	requestChannel api.RequestChannel
	queueName      string
}

type RedisSortedSetFlow struct {
	rdb             *redis.Client
	requestChannels []SortedSetRequestChannelData
	retryChannel    chan api.RetryMessage
	resultChannel   chan api.ResultMessage
	pollInterval    time.Duration
}

func NewRedisSortedSetFlow() *RedisSortedSetFlow {
	rdb := redis.NewClient(&redis.Options{
		Addr: *ssRedisAddr,
	})

	var configs []SortedSetQueueConfig
	if *ssQueuesConfigFile != "" {
		data, err := os.ReadFile(*ssQueuesConfigFile)
		if err != nil {
			panic(fmt.Sprintf("failed to read queues config file: %v", err))
		}

		if err := json.Unmarshal(data, &configs); err != nil {
			panic(fmt.Sprintf("failed to unmarshal queues config: %v", err))
		}
	} else {
		configs = []SortedSetQueueConfig{
			{
				QueueName:          *ssRequestQueueName,
				InferenceObjective: *ssInferenceObjective,
				RequestPathURL:     *ssRequestPathURL,
			},
		}
	}

	var channels []SortedSetRequestChannelData

	for _, cfg := range configs {
		ch := make(chan api.RequestMessage)

		channels = append(channels, SortedSetRequestChannelData{
			api.RequestChannel{
				Channel:            ch,
				InferenceObjective: cfg.InferenceObjective,
				RequestPathURL:     util.NormalizeURLPath(cfg.RequestPathURL),
			},
			cfg.QueueName,
		})
	}

	return &RedisSortedSetFlow{
		rdb:             rdb,
		requestChannels: channels,
		retryChannel:    make(chan api.RetryMessage),
		resultChannel:   make(chan api.ResultMessage),
		pollInterval:    time.Duration(*ssPollIntervalMs) * time.Millisecond,
	}
}

func (r *RedisSortedSetFlow) Start(ctx context.Context) {
	// Start request workers for each queue (sorted set)
	for _, channelData := range r.requestChannels {
		go r.sortedSetRequestWorker(ctx, channelData.requestChannel.Channel, channelData.queueName)
	}

	// Start retry worker - adds messages back to the sorted set
	go r.sortedSetRetryWorker(ctx)

	// Start result worker - writes to FIFO list
	go r.listResultWorker(ctx)
}

func (r *RedisSortedSetFlow) RequestChannels() []api.RequestChannel {
	var channels []api.RequestChannel
	for _, channelData := range r.requestChannels {
		channels = append(channels, channelData.requestChannel)
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
	return api.Characteristics{
		HasExternalBackoff: false,
	}
}

// Polls the sorted set and pushes messages to the request channel
// Messages are scored by their deadline (Unix timestamp)
func (r *RedisSortedSetFlow) sortedSetRequestWorker(ctx context.Context, msgChannel chan api.RequestMessage, queueName string) {
	logger := log.FromContext(ctx)
	logger.V(logutil.DEFAULT).Info("Starting sorted set request worker", "queue", queueName)

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.V(logutil.DEFAULT).Info("Sorted set request worker stopping", "queue", queueName)
			return

		case <-ticker.C:
			// Fetch messages that are ready to be processed (score <= current time)
			currentTimeSec := float64(time.Now().Unix())
			results, err := r.rdb.ZRangeByScoreWithScores(ctx, queueName, &redis.ZRangeBy{
				Min:    "0",
				Max:    strconv.FormatFloat(currentTimeSec, 'f', -1, 64),
				Offset: 0,
				Count:  10, // Process up to 10 messages per poll
			}).Result()

			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to read from sorted set", "queue", queueName)
				continue
			}

			for _, z := range results {
				var msg api.RequestMessage
				err := json.Unmarshal([]byte(z.Member.(string)), &msg)
				if err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message from sorted set")
					// Remove malformed message
					r.rdb.ZRem(ctx, queueName, z.Member)
					continue
				}

				// Remove from sorted set before processing
				err = r.rdb.ZRem(ctx, queueName, z.Member).Err()
				if err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to remove message from sorted set")
					continue
				}

				// Add queue name to metadata
				if msg.Metadata == nil {
					msg.Metadata = make(map[string]string)
				}
				msg.Metadata[SORTEDSET_QUEUE_NAME_KEY] = queueName

				// Send to processing channel
				select {
				case msgChannel <- msg:
					logger.V(logutil.DEBUG).Info("Message sent to processing channel", "id", msg.Id)
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// Listens on the retry channel and adds messages back to the sorted set
// with a new score based on the current time + backoff duration
func (r *RedisSortedSetFlow) sortedSetRetryWorker(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.V(logutil.DEFAULT).Info("Starting sorted set retry worker")

	for {
		select {
		case <-ctx.Done():
			logger.V(logutil.DEFAULT).Info("Sorted set retry worker stopping")
			return

		case msg := <-r.retryChannel:
			// Calculate new score: current time + backoff duration
			newScore := float64(time.Now().Unix()) + msg.BackoffDurationSeconds

			// Get the original queue name from metadata
			queueName := msg.Metadata[SORTEDSET_QUEUE_NAME_KEY]
			if queueName == "" {
				// Fallback to default queue if not found
				queueName = *ssRequestQueueName
			}

			// Marshal the request message
			bytes, err := json.Marshal(msg.RequestMessage)
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to marshal message for retry")
				continue
			}

			// Add back to the sorted set with new score
			err = r.rdb.ZAdd(ctx, queueName, redis.Z{
				Score:  newScore,
				Member: string(bytes),
			}).Err()

			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to add message for retry to sorted set")
				continue
			}

			logger.V(logutil.DEBUG).Info("Message scheduled for retry",
				"id", msg.Id,
				"queue", queueName,
				"retry_count", msg.RetryCount,
				"backoff_seconds", msg.BackoffDurationSeconds)
		}
	}
}

// Listens on the result channel and pushes results to a Redis list (FIFO)
func (r *RedisSortedSetFlow) listResultWorker(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.V(logutil.DEFAULT).Info("Starting list result worker", "queue", *ssResultQueueName)

	for {
		select {
		case <-ctx.Done():
			logger.V(logutil.DEFAULT).Info("List result worker stopping")
			return

		case msg := <-r.resultChannel:
			bytes, err := json.Marshal(msg)
			var msgStr string
			if err != nil {
				msgStr = fmt.Sprintf(`{"id" : "%s", "payload": "{\"error\": \"Failed to marshal result to string\"}"}`, msg.Id)
			} else {
				msgStr = string(bytes)
			}

			// Push to the left side of the list (LPUSH)
			// Consumers would use RPOP to get FIFO behavior
			err = r.rdb.LPush(ctx, *ssResultQueueName, msgStr).Err()
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to push result message to Redis list")
			} else {
				logger.V(logutil.DEBUG).Info("Result message pushed to list", "id", msg.Id)
			}
		}
	}
}
