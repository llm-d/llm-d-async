package redis

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/redis/go-redis/v9"

	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var (
	redisAddr = flag.String("redis.addr", "localhost:6379", "address of the Redis server")

	// TODO: support multiple request queues with metadata (for policy), maybe 'redis.request-1-inference-gateway' and other request related flags/parameters
	requestPathURL     = flag.String("redis.request-path-url", "/v1/completions", "request path url")
	inferenceObjective = flag.String("redis.inference-objective", "", "inference objective to use in requests")
	requestQueueName   = flag.String("redis.request-queue-name", "request-queue", "name of the Redis channel for request messages")

	retryQueueName  = flag.String("redis.retry-queue-name", "retry-sortedset", "name of the Redis sorted set for retry messages")
	resultQueueName = flag.String("redis.result-queue-name", "result-queue", "name of the Redis channel for result messages")
)

type RedisMQFlow struct {
	rdb            *redis.Client
	requestChannel chan api.RequestMessage
	retryChannel   chan api.RetryMessage
	resultChannel  chan api.ResultMessage
}

func NewRedisMQFlow() *RedisMQFlow {
	rdb := redis.NewClient(&redis.Options{
		Addr: *redisAddr,
	})
	return &RedisMQFlow{
		rdb:            rdb,
		requestChannel: make(chan api.RequestMessage),
		retryChannel:   make(chan api.RetryMessage),
		resultChannel:  make(chan api.ResultMessage),
	}
}

func (r *RedisMQFlow) Start(ctx context.Context) {
	go requestWorker(ctx, r.rdb, r.requestChannel, *requestQueueName)

	go addMsgToRetryWorker(ctx, r.rdb, r.retryChannel, *retryQueueName)

	go retryWorker(ctx, r.rdb, r.requestChannel)

	go resultWorker(ctx, r.rdb, r.resultChannel, *resultQueueName)
}
func (r *RedisMQFlow) RequestChannels() []api.RequestChannel {

	return []api.RequestChannel{{Channel: r.requestChannel, InferenceObjective: *inferenceObjective, RequestPathURL: *requestPathURL}}
}

func (r *RedisMQFlow) RetryChannel() chan api.RetryMessage {
	return r.retryChannel
}

func (r *RedisMQFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

// Listening on the results channel and responsible for writing results into Redis.
func resultWorker(ctx context.Context, rdb *redis.Client, resultChannel chan api.ResultMessage, resultsQueueName string) {
	logger := log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-resultChannel:
			bytes, err := json.Marshal(msg)
			var msgStr string
			if err != nil {
				msgStr = fmt.Sprintf(`{"id" : "%s", "error": "%s"}`, msg.Id, "Failed to marshal result to string")
			} else {
				msgStr = string(bytes)
			}
			err = publishRedis(ctx, rdb, resultsQueueName, msgStr)
			if err != nil {
				// Not going to retry here. Just log the error.
				logger.V(logutil.DEFAULT).Error(err, "Failed to publish result message to Redis")
			}
		}
	}
}

// pulls from Redis channel and put in the request channel
func requestWorker(ctx context.Context, rdb *redis.Client, msgChannel chan api.RequestMessage, queueName string) {
	logger := log.FromContext(ctx)
	sub := rdb.Subscribe(ctx, queueName)
	defer sub.Close()

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
			msgChannel <- msg
		}
	}

}

func (r *RedisMQFlow) Characteristics() api.Characteristics {
	return api.Characteristics{
		HasExternalBackoff: false,
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
func retryWorker(ctx context.Context, rdb *redis.Client, msgChannel chan api.RequestMessage) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			currentTimeSec := float64(time.Now().Unix())
			results, err := rdb.ZRangeByScore(ctx, *retryQueueName, &redis.ZRangeBy{
				Min: "0",
				Max: strconv.FormatFloat(currentTimeSec, 'f', -1, 64),
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
				// TODO: We probably want to write here back to the request queue/channel in Redis. Adding the msg to the
				// golang channel directly is not that wise as this might be blocking.
				msgChannel <- message
			}
			time.Sleep(time.Second)
		}
	}

}

func publishRedis(ctx context.Context, rdb *redis.Client, channelId, msg string) error {
	logger := log.FromContext(ctx)
	err := rdb.Publish(ctx, channelId, msg).Err()
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Error publishing message:%s\n", err.Error())
		return err
	}
	return nil
}
