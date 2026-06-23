//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ap "github.com/llm-d-incubation/llm-d-async/pkg/async"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
)

func TestRedisImpl(t *testing.T) {
	s := miniredis.RunT(t)
	redisURL := fmt.Sprintf("redis://%s:%s", s.Host(), s.Port())

	ctx := context.Background()

	flowOpts := redis.PubSubFlowOptions{
		IGWBaseURL:       "http://localhost:30800",
		RequestPathURL:   "/v1/completions",
		RequestQueueName: "request-queue",
		RetryQueueName:   "retry-sortedset",
		ResultQueueName:  "result-queue",
	}
	connOpts := redis.ConnectionOptions{URL: redisURL}
	flow, err := redis.NewRedisMQFlow(flowOpts, connOpts, redis.WithWorkerPools([]pipeline.WorkerPoolConfig{
		{
			ID:      "default",
			Workers: 1,
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	flow.Start(ctx)

	flow.RetryChannel() <- pipeline.RetryMessage{
		EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{
			InternalRequest: api.NewInternalRequest(
				api.InternalRouting{RequestQueueName: "request-queue"},
				&api.RequestMessage{
					ID:       "test-id",
					Created:  time.Now().Unix(),
					Deadline: time.Now().Add(time.Minute).Unix(),
					Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
				},
			),
			RequestURL: "http://localhost:30800/v1/completions",
		},
		BackoffDurationSeconds: 2,
	}
	totalReqCount := 0
	for _, value := range flow.RequestChannels() {
		totalReqCount += len(value.Channel)
	}

	if totalReqCount > 0 {
		t.Errorf("Expected no messages in request channels yet")
		return
	}
	if len(flow.ResultChannel()) > 0 {
		t.Errorf("Expected no messages in result channel yet")
		return
	}
	time.Sleep(3 * time.Second)

	pools := map[string]pipeline.WorkerPoolConfig{
		"default": {
			ID:      "default",
			Workers: 1,
		},
	}
	dispatch := ap.NewRandomRobinPolicy().MergeRequestChannels(flow.RequestChannels(), pools)
	mergedChannel := dispatch.Channels["default"]

	select {
	case req := <-mergedChannel:
		if req.PublicRequest == nil || req.PublicRequest.ReqID() != "test-id" {
			t.Errorf("Expected message id to be test-id, got %v", req.PublicRequest)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Expected message in request channel after backoff")
	}

}

func TestRedisImplWithAuth(t *testing.T) {
	s := miniredis.RunT(t)
	s.RequireAuth("test-password")
	redisURL := fmt.Sprintf("redis://default:test-password@%s:%s", s.Host(), s.Port())

	ctx := context.Background()

	flowOpts := redis.SortedSetFlowOptions{
		IGWBaseURL:       "http://localhost:30800",
		RequestPathURL:   "/v1/completions",
		RequestQueueName: "request-sortedset",
		ResultQueueName:  "result-list",
		PollIntervalMs:   1000,
		BatchSize:        10,
		GateParamsJSON:   "{}",
	}
	connOpts := redis.ConnectionOptions{URL: redisURL}
	flow, err := redis.NewRedisSortedSetFlow(flowOpts, connOpts, redis.WithSortedSetWorkerPools([]pipeline.WorkerPoolConfig{
		{
			ID:      "default",
			Workers: 1,
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	flow.Start(ctx)

	flow.ResultChannel() <- api.ResultMessage{
		ID: "test-auth-id",
	}

	time.Sleep(1 * time.Second)

	s.CheckList(t, "result-list", `{"id":"test-auth-id","payload":""}`)
}
