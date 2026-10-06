//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	randomrobin "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"
	goredis "github.com/redis/go-redis/v9"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/redis"
)

func TestRedisImpl(t *testing.T) {
	s := miniredis.RunT(t)
	redisURL := fmt.Sprintf("redis://%s:%s", s.Host(), s.Port())

	ctx := context.Background()

	cfg := redis.PubSubConfig{
		URL:             redisURL,
		RetryQueueName:  "retry-sortedset",
		ResultQueueName: "result-queue",
		Queues: []redis.QueueConfig{{
			QueueName:      "request-queue",
			WorkerPoolID:   "default",
			RequestPathURL: "/v1/completions",
			IGWBaseURL:     "http://localhost:30800",
		}},
	}
	cfg.ApplyDefaults()
	flow, err := redis.NewRedisMQFlow(cfg, []pipeline.WorkerPoolConfig{
		{
			ID:      "default",
			Workers: 1,
		},
	})
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
					Payload:  testPayload(map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0}),
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
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).MergeRequestChannels(flow.RequestChannels(), pools)
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

	cfg := redis.SortedSetConfig{
		URL:             redisURL,
		ResultQueueName: "result-list",
		PollIntervalMs:  50,
		BatchSize:       10,
		Queues: []redis.SortedSetQueueConfig{{
			QueueName:      "request-sortedset",
			WorkerPoolID:   "default",
			RequestPathURL: "/v1/completions",
			IGWBaseURL:     "http://localhost:30800",
		}},
	}
	cfg.ApplyDefaults()
	flow, err := redis.NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{
		{
			ID:      "default",
			Workers: 1,
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	pools := map[string]pipeline.WorkerPoolConfig{
		"default": {ID: "default", Workers: 1},
	}
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flow.RequestChannels(), pools)
	mergedChannel := dispatch.Channels["default"]

	flow.Start(ctx)
	defer func() {
		flow.StopConsuming()
		flow.Shutdown()
	}()

	rdb := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:%s", s.Host(), s.Port()),
		Password: "test-password",
	})
	defer rdb.Close() // nolint:errcheck

	ir := api.NewInternalRequest(
		api.InternalRouting{RequestQueueName: "request-sortedset"},
		&api.RequestMessage{
			ID:       "test-auth-id",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  testPayload(map[string]any{"model": "test"}),
		},
	)
	member, err := ir.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.ZAdd(ctx, "request-sortedset", goredis.Z{
		Score:  float64(time.Now().Add(5 * time.Minute).Unix()),
		Member: string(member),
	}).Err(); err != nil {
		t.Fatal(err)
	}

	select {
	case req := <-mergedChannel:
		if req.PublicRequest == nil || req.PublicRequest.ReqID() != "test-auth-id" {
			t.Fatalf("Expected message id to be test-auth-id, got %v", req.PublicRequest)
		}
		flow.ResultChannel() <- api.ResultMessage{
			ID:      req.PublicRequest.ReqID(),
			Payload: "",
			Routing: req.InternalRouting,
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request to be claimed and dispatched")
	}

	time.Sleep(200 * time.Millisecond)

	s.CheckList(t, "result-list", `{"id":"test-auth-id","payload":""}`)
}
