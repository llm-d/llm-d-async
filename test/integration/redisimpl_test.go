package integration_test

import (
	"context"
	"flag"
	"strconv"
	"testing"
	"time"

	ap "github.com/llm-d-incubation/llm-d-async/pkg/async"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
)

func TestRedisImpl(t *testing.T) {
	s := miniredis.RunT(t)
	rAddr := s.Host() + ":" + s.Port()

	ctx := context.Background()
	err := flag.Set("redis.addr", rAddr)
	if err != nil {
		t.Fatal(err)
	}

	flow := redis.NewRedisMQFlow()
	flow.Start(ctx)

	flow.RetryChannel() <- api.RetryMessage{
		EmbelishedRequestMessage: api.EmbelishedRequestMessage{
			RequestMessage: api.RequestMessage{
				Id:              "test-id",
				CreatedUnixSec:  strconv.FormatInt(time.Now().Unix(), 10),
				DeadlineUnixSec: strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10),
				Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
				Metadata:        map[string]string{redis.QUEUE_NAME_KEY: "request-queue"},
			},
			RequestURL:  "http://localhost:30800/v1/completions",
			HttpHeaders: map[string]string{},
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

	mergedChannel := ap.NewRandomRobinPolicy().MergeRequestChannels(flow.RequestChannels())

	select {
	case req := <-mergedChannel.Channel:
		if req.Id != "test-id" {
			t.Errorf("Expected message id to be test-id, got %s", req.Id)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Expected message in request channel after backoff")
	}

}

func TestRedisImplWithAuth(t *testing.T) {
	s := miniredis.RunT(t)
	s.RequireAuth("test-password")
	rAddr := s.Host() + ":" + s.Port()

	ctx := context.Background()
	_ = flag.Set("redis.addr", rAddr)
	_ = flag.Set("redis.password", "test-password")

	flow := redis.NewRedisSortedSetFlow()
	flow.Start(ctx)

	// Publish a result message
	flow.ResultChannel() <- api.ResultMessage{
		Id: "test-auth-id",
	}

	// Wait for processing
	time.Sleep(1 * time.Second)

	// Verify it was published to Redis successfully
	// By default, the result queue is "result-list" for SortedSetFlow
	s.CheckList(t, "result-list", `{"id":"test-auth-id","payload":""}`)
}
