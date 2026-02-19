package integration_test

import (
	"context"
	"flag"
	"strconv"
	"testing"
	"time"

	ap "github.com/llm-d-incubation/llm-d-async/pkg/async"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
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
				DeadlineUnixSec: strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10),
				Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
				Metadata:        map[string]string{redis.QUEUE_NAME_KEY: "request-queue"},
			},
			RequestPathURL: "/v1/completions",
			HttpHeaders:    map[string]string{},
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
