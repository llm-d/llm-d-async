package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/redis/go-redis/v9"
)

func TestPubsubResultWorker_BatchPublish(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "result-pubsub-queue"
	resultCh := make(chan api.ResultMessage, resultChannelBuffer)

	// Subscribe so published messages are captured.
	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	// Pre-fill the channel with multiple results before starting the worker
	// so they are all available for a single batch drain.
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		resultCh <- api.ResultMessage{
			Id:      "msg-" + string(rune('A'+i)),
			Payload: "payload-" + string(rune('A'+i)),
		}
	}

	go resultWorker(ctx, rdb, resultCh, queue)

	received := make(map[string]bool)
	timeout := time.After(2 * time.Second)
	for len(received) < numMessages {
		select {
		case msg := <-pubsubCh:
			var rm api.ResultMessage
			if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}
			received[rm.Id] = true
		case <-timeout:
			t.Fatalf("Timeout: received only %d/%d messages", len(received), numMessages)
		}
	}

	for i := 0; i < numMessages; i++ {
		id := "msg-" + string(rune('A'+i))
		if !received[id] {
			t.Errorf("Missing message %s", id)
		}
	}
}

func TestPubsubResultWorker_SingleMessage(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "result-single-queue"
	resultCh := make(chan api.ResultMessage, resultChannelBuffer)

	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	go resultWorker(ctx, rdb, resultCh, queue)

	// Send a single message — should be flushed immediately as a batch of 1.
	resultCh <- api.ResultMessage{Id: "solo", Payload: "data"}

	select {
	case msg := <-pubsubCh:
		var rm api.ResultMessage
		if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}
		if rm.Id != "solo" {
			t.Errorf("Expected id 'solo', got %s", rm.Id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for single message")
	}
}

func TestMarshalResultMessage_Fallback(t *testing.T) {
	// A normal message should marshal fine.
	msg := api.ResultMessage{Id: "ok", Payload: "data"}
	result := marshalResultMessage(msg)

	var rm api.ResultMessage
	if err := json.Unmarshal([]byte(result), &rm); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}
	if rm.Id != "ok" {
		t.Errorf("Expected id 'ok', got %s", rm.Id)
	}
}
