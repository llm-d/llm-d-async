package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
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

func TestPubsubResultWorker_ContextCancellation(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan api.ResultMessage, resultChannelBuffer)

	done := make(chan bool)
	go func() {
		resultWorker(ctx, rdb, resultCh, "cancel-queue")
		done <- true
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Worker stopped gracefully
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Worker did not stop after context cancellation")
	}
}

func TestPubsubResultWorker_BatchSizeCap(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "batch-cap-queue"
	resultCh := make(chan api.ResultMessage, resultChannelBuffer)

	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	// Send more than maxResultBatchSize messages. The worker should still
	// deliver all of them across multiple pipeline flushes.
	totalMessages := maxResultBatchSize + 10
	for i := 0; i < totalMessages; i++ {
		resultCh <- api.ResultMessage{
			Id:      "cap-" + strconv.Itoa(i),
			Payload: "data",
		}
	}

	go resultWorker(ctx, rdb, resultCh, queue)

	received := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for len(received) < totalMessages {
		select {
		case msg := <-pubsubCh:
			var rm api.ResultMessage
			if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}
			received[rm.Id] = true
		case <-timeout:
			t.Fatalf("Timeout: received only %d/%d messages", len(received), totalMessages)
		}
	}
}

func TestPubsubResultWorker_ConcurrentProducers(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "concurrent-queue"
	resultCh := make(chan api.ResultMessage, resultChannelBuffer)

	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	go resultWorker(ctx, rdb, resultCh, queue)

	// Simulate multiple inference workers sending results concurrently.
	numProducers := 8
	msgsPerProducer := 5
	totalMessages := numProducers * msgsPerProducer

	var wg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < msgsPerProducer; i++ {
				resultCh <- api.ResultMessage{
					Id:      "p" + strconv.Itoa(producerID) + "-" + strconv.Itoa(i),
					Payload: "data",
				}
			}
		}(p)
	}
	wg.Wait()

	received := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for len(received) < totalMessages {
		select {
		case msg := <-pubsubCh:
			var rm api.ResultMessage
			if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}
			if received[rm.Id] {
				t.Errorf("Duplicate message: %s", rm.Id)
			}
			received[rm.Id] = true
		case <-timeout:
			t.Fatalf("Timeout: received only %d/%d messages", len(received), totalMessages)
		}
	}
}
