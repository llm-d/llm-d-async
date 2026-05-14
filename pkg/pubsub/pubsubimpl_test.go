package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func TestPubSubMQFlow_Pools_EmptyByDefault(t *testing.T) {
	flow := &PubSubMQFlow{}
	pools := flow.Pools()
	if len(pools) != 0 {
		t.Errorf("expected empty pools, got %d", len(pools))
	}
}

func TestPubSubMQFlow_Pools_ReturnsCopy(t *testing.T) {
	flow := &PubSubMQFlow{
		pools: []pipeline.Pool{
			{ID: "pool-a", GatewayURL: "http://a"},
			{ID: "pool-b", GatewayURL: "http://b"},
		},
	}
	pools := flow.Pools()
	if len(pools) != 2 {
		t.Fatalf("expected 2 pools, got %d", len(pools))
	}
	if pools[0].ID != "pool-a" || pools[1].ID != "pool-b" {
		t.Errorf("unexpected pools: %v", pools)
	}
}

func TestBytesTrimLeftSpace(t *testing.T) {
	tests := []struct {
		input []byte
		want  byte
	}{
		{[]byte("  ["), '['},
		{[]byte("\t{"), '{'},
		{[]byte("["), '['},
		{[]byte(""), 0},
	}
	for _, tt := range tests {
		got := bytesTrimLeftSpace(tt.input)
		if tt.want == 0 {
			if len(got) != 0 {
				t.Errorf("input %q: expected empty, got %q", tt.input, got)
			}
		} else {
			if len(got) == 0 || got[0] != tt.want {
				t.Errorf("input %q: expected first byte %q, got %q", tt.input, tt.want, got)
			}
		}
	}
}

func TestResultWorkerAcksOnlyAfterPublishSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const pubsubID = "pubsub-result-success"
	resultCh := make(chan api.ResultMessage, 1)
	ackCh := make(chan ackAction, 1)
	resultChannels.Store(pubsubID, resultTracker{resultCh: ackCh})
	defer resultChannels.Delete(pubsubID)

	published := make(chan struct{}, 1)
	go resultWorker(ctx, func(context.Context, []byte, map[string]string) error {
		published <- struct{}{}
		return nil
	}, resultCh)

	resultCh <- api.ResultMessage{
		ID:      "request-1",
		Payload: "{}",
		Routing: api.InternalRouting{TransportCorrelationID: pubsubID},
	}

	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("result was not published")
	}
	select {
	case action := <-ackCh:
		if !action.ack {
			t.Fatal("successful publish should ack")
		}
	case <-time.After(time.Second):
		t.Fatal("request was not acked")
	}
}

func TestResultWorkerNacksWhenPublishFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const pubsubID = "pubsub-result-failure"
	resultCh := make(chan api.ResultMessage, 1)
	ackCh := make(chan ackAction, 1)
	resultChannels.Store(pubsubID, resultTracker{resultCh: ackCh})
	defer resultChannels.Delete(pubsubID)

	go resultWorker(ctx, func(context.Context, []byte, map[string]string) error {
		return errors.New("publish failed")
	}, resultCh)

	resultCh <- api.ResultMessage{
		ID:      "request-1",
		Payload: "{}",
		Routing: api.InternalRouting{TransportCorrelationID: pubsubID},
	}

	select {
	case action := <-ackCh:
		if action.ack {
			t.Fatal("failed publish should nack")
		}
	case <-time.After(time.Second):
		t.Fatal("request was not nacked")
	}
}

func TestPubSubTransportDeadlineKeepsSafetyMargin(t *testing.T) {
	now := time.Unix(100, 0)
	deadline := pubSubTransportDeadline(now, 10*time.Minute)
	want := now.Add(9*time.Minute + 30*time.Second)
	if !deadline.Equal(want) {
		t.Fatalf("deadline = %s, want %s", deadline, want)
	}
}

func TestSubscriptionGateDropWithResultDoesNotAckDirectly(t *testing.T) {
	ctx := context.Background()
	resultCh := make(chan api.ResultMessage, 1)
	ackCh := make(chan ackAction, 1)
	stats := &progressStats{}
	flow := &PubSubMQFlow{resultChannel: resultCh}
	result := api.ResultMessage{ID: "request-1", Payload: `{"error":"pool saturated"}`}

	flow.handleSubscriptionGateTerminate(
		ctx,
		ctx,
		pipeline.Drop(&result),
		&pipeline.EmbelishedRequestMessage{},
		ackCh,
		stats,
	)

	select {
	case got := <-resultCh:
		if got.ID != result.ID {
			t.Fatalf("result ID = %q, want %q", got.ID, result.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("result was not forwarded to result worker")
	}

	select {
	case action := <-ackCh:
		t.Fatalf("subscription gate result path acked directly: %+v", action)
	default:
	}
	if got := stats.gateDenied.Load(); got != 1 {
		t.Fatalf("gateDenied = %d, want 1", got)
	}
}

func TestSubscriptionGateSilentDropAcksDirectly(t *testing.T) {
	ctx := context.Background()
	ackCh := make(chan ackAction, 1)
	flow := &PubSubMQFlow{resultChannel: make(chan api.ResultMessage, 1)}

	flow.handleSubscriptionGateTerminate(
		ctx,
		ctx,
		pipeline.Drop(nil),
		&pipeline.EmbelishedRequestMessage{},
		ackCh,
		&progressStats{},
	)

	select {
	case action := <-ackCh:
		if !action.ack {
			t.Fatalf("silent drop action = %+v, want ack", action)
		}
	case <-time.After(time.Second):
		t.Fatal("silent drop did not ack")
	}
}
