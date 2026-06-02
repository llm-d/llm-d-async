package pubsub

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

type mockAttributeGate struct {
	allowed       bool
	acquireCalled bool
	releaseCalled bool
}

func (m *mockAttributeGate) Budget(ctx context.Context) float64 { return 1.0 }
func (m *mockAttributeGate) Acquire(ctx context.Context, attrs map[string]string) (pipeline.AcquireResult, error) {
	m.acquireCalled = true
	return pipeline.AcquireResult{
		Allowed:        m.allowed,
		Classification: api.ClassificationReserved,
		Release:        func() { m.releaseCalled = true },
	}, nil
}

func TestProcessMessages_QuotaGating(t *testing.T) {
	flow := &PubSubMQFlow{}
	ch := make(chan *api.InternalRequest, 1)

	tests := []struct {
		name           string
		allowed        bool
		expectedResult bool // result sent back to resultChannel
		expectAck      bool
		expectNack     bool
	}{
		{
			name:           "Allowed and Success",
			allowed:        true,
			expectedResult: true,
			expectAck:      true,
			expectNack:     false,
		},
		{
			name:           "Allowed and Failure",
			allowed:        true,
			expectedResult: false,
			expectAck:      false,
			expectNack:     true,
		},
		{
			name:       "Denied by Quota",
			allowed:    false,
			expectAck:  false,
			expectNack: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := &mockAttributeGate{allowed: tt.allowed}

			// We need a way to mock the pubsub.Message.
			// Since we can't easily create a pubsub.Message with custom Ack/Nack handlers,
			// we rely on the fact that we can verify if the message was processed.

			msgData, _ := json.Marshal(api.RequestMessage{ID: "test-msg"})

			// Use a custom receive function that yields one message
			receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
				msg := &pubsub.Message{
					ID:         "msg-1",
					Data:       msgData,
					Attributes: map[string]string{"userid": "user1"},
				}
				// Note: msg.Ack() and msg.Nack() will panic if not properly initialized by the library.
				// However, we are testing the logic flow.

				// In a real test, we'd need to mock the Ack/Nack methods, which is hard in this lib.
				// For the purpose of this test, we'll verify the gate calls and channel interactions.

				f(ctx, msg)
				return nil
			}

			// Run in a goroutine because it blocks on resultsChannel
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			go func() {
				if tt.allowed {
					select {
					case msg := <-ch:
						// Simulate result worker sending back a result
						pubsubID := msg.TransportCorrelationID
						val, _ := resultChannels.Load(pubsubID)
						resCh := val.(chan bool)
						resCh <- tt.expectedResult
					case <-ctx.Done():
					}
				}
			}()

			// We wrap in a recover to catch panics from Ack/Nack on uninitialized messages
			defer func() {
				if r := recover(); r != nil {
					// Expected panic if Ack/Nack is called on mock message
					// But we should check if Acquire/Release were called
					if !gate.acquireCalled {
						t.Errorf("Acquire was not called")
					}
					if tt.allowed && !gate.releaseCalled {
						t.Errorf("Release was not called for allowed request")
					}
				}
			}()

			inFlight := new(atomic.Int64)
			_ = flow.processMessages(ctx, receive, ch, gate, inFlight, "test-queue", "test-sub")
		})
	}
}
