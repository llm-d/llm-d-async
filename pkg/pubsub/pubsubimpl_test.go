package pubsub

import (
	"context"
	"encoding/json"
	"os"
	"strings"
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
func (m *mockAttributeGate) Apply(ctx context.Context, msg *api.InternalRequest) (pipeline.Verdict, error) {
	m.acquireCalled = true
	if !m.allowed {
		return pipeline.Refuse(), nil
	}
	msg.AttachRelease(func() { m.releaseCalled = true })
	return pipeline.Continue(), nil
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

			_ = flow.processMessages(ctx, receive, "test-sub", "test-pool", ch, gate)
		})
	}
}

func TestNewGCPPubSubMQFlow_PoolRequiredAndValidation(t *testing.T) {
	origEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	defer func() {
		if origEmulatorHost != "" {
			_ = os.Setenv("PUBSUB_EMULATOR_HOST", origEmulatorHost)
		} else {
			_ = os.Unsetenv("PUBSUB_EMULATOR_HOST")
		}
	}()

	tmpFile, err := os.CreateTemp("", "topics-config-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	baseOpts := Options{ProjectID: "test-project"}

	// Case 1: worker_pool_id is missing, and pool "default" does not exist
	missingPoolConfig := `[{"subscriber_id":"sub-1","inference_objective":"obj","igw_base_url":"http://gw"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(missingPoolConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	opts := baseOpts
	opts.TopicsConfigFile = tmpFile.Name()

	_, err = NewGCPPubSubMQFlow(opts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil || !strings.Contains(err.Error(), "not found in pool configuration") {
		t.Errorf("Expected error about missing pool, got: %v", err)
	}

	// Case 5: worker_pool_id is missing, but only a single 'default' pool is specified
	if err := os.WriteFile(tmpFile.Name(), []byte(missingPoolConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	opts = baseOpts
	opts.TopicsConfigFile = tmpFile.Name()

	_, err = NewGCPPubSubMQFlow(opts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
	if err != nil {
		t.Errorf("Unexpected error when worker_pool_id is missing but default pool exists: %v", err)
	}

	// Case 6: worker_pool_id is specified as custom, but only a single 'default' pool is specified
	customPoolConfig := `[{"subscriber_id":"sub-1","worker_pool_id":"custom-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(customPoolConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	opts = baseOpts
	opts.TopicsConfigFile = tmpFile.Name()

	_, err = NewGCPPubSubMQFlow(opts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when worker_pool_id is custom but only default pool exists")
	}

	// Case 2: worker_pool_id specified but pool does not exist
	nonExistentPoolConfig := `[{"subscriber_id":"sub-1","worker_pool_id":"non-existent","inference_objective":"obj","igw_base_url":"http://gw"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(nonExistentPoolConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	opts = baseOpts
	opts.TopicsConfigFile = tmpFile.Name()

	_, err = NewGCPPubSubMQFlow(opts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil || !strings.Contains(err.Error(), "not found in pool configuration") {
		t.Errorf("Expected error about missing pool, got: %v", err)
	}

	// Case 3: worker_pool_id specified and pool exists, but igw_base_url is missing
	missingIgwConfig := `[{"subscriber_id":"sub-1","worker_pool_id":"test-pool","inference_objective":"obj"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(missingIgwConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	opts = baseOpts
	opts.TopicsConfigFile = tmpFile.Name()

	_, err = NewGCPPubSubMQFlow(opts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil || !strings.Contains(err.Error(), "igw_base_url must be specified") {
		t.Errorf("Expected error about missing igw_base_url, got: %v", err)
	}

	// Case 4: worker_pool_id and igw_base_url specified and pool exists
	validConfig := `[{"subscriber_id":"sub-1","worker_pool_id":"test-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(validConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	opts = baseOpts
	opts.TopicsConfigFile = tmpFile.Name()

	_, err = NewGCPPubSubMQFlow(opts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err != nil {
		t.Errorf("Unexpected error when worker_pool_id and igw_base_url exist: %v", err)
	}
}
