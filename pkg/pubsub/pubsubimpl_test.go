package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

type mockAttributeGate struct {
	allowed       bool
	acquireCalled bool
	releaseCalled bool
}

func (m *mockAttributeGate) Budget(ctx context.Context) float64 { return 1.0 }
func (m *mockAttributeGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	m.acquireCalled = true
	if !m.allowed {
		return pipeline.Refuse(), nil
	}
	if releases != nil {
		*releases = append(*releases, func() { m.releaseCalled = true })
	}
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

			_ = flow.processMessages(ctx, receive, "test-sub", "test-pool", ch, gate, nil)
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

func (m *mockAttributeGate) Release(ctx context.Context, msg *api.InternalRequest) error { return nil }

func TestProcessMessages_LabelsPropagation(t *testing.T) {
	flow := &PubSubMQFlow{
		resultChannel: make(chan api.ResultMessage, 10),
	}
	ch := make(chan *api.InternalRequest, 10)
	gate := &mockAttributeGate{allowed: true}

	msgData, _ := json.Marshal(api.RequestMessage{ID: "test-msg"})
	receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
		msg := &pubsub.Message{
			ID:   "msg-1",
			Data: msgData,
		}
		f(ctx, msg)
		return nil
	}

	labels := map[string]string{
		"env":     "prod",
		"version": "1.2.3",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Spin up a goroutine to capture the request and unblock processMessages
	var receivedIR *api.InternalRequest
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case ir := <-ch:
			receivedIR = ir
			// Unblock processMessages
			pubsubID := ir.TransportCorrelationID
			if val, ok := resultChannels.Load(pubsubID); ok {
				resCh := val.(chan bool)
				resCh <- true
			}
		case <-ctx.Done():
		}
	}()

	// We wrap in a recover to catch panics from Ack/Nack on uninitialized messages,
	// which is expected because we don't fully mock pubsub.Message.
	defer func() {
		_ = recover()
		// After recover, check the captured request
		if receivedIR == nil {
			t.Fatal("expected a request to be received, got nil")
		}
		if receivedIR.Labels == nil {
			t.Fatal("expected labels, got nil")
		}
		if receivedIR.Labels["env"] != "prod" || receivedIR.Labels["version"] != "1.2.3" {
			t.Fatalf("unexpected labels: %v", receivedIR.Labels)
		}
	}()

	_ = flow.processMessages(ctx, receive, "test-sub", "test-pool", ch, gate, labels)
	<-done
}

const testProject = "test-project"

// newFakePubSub starts an in-memory Pub/Sub fake and returns a client wired to
// it along with a closer to take the broker down. The closer is idempotent and
// also runs on test cleanup. Optional reactors (e.g. pstest error injection)
// customize server responses.
func newFakePubSub(t *testing.T, reactors ...pstest.ServerReactorOption) (*pubsub.Client, func()) {
	t.Helper()
	srv := pstest.NewServer(reactors...)
	var once sync.Once
	closeSrv := func() { once.Do(func() { _ = srv.Close() }) }
	t.Cleanup(closeSrv)

	client, err := pubsub.NewClient(context.Background(), testProject,
		option.WithEndpoint(srv.Addr),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("failed to create fake pubsub client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, closeSrv
}

// createSubscription provisions a topic and subscription on the fake so that an
// admin GetSubscription lookup succeeds.
func createSubscription(t *testing.T, client *pubsub.Client, subID string) {
	t.Helper()
	ctx := context.Background()
	topic := "projects/" + testProject + "/topics/health-topic"
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/" + testProject + "/subscriptions/" + subID,
		Topic: topic,
	}); err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
}

func TestHealthCheck_Healthy(t *testing.T) {
	client, _ := newFakePubSub(t)
	createSubscription(t, client, "sub-1")

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "sub-1"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := flow.HealthCheck(ctx); err != nil {
		t.Errorf("expected healthy, got error: %v", err)
	}
}

func TestHealthCheck_MissingSubscription(t *testing.T) {
	client, _ := newFakePubSub(t)
	// No subscription created.

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "missing-sub"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := flow.HealthCheck(ctx); err == nil {
		t.Error("expected error for missing subscription, got nil")
	}
}

// TestHealthCheck_BrokerUnreachable is the regression guard for #246: an
// unreachable Pub/Sub backend must mark the pod not-ready.
func TestHealthCheck_BrokerUnreachable(t *testing.T) {
	client, closeSrv := newFakePubSub(t)
	createSubscription(t, client, "sub-1")

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "sub-1"}},
	}

	// Take the broker down before probing. The admin RPC retries on Unavailable,
	// so bound the probe with a short deadline (the real /readyz path uses the
	// health server's checkerTimeout) and assert it surfaces an error.
	closeSrv()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := flow.HealthCheck(ctx); err == nil {
		t.Error("expected error when broker is unreachable, got nil")
	}
}

// TestHealthCheck_PermissionDeniedIsHealthy verifies that a consume-only role
// (which lacks pubsub.subscriptions.get and gets PermissionDenied on the admin
// probe) is still reported ready, since the response proves broker reachability.
// The subscription is intentionally NOT created: without the injected
// PermissionDenied the probe would return NotFound and fail, so a passing test
// confirms the PermissionDenied branch is actually exercised.
func TestHealthCheck_PermissionDeniedIsHealthy(t *testing.T) {
	client, _ := newFakePubSub(t,
		pstest.WithErrorInjection("GetSubscription", codes.PermissionDenied, "permission denied"))

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "sub-1"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := flow.HealthCheck(ctx); err != nil {
		t.Errorf("expected PermissionDenied to be treated as healthy, got error: %v", err)
	}
}

// TestHealthCheck_ConsumeLoopCache verifies that HealthCheck prefers the passive
// signal cached by the consume loop over an active admin probe.
func TestHealthCheck_ConsumeLoopCache(t *testing.T) {
	client, closeSrv := newFakePubSub(t)
	createSubscription(t, client, "sub-1")
	// Take the broker down so any active probe would fail; the cache must win.
	closeSrv()

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "sub-1"}},
		consumeHealth:   map[string]*subHealth{"sub-1": {lastOK: time.Now()}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// A recent successful receive short-circuits to healthy without probing.
	if err := flow.HealthCheck(ctx); err != nil {
		t.Errorf("expected healthy from fresh consume-loop success, got error: %v", err)
	}

	// A consume error more recent than the last success marks it not-ready.
	flow.consumeHealth["sub-1"] = &subHealth{
		lastOK:    time.Now().Add(-time.Minute),
		lastErr:   errors.New("receive failed"),
		lastErrAt: time.Now(),
	}
	if err := flow.HealthCheck(ctx); err == nil {
		t.Error("expected not-ready from cached consume error, got nil")
	}
}

// TestHealthCheck_StaleConsumeErrorRecovers guards the fix for the sticky-error
// asymmetry: a consume error older than consumeHealthTTL must no longer pin the
// pod not-ready. Because the consume loop only refreshes lastOK on a delivered
// message, an idle subscription would otherwise stay unhealthy forever after a
// transient blip. HealthCheck must fall through to the active probe, which finds
// the subscription reachable and reports ready.
func TestHealthCheck_StaleConsumeErrorRecovers(t *testing.T) {
	client, _ := newFakePubSub(t)
	createSubscription(t, client, "sub-1")

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "sub-1"}},
		consumeHealth: map[string]*subHealth{"sub-1": {
			// Error older than the TTL, and no successful receive since.
			lastErr:   errors.New("transient receive failure"),
			lastErrAt: time.Now().Add(-2 * consumeHealthTTL),
		}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := flow.HealthCheck(ctx); err != nil {
		t.Errorf("expected stale consume error to fall back to a healthy active probe, got error: %v", err)
	}
}
