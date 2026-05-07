package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	redisgate "github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"github.com/redis/go-redis/v9"
)

func TestGating_EndToEnd(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	flow := &PubSubMQFlow{}
	ch := make(chan *api.InternalRequest, 10)

	// Helper to create a message that records its outcome
	createMsg := func(id string, userid string) *pubsub.Message {
		data, _ := json.Marshal(api.RequestMessage{ID: id})
		msg := &pubsub.Message{
			ID:         id,
			Data:       data,
			Attributes: map[string]string{"userid": userid},
		}
		// In a real e2e, we'd hook into Ack/Nack.
		// Since we're using the library's Message struct, we can't easily override them.
		// However, processMessages calls msg.Ack() and msg.Nack().
		// To verify this without panicking, we would need to mock the full Pub/Sub client,
		// but for this "extensive" test, we'll verify the gate logic and the fact that
		// messages reach the channel 'ch' only when allowed.
		return msg
	}

	t.Run("Concurrency Gating", func(t *testing.T) {
		gate := redisgate.NewRedisQuotaGate(rdb, "userid", redisgate.QuotaModeConcurrency, 1, 10*time.Second, "e2e-concurrency:")

		// 1. Send Request 1 for User A
		msg1 := createMsg("req-1", "user-a")

		// Run processMessages in a way we can control
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Mocking receive to deliver msg1
			receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
				f(ctx, msg1)
				return nil
			}
			_ = flow.processMessages(ctx, receive, ch, gate)
		}()

		// Verification: Request 1 should reach the channel
		var req1 *api.InternalRequest
		select {
		case req1 = <-ch:
			if req1.PublicRequest.ReqID() != "req-1" {
				t.Fatalf("Expected req-1, got %s", req1.PublicRequest.ReqID())
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for req-1")
		}

		// 2. Send Request 2 for User A (while Request 1 is still "in flight" in the gate)
		msg2 := createMsg("req-2", "user-a")
		deniedChan := make(chan bool, 1)
		go func() {
			receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
				// This should return immediately because Acquire fails
				f(ctx, msg2)
				return nil
			}
			// We expect this to return without putting anything in 'ch'
			_ = flow.processMessages(ctx, receive, ch, gate)
			deniedChan <- true
		}()

		select {
		case <-ch:
			t.Fatal("Request 2 should have been gated and not reached the channel")
		case <-deniedChan:
			// Success: processMessages returned (due to Nack) without sending to 'ch'
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for req-2 to be denied")
		}

		// 3. Complete Request 1
		pubsubID := req1.TransportCorrelationID
		val, _ := resultChannels.Load(pubsubID)
		resCh := val.(chan bool)
		resCh <- true // Success
		wg.Wait()     // Wait for processMessages to finish and release quota

		// 4. Send Request 3 for User A (should now be allowed)
		msg3 := createMsg("req-3", "user-a")
		go func() {
			receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
				f(ctx, msg3)
				return nil
			}
			_ = flow.processMessages(ctx, receive, ch, gate)
		}()

		select {
		case req3 := <-ch:
			if req3.PublicRequest.ReqID() != "req-3" {
				t.Fatalf("Expected req-3, got %s", req3.PublicRequest.ReqID())
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for req-3")
		}
	})

	t.Run("Rate Limit Gating", func(t *testing.T) {
		// 2 requests per 2 seconds
		gate := redisgate.NewRedisQuotaGate(rdb, "userid", redisgate.QuotaModeRateLimit, 2, 2*time.Second, "e2e-ratelimit:")

		// Send 2 allowed requests
		for i := 1; i <= 2; i++ {
			id := fmt.Sprintf("rl-%d", i)
			msg := createMsg(id, "user-b")
			go func() {
				receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
					f(ctx, msg)
					return nil
				}
				_ = flow.processMessages(context.Background(), receive, ch, gate)
			}()

			select {
			case req := <-ch:
				// Finish it immediately
				pubsubID := req.TransportCorrelationID
				val, _ := resultChannels.Load(pubsubID)
				resCh := val.(chan bool)
				resCh <- true
			case <-time.After(1 * time.Second):
				t.Fatalf("Timeout waiting for %s", id)
			}
		}

		// 3rd request should be denied
		msg3 := createMsg("rl-3", "user-b")
		deniedChan := make(chan bool, 1)
		go func() {
			receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
				f(ctx, msg3)
				return nil
			}
			_ = flow.processMessages(context.Background(), receive, ch, gate)
			deniedChan <- true
		}()

		select {
		case <-ch:
			t.Fatal("rl-3 should have been rate limited")
		case <-deniedChan:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for rl-3 denial")
		}

		// Wait for window to reset
		time.Sleep(2100 * time.Millisecond)

		// 4th request should be allowed
		msg4 := createMsg("rl-4", "user-b")
		go func() {
			receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
				f(ctx, msg4)
				return nil
			}
			_ = flow.processMessages(context.Background(), receive, ch, gate)
		}()

		select {
		case <-ch:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for rl-4 after reset")
		}
	})
}
