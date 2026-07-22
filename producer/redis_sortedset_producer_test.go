package producer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// customRequest is a user-defined Request implementation for testing the
// default branch of toInternalRequest.
type customRequest struct {
	id, endpoint      string
	created, deadline int64
	payload           map[string]any
	metadata          map[string]string
	headers           map[string]string
}

func (r *customRequest) ReqID() string                  { return r.id }
func (r *customRequest) ReqCreated() int64              { return r.created }
func (r *customRequest) ReqDeadline() int64             { return r.deadline }
func (r *customRequest) ReqPayload() map[string]any     { return r.payload }
func (r *customRequest) ReqMetadata() map[string]string { return r.metadata }
func (r *customRequest) ReqHeaders() map[string]string  { return r.headers }
func (r *customRequest) ReqEndpoint() string            { return r.endpoint }

func setupTestProducer(t *testing.T) (*RedisSortedSetProducer, *miniredis.Miniredis) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(func() { mr.Close() })

	producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:         "redis://" + mr.Addr(),
		RequestQueueName: "test-request-queue",
		ResultQueueName:  "test-result-queue",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Logf("failed to close producer: %v", err)
		}
	})

	return producer, mr
}

func TestSubmitRequest(t *testing.T) {
	producer, mr := setupTestProducer(t)

	ctx := context.Background()

	req := &api.RequestMessage{
		ID:       "test-123",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(1 * time.Hour).Unix(),
		Payload: map[string]interface{}{
			"model":  "gpt-3.5-turbo",
			"prompt": "Hello, world!",
		},
		Metadata: map[string]string{
			"user": "test-user",
		},
	}

	err := producer.SubmitRequest(ctx, req)
	assert.NoError(t, err)

	// Verify the message was added to the sorted set
	assert.True(t, mr.Exists("test-request-queue"))
	members, err := mr.ZMembers("test-request-queue")
	require.NoError(t, err)
	assert.Len(t, members, 1)

	var ir api.InternalRequest
	err = json.Unmarshal([]byte(members[0]), &ir)
	assert.NoError(t, err)
	assert.Equal(t, "test-123", ir.PublicRequest.ReqID())
	assert.Equal(t, "test-user", ir.PublicRequest.ReqMetadata()["user"])
	assert.Equal(t, "test-result-queue", ir.ResultQueueName)
}

func TestToInternalRequest_PubSubIDCopiesToInternalRouting(t *testing.T) {
	req := &api.PubSubRequest{
		RequestMessage: api.RequestMessage{
			ID: "x", Created: 1, Deadline: 2,
		},
		PubSubID: "ps-123",
	}
	ir := toInternalRequest(req)
	assert.Equal(t, "ps-123", ir.TransportCorrelationID)
}

func TestToInternalRequest_CustomRequestPreservesHeadersAndEndpoint(t *testing.T) {
	req := &customRequest{
		id: "custom-1", created: 1, deadline: 2,
		payload:  map[string]any{"k": "v"},
		metadata: map[string]string{"m": "d"},
		headers:  map[string]string{"Authorization": "Bearer tok"},
		endpoint: "/v1/chat/completions",
	}
	ir := toInternalRequest(req)
	rm, ok := ir.PublicRequest.(*api.RequestMessage)
	require.True(t, ok, "expected *api.RequestMessage for custom Request type")
	assert.Equal(t, "custom-1", rm.ID)
	assert.Equal(t, map[string]string{"Authorization": "Bearer tok"}, rm.Headers)
	assert.Equal(t, "/v1/chat/completions", rm.Endpoint)
	assert.Equal(t, map[string]string{"m": "d"}, rm.Metadata)
}

func TestToInternalRequest_RedisQueueFieldsCopyToInternalRouting(t *testing.T) {
	req := &api.RedisRequest{
		RequestMessage: api.RequestMessage{
			ID: "x", Created: 1, Deadline: 2,
		},
		RequestQueueName: "req-q",
		ResultQueueName:  "res-q",
	}
	ir := toInternalRequest(req)
	assert.Equal(t, "req-q", ir.RequestQueueName)
	assert.Equal(t, "res-q", ir.ResultQueueName)
}

func TestSubmitRequest_NilRequest(t *testing.T) {
	producer, _ := setupTestProducer(t)
	ctx := context.Background()

	t.Run("untyped nil", func(t *testing.T) {
		err := producer.SubmitRequest(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "request is required")
	})

	t.Run("typed nil RequestMessage", func(t *testing.T) {
		var req *api.RequestMessage
		err := producer.SubmitRequest(ctx, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "request is required")
	})

	t.Run("typed nil RedisRequest", func(t *testing.T) {
		var req *api.RedisRequest
		err := producer.SubmitRequest(ctx, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "request is required")
	})

	t.Run("typed nil PubSubRequest", func(t *testing.T) {
		var req *api.PubSubRequest
		err := producer.SubmitRequest(ctx, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "request is required")
	})
}

func TestSubmitRequest_Validation(t *testing.T) {
	producer, _ := setupTestProducer(t)

	ctx := context.Background()

	tests := []struct {
		name    string
		req     *api.RequestMessage
		wantErr string
	}{
		{
			name: "missing ID",
			req: &api.RequestMessage{
				Created:  time.Now().Unix(),
				Deadline: time.Now().Unix(),
				Payload:  map[string]interface{}{},
			},
			wantErr: "request ID is required",
		},
		{
			name: "missing deadline",
			req: &api.RequestMessage{
				ID:       "test",
				Created:  time.Now().Unix(),
				Deadline: 0,
				Payload:  map[string]interface{}{},
			},
			wantErr: "deadline is required",
		},
		{
			name: "invalid deadline",
			req: &api.RequestMessage{
				ID:       "test",
				Created:  time.Now().Unix(),
				Deadline: 0,
				Payload:  map[string]interface{}{},
			},
			wantErr: "deadline is required",
		},
		{
			name: "expired deadline",
			req: &api.RequestMessage{
				ID:       "test",
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(-1 * time.Minute).Unix(),
				Payload:  map[string]interface{}{},
			},
			wantErr: "deadline has already expired",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := producer.SubmitRequest(ctx, tt.req)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestCancelRequests(t *testing.T) {
	producer, mr := setupTestProducer(t)

	ctx := context.Background()
	mr.Set(api.RequestActiveTokenKey("req-1"), "token-1")
	mr.Set(api.RequestActiveTokenKey("req-2"), "token-2")
	err := producer.CancelRequests(ctx, []string{"req-1", "", "req-2"})
	require.NoError(t, err)

	got1, err := mr.Get(api.RequestCancellationKey("req-1"))
	require.NoError(t, err)
	assert.Equal(t, "token-1", got1)
	got2, err := mr.Get(api.RequestCancellationKey("req-2"))
	require.NoError(t, err)
	assert.Equal(t, "token-2", got2)
	assert.False(t, mr.Exists(api.RequestCancellationKey("")))
}

func TestCancelRequests_UnknownRequestIDIsNoOp(t *testing.T) {
	producer, mr := setupTestProducer(t)

	ctx := context.Background()
	err := producer.CancelRequests(ctx, []string{"unknown-request"})
	require.NoError(t, err)

	assert.False(t, mr.Exists(api.RequestActiveTokenKey("unknown-request")))
	assert.False(t, mr.Exists(api.RequestCancellationKey("unknown-request")))
}

func TestSubmitRequest_ClearsStaleCancellationMarker(t *testing.T) {
	producer, mr := setupTestProducer(t)

	ctx := context.Background()
	requestID := "reused-id"
	mr.Set(api.RequestCancellationKey(requestID), "1")

	err := producer.SubmitRequest(ctx, &api.RequestMessage{
		ID:       requestID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(1 * time.Hour).Unix(),
		Payload:  map[string]any{"model": "test"},
	})
	require.NoError(t, err)
	assert.False(t, mr.Exists(api.RequestCancellationKey(requestID)))
	assert.True(t, mr.Exists(api.RequestActiveTokenKey(requestID)))
}

func TestGetResult(t *testing.T) {
	producer, mr := setupTestProducer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Push a result to the result list
	resultMsg := api.ResultMessage{
		ID:      "test-123",
		Payload: `{"response": "Hello!"}`,
	}
	resultJSON, _ := json.Marshal(resultMsg)
	_, err := mr.Lpush("test-result-queue", string(resultJSON))
	require.NoError(t, err)

	// Get the result
	result, err := producer.GetResult(ctx)
	require.NoError(t, err)
	assert.Equal(t, "test-123", result.ID)
	assert.Contains(t, result.Payload, "Hello!")
}

func TestGetResultWithContextTimeout(t *testing.T) {
	producer, mr := setupTestProducer(t)

	t.Run("get result before timeout", func(t *testing.T) {
		resultMsg := api.ResultMessage{
			ID:      "test-456",
			Payload: "test response",
		}
		resultJSON, _ := json.Marshal(resultMsg)
		_, err := mr.Lpush("test-result-queue", string(resultJSON))
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		result, err := producer.GetResult(ctx)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test-456", result.ID)
	})
}

func TestMultipleTenantsIsolation(t *testing.T) {
	// Start mini Redis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ctx := context.Background()

	alphaResultQueue := "results:tenant-alpha:my-results"
	betaResultQueue := "results:tenant-beta:my-results"

	tenant1Producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:         "redis://" + mr.Addr(),
		RequestQueueName: "shared-request-queue",
		ResultQueueName:  alphaResultQueue,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := tenant1Producer.Close(); err != nil {
			t.Logf("failed to close tenant1Producer: %v", err)
		}
	})

	tenant2Producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:         "redis://" + mr.Addr(),
		RequestQueueName: "shared-request-queue",
		ResultQueueName:  betaResultQueue,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := tenant2Producer.Close(); err != nil {
			t.Logf("failed to close tenant2Producer: %v", err)
		}
	})

	assert.Equal(t, alphaResultQueue, tenant1Producer.resultQueueName)
	assert.Equal(t, betaResultQueue, tenant2Producer.resultQueueName)
	assert.NotEqual(t, tenant1Producer.resultQueueName, tenant2Producer.resultQueueName)

	// Submit requests from both tenants
	req1 := &api.RequestMessage{
		ID:       "alpha-request",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(1 * time.Hour).Unix(),
		Payload:  map[string]interface{}{"tenant": "alpha"},
	}
	err = tenant1Producer.SubmitRequest(ctx, req1)
	require.NoError(t, err)

	req2 := &api.RequestMessage{
		ID:       "beta-request",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(1 * time.Hour).Unix(),
		Payload:  map[string]interface{}{"tenant": "beta"},
	}
	err = tenant2Producer.SubmitRequest(ctx, req2)
	require.NoError(t, err)

	// Verify both requests have different result queue routing
	members, err := mr.ZMembers("shared-request-queue")
	require.NoError(t, err)
	assert.Len(t, members, 2)

	var ir1, ir2 api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(members[0]), &ir1))
	require.NoError(t, json.Unmarshal([]byte(members[1]), &ir2))

	// Both requests share one queue with equal deadline scores, so the ZMembers
	// order is not deterministic. Assert routing keyed by request ID rather than
	// by member position (which previously made this test flaky).
	routing := map[string]string{
		ir1.PublicRequest.ReqID(): ir1.ResultQueueName,
		ir2.PublicRequest.ReqID(): ir2.ResultQueueName,
	}
	assert.Equal(t, alphaResultQueue, routing["alpha-request"])
	assert.Equal(t, betaResultQueue, routing["beta-request"])

	// Simulate worker routing results to correct tenant queues
	result1 := api.ResultMessage{
		ID:      "alpha-request",
		Payload: `{"response": "alpha result"}`,
	}
	result1JSON, _ := json.Marshal(result1)
	_, err = mr.Lpush(alphaResultQueue, string(result1JSON))
	require.NoError(t, err)

	result2 := api.ResultMessage{
		ID:      "beta-request",
		Payload: `{"response": "beta result"}`,
	}
	result2JSON, _ := json.Marshal(result2)
	_, err = mr.Lpush(betaResultQueue, string(result2JSON))
	require.NoError(t, err)

	// Each tenant should only receive their own result
	ctxT, cancelT := context.WithTimeout(ctx, 2*time.Second)
	defer cancelT()

	res1, err := tenant1Producer.GetResult(ctxT)
	require.NoError(t, err)
	require.NotNil(t, res1)
	assert.Equal(t, "alpha-request", res1.ID)
	assert.Contains(t, res1.Payload, "alpha result")

	res2, err := tenant2Producer.GetResult(ctxT)
	require.NoError(t, err)
	require.NotNil(t, res2)
	assert.Equal(t, "beta-request", res2.ID)
	assert.Contains(t, res2.Payload, "beta result")
}

func TestProducerAuth(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	mr.RequireAuth("producer-secret")

	t.Run("fails without password", func(t *testing.T) {
		_, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
			RedisURL:        "redis://" + mr.Addr(),
			ResultQueueName: "test-result-queue",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NOAUTH")
	})

	t.Run("succeeds with password in URL", func(t *testing.T) {
		producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
			RedisURL:        "redis://default:producer-secret@" + mr.Addr(),
			ResultQueueName: "test-result-queue",
		})
		assert.NoError(t, err)
		defer producer.Close() //nolint:errcheck

		ctx := context.Background()
		req := &api.RequestMessage{
			ID:       "auth-test",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(1 * time.Hour).Unix(),
			Payload:  map[string]interface{}{"test": true},
		}
		err = producer.SubmitRequest(ctx, req)
		assert.NoError(t, err)
	})
}

func TestResultQueueNameRequired(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	_, err = NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:         "redis://" + mr.Addr(),
		RequestQueueName: "test",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ResultQueueName is required")
}

func TestContextCancellation(t *testing.T) {
	producer, mr := setupTestProducer(t)

	// Push a result so BRPop returns, but cancel context first to test cancellation.
	// With miniredis, BRPop(ctx,0) blocks indefinitely if no data and ctx isn't pre-cancelled,
	// so we pre-cancel the context to verify the error path.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := producer.GetResult(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)

	_ = mr // keep linter happy
}

func TestMalformedResultHandling(t *testing.T) {
	producer, mr := setupTestProducer(t)

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := mr.Lpush("test-result-queue", "invalid-json{{{")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		result, err := producer.GetResult(ctx)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("missing id field", func(t *testing.T) {
		invalidResult := map[string]interface{}{"payload": "data"}
		resultJSON, _ := json.Marshal(invalidResult)
		_, err := mr.Lpush("test-result-queue", string(resultJSON))
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		result, err := producer.GetResult(ctx)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "missing 'id' field")
	})
}

func TestWithRedisClient(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close() //nolint:errcheck

	t.Run("injected client is used, RedisURL not required", func(t *testing.T) {
		p, err := NewRedisSortedSetProducer(
			RedisSortedSetConfig{ResultQueueName: "test-result-queue"},
			WithRedisClient(client),
		)
		require.NoError(t, err)

		ctx := context.Background()
		req := &api.RequestMessage{
			ID:       "inject-test",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(1 * time.Hour).Unix(),
			Payload:  map[string]interface{}{"test": true},
		}
		err = p.SubmitRequest(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("fails with nil client", func(t *testing.T) {
		_, err := NewRedisSortedSetProducer(
			RedisSortedSetConfig{ResultQueueName: "test-result-queue"},
			WithRedisClient(nil),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client must not be nil")
	})

	t.Run("fails without RedisURL and without WithRedisClient", func(t *testing.T) {
		_, err := NewRedisSortedSetProducer(RedisSortedSetConfig{ResultQueueName: "test-result-queue"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RedisURL is required when no RedisClient is provided")
	})
}

func TestCloseOwnership(t *testing.T) {
	t.Run("managed client is closed", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		p, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
			RedisURL:        "redis://" + mr.Addr(),
			ResultQueueName: "test-result-queue",
		})
		require.NoError(t, err)

		err = p.Close()
		assert.NoError(t, err)

		// After Close, operations on the managed client should fail
		err = p.SubmitRequest(context.Background(), &api.RequestMessage{
			ID:       "post-close",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(1 * time.Hour).Unix(),
			Payload:  map[string]interface{}{},
		})
		assert.Error(t, err)
	})

	t.Run("injected client is not closed", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close() //nolint:errcheck

		p, err := NewRedisSortedSetProducer(
			RedisSortedSetConfig{ResultQueueName: "test-result-queue"},
			WithRedisClient(client),
		)
		require.NoError(t, err)

		err = p.Close()
		assert.NoError(t, err)

		// Injected client should still be usable after producer Close
		err = client.Ping(context.Background()).Err()
		assert.NoError(t, err)
	})
}

func TestMultipleResultQueues(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	batchQueue := "results:tenant-acme:batch-jobs"
	realtimeQueue := "results:tenant-acme:realtime"

	prod1, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:        "redis://" + mr.Addr(),
		ResultQueueName: batchQueue,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := prod1.Close(); err != nil {
			t.Logf("failed to close prod1: %v", err)
		}
	})

	prod2, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:        "redis://" + mr.Addr(),
		ResultQueueName: realtimeQueue,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := prod2.Close(); err != nil {
			t.Logf("failed to close prod2: %v", err)
		}
	})

	assert.Equal(t, batchQueue, prod1.resultQueueName)
	assert.Equal(t, realtimeQueue, prod2.resultQueueName)
	assert.NotEqual(t, prod1.resultQueueName, prod2.resultQueueName)
}

func TestResultQueueNameNoNamespacing(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	shortName := "batch-jobs"
	legacyNamespaced := "results:tenant-acme:batch-jobs"

	producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:        "redis://" + mr.Addr(),
		ResultQueueName: shortName,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Logf("failed to close producer: %v", err)
		}
	})

	assert.Equal(t, shortName, producer.resultQueueName)
	assert.NotEqual(t, legacyNamespaced, producer.resultQueueName,
		"producer must not prepend results:{tenant}: to ResultQueueName")

	ctx := context.Background()
	req := &api.RequestMessage{
		ID:       "short-name-request",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(1 * time.Hour).Unix(),
		Payload:  map[string]interface{}{"job": "batch"},
	}
	require.NoError(t, producer.SubmitRequest(ctx, req))

	members, err := mr.ZMembers("request-sortedset")
	require.NoError(t, err)
	require.Len(t, members, 1)

	var ir api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(members[0]), &ir))
	assert.Equal(t, shortName, ir.ResultQueueName)
	assert.NotEqual(t, legacyNamespaced, ir.ResultQueueName)

	resultJSON, _ := json.Marshal(api.ResultMessage{
		ID:      "short-name-request",
		Payload: `{"status":"done"}`,
	})
	_, err = mr.Lpush(shortName, string(resultJSON))
	require.NoError(t, err)

	ctxT, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result, err := producer.GetResult(ctxT)
	require.NoError(t, err)
	assert.Equal(t, "short-name-request", result.ID)
}

func TestComplexResultQueueKeyShape(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	complexKey := "llm-d-async:results:pool-a:$batch"

	producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:         "redis://" + mr.Addr(),
		RequestQueueName: "llm-d-async:requests:pool-a",
		ResultQueueName:  complexKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Logf("failed to close producer: %v", err)
		}
	})

	assert.Equal(t, complexKey, producer.resultQueueName)

	ctx := context.Background()
	req := &api.RequestMessage{
		ID:       "complex-key-request",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(1 * time.Hour).Unix(),
		Payload:  map[string]interface{}{"pool": "a"},
	}
	require.NoError(t, producer.SubmitRequest(ctx, req))

	members, err := mr.ZMembers("llm-d-async:requests:pool-a")
	require.NoError(t, err)
	require.Len(t, members, 1)

	var ir api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(members[0]), &ir))
	assert.Equal(t, complexKey, ir.ResultQueueName)

	resultJSON, _ := json.Marshal(api.ResultMessage{
		ID:      "complex-key-request",
		Payload: `{"response":"ok"}`,
	})
	_, err = mr.Lpush(complexKey, string(resultJSON))
	require.NoError(t, err)

	ctxT, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result, err := producer.GetResult(ctxT)
	require.NoError(t, err)
	assert.Equal(t, "complex-key-request", result.ID)
}
