package producer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestProducer(t *testing.T) (*RedisSortedSetProducer, *miniredis.Miniredis) {
	t.Helper()

	// Start mini Redis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(func() { mr.Close() })

	producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisAddr:        mr.Addr(),
		TenantID:         "test-tenant",
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
	assert.Equal(t, "results:test-tenant:test-result-queue", ir.ResultQueueName)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := producer.SubmitRequest(ctx, tt.req)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestGetResult(t *testing.T) {
	producer, mr := setupTestProducer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Push a result to the namespaced list
	resultMsg := api.ResultMessage{
		ID:      "test-123",
		Payload: `{"response": "Hello!"}`,
	}
	resultJSON, _ := json.Marshal(resultMsg)
	_, err := mr.Lpush("results:test-tenant:test-result-queue", string(resultJSON))
	require.NoError(t, err)

	// Get the result
	result, err := producer.GetResult(ctx)
	require.NoError(t, err)
	assert.Equal(t, "test-123", result.ID)
	assert.Contains(t, result.Payload, "Hello!")
}

func TestGetResultWithTimeout(t *testing.T) {
	producer, mr := setupTestProducer(t)

	ctx := context.Background()

	t.Run("timeout with no result", func(t *testing.T) {
		result, err := producer.GetResultWithTimeout(ctx, 100*time.Millisecond)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("get result before timeout", func(t *testing.T) {
		// Push a result to namespaced queue
		resultMsg := api.ResultMessage{
			ID:      "test-456",
			Payload: "test response",
		}
		resultJSON, _ := json.Marshal(resultMsg)
		_, err := mr.Lpush("results:test-tenant:test-result-queue", string(resultJSON))
		require.NoError(t, err)

		result, err := producer.GetResultWithTimeout(ctx, 1*time.Second)
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

	// Create producers for two different tenants
	tenant1Producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisAddr:        mr.Addr(),
		TenantID:         "tenant-alice",
		RequestQueueName: "shared-request-queue",
		ResultQueueName:  "my-results", // Same name but different tenant
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := tenant1Producer.Close(); err != nil {
			t.Logf("failed to close tenant1Producer: %v", err)
		}
	})

	tenant2Producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisAddr:        mr.Addr(),
		TenantID:         "tenant-bob",
		RequestQueueName: "shared-request-queue",
		ResultQueueName:  "my-results", // Same name but different tenant
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := tenant2Producer.Close(); err != nil {
			t.Logf("failed to close tenant2Producer: %v", err)
		}
	})

	// Verify they have different namespaced result queues
	assert.Equal(t, "results:tenant-alice:my-results", tenant1Producer.resultQueueName)
	assert.Equal(t, "results:tenant-bob:my-results", tenant2Producer.resultQueueName)
	assert.NotEqual(t, tenant1Producer.resultQueueName, tenant2Producer.resultQueueName,
		"Different tenants should have different result queues even with same ResultQueueName")

	// Submit requests from both tenants
	req1 := &api.RequestMessage{
		ID:       "alice-request",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(1 * time.Hour).Unix(),
		Payload:  map[string]interface{}{"tenant": "alice"},
	}
	err = tenant1Producer.SubmitRequest(ctx, req1)
	require.NoError(t, err)

	req2 := &api.RequestMessage{
		ID:       "bob-request",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(1 * time.Hour).Unix(),
		Payload:  map[string]interface{}{"tenant": "bob"},
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

	assert.Equal(t, "results:tenant-alice:my-results", ir1.ResultQueueName)
	assert.Equal(t, "results:tenant-bob:my-results", ir2.ResultQueueName)

	// Simulate worker routing results to correct tenant queues
	result1 := api.ResultMessage{
		ID:      "alice-request",
		Payload: `{"response": "alice result"}`,
	}
	result1JSON, _ := json.Marshal(result1)
	_, err = mr.Lpush("results:tenant-alice:my-results", string(result1JSON))
	require.NoError(t, err)

	result2 := api.ResultMessage{
		ID:      "bob-request",
		Payload: `{"response": "bob result"}`,
	}
	result2JSON, _ := json.Marshal(result2)
	_, err = mr.Lpush("results:tenant-bob:my-results", string(result2JSON))
	require.NoError(t, err)

	// Each tenant should only receive their own result
	res1, err := tenant1Producer.GetResultWithTimeout(ctx, 1*time.Second)
	require.NoError(t, err)
	require.NotNil(t, res1)
	assert.Equal(t, "alice-request", res1.ID)
	assert.Contains(t, res1.Payload, "alice result")

	res2, err := tenant2Producer.GetResultWithTimeout(ctx, 1*time.Second)
	require.NoError(t, err)
	require.NotNil(t, res2)
	assert.Equal(t, "bob-request", res2.ID)
	assert.Contains(t, res2.Payload, "bob result")
}

func TestProducerAuth(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	mr.RequireAuth("producer-secret")

	t.Run("fails without password", func(t *testing.T) {
		_, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
			RedisAddr: mr.Addr(),
			TenantID:  "test",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NOAUTH")
	})

	t.Run("succeeds with password", func(t *testing.T) {
		producer, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
			RedisAddr:     mr.Addr(),
			RedisPassword: "producer-secret",
			TenantID:      "test",
		})
		assert.NoError(t, err)
		defer producer.Close() // nolint:errcheck

		ctx := context.Background()
		err = producer.client.Ping(ctx).Err()
		assert.NoError(t, err)
	})
}

func TestTenantIDRequired(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Should fail without TenantID
	_, err = NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisAddr:        mr.Addr(),
		RequestQueueName: "test",
		ResultQueueName:  "test",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TenantID is required")
}

func TestContextCancellation(t *testing.T) {
	producer, _ := setupTestProducer(t)

	// Test context cancellation with timeout-based retrieval
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	result, err := producer.GetResultWithTimeout(ctx, 5*time.Second)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMalformedResultHandling(t *testing.T) {
	producer, mr := setupTestProducer(t)

	ctx := context.Background()

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := mr.Lpush("results:test-tenant:test-result-queue", "invalid-json{{{")
		require.NoError(t, err)

		result, err := producer.GetResultWithTimeout(ctx, 1*time.Second)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("missing id field", func(t *testing.T) {
		invalidResult := map[string]interface{}{"payload": "data"}
		resultJSON, _ := json.Marshal(invalidResult)
		_, err := mr.Lpush("results:test-tenant:test-result-queue", string(resultJSON))
		require.NoError(t, err)

		result, err := producer.GetResultWithTimeout(ctx, 1*time.Second)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "missing 'id' field")
	})
}

func TestSameTenantMultipleQueues(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Same tenant creates two producers with different result queues
	prod1, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisAddr:       mr.Addr(),
		TenantID:        "alice",
		ResultQueueName: "batch-jobs",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := prod1.Close(); err != nil {
			t.Logf("failed to close prod1: %v", err)
		}
	})

	prod2, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisAddr:       mr.Addr(),
		TenantID:        "alice",
		ResultQueueName: "realtime",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := prod2.Close(); err != nil {
			t.Logf("failed to close prod2: %v", err)
		}
	})

	// Verify different namespaced queues
	assert.Equal(t, "results:alice:batch-jobs", prod1.resultQueueName)
	assert.Equal(t, "results:alice:realtime", prod2.resultQueueName)
	assert.NotEqual(t, prod1.resultQueueName, prod2.resultQueueName)
}
