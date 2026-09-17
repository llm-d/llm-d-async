//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	randomrobin "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	"github.com/llm-d/llm-d-async/producer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPayloadRef_DispatchRetryAndCleanup(t *testing.T) {
	payload := `{"model":"test","prompt":"stored apart"}`
	var mu sync.Mutex
	var bodies []string
	inference := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		attempt := len(bodies)
		mu.Unlock()
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer inference.Close()

	flow, rdb, queue := newShutdownLossFlow(t, 1, inference.URL, 30, 100)
	submitter, err := producer.NewRedisSortedSetProducer(producer.RedisSortedSetConfig{
		RedisURL:         "redis://" + rdb.Options().Addr,
		RequestQueueName: queue,
		ResultQueueName:  "result-list",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = submitter.Close() })

	require.NoError(t, submitter.SubmitRequest(context.Background(), &api.RequestMessage{
		ID:       "pointer",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(time.Minute).Unix(),
		Payload:  json.RawMessage(payload),
	}))
	members, err := rdb.ZRange(context.Background(), queue, 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.NotContains(t, members[0], "stored apart")
	var queued api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(members[0]), &queued))
	require.NotEmpty(t, queued.PayloadRef)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	flowCtx, flowCancel := context.WithCancel(context.Background())
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flow.RequestChannels(), pools)
	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		asyncworker.WorkerWithGate(workerCtx, workerCtx, pipeline.Characteristics{},
			asyncworker.NewHTTPInferenceClient(inference.Client()), dispatch.Channels["default"],
			flow.RetryChannel(), flow.ResultChannel(), time.Minute, nil, nil)
	}()
	flow.Start(flowCtx)
	t.Cleanup(func() {
		flow.StopConsuming()
		workerCancel()
		flowCancel()
		workerWG.Wait()
		flow.Shutdown()
	})

	receiveCtx, receiveCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer receiveCancel()
	delivery, err := submitter.ReceiveResult(receiveCtx)
	require.NoError(t, err)
	assert.Equal(t, "pointer", delivery.Result.ID)
	assert.Equal(t, http.StatusOK, delivery.Result.StatusCode)
	require.NoError(t, submitter.AckResult(context.Background(), delivery))

	mu.Lock()
	require.Len(t, bodies, 2, "one failed attempt and one retry")
	for i, body := range bodies {
		assert.JSONEq(t, payload, body, "attempt %d sent the stored payload", i+1)
	}
	mu.Unlock()

	exists, err := rdb.Exists(context.Background(), queued.PayloadRef).Result()
	require.NoError(t, err)
	assert.Zero(t, exists, "the payload key is deleted once the result is recorded")
}
