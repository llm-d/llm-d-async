//go:build integration

package integration_test

import (
	"context"
	"errors"
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

func TestDurableResultDelivery_RedeliversAfterConsumerLoss(t *testing.T) {
	inference := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer inference.Close()

	flow, rdb, _ := newShutdownLossFlow(t, 1, inference.URL, 1, 100)
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

	producerConfig := producer.RedisSortedSetConfig{
		RedisURL:         "redis://" + rdb.Options().Addr,
		RequestQueueName: "request-sortedset",
		ResultQueueName:  "result-list",
	}
	producerOptions := []producer.ProducerOption{
		producer.WithResultClaimLeaseTTL(50 * time.Millisecond),
		producer.WithResultClaimReclaimInterval(5 * time.Millisecond),
	}
	first, err := producer.NewRedisSortedSetProducer(producerConfig, producerOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	second, err := producer.NewRedisSortedSetProducer(producerConfig, producerOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	require.NoError(t, first.SubmitRequest(context.Background(), &api.RequestMessage{
		ID:       "durable-result",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(time.Minute).Unix(),
		Payload:  testPayload(map[string]any{"model": "test", "prompt": "hello"}),
	}))

	receiveCtx, receiveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer receiveCancel()
	stale, err := first.ReceiveResult(receiveCtx)
	require.NoError(t, err)
	require.NotEmpty(t, stale.Result.Routing.RequestToken)

	// The first consumer disappears without ACK. A replacement claims the
	// same durable record after lease expiry.
	time.Sleep(75 * time.Millisecond)
	redelivered, err := second.ReceiveResult(receiveCtx)
	require.NoError(t, err)
	assert.Equal(t, stale.Result.ID, redelivered.Result.ID)
	assert.Equal(t, stale.Result.Routing.RequestToken, redelivered.Result.Routing.RequestToken)
	assert.ErrorIs(t, first.AckResult(context.Background(), stale), producer.ErrResultDeliveryOwnershipLost)

	checkpoint := map[string]bool{
		redelivered.Result.ID + "\x00" + redelivered.Result.Routing.RequestToken: true,
	}
	require.Len(t, checkpoint, 1)
	require.NoError(t, second.AckResult(context.Background(), redelivered))

	noResultCtx, noResultCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer noResultCancel()
	result, err := second.ReceiveResult(noResultCtx)
	assert.Nil(t, result)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "unexpected receive error: %v", err)
}
