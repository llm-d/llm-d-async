//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker"
	"github.com/stretchr/testify/assert"
)

func TestTierPriorityGate_Integration(t *testing.T) {
	serverHitCount := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		serverHitCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer server.Close()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 5)
	retryChannel := make(chan pipeline.RetryMessage, 5)
	resultChannel := make(chan asyncapi.ResultMessage, 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var isSaturated bool
	var satMu sync.Mutex
	satGate := flowcontrol.DispatchGateFunc(func(ctx context.Context) float64 {
		satMu.Lock()
		defer satMu.Unlock()
		if isSaturated {
			return 0.0
		}
		return 1.0
	})

	gate := flowcontrol.NewTierPriorityAdmissionGate(satGate, "tier")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		asyncworker.WorkerWithGate(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
			client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil, gate)
	}()

	t.Run("Saturated - Interactive - Overflow => Drop with 429", func(t *testing.T) {
		satMu.Lock()
		isSaturated = true
		satMu.Unlock()

		ir := asyncapi.NewInternalRequest(
			asyncapi.InternalRouting{
				RequestQueueName: "test-queue",
				Labels: map[string]string{
					"tier": "interactive",
				},
			},
			&asyncapi.RequestMessage{
				ID:       "req-drop",
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(5 * time.Minute).Unix(),
				Payload:  map[string]any{"model": "test"},
			},
		)
		ir.SetClassification(asyncapi.ClassificationOverflow)

		requestChannel <- pipeline.EmbelishedRequestMessage{
			InternalRequest: ir,
			RequestURL:      server.URL + "/v1/completions",
		}

		select {
		case res := <-resultChannel:
			assert.Equal(t, "req-drop", res.ID)
			assert.Contains(t, res.Payload, `"code": 429`)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for drop result")
		}
	})

	t.Run("Saturated - Async - Overflow => Refuse (Retry)", func(t *testing.T) {
		satMu.Lock()
		isSaturated = true
		satMu.Unlock()

		ir := asyncapi.NewInternalRequest(
			asyncapi.InternalRouting{
				RequestQueueName: "test-queue",
				Labels: map[string]string{
					"tier": "async",
				},
			},
			&asyncapi.RequestMessage{
				ID:       "req-refuse",
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(5 * time.Minute).Unix(),
				Payload:  map[string]any{"model": "test"},
			},
		)
		ir.SetClassification(asyncapi.ClassificationOverflow)

		requestChannel <- pipeline.EmbelishedRequestMessage{
			InternalRequest: ir,
			RequestURL:      server.URL + "/v1/completions",
		}

		select {
		case retryMsg := <-retryChannel:
			assert.Equal(t, "req-refuse", retryMsg.PublicRequest.ReqID())
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for retry message")
		}
	})

	t.Run("Saturated - Batch - Overflow => Refuse (Retry)", func(t *testing.T) {
		satMu.Lock()
		isSaturated = true
		satMu.Unlock()

		ir := asyncapi.NewInternalRequest(
			asyncapi.InternalRouting{
				RequestQueueName: "test-queue",
				Labels: map[string]string{
					"tier": "batch",
				},
			},
			&asyncapi.RequestMessage{
				ID:       "req-refuse-batch",
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(5 * time.Minute).Unix(),
				Payload:  map[string]any{"model": "test"},
			},
		)
		ir.SetClassification(asyncapi.ClassificationOverflow)

		requestChannel <- pipeline.EmbelishedRequestMessage{
			InternalRequest: ir,
			RequestURL:      server.URL + "/v1/completions",
		}

		select {
		case retryMsg := <-retryChannel:
			assert.Equal(t, "req-refuse-batch", retryMsg.PublicRequest.ReqID())
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for retry message")
		}
	})

	cancel()
	wg.Wait()
}
