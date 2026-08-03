package asyncworker

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	flowcontrol "github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
)

// TestAIMDGate_WorkerScenario exercises the full path a capacity rejection
// takes through the real worker loop: the HTTP client parses the drop reason
// and Retry-After, the worker classifies the outcome and reports it to the
// pool gate, the gate closes its window, the next message parks, and the
// worker resumes dispatch once the window reopens.
func TestAIMDGate_WorkerScenario(t *testing.T) {
	const retryAfterSeconds = 1

	var calls atomic.Int32
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		h := make(http.Header)
		if calls.Add(1) == 1 {
			h.Set("Retry-After", "1")
			h.Set(asyncapi.DroppedReasonHeader, asyncapi.DroppedReasonSaturated)
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     h,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Header:     h,
		}, nil
	})

	gate := flowcontrol.NewAIMDGate(flowcontrol.AIMDConfig{
		MinWindow:    1,
		MaxWindow:    4,
		Increase:     1.0,
		Decrease:     0.5,
		HoldDuration: 100 * time.Millisecond,
	})

	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 2)
	retryChannel := make(chan pipeline.RetryMessage, 2)
	resultChannel := make(chan asyncapi.ResultMessage, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go WorkerWithGate(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel,
		retryChannel, resultChannel, defaultRequestTimeout, nil, gate)

	deadline := time.Now().Add(5 * time.Minute).Unix()
	newMsg := func(id string) pipeline.EmbelishedRequestMessage {
		msg := newEmb(asyncapi.RequestMessage{
			ID:       id,
			Created:  time.Now().Unix(),
			Deadline: deadline,
			Payload:  map[string]any{"model": "test", "prompt": "hi"},
		}, "http://gateway/v1/completions", nil)
		// Reserved traffic parks on a closed window (overflow yields back to
		// the broker instead), which is the path this scenario exercises.
		msg.SetClassification(asyncapi.ClassificationReserved)
		return msg
	}

	// Message 1 hits the saturated gateway: it must be re-enqueued for
	// retry, and its feedback closes the gate for Retry-After.
	requestChannel <- newMsg("m1")
	select {
	case r := <-retryChannel:
		if r.PublicRequest.ReqID() != "m1" {
			t.Fatalf("retry for %q, want m1", r.PublicRequest.ReqID())
		}
	case res := <-resultChannel:
		t.Fatalf("unexpected result before retry: %+v", res)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for m1 retry")
	}
	closedAt := time.Now()

	// Message 2 must park on the closed window and dispatch only after the
	// Retry-After elapses, then succeed against the recovered gateway.
	requestChannel <- newMsg("m2")
	select {
	case res := <-resultChannel:
		elapsed := time.Since(closedAt)
		if res.ID != "m2" || res.StatusCode != http.StatusOK {
			t.Fatalf("result = %+v, want m2 with 200", res)
		}
		if elapsed < 700*time.Millisecond {
			t.Errorf("m2 dispatched after %v; want it parked for ~%ds of Retry-After", elapsed, retryAfterSeconds)
		}
	case r := <-retryChannel:
		t.Fatalf("unexpected retry for %q; want m2 to park and then succeed", r.PublicRequest.ReqID())
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for m2 result")
	}
}
