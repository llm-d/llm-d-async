package asyncworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"testing"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

const defaultRequestTimeout = 5 * time.Minute

// newEmb wraps a RequestMessage in a minimal InternalRequest for tests.
func newEmb(rm asyncapi.RequestMessage, requestURL string, h map[string]string) pipeline.EmbelishedRequestMessage {
	if h == nil {
		h = map[string]string{}
	}
	return pipeline.EmbelishedRequestMessage{
		InternalRequest: asyncapi.NewInternalRequest(asyncapi.InternalRouting{}, &rm),
		HttpHeaders:     h,
		RequestURL:      requestURL,
	}
}

// newEmbR uses explicit internal routing (e.g. retry count) for tests.
func newEmbR(routing asyncapi.InternalRouting, rm asyncapi.RequestMessage, requestURL string, h map[string]string) pipeline.EmbelishedRequestMessage {
	if h == nil {
		h = map[string]string{}
	}
	return pipeline.EmbelishedRequestMessage{
		InternalRequest: asyncapi.NewInternalRequest(routing, &rm),
		HttpHeaders:     h,
		RequestURL:      requestURL,
	}
}

func TestRetryMessage_deadlinePassed(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "123",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(-10 * time.Second).Unix(),
	}, "", map[string]string{})
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0)
	if len(retryChannel) > 0 {
		t.Errorf("Message that its deadline passed should not be retried. Got a message in the retry channel")
		return
	}
	if len(resultChannel) != 1 {
		t.Errorf("Expected one message in the result channel")
		return

	}
	result := <-resultChannel
	var resultMap map[string]any
	json.Unmarshal([]byte(result.Payload), &resultMap) // nolint:errcheck
	if resultMap["error"] != "deadline exceeded" {
		t.Errorf("Expected error to be: 'deadline exceeded', got: %s", resultMap["error"])
	}

}

func TestRetryMessage_retry(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "123",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(10 * time.Second).Unix(),
	}, "", map[string]string{})
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0)
	if len(resultChannel) > 0 {
		t.Errorf("Should not have any messages in the result channel")
		return
	}
	if len(retryChannel) != 1 {
		t.Errorf("Expected one message in the retry channel")
		return
	}
	retryMsg := <-retryChannel
	if retryMsg.RetryCount != 1 {
		t.Errorf("Expected retry count to be 1, got %d", retryMsg.RetryCount)
	}

}

// RoundTripFunc is a type that implements http.RoundTripper
type RoundTripFunc func(req *http.Request) (*http.Response, error)

// RoundTrip executes a single HTTP transaction, obtaining the Response for a given Request.
func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// NewTestClient returns an *http.Client with its Transport replaced by a custom RoundTripper.
func NewTestClient(fn RoundTripFunc) *http.Client {
	return &http.Client{
		Transport: RoundTripFunc(fn),
	}
}

func TestSheddedRequest(t *testing.T) {
	msgId := "123"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case r := <-retryChannel:
		if r.PublicRequest == nil || r.PublicRequest.ReqID() != msgId {
			t.Errorf("Expected retry message id to be %s, got %v", msgId, r.PublicRequest)
		}
	case <-resultChannel:
		t.Errorf("Should not get result from a 5xx response")

	}

}
func TestSuccessfulRequest(t *testing.T) {
	msgId := "123"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Errorf("Should not get a retry from a 200 response")
	case r := <-resultChannel:
		if r.ID != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.ID)
		}
	}

}

func TestFatalError_NoRetry(t *testing.T) {
	msgId := "456"
	// Simulate a transport error (fatal)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("network unreachable")
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Errorf("Should not retry a fatal error")
	case r := <-resultChannel:
		if r.ID != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.ID)
		}
		var resultMap map[string]any
		err := json.Unmarshal([]byte(r.Payload), &resultMap)
		if err != nil {
			t.Errorf("Failed to unmarshal result payload: %s. Payload was: %s", err, r.Payload)
		}
		if _, hasError := resultMap["error"]; !hasError {
			t.Errorf("Expected error in result payload, got: %s", r.Payload)
		}
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for result")
	}
}

func TestRateLimitRequest(t *testing.T) {
	msgId := "789"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case r := <-retryChannel:
		if r.PublicRequest == nil || r.PublicRequest.ReqID() != msgId {
			t.Errorf("Expected retry message id to be %s, got %v", msgId, r.PublicRequest)
		}
	case <-resultChannel:
		t.Errorf("Should not get result from a 429 response, should retry")
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for retry")
	}
}

func TestRequestTimeout(t *testing.T) {
	msgId := "timeout-test"
	// Simulate a slow server that blocks longer than the request timeout.
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	// Use a very short request timeout to trigger the deadline.
	go Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, 100*time.Millisecond)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case r := <-resultChannel:
		// The request should fail due to context deadline exceeded (fatal unknown error).
		if r.ID != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.ID)
		}
	case <-retryChannel:
		// Context cancellation errors are wrapped as ErrCategoryUnknown (fatal), so no retry.
		t.Errorf("Timed-out request should not be retried")
	case <-time.After(5 * time.Second):
		t.Errorf("Worker did not return within 5s — per-request timeout was not enforced")
	}
}

func TestExpBackoffDuration(t *testing.T) {
	const iterations = 1000

	t.Run("normal backoff grows exponentially", func(t *testing.T) {
		deadline := 300
		for retry := 0; retry < 5; retry++ {
			expectedTemp := math.Min(float64(maxDelaySeconds), float64(baseDelaySeconds)*math.Pow(2, float64(retry)))
			lo := expectedTemp / 2
			hi := expectedTemp

			for i := 0; i < iterations; i++ {
				got := expBackoffDuration(retry, deadline)
				if got < lo || got >= hi {
					t.Errorf("retry=%d: got %f, want [%f, %f)", retry, got, lo, hi)
				}
			}
		}
	})

	t.Run("capped by maxDelaySeconds", func(t *testing.T) {
		deadline := 300
		// retry=10 → baseDelay*2^10 = 2048, far above maxDelaySeconds=60
		for i := 0; i < iterations; i++ {
			got := expBackoffDuration(10, deadline)
			if got < float64(maxDelaySeconds)/2 || got >= float64(maxDelaySeconds) {
				t.Errorf("got %f, want [%f, %f)", got, float64(maxDelaySeconds)/2, float64(maxDelaySeconds))
			}
		}
	})

	t.Run("capped by secondsToDeadline", func(t *testing.T) {
		deadline := 3
		// retry=10 → exponential is huge, but capped to deadline=3
		for i := 0; i < iterations; i++ {
			got := expBackoffDuration(10, deadline)
			if got < float64(deadline)/2 || got >= float64(deadline) {
				t.Errorf("got %f, want [%f, %f)", got, float64(deadline)/2, float64(deadline))
			}
		}
	})

	t.Run("small deadline respected over baseDelay", func(t *testing.T) {
		// secondsToDeadline=1 → cap=1, temp=1, result in [0.5, 1.0)
		for i := 0; i < iterations; i++ {
			got := expBackoffDuration(1, 1)
			if got < 0.5 || got >= 1.0 {
				t.Errorf("got %f, want [0.5, 1.0)", got)
			}
		}
	})

	t.Run("zero deadline returns zero", func(t *testing.T) {
		got := expBackoffDuration(1, 0)
		if got != 0 {
			t.Errorf("got %f, want 0", got)
		}
	})

	t.Run("negative deadline returns zero", func(t *testing.T) {
		got := expBackoffDuration(1, -5)
		if got != 0 {
			t.Errorf("got %f, want 0", got)
		}
	})
}

func TestRetryMessage_deadlineExact(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	now := time.Now().Unix()
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "exact-deadline",
		Created:  now,
		Deadline: now, // same second → no time left to retry
	}, "", nil)
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0)
	if len(retryChannel) > 0 {
		t.Errorf("secondsToDeadline==0 should not produce a retry")
	}
	if len(resultChannel) != 1 {
		t.Errorf("expected deadline-exceeded result")
		return
	}
	result := <-resultChannel
	var resultMap map[string]any
	json.Unmarshal([]byte(result.Payload), &resultMap) // nolint:errcheck
	if resultMap["error"] != "deadline exceeded" {
		t.Errorf("expected 'deadline exceeded', got: %s", resultMap["error"])
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		wantOK   bool
		wantZero bool
	}{
		{name: "integer seconds", value: "120", wantOK: true},
		{name: "zero seconds", value: "0", wantOK: true, wantZero: true},
		{name: "HTTP-date future", value: time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat), wantOK: true},
		{name: "HTTP-date past", value: time.Now().Add(-10 * time.Second).UTC().Format(http.TimeFormat), wantOK: true, wantZero: true},
		{name: "negative integer", value: "-5", wantOK: false},
		{name: "invalid value", value: "abc", wantOK: false},
		{name: "empty string", value: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, ok := parseRetryAfter(tt.value)
			if ok != tt.wantOK {
				t.Fatalf("parseRetryAfter(%q): ok = %v, want %v", tt.value, ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if tt.wantZero && d != 0 {
				t.Errorf("expected zero duration, got %v", d)
			}
			if !tt.wantZero && d <= 0 {
				t.Errorf("expected positive duration, got %v", d)
			}
		})
	}
}

func TestRetryMessage_retryAfterHonored(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "retry-after-test",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
	}, "", nil)

	// Server says wait 30s; expBackoff for retry 1 would be ~[1,2) seconds,
	// so the Retry-After value should win.
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 30*time.Second)
	if len(retryChannel) != 1 {
		t.Fatalf("expected one message in retry channel, got %d", len(retryChannel))
	}
	retryMsg := <-retryChannel
	if retryMsg.BackoffDurationSeconds < 30 {
		t.Errorf("expected backoff >= 30s (Retry-After), got %f", retryMsg.BackoffDurationSeconds)
	}
}

func TestRetryMessage_retryAfterIgnoredWhenSmaller(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmbR(asyncapi.InternalRouting{RetryCount: 5}, asyncapi.RequestMessage{
		ID:       "retry-after-small",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
	}, "", nil)

	// Server says wait 1s, but expBackoff at retry 5 is much larger.
	// expBackoff should win.
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 1*time.Second)
	if len(retryChannel) != 1 {
		t.Fatalf("expected one message in retry channel, got %d", len(retryChannel))
	}
	retryMsg := <-retryChannel
	if retryMsg.BackoffDurationSeconds <= 1.0 {
		t.Errorf("expected backoff > 1s (expBackoff should dominate), got %f", retryMsg.BackoffDurationSeconds)
	}
}

func TestRetryMessage_retryAfterExceedsDeadline(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "retry-after-exceeds-deadline",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Second).Unix(),
	}, "", nil)

	// Server says wait 30s, but deadline is only 5s away → deadline exceeded.
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 30*time.Second)
	if len(retryChannel) > 0 {
		t.Errorf("should not retry when Retry-After exceeds deadline")
	}
	if len(resultChannel) != 1 {
		t.Fatalf("expected deadline-exceeded result, got %d messages", len(resultChannel))
	}
	result := <-resultChannel
	var resultMap map[string]any
	json.Unmarshal([]byte(result.Payload), &resultMap) // nolint:errcheck
	if resultMap["error"] != "deadline exceeded" {
		t.Errorf("expected 'deadline exceeded', got: %s", resultMap["error"])
	}
}

func TestRateLimitRequest_WithRetryAfterHeader(t *testing.T) {
	msgId := "429-with-header"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Retry-After", "25")
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       nil,
			Header:     header,
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case r := <-retryChannel:
		if r.PublicRequest == nil || r.PublicRequest.ReqID() != msgId {
			t.Errorf("expected retry message id %s, got %v", msgId, r.PublicRequest)
		}
		if r.BackoffDurationSeconds < 25 {
			t.Errorf("expected backoff >= 25s (Retry-After header), got %f", r.BackoffDurationSeconds)
		}
	case <-resultChannel:
		t.Errorf("should not get result from a 429 response, should retry")
	case <-time.After(time.Second):
		t.Errorf("timeout waiting for retry")
	}
}

func TestValidateAndMarshal_cancelledCtxDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Unbuffered channel: without the select guard this would block forever.
	resultChannel := make(chan asyncapi.ResultMessage)

	emb := newEmb(asyncapi.RequestMessage{
		ID:       "cancel-test",
		Created:  time.Now().Unix(),
		Deadline: 0, // invalid deadline → error path
	}, "", nil)

	done := make(chan struct{})
	go func() {
		validateAndMarshal(ctx, resultChannel, emb)
		close(done)
	}()

	select {
	case <-done:
		// Function returned without blocking — test passes.
	case <-time.After(2 * time.Second):
		t.Fatal("validateAndMarshal blocked on cancelled ctx with full/unbuffered channel")
	}
}

func TestRetryMessage_cancelledCtxDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Unbuffered channels: sends would block without the select guard.
	retryChannel := make(chan pipeline.RetryMessage)
	resultChannel := make(chan asyncapi.ResultMessage)

	msg := newEmb(asyncapi.RequestMessage{
		ID:       "cancel-retry-test",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(10 * time.Second).Unix(),
	}, "", nil)

	done := make(chan struct{})
	go func() {
		retryMessage(ctx, msg, retryChannel, resultChannel, 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retryMessage blocked on cancelled ctx with unbuffered channels")
	}
}

func TestWorker_cancelledCtxExitsPromptly(t *testing.T) {
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	// Unbuffered result channel: the worker must not block trying to send.
	retryChannel := make(chan pipeline.RetryMessage)
	resultChannel := make(chan asyncapi.ResultMessage)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
		close(done)
	}()

	deadline := time.Now().Add(100 * time.Second).Unix()
	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "worker-cancel-test",
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	// Give the worker a moment to pick up the message and attempt the send,
	// then cancel so it must exit via the ctx.Done() branch.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Worker goroutine did not exit after context cancellation")
	}
}

func TestClientError_NoRetry(t *testing.T) {
	msgId := "101112"
	errorBody := `{"error": "invalid request"}`
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(errorBody)),
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Errorf("Should not retry a 4xx client error")
	case r := <-resultChannel:
		if r.ID != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.ID)
		}
		expectedPayload := `{"error":"Failed to send request to inference: INVALID_REQ: client error: status code 400"}`
		if r.Payload != expectedPayload {
			t.Errorf("Expected payload to be %s, got %s", expectedPayload, r.Payload)
		}
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for result")
	}
}
