package api

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
)

const defaultRequestTimeout = 5 * time.Minute

func TestRetryMessage_deadlinePassed(t *testing.T) {
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	msg := EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              "123",
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf("%d", time.Now().Add(time.Second*-10).Unix()),
		},
		HttpHeaders: map[string]string{},
		RequestURL:  "",
	}
	retryMessage(msg, retryChannel, resultChannel)
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
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	msg := EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              "123",
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf("%d", time.Now().Add(time.Second*10).Unix()),
		},
		HttpHeaders: map[string]string{},
		RequestURL:  "",
	}
	retryMessage(msg, retryChannel, resultChannel)
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
		t.Errorf("Expected retry count to be 1, got %d", msg.RetryCount)
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
	requestChannel := make(chan EmbelishedRequestMessage, 1)
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              msgId,
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf(("%d"), deadline),
			Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
		},
		RequestURL:  "http://localhost:30800/v1/completions",
		HttpHeaders: map[string]string{},
	}

	select {
	case r := <-retryChannel:
		if r.Id != msgId {
			t.Errorf("Expected retry message id to be %s, got %s", msgId, r.Id)
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
	requestChannel := make(chan EmbelishedRequestMessage, 1)
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              msgId,
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf(("%d"), deadline),
			Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
		},
		RequestURL:  "http://localhost:30800/v1/completions",
		HttpHeaders: map[string]string{},
	}

	select {
	case <-retryChannel:
		t.Errorf("Should not get a retry from a 200 response")
	case r := <-resultChannel:
		if r.Id != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.Id)
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
	requestChannel := make(chan EmbelishedRequestMessage, 1)
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              msgId,
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf(("%d"), deadline),
			Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
		},
		RequestURL:  "http://localhost:30800/v1/completions",
		HttpHeaders: map[string]string{},
	}

	select {
	case <-retryChannel:
		t.Errorf("Should not retry a fatal error")
	case r := <-resultChannel:
		if r.Id != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.Id)
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
	requestChannel := make(chan EmbelishedRequestMessage, 1)
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              msgId,
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf(("%d"), deadline),
			Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
		},
		RequestURL:  "http://localhost:30800/v1/completions",
		HttpHeaders: map[string]string{},
	}

	select {
	case r := <-retryChannel:
		if r.Id != msgId {
			t.Errorf("Expected retry message id to be %s, got %s", msgId, r.Id)
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
	requestChannel := make(chan EmbelishedRequestMessage, 1)
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	ctx := context.Background()

	// Use a very short request timeout to trigger the deadline.
	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, 100*time.Millisecond)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              msgId,
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf("%d", deadline),
			Payload:         map[string]any{"model": "test", "prompt": "hi"},
		},
		RequestURL:  "http://localhost:30800/v1/completions",
		HttpHeaders: map[string]string{},
	}

	select {
	case r := <-resultChannel:
		// The request should fail due to context deadline exceeded (fatal unknown error).
		if r.Id != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.Id)
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
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	msg := EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              "exact-deadline",
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf("%d", time.Now().Unix()), // exactly now
		},
	}
	retryMessage(msg, retryChannel, resultChannel)
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
	requestChannel := make(chan EmbelishedRequestMessage, 1)
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              msgId,
			CreatedUnixSec:  fmt.Sprintf("%d", time.Now().Unix()),
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf(("%d"), deadline),
			Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
		},
		RequestURL:  "http://localhost:30800/v1/completions",
		HttpHeaders: map[string]string{},
	}

	select {
	case <-retryChannel:
		t.Errorf("Should not retry a 4xx client error")
	case r := <-resultChannel:
		if r.Id != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.Id)
		}
		expectedPayload := `{"error":"Failed to send request to inference: INVALID_REQ: client error: status code 400"}`
		if r.Payload != expectedPayload {
			t.Errorf("Expected payload to be %s, got %s", expectedPayload, r.Payload)
		}
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for result")
	}
}
