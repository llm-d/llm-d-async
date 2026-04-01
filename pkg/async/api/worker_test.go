package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

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

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel)
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

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel)

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

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel)

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

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel)
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

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel)
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
