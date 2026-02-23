package api

import (
	"context"
	"encoding/json"
	"fmt"
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
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf("%d", time.Now().Add(time.Second*-10).Unix()),
		},
		HttpHeaders:    map[string]string{},
		RequestPathURL: "",
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
	json.Unmarshal([]byte(result.Payload), &resultMap)
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
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf("%d", time.Now().Add(time.Second*10).Unix()),
		},
		HttpHeaders:    map[string]string{},
		RequestPathURL: "",
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
	requestChannel := make(chan EmbelishedRequestMessage, 1)
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, "http://localhost:30800", httpclient, requestChannel, retryChannel, resultChannel)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              msgId,
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf(("%d"), deadline),
			Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
		},
		RequestPathURL: "/v1/completions",
		HttpHeaders:    map[string]string{},
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
	requestChannel := make(chan EmbelishedRequestMessage, 1)
	retryChannel := make(chan RetryMessage, 1)
	resultChannel := make(chan ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, Characteristics{HasExternalBackoff: false}, "http://localhost:30800", httpclient, requestChannel, retryChannel, resultChannel)

	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- EmbelishedRequestMessage{
		RequestMessage: RequestMessage{
			Id:              msgId,
			RetryCount:      0,
			DeadlineUnixSec: fmt.Sprintf(("%d"), deadline),
			Payload:         map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
		},
		RequestPathURL: "/v1/completions",
		HttpHeaders:    map[string]string{},
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
