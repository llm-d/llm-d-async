package asyncworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
)

func TestSendRequest_success(t *testing.T) {
	body := `{"result":"ok"}`
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(body)),
			Header:     make(http.Header),
		}, nil
	}))

	got, sc, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != body {
		t.Errorf("body = %q, want %q", string(got), body)
	}
	if sc != http.StatusOK {
		t.Errorf("statusCode = %d, want %d", sc, http.StatusOK)
	}
}

func TestSendRequest_headersForwarded(t *testing.T) {
	var capturedReq *http.Request
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		capturedReq = req
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}, nil
	}))

	headers := map[string]string{
		"X-Custom":     "value1",
		"Content-Type": "application/json",
	}
	_, _, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", headers, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for k, v := range headers {
		if got := capturedReq.Header.Get(k); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
}

func TestSendRequest_rateLimitWithoutRetryAfter(t *testing.T) {
	respBody := `{"error":"rate limited"}`
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       io.NopCloser(bytes.NewBufferString(respBody)),
			Header:     make(http.Header),
		}, nil
	}))

	body, sc, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for 429 response")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryRateLimit {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryRateLimit)
	}
	if ce.StatusCode != http.StatusTooManyRequests {
		t.Errorf("StatusCode = %d, want %d", ce.StatusCode, http.StatusTooManyRequests)
	}
	if sc != http.StatusTooManyRequests {
		t.Errorf("returned statusCode = %d, want %d", sc, http.StatusTooManyRequests)
	}
	if ce.RetryAfter != 0 {
		t.Errorf("RetryAfter = %v, want 0 (no header)", ce.RetryAfter)
	}
	if string(body) != respBody {
		t.Errorf("body = %q, want %q", string(body), respBody)
	}
}

func TestSendRequest_rateLimitWithRetryAfter(t *testing.T) {
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		h := make(http.Header)
		h.Set("Retry-After", "30")
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     h,
		}, nil
	}))

	_, _, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for 429 response")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.RetryAfter != 30*time.Second {
		t.Errorf("RetryAfter = %v, want 30s", ce.RetryAfter)
	}
}

func TestSendRequest_clientError(t *testing.T) {
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(`{"error":"bad"}`)),
			Header:     make(http.Header),
		}, nil
	}))

	body, sc, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryInvalidReq {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryInvalidReq)
	}
	if ce.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want %d", ce.StatusCode, http.StatusBadRequest)
	}
	if sc != http.StatusBadRequest {
		t.Errorf("returned statusCode = %d, want %d", sc, http.StatusBadRequest)
	}
	if len(body) == 0 {
		t.Error("expected response body to be returned with 4xx error")
	}
}

func TestSendRequest_serverError(t *testing.T) {
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(bytes.NewBufferString(`{"error":"internal"}`)),
			Header:     make(http.Header),
		}, nil
	}))

	body, sc, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryServer {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryServer)
	}
	if ce.StatusCode != http.StatusInternalServerError {
		t.Errorf("StatusCode = %d, want %d", ce.StatusCode, http.StatusInternalServerError)
	}
	if sc != http.StatusInternalServerError {
		t.Errorf("returned statusCode = %d, want %d", sc, http.StatusInternalServerError)
	}
	if len(body) == 0 {
		t.Error("expected response body to be returned with 5xx error")
	}
}

func TestSendRequest_transportError(t *testing.T) {
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("connection refused")
	}))

	_, sc, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for transport failure")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryUnknown {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryUnknown)
	}
	if ce.StatusCode != 0 {
		t.Errorf("StatusCode = %d, want 0 for transport error", ce.StatusCode)
	}
	if sc != 0 {
		t.Errorf("returned statusCode = %d, want 0 for transport error", sc)
	}
}

func TestSendRequest_invalidURL(t *testing.T) {
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		t.Fatal("RoundTripper should not be called for an invalid URL")
		return nil, nil
	}))

	_, _, err := client.SendRequest(context.Background(), "://invalid", nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryInvalidReq {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryInvalidReq)
	}
}

func TestSendRequest_contextCancellation(t *testing.T) {
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := client.SendRequest(ctx, "http://localhost/v1/completions", nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryUnknown {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryUnknown)
	}
}

func TestSendRequest_bodyReadFailurePreservesStatusCode(t *testing.T) {
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(&failReader{}),
			Header:     make(http.Header),
		}, nil
	}))

	_, sc, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for body read failure")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryServer {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryServer)
	}
	if sc != http.StatusOK {
		t.Errorf("returned statusCode = %d, want %d (response was received)", sc, http.StatusOK)
	}
	if ce.StatusCode != http.StatusOK {
		t.Errorf("ClientError.StatusCode = %d, want %d", ce.StatusCode, http.StatusOK)
	}
}

type failReader struct{}

func (f *failReader) Read([]byte) (int, error) {
	return 0, fmt.Errorf("simulated read error")
}

func TestNewHTTPInferenceClient(t *testing.T) {
	httpClient := &http.Client{}
	client := NewHTTPInferenceClient(httpClient)
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.client != httpClient {
		t.Error("expected client to wrap the provided http.Client")
	}
}
