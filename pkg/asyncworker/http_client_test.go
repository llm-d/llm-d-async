package asyncworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
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

	resp, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(resp.Body) != body {
		t.Errorf("body = %q, want %q", string(resp.Body), body)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("statusCode = %d, want %d", resp.StatusCode, http.StatusOK)
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
	_, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", headers, []byte(`{}`))
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

	resp, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
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
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("returned statusCode = %d, want %d", resp.StatusCode, http.StatusTooManyRequests)
	}
	if ce.RetryAfter != 0 {
		t.Errorf("RetryAfter = %v, want 0 (no header)", ce.RetryAfter)
	}
	if string(resp.Body) != respBody {
		t.Errorf("body = %q, want %q", string(resp.Body), respBody)
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

	_, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
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

	resp, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
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
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("returned statusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if len(resp.Body) == 0 {
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

	resp, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
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
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("returned statusCode = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
	if len(resp.Body) == 0 {
		t.Error("expected response body to be returned with 5xx error")
	}
}

func closedServerURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	return url
}

func droppedConnectionURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("response writer is not a hijacker")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func hangingServerURL(t *testing.T) string {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv.URL
}

func untrustedTLSServerURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestSendRequest_transportErrorCategory(t *testing.T) {
	tests := []struct {
		name   string
		url    func(t *testing.T) string
		client *http.Client
		want   asyncapi.ErrorCategory
	}{
		{name: "connection refused", url: closedServerURL, client: &http.Client{}, want: asyncapi.ErrCategoryServer},
		{name: "connection closed before response", url: droppedConnectionURL, client: &http.Client{}, want: asyncapi.ErrCategoryServer},
		{name: "client timeout", url: hangingServerURL, client: &http.Client{Timeout: 50 * time.Millisecond}, want: asyncapi.ErrCategoryServer},
		{name: "unsupported scheme", url: func(*testing.T) string { return "ftp://127.0.0.1/v1/completions" }, client: &http.Client{}, want: asyncapi.ErrCategoryUnknown},
		{name: "missing host", url: func(*testing.T) string { return "http:///v1/completions" }, client: &http.Client{}, want: asyncapi.ErrCategoryUnknown},
		{name: "untrusted TLS certificate", url: untrustedTLSServerURL, client: &http.Client{}, want: asyncapi.ErrCategoryUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewHTTPInferenceClient(tt.client)

			resp, err := client.SendRequest(context.Background(), tt.url(t), nil, []byte(`{}`))
			if err == nil {
				t.Fatal("expected error for transport failure")
			}
			var ce *asyncapi.ClientError
			if !errors.As(err, &ce) {
				t.Fatalf("expected *ClientError, got %T", err)
			}
			if ce.ErrorCategory != tt.want {
				t.Errorf("category = %s, want %s (err: %v)", ce.ErrorCategory, tt.want, err)
			}
			if ce.ErrorCategory.Fatal() != (tt.want == asyncapi.ErrCategoryUnknown) {
				t.Errorf("Fatal() = %t for category %s", ce.ErrorCategory.Fatal(), ce.ErrorCategory)
			}
			if ce.StatusCode != 0 {
				t.Errorf("StatusCode = %d, want 0 for transport error", ce.StatusCode)
			}
			if resp != nil {
				t.Errorf("resp = %+v, want nil for transport error", resp)
			}
		})
	}
}

func TestSendRequest_redirectPolicyRejection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(srv.Close)
	client := NewHTTPInferenceClient(&http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirects are not followed") },
	})

	_, err := client.SendRequest(context.Background(), srv.URL, nil, []byte(`{}`))
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T (%v)", err, err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryUnknown {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryUnknown)
	}
}

func TestSendRequest_contextDeadlineStaysRetryable(t *testing.T) {
	client := NewHTTPInferenceClient(&http.Client{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)

	_, err := client.SendRequest(ctx, hangingServerURL(t), nil, []byte(`{}`))
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T (%v)", err, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if ce.ErrorCategory != asyncapi.ErrCategoryServer {
		t.Errorf("category = %s, want %s", ce.ErrorCategory, asyncapi.ErrCategoryServer)
	}
}

func TestSendRequest_invalidURL(t *testing.T) {
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		t.Fatal("RoundTripper should not be called for an invalid URL")
		return nil, nil
	}))

	_, err := client.SendRequest(context.Background(), "://invalid", nil, []byte(`{}`))
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

	_, err := client.SendRequest(ctx, "http://localhost/v1/completions", nil, []byte(`{}`))
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

	resp, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
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
	if resp == nil {
		t.Fatal("expected non-nil response when HTTP response was received")
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("returned statusCode = %d, want %d (response was received)", resp.StatusCode, http.StatusOK)
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

type errBody struct{}

func (errBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (errBody) Close() error             { return nil }

func TestSendRequest_droppedReasonCaptured(t *testing.T) {
	// Status/reason pairings match what llm-d-router emits: rejections for
	// capacity or queue TTL and post-dispatch evictions come back as 429,
	// while an empty pool, a disconnected client, or a shutting-down flow
	// controller come back as 503. The plain 400 is defensive — the router
	// does not set the header there, but the capture must not depend on
	// status.
	tests := []struct {
		name          string
		statusCode    int
		droppedReason string
	}{
		{"saturated 429", http.StatusTooManyRequests, "rejected-saturated"},
		{"ttl expired 429", http.StatusTooManyRequests, "rejected-ttl-expired"},
		{"evicted 429", http.StatusTooManyRequests, "evicted-queue-pressure"},
		{"no endpoints 503", http.StatusServiceUnavailable, "rejected-no-endpoints"},
		{"context cancelled 503", http.StatusServiceUnavailable, "rejected-context-cancelled"},
		{"plain 400", http.StatusBadRequest, "some-reason"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := make(http.Header)
			h.Set(asyncapi.DroppedReasonHeader, tt.droppedReason)
			client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: tt.statusCode,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Header:     h,
				}, nil
			}))
			_, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
			var ce *asyncapi.ClientError
			if !errors.As(err, &ce) {
				t.Fatalf("expected *ClientError, got %T", err)
			}
			if ce.DroppedReason != tt.droppedReason {
				t.Errorf("DroppedReason = %q, want %q", ce.DroppedReason, tt.droppedReason)
			}
			if !strings.Contains(ce.Error(), tt.droppedReason) {
				t.Errorf("Error() = %q, want it to surface the dropped reason", ce.Error())
			}
		})
	}
}

func TestSendRequest_serverErrorRetryAfterCaptured(t *testing.T) {
	h := make(http.Header)
	h.Set("Retry-After", "7")
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     h,
		}, nil
	}))
	_, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	if ce.RetryAfter != 7*time.Second {
		t.Errorf("RetryAfter = %v, want 7s", ce.RetryAfter)
	}
}

func TestSendRequest_bodyReadErrorKeepsDroppedReasonAndRetryAfter(t *testing.T) {
	h := make(http.Header)
	h.Set(asyncapi.DroppedReasonHeader, "evicted-queue-pressure")
	h.Set("Retry-After", "5")
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       errBody{},
			Header:     h,
		}, nil
	}))
	_, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ClientError, got %T", err)
	}
	// The headers arrived even though the body read failed.
	if ce.DroppedReason != "evicted-queue-pressure" {
		t.Errorf("DroppedReason = %q, want evicted-queue-pressure", ce.DroppedReason)
	}
	if ce.RetryAfter != 5*time.Second {
		t.Errorf("RetryAfter = %v, want 5s", ce.RetryAfter)
	}
}
