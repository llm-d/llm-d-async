package asyncworker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d/llm-d-async/pkg/plugins"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var _ transform.ResponseTransform = (*testResponseTransform)(nil)

// testResponseTransform is a minimal ResponseTransform for worker tests. It
// records the body and metadata it was called with.
type testResponseTransform struct {
	err         error
	handled     bool
	body        []byte
	contentType string

	calls       int
	gotBody     string
	gotMetadata map[string]string
	gotDeadline bool
}

func (t *testResponseTransform) TypedName() plugins.TypedName {
	return plugins.TypedName{Type: "test-response", Name: "test-response"}
}

func (t *testResponseTransform) Transform(ctx context.Context, responseBody []byte, metadata map[string]string) ([]byte, string, bool, error) {
	t.calls++
	t.gotBody = string(responseBody)
	t.gotMetadata = metadata
	_, t.gotDeadline = ctx.Deadline()
	if t.err != nil {
		return nil, "", false, t.err
	}
	if !t.handled {
		return nil, "", false, nil
	}
	return t.body, t.contentType, true, nil
}

// runResponseTransformWorker dispatches one message to a backend that answers
// with statusCode and backendBody, and returns the published result.
func runResponseTransformWorker(t *testing.T, rt *testResponseTransform, statusCode int, backendBody string) asyncapi.ResultMessage {
	t.Helper()
	httpClient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: statusCode, Body: io.NopCloser(strings.NewReader(backendBody)), Header: http.Header{"Content-Type": []string{"application/json"}}}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpClient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, transform.NewChain(nil, rt))

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "resp-1",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  testPayload(map[string]any{"model": "m"}),
		Metadata: map[string]string{"provider": "p"},
	}, "http://localhost/v1/completions", map[string]string{})

	select {
	case res := <-resultChannel:
		return res
	case <-retryChannel:
		t.Fatal("response transform path must not retry")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
	return asyncapi.ResultMessage{}
}

// TestWorker_ResponseTransformRewritesResult verifies that a handled response
// transform replaces the published payload and receives the request metadata.
func TestWorker_ResponseTransformRewritesResult(t *testing.T) {
	rt := &testResponseTransform{handled: true, body: []byte(`{"rewritten":true}`)}
	res := runResponseTransformWorker(t, rt, http.StatusOK, `{"original":true}`)

	if res.Payload != `{"rewritten":true}` {
		t.Errorf("result payload = %q, want rewritten body", res.Payload)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("result status = %d, want 200", res.StatusCode)
	}
	if res.ErrorCode != "" {
		t.Errorf("result error code = %q, want empty", res.ErrorCode)
	}
	// An empty content type from the transform keeps the response's.
	if res.ContentType != "application/json" {
		t.Errorf("result content type = %q, want the backend Content-Type", res.ContentType)
	}
	if !rt.gotDeadline {
		t.Error("transform ctx has no deadline, want the dispatch attempt's deadline")
	}
	if rt.gotBody != `{"original":true}` {
		t.Errorf("transform saw body %q, want the backend body", rt.gotBody)
	}
	if rt.gotMetadata["provider"] != "p" {
		t.Errorf("transform saw metadata %v, want provider=p", rt.gotMetadata)
	}
}

// TestWorker_ResponseTransformSetsContentType verifies that a content type
// returned by a handled response transform replaces the published one.
func TestWorker_ResponseTransformSetsContentType(t *testing.T) {
	rt := &testResponseTransform{handled: true, body: []byte("plain"), contentType: "text/plain"}
	res := runResponseTransformWorker(t, rt, http.StatusOK, `{"original":true}`)

	if res.Payload != "plain" {
		t.Errorf("result payload = %q, want rewritten body", res.Payload)
	}
	if res.ContentType != "text/plain" {
		t.Errorf("result content type = %q, want text/plain", res.ContentType)
	}
}

// TestWorker_ResponseTransformNotHandledKeepsBody verifies that an unhandled
// response is published verbatim.
func TestWorker_ResponseTransformNotHandledKeepsBody(t *testing.T) {
	rt := &testResponseTransform{}
	res := runResponseTransformWorker(t, rt, http.StatusOK, `{"original":true}`)

	if res.Payload != `{"original":true}` {
		t.Errorf("result payload = %q, want original body", res.Payload)
	}
	if rt.calls != 1 {
		t.Errorf("transform called %d times, want 1", rt.calls)
	}
	if res.ContentType != "application/json" {
		t.Errorf("result content type = %q, want the backend Content-Type", res.ContentType)
	}
}

// TestWorker_ResponseTransformSkippedOnNon2xx verifies that a 3xx response,
// which SendRequest treats as success, is not passed to response transforms.
func TestWorker_ResponseTransformSkippedOnNon2xx(t *testing.T) {
	rt := &testResponseTransform{handled: true, body: []byte("rewritten")}
	res := runResponseTransformWorker(t, rt, http.StatusMovedPermanently, "moved")

	if res.Payload != "moved" {
		t.Errorf("result payload = %q, want original body", res.Payload)
	}
	if rt.calls != 0 {
		t.Errorf("transform called %d times, want 0", rt.calls)
	}
}

// TestWorker_ResponseTransformErrorIsFatal verifies that a response transform
// error publishes an error result, records the error on the span, and is not
// retried.
func TestWorker_ResponseTransformErrorIsFatal(t *testing.T) {
	exporter := setupTestTracer(t)
	rt := &testResponseTransform{err: errors.New("cannot rewrite response")}
	res := runResponseTransformWorker(t, rt, http.StatusOK, `{"original":true}`)

	if res.ErrorCode != asyncapi.ErrCodeInferenceError {
		t.Errorf("error code = %q, want %q", res.ErrorCode, asyncapi.ErrCodeInferenceError)
	}
	if !strings.Contains(res.ErrorMessage, "cannot rewrite response") {
		t.Errorf("error message = %q, want it to include the transform error", res.ErrorMessage)
	}

	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	assertSpanAttributes(t, s,
		attribute.String("llm_d.async.error.category", "PARSE_ERROR"),
		attribute.String("error.category", "PARSE_ERROR"),
	)
	if s.Status.Code != codes.Error {
		t.Error("expected error status on response transform failure span")
	}
}
