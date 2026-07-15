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
)

var _ transform.RequestTransform = (*testTransform)(nil)

// testTransform is a minimal RequestTransform for worker integration tests.
type testTransform struct {
	validateErr error
	handled     bool
	body        []byte
	contentType string
}

func (t *testTransform) TypedName() plugins.TypedName {
	return plugins.TypedName{Type: "test", Name: "test"}
}

func (t *testTransform) Validate(_ []byte, _ map[string]string, _ int64) error {
	return t.validateErr
}

func (t *testTransform) Transform(_ []byte, _ map[string]string) ([]byte, string, bool, error) {
	if !t.handled {
		return nil, "", false, nil
	}
	return t.body, t.contentType, true, nil
}

// TestWorker_TransformRewritesOutgoingRequest verifies that when a transform
// handles a message, the worker sends the transformed body and Content-Type to
// the inference backend.
func TestWorker_TransformRewritesOutgoingRequest(t *testing.T) {
	var gotBody, gotContentType string
	httpClient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		gotBody = string(b)
		gotContentType = req.Header.Get("Content-Type")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpClient)

	chain := transform.NewChain([]transform.RequestTransform{&testTransform{
		handled:     true,
		body:        []byte("url=https://storage.example/audio.mp3?signature=test"),
		contentType: "multipart/form-data; boundary=test",
	}})

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "m1",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "whisper", "gcs_uri": "https://storage.example/audio.mp3?signature=test"},
		Metadata: map[string]string{"provider": "whisper"},
	}, "http://localhost/v1/audio/transcriptions", map[string]string{})

	select {
	case res := <-resultChannel:
		if res.ID != "m1" {
			t.Errorf("result ID = %q, want m1", res.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}

	if want := "url=https://storage.example/audio.mp3?signature=test"; gotBody != want {
		t.Errorf("outgoing body = %q, want %q", gotBody, want)
	}
	if want := "multipart/form-data; boundary=test"; gotContentType != want {
		t.Errorf("outgoing Content-Type = %q, want %q", gotContentType, want)
	}
}

// TestWorker_TransformValidateFatal verifies that a transform Validate failure
// is fatal: an error result is produced and no HTTP request is dispatched.
func TestWorker_TransformValidateFatal(t *testing.T) {
	httpCalled := false
	httpClient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		httpCalled = true
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpClient)

	chain := transform.NewChain([]transform.RequestTransform{&testTransform{
		validateErr: errors.New("signed URL expires before request deadline"),
	}})

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "m2",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "whisper"},
		Metadata: map[string]string{"provider": "whisper"},
	}, "http://localhost/v1/audio/transcriptions", map[string]string{})

	select {
	case res := <-resultChannel:
		if !strings.Contains(res.Payload, "validate") {
			t.Errorf("result payload = %q, want it to mention validate failure", res.Payload)
		}
	case <-retryChannel:
		t.Fatal("validate failure must not be retried")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}

	if httpCalled {
		t.Error("no HTTP request should be dispatched when validation fails")
	}
}
