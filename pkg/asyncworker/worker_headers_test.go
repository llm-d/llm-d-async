package asyncworker

import (
	"context"
	"fmt"
	"maps"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform/gcsmultipart"
)

func TestHeadersWithContentType(t *testing.T) {
	const contentType = "multipart/form-data; boundary=generated-boundary"
	for _, tc := range []struct {
		name    string
		headers map[string]string
	}{
		{name: "nil"},
		{name: "empty", headers: map[string]string{}},
		{name: "canonical", headers: map[string]string{"Content-Type": "application/json"}},
		{name: "lowercase", headers: map[string]string{"content-type": "application/json"}},
		{name: "mixed case", headers: map[string]string{"cOnTeNt-TyPe": "application/json"}},
		{name: "multiple spellings", headers: map[string]string{
			"Content-Type": "application/json", "content-type": "text/plain", "CONTENT-TYPE": "application/octet-stream",
			"Authorization": "Bearer test-token", "x-custom": "unchanged",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := maps.Clone(tc.headers)
			got := headersWithContentType(tc.headers, contentType)
			if got["Content-Type"] != contentType {
				t.Errorf("Content-Type = %q, want %q", got["Content-Type"], contentType)
			}
			for k, v := range got {
				if strings.EqualFold(k, "Content-Type") {
					if k != "Content-Type" {
						t.Errorf("unexpected Content-Type spelling %q", k)
					}
				} else if original[k] != v {
					t.Errorf("header %q = %q, want %q", k, v, original[k])
				}
			}
			for k, v := range original {
				if !strings.EqualFold(k, "Content-Type") && got[k] != v {
					t.Errorf("header %q = %q, want %q", k, got[k], v)
				}
			}
			got["x-custom"] = "modified copy"
			if !maps.Equal(tc.headers, original) {
				t.Errorf("original headers mutated: got %v, want %v", tc.headers, original)
			}
		})
	}
}

func TestWorker_MultipartContentTypeOverridesCallerHeaders(t *testing.T) {
	const signedURL = "https://storage.example/audio.mp3?signature=test"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentTypes := r.Header.Values("Content-Type")
		if len(contentTypes) != 1 {
			http.Error(w, fmt.Sprintf("Content-Type values = %v", contentTypes), http.StatusUnsupportedMediaType)
			return
		}
		mediaType, params, err := mime.ParseMediaType(contentTypes[0])
		if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
			http.Error(w, fmt.Sprintf("invalid multipart Content-Type: %q", contentTypes[0]), http.StatusUnsupportedMediaType)
			return
		}
		if err := r.ParseMultipartForm(1024); err != nil {
			http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
			return
		}
		defer r.MultipartForm.RemoveAll() // nolint:errcheck
		if r.FormValue("url") != signedURL || r.FormValue("model") != "whisper" ||
			r.Header.Get("Authorization") != "Bearer test-token" || r.Header.Get("X-Custom") != "unchanged" {
			http.Error(w, "form fields or other headers changed", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	plugin, err := gcsmultipart.New("whisper", []byte(`{"providers":["whisper"]}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	requestTransform, ok := plugin.(transform.RequestTransform)
	if !ok {
		t.Fatal("gcs_uri_multipart plugin must implement RequestTransform")
	}
	chain := transform.NewChain([]transform.RequestTransform{requestTransform})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(server.Client()), requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	headers := map[string]string{
		"Content-Type": "application/json", "content-type": "text/plain", "CONTENT-TYPE": "application/octet-stream",
		"Authorization": "Bearer test-token", "x-custom": "unchanged",
	}
	original := maps.Clone(headers)
	// Repeated dispatches exercise Go's varying map iteration order. The helper
	// test above deterministically rejects every stale Content-Type spelling.
	for i := range 16 {
		requestChannel <- newEmb(asyncapi.RequestMessage{
			ID: fmt.Sprintf("multipart-%d", i), Created: time.Now().Unix(), Deadline: time.Now().Add(time.Minute).Unix(),
			Payload: testPayload(map[string]any{"model": "whisper", "gcs_uri": signedURL}), Metadata: map[string]string{"provider": "whisper"},
		}, server.URL+"/v1/audio/transcriptions", headers)
		select {
		case result := <-resultChannel:
			if result.StatusCode != http.StatusOK {
				t.Errorf("dispatch %d: status = %d, payload = %q", i, result.StatusCode, result.Payload)
			}
		case <-retryChannel:
			t.Fatal("multipart request must not be retried")
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for multipart request result")
		}
	}
	if !maps.Equal(headers, original) {
		t.Errorf("caller headers mutated: got %v, want %v", headers, original)
	}
}
