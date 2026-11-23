package s3multipart

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	asyncapi "github.com/llm-d/llm-d-async/api"
)

const transcriptionsURL = "http://gw.example/prefix/v1/audio/transcriptions"

func TestTransform_AudioFromS3(t *testing.T) {
	audio := []byte("RIFF....WAVEfmt fake audio")
	store := newFakeS3(t, map[string]object{"/audio/calls/42.wav": {body: audio, contentType: "audio/wav"}})
	p := newTestPlugin(t, store.URL, 0)
	payload := []byte(`{
		"model": "openai/whisper-large-v3",
		"file": "s3://audio/calls/42.wav",
		"language": "en",
		"temperature": 0.2,
		"timestamp_granularities": ["word", "segment"],
		"vllm_xargs": {"a": 1},
		"prompt": null
	}`)

	for _, endpoint := range []string{transcriptionsURL, "http://gw/v1/audio/translations"} {
		body, contentType, handled, err := p.TransformContext(context.Background(), payload, nil, endpoint)
		if err != nil || !handled {
			t.Fatalf("%s: handled %v err %v, want true/nil", endpoint, handled, err)
		}
		parts := parseParts(t, contentType, body)

		file := parts["file"]
		if file == nil || len(file.values) != 1 || file.values[0] != string(audio) {
			t.Fatalf("%s: file part = %+v, want the object bytes", endpoint, file)
		}
		if file.filename != "42.wav" || file.contentType != "audio/wav" {
			t.Errorf("file part filename %q type %q, want 42.wav audio/wav", file.filename, file.contentType)
		}
		want := map[string][]string{
			"model":                     {"openai/whisper-large-v3"},
			"language":                  {"en"},
			"temperature":               {"0.2"},
			"timestamp_granularities[]": {"word", "segment"},
			"vllm_xargs":                {`{"a": 1}`},
		}
		for name, values := range want {
			if got := parts[name]; got == nil || fmt.Sprint(got.values) != fmt.Sprint(values) {
				t.Errorf("%s: field %q = %+v, want %v", endpoint, name, got, values)
			}
		}
		if len(parts) != len(want)+1 {
			t.Errorf("%s: parts = %v, want file plus %d fields (null prompt omitted)", endpoint, keys(parts), len(want))
		}
	}
	if auth, _ := store.lastAuth.Load().(string); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=test-access-key/") {
		t.Errorf("storage request Authorization = %q, want SigV4 with the env credentials", auth)
	}
}

func TestTransform_AudioFromDataURL(t *testing.T) {
	store := newFakeS3(t, nil)
	p := newTestPlugin(t, store.URL, 0)
	audio := []byte{0xff, 0xfb, 0x90, 0x00, 0x01}
	payload := fmt.Appendf(nil, `{"model":"whisper","file":"data:audio/mpeg;base64,%s"}`, base64.StdEncoding.EncodeToString(audio))

	body, contentType, handled, err := p.TransformContext(context.Background(), payload, nil, transcriptionsURL)
	if err != nil || !handled {
		t.Fatalf("handled %v err %v, want true/nil", handled, err)
	}
	file := parseParts(t, contentType, body)["file"]
	if file == nil || file.values[0] != string(audio) || file.contentType != "audio/mpeg" {
		t.Errorf("file part = %+v, want the decoded bytes as audio/mpeg", file)
	}
	if n := store.requests.Load(); n != 0 {
		t.Errorf("storage requests = %d, want 0 for a data URL", n)
	}
}

func TestTransform_AudioRejectsBadInput(t *testing.T) {
	store := newFakeS3(t, map[string]object{
		"/audio/big.wav":         {body: bytes.Repeat([]byte("x"), 64)},
		"/audio/big-chunked.wav": {body: bytes.Repeat([]byte("x"), 64), chunked: true},
	})
	p := newTestPlugin(t, store.URL, 32)
	tests := map[string]string{
		"not json":           `not json`,
		"no file":            `{"model":"whisper"}`,
		"file not a string":  `{"file":42}`,
		"http url":           `{"file":"https://example.com/a.wav"}`,
		"bucket not allowed": `{"file":"s3://other/a.wav"}`,
		"no key":             `{"file":"s3://audio"}`,
		// Valid base64 without the ;base64 marker: a percent-encoded data URL,
		// which the plugin does not decode.
		"data url not base64": `{"file":"data:audio/wav,QUJD"}`,
		// JSON escapes decode to CR/LF, which would start a new part header.
		"data url header injection": `{"file":"data:audio/wav\r\nX-Injected: yes;base64,QUJD"}`,
		"bad base64":                `{"file":"data:audio/wav;base64,!!!"}`,
		"data url too big":          fmt.Sprintf(`{"file":"data:audio/wav;base64,%s"}`, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), 33))),
		"object too big":            `{"file":"s3://audio/big.wav"}`,
		"chunked too big":           `{"file":"s3://audio/big-chunked.wav"}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, handled, err := p.TransformContext(context.Background(), []byte(payload), nil, transcriptionsURL)
			if handled {
				t.Error("handled = true, want false on rejection")
			}
			assertCategory(t, err, asyncapi.ErrCategoryInvalidReq)
		})
	}
	// Only the two size checks needed to read from storage.
	if n := store.requests.Load(); n != 2 {
		t.Errorf("storage requests = %d, want 2", n)
	}
}

// TestTransform_AudioHeadersCannotBeInjected checks that object names and stored
// metadata, which the request does not control the format of, cannot add
// headers to the file part.
func TestTransform_AudioHeadersCannotBeInjected(t *testing.T) {
	store := newFakeS3(t, map[string]object{
		"/audio/a\r\nX-Injected: yes.wav": {body: []byte("audio"), contentType: "not a media type"},
	})
	p := newTestPlugin(t, store.URL, 0)
	payload := []byte(`{"file":"s3://audio/a\r\nX-Injected: yes.wav"}`)

	body, contentType, handled, err := p.TransformContext(context.Background(), payload, nil, transcriptionsURL)
	if err != nil || !handled {
		t.Fatalf("handled %v err %v, want true/nil", handled, err)
	}
	file := parseParts(t, contentType, body)["file"]
	if file == nil || file.values[0] != "audio" {
		t.Fatalf("file part = %+v, want the object bytes", file)
	}
	if got := file.header.Get("X-Injected"); got != "" {
		t.Errorf("file part carries injected header X-Injected: %q", got)
	}
	if file.contentType != defaultMediaType {
		t.Errorf("file part Content-Type = %q, want %q for unusable stored metadata", file.contentType, defaultMediaType)
	}
	if file.filename != "a\r\nX-Injected: yes.wav" {
		t.Errorf("filename = %q, want the key's base name round-tripped intact", file.filename)
	}
}

func TestTransform_AudioStorageErrors(t *testing.T) {
	store := newFakeS3(t, map[string]object{
		"/audio/denied.wav":    {status: http.StatusForbidden, code: "AccessDenied"},
		"/audio/throttled.wav": {status: http.StatusTooManyRequests, code: "SlowDown"},
		"/audio/down.wav":      {status: http.StatusServiceUnavailable, code: "ServiceUnavailable"},
		// S3 reports an idle client socket as a 400 the SDK itself retries.
		"/audio/timeout.wav":   {status: http.StatusBadRequest, code: "RequestTimeout"},
		"/audio/truncated.wav": {body: []byte("short"), declaredLength: 100},
	})
	p := newTestPlugin(t, store.URL, 0)
	tests := map[string]asyncapi.ErrorCategory{
		"missing.wav":   asyncapi.ErrCategoryInvalidReq, // 404 NoSuchKey
		"denied.wav":    asyncapi.ErrCategoryAuth,
		"throttled.wav": asyncapi.ErrCategoryRateLimit,
		"down.wav":      asyncapi.ErrCategoryServer,
		"timeout.wav":   asyncapi.ErrCategoryServer,
		"truncated.wav": asyncapi.ErrCategoryServer, // body cut off mid-read
	}
	for key, want := range tests {
		t.Run(key, func(t *testing.T) {
			payload := fmt.Appendf(nil, `{"file":"s3://audio/%s"}`, key)
			_, _, _, err := p.TransformContext(context.Background(), payload, nil, transcriptionsURL)
			assertCategory(t, err, want)
		})
	}

	t.Run("store unreachable", func(t *testing.T) {
		gone := httptest.NewServer(http.NotFoundHandler())
		gone.Close()
		p := newTestPlugin(t, gone.URL, 0)
		_, _, _, err := p.TransformContext(context.Background(), []byte(`{"file":"s3://audio/a.wav"}`), nil, transcriptionsURL)
		assertCategory(t, err, asyncapi.ErrCategoryServer)
	})

	// A network-level timeout looks like context.DeadlineExceeded, but the
	// request still has time left, so it must stay retryable.
	t.Run("transport timeout with time left", func(t *testing.T) {
		slow := newFakeS3(t, map[string]object{"/audio/slow.wav": {block: true}})
		p := newTestPlugin(t, slow.URL, 0, func(o *s3.Options) { o.HTTPClient = &http.Client{Timeout: 50 * time.Millisecond} })
		_, _, _, err := p.TransformContext(context.Background(), []byte(`{"file":"s3://audio/slow.wav"}`), nil, transcriptionsURL)
		assertCategory(t, err, asyncapi.ErrCategoryServer)
	})

	// Missing credentials are a configuration error: fail instead of retrying
	// every request until its deadline.
	t.Run("no credentials", func(t *testing.T) {
		isolateAWSEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "")
		p, err := newPlugin(context.Background(), "nocreds", params{Endpoint: store.URL, Region: "us-east-1", UsePathStyle: true, AllowedBuckets: []string{"audio"}},
			func(o *s3.Options) { o.Retryer = aws.NopRetryer{} })
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, err = p.TransformContext(context.Background(), []byte(`{"file":"s3://audio/down.wav"}`), nil, transcriptionsURL)
		var ierr asyncapi.InferenceError
		if !errors.As(err, &ierr) || !ierr.Category().Fatal() {
			t.Errorf("err = %v, want a fatal category", err)
		}
	})
}

func TestTransform_AudioHonorsContext(t *testing.T) {
	store := newFakeS3(t, map[string]object{"/audio/slow.wav": {block: true}})
	p := newTestPlugin(t, store.URL, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, _, handled, err := p.TransformContext(ctx, []byte(`{"file":"s3://audio/slow.wav"}`), nil, transcriptionsURL)
	if handled || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handled %v err %v, want false and a deadline error", handled, err)
	}
	// The worker classifies a context error from its own context, so the
	// plugin must not dress it up as a retryable storage error.
	var cerr *asyncapi.ClientError
	if errors.As(err, &cerr) {
		t.Errorf("err carries category %s, want a bare context error", cerr.ErrorCategory)
	}
}
