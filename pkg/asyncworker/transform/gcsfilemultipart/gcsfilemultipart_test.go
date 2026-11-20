package gcsfilemultipart

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/plugins"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func newPlugin(t *testing.T, parameters string, transport http.RoundTripper) *Plugin {
	t.Helper()
	plugin, err := New("gcs-audio", json.RawMessage(parameters), plugins.NewHandle(context.Background()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p, ok := plugin.(*Plugin)
	if !ok {
		t.Fatalf("New returned %T, want *Plugin", plugin)
	}
	if transport != nil {
		p.client.Transport = transport
	}
	return p
}

func v4SignedURL(expiry time.Time) string {
	return v4SignedURLWithSignature(expiry, "GOOG4-RSA-SHA256", strings.Repeat("ab", 32))
}

func v4SignedURLWithSignature(expiry time.Time, algorithm, signature string) string {
	start := expiry.Add(-time.Hour).UTC()
	return fmt.Sprintf(
		"https://storage.googleapis.com/bucket/audio.wav?X-Goog-Algorithm=%s&X-Goog-Credential=test%%2F20260618%%2Fauto%%2Fstorage%%2Fgoog4_request&X-Goog-Date=%s&X-Goog-Expires=%d&X-Goog-Signature=%s",
		algorithm, start.Format("20060102T150405Z"), int(expiry.Sub(start).Seconds()), signature,
	)
}

func parseParts(t *testing.T, contentType string, body []byte) (map[string][]string, map[string][]byte, map[string]string) {
	t.Helper()
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		t.Fatalf("ParseMediaType: %v", err)
	}
	if mediaType != "multipart/form-data" {
		t.Fatalf("media type = %q, want multipart/form-data", mediaType)
	}

	fields := map[string][]string{}
	files := map[string][]byte{}
	filenames := map[string]string{}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextPart: %v", err)
		}
		value, err := io.ReadAll(part)
		if err != nil {
			t.Fatalf("read part %q: %v", part.FormName(), err)
		}
		fields[part.FormName()] = append(fields[part.FormName()], string(value))
		if part.FileName() != "" {
			files[part.FormName()] = value
			filenames[part.FormName()] = part.FileName()
		}
	}
	return fields, files, filenames
}

func assertInvalidRequest(t *testing.T, err error) {
	t.Helper()
	var clientErr *asyncapi.ClientError
	if !errors.As(err, &clientErr) {
		t.Fatalf("error = %v, want *asyncapi.ClientError", err)
	}
	if clientErr.ErrorCategory != asyncapi.ErrCategoryInvalidReq {
		t.Fatalf("error category = %s, want %s", clientErr.ErrorCategory, asyncapi.ErrCategoryInvalidReq)
	}
}

func TestTransformWithContext_BuildsAudioFileMultipart(t *testing.T) {
	audio := []byte("audio bytes")
	for _, endpoint := range []string{
		"https://gateway.example/v1/audio/transcriptions",
		"https://gateway.example/v1/audio/translations",
	} {
		t.Run(endpoint, func(t *testing.T) {
			p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet {
					t.Errorf("method = %s, want GET", req.Method)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(audio)), Header: make(http.Header)}, nil
			}))
			payload := []byte(fmt.Sprintf(`{"model":"whisper","gcs_uri":%q,"language":"en","response_format":"json"}`, v4SignedURL(time.Now().Add(time.Hour))))

			body, contentType, handled, err := p.TransformWithContext(context.Background(), payload, nil, endpoint)
			if err != nil || !handled {
				t.Fatalf("TransformWithContext = handled %v, err %v; want true, nil", handled, err)
			}
			fields, files, filenames := parseParts(t, contentType, body)
			if !bytes.Equal(files["file"], audio) {
				t.Errorf("file part = %q, want exact audio bytes", files["file"])
			}
			if filenames["file"] != "audio.wav" {
				t.Errorf("filename = %q, want audio.wav", filenames["file"])
			}
			for field, want := range map[string]string{"model": "whisper", "language": "en", "response_format": "json"} {
				if got := fields[field]; len(got) != 1 || got[0] != want {
					t.Errorf("field %s = %v, want [%s]", field, got, want)
				}
			}
			for _, absent := range []string{"gcs_uri", "url", "file_base64"} {
				if _, ok := fields[absent]; ok {
					t.Errorf("field %s must not be present", absent)
				}
			}
			if strings.Contains(string(body), "X-Goog-Signature") {
				t.Error("multipart body contains the signed URL")
			}
		})
	}
}

func TestTransformWithContext_RejectsInvalidURLsBeforeDownload(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"expired", v4SignedURL(time.Now().Add(-time.Minute))},
		{"invalid signature format", strings.Replace(v4SignedURL(time.Now().Add(time.Hour)), strings.Repeat("ab", 32), "not-hex!", 1)},
		{"unsupported host", strings.Replace(v4SignedURL(time.Now().Add(time.Hour)), "storage.googleapis.com", "evil.example", 1)},
		{"http scheme", strings.Replace(v4SignedURL(time.Now().Add(time.Hour)), "https://", "http://", 1)},
		{"unsigned", "https://storage.googleapis.com/bucket/audio.wav"},
		{"gs uri", "gs://bucket/audio.wav"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(*http.Request) (*http.Response, error) {
				t.Fatal("invalid URL must not be downloaded")
				return nil, nil
			}))
			payload, _ := json.Marshal(map[string]string{"model": "whisper", "gcs_uri": tc.url})

			_, _, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/audio/transcriptions")
			if handled {
				t.Error("handled = true, want false")
			}
			assertInvalidRequest(t, err)
			if strings.Contains(err.Error(), "X-Goog-Signature") {
				t.Error("error exposes the signed URL")
			}
		})
	}
}

func TestValidateWithContext(t *testing.T) {
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, nil)
	deadline := time.Now().Add(time.Hour).Unix()
	tests := []struct {
		name      string
		url       string
		endpoint  string
		wantError bool
	}{
		{"future expiry", v4SignedURL(time.Now().Add(2 * time.Hour)), "https://gateway.example/v1/audio/transcriptions", false},
		{"expired", v4SignedURL(time.Now().Add(-time.Minute)), "https://gateway.example/v1/audio/transcriptions", true},
		{"unrelated endpoint", v4SignedURL(time.Now().Add(-time.Minute)), "https://gateway.example/v1/chat/completions", false},
		{"no gcs uri", "", "https://gateway.example/v1/audio/transcriptions", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(`{"model":"whisper"}`)
			if tc.url != "" {
				payload = []byte(fmt.Sprintf(`{"model":"whisper","gcs_uri":%q}`, tc.url))
			}
			err := p.ValidateWithContext(payload, nil, tc.endpoint, deadline)
			if tc.wantError {
				assertInvalidRequest(t, err)
				return
			}
			if err != nil {
				t.Errorf("ValidateWithContext: %v", err)
			}
		})
	}
}

func TestValidateWithContext_ObjectPath(t *testing.T) {
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com","my-bucket.storage.googleapis.com"],"maxFileBytes":1024}`, nil)
	deadline := time.Now().Add(time.Hour).Unix()
	signed := v4SignedURL(time.Now().Add(2 * time.Hour))
	withTarget := func(target string) string {
		return strings.Replace(signed, "storage.googleapis.com/bucket/audio.wav", target, 1)
	}
	tests := []struct {
		name      string
		url       string
		wantError bool
	}{
		{"path-style bucket and object", signed, false},
		{"path-style without object", withTarget("storage.googleapis.com/audio.wav"), true},
		{"virtual-hosted root object", withTarget("my-bucket.storage.googleapis.com/audio.wav"), false},
		{"virtual-hosted nested object", withTarget("my-bucket.storage.googleapis.com/folder/audio.wav"), false},
		{"virtual-hosted without object", withTarget("my-bucket.storage.googleapis.com/"), true},
		{"path-style without bucket name", withTarget("storage.googleapis.com//audio.wav"), true},
		{"path-style bucket without object", withTarget("storage.googleapis.com/bucket/"), true},
		{"path-style object name starts with slash", withTarget("storage.googleapis.com/bucket/%2Faudio.wav"), false},
		{"path-style object named slash", withTarget("storage.googleapis.com/bucket/%2F"), false},
		{"virtual-hosted object name starts with slash", withTarget("my-bucket.storage.googleapis.com/%2Faudio.wav"), false},
		{"virtual-hosted object named slash", withTarget("my-bucket.storage.googleapis.com/%2F"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(fmt.Sprintf(`{"model":"whisper","gcs_uri":%q}`, tc.url))
			err := p.ValidateWithContext(payload, nil, "https://gateway.example/v1/audio/transcriptions", deadline)
			if tc.wantError {
				assertInvalidRequest(t, err)
				return
			}
			if err != nil {
				t.Errorf("ValidateWithContext: %v", err)
			}
		})
	}
}

func TestTransformWithContext_DownloadsVirtualHostedURL(t *testing.T) {
	signedURL := strings.Replace(v4SignedURL(time.Now().Add(time.Hour)), "storage.googleapis.com/bucket/audio.wav", "my-bucket.storage.googleapis.com/audio.wav", 1)
	p := newPlugin(t, `{"allowedHosts":["my-bucket.storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "my-bucket.storage.googleapis.com" || req.URL.Path != "/audio.wav" {
			t.Errorf("download URL = %s%s, want my-bucket.storage.googleapis.com/audio.wav", req.URL.Host, req.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("audio")), Header: make(http.Header)}, nil
	}))
	payload := []byte(fmt.Sprintf(`{"model":"whisper","gcs_uri":%q}`, signedURL))

	body, contentType, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/audio/transcriptions")
	if err != nil || !handled {
		t.Fatalf("TransformWithContext = handled %v, err %v; want true, nil", handled, err)
	}
	_, files, filenames := parseParts(t, contentType, body)
	if string(files["file"]) != "audio" || filenames["file"] != "audio.wav" {
		t.Errorf("file part = %q named %q, want %q named %q", files["file"], filenames["file"], "audio", "audio.wav")
	}
}

func TestValidateWithContext_AcceptsV4SignatureAlgorithms(t *testing.T) {
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, nil)
	deadline := time.Now().Add(time.Hour).Unix()
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "RSA 2048-bit signature",
			url:  v4SignedURLWithSignature(time.Now().Add(2*time.Hour), "GOOG4-RSA-SHA256", strings.Repeat("ab", 256)),
		},
		{
			name: "HMAC-SHA256 signature",
			url:  v4SignedURLWithSignature(time.Now().Add(2*time.Hour), "GOOG4-HMAC-SHA256", strings.Repeat("cd", 32)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(fmt.Sprintf(`{"model":"whisper","gcs_uri":%q}`, tc.url))
			if err := p.ValidateWithContext(payload, nil, "https://gateway.example/v1/audio/transcriptions", deadline); err != nil {
				t.Fatalf("ValidateWithContext: %v", err)
			}
		})
	}
}

func TestValidateWithContext_RejectsInvalidHMACSignatureLength(t *testing.T) {
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, nil)
	payload := []byte(fmt.Sprintf(`{"model":"whisper","gcs_uri":%q}`, v4SignedURLWithSignature(
		time.Now().Add(2*time.Hour), "GOOG4-HMAC-SHA256", strings.Repeat("ab", 256))))

	err := p.ValidateWithContext(payload, nil, "https://gateway.example/v1/audio/transcriptions", time.Now().Add(time.Hour).Unix())
	assertInvalidRequest(t, err)
}

func TestTransformWithContext_NotHandledForUnrelatedEndpoint(t *testing.T) {
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unrelated endpoint must not be downloaded")
		return nil, nil
	}))
	payload := []byte(fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(time.Now().Add(time.Hour))))

	body, _, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/chat/completions")
	if err != nil || handled || body != nil {
		t.Fatalf("TransformWithContext = (%q,%v,%v), want nil/false/nil", body, handled, err)
	}
}

func TestTransformWithContext_RejectsRedirectToUnapprovedHost(t *testing.T) {
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Location", "https://evil.example/audio.wav")
		return &http.Response{StatusCode: http.StatusFound, Header: header, Body: io.NopCloser(strings.NewReader(""))}, nil
	}))
	payload := []byte(fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(time.Now().Add(time.Hour))))

	_, _, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/audio/transcriptions")
	if handled {
		t.Error("handled = true, want false")
	}
	assertInvalidRequest(t, err)
}

func TestTransformWithContext_RejectsOversizedDownload(t *testing.T) {
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":3}`, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader([]byte("abcd")))}, nil
	}))
	payload := []byte(fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(time.Now().Add(time.Hour))))

	_, _, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/audio/transcriptions")
	if handled {
		t.Error("handled = true, want false")
	}
	assertInvalidRequest(t, err)
}

func TestTransformWithContext_HTTPStatuses(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		category   asyncapi.ErrorCategory
		retryAfter time.Duration
	}{
		{"missing object", http.StatusNotFound, asyncapi.ErrCategoryInvalidReq, 0},
		{"rate limited", http.StatusTooManyRequests, asyncapi.ErrCategoryRateLimit, 30 * time.Second},
		{"server error", http.StatusInternalServerError, asyncapi.ErrCategoryServer, 7 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(*http.Request) (*http.Response, error) {
				header := make(http.Header)
				if tc.retryAfter > 0 {
					header.Set("Retry-After", strconv.FormatInt(int64(tc.retryAfter/time.Second), 10))
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader("response")), Header: header}, nil
			}))
			payload := []byte(fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(time.Now().Add(time.Hour))))

			_, _, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/audio/transcriptions")
			if handled {
				t.Error("handled = true, want false")
			}
			var clientErr *asyncapi.ClientError
			if !errors.As(err, &clientErr) || clientErr.ErrorCategory != tc.category {
				t.Fatalf("error = %v, want category %s", err, tc.category)
			}
			if clientErr.RetryAfter != tc.retryAfter {
				t.Errorf("RetryAfter = %v, want %v", clientErr.RetryAfter, tc.retryAfter)
			}
		})
	}
}

func TestTransformWithContext_ConnectionFailureIsRetryable(t *testing.T) {
	signedURL := v4SignedURL(time.Now().Add(time.Hour))
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Get", URL: signedURL, Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}
	}))
	payload := []byte(fmt.Sprintf(`{"gcs_uri":%q}`, signedURL))

	_, _, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/audio/transcriptions")
	if handled {
		t.Error("handled = true, want false")
	}
	var clientErr *asyncapi.ClientError
	if !errors.As(err, &clientErr) || clientErr.ErrorCategory != asyncapi.ErrCategoryServer {
		t.Fatalf("error = %v, want SERVER_ERROR", err)
	}
	if strings.Contains(err.Error(), signedURL) || strings.Contains(err.Error(), "X-Goog-Signature") {
		t.Error("connection failure exposes the signed URL")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error = %v, want the network cause to be kept", err)
	}
}

// A redirect with a malformed Location makes the HTTP client copy that header
// into plain error text. The error must not carry any part of it.
func TestTransformWithContext_MalformedRedirectDoesNotExposeSignature(t *testing.T) {
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Location", "https://storage.googleapis.com/bucket/other.wav?X-Goog-Signature=deadbeef#%zz")
		return &http.Response{StatusCode: http.StatusFound, Header: header, Body: io.NopCloser(strings.NewReader(""))}, nil
	}))
	payload := []byte(fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(time.Now().Add(time.Hour))))

	_, _, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/audio/transcriptions")
	if handled {
		t.Error("handled = true, want false")
	}
	var clientErr *asyncapi.ClientError
	if !errors.As(err, &clientErr) || clientErr.ErrorCategory != asyncapi.ErrCategoryServer {
		t.Fatalf("error = %v, want SERVER_ERROR", err)
	}
	for _, leaked := range []string{"X-Goog-Signature", "deadbeef", "other.wav", "Location"} {
		if strings.Contains(err.Error(), leaked) {
			t.Errorf("error %q exposes %q", err.Error(), leaked)
		}
	}
}

func TestTransformWithContext_ContextCancellationAndTimeout(t *testing.T) {
	tests := []struct {
		name string
		ctx  func(t *testing.T) context.Context
		want error
	}{
		{
			name: "cancellation",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			want: context.Canceled,
		},
		{
			name: "timeout",
			ctx: func(*testing.T) context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
			want: context.DeadlineExceeded,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}))
			payload := []byte(fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(time.Now().Add(time.Hour))))

			_, _, handled, err := p.TransformWithContext(tc.ctx(t), payload, nil, "https://gateway.example/v1/audio/transcriptions")
			if handled {
				t.Error("handled = true, want false")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestTransformWithContext_SanitizesFilename(t *testing.T) {
	signedURL := strings.Replace(v4SignedURL(time.Now().Add(time.Hour)), "/bucket/audio.wav", "/bucket/audio%20name%22.wav", 1)
	p := newPlugin(t, `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("audio"))}, nil
	}))
	payload := []byte(fmt.Sprintf(`{"gcs_uri":%q}`, signedURL))

	body, contentType, handled, err := p.TransformWithContext(context.Background(), payload, nil, "https://gateway.example/v1/audio/transcriptions")
	if err != nil || !handled {
		t.Fatalf("TransformWithContext = handled %v, err %v", handled, err)
	}
	_, _, filenames := parseParts(t, contentType, body)
	if filenames["file"] != "audio_name_.wav" {
		t.Errorf("filename = %q, want audio_name_.wav", filenames["file"])
	}
}

func TestNew_Parameters(t *testing.T) {
	tests := []struct {
		name      string
		params    string
		wantError bool
	}{
		{"array and integer", `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024}`, false},
		{"encoded array and string", `{"allowedHosts":"[\"storage.googleapis.com\"]","maxFileBytes":"1024"}`, false},
		{"missing allowed hosts", `{"maxFileBytes":1024}`, true},
		{"empty allowed hosts", `{"allowedHosts":"[]","maxFileBytes":1024}`, true},
		{"missing max bytes", `{"allowedHosts":["storage.googleapis.com"]}`, true},
		{"zero max bytes", `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":0}`, true},
		{"invalid host", `{"allowedHosts":["https://storage.googleapis.com"],"maxFileBytes":1024}`, true},
		{"unknown field", `{"allowedHosts":["storage.googleapis.com"],"maxFileBytes":1024,"foo":1}`, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New("test", json.RawMessage(tc.params), plugins.NewHandle(context.Background()))
			if tc.wantError && err == nil {
				t.Error("New = nil error, want error")
			}
			if !tc.wantError && err != nil {
				t.Errorf("New = %v, want nil", err)
			}
		})
	}
}
