package s3multipart

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	asyncapi "github.com/llm-d/llm-d-async/api"
)

const transcriptionsURL = "http://gw.example/prefix/v1/audio/transcriptions"

// BenchmarkTransformAudio reports what one transform allocates for a file at
// the default size limit, read from S3 and decoded from a data URL. Nothing in
// the code calls it: `go test` runs Benchmark* functions only with -bench, e.g.
//
//	go test -run '^$' -bench BenchmarkTransformAudio -benchmem ./pkg/asyncworker/transform/s3multipart/
//
// B/op is cumulative allocation, not peak memory.
func BenchmarkTransformAudio(b *testing.B) {
	// 25 MiB of one arbitrary non-ASCII byte (0xA5 = 10100101) standing in for
	// audio; the plugin never looks at the content.
	media := bytes.Repeat([]byte{0xA5}, defaultMaxFileBytes)
	// S3 declares the object's length, which the plugin sizes the body from.
	store := newFakeS3(b, map[string]object{"/audio/big.wav": {body: media, contentType: "audio/wav", declaredLength: len(media)}})
	p := newTestPlugin(b, store.URL, 0)
	payloads := []struct{ name, payload string }{
		{"s3", `{"model":"whisper","file":"s3://audio/big.wav"}`},
		{"data", `{"model":"whisper","file":"data:audio/wav;base64,` + base64.StdEncoding.EncodeToString(media) + `"}`},
	}
	for _, tc := range payloads {
		payload := []byte(tc.payload)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := p.TransformContext(context.Background(), payload, nil, transcriptionsURL); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

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

	for _, endpoint := range []string{transcriptionsURL, "http://gw/v1/audio/translations", transcriptionsURL + "?api-version=1"} {
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
		// Part.FileName() applies filepath.Base, so check the header as sent.
		if cd := file.header.Get("Content-Disposition"); cd != `form-data; filename=42.wav; name=file` {
			t.Errorf("file part Content-Disposition = %q, want only the key's base name", cd)
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

// TestTransform_AudioDataURLDecodesLikeOneDecode checks that the streamed
// decode accepts and produces exactly what a single base64 decode does. The
// streaming decoder works in 1 KiB chunks and on its own would accept padding
// that ends a chunk in the middle of the data.
func TestTransform_AudioDataURLDecodesLikeOneDecode(t *testing.T) {
	p := newTestPlugin(t, newFakeS3(t, nil).URL, 0)
	cases := []string{
		"UklGRg==", "QUJD", "", "QU\nJD", "QUJD\r\n",
		"YQ", "YQ=", "YQ===", "YQ==Yg==", "!!!!", "QUJ=D",
		strings.Repeat("A", 1020) + "YQ==" + "Yg==",
		strings.Repeat("A", 1020) + "YQ==",
	}
	for _, encoded := range cases {
		name := encoded
		if len(name) > 20 {
			name = fmt.Sprintf("%d bytes ending %q", len(encoded), encoded[len(encoded)-8:])
		}
		t.Run(name, func(t *testing.T) {
			want, wantErr := base64.StdEncoding.DecodeString(encoded)
			payload, _ := json.Marshal(map[string]string{"file": "data:audio/wav;base64," + encoded})
			body, contentType, _, err := p.TransformContext(context.Background(), payload, nil, transcriptionsURL)
			if (err != nil) != (wantErr != nil) {
				t.Fatalf("transform err = %v, single decode err = %v", err, wantErr)
			}
			if err != nil {
				assertCategory(t, err, asyncapi.ErrCategoryInvalidReq)
				return
			}
			if file := parseParts(t, contentType, body)["file"]; file == nil || file.values[0] != string(want) {
				t.Errorf("file part = %+v, want %q", file, want)
			}
		})
	}
}

func TestTransform_AudioRejectsBadInput(t *testing.T) {
	store := newFakeS3(t, map[string]object{
		// One byte over the 32-byte limit.
		"/audio/big.wav":         {body: bytes.Repeat([]byte("x"), 33)},
		"/audio/big-chunked.wav": {body: bytes.Repeat([]byte("x"), 33), chunked: true},
		// Declares more than the limit, then sends less: only the declared
		// length shows the object is too big, before the body is read.
		"/audio/big-declared.wav": {body: []byte("x"), declaredLength: 33},
	})
	p := newTestPlugin(t, store.URL, 32)
	tests := map[string]string{
		"not json": `not json`,
		// Present but not a string: claimed, and rejected.
		"file not a string":  `{"file":42}`,
		"file null":          `{"file":null}`,
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
		"declared too big":          `{"file":"s3://audio/big-declared.wav"}`,
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
	// Only the three size checks needed to read from storage.
	if n := store.requests.Load(); n != 3 {
		t.Errorf("storage requests = %d, want 3", n)
	}
}

func TestTransform_AudioAtTheSizeLimit(t *testing.T) {
	limit := bytes.Repeat([]byte("x"), 32)
	store := newFakeS3(t, map[string]object{
		"/audio/max.wav":         {body: limit},
		"/audio/max-chunked.wav": {body: limit, chunked: true},
	})
	p := newTestPlugin(t, store.URL, 32)
	for _, file := range []string{"s3://audio/max.wav", "s3://audio/max-chunked.wav", "data:audio/wav;base64," + base64.StdEncoding.EncodeToString(limit)} {
		payload, _ := json.Marshal(map[string]string{"file": file})
		body, contentType, handled, err := p.TransformContext(context.Background(), payload, nil, transcriptionsURL)
		if err != nil || !handled {
			t.Fatalf("%.30s: handled %v err %v, want a file of exactly the limit accepted", file, handled, err)
		}
		if got := parseParts(t, contentType, body)["file"]; got == nil || got.values[0] != string(limit) {
			t.Errorf("%.30s: file part = %+v, want the 32 bytes", file, got)
		}
	}
}

// TestTransform_AudioFilenameIsASCII checks that a non-ASCII object name is
// sent as a plain ASCII filename: Go would otherwise write only RFC 2231
// filename*, which python-multipart ignores, and vLLM would read the part as
// a text field instead of an upload.
func TestTransform_AudioFilenameIsASCII(t *testing.T) {
	store := newFakeS3(t, map[string]object{
		"/audio/録音.wav": {body: []byte("a")},
		"/audio/é.wav":  {body: []byte("a")},
	})
	p := newTestPlugin(t, store.URL, 0)
	for key, want := range map[string]string{"録音.wav": "__.wav", "é.wav": "_.wav"} {
		body, contentType, handled, err := p.TransformContext(context.Background(), []byte(`{"file":"s3://audio/`+key+`"}`), nil, transcriptionsURL)
		if err != nil || !handled {
			t.Fatalf("%s: handled %v err %v, want true/nil", key, handled, err)
		}
		if cd := parseParts(t, contentType, body)["file"].header.Get("Content-Disposition"); cd != "form-data; filename="+want+"; name=file" {
			t.Errorf("%s: Content-Disposition = %q, want a plain filename=%s", key, cd, want)
		}
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
	if file.filename != "a__X-Injected: yes.wav" {
		t.Errorf("filename = %q, want the key's base name with CR/LF replaced", file.filename)
	}
}

func TestTransform_AudioStorageErrors(t *testing.T) {
	store := newFakeS3(t, map[string]object{
		"/audio/denied.wav":    {status: http.StatusForbidden, code: "AccessDenied"},
		"/audio/badkey.wav":    {status: http.StatusUnauthorized, code: "InvalidAccessKeyId"},
		"/audio/throttled.wav": {status: http.StatusTooManyRequests, code: "SlowDown"},
		"/audio/down.wav":      {status: http.StatusServiceUnavailable, code: "ServiceUnavailable"},
		// S3 reports an idle client socket as a 400 the SDK itself retries.
		"/audio/timeout.wav":   {status: http.StatusBadRequest, code: "RequestTimeout"},
		"/audio/truncated.wav": {body: []byte("short"), declaredLength: 100},
	})
	p := newTestPlugin(t, store.URL, 0)
	// The message carries the service's error code but never its error text
	// (the fake store answers "test"), which reaches results, spans and logs.
	tests := map[string]struct {
		category asyncapi.ErrorCategory
		code     string
	}{
		"missing.wav":   {asyncapi.ErrCategoryInvalidReq, ": NoSuchKey"},
		"denied.wav":    {asyncapi.ErrCategoryAuth, ": AccessDenied"},
		"badkey.wav":    {asyncapi.ErrCategoryAuth, ": InvalidAccessKeyId"},
		"throttled.wav": {asyncapi.ErrCategoryRateLimit, ": SlowDown"},
		"down.wav":      {asyncapi.ErrCategoryServer, ": ServiceUnavailable"},
		"timeout.wav":   {asyncapi.ErrCategoryServer, ": RequestTimeout"},
		"truncated.wav": {asyncapi.ErrCategoryServer, ""}, // body cut off mid-read
	}
	for key, want := range tests {
		t.Run(key, func(t *testing.T) {
			payload := fmt.Appendf(nil, `{"file":"s3://audio/%s"}`, key)
			_, _, _, err := p.TransformContext(context.Background(), payload, nil, transcriptionsURL)
			assertCategory(t, err, want.category)
			var cerr *asyncapi.ClientError
			if !errors.As(err, &cerr) {
				t.Fatalf("err = %v, want a ClientError", err)
			}
			if cerr.Message != "read s3://audio/"+key+want.code {
				t.Errorf("message = %q, want %q", cerr.Message, "read s3://audio/"+key+want.code)
			}
			// The SDK error stays available to errors.Is/As, just not in the message.
			if cerr.RawError == nil {
				t.Error("RawError = nil, want the storage error kept")
			}
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

	// A plugin whose default chain finds no credentials.
	noCredentialsPlugin := func(t *testing.T, env map[string]string) *Plugin {
		t.Helper()
		isolateAWSEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "")
		for k, v := range env {
			t.Setenv(k, v)
		}
		p, err := newPlugin(context.Background(), "nocreds", params{Endpoint: store.URL, Region: "us-east-1", UsePathStyle: true, AllowedBuckets: []string{"audio"}},
			func(o *s3.Options) { o.Retryer = aws.NopRetryer{} })
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	// Missing credentials fail the request instead of retrying it until its
	// deadline, as on a non-AWS cluster whose key Secret is missing.
	noCredentials := func(t *testing.T, env map[string]string) error {
		t.Helper()
		p := noCredentialsPlugin(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _, _, err := p.TransformContext(ctx, []byte(`{"file":"s3://audio/down.wav"}`), nil, transcriptionsURL)
		var cerr *asyncapi.ClientError
		if !errors.As(err, &cerr) || cerr.Message != "get credentials for s3://audio/down.wav" {
			t.Errorf("err = %v, want a credentials error for s3://audio/down.wav", err)
		}
		if n := store.requests.Load(); n != 0 {
			t.Errorf("storage requests = %d, want 0 without credentials", n)
		}
		return err
	}
	// The SDK reads AWS_EC2_METADATA_DISABLED itself; the plugin must fail the
	// same way with either setting.
	for name, env := range map[string]map[string]string{
		// The SDK gives up on the metadata service without calling it.
		"no credentials, metadata service disabled": {"AWS_EC2_METADATA_DISABLED": "true"},
		// The SDK tries a metadata service that refuses connections, as off
		// AWS: a network error that storage reads would retry.
		"no credentials, metadata service unreachable": {"AWS_EC2_METADATA_DISABLED": "false", "AWS_EC2_METADATA_SERVICE_ENDPOINT": "http://127.0.0.1:1"},
	} {
		t.Run(name, func(t *testing.T) {
			store.requests.Store(0)
			assertCategory(t, noCredentials(t, env), asyncapi.ErrCategoryAuth)
		})
	}
	// Once the request context is done the worker reports shutdown or the
	// deadline from its own context, so the failure must carry no category.
	t.Run("no credentials, request context done", func(t *testing.T) {
		p := noCredentialsPlugin(t, map[string]string{"AWS_EC2_METADATA_DISABLED": "true"})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, _, err := p.TransformContext(ctx, []byte(`{"file":"s3://audio/down.wav"}`), nil, transcriptionsURL)
		var cerr *asyncapi.ClientError
		if err == nil || errors.As(err, &cerr) {
			t.Errorf("err = %v, want an error without a category", err)
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

// closeCounter is an S3 HTTP client that counts response bodies opened and
// closed, so a test can tell when an object's connection is left open.
type closeCounter struct{ opened, closed atomic.Int32 }

func (c *closeCounter) Do(r *http.Request) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(r)
	if err == nil {
		c.opened.Add(1)
		resp.Body = countedBody{resp.Body, c}
	}
	return resp, err
}

type countedBody struct {
	io.ReadCloser
	c *closeCounter
}

func (b countedBody) Close() error {
	b.c.closed.Add(1)
	return b.ReadCloser.Close()
}

func TestTransform_AudioClosesTheObjectBody(t *testing.T) {
	store := newFakeS3(t, map[string]object{
		"/audio/ok.wav":           {body: []byte("audio")},
		"/audio/big-chunked.wav":  {body: bytes.Repeat([]byte("x"), 33), chunked: true},
		"/audio/big-declared.wav": {body: []byte("x"), declaredLength: 33},
		"/audio/truncated.wav":    {body: []byte("short"), declaredLength: 30},
	})
	for _, key := range []string{"ok.wav", "big-chunked.wav", "big-declared.wav", "truncated.wav"} {
		t.Run(key, func(t *testing.T) {
			c := &closeCounter{}
			p := newTestPlugin(t, store.URL, 32, func(o *s3.Options) { o.HTTPClient = c })
			_, _, _, _ = p.TransformContext(context.Background(), []byte(`{"file":"s3://audio/`+key+`"}`), nil, transcriptionsURL)
			if opened, closed := c.opened.Load(), c.closed.Load(); opened != 1 || closed != opened {
				t.Errorf("response bodies opened %d, closed %d, want 1 and 1", opened, closed)
			}
		})
	}
}
