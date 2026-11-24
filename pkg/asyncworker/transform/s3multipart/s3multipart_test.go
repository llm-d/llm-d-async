package s3multipart

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d/llm-d-async/pkg/plugins"
)

// object is what the fake store serves for a key; status, when set, makes it
// answer with that S3 error instead.
type object struct {
	body        []byte
	contentType string
	status      int
	code        string
	chunked     bool // omit Content-Length
	block       bool // hang until the client gives up
	// declaredLength, when set, is the Content-Length announced before writing
	// a shorter body, so the connection ends mid-read.
	declaredLength int
}

// fakeS3 is a path-style S3 endpoint serving GetObject for a fixed set of
// objects keyed by "/bucket/key".
type fakeS3 struct {
	*httptest.Server
	objects  map[string]object
	requests atomic.Int32
	lastAuth atomic.Value
}

func newFakeS3(t testing.TB, objects map[string]object) *fakeS3 {
	t.Helper()
	f := &fakeS3{objects: objects}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)
		f.lastAuth.Store(r.Header.Get("Authorization"))
		obj, ok := f.objects[r.URL.Path]
		switch {
		case !ok:
			obj = object{status: http.StatusNotFound, code: "NoSuchKey"}
		case obj.block:
			<-r.Context().Done()
			return
		}
		if obj.status != 0 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(obj.status)
			_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>test</Message></Error>`, obj.code)
			return
		}
		if obj.contentType != "" {
			w.Header().Set("Content-Type", obj.contentType)
		}
		if obj.declaredLength != 0 {
			w.Header().Set("Content-Length", fmt.Sprint(obj.declaredLength))
		}
		if obj.chunked {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
		}
		_, _ = w.Write(obj.body)
	}))
	t.Cleanup(f.Close)
	return f
}

// isolateAWSEnv points the SDK's default chain at test-only static credentials
// so tests never read the developer's AWS config or reach a metadata service.
func isolateAWSEnv(t testing.TB) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	// Credential sources the default chain would try before IMDS, so ambient
	// CI credentials cannot answer for a test.
	for _, name := range []string{"AWS_SESSION_TOKEN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN"} {
		t.Setenv(name, "")
	}
}

// newTestPlugin builds a plugin against the fake store, with SDK retries off so
// error tests see the first response. optFns adjust the S3 client further.
func newTestPlugin(t testing.TB, endpoint string, maxFileBytes int64, optFns ...func(*s3.Options)) *Plugin {
	t.Helper()
	isolateAWSEnv(t)
	p, err := newPlugin(context.Background(), "test", params{
		Endpoint:       endpoint,
		Region:         "us-east-1",
		UsePathStyle:   true,
		AllowedBuckets: []string{"audio"},
		MaxFileBytes:   maxFileBytes,
	}, append([]func(*s3.Options){func(o *s3.Options) { o.Retryer = aws.NopRetryer{} }}, optFns...)...)
	if err != nil {
		t.Fatalf("newPlugin: %v", err)
	}
	return p
}

type part struct {
	values      []string
	filename    string
	contentType string
	header      textproto.MIMEHeader
}

// parseParts parses a multipart body into its parts by form name.
func parseParts(t *testing.T, contentType string, body []byte) map[string]*part {
	t.Helper()
	mediaType, ps, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "multipart/form-data" || ps["boundary"] == "" {
		t.Fatalf("Content-Type = %q, want multipart/form-data with a boundary", contentType)
	}
	parts := map[string]*part{}
	r := multipart.NewReader(bytes.NewReader(body), ps["boundary"])
	for {
		p, err := r.NextPart()
		if errors.Is(err, io.EOF) {
			return parts
		}
		if err != nil {
			t.Fatalf("NextPart: %v", err)
		}
		b, err := io.ReadAll(p)
		if err != nil {
			t.Fatalf("read part %q: %v", p.FormName(), err)
		}
		got := parts[p.FormName()]
		if got == nil {
			got = &part{filename: p.FileName(), contentType: p.Header.Get("Content-Type"), header: p.Header}
			parts[p.FormName()] = got
		}
		got.values = append(got.values, string(b))
	}
}

func TestNew_ParsesOperatorStyleParameters(t *testing.T) {
	isolateAWSEnv(t)
	factory, ok := plugins.Lookup(PluginType)
	if !ok {
		t.Fatalf("plugin type %q is not registered", PluginType)
	}
	// The batch gateway operator renders string parameters, JSON-decoding
	// non-string values, so the plugin receives these flat typed fields.
	raw := `{"endpoint":"http://seaweedfs.batch.svc:8333","region":"us-east-1","usePathStyle":true,"allowedBuckets":["audio","img"],"maxFileBytes":1048576}`
	pl, err := factory("media", json.RawMessage(raw), plugins.NewHandle(context.Background()))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	p := pl.(*Plugin)
	if p.maxFileBytes != 1048576 || len(p.buckets) != 2 {
		t.Errorf("maxFileBytes=%d buckets=%v, want 1048576 and 2 buckets", p.maxFileBytes, p.buckets)
	}
	if got := p.TypedName(); got.Type != PluginType || got.Name != "media" {
		t.Errorf("TypedName = %+v", got)
	}
}

func TestNew_RejectsInvalidParameters(t *testing.T) {
	tests := map[string]string{
		"no buckets":   `{"region":"us-east-1"}`,
		"empty bucket": `{"region":"us-east-1","allowedBuckets":[""]}`,
		"negative max": `{"region":"us-east-1","allowedBuckets":["a"],"maxFileBytes":-1}`,
		// max+1 would wrap negative and read every file as empty.
		"max overflows":      `{"region":"us-east-1","allowedBuckets":["a"],"maxFileBytes":9223372036854775807}`,
		"endpoint no scheme": `{"region":"us-east-1","allowedBuckets":["a"],"endpoint":"seaweedfs:8333"}`,
		"endpoint not http":  `{"region":"us-east-1","allowedBuckets":["a"],"endpoint":"ftp://seaweedfs:8333"}`,
		"endpoint no host":   `{"region":"us-east-1","allowedBuckets":["a"],"endpoint":"http://"}`,
		"unknown field":      `{"region":"us-east-1","allowedBuckets":["a"],"bucket":"a"}`,
		"no region":          `{"allowedBuckets":["a"]}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			isolateAWSEnv(t)
			if _, err := New("x", json.RawMessage(raw), nil); err == nil {
				t.Fatalf("New(%s) succeeded, want an error", raw)
			}
		})
	}
}

func TestNew_RegionFromEnvironment(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "eu-west-1")
	if _, err := New("x", json.RawMessage(`{"allowedBuckets":["a"]}`), nil); err != nil {
		t.Fatalf("New with AWS_REGION set: %v", err)
	}
}

func TestTransform_LeavesOtherRequestsAlone(t *testing.T) {
	store := newFakeS3(t, nil)
	p := newTestPlugin(t, store.URL, 0)
	payload := []byte(`{"model":"whisper","file":"s3://audio/a.wav"}`)

	for _, u := range []string{"http://gw/v1/chat/completions", "http://gw/v1/audio/speech", "http://gw/v1/images/generations", "", "::bad"} {
		_, _, handled, err := p.TransformContext(context.Background(), payload, nil, u)
		if handled || err != nil {
			t.Errorf("TransformContext(%q) = handled %v err %v, want false/nil", u, handled, err)
		}
	}
	// Without a dispatch URL the context-free entry point cannot match either.
	if _, _, handled, err := p.Transform(payload, nil); handled || err != nil {
		t.Errorf("Transform = handled %v err %v, want false/nil", handled, err)
	}
	if n := store.requests.Load(); n != 0 {
		t.Errorf("storage requests = %d, want 0", n)
	}
}

// nextTransform stands for the transform after this plugin in a chain.
type nextTransform struct{ calls int }

func (n *nextTransform) TypedName() plugins.TypedName {
	return plugins.TypedName{Type: "next", Name: "next"}
}
func (n *nextTransform) Validate([]byte, map[string]string, int64) error { return nil }
func (n *nextTransform) Transform([]byte, map[string]string) ([]byte, string, bool, error) {
	n.calls++
	return []byte("next"), "text/plain", true, nil
}

// TestTransform_LeavesRequestsWithoutItsFieldToTheNextTransform checks that on
// its own paths the plugin only claims a body carrying the field it reads, so
// a request meant for another transform (here a gcs_uri one) reaches it.
func TestTransform_LeavesRequestsWithoutItsFieldToTheNextTransform(t *testing.T) {
	store := newFakeS3(t, nil)
	p := newTestPlugin(t, store.URL, 0)
	cases := map[string]struct{ url, payload string }{
		"transcription without file": {transcriptionsURL, `{"model":"whisper","gcs_uri":"gs://b/calls/42.wav"}`},
		"image edit without images":  {editsURL, `{"prompt":"x","gcs_uri":"gs://b/in.png"}`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, handled, err := p.TransformContext(context.Background(), []byte(tc.payload), nil, tc.url); handled || err != nil {
				t.Fatalf("plugin alone: handled %v err %v, want false/nil", handled, err)
			}
			next := &nextTransform{}
			chain := transform.NewChain([]transform.RequestTransform{p, next})
			body, _, handled, err := chain.ApplyContext(context.Background(), []byte(tc.payload), nil, tc.url)
			if err != nil || !handled || string(body) != "next" || next.calls != 1 {
				t.Errorf("chain: body %q handled %v err %v next calls %d, want the next transform's body", body, handled, err, next.calls)
			}
		})
	}
	if n := store.requests.Load(); n != 0 {
		t.Errorf("storage requests = %d, want 0", n)
	}
}

// assertCategory checks that err carries an InferenceError of the given
// category, which decides whether the worker retries.
func assertCategory(t *testing.T, err error, want asyncapi.ErrorCategory) {
	t.Helper()
	var ierr asyncapi.InferenceError
	if !errors.As(err, &ierr) {
		t.Fatalf("err = %v, want an InferenceError with category %s", err, want)
	}
	if got := ierr.Category(); got != want {
		t.Errorf("category = %s, want %s (err: %v)", got, want, err)
	}
}

func keys(m map[string]*part) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
