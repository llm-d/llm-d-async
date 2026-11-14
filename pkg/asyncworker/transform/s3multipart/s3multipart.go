// Package s3multipart provides a RequestTransform plugin that turns OpenAI-style
// JSON media requests into the multipart/form-data bodies their inference
// endpoints require, resolving media from S3-compatible object storage.
//
// Batch lines are JSON, but vLLM's /v1/audio/transcriptions and
// /v1/audio/translations accept only multipart uploads. For those endpoints the
// plugin reads the `file` field, an s3:// reference or a base64 data URL, and
// sends its bytes as the multipart file part; the remaining fields become form
// fields.
//
// The plugin picks requests by the path of the URL they are dispatched to and
// leaves every other request untouched. s3:// references are only resolved for
// buckets in the configured allowlist. Storage credentials come from the AWS SDK
// default chain (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, shared config files,
// or a workload identity), never from the plugin parameters.
package s3multipart

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d/llm-d-async/pkg/plugins"
)

// PluginType is the registry key under which this plugin's factory is registered.
const PluginType = "s3_multipart"

// defaultMaxFileBytes matches vLLM's default upload limit
// (VLLM_MAX_AUDIO_CLIP_FILESIZE_MB=25, in MiB).
const defaultMaxFileBytes = 25 << 20

const s3Scheme = "s3://"

var (
	_ transform.RequestTransform = (*Plugin)(nil)
	_ transform.ContextTransform = (*Plugin)(nil)
)

// params is the plugin's configuration, parsed from the factory parameters.
// The fields are flat so they can be set from a string map, as the batch
// gateway operator does.
type params struct {
	// Endpoint is the S3 API URL of a non-AWS store (e.g. SeaweedFS). Empty
	// means AWS S3.
	Endpoint string `json:"endpoint"`
	// Region is the signing region. Empty falls back to AWS_REGION or the
	// shared config.
	Region string `json:"region"`
	// UsePathStyle addresses objects as <endpoint>/<bucket>/<key>, which most
	// S3-compatible stores require.
	UsePathStyle bool `json:"usePathStyle"`
	// AllowedBuckets lists the only buckets s3:// references may name.
	AllowedBuckets []string `json:"allowedBuckets"`
	// MaxFileBytes caps the size of a media file read into a request body.
	// Zero means defaultMaxFileBytes.
	MaxFileBytes int64 `json:"maxFileBytes"`
}

// Plugin rewrites JSON media requests into multipart/form-data.
type Plugin struct {
	tn           plugins.TypedName
	client       *s3.Client
	buckets      map[string]struct{}
	maxFileBytes int64
}

func init() {
	plugins.MustRegister(PluginType, New)
}

// New is the FactoryFunc for the plugin. It validates the parameters and builds
// the S3 client; it does not contact storage.
func New(name string, parameters json.RawMessage, handle plugins.Handle) (plugins.Plugin, error) {
	var p params
	if len(parameters) > 0 {
		dec := json.NewDecoder(bytes.NewReader(parameters))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("invalid parameters: %w", err)
		}
	}
	ctx := context.Background()
	if handle != nil {
		ctx = handle.Context()
	}
	return newPlugin(ctx, name, p)
}

// newPlugin builds a Plugin from parsed parameters. optFns are applied to the
// S3 client options after the configured ones.
func newPlugin(ctx context.Context, name string, p params, optFns ...func(*s3.Options)) (*Plugin, error) {
	if len(p.AllowedBuckets) == 0 {
		return nil, fmt.Errorf("at least one entry in %q is required", "allowedBuckets")
	}
	buckets := make(map[string]struct{}, len(p.AllowedBuckets))
	for _, b := range p.AllowedBuckets {
		if b == "" {
			return nil, fmt.Errorf("%q entries must be non-empty", "allowedBuckets")
		}
		buckets[b] = struct{}{}
	}
	if p.MaxFileBytes < 0 {
		return nil, fmt.Errorf("%q must not be negative", "maxFileBytes")
	}
	maxFileBytes := p.MaxFileBytes
	if maxFileBytes == 0 {
		maxFileBytes = defaultMaxFileBytes
	}
	if p.Endpoint != "" {
		u, err := url.Parse(p.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("%q must be an http(s) URL, got %q", "endpoint", p.Endpoint)
		}
	}

	var loadOpts []func(*config.LoadOptions) error
	if p.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(p.Region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("%q is required (or set AWS_REGION)", "region")
	}
	opts := []func(*s3.Options){func(o *s3.Options) {
		if p.Endpoint != "" {
			o.BaseEndpoint = aws.String(p.Endpoint)
		}
		o.UsePathStyle = p.UsePathStyle
		// S3-compatible stores often return no checksum headers; without this
		// the SDK logs a line on every GetObject.
		o.DisableLogOutputChecksumValidationSkipped = true
	}}

	return &Plugin{
		tn:           plugins.TypedName{Type: PluginType, Name: name},
		client:       s3.NewFromConfig(cfg, append(opts, optFns...)...),
		buckets:      buckets,
		maxFileBytes: maxFileBytes,
	}, nil
}

// TypedName returns the plugin's type and instance name.
func (p *Plugin) TypedName() plugins.TypedName { return p.tn }

// Validate accepts every message: the checks need the dispatch URL, which only
// TransformContext receives.
func (p *Plugin) Validate(_ []byte, _ map[string]string, _ int64) error { return nil }

// Transform is the context-free entry point. Without the dispatch URL the
// plugin cannot tell which endpoint a request targets, so it handles nothing.
func (p *Plugin) Transform(payload []byte, metadata map[string]string) ([]byte, string, bool, error) {
	return p.TransformContext(context.Background(), payload, metadata, "")
}

// TransformContext rewrites requests whose dispatch URL path ends in a
// supported endpoint and returns handled=false for every other request.
func (p *Plugin) TransformContext(ctx context.Context, payload []byte, _ map[string]string, requestURL string) ([]byte, string, bool, error) {
	u, err := url.Parse(requestURL)
	if err != nil {
		return nil, "", false, nil
	}
	switch {
	case strings.HasSuffix(u.Path, "/v1/audio/transcriptions"), strings.HasSuffix(u.Path, "/v1/audio/translations"):
		return p.transformAudio(ctx, payload)
	default:
		return nil, "", false, nil
	}
}

// parseS3URI splits an s3://bucket/key reference and checks the bucket against
// the allowlist.
func (p *Plugin) parseS3URI(ref string) (bucket, key string, err error) {
	bucket, key, ok := strings.Cut(strings.TrimPrefix(ref, s3Scheme), "/")
	if !ok || bucket == "" || key == "" {
		return "", "", fatalf("%q is not an s3://bucket/key reference", ref)
	}
	if _, ok := p.buckets[bucket]; !ok {
		return "", "", fatalf("bucket %q is not in allowedBuckets", bucket)
	}
	return bucket, key, nil
}

// getObject reads an object of at most maxFileBytes and returns its bytes and
// stored Content-Type.
func (p *Plugin) getObject(ctx context.Context, bucket, key string) ([]byte, string, error) {
	ref := s3Scheme + bucket + "/" + key
	out, err := p.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, "", storageError(ctx, ref, err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(out.Body, p.maxFileBytes+1))
	if err != nil {
		return nil, "", storageError(ctx, ref, err)
	}
	if int64(len(data)) > p.maxFileBytes {
		return nil, "", fatalf("%s is over the %d-byte limit", ref, p.maxFileBytes)
	}
	return data, aws.ToString(out.ContentType), nil
}

// storageError classifies a failed storage call so the worker retries what can
// succeed later and fails the rest:
//   - the request context is done: the error is returned without a category,
//     and the worker reports shutdown or the request deadline from its own
//     context. A network or SDK timeout while the request still has time
//     left is not this case.
//   - an error the AWS SDK itself would throttle: RATE_LIMIT (retried).
//   - an error the AWS SDK itself would retry (5xx, RequestTimeout,
//     connection failures, timeouts), or a body cut off mid-read:
//     SERVER_ERROR (retried).
//   - HTTP 401/403: AUTH_ERROR; anything else (NoSuchKey, missing
//     credentials, invalid input): INVALID_REQ. None of these is retried.
func storageError(ctx context.Context, ref string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("read %s: %w", ref, err)
	}
	category := asyncapi.ErrCategoryInvalidReq
	var resp interface{ HTTPStatusCode() int }
	switch {
	case retry.IsErrorThrottles(retry.DefaultThrottles).IsErrorThrottle(err).Bool():
		category = asyncapi.ErrCategoryRateLimit
	case retry.IsErrorRetryables(retry.DefaultRetryables).IsErrorRetryable(err).Bool(), errors.Is(err, io.ErrUnexpectedEOF):
		category = asyncapi.ErrCategoryServer
	case errors.As(err, &resp) && (resp.HTTPStatusCode() == 401 || resp.HTTPStatusCode() == 403):
		category = asyncapi.ErrCategoryAuth
	}
	return &asyncapi.ClientError{ErrorCategory: category, Message: "read " + ref, RawError: err}
}

// decodeFields parses the JSON body into its top-level fields, preserving each
// value's raw JSON.
func decodeFields(payload []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, fatalf("failed to parse request body: %v", err)
	}
	return fields, nil
}

// stringField returns the value of a JSON string field, or ok=false if the
// field is absent or not a string.
func stringField(fields map[string]json.RawMessage, name string) (string, bool) {
	raw, ok := fields[name]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// writeFormFields writes every field not in skip as a form field, in name order
// so bodies are reproducible. JSON strings are unquoted; arrays become repeated
// `name[]` fields, the encoding vLLM's form models expect (e.g.
// timestamp_granularities[]); nulls are omitted; other values (numbers,
// booleans, objects such as vllm_xargs) are sent as their JSON text.
func writeFormFields(w *multipart.Writer, fields map[string]json.RawMessage, skip string) error {
	names := make([]string, 0, len(fields))
	for name := range fields {
		if name != skip {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		raw := fields[name]
		// null decodes to an empty slice, so null fields write nothing.
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) == nil {
			for _, item := range items {
				if err := w.WriteField(name+"[]", formValue(item)); err != nil {
					return fmt.Errorf("write %q: %w", name+"[]", err)
				}
			}
			continue
		}
		if err := w.WriteField(name, formValue(raw)); err != nil {
			return fmt.Errorf("write %q: %w", name, err)
		}
	}
	return nil
}

// formValue renders a raw JSON value as a form field value: strings are
// unquoted, everything else is its JSON text.
func formValue(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// fatalf builds a non-retryable invalid-request error.
func fatalf(format string, args ...any) error {
	return &asyncapi.ClientError{
		ErrorCategory: asyncapi.ErrCategoryInvalidReq,
		Message:       fmt.Sprintf(format, args...),
	}
}
