// Package gcsfilemultipart downloads a signed GCS audio object and rewrites the
// request into a multipart/form-data file upload.
package gcsfilemultipart

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d/llm-d-async/pkg/plugins"
	"github.com/llm-d/llm-d-async/pkg/util"
)

const PluginType = "gcs_file_multipart"

const (
	gcsURIField              = "gcs_uri"
	fileField                = "file"
	fileBase64Field          = "file_base64"
	gcsV4RSASHA256           = "GOOG4-RSA-SHA256"
	gcsV4HMACSHA256          = "GOOG4-HMAC-SHA256"
	hmacSHA256SignatureBytes = 32
	virtualHostedSuffix      = ".storage.googleapis.com"
)

var (
	_ transform.RequestTransform = (*Plugin)(nil)
	_ transform.ContextTransform = (*Plugin)(nil)

	errUnsupportedRedirect = errors.New("redirect target is not an allowed GCS host")
)

type params struct {
	Providers    []string        `json:"providers"`
	AllowedHosts json.RawMessage `json:"allowedHosts"`
	MaxFileBytes json.RawMessage `json:"maxFileBytes"`
}

// Plugin downloads an audio object with its signed URL and builds the multipart
// body expected by OpenAI-compatible audio endpoints.
type Plugin struct {
	tn           plugins.TypedName
	providers    map[string]struct{}
	allowedHosts map[string]struct{}
	maxFileBytes int64
	client       *http.Client
}

func init() {
	plugins.MustRegister(PluginType, New)
}

// New creates a gcs_file_multipart plugin. The HTTP client has no GCS credentials;
// the signed URL provides temporary read access.
func New(name string, parameters json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	var p params
	if len(parameters) > 0 {
		if err := util.DecodeJSONDisallowUnknownFields(parameters, &p); err != nil {
			return nil, fmt.Errorf("invalid parameters: %w", err)
		}
	}
	providers, err := util.NewProviderSet(p.Providers)
	if err != nil {
		return nil, fmt.Errorf("invalid parameters: %w", err)
	}

	maxFileBytes, err := parseMaxFileBytes(p.MaxFileBytes)
	if err != nil {
		return nil, err
	}
	hosts, err := parseAllowedHosts(p.AllowedHosts)
	if err != nil {
		return nil, err
	}
	allowedHosts := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		allowedHosts[host] = struct{}{}
	}

	plugin := &Plugin{
		tn:           plugins.TypedName{Type: PluginType, Name: name},
		providers:    providers,
		allowedHosts: allowedHosts,
		maxFileBytes: maxFileBytes,
	}
	plugin.client = &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("too many redirects")
			}
			if !util.IsAbsoluteHTTPSURL(req.URL) {
				return errUnsupportedRedirect
			}
			if err := plugin.allowedURL(req.URL); err != nil {
				return errUnsupportedRedirect
			}
			return nil
		},
	}
	return plugin, nil
}

// TypedName returns the plugin's type and instance name.
func (p *Plugin) TypedName() plugins.TypedName { return p.tn }

func (p *Plugin) owns(metadata map[string]string) bool {
	_, ok := p.providers[metadata["provider"]]
	return ok
}

// Validate is intentionally a no-op because endpoint selection needs the
// request URL, which this legacy method does not receive.
func (p *Plugin) Validate([]byte, map[string]string, int64) error { return nil }

// Transform is intentionally a no-op: this plugin requires the worker's
// context-aware transform path.
func (p *Plugin) Transform([]byte, map[string]string) ([]byte, string, bool, error) {
	return nil, "", false, nil
}

// TransformContext checks the signed URL and downloads the audio object. It puts
// the object bytes in the multipart "file" part and copies the other JSON fields
// as form fields. It returns handled=false for requests it does not handle.
func (p *Plugin) TransformContext(ctx context.Context, payload []byte, metadata map[string]string, requestURL string, requestDeadline int64) ([]byte, string, bool, error) {
	if !p.owns(metadata) || !supportedAudioEndpoint(requestURL) {
		return nil, "", false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, "", false, retryableDownloadError("audio download stopped", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, "", false, invalidRequestErrorf("failed to parse request body: %v", err)
	}
	signedURL, err := stringField(fields, gcsURIField)
	if err != nil {
		return nil, "", false, invalidRequestErrorf("%q must be a string", gcsURIField)
	}
	if signedURL == "" {
		return nil, "", false, nil
	}
	if _, ok := fields[fileField]; ok {
		return nil, "", false, invalidRequestErrorf("%q cannot be combined with %q", fileField, gcsURIField)
	}
	fileBase64, err := stringField(fields, fileBase64Field)
	if err != nil {
		return nil, "", false, invalidRequestErrorf("%q must be a string", fileBase64Field)
	}
	if fileBase64 != "" {
		return nil, "", false, invalidRequestErrorf("%q is not supported on the GCS download path", fileBase64Field)
	}

	deadline := time.Now()
	if ctxDeadline, hasDeadline := ctx.Deadline(); hasDeadline {
		deadline = ctxDeadline
	}
	// The original request deadline can be later than this attempt's context deadline.
	if requestDeadline > 0 {
		deadline = time.Unix(requestDeadline, 0)
	}
	parsedURL, err := p.signedURL(signedURL, deadline)
	if err != nil {
		return nil, "", false, err
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile(fileField, sanitizeFilename(parsedURL.Path))
	if err != nil {
		return nil, "", false, invalidRequestErrorf("failed to create file part: %v", err)
	}
	if err := p.download(ctx, signedURL, file); err != nil {
		return nil, "", false, err
	}
	for name, raw := range fields {
		if name == gcsURIField || name == fileBase64Field {
			continue
		}
		if err := util.WriteJSONField(writer, name, raw); err != nil {
			return nil, "", false, invalidRequestErrorf("failed to write %q field: %v", name, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", false, invalidRequestErrorf("failed to finalize multipart body: %v", err)
	}
	return body.Bytes(), writer.FormDataContentType(), true, nil
}

func (p *Plugin) download(ctx context.Context, signedURL string, dst io.Writer) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, signedURL, nil)
	if err != nil {
		return invalidRequestErrorf("failed to create audio download request")
	}
	response, err := p.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return retryableDownloadError("audio download stopped", ctx.Err())
		}
		if errors.Is(err, errUnsupportedRedirect) {
			return invalidRequestErrorf("audio download redirect rejected")
		}
		return retryableDownloadError("audio download failed", safeDownloadCause(err))
	}
	defer response.Body.Close() // nolint:errcheck
	retryAfter := util.ParseRetryAfter(response.Header.Get("Retry-After"))

	switch {
	case response.StatusCode == http.StatusTooManyRequests:
		return downloadHTTPError(
			asyncapi.ErrCategoryRateLimit,
			fmt.Sprintf("audio download rate limited: status code %d", response.StatusCode),
			response.StatusCode,
			retryAfter,
		)
	case response.StatusCode == http.StatusRequestTimeout:
		// GCS timed out waiting for the request. This is temporary, so retry.
		return downloadHTTPError(
			asyncapi.ErrCategoryServer,
			fmt.Sprintf("audio download timed out: status code %d", response.StatusCode),
			response.StatusCode,
			retryAfter,
		)
	case response.StatusCode >= 400 && response.StatusCode < 500:
		return invalidRequestErrorf("audio download rejected: status code %d", response.StatusCode)
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return downloadHTTPError(
			asyncapi.ErrCategoryServer,
			fmt.Sprintf("audio download failed: status code %d", response.StatusCode),
			response.StatusCode,
			retryAfter,
		)
	}
	if response.ContentLength > p.maxFileBytes {
		return invalidRequestErrorf("audio download exceeds maxFileBytes")
	}

	bytesRead, err := io.Copy(dst, io.LimitReader(response.Body, p.maxFileBytes))
	if err != nil {
		if ctx.Err() != nil {
			return retryableDownloadError("audio download stopped", ctx.Err())
		}
		return retryableDownloadError("audio download failed", safeDownloadCause(err))
	}
	if bytesRead == p.maxFileBytes {
		var extra [1]byte
		n, readErr := io.ReadFull(response.Body, extra[:])
		if n > 0 {
			return invalidRequestErrorf("audio download exceeds maxFileBytes")
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			if ctx.Err() != nil {
				return retryableDownloadError("audio download stopped", ctx.Err())
			}
			return retryableDownloadError("audio download failed", safeDownloadCause(readErr))
		}
	}
	return nil
}

func (p *Plugin) allowedURL(parsedURL *url.URL) error {
	if parsedURL.User != nil || parsedURL.Port() != "" {
		return invalidRequestErrorf("gcs_uri host is not an allowed hostname")
	}
	if _, ok := p.allowedHosts[strings.ToLower(parsedURL.Hostname())]; !ok {
		return invalidRequestErrorf("gcs_uri host is not allowed")
	}
	return nil
}

func (p *Plugin) signedURL(signedURL string, deadline time.Time) (*url.URL, error) {
	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		return nil, invalidRequestErrorf("gcs_uri is not a valid URL")
	}
	if !util.IsAbsoluteHTTPSURL(parsedURL) {
		return nil, invalidRequestErrorf("gcs_uri must use HTTPS")
	}
	if err := p.allowedURL(parsedURL); err != nil {
		return nil, err
	}
	if !identifiesObject(parsedURL) {
		return nil, invalidRequestErrorf("gcs_uri must identify a bucket and object")
	}

	query := parsedURL.Query()
	algorithm := value(query, "X-Goog-Algorithm")
	if (algorithm != gcsV4RSASHA256 && algorithm != gcsV4HMACSHA256) ||
		value(query, "X-Goog-Credential") == "" ||
		value(query, "X-Goog-Date") == "" ||
		value(query, "X-Goog-Expires") == "" {
		return nil, invalidRequestErrorf("gcs_uri is not a signed V4 URL")
	}
	signature := value(query, "X-Goog-Signature")
	if signature == "" || len(signature)%2 != 0 {
		return nil, invalidRequestErrorf("gcs_uri has an invalid signature format")
	}
	decodedSignature, err := hex.DecodeString(signature)
	if err != nil || (algorithm == gcsV4HMACSHA256 && len(decodedSignature) != hmacSHA256SignatureBytes) {
		return nil, invalidRequestErrorf("gcs_uri has an invalid signature format")
	}

	start, err := time.Parse("20060102T150405Z", value(query, "X-Goog-Date"))
	if err != nil {
		return nil, invalidRequestErrorf("gcs_uri has an invalid signing date")
	}
	seconds, err := strconv.ParseInt(value(query, "X-Goog-Expires"), 10, 64)
	if err != nil || seconds <= 0 {
		return nil, invalidRequestErrorf("gcs_uri has an invalid expiry")
	}
	expiry := start.Add(time.Duration(seconds) * time.Second)
	if !expiry.After(deadline) {
		return nil, invalidRequestErrorf("gcs_uri expires at or before the request deadline")
	}
	return parsedURL, nil
}

// identifiesObject reports whether the URL names a bucket and an object.
// Virtual-hosted URLs (BUCKET.storage.googleapis.com/OBJECT) carry the bucket in
// the hostname, so their path only needs the object. Other URLs are path-style
// (HOST/BUCKET/OBJECT) and need both.
//
// Only the leading slash of the path is removed. Object names may themselves
// start or end with "/" (for example "%2Faudio.wav"), so the rest is kept.
func identifiesObject(parsedURL *url.URL) bool {
	objectPath := strings.TrimPrefix(parsedURL.Path, "/")
	if strings.HasSuffix(strings.ToLower(parsedURL.Hostname()), virtualHostedSuffix) {
		return objectPath != ""
	}
	bucket, object, found := strings.Cut(objectPath, "/")
	return found && bucket != "" && object != ""
}

func supportedAudioEndpoint(requestURL string) bool {
	parsedURL, err := url.Parse(requestURL)
	if err != nil {
		return false
	}
	return parsedURL.Path == "/v1/audio/transcriptions" || parsedURL.Path == "/v1/audio/translations"
}

func parseAllowedHosts(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("allowedHosts is required")
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err == nil {
		raw = []byte(encoded)
	}
	var hosts []string
	if err := json.Unmarshal(raw, &hosts); err != nil {
		return nil, fmt.Errorf("allowedHosts must be a hostname array or JSON-encoded hostname array: %w", err)
	}
	if len(hosts) == 0 {
		return nil, errors.New("allowedHosts must contain at least one hostname")
	}
	for i, host := range hosts {
		normalized, err := normalizeHostname(host)
		if err != nil {
			return nil, fmt.Errorf("allowedHosts[%d]: %w", i, err)
		}
		hosts[i] = normalized
	}
	return hosts, nil
}

func normalizeHostname(host string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(host))
	if normalized == "" || strings.ContainsAny(normalized, "/?#") {
		return "", errors.New("must be a hostname without a scheme, port, path, query, or fragment")
	}
	parsed, err := url.Parse("//" + normalized)
	if err != nil || parsed.User != nil || parsed.Port() != "" || parsed.Host != normalized {
		return "", errors.New("must be a hostname without a port or credentials")
	}
	return normalized, nil
}

func parseMaxFileBytes(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, errors.New("maxFileBytes is required")
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err == nil {
		raw = []byte(encoded)
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("maxFileBytes must be an integer or integer string: %w", err)
	}
	if value <= 0 {
		return 0, errors.New("maxFileBytes must be positive")
	}
	return value, nil
}

func stringField(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '"' {
		return "", errors.New("not a JSON string")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

func value(query url.Values, name string) string {
	values := query[name]
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

func sanitizeFilename(objectPath string) string {
	filename := path.Base(objectPath)
	var builder strings.Builder
	for _, r := range filename {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
		if builder.Len() >= 128 {
			break
		}
	}
	sanitized := strings.Trim(builder.String(), ".")
	if sanitized == "" {
		return "audio"
	}
	return sanitized
}

func invalidRequestErrorf(format string, args ...any) error {
	return &asyncapi.ClientError{
		ErrorCategory: asyncapi.ErrCategoryInvalidReq,
		Message:       fmt.Sprintf(format, args...),
	}
}

func retryableDownloadError(message string, rawErr error) error {
	return &asyncapi.ClientError{
		ErrorCategory: asyncapi.ErrCategoryServer,
		Message:       message,
		RawError:      rawErr,
	}
}

func downloadHTTPError(
	category asyncapi.ErrorCategory,
	message string,
	statusCode int,
	retryAfter time.Duration,
) error {
	return &asyncapi.ClientError{
		ErrorCategory: category,
		Message:       message,
		RetryAfter:    retryAfter,
		StatusCode:    statusCode,
	}
}

// safeDownloadCause returns the part of a download error that is safe to record.
// Go's HTTP client can copy a redirect Location header into plain error text,
// and that header can hold the signature. So only network errors are kept: their
// text has a host and port, never a URL path or query. Any other error is dropped.
func safeDownloadCause(err error) error {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr
	}
	return nil
}
