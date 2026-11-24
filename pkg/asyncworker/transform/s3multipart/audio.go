package s3multipart

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"net/textproto"
	"path"
	"strings"
)

const (
	// fileField is the JSON field naming the audio and the multipart file part
	// vLLM reads it from.
	fileField = "file"
	// defaultMediaType is used when neither the object nor the data URL names
	// one; vLLM detects the audio format from the bytes.
	defaultMediaType = "application/octet-stream"
	// partOverhead is the room reserved for each multipart part's boundary and
	// headers when the body buffer is sized.
	partOverhead = 256
)

// transformAudio builds the multipart body for a transcription or translation:
// the audio named by `file` becomes the file part and every other field a form
// field. A body without `file` is left to the next transform. A `file` that is
// not an s3:// reference or a base64 data URL fails the request, since anything
// else would make the worker fetch arbitrary URLs.
//
// TransformContext calls it when the dispatch URL ends in
// /v1/audio/transcriptions or /v1/audio/translations. payload is the queued
// request's JSON body, e.g. {"model":"whisper","file":"s3://bucket/a.wav"}.
//
// The audio is copied straight into a body buffer sized up front, so the
// request holds the multipart body and no separate copy of the file.
func (p *Plugin) transformAudio(ctx context.Context, payload []byte) ([]byte, string, bool, error) {
	fields, err := decodeFields(payload)
	if err != nil {
		// The payload is not a JSON object: a fatal INVALID_REQ error, so the
		// request fails instead of being retried or sent as JSON.
		return nil, "", false, err
	}
	raw, present := fields[fileField]
	if !present {
		return nil, "", false, nil
	}
	// raw is the field's JSON text, not yet a Go value: `"s3://b/k"` decodes
	// to a string, but `42`, `true`, `[...]` or `{...}` fail. (`null` decodes
	// to "" and is rejected below as neither s3:// nor data:.)
	var ref string
	if json.Unmarshal(raw, &ref) != nil {
		return nil, "", false, fatalf("%q must be an s3:// reference or a base64 data URL", fileField)
	}

	var (
		src                 io.Reader
		size                int64 // expected file size, -1 when unknown
		mediaType, filename string
		source              string // names the file in errors
		readError           func(error) error
	)
	switch {
	case strings.HasPrefix(ref, s3Scheme):
		// Split s3://bucket/key and reject buckets outside allowedBuckets:
		// s3://batch-files/tenant-a/calls/42.wav gives bucket "batch-files"
		// and key "tenant-a/calls/42.wav".
		bucket, key, err := p.parseS3URI(ref)
		if err != nil {
			return nil, "", false, err
		}
		body, stored, n, err := p.openObject(ctx, bucket, key)
		if err != nil {
			return nil, "", false, err
		}
		defer func() { _ = body.Close() }()
		src, size, source = body, n, ref
		readError = func(err error) error { return storageError(ctx, "read", ref, err) }
		// Object metadata is not the request's to fix: an unusable stored
		// type falls back to the default and vLLM sniffs the format.
		var ok bool
		if mediaType, ok = canonicalMediaType(stored); !ok {
			mediaType = defaultMediaType
		}
		filename = path.Base(key)
	case strings.HasPrefix(ref, "data:"):
		encoded, dataType, err := p.parseDataURL(ref)
		if err != nil {
			return nil, "", false, err
		}
		src = base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded))
		size = int64(base64.StdEncoding.DecodedLen(len(encoded)))
		mediaType, filename, source = dataType, "audio", fmt.Sprintf("data URL in %q", fileField)
		// Malformed input, never a storage failure: not retried.
		readError = func(err error) error { return fatalWrap(err, "invalid base64 in %q data URL", fileField) }
	default:
		return nil, "", false, fatalf("%q must be an s3:// reference or a base64 data URL", fileField)
	}

	var buf bytes.Buffer
	buf.Grow(bodyCapacity(size, p.maxFileBytes, len(payload)-len(raw), len(fields)))
	w := multipart.NewWriter(&buf)
	// Both header values are built by mime.FormatMediaType, which quotes or
	// RFC 2231-encodes anything unsafe, so request data cannot inject headers.
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": fileField, "filename": filename}))
	h.Set("Content-Type", mediaType)
	part, err := w.CreatePart(h)
	if err != nil {
		return nil, "", false, fatalWrap(err, "failed to write %q part", fileField)
	}
	// One byte over the limit is read to tell a file at the limit from a longer
	// one; a declared size is not trusted.
	n, err := io.Copy(part, io.LimitReader(src, p.maxFileBytes+1))
	if err != nil {
		return nil, "", false, readError(err)
	}
	if n > p.maxFileBytes {
		return nil, "", false, fatalf("%s is over the %d-byte limit", source, p.maxFileBytes)
	}
	if err := writeFormFields(w, fields, fileField); err != nil {
		return nil, "", false, fatalWrap(err, "failed to write form fields")
	}
	if err := w.Close(); err != nil {
		return nil, "", false, fatalWrap(err, "failed to finalize multipart body")
	}
	return buf.Bytes(), w.FormDataContentType(), true, nil
}

// bodyCapacity estimates the multipart body's size: the file's expected size
// capped at the limit (a longer file is rejected while it is copied, and a
// base64 estimate can run two bytes over), the other fields' JSON, and room for
// each part's boundary and headers. An underestimate only costs a regrowth; an
// unknown size (-1) reserves nothing for the file rather than the whole limit.
func bodyCapacity(fileSize, limit int64, fieldBytes, fieldCount int) int {
	fileSize = min(fileSize, limit)
	if fileSize < 0 || fileSize > math.MaxInt/2 {
		fileSize = 0
	}
	return int(fileSize) + fieldBytes + (fieldCount+1)*partOverhead
}

// parseDataURL checks a base64 data URL (RFC 2397,
// data:[<mediatype>];base64,<data>) and returns its still-encoded data and
// canonical media type; the data is decoded while it is copied into the body.
func (p *Plugin) parseDataURL(ref string) (string, string, error) {
	meta, encoded, ok := strings.Cut(strings.TrimPrefix(ref, "data:"), ",")
	if !ok {
		return "", "", fatalf("malformed data URL in %q", fileField)
	}
	mediaType, isBase64 := strings.CutSuffix(meta, ";base64")
	if !isBase64 {
		return "", "", fatalf("data URL in %q must be base64-encoded", fileField)
	}
	mediaType, ok = canonicalMediaType(mediaType)
	if !ok {
		return "", "", fatalf("data URL in %q has an invalid media type", fileField)
	}
	if int64(base64.StdEncoding.DecodedLen(len(encoded))) > p.maxFileBytes+2 {
		return "", "", fatalf("data URL in %q is over the %d-byte limit", fileField, p.maxFileBytes)
	}
	// base64.NewDecoder decodes in chunks and accepts padding that ends a chunk
	// mid-data, which a single decode rejects: "=" may only end the data.
	if i := strings.IndexByte(encoded, '='); i >= 0 && strings.TrimRight(encoded[i:], "=\r\n") != "" {
		return "", "", fatalf("invalid base64 in %q data URL", fileField)
	}
	return encoded, mediaType, nil
}

// canonicalMediaType parses a media type and re-serializes it, so only a
// well-formed value (no CR/LF or other header syntax) reaches the part's
// Content-Type. An empty input yields defaultMediaType; ok is false when the
// input does not parse.
func canonicalMediaType(s string) (string, bool) {
	if s == "" {
		return defaultMediaType, true
	}
	mediaType, params, err := mime.ParseMediaType(s)
	if err != nil {
		return "", false
	}
	out := mime.FormatMediaType(mediaType, params)
	return out, out != ""
}
