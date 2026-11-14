package s3multipart

import (
	"bytes"
	"context"
	"encoding/base64"
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
)

// transformAudio builds the multipart body for a transcription or translation:
// the audio named by `file` becomes the file part and every other field a form
// field. Only s3:// references and base64 data URLs are accepted; anything else
// would make the worker fetch arbitrary URLs.
func (p *Plugin) transformAudio(ctx context.Context, payload []byte) ([]byte, string, bool, error) {
	fields, err := decodeFields(payload)
	if err != nil {
		return nil, "", false, err
	}
	ref, ok := stringField(fields, fileField)
	if !ok {
		return nil, "", false, fatalf("%q must be an s3:// reference or a base64 data URL", fileField)
	}

	var data []byte
	var mediaType, filename string
	switch {
	case strings.HasPrefix(ref, s3Scheme):
		bucket, key, err := p.parseS3URI(ref)
		if err != nil {
			return nil, "", false, err
		}
		var stored string
		if data, stored, err = p.getObject(ctx, bucket, key); err != nil {
			return nil, "", false, err
		}
		// Object metadata is not the request's to fix: an unusable stored
		// type falls back to the default and vLLM sniffs the format.
		if mediaType, ok = canonicalMediaType(stored); !ok {
			mediaType = defaultMediaType
		}
		filename = path.Base(key)
	case strings.HasPrefix(ref, "data:"):
		if data, mediaType, err = p.decodeDataURL(ref); err != nil {
			return nil, "", false, err
		}
		filename = "audio"
	default:
		return nil, "", false, fatalf("%q must be an s3:// reference or a base64 data URL", fileField)
	}

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	// Both header values are built by mime.FormatMediaType, which quotes or
	// RFC 2231-encodes anything unsafe, so request data cannot inject headers.
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": fileField, "filename": filename}))
	h.Set("Content-Type", mediaType)
	part, err := w.CreatePart(h)
	if err != nil {
		return nil, "", false, fatalf("failed to write %q part: %v", fileField, err)
	}
	if _, err := part.Write(data); err != nil {
		return nil, "", false, fatalf("failed to write %q part: %v", fileField, err)
	}
	if err := writeFormFields(w, fields, fileField); err != nil {
		return nil, "", false, fatalf("failed to write form fields: %v", err)
	}
	if err := w.Close(); err != nil {
		return nil, "", false, fatalf("failed to finalize multipart body: %v", err)
	}
	return buf.Bytes(), w.FormDataContentType(), true, nil
}

// decodeDataURL decodes a base64 data URL (RFC 2397,
// data:[<mediatype>];base64,<data>) and returns its bytes and canonical media
// type.
func (p *Plugin) decodeDataURL(ref string) ([]byte, string, error) {
	meta, encoded, ok := strings.Cut(strings.TrimPrefix(ref, "data:"), ",")
	if !ok {
		return nil, "", fatalf("malformed data URL in %q", fileField)
	}
	mediaType, isBase64 := strings.CutSuffix(meta, ";base64")
	if !isBase64 {
		return nil, "", fatalf("data URL in %q must be base64-encoded", fileField)
	}
	mediaType, ok = canonicalMediaType(mediaType)
	if !ok {
		return nil, "", fatalf("data URL in %q has an invalid media type", fileField)
	}
	if int64(base64.StdEncoding.DecodedLen(len(encoded))) > p.maxFileBytes+2 {
		return nil, "", fatalf("data URL in %q is over the %d-byte limit", fileField, p.maxFileBytes)
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, "", fatalf("invalid base64 in %q data URL: %v", fileField, err)
	}
	if int64(len(data)) > p.maxFileBytes {
		return nil, "", fatalf("data URL in %q is over the %d-byte limit", fileField, p.maxFileBytes)
	}
	return data, mediaType, nil
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
