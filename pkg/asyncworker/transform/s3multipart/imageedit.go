package s3multipart

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	asyncapi "github.com/llm-d/llm-d-async/api"
)

const (
	// imagesField is the OpenAI JSON field listing the input images.
	imagesField = "images"
	// urlFormField is the vllm-omni form field it fetches each image from.
	urlFormField = "url"

	// defaultPresignExpiry applies when the request context has no deadline.
	defaultPresignExpiry = 15 * time.Minute
	// maxPresignExpiry is the longest lifetime S3 SigV4 allows.
	maxPresignExpiry = 7 * 24 * time.Hour
	// credentialRefreshWindow is how much lifetime cached expiring
	// credentials must have left before they sign a URL; with less, they are
	// refreshed first so the URL is not cut short.
	credentialRefreshWindow = 10 * time.Minute
)

// transformImageEdit builds the multipart body for a vllm-omni image edit:
// each `images[].image_url` becomes a `url` form field, and every other field a
// form field. s3:// references are presigned for vllm-omni to fetch; data:image
// URLs pass through. Any other URL is rejected, since vllm-omni fetches `url`
// values without an allowlist.
func (p *Plugin) transformImageEdit(ctx context.Context, payload []byte) ([]byte, string, bool, error) {
	fields, err := decodeFields(payload)
	if err != nil {
		return nil, "", false, err
	}
	var images []struct {
		ImageURL string `json:"image_url"`
	}
	if err := json.Unmarshal(fields[imagesField], &images); err != nil || len(images) == 0 {
		return nil, "", false, fatalf("%q must be a non-empty list of {\"image_url\": ...}", imagesField)
	}
	// vllm-omni has no `mask` form field, so a mask would be silently dropped
	// and the whole image edited.
	if raw, ok := fields["mask"]; ok && string(raw) != "null" {
		return nil, "", false, fatalf("%q is not supported", "mask")
	}
	// vllm-omni fetches every image source it is given (`url`, `url[]`,
	// `mask_image`, `reference_image`) without a host allowlist, so only the
	// `url` fields this plugin derives from images may reach it.
	for _, name := range []string{urlFormField, urlFormField + "[]", "mask_image", "reference_image"} {
		if _, ok := fields[name]; ok {
			return nil, "", false, fatalf("%q is not accepted; list input images in %q", name, imagesField)
		}
	}

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, img := range images {
		var u string
		switch {
		case strings.HasPrefix(img.ImageURL, s3Scheme):
			if u, err = p.presign(ctx, img.ImageURL); err != nil {
				return nil, "", false, err
			}
		case strings.HasPrefix(img.ImageURL, "data:image/"):
			u = img.ImageURL
		default:
			return nil, "", false, fatalf("each image_url must be an s3:// reference or a data:image/ URL")
		}
		if err := w.WriteField(urlFormField, u); err != nil {
			return nil, "", false, fatalf("failed to write %q field: %v", urlFormField, err)
		}
	}
	if err := writeFormFields(w, fields, imagesField); err != nil {
		return nil, "", false, fatalf("failed to write form fields: %v", err)
	}
	if err := w.Close(); err != nil {
		return nil, "", false, fatalf("failed to finalize multipart body: %v", err)
	}
	return buf.Bytes(), w.FormDataContentType(), true, nil
}

// presign returns a GET URL for an allowlisted s3:// reference that stays valid
// for the rest of this dispatch attempt (see presignExpiry).
func (p *Plugin) presign(ctx context.Context, ref string) (string, error) {
	bucket, key, err := p.parseS3URI(ref)
	if err != nil {
		return "", err
	}
	// The credentials cache only consults ctx on a miss, so check it here:
	// past the deadline this is the worker's to report, not a storage error.
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("presign %s: %w", ref, err)
	}
	provider := p.client.Options().Credentials
	creds, err := provider.Retrieve(ctx)
	if err != nil {
		return "", storageError(ctx, ref, err)
	}
	// A URL stops working when the keys that signed it expire, so refresh
	// expiring keys (STS, workload identity) that are near their end.
	if cache, ok := provider.(*aws.CredentialsCache); ok && creds.CanExpire && time.Until(creds.Expires) < credentialRefreshWindow {
		cache.Invalidate()
		if creds, err = cache.Retrieve(ctx); err != nil {
			return "", storageError(ctx, ref, err)
		}
	}
	deadline, hasDeadline := ctx.Deadline()
	expires, ok := presignExpiry(time.Now(), deadline, hasDeadline, creds)
	if !ok {
		// Even freshly retrieved keys expire within a second; a retry signs
		// with rotated ones.
		return "", &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       fmt.Sprintf("presign %s: credentials expire at %s", ref, creds.Expires.UTC().Format(time.RFC3339)),
		}
	}
	req, err := s3.NewPresignClient(p.client).PresignGetObject(ctx,
		&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)},
		s3.WithPresignExpires(expires),
		func(o *s3.PresignOptions) {
			// Sign with exactly the keys whose expiry bounded the URL.
			o.ClientOptions = append(o.ClientOptions, func(co *s3.Options) {
				co.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil })
			})
		})
	if err != nil {
		return "", storageError(ctx, ref, err)
	}
	return req.URL, nil
}

// presignExpiry returns the X-Amz-Expires for a URL signed at now. SigV4 dates
// the signature in whole seconds, so the URL is valid from now truncated to the
// second. It stays valid through the attempt deadline (defaultPresignExpiry
// without one), capped at S3's 7-day maximum, and ends at least a second
// before the signing credentials expire: the URL stops working when they do,
// and the presigner reads the clock again slightly later. ok is false when
// less than a second remains.
func presignExpiry(now, deadline time.Time, hasDeadline bool, creds aws.Credentials) (time.Duration, bool) {
	signed := now.Truncate(time.Second)
	expires := defaultPresignExpiry
	if hasDeadline {
		expires = deadline.Sub(signed)
	}
	expires = min(roundUpSecond(expires), maxPresignExpiry)
	if creds.CanExpire {
		expires = min(expires, creds.Expires.Sub(signed).Truncate(time.Second)-time.Second)
	}
	return expires, expires >= time.Second
}

func roundUpSecond(d time.Duration) time.Duration {
	return (d + time.Second - 1).Truncate(time.Second)
}
