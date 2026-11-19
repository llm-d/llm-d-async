# Request Body Transform Reference


Some providers need a different body shape at dispatch time — for example multi-modal endpoints (Whisper transcription, OCR) that expect `multipart/form-data` with a `url` field rather than JSON. Request body-transform plugins handle this without special-casing the worker: they rewrite the outgoing body and `Content-Type` based on per-message `metadata`, and the default JSON path is preserved byte-for-byte when no plugin applies.

When a transform handles a request, its `Content-Type` replaces caller-supplied values regardless of header-name casing. Other headers are preserved, and the original per-message headers remain intact for retries.

Transforms are configured with `--transform-config-file`, pointing at a JSON object that groups plugins by direction:

```json
{
  "requestTransforms": [
    {
      "name": "whisper-multipart",
      "type": "gcs_uri_multipart",
      "parameters": { "providers": ["whisper"] }
    }
  ]
}
```

Each entry has a unique `name`, a registered plugin `type`, and opaque `parameters`. Unknown top-level fields are rejected. When the flag is empty, no transforms are loaded and behavior is unchanged.

With the Helm chart, set `ap.transformConfig` to this same object; the chart renders it to a config file and wires `--transform-config-file` automatically:

```yaml
ap:
  transformConfig:
    requestTransforms:
      - name: "whisper-multipart"
        type: "gcs_uri_multipart"
        parameters:
          providers: ["whisper"]
```

## `gcs_uri_multipart` plugin

Rewrites a JSON body into `multipart/form-data` for endpoints that take a signed object URL. Because producers can't put raw media bytes on the broker, the queued `payload` carries a signed URL (e.g. a GCS V4 signed URL) in a `gcs_uri` field.

- **Activation:** the message's `metadata.provider` must match one of the configured `providers`, and the `payload` must contain a non-empty `gcs_uri`. Otherwise the default JSON path is used unchanged.
- **Transform:** writes the `gcs_uri` value as a `url` form field (a plain field, not a file upload), passes the remaining payload fields through as form fields, and drops `gcs_uri`. A non-empty `file_base64` is rejected as a fatal, non-retryable error (inline media is not supported on this path).
- **Preflight:** parses the signed URL's expiry (V4 `X-Goog-Date` + `X-Goog-Expires`, or V2 `Expires`); if it expires at or before the message deadline, the request fails fatally before dispatch so the broker doesn't retry a request that cannot succeed.

## `gcs_file_multipart` plugin

Rewrites an audio JSON request into `multipart/form-data` with the actual GCS object bytes in the `file` part. The queued `payload` carries a signed HTTPS GCS URL in `gcs_uri`.

- **Endpoints:** `/v1/audio/transcriptions` and `/v1/audio/translations`. Other request URLs use the default JSON path unchanged.
- **Activation:** a supported request URL and a non-empty `gcs_uri`.
- **Preflight:** accepts V4-signed HTTPS URLs using `GOOG4-RSA-SHA256` or `GOOG4-HMAC-SHA256` on an `allowedHosts` hostname and with an expiry later than the request deadline. RSA signatures must be non-empty hex; HMAC-SHA256 signatures must be 32-byte hex. Path-style URLs (`storage.googleapis.com/BUCKET/OBJECT`) and virtual-hosted URLs (`BUCKET.storage.googleapis.com/OBJECT`) are accepted; list the exact hostname the URL uses in `allowedHosts`. Unsigned URLs, `gs://` URIs, unapproved hosts, and bad signature formats fail as invalid requests.
- **Transform:** performs a context-aware `GET` of the signed URL, reads at most `maxFileBytes`, writes those exact bytes to the `file` part, copies the remaining JSON fields as form fields, and removes `gcs_uri`.
- **Errors:** missing objects and other 4xx responses, except 429, are invalid requests. 429, 5xx, timeouts, and connection failures are retryable. Cancellation and shutdown stop the download.
- **Security:** redirects to unapproved hosts are rejected, the signed URL is not logged or copied into the multipart body, and no GCS credentials are added to the worker.

Example:

```yaml
requestTransforms:
  - name: gcs-audio
    type: gcs_file_multipart
    parameters:
      allowedHosts: '["storage.googleapis.com"]'
      maxFileBytes: "26214400"
```
