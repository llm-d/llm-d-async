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

## Failures

Transforms run on every dispatch attempt, including retries. A plugin that also implements the optional `ContextTransform` interface gets the request's context (deadline and shutdown) and the dispatch URL through `TransformContext`; other plugins keep using `Transform` unchanged. How a transform failure ends depends on the error it returns:

- An error carrying the `RATE_LIMIT` or `SERVER_ERROR` category (an `api.InferenceError` anywhere in its `%w` chain, such as storage being temporarily unavailable) is retried with backoff until the request deadline.
- If the request deadline expires while the transform runs, the result is `DEADLINE_EXCEEDED`.
- If the processor shuts down while the transform runs, the request is re-enqueued and the interrupted attempt is not counted.
- Any other error fails the request with `INVALID_REQUEST`.

## `gcs_uri_multipart` plugin

Rewrites a JSON body into `multipart/form-data` for endpoints that take a signed object URL. Because producers can't put raw media bytes on the broker, the queued `payload` carries a signed URL (e.g. a GCS V4 signed URL) in a `gcs_uri` field.

- **Activation:** the message's `metadata.provider` must match one of the configured `providers`, and the `payload` must contain a non-empty `gcs_uri`. Otherwise the default JSON path is used unchanged.
- **Transform:** writes the `gcs_uri` value as a `url` form field (a plain field, not a file upload), passes the remaining payload fields through as form fields, and drops `gcs_uri`. A non-empty `file_base64` is rejected as a fatal, non-retryable error (inline media is not supported on this path).
- **Preflight:** parses the signed URL's expiry (V4 `X-Goog-Date` + `X-Goog-Expires`, or V2 `Expires`); if it expires at or before the message deadline, the request fails fatally before dispatch so the broker doesn't retry a request that cannot succeed.
