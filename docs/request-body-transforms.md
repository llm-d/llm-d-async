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

## Which plugin handles a request

The chain does not work out which plugin owns a request; each plugin decides for itself. The worker runs it in two steps:

1. **Validate, before dispatch:** every plugin's `Validate` runs, in `requestTransforms` order. The first error fails the request; no plugin is skipped at this step.
2. **Transform, at each dispatch attempt:** plugins run in order until one handles the request (`handled=true`), whose body is sent; the rest are skipped. An error ends the chain, and the request is handled as described in [Failures](#failures).

An error from any plugin, at either step, stops the chain, even when that plugin was not the one meant to handle the request: no later plugin gets to run. A `Validate` error fails the request; a `Transform` error is retried or fails it as described in [Failures](#failures). So each plugin must claim only the requests it owns and pass on the rest, in both steps:

- A request a plugin owns carries the payload field the plugin reads (for example `gcs_uri`).
- When two plugins read the same field, configuration decides: the plugin's own filter (for example `gcs_uri_multipart`'s `providers`, matched against `metadata.provider`), then the order in `requestTransforms`.
- For any other request, a plugin passes: `Validate` returns no error and `Transform` returns `handled=false` with no error, so the next plugin gets the request. A plugin returns an error only for a request it has claimed.

## Failures

Transforms run on every dispatch attempt, including retries. A plugin that also implements the optional `ContextTransform` interface gets a context and the dispatch URL through `TransformContext`; other plugins keep using `Transform` unchanged. The context ends at the attempt's deadline, the earlier of `--request-timeout` and the request's own deadline, and is cancelled on shutdown. How a transform failure ends depends on the error it returns:

- An error carrying the `RATE_LIMIT` or `SERVER_ERROR` category (an `api.InferenceError` anywhere in its `%w` chain, such as storage being temporarily unavailable) is retried with backoff until the request deadline.
- If the request's own deadline expires while the transform runs, the result is `DEADLINE_EXCEEDED`.
- If `--request-timeout`, which limits one attempt, stops the transform while the request's deadline is still ahead, the request is retried with backoff.
- If the processor shuts down while the transform runs, the request is re-enqueued and the interrupted attempt is not counted.
- Any other error fails the request with `INVALID_REQUEST`.

For an `api.ClientError`, the error result, the span and the retry log carry its category and message but not the cause it wraps in `RawError`, so a plugin can keep an SDK error (with request IDs or signed URLs) for `errors.Is`/`errors.As` without it reaching results or traces. This only drops `RawError`: other errors, and any text wrapped around the `ClientError`, are reported as they are. The span records the error's own category, while the result code stays `INVALID_REQUEST`.

## `gcs_uri_multipart` plugin

Rewrites a JSON body into `multipart/form-data` for endpoints that take a signed object URL. Because producers can't put raw media bytes on the broker, the queued `payload` carries a signed URL (e.g. a GCS V4 signed URL) in a `gcs_uri` field.

- **Activation:** the message's `metadata.provider` must match one of the configured `providers`, and the `payload` must contain a non-empty `gcs_uri`. Otherwise the default JSON path is used unchanged.
- **Transform:** writes the `gcs_uri` value as a `url` form field (a plain field, not a file upload), passes the remaining payload fields through as form fields, and drops `gcs_uri`. A non-empty `file_base64` is rejected as a fatal, non-retryable error (inline media is not supported on this path).
- **Preflight:** parses the signed URL's expiry (V4 `X-Goog-Date` + `X-Goog-Expires`, or V2 `Expires`); if it expires at or before the message deadline, the request fails fatally before dispatch so the broker doesn't retry a request that cannot succeed.
