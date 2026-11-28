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

## Plugin lifecycle and ownership

At startup, Async calls each configured plugin's `New` factory with its `parameters`. For each request, the worker calls `Chain.Validate` on every plugin before dispatch. The first validation error fails the request; validation does not skip plugins. At each dispatch attempt, `Chain.ApplyContext` offers the request to plugins in `requestTransforms` order. A plugin implementing `ContextTransform` receives the attempt context, dispatch URL, and original request deadline. The first plugin returning `handled=true` supplies the outgoing body; if none handles it, Async sends the original JSON body.

The chain does not determine which plugin owns a request; each plugin decides for itself. An error from any plugin stops the chain, even if that plugin was not meant to handle the request. Each plugin must claim only requests it owns and pass on the rest:

- A request a plugin owns carries the payload field the plugin reads (for example `gcs_uri`). Both GCS plugins also require the request's `metadata.provider` to be in their `providers` list, and `gcs_file_multipart` also requires an audio endpoint.
- When several plugins would claim the same request, the first one in `requestTransforms` handles it, and later plugins are skipped.
- For any other request, `Validate` returns no error and `Transform` returns `handled=false` with no error, so the next plugin gets it. A plugin returns an error only for a request it has claimed.

## Failures

Transforms run on every dispatch attempt, including retries. A plugin that also implements the optional `ContextTransform` interface gets a context, the dispatch URL and the original request deadline through `TransformContext`; other plugins keep using `Transform` unchanged. The context ends at the attempt's deadline, the earlier of `--request-timeout` and the request's own deadline. It is cancelled on shutdown, and when the producer cancels the request (if the queue backend supports cancellation). How a transform failure ends depends on the error it returns:

- An error carrying the `RATE_LIMIT` or `SERVER_ERROR` category (an `api.InferenceError` anywhere in its `%w` chain, such as storage being temporarily unavailable) is retried with backoff until the request deadline.
- If the request's own deadline expires while the transform runs, the result is `DEADLINE_EXCEEDED`.
- If `--request-timeout`, which limits one attempt, stops the transform while the request's deadline is still ahead, the request is retried with backoff.
- If the processor shuts down while the transform runs, the request is re-enqueued and the interrupted attempt is not counted.
- If the producer cancels the request while the transform runs, the result is `CANCELLED` and the request is not retried.
- Any other error fails the request with `INVALID_REQUEST`.

For an `api.ClientError`, the error result, the span and the retry log carry its category and message but not the cause it wraps in `RawError`, so a plugin can keep an SDK error (with request IDs or signed URLs) for `errors.Is`/`errors.As` without it reaching results or traces. This only drops `RawError`: other errors, and any text wrapped around the `ClientError`, are reported as they are. The span records the error's own category, while the result code stays `INVALID_REQUEST`.

## `gcs_uri_multipart` plugin

Rewrites a JSON body into `multipart/form-data` for endpoints that take a signed object URL. Because producers can't put raw media bytes on the broker, the queued `payload` carries a signed HTTPS URL (e.g. a GCS V4 signed URL) in a `gcs_uri` field.

- **`New`:** requires a non-empty `providers` list.
- **`Validate`:** if `metadata.provider` is in `providers` and `payload.gcs_uri` is non-empty, requires an absolute HTTPS URL. When the URL has a parseable expiry (V4 `X-Goog-Date` + `X-Goog-Expires`, or V2 `Expires`), it rejects URLs that expire at or before the original request deadline. A provider mismatch or missing `gcs_uri` passes validation.
- **`Transform`:** for a matching provider and non-empty `gcs_uri`, requires an absolute HTTPS URL, writes the URL as a plain multipart `url` field, passes remaining payload fields as form fields, and removes `gcs_uri`. JSON strings are unquoted; other JSON values, including arrays, keep their JSON text. A non-empty `file_base64` is rejected; inline media isn't supported on this path. A provider mismatch or missing `gcs_uri` returns `handled=false`.

## `gcs_file_multipart` plugin

Rewrites an audio JSON request into `multipart/form-data` with the actual GCS object bytes in the `file` part. The queued `payload` carries a signed HTTPS GCS URL in `gcs_uri`.

- **`New`:** requires non-empty `providers` and `allowedHosts`, plus a positive `maxFileBytes`. `allowedHosts` can be a hostname array or a JSON string holding one, and `maxFileBytes` can be an integer or an integer string, as in the example below. It builds an HTTP client whose redirect policy checks each redirect target.
- **`Validate`:** is a no-op because the legacy method has no request URL. `TransformContext` performs request matching and URL validation.
- **`TransformContext`:**
  - **Request matching:** handles requests whose `metadata.provider` is in `providers`, whose URL path is `/v1/audio/transcriptions` or `/v1/audio/translations`, and whose payload has a non-empty `gcs_uri`. Other requests return `handled=false`.
  - **URL validation:** requires a V4-signed HTTPS URL on an `allowedHosts` hostname, with expiry later than the original request deadline (or the attempt deadline when the request has none). A present `gcs_uri` must be a string. RSA signatures must be non-empty hex; HMAC-SHA256 signatures must be 32-byte hex. Path-style URLs (`storage.googleapis.com/BUCKET/OBJECT`) and virtual-hosted URLs (`BUCKET.storage.googleapis.com/OBJECT`) are accepted; list the exact hostname the URL uses in `allowedHosts`. Only hosts ending in `.storage.googleapis.com` are recognized as virtual-hosted; custom CNAME hosts are treated as path-style and need `/BUCKET/OBJECT`. Unsigned URLs, `gs://` URIs, unapproved hosts, and bad signature formats fail as invalid requests.
  - **Download:** performs a context-aware `GET` and streams at most `maxFileBytes` directly into the multipart `file` part. A request that also contains `file` or a non-empty `file_base64` is rejected; a present `file_base64` must be a string. The signed URL and GCS credentials aren't copied into the multipart body.
  - **Multipart fields:** remaining payload fields become form fields; arrays become repeated `name[]` fields. `gcs_uri` is removed.
- **Errors:** missing objects and other 4xx responses, except 408 and 429, are invalid requests. 408, 429, 5xx, and connection failures (including dial and TLS timeouts) are retryable. Request cancellation and shutdown stop the download.
- **Timeout and capacity:** the download shares the per-attempt `--request-timeout` with the inference call. If that timeout ends the download while the request deadline is still ahead, the request is retried with backoff. If the request deadline expires, it reports `DEADLINE_EXCEEDED`. The download runs after gate admission, holds the in-flight slot, and is not included in the inference-latency metric.
- **Memory:** the multipart body is buffered in memory, at roughly `maxFileBytes` per active transform. The example below sets `maxFileBytes` to 4 MiB; with the chart's default concurrency of 64, file bodies use about 256 MiB before multipart and buffer overhead. Lower concurrency or raise `ap.resources.limits.memory` for larger files.

Example:

```yaml
ap:
  transformConfig:
    requestTransforms:
      - name: gcs-file-audio
        type: gcs_file_multipart
        parameters:
          providers: ["whisper-file"]
          allowedHosts: '["storage.googleapis.com"]'
          maxFileBytes: "4194304"
      - name: gcs-url-audio
        type: gcs_uri_multipart
        parameters:
          providers: ["whisper-url"]
```

Other transform plugin types can be added as additional `requestTransforms` entries in the example.
