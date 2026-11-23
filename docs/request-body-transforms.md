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

## `s3_multipart` plugin

Turns JSON media requests into the `multipart/form-data` bodies their endpoints require, resolving media from S3-compatible object storage (AWS S3, SeaweedFS, MinIO, ...). It picks requests by the path of the URL they're dispatched to and leaves every other request untouched.

| Endpoint path | What the plugin does |
| --- | --- |
| `.../v1/audio/transcriptions`, `.../v1/audio/translations` (vLLM) | Reads `file`, an `s3://bucket/key` reference or a base64 `data:` URL, and sends its bytes as the multipart `file` part. Every other field becomes a form field. |
| `.../v1/images/edits` (vllm-omni) | Maps each `images[].image_url` to a `url` form field, which vllm-omni fetches itself: `s3://bucket/key` references are presigned, base64 `data:image/` URLs pass through, and any other URL is rejected. Every other field becomes a form field. The worker never downloads the image. |

`/v1/audio/speech` and `/v1/images/generations` already take JSON, so they need no transform; the plugin leaves them unchanged.

```json
{
  "requestTransforms": [
    {
      "name": "media",
      "type": "s3_multipart",
      "parameters": {
        "endpoint": "http://seaweedfs.batch.svc:8333",
        "region": "us-east-1",
        "usePathStyle": true,
        "allowedBuckets": ["batch-files"]
      }
    }
  ]
}
```

| Parameter | Default | Meaning |
| --- | --- | --- |
| `allowedBuckets` | required | The only buckets an `s3://` reference may name. Other buckets are rejected. |
| `endpoint` | AWS S3 | S3 API URL of a non-AWS store. |
| `region` | `AWS_REGION` | Signing region. Startup fails if neither is set. |
| `usePathStyle` | `false` | Address objects as `<endpoint>/<bucket>/<key>`, which most S3-compatible stores require. |
| `maxFileBytes` | 25 MiB | Largest file read into a request body, matching vLLM's default `VLLM_MAX_AUDIO_CLIP_FILESIZE_MB`. |

- **Credentials** come from the AWS SDK default chain, never from the parameters: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, shared config files, or a workload identity. With the Helm chart, set them through `ap.env` from a Secret. Only read access to the allowed buckets is needed.
- **`allowedBuckets` is a bucket boundary, not per-user authorization.** Any reference to an object in an allowed bucket is fetched with the processor's credentials. When several users or tenants share a bucket, the producer must ensure each request only references objects its caller may read (e.g. batch-gateway checking the key is under the job's tenant prefix).
- **Prefer `s3://` for anything but small clips.** A `data:` URL carries the audio inside the queued message (base64, about 4/3 of the file size), so the transport stores it and the worker holds it in memory along with the request body.
- **Presigned URLs** are signed for the configured `endpoint` host, so vllm-omni must reach the store at that same address (in-cluster, the store's Service name). Each URL stays valid until the dispatch attempt's deadline: `--request-timeout` or the request's own deadline, whichever comes first, capped at S3's 7 days. Every retry signs a fresh URL. Cached expiring credentials (STS, workload identity) with less than 10 minutes left are refreshed before signing, and a URL never outlives the credentials that signed it.
- **Image edits accept `image_url` only.** OpenAI's `file_id` image inputs are rejected, and so is a `mask`, which vllm-omni would otherwise silently ignore. vllm-omni fetches every image source without a host allowlist, so anything but an allowlisted `s3://` or a `data:image/` URL is rejected. So are caller-supplied `url`, `url[]`, `mask_image` and `reference_image` fields.
- **Form fields:** JSON strings are sent unquoted; arrays become repeated `name[]` fields (e.g. `timestamp_granularities[]`); `null` fields are omitted; other values (numbers, booleans, objects such as `vllm_xargs`) are sent as their JSON text.
- **Failures:** storage errors the AWS SDK itself retries are retried until the request deadline (see [Failures](#failures)): throttling (`SlowDown`, 429), server errors (500/502/503/504), S3 `RequestTimeout`, network timeouts, refused or reset connections, and a download cut off mid-read. These fail the request: a missing or non-string `file`, any other URL scheme, a bucket outside `allowedBuckets`, a non-base64 `data:` URL or an invalid media type, a file over `maxFileBytes`, a missing object (404), denied access (401/403), and missing credentials.
