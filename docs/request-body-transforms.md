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

## `s3_multipart` plugin

Turns JSON media requests into the `multipart/form-data` bodies their endpoints require, resolving media from S3-compatible object storage (AWS S3, SeaweedFS, MinIO, ...). It picks requests by the path of the URL they're dispatched to and leaves every other request untouched.

| Endpoint path | What the plugin does |
| --- | --- |
| `.../v1/audio/transcriptions`, `.../v1/audio/translations` (vLLM) | Reads `file`, an `s3://bucket/key` reference or a base64 `data:` URL, and sends its bytes as the multipart `file` part. Every other field becomes a form field. |

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
- **Form fields:** JSON strings are sent unquoted; arrays become repeated `name[]` fields (e.g. `timestamp_granularities[]`); `null` fields are omitted; other values (numbers, booleans, objects such as `vllm_xargs`) are sent as their JSON text.
- **Failures:** storage errors the AWS SDK itself retries are retried until the request deadline (see [Failures](#failures)): throttling (`SlowDown`, 429), server errors (500/502/503/504), S3 `RequestTimeout`, network timeouts, refused or reset connections, and a download cut off mid-read. These fail the request: a missing or non-string `file`, any other URL scheme, a bucket outside `allowedBuckets`, a non-base64 `data:` URL or an invalid media type, a file over `maxFileBytes`, a missing object (404), denied access (401/403), and missing credentials.
