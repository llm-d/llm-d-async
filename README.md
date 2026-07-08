# Async Processor (AP) - User Guide

## Overview
**The Problem:** High-performance accelerators often suffer from low utilization in strictly online serving scenarios, or users may need to mix latency-insensitive workloads into slack capacity without impacting primary online serving.

**The Value:** This component enables efficient processing of requests where latency is not the primary constraint (i.e., the magnitude of the required SLO is ≥ minutes). <br>
By utilizing an asynchronous, queue-based approach, users can perform tasks such as product classification, bulk summarizations, summarizing forum discussion threads, or performing near-realtime sentiment analysis over large groups of social media tweets without blocking real-time traffic.

**Architecture Summary:** The Async Processor is a composable component that provides services for managing these requests. It functions as an asynchronous worker that pulls jobs from a message queue and dispatches them to an inference gateway, decoupling job submission from immediate execution.

## When to Use
• **Latency Insensitivity:** Suitable for workloads where immediate response is not required.

• **Capacity Optimization:** Useful for filling "slack" capacity in your inference pool.


## Design Principles

The architecture adheres to the following core principles:

1. **Bring Your Own Queue (BYOQ):** All aspects of prioritization, routing, retries, and scaling are decoupled from the message queue implementation.

2. **Composability:** The end-user does not interact directly with the processor via an API. Instead, the processor interacts solely with the message queues, making it highly composable with offline batch processing and asynchronous workflows.

3. **Resilience by Design:** If real-time traffic spikes or errors occur, the system triggers intelligent retries for jobs, ensuring they eventually complete without manual intervention.


## Table of Contents

- [Async Processor (AP) - User Guide](#async-processor-ap---user-guide)
  - [Overview](#overview)
  - [When to Use](#when-to-use)
  - [Design Principles](#design-principles)
  - [Table of Contents](#table-of-contents)
  - [Deployment](#deployment)
  - [Command line parameters](#command-line-parameters)
  - [Dispatch Gates](#dispatch-gates)
    - [Per-Queue Dispatch Gates](#per-queue-dispatch-gates)
  - [Request Messages and Consumption](#request-messages-and-consumption)
    - [Request Merge Policy](#request-merge-policy)
  - [Request Body Transforms](#request-body-transforms)
    - [`gcs_uri_multipart` plugin](#gcs_uri_multipart-plugin)
  - [Retries](#retries)
  - [Observability](#observability)
    - [OpenTelemetry Tracing](#opentelemetry-tracing)
    - [Prometheus Metrics](#prometheus-metrics)
  - [Results](#results)
  - [Metrics](#metrics)
  - [Implementations](#implementations)
    - [Redis Sorted Set (Persisted)](#redis-sorted-set-persisted)
      - [Redis Sorted Set Command line parameters](#redis-sorted-set-command-line-parameters)
    - [Redis Channels (Ephemeral)](#redis-channels-ephemeral)
      - [Redis Channels Command line parameters](#redis-channels-command-line-parameters)
      - [Multiple Queues Configuration File Syntax](#multiple-queues-configuration-file-syntax)
    - [GCP Pub/Sub](#gcp-pubsub)
      - [GCP PubSub Command line parameters](#gcp-pubsub-command-line-parameters)
      - [Multiple Topics Configuration File Syntax](#multiple-topics-configuration-file-syntax)
  - [Development](#development)

## Deployment

To deploy the Async Processor into your K8S cluster, follow these steps:
- Create an `.env` file with `export` statements overrides. E.g.:
```bash
IMAGE_TAG_BASE=<if needed to override for a private registry>
DEPLOY_LLM_D=false
DEPLOY_REDIS=false
DEPLOY_PROMETHEUS=false
AP_IMAGE_PULL_POLICY=Always
```
- Run:
```bash
make deploy-ap-on-k8s
```
- To test a request (only for the Redis implementation):
    - Subscribing to the result channel (different terminal window):
    ```bash
       export REDIS_IP=....
       kubectl run -i -t subscriberbox --rm --image=redis --restart=Never -- /usr/local/bin/redis-cli -h $REDIS_IP SUBSCRIBE result-queue
    ```
    - Publishing a request:
    ```bash
       export REDIS_IP=....
       kubectl run --rm -i -t publishmsgbox --image=redis --restart=Never -- /usr/local/bin/redis-cli -h $REDIS_IP PUBLISH request-queue '{"id" : "testmsg", "payload":{ "model":"food-review-1", "prompt":"Hi, good morning "}, "deadline" :23472348233323 }'
     ```

## Command line parameters
- `concurrency`: The number of concurrent workers (per pool if unspecified), default is 8.
- `request-merge-policy`: Currently only supporting <u>random-robin</u> policy.
- `message-queue-impl`: Implementation of the queueing system. Options are <u>gcp-pubsub</u> for GCP PubSub, <u>gcp-pubsub-gated</u> for GCP PubSub with per-topic gating, <u>redis-sortedset</u> for Redis Sorted Set (persisted and sorted), and <u>redis-pubsub</u> for ephemeral Redis-based implementation.
- `pool-config-file`: Path to the JSON configuration file containing the worker pool definitions. If omitted, a single `"default"` worker pool is created with concurrency determined by the global `concurrency` flag.

 - `prometheus-url`: Prometheus server URL for metric-based gates (e.g., http://localhost:9090). For Google Managed Prometheus (GMP), point this to a local proxy or GMP frontend that handles authentication — direct GMP URLs are not supported as the Async Processor does not perform GMP authentication.
   This flag is required when using metric-based per-queue gates (e.g., `prometheus-saturation`, `prometheus-budget`).
 - `prometheus-cache-ttl`: TTL for cached Prometheus metric sources (e.g. 1m, 0s to disable). Default is 5s. Increasing this reduces Prometheus load but also reduces the responsiveness of dispatch gates to metric changes.

<i>additional parameters may be specified for concrete message queue implementations</i>

## Worker Pools Configuration

When using multiple queues or topics, the worker capacities and pool-level gates for named pools can be configured via a dedicated worker pools file.

**JSON Schema:**
```json
[
  {
    "id": "qwen-pool",
    "workers": 4,
    "gate_type": "local-max-concurrency",
    "gate_params": {
      "limit": "2"
    }
  }
]
```

**Fields:**
- `id` (required): Unique pool identifier referenced by queue/topic configurations.
- `workers` (required): Number of concurrent workers dedicated to this pool. Must be positive.
- `gate_type` (optional): The type of dispatch gate to apply to the pool (e.g. `local-max-concurrency`, `prometheus-saturation`).
- `gate_params` (optional): Key-value parameters configuring the gate.

## Dispatch Gates

The Async Processor supports dispatch gates to control batch processing based on system capacity. Gates can be configured at two levels:
1. **Per-Queue Gates** (configured in the queue/topic config file).
2. **Per-Pool Gates** (configured in the worker pools config file).

### Difference between Queue and Pool Gates

* **Queue-level gates** run at the admission phase for a specific queue. When a queue-level gate denies admission (returning `ActionRefuse`), the request is immediately returned to the broker to be retried/re-delivered, freeing the worker to process other queues.
* **Pool-level gates** run directly inside the worker loop to regulate capacity constraints shared by all queues routing to that worker pool. When a pool-level gate returns `ActionWait`, the worker parks in-memory and polls until capacity is available, avoiding broker nack/retry overhead. If the pool-level gate returns `ActionRefuse`, the request is immediately returned to the broker.

### Per-Queue Dispatch Gates

For more fine-grained control, configure gates per queue in your configuration file. Each queue can have its own gate type and parameters.

**Gate Types:**

- `constant`: Always returns budget 1.0 (fully open) - no throttling.
- `redis`: Queries Redis for dispatch budget (managed by external system).
- `redis-quota`: Per-attribute quota management via Redis.
- `local-max-concurrency`: Limits the number of concurrent in-flight requests processed from a queue locally using thread-safe, in-process state.
- `composite`: Combines multiple gates. Returns the minimum budget across all inner dispatch gates and acquires quota across all inner attribute gates (all or nothing).
- `wait-on-refuse`: Decorator that wraps a single inner gate and converts any `ActionRefuse` verdict into `ActionWait` (parking/polling in-memory instead of immediate broker redelivery).
- `prometheus-saturation`: Queries Prometheus for pool saturation metric. The gate closes (returns `0.0`) when saturation ≥ threshold; when open it returns `(1 - saturation) - (1 - threshold)`, i.e. the margin below the threshold.
- `prometheus-budget`: Computes a dispatch budget D using a cascade of two Prometheus metric sources. Both sources compute `max_SYS = ready_pods × max_concurrency` dynamically. The primary source uses the EPP flow control queue size: `D = 1 − (queue_size / max_SYS)`. If the primary is unavailable, it falls back to a secondary source using vLLM and pool metrics: `D = 1 − (running_requests / max_SYS)`. The gate closes when D ≤ B (baseline); callers compute `N = max_SYS × (D − B)`. See [docs/dispatch-budget.md](docs/dispatch-budget.md) for details.
- `prometheus-query`: Evaluates an arbitrary user-supplied PromQL expression as the dispatch budget. The expression must resolve to an instant vector with a single sample whose value is in [0, 1]. Unlike `prometheus-saturation` and `prometheus-budget`, this gate does not construct queries internally — the user provides the complete PromQL expression. Values outside [0, 1] are clamped.
- `endpoint-scrape`: Scrapes a raw Prometheus `/metrics` endpoint directly (no Prometheus server required). Extracts a named metric, computes saturation, and returns the available budget. Supports two modes: **direct saturation** (metric value is already in [0, 1], e.g., from the EPP) and **computed saturation** (raw count divided by `max_count_per_pod`, e.g., `vllm:num_requests_waiting`). Optionally scrapes a second endpoint for dynamic pod count.

**Example Configuration with Per-Queue Gates:**

```json
[
    {
       "queue_name": "critical_queue",
       "inference_objective": "critical-task",
       "request_path_url": "/v1/completions",
       "igw_base_url": "http://localhost:80/",
       "worker_pool_id": "inference_pool_1",
       "gate_type": "constant"
    },
    {
       "queue_name": "batch_queue",
       "inference_objective": "batch-task",
       "request_path_url": "/v1/completions",
       "igw_base_url": "http://localhost:80/",
       "worker_pool_id": "inference_pool_1",
       "gate_type": "prometheus-saturation",
       "gate_params": {
          "pool": "inference_pool_1",
          "threshold": "0.8"
       }
    },
    {
       "queue_name": "batch_budget_queue",
       "inference_objective": "batch-task",
       "request_path_url": "/v1/completions",
       "igw_base_url": "http://localhost:80/",
       "worker_pool_id": "inference_pool_1",
       "gate_type": "prometheus-budget",
       "gate_params": {
          "pool": "inference_pool_1",
          "max_concurrency": "100",
          "baseline": "0.05"
       }
    },
    {
       "queue_name": "redis_gated_queue",
       "inference_objective": "gated-task",
       "request_path_url": "/v1/completions",
       "igw_base_url": "http://localhost:8000/",
       "worker_pool_id": "inference_pool_2",
       "gate_type": "redis",
       "gate_params": {
          "address": "localhost:6379",
          "budget_key": "my-budget-key"
       }
    },
    {
       "queue_name": "custom_metric_queue",
       "inference_objective": "custom-task",
       "request_path_url": "/v1/completions",
       "igw_base_url": "http://localhost:8000/",
       "worker_pool_id": "inference_pool_2",
       "gate_type": "prometheus-query",
       "gate_params": {
          "query": "1 - (sum(rate(http_requests_total{job=\"inference\"}[5m])) / 100)",
          "fallback": "0.0"
       }
    },
    {
       "queue_name": "composite_gated_queue",
       "inference_objective": "composite-task",
       "request_path_url": "/v1/completions",
       "igw_base_url": "http://localhost:80/",
       "worker_pool_id": "inference_pool_1",
       "gate_type": "composite",
       "gate_params": {
          "gates": "[{\"gate_type\":\"prometheus-saturation\",\"gate_params\":{\"pool\":\"inference_pool_1\"}},{\"gate_type\":\"redis-quota\",\"gate_params\":{\"address\":\"localhost:6379\",\"limit\":\"100\"}}]"
       }
    },
    {
       "queue_name": "scrape_gated_queue",
       "inference_objective": "batch-task",
       "request_path_url": "/v1/completions",
       "igw_base_url": "http://localhost:80/",
       "gate_type": "endpoint-scrape",
       "gate_params": {
          "url": "http://vllm-sim:8000/metrics",
          "metric": "vllm:num_requests_waiting",
          "max_count_per_pod": "5",
          "fallback": "1.0"
       }
    }
]
```

**Gate Parameters:**

- `composite`:
  - `gates` (**required**): A JSON array of gate configurations. Each configuration is an object with `gate_type` and `gate_params`.

- `wait-on-refuse`:
  - `gate` (**required**): A JSON string containing a single gate configuration (with `gate_type` and `gate_params`) to wrap. This can be used to wrap prometheus gates in pool configuration so that they park requests instead of redelivering them to the message broker when the gate is saturated.

- `redis`:
  - `address` (**required**): Redis server address for the dispatch gate (e.g., `localhost:6379`). Queues sharing the same address will share the same connection pool.
  - `budget_key` (optional): Redis key to read dispatch budget from. Default is `dispatch-gate-budget`.

- `redis-quota`:
  - `address` (**required**): Redis server address.
  - `attribute` (optional): The message attribute to use for quota (e.g., `userid`). Default is `userid`.
  - `mode` (optional): `rate-limit` or `concurrency`. Default is `rate-limit`.
  - `limit` (**required**): The quota limit.
  - `window` (optional): The time window for rate limiting (e.g., `1m`, `10s`). Default is `1m`.
  - `prefix` (optional): Redis key prefix. Default is `quota:`.
  - `gating_mode` (optional): `blocking` or `classifying`. In `classifying` mode, the gate never blocks but tags the message with its quota status ("reserved" or "overflow") in the internal metadata. Default is `blocking`.

- `local-max-concurrency`:
  - `limit` (**required**): The maximum number of concurrent requests allowed in-flight for this queue. Must be a positive integer.

- `prometheus-saturation`:
  - `pool` (**required**): The inference pool name to filter metrics by.
  - `namespace` (optional): Kubernetes namespace to scope metric queries. Required when multiple namespaces share the same pool name with a shared Prometheus instance.
  - `threshold` (optional): Saturation threshold (0.0-1.0). When saturation >= threshold, budget is 0.0. Default is `0.8`.
  - `fallback` (optional): Fallback saturation value (0.0-1.0) used when the metric source returns an error or empty data. Default is `0.0`.

  **Metric prerequisites:** The primary metric source requires llm-d's flow control plugin to be
  enabled: without it, the EPP flow control metrics will be missing and the gate will always use the fallback value.

- `prometheus-budget`: Cascades two Prometheus metric sources to compute a dispatch budget D.
  Both sources compute `max_SYS = ready_pods × max_concurrency` dynamically from the `inference_pool_ready_pods` metric.
  The primary source computes D from the EPP's flow control queue size: `D = 1 − (queue_size / max_SYS)`.
  If the primary metric is unavailable (e.g. EPP is down), the gate falls back to a secondary source
  that estimates saturation from vLLM and pool metrics: `D = 1 − (running_requests / max_SYS)`.
  The gate closes when `D ≤ baseline`; when open it returns `D − baseline`, so callers compute `N = max_SYS × (D − B)`.
  See [docs/dispatch-budget.md](docs/dispatch-budget.md) for the full derivation.

  - `pool` (**required**): The InferencePool name. This must match both the `name` field in
    `inference_pool_ready_pods{name="<pool>"}` (EPP metric) and, for the vLLM fallback,
    the `inference_pool` label on scraped vLLM metrics (added via relabeling from pod labels).
  - `namespace` (optional): Kubernetes namespace to scope metric queries. Required when multiple namespaces share the same pool name with a shared Prometheus instance.
  - `max_concurrency` (optional): Per-endpoint request capacity (`MaxConcurrency` in the [inference scheduler's saturation detector](https://github.com/llm-d/llm-d-inference-scheduler/blob/main/pkg/epp/framework/plugins/flowcontrol/saturationdetector/concurrency/config.go)). Default is `100` (matching the inference scheduler default).
  - `baseline` (optional): Reserved baseline B. The gate closes when D ≤ B. Default is `0.05`.
  - `fallback` (optional): Fallback budget value (0.0-1.0) returned when all metric sources are unavailable. Default is `0.0` (fail closed).

  **Metric prerequisites:** The primary metric source requires llm-d's flow control plugin to be
  enabled; without it, the gate falls back to vLLM metrics. The fallback filters by `inference_pool` label,
  which vLLM does not emit natively: configure Prometheus relabeling to propagate it from model server pod labels
  (the helm chart handles this):

  ```yaml
  relabelings:
    - sourceLabels: [__meta_kubernetes_pod_label_inference_pool]
      targetLabel: inference_pool
  ```

- `prometheus-query`: Evaluates a user-supplied PromQL expression directly as the dispatch budget.
  The expression must resolve to a Prometheus instant vector with a single sample whose value is in [0, 1].
  Values outside this range are clamped.

  - `query` (**required**): The PromQL expression to evaluate. This is sent to Prometheus exactly as provided.
    The result is used directly as the dispatch budget (no transformation is applied).
  - `fallback` (optional): Fallback budget value (0.0-1.0) returned when the query fails or returns no data.
    Default is `0.0` (fail closed).

- `endpoint-scrape`: Scrapes a raw Prometheus text-format `/metrics` endpoint directly.
  Computes budget as `clamp(1 - saturation - baseline, 0, 1)`.

  - `url` (**required**): Full URL to scrape (e.g., `http://vllm-sim:8000/metrics`).
  - `metric` (**required**): Metric name to extract (e.g., `vllm:num_requests_waiting`).
  - `labels` (optional): JSON object of label filters (e.g., `{"model_name":"my-model"}`). Only samples matching all labels are used.
  - `max_count_per_pod` (optional): Per-pod capacity. When > 0, saturation = `value / max_count`. When 0, the metric value is used directly as saturation (assumed to be in [0, 1]). Default is `0`.
  - `baseline` (optional): Reserved headroom subtracted from budget. Default is `0.0`.
  - `fallback` (optional): Budget returned when scrape fails or metric is missing. Default is `0.0` (fail closed).
  - `pods_url` (optional): URL to scrape for dynamic pod count (e.g., `http://epp-svc:9090/metrics`). When set with `pods_metric`, `max_count = ready_pods * max_count_per_pod`.
  - `pods_metric` (optional): Metric name for ready pods (e.g., `inference_pool_ready_pods`).
  - `pods_labels` (optional): JSON label filters for the pods metric (e.g., `{"name":"my-pool"}`).

  **No Prometheus server required.** This gate scrapes endpoints directly, making it suitable for
  deployments without a dedicated Prometheus instance. Use `max_count_per_pod` with `pods_url`/`pods_metric`
  for dynamic scaling, or set `max_count_per_pod` to a static value for single-pod setups.

## Request Messages and Consumption

The async processor expects request messages to have the following format:

```json
{
    "id": "unique identifier for result mapping",
    "created": "created timestamp in Unix seconds",
    "deadline": "deadline in Unix seconds",
    "payload": {"regular inference payload"}
}
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier for result mapping (required) |
| `created` | int64 | Created timestamp in Unix seconds |
| `deadline` | int64 | Deadline in Unix seconds (required, must be positive) |
| `payload` | object | Inference request payload |
| `metadata` | map[string]string | Optional caller-supplied pass-through data (e.g. tracing IDs, user labels) |
| `endpoint` | string | Optional per-request dispatch path; overrides the queue-level default when set |

**Example:**

```json
{
    "id": "19933123533434",
    "created": 1764044000,
    "deadline": 1764045130,
    "payload": {"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
    "metadata": {"user": "batch-job-42"}
}
```

Producers handle wrapping these into the internal wire format used for persistence and routing.

### Request Merge Policy

The Async Processor supports consuming from multiple request message queues concurrently. A `Request Merge Policy` merges messages from all active queues.

Instead of performing a single global merge, the policy groups input channels by their configured `worker_pool_id` and performs the merging per-pool. This returns a `PoolDispatch` mapping, where **each worker pool has its own independent merged channel**.

This per-pool topology provides complete backpressure and queue-level isolation: a slow or saturated pool will block only its own merged channel, leaving other pools completely unaffected and free to process requests.

Currently the only policy supported is `Random Robin Policy` which randomly picks messages from all queues configured for a given pool.

## Request Body Transforms

By default the worker dispatches the OpenAI-style JSON marshalled from a request's `payload`. Some providers need a different body shape at dispatch time — for example multi-modal endpoints (Whisper transcription, OCR) that expect `multipart/form-data` with a `url` field rather than JSON. Request body-transform plugins handle this without special-casing the worker: they rewrite the outgoing body and `Content-Type` based on per-message `metadata`, and the default JSON path is preserved byte-for-byte when no plugin applies.

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

### `gcs_uri_multipart` plugin

Rewrites a JSON body into `multipart/form-data` for endpoints that take a signed object URL. Because producers can't put raw media bytes on the broker, the queued `payload` carries a signed URL (e.g. a GCS V4 signed URL) in a `gcs_uri` field.

- **Activation:** the message's `metadata.provider` must match one of the configured `providers`, and the `payload` must contain a non-empty `gcs_uri`. Otherwise the default JSON path is used unchanged.
- **Transform:** writes the `gcs_uri` value as a `url` form field (a plain field, not a file upload), passes the remaining payload fields through as form fields, and drops `gcs_uri`. A non-empty `file_base64` is rejected as a fatal, non-retryable error (inline media is not supported on this path).
- **Preflight:** parses the signed URL's expiry (V4 `X-Goog-Date` + `X-Goog-Expires`, or V2 `Expires`); if it expires at or before the message deadline, the request fails fatally before dispatch so the broker doesn't retry a request that cannot succeed.

## Retries

When a message processing has failed, either shedded or due to a server-side error, it will be scheduled for a retry (assuming the deadline has not passed).

## Observability

### OpenTelemetry Tracing

The Async Processor supports distributed tracing via [OpenTelemetry](https://opentelemetry.io/). When enabled, it exports traces to an OTLP-compatible collector (e.g., Jaeger, Grafana Tempo, OpenTelemetry Collector).

**Spans emitted:**

| Span Name | Description |
|-----------|-------------|
| `process-request` | Per-request span covering validation, dispatch, and result routing |
| `http-request` | Child span for the outgoing HTTP call to the inference gateway (via `otelhttp`) |
| `re-enqueue` | Linked span created when a request is re-enqueued during graceful shutdown |

**Span attributes:**

| Attribute | Description |
|-----------|-------------|
| `request.id` | Request identifier |
| `queue.id` | Queue identifier (matches Prometheus `queue_id` label) |
| `queue.name` | Queue name (matches Prometheus `queue_name` label) |
| `retry.count` | Current retry attempt (0 for first attempt) |
| `error.category` | Error classification on failure (`RATE_LIMIT`, `SERVER_ERROR`, `UNKNOWN`, etc.) |

**Trace context propagation:**

Producers can inject W3C Trace Context (`traceparent`/`tracestate`) and Baggage into the request's `metadata` field. The processor extracts it and creates child spans under the producer's trace, enabling end-to-end distributed tracing across the queue boundary.

```json
{
    "id": "req-123",
    "deadline": 1764045130,
    "payload": {"model": "my-model", "prompt": "hello"},
    "metadata": {
        "traceparent": "00-a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6-1234567890abcdef-01"
    }
}
```

The processor also injects trace context into outgoing inference requests via W3C headers, so the inference gateway can continue the trace.

**Configuration:**

Tracing is controlled via standard OpenTelemetry environment variables. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to enable; leave it empty to disable (no-op).

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC collector endpoint (e.g., `http://jaeger:4317`). Empty disables tracing. | _(disabled)_ |
| `OTEL_EXPORTER_OTLP_INSECURE` | Use plaintext gRPC connection | `true` |
| `OTEL_SERVICE_NAME` | Service name for traces | `async-processor` |
| `OTEL_TRACES_SAMPLER` | Sampling strategy (`always_on`, `parentbased_traceidratio`, etc.) | `parentbased_traceidratio` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampling ratio (0.0–1.0) | `1.0` |

**CLI flag:**

- `--redis-tracing`: Enable per-command Redis tracing spans via `redisotel`. Produces high span volume — use only for debugging. Default: `false`.

**Helm chart:**

```yaml
ap:
  otel:
    endpoint: "http://jaeger:4317"  # leave empty to disable
    insecure: true
    sampler: "parentbased_traceidratio"
    samplerArg: "1.0"
    redisTracing: false
```

### Prometheus Metrics

The processor exports the following Prometheus metrics on the metrics port (default `9090`). All counters and histograms carry `queue_id` and `queue_name` labels for per-queue visibility.

| Metric | Type | Description |
|--------|------|-------------|
| `llm_d_async_async_request_total` | Counter | Total number of new requests (first attempt only) |
| `llm_d_async_async_request_retries_total` | Counter | Total retry attempts |
| `llm_d_async_async_successful_requests_total` | Counter | Successful requests |
| `llm_d_async_async_failed_requests_total` | Counter | Failed requests (fatal errors) |
| `llm_d_async_async_exceeded_deadline_requests_total` | Counter | Requests that exceeded their deadline |
| `llm_d_async_async_shedded_requests_total` | Counter | Rate-limited/shed requests (429) |
| `llm_d_async_async_message_latency_time_millis` | Histogram | Message latency (Pub/Sub only) |
| `llm_d_async_async_inference_latency_time_millis` | Histogram | Time spent calling the inference gateway (IGW), isolating model time from queue time |
| `llm_d_async_async_queue_residence_time_millis` | Histogram | Time a message spent buffered in-process from broker ingestion until a worker pulled it |

**Labels:**

| Label | Description |
|-------|-------------|
| `queue_id` | Queue identifier (from queue config `id` field, defaults to queue name) |
| `queue_name` | Queue name (Redis sorted set name, channel name, or Pub/Sub subscriber ID) |

## Results

Results will be written to the results queue and will have the following structure:

```json
{
    "id" : "id mapped to the request",
    "payload" : byte[]{/*inference result payload*/} ,
    // or
    "error" : "error's reason"
}
```

## Metrics

The Async Processor exposes Prometheus metrics under the `llm_d_async` subsystem. All counters and histograms carry `queue_id` and `queue_name` labels so you can filter and aggregate per queue.

| Metric | Type | Description |
|--------|------|-------------|
| `llm_d_async_async_request_total` | Counter | New async requests (first attempt only) |
| `llm_d_async_async_successful_requests_total` | Counter | Requests that received a successful inference response |
| `llm_d_async_async_failed_requests_total` | Counter | Requests that failed with a fatal or non-retryable error |
| `llm_d_async_async_shedded_requests_total` | Counter | Requests shedded due to rate limiting (429 / capacity) |
| `llm_d_async_async_exceeded_deadline_requests_total` | Counter | Requests that exceeded their deadline before completion |
| `llm_d_async_async_request_retries_total` | Counter | Retry attempts |
| `llm_d_async_async_message_latency_time_millis` | Histogram | End-to-end message latency in milliseconds (publish to successful processing). Only registered when the transport supports message latency (e.g., GCP Pub/Sub). |
| `llm_d_async_async_inference_latency_time_millis` | Histogram | Time in milliseconds spent calling the inference gateway (IGW), measured around each request attempt. Isolates "model time" from "queue time" and is always registered. |
| `llm_d_async_async_queue_residence_time_millis` | Histogram | Time in milliseconds a message spent buffered in-process, from broker ingestion until a worker pulled it for processing. Measures the async delay introduced by the system (queue time). Always registered. |
| `llm_d_async_async_dispatch_budget` | Gauge | Current dispatch budget [0.0–1.0] returned by the queue's gate; the fraction of system capacity available for new requests (0.0 = gate fully closed). Useful for diagnosing why throughput is throttled. |
| `llm_d_async_async_pool_worker_limit` | Gauge | Configured worker concurrency limit for a pool (carries only the `pool_name` label). Compare against `llm_d_async_async_inflight_requests` to compute worker utilization. |
| `llm_d_async_async_gate_decisions_total` | Counter | Count of gate decisions that prevented a message from being dispatched, by `reason`: `gate_closed` (no dispatch budget), `quota_exhausted` (per-attribute quota overflow), `dropped` (gate permanently rejected the request), `error` (gate evaluation failed). |

**Labels:**

| Label | Description |
|-------|-------------|
| `queue_id` | Transport-level queue identifier |
| `queue_name` | Logical queue name from the queue configuration |
| `pool_name` | Worker pool the queue routes to (`async_pool_worker_limit` carries only this label) |
| `reason` | Gate-decision reason (only on `async_gate_decisions_total`): `gate_closed`, `quota_exhausted`, `dropped`, `error` |

**Example PromQL queries:**

```promql
# Per-queue success ratio over the last 5 minutes
rate(llm_d_async_async_successful_requests_total[5m]) / rate(llm_d_async_async_request_total[5m])

# Which queues are getting rate-limited?
rate(llm_d_async_async_shedded_requests_total[5m])

# Retry ratio by queue
rate(llm_d_async_async_request_retries_total[5m]) / rate(llm_d_async_async_request_total[5m])

# p95 inference gateway latency by queue (model time, excluding queue time)
histogram_quantile(0.95, sum by (queue_name, le) (rate(llm_d_async_async_inference_latency_time_millis_bucket[5m])))

# p95 queue residence time by queue (async delay, excluding model time)
histogram_quantile(0.95, sum by (queue_name, le) (rate(llm_d_async_async_queue_residence_time_millis_bucket[5m])))
```

## Implementations

### Redis Sorted Set (Persisted)

A persisted implementation based on Redis SortedSets.

![Async Processor - Redis Sorted Set architecture](/docs/images/redis_sortedset_architecture.png "AP - Redis SortedSet")

#### Redis Sorted Set Command line parameters
- `redis.url`: Redis URL (e.g. `redis://user:pass@host:port/db` or `rediss://...` for TLS). Can also be set via `REDIS_URL` env var.
- `redis.ss.igw-base-url`: Base URL of the IGW (e.g. https://localhost:30800).<br> Mutually exclusive with `redis.ss.queues-config-file` flag.
- `redis.ss.request-path-url`: Request path url (e.g.: "/v1/completions"). <br> Mutually exclusive with `redis.ss.queues-config-file` flag.")
- `redis.ss.inference-objective`: InferenceObjective to use for requests (set as the HTTP header x-gateway-inference-objective if not empty).  <br> Mutually exclusive with `redis.ss.queues-config-file` flag.
- `redis.ss.request-queue-name`: The name of the sorted-set for the requests. Default is <u>request-sortedset</u>.  <br> Mutually exclusive with `redis.ss.queues-config-file` flag.
- `redis.ss.result-queue-name`: The name of the list for the results. Default is <u>result-list</u>.
- `redis.ss.queues-config-file`: The configuration file name when using multiple queues. <br> Mutually exclusive with `redis.ss.igw-base-url`, `redis.ss.request-queue-name`, `redis.ss.request-path-url` and `redis.ss.inference-objective` flags.
- `redis.ss.poll-interval-ms`: Poll interval in milliseconds. Default is <u>1000</u>.
- `redis.ss.batch-size`: Number of messages to process per poll. Default is <u>10</u>.
- `redis.ss.gate-type`: Gate type for single-queue mode (e.g., `redis`, `prometheus-saturation`). Only used when `redis.ss.queues-config-file` is not set.
- `redis.ss.gate-params`: JSON-encoded gate params map for single-queue mode (e.g., `{"address":"localhost:6379"}`). Only used when `redis.ss.queues-config-file` is not set.

### Redis Channels (Ephemeral)

<u>NOTE:</u> Consider using the [Redis Sorted Set](#redis-sorted-set-persisted) implementation for production use.
As it is offers persistence and priority sorting.

An example implementation based on Redis channels is provided.

- Redis Channels as the request queues.
- Redis Sorted Set as the retry exponential backoff implementation.
- Redis Channel as the result queue.


![Async Processor - Redis architecture](/docs/images/redis_pubsub_architecture.png "AP - Redis")

#### Redis Channels Command line parameters

- `redis.url`: Redis URL (e.g. `redis://user:pass@host:port/db` or `rediss://...` for TLS). Can also be set via `REDIS_URL` env var.
- `redis.igw-base-url`: Base URL of the IGW (e.g. https://localhost:30800).<br> Mutually exclusive with `redis.queues-config-file` flag.
- `redis.request-path-url`: Request path url (e.g.: "/v1/completions"). <br> Mutually exclusive with `redis.queues-config-file` flag.")
- `redis.inference-objective`: InferenceObjective to use for requests (set as the HTTP header x-gateway-inference-objective if not empty).  <br> Mutually exclusive with `redis.queues-config-file` flag.
- `redis.request-queue-name`: The name of the channel for the requests. Default is <u>request-queue</u>.  <br> Mutually exclusive with `redis.queues-config-file` flag.
- `redis.retry-queue-name`: The name of the channel for the retries. Default is <u>retry-sortedset</u>.
- `redis.result-queue-name`: The name of the channel for the results. Default is <u>result-queue</u>.
- `redis.queues-config-file`: The configuration file name when using multiple queues. <br> Mutually exclusive with `redis.igw-base-url`, `redis.request-queue-name`, `redis.request-path-url` and `redis.inference-objective` flags.

#### Multiple Queues Configuration File Syntax

The configuration file when using the `redis.queues-config-file` flag should have the following format:

```json
[
    {
       "queue_name": "some_queue_name",
       "igw_base_url": "http://localhost:30800",
       "worker_pool_id": "qwen-pool",
       "inference_objective": "some_inference_objective",
       "request_path_url": "/v1/completions",
       "labels": {
          "env": "prod",
          "team": "billing"
       }
    },
    {
       "queue_name": "another_queue",
       "igw_base_url": "http://localhost:8000/",
       "worker_pool_id": "llama-pool",
       "inference_objective": "batch_task",
       "request_path_url": "/v1/chat/completions"
    }
]
```

<u>Note:</u> The ephemeral Redis Channels implementation does not support per-queue dispatch gates. Use the [Redis Sorted Set](#redis-sorted-set-persisted) implementation for per-queue gating.

**Configuration Fields:**

- `queue_name`: The name of the Redis channel for this queue.
- `worker_pool_id` (optional): The ID of the worker pool to route to (defined in the worker pools configuration file). Defaults to `"default"` if omitted.
- `inference_objective`: The inference objective header value.
- `igw_base_url` (required): Base URL of the inference gateway or target model server for this queue.
- `request_path_url` (optional): Request path URL (e.g. `/v1/chat/completions`) for this queue.
- `labels` (optional): A map of key-value string pairs injected as routing metadata (`Labels`) into the `InternalRequest` envelope at ingestion/pull time.

### GCP Pub/Sub

The GCP PubSub implementation requires the user to configure the following:

- Requests Topic and a **Subscription** having the following configurations:
    - Exactly once delivery.
    - Retries with exponential backoff.
    - Dead Letter Queue (DLQ).
- Results Topic.

<u>Note:</u> If DLQ is NOT configured for the request topic. Retried messages will be counted multiple times in the #_of_requests metric.

![Async Processor - GCP PubSub Architecture](/docs/images/gcp_pubsub_architecture.png "AP - GCP PubSub")

#### GCP PubSub Command line parameters

- `pubsub.project-id`: The name GCP project ID using the PubSub API.
- `pubsub.igw-base-url`: Base URL of the IGW (e.g. https://localhost:30800).<br> Mutually exclusive with `pubsub.topics-config-file` flag.
- `pubsub.request-path-url`: Request path url (e.g.: "/v1/completions"). <br> Mutually exclusive with `pubsub.topics-config-file` flag.
- `pubsub.inference-objective`: InferenceObjective to use for requests (set as the HTTP header x-gateway-inference-objective if not empty). <br> Mutually exclusive with `pubsub.topics-config-file` flag.
- `pubsub.request-subscriber-id`: The subscriber ID for the requests topic.<br> Mutually exclusive with `pubsub.topics-config-file` flag.
- `pubsub.result-topic-id`: The results topic ID.
- `pubsub.batch-size`: Number of inflight messages. Default is <u>10</u>.
- `pubsub.topics-config-file`: The configuration file name when using multiple topics. <br> Mutually exclusive with `pubsub.request-subscriber-id`, `pubsub.request-path-url` and `pubsub.inference-objective` flags.

#### Multiple Topics Configuration File Syntax

The configuration file when using the `pubsub.topics-config-file` flag should have the following format:

```json
[
    {
       "worker_pool_id": "qwen-pool",
       "subscriber_id": "some_subscriber_id",
       "inference_objective": "some_inference_objective",
       "igw_base_url": "http://localhost:80/",
       "request_path_url": "/v1/completions",
       "gate_type": "constant",
       "gate_params": {},
       "labels": {
          "env": "prod",
          "team": "billing"
       }
    },
    {
       "subscriber_id": "another_subscriber",
       "worker_pool_id": "llama-pool",
       "inference_objective": "batch_task",
       "igw_base_url": "http://localhost:8000/",
       "request_path_url": "/v1/chat/completions",
       "gate_type": "prometheus-saturation",
       "gate_params": {
           "pool": "pool_2",
           "threshold": "0.75"
       }
    }
]
```

**Configuration Fields:**

- `subscriber_id`: The GCP PubSub subscriber ID for this topic.
- `worker_pool_id` (optional): The ID of the worker pool to route to (defined in the worker pools configuration file). Defaults to `"default"` if omitted.
- `inference_objective`: The inference objective header value.
- `igw_base_url` (required): Base URL of the inference gateway or target model server for this topic.
- `request_path_url` (required): Request path URL (e.g. `/v1/chat/completions`) for this topic.
- `gate_type`: Required type of dispatch gate for this topic.
- `gate_params` (optional): Parameters for the gate type (e.g., pool name, threshold for prometheus gates).
- `labels` (optional): A map of key-value string pairs injected as routing metadata (`Labels`) into the `InternalRequest` envelope at ingestion/pull time.

## Development

A setup based on a KIND cluster with a Redis server for MQ is provided.
In order to deploy everything run:

```bash
make deploy-ap-emulated-on-kind
```

Then, in a new terminal window register a subscriber:

```bash
kubectl exec -n redis redis-master-0 -- redis-cli SUBSCRIBE result-queue
```

Publish a message for async processing (uses internal wire format since this bypasses the producer):

```bash
kubectl exec -n redis redis-master-0 -- redis-cli PUBLISH request-queue '{"request_kind":"plain","data":{"id":"testmsg","created":1764044000,"deadline":9999999999,"payload":{"model":"unsloth/Meta-Llama-3.1-8B","prompt":"hi"}}}'
```
