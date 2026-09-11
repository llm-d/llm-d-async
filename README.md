# Async Processor (AP) - User Guide

## Overview
**The Problem:** High-performance accelerators often suffer from low utilization in strictly online serving scenarios, or users may need to mix latency-insensitive workloads into slack capacity without impacting primary online serving.

**The Value:** This component enables efficient processing of requests where latency is not the primary constraint (i.e., the magnitude of the required SLO is ≥ minutes). <br>
By utilizing an asynchronous, queue-based approach, users can perform tasks such as product classification, bulk summarizations, summarizing forum discussion threads, or performing near-realtime sentiment analysis over large groups of social media tweets without blocking real-time traffic.

**Architecture Summary:** The Async Processor is a composable component that provides services for managing these requests. It functions as an asynchronous worker that pulls jobs from a message queue and dispatches them to `llm-d-router` (or another inference gateway), decoupling job submission from immediate execution.

```mermaid
flowchart LR
    Producers["Producers<br/>batch jobs, workflows"] -- "enqueue requests" --> RQ

    subgraph Broker["Message queue (bring your own)"]
        RQ[("Request queues")]
        RESQ[("Result queue")]
    end

    subgraph AP["Async Processor"]
        direction LR
        Gates["Dispatch gates<br/>capacity & admission"] --> Merge["Merge policy<br/>per worker pool"] --> Workers["Worker pools"]
    end

    RQ --> Gates
    Workers -- "HTTP" --> IGW["llm-d-router /<br/>inference gateway"] --> Pool["Inference pool<br/>(vLLM)"]
    Workers -- "results" --> RESQ --> Producers
    Prom[("Prometheus")] -. "saturation & budget" .-> Gates
```

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
  - [Concepts](#concepts)
    - [Transports](#transports)
    - [Queues, Topics, and Worker Pools](#queues-topics-and-worker-pools)
    - [Dispatch Gates](#dispatch-gates)
    - [Reserved and Overflow](#reserved-and-overflow)
    - [Tiers and Priority Lanes](#tiers-and-priority-lanes)
    - [Request Merge Policies](#request-merge-policies)
    - [Retries and Deadlines](#retries-and-deadlines)
    - [Request Body Transforms](#request-body-transforms)
    - [Results](#results)
  - [Deployment](#deployment)
  - [Configuration Reference](#configuration-reference)
    - [Command Line Parameters](#command-line-parameters)
    - [Transport Configuration](#transport-configuration)
    - [Queue and Topic Entry Fields](#queue-and-topic-entry-fields)
    - [Worker Pools Configuration](#worker-pools-configuration)
    - [Dispatch Gate Reference](#dispatch-gate-reference)
    - [Request Merge Policy Reference](#request-merge-policy-reference)
    - [Request Body Transform Reference](#request-body-transform-reference)
  - [Message Formats](#message-formats)
    - [Request Messages](#request-messages)
    - [Result Messages](#result-messages)
    - [Internal Wire Format](#internal-wire-format)
  - [Observability](#observability)
    - [Prometheus Metrics](#prometheus-metrics)
    - [OpenTelemetry Tracing](#opentelemetry-tracing)
  - [Backend Compatibility](#backend-compatibility)
  - [Implementations](#implementations)
    - [Redis Sorted Set (Persisted)](#redis-sorted-set-persisted)
    - [Redis Channels (Ephemeral)](#redis-channels-ephemeral)
    - [GCP Pub/Sub](#gcp-pubsub)
  - [Development](#development)

## Concepts

Short orientation topics. Each links to the full reference section further down.

### Transports

The transport is the message queue backend the processor pulls requests from and writes results to. Three implementations are available: `redis-pubsub` (ephemeral Redis channels), `redis-sortedset` (persisted, priority-sorted Redis — recommended for production), and `gcp-pubsub` (GCP Pub/Sub). The transport is selected with `--transport` and configured with a single JSON document. Redis-protocol-compatible backends such as Valkey work unchanged (see [Backend Compatibility](#backend-compatibility)). → [Transport Configuration](#transport-configuration)

### Queues, Topics, and Worker Pools

Each queue (Redis) or topic (GCP Pub/Sub) entry in the transport config names the gateway it dispatches to (`igw_base_url`), the request path, an optional inference objective, and the worker pool that serves it (`worker_pool_id`). A worker pool has a fixed number of workers; each worker holds one in-flight request for its full duration, so pool concurrency caps throughput by Little's Law — tune it to your backend's latency/throughput target (see the [Async Processor Operations Guide](https://github.com/llm-d/llm-d/blob/main/docs/operations/async-processor.md)). Multiple queues can share a pool. → [Worker Pools Configuration](#worker-pools-configuration)

### Dispatch Gates

A gate decides whether a message pulled from the broker may be dispatched right now, based on system capacity or admission policy. Gates run at two levels:

* **Queue-level gates** run at the admission phase for a specific queue. When a queue-level gate denies admission (returning `ActionRefuse`), the request is immediately returned to the broker to be retried/re-delivered, freeing the worker to process other queues.
* **Pool-level gates** run directly inside the worker loop to regulate capacity constraints shared by all queues routing to that worker pool. When a pool-level gate returns `ActionWait`, the worker parks in-memory and polls until capacity is available, avoiding broker nack/retry overhead. If the pool-level gate returns `ActionRefuse`, the request is immediately returned to the broker.

Gates come in two flavors: **budget gates** report a fraction of available capacity in [0, 1] (e.g. the `prometheus-*` gates), and **admission gates** issue a per-message verdict — continue, wait, refuse, or drop (e.g. `redis-quota`, `tier-priority-admission`). Combinator gates (`composite`, `wait-on-refuse`) assemble them. → [Dispatch Gate Reference](#dispatch-gate-reference)

A request's path through the gates, from broker to result:

```mermaid
flowchart TD
    A["Message pulled from queue"] --> B{"Queue-level gate"}
    B -- "Continue" --> C["Merged per-pool channel<br/>(merge policy picks next)"]
    B -- "Refuse" --> RB["Returned to broker,<br/>retried / redelivered"]
    B -- "Drop" --> ER["Error result written"]
    C --> D{"Pool-level gate"}
    D -- "Continue" --> E["Worker dispatches<br/>to the gateway"]
    D -- "Wait" --> P["Worker parks in memory,<br/>polls until capacity frees"] --> D
    D -- "Refuse" --> RB
    E -- "success" --> RES["Result written<br/>to result queue"]
    E -- "shed / server error" --> DL{"Deadline passed?"}
    DL -- "no" --> RB
    DL -- "yes" --> ER
```

### Reserved and Overflow

Quota gates can run in *classifying* mode: instead of blocking a message that exceeds its quota, they tag it with a classification label — `reserved` (within quota) or `overflow` (over quota). Downstream components then act on the tag: the `tier-priority` merge policy buckets reserved traffic ahead of overflow traffic, and the `tier-priority-admission` gate parks reserved requests but sheds overflow requests when the pool is saturated. A message with no classification is treated as overflow by the merge policy. The `redis-quota` gate classifies when `gating_mode` is set to `classifying`.

### Tiers and Priority Lanes

Queues declare an SLA tier through their `labels` (label key configurable via `tier_label`, default `"tier"`), with values `interactive`, `async`, or `batch`. Combining classification × tier yields six strict priority lanes, ordered:

```mermaid
flowchart LR
    L0["0<br/>reserved<br/>interactive"] --> L1["1<br/>reserved<br/>async"] --> L2["2<br/>reserved<br/>batch"] --> L3["3<br/>overflow<br/>interactive"] --> L4["4<br/>overflow<br/>async"] --> L5["5<br/>overflow<br/>batch"]
```

Lane 0 is dispatched first, lane 5 last. A missing or unrecognized tier falls to `batch`; a missing classification falls to overflow. The `tier-priority` merge policy dispatches strictly by lane order and round-robins within a lane.

### Request Merge Policies

The processor consumes from multiple queues concurrently. A merge policy merges messages from all active queues — not globally, but **per worker pool**: input channels are grouped by `worker_pool_id` and each pool gets its own independent merged channel. This gives complete backpressure isolation: a slow or saturated pool blocks only its own merged channel. Two policies exist: `random-robin` (default) and `tier-priority`. Both can stamp a fairness-identity header for the gateway's flow control. → [Request Merge Policy Reference](#request-merge-policy-reference)

```mermaid
flowchart LR
    Q1[("interactive queue")] --> M1
    Q2[("batch queue")] --> M1["merged channel"]
    Q3[("bulk queue")] --> M2["merged channel"]
    M1 --> P1["worker pool: qwen-pool"] --> G1["gateway A"]
    M2 --> P2["worker pool: llama-pool"] --> G2["gateway B"]
```

A saturated `llama-pool` above blocks only its own merged channel — `qwen-pool` keeps dispatching.

### Retries and Deadlines

Every request message carries a mandatory `deadline` (Unix seconds). When processing fails — shed by a gate or a server-side error — the message is scheduled for a retry with exponential backoff, as long as the deadline has not passed. A message whose deadline passes produces a `DEADLINE_EXCEEDED` error result.

### Request Body Transforms

By default the worker dispatches the OpenAI-style JSON marshalled from a request's `payload`. Body-transform plugins rewrite the outgoing body and `Content-Type` based on per-message `metadata` — for example turning JSON into `multipart/form-data` for Whisper-style endpoints. The default JSON path is preserved byte-for-byte when no plugin applies. → [Request Body Transform Reference](#request-body-transform-reference)

### Results

For each request, a result message keyed by the request `id` is written to the result queue/topic — carrying the HTTP status and response payload on success, or an error code and message on failure. → [Result Messages](#result-messages)

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

### Command Line Parameters

**Core:**

| Flag | Default | Description |
|------|---------|-------------|
| `concurrency` | `64` | Number of concurrent workers (per pool if unspecified). The processor is I/O-bound (each worker holds one in-flight request for its full duration), so in-flight concurrency caps throughput — see [Queues, Topics, and Worker Pools](#queues-topics-and-worker-pools). |
| `transport` | `redis-pubsub` | The transport (message queue) implementation. One of `redis-pubsub`, `redis-sortedset`, `gcp-pubsub`. Gating is configured per queue/topic via `gate_type` in the transport config (this replaces the former `gcp-pubsub-gated` implementation). |
| `transport-config` | — | Inline JSON transport configuration. See [Transport Configuration](#transport-configuration). Mutually exclusive with `transport-config-file`; exactly one of the two is required. |
| `transport-config-file` | — | Path to a JSON file with the transport configuration. Mutually exclusive with `transport-config`. |
| `transport-config-watch-interval` | `0` | For `redis-sortedset` only, periodically reloads the `queues` field from `transport-config-file`. The file must contain a complete valid transport configuration. Changes to other transport fields require a restart. |
| `pool-config-file` | — | Path to the JSON worker pool definitions. If omitted, a single `"default"` worker pool is created with concurrency determined by the global `concurrency` flag. See [Worker Pools Configuration](#worker-pools-configuration). |
| `request-merge-policy-config-file` | — | Path to the JSON request merge policy specification (`type` and `parameters`). If not specified, defaults to the `random-robin` policy. The older `--request-merge-policy-config` name is a deprecated alias. |
| `transform-config-file` | — | Path to the JSON request body transform configuration. Empty disables transforms. See [Request Body Transform Reference](#request-body-transform-reference). |

**Gating and Prometheus:**

| Flag | Default | Description |
|------|---------|-------------|
| `prometheus-url` | — | Prometheus server URL for metric-based gates (e.g., http://localhost:9090). Required when using metric-based gates (`prometheus-saturation`, `prometheus-budget`, `prometheus-query`). For Google Managed Prometheus (GMP), point this to a local proxy or GMP frontend that handles authentication — direct GMP URLs are not supported as the Async Processor does not perform GMP authentication. |
| `prometheus-cache-ttl` | `5s` | TTL for cached Prometheus metric sources (e.g. `1m`, `0s` to disable). Increasing this reduces Prometheus load but also reduces the responsiveness of dispatch gates to metric changes. |

**Timeouts and draining:**

| Flag | Default | Description |
|------|---------|-------------|
| `request-timeout` | `5m` | Timeout for individual inference requests. |
| `drain-timeout` | `2m` | Maximum time to wait for in-flight requests to complete after SIGTERM. |

**Ports and endpoints:**

| Flag | Default | Description |
|------|---------|-------------|
| `metrics-port` | `9090` | Port serving Prometheus metrics. |
| `metrics-endpoint-auth` | `true` | Enables authentication and authorization of the metrics endpoint. |
| `health-port` | `8081` | The health probe port. |
| `metrics-backlog-poll-interval` | `15s` | Interval to poll the broker for queue backlog metrics (`0` disables). Only applies to transports that support it (`redis-sortedset`, `gcp-pubsub`). |

**TLS (outbound, towards the inference gateway):**

| Flag | Default | Description |
|------|---------|-------------|
| `tls-ca-cert` | — | Path to CA certificate file (PEM) for verifying the inference gateway. |
| `tls-cert` / `tls-key` | — | Paths to client certificate/key files (PEM) for mTLS. Must be provided together. |
| `tls-insecure-skip-verify` | `false` | Skip TLS certificate verification (dev/test only). |

**Logging:**

| Flag | Default | Description |
|------|---------|-------------|
| `v` | `2` | Log level verbosity. |
| `zap-*` | — | Standard controller-runtime zap flags (`zap-devel`, `zap-encoder`, `zap-log-level`, `zap-stacktrace-level`, `zap-time-encoding`). |

> **Deprecated:** The per-backend flags — `--message-queue-impl`, `--redis.url`, `--redis.*`, `--redis.ss.*`, `--pubsub.*`, `--redis-tracing`, and `--request-merge-policy-config` — still work for backwards compatibility but are deprecated. When used, the processor logs a warning and translates them into the transport config. `--transport`/`--transport-config` take precedence when both are set. The legacy flags are documented per backend under [Implementations](#implementations).

### Transport Configuration

The transport (message queue) is selected with `--transport` and configured with a single JSON document, supplied either inline via `--transport-config` or from a file via `--transport-config-file` (the two are mutually exclusive; exactly one is required). This is the recommended configuration surface for all backends.

**`redis-pubsub`:**
```json
{
  "url": "redis://user:pass@host:6379/0",
  "retry_queue_name": "retry-sortedset",
  "result_queue_name": "result-queue",
  "enable_tracing": false,
  "queues": [ { "queue_name": "request-queue", "igw_base_url": "http://localhost:30800" } ]
}
```

**`redis-sortedset`:**
```json
{
  "url": "redis://user:pass@host:6379/0",
  "result_queue_name": "result-list",
  "retry_queue_name": "retry-sortedset",
  "poll_interval_ms": 1000,
  "batch_size": 10,
  "enable_tracing": false,
  "queues": [ { "queue_name": "request-sortedset", "igw_base_url": "http://localhost:30800", "gate_type": "redis", "gate_params": { "address": "localhost:6379" } } ]
}
```

Queue hot reload is enabled with `--transport redis-sortedset --transport-config-file FILE --transport-config-watch-interval INTERVAL`. Only `queues` may change; URL, retry/result queue names, polling, batch, tracing, and any other transport fields are rejected until restart. An empty `queues` array drains all queues, and queues may be added again by a later reload. Inline `--transport-config` and deprecated per-backend queue configuration do not support hot reload.

**`gcp-pubsub`:**
```json
{
  "project_id": "my-project",
  "result_topic_id": "results",
  "batch_size": 10,
  "topics": [ { "subscriber_id": "requests-sub", "igw_base_url": "http://localhost:30800", "gate_type": "constant", "gate_params": {} } ]
}
```

**Top-level fields:**

| Field | Transports | Default | Description |
|-------|-----------|---------|-------------|
| `url` | redis-* | `REDIS_URL` env | Redis/Valkey URL (e.g. `redis://user:pass@host:port/db`, `rediss://...` for TLS). An explicit `url` takes precedence; `REDIS_URL` fills it in only when empty. Required (one of the two). |
| `retry_queue_name` | redis-* | `retry-sortedset` | Sorted set used for retry scheduling. |
| `result_queue_name` | redis-pubsub | `result-queue` | Channel for results. |
| `result_queue_name` | redis-sortedset | `result-list` | List for results. |
| `poll_interval_ms` | redis-sortedset | `1000` | Poll interval in milliseconds. |
| `batch_size` | redis-sortedset, gcp-pubsub | `10` | Messages per poll (sortedset) / inflight messages (Pub/Sub). |
| `enable_tracing` | redis-* | `false` | Per-command Redis tracing spans via `redisotel`. High span volume — debugging only. |
| `project_id` | gcp-pubsub | — | GCP project ID (required). |
| `result_topic_id` | gcp-pubsub | — | Results topic ID (required). |
| `queues` / `topics` | all | — | Array of queue/topic entries (at least one required). See below. |

### Queue and Topic Entry Fields

Each entry in `queues`/`topics` describes one request source and where its requests dispatch to:

```json
{
   "queue_name": "batch_queue",
   "igw_base_url": "http://localhost:30800",
   "request_path_url": "/v1/completions",
   "inference_objective": "batch-task",
   "worker_pool_id": "qwen-pool",
   "gate_type": "prometheus-saturation",
   "gate_params": { "pool": "inference_pool_1", "threshold": "0.8" },
   "labels": { "tier": "batch", "team": "billing" }
}
```

**Common fields (all transports):**

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `queue_name` (Redis) / `subscriber_id` (Pub/Sub) | yes | — | The Redis channel/sorted-set name, or the GCP Pub/Sub subscriber ID. |
| `igw_base_url` | yes | — | Base URL of `llm-d-router`, an inference gateway, or target model server. |
| `request_path_url` | no | `/v1/completions` | Request path (e.g. `/v1/chat/completions`). |
| `inference_objective` | no | — | InferenceObjective for requests (set as the HTTP header `x-llm-d-inference-objective` if not empty). |
| `worker_pool_id` | no | `default` | The worker pool to route to (defined in the [worker pools configuration](#worker-pools-configuration)). |
| `labels` | no | — | Key-value string pairs injected as routing metadata (`Labels`) into the internal request envelope at ingestion/pull time. Used e.g. for the `tier` label. |

**Gate fields (`redis-sortedset` and `gcp-pubsub` only):**

| Field | Required | Description |
|-------|----------|-------------|
| `gate_type` | no | The dispatch gate type for this queue/topic. See [Dispatch Gate Reference](#dispatch-gate-reference). |
| `gate_params` | no | Key-value parameters for the gate. |

> **Note:** The ephemeral `redis-pubsub` transport does not support per-queue dispatch gates — `gate_type`/`gate_params` on its queue entries are ignored. Use `redis-sortedset` for per-queue gating.

**Additional fields (`redis-sortedset` only):**

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `id` | no | `queue_name` | Unique queue identifier; becomes the `queue_id` metric label. |
| `result_queue_name` | no | top-level `result_queue_name` | Per-queue result destination override. |
| `result_ttl_seconds` | no | `0` (no expiry) | When > 0, sets an expiry on the result destination each time results are pushed. Used for per-request result keys (frontend enqueue mode) so unfetched results are cleaned up. |

### Worker Pools Configuration

When using multiple queues or topics, the worker capacities and pool-level gates for named pools can be configured via a dedicated worker pools file (`--pool-config-file`).

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

### Dispatch Gate Reference

The available gate types, at a glance:

| Gate | Kind | Purpose |
|------|------|---------|
| `constant` | budget | Always fully open (budget 1.0) — no throttling. |
| `redis` | budget | Reads the dispatch budget from a Redis key managed by an external system. |
| `prometheus-saturation` | budget | Closes when a pool saturation metric reaches a threshold. |
| `prometheus-budget` | budget | Computes a dispatch budget from a cascade of EPP/vLLM metrics. |
| `prometheus-query` | budget | Evaluates a user-supplied PromQL expression as the budget. |
| `endpoint-scrape` | budget | Scrapes a raw `/metrics` endpoint directly — no Prometheus server required. |
| `local-max-concurrency` | admission | Caps concurrent in-flight requests per queue using in-process state. |
| `redis-quota` | admission | Per-attribute quota (rate limit or concurrency) via Redis. |
| `tier-priority-admission` | admission | Three-way verdict from saturation × tier × classification. |
| `composite` | combinator | Combines multiple gates: minimum budget across all inner dispatch gates, all-or-nothing quota acquisition across inner attribute gates. |
| `wait-on-refuse` | combinator | Wraps an inner gate and converts `ActionRefuse` into `ActionWait` (parking in-memory instead of broker redelivery). |

> **Note:** An unrecognized `gate_type` does not fail startup — it resolves to the always-open constant gate.

**Example configuration with per-queue gates:**

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

#### Budget gates

- `constant`: Always returns budget 1.0 (fully open). No parameters.

- `redis`: Queries Redis for the dispatch budget (managed by an external system).
  - `address` (**required**): Redis server address for the dispatch gate (e.g., `localhost:6379`). Queues sharing the same address will share the same connection pool.
  - `budget_key` (optional): Redis key to read the dispatch budget from. Default is `dispatch-gate-budget`.

- `prometheus-saturation`: Queries Prometheus for a pool saturation metric. The gate closes (returns `0.0`) when saturation ≥ threshold; when open it returns `(1 - saturation) - (1 - threshold)`, i.e. the margin below the threshold.
  - `pool` (**required**): The inference pool name to filter metrics by.
  - `namespace` (optional): Kubernetes namespace to scope metric queries. Required when multiple namespaces share the same pool name with a shared Prometheus instance.
  - `threshold` (optional): Saturation threshold (0.0-1.0). When saturation >= threshold, budget is 0.0. Default is `0.8`.
  - `fallback` (optional): Fallback **saturation** value (0.0-1.0) used when the metric source returns an error or empty data. Default is `0.0` — i.e. the gate fails **open** (full budget) by default; set `fallback` to `1.0` to fail closed.

  **Metric prerequisites:** The primary metric source requires llm-d's flow control plugin to be
  enabled: without it, the EPP flow control metrics will be missing and the gate will always use the fallback value.

- `prometheus-budget`: Cascades three Prometheus metric sources to compute a dispatch budget D,
  using the first that returns a sample:

  | # | Metric | Budget | Available when |
  |---|--------|--------|----------------|
  | 0 | `inference_extension_flow_control_queue_size` | `D = 1 − (queue_size / max_SYS)` | EPP runs the flow control plugin |
  | 1 | `inference_pool_per_pod_queue_size` | `D = 1 − (mean per-pod queue depth / max_concurrency)` | Always — part of EPP's base metric set |
  | 2 | `vllm:num_requests_running` | `D = 1 − (running_requests / max_SYS)` | vLLM metrics carry an `inference_pool` label |

  Sources 0 and 2 compute `max_SYS = ready_pods × max_concurrency` dynamically from the
  `inference_pool_ready_pods` metric. Source 1 averages over pods, so the `ready_pods` factor
  cancels and no join is needed. That also keeps it honest when the pool drains: EPP's metrics
  refresh [returns early when the pool has no pods](https://github.com/kubernetes-sigs/gateway-api-inference-extension/blob/v1.2.1/pkg/epp/backend/metrics/logger.go#L89-L91),
  so `inference_pool_ready_pods` and `inference_pool_average_queue_size` freeze at their last
  values and a scaled-to-zero pool would read as idle capacity. `inference_pool_per_pod_queue_size`
  comes from a scrape-time collector that simply stops reporting, so the source yields no sample
  and the cascade moves on instead.
  The gate closes when `D ≤ baseline`; when open it returns `D − baseline`, so callers compute `N = max_SYS × (D − B)`.
  See [docs/dispatch-budget.md](docs/dispatch-budget.md) for the full derivation.

  The PromQL each source resolved to is logged at startup (`prometheus-budget metric source`), and
  `llm_d_async_async_gate_metric_source_available` reports whether the last evaluation got a reading from any
  of them or fell back to `fallback`.

  - `pool` (**required**): The InferencePool name. This must match the `name` field in
    `inference_pool_ready_pods{name="<pool>"}` and `inference_pool_per_pod_queue_size{name="<pool>"}`
    (EPP metrics) and, for the vLLM source, the `inference_pool` label on scraped vLLM metrics
    (added via relabeling from pod labels).
  - `namespace` (optional): Kubernetes namespace to scope metric queries. Required when multiple namespaces share the same pool name with a shared Prometheus instance.
  - `max_concurrency` (optional): Per-endpoint request capacity (`MaxConcurrency` in the [inference scheduler's saturation detector](https://github.com/llm-d/llm-d-inference-scheduler/blob/main/pkg/epp/framework/plugins/flowcontrol/saturationdetector/concurrency/config.go)). Default is `100` (matching the inference scheduler default). See [sizing `max_concurrency`](#sizing-max_concurrency) below — this is a **per-pod** number, and setting it above what a pod actually serves makes the gate inert.
  - `baseline` (optional): Reserved baseline B. The gate closes when D ≤ B. Default is `0.05`.
  - `fallback` (optional): Fallback budget value (0.0-1.0) returned when all metric sources are unavailable. Default is `0.0` (fail closed).

  <a id="sizing-max_concurrency"></a>
  **Sizing `max_concurrency`.** Every source in the cascade divides by `max_concurrency` per pod —
  sources 0 and 2 divide pool-wide load by `max_SYS = ready_pods × max_concurrency`, source 1
  averages per pod first — so whichever one resolves, the gate closes only once load reaches:

  ```
  max_concurrency × (1 − baseline)   concurrent requests per ready pod
  ```

  At the defaults that is 95 concurrent requests *per ready pod*. A pool that never gets near
  that — a large model on a few replicas, for instance — leaves the gate permanently open, so every
  batch request dispatches regardless of live traffic. The gate logs its resolved closing point at
  startup so you can compare it against reality:

  ```
  "prometheus-budget gate configured" pool=... maxConcurrency=100 baseline=0.05 closesAtLoadPerReadyPod=95
  ```

  Two ways to pick a value:

  - **Match the EPP.** `max_concurrency` mirrors `MaxConcurrency` in the inference scheduler's
    saturation detector. Using the same number keeps the async gate and the EPP's own admission
    control in agreement about when the pool is full. If you have not configured the saturation
    detector, both defaults are `100`.
  - **Measure it.** Drive your pool to the load you consider saturated and read the per-pod peak:

    ```promql
    max_over_time(
      (sum(vllm:num_requests_running{inference_pool="<pool>"}) / on() inference_pool_ready_pods{name="<pool>"})[1h:]
    )
    ```

    Set `max_concurrency` to that peak. Values well above it mean the gate never closes; values
    well below it mean the gate sheds while the pool still has room.

  **Metric prerequisites:** none for source 1, which is why it is in the cascade — the llm-d router's
  EPP does not enable the flow control plugin source 0 needs, and source 2 filters by the
  `inference_pool` label, which vLLM does not emit natively. To use source 2, configure Prometheus
  relabeling to propagate that label from model server pod labels (the helm chart handles this):

  ```yaml
  relabelings:
    - sourceLabels: [__meta_kubernetes_pod_label_inference_pool]
      targetLabel: inference_pool
  ```

- `prometheus-query`: Evaluates a user-supplied PromQL expression directly as the dispatch budget.
  The expression must resolve to a Prometheus instant vector with a single sample whose value is in [0, 1].
  Values outside this range are clamped. Unlike `prometheus-saturation` and `prometheus-budget`, this gate does not construct queries internally — the user provides the complete PromQL expression.

  - `query` (**required**): The PromQL expression to evaluate. This is sent to Prometheus exactly as provided.
    The result is used directly as the dispatch budget (no transformation is applied).
  - `fallback` (optional): Fallback budget value (0.0-1.0) returned when the query fails or returns no data.
    Default is `0.0` (fail closed).
  - `pool` (optional): The InferencePool the query is about. Purely descriptive — it does not
    affect the query, it only sets the `inference_pool` label on `async_gate_metric_value` and
    `async_gate_metric_threshold` so you can tell which pool a gauge is reporting on.

- `endpoint-scrape`: Scrapes a raw Prometheus text-format `/metrics` endpoint directly.
  Computes budget as `clamp(1 - saturation - baseline, 0, 1)`. Supports two modes: **direct saturation** (metric value is already in [0, 1], e.g., from the EPP) and **computed saturation** (raw count divided by `max_count_per_pod`, e.g., `vllm:num_requests_waiting`).

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

#### Admission gates

- `local-max-concurrency`: Limits the number of concurrent in-flight requests processed from a queue locally using thread-safe, in-process state.
  - `limit` (**required**): The maximum number of concurrent requests allowed in-flight for this queue. Must be a positive integer.
  - `gating_mode` (optional): `blocking` or `classifying`. In `blocking` mode the worker blocks until capacity frees up; in `classifying` (non-blocking) mode a request over the limit is refused (returned to the broker for redelivery). Default is `classifying`.

- `redis-quota`: Per-attribute quota management via Redis.
  - `address` (**required**): Redis server address.
  - `attribute` (optional): The message attribute to use for quota (e.g., `userid`). Default is `userid`.
  - `mode` (optional): `rate-limit` or `concurrency`. Default is `rate-limit`.
  - `limit` (**required**): The quota limit. Must be positive.
  - `window` (optional): The time window for rate limiting (e.g., `1m`, `10s`). Default is `1m`.
  - `prefix` (optional): Redis key prefix. Default is `quota:`.
  - `gating_mode` (optional): `blocking` or `classifying`. In `classifying` mode, the gate never blocks but tags the message with its quota status (`reserved` or `overflow`) in the internal metadata — see [Reserved and Overflow](#reserved-and-overflow). Default is `blocking`.

- `tier-priority-admission`: Implements a three-way admission verdict based on saturation, queue tier, and reservation classification. Saturation is determined by evaluating an inner gate: if the inner gate returns `ActionRefuse`, the pool is considered saturated. If the pool is saturated: (1) returns `ActionWait` if classification is `reserved` (parking worker threads cleanly); (2) drops immediately with a `429` status payload if tier is `interactive` and classification is `overflow`; (3) otherwise — including `async`/`batch` overflow and unclassified requests — returns `ActionRefuse` to place the request back in the queue. If not saturated, returns `ActionContinue`.
  - `saturation_gate` (**required**): The type string of the inner gate used to evaluate pool saturation (e.g. `"prometheus-query"`).
  - `saturation_gate_params` (optional): JSON-serialized string of parameters for the inner saturation gate.
  - `tier_label` (optional): The label key to check the queue's SLA tier. Default is `"tier"`.

#### Combinator gates

- `composite`: Combines multiple gates. Returns the minimum budget across all inner dispatch gates and acquires quota across all inner attribute gates (all or nothing).
  - `gates` (**required**): A JSON array of gate configurations. Each configuration is an object with `gate_type` and `gate_params`.

- `wait-on-refuse`: Decorator that wraps a single inner gate and converts any `ActionRefuse` verdict into `ActionWait` (parking/polling in-memory instead of immediate broker redelivery).
  - `gate` (**required**): A JSON string containing a single gate configuration (with `gate_type` and `gate_params`) to wrap. This can be used to wrap prometheus gates in pool configuration so that they park requests instead of redelivering them to the message broker when the gate is saturated.

### Request Merge Policy Reference

The merge policy is configured using the `--request-merge-policy-config-file` CLI flag (the older `--request-merge-policy-config` name is a deprecated alias). It points to a JSON configuration file specifying the policy `type` and optional custom `parameters`:

```json
{
  "type": "tier-priority",
  "parameters": {
    "lane_objectives": {
      "reserved-interactive": "premium-latency",
      "overflow-batch": "best-effort"
    }
  }
}
```

See [Request Merge Policies](#request-merge-policies) for how per-pool merging works.

1. **`random-robin`**: Randomly picks messages from all queues configured for a given pool. This is the default policy.
   - **Parameters**:
     - `fairness_header` (optional, string): The HTTP header name used to pass the tenant's fairness identity to the gateway's flow control. Set to `""` to disable stamping. A name that is not a legal HTTP header name is rejected at startup. Default is `"x-llm-d-inference-fairness-id"`.
     - `fairness_attribute` (optional, string): The message metadata attribute holding the tenant identity (the same attribute the `redis-quota` gate keys on). The stamped value replaces any caller-supplied header of the same name under any letter case, so the identity the gateway arbitrates on is the one quota is accounted against. The header is only stamped when the attribute is present, non-empty, at most 256 bytes, and a legal HTTP header value; otherwise the request dispatches with the header untouched. Default is `"userid"`.
   - **Note**: Stamping is on by default and sends the attribute's value to the gateway, where it may be recorded in access logs. Prefer an opaque tenant ID over personally identifying values such as email addresses, or set `fairness_header` to `""` to disable stamping.
2. **`tier-priority`**: Buckets requests into 6 strict priority lanes using routing tags (`(classification, tier)`) — see [Tiers and Priority Lanes](#tiers-and-priority-lanes) for the lane order and defaults. Within each bucket, it round-robins across different client channels. Priority reaches llm-d Routers through the lane's InferenceObjective (`objective_header` / `lane_objectives`), not the numeric priority header, which has no consumer in llm-d and is left unstamped unless `priority_header` is explicitly configured.
   - **Note**: The `tier-priority` merge policy assumes that all messages within a single queue share the same priority. Message classification relies on the FIFO order of an individual queue, and a message's classification does not change after it is pulled off the queue.
   - **Parameters**:
     - `priority_header` (optional, string): The HTTP header name stamped with the numeric lane index (0 = highest priority). This header has no consumer in llm-d — priority reaches them via `objective_header` / `lane_objectives` instead — so it is left unstamped by default. Set it to a header name to opt in; a name that is not a legal HTTP header name is rejected at startup. Default is `""` (unstamped).
     - `tier_label` (optional, string): The label name on `InternalRequest.Labels` used to look up the request's priority tier. Default is `"tier"`.
     - `objective_header` (optional, string): The HTTP header name used to stamp the lane's InferenceObjective name. A name that is not a legal HTTP header name is rejected at startup. Default is `"x-llm-d-inference-objective"` (`api.ObjectiveHeader`).
     - `lane_objectives` (optional, object): Maps lane keys (`"reserved-interactive"`, `"reserved-async"`, `"reserved-batch"`, `"overflow-interactive"`, `"overflow-async"`, `"overflow-batch"`) to InferenceObjective names. A request whose lane has an entry gets that objective stamped as `objective_header`, which overrides the queue-level `inference_objective`. Lanes without an entry fall back to the queue objective.
     - `fairness_header` / `fairness_attribute` (optional, string): Same as for `random-robin`.

### Request Body Transform Reference

Some providers need a different body shape at dispatch time — for example multi-modal endpoints (Whisper transcription, OCR) that expect `multipart/form-data` with a `url` field rather than JSON. Request body-transform plugins handle this without special-casing the worker: they rewrite the outgoing body and `Content-Type` based on per-message `metadata`, and the default JSON path is preserved byte-for-byte when no plugin applies.

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

#### `gcs_uri_multipart` plugin

Rewrites a JSON body into `multipart/form-data` for endpoints that take a signed object URL. Because producers can't put raw media bytes on the broker, the queued `payload` carries a signed URL (e.g. a GCS V4 signed URL) in a `gcs_uri` field.

- **Activation:** the message's `metadata.provider` must match one of the configured `providers`, and the `payload` must contain a non-empty `gcs_uri`. Otherwise the default JSON path is used unchanged.
- **Transform:** writes the `gcs_uri` value as a `url` form field (a plain field, not a file upload), passes the remaining payload fields through as form fields, and drops `gcs_uri`. A non-empty `file_base64` is rejected as a fatal, non-retryable error (inline media is not supported on this path).
- **Preflight:** parses the signed URL's expiry (V4 `X-Goog-Date` + `X-Goog-Expires`, or V2 `Expires`); if it expires at or before the message deadline, the request fails fatally before dispatch so the broker doesn't retry a request that cannot succeed.

## Message Formats

### Request Messages

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
| `headers` | map[string]string | Optional HTTP headers forwarded on the outgoing dispatch request |
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

### Result Messages

Results are written to the result queue/topic with the following structure:

```json
{
    "id": "id mapped to the request",
    "status_code": 200,
    "payload": "inference result payload"
}
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | The originating request's `id` |
| `status_code` | int | HTTP status code of the inference response. Present (> 0) whenever an HTTP response was received — including error statuses. |
| `payload` | string | The response body. On non-HTTP failures it carries a JSON `{"error": "<message>"}` object. |
| `error_code` | string | Set for non-HTTP failures (when `status_code` is absent): `DEADLINE_EXCEEDED`, `CANCELLED`, `GATE_DROPPED`, `GATE_ERROR`, `INFERENCE_ERROR`, `INVALID_REQUEST` |
| `error_message` | string | Human-readable description accompanying `error_code` |

### Internal Wire Format

On the broker itself, messages travel in a tagged envelope carrying the request kind and internal routing (producers create this automatically):

```json
{"request_kind": "plain", "data": { "id": "...", "deadline": 1764045130, "payload": {} }}
```

You only need this format when publishing directly to the broker, bypassing a producer (see [Development](#development)).

## Observability

### Prometheus Metrics

The Async Processor exposes Prometheus metrics under the `llm_d_async` subsystem on the metrics port (default `9090`). All counters and histograms carry `queue_id`, `queue_name`, and `pool_name` labels so you can filter and aggregate per queue.

**Request lifecycle:**

| Metric | Type | Description |
|--------|------|-------------|
| `llm_d_async_async_request_total` | Counter | New async requests (first attempt only) |
| `llm_d_async_async_successful_requests_total` | Counter | Requests that received a successful inference response |
| `llm_d_async_async_tokens_total` | Counter | Tokens processed by successfully-dispatched requests, by `direction`: `input` (prompt_tokens) and `output` (completion_tokens). Parsed best-effort from the OpenAI `usage` object in 2xx response bodies; no-op when usage is absent or the body is not parseable (e.g. streaming responses). Non-OpenAI gateways undercount by design. |
| `llm_d_async_async_failed_requests_total` | Counter | Requests that failed with a fatal or non-retryable error |
| `llm_d_async_async_shedded_requests_total` | Counter | Requests shedded due to rate limiting (429 / capacity) |
| `llm_d_async_async_exceeded_deadline_requests_total` | Counter | Requests that exceeded their deadline before completion |
| `llm_d_async_async_request_retries_total` | Counter | Retry attempts |

**Latency and deadlines:**

| Metric | Type | Description |
|--------|------|-------------|
| `llm_d_async_async_message_latency_time_millis` | Histogram | End-to-end message latency in milliseconds (publish to successful processing). Only registered when the transport supports message latency (GCP Pub/Sub only). |
| `llm_d_async_async_inference_latency_time_millis` | Histogram | Time in milliseconds spent calling `llm-d-router` (or other inference gateway), measured around each request attempt. Isolates "model time" from "queue time". Always registered. |
| `llm_d_async_async_queue_residence_time_millis` | Histogram | Time in milliseconds a message spent buffered in-process, from broker ingestion until a worker pulled it for processing. Measures the async delay introduced by the system (queue time). Always registered. |
| `llm_d_async_async_deadline_proximity_millis` | Histogram | Snapshot histogram of time in milliseconds remaining until each queued item's deadline, rebuilt once per backlog poll from exact cumulative `ZCOUNT` counts per bucket (`le="0"` holds items past their deadline but still queued; higher buckets are cumulative, so every item counts in each bucket it expires in). Redis sorted-set only; Cloud Pub/Sub cannot expose per-item deadlines. Because each poll replaces the snapshot, the series is not monotonic — `rate()` is meaningless; use `histogram_quantile` per scrape. The `_sum` is estimated from bucket midpoints. |

**Capacity and backlog:**

| Metric | Type | Description |
|--------|------|-------------|
| `llm_d_async_async_queue_depth` | Gauge | Requests received from the broker and buffered in-process awaiting an available worker |
| `llm_d_async_async_inflight_requests` | Gauge | Requests currently being processed by workers (dispatched to inference, awaiting a response) |
| `llm_d_async_async_broker_backlog` | Gauge | Undelivered/pending messages held by the broker queue (polled every `metrics-backlog-poll-interval`; `redis-sortedset` and `gcp-pubsub` only) |
| `llm_d_async_async_pool_worker_limit` | Gauge | Configured worker concurrency limit for a pool (carries only the `pool_name` label). Compare against `llm_d_async_async_inflight_requests` to compute worker utilization. |

**Gates:**

| Metric | Type | Description |
|--------|------|-------------|
| `llm_d_async_async_dispatch_budget` | Gauge | Current dispatch budget [0.0–1.0] returned by the queue's gate; the fraction of system capacity available for new requests (0.0 = gate fully closed). Useful for diagnosing why throughput is throttled. |
| `llm_d_async_async_gate_decisions_total` | Counter | Count of gate decisions that prevented dispatch, by `reason`: `gate_closed` (no dispatch budget), `quota_exhausted` (per-attribute quota overflow), `dropped` (gate permanently rejected the request), `error` (gate evaluation failed). `quota_exhausted`, `dropped` and `error` count individual messages refused after being dequeued. `gate_closed` counts those plus every dequeue round in which the budget shrank the batch to zero — the way budget-based gates (`prometheus-budget`/`-saturation`/`-query`) shed work *before* a message is dequeued — so its rate reflects throttled dispatch opportunities, not messages. All four `reason` series are created at 0 when a queue or gated worker pool starts, so a query returns 0 rather than an empty vector. |
| `llm_d_async_async_gate_metric_value` | Gauge | Raw metric value a metric-based gate (`prometheus-saturation`/`-budget`/`-query`) last read — the number compared against the threshold below. For the saturation gate this is `1 - saturation`. |
| `llm_d_async_async_gate_metric_threshold` | Gauge | Threshold the value above is compared against. The gate closes when `value <= threshold`, which is what drives `async_dispatch_budget` to 0. |
| `llm_d_async_async_gate_metric_source_available` | Gauge | Whether the gate's last evaluation got a reading from any metric source (1) or fell back to `fallback` (0) |

**Labels:**

| Label | Description |
|-------|-------------|
| `queue_id` | Queue identifier. For `redis-sortedset`, from the queue config `id` field (defaults to the queue name); other transports use the queue name / subscriber ID. |
| `queue_name` | Logical queue name (Redis sorted set name, channel name, or Pub/Sub subscriber ID) |
| `pool_name` | Worker pool the queue routes to (`async_pool_worker_limit` carries only this label) |
| `reason` | Gate-decision reason (only on `async_gate_decisions_total`): `gate_closed`, `quota_exhausted`, `dropped`, `error` |
| `inference_pool` | InferencePool a gate queries (only on the `async_gate_metric_*` gauges), from the gate's `pool` param. Empty when the gate does not name one. |
| `direction` | Token direction (only on `async_tokens_total`): `input` or `output` |

`pool_name` always names the **async worker pool** that owns the series, never the
InferencePool a gate happens to query — that is what `inference_pool` is for. Every
per-queue series therefore carries the same `queue_id`/`queue_name`/`pool_name`
triple and joins on it, including the gate gauges. A **pool-level** gate (one
configured on a worker pool rather than a queue) has no single queue, so its gauges
and `async_gate_decisions_total` counter carry an empty `queue_id` and `queue_name`
and are keyed by `pool_name` alone.

**Example PromQL queries:**

```promql
# Per-queue success ratio over the last 5 minutes
rate(llm_d_async_async_successful_requests_total[5m]) / rate(llm_d_async_async_request_total[5m])

# Which queues are getting rate-limited?
rate(llm_d_async_async_shedded_requests_total[5m])

# Retry ratio by queue
rate(llm_d_async_async_request_retries_total[5m]) / rate(llm_d_async_async_request_total[5m])

# p95 llm-d-router / inference gateway latency by queue (model time, excluding queue time)
histogram_quantile(0.95, sum by (queue_name, le) (rate(llm_d_async_async_inference_latency_time_millis_bucket[5m])))

# p95 queue residence time by queue (async delay, excluding model time)
histogram_quantile(0.95, sum by (queue_name, le) (rate(llm_d_async_async_queue_residence_time_millis_bucket[5m])))

# Worker utilization per pool
sum by (pool_name) (llm_d_async_async_inflight_requests) / llm_d_async_async_pool_worker_limit

# Why is a queue's gate closed? The gauges join on the queue triple, so you can
# put the budget, the value it came from, and the threshold on one panel.
llm_d_async_async_dispatch_budget
llm_d_async_async_gate_metric_value
llm_d_async_async_gate_metric_threshold

# How much headroom does each queue's gate have?
llm_d_async_async_gate_metric_value - on(queue_id, queue_name, pool_name) llm_d_async_async_gate_metric_threshold

# Throttling rate against the pool it is throttling
sum by (pool_name) (rate(llm_d_async_async_gate_decisions_total{reason="gate_closed"}[5m]))
```

### OpenTelemetry Tracing

The Async Processor supports distributed tracing via [OpenTelemetry](https://opentelemetry.io/). When enabled, it exports traces to an OTLP-compatible collector (e.g., Jaeger, Grafana Tempo, OpenTelemetry Collector).

**Spans emitted:**

| Span Name | Description |
|-----------|-------------|
| `process-request` | Per-request span covering validation, dispatch, and result routing |
| `http-request` | Child span for the outgoing HTTP call to `llm-d-router` (via `otelhttp`) |
| `re-enqueue` | Linked span created when a request is re-enqueued during graceful shutdown |

**Span attributes:**

The GenAI model attribute uses OpenTelemetry semantic conventions
[`v1.41.0`](https://pkg.go.dev/go.opentelemetry.io/otel/semconv/v1.41.0).
The request ID attribute matches llm-d-router's `gen_ai.request.id` extension;
internal attributes use the `llm_d.async.*` namespace.

| Attribute | Deprecated alias | Description |
|-----------|------------------|-------------|
| `gen_ai.request.id` | `request.id` | Request identifier on `process-request` and `re-enqueue` spans |
| `gen_ai.request.model` | — | Non-empty string `model` from the request payload on `process-request` spans; omitted when missing, empty, or not a string |
| `llm_d.async.queue.id` | `queue.id` | Queue identifier (matches Prometheus `queue_id` label); omitted when empty |
| `llm_d.async.queue.name` | `queue.name` | Queue name (matches Prometheus `queue_name` label); omitted when empty |
| `llm_d.async.retry_count` | `retry.count` | Current retry attempt (0 for first attempt) |
| `llm_d.async.error.category` | `error.category` | Error classification on failure (`RATE_LIMIT`, `SERVER_ERROR`, `UNKNOWN`, etc.) |

Deprecated aliases are emitted alongside the new attributes with identical values
for one release, then removed. Update trace queries and dashboards to use the new
attribute names during this migration window.

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

The processor also injects trace context into outgoing inference requests via W3C headers, so `llm-d-router` can continue the trace.

**Configuration:**

Tracing is controlled via standard OpenTelemetry environment variables. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to enable; leave it empty to disable (no-op).

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC collector endpoint (e.g., `http://jaeger:4317`). Empty disables tracing. | _(disabled)_ |
| `OTEL_EXPORTER_OTLP_INSECURE` | Use plaintext gRPC connection | SDK default (secure); Helm chart sets `true` |
| `OTEL_SERVICE_NAME` | Service name for traces | `llm-d-async` |
| `OTEL_TRACES_SAMPLER` | Sampling strategy (`always_on`, `parentbased_traceidratio`, etc.) | SDK default (`parentbased_always_on`); Helm chart sets `parentbased_traceidratio` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampling ratio (0.0–1.0) | Helm chart sets `1.0` |

The binary itself reads `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_SERVICE_NAME`; the remaining variables are handled by the OpenTelemetry SDK, and the defaults shown as "Helm chart sets" apply when deploying with the provided chart.

**Redis command tracing:**

- `enable_tracing` (transport config): Enable per-command Redis tracing spans via `redisotel`. Produces high span volume — use only for debugging. Default: `false`. Set it in the Redis `--transport-config` (the older `--redis-tracing` CLI flag is a deprecated alias).

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

## Backend Compatibility

The Async Processor uses the Redis wire protocol for its message queue implementations (`redis-sortedset`, `redis-pubsub`) and dispatch gates (`redis`, `redis-quota`). Redis-protocol-compatible backends such as [Valkey](https://valkey.io/) can be used with the existing Redis configuration surface.

The `url` field in the transport configuration (see [Transport Configuration](#transport-configuration)), the `REDIS_URL` environment variable, and the deprecated `--redis.*` CLI flags all work unchanged with Valkey — point them at your Valkey endpoint the same way you would with Redis.

> **Note:** The `url`/`redis.*` naming is retained because it refers to the wire protocol, not a specific product.

## Implementations

### Redis Sorted Set (Persisted)

A persisted implementation based on Redis SortedSets. Recommended for production: it offers persistence, priority sorting, and per-queue dispatch gates.

![Async Processor - Redis Sorted Set architecture](/docs/images/redis_sortedset_architecture.png "AP - Redis SortedSet")

#### Legacy Redis Sorted Set command line parameters

> **Deprecated:** Prefer `--transport redis-sortedset` with `--transport-config`/`--transport-config-file` (see [Transport Configuration](#transport-configuration)). The `--redis.ss.*` and `--redis.url` flags below still work but are deprecated aliases translated into the transport config: `--redis.url` → `url`, `--redis.ss.poll-interval-ms` → `poll_interval_ms`, `--redis.ss.batch-size` → `batch_size`, `--redis.ss.result-queue-name` → `result_queue_name`, and the single-queue `--redis.ss.igw-base-url`/`--redis.ss.request-queue-name`/`--redis.ss.request-path-url`/`--redis.ss.inference-objective`/`--redis.ss.gate-type`/`--redis.ss.gate-params` (or `--redis.ss.queues-config`/`--redis.ss.queues-config-file`) → the `queues` array.

- `redis.url`: Redis/Valkey URL (e.g. `redis://user:pass@host:port/db` or `rediss://...` for TLS). Supports Redis-protocol-compatible backends such as Valkey. Can also be set via `REDIS_URL` env var.
- `redis.ss.igw-base-url`: Base URL of the IGW (e.g. https://localhost:30800).<br> Mutually exclusive with `redis.ss.queues-config-file` flag.
- `redis.ss.request-path-url`: Request path url (e.g.: "/v1/completions"). <br> Mutually exclusive with `redis.ss.queues-config-file` flag.
- `redis.ss.inference-objective`: InferenceObjective to use for requests (set as the HTTP header x-gateway-inference-objective if not empty).  <br> Mutually exclusive with `redis.ss.queues-config-file` flag.
- `redis.ss.request-queue-name`: The name of the sorted-set for the requests. Default is <u>request-sortedset</u>.  <br> Mutually exclusive with `redis.ss.queues-config-file` flag.
- `redis.ss.result-queue-name`: The name of the list for the results. Default is <u>result-list</u>.
- `redis.ss.queues-config-file`: The configuration file name when using multiple queues — a JSON array of [queue entries](#queue-and-topic-entry-fields). <br> Mutually exclusive with `redis.ss.igw-base-url`, `redis.ss.request-queue-name`, `redis.ss.request-path-url` and `redis.ss.inference-objective` flags.
- `redis.ss.poll-interval-ms`: Poll interval in milliseconds. Default is <u>1000</u>.
- `redis.ss.batch-size`: Number of messages to process per poll. Default is <u>10</u>.
- `redis.ss.gate-type`: Gate type for single-queue mode (e.g., `redis`, `prometheus-saturation`). Only used when `redis.ss.queues-config-file` is not set.
- `redis.ss.gate-params`: JSON-encoded gate params map for single-queue mode (e.g., `{"address":"localhost:6379"}`). Only used when `redis.ss.queues-config-file` is not set.

### Redis Channels (Ephemeral)

<u>NOTE:</u> Consider using the [Redis Sorted Set](#redis-sorted-set-persisted) implementation for production use,
as it offers persistence and priority sorting.

An example implementation based on Redis channels is provided.

- Redis Channels as the request queues.
- Redis Sorted Set as the retry exponential backoff implementation.
- Redis Channel as the result queue.

This transport does not support per-queue dispatch gates (see [Queue and Topic Entry Fields](#queue-and-topic-entry-fields)).

![Async Processor - Redis architecture](/docs/images/redis_pubsub_architecture.png "AP - Redis")

#### Legacy Redis Channels command line parameters

> **Deprecated:** Prefer `--transport redis-pubsub` with `--transport-config`/`--transport-config-file` (see [Transport Configuration](#transport-configuration)). The `--redis.*` and `--redis.url` flags below still work but are deprecated aliases translated into the transport config: `--redis.url` → `url`, `--redis.retry-queue-name` → `retry_queue_name`, `--redis.result-queue-name` → `result_queue_name`, and the single-queue `--redis.igw-base-url`/`--redis.request-queue-name`/`--redis.request-path-url`/`--redis.inference-objective` (or `--redis.queues-config`/`--redis.queues-config-file`) → the `queues` array.

- `redis.url`: Redis/Valkey URL (e.g. `redis://user:pass@host:port/db` or `rediss://...` for TLS). Supports Redis-protocol-compatible backends such as Valkey. Can also be set via `REDIS_URL` env var.
- `redis.igw-base-url`: Base URL of the IGW (e.g. https://localhost:30800).<br> Mutually exclusive with `redis.queues-config-file` flag.
- `redis.request-path-url`: Request path url (e.g.: "/v1/completions"). <br> Mutually exclusive with `redis.queues-config-file` flag.
- `redis.inference-objective`: InferenceObjective to use for requests (set as the HTTP header x-gateway-inference-objective if not empty).  <br> Mutually exclusive with `redis.queues-config-file` flag.
- `redis.request-queue-name`: The name of the channel for the requests. Default is <u>request-queue</u>.  <br> Mutually exclusive with `redis.queues-config-file` flag.
- `redis.retry-queue-name`: The name of the channel for the retries. Default is <u>retry-sortedset</u>.
- `redis.result-queue-name`: The name of the channel for the results. Default is <u>result-queue</u>.
- `redis.queues-config-file`: The configuration file name when using multiple queues — a JSON array of [queue entries](#queue-and-topic-entry-fields). <br> Mutually exclusive with `redis.igw-base-url`, `redis.request-queue-name`, `redis.request-path-url` and `redis.inference-objective` flags.

### GCP Pub/Sub

The GCP PubSub implementation requires the user to configure the following:

- Requests Topic and a **Subscription** having the following configurations:
    - Exactly once delivery.
    - Retries with exponential backoff.
    - Dead Letter Queue (DLQ).
- Results Topic.

<u>Note:</u> If DLQ is NOT configured for the request topic, retried messages will be counted multiple times in the #_of_requests metric.

![Async Processor - GCP PubSub Architecture](/docs/images/gcp_pubsub_architecture.png "AP - GCP PubSub")

#### Legacy GCP PubSub command line parameters

> **Deprecated:** Prefer `--transport gcp-pubsub` with `--transport-config`/`--transport-config-file` (see [Transport Configuration](#transport-configuration)). The `--pubsub.*` flags below still work but are deprecated aliases translated into the transport config: `--pubsub.project-id` → `project_id`, `--pubsub.result-topic-id` → `result_topic_id`, `--pubsub.batch-size` → `batch_size`, and the single-topic `--pubsub.request-subscriber-id`/`--pubsub.igw-base-url`/`--pubsub.request-path-url`/`--pubsub.inference-objective` (or `--pubsub.topics-config-file`) → the `topics` array. Per-topic gating (formerly the `gcp-pubsub-gated` implementation) is now configured with `gate_type`/`gate_params` in each topic entry.

- `pubsub.project-id`: The name GCP project ID using the PubSub API.
- `pubsub.igw-base-url`: Base URL of the IGW (e.g. https://localhost:30800).<br> Mutually exclusive with `pubsub.topics-config-file` flag.
- `pubsub.request-path-url`: Request path url (e.g.: "/v1/completions"). <br> Mutually exclusive with `pubsub.topics-config-file` flag.
- `pubsub.inference-objective`: InferenceObjective to use for requests (set as the HTTP header x-gateway-inference-objective if not empty). <br> Mutually exclusive with `pubsub.topics-config-file` flag.
- `pubsub.request-subscriber-id`: The subscriber ID for the requests topic.<br> Mutually exclusive with `pubsub.topics-config-file` flag.
- `pubsub.result-topic-id`: The results topic ID.
- `pubsub.batch-size`: Number of inflight messages. Default is <u>10</u>.
- `pubsub.topics-config-file`: The configuration file name when using multiple topics — a JSON array of [topic entries](#queue-and-topic-entry-fields). <br> Mutually exclusive with `pubsub.request-subscriber-id`, `pubsub.request-path-url` and `pubsub.inference-objective` flags.

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

Publish a message for async processing (uses the [internal wire format](#internal-wire-format) since this bypasses the producer):

```bash
kubectl exec -n redis redis-master-0 -- redis-cli PUBLISH request-queue '{"request_kind":"plain","data":{"id":"testmsg","created":1764044000,"deadline":9999999999,"payload":{"model":"unsloth/Meta-Llama-3.1-8B","prompt":"hi"}}}'
```
