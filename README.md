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
  - [Retries](#retries)
  - [Results](#results)
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
- `concurrency`: The number of concurrent workers per pool (default: 8). Overridden per pool via `workers` in the pool config.
- `request-merge-policy`: Merge policy to use. Built-in: `random-robin`. Additional policies register via `pipeline.RegisterMergePolicy`.
- `message-queue-impl`: Queueing implementation. Options: `gcp-pubsub`, `gcp-pubsub-gated`, `redis-sortedset`, `redis-pubsub`.
- `prometheus-url`: Prometheus server URL (e.g. `http://localhost:9090`). Required for any gate using a Prometheus signal source.

<i>Additional parameters may be specified for concrete message queue implementations.</i>

## Gate System

The Async Processor evaluates gates at two points in the dispatch pipeline:

**Subscription gates** run in the message-queue receive callback, before the merge policy. They are fast and label-mutating — intended for cheap classification (e.g. deadline-drop, reservation classifiers). They must not block; a blocking subscription gate would stall the prefetch stream.

**Pool gates** run in the per-pool worker pool, after the merge policy routes the message, before HTTP dispatch. They may block — a semaphore-style gate parks the worker rather than nacking, keeping the message hot and dispatching the instant capacity opens.

### Built-in Gate Types

- `deadline-drop`: Silently drops messages whose deadline has already passed (ack without dispatch). Stateless; no params.
- `local-max-concurrency`: Per-pod semaphore. Blocks until a slot is available. Params: `max_concurrency` (required, integer ≥ 1).
- `local-rate-limit`: Per-pod token bucket. Blocks until a token is available. Params: `requests_per_minute` (required), `burst` (default 1).
- `constant-decision`: Always returns the configured verdict. Useful as a kill-switch or test fixture. Params: `decision` (`continue` | `drop` | `refuse`).

### Verdict Contract

Every gate's `Apply` method returns a `Verdict`:

- `Continue`: forward the message to the next stage.
- `Drop(result)`: ack the message without redelivery. If `result` is non-nil, publish it to the result topic first (e.g. for fail-fast 429 responses).
- `Refuse()`: nack the message; the transport's redelivery policy decides when it comes back.

### Example Config (GCP Pub/Sub `FlowConfig`)

```json
{
  "pools": [
    {
      "id": "my-pool",
      "gateway_url": "http://igw.svc:8000",
      "request_path": "/v1/completions",
      "workers": 32,
      "gates": [
        { "type": "local-max-concurrency", "params": { "max_concurrency": "64" } }
      ]
    }
  ],
  "subscriptions": [
    {
      "subscriber_id": "inference-requests-teamA-async",
      "pool": "my-pool",
      "labels": { "team": "teamA", "tier": "async" },
      "gates": [
        { "type": "deadline-drop" }
      ]
    },
    {
      "subscriber_id": "inference-requests-teamB-async",
      "pool": "my-pool",
      "labels": { "team": "teamB", "tier": "async" },
      "gates": [
        { "type": "deadline-drop" }
      ]
    }
  ]
}
```

A raw JSON array of topic configs (the legacy format) is still accepted; each entry synthesizes a single pool from its `igw_base_url` / `request_path_url` / `gate_type` / `gate_params` fields.

### Labels

Subscriptions declare static labels in their config. The Flow merges them onto every pulled message with subscription labels winning over transport-supplied attributes (Pub/Sub `Attributes`, Redis `Metadata`), so operators can pin `tier`, `team`, and `model` labels that producers cannot override.

Labels flow through to the result message and are available to the merge policy for routing and prioritisation decisions.

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

The Async Processor supports multiple request message queues. A `Request Merge Policy` fans the per-subscription channels into one buffered channel per inference pool; each pool gets its own dedicated worker pool so backpressure on one pool's downstream endpoint stays local.

Built-in: `random-robin` (randomly picks messages from all subscriptions, routing each to its declared pool). Additional policies can be registered via `pipeline.RegisterMergePolicy` from a caller-owned `main` package without modifying this repo.

## Retries

When a message processing has failed, either shedded or due to a server-side error, it will be scheduled for a retry (assuming the deadline has not passed).


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
       "inference_objective": "some_inference_objective",
       "request_path_url": "/v1/completions"
    },
    {
       "queue_name": "another_queue",
       "igw_base_url": "http://localhost:30800",
       "inference_objective": "batch_task",
       "request_path_url": "/v1/inference"
    }
]
```

<u>Note:</u> Per-queue / per-pool dispatch gates are currently only wired on the `gcp-pubsub-gated` flow. The Redis Channels and Redis Sorted Set flows do not yet apply gate chains.

**Configuration Fields:**

- `queue_name`: The name of the Redis channel for this queue.
- `igw_base_url`: Base URL of the IGW.
- `inference_objective`: The inference objective header value.
- `request_path_url`: The request path URL.

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
- `pubsub.max-extension`: Maximum Pub/Sub lease extension for a pulled request message. Default is <u>10m</u>. Pool gates and HTTP dispatch are bounded below this value to avoid redelivery races.
- `pubsub.topics-config-file`: The configuration file name when using multiple topics. <br> Mutually exclusive with `pubsub.request-subscriber-id`, `pubsub.request-path-url` and `pubsub.inference-objective` flags.

#### Multiple Topics Configuration File Syntax

The configuration file for `pubsub.topics-config-file` uses the `FlowConfig` format with named pools and subscriptions. See the [Gate System](#gate-system) section for a full example and available gate types.

**Legacy format** (raw JSON array) is still accepted; each entry synthesizes a singleton pool from its `igw_base_url`, `request_path_url`, `gate_type`, and `gate_params` fields.

**`FlowConfig` fields:**

- `pools`: Array of pool definitions. Each pool declares `id`, `gateway_url`, `request_path`, `workers`, `http_headers`, `model_name_override`, `labels`, and `gates` (pool gate chain).
- `subscriptions`: Array of subscription definitions. Each subscription declares `subscriber_id`, `pool` (pool ID reference), `labels`, `inference_objective`, and `gates` (subscription gate chain).

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
