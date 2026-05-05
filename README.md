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
- `concurrency`: The number of concurrenct workers, default is 8.
- `request-merge-policy`: Currently only supporting <u>random-robin</u> policy.
- `message-queue-impl`: Implementation of the queueing system. Options are <u>gcp-pubsub</u> for GCP PubSub, <u>gcp-pubsub-gated</u> for GCP PubSub with per-topic gating, <u>redis-sortedset</u> for Redis Sorted Set (persisted and sorted), and <u>redis-pubsub</u> for ephemeral Redis-based implementation.

 - `prometheus-url`: Prometheus server URL for metric-based gates (e.g., http://localhost:9090). For Google Managed Prometheus (GMP), point this to a local proxy or GMP frontend that handles authentication — direct GMP URLs are not supported as the Async Processor does not perform GMP authentication.  
   This flag is required when using metric-based per-queue gates (e.g., `prometheus-saturation`, `prometheus-budget`).

<i>additional parameters may be specified for concrete message queue implementations</i>

## Dispatch Gates

The Async Processor supports dispatch gates to control batch processing based on system capacity. Gates can be configured per-queue (via configuration files).

### Per-Queue Dispatch Gates

For more fine-grained control, configure gates per queue in your configuration file. Each queue can have its own gate type and parameters.

**Gate Types:**

- `constant`: Always returns budget 1.0 (fully open) - no throttling.
- `redis`: Queries Redis for dispatch budget (managed by external system).
- `prometheus-saturation`: Queries Prometheus for pool saturation metric. The gate closes (returns `0.0`) when saturation ≥ threshold; when open it returns `(1 - saturation) - (1 - threshold)`, i.e. the margin below the threshold.
- `prometheus-budget`: Computes a dispatch budget D using a cascade of two Prometheus metric sources. Both sources compute `max_SYS = ready_pods × max_concurrency` dynamically. The primary source uses the EPP flow control queue size: `D = 1 − (queue_size / max_SYS)`. If the primary is unavailable, it falls back to a secondary source using vLLM and pool metrics: `D = 1 − (running_requests / max_SYS)`. The gate closes when D ≤ B (baseline); callers compute `N = max_SYS × (D − B)`. See [docs/dispatch-budget.md](docs/dispatch-budget.md) for details.

**Example Configuration with Per-Queue Gates:**

```json
[
    {
       "queue_name": "critical_queue",
       "inference_objective": "critical-task",
       "request_path_url": "/v1/inference",
       "gate_type": "constant"
    },
    {
       "queue_name": "batch_queue",
       "inference_objective": "batch-task",
       "request_path_url": "/v1/inference",
       "gate_type": "prometheus-saturation",
       "gate_params": {
          "pool": "inference_pool_1",
          "threshold": "0.8"
       }
    },
    {
       "queue_name": "batch_budget_queue",
       "inference_objective": "batch-task",
       "request_path_url": "/v1/inference",
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
       "request_path_url": "/v1/inference",
       "gate_type": "redis",
       "gate_params": {
          "address": "localhost:6379",
          "budget_key": "my-budget-key"
       }
    }
]
```

**Gate Parameters:**

- `redis`:
  - `address` (**required**): Redis server address for the dispatch gate (e.g., `localhost:6379`). Queues sharing the same address will share the same connection pool.
  - `budget_key` (optional): Redis key to read dispatch budget from. Default is `dispatch-gate-budget`.

- `prometheus-saturation`:
  - `pool` (**required**): The inference pool name to filter metrics by.
  - `threshold` (optional): Saturation threshold (0.0-1.0). When saturation >= threshold, budget is 0.0. Default is `0.8`.
  - `fallback` (optional): Fallback saturation value (0.0-1.0) used when the metric source returns an error or empty data. Default is `0.0`.

- `prometheus-budget`: Cascades two Prometheus metric sources to compute a dispatch budget D.
  Both sources compute `max_SYS = ready_pods × max_concurrency` dynamically from the `inference_pool_ready_pods` metric.
  The primary source computes D from the EPP's flow control queue size: `D = 1 − (queue_size / max_SYS)`.
  If the primary metric is unavailable (e.g. EPP is down), the gate falls back to a secondary source
  that estimates saturation from vLLM and pool metrics: `D = 1 − (running_requests / max_SYS)`.
  The gate closes when `D ≤ baseline`; when open it returns `D − baseline`, so callers compute `N = max_SYS × (D − B)`.
  See [docs/dispatch-budget.md](docs/dispatch-budget.md) for the full derivation.
  - `pool` (**required**): The inference pool name to filter metrics by.
  - `max_concurrency` (optional): Per-endpoint request capacity (`MaxConcurrency` in the [inference scheduler's saturation detector](https://github.com/llm-d/llm-d-inference-scheduler/blob/main/pkg/epp/framework/plugins/flowcontrol/saturationdetector/concurrency/config.go)). Default is `100` (matching the inference scheduler default).
  - `baseline` (optional): Reserved baseline B. The gate closes when D ≤ B. Default is `0.05`.
  - `fallback` (optional): Fallback budget value (0.0-1.0) returned when all metric sources are unavailable. Default is `0.0` (fail closed).

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

The Async Processor supports multiple request message queues. A `Request Merge Policy` can be specified to define the merge strategy of messages from the different queues.

Currently the only policy supported is `Random Robin Policy` which randomly picks messages from the queues.

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

<u>Note:</u> The ephemeral Redis Channels implementation does not support per-queue dispatch gates. Use the [Redis Sorted Set](#redis-sorted-set-persisted) implementation for per-queue gating.

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
- `pubsub.topics-config-file`: The configuration file name when using multiple topics. <br> Mutually exclusive with `pubsub.request-subscriber-id`, `pubsub.request-path-url` and `pubsub.inference-objective` flags.

#### Multiple Topics Configuration File Syntax

The configuration file when using the `pubsub.topics-config-file` flag should have the following format:

```json
[
    {
       "igw_base_url": "http://localhost:30800",
       "subscriber_id": "some_subscriber_id",
       "inference_objective": "some_inference_objective",
       "request_path_url": "e.g.: /v1/completions",
       "gate_type": "constant",
       "gate_params": {}
    },
    {
       "subscriber_id": "another_subscriber",
       "inference_objective": "batch_task",
       "request_path_url": "/v1/inference",
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
- `inference_objective`: The inference objective header value.
- `request_path_url`: The request path URL.
- `gate_type`: Required type of dispatch gate for this topic.
- `gate_params` (optional): Parameters for the gate type (e.g., pool name, threshold for prometheus gates).

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