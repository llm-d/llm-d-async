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

- [Overview](#overview)
- [When to Use](#when-to-use)
- [Design Principles](#design-principles)
- [Concepts](#concepts)
- [Deployment](docs/deployment.md): installing the processor, command line parameters, worker pools; to deploy on llm-d, follow the [asynchronous processing guide](https://github.com/llm-d/llm-d/blob/main/guides/batch-serving/asynchronous-processing/README.md)
- [Transports](docs/transports.md): transport configuration, queue and topic entries, message formats, backend compatibility, implementations
- [Dispatch Gate Reference](docs/dispatch-gates.md): every gate type and its parameters, the budget cascade, sizing `max_concurrency`
- [Request Merge Policy Reference](docs/merge-policies.md)
- [Request Body Transform Reference](docs/request-body-transforms.md)
- [Observability](docs/observability.md): Prometheus metrics and OpenTelemetry tracing
- [Development](docs/development.md)

## Concepts

Short orientation topics. Each links to the full reference section further down.

### Transports

The transport is the message queue backend the processor pulls requests from and writes results to. Three implementations are available: `redis-pubsub` (ephemeral Redis channels — **deprecated**, prefer `redis-sortedset`), `redis-sortedset` (persisted, priority-sorted Redis — recommended for production), and `gcp-pubsub` (GCP Pub/Sub). The transport is selected with `--transport` and configured with a single JSON document. Redis-protocol-compatible backends such as Valkey work unchanged (see [Backend Compatibility](docs/transports.md#backend-compatibility)). → [Transport Configuration](docs/transports.md#transport-configuration)

### Queues, Topics, and Worker Pools

Each queue (Redis) or topic (GCP Pub/Sub) entry in the transport config names the gateway it dispatches to (`igw_base_url`), the request path, an optional inference objective, and the worker pool that serves it (`worker_pool_id`). A worker pool has a fixed number of workers; each worker holds one in-flight request for its full duration, so pool concurrency caps throughput by Little's Law — tune it to your backend's latency/throughput target (see the [Async Processor Operations Guide](https://github.com/llm-d/llm-d/blob/main/docs/operations/async-processor.md)). Multiple queues can share a pool. → [Worker Pools Configuration](docs/deployment.md#worker-pools-configuration)

### Dispatch Gates

A gate decides whether a message pulled from the broker may be dispatched right now, based on system capacity or admission policy. Gates run at two levels:

* **Queue-level gates** run at the admission phase for a specific queue. When a queue-level gate denies admission (returning `ActionRefuse`), the request is immediately returned to the broker to be retried/re-delivered, freeing the worker to process other queues.
* **Pool-level gates** run directly inside the worker loop to regulate capacity constraints shared by all queues routing to that worker pool. When a pool-level gate returns `ActionWait`, the worker parks in-memory and polls until capacity is available, avoiding broker nack/retry overhead. If the pool-level gate returns `ActionRefuse`, the request is immediately returned to the broker.

Gates come in two flavors: **budget gates** report a fraction of available capacity in [0, 1] (e.g. the `prometheus-*` gates), and **admission gates** issue a per-message verdict — continue, wait, refuse, or drop (e.g. `redis-quota`, `tier-priority-admission`). Combinator gates (`composite`, `wait-on-refuse`) assemble them. → [Dispatch Gate Reference](docs/dispatch-gates.md#dispatch-gate-reference)

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

The processor consumes from multiple queues concurrently. A merge policy merges messages from all active queues — not globally, but **per worker pool**: input channels are grouped by `worker_pool_id` and each pool gets its own independent merged channel. This gives complete backpressure isolation: a slow or saturated pool blocks only its own merged channel. Two policies exist: `random-robin` (default) and `tier-priority`. Both can stamp a fairness-identity header for the gateway's flow control. → [Request Merge Policy Reference](docs/merge-policies.md#request-merge-policy-reference)

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

By default the worker dispatches the OpenAI-style JSON marshalled from a request's `payload`. Body-transform plugins rewrite the outgoing body and `Content-Type` based on per-message `metadata` — for example turning JSON into `multipart/form-data` for Whisper-style endpoints. The default JSON path is preserved byte-for-byte when no plugin applies. → [Request Body Transform Reference](docs/request-body-transforms.md#request-body-transform-reference)

### Results

For each request, a result message keyed by the request `id` is written to the result queue/topic — carrying the HTTP status and response payload on success, or an error code and message on failure. → [Result Messages](docs/transports.md#result-messages)

## Moved sections

These headings stay so that links from before the split keep resolving; the content lives under `docs/`.

### Transport Configuration

[docs/transports.md](docs/transports.md#transport-configuration)

### Dispatch Gate Reference

[docs/dispatch-gates.md](docs/dispatch-gates.md#dispatch-gate-reference)

### Prometheus Metrics

[docs/observability.md](docs/observability.md#prometheus-metrics)
