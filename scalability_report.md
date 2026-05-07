# Scalability Report: Goroutine Interactions and Back-pressure Analysis

## 1. Overview
The Async Processor is built as a concurrent pipeline designed to handle high-throughput inference requests. It employs a "Pull-Process-Push" architecture with external persistence via GCP Pub/Sub or Redis.

## 2. Goroutine Architecture and Interaction Map

| Component | Goroutine(s) | Input Source | Output Destination | Concurrency / Cardinality |
| :--- | :--- | :--- | :--- | :--- |
| **Worker Pool** | `asyncworker.Worker` | `requestChannel` | `resultChannel`, `retryChannel` | `concurrency` (default 8) |
| **PubSub Ingest**| `requestWorker` | GCP Pub/Sub | `requestChannel` | 1 per Subscriber |
| **Redis Ingest** | `requestWorker` | Redis List/Channel| `requestChannel` | 1 per Queue |
| **Result Sink** | `resultWorker` | `resultChannel` | External MQ (PubSub/Redis) | 1 (Single point of bottleneck) |
| **Retry Manager**| `retryWorker` | `retryChannel` | External MQ (Sorted Set) | 1 |

### Interaction Dynamics:
- **Strict Back-pressure:** The system primarily uses **unbuffered channels** for the request path. This ensures that the ingestion goroutines (`requestWorker`) cannot pull more messages from external queues than the `Worker Pool` can currently process.
- **Worker Starvation/Saturation:** If the inference service (IGW) is slow, workers block on HTTP calls. This fills the `requestChannel`, which in turn blocks the `requestWorker`.

## 3. Implementation Analysis

### GCP Pub/Sub Flow
- **Back-pressure Mechanism:** Uses `MaxOutstandingMessages` in the PubSub library. This is dynamically scaled by the `DispatchGate` budget.
- **Bottleneck - Synchronization Overhead:** Each message handler in `sub.Receive` blocks twice:
  1. `ch <- msg`: Waiting for an available worker.
  2. `<-resultsChannel`: Waiting for the `resultWorker` to confirm the result has been published before Acknowledging (Ack) the message.
- **Bottleneck - Unbuffered Result Path:** The `resultChannel` in the PubSub implementation is **unbuffered**. If the `resultWorker` is slow in publishing to PubSub, all 8 workers will eventually block, halting the entire pipeline.

### Redis Sorted-Set Flow
- **Back-pressure Mechanism:** The `requestWorker` polls every `pollIntervalMs` (default 1s). It pops a batch of messages and attempts to push them into the `msgChannel`. If workers are busy, the poller blocks mid-batch, effectively pausing ingestion.
- **Scalability Feature - Result Batching:** Unlike PubSub, the Redis implementation uses a **buffered channel (size 64)** for results and a `resultWorker` that batches up to **32 results** in a single Redis pipeline call. This significantly reduces network round-trips and decoupling.
- **Priority Handling:** Uses Redis Sorted Sets to prioritize messages by deadline, ensuring that even under back-pressure, the most urgent tasks are processed first.

## 4. Back-pressure Bottlenecks & Scalability Risks

### High Risk: PubSub Result Pipeline
The PubSub implementation lacks the buffering and batching present in the Redis implementation.
- **Impact:** Under heavy load, the single `resultWorker` becomes a serial bottleneck. Because the worker channel is unbuffered, the inference workers (which are expensive resources) spend idle time waiting for the PubSub publisher.

### Moderate Risk: Single-threaded Retry Logic
The `retryWorker` and `addMsgToRetryQueue` are single goroutines handling all queues/topics.
- **Impact:** While retries are generally a smaller fraction of traffic, a "storm" of retryable errors (e.g., inference service 503s) could overwhelm these single goroutines, causing back-pressure to propagate back to the workers and further slowing down the system.

### Moderate Risk: Unbuffered Request Channels
While unbuffered channels provide excellent back-pressure, they offer zero "smoothing" for bursts.
- **Impact:** Any jitter in the worker processing time immediately reflects back to the ingestion layer. Small buffers (e.g., equal to `concurrency`) could improve throughput by allowing the next batch of messages to be pre-fetched while workers are finishing.

## 5. Summary of Redis vs. PubSub Scalability

| Feature | Redis Sorted-Set | GCP Pub/Sub |
| :--- | :--- | :--- |
| **Ingestion Scaling** | Polling Batching | `MaxOutstandingMessages` |
| **Result Buffering** | **64 (Buffered)** | **0 (Unbuffered)** |
| **Result Throughput** | **High (Pipelined Batching)**| **Low (Serial Publish)** |
| **Back-pressure** | Blocking Poller | Handler Synchronization |
| **Priority Support** | Yes (Sorted Set) | No (FIFO/Random) |

## 6. Strategic Recommendations
1. **PubSub Optimization:** Implement a buffered `resultChannel` and use PubSub's internal batching or a manual batching `resultWorker` similar to the Redis implementation.
2. **Decoupling:** Introduce small buffers (size 1-2x concurrency) in the `requestChannel` to smooth out ingestion jitter.
3. **Horizontal Scaling:** Ensure the `resultWorker` and `retryWorker` can be scaled if the number of workers (concurrency) is increased significantly beyond the default of 8.
