# Durable Result Delivery (Receive / Lease / Ack)

Related:

- [llm-d-async #423: Results can be lost after Producer GetResult removes them before consumer persistence](https://github.com/llm-d/llm-d-async/issues/423)
- [llm-d-batch-gateway #645: Resume in-progress batches after Processor pod or node loss](https://github.com/llm-d/llm-d-batch-gateway/issues/645)
- [llm-d-batch-gateway #662: Route results to the owning Processor replica](https://github.com/llm-d/llm-d-batch-gateway/issues/662)

## The problem

`Producer.GetResult` uses destructive `BRPOP`. Once it returns, a consumer crash before durable checkpointing permanently loses the terminal result. Request-side durable dequeue protects work only until Async publishes the terminal result; it does not close this consumer-side loss window.

## The model

The Redis sorted-set producer now also implements the additive `DurableResultProducer` interface:

1. `ReceiveResult` atomically claims the oldest result from the producer's configured `ResultQueueName`. The payload leaves the pending list but remains in route-local Redis claim state.
2. The consumer durably checkpoints the returned `ResultDelivery.Result`. Its stable deduplication key is `(Result.ID, Result.Routing.RequestToken)`.
3. `RenewResult` extends the owner-fenced lease if checkpointing takes longer than the configured lease TTL.
4. `AckResult` is owner-fenced and atomically removes the claimed payload only after durable acceptance. Repeating a successful ACK is safe.
5. If the consumer stops renewing before ACK, a later `ReceiveResult` moves the expired payload back to the same route and claims it for redelivery. The stale owner can no longer renew or ACK it.

The claim state for result route `<route>` is:

| Redis key | Type | Purpose |
| --- | --- | --- |
| `<route>` | list | Pending results; publication remains `LPUSH` and receive remains oldest-first (`RPOP`) |
| `<route>:result-claimed` | hash | Generation identity to original result JSON |
| `<route>:result-claim-owners` | hash | Generation identity to random lease-owner token |
| `<route>:result-claims-idx` | sorted set | Generation identity scored by lease-expiry Unix milliseconds |
| `<route>:result-ack-tombstones` | sorted set | Recently ACKed generations used for idempotency and duplicate suppression |

ACK tombstones expire after seven days. Each receive and ACK removes expired entries, and the tombstone key itself has a seven-day Redis expiry, so idle routes clean themselves up.

Against a Redis Cluster (`RedisMode: producer.RedisModeCluster`, or an injected cluster client) the four private keys are hash-tagged onto the route — `{<route>}:result-claimed` and so on — so every script stays within the route's hash slot. The pending list itself keeps its plain name, which is the contract with the dispatcher.

## Wire compatibility

Published result JSON keeps the existing top-level `ResultMessage` fields and adds only the optional top-level `request_token` field. Legacy `GetResult` and consumers that unmarshal `ResultMessage` continue to work because unknown JSON fields are ignored. New producer parsing restores `request_token` into `Result.Routing.RequestToken`.

Results published by older Async versions do not contain `request_token`. Durable receive accepts those records and falls back to ID-only identity, but repeated submissions that reuse the same result ID cannot be generation-isolated during the tombstone window. Complete the Async publisher rollout before relying on generation-safe deduplication.

## Configuration and recovery time

`WithResultClaimLeaseTTL` controls the crash-detection window and defaults to five minutes. `WithResultClaimReclaimInterval` controls how often a blocked `ReceiveResult` checks pending results and expired claims and defaults to one second. The expected redelivery delay after a hard failure is at most the remaining lease plus one reclaim interval, subject to Redis availability. Consumers should renew well before the lease expires while a durable checkpoint is in progress.

Both values are explicit producer options, and `ResultDeliveryConfig` reports the effective values so deployments can observe their recovery contract alongside the consumer configuration. Non-positive durations are rejected.

## Delivery guarantees and boundaries

- Delivery is at least once until ACK. A failure after checkpointing but before ACK can redeliver, so the consumer must deduplicate before producing externally visible output.
- Only the current lease owner can renew or ACK. Lease expiry fences stale replicas.
- Malformed payloads remain in leased claim state instead of being discarded. `ReceiveResult` returns `ErrUnparsableResult`; consumers should report it and continue receiving so valid records behind it can progress. The malformed payload becomes eligible for redelivery after its lease expires, which permits recovery after a compatible reader is rolled out.
- FIFO ordering and arbitrary exact `ResultQueueName` routes are preserved. Claims never move a result to a different route.
- Redis persistence and replication remain part of the durability contract; a non-persistent Redis loss is outside this guarantee.
- Result transport durability does not implement Batch Gateway job manifests, replacement-pod route takeover, output reconstruction, or checkpointing. Those remain Batch Gateway #645 responsibilities.

## Rollout

Keep existing `Producer.GetResult` consumers on their current routes during migration. A destructive `GetResult` consumer and a durable `ReceiveResult` consumer must never share a route because `BRPOP` bypasses claim state and can still delete a result permanently. Roll out enriched Async publishers first, then switch each configured result route to a consumer that checkpoints and deduplicates by `(ID, RequestToken)` before ACK. The legacy method remains available for staged rollout and rollback.
