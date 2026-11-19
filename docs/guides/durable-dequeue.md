# Durable Dequeue (Claim / Lease / Ack)

Related:
- [Bug #404: Accepted Redis requests can be lost when an Async pod is hard-killed](https://github.com/llm-d/llm-d-async/issues/404)
- [batch-gateway #644: Async results can be lost with multiple Batch Processor replicas](https://github.com/llm-d/llm-d-batch-gateway/issues/644) (result-side counterpart)
- [batch-gateway #645: Resume in-progress batches after Processor pod or node loss](https://github.com/llm-d/llm-d-batch-gateway/issues/645)
- [Durable result delivery](durable-result-delivery.md) closes the separate result-consumer checkpoint window.

## The problem

The redis-sortedset transport used to dequeue with `ZPOPMIN`: the request was
removed from Redis before any processing happened. Between that pop and the
result being pushed back to Redis, the request existed only in process memory.
Any hard stop in that window — SIGKILL, OOM, node loss — silently lost every
accepted request it held. Graceful shutdown covered only the requests still
sitting on a single channel send.

## The model

Dequeue is now **peek → claim → ack**:

1. **Peek** — each poll reads up to `batch_size` entries with `ZRANGEBYSCORE`
   (non-destructive). Nothing leaves the pending sorted set yet.
2. **Claim** — for each entry that passes deadline/cancellation/gate checks, a
   Lua script atomically moves it out of the pending set into claim
   bookkeeping, keyed by generation identity (`claimKey = ID + RequestToken`):
   - `<queue>:claimed` — hash of `claimKey -> original member JSON`,
   - `<queue>:claim-owners` — hash of `claimKey -> random ownership token`,
   - `<queue>:claims-idx` — zset of `claimKey` scored by lease expiry
      (`min(claim_lease_ttl, deadline + 5m)`).
   Because entries in Redis are keyed by `ID + RequestToken`, concurrent or
   repeated submissions using the same request ID never overwrite each other's
   durable claim payload, owner token, or expiry index.
   With `mode: cluster` the three private keys are hash-tagged onto the
   queue (`{<queue>}:claimed` and so on) so each script stays within the
   queue's hash slot; the pending sorted set keeps its plain name.
3. **Process as before** — claimed requests flow through the same channels,
   merge policy, and workers. No downstream change.
4. **Ack** — when a terminal result is flushed, one Lua script checks
   `owners[claimKey]==claimToken`, pushes the record, and drops the claim —
   atomically. Stale owners are fenced and cannot publish. A crash between
   "inference done" and "result written" therefore redelivers the request
   instead of losing it. The request-generation identity is `RequestToken`
   (fresh per enqueue), so ID reuse with a new token is not suppressed.
   In cluster mode, when the result list does not share the queue's hash
   slot, the ack becomes three single-slot steps: renew the lease under the
   owner fence (so the reclaimer cannot redeliver mid-ack), push the record,
   drop the claim. A crash between push and drop lets the claim lapse and be
   redelivered, which can publish the record twice; durable result delivery
   de-duplicates by generation. A `request-payload` key is deleted with a
   separate command after the ack in cluster mode, and expires on its own if
   that command never runs.

While a request is held, a background **heartbeater** renews its lease every
`claim_lease_ttl / 3` (clamped to 1s–30s) with token fencing — stale owners
cannot extend a lease they no longer own. The lease TTL is therefore the
crash *detection* window, not a processing-time budget. Graceful shutdown
simply stops heartbeating; unacked claims are then redelivered via the same
lease-expiry path.

Every exit path is paired with exactly one claim outcome:

| Path | Claim outcome |
|---|---|
| Result produced (success/error/cancelled/deadline/drop) | acked after the record is durably pushed (fenced) |
| Request parked for retry | lease renewed (fenced) while it waits |
| Consumer context cancelled mid-hand-off | released back to pending at its original sort score |
| Owner dies or graceful shutdown stops heartbeating | reclaimer redelivers after lease expiry; another instance picks it up |
| Gate refuses / gate error | no claim was taken; entry simply stays pending |

## Delivery guarantees

- **At-least-once execution**: a request whose owner crashed is re-run by the
  survivor. Expensive inference may execute twice across a failure.
- **At-most-once terminal record per claim lease**: only the current lease owner
  may publish; completions from stale owners whose lease has lapsed are fenced
  (`owners[claimKey]==claimToken`). Note that claim fencing is per-claim; it does
  not by itself guarantee a single terminal record across separate redelivery
  generations if downstream systems allow multiple active executions.
- Ordering within a queue remains earliest-deadline-first; release and
  redelivery restore the original sort score.

## Configuration

| Config JSON field           | Default | Meaning                                                                                                                                                                                                                                                          |
| ----------------------------| --------| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `claim_lease_ttl_seconds`   | `300`   | Crash-detection window: how long a claim survives without a heartbeat before survivors redeliver the request. Because in-flight claims are renewed periodically by the heartbeater, this TTL does not need to cover the entire inference duration; it only needs to exceed several heartbeat intervals to absorb transient network jitter. |
| `claim_reclaim_interval_ms` | `15000` | How often expired claims are scanned for redelivery. This bounds how long a crashed instance's work stalls.                                                                                                                                                    |

Tuning is via `transport-config` JSON (`claim_lease_ttl_seconds` / `claim_reclaim_interval_ms`); CLI flags are deferred to a follow-up. The heartbeat interval is derived (`lease TTL / 3`, clamped to 1s–30s) and is not separately configurable. Claim metrics are deferred to a follow-up once the core path is proven.

## Operational requirements

- **Redis persistence is part of the durability contract.** Claims and
  queued requests live in Redis; run it with AOF (`appendonly
  yes`, e.g. `appendfsync everysec`) and/or replication. Without persistence a
  Redis restart reintroduces a loss window this feature cannot close.
- Multiple Async replicas may share one queue: atomic claims prevent double
  dispatch, and lease expiry hands work over automatically when a replica
  disappears.
- Rolling upgrades are safe for the pending format, but during the mixed-fleet
  window old instances still run destructive `ZPOPMIN` and can still lose
  requests until the rollout completes.

## Known limitations

- Graceful shutdown now relies on lease expiry like hard-kill; unacked work
  waits out its remaining lease instead of being handed back immediately. An
  immediate handback can be added as a follow-up once the core path is proven.
- If a crash occurs while a request sits in the retry queue, the reclaimer
  may redeliver the original claimed payload while the retry-queue copy
  re-enters pending via the mover — at-most one extra pending entry, bounded,
  with stale claim publication prevented by token fencing on each claim. This
  is a known trade-off to be addressed with a unified retry/claim lifecycle.
- No delivery counter or dead-letter queue: redelivery is bounded by each
  request's deadline — once it passes, the deadline-exceeded path produces the
  terminal record. Projects like asynq cap attempts and archive instead; here
  the deadline plays that role for batch workloads.
