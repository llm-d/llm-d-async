# Multi-replica deployments

Multi-replica Async Processor deployments are under validation and are not yet
a generally supported production configuration. Use one replica unless you are
participating in validation and can tolerate the limitations below.

## Current validation matrix

| Area | Status | Current coverage |
|---|---|---|
| `redis-sortedset` with Valkey 8 | Validation in progress | E2E runs 2–3 replicas, scales during active dispatch, kills a claim owner, and checks simulator dispatches and terminal records by request ID. |
| `redis-sortedset` with Redis 8 | Validation in progress | The same nightly E2E suite runs against Redis. |
| `gcp-pubsub` | Unvalidated | Multi-replica load balancing, nack, and redelivery are not covered yet. |
| `sql` | Unvalidated/experimental | Partition rebalance, handoff, and concurrent startup are not covered yet. |
| `redis-pubsub` | Unsupported | The transport is deprecated and is intentionally excluded from multi-replica validation. |
| `prometheus-saturation` | Validation in progress | A representative metric-gate E2E verifies that three replicas leave the full batch pending and unclaimed, then complete it when saturation clears. |
| Other shared and metric gates | Unvalidated | Do not infer support from the representative saturation test. |

The chart intentionally keeps the Deployment at one replica while this matrix
is incomplete. The E2E suite scales its dedicated test Deployment directly; it
does not expose a production `replicaCount` setting.

## Concurrency and gates

Worker concurrency is local to a process. With `N` ready replicas and `W`
workers configured in each replica, the deployment has up to `N × W` workers.
The multi-replica E2E verifies the aggregate
`llm_d_async_async_pool_worker_limit` series at two and three replicas.

That multiplication does not make every gate global:

- `local-max-concurrency` is per replica, so its effective deployment limit is
  `N × max_concurrency`.
- Redis-backed shared gates coordinate through Redis, but each gate's precise
  aggregate semantics still need an explicit matrix result.
- Metric gates read the same signal independently. Cache timing can let several
  replicas act on one observation, and `endpoint-scrape` multiplies scrape load
  by the replica count. The representative saturation test proves coordinated
  stop/resume behavior, not a bound on overshoot for every metric gate.
- Gate-wait state and merge-policy state live in each process. Fairness is
  therefore per replica unless a shared mechanism explicitly provides a global
  guarantee.

## Ordering

`redis-sortedset` selects pending requests by score, which is normally the
request deadline. Atomic claims ensure that only one replica owns a particular
request generation at a time. With several consumers, however, multiple
requests can be claimed and dispatched concurrently. The result queue reflects
completion order, not a globally serialized deadline order.

Consequently, earlier-deadline work is selected first from the broker, but
strict result ordering is not guaranteed across replicas. Requests with equal
deadlines may complete in any order. Applications that require ordered output
must reorder results using application-level sequence data.

## Failure and scaling behavior under test

The Redis/Valkey E2E exercises these invariants:

1. A workload starts dispatching on two replicas, scales to three while all
   replicas have active requests, then scales back to two under the same load.
2. Per-pod and aggregate worker, dispatch, and in-flight metrics match the
   ready replicas, and the simulator observes each request ID exactly once.
3. A single replica claims a delayed request; a survivor is added; the claim
   owner is force-deleted; and the survivor completes the request after the
   recorded lease expiry, within the remaining TTL plus reclaim interval and a
   bounded scheduling allowance.
4. A three-replica saturation-gate deployment leaves the full batch pending
   and unclaimed while saturated and completes it after the signal clears.

These checks establish focused Redis/Valkey evidence. They do not yet cover
rolling upgrades, node drains, PodDisruptionBudgets, topology spreading, or
cross-version compatibility.

## Metrics aggregation

In-process gauges such as queue depth, gate waiting, in-flight requests, and
worker limits describe one replica and generally need `sum()` for a deployment
total. A broker backlog is observed independently by every replica; summing it
would multiply the same broker value by the replica count. Until the metric is
made single-writer or gains a replica-independent aggregation contract, use a
de-duplicating aggregation such as `max()` for replicas observing the same
queue.
