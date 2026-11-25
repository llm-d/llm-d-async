# Deadline Scaling

> **Experimental:** opt in per queue with a `deadline_scaling` block. The settings and the gauge they enable may change between releases without a deprecation period.

A queue with a `deadline_scaling` block gets the gauge `llm_d_async_async_deadline_required_replicas`: the replicas its deployment needs to finish the queue's queued items before their deadlines. It is meant as the trigger of an autoscaler such as KEDA. The block only adds the gauge; dispatch is the same with or without it.

## Configuration

`deadline_scaling` is a field of a queue entry on the `redis-sortedset` and `sql` transports, the two that can read per-item deadlines (see [Queue and Topic Entry Fields](transports.md#queue-and-topic-entry-fields)):

```json
{
  "queue_name": "batch_queue",
  "igw_base_url": "http://localhost:30800",
  "deadline_scaling": {
    "throughput_rps": 2,
    "cold_start_seconds": 120,
    "max_replicas": 8,
    "safety_factor": 0.8
  }
}
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `throughput_rps` | yes | — | Items of this queue that one replica finishes per second (R). Must be positive. |
| `cold_start_seconds` | no | `0` | Seconds a new replica takes to start serving (C): scheduling, image pull and model load. Zero or more. |
| `max_replicas` | yes | — | Cap on the gauge, normally the deployment's maximum replica count. At least 1. |
| `safety_factor` | no | `1` | In (0, 1]: the share of a replica's serving time before each bucket boundary that counts as usable (s), to absorb the coarseness of the buckets. Lower values scale up sooner. |

The block is checked when the configuration loads and, for `redis-sortedset`, on each queue hot reload. An invalid block fails startup, or fails the reload and leaves the last good configuration in effect.

## How the value is computed

Each backlog poll (`--metrics-backlog-poll-interval`, default `15s`; `0` turns polling and the gauge off) reads the deadline-proximity counts. For each bucket boundary t (1s, 5s, 15s, 30s, 1m, 2m, 5m, 10m, 30m, 1h, 2h, 6h and 24h), B(t) is the number of the queue's items due within t that haven't expired. A replica added now serves from C on, so by t it finishes R · (t − C) items, scaled down by s. The gauge is the largest need over the boundaries:

```text
max over t of  B(t) / (R · (t − C) · s)
```

rounded up and capped at `max_replicas`.

- Items past their deadline don't count: no number of replicas makes them on time.
- Items due within a boundary t ≤ C set the gauge to `max_replicas`, since they are due before a new replica could start.
- A queue with nothing due within 24 hours reads 0, and any item due within 24 hours keeps it at 1 or more. Items due later don't count until they are within 24 hours.
- A failed deadline read drops the queue's series for that poll, rather than reporting 0 from zeroed counts. Removing the block on a reload, or removing the queue, removes the series.

## Scaling with KEDA

The value is per queue, so it is right only when each queue has its own deployment: summing the gauges of queues that share one over-counts. The gauge carries only `queue_id` and `queue_name` for that reason. Every processor replica reads the whole queue and publishes the same value, so aggregate across them with `max()`. With `threshold: "1"`, the replica count the HPA aims for follows the gauge:

```yaml
triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.monitoring.svc:9090
      query: max(llm_d_async_async_deadline_required_replicas{queue_id="batch_queue"})
      threshold: "1"
      ignoreNullValues: "false"
```

KEDA reads an empty query result as 0 by default. `ignoreNullValues: "false"` makes it an error instead, so that a ScaledObject's `fallback` takes over when the series stays missing, for example after repeated failed deadline reads or while no processor is running.
