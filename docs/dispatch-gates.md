# Dispatch Gate Reference


The available gate types, at a glance:

| Gate | Kind | Purpose |
|------|------|---------|
| `constant` | budget | Always fully open (budget 1.0) — no throttling. |
| `redis` | budget | Reads the dispatch budget from a Redis key managed by an external system. |
| `redis-leased-rate` | admission | Enforces a leased, pool-wide dispatch-attempt rate from Redis. Missing, invalid, unreadable, or expired commands fail closed. |
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

## Budget gates

- `constant`: Always returns budget 1.0 (fully open). No parameters.

- `redis`: Queries Redis for the dispatch budget (managed by an external system).
  - `address` (**required**): Redis server address for the dispatch gate (e.g., `localhost:6379`). Queues sharing the same address will share the same connection pool.
  - `budget_key` (optional): Redis key to read the dispatch budget from. Default is `dispatch-gate-budget`.

- `redis-leased-rate`:
  - `address` (**required**): Redis server address. Gate instances using the same address share a client connection pool.
  - `control_key` (**required**): Redis hash containing the controller's leased rate command.
  - `pool_id` (optional): Worker-pool ID the command must match. Defaults to the owning worker pool and is required when no owner is available.
  - `state_key` (optional): Redis hash used by the shared token bucket. Defaults to `<control_key>:state`.
  - `burst_seconds` (optional): Token-bucket burst window in seconds. Must be finite and positive; defaults to `1`.

  Configure this as a **pool-level** gate inside `wait-on-refuse`, so a closed
  lease parks workers in memory and rechecks with bounded jittered backoff instead
  of repeatedly returning requests to the broker. If `gate-wait-timeout` elapses,
  the held request is recoverably re-enqueued rather than completed with an error:

  ```json
  [
    {
      "id": "batch-pool",
      "workers": 64,
      "gate_type": "wait-on-refuse",
      "gate_params": {
        "gate": {
          "gate_type": "redis-leased-rate",
          "gate_params": {
            "address": "batch-gateway-valkey:6379",
            "control_key": "llm-d-async:drain-limit:batch-pool",
            "burst_seconds": "0.25"
          }
        }
      }
    }
  ]
  ```

  The controller writes the command as one Redis transaction. The field names
  and values are the public `llm-d.ai/v1alpha1` wire contract:

  ```text
  MULTI
  HSET llm-d-async:drain-limit:batch-pool \
    api_version llm-d.ai/v1alpha1 \
    pool_id batch-pool \
    max_admission_rps 12.5 \
    valid_until_unix_ms 1787928000000 \
    decision_id planner-tick-000042
  PEXPIREAT llm-d-async:drain-limit:batch-pool 1787928000000
  EXEC
  ```

  `max_admission_rps` is a ceiling on attempts sent to the downstream inference
  endpoint, including retries; it is not a promised completion rate. A value of
  `0` is an explicit pause. The lease timestamp is authoritative even if a Redis
  deployment retains the key longer than requested. The gate fails closed when
  the key cannot be read or validated, so controllers should renew leases well
  before expiry. All Async replicas using the same `control_key` and `state_key`
  share one aggregate token bucket.

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
  | 0 | `llm_d_epp_flow_control_queue_size` | `D = 1 − (queue_size / max_SYS)` | EPP runs the flow control plugin |
  | 1 | `llm_d_epp_per_endpoint_queue_size` | `D = 1 − (mean per-pod **waiting** requests / max_concurrency)` | Always — part of EPP's base metric set |
  | 2 | `vllm:num_requests_running` | `D = 1 − (running_requests / max_SYS)` | vLLM metrics carry an `inference_pool` label |

  Sources 0 and 2 compute `max_SYS = ready_pods × max_concurrency` dynamically from the
  `llm_d_epp_ready_endpoints` metric. Source 1 averages over pods, so the `ready_pods` factor
  cancels and no join is needed. That also keeps it honest when the pool drains: EPP's metrics
  refresh [returns early when the pool has no pods](https://github.com/kubernetes-sigs/gateway-api-inference-extension/blob/v1.2.1/pkg/epp/backend/metrics/logger.go#L89-L91),
  so `llm_d_epp_ready_endpoints` and `llm_d_epp_average_queue_size` freeze at their last
  values and a scaled-to-zero pool would read as idle capacity. `llm_d_epp_per_endpoint_queue_size`
  comes from a scrape-time collector that simply stops reporting, so the source yields no sample
  and the cascade moves on instead.
  The gate closes when `D ≤ baseline`; when open it returns `D − baseline`, so callers compute `N = max_SYS × (D − B)`.
  See [docs/dispatch-budget.md](dispatch-budget.md) for the full derivation.

  > [!WARNING]
  > **Source 1 measures the model server's waiting queue, not in-flight load.** The router fills
  > `llm_d_epp_per_endpoint_queue_size` from `vllm:num_requests_waiting`: requests the model server
  > has accepted but not yet scheduled. Running requests do not count. vLLM runs up to `max_num_seqs`
  > (256 by default) requests per pod before anything waits, so on a stock llm-d router, where source 1
  > is the one that resolves, the gate closes only once a pod holds about
  > `max_num_seqs + max_concurrency × (1 − baseline)` requests — roughly 350 with the defaults, not 95.
  > The saturation detector's `MaxConcurrency`, which `max_concurrency` is documented to mirror, counts
  > running plus waiting. Tracked in [#501](https://github.com/llm-d/llm-d-async/issues/501).
  >
  > For in-flight semantics today, use `prometheus-saturation` on
  > `llm_d_epp_flow_control_pool_saturation` where the router runs flow control, or `prometheus-query`
  > with the EPP's pool averages (no cascade or vLLM fallback, threshold pinned at 0):
  >
  > ```json
  > {"gate_type":"prometheus-query","gate_params":{"pool":"<pool>","fallback":"0",
  >  "query":"clamp((1 - (avg by(name)(llm_d_epp_average_running_requests{name=\"<pool>\"} + llm_d_epp_average_queue_size{name=\"<pool>\"}) / 100)) and on() (llm_d_epp_ready_endpoints{name=\"<pool>\"} > 0), 0, 1)"}}
  > ```
  >
  > `100` is the per-pod capacity; the `ready_endpoints > 0` guard makes a drained pool yield no
  > sample (the fallback) instead of the frozen averages. Needs llm-d-router v0.11.0 or later.

  The PromQL each source resolved to is logged at startup (`prometheus-budget metric source`), and
  `llm_d_async_async_gate_metric_source_available` reports whether the last evaluation got a reading from any
  of them or fell back to `fallback`.

  - `pool` (**required**): The InferencePool name. This must match the `name` field in
    `llm_d_epp_ready_endpoints{name="<pool>"}` and `llm_d_epp_per_endpoint_queue_size{name="<pool>"}`
    (EPP metrics) and, for the vLLM source, the `inference_pool` label on scraped vLLM metrics
    (added via relabeling from pod labels).
  - `namespace` (optional): Kubernetes namespace to scope metric queries. When omitted, each source requires its matching metrics to identify a single namespace. An ambiguous source returns no samples, so the cascade tries the next source; if none resolve, the configured `fallback` applies (default `0`). Set this explicitly when a shared Prometheus instance contains same-named pools in multiple namespaces.
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
    detector, both defaults are `100`. This holds for sources 0 and 2; source 1 compares the value
    against the waiting queue alone (see the warning above), so on a stock router the pool is well
    past `MaxConcurrency` before the gate reacts.
  - **Measure it.** Drive your pool to the load you consider saturated and read the per-pod peak:

    ```promql
    max_over_time(
      (sum(vllm:num_requests_running{inference_pool="<pool>",namespace="<namespace>"}) / on() max(llm_d_epp_ready_endpoints{name="<pool>",namespace="<namespace>"}))[1h:]
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
  Normalizes the metric to `[0, 1]`, then interprets it as saturation by default or as a direct budget when `value_type` is `budget`. The final budget is `clamp(interpreted_budget - baseline, 0, 1)`. Saturation metrics use `1 - normalized_value`; budget metrics use `normalized_value`.

  - `url` (**required**): Full URL to scrape (e.g., `http://vllm-sim:8000/metrics`).
  - `metric` (**required**): Metric name to extract (e.g., `vllm:num_requests_waiting`).
  - `labels` (optional): JSON object of label filters (e.g., `{"model_name":"my-model"}`). Only samples matching all labels are used.
  - `value_type` (optional): How to interpret the normalized metric. `saturation` computes budget as `1 - normalized_value`; `budget` uses `normalized_value` directly. Default is `saturation`.
  - `max_count_per_pod` (optional): Per-pod capacity. When > 0, the normalized value is `value / max_count`. When 0, the metric value is assumed to already be in [0, 1]. Default is `0`.
  - `baseline` (optional): Reserved headroom subtracted from budget. Default is `0.0`.
  - `fallback` (optional): Budget returned when scrape fails or metric is missing. Default is `0.0` (fail closed).
  - `pods_url` (optional): URL to scrape for dynamic pod count (e.g., `http://epp-svc:9090/metrics`). When set with `pods_metric`, `max_count = ready_pods * max_count_per_pod`.
  - `pods_metric` (optional): Metric name for ready pods (e.g., `llm_d_epp_ready_endpoints`).
  - `pods_labels` (optional): JSON label filters for the pods metric (e.g., `{"name":"my-pool"}`).

  **No Prometheus server required.** This gate scrapes endpoints directly, making it suitable for
  deployments without a dedicated Prometheus instance. Use `max_count_per_pod` with `pods_url`/`pods_metric`
  for dynamic scaling, or set `max_count_per_pod` to a static value for single-pod setups. For a
  readiness metric where `1` means ready and `0` means unavailable, set `value_type` to `budget`.

## Admission gates

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
  - `gating_mode` (optional): `blocking` or `classifying`. In `classifying` mode, the gate never blocks but tags the message with its quota status (`reserved` or `overflow`) in the internal metadata — see [Reserved and Overflow](../README.md#reserved-and-overflow). Default is `blocking`.

- `tier-priority-admission`: Implements a three-way admission verdict based on saturation, queue tier, and reservation classification. Saturation is determined by evaluating an inner gate: if the inner gate returns `ActionRefuse`, the pool is considered saturated. If the pool is saturated: (1) returns `ActionWait` if classification is `reserved` (parking worker threads cleanly); (2) drops immediately with a `429` status payload if tier is `interactive` and classification is `overflow`; (3) otherwise — including `async`/`batch` overflow and unclassified requests — returns `ActionRefuse` to place the request back in the queue. If not saturated, returns `ActionContinue`.
  - `saturation_gate` (**required**): The type string of the inner gate used to evaluate pool saturation (e.g. `"prometheus-query"`).
  - `saturation_gate_params` (optional): JSON-serialized string of parameters for the inner saturation gate.
  - `tier_label` (optional): The label key to check the queue's SLA tier. Default is `"tier"`.

## Combinator gates

- `composite`: Combines multiple gates. Returns the minimum budget across all inner dispatch gates and acquires quota across all inner attribute gates (all or nothing).
  - `gates` (**required**): A JSON array of gate configurations. Each configuration is an object with `gate_type` and `gate_params`.

- `wait-on-refuse`: Decorator that wraps a single inner gate and converts any `ActionRefuse` verdict into `ActionWait` (parking/polling in-memory instead of immediate broker redelivery).
  - `gate` (**required**): A JSON string containing a single gate configuration (with `gate_type` and `gate_params`) to wrap. This can be used to wrap prometheus gates in pool configuration so that they park requests instead of redelivering them to the message broker when the gate is saturated.
