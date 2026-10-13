# Deployment


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

## Command Line Parameters

**Core:**

| Flag | Default | Description |
|------|---------|-------------|
| `concurrency` | `64` | Number of concurrent workers (per pool if unspecified). The processor is I/O-bound (each worker holds one in-flight request for its full duration), so in-flight concurrency caps throughput — see [Queues, Topics, and Worker Pools](../README.md#queues-topics-and-worker-pools). |
| `gate-wait-timeout` | `5m` | Maximum time a worker parks one request at a pool gate before recoverably re-enqueueing it. Independent of `request-timeout`; `0` waits until the request's own deadline. |
| `transport` | `redis-pubsub` | The transport (message queue) implementation. One of `redis-pubsub` (**deprecated**: it still works but will be removed in a future release), `redis-sortedset`, `gcp-pubsub`. Gating is configured per queue/topic via `gate_type` in the transport config (this replaces the former `gcp-pubsub-gated` implementation). |
| `transport-config` | — | Inline JSON transport configuration. See [Transport Configuration](transports.md#transport-configuration). Mutually exclusive with `transport-config-file`; exactly one of the two is required. |
| `transport-config-file` | — | Path to a JSON file with the transport configuration. Mutually exclusive with `transport-config`. |
| `transport-config-watch-interval` | `0` | For `redis-sortedset` only, periodically reloads the `queues` field from `transport-config-file`. The file must contain a complete valid transport configuration. Changes to other transport fields require a restart. |
| `pool-config-file` | — | Path to the JSON worker pool definitions. If omitted, a single `"default"` worker pool is created with concurrency determined by the global `concurrency` flag. See [Worker Pools Configuration](#worker-pools-configuration). |
| `request-merge-policy-config-file` | — | Path to the JSON request merge policy specification (`type` and `parameters`). If not specified, defaults to the `random-robin` policy. The older `--request-merge-policy-config` name is a deprecated alias. |
| `transform-config-file` | — | Path to the JSON request body transform configuration. Empty disables transforms. See [Request Body Transform Reference](request-body-transforms.md#request-body-transform-reference). |

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

> **Deprecated:** The per-backend flags — `--message-queue-impl`, `--redis.url`, `--redis.*`, `--redis.ss.*`, `--pubsub.*`, `--redis-tracing`, and `--request-merge-policy-config` — still work for backwards compatibility but are deprecated. When used, the processor logs a warning and translates them into the transport config. `--transport`/`--transport-config` take precedence when both are set. The legacy flags are documented per backend under [Implementations](transports.md#implementations).

## Worker Pools Configuration

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
