# E2E Deploy: Async Processor with Dispatch Budget Gate

Deploy llm-d-async with a `prometheus-budget` gate on a real Kubernetes cluster,
backed by a real vLLM model server and the upstream llm-d stack.

## Prerequisites

- Kubernetes cluster with GPU nodes (A100 tested)
- `kubectl`, `helm`, `jq` installed
- Access to the [llm-d](https://github.com/llm-d/llm-d) repo (checked out locally)

```bash
export LLM_D_REPO=/path/to/llm-d        # local checkout of github.com/llm-d/llm-d
export ASYNC_REPO=/path/to/llm-d-async   # this repo
export NAMESPACE=llm-d-async              # choose your namespace
export GAIE_VERSION=v1.5.0
export ROUTER_CHART_VERSION=v0.9.0
export GATEWAY_API_VERSION=v1.5.1
export GUIDE_NAME=optimized-baseline
```

`GAIE_VERSION` and `ROUTER_CHART_VERSION` track upstream; `${LLM_D_REPO}/guides/env.sh`
is the source of truth for the versions the llm-d guides are tested against.

## Step 1: Install CRDs

```bash
kubectl apply -k "https://github.com/kubernetes-sigs/gateway-api/config/crd?ref=${GATEWAY_API_VERSION}"
kubectl apply -k "https://github.com/kubernetes-sigs/gateway-api-inference-extension/config/crd?ref=${GAIE_VERSION}"
```

## Step 2: Create namespace

```bash
kubectl create namespace ${NAMESPACE}
```

## Step 3: Install Istio (skip if already installed)

```bash
kubectl get pods -n istio-system  # check first

# If not installed:
ISTIO_VERSION=1.29.0
curl -L https://istio.io/downloadIstio | ISTIO_VERSION=${ISTIO_VERSION} sh -
export PATH="$PWD/istio-${ISTIO_VERSION}/bin:$PATH"
istioctl install -y --set values.pilot.env.ENABLE_GATEWAY_API_INFERENCE_EXTENSION=true
```

Reference: [llm-d istio gateway guide](https://github.com/llm-d/llm-d/blob/main/docs/infrastructure/gateway/istio.md)

## Step 4: Deploy Gateway

```bash
kubectl apply -k ${LLM_D_REPO}/guides/recipes/gateway/istio -n ${NAMESPACE}
kubectl wait --for=jsonpath='{.status.conditions[?(@.type=="Programmed")].status}'=True \
    gateway/llm-d-inference-gateway -n ${NAMESPACE} --timeout=120s
```

## Step 5: Deploy llm-d Router (EPP) with monitoring enabled

```bash
helm install ${GUIDE_NAME} \
    oci://ghcr.io/llm-d/charts/llm-d-router-gateway \
    -f ${LLM_D_REPO}/guides/recipes/router/base.values.yaml \
    -f ${LLM_D_REPO}/guides/${GUIDE_NAME}/router/${GUIDE_NAME}.values.yaml \
    -f ${LLM_D_REPO}/guides/recipes/router/features/monitoring.values.yaml \
    --set provider.name=istio \
    --set httpRoute.create=true \
    --set httpRoute.inferenceGatewayName=llm-d-inference-gateway \
    -n ${NAMESPACE} --version ${ROUTER_CHART_VERSION}
```

This creates the EPP `Deployment` and `Service`, an `InferencePool` named
`optimized-baseline` (selecting pods labeled `llm-d.ai/guide: optimized-baseline`,
which is what Step 6 applies), an `HTTPRoute` attached to the gateway, and — from
`monitoring.values.yaml` — a `ServiceMonitor` named `optimized-baseline-epp-monitor`
scraping EPP `/metrics` every 10s. The chart also creates a dedicated `ClusterRole`
and `ClusterRoleBinding` for the EPP ServiceAccount granting `tokenreviews`,
`subjectaccessreviews`, and `nonResourceURLs: /metrics`.

The recipe leaves metrics authentication off (`auth.enabled: false`), so the
ServiceMonitor scrapes without a bearer token and no metrics-reader `Secret` is
created. If you enable auth, Prometheus needs a token with the `/metrics`
permission granted by that ClusterRole.

Reference: [llm-d observability setup](https://github.com/llm-d/llm-d/blob/main/docs/operations/observability/setup.md)

## Step 6: Deploy vLLM model server (Qwen/Qwen3-0.6B)

The model server manifests use the same kustomize base+overlay pattern as the upstream
[llm-d optimized-baseline guide](https://github.com/llm-d/llm-d/tree/main/guides/optimized-baseline/modelserver),
but configured for a single-replica Qwen3-0.6B on 1x GPU.

```bash
kubectl apply -n ${NAMESPACE} -k ${ASYNC_REPO}/docs/guides/e2e-deploy/modelserver/
```

This deploys a single-replica vLLM (v0.19.1) serving `Qwen/Qwen3-0.6B` on 1x A100 GPU.

Pod labels serve two purposes:
- `llm-d.ai/guide: optimized-baseline` — matches the InferencePool selector (EPP discovers this pod)
- `inference_pool: optimized-baseline` — carried into vLLM metrics via PodMonitor relabeling
  (required for the dispatch budget gate's PromQL queries)

> **Using the upstream llm-d overlays instead?** They label decode pods with `llm-d.ai/*` only
> and do not set `inference_pool`, so the relabeling produces nothing and the gate's PromQL
> matches an empty vector, reads `NaN`, and never opens. Either add the label to your overlay as
> the kustomization above does, or set `ap.modelServerMonitor.inferencePool` to the same value as
> `gate_params.pool` and the chart will apply it at scrape time.

Wait for the model to load:

```bash
kubectl wait --for=condition=Ready pod -l llm-d.ai/role=decode -n ${NAMESPACE} --timeout=300s
```

## Step 7: Install Prometheus (skip if already installed)

```bash
cd ${LLM_D_REPO}
./guides/recipes/observability/install-prometheus-grafana.sh
```

This installs the kube-prometheus-stack into the `llm-d-monitoring` namespace,
watching all namespaces.

Reference: [llm-d observability setup](https://github.com/llm-d/llm-d/blob/main/docs/operations/observability/setup.md)

Verify EPP metrics are flowing into Prometheus:

```bash
kubectl run --rm -i prom-check --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s "http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query?query=inference_pool_ready_pods"
```

Expected: `inference_pool_ready_pods{name="optimized-baseline"} = 1`

## Step 8: Install Redis (or Valkey)

[Valkey](https://valkey.io/) is a BSD-licensed, Redis-compatible alternative that works as a drop-in replacement. All `redis.*` flags and configurations work unchanged with Valkey.

**Option A — Redis:**

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install redis bitnami/redis -n redis --create-namespace --set auth.enabled=false
```

**Option B — Valkey:**

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install redis bitnami/valkey -n redis --create-namespace --set auth.enabled=false
```

## Step 9: Deploy Async Processor with dispatch budget gate

```bash
helm install llm-d-async ${ASYNC_REPO}/charts/llm-d-async/ \
    -f ${ASYNC_REPO}/docs/guides/e2e-deploy/llm-d-async-values.yaml \
    -n ${NAMESPACE}
```

The values file (`docs/guides/e2e-deploy/llm-d-async-values.yaml`) configures:
- Image: `ghcr.io/llm-d/llm-d-async:938cd44`
- Queue: `redis-sortedset` transport, configured via `transport` / `transportConfig`.
  Connection: `transportConfig.urlSecret.url` — the chart creates a Secret from the URL and
  injects it as `REDIS_URL` (kept out of the pod args). This dev Redis is unauthenticated;
  for a **credentialed or production** Redis, reference an existing Secret instead with
  `ap.transportConfig.urlSecret: {name: <secret>, key: url}`
- Gate: `prometheus-budget` with pool=`optimized-baseline`, max_concurrency=100, baseline=0.05 (per-queue).
  `max_concurrency` is a **per ready pod** capacity — see [Size `max_concurrency` for your pool](#size-max_concurrency-for-your-pool)
  before reusing this value on your own model
- Prometheus URL pointing to the cluster's `llmd-kube-prometheus-stack-prometheus` service

> **Multi-namespace deployments:** If the cluster has multiple inference pools
> with the same name in different namespaces, the `prometheus-budget` and
> `prometheus-saturation` gate queries may match multiple time series and fail
> with many-to-many matching errors. Add `namespace` to `gate_params` to scope
> the queries:
> ```yaml
> gate_params:
>   pool: "optimized-baseline"
>   namespace: "my-namespace"    # scope metrics to this namespace
>   max_concurrency: "100"
>   baseline: "0.05"
> ```
- `modelServerMonitor.enabled: true` — creates a PodMonitor that relabels the `inference_pool`
  pod label into vLLM metrics (required for the dispatch budget gate fallback). This guide puts
  the model server and llm-d-async in the same namespace; if yours are split, also set
  `modelServerMonitor.namespaceSelector.matchNames` to the model server's namespace, or the
  monitor matches no pods and scrapes nothing without reporting an error
- `podMonitor.enabled: true` — creates a PodMonitor that scrapes the llm-d-async's own
  Prometheus metrics (retry rate, success rate, latency, etc.)
- `prometheusRule.enabled: true` — installs alert rules for high retry rate, deadline exceeded
  rate, low success rate, and high shed rate
- `grafana.dashboards.enabled: true` — provisions a Grafana dashboard (via sidecar) with
  request rate, outcome breakdown, success/retry gauges, and latency percentiles

### Size `max_concurrency` for your pool

`max_concurrency` is the request capacity of **one ready pod**, not of the pool. Every source in
the cascade divides by it per pod — sources 0 and 2 divide pool-wide load by
`max_SYS = ready_pods × max_concurrency`, source 1 averages per pod first — so whichever one
resolves, the gate closes only once load reaches:

```
max_concurrency × (1 - baseline)   concurrent requests per ready pod
```

With the values above that is `100 × 0.95` = **95 concurrent requests per pod**, and this guide
deploys a single Qwen3-0.6B replica. That is reachable here — the saturation test below drives
200 concurrent requests, and 100 also matches the default `MaxConcurrency` of the EPP's saturation
detector, so the async gate and the EPP agree on when the pool is full.

It is not automatically reachable anywhere else. Point this configuration at a larger model on a
few replicas and a realistic workload may peak in the single digits per pod, far below the closing
point — in which case the gate never closes and every batch request dispatches regardless of live
traffic, which is the exact failure the gate exists to prevent.

The processor logs its resolved closing point when it builds the gate, so check it against reality:

```bash
kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=llm-d-async | grep "prometheus-budget gate configured"
# "prometheus-budget gate configured" pool=optimized-baseline maxConcurrency=100 baseline=0.05 closesAtLoadPerReadyPod=95
```

To derive the value for your own model and hardware, drive the pool to the load you consider
saturated and read the per-pod peak:

```bash
kubectl run --rm -i prom-peak --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode \
    'query=max_over_time((sum(vllm:num_requests_running{inference_pool="optimized-baseline"}) / on() inference_pool_ready_pods{name="optimized-baseline"})[1h:])' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
```

Set `max_concurrency` to that peak. Well above it and the gate never closes; well below it and the
gate sheds while the pool still has room. If you change it, update the `* 100` divisor in the
verification queries below to match.

## Verify

### Check pods and gate status

```bash
# All pods running
kubectl get pods -n ${NAMESPACE}

# At startup, "prometheus-budget metric source" lines print the PromQL each cascade
# source resolved to, indexed 0-2. Then "metric source resolved" reports which one
# answered — expect sourceIndex=1 (EPP per-pod queue depth), since the llm-d router's
# EPP does not enable the flow control plugin source 0 needs.
# You should NOT see "all metric sources unavailable".
kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=llm-d-async --tail=20

```

### Verify metrics pipeline

```bash
# EPP metrics in Prometheus
kubectl run --rm -i prom-check --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s "http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query?query=inference_pool_ready_pods"
# Expected: inference_pool_ready_pods{name="optimized-baseline"} = 1

# Wait for vLLM metrics with inference_pool label to appear (via PodMonitor relabeling).
# The entire process might take a couple of minutes.
echo "Waiting for vLLM metrics with inference_pool label..."
until kubectl run --rm -i prom-wait-$RANDOM --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -sf --data-urlencode 'query=count(vllm:num_requests_running{inference_pool="optimized-baseline"})' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query' \
    | grep -q '"result":\[{"metric"'; do
  echo "  not yet, retrying in 10s..."
  sleep 10
done
echo "vLLM metrics available."

# Full gate budget query (should return 1.0 at idle). This is cascade source 1, the
# one a stock llm-d install lands on; source 2 (vLLM) is only reached if this returns
# nothing, and source 0 needs EPP's flow control plugin.
kubectl run --rm -i prom-budget --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode \
    'query=1 - (avg by(name)(inference_pool_per_pod_queue_size{name="optimized-baseline"}) / 100)' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: value = 1

# The vLLM fallback (source 2), which needs the relabeled inference_pool label
kubectl run --rm -i prom-budget-vllm --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode \
    'query=1 - (sum(vllm:num_requests_running{inference_pool="optimized-baseline"}) / on() (inference_pool_ready_pods{name="optimized-baseline"} * 100))' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: value = 1
```

### Verify llm-d-async monitoring

Once you have sent at least one async request (see next section), verify that the
llm-d-async's own metrics are being scraped and that alerts/dashboards are available:

```bash
# Async-processor metrics in Prometheus (should show request counters)
kubectl run --rm -i prom-ap --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode 'query=llm_d_async_async_request_total' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: one or more time series with queue_id/queue_name labels

# Verify PrometheusRule is installed
kubectl get prometheusrules -n ${NAMESPACE}
# Expected: llm-d-async rule listed

# Verify Grafana dashboard ConfigMap is present
kubectl get configmap -n ${NAMESPACE} -l grafana_dashboard=1
# Expected: llm-d-async-dashboards ConfigMap listed
```

### Test async request end-to-end

Requests use the `InternalRequest` wire format with a `request_kind` envelope:

```bash
export REDIS_HOST=redis-master.redis.svc.cluster.local

# Push a valid request
kubectl run --rm -i test-push --image=redis --restart=Never -n ${NAMESPACE} -- \
    redis-cli -h ${REDIS_HOST} ZADD request-sortedset 1999999999 \
    '{"request_kind":"redis","internal":{},"data":{"id":"test-1","created":1700000000,"deadline":1999999999,"payload":{"model":"Qwen/Qwen3-0.6B","prompt":"What is 2+2?"}}}'

# Wait a few seconds, then check result
sleep 5
kubectl run --rm -i test-result --image=redis --restart=Never -n ${NAMESPACE} -- \
    redis-cli -h ${REDIS_HOST} LPOP result-list
```

Expected: JSON with `"id":"test-1"` and a completion payload from Qwen3-0.6B.

### Test gate closed (scale vLLM to 0)

```bash
# Scale down — gate should close (budget=0, metrics return NaN)
kubectl scale deployment vllm-qwen3-0-6b-decode -n ${NAMESPACE} --replicas=0

# Push a request while gate is closed
kubectl run --rm -i test-closed --image=redis --restart=Never -n ${NAMESPACE} -- \
    redis-cli -h ${REDIS_HOST} ZADD request-sortedset 1999999999 \
    '{"request_kind":"redis","internal":{},"data":{"id":"gate-test","created":1700000000,"deadline":1999999999,"payload":{"model":"Qwen/Qwen3-0.6B","prompt":"Hello"}}}'

# Verify request stays queued (not dispatched)
kubectl run --rm -i test-queued --image=redis --restart=Never -n ${NAMESPACE} -- \
    redis-cli -h ${REDIS_HOST} ZCARD request-sortedset
# Expected: 1

# Async processor logs should show "using fallback value" {"fallback": 0}. The accompanying
# error depends on which source ran out last — "all metric sources unavailable" once every
# source has gone empty, or "invalid metric value: NaN" while the vLLM source still has
# series inside Prometheus' staleness window but ready_pods has reached zero.
kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=llm-d-async --tail=5

# The same thing as a metric: 0 means the budget below is the configured fallback,
# not a reading. (1 would mean the pool really is reporting no capacity.)
kubectl run --rm -i prom-gate-src --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode 'query=llm_d_async_async_gate_metric_source_available' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: value = 0 while the pool is scaled down

# Scale back up — gate opens, queued request gets dispatched
kubectl scale deployment vllm-qwen3-0-6b-decode -n ${NAMESPACE} --replicas=1
kubectl wait --for=condition=Ready pod -l llm-d.ai/role=decode -n ${NAMESPACE} --timeout=300s

# Wait for Prometheus scrape + gate to open (~30s)
sleep 30

# Queue should be empty, result should appear
kubectl run --rm -i test-drained --image=redis --restart=Never -n ${NAMESPACE} -- \
    redis-cli -h ${REDIS_HOST} ZCARD request-sortedset
# Expected: 0

kubectl run --rm -i test-gate-result --image=redis --restart=Never -n ${NAMESPACE} -- \
    redis-cli -h ${REDIS_HOST} LPOP result-list
# Expected: JSON with "id":"gate-test"
```

### Test gate closed under real load (saturation test)

This test floods the inference gateway with interactive requests to saturate
vLLM, then verifies that the dispatch budget gate closes and async requests
queue. When the load stops, the gate re-opens and queued requests drain.

Two load generators are available:
- **hey** (`docs/guides/e2e-deploy/hey-loadtest.yaml`) — simple HTTP load generator, 200 concurrent workers
- **guidellm** (`docs/guides/e2e-deploy/guidellm-loadtest.yaml`) — LLM-specific load testing with synthetic
  prompts (256 prompt tokens, 512 output tokens), constant 50 req/s

#### Option A: hey

```bash
# 1. Start the load test (200 concurrent workers, runs until killed)
kubectl apply -n ${NAMESPACE} -f ${ASYNC_REPO}/docs/guides/e2e-deploy/hey-loadtest.yaml

# 2. Wait ~20s for Prometheus to scrape the load, then verify saturation
sleep 20
kubectl run --rm -i prom-running --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode \
    'query=vllm:num_requests_running{inference_pool="optimized-baseline"}' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: vllm:num_requests_running = 200 (saturated)

# Verify budget is negative (gate closed)
kubectl run --rm -i prom-budget --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode \
    'query=1 - (sum(vllm:num_requests_running{inference_pool="optimized-baseline"}) / on() (inference_pool_ready_pods{name="optimized-baseline"} * 100))' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: value = -1 (200 running / 100 max = 200% utilization)

# 3. Push async requests while gate is closed
kubectl run --rm -i push-async --image=redis --restart=Never -n ${NAMESPACE} -- \
    sh -c 'for i in 1 2 3 4 5; do
      redis-cli -h ${REDIS_HOST} ZADD request-sortedset 1999999999 \
        "{\"request_kind\":\"redis\",\"internal\":{},\"data\":{\"id\":\"sat-$i\",\"created\":1700000000,\"deadline\":1999999999,\"payload\":{\"model\":\"Qwen/Qwen3-0.6B\",\"prompt\":\"Count slowly.\",\"max_tokens\":128}}}"
    done'

# 4. Verify requests stay queued (gate closed, no dispatch)
sleep 10
kubectl run --rm -i check-queued --image=redis --restart=Never -n ${NAMESPACE} -- \
    sh -c 'echo "queue: $(redis-cli -h ${REDIS_HOST} ZCARD request-sortedset)"; echo "results: $(redis-cli -h ${REDIS_HOST} LLEN result-list)"'
# Expected: queue: 5, results: 0

# 5. Kill the load test — gate re-opens, queued requests drain
kubectl delete job hey-loadtest -n ${NAMESPACE}

# Wait for Prometheus to scrape idle state + gate to open (~30s)
sleep 30

kubectl run --rm -i check-drained --image=redis --restart=Never -n ${NAMESPACE} -- \
    sh -c 'echo "queue: $(redis-cli -h ${REDIS_HOST} ZCARD request-sortedset)"; echo "results: $(redis-cli -h ${REDIS_HOST} LLEN result-list)"'
# Expected: queue: 0, results: 5
```

#### Option B: guidellm

```bash
# 1. Start the load test (constant 50 req/s, synthetic data, runs until killed)
kubectl apply -n ${NAMESPACE} -f ${ASYNC_REPO}/docs/guides/e2e-deploy/guidellm-loadtest.yaml

# 2. Wait ~40s for startup + Prometheus scrape, then verify saturation
sleep 40
kubectl run --rm -i prom-running --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode \
    'query=vllm:num_requests_running{inference_pool="optimized-baseline"}' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: vllm:num_requests_running ~= 110 (saturated)

# Verify budget is negative (gate closed)
kubectl run --rm -i prom-budget --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode \
    'query=1 - (sum(vllm:num_requests_running{inference_pool="optimized-baseline"}) / on() (inference_pool_ready_pods{name="optimized-baseline"} * 100))' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: value ~= -0.1 (gate closed)

# 3. Push async requests while gate is closed
kubectl run --rm -i push-async --image=redis --restart=Never -n ${NAMESPACE} -- \
    sh -c 'for i in 1 2 3 4 5; do
      redis-cli -h ${REDIS_HOST} ZADD request-sortedset 1999999999 \
        "{\"request_kind\":\"redis\",\"internal\":{},\"data\":{\"id\":\"gl-$i\",\"created\":1700000000,\"deadline\":1999999999,\"payload\":{\"model\":\"Qwen/Qwen3-0.6B\",\"prompt\":\"Count slowly.\",\"max_tokens\":128}}}"
    done'

# 4. Verify requests stay queued (gate closed, no dispatch)
sleep 10
kubectl run --rm -i check-queued --image=redis --restart=Never -n ${NAMESPACE} -- \
    sh -c 'echo "queue: $(redis-cli -h ${REDIS_HOST} ZCARD request-sortedset)"; echo "results: $(redis-cli -h ${REDIS_HOST} LLEN result-list)"'
# Expected: queue: 5, results: 0

# 5. Kill the load test — gate re-opens, queued requests drain
kubectl delete job guidellm-loadtest -n ${NAMESPACE}

# Wait ~60s for vLLM to drain in-flight requests + Prometheus scrape + gate to open
sleep 60

kubectl run --rm -i check-drained --image=redis --restart=Never -n ${NAMESPACE} -- \
    sh -c 'echo "queue: $(redis-cli -h ${REDIS_HOST} ZCARD request-sortedset)"; echo "results: $(redis-cli -h ${REDIS_HOST} LLEN result-list)"'
# Expected: queue: 0, results: 5
```

## Cleanup

```bash
kubectl delete job hey-loadtest guidellm-loadtest -n ${NAMESPACE} --ignore-not-found
helm uninstall llm-d-async -n ${NAMESPACE}
helm uninstall redis -n redis
kubectl delete -n ${NAMESPACE} -k ${ASYNC_REPO}/docs/guides/e2e-deploy/modelserver/
helm uninstall ${GUIDE_NAME} -n ${NAMESPACE}
kubectl delete -k ${LLM_D_REPO}/guides/recipes/gateway/istio -n ${NAMESPACE}
kubectl delete namespace ${NAMESPACE}
```
