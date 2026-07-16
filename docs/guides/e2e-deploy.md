# E2E Deploy: Async Processor with Dispatch Budget Gate

Deploy async-processor with a `prometheus-budget` gate on a real Kubernetes cluster,
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
export GATEWAY_API_VERSION=v1.5.1
export GUIDE_NAME=optimized-baseline
```

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

Reference: [llm-d istio gateway guide](https://github.com/llm-d/llm-d/blob/main/guides/prereq/gateways/istio.md)

## Step 4: Deploy Gateway

```bash
kubectl apply -k ${LLM_D_REPO}/guides/recipes/gateway/istio -n ${NAMESPACE}
kubectl wait --for=jsonpath='{.status.conditions[?(@.type=="Programmed")].status}'=True \
    gateway/llm-d-inference-gateway -n ${NAMESPACE} --timeout=120s
```

## Step 5: Deploy llm-d Router (EPP) with monitoring enabled

```bash
helm install ${GUIDE_NAME} \
    oci://registry.k8s.io/gateway-api-inference-extension/charts/inferencepool \
    -f ${LLM_D_REPO}/guides/recipes/scheduler/base.values.yaml \
    -f ${LLM_D_REPO}/guides/${GUIDE_NAME}/scheduler/${GUIDE_NAME}.values.yaml \
    --set provider.name=istio \
    --set experimentalHttpRoute.enabled=true \
    --set experimentalHttpRoute.inferenceGatewayName=llm-d-inference-gateway \
    --set inferenceExtension.monitoring.prometheus.enabled=true \
    -n ${NAMESPACE} --version ${GAIE_VERSION}
```

`monitoring.prometheus.enabled=true` creates:
- A `ServiceMonitor` for EPP metrics (`optimized-baseline-epp-monitor`)
- A metrics reader `Secret` with a bearer token (`inference-gateway-sa-metrics-reader-secret`)
- Proper RBAC (ClusterRoleBinding to `system:auth-delegator` for the EPP ServiceAccount)

Reference: [llm-d monitoring docs](https://github.com/llm-d/llm-d/blob/main/docs/monitoring/README.md)

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

Wait for the model to load:

```bash
kubectl wait --for=condition=Ready pod -l llm-d.ai/role=decode -n ${NAMESPACE} --timeout=300s
```

## Step 7: Install Prometheus (skip if already installed)

```bash
cd ${LLM_D_REPO}
./docs/monitoring/scripts/install-prometheus-grafana.sh
```

Reference: [Prometheus/Grafana quickstart](https://github.com/llm-d/llm-d/blob/main/docs/monitoring/prometheus-grafana-stack.md)

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
helm install async-processor ${ASYNC_REPO}/charts/async-processor/ \
    -f ${ASYNC_REPO}/docs/guides/e2e-deploy/async-processor-values.yaml \
    -n ${NAMESPACE}
```

The values file (`docs/guides/e2e-deploy/async-processor-values.yaml`) configures:
- Image: `ghcr.io/llm-d/llm-d-async:938cd44`
- Queue: Redis sorted-set with `redis.url` set directly (chart creates the Secret), configured via `queuesConfig`
- Gate: `prometheus-budget` with pool=`optimized-baseline`, max_concurrency=100, baseline=0.05 (per-queue)
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
  pod label into vLLM metrics (required for the dispatch budget gate fallback)
- `podMonitor.enabled: true` — creates a PodMonitor that scrapes the async-processor's own
  Prometheus metrics (retry rate, success rate, latency, etc.)
- `prometheusRule.enabled: true` — installs alert rules for high retry rate, deadline exceeded
  rate, low success rate, and high shed rate
- `grafana.dashboards.enabled: true` — provisions a Grafana dashboard (via sidecar) with
  request rate, outcome breakdown, success/retry gauges, and latency percentiles

## Verify

### Check pods and gate status

```bash
# All pods running
kubectl get pods -n ${NAMESPACE}

# Async processor logs should show "using fallback metric source" (vLLM saturation),
# NOT "all metric sources unavailable"
kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=async-processor --tail=10
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

# Full gate budget query (should return 1.0 at idle)
kubectl run --rm -i prom-budget --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode \
    'query=1 - (sum(vllm:num_requests_running{inference_pool="optimized-baseline"}) / on() (inference_pool_ready_pods{name="optimized-baseline"} * 100))' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: value = 1
```

### Verify async-processor monitoring

Once you have sent at least one async request (see next section), verify that the
async-processor's own metrics are being scraped and that alerts/dashboards are available:

```bash
# Async-processor metrics in Prometheus (should show request counters)
kubectl run --rm -i prom-ap --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s --data-urlencode 'query=llm_d_async_async_request_total' \
    'http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query'
# Expected: one or more time series with queue_id/queue_name labels

# Verify PrometheusRule is installed
kubectl get prometheusrules -n ${NAMESPACE}
# Expected: async-processor rule listed

# Verify Grafana dashboard ConfigMap is present
kubectl get configmap -n ${NAMESPACE} -l grafana_dashboard=1
# Expected: async-processor-dashboards ConfigMap listed
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

# Async processor logs should show: "using fallback value" {"fallback": 0, "error": "invalid metric value: NaN"}
kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=async-processor --tail=5

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
helm uninstall async-processor -n ${NAMESPACE}
helm uninstall redis -n redis
kubectl delete -n ${NAMESPACE} -k ${ASYNC_REPO}/docs/guides/e2e-deploy/modelserver/
helm uninstall ${GUIDE_NAME} -n ${NAMESPACE}
kubectl delete -k ${LLM_D_REPO}/guides/recipes/gateway/istio -n ${NAMESPACE}
kubectl delete namespace ${NAMESPACE}
```
