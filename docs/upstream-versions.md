# Upstream dependency versions

| Dependency | Current Pin | Pin Type | File Location | Upstream Repo |
|---|---|---|---|---|
| Gateway API Inference Extension (GAIE) Go module | `v1.6.2` | tag ([#558](https://github.com/llm-d/llm-d-async/pull/558)) | [go.mod](../go.mod) | [kubernetes-sigs/gateway-api-inference-extension](https://github.com/kubernetes-sigs/gateway-api-inference-extension) |
| GAIE InferencePool CRDs used by end-to-end tests | `v1.6.2` | tag (`GAIE_VERSION` default) | [E2E suite](../test/e2e/e2e_suite_test.go) | [GAIE releases](https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases) |
| llm-d-router endpoint picker and router CRDs used by end-to-end tests | `main` | branch (`EPP_IMAGE` and `ROUTER_VERSION` defaults) | [E2E suite](../test/e2e/e2e_suite_test.go) | [llm-d/llm-d-router](https://github.com/llm-d/llm-d-router) |

The Go module is a build dependency. The `v0.11.0` and `v1.0.0` processor releases used in the upgrade guide both pin GAIE `v1.2.1`. For a deployment, select the GAIE CRDs expected by the router image and take router-owned CRDs from the matching router release. The E2E defaults exercise router `main` with the `llm-d.ai/v1` configuration.

## Router compatibility for the processor

Router release tags **v0.9.0, v0.10.0, v0.11.0 and v1.0.0** expose the metric and header names below in source. This is a feature-level compatibility range. The E2E suite tracks a moving router ref and does not certify every released processor/router pairing.

| Processor feature | Contract to check on the target router |
|---|---|
| Pool readiness and budget gates | `llm_d_epp_ready_endpoints{name="…"}` reaches zero when the pool drains. `llm_d_epp_per_endpoint_queue_size{name="…"}` provides per-endpoint queue depth. Check the pool and namespace used by your [gate configuration](dispatch-gates.md). |
| Flow-control gates | With flow control enabled, `llm_d_epp_flow_control_queue_size` and `llm_d_epp_flow_control_pool_saturation` use the `inference_pool` label. Verify the configured PromQL returns data in the target cluster. |
| Request routing and drops | The processor uses `x-llm-d-inference-fairness-id` and `x-llm-d-inference-objective`. Some policies send the legacy `x-gateway-inference-objective` alias. Router flow control reports a drop in the response header `x-llm-d-request-dropped-reason`. |

The [v0.9.0 readiness publisher](https://github.com/llm-d/llm-d-router/blob/v0.9.0/pkg/epp/datalayer/logger/logger.go#L111-L118) records the endpoint count before returning on an empty pool. See the [router metric definitions](https://github.com/llm-d/llm-d-router/blob/v1.0.0/pkg/epp/metrics/llm_d_router_metrics.go), [request header aliases](https://github.com/llm-d/llm-d-router/blob/v1.0.0/pkg/epp/metadata/consts.go) and [processor header handling](../api/inference_error.go). Check these contracts before following the [upgrade procedure](upgrading.md).
