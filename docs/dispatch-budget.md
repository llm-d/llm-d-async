# Dispatch Budget

Related:
- [Batch Dispatcher: Architecture](https://github.com/llm-d-incubation/batch-gateway/blob/main/docs/design/batch-dispatcher.md) (batch-gateway)
- [\[Public Doc\] Serving Online Batch via Inference Gateway](https://docs.google.com/document/d/1notkq9s0qOmWmUNonZ8CfI-5jtGtHA4PGMI-xz8sGRE/edit?tab=t.0#heading=h.i76kzr3j3swj)
- [\[PUBLIC\] EPP Flow Controller for Priority, Fairness, and Queuing](https://docs.google.com/document/d/1VZL7opFWuwgWquvgiOzLlXAJ633qZ9U-A0ZixGjBgaI/edit?tab=t.0#heading=h.hfyow92z2d0t)
- [\[PUBLIC\] Improved Flow Control Request Management](https://docs.google.com/document/d/1JxzJc8gNv2wKK5-a8ohb0btn78ymVKw9XMIb4-S-ncA/edit?tab=t.0#heading=h.rutawybt03nl)
- [https://gateway-api-inference-extension.sigs.k8s.io/api-types/inferencepool/](https://gateway-api-inference-extension.sigs.k8s.io/api-types/inferencepool/)


The **Async Processor** ([sometimes also called "Batch Dispatcher"](https://github.com/llm-d-incubation/batch-gateway/blob/main/docs/design/batch-dispatcher.md)) pulls requests from the Message Queue and decides whether to forward them to the IGW based on a **`DispatchGate`** — a pluggable policy component that returns a budget value in $[0, 1]$ representing current system capacity. When the gate is open, a proportional number of requests are forwarded; otherwise the Async Processor waits until the gate opens again.

Several `DispatchGate` implementations are available (see [Dispatch Gates](https://github.com/llm-d-incubation/llm-d-async/blob/main/README.md#dispatch-gates)). This document describes the **Dispatch Budget**, a metric-based strategy for gating. Metric-based gates rely on a `MetricSource` (e.g., a PromQL query against Prometheus) to read inference pool state.

## Dispatch Budget (`prometheus-budget`)

The dispatcher computes a "**Dispatch Budget"**, to determine **the number of concurrent batch requests** allowed to enter the IGW at a given time.

The **Dispatch Budget** $D$ is a measure of capacity of the gateway and the inference pool. In particular, depending on available metrics, $D$ could be:

- the complement of the **"fullness" ($F$) of the EPP** to ingest and manage requests:

  $$D = 1 - F$$

  because the EPP has an up-to-date view of its corresponding inference pool at all times, $F$ is assumed to capture both the capacity of the EPP itself and the capacity of the underlying inference pool (i.e., its saturation, see below)

- the complement of the **Inference Pool Saturation** ($S$)

  $$D = 1 - S$$

  $S$ is a measure of "fullness" of the inference backend (e.g., GPU utilization, request concurrency, KV cache pressure).
  For instance, $S$ could be the saturation metric that the EPP's saturation detector publishes (which should be updated more frequently), or it may be computed using a Prometheus query over the inference pool.

- in general any quantity $D \in [0,1]$ that estimates the remaining capacity of the system is a valid dispatch budget

### Acceptable Range

For a given Dispatch Budget $D$, we also define a **Reserved Baseline** $B \in [0,1]$ as a threshold, and we pose
that when $D\leq B$ "the gate is closed", i.e., no requests can be forwarded.
Hence, **a dispatcher only forwards requests when $D > B$**.

The number of requests that can be forwarded is estimated by scaling the total system capacity $\mathrm{max}\_\mathrm{SYS}$ by the dispatch budget, minus the baseline: $(D-B)$; depending on configuration, the unit of $\mathrm{max}\_\mathrm{SYS}$ could be bytes, tokens, or just number of requests.

$$N = \mathrm{max}\_\mathrm{SYS} \times (D - B)$$

### Example

When capacity is measured in requests, a natural estimate for $\mathrm{max}\_\mathrm{SYS}$ is the aggregate pool capacity, the same value the inference scheduler's [saturation detector](https://github.com/llm-d/llm-d-inference-scheduler/blob/b9f77ee9/pkg/epp/framework/plugins/flowcontrol/saturationdetector/concurrency/detector.go#L119-L147) uses as the denominator for saturation:

$$\mathrm{max}\_\mathrm{SYS} = \texttt{ready\\_model\\_servers} \times \texttt{max\\_concurrency}$$

where `ready_model_servers` is the number of ready endpoints in the inference pool (available as the `inference_pool_ready_pods` metric) and `max_concurrency` is the per-endpoint request capacity, corresponding to the [`MaxConcurrency` config value in the inference scheduler's saturation detector (default 100)](https://github.com/llm-d/llm-d-inference-scheduler/blob/b9f77ee9/pkg/epp/framework/plugins/flowcontrol/saturationdetector/concurrency/config.go#L32-L58). The number of dispatchable requests is then:

$$N = \mathrm{max}\_\mathrm{SYS} \times (D-B) = \texttt{ready\\_model\\_servers} \times \texttt{max\\_concurrency} \times (D-B) $$

For instance, if an EPP metric ($F$) is available:

$$F = 0.3, \quad B = 0.1$$

- $\texttt{ready\\_model\\_servers} = 5, \quad \texttt{max\\_concurrency} = 10, \quad \mathrm{max}\_\mathrm{SYS} = 50$
- Dispatch Budget $D = (1 - 0.3) = 0.7$
- Since 0.7 > 0.1, then we can compute $N$
- Dispatchable Requests $N = 5 \times 10 \times (0.7 - 0.1) = 30$

#### Notes

1. $N$ is a real-valued estimate. The rounding strategy (e.g., floor, ceiling, or ensuring a minimum of 1 when $D > B$) is left to the implementation, as edge cases just above the baseline require care to avoid prematurely stopping dispatch.
2. `max_concurrency` is expressed in request units here, but the same approach generalizes to other capacity units (e.g., tokens, bytes) given an equivalent per-endpoint capacity constant.
3. We may also define system capacity in terms of number of bytes; in this case $D$ would be truncated at the boundary of a valid request; for instance, if we expect to be able to forward a maximum of 1024 KiB of data, then we may forward at most $1024 \times (0.7-0.1) = 614.4$ KiB; which means if we have $r_1, r_2, ...$ enqueued; if $r_1$ is 600 KiB and $r_2$ is 100 KiB, then we would only dispatch $r_1$

### Failure Modes

* **Queue Consumer Failures:** If the Async Processor pod crashes, the messages remain in the persistent Message Queue (Redis/PubSub), ensuring no data loss.
* **IGW Backpressure:** If the IGW returns an HTTP error indicating overload (e.g. HTTP 429\) despite the computed budget, then the **saturation is assumed to be close to 1**, and therefore a **dispatch budget close to 0\.** The Async Processor will not retry until a subsequent update from the metrics store will bring it back within the acceptable limits.
  * This might happen if a sudden spike of traffic enters the gateway and the metrics did not update on-time.
* **Unreadable Metrics:** In case of unreadable metrics, the Async Processor cannot take informed decisions, and therefore will assume a **dispatch budget of 0\.**
