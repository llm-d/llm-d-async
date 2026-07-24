# Release Notes

User-facing changes for each release, newest first. Release sections are
assembled from per-PR fragments in `release-notes.d/unreleased/` at each release
tag by `make release-notes VERSION=vX.Y.Z`.

Do not edit release sections by hand. To record a change, add a fragment via the
PR template's `release-note` block — see
[CONTRIBUTING.md → Release notes](CONTRIBUTING.md#release-notes).

<!-- BEGIN RELEASES -->

RELEASE v0.8.0 2026-07-24
2026-07-09 https://github.com/llm-d/llm-d-async/pull/300 **Breaking:** `ResultMessage` now carries structured result fields so consumers can distinguish HTTP successes, HTTP errors, and non-HTTP failures — added `StatusCode` (int, non-zero means an HTTP response was received), `ErrorCode` (string, e.g. `DEADLINE_EXCEEDED`, `GATE_DROPPED`, `GATE_ERROR`, `INFERENCE_ERROR`, `INVALID_REQUEST`), and `ErrorMessage` (string). Wire-format changes to be aware of: - HTTP-error `Payload` now contains the raw upstream response body instead of a wrapped error string. - Gate-drop `Payload` changed from `{"status":"dropped"}` to `{"error":"Pool gating dropped request"}` and is now unified across all transports via `NewGateDroppedResult`. Detect gate drops via `ErrorCode == "GATE_DROPPED"` rather than parsing the payload body. - Gate-drop results now include `Metadata` (from `req.ReqMetadata()`), which the previous inline construction omitted. - `InferenceClient.SendRequest` now returns `(*InferenceResponse, error)`, where `InferenceResponse` has `StatusCode int` and `Body []byte` fields (previously `([]byte, int, error)`).
2026-07-10 https://github.com/llm-d/llm-d-async/pull/306 The default worker `concurrency` is now **64** (was 8). The processor is I/O-bound, so by Little's Law in-flight concurrency caps throughput; the old default of 8 left real inference pools mostly idle. Tune `concurrency` to your backend's latency/throughput target (see the Async Processor Operations Guide).
2026-07-21 https://github.com/llm-d/llm-d-async/pull/334 Fixed a gate capacity reservation leak in the Redis sorted-set flow: retried requests never released their per-queue gate reservation, so `inFlight` ratcheted up on every retry until the queue stopped dispatching entirely (`local-max-concurrency`) or the `redis-quota` counter over-admitted on TTL reset. Retries now release the reservation before re-enqueue (and re-reserve on re-dispatch).
2026-07-21 https://github.com/llm-d/llm-d-async/pull/336 Fixed over-admission in concurrency-mode `redis-quota`: the in-flight counter's TTL was set only on the first acquire and never refreshed, so under sustained load the key expired mid-flight and the counter reset to 0, admitting requests beyond the configured limit. The TTL is now refreshed on every acquire/release, so it only expires after a full `window` of inactivity (crash-orphan cleanup).
2026-07-21 https://github.com/llm-d/llm-d-async/pull/339 Added two Prometheus gauges (labeled by `pool_name`) that expose the raw value a metric-based dispatch gate reads and the threshold it compares against, so you can see why a gate is open or closed: `async_gate_metric_value` (for the saturation gate, `1 - saturation`) and `async_gate_metric_threshold`. Recorded by the `prometheus-saturation`, `prometheus-budget`, and `prometheus-query` gates.
2026-07-21 https://github.com/llm-d/llm-d-async/pull/340 Breaking: the Helm chart is renamed from `async-processor` to `llm-d-async` and now publishes to `oci://ghcr.io/llm-d/charts/llm-d-async`. Rendered resources are named `llm-d-async-*` with `app.kubernetes.io/name=llm-d-async`; because a Deployment's selector is immutable, upgrading from an existing `async-processor` install requires uninstall + reinstall. The old `charts/async-processor` package remains available (no redirect) but receives no new releases. The default OpenTelemetry trace service name also changes from `async-processor` to `llm-d-async` (`OTEL_SERVICE_NAME`) — update any trace queries/dashboards accordingly.

