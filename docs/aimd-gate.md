# AIMD Dispatch Gate

The `aimd` gate sizes dispatch windows from the gateway's responses instead of scraped metrics. This page specifies the gate's behavior; the design rationale, rejected alternatives, and open work live in the feedback-based dispatch control proposal ([proposals/feedback-based-dispatch-control.md](proposals/feedback-based-dispatch-control.md), lands with llm-d-async#379). Every dispatched request already returns an acceptance, a rejection with a reason, or an error, and the gate treats that stream as its capacity signal. It needs no metric source or copied capacity constants, and the window updates on each response with no cache or scrape interval in between.

It is intended as a pool gate (`gate_type` on a worker pool). A window bounds in-flight requests per priority band. When a band's window is full or closed, a reserved request parks its worker in memory; an overflow request is refused back to the broker. A freed slot or a window increase wakes one parked worker. The wake is a hint shared across bands rather than per-band delivery, so parked workers also keep a poll timer as a fallback. Only reserved requests park because a parked worker is unavailable to every band: a closed low-priority band with a deep backlog would otherwise park the whole pool while higher bands have room. A refused request stays in the broker and is redelivered; those redeliveries also serve as the band's probe traffic.

Status: the worker-side wiring that reports response outcomes to the gate lands in a follow-up PR. Until then a configured `aimd` gate admits against its windows but observes no outcomes, so every band stays at `min_window`.

## Response feedback

The worker classifies each response and reports it to the gate. Classification uses the drop-reason response header (`x-llm-d-request-dropped-reason`) that llm-d-router's flow control attaches to rejections; a `429` without a reason header counts as a capacity rejection, so the gate also works against gateways that send only status codes.

| Response | Outcome | Window effect |
|---|---|---|
| 2xx | accepted | grows (slow start or additive increase) |
| reason `rejected-saturated`, or bare 429 | capacity rejection | multiplicative decrease; `Retry-After` closes the window |
| reason `rejected-ttl-expired` | queue-TTL expiry | gentle decrease (×0.9) |
| reason `evicted*` | eviction | none |
| any other reason | non-capacity rejection | none |
| transport or bare 5xx error | error | none |

Evictions leave the window unchanged by design: an evicted request was admitted, ran, and was revoked to make room for higher-priority work. The eviction carries no information about pool capacity, and the request retries through the broker as usual.

Two advisory view headers refine the controller when the gateway sends them. Both are provisional names pending ratification of the flow-control contract; absent headers mean no signal, and the gate runs on outcomes alone.

- `x-llm-d-flow-queue-duration-ms`: time the request spent queued in the gateway before dispatch.
- `x-llm-d-flow-band-headroom`: remaining queue capacity, in requests, of the band the request occupied.

## The controller

The gate keeps one congestion window per band, where a band is the `(classification, tier)` pair also used by the `tier-priority` merge policy: `reserved`/`overflow` crossed with `interactive`/`async`/`batch`, unknown tiers defaulting to `batch`. Per-band windows keep a rejection on sheddable batch traffic from shrinking the window that reserved interactive traffic uses.

The bands are not independent, because the gateway dispatches them in strict priority order: as pool saturation rises it gates the lower bands first (their dispatch ceilings are lower where a usage-limit policy is configured, and they starve first under plain priority-descending dispatch even without one), so congestion appears at the bottom of the priority ladder and climbs. Each band's outcome stream is therefore a sensor for one shared quantity, the level the congestion has climbed to, and the gate propagates evidence along the ladder in both directions:

- A capacity rejection in a band also decreases every band **below** it (and a `Retry-After` close propagates down): if the pool rejected higher-priority work, lower-priority work is at least as gated, and there is no reason to wait for each lower band to observe its own rejections.
- The same rejection is a leading signal for every band **above** it: their margin is shrinking but not gone, so they hold window growth for `hold_duration` instead of decreasing. A queue-TTL expiry propagates the same hold upward, since starvation also reaches the lowest bands first.
- An accept in a band clears the holds on every band above it before the timer expires: that accept proves the pool still has margin below them. In stable partial saturation, where the lowest bands are squeezed out while a middle band keeps flowing, the flowing band keeps the bands above it growing instead of leaving them held by the rejection stream below.

Outcomes arrive out of order with respect to sends: a rejection returns in one round trip, while an accept returns after queueing plus generation, so an accept observed after a rejection may still describe conditions from before it. The band's last decrease acts as a recovery point, in TCP's sense, on both sides of the controller. Optimistic updates require the request to have been *sent after* the evidence they would override — an accept only clears a hold set before its send time, and only grows a window whose last decrease predates its send time. Decreases use the same boundary for coalescing, as described above: one per flight. Retry-After closes are an explicit server instruction and apply regardless of ordering.

Most of the cross-band evidence comes from batch overflow, which runs at the pool's edge where a rejection is cheap: the request returns to the broker and retries.

Each band's window `w` starts at `min_window` and updates on feedback:

- **Slow start.** While `w < ssthresh`: each accept adds `increase`, which compounds to roughly doubling per window's worth of accepts. `ssthresh` starts at `max_window`, so a fresh band opens quickly.
- **Congestion avoidance.** At or above `ssthresh`: each accept adds `increase / w`, roughly `increase` per full window.
- **Multiplicative decrease.** A capacity rejection sets `ssthresh = w × decrease_factor` and `w = ssthresh`, at most once per flight of sends: a rejection whose request was sent before the band's previous decrease belongs to the congestion event already acted on and is ignored. The flight is the coalescing unit because it is dimensionless — it scales with both concurrency and generation time, where a wall-clock interval is many flights at high concurrency and a fraction of one at low. Without coalescing, ten rejections arriving together would collapse the window to the floor.
- **Full close.** A capacity rejection carrying `Retry-After` closes the band until that time and resets `w` to `min_window`. The close is not cooldown-gated. On reopen the band grows in slow start toward the `ssthresh` recorded at the close, mirroring TCP's retransmission-timeout recovery.
- **Headroom cap.** When a response advertises band headroom `h`, the window is capped at `inflight + h`: the band cannot hold more than what it reports free plus what this gate already has in flight.
- **Queue-duration signal.** With `queue_duration_target` set, an accepted response that waited past the target holds the window instead of growing it, and one past twice the target applies the gentle decrease. Rising queue time is the earliest congestion signal, arriving before any rejection; this keeps the gateway's queues short instead of probing until they overflow.

`min_window` is at least 1, so a saturated band still probes with one in-flight request rather than freezing, and recovery is observed from the probe's response.

The window self-clocks: an accept frees a slot and grows the window in the same event, so the dispatch rate tracks the pool's service rate without estimating it, and long generations hold their slot for as long as they hold pool capacity. When several processor replicas run this gate against one pool, AIMD converges toward equal windows across senders sharing a congestion signal (Chiu and Jain, "Analysis of the Increase and Decrease Algorithms for Congestion Avoidance in Computer Networks", 1989), so horizontal scaling needs no coordination between replicas.

## Configuration

```json
{
  "id": "inference_pool_1",
  "workers": 64,
  "gate_type": "aimd",
  "gate_params": {
    "min_window": 1,
    "max_window": 256,
    "increase": 1.0,
    "decrease_factor": 0.5,
    "hold_duration": "1s",
    "tier_label": "tier",
    "queue_duration_target": "0s"
  }
}
```

All parameters are optional; the values above are the defaults. `queue_duration_target` of zero disables the queue-duration signal. Parameter semantics are listed in the [README's gate parameters section](../README.md#per-queue-dispatch-gates).

The gate exports its controller state as gauges labeled `(pool_name, band)`: `llm_d_async_async_aimd_window`, `..._aimd_ssthresh`, and `..._aimd_inflight`. Window against ssthresh shows which growth mode a band is in; inflight against window shows band utilization; the owning worker pool's ID supplies the `pool_name` label, matching the pool's other series.

Worker count interacts with the windows: a band can never have more requests in flight than there are workers to carry them, so `max_window` beyond the pool's worker count is not reachable. Size workers for the concurrency you want at full health and let the windows do the throttling below that.

## Relation to the metric gates

The `prometheus-saturation`, `prometheus-budget`, and `endpoint-scrape` gates estimate pool capacity from outside: they read exported metrics through a cache, reconstruct the gateway's capacity formula from copied constants, and fail toward a configured fallback when the metric source is unavailable. The `aimd` gate replaces that estimation with feedback for pools fronted by a gateway that returns reason-coded rejections. The metric gates remain the supported path for backends with no gateway in front of them.
