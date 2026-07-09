# Formal specifications (TLA+)

Machine-checked models of subtle concurrency in the Async Processor. These are
*specifications*, not tests of the Go code — they model the intended algorithm so
we can prove safety/liveness properties hold for **every** interleaving, including
the rare ones load tests never hit (concurrent admits, pod crashes mid-flight).

| Spec | Models | Result |
| :-- | :-- | :-- |
| [`GateQuota.tla`](GateQuota.tla) | the `redis-quota` concurrency gate: acquire / dispatch / release across many workers, plus crash-induced leaks and TTL reclaim | **all properties hold** |
| [`GateQuotaRace.tla`](GateQuotaRace.tla) | the same gate with a **non-atomic** acquire (check-then-increment in two steps) | **NoOverAdmission violated** (TLC prints the trace) |
| [`MsgLifecycle.tla`](MsgLifecycle.tla) | the Pub/Sub message → dispatch → result → ack flow, where completion is correlated through a process-global map keyed by `msg.ID`, under at-least-once redelivery | **current design: `AckIntegrity` and `NoStuckCallback` both violated.** A corrected design (flip one constant) holds. |
| [`GateThrottle.tla`](GateThrottle.tla) | issue [#304](https://github.com/llm-d-incubation/llm-d-async/issues/304): a worker-pool gate that makes a **binary** admit decision (`observed < threshold`) across N parallel workers on a **lagging** scrape | **current design: `RespectsThreshold` violated** — TLC finds `inflight = |Workers| > Threshold`. The budget-throttled variant (flip one constant) holds. |

## Why this gate

The `redis-quota` gate (`pkg/async/inference/flowcontrol`, mode `concurrency`)
caps in-flight requests per tenant attribute (e.g. `team`) using a shared Redis
counter. Two things make it worth modeling formally:

1. **Atomicity.** The limit check and the increment must be one atomic operation
   (a Lua script / atomic INCR-with-compare). If they are two operations, two
   workers can both observe `counter < Limit` and both increment — over-admission.
2. **Crash leaks.** A worker that acquires a slot and then crashes never runs its
   release. The counter stays over-counted until the Redis key's TTL expires. This
   was observed for real: killing pods mid-flight left quota counters stuck high
   until expiry.

`GateQuota.tla` captures both. The model has *K* workers sharing per-team
counters; each worker loops over `acquire → release`, may `crash` while holding a
slot (the slot becomes "orphaned"), and a `reaper` process models TTL expiry
reclaiming orphaned slots.

## Properties

Checked over 3 workers × 2 tiers (limits 2 and 1) — 325 distinct states:

- **`TypeOK`** — state stays well-formed.
- **`NoOverAdmission`** *(safety)* — `counter[t] ≤ Limit[t]` always. This is the
  whole point of the gate, and it holds **because acquire is one atomic step**.
- **`AccountingConsistent`** *(safety)* — `counter[t] = (live holders) + (orphaned)`.
  No phantom slots, no under-count. Crash moves a slot from "held" to "orphaned"
  without changing the counter; the counter only drops on release or reclaim.
- **`ConcurrencyBounded`** *(safety, derived)* — real in-flight per tier ≤ limit.
- **`NoPermanentLeak`** *(liveness)* — `<>[] (orphan = 0)`: leaked slots are
  *eventually* reclaimed (modeled with weak fairness on the reaper), so a crash
  cannot pin tenant capacity forever. **This is exactly the property a missing or
  too-long Redis TTL would break** — drop the reaper and TLC reports the leak.

## What the race model proves

`GateQuotaRace.tla` is identical except the acquire is split into `CHECK` (pass the
limit test) and `COMMIT` (increment) as two steps. TLC immediately finds a 5-state
counter-example with limit 1 and two workers:

```
both workers CHECK (counter=0 < 1)  -> both decide to admit
worker w1 COMMITs                   -> counter = 1
worker w2 COMMITs                   -> counter = 2   <-- exceeds Limit = 1
```

This is the concrete justification for implementing the gate with an atomic Redis
operation rather than a read-modify-write.

## What the message-lifecycle model found (a real gap)

`MsgLifecycle.tla` is the most complex model here, and unlike the first two it was
written to *probe* the current implementation rather than confirm a known fact — and
it surfaced a latent defect.

**The code** (`pkg/pubsub/pubsubimpl.go`). For each received message the per-message
callback:

1. stores its completion channel in a **process-global map keyed by the Pub/Sub
   message id** — `resultChannels.Store(msg.ID, ch)` (`:494`);
2. dispatches the request to a worker and then **blocks on a bare channel receive**,
   `result := <-resultsChannel` (`:554`), *not* guarded by `ctx`;
3. on return runs, unconditionally, `defer resultChannels.Delete(msg.ID)` (`:495`).

The result worker signals completion by doing `resultChannels.Load(id)` and sending
to **whatever channel is currently stored for that id** (`:350-356`) — not the
specific delivery whose work just finished.

**The catch.** Pub/Sub is *at-least-once*: the same `msg.ID` can be delivered again
while a prior delivery is still being processed. Two concrete triggers exist in this
code:

- a server-side ack-deadline expiry while a message is parked by a **pool-level
  `Wait` gate** or held during a long dispatch, and
- the **budget-change restart** at `:422-444`, where `cancel()` tears down `Receive`
  while a callback is still blocked on the unguarded `<-resultsChannel`; the message
  is redelivered into the next `Receive` while the old callback leaks.

So `msg.ID` is **not unique across overlapping deliveries**, yet it is the
correlation key. The model encodes exactly this and checks two properties:

- **`AckIntegrity`** *(safety)* — a message is Ack'd only by a callback whose **own**
  dispatched work completed.
- **`NoStuckCallback`** *(liveness)* — every callback that blocks on its channel
  eventually unblocks (no leaked goroutine).

With `AtomicCorrelation = FALSE` (the current design), TLC finds a 5-step trace
violating **both**:

```
deliver #1        regOwner := 1, dispatch #1, callback#1 blocks on its channel
deliver #2 (redelivery, same msg.ID, before #1 finished)
                  regOwner := 2 (OVERWRITES), dispatch #2, callback#2 blocks
worker finishes #1's work
                  loads resultChannels[id] = channel #2  -> signals callback#2
callback#2 wakes  -> msg.Ack()   <-- AckIntegrity violated: acked on #1's result
                                     while #2's own request is still outstanding
                  -> defer Delete(msg.ID) removes the registration
callback#1 never gets signaled    <-- NoStuckCallback violated: leaked forever
```

The consequences in production: a duplicate inference dispatch, a message **Ack'd on
the wrong delivery's result** (so the retry safety net is silently disarmed), a
leaked goroutine per occurrence, and a wrong `message_latency` sample.

**The fix the model validates.** Set `AtomicCorrelation = TRUE` and both properties
hold. That corresponds to: give each *delivery* its own correlation (don't key shared
state on `msg.ID` alone), have the worker signal the delivery that actually produced
the work, and have a callback clear only its own registration (plus guard the receive
with `ctx`). Note the model still shows `dispatchCount` reaching 2 even when fixed —
at-least-once delivery means **duplicate dispatch is inherent**; the fix removes the
*mis-correlation and the leak*, but genuine end-to-end dedup still needs an
idempotency key downstream.

## What the throttle model found (issue #304)

`GateThrottle.tla` models the reported overshoot: a **worker-pool** gate does not
respect its saturation threshold. Every pool worker evaluates the gate as a
*binary* verdict (`ActionContinue`/`ActionWait`) against the last Prometheus
scrape, which lags the real in-flight count. While that reading is stale, many
idle workers all pass the check and dispatch at once, so real saturation bursts
past the threshold before the next scrape catches up (the benchmark's average
saturation 1.73 vs the configured 0.8).

The model has `|Workers|` workers, an `inflight` count (true saturation), an
`observed` reading refreshed only by a `scraper` process (the lag), and one
`Threshold`. Flip the `Proportional` constant:

- **`FALSE`** — the current worker-pool gate: admit while `observed < Threshold`.
  TLC finds a 3-step trace where, with `observed` still `0`, all three workers
  admit and `inflight` reaches `3 > Threshold = 2` — **`RespectsThreshold`
  violated**. The overshoot grows with the worker count.
- **`TRUE`** — admission bounded by the *real* remaining budget (the proposed
  fix: throttle the number of active workers by `Budget()`, mirroring the queue
  gate's `batchSize * budget`): admit only while `inflight < Threshold`. TLC
  reports **no error** — `inflight` never exceeds `Threshold`.

This is the formal statement of #304: the binary pool gate cannot hold the
threshold under scrape lag, whereas budget-proportional admission can. (The model
idealizes the single serialized queue worker's scrape+admit as atomic, so the fix
holds `Threshold` exactly; real scrape lag still leaves the queue path a small,
*bounded* residual overshoot — the benchmark's 0.8118 — while the pool path's
overshoot scales with the worker count.)

## Running TLC

You need Java and `tla2tools.jar`
([download](https://github.com/tlaplus/tlaplus/releases)):

```bash
cd docs/specs

# 1. translate the PlusCal algorithm into TLA+ (regenerates the TRANSLATION block)
java -cp tla2tools.jar pcal.trans GateQuota.tla

# 2. model-check (uses GateQuota.cfg)
java -cp tla2tools.jar tlc2.TLC GateQuota.tla
#    -> "Model checking completed. No error has been found."

# the race variant — expected to FAIL, printing the over-admission trace
java -cp tla2tools.jar pcal.trans GateQuotaRace.tla
java -cp tla2tools.jar tlc2.TLC GateQuotaRace.tla
#    -> "Invariant NoOverAdmission is violated."

# the message-lifecycle model. -deadlock disables deadlock checking because clean
# termination (message acked, all callbacks done) is the expected end state here.
java -cp tla2tools.jar pcal.trans MsgLifecycle.tla

# current design -> AckIntegrity violated (5-state trace)
java -cp tla2tools.jar tlc2.TLC -deadlock -config MsgLifecycle_buggy.cfg MsgLifecycle.tla
# current design, liveness only -> NoStuckCallback violated (a callback stutters forever)
java -cp tla2tools.jar tlc2.TLC -deadlock -config MsgLifecycle_buggy_live.cfg MsgLifecycle.tla
# corrected design -> "No error has been found."
java -cp tla2tools.jar tlc2.TLC -deadlock -config MsgLifecycle_fixed.cfg MsgLifecycle.tla

# the worker-pool overshoot model (issue #304)
java -cp tla2tools.jar pcal.trans GateThrottle.tla

# current worker-pool gate -> RespectsThreshold violated (3-state burst trace)
java -cp tla2tools.jar tlc2.TLC -config GateThrottle_binary.cfg GateThrottle.tla
# budget-throttled admission (the fix) -> "No error has been found."
java -cp tla2tools.jar tlc2.TLC -config GateThrottle_proportional.cfg GateThrottle.tla
```

(The buggy run uses two configs only because TLC stops at the first invariant
violation, so liveness gets its own config with no invariants.)

The `.cfg` files fix the model size (workers/tiers/limits). The `\* BEGIN/END
TRANSLATION` block in each `.tla` is generated by `pcal.trans` from the
`--algorithm` block above it — edit the PlusCal, re-run `pcal.trans`, re-check.

## Scope and caveats

- This models the **algorithm**, not the Go implementation. It gives confidence the
  design is correct; it does not verify the code matches the design. For
  `MsgLifecycle` in particular, the model asserts the *hazard* is real and reachable
  under at-least-once redelivery — confirm against the code paths cited above before
  acting; a fix should come with a regression test, not just a green model.
- It assumes Redis operations are linearizable and the Lua check-and-increment is
  atomic — which is what justifies single-step `acquire` in `GateQuota.tla`.
- Numbers in the `.cfg` are small on purpose: model checking is exhaustive, so a
  few workers and tiers (or, for `MsgLifecycle`, two deliveries of one message)
  already cover every interleaving class. Larger models cost state space without
  changing the conclusions.
