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
| [`GateCancelGen.tla`](GateCancelGen.tla) | PR [#307](https://github.com/llm-d-incubation/llm-d-async/pull/307): generation-aware cancellation — a reused request id, a per-submission token, and the `request-active` / `request-cancel` keys under concurrent submit / cancel / deferred cleanup | **token-guarded design (#307): all hold.** The id-only design violates both `NoSpuriousCancel` and `NoLostCancel` (an ABA where a stale generation's cleanup clobbers a newer one's keys). |
| [`MergePriority.tla`](MergePriority.tla) | PR [#294](https://github.com/llm-d-incubation/llm-d-async/pull/294): the tier-priority merge policy's strict-priority `Pop()` under sustained high-priority load | **current design: `NoStarvation` violated** — TLC finds a fair lasso where a low-priority lane is never served. A starvation-free policy (flip one constant) holds. |
| [`ResultCorrelation.tla`](ResultCorrelation.tla) | issue [#308](https://github.com/llm-d-incubation/llm-d-async/issues/308): the producer's `GetResult` doing `BRPOP` on one **shared** result list with N concurrent callers | **current design: `EachCallerGetsOwnResult` violated** — a caller pops another caller's result. The per-request result queue (flip one constant) holds. |
| [`GateReservationLeak.tla`](GateReservationLeak.tla) | the per-queue gate capacity reservation: acquired on `ActionContinue`, released **only** on a `ResultMessage`, but every retry re-enqueues without releasing (`activeReleases.Store` also overwrites) | **current code: `AccountingExact` and `EventuallyServed` both violated** — leaked slots ratchet `inFlight` up until `Budget()=0` wedges the queue. Releasing on retry (flip one constant) holds. |
| [`GateShutdownDrain.tla`](GateShutdownDrain.tla) | the two-phase graceful shutdown: workers drain their backlog with **bare, unguarded** `retryChannel <-` sends; `flow.Shutdown()` (which stops the retry worker) runs only after `wg.Wait()` | **current code: all properties hold** — TLC proves the retry worker outlives every async worker, so the bare sends never wedge. A reordered variant (flip one constant) violates `NoStuckWorker`. |
| [`GateQuotaTTL.tla`](GateQuotaTTL.tla) | the `redis-quota` concurrency counter, whose `EXPIRE` is armed only on the 0→1 `INCR` and never refreshed — so the whole key can expire while reserved holders are still in flight | **current code: `NoOverAdmission` violated** — a mid-flight expiry lets new acquires re-admit up to `Limit` again (real concurrency `2 > Limit`). Reclaiming only leaked slots (flip one constant) holds. |

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

## What the cancellation model found (PR #307)

`GateCancelGen.tla` models generation-aware cancellation. A request id can be
submitted more than once — a retry, or an at-least-once redelivery — so one id
names several *generations*. #307 tells them apart with a per-submission random
token and two Redis keys, `request-active:<id>` and `request-cancel:<id>`, touched
by four atomic operations (`producer/redis_sortedset_producer.go`,
`pkg/redis/sortedset_impl.go`):

- **Submit(g)** — `DEL cancel; SET active := token(g); enqueue` (a `MULTI/EXEC`).
- **Cancel** — `active := GET active; if active != nil: SET cancel := active`
  (the `markRequestCancelledScript` — it captures whatever generation is active *now*).
- **IsCancelled(g)** — `GET cancel == token(g)` (a **token compare**).
- **Cleanup(g)** — delete `active`/`cancel` **only if they still hold `token(g)`**
  (the `cleanupRequestStateScript` — a **token-guarded** delete), run on the
  deferred result-flush path.

The hazard is ABA: cleanup for a *finished* generation runs on the flush path,
which can land **after** the id has been resubmitted as a newer generation. The
`TokenGuarded` constant flips between #307 (token compare + guarded delete) and an
id-only design (a cancel marker just has to *exist*; cleanup deletes
unconditionally). Two properties:

- **`NoSpuriousCancel`** *(safety)* — a generation is cancelled only if the user
  targeted *that* generation.
- **`NoLostCancel`** *(safety)* — a generation cancelled while still queued is never
  dispatched.

With `TokenGuarded = TRUE`, both hold (plus `NoKeyLeak`, liveness). With
`TokenGuarded = FALSE`, TLC finds a 7-state ABA trace violating both: gen 1 and
gen 2 (a resubmission of the same id) are both queued; Cancel targets gen 2 and
sets the marker; the worker drops **gen 1** on the id-only check (spurious); gen 1's
unconditional cleanup then **deletes gen 2's cancel marker**; so gen 2 dispatches
despite being cancelled (lost). The token guard on both `IsCancelled` and `Cleanup`
is exactly what closes this — the model is the justification for carrying the token
rather than keying cancellation on the id alone.

The model also pins the scope: it fires Cancel only on the **final** generation.
A resubmit *after* a cancel legitimately revives the id (Submit's `DEL cancel`), so
that is not a lost cancel — an earlier draft's property wrongly flagged it, and
tightening the model to the real guarantee is what made `TokenGuarded = TRUE` clean.

## What the merge-policy model found (PR #294)

`MergePriority.tla` models the tier-priority merge scheduler
(`pkg/async/mergepolicy/tierpriority/tier_priority_policy.go`). Its `Pop()` scans
the 6 priority buckets high → low and serves the first non-empty one — **strict**
priority. Under sustained high-priority load a lower lane is therefore never
reached. Abstracted to a HIGH bucket under continuous arrivals plus one
distinguished LOW request, and flipping `StrictPriority`:

- **`NoStarvation`** *(liveness)* — `loArrived ~> loServed`: a buffered low-priority
  request is eventually served.
- `TRUE` (current `Pop()`) → TLC finds a **fair lasso**: arrivals keep refilling the
  HIGH bucket, `Pop()` forever serves HIGH, and the LOW request is never served —
  `NoStarvation` violated. Weak fairness on the reader does not save it, because
  serve-low is only intermittently enabled.
- `FALSE` (any starvation-free policy — aging, weighted, round-robin) → holds.

The same unconditional strict ordering also head-of-line blocks: each input channel
has a single FIFO reader goroutine that blocks in `Push()` when its target bucket
is full, so a high-priority message queued behind a low-priority one *on the same
channel* cannot advance (priority inversion). Both symptoms share the root cause;
the model checks the starvation half, which is the cleaner liveness statement.

## What the result-correlation model found (issue #308)

`ResultCorrelation.tla` models `GetResult`, which does `BRPOP <resultQueueName>`
(`producer/redis_sortedset_producer.go:208`) on the producer's **one shared**
result list. The processor pushes each request's result (tagged with its id) onto
that same list, so `BRPOP` hands a caller *whatever* result is at the tail — there
is no caller↔result correlation. Flipping `PerRequestQueue`:

- **`EachCallerGetsOwnResult`** *(safety)* — a caller only ever receives the result
  for the id it submitted.
- `FALSE` (one shared list) → TLC finds a 3-state trace where a caller pops another
  caller's result — violated.
- `TRUE` (per-request list, via `api.RedisRequest`'s existing `ResultQueueName`
  override) → holds.

This formalizes the correlation limitation behind #308: a synchronous OpenAI-style
gateway sitting in front of the queues needs per-request result routing, not the
shared list.

## What the reservation-lifecycle model found (a real, latent bug)

`GateReservationLeak.tla` is, like `MsgLifecycle`, a model written to *probe* the
current code rather than confirm a known fact — and it found a reachable defect.

The Redis sorted-set flow reserves gate capacity when the queue gate returns
`ActionContinue` (`LocalConcurrencyGate.inFlight++`, or a redis-quota `INCR`) and
stores the release closure keyed by request id:

```go
r.activeReleases.Store(rview.ReqID(), releases)   // sortedset_impl.go:527
```

That release runs in **exactly one place** — when a `ResultMessage` for the id passes
through `resultWorker`:

```go
if val, ok := r.activeReleases.LoadAndDelete(m.ID); ok { ... }   // :648
```

But **every retry path emits a `RetryMessage` and no result**
(`worker.go:67,129,169,183,282,427,491`): a retriable inference error, an
`ActionRefuse`, an `ActionWait` that times out, a cancellation-check error, an
in-flight ctx cancellation. So a retried request is re-enqueued **without releasing
its reservation**, and — because `activeReleases.Store` (`:527`) is a plain store — a
later re-dispatch of the same id **overwrites** the previous closure, orphaning it
forever. `LocalConcurrencyGate` has no TTL and `Budget()` returns 0 once
`inFlight >= limit` (`local_concurrency_gate.go:59-61`), which floors `batchSize` to 0
— so leaked slots wedge the queue permanently.

The `ReleaseOnRetry` constant flips between the current code (retry does not release)
and the fix (retry releases). Two properties:

- **`AccountingExact`** *(safety)* — `inFlight` equals the number of ids that actually
  hold a reservation. With `ReleaseOnRetry = FALSE` and `Limit = 2`, TLC finds a
  4-state trace: dispatch r1 (`inFlight=1`) → retry r1 (`inFlight` still 1, r1 still
  reserved) → re-dispatch r1 (`inFlight=2`, still only r1 reserved) → `2 ≠ 1`.
- **`EventuallyServed`** *(liveness)* — every request eventually completes. With
  `ReleaseOnRetry = FALSE` and `Limit = 1`, TLC finds a lasso: r1 dispatches, retries,
  and can never be re-dispatched because its own un-released reservation occupies the
  only slot — r1 (and the whole queue) wedge forever.

With `ReleaseOnRetry = TRUE` both hold. The fix is to release the reservation on the
retry paths (or `LoadAndDelete` before re-`Store`, or carry the reservation across the
retry). Unlike the other models here, this one describes a defect in **shipping
code**, so it should be paired with a regression test, not just a green model.

## What the shutdown model confirmed (a green result)

`GateShutdownDrain.tla` was written to *hunt* for a bug in the two-phase graceful
shutdown — the most-churned area of the code (fixes in #197, #236, #238, #245, #296) —
and instead it **validated** the design. When the runner shuts down, each async worker
drains its request backlog by re-enqueuing every message with a **bare, unguarded**
send:

```go
retryChannel <- retryMsg   // worker.go:67 — no `select { case <-ctx.Done() }` escape
```

That send can only be safe if the retry worker is still receiving. The code relies on
an ordering — `flow.Shutdown()` (which stops the retry worker) runs only *after*
`wg.Wait()` — asserted in a comment ("retryWorker is guaranteed to outlive Workers")
but never checked. Note the two *distinct* drain contexts: the runner's `drainCtx`
(the workers' `requestCtx`, cancelled on a drain timeout) is not the flow's `drainCtx`
(the retry worker's, cancelled only by `flow.Shutdown()`), so a drain timeout does not
stop the retry worker.

Flipping `SafeOrdering`:

- **`TRUE`** (current code) → **all properties hold**: `NoStuckWorker`,
  `AllBacklogFlushed`, `CleanShutdown`. TLC confirms across every interleaving —
  including the drain-timeout path — that the retry worker outlives every async worker,
  so no bare send ever wedges and no backlog is lost.
- **`FALSE`** (a reorder that stops the retry worker before the workers finish) → TLC
  finds a lasso violating `NoStuckWorker`: a worker blocks forever on its bare send.

So this one is a **confidence result, not a discovery** — the ordering the code depends
on is race-free, and the model pins down exactly the reorder that would break it (and
would make a good guard against a future refactor).

## What the quota-TTL model found (a latent over-admission)

`GateQuotaTTL.tla` models the `redis-quota` concurrency counter
(`pkg/redis/quota_gate.go`, `acquireConcurrency`). The acquire Lua arms the key's
`EXPIRE` **only** on the 0→1 `INCR` (`if tonumber(new_val) == 1 then ... EXPIRE`) and
never refreshes it; release is a guarded `DECR`. A concurrency limiter has no natural
time window, yet the key carries a TTL (the `window`, or a 300s default) anchored at
the *first* acquire — so the whole counter can expire while reserved holders are still
in flight.

With `BlindTTL = TRUE` (current code), TLC finds a 4-step `NoOverAdmission` violation
at `Limit = 1`:

```
w1 acquire        counter 0->1, key armed, w1 reserved (in flight)
TTL fires         key expires -> counter = 0, key gone  (w1 STILL in flight)
w2 acquire        key absent -> INCR -> counter = 1, w2 reserved (in flight)
                  -> RealReserved = {w1, w2} = 2 > Limit = 1
```

The old holders' guarded `DECR` then decrements the *new* generation's counter,
compounding the drift. With `BlindTTL = FALSE` (expiry reclaims only genuinely-leaked
slots, not live ones) the property holds. Like `GateReservationLeak`, this is a defect
in **shipping code** (the earlier `GateQuota.tla` idealized the TTL as an
orphan-only reaper, which hid it) — worth filing with a regression test, and pairing
with a lease/refresh or leak-scoped reclaim rather than a blind whole-key TTL.

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

# the generation-aware cancellation model (PR #307). -deadlock: clean quiescence
# (everything cancelled/dispatched and cleaned) is the expected end state.
java -cp tla2tools.jar pcal.trans GateCancelGen.tla
# #307 token-guarded design -> "No error has been found."
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateCancelGen_guarded.cfg GateCancelGen.tla
# id-only design -> NoLostCancel violated (the ABA: stale cleanup drops gen 2's cancel)
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateCancelGen_naive_lostcancel.cfg GateCancelGen.tla
# id-only design -> NoSpuriousCancel violated (gen 1 dropped on a marker meant for gen 2)
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateCancelGen_naive_spurious.cfg GateCancelGen.tla

# the tier-priority merge starvation model (PR #294)
java -cp tla2tools.jar pcal.trans MergePriority.tla
# current strict-priority Pop() -> NoStarvation violated (fair lasso; low never served)
java -cp tla2tools.jar tlc2.TLC -config MergePriority_strict.cfg MergePriority.tla
# starvation-free policy -> "No error has been found."
java -cp tla2tools.jar tlc2.TLC -config MergePriority_fair.cfg MergePriority.tla

# the shared-result-queue correlation model (issue #308)
java -cp tla2tools.jar pcal.trans ResultCorrelation.tla
# one shared result list -> EachCallerGetsOwnResult violated (caller pops another's result)
java -cp tla2tools.jar tlc2.TLC -deadlock -config ResultCorrelation_shared.cfg ResultCorrelation.tla
# per-request result list -> "No error has been found."
java -cp tla2tools.jar tlc2.TLC -deadlock -config ResultCorrelation_perrequest.cfg ResultCorrelation.tla

# the gate reservation-leak model (a latent bug in shipping code)
java -cp tla2tools.jar pcal.trans GateReservationLeak.tla
# current code -> AccountingExact violated (Store overwrite double-counts inFlight)
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateReservationLeak_buggy.cfg GateReservationLeak.tla
# current code, liveness -> EventuallyServed violated (retried request wedges the queue)
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateReservationLeak_buggy_live.cfg GateReservationLeak.tla
# release-on-retry (the fix) -> "No error has been found."
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateReservationLeak_fixed.cfg GateReservationLeak.tla

# the graceful-shutdown / drain model (validates the current ordering)
java -cp tla2tools.jar pcal.trans GateShutdownDrain.tla
# current code (Shutdown after wg.Wait) -> "No error has been found."
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateShutdownDrain_safe.cfg GateShutdownDrain.tla
# reordered (retry worker stopped early) -> NoStuckWorker violated (worker wedges on bare send)
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateShutdownDrain_unsafe.cfg GateShutdownDrain.tla

# the quota-counter TTL model (a latent over-admission in shipping code)
java -cp tla2tools.jar pcal.trans GateQuotaTTL.tla
# current code (whole-key EXPIRE) -> NoOverAdmission violated (mid-flight expiry re-admits)
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateQuotaTTL_blind.cfg GateQuotaTTL.tla
# leak-scoped reclaim (the fix) -> "No error has been found."
java -cp tla2tools.jar tlc2.TLC -deadlock -config GateQuotaTTL_scoped.cfg GateQuotaTTL.tla
```

(A buggy model with two counter-examples uses two configs only because TLC stops at
the first invariant violation — e.g. `GateCancelGen` splits `NoLostCancel` and
`NoSpuriousCancel`, and `MsgLifecycle` gives liveness its own config with no invariants.)

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
