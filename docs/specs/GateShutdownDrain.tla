------------------------------ MODULE GateShutdownDrain ------------------------------
(***************************************************************************)
(* Model of the two-phase graceful-shutdown / drain protocol             *)
(* (pkg/server/runner.go, pkg/asyncworker/worker.go, pkg/redis/            *)
(* sortedset_impl.go).                                                     *)
(*                                                                         *)
(* On SIGTERM the runner:                                                  *)
(*   1. cancels the async workers' consumeCtx (the signal ctx);            *)
(*   2. each worker enters its DRAIN loop and re-enqueues every backlog     *)
(*      message with a BARE, unguarded `retryChannel <- msg` (worker.go:67) *)
(*      -- there is no `select { ... case <-ctx.Done() }` escape on that    *)
(*      send;                                                              *)
(*   3. waits for all workers (`wg.Wait()`), optionally cancelling their    *)
(*      requestCtx first on a drain timeout;                               *)
(*   4. ONLY THEN calls `flow.Shutdown()`, which cancels the flow's         *)
(*      (separate) drainCtx and stops the retry worker.                    *)
(*                                                                         *)
(* The bare send at step 2 is safe only because of the step 3->4 ordering: *)
(* the retry worker must outlive every async worker. The code asserts this  *)
(* in a comment ("retryWorker is guaranteed to outlive Workers ... Shutdown *)
(* runs after wg.Wait") but nothing checks it. This model does.            *)
(*                                                                         *)
(* `SafeOrdering` flips the runner's step 3<->4 discipline:                *)
(*   TRUE  -> flow.Shutdown() only after every worker has exited (the       *)
(*            current code). Expected: all properties hold.                *)
(*   FALSE -> the retry worker may be stopped while a worker still has      *)
(*            backlog to re-enqueue (what a careless reorder would do).     *)
(*            Expected: a worker blocks forever on its bare send.          *)
(*                                                                         *)
(* Properties (see the .cfg files):                                        *)
(*   NoStuckWorker   (liveness) every worker eventually exits its drain.    *)
(*   AllBacklogFlushed (liveness) every re-enqueued message reaches Redis.  *)
(*   CleanShutdown   (liveness) workers + retry worker all terminate.       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Workers,       \* set of async worker goroutines, e.g. {w1, w2}
    Backlog,       \* messages each worker must re-enqueue on drain (>=1)
    SafeOrdering   \* TRUE = Shutdown after wg.Wait (current code); FALSE = careless reorder

\* Retry-channel capacity. 1 approximates the real unbuffered channel closely
\* enough: a send blocks whenever the retry worker is not currently receiving.
Cap == 1

TotalBacklog == Cardinality(Workers) * Backlog

(* --algorithm GateShutdownDrain
variables
    signaled     = FALSE,                     \* consumeCtx cancelled (drain trigger)
    reqCancelled = FALSE,                     \* runner drainCtx (workers' requestCtx); set on timeout
    flowDrained  = FALSE,                     \* flow drainCtx cancelled by flow.Shutdown()
    retryCh      = 0,                         \* messages buffered in the retry channel (0..Cap)
    flushed      = 0,                         \* messages the retry worker has flushed to Redis
    workerDone   = [w \in Workers |-> FALSE], \* worker exited its drain (wg.Done)
    todo         = [w \in Workers |-> Backlog],\* backlog left to re-enqueue
    retryDone    = FALSE;                     \* retry worker exited

define
    AllWorkersDone == \A w \in Workers : workerDone[w]

    TypeOK ==
        /\ retryCh \in 0..Cap
        /\ flushed \in 0..TotalBacklog
        /\ workerDone \in [Workers -> BOOLEAN]
        /\ todo \in [Workers -> 0..Backlog]

    \* LIVENESS: no worker is wedged forever on its bare retry send.
    NoStuckWorker == \A w \in Workers : <>(workerDone[w])

    \* LIVENESS: every re-enqueued backlog message eventually reaches Redis.
    AllBacklogFlushed == <>(flushed = TotalBacklog)

    \* LIVENESS: everything terminates cleanly.
    CleanShutdown == <>(AllWorkersDone /\ retryDone)
end define;

\* Async worker: once the signal fires, drain the backlog with bare retry sends,
\* then exit. The send blocks until the retry channel has room (i.e. until the
\* retry worker receives) -- there is no ctx escape on this send.
fair process worker \in Workers
begin
  Wait:
    await signaled;
  Drain:
    while todo[self] > 0 do
      SendRetry:
        await retryCh < Cap;                 \* bare blocking send: waits for the retry worker
        retryCh    := retryCh + 1 ||
        todo[self] := todo[self] - 1;
    end while;
    workerDone[self] := TRUE;
end process;

\* Retry worker: receive + flush, until its drainCtx is cancelled AND the channel
\* is empty (matching retryWorker's drain-then-return on ctx.Done). Never returns
\* early for any other reason.
fair process retry = "retry"
begin
  RetryLoop:
    while ~(flowDrained /\ retryCh = 0) do
      either
        await retryCh > 0;                   \* receive one and flush to Redis
        retryCh := retryCh - 1 ||
        flushed := flushed + 1;
      or
        await flowDrained;                   \* cancelled but drains via the loop guard
        skip;
      end either;
    end while;
    retryDone := TRUE;
end process;

\* The runner's shutdown sequence.
fair process runner = "runner"
begin
  Signal:
    signaled := TRUE;                        \* cancel consumeCtx -> workers start draining
  DrainWait:
    either
      \* graceful: all workers drained within the timeout
      await AllWorkersDone;
    or
      \* drain timeout: cancel the workers' requestCtx, then wait. Note this does
      \* NOT stop the retry worker (a different ctx) and does NOT unblock the
      \* bare retry send in the drain loop.
      reqCancelled := TRUE;
      await AllWorkersDone;
    end either;
  Shutdown:
    \* Current code: flow.Shutdown() (which stops the retry worker) runs only
    \* after wg.Wait() above, i.e. once every worker has exited.
    if SafeOrdering then
      flowDrained := TRUE;
    end if;
end process;

\* Unsafe variant only: a flow.Shutdown() that races the drain -- it can stop the
\* retry worker any time after the signal, before workers finish re-enqueuing.
\* Models a reorder that drops the "retry worker outlives Workers" rule.
fair process earlyShutdown = "early"
begin
  Early:
    await ~SafeOrdering /\ signaled;
    flowDrained := TRUE;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "7d7e1e0e" /\ chksum(tla) = "fc370b8d")
VARIABLES signaled, reqCancelled, flowDrained, retryCh, flushed, workerDone, 
          todo, retryDone, pc

(* define statement *)
AllWorkersDone == \A w \in Workers : workerDone[w]

TypeOK ==
    /\ retryCh \in 0..Cap
    /\ flushed \in 0..TotalBacklog
    /\ workerDone \in [Workers -> BOOLEAN]
    /\ todo \in [Workers -> 0..Backlog]


NoStuckWorker == \A w \in Workers : <>(workerDone[w])


AllBacklogFlushed == <>(flushed = TotalBacklog)


CleanShutdown == <>(AllWorkersDone /\ retryDone)


vars == << signaled, reqCancelled, flowDrained, retryCh, flushed, workerDone, 
           todo, retryDone, pc >>

ProcSet == (Workers) \cup {"retry"} \cup {"runner"} \cup {"early"}

Init == (* Global variables *)
        /\ signaled = FALSE
        /\ reqCancelled = FALSE
        /\ flowDrained = FALSE
        /\ retryCh = 0
        /\ flushed = 0
        /\ workerDone = [w \in Workers |-> FALSE]
        /\ todo = [w \in Workers |-> Backlog]
        /\ retryDone = FALSE
        /\ pc = [self \in ProcSet |-> CASE self \in Workers -> "Wait"
                                        [] self = "retry" -> "RetryLoop"
                                        [] self = "runner" -> "Signal"
                                        [] self = "early" -> "Early"]

Wait(self) == /\ pc[self] = "Wait"
              /\ signaled
              /\ pc' = [pc EXCEPT ![self] = "Drain"]
              /\ UNCHANGED << signaled, reqCancelled, flowDrained, retryCh, 
                              flushed, workerDone, todo, retryDone >>

Drain(self) == /\ pc[self] = "Drain"
               /\ IF todo[self] > 0
                     THEN /\ pc' = [pc EXCEPT ![self] = "SendRetry"]
                          /\ UNCHANGED workerDone
                     ELSE /\ workerDone' = [workerDone EXCEPT ![self] = TRUE]
                          /\ pc' = [pc EXCEPT ![self] = "Done"]
               /\ UNCHANGED << signaled, reqCancelled, flowDrained, retryCh, 
                               flushed, todo, retryDone >>

SendRetry(self) == /\ pc[self] = "SendRetry"
                   /\ retryCh < Cap
                   /\ /\ retryCh' = retryCh + 1
                      /\ todo' = [todo EXCEPT ![self] = todo[self] - 1]
                   /\ pc' = [pc EXCEPT ![self] = "Drain"]
                   /\ UNCHANGED << signaled, reqCancelled, flowDrained, 
                                   flushed, workerDone, retryDone >>

worker(self) == Wait(self) \/ Drain(self) \/ SendRetry(self)

RetryLoop == /\ pc["retry"] = "RetryLoop"
             /\ IF ~(flowDrained /\ retryCh = 0)
                   THEN /\ \/ /\ retryCh > 0
                              /\ /\ flushed' = flushed + 1
                                 /\ retryCh' = retryCh - 1
                           \/ /\ flowDrained
                              /\ TRUE
                              /\ UNCHANGED <<retryCh, flushed>>
                        /\ pc' = [pc EXCEPT !["retry"] = "RetryLoop"]
                        /\ UNCHANGED retryDone
                   ELSE /\ retryDone' = TRUE
                        /\ pc' = [pc EXCEPT !["retry"] = "Done"]
                        /\ UNCHANGED << retryCh, flushed >>
             /\ UNCHANGED << signaled, reqCancelled, flowDrained, workerDone, 
                             todo >>

retry == RetryLoop

Signal == /\ pc["runner"] = "Signal"
          /\ signaled' = TRUE
          /\ pc' = [pc EXCEPT !["runner"] = "DrainWait"]
          /\ UNCHANGED << reqCancelled, flowDrained, retryCh, flushed, 
                          workerDone, todo, retryDone >>

DrainWait == /\ pc["runner"] = "DrainWait"
             /\ \/ /\ AllWorkersDone
                   /\ UNCHANGED reqCancelled
                \/ /\ reqCancelled' = TRUE
                   /\ AllWorkersDone
             /\ pc' = [pc EXCEPT !["runner"] = "Shutdown"]
             /\ UNCHANGED << signaled, flowDrained, retryCh, flushed, 
                             workerDone, todo, retryDone >>

Shutdown == /\ pc["runner"] = "Shutdown"
            /\ IF SafeOrdering
                  THEN /\ flowDrained' = TRUE
                  ELSE /\ TRUE
                       /\ UNCHANGED flowDrained
            /\ pc' = [pc EXCEPT !["runner"] = "Done"]
            /\ UNCHANGED << signaled, reqCancelled, retryCh, flushed, 
                            workerDone, todo, retryDone >>

runner == Signal \/ DrainWait \/ Shutdown

Early == /\ pc["early"] = "Early"
         /\ ~SafeOrdering /\ signaled
         /\ flowDrained' = TRUE
         /\ pc' = [pc EXCEPT !["early"] = "Done"]
         /\ UNCHANGED << signaled, reqCancelled, retryCh, flushed, workerDone, 
                         todo, retryDone >>

earlyShutdown == Early

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == retry \/ runner \/ earlyShutdown
           \/ (\E self \in Workers: worker(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Workers : WF_vars(worker(self))
        /\ WF_vars(retry)
        /\ WF_vars(runner)
        /\ WF_vars(earlyShutdown)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 
=============================================================================
