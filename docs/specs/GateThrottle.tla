------------------------------- MODULE GateThrottle -------------------------------
(***************************************************************************)
(* Formal model of issue #304: a worker-pool gate does not respect its    *)
(* saturation threshold.                                                   *)
(*                                                                         *)
(* The gate's Budget()/Apply() is evaluated against the last Prometheus    *)
(* scrape, which LAGS the real in-flight count. Two designs differ in how  *)
(* they use it (flip the `Proportional` constant):                        *)
(*                                                                         *)
(*   Proportional = FALSE  -- the worker-POOL gate (current behavior).     *)
(*     Each of N pool workers makes an independent BINARY decision         *)
(*     (`observed < Threshold` -> dispatch). While the scrape is stale,    *)
(*     many idle workers all pass the check and dispatch at once, so the   *)
(*     real in-flight count bursts past Threshold before the next scrape   *)
(*     catches up. This is the #304 overshoot (benchmark avg 1.73 vs 0.8). *)
(*                                                                         *)
(*   Proportional = TRUE   -- admission bounded by REAL remaining budget   *)
(*     (the proposed fix / the queue gate's `batchSize * budget`). A       *)
(*     worker only proceeds while `inflight < Threshold`, so the number of *)
(*     active workers is throttled to the remaining capacity and the real  *)
(*     in-flight count never exceeds Threshold.                            *)
(*                                                                         *)
(* Property (see the .cfg files):                                          *)
(*   RespectsThreshold (safety)  inflight <= Threshold, always.            *)
(*     FALSE -> TLC prints a trace with inflight = |Workers| > Threshold.  *)
(*     TRUE  -> "No error has been found."                                 *)
(*                                                                         *)
(* Idealization: the scrape+admit of the single serialized queue worker is *)
(* modeled as an atomic check on `inflight`, so the fix holds Threshold    *)
(* exactly. Real scrape lag still leaves the queue path a small, bounded   *)
(* residual overshoot (the benchmark's 0.8118); the point the model proves *)
(* is that the pool path's overshoot instead grows with the worker count.  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Workers,      \* set of worker identities, e.g. {w1, w2, w3}
    Proportional  \* TRUE = budget-throttled admission (fix); FALSE = binary pool gate (bug)

\* The saturation cap in integer in-flight "slots" (an abstraction of the 0.8
\* threshold). Defined here rather than in the .cfg so the model stays small and
\* self-contained; |Workers| > Threshold is what makes the overshoot observable.
Threshold == 2

(* --algorithm GateThrottle
variables
    inflight = 0,                         \* real in-flight requests at EPP (true saturation)
    observed = 0,                         \* last-scraped saturation reading (LAGS inflight)
    active   = [w \in Workers |-> FALSE]; \* whether a worker currently holds an in-flight request

define
    \* SAFETY: real saturation never exceeds the configured threshold.
    RespectsThreshold == inflight <= Threshold

    TypeOK ==
        /\ inflight \in 0..Cardinality(Workers)
        /\ observed \in 0..Cardinality(Workers)
        /\ active   \in [Workers -> BOOLEAN]

    \* The real in-flight count is exactly the workers currently holding a request.
    Consistent == inflight = Cardinality({w \in Workers : active[w]})
end define;

fair process worker \in Workers
begin
  Step:
    while TRUE do
      either
        \* ADMIT — the gate decision.
        if ~active[self] then
          if Proportional then
            \* FIX: proceed only while real remaining budget exists. Active workers
            \* are thereby throttled to the remaining capacity.
            if inflight < Threshold then
              inflight := inflight + 1 || active[self] := TRUE;
            end if;
          else
            \* BUG: binary decision on the stale scrape. Many idle workers pass this
            \* check simultaneously during the scrape-lag window and all dispatch.
            if observed < Threshold then
              inflight := inflight + 1 || active[self] := TRUE;
            end if;
          end if;
        end if;
      or
        \* COMPLETE — the request finishes and drains its slot.
        if active[self] then
          inflight := inflight - 1 || active[self] := FALSE;
        end if;
      end either;
    end while;
end process;

\* Prometheus scrape: refresh the observed reading from the real value. Between
\* scrapes `observed` is stale — that lag is the crux of the burst overshoot.
fair process scraper = "scraper"
begin
  Scrape:
    while TRUE do
      observed := inflight;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION
VARIABLES inflight, observed, active

(* define statement *)
RespectsThreshold == inflight <= Threshold

TypeOK ==
    /\ inflight \in 0..Cardinality(Workers)
    /\ observed \in 0..Cardinality(Workers)
    /\ active   \in [Workers -> BOOLEAN]


Consistent == inflight = Cardinality({w \in Workers : active[w]})


vars == << inflight, observed, active >>

ProcSet == (Workers) \cup {"scraper"}

Init == (* Global variables *)
        /\ inflight = 0
        /\ observed = 0
        /\ active = [w \in Workers |-> FALSE]

worker(self) == /\ \/ /\ IF ~active[self]
                            THEN /\ IF Proportional
                                       THEN /\ IF inflight < Threshold
                                                  THEN /\ /\ active' = [active EXCEPT ![self] = TRUE]
                                                          /\ inflight' = inflight + 1
                                                  ELSE /\ TRUE
                                                       /\ UNCHANGED << inflight, 
                                                                       active >>
                                       ELSE /\ IF observed < Threshold
                                                  THEN /\ /\ active' = [active EXCEPT ![self] = TRUE]
                                                          /\ inflight' = inflight + 1
                                                  ELSE /\ TRUE
                                                       /\ UNCHANGED << inflight, 
                                                                       active >>
                            ELSE /\ TRUE
                                 /\ UNCHANGED << inflight, active >>
                   \/ /\ IF active[self]
                            THEN /\ /\ active' = [active EXCEPT ![self] = FALSE]
                                    /\ inflight' = inflight - 1
                            ELSE /\ TRUE
                                 /\ UNCHANGED << inflight, active >>
                /\ UNCHANGED observed

scraper == /\ observed' = inflight
           /\ UNCHANGED << inflight, active >>

Next == scraper
           \/ (\E self \in Workers: worker(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Workers : WF_vars(worker(self))
        /\ WF_vars(scraper)

\* END TRANSLATION
=============================================================================
