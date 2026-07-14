------------------------------ MODULE GateSyncGateway ------------------------------
(***************************************************************************)
(* DESIGN-TIME model of the proposed synchronous OpenAI gateway            *)
(* (issue #308) -- a service that fronts the async queues: it submits each  *)
(* HTTP request via the producer, blocks on the correlated result, and      *)
(* returns it. The component does not exist yet; this checks the            *)
(* correlation design before it is built.                                  *)
(*                                                                         *)
(* Design A (the #308 recommendation): each request gets its own result     *)
(* queue name; the handler BRPOPs exactly that queue. Two facts from #308    *)
(* make this non-trivial:                                                   *)
(*   - result queue names are short-lived Redis keys, so a real gateway      *)
(*     RECYCLES them (or a client may reuse a request id);                  *)
(*   - on client disconnect/timeout the gateway abandons the wait and calls  *)
(*     CancelRequests, but cancel is best-effort: an already-dispatched      *)
(*     request still produces a result LATER, which lands on the queue name  *)
(*     that request used -- possibly after that name was recycled to a new   *)
(*     request.                                                             *)
(*                                                                         *)
(* Two constants flip the design:                                          *)
(*   Recycle    TRUE  -> result-queue names are drawn from a bounded pool    *)
(*                       and returned on done/timeout (names get reused).    *)
(*              FALSE -> every request gets a globally-unique, never-reused   *)
(*                       name (UUID-style; leaks a key, cleaned by TTL).     *)
(*   TokenGuard TRUE  -> the result carries a per-submission token and the   *)
(*                       handler accepts a popped result only if the token   *)
(*                       matches its own submission; a foreign result is      *)
(*                       discarded and the handler keeps waiting.            *)
(*              FALSE -> the handler accepts whatever it pops from its queue  *)
(*                       (name-only correlation).                            *)
(*                                                                         *)
(* Property (see the .cfg files):                                          *)
(*   CorrectResponse (safety)  a handler only ever returns the result of     *)
(*                             ITS OWN submission.                          *)
(*     Recycle=FALSE                 -> holds (unique names).                *)
(*     Recycle=TRUE,  TokenGuard=FALSE -> VIOLATED: a late result on a        *)
(*        recycled name is returned to the wrong caller.                    *)
(*     Recycle=TRUE,  TokenGuard=TRUE  -> holds (token rejects the foreign    *)
(*        result).                                                          *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Reqs,        \* set of concurrent requests/handlers (each also its own token), e.g. {r1, r2}
    MaxDup,      \* max result copies the async backend may deliver per request (>=1)
    Recycle,     \* TRUE = result-queue names reused from a bounded pool; FALSE = unique per request
    TokenGuard   \* TRUE = handler matches results on a per-submission token; FALSE = name-only

\* Result-queue name pool. One name when recycling (forces reuse); one-per-request
\* otherwise (never reused).
NameCount == IF Recycle THEN 1 ELSE Cardinality(Reqs)
Names == 1..NameCount

(* --algorithm GateSyncGateway
variables
    free      = Names,                     \* free result-queue names
    nameOf    = [g \in Reqs |-> 0],        \* name a request submitted with (0 = unassigned); never cleared
    resultQ   = [n \in Names |-> <<>>],    \* results written per queue name, each tagged with its owning request
    produced  = [g \in Reqs |-> 0],        \* result copies the backend has written for g
    responded = [g \in Reqs |-> "none"],   \* the request whose result this handler returned ("none" = none yet)
    hstate    = [g \in Reqs |-> "new"];    \* new | waiting | done | timedout

define
    Submitted(g) == hstate[g] # "new"

    TypeOK ==
        /\ free \subseteq Names
        /\ nameOf \in [Reqs -> Names \cup {0}]
        /\ produced \in [Reqs -> 0..MaxDup]
        /\ responded \in [Reqs -> Reqs \cup {"none"}]
        /\ hstate \in [Reqs -> {"new","waiting","done","timedout"}]

    \* SAFETY: a handler only ever returns the result of its own submission.
    CorrectResponse == \A g \in Reqs : responded[g] \in {"none", g}
end define;

\* One gateway handler per request: acquire a result-queue name, submit, then wait
\* for a result on that queue -- or time out (client disconnect) and give up.
fair process client \in Reqs
begin
  Acquire:
    await hstate[self] = "new" /\ free # {};
    with n \in free do
      nameOf[self] := n ||
      free         := free \ {n} ||
      hstate[self] := "waiting";
    end with;
  Serve:
    while hstate[self] = "waiting" do
      either
        \* BRPOP the handler's own result queue (tail = right pop).
        await resultQ[nameOf[self]] # <<>>;
        with q = resultQ[nameOf[self]], e = q[Len(q)] do
          if TokenGuard /\ e # self then
            \* token mismatch: discard the foreign result, keep waiting.
            resultQ[nameOf[self]] := SubSeq(q, 1, Len(q) - 1);
          else
            responded[self]       := e ||
            resultQ[nameOf[self]] := SubSeq(q, 1, Len(q) - 1) ||
            hstate[self]          := "done" ||
            free := IF Recycle THEN free \cup {nameOf[self]} ELSE free;
          end if;
        end with;
      or
        \* client disconnect / timeout: abandon the wait, release the name if recycling.
        hstate[self] := "timedout" ||
        free := IF Recycle THEN free \cup {nameOf[self]} ELSE free;
      end either;
    end while;
end process;

\* The async backend: for each submitted request, eventually write its result (tagged
\* with the request) onto the name that request submitted with -- possibly LATE (after
\* the handler timed out and the name was recycled) and possibly more than once.
fair process backend = "async"
begin
  Produce:
    while \E g \in Reqs : Submitted(g) /\ produced[g] < MaxDup do
      with g \in {gg \in Reqs : Submitted(gg) /\ produced[gg] < MaxDup} do
        produced[g]          := produced[g] + 1 ||
        resultQ[nameOf[g]]   := Append(resultQ[nameOf[g]], g);
      end with;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "82cdead8" /\ chksum(tla) = "54f5e16")
VARIABLES free, nameOf, resultQ, produced, responded, hstate, pc

(* define statement *)
Submitted(g) == hstate[g] # "new"

TypeOK ==
    /\ free \subseteq Names
    /\ nameOf \in [Reqs -> Names \cup {0}]
    /\ produced \in [Reqs -> 0..MaxDup]
    /\ responded \in [Reqs -> Reqs \cup {"none"}]
    /\ hstate \in [Reqs -> {"new","waiting","done","timedout"}]


CorrectResponse == \A g \in Reqs : responded[g] \in {"none", g}


vars == << free, nameOf, resultQ, produced, responded, hstate, pc >>

ProcSet == (Reqs) \cup {"async"}

Init == (* Global variables *)
        /\ free = Names
        /\ nameOf = [g \in Reqs |-> 0]
        /\ resultQ = [n \in Names |-> <<>>]
        /\ produced = [g \in Reqs |-> 0]
        /\ responded = [g \in Reqs |-> "none"]
        /\ hstate = [g \in Reqs |-> "new"]
        /\ pc = [self \in ProcSet |-> CASE self \in Reqs -> "Acquire"
                                        [] self = "async" -> "Produce"]

Acquire(self) == /\ pc[self] = "Acquire"
                 /\ hstate[self] = "new" /\ free # {}
                 /\ \E n \in free:
                      /\ free' = free \ {n}
                      /\ hstate' = [hstate EXCEPT ![self] = "waiting"]
                      /\ nameOf' = [nameOf EXCEPT ![self] = n]
                 /\ pc' = [pc EXCEPT ![self] = "Serve"]
                 /\ UNCHANGED << resultQ, produced, responded >>

Serve(self) == /\ pc[self] = "Serve"
               /\ IF hstate[self] = "waiting"
                     THEN /\ \/ /\ resultQ[nameOf[self]] # <<>>
                                /\ LET q == resultQ[nameOf[self]] IN
                                     LET e == q[Len(q)] IN
                                       IF TokenGuard /\ e # self
                                          THEN /\ resultQ' = [resultQ EXCEPT ![nameOf[self]] = SubSeq(q, 1, Len(q) - 1)]
                                               /\ UNCHANGED << free, responded, 
                                                               hstate >>
                                          ELSE /\ /\ free' = (IF Recycle THEN free \cup {nameOf[self]} ELSE free)
                                                  /\ hstate' = [hstate EXCEPT ![self] = "done"]
                                                  /\ responded' = [responded EXCEPT ![self] = e]
                                                  /\ resultQ' = [resultQ EXCEPT ![nameOf[self]] = SubSeq(q, 1, Len(q) - 1)]
                             \/ /\ /\ free' = (IF Recycle THEN free \cup {nameOf[self]} ELSE free)
                                   /\ hstate' = [hstate EXCEPT ![self] = "timedout"]
                                /\ UNCHANGED <<resultQ, responded>>
                          /\ pc' = [pc EXCEPT ![self] = "Serve"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << free, resultQ, responded, hstate >>
               /\ UNCHANGED << nameOf, produced >>

client(self) == Acquire(self) \/ Serve(self)

Produce == /\ pc["async"] = "Produce"
           /\ IF \E g \in Reqs : Submitted(g) /\ produced[g] < MaxDup
                 THEN /\ \E g \in {gg \in Reqs : Submitted(gg) /\ produced[gg] < MaxDup}:
                           /\ produced' = [produced EXCEPT ![g] = produced[g] + 1]
                           /\ resultQ' = [resultQ EXCEPT ![nameOf[g]] = Append(resultQ[nameOf[g]], g)]
                      /\ pc' = [pc EXCEPT !["async"] = "Produce"]
                 ELSE /\ pc' = [pc EXCEPT !["async"] = "Done"]
                      /\ UNCHANGED << resultQ, produced >>
           /\ UNCHANGED << free, nameOf, responded, hstate >>

backend == Produce

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == backend
           \/ (\E self \in Reqs: client(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Reqs : WF_vars(client(self))
        /\ WF_vars(backend)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 
=============================================================================
