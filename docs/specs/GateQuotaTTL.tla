------------------------------- MODULE GateQuotaTTL -------------------------------
(***************************************************************************)
(* Model of the `redis-quota` concurrency gate's counter TTL              *)
(* (pkg/redis/quota_gate.go, acquireConcurrency).                          *)
(*                                                                         *)
(* The acquire Lua increments a shared per-tenant counter and arms an       *)
(* EXPIRE **only** on the 0 -> 1 transition, never refreshing it:          *)
(*                                                                         *)
(*   local current = redis.call("GET", KEYS[1])                            *)
(*   if current and tonumber(current) >= tonumber(ARGV[1]) then return 0 end*)
(*   local new_val = redis.call("INCR", KEYS[1])                           *)
(*   if tonumber(new_val) == 1 then redis.call("EXPIRE", KEYS[1], ARGV[2]) end*)
(*   return 1                                                              *)
(*                                                                         *)
(* Release is a guarded DECR (`if current > 0 then DECR`). A concurrency    *)
(* limiter has no natural time window, yet the key carries a TTL (the       *)
(* `window`, or a 300s default) anchored at the FIRST acquire. If the key   *)
(* expires while reserved holders are still in flight, the counter vanishes:*)
(* new acquires INCR a fresh counter (re-admitting up to `Limit` while the  *)
(* old holders are still running), and the old holders' guarded DECR then   *)
(* decrements the NEW generation's counter -- compounding the over-count.   *)
(*                                                                         *)
(* Classifying mode: over-limit requests are `overflow` (admitted, no slot, *)
(* no release); under-limit are `reserved` (INCR + a DECR release).         *)
(*                                                                         *)
(* `BlindTTL` flips the expiry discipline:                                  *)
(*   TRUE  -> the current code: the whole counter key can expire while      *)
(*            reserved holders are still in flight.                        *)
(*   FALSE -> no whole-key expiry (a correct concurrency limiter reclaims    *)
(*            only genuinely-leaked slots, not live ones).                  *)
(*                                                                         *)
(* Property (see the .cfg files):                                          *)
(*   NoOverAdmission (safety)  reserved requests actually in flight never   *)
(*                             exceed Limit.                               *)
(*     TRUE  -> VIOLATED: TLC finds an expiry mid-flight that re-admits.    *)
(*     FALSE -> "No error has been found."                                  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Workers,   \* set of concurrent workers contending for one tenant's quota
    Limit,     \* the concurrency quota (reserved slots)
    BlindTTL   \* TRUE = current whole-key expiry (bug); FALSE = no live-counter expiry (fix)

(* --algorithm GateQuotaTTL
variables
    counter = 0,                          \* the Redis counter value (0 when key absent)
    keyLive = FALSE,                      \* whether the counter key currently exists
    armed   = FALSE,                      \* an EXPIRE is pending (armed on the 0->1 INCR)
    holder  = [w \in Workers |-> "idle"]; \* idle | reserved (holds a slot, in flight) | overflow

define
    \* Reserved requests currently in flight (the TRUE concurrency the gate must cap).
    RealReserved == Cardinality({w \in Workers : holder[w] = "reserved"})

    TypeOK ==
        /\ counter \in 0..(Limit + Cardinality(Workers))
        /\ keyLive \in BOOLEAN
        /\ armed \in BOOLEAN
        /\ holder \in [Workers -> {"idle","reserved","overflow"}]

    \* SAFETY: the gate never admits more than Limit reserved requests at once.
    NoOverAdmission == RealReserved <= Limit
end define;

fair process worker \in Workers
begin
  Loop:
    while TRUE do
      either
        \* ACQUIRE (atomic Lua). Key absent (~keyLive) reads as current=nil, so the
        \* limit check is skipped and we INCR a fresh counter.
        await holder[self] = "idle";
        if keyLive /\ counter >= Limit then
          holder[self] := "overflow";        \* classifying: admitted, no slot
        else
          \* INCR; arm EXPIRE iff new_val == 1, i.e. the pre-increment counter was 0.
          if counter = 0 then armed := TRUE; end if;
          counter := counter + 1 ||
          keyLive := TRUE ||
          holder[self] := "reserved";
        end if;
      or
        \* RELEASE of a reserved slot: guarded DECR (no key-liveness / generation check).
        await holder[self] = "reserved";
        if counter > 0 then
          counter := counter - 1 ||
          holder[self] := "idle";
        else
          holder[self] := "idle";
        end if;
      or
        \* An overflow request completes (held no slot).
        await holder[self] = "overflow";
        holder[self] := "idle";
      end either;
    end while;
end process;

\* Redis key TTL. Current code (BlindTTL): the whole key expires on the timer armed
\* at the first acquire, wiping the counter regardless of live holders.
fair process ttl = "ttl"
begin
  Expire:
    while TRUE do
      await BlindTTL /\ armed /\ keyLive;
      counter := 0 ||
      keyLive := FALSE ||
      armed   := FALSE;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "d08af6d8" /\ chksum(tla) = "2a79f45d")
VARIABLES counter, keyLive, armed, holder

(* define statement *)
RealReserved == Cardinality({w \in Workers : holder[w] = "reserved"})

TypeOK ==
    /\ counter \in 0..(Limit + Cardinality(Workers))
    /\ keyLive \in BOOLEAN
    /\ armed \in BOOLEAN
    /\ holder \in [Workers -> {"idle","reserved","overflow"}]


NoOverAdmission == RealReserved <= Limit


vars == << counter, keyLive, armed, holder >>

ProcSet == (Workers) \cup {"ttl"}

Init == (* Global variables *)
        /\ counter = 0
        /\ keyLive = FALSE
        /\ armed = FALSE
        /\ holder = [w \in Workers |-> "idle"]

worker(self) == \/ /\ holder[self] = "idle"
                   /\ IF keyLive /\ counter >= Limit
                         THEN /\ holder' = [holder EXCEPT ![self] = "overflow"]
                              /\ UNCHANGED << counter, keyLive, armed >>
                         ELSE /\ IF counter = 0
                                    THEN /\ armed' = TRUE
                                    ELSE /\ TRUE
                                         /\ armed' = armed
                              /\ /\ counter' = counter + 1
                                 /\ holder' = [holder EXCEPT ![self] = "reserved"]
                                 /\ keyLive' = TRUE
                \/ /\ holder[self] = "reserved"
                   /\ IF counter > 0
                         THEN /\ /\ counter' = counter - 1
                                 /\ holder' = [holder EXCEPT ![self] = "idle"]
                         ELSE /\ holder' = [holder EXCEPT ![self] = "idle"]
                              /\ UNCHANGED counter
                   /\ UNCHANGED <<keyLive, armed>>
                \/ /\ holder[self] = "overflow"
                   /\ holder' = [holder EXCEPT ![self] = "idle"]
                   /\ UNCHANGED <<counter, keyLive, armed>>

ttl == /\ BlindTTL /\ armed /\ keyLive
       /\ /\ armed' = FALSE
          /\ counter' = 0
          /\ keyLive' = FALSE
       /\ UNCHANGED holder

Next == ttl
           \/ (\E self \in Workers: worker(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Workers : WF_vars(worker(self))
        /\ WF_vars(ttl)

\* END TRANSLATION 
=============================================================================
