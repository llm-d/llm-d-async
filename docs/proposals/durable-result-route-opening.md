# Durable Result Route Opening

## Summary

Add an opt-in producer capability that opens a durable result consumer for a
previously resolved result route. A replacement Batch Gateway processor can then
continue receiving results from the route accepted during the original
submission.

## Motivation

### Goals

- Let a replacement consumer reopen the exact result route returned by an
  accepted submission.
- Preserve the existing durable claim, renew, and acknowledgement behavior.
- Keep result-route recovery limited to the producer transport that owns the
  route.

### Non-Goals

- Idempotent request submission or submission lookup. Those are proposed
  separately in #471.
- Allowing arbitrary result-route discovery or enumeration.
- Changing existing `DurableResultProducer` callers that use their configured
  result route.
- Batch Gateway manifest, checkpoint, output construction, or finalization
  behavior.

## Proposal

Add an additive producer capability, tentatively named `ResultRouteOpener`:

```go
type ResultRouteOpener interface {
  OpenResultRoute(resultRoute string) (DurableResultProducer, error)
}
```

`OpenResultRoute` accepts only a non-empty route previously returned by the
same producer's accepted submission record. It returns a durable consumer bound
to that exact route. The returned consumer uses the existing `ReceiveResult`,
`RenewResult`, `AckResult`, and `ResultDeliveryConfig` contract unchanged.

The method returns an error when the route is empty, is not valid for the
producer transport, or cannot be opened. Errors for invalid routes must support
`errors.Is`.

## Design Details

For Milestone 1, `RedisSortedSetProducer` is the selected implementation. It
opens a route using the producer's existing Redis connection and the requested
result-list key. It must preserve the durable result claim namespace, lease TTL,
and reclaim interval used by the original producer. The returned consumer shares
the existing client's lifetime; opening a route does not create an independently
managed Redis connection.

Batch Gateway persists `Submission.ResultRoute` from the idempotent-submit and
lookup contract before dispatching recovery work. After a replacement starts, it
opens that stored route and consumes only results carrying the persisted request
token and stable request identity. It checkpoints a result before acknowledging
it. A result that does not match the persisted attempt remains unacknowledged.

This proposal deliberately does not add a way to discover routes. The Gateway
uses the exact route returned by the same producer's accepted submission. The
transport remains responsible for validating that route and for preserving the
existing durable-delivery isolation rules.

## Alternatives

### Use a replacement producer's default result route

Rejected. The original request may have used a producer-resolved or
message-specific route. A replacement default can therefore consume nothing or
the wrong route.

### Add route opening to idempotent submission lookup

Rejected. Submission lookup identifies an accepted request. Route opening is a
separate durable-consumer capability with its own ownership and lifecycle
requirements.

### Let Batch Gateway read Redis directly

Rejected. Async owns result claiming, lease renewal, acknowledgement, and
transport-specific isolation. Bypassing it would duplicate and weaken that
contract.