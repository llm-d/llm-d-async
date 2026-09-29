# Idempotent Producer Submit and Lookup

## Summary

Add an opt-in producer API that accepts a caller-supplied idempotency key when
submitting a request and can later look up the accepted submission by that key.
The API closes the ambiguity when a caller loses the response after Async has
accepted a request.

## Motivation

### Goals

- Let a caller determine whether a request with a stable idempotency key was
  accepted after a timeout, connection loss, or process crash.
- Return the same accepted request token for repeated equivalent submissions.
- Reject reuse of a key with a different request payload or route.
- Preserve the existing durable result claim, renew, and acknowledgement
  contract.

### Non-Goals

- Exactly-once inference execution.
- Batch Gateway manifests, checkpoints, output construction, or finalization.
- Changing existing `SubmitRequest` callers that do not opt in.
- Cross-route result consumption; result-route opening remains a separate
  producer configuration concern.

## Proposal

Add an additive producer capability, tentatively named
`IdempotentSubmitProducer`, with two operations:

```go
SubmitIdempotent(ctx context.Context, request api.Request, key string) (*Submission, error)
LookupSubmission(ctx context.Context, key string) (*Submission, error)
```

`Submission` is the durable lookup record returned by both operations:

```go
type Submission struct {
  RequestToken  string
  IdentitySHA256 string
  RequestRoute  string
  ResultRoute   string
}
```

The producer scopes `key` by its configured producer namespace. A key is
therefore stable across producer restarts and replacement consumers for that
namespace, but is not shared across independently configured producers.
`SubmitIdempotent` returns the existing `Submission` when the scoped key
identifies an equivalent request. It returns
`ErrIdempotencyConflict` when the scoped key exists with a different canonical
identity. `LookupSubmission` returns `ErrSubmissionNotFound` when no retained
submission exists. Both errors must support `errors.Is`.

## Design Details

The selected producer transport persists an idempotency record atomically with
accepting the request. The record maps the scoped caller key to the accepted
request token, resolved routes, and immutable canonical identity. The canonical
identity is the SHA-256 of deterministic JSON containing:

- the concrete `api.Request` type;
- `ID`, `Created`, `Deadline`, `Payload`, `Metadata`, `Headers`, and `Endpoint`;
- resolved request and result routes after producer defaults are applied; and
- concrete transport fields: `RedisRequest.RequestQueueName`,
  `RedisRequest.ResultQueueName`, or `PubSubRequest.PubSubID`.

Maps must be encoded with sorted keys. The generated request token, lease data,
and producer timestamps are excluded. This lets a caller persist the identity
hash before submit and reject a lookup result that does not match its manifest.
A retry:

1. Creates the record and enqueues the request when the key is new.
2. Returns the recorded submission when the key and canonical identity match.
3. Returns a conflict when the key matches but the identity differs.

The record must survive producer restart through request expiry or terminal
result acknowledgement, and through the durable-result tombstone retention
period after acknowledgement. Cleanup is transport-owned but must be bounded:
the transport records an expiry when the acknowledgement tombstone becomes
eligible for deletion and deletes both records together. A transport may retain
them longer, but never shorter. Lookup after expiry returns
`ErrSubmissionNotFound`; callers must not resubmit that key as recovery.

This proposal does not alter `DurableResultProducer`. A caller uses its accepted
request token with the existing route-local result claim, renew, and ACK flow.

## Alternatives

### Retry submit without an idempotency key

Rejected. A response can be lost after acceptance, so a retry can enqueue a
duplicate request with no safe caller-side reconciliation.

### Persist Gateway-only submission state

Rejected. The producer is the authority on whether it accepted a request; a
Gateway-local record cannot distinguish an unaccepted request from an accepted
request whose response was lost.

### Derive identity from caller payload without lookup

Rejected. Deterministic identity alone does not let the caller retrieve the
accepted producer token after an ambiguous response.