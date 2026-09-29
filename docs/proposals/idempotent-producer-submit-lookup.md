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
- Changing existing `Submit` callers that do not opt in.
- Cross-route result consumption; result-route opening remains a separate
  producer configuration concern.

## Proposal

Add an additive producer capability, tentatively named
`IdempotentSubmitProducer`, with two operations:

```go
SubmitIdempotent(ctx context.Context, request *RequestMessage, key string) (*Submission, error)
LookupSubmission(ctx context.Context, key string) (*Submission, error)
```

`Submission` contains the accepted request token and the immutable request
identity needed by a caller to reconcile the submission. `SubmitIdempotent`
returns the existing submission when `key` identifies an equivalent request.
It returns a conflict when the key identifies a different payload, request
route, result route, or deadline. `LookupSubmission` returns `ErrNotFound` when
no accepted submission exists for the key.

The exact exported names, error types, and request identity fields are subject
to producer-maintainer review. The required behavior is stable-key submission,
equivalence validation, and lookup of the accepted request token.

## Design Details

The selected producer transport persists an idempotency record atomically with
accepting the request. The record maps the caller key to the accepted request
token and an immutable canonical identity of the submitted request. A retry:

1. Creates the record and enqueues the request when the key is new.
2. Returns the recorded submission when the key and canonical identity match.
3. Returns a conflict when the key matches but the identity differs.

The record must survive producer restart for at least as long as the request can
be recovered or its terminal result can be redelivered. Its retention and
cleanup policy are transport-owned and must be documented with the API.

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