# GCP Pub/Sub Producer Library

Status: accepted on [issue #438](https://github.com/llm-d/llm-d-async/issues/438).
This document records the design locked there so the implementation can be reviewed against it.

## Summary

Add a top-level `producer-gcp/` Go module that implements `api.Producer` against GCP Pub/Sub.
Redis callers keep using `producer/`.
A GCP-only caller never imports `go-redis`.
The processor stamps a dedicated `result_route` attribute on result publications so each producer can attach a filtered result subscription and see only its own results.

## Motivation

The producer library today lives next to the Redis client.
`Producer` is defined in that same module, so any implementation that satisfies it pulls `go-redis` into the caller's build.
The processor already speaks Pub/Sub: it unmarshals a plain `RequestMessage` from message data, copies attributes into metadata, and publishes every result to a single `result_topic_id` with empty attributes.
That last part is the parity gap.
Redis routes results per request via `result_queue_name`.
On Pub/Sub, every producer on a result topic would otherwise see every result.

### Goals

- Ship a Pub/Sub producer that can submit requests and receive only its own results.
- Keep Redis and GCP client libraries in separate modules.
- Idempotently provision the topics and subscriptions the README already requires for the processor, including a DLQ.
- Leave a publish-only identity a way to skip admin calls.

### Non-Goals

- Durable result delivery (`ReceiveResult` / `RenewResult` / `AckResult`).
  Left out of v1; Pub/Sub ack maps onto that later if a caller appears.
- End-to-end cancel.
  The Pub/Sub consume path has no cancellation check, so `CancelRequests` returns `api.ErrNotSupported`.
- Granting DLQ IAM.
  The producer creates the DLQ topic and subscription; operators grant the Pub/Sub service agent.
- Echoing all request metadata onto result publications.
  Only `result_route` is stamped.

## Proposal

1. Move `Producer` (and `ErrNotSupported`) into `api`.
   Keep `DurableResultProducer` and `ResultDelivery` in `producer`.
   Their ownership fields cannot be populated cross-package.
   `producer` keeps `type Producer = api.Producer` so existing Redis callers compile.
2. Add `github.com/llm-d/llm-d-async/producer-gcp` as a top-level module so `make set-version` and the submodule-tag workflow pick it up without Makefile changes.
3. The producer publishes a plain `RequestMessage` as message data and puts caller metadata on attributes, plus `result_route` set to the configured route.
   This is what `pkg/pubsub` already decodes.
   There is no Redis-style `InternalRequest` envelope.
4. `resultWorker` stamps that same `result_route` value as the only result attribute.
   Each producer creates a result subscription filtered on `attributes.result_route = "<route>"`.
5. `NewProducer` (default) idempotently creates the request topic, DLQ topic and subscription, request subscription, result topic, and this producer's result subscription.
   `AlreadyExists` is success after verifying the existing subscription's topic (and, for the result subscription, its filter).
   `WithoutCreateResources` skips provisioning.

## Design Details

### Module layout

| Module | Import | Client libraries |
| --- | --- | --- |
| `api` | `github.com/llm-d/llm-d-async/api` | none |
| `producer` | `github.com/llm-d/llm-d-async/producer` | `go-redis` |
| `producer-gcp` | `github.com/llm-d/llm-d-async/producer-gcp` | `cloud.google.com/go/pubsub/v2` |

### Config

Required: `ProjectID`, `RequestTopicID`, `RequestSubscriptionID`, `ResultTopicID`, `ResultRoute`.
`RequestSubscriptionID` must match the processor's `subscriber_id`.
`ResultRoute` is the filter value and, by default, the result subscription ID.
Optional: `ResultSubscriptionID`, `DeadLetterTopicID` (default `<RequestTopicID>-dlq`), `DeadLetterSubscriptionID` (default `<DeadLetterTopicID>-sub`).

Two producers that share `ResultRoute` and `ResultSubscriptionID` compete on one subscription (Redis-like).
Two producers that share `ResultRoute` but use different subscription IDs each receive a copy.

### Provisioning

On create, the request subscription is configured as the README describes:

- exactly-once delivery
- exponential backoff retry (10s–600s)
- dead-letter topic with `max_delivery_attempts=5`
- ack deadline of 600s, covering one in-flight inference attempt
- expiration policy with no TTL, so the processor subscription never expires

The emulator and `pstest` accept exactly-once but do not honor it.
Tests assert the field is set, not the delivery effect.

The result subscription gets a 7-day inactivity expiration so abandoned producer routes disappear.
The DLQ subscription never expires, so poison messages are not dropped by idle TTL.

`AlreadyExists` handling:

- Topic: treat as success.
- Subscription: `GetSubscription` and fail if `topic` does not match.
- Result subscription: also fail if `filter` does not match.
- Other settings (exactly-once, retry, DLQ, ack deadline) are not mutated on an existing subscription.
  Operators who created the processor subscription by hand keep their values.

### DLQ IAM

Creating a dead-letter policy is not enough in a real project.
The Pub/Sub service agent `service-{project-number}@gcp-sa-pubsub.iam.gserviceaccount.com` must be able to:

- `pubsub.subscriber` (or `pubsub.subscriptions.consume`) on the request subscription
- `pubsub.publisher` on the DLQ topic

`gcloud pubsub subscriptions create --dead-letter-topic=...` grants these.
The producer does not call the IAM API.
A publish-only identity should use `WithoutCreateResources` and leave provisioning to Terraform or `gcloud`.

### Result routing

`api.ResultRouteAttribute` is `"result_route"`.
The producer overwrites that attribute on the request even if the caller put it in metadata, so a client cannot send another producer's results to itself.
The processor copies request attributes into `RequestMessage.Metadata`, and `NewHTTPResult` / `NewErrorResult` copy that onto `ResultMessage.Metadata`.
`resultWorker` reads only `Metadata["result_route"]` and publishes it as a Pub/Sub attribute.
Requests with no route keep today's empty attributes, so existing e2e publishers and unfiltered result subscriptions keep working.

### CancelRequests

Returns `nil` for an empty ID list (a no-op) and `api.ErrNotSupported` otherwise.
There is no cancel topic and no worker-side cancel check on the Pub/Sub path.

### Close and concurrency

`GetResult` is serialized on one producer: Pub/Sub allows a single `Receive` per subscriber client.
`SubmitRequest` may be called concurrently.
`Close` stops the request publisher and, when the producer owns the client, closes it.

## Alternatives

- Nested `producer/gcp/` module.
  Cleaner path, but `make set-version` and the tag workflow only look one level deep for `go.mod`.
  Top-level `producer-gcp/` matches the existing tooling.
- Move `DurableResultProducer` into `api`.
  Rejected: `ResultDelivery`'s claim fields are private and only a same-package implementation can populate them.
- Echo every request attribute onto the result.
  Rejected: a result subscription filter on one dedicated key is enough, and it avoids leaking caller metadata onto the result topic.
- Invent a cancel topic for v1.
  Rejected: the consume path would ignore it, so a producer-side cancel would be a no-op end to end.
- Include durable results in v1.
  Rejected: no in-tree caller yet; Pub/Sub ack/lease maps cleanly onto that interface later.
