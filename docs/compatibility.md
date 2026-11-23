# Producer and processor compatibility

The producer libraries (`github.com/llm-d/llm-d-async/producer` for the Redis
transports, `github.com/llm-d/llm-d-async/producer-sql` for the `sql` transport)
and the Async Processor binary are released together from one commit, but they
don't always run at the same version: the library is compiled into your
application, the processor is a deployment. This page states which combinations work and what contributors
must keep true so that they stay working.

## What couples them

The library and the processor meet in the transport, not in Go. The coupling is:

- the request envelope and result JSON (`api.RequestMessage`,
  `api.ResultMessage` and their internal routing fields) written to and read
  from Redis;
- the Redis key layout (request sorted set, result list, active-token,
  cancellation, claim and payload keys) and the Lua scripts that act on it;
- for the `sql` transport, the Postgres schema, versioned by the processor's
  migrations ([SQL Transport Schema Migrations](sql-migrations.md));
- the `api` module the library is built against.

The Go surface of the library follows the module promise in
[CONTRIBUTING](../CONTRIBUTING.md#versioning-and-compatibility) and is not
repeated here. Applications that publish to GCP Pub/Sub with their own code are
bound by the same envelope rules; there is no Pub/Sub producer library yet.

## Supported skew

### Redis transports

**Older producer, newer processor.** The processor accepts requests from every
`producer` release since v0.10.0 and writes results they can read. There
is no window: support continues until a release deliberately drops it. Dropping
support for a producer release is a breaking change. It needs a major version
bump of the processor, a release note naming the oldest producer still accepted,
and a deprecation warning in the processor log for at least two minor releases
before the drop. Releases before v0.10.0 probably work but are not tested.

**Newer producer, older processor.** Works with the library's default options
against any processor the promise above covers. A library option that needs a
newer processor is opt-in, off by default, and its documentation names the first
processor release that honors it. Flipping such a default is a breaking change
under the same rule as above.

### SQL transport

The schema version gates skew here. The processor applies migrations when it
starts and accepts a schema newer than it was built with. `producer-sql` never
runs DDL and refuses to start if the schema is behind the version it was built
with, so an application that submits through it needs a processor of the same
release or newer to have started against the database first. The other
direction, an older `producer-sql` against a newer schema, is kept working by
the expand-first migration rules in
[SQL Transport Schema Migrations](sql-migrations.md). Roll out the processor
before the application.

### Library versions

Keep `api`, `producer` and `producer-sql` at the same tag in your `go.mod`.
They are released from one commit, and mixing them is untested.

## Rules for changes to the wire format

1. Additive only. A new JSON field is optional, uses `omitempty`, and its zero
   value means the behavior before the change. Never rename, remove or repurpose
   a field within a major version.
2. Both sides ignore unknown fields. Do not enable strict decoding on transport
   messages.
3. A new producer capability that depends on a processor change is opt-in and
   off by default.
4. Key-layout and Lua script changes keep reading what earlier releases wrote,
   or ship a migration path and a release note. This is where a silent break
   would come from, so it gets the closest review. Schema changes for the
   `sql` transport follow the expand-first rules in
   [SQL Transport Schema Migrations](sql-migrations.md).
5. Record every wire-format change in the PR's release note.

## How it is enforced

The nightly e2e suite (`test/e2e/e2e_producer_compat_test.go`) builds the
producer pinned in `test/e2e/oldproducer/go.mod` and runs it against the current
processor, then runs the current producer against the processor image in
`OLD_AP_IMAGE`. Both pins are v0.10.0, the floor of the promise, and stay there
until a release deliberately raises them. Because wire changes are additive only,
a floor that passes implies every release between the floor and the current one
passes too.

The `sql` transport has no cross-release e2e yet; its promise rests on the
schema version gate and the migration rules above.
