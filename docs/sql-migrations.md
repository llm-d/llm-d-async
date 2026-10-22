# SQL Transport Schema Migrations

The `sql` transport's Postgres schema is owned by the `migrate` package
(`producer-sql/sqlqueue/migrate`). The processor and `producer-sql` never run
DDL. On startup they read the `async_schema_migrations` table and refuse to
start if it is missing or behind the version they were built with.

## How the schema gets applied

The processor binary has a `migrate` subcommand:

```bash
llm-d-async migrate                                          # URL from SQL_URL
llm-d-async migrate --postgresql-url 'postgres://user:pass@host:5432/async'
```

The Helm chart renders it as an initContainer on the processor Deployment when
`ap.transport` is `sql`, using the processor's own image and the same `SQL_URL`
Secret, so `helm upgrade` migrates the database before the new processor
starts. Nothing else is needed to upgrade.

A run takes a transaction-scoped advisory lock before it does anything else,
creates `async_schema_migrations` if needed, applies every migration newer than
the recorded version, and commits. Concurrent runs (several replicas starting
at once, or several pods in a rollout) queue on the lock; the first one applies
the migrations and the rest find nothing to do. A failed migration rolls back
completely, so the database is never left half-migrated.

A processor whose database is newer than it expects logs that and keeps
running, so a rollback after a migration works. `producer-sql` applies the same
check, so an application that submits through it needs the database migrated
before it starts: run the subcommand from its deploy, or start the processor
first.

Upgrading a database created before migrations existed needs nothing special.
Migration `0001` uses `IF NOT EXISTS` and `CREATE OR REPLACE`, so it adopts
the existing tables, drops the old `async_schema` version table, and records
version 1.

## Draining

Upgrades do not drain the queues. Requests stay in `async_requests` across a
rollout, and an old processor that is still running keeps dispatching against
the migrated schema, because every migration must stay compatible with the
previous release (see below). A release note calls out the rare release that
needs a drain.

## Writing a migration

1. Add `producer-sql/sqlqueue/migrate/migrations/NNNN_short_name.sql`. `NNNN`
   is the next number in sequence, zero-padded to four digits. The name is
   lowercase letters, digits, and underscores. `TestLoad` fails on gaps,
   duplicates, or misnamed files.
2. Keep it transactional. The whole run happens in one transaction, so
   `CREATE INDEX CONCURRENTLY` and other non-transactional statements are not
   allowed.
3. Keep it compatible with the previous release. Rolling updates run the new
   pods' migration while old pods are still serving, and a rollback runs old
   binaries against the new schema. Expand first (add columns with defaults,
   add tables, add functions under a new name), and contract (drop, rename,
   change a function's signature or meaning, tighten constraints) only in a
   later release once nothing reads the old shape. Queries in `sqlqueue` name
   their columns, so an added column does not break an older build.
4. Mind the queue tables' locks. `ALTER TABLE` on `async_requests` or
   `async_results` takes an exclusive lock that waits behind every in-flight
   dispatch and blocks the ones after it. Prefer statements that only touch the
   catalog (adding a nullable column or one with a constant default) over ones
   that rewrite or scan the table.
5. Write plain DDL, no `IF NOT EXISTS` guards. Migration `0001` uses them only
   to adopt databases created before versioning existed. From `0002` on, the
   version table is authoritative and a statement that finds its object already
   present is a bug worth failing on.
6. Bump nothing else. The expected version is the number of embedded files.

## Running locally

Point `TEST_POSTGRES_URL` at a disposable Postgres and run the sql tests:

```bash
TEST_POSTGRES_URL='postgres://postgres@localhost:5432/asyncq_test?sslmode=disable' make test-sql
```

Without the variable the database-backed tests skip.
