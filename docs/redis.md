# Redis requirements

These apply to every Redis the processor uses: the server behind the `redis-sortedset` transport (or the deprecated `redis-pubsub`), and any separate server named in the `address` of a `redis`, `redis-quota` or `redis-leased-rate` gate. Valkey has the same requirements.

## Topology

The transports connect to a standalone server, a Redis Cluster or a Sentinel-managed primary, chosen with `mode` in the transport config (see [Redis Cluster and Sentinel](transports.md#redis-cluster-and-sentinel)). A gate whose `address` differs from the transport's opens its own standalone connection.

## No eviction

The processor treats the presence of a key as state: cancellation and active-request markers, quota counters, payload keys and result lists. An evicted key does not raise an error; it reads as a different answer. A cancelled request gets dispatched, a quota counter restarts at zero, a result is lost.

Set both:

- `maxmemory` to a real limit, below the container's memory limit, leaving room for the fork that AOF rewrites and RDB snapshots use;
- `maxmemory-policy noeviction`.

At the limit, writes then fail with an `OOM` error instead of dropping keys.

## Persistence

Keep AOF on (`appendonly yes`) so queued requests survive a restart. The Bitnami Redis and Valkey charts turn it on in their `commonConfiguration` value. Pass the memory settings through `extraFlags` instead: setting `commonConfiguration` replaces the chart's defaults, AOF included.

## Sizing

A queued request takes about the length of its JSON plus 140 bytes (measured on Redis 7.0 with 10,000 requests of 1 KB and of 4 KB in one queue). A result takes about its JSON length until it is consumed. Size `maxmemory` for the deepest backlog you accept plus the results not yet consumed, then check `used_memory` (`INFO memory`) under a test load.

## Reference install

The Bitnami charts default to the `nano` resources preset, a 192Mi memory limit, which is below a 512mb `maxmemory`. The examples set the container limit to 1Gi, twice `maxmemory`, to leave room for the pages an AOF rewrite copies.

Redis, Bitnami chart:

```bash
helm install redis bitnami/redis -n redis --create-namespace --set auth.enabled=false \
    --set master.resources.requests.memory=1Gi --set master.resources.limits.memory=1Gi \
    --set-json 'master.extraFlags=["--maxmemory 512mb","--maxmemory-policy noeviction"]'
kubectl exec -n redis redis-master-0 -- redis-cli CONFIG GET 'maxmemory*'
```

Valkey, Bitnami chart. The chart prefixes its names with `<release>-valkey` when the release name does not contain `valkey`, so the Service here is `redis-valkey-primary`:

```bash
helm install redis bitnami/valkey -n redis --create-namespace --set auth.enabled=false \
    --set primary.resources.requests.memory=1Gi --set primary.resources.limits.memory=1Gi \
    --set-json 'primary.extraFlags=["--maxmemory 512mb","--maxmemory-policy noeviction"]'
kubectl exec -n redis redis-valkey-primary-0 -- valkey-cli CONFIG GET 'maxmemory*'
```
