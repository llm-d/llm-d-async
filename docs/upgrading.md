# Upgrade or roll back a single-replica Async Processor

Use this procedure for a patch or an upgrade from the previous release (N−1 to N) of the `llm-d-async` Helm chart. Multiple active processor replicas are outside the supported topology. Even with `replicas: 1`, the default Kubernetes rolling update can start the new Pod while the old one is still consuming work.

## Before changing the release

1. Record the deployed Helm revision, chart and image. Keep the release values in a protected location and check that referenced Secrets and broker storage will be available to both revisions. Read the [release notes](../RELEASE-NOTES.md) between the two versions.
2. Check the [router metrics, headers and CRD versions](upstream-versions.md). Upgrade the router first if the target processor needs a newer contract, and verify the configured gates against the live pool. Use the [supported producer and processor versions](https://github.com/llm-d/llm-d-async/blob/main/docs/compatibility.md#supported-skew) for your transport. Upgrade the processor before enabling a producer feature that writes a new format.
3. Choose a rolling update only when **both versions can read everything written to the shared queues, retries and result routes**. An older reader can discard a message it cannot decode ([#561](https://github.com/llm-d/llm-d-async/issues/561)). For incompatible or unknown formats, or for Redis Pub/Sub, use the drained cutover below. Keep new write formats disabled during the rollout.

Set the names to your installation. This example uses the published v1.0.0 chart as the target for v0.11.0. Prepare `target-values.yaml` for the target release, checking any explicit `ap.image.tag` against the intended image.

```sh
release=llm-d-async
namespace=llm-d-async
deployment=llm-d-async
selector="app.kubernetes.io/name=llm-d-async,app.kubernetes.io/instance=$release"
chart=oci://ghcr.io/llm-d/charts/llm-d-async
target=v1.0.0
values=target-values.yaml

helm history "$release" -n "$namespace"
kubectl get deployment "$deployment" -n "$namespace" -o wide
helm template "$release" "$chart" -n "$namespace" --version "$target" \
  -f "$values" --is-upgrade --show-only templates/ap-deployments.yaml
```

Record the healthy `deployed` revision as `good_revision` for rollback. Compare the rendered image, flags, selector, Secret references and termination grace period with the running Deployment. The current chart grants 130 seconds for termination and defaults `ap.drainTimeout` to two minutes. Leave time for shutdown after draining. Deprecated transport values are translated by the chart and reported in its NOTES.

## What happens to queued work

| Transport | During the handover |
|---|---|
| `redis-sortedset` | Keep the same queue, retry queue and Redis database. A claimed request is fenced by its owner token. An unfinished claim can wait for the remaining lease and the next reclaim scan, even after graceful shutdown. Allow that time in request deadlines. A crash after a destructive pop can lose a request on v0.9.0 and earlier. [v0.9.1](https://github.com/llm-d/llm-d-async/releases/tag/v0.9.1) introduced leased dequeue. [Durable dequeue](guides/durable-dequeue.md) |
| `gcp-pubsub` | Keep the same subscription and result topic. The processor ACKs valid requests after the result is published. NACKs and expired ACK deadlines permit redelivery. Keep the result consumer running and deduplicate redeliveries. On v0.11.0 and v1.0.0, overlapping deliveries of the same message ID to one processor can stall shutdown before `ap.drainTimeout` starts. [Pub/Sub shutdown issue](https://github.com/llm-d/llm-d-async/issues/576) |
| `sql` (experimental) | The new processor applies expand-first migrations before new SQL producers start. A clean handover drains in-flight work on leased partitions. After a crash, work can wait for lease expiry and be delivered again. Both versions must implement the SQL transport and its migration contract. The previous processor then accepts a newer schema. Leave it in place on rollback and check the release notes for exceptions. [SQL migrations](sql-migrations.md) |
| `redis-pubsub` (deprecated) | Pause publishers and finish outstanding requests with the original processor and result subscribers connected. Two subscribed processors can both receive a publication, and publications during a subscriber gap have no replay. Resume publishers only after the replacement is subscribed. |

**Drained cutover.** Pause submissions and let the current processor and consumers finish. Reconcile submitted IDs with terminal results. Confirm that every request queue, delayed retry queue, in-flight claim and result route has drained, including unacknowledged results. Check broker state and the [processor metrics](observability.md). An empty pending queue can still have claimed or buffered work. For polled backlog, require `llm_d_async_async_broker_backlog_source_available == 1` before using its value. On releases without that gauge, inspect the broker directly. If the target uses a different key layout, do this on the source layout before changing it.

Use a maintenance window to exclude overlapping readers. The chart currently exposes no strategy value. If you manage a Deployment overlay, use `spec.strategy.type: Recreate` without a `rollingUpdate` field. Alternatively, after draining, stop the old Pod before either a Helm upgrade or rollback. Verify the example selector matches the live Deployment and pause any external reconciler that would restore replicas during the cutover.

```sh
kubectl scale deployment/"$deployment" -n "$namespace" --replicas=0
kubectl wait -n "$namespace" --for=delete pod -l "$selector" --timeout=5m
```

## Upgrade and verify

```sh
helm upgrade "$release" "$chart" -n "$namespace" --version "$target" \
  -f "$values" --wait --timeout 10m
```

If you scaled down, restore one replica with `kubectl scale deployment/"$deployment" -n "$namespace" --replicas=1`. A ConfigMap-only change to startup settings such as the worker-pool file needs a Pod restart when the Pod template did not change. Apply the same handover policy to that restart. For a rolling update, use `kubectl rollout restart deployment/"$deployment" -n "$namespace"`.

```sh
kubectl rollout status deployment/"$deployment" -n "$namespace" --timeout=10m
kubectl get pods -n "$namespace" -l "$selector" \
  -o 'custom-columns=POD:.metadata.name,IMAGE:.spec.containers[*].image,READY:.status.containerStatuses[*].ready'
helm history "$release" -n "$namespace"
```

Check that only the intended processor image is running and ready. Submit one request with a known ID and read its terminal result from the expected route. Verify the configured gates open with capacity and that the queue drains before resuming normal submissions or enabling new formats.

## Roll back

1. Pause new writes in any format the old version cannot consume. Drain with the **new** processor and compatible consumers still running. Apply these checks to each enabled feature.

   | Feature | Handoff to the old version |
   |---|---|
   | Redis payload references | Disable `WithPayloadKeys` on all producers. Finish queued, delayed and claimed requests that refer to external payloads. Older dispatchers can send a null body. [Transport configuration](transports.md) |
   | Durable Redis results | Keep a durable consumer on its route until pending results are checkpointed and all claims ACKed (or reclaimed and ACKed). Stop that consumer before switching the same route to destructive `GetResult`. When upgrading, roll out enriched result publishers before durable consumers. [Result delivery](guides/durable-result-delivery.md#rollout) |
   | Redis Cluster key layout | Drain requests, retries, claims and outstanding results on the active backend before switching clients or key mode. Cluster and standalone/Sentinel use different private claim keys. Move producers, processor and consumers together. [Cluster cutover](transports.md#redis-cluster-and-sentinel) |

2. If mixed-version reads are unsafe, use the scale-to-zero steps above before restoring the old revision. This also covers a saved revision that used the default rolling strategy. Verify the saved revision's image, Secrets and broker settings. Roll back to the recorded revision (set `good_revision` to its numeric value from `helm history`).

   ```sh
   helm rollback "$release" "$good_revision" -n "$namespace" --wait --timeout 10m
   ```

3. If you scaled down, restore one replica with `kubectl scale deployment/"$deployment" -n "$namespace" --replicas=1`. Repeat the rollout, image, gate and end-to-end request checks before resuming submissions. Roll back the router only after verifying that the restored processor can use its metrics and headers.

For installations still using the old `async-processor` chart, the [chart rename](https://github.com/llm-d/llm-d-async/pull/340) changed the immutable Deployment selector. Plan a drained uninstall and reinstall, retain broker data and external Secrets, and keep the old chart and values for recovery. Helm revision rollback applies within a release that still has its history.

Cross-component rollout work is tracked in [llm-d#3073](https://github.com/llm-d/llm-d/issues/3073).
