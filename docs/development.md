# Development


A setup based on a KIND cluster with a Redis server for MQ is provided.
In order to deploy everything run:

```bash
make deploy-ap-emulated-on-kind
```

Then, in a new terminal window register a subscriber:

```bash
kubectl exec -n redis redis-master-0 -- redis-cli SUBSCRIBE result-queue
```

Publish a message for async processing (uses the [internal wire format](transports.md#internal-wire-format) since this bypasses the producer):

```bash
kubectl exec -n redis redis-master-0 -- redis-cli PUBLISH request-queue '{"request_kind":"plain","data":{"id":"testmsg","created":1764044000,"deadline":9999999999,"payload":{"model":"unsloth/Meta-Llama-3.1-8B","prompt":"hi"}}}'
```
