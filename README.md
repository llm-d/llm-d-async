# Async Processor (AP) - User Guide

## Overview
**The Problem:** High-performance accelerators often suffer from low utilization in strictly online serving scenarios, or users may need to mix latency-insensitive workloads into slack capacity without impacting primary online serving.

**The Value:** This component enables efficient processing of requests where latency is not the primary constraint (i.e., the magnitude of the required SLO is ≥ minutes). <br>
By utilizing an asynchronous, queue-based approach, users can perform tasks such as product classification, bulk summarizations, summarizing forum discussion threads, or performing near-realtime sentiment analysis over large groups of social media tweets without blocking real-time traffic.

**Architecture Summary:** The Async Processor is a composable component that provides services for managing these requests. It functions as an asynchronous worker that pulls jobs from a message queue and dispatches them to an inference gateway, decoupling job submission from immediate execution.

## When to Use
• **Latency Insensitivity:** Suitable for workloads where immediate response is not required.

• **Capacity Optimization:** Useful for filling "slack" capacity in your inference pool.


## Design Principles

The architecture adheres to the following core principles:

1. **Bring Your Own Queue (BYOQ):** All aspects of prioritization, routing, retries, and scaling are decoupled from the message queue implementation. 

2. **Composability:** The end-user does not interact directly with the processor via an API. Instead, the processor interacts solely with the message queues, making it highly composable with offline batch processing and asynchronous workflows.

3. **Resilience by Design:** If real-time traffic spikes or errors occur, the system triggers intelligent retries for jobs, ensuring they eventually complete without manual intervention.


## Table of Contents

- [Overview](#overview)
- [When to use](#when-to-use)
- [Design Principles](#design-principles)
- [Deployment](#deployment)
- [Command line parameters](#command-line-parameters)
- [Request Messages and Consusmption](#request-messages-and-consomption)
    - [Request Merge Policy](#request-merge-policy)
- [Retries](#retries)
- [Results](#results)   
- [Implementations](#implementations)
    - [Redis Channels](#redis-channels)
      - [Redis Command line parameters](#redis-command-line-parameters)
    - [GCP Pub/Sub](#gcp-pub-sub)
- [Development](#development)


## Deployment

To deploy the Async Processor into your K8S cluster, follow these steps:
- Create an `.env` file with `export` statements overrides. E.g.:
```bash
IMAGE_TAG_BASE=<if needed to override for a private registry>
DEPLOY_LLM_D=false
DEPLOY_REDIS=false
DEPLOY_PROMETHEUS=false
AP_IMAGE_PULL_POLICY=Always
```
- Run: 
```bash 
make deploy-ap-on-k8s
```
- To test a request (only for the Redis implementation):
    - Subscribing to the result channel (different terminal window):
    ```bash
       export REDIS_IP=....
       kubectl run -i -t subscriberbox --rm --image=redis --restart=Never -- /usr/local/bin/redis-cli -h $REDIS_IP SUBSCRIBE result-queue
    ```
    - Publishing a request:
    ```bash
       export REDIS_IP=....
       kubectl run --rm -i -t publishmsgbox --image=redis --restart=Never -- /usr/local/bin/redis-cli -h $REDIS_IP PUBLISH request-queue '{"id" : "testmsg", "payload":{ "model":"food-review-1", "prompt":"Hi, good morning "}, "deadline" :"23472348233323" }'
     ```

## Command line parameters

- `concurrency`: the number of concurrenct workers, default is 8.
- `request-merge-policy`: Currently only supporting <u>random-robin</u> policy.
- `message-queue-impl`: Implementation of the queueing system. Options are <u>gcp-pubsub</u> for GCP PubSub and  <u>redis-pubsub</u> for ephemeral Redis-based implementation.

<i>additional parameters may be specified for concrete message queue implementations</i>


## Request Messages and Consusmption

The async processor expects request messages to have the following format:
```json
{
    "id" : "unique identifier for result mapping",
    "deadline" : "deadline in Unix seconds",
    "payload" : {regular inference payload as a byte array}
}
```

Example:
```json
{
    "id" : "19933123533434",
    "deadline" : "1764045130",
    "payload": byte[]({"model":"food-review","prompt":"hi", "max_tokens":10,"temperature":0})
}
```

### Request Merge Policy

The Async Processor supports multiple request message queues. A `Request Merge Policy` can be specified to define the merge strategy of messages from the different queues.

Currently the only policy supported is `Random Robin Policy` which randomly picks messages from the queues.

## Retries

When a message processing has failed, either shedded or due to a server-side error, it will be scheduled for a retry (assuming the deadline has not passed).

The async processor supports exponential-backoff and fixed-rate backoff (TBD).

## Results

Results will be written to the results queue and will have the following structure:

```json
{
    "id" : "id mapped to the request",
    "payload" : byte[]{/*inference result payload*/} ,
    // or
    "error" : "error's reason"
}
```

## Implementations

### Redis Channels

An example implementation based on Redis channels is provided.

- Redis Channels as the request queues.
- Redis Sorted Set as the retry exponential backoff implementation.
- Redis Channel as the result queue.


![Async Processor - Redis architecture](/docs/images/redis_pubsub_architecture.png "AP - Redis")

#### Redis Command line parameters

- `redis.inference-gateway`: Inference gateway endppoint. Requests will be sent to this endpoint.
- `redis.inference-objective`: InferenceObjective to use for requests (set as the HTTP header x-gateway-inference-objective if not empty). 
- `redis.request-queue-name`: The name of the channel for the requests. Default is <u>request-queue</u>.
- `redis.retry-queue-name`: The name of the channel for the retries. Default is <u>retry-sortedset</u>.
- `redis.result-queue-name`: The name of the channel for the results. Default is <u>result-queue</u>.

**NOTE:** the `redis.inference-gateway` and `redis.inference-objective` will soon migrate to a per request queue definitions so an index number will be added to the flag name.

### GCP Pub/Sub

The GCP PubSub implementation requires the user to configure the following:

- Requests Topic and a **Subscription** having the following configurations:
    - Exactly once delivery.
    - Retries with exponential backoff.
    - Dead Letter Queue (DLQ).
- Results Topic.

<u>Note:</u> If DLQ is NOT configured for the request topic. Retried messages will be counted multiple times in the #_of_requests metric.

![Async Processor - GCP PubSub Architecture](/docs/images/gcp_pubsub_architecture.png "AP - GCP PubSub") 

#### GCP PubSub Command line parameters

- `pubsub.project-id`: The name GCP project ID using the PubSub API.
- `pubsub.inference-gateway`: Inference gateway endppoint. Requests will be sent to this endpoint.
- `pubsub.inference-objective`: InferenceObjective to use for requests (set as the HTTP header x-gateway-inference-objective if not empty). 
- `pubsub.request-subscriber-id`: The subscriber ID for the requests topic.
- `pubsub.result-topic-id`: The results topic ID.

**NOTE:** the `pubsub.inference-gateway` and `pubsub.inference-objective` will soon migrate to a per request queue definitions so an index number will be added to the flag name.

## Development

A setup based on a KIND cluster with a Redis server for MQ is provided.
In order to deploy everything run:
```bash
make deploy-ap-emulated-on-kind
```

Then, in a new terminal window register a subscriber:

```bash
kubectl exec -n redis redis-master-0 -- redis-cli SUBSCRIBE result-queue
```

Publish a message for async processing:
```bash
kubectl exec -n redis redis-master-0 -- redis-cli PUBLISH request-queue '{"id" : "testmsg", "payload":{ "model":"unsloth/Meta-Llama-3.1-8B", "prompt":"hi"}, "deadline" :"9999999999" }'
```

