package async

import (
	"fmt"
	"net/url"
	"reflect"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
)

func NewRandomRobinPolicy() pipeline.RequestMergePolicy {
	return &RandomRobinPolicy{}
}

var _ pipeline.RequestMergePolicy = (*RandomRobinPolicy)(nil)

type RandomRobinPolicy struct {
}

func (r *RandomRobinPolicy) MergeRequestChannels(channels []pipeline.RequestChannel, pools map[string]pipeline.WorkerPoolConfig) pipeline.PoolDispatch {
	channelsByPool := make(map[string][]pipeline.RequestChannel)
	for _, ch := range channels {
		workerPoolID := ch.WorkerPoolID
		if workerPoolID == "" {
			workerPoolID = "default"
		}
		if _, ok := pools[workerPoolID]; !ok {
			panic(fmt.Sprintf("worker pool %q not found in pools map", workerPoolID))
		}
		channelsByPool[workerPoolID] = append(channelsByPool[workerPoolID], ch)
	}

	dispatch := pipeline.PoolDispatch{
		Channels: make(map[string]chan pipeline.EmbelishedRequestMessage),
	}

	for workerPoolID, poolChs := range channelsByPool {
		mergedChannel := make(chan pipeline.EmbelishedRequestMessage, len(poolChs))
		dispatch.Channels[workerPoolID] = mergedChannel

		if len(poolChs) == 0 {
			close(mergedChannel)
			continue
		}

		cases := make([]reflect.SelectCase, len(poolChs))
		meta := make([]pipeline.RequestChannel, len(poolChs))
		for i, ch := range poolChs {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch.Channel)}
			meta[i] = ch
		}

		go func(workerPoolID string, cases []reflect.SelectCase, meta []pipeline.RequestChannel, mergedChannel chan pipeline.EmbelishedRequestMessage) {
			for {
				i1, val, ok := reflect.Select(cases)
				if !ok {
					// one of the channels is closed, remove it
					cases = append(cases[:i1], cases[i1+1:]...)
					meta = append(meta[:i1], meta[i1+1:]...)
					if len(cases) == 0 {
						close(mergedChannel)
						break
					}
				} else {
					ir, ok := val.Interface().(*api.InternalRequest)
					if !ok || ir == nil {
						continue
					}
					chMeta := meta[i1]

					requestPath := chMeta.RequestPathURL
					if ep := ir.PublicRequest.ReqEndpoint(); ep != "" {
						requestPath = ep
					}
					requestURL, _ := url.JoinPath(chMeta.IGWBaseURL, requestPath)
					headers := map[string]string{
						"Content-Type": "application/json",
					}
					if chMeta.InferenceObjective != "" {
						headers["x-gateway-inference-objective"] = chMeta.InferenceObjective
					}
					for k, v := range ir.PublicRequest.ReqHeaders() {
						headers[k] = v
					}
					erm := pipeline.EmbelishedRequestMessage{
						InternalRequest: ir,
						HttpHeaders:     headers,
						RequestURL:      requestURL,
						WorkerPoolID:    workerPoolID,
					}
					metrics.IncQueueDepth(ir.QueueID, ir.RequestQueueName, workerPoolID)
					mergedChannel <- erm
				}
			}
		}(workerPoolID, cases, meta, mergedChannel)
	}

	return dispatch
}
