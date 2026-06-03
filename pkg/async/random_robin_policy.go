package async

import (
	"fmt"
	"net/url"
	"reflect"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func NewRandomRobinPolicy() pipeline.RequestMergePolicy {
	return &RandomRobinPolicy{}
}

var _ pipeline.RequestMergePolicy = (*RandomRobinPolicy)(nil)

type RandomRobinPolicy struct {
}

func (r *RandomRobinPolicy) MergeRequestChannels(channels []pipeline.RequestChannel, pools map[string]pipeline.PoolConfig) pipeline.PoolDispatch {
	channelsByPool := make(map[string][]pipeline.RequestChannel)
	for _, ch := range channels {
		poolID := ch.PoolID
		if poolID == "" {
			poolID = "default"
		}
		if _, ok := pools[poolID]; !ok {
			panic(fmt.Sprintf("pool %q not found in pools map", poolID))
		}
		channelsByPool[poolID] = append(channelsByPool[poolID], ch)
	}

	dispatch := pipeline.PoolDispatch{
		Channels: make(map[string]chan pipeline.EmbelishedRequestMessage),
	}

	for poolID, poolChs := range channelsByPool {
		mergedChannel := make(chan pipeline.EmbelishedRequestMessage, len(poolChs))
		dispatch.Channels[poolID] = mergedChannel

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

		go func(poolID string, cases []reflect.SelectCase, meta []pipeline.RequestChannel, mergedChannel chan pipeline.EmbelishedRequestMessage) {
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
					pool := pools[poolID]

					requestPath := pool.RequestPathURL
					if ep := ir.PublicRequest.ReqEndpoint(); ep != "" {
						requestPath = ep
					}
					requestURL, _ := url.JoinPath(pool.IGWBaseURL, requestPath)
					headers := map[string]string{
						"Content-Type": "application/json",
					}
					for k, v := range pool.HTTPHeaders {
						headers[k] = v
					}
					if meta[i1].InferenceObjective != "" {
						headers["x-gateway-inference-objective"] = meta[i1].InferenceObjective
					}
					for k, v := range ir.PublicRequest.ReqHeaders() {
						headers[k] = v
					}
					erm := pipeline.EmbelishedRequestMessage{
						InternalRequest: ir,
						HttpHeaders:     headers,
						RequestURL:      requestURL,
					}
					mergedChannel <- erm
				}
			}
		}(poolID, cases, meta, mergedChannel)
	}

	return dispatch
}
