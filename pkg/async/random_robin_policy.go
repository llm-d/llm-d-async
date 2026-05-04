package async

import (
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

func (r *RandomRobinPolicy) MergeRequestChannels(channels []pipeline.RequestChannel) pipeline.EmbelishedRequestChannel {
	mergedChannel := make(chan pipeline.EmbelishedRequestMessage, len(channels))

	// reflect.Select blocks forever on an empty cases slice, so return
	// a closed channel immediately to avoid goroutine leaks.
	if len(channels) == 0 {
		close(mergedChannel)
		return pipeline.EmbelishedRequestChannel{Channel: mergedChannel}
	}

	cases := make([]reflect.SelectCase, len(channels)) //nolint:staticcheck
	meta := make([]pipeline.RequestChannel, len(channels))
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch.Channel)}
		meta[i] = ch
	}

	go func() {
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
				requestURL := meta[i1].IGWBaseURl + meta[i1].RequestPathURL
				if ep := ir.PublicRequest.ReqEndpoint(); ep != "" {
					requestURL = meta[i1].IGWBaseURl + ep
				}
				erm := pipeline.EmbelishedRequestMessage{
					InternalRequest: ir,
					HttpHeaders: map[string]string{
						"Content-Type":                  "application/json",
						"x-gateway-inference-objective": meta[i1].InferenceObjective,
					},
					RequestURL: requestURL,
				}
				mergedChannel <- erm
			}

		}
	}()

	return pipeline.EmbelishedRequestChannel{
		Channel: mergedChannel,
	}
}
