package async

import (
	"reflect"

	"github.com/llm-d-incubation/llm-d-async/api"
)

func NewRandomRobinPolicy() api.RequestMergePolicy {
	return &RandomRobinPolicy{}
}

var _ api.RequestMergePolicy = (*RandomRobinPolicy)(nil)

type RandomRobinPolicy struct {
}

func (r *RandomRobinPolicy) MergeRequestChannels(channels []api.RequestChannel) api.EmbelishedRequestChannel {
	mergedChannel := make(chan api.EmbelishedRequestMessage, len(channels))

	// reflect.Select blocks forever on an empty cases slice, so return
	// a closed channel immediately to avoid goroutine leaks.
	if len(channels) == 0 {
		close(mergedChannel)
		return api.EmbelishedRequestChannel{Channel: mergedChannel}
	}

	cases := make([]reflect.SelectCase, len(channels)) //nolint:staticcheck
	meta := make([]api.RequestChannel, len(channels))
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
				erm := api.EmbelishedRequestMessage{
					InternalRequest: ir,
					HttpHeaders: map[string]string{
						"Content-Type":                  "application/json",
						"x-gateway-inference-objective": meta[i1].InferenceObjective,
					},
					RequestURL: meta[i1].IGWBaseURl + meta[i1].RequestPathURL,
				}
				mergedChannel <- erm
			}

		}
	}()

	return api.EmbelishedRequestChannel{
		Channel: mergedChannel,
	}
}
