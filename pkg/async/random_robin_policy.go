package async

import (
	"reflect"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func init() {
	pipeline.RegisterMergePolicy("random-robin", func(_ pipeline.MergePolicyDeps) (pipeline.RequestMergePolicy, error) {
		return NewRandomRobinPolicy(), nil
	})
}

func NewRandomRobinPolicy() pipeline.RequestMergePolicy {
	return &RandomRobinPolicy{}
}

var _ pipeline.RequestMergePolicy = (*RandomRobinPolicy)(nil)

// RandomRobinPolicy fans subscription channels into per-pool merged
// channels via reflect.Select. Within a pool, sources interleave by
// the Go runtime's randomized select tie-breaking.
type RandomRobinPolicy struct{}

func (r *RandomRobinPolicy) MergeRequestChannels(channels []pipeline.RequestChannel) pipeline.PoolDispatch {
	poolToInputs := map[string][]pipeline.RequestChannel{}
	for _, ch := range channels {
		poolToInputs[ch.PoolID] = append(poolToInputs[ch.PoolID], ch)
	}

	out := make(map[string]chan pipeline.EmbelishedRequestMessage, len(poolToInputs))
	for poolID, inputs := range poolToInputs {
		bufSize := len(inputs) * 100
		if bufSize < 256 {
			bufSize = 256
		}
		out[poolID] = make(chan pipeline.EmbelishedRequestMessage, bufSize)
	}

	for poolID, inputs := range poolToInputs {
		go runPoolMerge(poolID, inputs, out[poolID])
	}

	return pipeline.PoolDispatch{Channels: out}
}

// runPoolMerge drains all input channels for a single pool and forwards
// to the pool's output channel. Messages arrive fully embellished (with
// subscription-gate releases attached) from the Flow's pull callback.
func runPoolMerge(poolID string, channels []pipeline.RequestChannel, out chan<- pipeline.EmbelishedRequestMessage) {
	if len(channels) == 0 {
		close(out)
		return
	}
	cases := make([]reflect.SelectCase, len(channels))
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch.Channel)}
	}

	for {
		i1, val, ok := reflect.Select(cases)
		if !ok {
			cases = append(cases[:i1], cases[i1+1:]...)
			if len(cases) == 0 {
				close(out)
				return
			}
			continue
		}
		emb, ok := val.Interface().(*pipeline.EmbelishedRequestMessage)
		if !ok || emb == nil {
			continue
		}
		out <- *emb
	}
}
