package tierpriority

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/plugins"
)

func init() {
	plugins.MustRegister("tier-priority", func(name string, parameters json.RawMessage, handle plugins.Handle) (plugins.Plugin, error) {
		var params struct {
			PriorityHeader string `json:"priority_header"`
			TierLabel      string `json:"tier_label"`
		}
		if len(parameters) > 0 {
			if err := json.Unmarshal(parameters, &params); err != nil {
				return nil, fmt.Errorf("failed to parse tier-priority parameters: %w", err)
			}
		}
		if params.PriorityHeader == "" {
			params.PriorityHeader = "x-gateway-priority"
		}
		if params.TierLabel == "" {
			params.TierLabel = "tier"
		}
		return NewTierPriorityPolicy(name, params.PriorityHeader, params.TierLabel), nil
	})
}

func NewTierPriorityPolicy(name string, priorityHeader string, tierLabel string) *TierPriorityPolicy {
	if tierLabel == "" {
		tierLabel = "tier"
	}
	return &TierPriorityPolicy{
		name:           name,
		priorityHeader: priorityHeader,
		tierLabel:      tierLabel,
	}
}

var _ pipeline.RequestMergePolicy = (*TierPriorityPolicy)(nil)
var _ plugins.Plugin = (*TierPriorityPolicy)(nil)

type TierPriorityPolicy struct {
	name           string
	priorityHeader string
	tierLabel      string
}

func (p *TierPriorityPolicy) TypedName() plugins.TypedName {
	return plugins.TypedName{
		Type: "tier-priority",
		Name: p.name,
	}
}

type msgAndMeta struct {
	ir     *api.InternalRequest
	chMeta pipeline.RequestChannel
}

type bucket struct {
	queues  map[string][]msgAndMeta
	keys    []string
	nextIdx int
	size    int
}

func newBucket() *bucket {
	return &bucket{
		queues: make(map[string][]msgAndMeta),
	}
}

func (b *bucket) push(key string, mm msgAndMeta) {
	q, exists := b.queues[key]
	if !exists {
		b.keys = append(b.keys, key)
	}
	b.queues[key] = append(q, mm)
	b.size++
}

func (b *bucket) pop() (msgAndMeta, bool) {
	if len(b.keys) == 0 {
		return msgAndMeta{}, false
	}

	key := b.keys[b.nextIdx]
	q := b.queues[key]
	mm := q[0]
	b.queues[key] = q[1:]
	b.size--

	if len(b.queues[key]) == 0 {
		b.keys = append(b.keys[:b.nextIdx], b.keys[b.nextIdx+1:]...)
		delete(b.queues, key)
		if len(b.keys) > 0 {
			b.nextIdx = b.nextIdx % len(b.keys)
		} else {
			b.nextIdx = 0
		}
	} else {
		b.nextIdx = (b.nextIdx + 1) % len(b.keys)
	}

	return mm, true
}

type scheduler struct {
	mu            sync.Mutex
	cond          *sync.Cond
	totalBuffered int
	maxBuffered   int
	tierLabel     string
	closed        bool
	activeReaders int
	buckets       [6]*bucket
}

func newScheduler(maxBuffered int, activeReaders int, tierLabel string) *scheduler {
	s := &scheduler{
		maxBuffered:   maxBuffered,
		activeReaders: activeReaders,
		tierLabel:     tierLabel,
	}
	s.cond = sync.NewCond(&s.mu)
	for i := range s.buckets {
		s.buckets[i] = newBucket()
	}
	return s
}

func getPriorityIndex(ir *api.InternalRequest, tierLabel string) int {
	tierPri := 2 // default to batch (lowest priority tier)
	if ir.Labels != nil {
		switch ir.Labels[tierLabel] {
		case string(api.TierInteractive):
			tierPri = 0
		case string(api.TierAsync):
			tierPri = 1
		case string(api.TierBatch):
			tierPri = 2
		}
	}

	classPri := 1 // default to overflow
	if ir.GetClassification() == api.ClassificationReserved {
		classPri = 0
	}

	return classPri*3 + tierPri
}

func (s *scheduler) Push(ir *api.InternalRequest, chMeta pipeline.RequestChannel) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	pri := getPriorityIndex(ir, s.tierLabel)

	// Backpressure: block only if this specific bucket is full
	for s.buckets[pri].size >= s.maxBuffered && !s.closed {
		s.cond.Wait()
	}

	if s.closed {
		return false
	}

	key := fmt.Sprintf("%p", chMeta.Channel)
	s.buckets[pri].push(key, msgAndMeta{ir: ir, chMeta: chMeta})
	s.totalBuffered++
	s.cond.Broadcast()
	return true
}

func (s *scheduler) Pop() (msgAndMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.totalBuffered == 0 {
		if s.activeReaders == 0 || s.closed {
			return msgAndMeta{}, false
		}
		s.cond.Wait()
	}

	for _, b := range s.buckets {
		if mm, ok := b.pop(); ok {
			s.totalBuffered--
			s.cond.Broadcast()
			return mm, true
		}
	}

	return msgAndMeta{}, false
}

func (s *scheduler) DecrementReaders() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeReaders--
	s.cond.Broadcast()
}

func (s *scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.cond.Broadcast()
}

func (p *TierPriorityPolicy) MergeRequestChannels(channels []pipeline.RequestChannel, pools map[string]pipeline.WorkerPoolConfig) pipeline.PoolDispatch {
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

		s := newScheduler(1000, len(poolChs), p.tierLabel)

		for _, ch := range poolChs {
			go func(ch pipeline.RequestChannel, s *scheduler) {
				defer s.DecrementReaders()
				for {
					val, ok := <-ch.Channel
					if !ok {
						break
					}
					if val == nil {
						continue
					}
					if !s.Push(val, ch) {
						break
					}
				}
			}(ch, s)
		}

		go func(workerPoolID string, s *scheduler, mergedChannel chan pipeline.EmbelishedRequestMessage, priorityHeader string, tierLabel string) {
			defer close(mergedChannel)
			defer s.Close()
			for {
				mm, ok := s.Pop()
				if !ok {
					break
				}
				ir := mm.ir
				chMeta := mm.chMeta

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

				pri := getPriorityIndex(ir, tierLabel)
				if priorityHeader != "" {
					headers[priorityHeader] = strconv.Itoa(pri)
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
		}(workerPoolID, s, mergedChannel, p.priorityHeader, p.tierLabel)
	}

	return dispatch
}
