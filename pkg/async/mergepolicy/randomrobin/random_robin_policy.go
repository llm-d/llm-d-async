package randomrobin

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sync"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/mergepolicy/internal/fairness"
	"github.com/llm-d/llm-d-async/pkg/async/mergepolicy/internal/header"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/llm-d/llm-d-async/pkg/plugins"
)

func init() {
	plugins.MustRegister("random-robin", func(name string, parameters json.RawMessage, handle plugins.Handle) (plugins.Plugin, error) {
		var params fairness.Params
		if len(parameters) > 0 {
			if err := json.Unmarshal(parameters, &params); err != nil {
				return nil, fmt.Errorf("failed to parse random-robin parameters: %w", err)
			}
		}
		fairnessHeader, err := params.ResolveHeader()
		if err != nil {
			return nil, fmt.Errorf("invalid random-robin parameters: %w", err)
		}
		return NewRandomRobinPolicy(name, Config{
			FairnessHeader:    fairnessHeader,
			FairnessAttribute: params.Attribute,
		}), nil
	})
}

// Config configures the random-robin merge policy. The zero Config disables
// fairness stamping.
type Config struct {
	// FairnessHeader is the HTTP header stamped with the tenant identity so the
	// gateway's flow control can arbitrate between tenants. Empty disables
	// stamping.
	FairnessHeader string
	// FairnessAttribute is the message metadata attribute holding the tenant
	// identity. Empty falls back to fairness.DefaultAttribute.
	FairnessAttribute string
}

// NewRandomRobinPolicy returns a merge policy that randomly picks messages from
// all of a pool's queues.
func NewRandomRobinPolicy(name string, cfg Config) *RandomRobinPolicy {
	return &RandomRobinPolicy{
		name:     name,
		fairness: fairness.New(cfg.FairnessHeader, cfg.FairnessAttribute),
		pools:    make(map[string]*poolFanIn),
	}
}

var _ pipeline.RequestMergePolicy = (*RandomRobinPolicy)(nil)
var _ pipeline.DynamicRequestMergePolicy = (*RandomRobinPolicy)(nil)
var _ plugins.Plugin = (*RandomRobinPolicy)(nil)

type RandomRobinPolicy struct {
	name     string
	fairness fairness.Stamper

	mu    sync.Mutex
	pools map[string]*poolFanIn
}

func (r *RandomRobinPolicy) TypedName() plugins.TypedName {
	return plugins.TypedName{
		Type: "random-robin",
		Name: r.name,
	}
}

// poolFanIn merges a dynamic set of source channels into a single merged
// channel. Sources are added via AddRequestChannels and removed by closing
// the source channel. The merged channel lives for as long as the policy
// does and is never closed: the number of sources may legitimately drop to
// zero (e.g. every queue removed by a config reload) and grow again later.
type poolFanIn struct {
	merged chan pipeline.EmbelishedRequestMessage

	mu      sync.Mutex
	sources map[chan *api.InternalRequest]pipeline.RequestChannel
	// changed is buffered and sent non-blockingly whenever sources changes,
	// so the fan-in goroutine rebuilds its select cases.
	changed chan struct{}
}

func newPoolFanIn(buffer int) *poolFanIn {
	return &poolFanIn{
		merged:  make(chan pipeline.EmbelishedRequestMessage, buffer),
		sources: make(map[chan *api.InternalRequest]pipeline.RequestChannel),
		changed: make(chan struct{}, 1),
	}
}

func (f *poolFanIn) addSource(ch pipeline.RequestChannel) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sources[ch.Channel]; !ok {
		f.sources[ch.Channel] = ch
		f.notifyChanged()
	}
}

func (f *poolFanIn) removeSource(c chan *api.InternalRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sources[c]; ok {
		delete(f.sources, c)
		f.notifyChanged()
	}
}

func (f *poolFanIn) notifyChanged() {
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

// snapshot returns the current source set plus a matching case list. Case 0
// is always the changed-notification channel; source channels follow.
func (f *poolFanIn) snapshot() ([]pipeline.RequestChannel, []reflect.SelectCase) {
	f.mu.Lock()
	defer f.mu.Unlock()
	metas := make([]pipeline.RequestChannel, 0, len(f.sources))
	cases := make([]reflect.SelectCase, 0, len(f.sources)+1)
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(f.changed)})
	for c, meta := range f.sources {
		metas = append(metas, meta)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c)})
	}
	return metas, cases
}

func (f *poolFanIn) run(stamp func(headers map[string]string, msg pipeline.RequestChannel, ir *api.InternalRequest), workerPoolID string) {
	for {
		metas, cases := f.snapshot()
		if len(metas) == 0 {
			// No sources: block on the change notification alone.
			<-f.changed
			continue
		}
		idx, val, ok := reflect.Select(cases)
		if idx == 0 {
			// Source set changed; rebuild the cases.
			continue
		}
		if !ok {
			f.removeSource(metas[idx-1].Channel)
			continue
		}
		ir, ok := val.Interface().(*api.InternalRequest)
		if !ok || ir == nil {
			continue
		}
		chMeta := metas[idx-1]

		headers := map[string]string{}
		stamp(headers, chMeta, ir)
		erm := pipeline.EmbelishedRequestMessage{
			InternalRequest: ir,
			HttpHeaders:     headers,
			RequestURL:      requestURL(chMeta, ir),
			WorkerPoolID:    workerPoolID,
		}
		metrics.IncQueueDepth(ir.QueueID, ir.RequestQueueName, workerPoolID)
		f.merged <- erm
	}
}

// requestURL joins the queue's IGW base URL with the request's effective
// path: the per-request endpoint wins over the queue default when set.
func requestURL(chMeta pipeline.RequestChannel, ir *api.InternalRequest) string {
	requestPath := chMeta.RequestPathURL
	if ep := ir.PublicRequest.ReqEndpoint(); ep != "" {
		requestPath = ep
	}
	u, _ := url.JoinPath(chMeta.IGWBaseURL, requestPath)
	return u
}

func (r *RandomRobinPolicy) stampHeaders(headers map[string]string, chMeta pipeline.RequestChannel, ir *api.InternalRequest) {
	header.Set(headers, "Content-Type", "application/json")
	if chMeta.InferenceObjective != "" {
		header.Set(headers, "x-gateway-inference-objective", chMeta.InferenceObjective)
	}
	// Caller headers override the defaults above in any casing.
	for k, v := range ir.PublicRequest.ReqHeaders() {
		header.Set(headers, k, v)
	}
	// Stamped after the caller's headers so the tenant the quota
	// gate accounts on is the one the gateway arbitrates on.
	r.fairness.Stamp(headers, ir.PublicRequest)
}

func (r *RandomRobinPolicy) merge(channels []pipeline.RequestChannel, pools map[string]pipeline.WorkerPoolConfig) error {
	for _, ch := range channels {
		workerPoolID := ch.WorkerPoolID
		if workerPoolID == "" {
			workerPoolID = "default"
		}
		f, ok := r.pools[workerPoolID]
		if !ok {
			return fmt.Errorf("worker pool %q not found in pools map", workerPoolID)
		}
		f.addSource(ch)
	}
	return nil
}

func (r *RandomRobinPolicy) MergeRequestChannels(channels []pipeline.RequestChannel, pools map[string]pipeline.WorkerPoolConfig) pipeline.PoolDispatch {
	r.mu.Lock()
	defer r.mu.Unlock()

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

	// A fan-in exists for every configured pool, even those with no source
	// channels yet, so queue reloads can add sources to any pool and the
	// merged channel the workers already read never has to be replaced. The
	// buffer matches the pool's initial source count, preserving the
	// historical backpressure of a statically configured pool.
	for workerPoolID := range pools {
		f := newPoolFanIn(len(channelsByPool[workerPoolID]))
		r.pools[workerPoolID] = f
		go f.run(r.stampHeaders, workerPoolID)
	}
	if err := r.merge(channels, pools); err != nil {
		// Unreachable: the pool membership precheck above covers the groups
		// merge registers. Kept as a hard failure rather than silently
		// dropping a source channel.
		panic(err.Error())
	}

	dispatch := pipeline.PoolDispatch{
		Channels: make(map[string]chan pipeline.EmbelishedRequestMessage),
	}
	for workerPoolID, f := range r.pools {
		dispatch.Channels[workerPoolID] = f.merged
	}
	return dispatch
}

func (r *RandomRobinPolicy) AddRequestChannels(channels []pipeline.RequestChannel, pools map[string]pipeline.WorkerPoolConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ch := range channels {
		workerPoolID := ch.WorkerPoolID
		if workerPoolID == "" {
			workerPoolID = "default"
		}
		if _, ok := r.pools[workerPoolID]; !ok {
			return fmt.Errorf("worker pool %q not found in pools map", workerPoolID)
		}
	}
	return r.merge(channels, pools)
}
