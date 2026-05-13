// Package tierpriority implements a label-driven N-pass merge policy
// that emits one channel per inference pool.
//
// The policy buckets each pulled message by (pool, tier, class) labels
// and within each pool dispatches in (class, tier) priority order —
// class is the dominant axis, tier is the tiebreaker. With default
// ordering [reserved, overflow] × [interactive, async, batch],
// that means reserved/batch outranks overflow/interactive: reserved
// capacity is a floor that overflow at any tier cannot displace.
// Round-robin across (team × model) source channels within each
// (class, tier) bucket. Per-pool buckets share no state with other
// pools' buckets — each pool dispatches independently to its own
// output channel.
//
// Reserved/Overflow class names are policy-internal strings; upstream
// pipeline knows nothing about them. Operators configure the label key
// and value-ordering, so different vocabularies (or more than two
// classes) drop in without code changes.
//
// Messages whose tier or class label values aren't in the configured
// order (operator typos, schema drift) aren't lost: they drain at
// lowest priority via a catch-all pass after the configured priority
// passes complete. The policy is total over its input domain.
package tierpriority

import (
	"sort"
	"strconv"
	"sync"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

// Defaults match the RFC vocabulary; operators can override via Options.
const (
	DefaultTierLabel      = "tier"
	DefaultClassLabel     = "class"
	DefaultPriorityHeader = "x-priority"
)

// Config captures everything the policy reads from labels and how it
// translates dispatch decisions into outgoing-request shape. The policy
// is pure routing/ordering: it does not consult any gate, does not
// decide fail-fast, and never reads a Verdict. Saturation and fail-fast
// behavior are expressed as label-aware pool gates in the per-pool gate
// chain, evaluated in the worker pool downstream of this policy.
type Config struct {
	TierLabel      string
	TierOrder      []string
	ClassLabel     string
	ClassOrder     []string
	PriorityHeader string
	PriorityFor    func(tier, class string) int
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.TierLabel == "" {
		out.TierLabel = DefaultTierLabel
	}
	if out.ClassLabel == "" {
		out.ClassLabel = DefaultClassLabel
	}
	if out.PriorityHeader == "" {
		out.PriorityHeader = DefaultPriorityHeader
	}
	if out.PriorityFor == nil {
		out.PriorityFor = defaultPriorityFor(out.TierOrder, out.ClassOrder)
	}
	return out
}

func defaultPriorityFor(tierOrder, classOrder []string) func(string, string) int {
	tierIdx := indexOf(tierOrder)
	classIdx := indexOf(classOrder)
	tierWeight := len(classOrder) + 1
	if tierWeight < 2 {
		tierWeight = 2
	}
	return func(tier, class string) int {
		ti, ok := tierIdx[tier]
		if !ok {
			ti = len(tierOrder)
		}
		ci, ok := classIdx[class]
		if !ok {
			ci = len(classOrder)
		}
		return (len(tierOrder)-ti)*tierWeight + (len(classOrder) - ci)
	}
}

func indexOf(values []string) map[string]int {
	out := make(map[string]int, len(values))
	for i, v := range values {
		out[v] = i
	}
	return out
}

// New constructs a Policy with the given Config.
func New(cfg Config) *Policy {
	return &Policy{cfg: cfg.withDefaults()}
}

// Policy implements pipeline.RequestMergePolicy. One Policy instance
// owns one bucket set per pool; each pool has its own dispatcher
// goroutine that runs the N-pass priority loop independently. The
// policy is pure routing/ordering — it never consults a gate.
type Policy struct {
	cfg Config
}

// isConfiguredKey reports whether (tier, class) is one of the buckets
// popNext drains in its configured passes. Used by popUnconfigured to
// identify catch-all candidates. Empty-string values count as
// configured (the configured passes consult them explicitly).
func (p *Policy) isConfiguredKey(k bucketKey) bool {
	tierOK := k.tier == ""
	if !tierOK {
		for _, t := range p.cfg.TierOrder {
			if t == k.tier {
				tierOK = true
				break
			}
		}
	}
	classOK := k.class == ""
	if !classOK {
		for _, c := range p.cfg.ClassOrder {
			if c == k.class {
				classOK = true
				break
			}
		}
	}
	return tierOK && classOK
}

var _ pipeline.RequestMergePolicy = (*Policy)(nil)

// MergeRequestChannels groups incoming RequestChannels by pool, spawns
// one reader goroutine per input channel that routes messages into the
// pool's bucket set, and one dispatcher goroutine per pool that pops
// in (class × tier) priority order and forwards to the pool's output
// channel.
func (p *Policy) MergeRequestChannels(channels []pipeline.RequestChannel) pipeline.PoolDispatch {
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

		ps := newPoolState(p, poolID, inputs, out[poolID])
		for i, ch := range inputs {
			go ps.reader(i, ch)
		}
		go ps.dispatcher()
	}

	return pipeline.PoolDispatch{Channels: out}
}

// poolState owns the per-pool buckets and goroutines.
type poolState struct {
	policy  *Policy
	poolID  string
	inputs  []pipeline.RequestChannel
	out     chan<- pipeline.EmbelishedRequestMessage

	mu        sync.Mutex
	cond      *sync.Cond
	buckets   map[bucketKey]*bucket
	remaining int
	closed    bool
}

type bucketKey struct {
	tier  string
	class string
}

type bucket struct {
	queues map[int][]*pipeline.EmbelishedRequestMessage
	rrIdx  int
}

func newPoolState(policy *Policy, poolID string, inputs []pipeline.RequestChannel, out chan<- pipeline.EmbelishedRequestMessage) *poolState {
	ps := &poolState{
		policy:    policy,
		poolID:    poolID,
		inputs:    inputs,
		out:       out,
		buckets:   map[bucketKey]*bucket{},
		remaining: len(inputs),
	}
	ps.cond = sync.NewCond(&ps.mu)
	return ps
}

// reader drains a single input channel and places each message into the
// right (tier, class) bucket for this pool. Messages arrive fully
// embellished from the Flow's pull callback; no re-wrapping needed.
func (ps *poolState) reader(idx int, ch pipeline.RequestChannel) {
	for emb := range ch.Channel {
		if emb == nil {
			continue
		}
		key := bucketKey{
			tier:  emb.Labels.Get(ps.policy.cfg.TierLabel),
			class: emb.Labels.Get(ps.policy.cfg.ClassLabel),
		}
		ps.mu.Lock()
		b := ps.buckets[key]
		if b == nil {
			b = &bucket{}
			ps.buckets[key] = b
		}
		if b.queues == nil {
			b.queues = map[int][]*pipeline.EmbelishedRequestMessage{}
		}
		ps.cond.Signal()
		b.queues[idx] = append(b.queues[idx], emb)
		ps.mu.Unlock()
	}
	ps.mu.Lock()
	ps.remaining--
	if ps.remaining == 0 {
		ps.closed = true
		ps.cond.Broadcast()
	}
	ps.mu.Unlock()
}

// dispatcher pops messages in (class × tier) priority order and forwards
// to the pool's output channel. Exits when all inputs have closed and
// all buckets are drained.
func (ps *poolState) dispatcher() {
	defer close(ps.out)
	for {
		emb := ps.popNext()
		if emb == nil {
			return
		}
		ps.policy.dispatch(emb, ps.out)
	}
}

func (ps *poolState) popNext() *pipeline.EmbelishedRequestMessage {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for {
		// Iterate class × tier in priority order.
		for _, class := range ps.policy.cfg.ClassOrder {
			for _, tier := range ps.policy.cfg.TierOrder {
				if m := ps.popBucket(bucketKey{tier, class}); m != nil {
					return m
				}
			}
			// Empty-tier bucket for this class.
			if m := ps.popBucket(bucketKey{"", class}); m != nil {
				return m
			}
		}
		// Empty-class buckets in tier order, for messages with unknown
		// classes (after all named classes).
		for _, tier := range ps.policy.cfg.TierOrder {
			if m := ps.popBucket(bucketKey{tier, ""}); m != nil {
				return m
			}
		}
		if m := ps.popBucket(bucketKey{"", ""}); m != nil {
			return m
		}
		// Catch-all: any bucket whose (tier, class) isn't covered by
		// the configured ClassOrder / TierOrder still contains real
		// work. Drain it at lowest priority — keeps the policy total
		// over its input domain so typos or schema drift (e.g.
		// `tier=urgent` against a config that only declares
		// interactive/async/batch) don't strand messages.
		if m := ps.popUnconfigured(); m != nil {
			return m
		}
		if ps.closed {
			return nil
		}
		ps.cond.Wait()
	}
}

// popUnconfigured pops one message from any bucket whose (tier, class)
// key is not in the configured ClassOrder / TierOrder. The configured
// passes above already drained their buckets; whatever remains is, by
// definition, unconfigured. Iteration order is map-iteration (random)
// — these are operator-misconfigured messages, so we don't promise any
// particular interleaving among them.
func (ps *poolState) popUnconfigured() *pipeline.EmbelishedRequestMessage {
	for key := range ps.buckets {
		if ps.policy.isConfiguredKey(key) {
			continue
		}
		if m := ps.popBucket(key); m != nil {
			return m
		}
	}
	return nil
}

func (ps *poolState) popBucket(key bucketKey) *pipeline.EmbelishedRequestMessage {
	b := ps.buckets[key]
	if b == nil {
		return nil
	}
	if len(b.queues) == 0 {
		return nil
	}
	keys := make([]int, 0, len(b.queues))
	for k, q := range b.queues {
		if len(q) > 0 {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Ints(keys)
	pick := keys[b.rrIdx%len(keys)]
	b.rrIdx++
	msg := b.queues[pick][0]
	b.queues[pick] = b.queues[pick][1:]
	if len(b.queues[pick]) == 0 {
		delete(b.queues, pick)
	}
	return msg
}

// dispatch stamps the priority header and forwards the message to the
// pool's output channel. The merge policy does not consult a gate;
// saturation and fail-fast are pool-gate concerns evaluated downstream
// in the per-pool worker pool.
func (p *Policy) dispatch(emb *pipeline.EmbelishedRequestMessage, out chan<- pipeline.EmbelishedRequestMessage) {
	p.stampPriority(emb)
	out <- *emb
}

func (p *Policy) stampPriority(emb *pipeline.EmbelishedRequestMessage) {
	tier := emb.Labels.Get(p.cfg.TierLabel)
	class := emb.Labels.Get(p.cfg.ClassLabel)
	priority := p.cfg.PriorityFor(tier, class)
	if emb.HttpHeaders == nil {
		emb.HttpHeaders = map[string]string{}
	}
	emb.HttpHeaders[p.cfg.PriorityHeader] = strconv.Itoa(priority)
}
