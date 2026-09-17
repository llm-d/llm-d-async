package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

// SortedSetQueueConfig defines a single queue entry in the sorted-set transport config.
type SortedSetQueueConfig struct {
	ID              string `json:"id,omitempty"`
	QueueName       string `json:"queue_name,omitempty"`
	ResultQueueName string `json:"result_queue_name,omitempty"`
	// ResultTTLSeconds, when > 0, sets an expiry on the result destination
	// each time results are pushed. Used for per-request result keys
	// (frontend enqueue mode) so unfetched results are cleaned up. Queues
	// without it behave as before (no expiry).
	ResultTTLSeconds   int64  `json:"result_ttl_seconds,omitempty"`
	WorkerPoolID       string `json:"worker_pool_id"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
	IGWBaseURL         string `json:"igw_base_url"`
	pipeline.GateConfig
	Labels map[string]string `json:"labels,omitempty"`
}

type requestChannelData struct {
	channel   pipeline.RequestChannel
	queueName string
	queueID   string
	gate      pipeline.Gate
}

// queueRuntime tracks one live queue: its dispatch channel plus the consume
// worker's lifecycle hooks. cancel stops the worker; done closes after the
// worker returned, at which point the dispatch channel is safe to close.
// started marks whether a consume worker has ever been launched, so a queue
// registered before Start still gets a worker when Start runs.
type queueRuntime struct {
	data    requestChannelData
	config  SortedSetQueueConfig
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

// QueueReconfigureResult describes what one ReconfigureQueues call changed,
// in the terms the merge policy layer understands: channels that appeared
// (to be added to the fan-in) and channels that were closed (removed from
// it; closing the source channel is how a merge policy forgets a queue).
type QueueReconfigureResult struct {
	Added   []pipeline.RequestChannel
	Removed []pipeline.RequestChannel
}

// SortedSetQueueReconfigurer is the optional capability of a Redis
// sorted-set flow that supports replacing its queue set at runtime, e.g.
// after a hot reload of a queues configuration file. beforeCommit receives
// every new or replacement channel after preparation but before live state is
// changed. If it returns an error, the old registry and workers are untouched.
type SortedSetQueueReconfigurer interface {
	ReconfigureQueues(queues []SortedSetQueueConfig, beforeCommit func([]pipeline.RequestChannel) error) (QueueReconfigureResult, error)
}

var errFlowStopped = errors.New("flow is stopped; cannot reconfigure queues")

var (
	_ pipeline.Flow                        = (*RedisSortedSetFlow)(nil)
	_ pipeline.HealthChecker               = (*RedisSortedSetFlow)(nil)
	_ pipeline.CancellationCheckerProvider = (*RedisSortedSetFlow)(nil)
	_ SortedSetQueueReconfigurer           = (*RedisSortedSetFlow)(nil)
)

var cleanupRequestStateScript = redis.NewScript(`
local token = ARGV[1]
if token == "" then
  return 0
end
if redis.call("GET", KEYS[1]) == token then
  redis.call("DEL", KEYS[1])
end
if redis.call("GET", KEYS[2]) == token then
  redis.call("DEL", KEYS[2])
end
return 1
`)

type RedisSortedSetFlow struct {
	rdb                 *redis.Client
	cancellationChecker api.CancellationChecker
	retryChannel        chan pipeline.RetryMessage
	resultChannel       chan api.ResultMessage
	pollInterval        time.Duration
	batchSize           int
	retryQueueName      string
	activeReleases      sync.Map
	gate                pipeline.Gate
	gateFactory         pipeline.GateFactory

	// queueMu guards the complete live queue state: queues, queueOrder,
	// configMap, defaultRequestQueueName and stopped. consumeWg.Add for a
	// queue worker must happen while holding it, so Wait in StopConsuming can
	// never undercount a worker started concurrently.
	queueMu       sync.RWMutex
	reconfigureMu sync.Mutex
	queues        map[string]*queueRuntime
	queueOrder    []string
	configMap     map[string]SortedSetQueueConfig
	stopped       bool
	ctxForQueues  context.Context

	defaultRequestQueueName string
	defaultResultQueueName  string
	workerPools             []pipeline.WorkerPoolConfig
	consumeCancel           context.CancelFunc
	consumeWg               sync.WaitGroup
	drainCancel             context.CancelFunc
	drainWg                 sync.WaitGroup
	hbCancel                context.CancelFunc
	hbWg                    sync.WaitGroup
	enableTracing           bool

	// Durable-dequeue state and tuning knobs.
	claimTokens          sync.Map // ownership token per in-flight request
	claimLeaseTTL        time.Duration
	claimReclaimInterval time.Duration
}

type redisCancellationChecker struct {
	rdb *redis.Client
}

func (c *redisCancellationChecker) IsCancelled(ctx context.Context, requestID, requestToken string) (bool, error) {
	if requestID == "" || requestToken == "" {
		return false, nil
	}
	token, err := c.rdb.Get(ctx, api.RequestCancellationKey(requestID)).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return token == requestToken, nil
}

// NewRedisSortedSetFlow builds a Redis sorted-set flow from a parsed
// SortedSetConfig. The config is expected to have had ApplyDefaults applied
// (LoadSortedSetConfig does this). workerPools resolves the named pool each
// queue routes to; gateFactory, when non-nil, instantiates a per-queue gate for
// any queue that declares a gate_type.
// normalizeSortedSetQueue fills the Optional fields of a queue config with
// the same defaults NewRedisSortedSetFlow has always applied: empty worker
// pool → "default", empty request path → "/v1/completions", and empty ID →
// the queue name. ReconfigureQueues applies these too so hot-loaded configs
// behave exactly like startup configs.
func normalizeSortedSetQueue(cfg *SortedSetQueueConfig) {
	// Normalize before anything reads it: configMap is the source of the
	// pool_name label on this queue's metrics, and an unset WorkerPoolID
	// there would label them "" while the request channel below — and every
	// pool-keyed series — says "default".
	if cfg.WorkerPoolID == "" {
		cfg.WorkerPoolID = "default"
	}
	if cfg.RequestPathURL == "" {
		cfg.RequestPathURL = "/v1/completions"
	}
	if cfg.ID == "" {
		cfg.ID = cfg.QueueName
	}
}

// validateSortedSetQueues enforces the cross-entry constraints of a queue
// set (unique IDs, unique names, every entry dispatchable) before any of
// them is allowed to change live state.
func (r *RedisSortedSetFlow) validateSortedSetQueues(queues []SortedSetQueueConfig) error {
	seenID := make(map[string]bool, len(queues))
	seenQueue := make(map[string]bool, len(queues))
	for _, q := range queues {
		if q.QueueName == "" {
			return fmt.Errorf("queue_name is required for each queue")
		}
		if seenID[q.ID] {
			return fmt.Errorf("duplicate queue id %q in queue configuration", q.ID)
		}
		seenID[q.ID] = true
		if seenQueue[q.QueueName] {
			return fmt.Errorf("duplicate queue name %q in queue configuration", q.QueueName)
		}
		seenQueue[q.QueueName] = true
		if q.IGWBaseURL == "" {
			return fmt.Errorf("queue config for queue %q: igw_base_url must be specified", q.QueueName)
		}
		found := false
		for _, pool := range r.workerPools {
			if pool.ID == q.WorkerPoolID {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("worker pool %q specified in queue config not found in pool configuration", q.WorkerPoolID)
		}
	}
	return nil
}

// newRequestChannel builds the dispatch channel (and attached gate) for a
// single already-normalized, already-validated queue config.
func (r *RedisSortedSetFlow) newRequestChannel(cfg SortedSetQueueConfig) (requestChannelData, error) {
	workerPoolID := cfg.WorkerPoolID

	var gate pipeline.Gate
	if r.gateFactory != nil && cfg.GateType != "" {
		gateCfg := cfg.GateConfig
		gateCfg.Owner = pipeline.GateOwner{
			QueueID:      cfg.ID,
			QueueName:    cfg.QueueName,
			WorkerPoolID: workerPoolID,
		}
		created, err := r.gateFactory.CreateGate(gateCfg)
		if err != nil {
			return requestChannelData{}, fmt.Errorf("failed to create gate for queue %q (gate_type=%q): %w", cfg.QueueName, cfg.GateType, err)
		}
		gate = created
	} else if r.gate != nil {
		gate = r.gate
	} else {
		gate = pipeline.ConstOpenGate()
	}

	ch := pipeline.RequestChannel{
		Channel:            make(chan *api.InternalRequest),
		InferenceObjective: cfg.InferenceObjective,
		RequestPathURL:     cfg.RequestPathURL,
		IGWBaseURL:         cfg.IGWBaseURL,
		Gate:               gate,
		WorkerPoolID:       workerPoolID,
	}
	return requestChannelData{
		channel:   ch,
		queueName: cfg.QueueName,
		queueID:   cfg.ID,
		gate:      gate,
	}, nil
}

func NewRedisSortedSetFlow(cfg SortedSetConfig, workerPools []pipeline.WorkerPoolConfig, gateFactory pipeline.GateFactory) (*RedisSortedSetFlow, error) {
	redisOpts, err := ParseRedisOptions(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis connection config: %w", err)
	}
	r := &RedisSortedSetFlow{
		rdb:                    redis.NewClient(redisOpts),
		queues:                 make(map[string]*queueRuntime, len(cfg.Queues)),
		queueOrder:             make([]string, 0, len(cfg.Queues)),
		retryChannel:           make(chan pipeline.RetryMessage),
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           time.Duration(cfg.PollIntervalMs) * time.Millisecond,
		batchSize:              cfg.BatchSize,
		retryQueueName:         cfg.RetryQueueName,
		defaultResultQueueName: cfg.ResultQueueName,
		workerPools:            workerPools,
		gateFactory:            gateFactory,
		enableTracing:          cfg.EnableTracing,
		claimLeaseTTL:          time.Duration(cfg.ClaimLeaseTTLSeconds) * time.Second,
		claimReclaimInterval:   time.Duration(cfg.ClaimReclaimIntervalMs) * time.Millisecond,
	}
	if r.claimLeaseTTL <= 0 {
		r.claimLeaseTTL = 300 * time.Second
	}
	if r.claimReclaimInterval <= 0 {
		r.claimReclaimInterval = 15 * time.Second
	}

	if r.enableTracing {
		if err := redisotel.InstrumentTracing(r.rdb); err != nil {
			_ = r.rdb.Close()
			return nil, fmt.Errorf("failed to instrument Redis tracing: %w", err)
		}
	}

	queues := make([]SortedSetQueueConfig, len(cfg.Queues))
	for i := range queues {
		queues[i] = cfg.Queues[i]
		normalizeSortedSetQueue(&queues[i])
	}
	if err := r.validateSortedSetQueues(queues); err != nil {
		_ = r.rdb.Close()
		return nil, err
	}

	// Retry messages that lack an explicit RequestQueueName fall back to this
	// name when re-enqueued (flushRetryBatch). Default to the first configured
	// queue so retries land on a real key rather than "" — preserving the
	// previous single-queue fallback behavior.
	if len(queues) > 0 {
		r.defaultRequestQueueName = queues[0].QueueName
	}

	r.configMap = make(map[string]SortedSetQueueConfig, len(queues))
	for _, queueCfg := range queues {
		data, err := r.newRequestChannel(queueCfg)
		if err != nil {
			_ = r.rdb.Close()
			return nil, err
		}
		r.configMap[queueCfg.ID] = queueCfg
		r.queues[queueCfg.ID] = &queueRuntime{data: data, config: queueCfg, done: make(chan struct{})}
		r.queueOrder = append(r.queueOrder, queueCfg.ID)
	}

	if r.gate == nil {
		r.gate = pipeline.ConstOpenGate()
	}

	return r, nil
}

// startQueueWorkerLocked launches the consume worker for a registered
// queue. Callers hold queueMu, which serializes the consumeWg.Add against
// Wait in StopConsuming; queues added by a hot reload before StopConsumer
// is flagged are therefore always waited on.
func (r *RedisSortedSetFlow) startQueueWorkerLocked(consumeCtx context.Context, entry *queueRuntime) {
	if entry.started {
		return
	}
	entry.started = true
	entry.done = make(chan struct{})
	queueCtx, cancel := context.WithCancel(consumeCtx)
	entry.cancel = cancel
	r.consumeWg.Add(1)
	go func() {
		defer r.consumeWg.Done()
		defer close(entry.done)
		r.requestWorker(queueCtx, entry)
	}()
}

func (r *RedisSortedSetFlow) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	consumeCtx, consumeCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.consumeCancel = consumeCancel

	drainCtx, drainCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.drainCancel = drainCancel

	hbCtx, hbCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.hbCancel = hbCancel

	r.queueMu.Lock()
	r.ctxForQueues = consumeCtx
	for _, id := range r.queueOrder {
		r.startQueueWorkerLocked(consumeCtx, r.queues[id])
	}
	r.queueMu.Unlock()

	r.consumeWg.Add(1)
	go func() { defer r.consumeWg.Done(); r.retryMover(consumeCtx) }()

	r.drainWg.Add(3)
	go func() { defer r.drainWg.Done(); r.retryWorker(drainCtx) }()  // #nosec G118
	go func() { defer r.drainWg.Done(); r.resultWorker(drainCtx) }() // #nosec G118
	// The reclaimer runs on the drain context: it must stay alive into
	// the shutdown window so claims abandoned by *other* (crashed) instances
	// are still redelivered while this one drains its own work.
	go func() { defer r.drainWg.Done(); r.startReclaimer(drainCtx) }() // #nosec G118
	// The heartbeater proves this instance is alive for the work it holds;
	// without it a slow-but-healthy inference call would be redelivered
	// mid-flight by a peer that saw the lease lapse. It lives on its own
	// context so it keeps renewing through the whole drain window and stops
	// only after the drain workers are done.
	r.hbWg.Add(1)
	go func() { defer r.hbWg.Done(); r.startHeartbeat(hbCtx) }()
}

func (r *RedisSortedSetFlow) StopHeartbeatForTest() {
	if r.hbCancel != nil {
		r.hbCancel()
	}
	r.hbWg.Wait()
}

func (r *RedisSortedSetFlow) StopConsuming() {
	// Flag first: any ReconfigureQueues racing the shutdown sees stopped
	// (under queueMu) before it can start a worker Wait would miss.
	r.queueMu.Lock()
	r.stopped = true
	r.queueMu.Unlock()

	if r.consumeCancel != nil {
		r.consumeCancel()
	}
	r.consumeWg.Wait()
}

// ReconfigureQueues swaps the live queue set against the registry in one
// atomic step:
//
//   - A queue left unchanged across configs keeps its channel, its gate and
//     its consume worker untouched.
//   - A new or modified queue is validated and built before anything live
//     changes; the fresh channel and worker start inside the registry swap,
//     so a new queue is consumable the moment the function returns.
//   - A removed or replaced queue's worker is cancelled; after it exits, the
//     flow itself closes the queue's channel. Closing the source channel is
//     how merge policies unregister a queue, keeping the policy's merged
//     channel (and the inference workers reading it) untouched.
//
// Neither Redis backlog of removed queues nor in-flight messages are lost:
// a worker cancelled mid-send re-enqueues the message into the queue before
// exiting. Empty queue slices are legal — the flow idles and can still
// accept queues later. Any validation error leaves the previous, last-good
// configuration untouched. beforeCommit runs after all preparation and the
// final stopped check, but before any live state changes. Once it succeeds,
// the remaining commit path cannot fail.
func (r *RedisSortedSetFlow) ReconfigureQueues(queues []SortedSetQueueConfig, beforeCommit func([]pipeline.RequestChannel) error) (QueueReconfigureResult, error) {
	r.reconfigureMu.Lock()
	defer r.reconfigureMu.Unlock()

	normalized := make([]SortedSetQueueConfig, len(queues))
	for i := range normalized {
		normalized[i] = queues[i]
		normalizeSortedSetQueue(&normalized[i])
	}
	if err := r.validateSortedSetQueues(normalized); err != nil {
		return QueueReconfigureResult{}, err
	}

	r.queueMu.Lock()
	if r.stopped {
		r.queueMu.Unlock()
		return QueueReconfigureResult{}, errFlowStopped
	}
	r.queueMu.Unlock()

	// Gate creation and channel allocation may fail or be slow. Prepare only
	// new or changed queues before touching live state; unchanged gates may
	// own resources and must not be recreated and discarded on every poll.
	prepared := make(map[string]*queueRuntime, len(normalized))
	r.queueMu.RLock()
	currentConfigs := make(map[string]SortedSetQueueConfig, len(r.queues))
	for id, entry := range r.queues {
		currentConfigs[id] = entry.config
	}
	r.queueMu.RUnlock()
	for _, queueCfg := range normalized {
		oldConfig, exists := currentConfigs[queueCfg.ID]
		if exists && reflect.DeepEqual(oldConfig, queueCfg) {
			continue
		}
		data, err := r.newRequestChannel(queueCfg)
		if err != nil {
			return QueueReconfigureResult{}, err
		}
		prepared[queueCfg.ID] = &queueRuntime{data: data, config: queueCfg}
	}

	newConfigMap := make(map[string]SortedSetQueueConfig, len(normalized))
	for _, queueCfg := range normalized {
		newConfigMap[queueCfg.ID] = queueCfg
	}

	r.queueMu.Lock()
	if r.stopped {
		r.queueMu.Unlock()
		return QueueReconfigureResult{}, errFlowStopped
	}

	var result QueueReconfigureResult
	var removed []*queueRuntime
	newQueues := make(map[string]*queueRuntime, len(normalized))
	newOrder := make([]string, 0, len(normalized))
	for _, queueCfg := range normalized {
		old, exists := r.queues[queueCfg.ID]
		if exists && reflect.DeepEqual(old.config, queueCfg) {
			// Unchanged: keep channel, gate and worker exactly as they are.
			newQueues[queueCfg.ID] = old
			newOrder = append(newOrder, queueCfg.ID)
			continue
		}
		if exists {
			// Modified: drain the old incarnation and replace it after the
			// commit callback succeeds.
			removed = append(removed, old)
			result.Removed = append(result.Removed, old.data.channel)
		}
		entry := prepared[queueCfg.ID]
		newQueues[queueCfg.ID] = entry
		newOrder = append(newOrder, queueCfg.ID)
		result.Added = append(result.Added, entry.data.channel)
	}
	for _, id := range r.queueOrder {
		if _, kept := newQueues[id]; kept {
			continue
		}
		// Removed outright: drain and unregister.
		old := r.queues[id]
		removed = append(removed, old)
		result.Removed = append(result.Removed, old.data.channel)
	}
	if beforeCommit != nil && len(result.Added) > 0 {
		if err := beforeCommit(result.Added); err != nil {
			r.queueMu.Unlock()
			return QueueReconfigureResult{}, err
		}
	}
	for _, old := range removed {
		if old.cancel != nil {
			old.cancel()
		}
	}
	for _, queueCfg := range normalized {
		old, exists := r.queues[queueCfg.ID]
		if exists && reflect.DeepEqual(old.config, queueCfg) {
			continue
		}
		if r.ctxForQueues != nil {
			r.startQueueWorkerLocked(r.ctxForQueues, newQueues[queueCfg.ID])
		}
	}

	r.queues = newQueues
	r.queueOrder = newOrder
	r.configMap = newConfigMap
	if len(normalized) > 0 {
		r.defaultRequestQueueName = normalized[0].QueueName
	} else {
		r.defaultRequestQueueName = ""
	}
	r.queueMu.Unlock()

	// Closing a channel whose worker might still send panics, so every
	// removed worker must have fully returned first. Their cancel was issued
	// above; a worker blocked pushing to its channel exits via the ctx.Done
	// branch of the send select and re-enqueues the message into Redis. A
	// queue removed before Start never had a worker at all: its channel was
	// also never handed out, so it needs neither wait nor close.
	for _, old := range removed {
		newCfg, replaced := newConfigMap[old.data.queueID]
		removeSnapshots := !replaced || newCfg.QueueName != old.data.queueName || newCfg.WorkerPoolID != old.config.WorkerPoolID
		if !old.started {
			if removeSnapshots {
				metrics.RemoveQueueSnapshots(old.data.queueID, old.data.queueName, old.config.WorkerPoolID)
			}
			continue
		}
		<-old.done
		close(old.data.channel.Channel)
		if removeSnapshots {
			metrics.RemoveQueueSnapshots(old.data.queueID, old.data.queueName, old.config.WorkerPoolID)
		}
	}

	return result, nil
}

func (r *RedisSortedSetFlow) Shutdown() {
	if r.drainCancel != nil {
		r.drainCancel()
	}
	r.drainWg.Wait()
	if r.hbCancel != nil {
		r.hbCancel()
	}
	r.hbWg.Wait()
}

func (r *RedisSortedSetFlow) RequestChannels() []pipeline.RequestChannel {
	r.queueMu.RLock()
	defer r.queueMu.RUnlock()
	channels := make([]pipeline.RequestChannel, 0, len(r.queueOrder))
	for _, id := range r.queueOrder {
		channels = append(channels, r.queues[id].data.channel)
	}
	return channels
}

// zeroExpiringCounts builds a non-nil ExpiringCounts slice with every bucket
// present and count 0. Used on failed reads so pollBacklog clears the
// deadline-proximity snapshot instead of leaving its last value stale; nil
// (Pub/Sub) still means the broker cannot report deadlines at all.
func zeroExpiringCounts(labels []string) []pipeline.ExpiringCount {
	counts := make([]pipeline.ExpiringCount, 0, len(labels))
	for _, l := range labels {
		counts = append(counts, pipeline.ExpiringCount{Window: l})
	}
	return counts
}

// QueueBacklog reports the number of pending members in each queue's sorted
// set and exact cumulative per-bucket counts of members nearing their
// deadline. Bucket counts come from ZCOUNT, which compares only scores and
// never reads member payloads, so the read cost is independent of request
// size.
// queueSnapshot copies the registered queues in config order; Redis I/O
// afterwards must not hold queueMu.
func (r *RedisSortedSetFlow) queueSnapshot() []requestChannelData {
	r.queueMu.RLock()
	defer r.queueMu.RUnlock()
	out := make([]requestChannelData, 0, len(r.queueOrder))
	for _, id := range r.queueOrder {
		out = append(out, r.queues[id].data)
	}
	return out
}

func (r *RedisSortedSetFlow) QueueBacklog(ctx context.Context) ([]pipeline.QueueBacklogStat, error) {
	snapshot := r.queueSnapshot()
	stats := make([]pipeline.QueueBacklogStat, 0, len(snapshot))
	var firstErr error
	buckets := metrics.DeadlineProximityBuckets()
	bucketLabels := metrics.DeadlineProximityBucketLabels()
	for _, cd := range snapshot {
		stat := pipeline.QueueBacklogStat{
			QueueID:   cd.queueID,
			QueueName: cd.queueName,
			PoolName:  cd.channel.WorkerPoolID,
		}
		now := time.Now().Unix()
		var cardCmd *redis.IntCmd
		countCmds := make([]*redis.IntCmd, 0, len(buckets))
		// One MULTI/EXEC round trip per queue: the sorted set is mutated by
		// claims between polls, so a single snapshot keeps Depth and the
		// bucket counts mutually consistent.
		_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			cardCmd = pipe.ZCard(ctx, cd.queueName)
			for _, b := range buckets {
				countCmds = append(countCmds, pipe.ZCount(ctx, cd.queueName, "-inf",
					strconv.FormatInt(now+int64(b.Seconds()), 10)))
			}
			return nil
		})
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("pipelined backlog read on queue %q: %w", cd.queueName, err)
			}
			// Report 0 rather than skipping so the gauges do not retain a
			// stale value for this queue after a failed poll. The zeroed
			// expiring counts clear the deadline-proximity snapshot.
			stat.ExpiringCounts = zeroExpiringCounts(bucketLabels)
			stats = append(stats, stat)
			continue
		}
		if err := cardCmd.Err(); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("ZCard on queue %q: %w", cd.queueName, err)
			}
			// Report 0 rather than skipping so the gauges do not retain a
			// stale value for this queue after a failed poll.
			stat.ExpiringCounts = zeroExpiringCounts(bucketLabels)
			stats = append(stats, stat)
			continue
		}
		stat.Depth = cardCmd.Val()
		stat.SourceAvailable = true
		var secondaryErr error
		for _, cc := range countCmds {
			if cc.Err() != nil {
				secondaryErr = cc.Err()
			}
		}
		if secondaryErr != nil {
			// The depth read succeeded; keep it so async_broker_backlog does
			// not look drained. The deadline view clears instead: the counts
			// are zeroed so the deadline-proximity snapshot is cleared, not
			// stale.
			if firstErr == nil {
				firstErr = fmt.Errorf("deadline reads on queue %q: %w", cd.queueName, secondaryErr)
			}
			stat.ExpiringCounts = zeroExpiringCounts(bucketLabels)
			stats = append(stats, stat)
			continue
		}
		stat.ExpiringCounts = make([]pipeline.ExpiringCount, 0, len(countCmds))
		for i, l := range bucketLabels {
			stat.ExpiringCounts = append(stat.ExpiringCounts, pipeline.ExpiringCount{
				Window: l,
				Count:  countCmds[i].Val(),
			})
		}
		stats = append(stats, stat)
	}
	return stats, firstErr
}

var _ pipeline.BacklogReporter = (*RedisSortedSetFlow)(nil)

func (r *RedisSortedSetFlow) RetryChannel() chan pipeline.RetryMessage {
	return r.retryChannel
}

func (r *RedisSortedSetFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *RedisSortedSetFlow) CancellationChecker() api.CancellationChecker {
	if r.cancellationChecker != nil {
		return r.cancellationChecker
	}
	return &redisCancellationChecker{rdb: r.rdb}
}

func (r *RedisSortedSetFlow) HealthCheck(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

func (r *RedisSortedSetFlow) Characteristics() pipeline.Characteristics {
	return pipeline.Characteristics{HasExternalBackoff: false, SupportsMessageLatency: false}
}

// Polls sorted set and processes messages by deadline priority (earliest first)
func (r *RedisSortedSetFlow) requestWorker(ctx context.Context, entry *queueRuntime) {
	d := entry.data
	cfg := entry.config
	if cfg.ID == "" && cfg.QueueName == "" {
		cfg, _ = r.queueConfigOf(d.queueID)
	}
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	gate := d.gate
	if gate == nil {
		gate = r.gate
	}

	metrics.InitGateDecisions(d.queueID, d.queueName, cfg.WorkerPoolID)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.processMessagesWithConfig(ctx, d.channel.Channel, d.queueName, d.queueID, gate, logger, cfg)
		}
	}
}

// fallbackRequestQueue returns the queue retry messages without an explicit
// origin queue land on. ReconfigureQueues keeps it pointing at the first
// configured queue, so it follows hot reloads just like startup defaults.
func (r *RedisSortedSetFlow) fallbackRequestQueue() string {
	r.queueMu.RLock()
	defer r.queueMu.RUnlock()
	return r.defaultRequestQueueName
}

// queueConfigOf reads the currently live config of one queue. Queue registry
// and config map are swapped together, so hot-path readers cannot observe a
// mixed generation.
func (r *RedisSortedSetFlow) queueConfigOf(queueID string) (SortedSetQueueConfig, bool) {
	r.queueMu.RLock()
	defer r.queueMu.RUnlock()
	cfg, ok := r.configMap[queueID]
	return cfg, ok
}

func (r *RedisSortedSetFlow) processMessages(ctx context.Context, msgChannel chan *api.InternalRequest, queueName string, queueID string, gate pipeline.Gate, logger logr.Logger) {
	cfg, _ := r.queueConfigOf(queueID)
	r.processMessagesWithConfig(ctx, msgChannel, queueName, queueID, gate, logger, cfg)
}

func (r *RedisSortedSetFlow) processMessagesWithConfig(ctx context.Context, msgChannel chan *api.InternalRequest, queueName string, queueID string, gate pipeline.Gate, logger logr.Logger, cfg SortedSetQueueConfig) {
	currentTime := float64(time.Now().Unix())

	budget := gate.Budget(ctx)
	poolName := cfg.WorkerPoolID
	metrics.SetDispatchBudget(budget, queueID, queueName, poolName)
	batchSize := int(math.Floor(float64(r.batchSize) * budget))
	if batchSize <= 0 {
		// Back-pressure here is applied pre-dequeue: the budget shrank the batch
		// to zero, so no message reaches gate.Apply below — the only other site
		// that records a refusal. Count the throttled poll itself, or gate_closed
		// stays silent exactly while the gate is doing its job (#368). Only count
		// when work is actually waiting; an idle queue was not held back.
		depth, err := r.rdb.ZCard(ctx, queueName).Result()
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to read queue depth for a closed gate", "queue", queueName)
		} else if depth > 0 {
			metrics.RecordGateDecision(metrics.ReasonGateClosed, queueID, queueName, poolName)
		}
		return
	}

	// Peek instead of pop (#404): a crash below loses nothing because the
	// claim lease, not the pop, transfers ownership. Scores are kept so
	// release/redelivery restores the original position.
	zs, err := r.rdb.ZRangeByScoreWithScores(ctx, queueName, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   "+inf",
		Count: int64(batchSize),
	}).Result()
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to peek from sorted set")
		return
	}

	peeked, err := r.loadRequests(ctx, zs, currentTime, logger)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to load request payloads", "queue", queueName)
		return
	}

	for _, p := range peeked {
		member, ir, deadline, ok := p.member, p.ir, p.deadline, p.ok
		if !ok || ir == nil || ir.PublicRequest == nil {
			// Unparsable entry: no request identity survives to redeliver,
			// so remove it rather than letting it wedge the peek window.
			if err := r.rdb.ZRem(ctx, queueName, member).Err(); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to remove unparsable message", "queue", queueName)
			}
			continue
		}
		rview := ir.PublicRequest
		reqID := rview.ReqID()

		// Stamp before any terminal outcome: an unstamped result would
		// orphan its claim at ack time.
		if ir.RequestQueueName == "" {
			ir.RequestQueueName = queueName
		}
		if ir.QueueID == "" {
			ir.QueueID = queueID
		}
		if !ir.ResultRoutingResolved {
			if cfg.ResultQueueName != "" {
				ir.ResultQueueName = cfg.ResultQueueName
			}
			ir.ResultTTLSeconds = cfg.ResultTTLSeconds
			ir.ResultRoutingResolved = true
		}

		if len(cfg.Labels) > 0 {
			if ir.Labels == nil {
				ir.Labels = make(map[string]string, len(cfg.Labels))
			}
			for k, v := range cfg.Labels {
				ir.Labels[k] = v
			}
		}

		// Runs off the cancelled ctx so the hand-back still reaches Redis.
		releaseOnShutdown := func(token string, requestToken string) {
			if err := retryRedisOp(context.Background(), func(ctx context.Context) error {
				return r.releaseClaim(ctx, queueName, reqID, requestToken, member, deadline, token)
			}); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to release claim on shutdown", "id", reqID)
			}
		}

		if deadline < currentTime {
			logger.V(logutil.DEFAULT).Info("Deadline expired", "id", reqID)
			metrics.RecordExceededDeadlineReq(queueID, queueName, poolName)
			token, claimed, claimErr := r.claimRequest(ctx, queueName, ir, member, deadline)
			if claimErr != nil {
				logger.V(logutil.DEFAULT).Error(claimErr, "Failed to claim expired request", "id", reqID)
				continue
			}
			if !claimed {
				// Lease lapsed elsewhere; the winning consumer owns the
				// terminal record for this request now.
				continue
			}
			if err := r.cleanupRequestStateByIDAndToken(ctx, reqID, ir.RequestToken); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to cleanup expired request state", "id", reqID)
			}
			// Surface the expiry instead of dropping silently: without a
			// result, a fetch cannot distinguish a request that timed out in
			// the queue from one that never existed.
			select {
			case r.resultChannel <- api.NewDeadlineExceededResult(rview, ir.InternalRouting):
			case <-ctx.Done():
				releaseOnShutdown(token, ir.RequestToken)
				return
			}
			continue
		}

		cancelled, err := r.CancellationChecker().IsCancelled(ctx, reqID, ir.RequestToken)
		if err != nil {
			// Best-effort at dequeue time only. The worker path performs the
			// authoritative pre-dispatch cancellation check and fails closed.
			logger.V(logutil.DEFAULT).Error(err, "Failed to check request cancellation", "id", reqID)
		} else if cancelled {
			token, claimed, claimErr := r.claimRequest(ctx, queueName, ir, member, deadline)
			if claimErr != nil {
				logger.V(logutil.DEFAULT).Error(claimErr, "Failed to claim cancelled request", "id", reqID)
				continue
			}
			if !claimed {
				continue
			}
			select {
			case r.resultChannel <- api.NewCancelledResult(rview, ir.InternalRouting):
			case <-ctx.Done():
				releaseOnShutdown(token, ir.RequestToken)
				return
			}
			continue
		}

		if p.payloadMissing {
			token, claimed, claimErr := r.claimRequest(ctx, queueName, ir, member, deadline)
			if claimErr != nil {
				logger.V(logutil.DEFAULT).Error(claimErr, "Failed to claim request with a missing payload", "id", reqID)
				continue
			}
			if !claimed {
				continue
			}
			select {
			case r.resultChannel <- api.NewErrorResult(rview, ir.InternalRouting, api.ErrCodeInvalidRequest, "request payload is missing"):
			case <-ctx.Done():
				releaseOnShutdown(token, ir.RequestToken)
				return
			}
			continue
		}

		// Apply gate
		var releases []pipeline.GateReleaseFunc
		verdict, err := gate.Apply(ctx, ir, &releases)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Gating failed")
			metrics.RecordGateDecision(metrics.ReasonError, queueID, queueName, poolName)
			// No claim was taken: the entry simply stays pending in the
			// sorted set and is retried on a later poll.
			pipeline.ReleaseGateReleases(releases)
			continue
		}

		if verdict.Action == pipeline.ActionRefuse {
			reason := metrics.ReasonGateClosed
			if ir.GetClassification() == api.ClassificationOverflow {
				reason = metrics.ReasonQuotaExhausted
			}
			metrics.RecordGateDecision(reason, queueID, queueName, poolName)
			// Entry remains pending (peek, not pop): no re-enqueue churn.
			pipeline.ReleaseGateReleases(releases)
			continue
		}

		if verdict.Action == pipeline.ActionDrop {
			metrics.RecordGateDecision(metrics.ReasonDropped, queueID, queueName, poolName)
			var resultMsg api.ResultMessage
			if verdict.Result != nil {
				resultMsg = *verdict.Result
				resultMsg.Routing = ir.InternalRouting
			} else {
				resultMsg = api.NewGateDroppedResult(rview, ir.InternalRouting)
			}
			token, claimed, claimErr := r.claimRequest(ctx, queueName, ir, member, deadline)
			if claimErr != nil {
				logger.V(logutil.DEFAULT).Error(claimErr, "Failed to claim dropped request", "id", reqID)
				continue
			}
			if !claimed {
				continue
			}
			select {
			case r.resultChannel <- resultMsg:
			case <-ctx.Done():
				releaseOnShutdown(token, ir.RequestToken)
				return
			}
			continue
		}

		// Claim before handing downstream: past msgChannel the request
		// lives only in process memory, so this lease is its sole recovery
		// path if the process dies.
		token, claimed, claimErr := r.claimRequest(ctx, queueName, ir, member, deadline)
		if claimErr != nil {
			logger.V(logutil.DEFAULT).Error(claimErr, "Failed to claim request", "id", reqID)
			pipeline.ReleaseGateReleases(releases)
			continue
		}
		if !claimed {
			pipeline.ReleaseGateReleases(releases)
			continue
		}

		if len(releases) > 0 {
			// Defensive: never orphan a lingering reservation for this id — release
			// any prior closure instead of silently overwriting it (see #311).
			if prev, loaded := r.activeReleases.Swap(reqID, releases); loaded {
				if rels, ok := prev.([]pipeline.GateReleaseFunc); ok {
					pipeline.ReleaseGateReleases(rels)
				}
			}
		}

		// Stamp ingestion time as the message enters the in-process buffer so the
		// worker can record queue residence time when it pulls the message.
		ir.IngestionTime = time.Now()

		select {
		case msgChannel <- ir:
		case <-ctx.Done():
			r.activeReleases.Delete(reqID)
			releaseOnShutdown(token, ir.RequestToken)
			pipeline.ReleaseGateReleases(releases)
			return
		}
	}
}

type peekedRequest struct {
	member         string
	ir             *api.InternalRequest
	deadline       float64
	ok             bool
	payloadMissing bool
}

func (r *RedisSortedSetFlow) loadRequests(ctx context.Context, zs []redis.Z, now float64, logger logr.Logger) ([]peekedRequest, error) {
	out := make([]peekedRequest, len(zs))
	var refs []string
	var at []int
	for i, z := range zs {
		member, _ := z.Member.(string)
		ir, deadline, ok := r.parseMessage(member, logger)
		out[i] = peekedRequest{member: member, ir: ir, deadline: deadline, ok: ok}
		if ok && ir != nil && ir.PayloadRef != "" && deadline >= now {
			refs = append(refs, ir.PayloadRef)
			at = append(at, i)
		}
	}
	if len(refs) == 0 {
		return out, nil
	}
	values, err := r.rdb.MGet(ctx, refs...).Result()
	if err != nil {
		return nil, fmt.Errorf("fetch %d request payloads: %w", len(refs), err)
	}
	for j, v := range values {
		p := &out[at[j]]
		payload, found := v.(string)
		if !found {
			p.payloadMissing = true
			continue
		}
		if err := api.AttachPayload(p.ir, json.RawMessage(payload)); err != nil {
			p.payloadMissing = true
		}
	}
	return out, nil
}

func encodeRequest(ir *api.InternalRequest) ([]byte, error) {
	if ir.PayloadRef == "" {
		return json.Marshal(ir)
	}
	envelope, _, err := api.SplitPayload(ir)
	return envelope, err
}

func (r *RedisSortedSetFlow) parseMessage(member string, logger logr.Logger) (*api.InternalRequest, float64, bool) {
	var ir api.InternalRequest
	if err := json.Unmarshal([]byte(member), &ir); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message")
		return nil, 0, false
	}
	if ir.PublicRequest == nil {
		logger.V(logutil.DEFAULT).Error(nil, "Missing specific request in message", "member", member)
		return nil, 0, false
	}
	deadline := ir.PublicRequest.ReqDeadline()
	if deadline <= 0 {
		logger.V(logutil.DEFAULT).Error(nil, "Invalid deadline", "id", ir.PublicRequest.ReqID())
		return &ir, 0, false
	}

	return &ir, float64(deadline), true
}

// Re-queues failed messages with exponential backoff
func (r *RedisSortedSetFlow) retryWorker(ctx context.Context) {
	processMsg := func(processCtx context.Context, msg pipeline.RetryMessage) {
		batch := drainBatch(msg, r.retryChannel, maxBatchSize)
		// #311: a retried request returns to the queue and is re-gated (and thus
		// re-reserved) on its next dispatch, so its current gate reservation must
		// be released here — otherwise inFlight ratchets up on every retry until
		// Budget() reaches 0 and the queue stops dispatching entirely. Release
		// BEFORE re-enqueue: a re-dispatch can only occur after flushRetryBatch's
		// ZAdd, so releasing first prevents the reservation from being overwritten
		// and orphaned.
		for _, m := range batch {
			if m.InternalRequest == nil || m.PublicRequest == nil {
				continue
			}
			if val, ok := r.activeReleases.LoadAndDelete(m.PublicRequest.ReqID()); ok {
				if rels, ok := val.([]pipeline.GateReleaseFunc); ok {
					pipeline.ReleaseGateReleases(rels)
				}
			}
		}
		r.flushRetryBatch(processCtx, batch)
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-r.retryChannel:
					processMsg(context.Background(), msg)
				default:
					return
				}
			}
		case msg := <-r.retryChannel:
			processMsg(ctx, msg)
		}
	}
}

func (r *RedisSortedSetFlow) flushRetryBatch(ctx context.Context, batch []pipeline.RetryMessage) {
	if len(batch) == 0 {
		return
	}

	logger := log.FromContext(ctx)
	type retryEntry struct {
		queue           string
		value           redis.Z
		originQueue     string // where the claim bookkeeping lives
		requestID       string
		requestToken    string
		requestDeadline float64
	}

	entries := make([]retryEntry, 0, len(batch))
	for _, msg := range batch {
		if msg.InternalRequest == nil {
			logger.V(logutil.DEFAULT).Error(nil, "Retry message missing InternalRequest")
			continue
		}
		queueName := msg.RequestQueueName
		if queueName == "" {
			queueName = r.fallbackRequestQueue()
		}
		// Preserve the origin queue in the envelope so the retry mover can
		// re-enter the message into the right queue once it is due.
		msg.RequestQueueName = queueName
		bytes, err := encodeRequest(msg.InternalRequest)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to marshal retry")
			continue
		}

		// Score is the retry-due time. The retry queue is drained by the
		// retry mover strictly at or after this time, so the backoff is
		// enforced. Retries must NOT be ZADDed into the request queue
		// directly: there the score means deadline and a peek would pick a
		// future-scored retry immediately (and ahead of all fresh traffic,
		// since now+backoff sorts below any realistic deadline).
		retryScore := float64(time.Now().Unix()) + msg.BackoffDurationSeconds
		entry := retryEntry{
			queue:       r.retryQueue(),
			value:       redis.Z{Score: retryScore, Member: string(bytes)},
			originQueue: queueName,
		}
		if msg.PublicRequest != nil {
			entry.requestID = msg.PublicRequest.ReqID()
			entry.requestToken = msg.RequestToken
			entry.requestDeadline = float64(msg.PublicRequest.ReqDeadline())
		}
		entries = append(entries, entry)
	}

	if err := retryRedisOp(ctx, func(ctx context.Context) error {
		pipe := r.rdb.Pipeline()
		for _, entry := range entries {
			pipe.ZAdd(ctx, entry.queue, entry.value)
		}
		_, err := pipe.Exec(ctx)
		return err
	}); err != nil {
		// Persistence failed after retries — stop heartbeating so the
		// lease can expire and the request is redelivered via reclaim
		// instead of being orphaned forever.
		for _, entry := range entries {
			if entry.requestID != "" {
				r.claimTokens.Delete(claimKey(entry.requestID, entry.requestToken))
			}
		}
		logger.V(logutil.DEFAULT).Error(err, "Failed to push retry batch, claim released for redelivery", "batchSize", len(batch))
		return
	}
	for _, entry := range entries {
		if entry.requestID == "" {
			continue
		}
		var token string
		if v, ok := r.claimTokens.Load(claimKey(entry.requestID, entry.requestToken)); ok {
			if h, ok := v.(*claimHandle); ok {
				token = h.token
			}
		}
		if token == "" {
			continue
		}
		res, err := r.renewClaim(ctx, entry.originQueue, entry.requestID, entry.requestToken, entry.requestDeadline, token)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to renew claim for retried request", "id", entry.requestID)
			continue
		}
		if res != 1 {
			r.claimTokens.Delete(claimKey(entry.requestID, entry.requestToken))
		}
	}
	logger.V(logutil.DEBUG).Info("Pushed retry batch", "batchSize", len(batch))
}

// Pushes results to Redis list (FIFO)
// Routes results to the queue specified in request metadata, or default queue if not specified.
// Batches multiple results into a single Redis pipeline call to reduce round-trips.
func (r *RedisSortedSetFlow) resultWorker(ctx context.Context) {
	processMsg := func(flushCtx context.Context, msg api.ResultMessage) {
		batch := drainBatch(msg, r.resultChannel, maxBatchSize)
		for _, m := range batch {
			if val, ok := r.activeReleases.LoadAndDelete(m.ID); ok {
				if rels, ok := val.([]pipeline.GateReleaseFunc); ok {
					pipeline.ReleaseGateReleases(rels)
				}
			}
		}
		r.flushResultBatch(flushCtx, batch)
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-r.resultChannel:
					processMsg(context.Background(), msg)
				default:
					return
				}
			}
		case msg := <-r.resultChannel:
			processMsg(ctx, msg)
		}
	}
}

func (r *RedisSortedSetFlow) flushResultBatch(ctx context.Context, batch []api.ResultMessage) {
	logger := log.FromContext(ctx)
	defaultQueue := r.defaultResultQueueName
	pushed := 0
	for _, result := range batch {
		resultQueue := defaultQueue
		var resultTTL int64
		if result.Routing.ResultRoutingResolved {
			if result.Routing.ResultQueueName != "" {
				resultQueue = result.Routing.ResultQueueName
			}
			resultTTL = result.Routing.ResultTTLSeconds
		} else if cfg, hasCfg := r.queueConfigOf(result.Routing.QueueID); hasCfg {
			if cfg.ResultQueueName != "" {
				resultQueue = cfg.ResultQueueName
			} else if result.Routing.ResultQueueName != "" {
				resultQueue = result.Routing.ResultQueueName
			}
			resultTTL = cfg.ResultTTLSeconds
		} else if result.Routing.ResultQueueName != "" {
			resultQueue = result.Routing.ResultQueueName
		}
		listTTL := time.Duration(0)
		if resultTTL > 0 {
			listTTL = time.Duration(resultTTL) * time.Second
		}
		// The claim bookkeeping lives under the request's ORIGIN queue, which
		// need not be the result destination above.
		claimQueue := result.Routing.RequestQueueName
		if claimQueue == "" {
			if cfg, hasCfg := r.queueConfigOf(result.Routing.QueueID); hasCfg {
				claimQueue = cfg.QueueName
			}
		}
		if claimQueue == "" {
			claimQueue = resultQueue
		}

		// Ack atomically pushes the record and drops this instance's claim
		// only if it still owns the lease; stale owners are fenced.
		var ok bool
		err := retryRedisOp(ctx, func(ctx context.Context) error {
			var aerr error
			ok, aerr = r.ackResult(ctx, claimQueue, resultQueue, result.ID, result.Routing.RequestToken, result.Routing.PayloadRef, r.marshalResult(result), listTTL)
			return aerr
		})
		if err != nil {
			// Redis down after retries — drop handle so lease expires and
			// request is redelivered, instead of heartbeating forever.
			r.claimTokens.Delete(claimKey(result.ID, result.Routing.RequestToken))
			logger.V(logutil.DEFAULT).Error(err, "Failed to flush result, claim released for redelivery", "id", result.ID)
			continue
		}
		if !ok {
			continue
		}
		pushed++
		if err := r.cleanupRequestState(ctx, result); err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to cleanup request state after result flush", "id", result.ID)
		}
	}
	logger.V(logutil.DEBUG).Info("Pushed result batch", "batchSize", pushed)
}

func (r *RedisSortedSetFlow) cleanupRequestState(ctx context.Context, result api.ResultMessage) error {
	return r.cleanupRequestStateByIDAndToken(ctx, result.ID, result.Routing.RequestToken)
}

func (r *RedisSortedSetFlow) cleanupRequestStateByIDAndToken(ctx context.Context, requestID, requestToken string) error {
	if requestID == "" || requestToken == "" {
		return nil
	}
	_, err := cleanupRequestStateScript.Run(
		ctx,
		r.rdb,
		[]string{api.RequestActiveTokenKey(requestID), api.RequestCancellationKey(requestID)},
		requestToken,
	).Result()
	return err
}

func (r *RedisSortedSetFlow) marshalResult(msg api.ResultMessage) string {
	if bytes, err := marshalInternalResult(msg); err == nil {
		return string(bytes)
	}
	fallback := map[string]string{"id": msg.ID, "payload": `{"error":"marshal failed"}`}
	fallbackBytes, _ := json.Marshal(fallback)
	return string(fallbackBytes)
}

// retryQueue returns the retry queue name, defaulting for flows constructed
// directly (tests) without ApplyDefaults.
func (r *RedisSortedSetFlow) retryQueue() string {
	if r.retryQueueName == "" {
		return "retry-sortedset"
	}
	return r.retryQueueName
}

// retryMover re-enters due retries into their request queues. Retries wait in
// the retry queue scored by retry-due time; once due, they return to their
// origin queue with the message's original deadline as the score, restoring
// earliest-deadline-first ordering among fresh traffic.
func (r *RedisSortedSetFlow) retryMover(ctx context.Context) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := float64(time.Now().Unix())
			members, err := r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key: r.retryQueue(), ByScore: true,
				Start: "-inf", Stop: fmt.Sprintf("%f", now),
				Count: int64(r.batchSize), Offset: 0,
			}).Result()
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to read due retries", "queue", r.retryQueue())
				continue
			}
			if len(members) == 0 {
				continue
			}
			for _, member := range members {
				// ZRem guards against double-move: only the remover that wins
				// the removal re-enters the message.
				removed, err := r.rdb.ZRem(ctx, r.retryQueue(), member).Result()
				if err != nil || removed == 0 {
					continue
				}
				var ir api.InternalRequest
				if err := json.Unmarshal([]byte(member), &ir); err != nil || ir.PublicRequest == nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to parse due retry, dropping", "member", member[:min(len(member), 120)])
					continue
				}
				queueName := ir.RequestQueueName
				if queueName == "" {
					queueName = r.fallbackRequestQueue()
				}
				if err := r.rdb.ZAdd(ctx, queueName, redis.Z{
					Score:  float64(ir.PublicRequest.ReqDeadline()),
					Member: member,
				}).Err(); err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to re-enter due retry", "queue", queueName)
					// Put it back in the retry queue so it is not lost.
					_ = r.rdb.ZAdd(ctx, r.retryQueue(), redis.Z{Score: now, Member: member}).Err()
				}
			}
		}
	}
}
