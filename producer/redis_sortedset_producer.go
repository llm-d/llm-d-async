package producer

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
)

var (
	_ Producer              = (*RedisSortedSetProducer)(nil)
	_ DurableResultProducer = (*RedisSortedSetProducer)(nil)
)

// RedisSortedSetProducer implements Producer using Redis sorted set for requests
// and Redis list for results.
type RedisSortedSetProducer struct {
	client        redis.UniversalClient
	managedClient bool
	// clusterKeys selects the hash-tagged layout for private keys (enqueue
	// sequence, result claims) so each script stays within one Redis
	// Cluster slot, and splits the per-request markers out of scripts.
	clusterKeys                bool
	requestQueueName           string
	resultQueueName            string
	resultClaimLeaseTTL        time.Duration
	resultClaimReclaimInterval time.Duration
	payloadKeys                bool
}

const (
	cancellationMarkerTTL = 7 * 24 * time.Hour
	payloadTTLGrace       = 10 * time.Minute
)

// submitRequestScript assigns the enqueue sequence and enqueues the request
// as ARGV[4] .. seq .. ARGV[5], scored as api.InternalRequest.QueueScore.
// Standalone only: its keys span several cluster slots.
var submitRequestScript = redis.NewScript(`
local seq = redis.call("INCR", KEYS[1])
redis.call("EXPIREAT", KEYS[1], ARGV[3])
redis.call("DEL", KEYS[2])
redis.call("SET", KEYS[3], ARGV[1], "PX", ARGV[2])
local score = tonumber(ARGV[6]) + math.min(seq, 2097151) / 2097152
redis.call("ZADD", KEYS[4], string.format("%.17g", score), ARGV[4] .. seq .. ARGV[5])
if KEYS[5] then
  redis.call("SET", KEYS[5], ARGV[7], "PX", ARGV[8])
end
return seq
`)

// enqueueRequestScript is the cluster-mode half of submitRequestScript: the
// sequence counter and the queue share a slot, while the per-request markers
// are written beforehand with single-key commands.
// KEYS: seq, queue. ARGV: seqExpireAt, jsonBefore, jsonAfter, deadline.
var enqueueRequestScript = redis.NewScript(`
local seq = redis.call("INCR", KEYS[1])
redis.call("EXPIREAT", KEYS[1], ARGV[1])
local score = tonumber(ARGV[4]) + math.min(seq, 2097151) / 2097152
redis.call("ZADD", KEYS[2], string.format("%.17g", score), ARGV[2] .. seq .. ARGV[3])
return seq
`)

const (
	enqueueSeqPlaceholder     = -1
	enqueueSeqFieldJSON       = `"enqueue_seq":`
	enqueueSeqPlaceholderJSON = enqueueSeqFieldJSON + "-1"
	enqueueSeqGrace           = time.Hour
)

// enqueueSeqKey names the per-queue, per-deadline sequence counter. In
// cluster mode the queue segment is hash-tagged so the counter shares the
// queue's slot and the enqueue stays atomic.
func enqueueSeqKey(queueName string, deadline int64, cluster bool) string {
	return fmt.Sprintf("request-seq:%s:%d", slotKey(queueName, cluster), deadline)
}

// ProducerOption is a functional option for NewRedisSortedSetProducer.
type ProducerOption func(*RedisSortedSetProducer) error

// WithRedisClient injects a pre-configured client (standalone, cluster or
// failover), allowing callers to instrument it (e.g. with OpenTelemetry
// tracing/metrics hooks) before use. When provided, RedisURL and the other
// Redis* fields in the config are not required.
// The caller retains ownership of the client; Close() will not close it.
func WithRedisClient(client redis.UniversalClient) ProducerOption {
	return func(p *RedisSortedSetProducer) error {
		if client == nil {
			return errors.New("WithRedisClient: client must not be nil")
		}
		p.client = client
		return nil
	}
}

// WithResultClaimLeaseTTL configures the crash-detection window for durable
// result deliveries. The default is five minutes.
func WithResultClaimLeaseTTL(ttl time.Duration) ProducerOption {
	return func(p *RedisSortedSetProducer) error {
		if ttl <= 0 {
			return errors.New("WithResultClaimLeaseTTL: duration must be positive")
		}
		p.resultClaimLeaseTTL = ttl
		return nil
	}
}

// WithResultClaimReclaimInterval configures how often ReceiveResult checks for
// pending results and expired claims. The default is one second.
func WithResultClaimReclaimInterval(interval time.Duration) ProducerOption {
	return func(p *RedisSortedSetProducer) error {
		if interval <= 0 {
			return errors.New("WithResultClaimReclaimInterval: duration must be positive")
		}
		p.resultClaimReclaimInterval = interval
		return nil
	}
}

// WithPayloadKeys stores each request's payload under its own Redis key.
func WithPayloadKeys() ProducerOption {
	return func(p *RedisSortedSetProducer) error {
		p.payloadKeys = true
		return nil
	}
}

// RedisSortedSetConfig contains configuration for the Redis sorted set producer.
type RedisSortedSetConfig struct {
	// RedisURL is a Redis URL (e.g. "redis://user:pass@host:port/db" or "rediss://..." for TLS).
	// Required unless a client is injected via WithRedisClient.
	RedisURL string

	// RedisMode selects the topology: RedisModeStandalone (default),
	// RedisModeCluster or RedisModeSentinel. Credentials, TLS and tuning
	// still come from RedisURL. Ignored when a client is injected.
	RedisMode string

	// RedisAddrs lists additional cluster seed nodes or sentinel addresses
	// beyond the host in RedisURL. Only valid with cluster or sentinel mode.
	RedisAddrs []string

	// RedisMasterName is the sentinel-monitored master. Required in
	// sentinel mode.
	RedisMasterName string

	// RequestQueueName is the name of the Redis sorted set for requests.
	// Typically shared across all tenants.
	// Default: "request-sortedset"
	RequestQueueName string

	// ResultQueueName is the full Redis list key for results.
	// Must match the dispatcher/consumer result_queue_name configuration.
	// Example: "llm-d-async:results:pool-a:$batch"
	ResultQueueName string
}

// NewRedisSortedSetProducer creates a new producer using Redis sorted set.
// Use WithRedisClient to inject a pre-configured client (e.g. with tracing hooks).
func NewRedisSortedSetProducer(config RedisSortedSetConfig, opts ...ProducerOption) (*RedisSortedSetProducer, error) {
	if config.ResultQueueName == "" {
		return nil, errors.New("ResultQueueName is required")
	}

	if config.RequestQueueName == "" {
		config.RequestQueueName = "request-sortedset"
	}

	p := &RedisSortedSetProducer{
		requestQueueName:           config.RequestQueueName,
		resultQueueName:            config.ResultQueueName,
		resultClaimLeaseTTL:        defaultResultClaimLeaseTTL,
		resultClaimReclaimInterval: defaultResultClaimReclaimInterval,
	}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	if p.client == nil {
		client, err := newRedisClient(config)
		if err != nil {
			return nil, err
		}
		p.client = client
		p.managedClient = true
	}
	p.clusterKeys = isClusterClient(p.client)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return p, nil
}

// toInternalRequest builds an InternalRequest with routing merged from the concrete
// *RedisRequest / *PubSubRequest, or a *RequestMessage / default Request message view.
func toInternalRequest(req api.Request) *api.InternalRequest {
	ir := &api.InternalRequest{InternalRouting: api.InternalRouting{}}
	switch v := req.(type) {
	case *api.RequestMessage:
		if v == nil {
			return ir
		}
		cp := *v
		ir.PublicRequest = &cp
		return ir
	case *api.RedisRequest:
		if v == nil {
			return ir
		}
		ir2 := *v
		if ir2.RequestQueueName != "" {
			ir.RequestQueueName = ir2.RequestQueueName
		}
		if ir2.ResultQueueName != "" {
			ir.ResultQueueName = ir2.ResultQueueName
		}
		ir.PublicRequest = &ir2
		return ir
	case *api.PubSubRequest:
		if v == nil {
			return ir
		}
		ir2 := *v
		if ir2.PubSubID != "" {
			ir.TransportCorrelationID = ir2.PubSubID
		}
		ir.PublicRequest = &ir2
		return ir
	default:
		ir.PublicRequest = &api.RequestMessage{
			ID:       req.ReqID(),
			Created:  req.ReqCreated(),
			Deadline: req.ReqDeadline(),
			Payload:  req.ReqPayload(),
			Metadata: req.ReqMetadata(),
			Headers:  req.ReqHeaders(),
			Endpoint: req.ReqEndpoint(),
			Model:    req.ReqModel(),
		}
		return ir
	}
}

// SubmitRequest adds a request to the Redis sorted set, scored by QueueScore.
func (p *RedisSortedSetProducer) SubmitRequest(ctx context.Context, req api.Request) error {
	if req == nil {
		return errors.New("request is required")
	}
	ir := toInternalRequest(req)
	r := ir.PublicRequest
	if r == nil {
		return errors.New("request is required")
	}

	if r.ReqID() == "" {
		return errors.New("request ID is required")
	}

	deadline := r.ReqDeadline()
	if deadline <= 0 {
		return errors.New("deadline is required and must be a positive Unix timestamp")
	}

	// Apply producer-level defaults for queue routing if not set by caller
	if ir.ResultQueueName == "" {
		ir.ResultQueueName = p.resultQueueName
	}

	if ir.RequestQueueName == "" {
		ir.RequestQueueName = p.requestQueueName
	}
	activeTTL := time.Until(time.Unix(deadline, 0))
	if activeTTL <= 0 {
		return errors.New("deadline has already expired")
	}
	token, err := newRequestToken()
	if err != nil {
		return fmt.Errorf("failed to create request token: %w", err)
	}
	ir.RequestToken = token
	ir.EnqueueSeq = enqueueSeqPlaceholder

	if payload := r.ReqPayload(); payload != nil {
		trimmed := bytes.TrimSpace(payload)
		if !json.Valid(trimmed) || (trimmed[0] != '{' && string(trimmed) != "null") {
			return errors.New("invalid payload: must be a JSON object or null")
		}
	}

	var envelope, payload []byte
	if p.payloadKeys {
		ir.PayloadRef = api.RequestPayloadKey(r.ReqID(), token)
		envelope, payload, err = api.SplitPayload(ir)
	} else {
		envelope, err = json.Marshal(ir)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}
	at := bytes.Index(envelope, []byte(enqueueSeqPlaceholderJSON))
	if at < 0 {
		return errors.New("marshaled request is missing the enqueue_seq placeholder")
	}

	// Clear any stale cancellation marker for this request ID before enqueue.
	// This prevents a previously cancelled/completed request ID from poisoning
	// a later submission that legitimately reuses the same ID.
	targetQueue := ir.RequestQueueName
	seqExpireAt := time.Unix(deadline, 0).Add(enqueueSeqGrace).Unix()
	payloadTTL := activeTTL + payloadTTLGrace
	if p.clusterKeys {
		err = p.enqueueCluster(ctx, r.ReqID(), ir.RequestToken, targetQueue, deadline, activeTTL, seqExpireAt,
			envelope[:at+len(enqueueSeqFieldJSON)], envelope[at+len(enqueueSeqPlaceholderJSON):],
			ir.PayloadRef, payload, payloadTTL)
	} else {
		keys := []string{
			enqueueSeqKey(targetQueue, deadline, false),
			api.RequestCancellationKey(r.ReqID()),
			api.RequestActiveTokenKey(r.ReqID()),
			targetQueue,
		}
		args := []any{
			ir.RequestToken,
			max(activeTTL.Milliseconds(), 1),
			seqExpireAt,
			envelope[:at+len(enqueueSeqFieldJSON)],
			envelope[at+len(enqueueSeqPlaceholderJSON):],
			deadline,
		}
		if ir.PayloadRef != "" {
			keys = append(keys, ir.PayloadRef)
			args = append(args, payload, payloadTTL.Milliseconds())
		}
		err = submitRequestScript.Run(ctx, p.client, keys, args...).Err()
	}
	if err != nil {
		return fmt.Errorf("failed to add request to queue: %w", err)
	}

	return nil
}

// enqueueCluster is SubmitRequest's write path when the keys cannot share a
// slot. The markers and the payload are written first with single-key
// commands, then the sequence stamp and ZADD run in one script on the queue's
// slot. If the script fails, the marker and payload merely outlive a request
// that never enqueued, and expire with the deadline.
func (p *RedisSortedSetProducer) enqueueCluster(ctx context.Context, reqID, token, queue string, deadline int64, activeTTL time.Duration, seqExpireAt int64, jsonBefore, jsonAfter []byte, payloadRef string, payload []byte, payloadTTL time.Duration) error {
	pipe := p.client.Pipeline()
	pipe.Del(ctx, api.RequestCancellationKey(reqID))
	pipe.Set(ctx, api.RequestActiveTokenKey(reqID), token, activeTTL)
	if payloadRef != "" {
		pipe.Set(ctx, payloadRef, payload, payloadTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return enqueueRequestScript.Run(ctx, p.client,
		[]string{enqueueSeqKey(queue, deadline, true), queue},
		seqExpireAt, jsonBefore, jsonAfter, deadline,
	).Err()
}

// CancelRequests marks request IDs as cancelled so dequeue/dispatch paths can drop them.
func (p *RedisSortedSetProducer) CancelRequests(ctx context.Context, requestIDs []string) error {
	if len(requestIDs) == 0 {
		return nil
	}

	// The marker copies the active generation token so a later submission
	// reusing the ID is not cancelled with it. Two single-key commands
	// rather than one script: the keys sit in different cluster slots, and
	// a resubmission racing the copy only ever leaves the older token in the
	// marker, which the dispatcher already treats as "not this generation".
	for _, requestID := range requestIDs {
		if requestID == "" {
			continue
		}
		active, err := p.client.Get(ctx, api.RequestActiveTokenKey(requestID)).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to mark request %q as cancelled: %w", requestID, err)
		}
		if err := p.client.Set(ctx, api.RequestCancellationKey(requestID), active, cancellationMarkerTTL).Err(); err != nil {
			return fmt.Errorf("failed to mark request %q as cancelled: %w", requestID, err)
		}
	}
	return nil
}

func newRequestToken() (string, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return hex.EncodeToString(token), nil
}

// GetResult retrieves a result from the Redis list, blocking until one is available.
// Cancellation is checked between one-second blocking waits, so an empty queue
// can delay observing cancellation by approximately one second.
func (p *RedisSortedSetProducer) GetResult(ctx context.Context) (*api.ResultMessage, error) {
	// A zero BRPOP timeout blocks indefinitely: go-redis does not interrupt an
	// in-flight read when the context is cancelled. Bound each empty wait so we
	// can check the context without leaving a background pop consuming results.
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("failed to get result: %w", err)
		}
		result, err := p.client.BRPop(ctx, time.Second, p.resultQueueName).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get result: %w", err)
		}

		// BRPOP returns [queueName, value]
		if len(result) != 2 {
			return nil, errors.New("unexpected BRPOP result format")
		}

		return p.parseResult(result[1])
	}
}

// parseResult parses a JSON result message.
func (p *RedisSortedSetProducer) parseResult(data string) (*api.ResultMessage, error) {
	return parseInternalResult(data)
}

// Close closes the Redis connection if the client was created internally.
// Externally injected clients (via WithRedisClient) are not closed.
func (p *RedisSortedSetProducer) Close() error {
	if p.managedClient {
		return p.client.Close()
	}
	return nil
}

// RequestQueueDepth returns the number of pending requests in the queue.
func (p *RedisSortedSetProducer) RequestQueueDepth(ctx context.Context) (int64, error) {
	return p.client.ZCard(ctx, p.requestQueueName).Result()
}

// ResultQueueDepth returns the number of results waiting to be consumed.
func (p *RedisSortedSetProducer) ResultQueueDepth(ctx context.Context) (int64, error) {
	return p.client.LLen(ctx, p.resultQueueName).Result()
}

// ClearRequestQueue removes all pending requests from the queue.
func (p *RedisSortedSetProducer) ClearRequestQueue(ctx context.Context) error {
	return p.client.Del(ctx, p.requestQueueName).Err()
}

// ClearResultQueue removes all results from the queue.
func (p *RedisSortedSetProducer) ClearResultQueue(ctx context.Context) error {
	keys := newResultClaimKeys(p.resultQueueName, p.clusterKeys)
	// One DEL per key: a multi-key DEL across slots is illegal in cluster
	// mode, and the pending list never shares a slot with the others there.
	for _, key := range []string{keys.pending, keys.claimed, keys.owners, keys.idx, keys.tombstones} {
		if err := p.client.Del(ctx, key).Err(); err != nil {
			return err
		}
	}
	return nil
}
