package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/redis/go-redis/v9"
)

var _ Producer = (*RedisSortedSetProducer)(nil)

// RedisSortedSetProducer implements Producer using Redis sorted set for requests
// and Redis list for results.
type RedisSortedSetProducer struct {
	client           *redis.Client
	managedClient    bool
	requestQueueName string
	resultQueueName  string
}

// ProducerOption is a functional option for NewRedisSortedSetProducer.
type ProducerOption func(*RedisSortedSetProducer)

// WithRedisClient injects a pre-configured *redis.Client, allowing callers to
// instrument it (e.g. with OpenTelemetry tracing/metrics hooks) before use.
// When provided, RedisURL in the config is not required.
func WithRedisClient(client *redis.Client) ProducerOption {
	return func(p *RedisSortedSetProducer) {
		p.client = client
	}
}

// RedisSortedSetConfig contains configuration for the Redis sorted set producer.
type RedisSortedSetConfig struct {
	// RedisURL is a Redis URL (e.g. "redis://user:pass@host:port/db" or "rediss://..." for TLS).
	// Required.
	RedisURL string

	// TenantID is the unique identifier for the tenant/customer.
	// Required for multi-tenant isolation.
	// System should assign this (e.g., from auth token, API key).
	// Example: "tenant-abc123", "customer-xyz"
	TenantID string

	// RequestQueueName is the name of the Redis sorted set for requests.
	// Typically shared across all tenants.
	// Default: "request-sortedset"
	RequestQueueName string

	// ResultQueueName is the customer's choice for their result queue name.
	// Will be namespaced as: results:{tenantID}:{resultQueueName}
	// Default: "default"
	ResultQueueName string
}

// NewRedisSortedSetProducer creates a new producer using Redis sorted set.
// Multi-tenant safe: TenantID ensures result queue isolation between tenants.
// Use WithRedisClient to inject a pre-configured client (e.g. with tracing hooks).
func NewRedisSortedSetProducer(config RedisSortedSetConfig, opts ...ProducerOption) (*RedisSortedSetProducer, error) {
	if config.TenantID == "" {
		return nil, errors.New("TenantID is required for multi-tenant isolation")
	}

	if strings.Contains(config.TenantID, ":") {
		return nil, errors.New("TenantID must not contain ':' ")
	}

	if config.RequestQueueName == "" {
		config.RequestQueueName = "request-sortedset"
	}

	if config.ResultQueueName == "" {
		config.ResultQueueName = "default"
	}

	namespacedResultQueue := fmt.Sprintf("results:%s:%s", config.TenantID, config.ResultQueueName)

	p := &RedisSortedSetProducer{
		requestQueueName: config.RequestQueueName,
		resultQueueName:  namespacedResultQueue,
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.client == nil {
		if config.RedisURL == "" {
			return nil, errors.New("RedisURL is required when no RedisClient is provided via WithRedisClient")
		}
		redisOpts, err := redis.ParseURL(config.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse RedisURL: %w", err)
		}
		p.client = redis.NewClient(redisOpts)
		p.managedClient = true
	}

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
		cp := *v
		ir.PublicRequest = &cp
		return ir
	case *api.RedisRequest:
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
		}
		return ir
	}
}

// SubmitRequest adds a request to the Redis sorted set.
// The score is the deadline, ensuring earlier deadlines are processed first.
func (p *RedisSortedSetProducer) SubmitRequest(ctx context.Context, req api.Request) error {
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

	if deadline <= time.Now().Unix() {
		return errors.New("deadline has already expired")
	}

	// Apply producer-level defaults for queue routing if not set by caller
	if ir.ResultQueueName == "" {
		ir.ResultQueueName = p.resultQueueName
	}

	if ir.RequestQueueName == "" {
		ir.RequestQueueName = p.requestQueueName
	}

	msgBytes, err := json.Marshal(ir)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Add to sorted set with deadline as score
	targetQueue := ir.RequestQueueName
	score := float64(deadline)
	if err := p.client.ZAdd(ctx, targetQueue, redis.Z{
		Score:  score,
		Member: string(msgBytes),
	}).Err(); err != nil {
		return fmt.Errorf("failed to add request to queue: %w", err)
	}

	return nil
}

// GetResult retrieves a result from the Redis list, blocking until one is available.
func (p *RedisSortedSetProducer) GetResult(ctx context.Context) (*api.ResultMessage, error) {
	// Use BRPOP (blocking right pop) to wait for a result
	result, err := p.client.BRPop(ctx, 0, p.resultQueueName).Result()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get result: %w", err)
	}

	// BRPOP returns [queueName, value]
	if len(result) != 2 {
		return nil, errors.New("unexpected BRPOP result format")
	}

	return p.parseResult(result[1])
}

// parseResult parses a JSON result message.
func (p *RedisSortedSetProducer) parseResult(data string) (*api.ResultMessage, error) {
	var result api.ResultMessage
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	if result.ID == "" {
		return nil, errors.New("result missing 'id' field")
	}

	return &result, nil
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
	return p.client.Del(ctx, p.resultQueueName).Err()
}
