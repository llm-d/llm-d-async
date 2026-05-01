package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/redis/go-redis/v9"
)

var _ Producer = (*RedisSortedSetProducer)(nil)

// RedisSortedSetProducer implements Producer using Redis sorted set for requests
// and Redis list for results.
type RedisSortedSetProducer struct {
	client           *redis.Client
	requestQueueName string
	resultQueueName  string
}

// RedisSortedSetConfig contains configuration for the Redis sorted set producer.
type RedisSortedSetConfig struct {
	// RedisAddr is the Redis server address (e.g., "localhost:6379").
	// Required.
	RedisAddr string

	// RedisUser is the username for the Redis server.
	// Optional.
	RedisUser string

	// RedisPassword is the password for the Redis server.
	// Optional.
	RedisPassword string

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
func NewRedisSortedSetProducer(config RedisSortedSetConfig) (*RedisSortedSetProducer, error) {
	if config.RedisAddr == "" {
		return nil, errors.New("RedisAddr is required")
	}

	if config.TenantID == "" {
		return nil, errors.New("TenantID is required for multi-tenant isolation")
	}

	if config.RequestQueueName == "" {
		config.RequestQueueName = "request-sortedset"
	}

	if config.ResultQueueName == "" {
		config.ResultQueueName = "default"
	}

	// Namespace result queue with tenant ID to prevent collisions
	// Format: results:{tenantID}:{customerQueueName}
	namespacedResultQueue := fmt.Sprintf("results:%s:%s", config.TenantID, config.ResultQueueName)

	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Username: config.RedisUser,
		Password: config.RedisPassword,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisSortedSetProducer{
		client:           client,
		requestQueueName: config.RequestQueueName,
		resultQueueName:  namespacedResultQueue,
	}, nil
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

// GetResultWithTimeout retrieves a result with a timeout.
func (p *RedisSortedSetProducer) GetResultWithTimeout(ctx context.Context, timeout time.Duration) (*api.ResultMessage, error) {
	// Use BRPOP with timeout
	result, err := p.client.BRPop(ctx, timeout, p.resultQueueName).Result()
	if err != nil {
		if err == redis.Nil {
			// Timeout occurred
			return nil, nil
		}
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

// Close closes the Redis connection.
func (p *RedisSortedSetProducer) Close() error {
	return p.client.Close()
}

// QueueDepth returns the number of pending requests in the queue.
func (p *RedisSortedSetProducer) QueueDepth(ctx context.Context) (int64, error) {
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
