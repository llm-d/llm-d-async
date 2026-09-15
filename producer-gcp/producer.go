package producergcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/llm-d/llm-d-async/api"
)

const (
	newProducerTimeout              = 30 * time.Second
	requestAckDeadlineSeconds       = 600
	resultAckDeadlineSeconds        = 60
	maxDeliveryAttempts             = 5
	minRetryBackoff                 = 10 * time.Second
	maxRetryBackoff                 = 600 * time.Second
	defaultResultSubscriptionExpiry = 7 * 24 * time.Hour
	minPubSubResourceIDLen          = 3
	maxPubSubResourceIDLen          = 255
)

var _ api.Producer = (*Producer)(nil)

// Config identifies the Pub/Sub resources a producer publishes to and reads from.
type Config struct {
	// ProjectID is the GCP project that owns the topics and subscriptions.
	ProjectID string

	// RequestTopicID is the topic requests are published to.
	RequestTopicID string

	// RequestSubscriptionID must match the processor's subscriber_id.
	RequestSubscriptionID string

	// ResultTopicID is the topic the processor publishes results to.
	ResultTopicID string

	// ResultRoute is the result_route attribute value used to filter this
	// producer's result subscription. Required.
	ResultRoute string

	// ResultSubscriptionID is the result subscription ID. Defaults to ResultRoute.
	// Producers that share ResultRoute and ResultSubscriptionID compete on one
	// subscription. Distinct subscription IDs with the same route each get a copy.
	ResultSubscriptionID string

	// DeadLetterTopicID defaults to RequestTopicID + "-dlq".
	DeadLetterTopicID string

	// DeadLetterSubscriptionID defaults to DeadLetterTopicID + "-sub".
	DeadLetterSubscriptionID string
}

// Option configures a Producer.
type Option func(*Producer) error

// WithPubSubClient injects a pre-configured client (tests, tracing hooks, emulator).
// The caller retains ownership; Close() will not close it.
func WithPubSubClient(client *pubsub.Client) Option {
	return func(p *Producer) error {
		if client == nil {
			return errors.New("WithPubSubClient: client must not be nil")
		}
		p.client = client
		return nil
	}
}

// WithoutCreateResources skips topic and subscription provisioning. Use this
// when the caller has a publish-only identity or resources are managed elsewhere.
func WithoutCreateResources() Option {
	return func(p *Producer) error {
		p.createResources = false
		return nil
	}
}

// Producer implements api.Producer against GCP Pub/Sub.
type Producer struct {
	client                   *pubsub.Client
	managedClient            bool
	publisher                *pubsub.Publisher
	projectID                string
	requestTopicID           string
	requestSubscriptionID    string
	resultTopicID            string
	resultRoute              string
	resultSubscriptionID     string
	deadLetterTopicID        string
	deadLetterSubscriptionID string
	createResources          bool
	receiveMu                sync.Mutex
}

// NewProducer creates a Pub/Sub producer. By default it idempotently creates
// the request topic, request subscription, result topic, this producer's
// filtered result subscription, and a DLQ topic plus subscription.
func NewProducer(cfg Config, opts ...Option) (*Producer, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	p := &Producer{
		projectID:                cfg.ProjectID,
		requestTopicID:           cfg.RequestTopicID,
		requestSubscriptionID:    cfg.RequestSubscriptionID,
		resultTopicID:            cfg.ResultTopicID,
		resultRoute:              cfg.ResultRoute,
		resultSubscriptionID:     cfg.ResultSubscriptionID,
		deadLetterTopicID:        cfg.DeadLetterTopicID,
		deadLetterSubscriptionID: cfg.DeadLetterSubscriptionID,
		createResources:          true,
	}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), newProducerTimeout)
	defer cancel()

	if p.client == nil {
		client, err := pubsub.NewClient(ctx, cfg.ProjectID)
		if err != nil {
			return nil, fmt.Errorf("create pubsub client: %w", err)
		}
		p.client = client
		p.managedClient = true
	}

	if p.createResources {
		if err := p.ensureResources(ctx); err != nil {
			_ = p.Close()
			return nil, err
		}
	}

	p.publisher = p.client.Publisher(p.requestTopicID)
	return p, nil
}

// SubmitRequest publishes a plain RequestMessage. Caller metadata is copied to
// message attributes and result_route is set to this producer's configured route.
func (p *Producer) SubmitRequest(ctx context.Context, req api.Request) error {
	if p.publisher == nil {
		return errors.New("producer is closed")
	}
	if req == nil {
		return errors.New("request is required")
	}
	if req.ReqID() == "" {
		return errors.New("request ID is required")
	}
	deadline := req.ReqDeadline()
	if deadline <= 0 {
		return errors.New("deadline is required and must be a positive Unix timestamp")
	}
	if time.Unix(deadline, 0).Before(time.Now()) {
		return errors.New("deadline has already expired")
	}

	body := api.RequestMessage{
		ID:       req.ReqID(),
		Created:  req.ReqCreated(),
		Deadline: deadline,
		Payload:  req.ReqPayload(),
		Metadata: req.ReqMetadata(),
		Headers:  req.ReqHeaders(),
		Endpoint: req.ReqEndpoint(),
	}
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	_, err = p.publisher.Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: requestAttributes(req.ReqMetadata(), p.resultRoute),
	}).Get(ctx)
	if err != nil {
		return fmt.Errorf("publish request: %w", err)
	}
	return nil
}

// CancelRequests is not supported on Pub/Sub. An empty ID list is a no-op;
// otherwise ErrNotSupported is returned. The consume path has no cancel check.
func (p *Producer) CancelRequests(_ context.Context, requestIDs []string) error {
	if len(requestIDs) == 0 {
		return nil
	}
	return api.ErrNotSupported
}

// GetResult blocks until a result matching this producer's result_route is
// delivered or ctx is cancelled. Concurrent GetResult calls on the same
// Producer are serialized.
func (p *Producer) GetResult(ctx context.Context) (*api.ResultMessage, error) {
	if p.publisher == nil || p.client == nil {
		return nil, errors.New("producer is closed")
	}

	p.receiveMu.Lock()
	defer p.receiveMu.Unlock()

	sub := p.client.Subscriber(p.resultSubscriptionID)
	sub.ReceiveSettings.MaxOutstandingMessages = 1
	sub.ReceiveSettings.NumGoroutines = 1

	recvCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		result   *api.ResultMessage
		parseErr error
	)
	err := sub.Receive(recvCtx, func(_ context.Context, msg *pubsub.Message) {
		var r api.ResultMessage
		if uerr := json.Unmarshal(msg.Data, &r); uerr != nil {
			msg.Ack()
			parseErr = fmt.Errorf("unmarshal result: %w", uerr)
			cancel()
			return
		}
		if r.ID == "" {
			msg.Ack()
			parseErr = errors.New("result missing 'id' field")
			cancel()
			return
		}
		result = &r
		msg.Ack()
		cancel()
	})
	if result != nil {
		return result, nil
	}
	if parseErr != nil {
		return nil, parseErr
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to get result: %w", ctx.Err())
		}
		return nil, fmt.Errorf("failed to get result: %w", err)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("failed to get result: %w", ctx.Err())
	}
	return nil, errors.New("failed to get result: receive ended without a message")
}

// Close stops the publisher and, when the producer owns the client, closes it.
func (p *Producer) Close() error {
	if p.publisher != nil {
		p.publisher.Stop()
		p.publisher = nil
	}
	if p.managedClient && p.client != nil {
		err := p.client.Close()
		p.client = nil
		if err != nil {
			return fmt.Errorf("close pubsub client: %w", err)
		}
	}
	return nil
}

func validateConfig(cfg *Config) error {
	if cfg.ProjectID == "" {
		return errors.New("ProjectID is required")
	}
	for _, field := range []struct {
		name, value string
	}{
		{"RequestTopicID", cfg.RequestTopicID},
		{"RequestSubscriptionID", cfg.RequestSubscriptionID},
		{"ResultTopicID", cfg.ResultTopicID},
		{"ResultRoute", cfg.ResultRoute},
	} {
		if err := validateResourceID(field.name, field.value); err != nil {
			return err
		}
	}
	if cfg.ResultSubscriptionID == "" {
		cfg.ResultSubscriptionID = cfg.ResultRoute
	} else if err := validateResourceID("ResultSubscriptionID", cfg.ResultSubscriptionID); err != nil {
		return err
	}
	if cfg.DeadLetterTopicID == "" {
		cfg.DeadLetterTopicID = cfg.RequestTopicID + "-dlq"
	}
	if err := validateResourceID("DeadLetterTopicID", cfg.DeadLetterTopicID); err != nil {
		return err
	}
	if cfg.DeadLetterSubscriptionID == "" {
		cfg.DeadLetterSubscriptionID = cfg.DeadLetterTopicID + "-sub"
	}
	if err := validateResourceID("DeadLetterSubscriptionID", cfg.DeadLetterSubscriptionID); err != nil {
		return err
	}
	return nil
}

func validateResourceID(field, id string) error {
	if id == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(id) < minPubSubResourceIDLen || len(id) > maxPubSubResourceIDLen {
		return fmt.Errorf("%s must be between %d and %d characters", field, minPubSubResourceIDLen, maxPubSubResourceIDLen)
	}
	if strings.HasPrefix(strings.ToLower(id), "goog") {
		return fmt.Errorf("%s must not start with goog", field)
	}
	first := id[0]
	if first < 'A' || (first > 'Z' && first < 'a') || first > 'z' {
		return fmt.Errorf("%s must start with a letter", field)
	}
	for i := 1; i < len(id); i++ {
		if !isResourceIDChar(id[i]) {
			return fmt.Errorf("%s must start with a letter and contain only letters, numbers, dashes, underscores, periods, tildes, plus, or percent", field)
		}
	}
	return nil
}

func isResourceIDChar(c byte) bool {
	switch {
	case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		return true
	case c == '-' || c == '_' || c == '.' || c == '~' || c == '%' || c == '+':
		return true
	default:
		return false
	}
}

func requestAttributes(metadata map[string]string, route string) map[string]string {
	attrs := make(map[string]string, len(metadata)+1)
	for k, v := range metadata {
		attrs[k] = v
	}
	attrs[api.ResultRouteAttribute] = route
	return attrs
}

func topicResource(project, id string) string {
	return fmt.Sprintf("projects/%s/topics/%s", project, id)
}

func subscriptionResource(project, id string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", project, id)
}

func resultRouteFilter(route string) string {
	return fmt.Sprintf("attributes.%s = %q", api.ResultRouteAttribute, route)
}
