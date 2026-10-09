package producergcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/llm-d/llm-d-async/api"
)

const (
	newProducerTimeout        = 30 * time.Second
	requestAckDeadlineSeconds = 600
	resultAckDeadlineSeconds  = 60
	maxDeliveryAttempts       = 5
	minRetryBackoff           = 10 * time.Second
	maxRetryBackoff           = 600 * time.Second
	minPubSubResourceIDLen    = 3
	maxPubSubResourceIDLen    = 255
	maxSubscriptionFilterLen  = 256
	resultReceiveBuffer       = 16
)

var errProducerClosed = errors.New("producer is closed")

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

	// ResultSubscriptionID is the result subscription ID. Defaults to ResultRoute,
	// which must then be a valid Pub/Sub resource ID.
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
	// results is filled by one Receive. Messages stay unacked until GetResult
	// takes them, so a crash redelivers anything still in this buffer.
	results    chan receivedResult
	recvCtx    context.Context
	recvCancel context.CancelFunc
	recvWG     sync.WaitGroup
	recvMu     sync.Mutex
	receiving  bool
	streamDone chan struct{}
	recvErr    error
	closeOnce  sync.Once
	closed     chan struct{}
}

type receivedResult struct {
	result *api.ResultMessage
	err    error
	ack    func()
	nack   func()
}

// NewProducer creates a Pub/Sub producer. ctx bounds resource provisioning.
// When ctx has no deadline, provisioning stops after 30s.
// By default it idempotently creates the request topic, request subscription,
// result topic, this producer's filtered result subscription, and a DLQ topic
// plus subscription. None of those subscriptions expire.
func NewProducer(ctx context.Context, cfg Config, opts ...Option) (*Producer, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	recvCtx, recvCancel := context.WithCancel(context.Background())
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
		results:                  make(chan receivedResult, resultReceiveBuffer),
		recvCtx:                  recvCtx,
		recvCancel:               recvCancel,
		closed:                   make(chan struct{}),
	}

	opened := false
	defer func() {
		if !opened {
			_ = p.Close()
		}
	}()

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	provisionCtx, cancel := provisionContext(ctx)
	defer cancel()

	if p.client == nil {
		client, err := pubsub.NewClient(provisionCtx, cfg.ProjectID)
		if err != nil {
			return nil, fmt.Errorf("create pubsub client: %w", err)
		}
		p.client = client
		p.managedClient = true
	}

	if p.createResources {
		if err := p.ensureResources(provisionCtx); err != nil {
			return nil, err
		}
	}

	p.publisher = p.client.Publisher(p.requestTopicID)
	opened = true
	return p, nil
}

// provisionContext uses the caller's deadline when one is set.
// Otherwise it limits provisioning to newProducerTimeout.
func provisionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, newProducerTimeout)
}

// SubmitRequest publishes a plain RequestMessage. Caller metadata stays in the
// body. The only message attribute is result_route, set to this producer's route.
func (p *Producer) SubmitRequest(ctx context.Context, req api.Request) error {
	if err := p.errIfClosed(); err != nil {
		return err
	}
	// An interface holding a typed nil (e.g. (*api.RequestMessage)(nil)) passes
	// a plain nil check but panics in the accessors below.
	if req == nil || (reflect.ValueOf(req).Kind() == reflect.Pointer && reflect.ValueOf(req).IsNil()) {
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
		Attributes: map[string]string{api.ResultRouteAttribute: p.resultRoute},
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
// delivered or ctx is cancelled. The first call starts one Receive. Later
// calls drain that stream, and the message is acknowledged only after this
// call takes it. If Receive returns an error, this call returns that error
// and the next call starts a new Receive.
func (p *Producer) GetResult(ctx context.Context) (*api.ResultMessage, error) {
	if ctx.Err() != nil {
		return nil, fmt.Errorf("failed to get result: %w", ctx.Err())
	}
	if err := p.errIfClosed(); err != nil {
		return nil, err
	}
	done := p.ensureReceive()

	// A result already in the buffer is handed off before a dead stream.
	select {
	case item := <-p.results:
		return takeResult(item)
	default:
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("failed to get result: %w", ctx.Err())
	case <-p.closed:
		return nil, errProducerClosed
	case <-done:
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to get result: %w", ctx.Err())
		}
		if err := p.errIfClosed(); err != nil {
			return nil, err
		}
		select {
		case item := <-p.results:
			return takeResult(item)
		default:
			return nil, p.resultStreamErr()
		}
	case item := <-p.results:
		return takeResult(item)
	}
}

func takeResult(item receivedResult) (*api.ResultMessage, error) {
	if item.err != nil {
		return nil, item.err
	}
	if item.ack != nil {
		item.ack()
	}
	return item.result, nil
}

// ensureReceive starts a Receive unless one is already running. A previous
// stream that returned an error does not stick; the next call starts another.
func (p *Producer) ensureReceive() <-chan struct{} {
	p.recvMu.Lock()
	defer p.recvMu.Unlock()
	select {
	case <-p.closed:
		return p.closed
	default:
	}
	if p.receiving {
		return p.streamDone
	}
	p.receiving = true
	p.recvErr = nil
	done := make(chan struct{})
	p.streamDone = done
	p.recvWG.Add(1)
	go p.receiveResults(p.recvCtx, done)
	return done
}

func (p *Producer) receiveResults(ctx context.Context, done chan struct{}) {
	defer p.recvWG.Done()
	defer close(done)

	sub := p.client.Subscriber(p.resultSubscriptionID)
	sub.ReceiveSettings.MaxOutstandingMessages = resultReceiveBuffer
	sub.ReceiveSettings.NumGoroutines = 1
	err := sub.Receive(ctx, p.enqueueResult)

	p.recvMu.Lock()
	p.receiving = false
	if err != nil && ctx.Err() == nil {
		p.recvErr = err
	}
	p.recvMu.Unlock()
}

func (p *Producer) enqueueResult(ctx context.Context, msg *pubsub.Message) {
	item := decodeResult(msg)
	if item.err != nil {
		// Poison is acked here so it is not redelivered.
		msg.Ack()
		select {
		case p.results <- item:
		case <-ctx.Done():
		}
		return
	}
	select {
	case p.results <- receivedResult{result: item.result, ack: msg.Ack, nack: msg.Nack}:
	case <-ctx.Done():
		msg.Nack()
	}
}

func decodeResult(msg *pubsub.Message) receivedResult {
	var result api.ResultMessage
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return receivedResult{err: fmt.Errorf("unmarshal result: %w", err)}
	}
	if result.ID == "" {
		return receivedResult{err: errors.New("result missing 'id' field")}
	}
	return receivedResult{result: &result}
}

func (p *Producer) resultStreamErr() error {
	if err := p.errIfClosed(); err != nil {
		return err
	}
	p.recvMu.Lock()
	err := p.recvErr
	p.recvMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to get result: %w", err)
	}
	return errors.New("failed to get result: receive ended without a message")
}

func (p *Producer) errIfClosed() error {
	select {
	case <-p.closed:
		return errProducerClosed
	default:
		return nil
	}
}

// Close stops the result receive and the publisher. When the producer owns
// the client, it closes that too. The fields stay set so a call racing with
// Close fails on the stopped publisher or the closed producer.
func (p *Producer) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.recvMu.Lock()
		p.recvCancel()
		close(p.closed)
		done := p.streamDone
		p.recvMu.Unlock()

		drained := make(chan struct{})
		go func() {
			defer close(drained)
			p.nackBuffered(done)
		}()
		p.recvWG.Wait()
		<-drained
		if p.publisher != nil {
			p.publisher.Stop()
		}
		if p.managedClient && p.client != nil {
			if cerr := p.client.Close(); cerr != nil {
				err = fmt.Errorf("close pubsub client: %w", cerr)
			}
		}
	})
	return err
}

// nackBuffered releases results that were queued but never handed to GetResult.
// When a stream is still running, it keeps nacking until that stream exits so
// Receive can finish. Close then redelivers those results instead of dropping them.
func (p *Producer) nackBuffered(done <-chan struct{}) {
	if done == nil {
		p.drainResults()
		return
	}
	for {
		select {
		case item := <-p.results:
			nackResult(item)
		case <-done:
			p.drainResults()
			return
		}
	}
}

func (p *Producer) drainResults() {
	for {
		select {
		case item := <-p.results:
			nackResult(item)
		default:
			return
		}
	}
}

func nackResult(item receivedResult) {
	if item.nack != nil {
		item.nack()
	}
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
	} {
		if err := validateResourceID(field.name, field.value); err != nil {
			return err
		}
	}
	if cfg.ResultRoute == "" {
		return errors.New("ResultRoute is required")
	}
	if cfg.ResultSubscriptionID == "" {
		// The route is the subscription ID in this case, so it has to be a
		// legal Pub/Sub resource name as well as a filter value.
		if err := validateResourceID("ResultRoute", cfg.ResultRoute); err != nil {
			return err
		}
		cfg.ResultSubscriptionID = cfg.ResultRoute
	} else if err := validateResourceID("ResultSubscriptionID", cfg.ResultSubscriptionID); err != nil {
		return err
	}
	if err := validateResultRouteFilter(cfg.ResultRoute); err != nil {
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

func validateResultRouteFilter(route string) error {
	filter := resultRouteFilter(route)
	if len(filter) > maxSubscriptionFilterLen {
		return fmt.Errorf("ResultRoute produces a subscription filter of %d bytes, over the %d byte Pub/Sub limit", len(filter), maxSubscriptionFilterLen)
	}
	return nil
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
