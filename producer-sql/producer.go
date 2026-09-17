// Package producersql submits requests to and reads results from the llm-d-async sql transport.
package producersql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
)

const (
	defaultRequestQueueName = "request-sql"
	defaultPollInterval     = time.Second
)

type Config struct {
	URL              string
	RequestQueueName string
	ResultQueueName  string
	PollInterval     time.Duration
}

type Option func(*Producer) error

func WithStore(store *sqlqueue.Store) Option {
	return func(p *Producer) error {
		if store == nil {
			return errors.New("store is required")
		}
		p.store = store
		return nil
	}
}

type Producer struct {
	store            *sqlqueue.Store
	managedStore     bool
	requestQueueName string
	resultQueueName  string
	pollInterval     time.Duration
}

func New(ctx context.Context, config Config, opts ...Option) (*Producer, error) {
	if config.ResultQueueName == "" {
		return nil, errors.New("ResultQueueName is required")
	}
	p := &Producer{
		requestQueueName: config.RequestQueueName,
		resultQueueName:  config.ResultQueueName,
		pollInterval:     config.PollInterval,
	}
	if p.requestQueueName == "" {
		p.requestQueueName = defaultRequestQueueName
	}
	if p.pollInterval <= 0 {
		p.pollInterval = defaultPollInterval
	}
	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}
	if p.store == nil {
		if config.URL == "" {
			return nil, errors.New("URL is required when no store is provided via WithStore")
		}
		store, err := sqlqueue.Open(ctx, config.URL)
		if err != nil {
			return nil, fmt.Errorf("failed to open SQL store: %w", err)
		}
		p.store = store
		p.managedStore = true
	}
	return p, nil
}

func (p *Producer) SubmitRequest(ctx context.Context, req api.Request) error {
	return p.SubmitRequests(ctx, []api.Request{req})
}

func (p *Producer) SubmitRequests(ctx context.Context, reqs []api.Request) error {
	rows := make([]sqlqueue.Request, 0, len(reqs))
	for i, req := range reqs {
		row, err := p.toRow(req)
		if err != nil {
			return fmt.Errorf("request %d: %w", i, err)
		}
		rows = append(rows, row)
	}
	if err := p.store.Enqueue(ctx, rows...); err != nil {
		return fmt.Errorf("failed to add requests to queue: %w", err)
	}
	return nil
}

func (p *Producer) toRow(req api.Request) (sqlqueue.Request, error) {
	if req == nil {
		return sqlqueue.Request{}, errors.New("request is required")
	}
	ir := toInternalRequest(req)
	r := ir.PublicRequest
	if r.ReqID() == "" {
		return sqlqueue.Request{}, errors.New("request ID is required")
	}
	deadline := r.ReqDeadline()
	if deadline <= 0 {
		return sqlqueue.Request{}, errors.New("deadline is required and must be a positive Unix timestamp")
	}
	if time.Until(time.Unix(deadline, 0)) <= 0 {
		return sqlqueue.Request{}, errors.New("deadline has already expired")
	}
	if ir.ResultQueueName == "" {
		ir.ResultQueueName = p.resultQueueName
	}
	if ir.RequestQueueName == "" {
		ir.RequestQueueName = p.requestQueueName
	}
	token, err := newToken()
	if err != nil {
		return sqlqueue.Request{}, fmt.Errorf("failed to create request token: %w", err)
	}
	ir.RequestToken = token
	envelope, payload, err := api.SplitPayload(ir)
	if err != nil {
		return sqlqueue.Request{}, fmt.Errorf("failed to marshal request: %w", err)
	}
	if payload == nil {
		payload = json.RawMessage{}
	}
	return sqlqueue.Request{
		ID:       r.ReqID(),
		Token:    token,
		Queue:    ir.RequestQueueName,
		Deadline: deadline,
		Envelope: string(envelope),
		Payload:  payload,
	}, nil
}

func (p *Producer) CancelRequests(ctx context.Context, requestIDs []string) error {
	if err := p.store.Cancel(ctx, requestIDs); err != nil {
		return fmt.Errorf("failed to mark requests as cancelled: %w", err)
	}
	return nil
}

func (p *Producer) GetResult(ctx context.Context) (*api.ResultMessage, error) {
	results, err := p.GetResults(ctx, 1)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("failed to get result: empty result batch")
	}
	return results[0], nil
}

// GetResults deletes the results it returns. An unparsable result comes back as an error result.
func (p *Producer) GetResults(ctx context.Context, limit int) ([]*api.ResultMessage, error) {
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	for {
		rows, err := p.store.PopResults(ctx, p.resultQueueName, time.Now().Unix(), limit)
		if err != nil {
			return nil, fmt.Errorf("failed to get results: %w", err)
		}
		if len(rows) > 0 {
			out := make([]*api.ResultMessage, 0, len(rows))
			for _, row := range rows {
				res, err := parseResult(row.Payload)
				if err != nil {
					res = &api.ResultMessage{
						ID:           row.ID,
						ErrorCode:    api.ErrCodeInferenceError,
						ErrorMessage: fmt.Sprintf("unparsable result payload: %v", err),
					}
					res.Routing.RequestToken = row.Token
				}
				out = append(out, res)
			}
			return out, nil
		}
		timer := time.NewTimer(p.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("failed to get results: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (p *Producer) Close() error {
	if !p.managedStore {
		return nil
	}
	if err := p.store.Close(); err != nil {
		return fmt.Errorf("close SQL store: %w", err)
	}
	return nil
}

func toInternalRequest(req api.Request) *api.InternalRequest {
	ir := &api.InternalRequest{}
	switch v := req.(type) {
	case *api.RequestMessage:
		cp := *v
		ir.PublicRequest = &cp
	case *api.RedisRequest:
		cp := *v
		ir.RequestQueueName = cp.RequestQueueName
		ir.ResultQueueName = cp.ResultQueueName
		ir.PublicRequest = &cp
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
	}
	return ir
}

type resultEnvelope struct {
	api.ResultMessage
	RequestToken string `json:"request_token,omitempty"`
}

func parseResult(data string) (*api.ResultMessage, error) {
	var result resultEnvelope
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}
	if result.ID == "" {
		return nil, errors.New("result missing 'id' field")
	}
	result.Routing.RequestToken = result.RequestToken
	return &result.ResultMessage, nil
}

func newToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
