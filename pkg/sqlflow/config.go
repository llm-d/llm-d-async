package sqlflow

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
)

type QueueConfig struct {
	ID                 string `json:"id,omitempty"`
	QueueName          string `json:"queue_name,omitempty"`
	ResultQueueName    string `json:"result_queue_name,omitempty"`
	ResultTTLSeconds   int64  `json:"result_ttl_seconds,omitempty"`
	WorkerPoolID       string `json:"worker_pool_id"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
	IGWBaseURL         string `json:"igw_base_url"`
	pipeline.GateConfig
	Labels map[string]string `json:"labels,omitempty"`
}

type Config struct {
	URL                   string        `json:"url,omitempty"`
	ResultQueueName       string        `json:"result_queue_name,omitempty"`
	PollIntervalMs        int           `json:"poll_interval_ms,omitempty"`
	BatchSize             int           `json:"batch_size,omitempty"`
	ResultBatchSize       int           `json:"result_batch_size,omitempty"`
	EnableTracing         bool          `json:"enable_tracing,omitempty"`
	CancelCheckBatchSize  int           `json:"cancel_check_batch_size,omitempty"`
	CancelCheckLingerMs   int           `json:"cancel_check_linger_ms,omitempty"`
	LeaseTTLSeconds       int64         `json:"lease_ttl_seconds,omitempty"`
	MaxConnections        int           `json:"max_connections,omitempty"`
	HandoffTimeoutSeconds int64         `json:"handoff_timeout_seconds,omitempty"`
	Queues                []QueueConfig `json:"queues"`
}

func LoadConfig(data []byte) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse sql transport config: %w", err)
	}
	if v := os.Getenv("SQL_URL"); v != "" {
		cfg.URL = v
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid sql transport config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) ApplyDefaults() {
	if c.ResultQueueName == "" {
		c.ResultQueueName = "result-sql"
	}
	if c.PollIntervalMs == 0 {
		c.PollIntervalMs = 1000
	}
	if c.BatchSize == 0 {
		c.BatchSize = 10
	}
	if c.ResultBatchSize == 0 {
		c.ResultBatchSize = 32
	}
	if c.CancelCheckBatchSize == 0 {
		c.CancelCheckBatchSize = 256
	}
	if c.CancelCheckLingerMs == 0 {
		c.CancelCheckLingerMs = 5
	}
	if c.LeaseTTLSeconds == 0 {
		c.LeaseTTLSeconds = 30
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = sqlqueue.DefaultMaxConns
	}
	if c.HandoffTimeoutSeconds == 0 {
		c.HandoffTimeoutSeconds = 900
	}
	for i := range c.Queues {
		q := &c.Queues[i]
		if q.WorkerPoolID == "" {
			q.WorkerPoolID = "default"
		}
		if q.RequestPathURL == "" {
			q.RequestPathURL = "/v1/completions"
		}
		if q.ID == "" {
			q.ID = q.QueueName
		}
	}
}

func (c *Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("url is required (set url in the transport config or SQL_URL)")
	}
	if c.PollIntervalMs < 0 {
		return fmt.Errorf("poll_interval_ms must be non-negative")
	}
	if c.BatchSize < 0 {
		return fmt.Errorf("batch_size must be non-negative")
	}
	if c.ResultBatchSize < 0 {
		return fmt.Errorf("result_batch_size must be non-negative")
	}
	if c.CancelCheckBatchSize < 1 {
		return fmt.Errorf("cancel_check_batch_size must be positive")
	}
	if c.CancelCheckLingerMs < 0 {
		return fmt.Errorf("cancel_check_linger_ms must be non-negative")
	}
	if c.LeaseTTLSeconds < 0 {
		return fmt.Errorf("lease_ttl_seconds must be non-negative")
	}
	if c.MaxConnections < 0 {
		return fmt.Errorf("max_connections must be non-negative")
	}
	if c.HandoffTimeoutSeconds < 0 {
		return fmt.Errorf("handoff_timeout_seconds must be non-negative")
	}
	if len(c.Queues) == 0 {
		return fmt.Errorf("at least one queue is required")
	}
	seenID := make(map[string]bool, len(c.Queues))
	seenName := make(map[string]bool, len(c.Queues))
	for _, q := range c.Queues {
		if q.QueueName == "" {
			return fmt.Errorf("queue_name is required for each queue")
		}
		if seenID[q.ID] {
			return fmt.Errorf("duplicate queue id %q", q.ID)
		}
		seenID[q.ID] = true
		if seenName[q.QueueName] {
			return fmt.Errorf("duplicate queue name %q", q.QueueName)
		}
		seenName[q.QueueName] = true
		if q.IGWBaseURL == "" {
			return fmt.Errorf("queue %q: igw_base_url must be specified", q.QueueName)
		}
	}
	return nil
}
