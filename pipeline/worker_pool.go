package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
)

// WorkerPoolConfig defines the configuration for a worker pool,
// specifying the concurrency limit (number of workers) and its ID.
type WorkerPoolConfig struct {
	ID         string         `json:"id"`
	Workers    int            `json:"workers"`
	GateType   string         `json:"gate_type,omitempty"`
	GateParams map[string]any `json:"gate_params,omitempty"`
	// DeadlineScaling, when set, makes the backlog poller publish the replicas
	// the pool's deployment needs to finish its queues' items before their
	// deadlines.
	DeadlineScaling *DeadlineScaling `json:"deadline_scaling,omitempty"`
}

// DeadlineScaling is a deadline_scaling block, on a worker pool or a queue.
type DeadlineScaling struct {
	// ThroughputRPS is the number of items one replica finishes per second (R).
	// On a pool, a queue's own deadline_scaling overrides it for that queue.
	ThroughputRPS float64 `json:"throughput_rps"`
	// ColdStartSeconds is how long a new replica takes to start serving (C).
	ColdStartSeconds float64 `json:"cold_start_seconds"`
	// MaxReplicas caps the result. Items due before a new replica could start,
	// but not yet expired, pin the result to it.
	MaxReplicas int `json:"max_replicas"`
	// SafetyFactor, in (0, 1], shrinks the time a replica has for the items due
	// within a horizon, to absorb the coarseness of the buckets. 0 means 1.
	SafetyFactor float64 `json:"safety_factor,omitempty"`
}

// LoadWorkerPools loads and validates worker pool configurations from a JSON file.
// It ensures that all pool IDs are non-empty and unique.
func LoadWorkerPools(path string) ([]WorkerPoolConfig, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path from trusted config flag
	if err != nil {
		return nil, fmt.Errorf("failed to read pool config file: %w", err)
	}

	var pools []WorkerPoolConfig
	if err := json.Unmarshal(data, &pools); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pool config: %w", err)
	}

	seenIDs := make(map[string]bool)
	for i, pool := range pools {
		if pool.ID == "" {
			return nil, fmt.Errorf("pool config at index %d has an empty ID", i)
		}
		if pool.Workers <= 0 {
			return nil, fmt.Errorf("pool %q must have at least 1 worker", pool.ID)
		}
		if seenIDs[pool.ID] {
			return nil, fmt.Errorf("duplicate pool ID found: %q", pool.ID)
		}
		seenIDs[pool.ID] = true
	}

	return pools, nil
}
