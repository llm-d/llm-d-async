package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
)

// WorkerPoolConfig defines the configuration for a worker pool,
// specifying the concurrency limit (number of workers) and its ID.
type WorkerPoolConfig struct {
	ID      string `json:"id"`
	Workers int    `json:"workers"`
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
