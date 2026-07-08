package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// StringMap is a map[string]string that tolerates non-string JSON values
// by converting them to their string representation during unmarshaling.
type StringMap map[string]string

func (m *StringMap) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	result := make(map[string]string, len(raw))
	for k, v := range raw {
		switch val := v.(type) {
		case string:
			result[k] = val
		case float64:
			result[k] = strconv.FormatFloat(val, 'f', -1, 64)
		case bool:
			result[k] = strconv.FormatBool(val)
		case nil:
			result[k] = ""
		default:
			return fmt.Errorf("gate_params key %q: unsupported value type %T (only strings, numbers, and booleans are allowed)", k, v)
		}
	}
	*m = result
	return nil
}

// WorkerPoolConfig defines the configuration for a worker pool,
// specifying the concurrency limit (number of workers) and its ID.
type WorkerPoolConfig struct {
	ID         string    `json:"id"`
	Workers    int       `json:"workers"`
	GateType   string    `json:"gate_type,omitempty"`
	GateParams StringMap `json:"gate_params,omitempty"`
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
