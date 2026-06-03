package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
)

// PoolConfig defines the configuration for a dedicated inference worker pool,
// isolating its gateway routing, worker count, and default HTTP request headers.
//
// "Pool" here is a config-level grouping concept, NOT a binding to the
// Kubernetes InferencePool CRD from sigs.k8s.io/gateway-api-inference-
// extension. This struct doesn't reference, watch, or import the
// InferencePool API type; the async-processor never reads InferencePool
// objects from the cluster. Pools in this config are just a string ID
// and a bag of HTTP-dispatch fields.
//
// When the destination is fronted by an IGW EndpointPicker (EPP),
// operators typically align Pool.ID with the IGW InferencePool name by
// convention. For destinations that aren't IGW-fronted (external
// providers, plain HTTP servers, etc.), Pool.ID is a free-form
// operator label with no IGW meaning.
type PoolConfig struct {
	ID             string            `json:"id"`
	IGWBaseURL     string            `json:"igw_base_url"`
	RequestPathURL string            `json:"request_path_url"`
	Workers        int               `json:"workers"`
	HTTPHeaders    map[string]string `json:"http_headers,omitempty"`
}

// LoadPools loads and parses a pools JSON config file.
func LoadPools(path string) ([]PoolConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pools []PoolConfig
	if err := json.Unmarshal(data, &pools); err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	for _, p := range pools {
		if p.ID == "" {
			return nil, fmt.Errorf("pool config has empty ID")
		}
		if seen[p.ID] {
			return nil, fmt.Errorf("duplicate pool ID: %q", p.ID)
		}
		seen[p.ID] = true
	}

	return pools, nil
}
