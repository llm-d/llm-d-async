package mergepolicy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"
	_ "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/tierpriority"
)

// MergePolicySpec represents the JSON configuration structure for a request merge policy.
type MergePolicySpec struct {
	Type       string          `json:"type"`
	Parameters json.RawMessage `json:"parameters,omitempty"`
}

// LoadMergePolicyConfig reads and parses a request merge policy configuration JSON file.
func LoadMergePolicyConfig(path string) (MergePolicySpec, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path from trusted CLI flag
	if err != nil {
		return MergePolicySpec{}, fmt.Errorf("failed to read merge policy config file: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cfg MergePolicySpec
	if err := dec.Decode(&cfg); err != nil {
		return MergePolicySpec{}, fmt.Errorf("failed to unmarshal merge policy config: %w", err)
	}
	return cfg, nil
}
