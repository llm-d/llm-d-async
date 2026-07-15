package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/llm-d/llm-d-async/pkg/plugins"
)

// PluginSpec is one entry in the transform configuration: a uniquely-named
// instance of a registered plugin type, plus opaque parameters passed verbatim
// to the plugin's factory. Mirrors EPP's plugin configuration shape.
type PluginSpec struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Parameters json.RawMessage `json:"parameters,omitempty"`
}

// Config is the top-level transform plugins configuration. Transforms are keyed
// by direction so request and (in the future) response plugins can share a
// single config file. Only requestTransforms is consumed today; responseTransforms
// is reserved for the symmetric response body-transform extension point (#259).
type Config struct {
	RequestTransforms []PluginSpec `json:"requestTransforms"`
}

// LoadConfig reads and parses a transform plugins configuration JSON file. The
// file is a JSON object with transforms grouped by direction, e.g.
// {"requestTransforms": [ ... ]}. Unknown top-level fields are rejected so a
// typo (or a not-yet-supported direction such as responseTransforms) fails
// loudly instead of being silently ignored. It does not instantiate plugins;
// pass the result to BuildChain.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path from trusted CLI flag
	if err != nil {
		return Config{}, fmt.Errorf("failed to read transform config file: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal transform config: %w", err)
	}
	return cfg, nil
}

// BuildChain instantiates the configured transforms in order, registering each
// instance on handle, and returns the resulting Chain. Plugin names must be
// non-empty and unique; each type must be registered with plugins.Register and
// the instantiated plugin must implement RequestTransform.
//
// An empty spec list returns an empty (no-op) Chain.
func BuildChain(specs []PluginSpec, handle plugins.Handle) (*Chain, error) {
	transforms := make([]RequestTransform, 0, len(specs))
	seen := make(map[string]bool, len(specs))

	for i, spec := range specs {
		if spec.Name == "" {
			return nil, fmt.Errorf("transform plugin at index %d has an empty name", i)
		}
		if spec.Type == "" {
			return nil, fmt.Errorf("transform plugin %q is missing a type", spec.Name)
		}
		if seen[spec.Name] {
			return nil, fmt.Errorf("duplicate transform plugin name %q", spec.Name)
		}
		seen[spec.Name] = true

		factory, ok := plugins.Lookup(spec.Type)
		if !ok {
			return nil, fmt.Errorf("transform plugin %q has unregistered type %q", spec.Name, spec.Type)
		}

		plugin, err := factory(spec.Name, spec.Parameters, handle)
		if err != nil {
			return nil, fmt.Errorf("failed to create transform plugin %q (type %q): %w", spec.Name, spec.Type, err)
		}

		t, ok := plugin.(RequestTransform)
		if !ok {
			return nil, fmt.Errorf("transform plugin %q (type %q) does not implement RequestTransform", spec.Name, spec.Type)
		}
		handle.AddPlugin(spec.Name, plugin)
		transforms = append(transforms, t)
	}

	return NewChain(transforms), nil
}
