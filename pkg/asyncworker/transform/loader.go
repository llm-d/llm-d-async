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
// by direction so request and response plugins share a single config file.
type Config struct {
	RequestTransforms  []PluginSpec `json:"requestTransforms"`
	ResponseTransforms []PluginSpec `json:"responseTransforms"`
}

// LoadConfig reads and parses a transform plugins configuration JSON file. The
// file is a JSON object with transforms grouped by direction, e.g.
// {"requestTransforms": [ ... ], "responseTransforms": [ ... ]}. Unknown
// top-level fields are rejected so a typo fails loudly instead of being silently
// ignored. It does not instantiate plugins; pass the result to
// BuildChainFromConfig.
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

// BuildChain instantiates the configured request transforms in order,
// registering each instance on handle, and returns the resulting Chain. Plugin
// names must be non-empty and unique; each type must be registered with
// plugins.Register and the instantiated plugin must implement RequestTransform.
//
// An empty spec list returns an empty (no-op) Chain. Use BuildChainFromConfig
// to also build response transforms.
func BuildChain(specs []PluginSpec, handle plugins.Handle) (*Chain, error) {
	return BuildChainFromConfig(Config{RequestTransforms: specs}, handle)
}

// BuildChainFromConfig instantiates the configured request and response
// transforms in order, registering each instance on handle, and returns the
// resulting Chain. Plugin names must be non-empty and unique across both
// directions; each type must be registered with plugins.Register, and the
// instantiated plugin must implement RequestTransform or ResponseTransform for
// its direction.
//
// An empty config returns an empty (no-op) Chain.
func BuildChainFromConfig(cfg Config, handle plugins.Handle) (*Chain, error) {
	seen := make(map[string]bool, len(cfg.RequestTransforms)+len(cfg.ResponseTransforms))

	requests := make([]RequestTransform, 0, len(cfg.RequestTransforms))
	if err := buildPlugins(cfg.RequestTransforms, handle, seen, "RequestTransform", func(p plugins.Plugin) bool {
		t, ok := p.(RequestTransform)
		if ok {
			requests = append(requests, t)
		}
		return ok
	}); err != nil {
		return nil, err
	}

	responses := make([]ResponseTransform, 0, len(cfg.ResponseTransforms))
	if err := buildPlugins(cfg.ResponseTransforms, handle, seen, "ResponseTransform", func(p plugins.Plugin) bool {
		t, ok := p.(ResponseTransform)
		if ok {
			responses = append(responses, t)
		}
		return ok
	}); err != nil {
		return nil, err
	}

	return NewChain(requests, responses...), nil
}

// buildPlugins instantiates specs in order and registers each plugin on handle.
// accept reports whether a plugin implements the capability required for its
// direction, and keeps it when it does. seen tracks names across directions.
func buildPlugins(specs []PluginSpec, handle plugins.Handle, seen map[string]bool, capability string, accept func(plugins.Plugin) bool) error {
	for i, spec := range specs {
		if spec.Name == "" {
			return fmt.Errorf("transform plugin at index %d has an empty name", i)
		}
		if spec.Type == "" {
			return fmt.Errorf("transform plugin %q is missing a type", spec.Name)
		}
		if seen[spec.Name] {
			return fmt.Errorf("duplicate transform plugin name %q", spec.Name)
		}
		seen[spec.Name] = true

		factory, ok := plugins.Lookup(spec.Type)
		if !ok {
			return fmt.Errorf("transform plugin %q has unregistered type %q", spec.Name, spec.Type)
		}

		plugin, err := factory(spec.Name, spec.Parameters, handle)
		if err != nil {
			return fmt.Errorf("failed to create transform plugin %q (type %q): %w", spec.Name, spec.Type, err)
		}

		if !accept(plugin) {
			return fmt.Errorf("transform plugin %q (type %q) does not implement %s", spec.Name, spec.Type, capability)
		}
		handle.AddPlugin(spec.Name, plugin)
	}
	return nil
}
