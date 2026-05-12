package pipeline

import (
	"fmt"
	"sort"
	"sync"
)

// MergePolicyDeps carries the shared dependencies a registered policy
// factory may consult when constructing a RequestMergePolicy. Most
// fields are optional; parameterless policies (e.g. random-robin)
// ignore them.
type MergePolicyDeps struct {
	GateFactory GateFactory
	// Config is free-form policy-specific config sourced from CLI
	// flags or a configmap. Registered factories interpret keys they
	// recognize.
	Config map[string]string
}

// MergePolicyFactory constructs a RequestMergePolicy from shared deps.
// Factories are invoked once per process by NewMergePolicy.
type MergePolicyFactory func(deps MergePolicyDeps) (RequestMergePolicy, error)

var (
	mergePolicyMu       sync.RWMutex
	mergePolicyRegistry = map[string]MergePolicyFactory{}
)

// RegisterMergePolicy registers a RequestMergePolicy factory under the given
// name. Intended to be called from package init() so callers can select a
// policy by name at startup. Panics if the name is already registered.
func RegisterMergePolicy(name string, factory MergePolicyFactory) {
	mergePolicyMu.Lock()
	defer mergePolicyMu.Unlock()
	if _, exists := mergePolicyRegistry[name]; exists {
		panic(fmt.Sprintf("pipeline: merge policy %q already registered", name))
	}
	mergePolicyRegistry[name] = factory
}

// NewMergePolicy builds a RequestMergePolicy by name. Returns an error
// if no factory has been registered under the given name. Most callers
// pass a fully-populated MergePolicyDeps; parameterless policies ignore
// unused fields.
func NewMergePolicy(name string, deps MergePolicyDeps) (RequestMergePolicy, error) {
	mergePolicyMu.RLock()
	factory, ok := mergePolicyRegistry[name]
	mergePolicyMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown request merge policy %q (registered: %v)", name, registeredMergePolicies())
	}
	return factory(deps)
}

func registeredMergePolicies() []string {
	mergePolicyMu.RLock()
	defer mergePolicyMu.RUnlock()
	names := make([]string, 0, len(mergePolicyRegistry))
	for name := range mergePolicyRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
