package plugins

import (
	"encoding/json"
	"fmt"
)

// FactoryFunc instantiates a plugin from its instance name, raw JSON parameters
// (as found in configuration), and a Handle to host-provided context. The
// signature mirrors EPP so plugin authoring is familiar across the projects.
type FactoryFunc func(name string, parameters json.RawMessage, handle Handle) (Plugin, error)

// registry maps a plugin type to its FactoryFunc. It is unexported so the only
// ways to mutate it are Register/MustRegister and the only way to read it is
// Lookup, keeping the package free of exported mutable global state.
var registry = map[string]FactoryFunc{}

// Register registers a plugin factory under the given type. It returns an error
// if the type is already registered, so duplicate registrations fail loudly
// rather than silently overwriting.
func Register(pluginType string, factory FactoryFunc) error {
	if _, exists := registry[pluginType]; exists {
		return fmt.Errorf("plugin type %q is already registered", pluginType)
	}
	registry[pluginType] = factory
	return nil
}

// MustRegister is like Register but panics on error. It is intended for use in
// plugin package init() functions, where a duplicate registration is a
// programming error that should fail at startup.
func MustRegister(pluginType string, factory FactoryFunc) {
	if err := Register(pluginType, factory); err != nil {
		panic(err)
	}
}

// Lookup returns the factory registered under the given type, if any.
func Lookup(pluginType string) (FactoryFunc, bool) {
	factory, ok := registry[pluginType]
	return factory, ok
}

// Unregister removes the factory registered under the given type, if any. It is
// primarily intended to let tests clean up registrations they make so the
// registry stays isolated between test runs.
func Unregister(pluginType string) {
	delete(registry, pluginType)
}
