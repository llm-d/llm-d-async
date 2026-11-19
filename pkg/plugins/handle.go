package plugins

import "context"

// Handle provides plugins a set of standard data and tools to work with. It is a
// trimmed, Kubernetes-free analog of EPP's Handle: it carries a context and the
// set of instantiated plugins, but omits EPP's pod-listing concerns.
type Handle interface {
	// Context returns a context the plugins can use, if they need one.
	Context() context.Context

	HandlePlugins
}

// HandlePlugins defines a set of APIs to work with instantiated plugins.
type HandlePlugins interface {
	// Plugin returns the named plugin instance, or nil if none is registered.
	Plugin(name string) Plugin

	// AddPlugin adds a plugin to the set of known plugin instances.
	AddPlugin(name string, plugin Plugin)
}

var (
	_ Handle        = (*handle)(nil)
	_ HandlePlugins = (*handlePlugins)(nil)
)

// handle is the default Handle implementation.
type handle struct {
	ctx context.Context
	HandlePlugins
}

// Context returns a context the plugins can use, if they need one.
func (h *handle) Context() context.Context {
	return h.ctx
}

// handlePlugins implements the set of APIs to work with instantiated plugins.
type handlePlugins struct {
	plugins map[string]Plugin
}

// Plugin returns the named plugin instance.
func (h *handlePlugins) Plugin(name string) Plugin {
	return h.plugins[name]
}

// AddPlugin adds a plugin to the set of known plugin instances.
func (h *handlePlugins) AddPlugin(name string, plugin Plugin) {
	h.plugins[name] = plugin
}

// NewHandle returns a Handle backed by the given context and an empty plugin set.
func NewHandle(ctx context.Context) Handle {
	return &handle{
		ctx:           ctx,
		HandlePlugins: &handlePlugins{plugins: map[string]Plugin{}},
	}
}
