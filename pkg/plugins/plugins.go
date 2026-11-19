package plugins

// Plugin defines the base interface every plugin must satisfy. Typed capability
// interfaces (e.g. a request body-transform) embed this interface and add their
// own methods, mirroring the EPP plugin model.
type Plugin interface {
	// TypedName returns the type and name tuple of this plugin instance.
	TypedName() TypedName
}
