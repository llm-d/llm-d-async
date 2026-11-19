// Package plugins provides a small, transport-agnostic plugin framework
// modeled on the Endpoint Picker (EPP) plugin model
// (sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins). It defines the
// base Plugin identity, a configuration-driven factory Registry, and a Handle
// that gives plugins access to shared, host-provided context.
//
// This package is intentionally free of Kubernetes/controller-runtime
// dependencies: the EPP-specific RunnablePlugin and pod-listing concerns are not
// replicated, since they have no analog for in-process request plugins.
package plugins

const separator = "/"

// TypedName is a utility struct providing a type and a name to plugins.
type TypedName struct {
	// Type is the registered plugin type (the key under which a FactoryFunc is
	// registered in the Registry).
	Type string
	// Name is the instance name of a plugin, unique within a configuration.
	Name string
}

// String returns the type and name rendered as "<name>/<type>".
func (tn TypedName) String() string {
	return tn.Name + separator + tn.Type
}
