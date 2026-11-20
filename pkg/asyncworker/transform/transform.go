// Package transform defines the request body-transform extension point: a typed
// plugin capability that lets providers rewrite the outgoing HTTP body at
// dispatch time (e.g. from OpenAI-style JSON into multipart/form-data) based on
// per-message metadata, while leaving the default JSON path untouched when no
// plugin applies.
//
// It builds on the generic plugin framework in pkg/plugins (EPP-style Plugin,
// Registry, Handle). Concrete transforms (such as a gcs_uri multipart provider)
// are separate plugins that register a factory under their own type.
package transform

import (
	"context"

	"github.com/llm-d/llm-d-async/pkg/plugins"
)

// RequestTransform is a dispatch-time body-transform plugin. The worker invokes
// the configured chain after marshalling the request payload and before sending
// it to the inference backend. A plugin that does not recognize a message
// returns handled=false from Transform, preserving the default JSON path.
type RequestTransform interface {
	plugins.Plugin

	// Validate runs before dispatch. It returns a non-nil error to reject the
	// message outright (the worker treats this as fatal/non-retryable) — for
	// example, a signed object URL that expires before reqDeadline. Plugins that
	// do not recognize the message must return nil.
	//
	// reqDeadline is the request deadline in Unix seconds.
	Validate(payload []byte, metadata map[string]string, reqDeadline int64) error

	// Transform optionally rewrites the outgoing body. When it recognizes the
	// message it returns the new body, the Content-Type to set on the request,
	// and handled=true. Otherwise it returns handled=false and the default JSON
	// body is used unchanged.
	Transform(payload []byte, metadata map[string]string) (body []byte, contentType string, handled bool, err error)
}

// ContextRequestTransform is the context-aware form of RequestTransform.
// Plugins that perform I/O must implement it so their work honors cancellation,
// request deadlines, and worker shutdown. RequestURL lets endpoint-specific
// plugins decide whether they own a message.
type ContextRequestTransform interface {
	RequestTransform

	ValidateWithContext(payload []byte, metadata map[string]string, requestURL string, reqDeadline int64) error

	TransformWithContext(ctx context.Context, payload []byte, metadata map[string]string, requestURL string) (body []byte, contentType string, handled bool, err error)
}
