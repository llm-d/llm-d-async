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
//
// Several plugins can serve the same endpoint, and the chain does not decide
// which one owns a request: it runs every plugin's Validate, then Transform
// until one handles the request, and an error from either stops the chain
// (a Validate error fails the request; a Transform error is retried or fails
// it, depending on its category).
// So each plugin claims only the requests it owns: a request it owns carries
// the payload field the plugin reads (for example gcs_uri or file), and, when
// two plugins read the same field, matches the plugin's configuration (for
// example its providers list). For any other request a plugin returns nil
// from Validate and handled=false with no error from Transform, so the next
// plugin in the chain gets it. Errors are only for requests the plugin has
// claimed.
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
	//
	// An error whose chain contains an api.InferenceError with a retryable
	// category (RATE_LIMIT, SERVER_ERROR) makes the worker retry the message
	// with backoff. Any other error fails it as an invalid request.
	Transform(payload []byte, metadata map[string]string) (body []byte, contentType string, handled bool, err error)
}

// ContextTransform is an optional interface a RequestTransform may also
// implement, like the ...Context interfaces in database/sql/driver. When a
// plugin implements it, the chain calls TransformContext instead of Transform,
// so existing plugins keep working unchanged.
type ContextTransform interface {
	// TransformContext behaves like Transform, with request context and routing
	// information. ctx ends at the attempt's deadline, the earlier of
	// request-timeout and the request's own deadline, and is cancelled on
	// shutdown or request cancellation. Any I/O must honor it. requestURL is
	// the full dispatch URL. requestDeadline is the original request deadline
	// in Unix seconds, or zero when there is no deadline; it can be later than
	// ctx's deadline.
	TransformContext(ctx context.Context, payload []byte, metadata map[string]string, requestURL string, requestDeadline int64) (body []byte, contentType string, handled bool, err error)
}
