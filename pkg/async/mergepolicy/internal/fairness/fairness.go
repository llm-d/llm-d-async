// Package fairness stamps a tenant identity carried in request metadata into an
// HTTP header, so the gateway's flow control can arbitrate between tenants after
// dispatch. Both merge policies share it; stamping is best effort and never
// fails a request.
package fairness

import (
	"fmt"

	"golang.org/x/net/http/httpguts"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/async/mergepolicy/internal/header"
)

// DefaultAttribute is the message metadata attribute stamped when a policy does
// not configure one. It matches the redis-quota gate's default so both sides key
// on the same tenant.
const DefaultAttribute = "userid"

// maxIdentityLen bounds the stamped identity. Envoy's default header limit is
// around 60 KiB, so an oversized attribute would turn every one of a tenant's
// requests into a 431 at the gateway, and it amplifies gateway access logs. A
// tenant ID is far shorter than this in practice; anything longer is skipped
// rather than stamped.
const maxIdentityLen = 256

// Params are the fairness parameters every merge policy accepts in its plugin
// configuration. Embed it in a policy's parameter struct; encoding/json promotes
// the embedded fields.
type Params struct {
	// Header is a pointer so an absent parameter (use the default header) is
	// distinguishable from an explicit "" (stamping disabled).
	Header    *string `json:"fairness_header"`
	Attribute string  `json:"fairness_attribute"`
}

// ResolveHeader returns the configured header name, falling back to
// api.FairnessIDHeader when the parameter was absent. An explicit "" is returned
// unchanged, since that disables stamping.
//
// It errors when the configured name is not a legal HTTP header name. net/http
// refuses to write such a header, which would fail every dispatched request
// permanently as a non-retryable error, so an operator's typo has to surface at
// startup rather than at dispatch.
func (p Params) ResolveHeader() (string, error) {
	if p.Header == nil {
		return api.FairnessIDHeader, nil
	}
	if *p.Header != "" && !httpguts.ValidHeaderFieldName(*p.Header) {
		return "", fmt.Errorf("fairness_header %q is not a legal HTTP header name", *p.Header)
	}
	return *p.Header, nil
}

// Stamper writes a tenant identity into an HTTP header. The zero Stamper is
// disabled and stamps nothing.
type Stamper struct {
	header    string
	attribute string
}

// New returns a Stamper that writes the tenant identity found under the given
// message metadata attribute into the given header. An empty header disables
// stamping; an empty attribute falls back to DefaultAttribute.
func New(header string, attribute string) Stamper {
	if attribute == "" {
		attribute = DefaultAttribute
	}
	return Stamper{header: header, attribute: attribute}
}

// Stamp writes the tenant identity from req's metadata into headers, replacing
// any value already there. Call it after the caller's own headers have been
// merged in: the identity the quota gate accounts on must be the one the gateway
// arbitrates on, or a tenant could be charged as one flow while presenting as
// another.
//
// It is a no-op when stamping is disabled, when the attribute is absent or
// empty, or when the identity is too long or not a legal HTTP header value. In
// those cases there is no identity to assert, so a caller-supplied header is
// left as it was rather than dropped for nothing.
func (s Stamper) Stamp(headers map[string]string, req api.Request) {
	if s.header == "" {
		return
	}
	// Metadata is caller-supplied and has never reached the wire before now.
	// net/http fails the entire request on a header value it cannot write, and a
	// best-effort fairness hint must not cost a request, so an identity that
	// cannot be stamped is skipped instead.
	id := req.ReqMetadata()[s.attribute]
	if id == "" || len(id) > maxIdentityLen || !httpguts.ValidHeaderFieldValue(id) {
		return
	}
	header.Set(headers, s.header, id)
}
