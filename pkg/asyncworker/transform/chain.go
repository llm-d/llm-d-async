package transform

import (
	"context"
	"fmt"
)

// Chain is an ordered set of RequestTransform plugins applied to each outgoing
// request. A nil *Chain is valid and behaves as a no-op, so callers that have no
// transforms configured can pass nil and preserve the default JSON path exactly.
type Chain struct {
	transforms []RequestTransform
}

// NewChain returns a Chain over the given transforms, in priority order.
func NewChain(transforms []RequestTransform) *Chain {
	return &Chain{transforms: transforms}
}

// Len reports the number of transforms in the chain.
func (c *Chain) Len() int {
	if c == nil {
		return 0
	}
	return len(c.transforms)
}

// Validate runs every transform's Validate in order and returns the first error.
// Each transform decides whether it applies to the message; non-applicable
// transforms return nil. A nil/empty chain validates successfully.
func (c *Chain) Validate(payload []byte, metadata map[string]string, reqDeadline int64) error {
	if c == nil {
		return nil
	}
	for _, t := range c.transforms {
		if err := t.Validate(payload, metadata, reqDeadline); err != nil {
			return fmt.Errorf("transform %s: %w", t.TypedName(), err)
		}
	}
	return nil
}

// Apply runs transforms in order and returns the body produced by the first one
// that handles the message (first-handled-wins, short-circuit): once a transform
// returns handled=true, its body and Content-Type are returned and no further
// transforms run. If no transform handles the message, handled is false and the
// caller should send the original payload unchanged. A nil/empty chain returns
// handled=false. An error from any transform short-circuits and is returned,
// wrapped with %w so callers can still classify it with errors.As.
//
// Apply is ApplyContext without a request context or URL, so transforms
// cannot see a deadline or cancellation.
//
// Deprecated: use ApplyContext.
func (c *Chain) Apply(payload []byte, metadata map[string]string) (body []byte, contentType string, handled bool, err error) {
	return c.ApplyContext(context.Background(), payload, metadata, "")
}

// ApplyContext is Apply for a request with a deadline and destination. A
// transform that implements ContextTransform gets ctx and requestURL through
// TransformContext; any other transform is called through Transform.
func (c *Chain) ApplyContext(ctx context.Context, payload []byte, metadata map[string]string, requestURL string) (body []byte, contentType string, handled bool, err error) {
	if c == nil {
		return nil, "", false, nil
	}
	for _, t := range c.transforms {
		if ct, ok := t.(ContextTransform); ok {
			body, contentType, handled, err = ct.TransformContext(ctx, payload, metadata, requestURL)
		} else {
			body, contentType, handled, err = t.Transform(payload, metadata)
		}
		if err != nil {
			return nil, "", false, fmt.Errorf("transform %s: %w", t.TypedName(), err)
		}
		if handled {
			return body, contentType, true, nil
		}
	}
	return nil, "", false, nil
}
