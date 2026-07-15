package asyncworker

import (
	"context"

	asyncapi "github.com/llm-d/llm-d-async/api"
)

type cancellationCheckerKey struct{}

// WithCancellationChecker attaches a request cancellation checker to the worker context.
func WithCancellationChecker(ctx context.Context, checker asyncapi.CancellationChecker) context.Context {
	if checker == nil {
		return ctx
	}
	return context.WithValue(ctx, cancellationCheckerKey{}, checker)
}

func cancellationCheckerFromContext(ctx context.Context) asyncapi.CancellationChecker {
	if ctx == nil {
		return nil
	}
	checker, _ := ctx.Value(cancellationCheckerKey{}).(asyncapi.CancellationChecker)
	return checker
}
