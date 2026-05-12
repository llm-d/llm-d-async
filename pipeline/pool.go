package pipeline

// Pool is a first-class config concept for the inference endpoint a group
// of subscriptions targets together. A pool is the unit of:
//
//   - HTTP dispatch identity: gateway URL, request path, headers,
//     model-name overrides
//   - Worker concurrency: each pool gets its own worker pool with K
//     workers consuming from its own per-pool channel out of the RMP
//   - Gate placement: capacity admission control specific to this pool's
//     downstream backend lives on the pool, not on a subscription (a
//     blocking semaphore for one pool shouldn't affect another pool's
//     throughput)
//
// Subscriptions reference a pool by ID.
type Pool struct {
	ID                string            `json:"id"`
	GatewayURL        string            `json:"gateway_url"`
	RequestPath       string            `json:"request_path,omitempty"`
	HTTPHeaders       map[string]string `json:"http_headers,omitempty"`
	ModelNameOverride string            `json:"model_name_override,omitempty"`

	// Workers is the per-pool worker concurrency. If 0, falls back to the
	// processor-wide --concurrency flag at startup.
	Workers int `json:"workers,omitempty"`

	// Gates is the chain of pool gates: admission gates evaluated in
	// the per-pool worker pool, downstream of the merge policy. Each
	// dispatched message runs through the chain in order; first
	// non-Continue short-circuits, releases unwound in LIFO. Blocking
	// gates (local-max-concurrency, local-rate-limit) belong here so
	// their backpressure isolates to this pool only.
	//
	// Empty or absent = always-open (no admission control beyond what
	// subscription gates provide upstream).
	Gates []GateConfig `json:"gates,omitempty"`

}

// GateConfig declares a gate type + params to be instantiated via the
// configured GateFactory. Used in Pool.Gates (pool gates, evaluated in
// the worker pool downstream of the merge policy) and in
// TopicConfig.Gates / queueConfig.Gates (subscription gates, evaluated
// at pull time before the merge policy). Same shape; the call site
// determines blocking semantics and release plumbing.
type GateConfig struct {
	Type   string            `json:"type"`
	Params map[string]string `json:"params,omitempty"`
}
