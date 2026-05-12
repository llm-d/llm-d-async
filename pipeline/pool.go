package pipeline

// Pool is a first-class config concept for the inference endpoint a group
// of subscriptions targets together. A pool is the unit of:
//
//   - HTTP dispatch identity: gateway URL, request path, headers,
//     model-name overrides
//   - Worker concurrency: each pool gets its own worker pool with K
//     workers consuming from its own per-pool channel out of the RMP
//   - Post-RMP gate placement: capacity admission control specific to
//     this pool's downstream backend lives on the pool, not on a
//     subscription (a blocking semaphore for one pool shouldn't affect
//     another pool's throughput)
//
// Subscriptions reference a pool by ID. When a Flow processes a pulled
// message, the resolved Pool travels with the message via the
// EmbelishedRequestMessage so the merge policy can route to the pool's
// channel and the per-pool worker pool can pick up the right gateway
// context.
//
// Pool-level Labels are merged onto every message routed through this
// pool, layered between subscription labels (which win) and producer
// attributes (which lose). Lets operators stamp a pool-wide label like
// `model=kimi-k2-6` once rather than per subscription.
//
// # Relationship to IGW InferencePool
//
// "Pool" here is a config-level grouping concept, NOT a binding to the
// Kubernetes InferencePool CRD from sigs.k8s.io/gateway-api-inference-
// extension. This struct doesn't reference, watch, or import the
// InferencePool API type; the async-processor never reads InferencePool
// objects from the cluster. Pools in this config are just a string ID
// and a bag of HTTP-dispatch fields.
//
// When the destination is fronted by an IGW EndpointPicker (EPP),
// operators typically align Pool.ID with the IGW InferencePool name by
// convention. For destinations that aren't IGW-fronted (external
// providers, plain HTTP servers, etc.), Pool.ID is a free-form
// operator label with no IGW meaning.
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

	// Labels are pool-level static labels merged onto every message
	// routed through this pool. Subscription labels override on
	// collision; producer attributes lose to both.
	Labels map[string]string `json:"labels,omitempty"`
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
