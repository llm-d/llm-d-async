package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	_ "github.com/llm-d-incubation/llm-d-async/pkg/async"                                    // register built-in merge policies
	_ "github.com/llm-d-incubation/llm-d-async/pkg/async/inference/mergepolicy/tierpriority" // register tier-priority merge policy
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/pubsub"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"github.com/llm-d-incubation/llm-d-async/pkg/version"
	goredis "github.com/redis/go-redis/v9"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {

	var loggerVerbosity int

	var metricsPort int
	var metricsEndpointAuth bool

	var concurrency int
	var requestTimeout time.Duration
	var requestMergePolicy string
	var messageQueueImpl string

	var tlsCACert string
	var tlsCert string
	var tlsKey string
	var tlsInsecureSkipVerify bool

	flag.IntVar(&loggerVerbosity, "v", logging.DEFAULT, "number for the log level verbosity")

	flag.IntVar(&metricsPort, "metrics-port", 9090, "The metrics port")
	flag.BoolVar(&metricsEndpointAuth, "metrics-endpoint-auth", true, "Enables authentication and authorization of the metrics endpoint")

	flag.IntVar(&concurrency, "concurrency", 8, "number of concurrent workers")
	flag.DurationVar(&requestTimeout, "request-timeout", 5*time.Minute, "timeout for individual inference requests")

	flag.StringVar(&requestMergePolicy, "request-merge-policy", "random-robin", "The request merge policy to use. Supported policies: random-robin, tier-priority")
	flag.StringVar(&messageQueueImpl, "message-queue-impl", "redis-pubsub", "The message queue implementation to use. Supported implementations: redis-pubsub, redis-sortedset, gcp-pubsub, gcp-pubsub-gated")

	var mergePolicyConfigJSON = flag.String("merge-policy-config", "{}", "JSON-encoded free-form config map passed to the selected merge policy's factory (see policy package docs for recognized keys).")

	flag.StringVar(&tlsCACert, "tls-ca-cert", "", "Path to CA certificate file (PEM) for verifying the inference gateway")
	flag.StringVar(&tlsCert, "tls-cert", "", "Path to client certificate file (PEM) for mTLS")
	flag.StringVar(&tlsKey, "tls-key", "", "Path to client key file (PEM) for mTLS")
	flag.BoolVar(&tlsInsecureSkipVerify, "tls-insecure-skip-verify", false, "Skip TLS certificate verification (dev/test only)")

	var prometheusURL = flag.String("prometheus-url", "", "Prometheus server URL for metric-based gates (e.g., http://localhost:9090)")

	var prometheusCacheTTL = flag.Duration("prometheus-cache-ttl", flowcontrol.DefaultCacheTTL, "TTL for cached Prometheus metrics (e.g., 5s, 0s to disable)")

	opts := zap.Options{
		Development: true,
	}

	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	logging.InitLogging(&opts, loggerVerbosity)
	defer logging.Sync() // nolint:errcheck

	setupLog := ctrl.Log.WithName("setup")
	setupLog.Info("Logger initialized")

	setupLog.Info("Async Processor starting", "version", version.Version, "commit", version.Commit, "buildDate", version.BuildDate)

	printAllFlags(setupLog)

	// Set up the process-lifetime context before the gate factory so
	// background goroutines owned by gates (e.g. PromQL refresh loops
	// for tier-priority-admission) are driven by the signal-handler ctx.
	ctx := ctrl.SetupSignalHandler()

	// Shared redis client for gates that need it (e.g.
	// reservation-classifier, tier-priority-admission redis-counter).
	// Best-effort: if --redis.url is not set, those gates will fail at
	// construction. Other gate types and Flow impls that don't need
	// redis are unaffected.
	var gateRedisClient *goredis.Client
	if redisOpts, err := redis.RedisOptions(); err == nil {
		gateRedisClient = goredis.NewClient(redisOpts)
	} else {
		setupLog.Info("Redis client for gates not available; redis-backed gates will not initialize", "reason", err.Error())
	}

	gateFactoryOpts := []flowcontrol.GateFactoryOption{
		flowcontrol.WithBackgroundContext(ctx),
	}
	if gateRedisClient != nil {
		gateFactoryOpts = append(gateFactoryOpts, flowcontrol.WithRedisClient(gateRedisClient))
	}
	gateFactory := flowcontrol.NewGateFactoryWithCacheTTL(*prometheusURL, *prometheusCacheTTL, gateFactoryOpts...)

	var impl pipeline.Flow
	switch messageQueueImpl {
	case "redis-pubsub":
		flow, err := redis.NewRedisMQFlow()
		if err != nil {
			setupLog.Error(err, "Failed to create Redis pub/sub flow")
			os.Exit(1)
		}
		impl = flow
	case "redis-sortedset":
		flow, err := redis.NewRedisSortedSetFlow(redis.WithGateFactory(gateFactory))
		if err != nil {
			setupLog.Error(err, "Failed to create Redis sorted-set flow")
			os.Exit(1)
		}
		impl = flow
		setupLog.Info("Using Redis sorted-set flow with per-queue gating")
	case "gcp-pubsub":
		impl = pubsub.NewGCPPubSubMQFlow()
	case "gcp-pubsub-gated":
		impl = pubsub.NewGCPPubSubMQFlow(pubsub.WithGateFactory(gateFactory))
		setupLog.Info("Using GCP PubSub flow with per-queue gating")
	default:
		setupLog.Error(fmt.Errorf("unknown message queue implementation: %s", messageQueueImpl), "Unknown message queue implementation",
			"message-queue-impl", messageQueueImpl)
		os.Exit(1)
	}

	// Build the merge policy via the registry after the Flow is constructed.
	policy, err := pipeline.NewMergePolicy(requestMergePolicy, pipeline.MergePolicyDeps{
		GateFactory: gateFactory,
		Config:      parseStringMap(*mergePolicyConfigJSON),
	})
	if err != nil {
		setupLog.Error(err, "Failed to construct merge policy", "request-merge-policy", requestMergePolicy)
		os.Exit(1)
	}

	metrics.Register(metrics.GetAsyncProcessorCollectors(impl.Characteristics().SupportsMessageLatency)...)

	// Register metrics handler.
	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress: fmt.Sprintf(":%d", metricsPort),
		FilterProvider: func() func(c *rest.Config, httpClient *http.Client) (metricsserver.Filter, error) {
			if metricsEndpointAuth {
				return filters.WithAuthenticationAndAuthorization
			}

			return nil
		}(),
	}
	restConfig := ctrl.GetConfigOrDie()

	msrv, err := metricsserver.NewServer(metricsServerOptions, restConfig, http.DefaultClient)
	if err != nil {
		setupLog.Error(err, "Failed to create metrics server")
		os.Exit(1)
	}
	go msrv.Start(ctx) // nolint:errcheck

	tlsConfig, err := buildTLSConfig(tlsCACert, tlsCert, tlsKey, tlsInsecureSkipVerify)
	if err != nil {
		setupLog.Error(err, "Failed to build TLS configuration")
		os.Exit(1)
	}

	// Create inference client with a connection pool sized for the worker count.
	inferenceTransport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: concurrency,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig:     tlsConfig,
	}
	inferenceHTTPClient := &http.Client{Transport: inferenceTransport}
	inferenceClient := asyncworker.NewHTTPInferenceClient(inferenceHTTPClient)

	// Per-pool dispatch: the merge policy fans subscriptions into one
	// channel per inference pool. Each pool gets its own dedicated
	// worker pool so backpressure on one pool's downstream endpoint
	// stays local — a saturated pool's workers and prefetch stall
	// without affecting any other pool's throughput.
	dispatch := policy.MergeRequestChannels(impl.RequestChannels())
	poolByID := map[string]pipeline.Pool{}
	for _, p := range impl.Pools() {
		poolByID[p.ID] = p
	}
	totalWorkers := 0
	for poolID, ch := range dispatch.Channels {
		pool := poolByID[poolID]
		workers := pool.Workers
		if workers <= 0 {
			workers = concurrency
		}
		totalWorkers += workers
		setupLog.Info("Spawning per-pool worker pool",
			"poolID", poolID,
			"gatewayURL", pool.GatewayURL,
			"workers", workers)
		for w := 0; w < workers; w++ {
			go asyncworker.Worker(ctx, impl.Characteristics(), inferenceClient, ch, impl.RetryChannel(), impl.ResultChannel(), requestTimeout)
		}
	}
	setupLog.Info("Per-pool worker pools started", "pools", len(dispatch.Channels), "totalWorkers", totalWorkers)

	impl.Start(ctx)
	<-ctx.Done()
}

func buildTLSConfig(caCertPath, certPath, keyPath string, insecureSkipVerify bool) (*tls.Config, error) {
	if caCertPath == "" && certPath == "" && keyPath == "" && !insecureSkipVerify {
		return nil, nil
	}

	if (certPath != "") != (keyPath != "") {
		return nil, fmt.Errorf("both tls-cert and tls-key must be provided together")
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12} //nolint:gosec

	if insecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true //nolint:gosec
	}

	if caCertPath != "" {
		caCert, err := os.ReadFile(caCertPath) // #nosec G304 -- path from trusted CLI flag
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", caCertPath, err)
		}
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			caCertPool = x509.NewCertPool()
		}
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("no valid certificates found in %s", caCertPath)
		}
		tlsConfig.RootCAs = caCertPool
	}

	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// parseStringMap parses a JSON object string into a map[string]string.
// Empty / "{}" yields an empty map; parse failures yield nil. Callers
// reading individual keys are not affected by missing entries.
func parseStringMap(s string) map[string]string {
	if s == "" || s == "{}" {
		return map[string]string{}
	}
	out := map[string]string{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return out
}

func printAllFlags(setupLog logr.Logger) {
	flags := make(map[string]any)
	flag.VisitAll(func(f *flag.Flag) {
		flags[f.Name] = f.Value
	})
	setupLog.Info("Flags processed", "flags", flags)
}
