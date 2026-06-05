package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	uotel "github.com/llm-d-incubation/llm-d-async/internal/otel"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/async"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/pubsub"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"github.com/llm-d-incubation/llm-d-async/pkg/version"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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

	var redisTracing bool

	var tlsCACert string
	var tlsCert string
	var tlsKey string
	var tlsInsecureSkipVerify bool
	var poolConfigFile string

	flag.IntVar(&loggerVerbosity, "v", logging.DEFAULT, "number for the log level verbosity")

	flag.IntVar(&metricsPort, "metrics-port", 9090, "The metrics port")
	flag.BoolVar(&metricsEndpointAuth, "metrics-endpoint-auth", true, "Enables authentication and authorization of the metrics endpoint")

	flag.IntVar(&concurrency, "concurrency", 8, "number of concurrent workers")
	flag.DurationVar(&requestTimeout, "request-timeout", 5*time.Minute, "timeout for individual inference requests")

	flag.StringVar(&requestMergePolicy, "request-merge-policy", "random-robin", "The request merge policy to use. Supported policies: random-robin")
	flag.StringVar(&messageQueueImpl, "message-queue-impl", "redis-pubsub", "The message queue implementation to use. Supported implementations: redis-pubsub, redis-sortedset, gcp-pubsub, gcp-pubsub-gated")

	flag.BoolVar(&redisTracing, "redis-tracing", false, "Enable per-command Redis tracing spans (high volume, use for debugging only)")

	flag.StringVar(&tlsCACert, "tls-ca-cert", "", "Path to CA certificate file (PEM) for verifying the inference gateway")
	flag.StringVar(&tlsCert, "tls-cert", "", "Path to client certificate file (PEM) for mTLS")
	flag.StringVar(&tlsKey, "tls-key", "", "Path to client key file (PEM) for mTLS")
	flag.BoolVar(&tlsInsecureSkipVerify, "tls-insecure-skip-verify", false, "Skip TLS certificate verification (dev/test only)")
	flag.StringVar(&poolConfigFile, "pool-config-file", "", "Path to the pools configuration JSON file")

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

	tracerShutdown, err := uotel.InitTracer(logr.NewContext(context.Background(), setupLog))
	if err != nil {
		setupLog.Error(err, "Failed to initialize OpenTelemetry tracer")
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerShutdown(shutdownCtx); err != nil {
			setupLog.Error(err, "Failed to shutdown tracer")
		}
	}()

	setupLog.Info("Async Processor starting", "version", version.Version, "commit", version.Commit, "buildDate", version.BuildDate)

	printAllFlags(setupLog)

	hasQueueConfig := isQueueConfigSet()
	hasPoolConfig := (poolConfigFile != "")

	if hasQueueConfig != hasPoolConfig {
		setupLog.Error(fmt.Errorf("both pool-config-file and queues/topics config file must be specified together, or both must be omitted"), "Configuration error")
		os.Exit(1)
	}

	var pools []pipeline.PoolConfig
	if hasPoolConfig {
		var err error
		pools, err = pipeline.LoadPools(poolConfigFile)
		if err != nil {
			setupLog.Error(err, "Failed to load pool configuration file")
			os.Exit(1)
		}
		setupLog.Info("Loaded named pools config", "count", len(pools))
	} else {
		// Both are unset. Create a "default" pool.
		var igwURL, requestPath string
		switch messageQueueImpl {
		case "redis-pubsub":
			igwURL = flag.Lookup("redis.igw-base-url").Value.String()
			requestPath = flag.Lookup("redis.request-path-url").Value.String()
		case "redis-sortedset":
			igwURL = flag.Lookup("redis.ss.igw-base-url").Value.String()
			requestPath = flag.Lookup("redis.ss.request-path-url").Value.String()
		case "gcp-pubsub", "gcp-pubsub-gated":
			igwURL = flag.Lookup("pubsub.igw-base-url").Value.String()
			requestPath = flag.Lookup("pubsub.request-path-url").Value.String()
		}

		pools = []pipeline.PoolConfig{{
			ID:             "default",
			IGWBaseURL:     igwURL,
			RequestPathURL: requestPath,
			Workers:        concurrency,
		}}
		setupLog.Info("No queue/pool configs set. Created default pool", "igw_base_url", igwURL, "request_path_url", requestPath, "workers", concurrency)
	}

	// Create Gate Factory for per-queue gate instantiation
	gateFactory := flowcontrol.NewGateFactoryWithCacheTTL(*prometheusURL, *prometheusCacheTTL)
	defer func() {
		if err := gateFactory.Close(); err != nil {
			setupLog.Error(err, "Failed to close gate factory")
		}
	}()

	var policy pipeline.RequestMergePolicy
	switch requestMergePolicy {
	case "random-robin":
		policy = async.NewRandomRobinPolicy()
	default:
		setupLog.Error(fmt.Errorf("unknown request merge policy: %s", requestMergePolicy), "Unknown request merge policy", "request-merge-policy",
			requestMergePolicy)
		os.Exit(1)
	}
	var impl pipeline.Flow
	switch messageQueueImpl {
	case "redis-pubsub":
		flow, err := redis.NewRedisMQFlow(redis.WithRedisTracing(redisTracing), redis.WithPools(pools))
		if err != nil {
			setupLog.Error(err, "Failed to create Redis pub/sub flow")
			os.Exit(1)
		}
		impl = flow
	case "redis-sortedset":
		flow, err := redis.NewRedisSortedSetFlow(redis.WithGateFactory(gateFactory), redis.WithSortedSetRedisTracing(redisTracing), redis.WithSortedSetPools(pools))
		if err != nil {
			setupLog.Error(err, "Failed to create Redis sorted-set flow")
			os.Exit(1)
		}
		impl = flow
		setupLog.Info("Using Redis sorted-set flow with per-queue gating")
	case "gcp-pubsub":
		impl = pubsub.NewGCPPubSubMQFlow(pubsub.WithPools(pools))
	case "gcp-pubsub-gated":
		impl = pubsub.NewGCPPubSubMQFlow(pubsub.WithGateFactory(gateFactory), pubsub.WithPools(pools))
		setupLog.Info("Using GCP PubSub flow with per-queue gating")
	default:
		setupLog.Error(fmt.Errorf("unknown message queue implementation: %s", messageQueueImpl), "Unknown message queue implementation",
			"message-queue-impl", messageQueueImpl)
		os.Exit(1)
	}

	metrics.Register(metrics.GetAsyncProcessorCollectors(impl.Characteristics().SupportsMessageLatency)...)

	ctx := ctrl.SetupSignalHandler()

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

	totalConcurrency := 0
	poolsMap := make(map[string]pipeline.PoolConfig)
	for _, p := range pools {
		if p.Workers <= 0 {
			p.Workers = concurrency
		}
		poolsMap[p.ID] = p
		totalConcurrency += p.Workers
	}

	// Create inference client with a connection pool sized for the worker count.
	inferenceTransport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: totalConcurrency,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig:     tlsConfig,
	}
	inferenceHTTPClient := &http.Client{Transport: otelhttp.NewTransport(inferenceTransport,
		otelhttp.WithSpanNameFormatter(func(_ string, _ *http.Request) string {
			return "http-request"
		}),
	)}
	inferenceClient := asyncworker.NewHTTPInferenceClient(inferenceHTTPClient)

	dispatch := policy.MergeRequestChannels(impl.RequestChannels(), poolsMap)

	var wg sync.WaitGroup
	for poolID, mergedChan := range dispatch.Channels {
		pool, ok := poolsMap[poolID]
		if !ok {
			setupLog.Error(fmt.Errorf("pool %s not found", poolID), "Pool not found")
			os.Exit(1)
		}
		workersCount := pool.Workers

		setupLog.Info("Spawning workers for pool", "poolID", poolID, "workers", workersCount)
		for w := 1; w <= workersCount; w++ {
			wg.Add(1)
			go func(mergedChan chan pipeline.EmbelishedRequestMessage) {
				defer wg.Done()
				asyncworker.Worker(ctx, impl.Characteristics(), inferenceClient, mergedChan, impl.RetryChannel(), impl.ResultChannel(), requestTimeout)
			}(mergedChan)
		}
	}

	impl.Start(ctx)
	<-ctx.Done()
	wg.Wait()
	impl.Shutdown()
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

func printAllFlags(setupLog logr.Logger) {
	flags := make(map[string]any)
	flag.VisitAll(func(f *flag.Flag) {
		flags[f.Name] = f.Value
	})
	setupLog.Info("Flags processed", "flags", flags)
}

func isQueueConfigSet() bool {
	for _, flagName := range []string{
		"redis.queues-config-file",
		"redis.queues-config",
		"redis.ss.queues-config-file",
		"redis.ss.queues-config",
		"pubsub.topics-config-file",
	} {
		if f := flag.Lookup(flagName); f != nil && f.Value.String() != "" {
			return true
		}
	}
	return false
}
