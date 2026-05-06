package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/async"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/pubsub"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"github.com/llm-d-incubation/llm-d-async/pkg/version"
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

	flag.StringVar(&requestMergePolicy, "request-merge-policy", "random-robin", "The request merge policy to use. Supported policies: random-robin")
	flag.StringVar(&messageQueueImpl, "message-queue-impl", "redis-pubsub", "The message queue implementation to use. Supported implementations: redis-pubsub, redis-sortedset, gcp-pubsub, gcp-pubsub-gated")

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
	// Create Gate Factory for per-queue gate instantiation
	gateFactory := flowcontrol.NewGateFactoryWithCacheTTL(*prometheusURL, *prometheusCacheTTL)

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

	msrv, _ := metricsserver.NewServer(metricsServerOptions, restConfig, http.DefaultClient)
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

	requestChannel := policy.MergeRequestChannels(impl.RequestChannels()).Channel
	for w := 1; w <= concurrency; w++ {

		go asyncworker.Worker(ctx, impl.Characteristics(), inferenceClient, requestChannel, impl.RetryChannel(), impl.ResultChannel(), requestTimeout)
	}

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
		caCert, err := os.ReadFile(caCertPath)
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
