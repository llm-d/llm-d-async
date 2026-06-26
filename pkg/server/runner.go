package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/internal/health"
	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	uotel "github.com/llm-d-incubation/llm-d-async/internal/otel"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/async"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker"
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/plugins"
	"github.com/llm-d-incubation/llm-d-async/pkg/pubsub"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"github.com/llm-d-incubation/llm-d-async/pkg/version"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type Runner struct {
	opts *Options
}

func NewRunner(opts *Options) *Runner {
	return &Runner{opts: opts}
}

func (r *Runner) Run(ctx context.Context) error {
	opts := r.opts

	logging.InitLogging(opts.LoggingOptions(), opts.Observability.Verbosity)
	defer logging.Sync() //nolint:errcheck

	setupLog := ctrl.Log.WithName("setup")
	setupLog.Info("Logger initialized")

	tracerShutdown, err := uotel.InitTracer(logr.NewContext(context.Background(), setupLog))
	if err != nil {
		setupLog.Error(err, "Failed to initialize OpenTelemetry tracer")
		return err
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

	var workerPools []pipeline.WorkerPoolConfig
	if opts.Worker.PoolConfigFile != "" {
		workerPools, err = pipeline.LoadWorkerPools(opts.Worker.PoolConfigFile)
		if err != nil {
			setupLog.Error(err, "Failed to load pool configuration file")
			return err
		}
		setupLog.Info("Loaded named pools config", "count", len(workerPools))
	} else {
		workerPools = []pipeline.WorkerPoolConfig{{
			ID:      "default",
			Workers: opts.Worker.Concurrency,
		}}
		setupLog.Info("No queue/pool configs set. Created default pool", "workers", opts.Worker.Concurrency)
	}

	gateFactory := flowcontrol.NewGateFactoryWithCacheTTL(opts.Prometheus.URL, opts.Prometheus.CacheTTL)
	defer func() {
		if err := gateFactory.Close(); err != nil {
			setupLog.Error(err, "Failed to close gate factory")
		}
	}()

	var policy pipeline.RequestMergePolicy
	switch opts.Queue.MergePolicy {
	case "random-robin":
		policy = async.NewRandomRobinPolicy()
	default:
		return fmt.Errorf("unknown request merge policy: %s", opts.Queue.MergePolicy)
	}

	var impl pipeline.Flow
	switch opts.Queue.Impl {
	case "redis-pubsub":
		flow, err := redis.NewRedisMQFlow(opts.Redis, opts.RedisConnection, redis.WithRedisTracing(opts.Observability.RedisTracing), redis.WithWorkerPools(workerPools))
		if err != nil {
			setupLog.Error(err, "Failed to create Redis pub/sub flow")
			return err
		}
		impl = flow
	case "redis-sortedset":
		flow, err := redis.NewRedisSortedSetFlow(opts.RedisSortedSet, opts.RedisConnection, redis.WithGateFactory(gateFactory), redis.WithSortedSetRedisTracing(opts.Observability.RedisTracing), redis.WithSortedSetWorkerPools(workerPools))
		if err != nil {
			setupLog.Error(err, "Failed to create Redis sorted-set flow")
			return err
		}
		impl = flow
		setupLog.Info("Using Redis sorted-set flow with per-queue gating")
	case "gcp-pubsub":
		flow, err := pubsub.NewGCPPubSubMQFlow(opts.PubSub, pubsub.WithWorkerPools(workerPools))
		if err != nil {
			setupLog.Error(err, "Failed to create GCP PubSub flow")
			return err
		}
		impl = flow
	case "gcp-pubsub-gated":
		flow, err := pubsub.NewGCPPubSubMQFlow(opts.PubSub, pubsub.WithGateFactory(gateFactory), pubsub.WithWorkerPools(workerPools))
		if err != nil {
			setupLog.Error(err, "Failed to create GCP PubSub gated flow")
			return err
		}
		impl = flow
		setupLog.Info("Using GCP PubSub flow with per-queue gating")
	default:
		return fmt.Errorf("unknown message queue implementation: %s", opts.Queue.Impl)
	}

	metrics.Register(metrics.GetAsyncProcessorCollectors(impl.Characteristics().SupportsMessageLatency)...)

	var checker health.Checker
	if hc, ok := impl.(pipeline.HealthChecker); ok {
		checker = hc.HealthCheck
	}
	healthServer := health.NewServer(opts.Server.HealthPort, checker, setupLog.WithName("health"))
	healthLn, err := healthServer.ListenAndServe()
	if err != nil {
		setupLog.Error(err, "Failed to bind health server")
		return err
	}
	go func() {
		if err := healthServer.Serve(healthLn); err != nil {
			setupLog.Error(err, "Health server failed")
		}
	}()

	signalCtx := ctx

	drainCtx, drainCancel := context.WithCancel(logr.NewContext(context.Background(), setupLog))
	defer drainCancel()

	metricsServerOptions := metricsserver.Options{
		BindAddress: fmt.Sprintf(":%d", opts.Server.MetricsPort),
		FilterProvider: func() func(c *rest.Config, httpClient *http.Client) (metricsserver.Filter, error) {
			if opts.Server.MetricsEndpointAuth {
				return filters.WithAuthenticationAndAuthorization
			}
			return nil
		}(),
	}
	restConfig := ctrl.GetConfigOrDie()

	msrv, err := metricsserver.NewServer(metricsServerOptions, restConfig, http.DefaultClient)
	if err != nil {
		setupLog.Error(err, "Failed to create metrics server")
		return err
	}
	go msrv.Start(signalCtx) //nolint:errcheck

	tlsConfig, err := buildTLSConfig(opts.TLS)
	if err != nil {
		setupLog.Error(err, "Failed to build TLS configuration")
		return err
	}

	totalConcurrency := 0
	poolsMap := make(map[string]pipeline.WorkerPoolConfig)
	for _, p := range workerPools {
		if p.Workers <= 0 {
			p.Workers = opts.Worker.Concurrency
		}
		poolsMap[p.ID] = p
		totalConcurrency += p.Workers
		metrics.SetPoolWorkerLimit(p.ID, float64(p.Workers))
	}

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

	var transforms *transform.Chain
	if opts.TransformConfigFile != "" {
		cfg, err := transform.LoadConfig(opts.TransformConfigFile)
		if err != nil {
			setupLog.Error(err, "Failed to load transform configuration file")
			return err
		}
		transforms, err = transform.BuildChain(cfg.RequestTransforms, plugins.NewHandle(signalCtx))
		if err != nil {
			setupLog.Error(err, "Failed to build request transform chain")
			return err
		}
		setupLog.Info("Loaded request transform plugins", "count", transforms.Len())
	}

	dispatch := policy.MergeRequestChannels(impl.RequestChannels(), poolsMap)

	poolGates := make(map[string]pipeline.Gate)
	for poolID, pool := range poolsMap {
		if pool.GateType != "" {
			gate, err := gateFactory.CreateGate(pool.GateType, pool.GateParams)
			if err != nil {
				setupLog.Error(err, "Failed to create pool gate", "poolID", poolID, "gateType", pool.GateType)
				os.Exit(1)
			}
			poolGates[poolID] = gate
			setupLog.Info("Created pool gate", "poolID", poolID, "gateType", pool.GateType, "gateParams", pool.GateParams)
		}
	}

	var wg sync.WaitGroup
	for poolID, mergedChan := range dispatch.Channels {
		pool, ok := poolsMap[poolID]
		if !ok {
			return fmt.Errorf("pool %s not found", poolID)
		}
		workersCount := pool.Workers
		poolGate := poolGates[poolID]

		setupLog.Info("Spawning workers for pool", "poolID", poolID, "workers", workersCount, "hasGate", poolGate != nil)
		for w := 1; w <= workersCount; w++ {
			wg.Add(1)
			go func(mergedChan chan pipeline.EmbelishedRequestMessage, poolGate pipeline.Gate) {
				defer wg.Done()
				asyncworker.WorkerWithGate(signalCtx, drainCtx, impl.Characteristics(), inferenceClient, mergedChan, impl.RetryChannel(), impl.ResultChannel(), opts.Worker.RequestTimeout, transforms, poolGate)
			}(mergedChan, poolGate)
		}
	}

	impl.Start(signalCtx)
	healthServer.SetReady()

	if reporter, ok := impl.(pipeline.BacklogReporter); ok && opts.Queue.BacklogPollInterval > 0 {
		go pollBacklog(signalCtx, reporter, opts.Queue.BacklogPollInterval)
	} else if !ok {
		setupLog.Info("Selected flow does not support broker backlog metrics", "message-queue-impl", opts.Queue.Impl)
	}

	<-signalCtx.Done()
	healthServer.SetNotReady()

	setupLog.Info("Signal received, stopping message consumption")
	impl.StopConsuming()

	setupLog.Info("Draining in-flight requests", "timeout", opts.Worker.DrainTimeout)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		setupLog.Info("All workers drained successfully")
	case <-time.After(opts.Worker.DrainTimeout):
		setupLog.Info("Drain timeout reached, cancelling in-flight requests")
		drainCancel()
		wg.Wait()
	}

	impl.Shutdown()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		setupLog.Error(err, "Health server shutdown error")
	}

	return nil
}

func buildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if cfg.CACert == "" && cfg.Cert == "" && cfg.Key == "" && !cfg.InsecureSkipVerify {
		return nil, nil
	}

	if (cfg.Cert != "") != (cfg.Key != "") {
		return nil, fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12} //nolint:gosec

	if cfg.InsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true //nolint:gosec
	}

	if cfg.CACert != "" {
		caCert, err := os.ReadFile(cfg.CACert) // #nosec G304 -- path from trusted CLI flag
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", cfg.CACert, err)
		}
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			caCertPool = x509.NewCertPool()
		}
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("no valid certificates found in %s", cfg.CACert)
		}
		tlsConfig.RootCAs = caCertPool
	}

	if cfg.Cert != "" && cfg.Key != "" {
		cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func pollBacklog(ctx context.Context, reporter pipeline.BacklogReporter, interval time.Duration) {
	logger := ctrl.Log.WithName("backlog-poller")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	poll := func() {
		stats, err := reporter.QueueBacklog(ctx)
		if err != nil {
			logger.V(logging.DEFAULT).Error(err, "Failed to poll broker backlog")
		}
		for _, s := range stats {
			metrics.SetBrokerBacklog(s.QueueID, s.QueueName, s.PoolName, float64(s.Depth))
		}
	}

	poll()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}

var sensitiveFlags = map[string]bool{
	"redis.url": true,
}

func printAllFlags(setupLog logr.Logger) {
	flags := make(map[string]any)
	pflag.VisitAll(func(f *pflag.Flag) {
		if sensitiveFlags[f.Name] {
			flags[f.Name] = "REDACTED"
		} else {
			flags[f.Name] = f.Value
		}
	})
	setupLog.Info("Flags processed", "flags", flags)
}
