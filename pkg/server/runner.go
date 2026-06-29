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

func (r *Runner) Run(ctx context.Context) (err error) {
	opts := r.opts

	logging.InitLogging(opts.LoggingOptions(), opts.Observability.Verbosity)
	defer logging.Sync() //nolint:errcheck

	setupLog := ctrl.Log.WithName("setup")
	setupLog.Info("Logger initialized")

	baseCtx := logr.NewContext(context.Background(), setupLog)

	var (
		healthServer   *health.Server
		gateFactory    *flowcontrol.GateFactory
		tracerShutdown func(ctx context.Context) error
	)
	defer func() {
		if err != nil {
			setupLog.Error(err, "Runner failed")
		}
		shutdownCtx, cancel := context.WithTimeout(baseCtx, 5*time.Second)
		defer cancel()
		if healthServer != nil {
			if err := healthServer.Shutdown(shutdownCtx); err != nil {
				setupLog.Error(err, "Health server shutdown error")
			}
		}
		if gateFactory != nil {
			if err := gateFactory.Close(); err != nil {
				setupLog.Error(err, "Failed to close gate factory")
			}
		}
		if tracerShutdown != nil {
			if err := tracerShutdown(shutdownCtx); err != nil {
				setupLog.Error(err, "Failed to shutdown tracer")
			}
		}
	}()

	tracerShutdown, err = initTracer(baseCtx)
	if err != nil {
		return err
	}

	setupLog.Info("Async Processor starting", "version", version.Version, "commit", version.Commit, "buildDate", version.BuildDate)

	printAllFlags(setupLog)

	poolsMap, totalConcurrency, err := loadWorkerPools(opts.Worker, setupLog)
	if err != nil {
		return err
	}

	gateFactory = flowcontrol.NewGateFactoryWithCacheTTL(opts.Prometheus.URL, opts.Prometheus.CacheTTL)

	policy, err := loadRequestMergePolicy(opts.Queue.MergePolicy)
	if err != nil {
		return err
	}

	flow, err := loadFlow(opts, gateFactory, poolsMap)
	if err != nil {
		return err
	}

	metrics.Register(metrics.GetAsyncProcessorCollectors(flow.Characteristics().SupportsMessageLatency)...)

	healthServer, err = initHealthServer(flow, opts.Server, setupLog)
	if err != nil {
		return err
	}

	if err = startMetricsServer(ctx, opts.Server, setupLog); err != nil {
		return err
	}

	inferenceClient, err := initInferenceClient(opts.TLS, totalConcurrency)
	if err != nil {
		return err
	}

	transforms, err := loadTransforms(ctx, opts.TransformConfigFile, setupLog)
	if err != nil {
		return err
	}

	drainCtx, drainCancel := context.WithCancel(baseCtx)
	defer drainCancel()

	dispatch := policy.MergeRequestChannels(flow.RequestChannels(), poolsMap)

	poolGates := make(map[string]pipeline.Gate)
	for poolID, pool := range poolsMap {
		if pool.GateType != "" {
			gate, err := gateFactory.CreateGate(pipeline.GateConfig{GateType: pool.GateType, GateParams: pool.GateParams})
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
		pool := poolsMap[poolID]
		workersCount := pool.Workers
		poolGate := poolGates[poolID]

		setupLog.Info("Spawning workers for pool", "poolID", poolID, "workers", workersCount, "hasGate", poolGate != nil)
		for w := 1; w <= workersCount; w++ {
			wg.Add(1)
			go func(mergedChan chan pipeline.EmbelishedRequestMessage, poolGate pipeline.Gate) {
				defer wg.Done()
				asyncworker.WorkerWithGate(ctx, drainCtx, flow.Characteristics(), inferenceClient, mergedChan, flow.RetryChannel(), flow.ResultChannel(), opts.Worker.RequestTimeout, transforms, poolGate)
			}(mergedChan, poolGate)
		}
	}

	flow.Start(ctx)
	healthServer.SetReady()

	if reporter, ok := flow.(pipeline.BacklogReporter); ok && opts.Queue.BacklogPollInterval > 0 {
		go pollBacklog(ctx, reporter, opts.Queue.BacklogPollInterval)
	} else if !ok {
		setupLog.Info("Selected flow does not support broker backlog metrics", "message-queue-impl", opts.Queue.Impl)
	}

	<-ctx.Done()
	healthServer.SetNotReady()

	setupLog.Info("Signal received, stopping message consumption")
	flow.StopConsuming()

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

	flow.Shutdown()

	return nil
}

func initTracer(baseCtx context.Context) (func(context.Context) error, error) {
	shutdown, err := uotel.InitTracer(baseCtx)
	if err != nil {
		logr.FromContextOrDiscard(baseCtx).Error(err, "Failed to initialize OpenTelemetry tracer")
		return nil, err
	}
	return shutdown, nil
}

func loadWorkerPools(workerConfig WorkerConfig, setupLog logr.Logger) (poolsMap map[string]pipeline.WorkerPoolConfig, totalConcurrency int, err error) {
	var pools []pipeline.WorkerPoolConfig
	if workerConfig.PoolConfigFile != "" {
		pools, err = pipeline.LoadWorkerPools(workerConfig.PoolConfigFile)
		if err != nil {
			return nil, -1, err
		}
		setupLog.Info("Loaded named pools config", "count", len(pools))
	} else {
		pools = []pipeline.WorkerPoolConfig{{
			ID:      "default",
			Workers: workerConfig.Concurrency,
		}}
		setupLog.Info("No queue/pool configs set. Created default pool", "workers", workerConfig.Concurrency)
	}

	poolsMap = make(map[string]pipeline.WorkerPoolConfig)
	for _, p := range pools {
		if p.Workers <= 0 {
			p.Workers = workerConfig.Concurrency
		}
		poolsMap[p.ID] = p
		totalConcurrency += p.Workers
		metrics.SetPoolWorkerLimit(p.ID, float64(p.Workers))
	}
	return poolsMap, totalConcurrency, err

}

func loadRequestMergePolicy(name string) (pipeline.RequestMergePolicy, error) {
	switch name {
	case "random-robin":
		return async.NewRandomRobinPolicy(), nil
	default:
		return nil, fmt.Errorf("unknown request merge policy: %s", name)
	}
}

func loadFlow(opts *Options, gateFactory *flowcontrol.GateFactory, poolsMap map[string]pipeline.WorkerPoolConfig) (pipeline.Flow, error) {
	workerPools := make([]pipeline.WorkerPoolConfig, 0, len(poolsMap))
	for _, p := range poolsMap {
		workerPools = append(workerPools, p)
	}
	switch opts.Queue.Impl {
	case "redis-pubsub":
		return redis.NewRedisMQFlow(opts.Redis, opts.RedisConnection, redis.WithRedisTracing(opts.Observability.RedisTracing), redis.WithWorkerPools(workerPools))
	case "redis-sortedset":
		return redis.NewRedisSortedSetFlow(opts.RedisSortedSet, opts.RedisConnection, redis.WithGateFactory(gateFactory), redis.WithSortedSetRedisTracing(opts.Observability.RedisTracing), redis.WithSortedSetWorkerPools(workerPools))
	case "gcp-pubsub":
		return pubsub.NewGCPPubSubMQFlow(opts.PubSub, pubsub.WithWorkerPools(workerPools))
	case "gcp-pubsub-gated":
		return pubsub.NewGCPPubSubMQFlow(opts.PubSub, pubsub.WithGateFactory(gateFactory), pubsub.WithWorkerPools(workerPools))
	default:
		return nil, fmt.Errorf("unknown message queue implementation: %s", opts.Queue.Impl)
	}
}

func initHealthServer(impl pipeline.Flow, serverCfg ServerConfig, setupLog logr.Logger) (*health.Server, error) {
	var checker health.Checker
	if hc, ok := impl.(pipeline.HealthChecker); ok {
		checker = hc.HealthCheck
	}
	healthServer := health.NewServer(serverCfg.HealthPort, checker, setupLog.WithName("health"))
	healthLn, err := healthServer.ListenAndServe()
	if err != nil {
		setupLog.Error(err, "Failed to bind health server")
		return nil, err
	}
	go func() {
		if err := healthServer.Serve(healthLn); err != nil {
			setupLog.Error(err, "Health server failed")
		}
	}()
	return healthServer, nil
}

func startMetricsServer(ctx context.Context, serverCfg ServerConfig, setupLog logr.Logger) error {
	metricsServerOptions := metricsserver.Options{
		BindAddress: fmt.Sprintf(":%d", serverCfg.MetricsPort),
		FilterProvider: func() func(c *rest.Config, httpClient *http.Client) (metricsserver.Filter, error) {
			if serverCfg.MetricsEndpointAuth {
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
	go msrv.Start(ctx) //nolint:errcheck
	return nil
}

func initInferenceClient(tlsCfg TLSConfig, totalConcurrency int) (*asyncworker.HTTPInferenceClient, error) {
	tlsConfig, err := buildTLSConfig(tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS configuration: %w", err)
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
	return asyncworker.NewHTTPInferenceClient(inferenceHTTPClient), nil
}

func loadTransforms(ctx context.Context, configFile string, setupLog logr.Logger) (*transform.Chain, error) {
	if configFile == "" {
		return nil, nil
	}
	cfg, err := transform.LoadConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load transform configuration: %w", err)
	}
	chain, err := transform.BuildChain(cfg.RequestTransforms, plugins.NewHandle(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to build request transform chain: %w", err)
	}
	setupLog.Info("Loaded request transform plugins", "count", chain.Len())
	return chain, nil
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
