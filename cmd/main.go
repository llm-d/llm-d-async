package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	"github.com/llm-d-incubation/llm-d-async/pkg/async"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/pubsub"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"
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
	var requestMergePolicy string
	var messageQueueImpl string
	var dispatchGateType string

	var igwBaseURL string

	flag.IntVar(&loggerVerbosity, "v", logging.DEFAULT, "number for the log level verbosity")

	flag.IntVar(&metricsPort, "metrics-port", 9090, "The metrics port")
	flag.BoolVar(&metricsEndpointAuth, "metrics-endpoint-auth", true, "Enables authentication and authorization of the metrics endpoint")

	flag.IntVar(&concurrency, "concurrency", 8, "number of concurrent workers")

	flag.StringVar(&igwBaseURL, "igw-base-url", "", "Base URL of the IGW (e.g. https://localhost:30800)")
	flag.StringVar(&requestMergePolicy, "request-merge-policy", "random-robin", "The request merge policy to use. Supported policies: random-robin")
	flag.StringVar(&dispatchGateType, "dispatch-gate", "noop", "The dispatch gate policy to use. Supported policies: noop, redis, metric-avg-queue-size")
	flag.StringVar(&messageQueueImpl, "message-queue-impl", "redis-pubsub", "The message queue implementation to use. Supported implementations: redis-pubsub, redis-sortedset, redis-sortedset-gated, gcp-pubsub, gcp-pubsub-gated")

	opts := zap.Options{
		Development: true,
	}

	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	logging.InitLogging(&opts, loggerVerbosity)
	defer logging.Sync() // nolint:errcheck

	setupLog := ctrl.Log.WithName("setup")
	setupLog.Info("Logger initialized")

	////////setupLog.Info("GIE build", "commit-sha", version.CommitSHA, "build-ref", version.BuildRef)

	printAllFlags(setupLog)
	// Create dispatch gate
	var gate flowcontrol.DispatchGate
	switch dispatchGateType {
	case "noop":
		gate = flowcontrol.ConstOpenGate()
	case "redis":
		gate = redis.NewRedisDispatchGate()
		setupLog.Info("Using Redis-based dispatch gate")
	case "metric-avg-queue-size":
		gate = flowcontrol.AverageQueueSizeGate()
	default:
		setupLog.Error(fmt.Errorf("unknown dispatch gate type: %s", dispatchGateType), "Unknown dispatch gate type", "dispatch-gate", dispatchGateType)
		os.Exit(1)
	}

	var policy api.RequestMergePolicy
	switch requestMergePolicy {
	case "random-robin":
		policy = async.NewRandomRobinPolicy()
	default:
		setupLog.Error(fmt.Errorf("unknown request merge policy: %s", requestMergePolicy), "Unknown request merge policy", "request-merge-policy",
			requestMergePolicy)
		os.Exit(1)
	}
	var impl api.Flow
	switch messageQueueImpl {
	case "redis-pubsub":
		impl = redis.NewRedisMQFlow()
	case "redis-sortedset":
		impl = redis.NewRedisSortedSetFlow()
	case "redis-sortedset-gated":
		impl = redis.NewRedisSortedSetFlow(redis.WithDispatchGate(gate))
		setupLog.Info("Using Redis sorted-set flow with dispatch gating", "gate-type", dispatchGateType)
	case "gcp-pubsub":
		impl = pubsub.NewGCPPubSubMQFlow()
	case "gcp-pubsub-gated":
		impl = pubsub.NewGCPPubSubMQFlow(pubsub.WithDispatchGate(gate))
		setupLog.Info("Using GCP PubSub flow with dispatch gating", "gate-type", dispatchGateType)
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
	httpClient := http.DefaultClient

	msrv, _ := metricsserver.NewServer(metricsServerOptions, restConfig, httpClient)
	go msrv.Start(ctx) // nolint:errcheck

	/////

	igwBaseURL = util.NormalizeBaseURL(igwBaseURL)

	requestChannel := policy.MergeRequestChannels(impl.RequestChannels()).Channel
	for w := 1; w <= concurrency; w++ {

		go api.Worker(ctx, impl.Characteristics(), igwBaseURL, httpClient, requestChannel, impl.RetryChannel(), impl.ResultChannel())
	}

	impl.Start(ctx)
	<-ctx.Done()
}

func printAllFlags(setupLog logr.Logger) {
	flags := make(map[string]any)
	flag.VisitAll(func(f *flag.Flag) {
		flags[f.Name] = f.Value
	})
	setupLog.Info("Flags processed", "flags", flags)
}
