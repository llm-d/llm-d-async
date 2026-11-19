package server

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/llm-d/llm-d-async/pkg/redis"
)

// startQueuesConfigReload periodically re-reads the sorted-set queues config
// file and applies queue set changes to the running flow: new and modified
// queues become consumable without a restart, removed queues are drained and
// their channels closed. An unchanged normalized config is a no-op, and a
// file that fails to read, parse or apply leaves the last good configuration
// exactly as it was.
//
// The merged channels observed by inference pool workers never change: the
// merge policy learns new sources through AddRequestChannels and forgets
// removed ones when the flow closes their channels.
func startQueuesConfigReload(
	ctx context.Context,
	path string,
	interval time.Duration,
	lastGood *redis.SortedSetConfig,
	flow redis.SortedSetQueueReconfigurer,
	policy pipeline.DynamicRequestMergePolicy,
	pools map[string]pipeline.WorkerPoolConfig,
	logger logr.Logger,
) <-chan struct{} {
	logger = logger.WithName("queues-config-reload").WithValues("path", path, "interval", interval)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		reload := func() {
			data, err := os.ReadFile(path) // #nosec G304 -- path from trusted CLI flag
			if err != nil {
				metrics.RecordQueueConfigReload(false)
				logger.Error(err, "Failed to read queues config file; keeping last good queues")
				return
			}
			cfg, err := redis.LoadSortedSetConfigAllowEmptyQueues(data)
			if err != nil {
				metrics.RecordQueueConfigReload(false)
				logger.Error(err, "Failed to parse queues config file; keeping last good queues")
				return
			}
			if reflect.DeepEqual(cfg, lastGood) {
				return
			}
			previous := *lastGood
			previous.Queues = nil
			next := *cfg
			next.Queues = nil
			if !reflect.DeepEqual(previous, next) {
				metrics.RecordQueueConfigReload(false)
				logger.Error(fmt.Errorf("non-queue transport fields changed"), "Rejected transport config reload; restart required")
				return
			}
			for _, queue := range cfg.Queues {
				if _, ok := pools[queue.WorkerPoolID]; !ok {
					metrics.RecordQueueConfigReload(false)
					logger.Error(fmt.Errorf("worker pool %q not found", queue.WorkerPoolID), "Rejected queues config reload; keeping last good queues")
					return
				}
			}

			result, err := flow.ReconfigureQueues(cfg.Queues, func(added []pipeline.RequestChannel) error {
				return policy.AddRequestChannels(added, pools)
			})
			if err != nil {
				metrics.RecordQueueConfigReload(false)
				logger.Error(err, "Failed to apply queues config; keeping last good queues")
				return
			}
			*lastGood = *cfg

			metrics.RecordQueueConfigReload(true)
			logger.Info("Applied queues config reload", "added", len(result.Added), "removed", len(result.Removed))
		}

		reload()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reload()
			}
		}
	}()
	return done
}

// validateQueuesConfigWatch enforces that hot reload is only requested in a
// configuration where it can actually work: file-based queues config and a
// flow/policy pair that both support runtime changes. Called once at
// startup so a miswired deployment fails fast instead of drifting silently.
func validateQueuesConfigWatch(opts *Options, flow pipeline.Flow, policy pipeline.RequestMergePolicy) (redis.SortedSetQueueReconfigurer, pipeline.DynamicRequestMergePolicy, bool, error) {
	interval := opts.Transport.ConfigWatchInterval
	if interval <= 0 {
		return nil, nil, false, nil
	}
	if !opts.usingNewTransport() || opts.Transport.Type != "redis-sortedset" || opts.Transport.ConfigFile == "" || opts.Transport.Config != "" {
		return nil, nil, false, fmt.Errorf("--transport-config-watch-interval requires --transport redis-sortedset with --transport-config-file")
	}
	reconfigurer, ok := flow.(redis.SortedSetQueueReconfigurer)
	if !ok {
		return nil, nil, false, fmt.Errorf("queues config hot reload requires the redis sorted-set transport")
	}
	dynamicPolicy, ok := policy.(pipeline.DynamicRequestMergePolicy)
	if !ok {
		return nil, nil, false, fmt.Errorf("queues config hot reload requires a merge policy supporting runtime channel changes")
	}
	return reconfigurer, dynamicPolicy, true, nil
}
