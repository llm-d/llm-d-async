/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flowcontrol

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	redisflow "github.com/llm-d/llm-d-async/pkg/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestGateInstanceMetricsAcrossQueueReload(t *testing.T) {
	var holdOldQuery atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sample := `[{"metric":{},"value":[1,"0.7"]}]`
		switch r.FormValue("query") {
		case "high":
			if holdOldQuery.CompareAndSwap(true, false) {
				close(started)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
		case "low":
			sample = `[{"metric":{},"value":[1,"0.1"]}]`
		case "missing":
			sample = `[]`
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":%s}}`, sample)
	}))
	defer server.Close()
	defer resume()
	factory := NewGateFactoryWithCacheTTL(server.URL, 0)
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.GetAsyncProcessorCollectors(false)...)

	queue := redisflow.SortedSetQueueConfig{
		ID: t.Name(), QueueName: "metric-instances", IGWBaseURL: "http://gw",
		GateConfig: pipeline.GateConfig{GateType: "wait-on-refuse", GateParams: map[string]any{
			"gate": `{"gate_type":"composite","gate_params":{"gates":[
				{"gate_type":"prometheus-query","gate_params":{"query":"high","pool":"shared"}},
				{"gate_type":"wait-on-refuse","gate_params":{"gate":
					{"gate_type":"prometheus-query","gate_params":{"query":"low","pool":"shared"}}}},
				{"gate_type":"prometheus-query","gate_params":{"query":"missing","pool":"shared"}}
			]}}`,
		}},
	}
	flow := newMetricTestFlow(t, factory, queue)
	ctx := context.Background()
	old := flow.RequestChannels()[0].Gate
	old.Budget(ctx)
	owner := pipeline.GateOwner{QueueID: queue.ID, QueueName: queue.QueueName, WorkerPoolID: "default"}
	samples := func(suffix string) map[string]float64 {
		t.Helper()
		return gateInstanceSamples(t, registry, suffix, owner, "shared")
	}
	const first, second, missing = "prometheus-query/0.0", "prometheus-query/0.1.0", "prometheus-query/0.2"
	require.Equal(t, map[string]float64{first: .7, second: .1}, samples("value"))
	require.Equal(t, map[string]float64{first: 0, second: 0}, samples("threshold"))
	require.Equal(t, map[string]float64{first: 1, second: 1, missing: 0}, samples("source_available"))

	queue.GateConfig = pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{"threshold": .5, "pool": "shared"}}
	_, err := flow.ReconfigureQueues([]redisflow.SortedSetQueueConfig{queue}, func([]pipeline.RequestChannel) error {
		return errors.New("reject replacement")
	})
	require.Error(t, err)
	require.Equal(t, map[string]float64{first: .7, second: .1}, samples("value"))
	holdOldQuery.Store(true)
	finished := make(chan struct{})
	go func() { defer close(finished); old.Budget(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("old gate did not start its query")
	}
	_, err = flow.ReconfigureQueues([]redisflow.SortedSetQueueConfig{queue}, nil)
	require.NoError(t, err)
	current := flow.RequestChannels()[0].Gate
	current.Budget(ctx)
	resume()
	<-finished
	require.Equal(t, map[string]float64{"prometheus-saturation/": .7}, samples("value"))
	require.Equal(t, map[string]float64{"prometheus-saturation/": .5}, samples("threshold"))
	require.Equal(t, map[string]float64{"prometheus-saturation/": 1}, samples("source_available"))

	_, err = flow.ReconfigureQueues(nil, nil)
	require.NoError(t, err)
	current.Budget(ctx)
	for _, metric := range []string{"value", "threshold", "source_available"} {
		require.Empty(t, samples(metric))
	}
}

func gateInstanceSamples(t *testing.T, registry *prometheus.Registry, suffix string, owner pipeline.GateOwner, inferencePool string) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	got := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != metrics.SchedulerSubsystem+"_async_gate_instance_metric_"+suffix {
			continue
		}
		for _, sample := range family.Metric {
			labels := make(map[string]string)
			for _, label := range sample.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels[metrics.LabelQueueID] == owner.QueueID && labels[metrics.LabelQueueName] == owner.QueueName &&
				labels[metrics.LabelPoolName] == owner.WorkerPoolID && labels[metrics.LabelInferencePool] == inferencePool {
				got[labels["gate_type"]+"/"+labels["gate_index"]] = sample.GetGauge().GetValue()
			}
		}
	}
	return got
}

func newMetricTestFlow(t *testing.T, factory pipeline.GateFactory, queues ...redisflow.SortedSetQueueConfig) *redisflow.RedisSortedSetFlow {
	t.Helper()
	flow, err := redisflow.NewRedisSortedSetFlow(redisflow.SortedSetConfig{
		URL: "redis://" + miniredis.RunT(t).Addr(), Queues: queues,
	}, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, factory)
	require.NoError(t, err)
	t.Cleanup(flow.Shutdown)
	return flow
}

func TestEndpointScrapeInstancesHaveSeparateQueueOwners(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "# TYPE capacity gauge\ncapacity 0.75\n")
	}))
	defer server.Close()
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.GetAsyncProcessorCollectors(false)...)
	queue := func(id string) redisflow.SortedSetQueueConfig {
		return redisflow.SortedSetQueueConfig{ID: id, QueueName: id, IGWBaseURL: "http://gw",
			GateConfig: pipeline.GateConfig{GateType: "endpoint-scrape",
				GateParams: map[string]any{"url": server.URL, "metric": "capacity", "value_type": "budget"}},
		}
	}
	first, second := queue(t.Name()+"-a"), queue(t.Name()+"-b")
	flow := newMetricTestFlow(t, NewGateFactoryWithCacheTTL("", 0), first, second)
	for _, ch := range flow.RequestChannels() {
		require.Equal(t, .75, ch.Gate.Budget(context.Background()))
	}
	values := func(queue redisflow.SortedSetQueueConfig) map[string]float64 {
		owner := pipeline.GateOwner{QueueID: queue.ID, QueueName: queue.QueueName, WorkerPoolID: "default"}
		return gateInstanceSamples(t, registry, "value", owner, "")
	}
	expected := map[string]float64{"endpoint-scrape/": .75}
	require.Equal(t, expected, values(first))
	_, err := flow.ReconfigureQueues([]redisflow.SortedSetQueueConfig{second}, nil)
	require.NoError(t, err)
	require.Empty(t, values(first))
	require.Equal(t, expected, values(second))
	_, err = flow.ReconfigureQueues(nil, nil)
	require.NoError(t, err)
}
