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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eppMetrics serves an EPP-style /metrics page whose batch queue depth and ready endpoint count
// the test sets; status is the HTTP status to answer with.
type eppMetrics struct {
	queue, ready, status atomic.Int64
	scrapes              atomic.Int64
	absent               atomic.Bool
}

func newEPPMetrics(t *testing.T, queue, ready int64) (*eppMetrics, string) {
	t.Helper()
	m := &eppMetrics{}
	m.queue.Store(queue)
	m.ready.Store(ready)
	m.status.Store(http.StatusOK)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		m.scrapes.Add(1)
		w.WriteHeader(int(m.status.Load()))
		_, _ = fmt.Fprintf(w, "llm_d_epp_ready_endpoints{name=\"pool\"} %d\n", m.ready.Load())
		if !m.absent.Load() {
			_, _ = fmt.Fprintf(w, "llm_d_epp_flow_control_queue_size{priority=\"-10\"} %d\n", m.queue.Load())
		}
	}))
	t.Cleanup(srv.Close)
	return m, srv.URL
}

func headroomSourceFor(url string, perPod float64) *ScrapeMetricSource {
	zero := 0.0
	return NewScrapeMetricSource(ScrapeConfig{
		URL:            url,
		MetricName:     "llm_d_epp_flow_control_queue_size",
		Labels:         map[string]string{"priority": "-10"},
		MaxCountPerPod: perPod,
		PodsURL:        url,
		PodsMetric:     "llm_d_epp_ready_endpoints",
		AbsentValue:    &zero,
	})
}

func admitCount(t *testing.T, g *HeadroomGate, n int) int {
	t.Helper()
	admitted := 0
	for range n {
		v, err := g.Apply(context.Background(), nil, nil)
		require.NoError(t, err)
		if v.Action == pipeline.ActionContinue {
			admitted++
		}
	}
	return admitted
}

func TestHeadroomGateAdmitsEachReadingsFreeSlots(t *testing.T) {
	m, url := newEPPMetrics(t, 60, 8)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
	clock := time.Unix(1000, 0)
	g.now = func() time.Time { return clock }

	assert.Equal(t, 4, admitCount(t, g, 10), "8 pods x 8 = 64 slots, 60 used")
	m.queue.Store(10)
	assert.Zero(t, admitCount(t, g, 10), "a new value is not read before the interval passes")
	assert.EqualValues(t, 1, m.scrapes.Load()/2, "one reading (queue and pods scrapes) per interval")

	clock = clock.Add(time.Minute)
	assert.Equal(t, 54, admitCount(t, g, 100), "the next reading grants its own headroom")

	m.ready.Store(4)
	m.queue.Store(0)
	clock = clock.Add(time.Minute)
	assert.Equal(t, 32, admitCount(t, g, 100), "capacity follows the ready pod count")
}

func TestHeadroomGateFailedReadingAdmitsNothing(t *testing.T) {
	m, url := newEPPMetrics(t, 0, 8)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
	clock := time.Unix(1000, 0)
	g.now = func() time.Time { return clock }

	m.status.Store(http.StatusServiceUnavailable)
	assert.Zero(t, admitCount(t, g, 10))
	assert.Zero(t, g.Budget(context.Background()))

	m.status.Store(http.StatusOK)
	m.ready.Store(0)
	clock = clock.Add(time.Minute)
	assert.Zero(t, admitCount(t, g, 10), "an empty pool has no capacity")

	m.ready.Store(8)
	clock = clock.Add(time.Minute)
	assert.Equal(t, 64, admitCount(t, g, 100), "the gate reopens on the next good reading")
}

func TestHeadroomGateReadsAnAbsentSeriesAsEmpty(t *testing.T) {
	m, url := newEPPMetrics(t, 0, 2)
	m.absent.Store(true)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
	assert.Equal(t, 16, admitCount(t, g, 100))
}

func TestHeadroomGateBudgetTracksClaimedSlots(t *testing.T) {
	_, url := newEPPMetrics(t, 0, 1)
	g := NewHeadroomGate(headroomSourceFor(url, 4), time.Minute)
	assert.InDelta(t, 1.0, g.Budget(context.Background()), 1e-9)
	admitCount(t, g, 1)
	assert.InDelta(t, 0.75, g.Budget(context.Background()), 1e-9)
	admitCount(t, g, 3)
	assert.Zero(t, g.Budget(context.Background()))
}

func TestHeadroomGateNeverOverAdmitsUnderConcurrency(t *testing.T) {
	_, url := newEPPMetrics(t, 54, 8)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := g.Apply(context.Background(), nil, nil)
			if err == nil && v.Action == pipeline.ActionContinue {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 10, admitted.Load())
}

func TestGateFactoryEndpointScrapeCountedAdmission(t *testing.T) {
	_, url := newEPPMetrics(t, 60, 8)
	base := func() map[string]any {
		return map[string]any{
			"url":               url,
			"metric":            "llm_d_epp_flow_control_queue_size",
			"labels":            map[string]any{"priority": "-10"},
			"max_count_per_pod": 8,
			"pods_url":          url,
			"pods_metric":       "llm_d_epp_ready_endpoints",
			"admission":         "counted",
		}
	}
	factory := NewGateFactoryWithCacheTTL("", time.Minute)

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: base()})
	require.NoError(t, err)
	require.IsType(t, &HeadroomGate{}, gate)
	assert.Equal(t, 4, admitCount(t, gate.(*HeadroomGate), 10))

	for name, change := range map[string]func(map[string]any){
		"no capacity":       func(p map[string]any) { delete(p, "max_count_per_pod") },
		"budget value type": func(p map[string]any) { p["value_type"] = "budget" },
		"baseline":          func(p map[string]any) { p["baseline"] = 0.1 },
		"fallback":          func(p map[string]any) { p["fallback"] = 1 },
		"unknown admission": func(p map[string]any) { p["admission"] = "greedy" },
	} {
		t.Run(name, func(t *testing.T) {
			p := base()
			change(p)
			_, err := factory.CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: p})
			require.Error(t, err)
		})
	}
}
