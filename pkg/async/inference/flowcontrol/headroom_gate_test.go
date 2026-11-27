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
	"math"
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
	// queueText and readyText, when set, replace the values as written, for samples that are
	// not integers.
	queueText, readyText atomic.Pointer[string]
	// hold, when set, keeps every scrape open until the channel is closed.
	hold atomic.Pointer[chan struct{}]
}

func (m *eppMetrics) text(override *atomic.Pointer[string], v *atomic.Int64) string {
	if t := override.Load(); t != nil {
		return *t
	}
	return fmt.Sprint(v.Load())
}

func newEPPMetrics(t *testing.T, queue, ready int64) (*eppMetrics, string) {
	t.Helper()
	m := &eppMetrics{}
	m.queue.Store(queue)
	m.ready.Store(ready)
	m.status.Store(http.StatusOK)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		m.scrapes.Add(1)
		if hold := m.hold.Load(); hold != nil {
			<-*hold
		}
		w.WriteHeader(int(m.status.Load()))
		_, _ = fmt.Fprintf(w, "llm_d_epp_ready_endpoints{name=\"pool\"} %s\n", m.text(&m.readyText, &m.ready))
		if !m.absent.Load() {
			_, _ = fmt.Fprintf(w, "llm_d_epp_flow_control_queue_size{priority=\"-10\"} %s\n", m.text(&m.queueText, &m.queue))
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

func TestHeadroomGateBudgetStaysOpenUntilTheLastSlot(t *testing.T) {
	_, url := newEPPMetrics(t, 0, 1)
	g := NewHeadroomGate(headroomSourceFor(url, 4), time.Minute)
	assert.Equal(t, 1.0, g.Budget(context.Background()))
	admitCount(t, g, 3)
	assert.Equal(t, 1.0, g.Budget(context.Background()),
		"the dispatcher scales its batch by the budget, so a fraction would strand the last slot")
	admitCount(t, g, 1)
	assert.Zero(t, g.Budget(context.Background()))
}

func TestHeadroomGateInvalidReadingAdmitsNothing(t *testing.T) {
	text := func(s string) *string { return &s }
	for name, set := range map[string]func(m *eppMetrics){
		"NaN queue":      func(m *eppMetrics) { m.queueText.Store(text("NaN")) },
		"negative queue": func(m *eppMetrics) { m.queueText.Store(text("-5")) },
		"+Inf queue":     func(m *eppMetrics) { m.queueText.Store(text("+Inf")) },
		"NaN pods":       func(m *eppMetrics) { m.readyText.Store(text("NaN")) },
		"+Inf pods":      func(m *eppMetrics) { m.readyText.Store(text("+Inf")) },
	} {
		t.Run(name, func(t *testing.T) {
			m, url := newEPPMetrics(t, 0, 8)
			set(m)
			g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
			assert.Zero(t, g.Budget(context.Background()))
			assert.Zero(t, admitCount(t, g, 100))
		})
	}
}

func TestHeadroomGateRejectsANonFiniteHeadroom(t *testing.T) {
	for _, h := range []float64{math.NaN(), math.Inf(1), -1} {
		g := NewHeadroomGate(fixedHeadroom(h), time.Minute)
		assert.Zero(t, admitCount(t, g, 10), "headroom %g", h)
	}
}

type fixedHeadroom float64

func (f fixedHeadroom) Headroom(context.Context) (float64, error) { return float64(f), nil }

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

func TestHeadroomGateReleaseReturnsTheSlot(t *testing.T) {
	_, url := newEPPMetrics(t, 60, 8)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
	ctx := context.Background()

	var releases []pipeline.GateReleaseFunc
	for range 4 {
		v, err := g.Apply(ctx, nil, &releases)
		require.NoError(t, err)
		require.Equal(t, pipeline.ActionContinue, v.Action)
	}
	require.Len(t, releases, 4, "each admission registers one release")
	assert.Zero(t, admitCount(t, g, 5))
	assert.Zero(t, g.Budget(ctx))

	releases[0]()
	assert.Equal(t, 1.0, g.Budget(ctx))
	assert.Equal(t, 1, admitCount(t, g, 5), "a released slot is admitted once more")

	pipeline.ReleaseGateReleases(releases[1:])
	assert.Equal(t, 3, admitCount(t, g, 10))
}

func TestHeadroomGateRefusalRegistersNoRelease(t *testing.T) {
	_, url := newEPPMetrics(t, 64, 8)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)

	var releases []pipeline.GateReleaseFunc
	v, err := g.Apply(context.Background(), nil, &releases)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, v.Action)
	assert.Empty(t, releases)
}

func TestHeadroomGateIgnoresAReleaseFromAnOlderReading(t *testing.T) {
	m, url := newEPPMetrics(t, 60, 8)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
	clock := time.Unix(1000, 0)
	g.now = func() time.Time { return clock }
	ctx := context.Background()

	var stale []pipeline.GateReleaseFunc
	for range 4 {
		_, err := g.Apply(ctx, nil, &stale)
		require.NoError(t, err)
	}

	m.queue.Store(62)
	clock = clock.Add(time.Minute)
	assert.Equal(t, 2, admitCount(t, g, 10), "the next reading has two free slots")

	pipeline.ReleaseGateReleases(stale)
	assert.Zero(t, g.Budget(ctx))
	assert.Zero(t, admitCount(t, g, 10), "slots of an older reading do not reopen the current one")
}

func TestHeadroomGateDoesNotRecordAReadingThatFailedOnTheCallersContext(t *testing.T) {
	_, url := newEPPMetrics(t, 60, 8)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
	clock := time.Unix(1000, 0)
	g.now = func() time.Time { return clock }

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := g.Apply(canceled, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, g.Budget(canceled))

	assert.Equal(t, 4, admitCount(t, g, 10), "the next caller takes its own reading within the same interval")
}

func TestHeadroomGateInFlightReadingBlocksNeitherReleaseNorACanceledCaller(t *testing.T) {
	m, url := newEPPMetrics(t, 60, 8)
	g := NewHeadroomGate(headroomSourceFor(url, 8), time.Minute)
	clock := time.Unix(1000, 0)
	g.now = func() time.Time { return clock }

	var releases []pipeline.GateReleaseFunc
	_, err := g.Apply(context.Background(), nil, &releases)
	require.NoError(t, err)
	require.Len(t, releases, 1)

	hold := make(chan struct{})
	m.hold.Store(&hold)
	clock = clock.Add(time.Minute)
	before := m.scrapes.Load()
	reading := make(chan error, 1)
	go func() {
		_, err := g.Apply(context.Background(), nil, nil)
		reading <- err
	}()
	require.Eventually(t, func() bool { return m.scrapes.Load() > before }, 5*time.Second, time.Millisecond,
		"the reading is in flight")

	released := make(chan struct{})
	go func() {
		releases[0]()
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("a release waited for the in-flight reading")
	}

	waiter, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = g.Apply(waiter, nil, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Zero(t, g.Budget(waiter))

	close(hold)
	require.NoError(t, <-reading)
	assert.Equal(t, 3, admitCount(t, g, 10), "64 slots, 60 used, one taken by the caller that read")
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

	_, err = NewGateFactoryWithCacheTTL("", 0).CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: base()})
	require.ErrorContains(t, err, "--prometheus-cache-ttl", "counted admission has no reading interval without a cache TTL")
}
