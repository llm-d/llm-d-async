package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestGetAsyncProcessorCollectors_withoutLatency(t *testing.T) {
	collectors := GetAsyncProcessorCollectors(false)
	if containsCollector(collectors, MessageLatencyTime) {
		t.Error("expected MessageLatencyTime to be absent when supportsMessageLatency=false")
	}
}

func TestGetAsyncProcessorCollectors_withLatency(t *testing.T) {
	collectors := GetAsyncProcessorCollectors(true)
	if !containsCollector(collectors, MessageLatencyTime) {
		t.Error("expected MessageLatencyTime to be present when supportsMessageLatency=true")
	}
}

func containsCollector(collectors []prometheus.Collector, target prometheus.Collector) bool {
	for _, c := range collectors {
		if c == target {
			return true
		}
	}
	return false
}
