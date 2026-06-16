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

func TestGetAsyncProcessorCollectors_includesGauges(t *testing.T) {
	for _, withLatency := range []bool{false, true} {
		collectors := GetAsyncProcessorCollectors(withLatency)
		for name, gauge := range map[string]prometheus.Collector{
			"QueueDepth":       QueueDepth,
			"InflightRequests": InflightRequests,
			"BrokerBacklog":    BrokerBacklog,
		} {
			if !containsCollector(collectors, gauge) {
				t.Errorf("expected %s gauge to be present (supportsMessageLatency=%v)", name, withLatency)
			}
		}
	}
}

func TestGetAsyncProcessorCollectors_includesInferenceLatency(t *testing.T) {
	// Inference latency is measured in-process and is not gated on broker
	// support for publish timestamps, so it is always registered.
	for _, withLatency := range []bool{false, true} {
		collectors := GetAsyncProcessorCollectors(withLatency)
		if !containsCollector(collectors, InferenceLatencyTime) {
			t.Errorf("expected InferenceLatencyTime to be present (supportsMessageLatency=%v)", withLatency)
		}
	}
}

func TestGetAsyncProcessorCollectors_includesQueueResidence(t *testing.T) {
	// Queue residence time is measured in-process and is not gated on broker
	// support for publish timestamps, so it is always registered.
	for _, withLatency := range []bool{false, true} {
		collectors := GetAsyncProcessorCollectors(withLatency)
		if !containsCollector(collectors, QueueResidenceTime) {
			t.Errorf("expected QueueResidenceTime to be present (supportsMessageLatency=%v)", withLatency)
		}
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
