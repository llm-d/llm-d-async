package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
			"DispatchBudget":   DispatchBudget,
			"PoolWorkerLimit":  PoolWorkerLimit,
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

func TestSetDispatchBudget(t *testing.T) {
	SetDispatchBudget(0.42, "q1", "queue-1", "pool-a")
	got := testutil.ToFloat64(DispatchBudget.WithLabelValues("q1", "queue-1", "pool-a"))
	if got != 0.42 {
		t.Errorf("DispatchBudget = %v, want 0.42", got)
	}
}

func TestSetPoolWorkerLimit(t *testing.T) {
	SetPoolWorkerLimit("pool-a", 8)
	got := testutil.ToFloat64(PoolWorkerLimit.WithLabelValues("pool-a"))
	if got != 8 {
		t.Errorf("PoolWorkerLimit = %v, want 8", got)
	}
}

func TestRecordGateDecision(t *testing.T) {
	RecordGateDecision(ReasonQuotaExhausted, "q9", "queue-9", "pool-z")
	RecordGateDecision(ReasonQuotaExhausted, "q9", "queue-9", "pool-z")
	RecordGateDecision(ReasonGateClosed, "q9", "queue-9", "pool-z")

	if got := testutil.ToFloat64(GateDecisions.WithLabelValues("q9", "queue-9", "pool-z", ReasonQuotaExhausted)); got != 2 {
		t.Errorf("GateDecisions[quota_exhausted] = %v, want 2", got)
	}
	if got := testutil.ToFloat64(GateDecisions.WithLabelValues("q9", "queue-9", "pool-z", ReasonGateClosed)); got != 1 {
		t.Errorf("GateDecisions[gate_closed] = %v, want 1", got)
	}
}

func TestGetAsyncProcessorCollectors_includesGateDecisions(t *testing.T) {
	for _, withLatency := range []bool{false, true} {
		collectors := GetAsyncProcessorCollectors(withLatency)
		if !containsCollector(collectors, GateDecisions) {
			t.Errorf("expected GateDecisions to be present (supportsMessageLatency=%v)", withLatency)
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
