package metrics

import "testing"

func TestGetAsyncProcessorCollectors_withoutLatency(t *testing.T) {
	collectors := GetAsyncProcessorCollectors(false)
	if len(collectors) != 6 {
		t.Errorf("expected 6 collectors without latency, got %d", len(collectors))
	}
}

func TestGetAsyncProcessorCollectors_withLatency(t *testing.T) {
	collectors := GetAsyncProcessorCollectors(true)
	if len(collectors) != 7 {
		t.Errorf("expected 7 collectors with latency, got %d", len(collectors))
	}
}
