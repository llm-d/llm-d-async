package metrics

import (
	"sync"

	"github.com/llm-d/llm-d-async/pipeline"
)

type gateMetricsRetirer interface {
	RetireGateMetrics()
}

// RetireGateMetrics releases instance series when a gate that owns them is removed.
func RetireGateMetrics(gate pipeline.Gate) {
	if r, ok := gate.(gateMetricsRetirer); ok {
		r.RetireGateMetrics()
	}
}

// GateInstanceMetrics identifies one metric gate in its owner's configuration.
// Its labels are set before the gate is used.
type GateInstanceMetrics struct {
	Owner         pipeline.GateOwner
	InferencePool string
	Type          string
	Index         string

	mu      sync.Mutex
	retired bool
}

func (g *GateInstanceMetrics) labels() []string {
	return []string{g.Owner.QueueID, g.Owner.QueueName, g.Owner.WorkerPoolID, g.InferencePool, g.Type, g.Index}
}

// Record preserves the last usable value and threshold while a source is unavailable.
func (g *GateInstanceMetrics) Record(value, threshold float64, available bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.retired || g.Type == "" {
		return
	}
	labels := g.labels()
	sourceAvailable := 0.0
	if available {
		gateInstanceValue.WithLabelValues(labels...).Set(value)
		gateInstanceThreshold.WithLabelValues(labels...).Set(threshold)
		sourceAvailable = 1
	}
	gateInstanceSourceAvailable.WithLabelValues(labels...).Set(sourceAvailable)
}

// Retire removes this gate's instance series before its replacement starts.
// A late query must not recreate those series or update the replacement's readings.
// Sibling gates share the four-label v1 gauges, so retiring one leaves them intact.
func (g *GateInstanceMetrics) Retire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.retired {
		return
	}
	g.retired = true
	labels := g.labels()
	gateInstanceValue.DeleteLabelValues(labels...)
	gateInstanceThreshold.DeleteLabelValues(labels...)
	gateInstanceSourceAvailable.DeleteLabelValues(labels...)
}
