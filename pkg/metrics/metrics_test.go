package metrics

import (
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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
			"QueueDepth":                   QueueDepth,
			"InflightRequests":             InflightRequests,
			"BrokerBacklog":                BrokerBacklog,
			"BrokerBacklogSourceAvailable": BrokerBacklogSourceAvailable,
			"DispatchBudget":               DispatchBudget,
			"PoolWorkerLimit":              PoolWorkerLimit,
			"DrainLimitRPS":                DrainLimitRPS,
			"DrainLimitLeaseValid":         DrainLimitLeaseValid,
			"DrainLimitValidUntil":         DrainLimitValidUntil,
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

func TestRemoveQueueSnapshots(t *testing.T) {
	BrokerBacklog.Reset()
	BrokerBacklogSourceAvailable.Reset()
	DispatchBudget.Reset()
	DeadlineProximity.Reset()
	SetBrokerBacklog("q1", "queue-1", "pool-a", 2)
	SetBrokerBacklogSourceAvailable("q1", "queue-1", "pool-a", true)
	SetDispatchBudget(0.42, "q1", "queue-1", "pool-a")
	SetDeadlineProximity("q1", "queue-1", "pool-a", make([]int64, len(DeadlineProximityBuckets())))

	RemoveQueueSnapshots("q1", "queue-1", "pool-a")
	if got := testutil.CollectAndCount(BrokerBacklog); got != 0 {
		t.Errorf("BrokerBacklog still exposes %d series, want 0", got)
	}
	if got := testutil.CollectAndCount(BrokerBacklogSourceAvailable); got != 0 {
		t.Errorf("BrokerBacklogSourceAvailable still exposes %d series, want 0", got)
	}
	if got := testutil.CollectAndCount(DispatchBudget); got != 0 {
		t.Errorf("DispatchBudget still exposes %d series, want 0", got)
	}
	if got := testutil.CollectAndCount(DeadlineProximity); got != 0 {
		t.Errorf("DeadlineProximity still exposes %d series, want 0", got)
	}
}

func TestSetPoolWorkerLimit(t *testing.T) {
	SetPoolWorkerLimit("pool-a", 8)
	got := testutil.ToFloat64(PoolWorkerLimit.WithLabelValues("pool-a"))
	if got != 8 {
		t.Errorf("PoolWorkerLimit = %v, want 8", got)
	}
}

func TestSetBrokerBacklogSourceAvailable(t *testing.T) {
	labels := []string{"q-backlog", "queue-backlog", "pool-backlog"}
	SetBrokerBacklogSourceAvailable(labels[0], labels[1], labels[2], true)
	if got := testutil.ToFloat64(BrokerBacklogSourceAvailable.WithLabelValues(labels...)); got != 1 {
		t.Errorf("BrokerBacklogSourceAvailable = %v, want 1", got)
	}
	SetBrokerBacklogSourceAvailable(labels[0], labels[1], labels[2], false)
	if got := testutil.ToFloat64(BrokerBacklogSourceAvailable.WithLabelValues(labels...)); got != 0 {
		t.Errorf("BrokerBacklogSourceAvailable = %v, want 0", got)
	}
}

func TestRecordDispatchedReq(t *testing.T) {
	before := testutil.ToFloat64(DispatchedReqs.WithLabelValues("q-dispatch", "queue-dispatch", "pool-dispatch"))
	RecordDispatchedReq("q-dispatch", "queue-dispatch", "pool-dispatch")
	got := testutil.ToFloat64(DispatchedReqs.WithLabelValues("q-dispatch", "queue-dispatch", "pool-dispatch"))
	if got != before+1 {
		t.Errorf("DispatchedReqs = %v, want %v", got, before+1)
	}
}

func TestSetDrainLimit(t *testing.T) {
	SetDrainLimit("pool-limited", 12.5, 1_700_000_123_000, true)
	if got := testutil.ToFloat64(DrainLimitRPS.WithLabelValues("pool-limited")); got != 12.5 {
		t.Errorf("DrainLimitRPS = %v, want 12.5", got)
	}
	if got := testutil.ToFloat64(DrainLimitLeaseValid.WithLabelValues("pool-limited")); got != 1 {
		t.Errorf("DrainLimitLeaseValid = %v, want 1", got)
	}
	if got := testutil.ToFloat64(DrainLimitValidUntil.WithLabelValues("pool-limited")); got != 1_700_000_123 {
		t.Errorf("DrainLimitValidUntil = %v, want 1700000123", got)
	}

	SetDrainLimit("pool-limited", 99, 1_800_000_000_000, false)
	if got := testutil.ToFloat64(DrainLimitRPS.WithLabelValues("pool-limited")); got != 0 {
		t.Errorf("invalid lease DrainLimitRPS = %v, want 0", got)
	}
	if got := testutil.ToFloat64(DrainLimitLeaseValid.WithLabelValues("pool-limited")); got != 0 {
		t.Errorf("invalid lease DrainLimitLeaseValid = %v, want 0", got)
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

func TestInitGateDecisions(t *testing.T) {
	InitGateDecisions("q10", "queue-10", "pool-y")

	for _, reason := range []string{ReasonGateClosed, ReasonQuotaExhausted, ReasonDropped, ReasonError} {
		// A never-incremented CounterVec label set is absent from /metrics, so
		// the point of the pre-creation is that these series exist and read 0.
		if got := testutil.ToFloat64(GateDecisions.WithLabelValues("q10", "queue-10", "pool-y", reason)); got != 0 {
			t.Errorf("GateDecisions[%s] = %v, want 0", reason, got)
		}
	}
	if got := testutil.CollectAndCount(GateDecisions, "llm_d_async_async_gate_decisions_total"); got < 4 {
		t.Errorf("GateDecisions series count = %d, want at least 4", got)
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

func TestGetAsyncProcessorCollectors_includesTokens(t *testing.T) {
	for _, withLatency := range []bool{false, true} {
		collectors := GetAsyncProcessorCollectors(withLatency)
		if !containsCollector(collectors, Tokens) {
			t.Errorf("expected Tokens to be present (supportsMessageLatency=%v)", withLatency)
		}
	}
}

func TestGetAsyncProcessorCollectors_includesDeadlineProximity(t *testing.T) {
	for _, withLatency := range []bool{false, true} {
		collectors := GetAsyncProcessorCollectors(withLatency)
		if !containsCollector(collectors, DeadlineProximity) {
			t.Errorf("expected DeadlineProximity to be present (supportsMessageLatency=%v)", withLatency)
		}
	}
}

func TestRecordTokens(t *testing.T) {
	Tokens.Reset()
	RecordTokens(25, 13, "q1", "queue-1", "pool-a")
	if got := testutil.ToFloat64(Tokens.WithLabelValues("q1", "queue-1", "pool-a", "input")); got != 25 {
		t.Errorf("input tokens = %v, want 25", got)
	}
	if got := testutil.ToFloat64(Tokens.WithLabelValues("q1", "queue-1", "pool-a", "output")); got != 13 {
		t.Errorf("output tokens = %v, want 13", got)
	}
}

// The bucket boundaries and their le labels define the histogram shape fed by
// the backlog poller, so both must be stable and in ascending order.
func TestDeadlineProximityBuckets(t *testing.T) {
	want := []time.Duration{0, time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second,
		time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute,
		time.Hour, 2 * time.Hour, 6 * time.Hour, 24 * time.Hour}
	buckets := DeadlineProximityBuckets()
	if len(buckets) != len(want) {
		t.Fatalf("len(DeadlineProximityBuckets()) = %d, want %d", len(buckets), len(want))
	}
	for i, w := range want {
		if buckets[i] != w {
			t.Errorf("DeadlineProximityBuckets()[%d] = %v, want %v", i, buckets[i], w)
		}
	}
	if !sort.SliceIsSorted(buckets, func(i, j int) bool { return buckets[i] < buckets[j] }) {
		t.Error("DeadlineProximityBuckets() is not ascending")
	}

	wantLabels := []string{"0", "1000", "5000", "15000", "30000", "60000", "120000",
		"300000", "600000", "1800000", "3600000", "7200000", "21600000", "86400000"}
	labels := DeadlineProximityBucketLabels()
	if len(labels) != len(wantLabels) {
		t.Fatalf("len(DeadlineProximityBucketLabels()) = %d, want %d", len(labels), len(wantLabels))
	}
	for i, l := range wantLabels {
		if labels[i] != l {
			t.Errorf("DeadlineProximityBucketLabels()[%d] = %q, want %q", i, labels[i], l)
		}
	}

	// Mutating the returned copies must not affect subsequent callers.
	buckets[0] = 99
	if got := DeadlineProximityBuckets()[0]; got != 0 {
		t.Errorf("DeadlineProximityBuckets() mutated by caller, got %v", got)
	}
	labels[0] = "tampered"
	if got := DeadlineProximityBucketLabels()[0]; got != "0" {
		t.Errorf("DeadlineProximityBucketLabels() mutated by caller, got %q", got)
	}
}

func TestSetDeadlineProximity(t *testing.T) {
	DeadlineProximity.Reset()
	// Cumulative counts for buckets 0, 1s, 5s, 15s, 30s, 1m, 2m, 5m, 10m, 30m,
	// 1h, 2h, 6h, 24h.
	cumulative := []int64{1, 2, 2, 2, 3, 3, 3, 4, 5, 6, 7, 7, 8, 9}
	SetDeadlineProximity("q1", "queue-1", "pool-a", cumulative)

	h := collectHistogram(t, DeadlineProximity, map[string]string{
		"queue_id": "q1", "queue_name": "queue-1", "pool_name": "pool-a",
	})
	if got := h.GetSampleCount(); got != 9 {
		t.Errorf("sample count = %d, want 9", got)
	}
	// Estimated from bucket midpoints: (0+1000)/2, (15000+30000)/2,
	// (120000+300000)/2, (300000+600000)/2, (600000+1800000)/2,
	// (1800000+3600000)/2, (7200000+21600000)/2, (21600000+86400000)/2.
	if got := h.GetSampleSum(); got != 72983000 {
		t.Errorf("sample sum = %v, want 72983000", got)
	}
	// Every le bucket is present, zero-count ones included.
	buckets := h.GetBucket()
	if len(buckets) != 14 {
		t.Fatalf("bucket count = %d, want 14", len(buckets))
	}
	wantCumulative := []uint64{1, 2, 2, 2, 3, 3, 3, 4, 5, 6, 7, 7, 8, 9}
	wantBounds := []float64{0, 1000, 5000, 15000, 30000, 60000, 120000, 300000, 600000,
		1800000, 3600000, 7200000, 21600000, 86400000}
	for i, b := range buckets {
		if b.GetUpperBound() != wantBounds[i] {
			t.Errorf("bucket[%d] upper bound = %v, want %v", i, b.GetUpperBound(), wantBounds[i])
		}
		if b.GetCumulativeCount() != wantCumulative[i] {
			t.Errorf("bucket[%d] cumulative count = %d, want %d", i, b.GetCumulativeCount(), wantCumulative[i])
		}
	}

	// Snapshot semantics: the next poll replaces, not accumulates.
	SetDeadlineProximity("q1", "queue-1", "pool-a", make([]int64, 14))
	h = collectHistogram(t, DeadlineProximity, map[string]string{
		"queue_id": "q1", "queue_name": "queue-1", "pool_name": "pool-a",
	})
	if got := h.GetSampleCount(); got != 0 {
		t.Errorf("sample count after empty poll = %d, want 0", got)
	}

	// A length mismatch is an internal invariant violation and is ignored.
	SetDeadlineProximity("q1", "queue-1", "pool-a", []int64{1})
	if got := collectHistogram(t, DeadlineProximity, map[string]string{
		"queue_id": "q1", "queue_name": "queue-1", "pool_name": "pool-a",
	}).GetSampleCount(); got != 0 {
		t.Errorf("sample count after bad-length Set = %d, want unchanged 0", got)
	}
}

// collectHistogram gathers a collector and returns the histogram whose labels
// match wantLabels.
func collectHistogram(t *testing.T, c prometheus.Collector, wantLabels map[string]string) *dto.Histogram {
	t.Helper()
	ch := make(chan prometheus.Metric, 8)
	c.Collect(ch)
	close(ch)
	for m := range ch {
		var dto dto.Metric
		if err := m.Write(&dto); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		matches := true
		for _, lp := range dto.GetLabel() {
			if want, ok := wantLabels[lp.GetName()]; !ok || lp.GetValue() != want {
				matches = false
				break
			}
		}
		if matches {
			return dto.GetHistogram()
		}
	}
	t.Fatalf("no histogram matched labels %v", wantLabels)
	return nil
}

func containsCollector(collectors []prometheus.Collector, target prometheus.Collector) bool {
	for _, c := range collectors {
		if c == target {
			return true
		}
	}
	return false
}
