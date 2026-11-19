package pipeline

import "testing"

// The nil-means-unsupported contract: a zero-value stat must not be confused
// with "queue is drained" for the deadline views.
func TestQueueBacklogStatZeroValueHasNilDeadlineViews(t *testing.T) {
	var s QueueBacklogStat
	if s.ExpiringCounts != nil {
		t.Error("expected ExpiringCounts to be nil on a zero-value stat")
	}
	if s.Depth != 0 {
		t.Errorf("Depth = %d, want 0", s.Depth)
	}
}
