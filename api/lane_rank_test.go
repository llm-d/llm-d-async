package api

import "testing"

func TestLaneRank(t *testing.T) {
	tests := []struct {
		tier  PriorityTier
		class QuotaClassification
		want  int
	}{
		{TierInteractive, ClassificationReserved, 0},
		{TierAsync, ClassificationReserved, 1},
		{TierBatch, ClassificationReserved, 2},
		{TierInteractive, ClassificationOverflow, 3},
		{TierAsync, ClassificationOverflow, 4},
		{TierBatch, ClassificationOverflow, 5},
		// Unknown tiers rank as batch; unclassified ranks as overflow.
		{PriorityTier("mystery"), ClassificationNone, 5},
		{PriorityTier(""), ClassificationReserved, 2},
	}
	for _, tt := range tests {
		if got := LaneRank(tt.tier, tt.class); got != tt.want {
			t.Errorf("LaneRank(%q, %q) = %d, want %d", tt.tier, tt.class, got, tt.want)
		}
	}
}

func TestLaneLabel(t *testing.T) {
	tests := []struct {
		rank int
		want string
	}{
		{0, "reserved/interactive"},
		{1, "reserved/async"},
		{2, "reserved/batch"},
		{3, "overflow/interactive"},
		{4, "overflow/async"},
		{5, "overflow/batch"},
		// Out-of-range ranks name the lowest lane.
		{-1, "overflow/batch"},
		{6, "overflow/batch"},
	}
	for _, tt := range tests {
		if got := LaneLabel(tt.rank); got != tt.want {
			t.Errorf("LaneLabel(%d) = %q, want %q", tt.rank, got, tt.want)
		}
	}
}
