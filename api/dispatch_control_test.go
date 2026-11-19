package api

import (
	"math"
	"testing"
	"time"
)

func TestDispatchRateLimitValidateAt(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	valid := DispatchRateLimit{
		APIVersion:           DispatchRateLimitAPIVersion,
		PoolID:               "pool-a",
		MaxAdmissionRPS:      0,
		ValidUntilUnixMillis: now.Add(time.Minute).UnixMilli(),
		DecisionID:           "decision-1",
	}

	if err := valid.ValidateAt(now); err != nil {
		t.Fatalf("valid pause command rejected: %v", err)
	}

	tests := map[string]func(*DispatchRateLimit){
		"wrong version": func(v *DispatchRateLimit) { v.APIVersion = "v2" },
		"missing pool":  func(v *DispatchRateLimit) { v.PoolID = "" },
		"negative rate": func(v *DispatchRateLimit) { v.MaxAdmissionRPS = -1 },
		"NaN rate":      func(v *DispatchRateLimit) { v.MaxAdmissionRPS = math.NaN() },
		"infinite rate": func(v *DispatchRateLimit) { v.MaxAdmissionRPS = math.Inf(1) },
		"expired":       func(v *DispatchRateLimit) { v.ValidUntilUnixMillis = now.UnixMilli() },
		"missing id":    func(v *DispatchRateLimit) { v.DecisionID = "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := candidate.ValidateAt(now); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
