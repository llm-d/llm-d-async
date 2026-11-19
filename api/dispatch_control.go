package api

import (
	"fmt"
	"math"
	"time"
)

const (
	// DispatchRateLimitAPIVersion is the wire-contract version stored by an
	// external controller and consumed by llm-d Async.
	DispatchRateLimitAPIVersion = "llm-d.ai/v1alpha1"
)

// DispatchRateLimit is a leased, pool-scoped ceiling on new inference
// dispatches. It controls admission attempts, not completion throughput: the
// downstream gateway, inference pool, and other dispatch gates may deliver a
// lower rate.
//
// ValidUntilUnixMillis is deliberately part of the payload even when the
// backing store also applies a TTL. Consumers must fail closed when either the
// payload lease or the backing-store lease has expired.
type DispatchRateLimit struct {
	APIVersion           string  `json:"api_version"`
	PoolID               string  `json:"pool_id"`
	MaxAdmissionRPS      float64 `json:"max_admission_rps"`
	ValidUntilUnixMillis int64   `json:"valid_until_unix_ms"`
	DecisionID           string  `json:"decision_id"`
}

// ValidateAt validates a dispatch-rate command against the supplied time.
// MaxAdmissionRPS == 0 is valid and means an explicit pause.
func (l DispatchRateLimit) ValidateAt(now time.Time) error {
	if l.APIVersion != DispatchRateLimitAPIVersion {
		return fmt.Errorf("unsupported api_version %q", l.APIVersion)
	}
	if l.PoolID == "" {
		return fmt.Errorf("pool_id is required")
	}
	if math.IsNaN(l.MaxAdmissionRPS) || math.IsInf(l.MaxAdmissionRPS, 0) || l.MaxAdmissionRPS < 0 {
		return fmt.Errorf("max_admission_rps must be finite and non-negative")
	}
	if l.ValidUntilUnixMillis <= now.UnixMilli() {
		return fmt.Errorf("dispatch-rate lease has expired")
	}
	if l.DecisionID == "" {
		return fmt.Errorf("decision_id is required")
	}
	return nil
}
