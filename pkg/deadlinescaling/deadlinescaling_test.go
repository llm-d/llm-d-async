package deadlinescaling

import (
	"testing"
	"time"
)

// buckets mirrors the deadline-proximity boundaries: expired, then 1s ... 24h.
var buckets = []time.Duration{
	0, time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute,
	5 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 6 * time.Hour, 24 * time.Hour,
}

// counts builds cumulative counts from the number of items first due within
// each bucket.
func counts(perBucket map[time.Duration]int64) []int64 {
	out := make([]int64, len(buckets))
	var total int64
	for i, b := range buckets {
		total += perBucket[b]
		out[i] = total
	}
	return out
}

// required is RequiredReplicas for a single queue.
func required(c Config, due map[time.Duration]int64) int {
	work := make([]float64, len(buckets))
	AddWork(work, buckets, counts(due), c.ThroughputRPS)
	return c.RequiredReplicas(buckets, work)
}

func TestRequiredReplicas(t *testing.T) {
	base := Config{ThroughputRPS: 2, ColdStartSeconds: 60, MaxReplicas: 10}
	for _, tc := range []struct {
		name string
		cfg  Config
		due  map[time.Duration]int64
		want int
	}{
		{"empty queue", base, nil, 0},
		{"due before a replica can start", base, map[time.Duration]int64{30 * time.Second: 1}, 10},
		{"due exactly at the cold start", base, map[time.Duration]int64{time.Minute: 1}, 10},
		// Expired items count in every later bucket; they are left out of each.
		// 5 / (2 * 3540s) -> 1.
		{"expired items don't count", base, map[time.Duration]int64{0: 1000, time.Hour: 5}, 1},
		{"only expired items", base, map[time.Duration]int64{0: 3}, 0},
		// 240 items due within 2m: 2 rps * (120s - 60s) = 120 per replica -> 2.
		{"one horizon", base, map[time.Duration]int64{2 * time.Minute: 240}, 2},
		// One item more than two replicas finish needs a third.
		{"rounds up", base, map[time.Duration]int64{2 * time.Minute: 241}, 3},
		// 240 within 2m needs 2; 600 more within 5m: 840 / (2 * 240s) = 1.75 -> the 2m horizon dominates.
		{"the tightest horizon wins", base, map[time.Duration]int64{2 * time.Minute: 240, 5 * time.Minute: 600}, 2},
		{"the far horizon wins", base, map[time.Duration]int64{2 * time.Minute: 10, 5 * time.Minute: 2000}, 5},
		{"capped", base, map[time.Duration]int64{2 * time.Minute: 100000}, 10},
		// Half the time budget doubles the need: 240 / (2 * 60s * 0.5) = 4.
		{"safety factor", Config{ThroughputRPS: 2, ColdStartSeconds: 60, MaxReplicas: 10, SafetyFactor: 0.5},
			map[time.Duration]int64{2 * time.Minute: 240}, 4},
		{"no cold start", Config{ThroughputRPS: 1, MaxReplicas: 10}, map[time.Duration]int64{time.Second: 3}, 3},
		// 1 / (20000 rps * 86400s) is below the rounding tolerance, but one item still needs a replica.
		{"one item far out", Config{ThroughputRPS: 20000, MaxReplicas: 10}, map[time.Duration]int64{24 * time.Hour: 1}, 1},
		// 1 / (1 rps * 1s * 1e-20) is past the int range; the cap applies before the conversion.
		{"demand past the int range", Config{ThroughputRPS: 1, MaxReplicas: 10, SafetyFactor: 1e-20},
			map[time.Duration]int64{time.Second: 1}, 10},
		// 1e-300 rps * 1s * 1e-300 underflows to 0, so the demand is +Inf.
		{"infinite demand", Config{ThroughputRPS: 1e-300, MaxReplicas: 10, SafetyFactor: 1e-300},
			map[time.Duration]int64{time.Second: 1}, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := required(tc.cfg, tc.due); got != tc.want {
				t.Fatalf("RequiredReplicas = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRequiredReplicasIgnoresMissingCounts(t *testing.T) {
	cfg := Config{ThroughputRPS: 1, MaxReplicas: 3}
	work := make([]float64, len(buckets))
	AddWork(work, buckets, nil, cfg.ThroughputRPS)
	if got := cfg.RequiredReplicas(buckets, work); got != 0 {
		t.Fatalf("RequiredReplicas without counts = %d, want 0", got)
	}
}

// The pool's work is summed across its queues before the division. R = 1 and
// C = 60s, as in the review of #526.
func TestPoolRequiredReplicas(t *testing.T) {
	pool := Config{ThroughputRPS: 1, ColdStartSeconds: 60, MaxReplicas: 10}
	type queue struct {
		rps float64
		due map[time.Duration]int64
	}
	for _, tc := range []struct {
		name   string
		queues []queue
		want   int
	}{
		// Alone, 100 due within 2m need 2 and 600 within 30m need 1. Together:
		// max(100 / 60s, 700 / 1740s) -> 2, not 3.
		{"queues peak at different horizons", []queue{
			{1, map[time.Duration]int64{2 * time.Minute: 100}},
			{1, map[time.Duration]int64{30 * time.Minute: 600}},
		}, 2},
		// Each needs 2 alone; together 200 / 60s -> 4, not 2.
		{"queues peak at the same horizon", []queue{
			{1, map[time.Duration]int64{2 * time.Minute: 100}},
			{1, map[time.Duration]int64{2 * time.Minute: 100}},
		}, 4},
		// A queue whose items take half as long adds half the work: (50 + 100) / 60s -> 3.
		{"per-queue throughput", []queue{
			{2, map[time.Duration]int64{2 * time.Minute: 100}},
			{1, map[time.Duration]int64{2 * time.Minute: 100}},
		}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work := make([]float64, len(buckets))
			for _, q := range tc.queues {
				AddWork(work, buckets, counts(q.due), q.rps)
			}
			if got := pool.RequiredReplicas(buckets, work); got != tc.want {
				t.Fatalf("RequiredReplicas = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	ok := Config{ThroughputRPS: 1.5, ColdStartSeconds: 30, MaxReplicas: 4, SafetyFactor: 0.8}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, cfg := range map[string]Config{
		"zero throughput":     {MaxReplicas: 1},
		"negative cold start": {ThroughputRPS: 1, ColdStartSeconds: -1, MaxReplicas: 1},
		"no replicas":         {ThroughputRPS: 1},
		"safety above 1":      {ThroughputRPS: 1, MaxReplicas: 1, SafetyFactor: 1.5},
		"negative safety":     {ThroughputRPS: 1, MaxReplicas: 1, SafetyFactor: -0.1},
	} {
		if cfg.Validate() == nil {
			t.Errorf("%s: accepted %+v", name, cfg)
		}
	}
}
