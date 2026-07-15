package e2e

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/llm-d/llm-d-async/api"
)

// Validates the multi-tenant guide's reserved-vs-overflow priority model
// end-to-end: a shared pool + classifying redis-quota gate +
// the tier-priority merge policy. With a single worker, dispatch order equals
// the merge policy's lane order, so we can assert that `reserved` traffic drains
// before `overflow` even when the overflow was enqueued first.
const (
	mtMergeHiQueue = "mt-merge-hi-sortedset" // tier=interactive
	mtMergeLoQueue = "mt-merge-lo-sortedset" // tier=batch
	mtMergeResults = "mt-merge-result-list"
	mtMergePrefix  = "quota:"
)

// makeTeamMessages builds n requests for a team: ID "<team>-<i>", the `team`
// metadata the quota gate keys on, and a non-trivial max_tokens so each request
// takes long enough that both lanes are buffered before the single worker
// pulls ahead.
func makeTeamMessages(team string, n int) []api.RequestMessage {
	msgs := make([]api.RequestMessage, 0, n)
	for i := 0; i < n; i++ {
		m := makeRequestMessage(fmt.Sprintf("%s-%d", team, i), 5*time.Minute)
		m.Metadata = map[string]string{"team": team}
		m.Payload = map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 64}
		msgs = append(msgs, m)
	}
	return msgs
}

var _ = ginkgo.Describe("Multi-tenant Merge-Policy Priority E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		for _, k := range []string{
			mtMergeHiQueue, mtMergeLoQueue, mtMergeResults,
			mtMergePrefix + "team:hi", mtMergePrefix + "team:lo",
		} {
			rdb.Del(ctx, k) //nolint:errcheck
		}
		setSimWaitingRequests(simAdminURL, 0)
	})

	ginkgo.It("dispatches reserved before overflow via the tier-priority merge policy", func() {
		const nLo, nHi = 20, 5

		// Pre-seed the lo team's concurrency counter to its limit (1). The
		// classifying gate then returns overflow for every lo request WITHOUT
		// incrementing, so the counter is stable and lo is deterministically
		// overflow. hi is unseeded -> reserved.
		rdb.Set(ctx, mtMergePrefix+"team:lo", "1", 5*time.Minute) //nolint:errcheck

		// Enqueue overflow (lo) FIRST, then reserved (hi) — atomically per queue.
		enqueueMessages(ctx, rdb, mtMergeLoQueue, makeTeamMessages("lo", nLo)...)
		enqueueMessages(ctx, rdb, mtMergeHiQueue, makeTeamMessages("hi", nHi)...)

		// Wait for all requests to be dispatched and their results collected.
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, mtMergeResults)
		}, 90*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", int64(nLo+nHi)))

		// Pop results in completion order (RPOP = oldest first).
		order := make([]string, 0, nLo+nHi)
		for i := 0; i < nLo+nHi; i++ {
			r := popResult(ctx, rdb, mtMergeResults)
			gomega.Expect(r).NotTo(gomega.BeNil())
			order = append(order, r.ID)
		}

		lastReserved := -1
		reservedCount := 0
		for i, id := range order {
			if strings.HasPrefix(id, "hi-") { // reserved + interactive (lane 0)
				lastReserved = i
				reservedCount++
			}
		}

		// All reserved requests were delivered...
		gomega.Expect(reservedCount).To(gomega.Equal(nHi))
		// ...and drained near the front, ahead of the 20 overflow requests that were
		// enqueued first (small tolerance for the one/two overflow that may reach the
		// scheduler before hi is buffered).
		gomega.Expect(lastReserved).To(gomega.BeNumerically("<=", nHi+2),
			"reserved (hi) must drain before overflow (lo); order=%v", order)
		// The tail is overflow — reserved never ends up last.
		gomega.Expect(order[len(order)-1]).To(gomega.HavePrefix("lo-"))
	})
})
