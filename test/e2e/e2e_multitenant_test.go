package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// Multi-tenant scenario from the multi-tenant guide on the Redis SortedSet
// backend: three teams (premium/standard/batch) with per-team quota gates and a
// per-team saturation (priority) gate. These specs assert the guide's two
// headline behaviors:
//
//   - Scenario B (quota): a team's own concurrency quota throttles it without
//     affecting other teams.
//   - Scenario C (priority under saturation): under inference saturation the
//     batch pool parks (ActionWait) while premium keeps dispatching.
const (
	mtPremiumQueue   = "mt-premium-request-sortedset"
	mtBatchQueue     = "mt-batch-request-sortedset"
	mtPremiumResults = "mt-premium-result-list"
	mtBatchResults   = "mt-batch-result-list"
	mtQuotaPrefix    = "quota:"
)

var _ = ginkgo.Describe("Multi-tenant Quota and Priority E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeAll(func() {
		// The batch pool gate reads inference_extension_flow_control_pool_saturation,
		// which only exists once EPP runs with flow control enabled.
		redeployEPPWithFlowControl()
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		for _, k := range []string{
			mtPremiumQueue, mtBatchQueue, mtPremiumResults, mtBatchResults,
			mtQuotaPrefix + "team:batch", mtQuotaPrefix + "team:premium",
		} {
			rdb.Del(ctx, k) //nolint:errcheck
		}
		setSimWaitingRequests(simAdminURL, 0)
	})

	ginkgo.It("throttles a team by its own quota without affecting other teams", func() {
		// Keep saturation low so the batch pool gate is open — only the per-team
		// quota gate is in play here.
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v < 0.5 })

		// Close batch's concurrency quota (limit 1) by pre-setting the counter,
		// exactly as the guide's Scenario B drives it past the limit.
		rdb.Set(ctx, mtQuotaPrefix+"team:batch", "1", 5*time.Minute) //nolint:errcheck

		premium := makeRequestMessage("mt-premium-quota", 5*time.Minute)
		premium.Metadata = map[string]string{"team": "premium"}
		enqueueMessage(ctx, rdb, mtPremiumQueue, premium)

		batch := makeRequestMessage("mt-batch-quota", 5*time.Minute)
		batch.Metadata = map[string]string{"team": "batch"}
		enqueueMessage(ctx, rdb, mtBatchQueue, batch)

		// premium (ungated) is delivered promptly.
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, mtPremiumResults)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		// batch stays blocked by its own quota (not shed — re-enqueued).
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, mtBatchResults)
		}, 8*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Releasing batch's quota lets it drain.
		rdb.Del(ctx, mtQuotaPrefix+"team:batch") //nolint:errcheck
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, mtBatchResults)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))
	})

	ginkgo.It("parks the batch pool under saturation and resumes when it clears", func() {
		// Batch is the least saturation-tolerant tier: its wait-on-refuse pool gate
		// parks the workers (ActionWait) under load, while premium (no pool gate) is
		// unthrottled at the llm-d-async. Note: the e2e drives saturation via the
		// EPP admission metric, which also throttles real gateway traffic, so this
		// spec asserts the batch pool-gate mechanism directly (premium-vs-batch
		// prioritization is covered at the quota level above, with saturation low).
		setSimWaitingRequests(simAdminURL, 10)
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v >= 0.7 })

		batch := makeRequestMessage("mt-batch-sat", 5*time.Minute)
		batch.Metadata = map[string]string{"team": "batch"}
		enqueueMessage(ctx, rdb, mtBatchQueue, batch)

		// batch parks in-memory — no result while saturated (not shed).
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, mtBatchResults)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Relieving saturation reopens the batch pool gate; batch drains.
		setSimWaitingRequests(simAdminURL, 0)
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, mtBatchResults)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))
	})
})
