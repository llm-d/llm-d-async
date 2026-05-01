package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	budgetRequestQueue = "budget-request-sortedset"
	budgetResultQueue  = "budget-result-list"
)

var _ = ginkgo.Describe("Budget Metric Dispatch Gate E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, budgetRequestQueue) //nolint:errcheck
		rdb.Del(ctx, budgetResultQueue)  //nolint:errcheck
		resetMock(adminURL)
		// Start with full dispatch budget (no load)
		setPromMockBudget(promMockURL, "1.0")
	})

	ginkgo.It("processes a message when dispatch budget is positive", func() {
		// D = 0.35: 35% capacity available
		setPromMockBudget(promMockURL, "0.35")

		msg := makeRequestMessage("budget-positive", 5*time.Minute)
		enqueueMessage(ctx, rdb, budgetRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, budgetResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("budget-positive"))
	})

	ginkgo.It("pauses processing when dispatch budget is zero", func() {
		// D = 0: system fully loaded
		setPromMockBudget(promMockURL, "0.0")

		msg := makeRequestMessage("budget-zero", 5*time.Minute)
		enqueueMessage(ctx, rdb, budgetRequestQueue, msg)

		// Message should NOT be processed while budget is zero
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Restore budget to allow processing
		setPromMockBudget(promMockURL, "0.5")

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, budgetResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("budget-zero"))
	})

	ginkgo.It("resumes processing when dispatch budget is restored", func() {
		// Start with zero budget
		setPromMockBudget(promMockURL, "0.0")

		for i := 1; i <= 3; i++ {
			msg := makeRequestMessage(fmt.Sprintf("budget-resume-%d", i), 5*time.Minute)
			enqueueMessage(ctx, rdb, budgetRequestQueue, msg)
		}

		// Nothing should be processed
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Restore dispatch budget
		setPromMockBudget(promMockURL, "0.9")

		// All 3 messages should eventually be processed
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))
	})
})
