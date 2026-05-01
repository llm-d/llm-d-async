package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Attribute Gating E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		cleanupQueues(ctx, rdb)
		resetMock(adminURL)
		setMockDelay(adminURL, 0)

		// Ensure redis quota keys are clean for the user we use in tests
		rdb.Del(ctx, "quota:userid:user-a").Err() // nolint:errcheck
	})

	ginkgo.It("enforces per-user concurrency limit", func() {
		quotaRequestQueue := "quota-request-sortedset"

		// 1. Setup: The quota-gated deployment is already running from BeforeSuite.
		// We set a 5-second delay on the mock so that the first request stays "in-flight"
		// long enough for us to verify the concurrency gate blocks the second request.
		setMockDelay(adminURL, 5000)

		// 2. Enqueue Request 1 for User-A
		msg1 := makeRequestMessage("quota-1", 5*time.Minute)
		msg1.Metadata = map[string]string{"userid": "user-a"}
		enqueueMessage(ctx, rdb, quotaRequestQueue, msg1)

		// Wait for Request 1 to be processed by the mock (making it "in-flight")
		gomega.Eventually(func() []string {
			return getRequestLog(adminURL)
		}, 30*time.Second, 1*time.Second).Should(gomega.ContainElement("quota-1"))

		// 3. Enqueue Request 2 for User-A
		// With concurrency limit = 1, this should NOT be processed while Request 1 is active.
		msg2 := makeRequestMessage("quota-2", 5*time.Minute)
		msg2.Metadata = map[string]string{"userid": "user-a"}
		enqueueMessage(ctx, rdb, quotaRequestQueue, msg2)

		// Verify Request 2 is NOT in the mock log after some time
		gomega.Consistently(func() []string {
			return getRequestLog(adminURL)
		}, 3*time.Second, 1*time.Second).ShouldNot(gomega.ContainElement("quota-2"))

		// 4. Complete Request 1
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 30*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))
		result1 := popResult(ctx, rdb, resultQueue)
		gomega.Expect(result1.Id).To(gomega.Equal("quota-1"))

		// 5. Verify Request 2 is now processed
		gomega.Eventually(func() []string {
			return getRequestLog(adminURL)
		}, 30*time.Second, 1*time.Second).Should(gomega.ContainElement("quota-2"))

		// 6. Complete Request 2
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 30*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))
		result2 := popResult(ctx, rdb, resultQueue)
		gomega.Expect(result2.Id).To(gomega.Equal("quota-2"))
	})

	// I will implement a "pseudo" E2E that verifies the logic if I can.
	// Actually, I'll provide a real test that assumes the 'redis-quota' gate is active.
})
