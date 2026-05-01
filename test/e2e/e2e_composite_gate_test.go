package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	compositeRequestQueue = "composite-request-sortedset"
	compositeQuotaPrefix  = "test-composite-quota:userid:"
)

var _ = ginkgo.Describe("Composite Gate E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, compositeRequestQueue) //nolint:errcheck
		rdb.Del(ctx, "result-list")         //nolint:errcheck
		resetMock(adminURL)
		setMockDelay(adminURL, 0)

		// Ensure redis quota keys are clean for the user we use in tests
		rdb.Del(ctx, compositeQuotaPrefix+"user-b").Err() // nolint:errcheck

		// Start with zero saturation (full capacity)
		setPromMockSaturation(promMockURL, "0")
	})

	ginkgo.It("processes a message when saturation is low and quota is available", func() {
		setPromMockSaturation(promMockURL, "0.3")

		msg := makeRequestMessage("composite-1", 5*time.Minute)
		msg.Metadata = map[string]string{"userid": "user-b"}
		enqueueMessage(ctx, rdb, compositeRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, "result-list")
		}, 30*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, "result-list")
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.Id).To(gomega.Equal("composite-1"))
	})

	ginkgo.It("enforces quota limit even when saturation is low", func() {
		setPromMockSaturation(promMockURL, "0.1")

		// 1. We set a 5-second delay on the mock so that the first request stays "in-flight"
		// long enough for us to verify the concurrency gate blocks the second request.
		setMockDelay(adminURL, 5000)

		// 2. Enqueue Request 1 for User-B
		msg1 := makeRequestMessage("composite-quota-1", 5*time.Minute)
		msg1.Metadata = map[string]string{"userid": "user-b"}
		enqueueMessage(ctx, rdb, compositeRequestQueue, msg1)

		// Wait for Request 1 to be processed by the mock (making it "in-flight")
		gomega.Eventually(func() []string {
			return getRequestLog(adminURL)
		}, 30*time.Second, 1*time.Second).Should(gomega.ContainElement("composite-quota-1"))

		// 3. Enqueue Request 2 for User-B
		// With concurrency limit = 1, this should NOT be processed while Request 1 is active.
		msg2 := makeRequestMessage("composite-quota-2", 5*time.Minute)
		msg2.Metadata = map[string]string{"userid": "user-b"}
		enqueueMessage(ctx, rdb, compositeRequestQueue, msg2)

		// Verify Request 2 is NOT in the mock log after some time
		gomega.Consistently(func() []string {
			return getRequestLog(adminURL)
		}, 3*time.Second, 1*time.Second).ShouldNot(gomega.ContainElement("composite-quota-2"))

		// 4. Wait for Request 1 to complete and pop its result
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, "result-list")
		}, 30*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))
		result1 := popResult(ctx, rdb, "result-list")
		gomega.Expect(result1.Id).To(gomega.Equal("composite-quota-1"))

		// 5. Verify Request 2 is now processed
		gomega.Eventually(func() []string {
			return getRequestLog(adminURL)
		}, 30*time.Second, 1*time.Second).Should(gomega.ContainElement("composite-quota-2"))

		// 6. Wait for Request 2 to complete and pop its result
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, "result-list")
		}, 30*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))
		result2 := popResult(ctx, rdb, "result-list")
		gomega.Expect(result2.Id).To(gomega.Equal("composite-quota-2"))
	})

	ginkgo.It("blocks request when saturation is high even if quota is available", func() {
		// Set saturation above threshold (0.8)
		setPromMockSaturation(promMockURL, "0.9")

		msg := makeRequestMessage("composite-sat-above", 5*time.Minute)
		msg.Metadata = map[string]string{"userid": "user-b"}
		enqueueMessage(ctx, rdb, compositeRequestQueue, msg)

		// Message should NOT be processed while saturated
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, "result-list")
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Lower saturation below threshold
		setPromMockSaturation(promMockURL, "0.2")

		// Message should now be processed
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, "result-list")
		}, 30*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, "result-list")
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.Id).To(gomega.Equal("composite-sat-above"))
	})
})
