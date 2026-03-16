package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	saturationRequestQueue = "saturation-request-sortedset"
	saturationResultQueue  = "saturation-result-list"
)

var _ = ginkgo.Describe("Saturation Metric Dispatch Gate E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, saturationRequestQueue) //nolint:errcheck
		rdb.Del(ctx, saturationResultQueue)  //nolint:errcheck
		resetMock(adminURL)
		// Start with zero saturation (full capacity)
		setPromMockSaturation(promMockURL, "0")
	})

	ginkgo.It("processes a message when saturation is below threshold", func() {
		setPromMockSaturation(promMockURL, "0.3")

		msg := makeRequestMessage("sat-below-threshold", 5*time.Minute)
		enqueueMessage(ctx, rdb, saturationRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, saturationResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.Id).To(gomega.Equal("sat-below-threshold"))
	})

	ginkgo.It("pauses processing when saturation is at or above threshold", func() {
		// Set saturation above threshold (0.8)
		setPromMockSaturation(promMockURL, "0.9")

		msg := makeRequestMessage("sat-above-threshold", 5*time.Minute)
		enqueueMessage(ctx, rdb, saturationRequestQueue, msg)

		// Message should NOT be processed while saturated
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Lower saturation below threshold
		setPromMockSaturation(promMockURL, "0.2")

		// Message should now be processed
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, saturationResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.Id).To(gomega.Equal("sat-above-threshold"))
	})

	ginkgo.It("resumes processing when saturation drops below threshold", func() {
		// Start saturated
		setPromMockSaturation(promMockURL, "1.0")

		// Enqueue multiple messages
		for i := 1; i <= 3; i++ {
			msg := makeRequestMessage(fmt.Sprintf("sat-resume-%d", i), 5*time.Minute)
			enqueueMessage(ctx, rdb, saturationRequestQueue, msg)
		}

		// Nothing should be processed
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Drop saturation to allow processing
		setPromMockSaturation(promMockURL, "0.1")

		// All 3 messages should eventually be processed
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))
	})
})
