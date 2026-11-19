package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	compositeRequestQueue = "composite-request-sortedset"
	compositeResultQueue  = "composite-result-list"
	compositeQuotaPrefix  = "test-composite-quota:"
)

var _ = ginkgo.Describe("Composite Gate E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeAll(func() {
		redeployEPPWithFlowControl()
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, compositeRequestQueue)                //nolint:errcheck
		rdb.Del(ctx, compositeResultQueue)                 //nolint:errcheck
		rdb.Del(ctx, compositeQuotaPrefix+"userid:user-b") //nolint:errcheck
		setSimWaitingRequests(simAdminURL, 0)
	})

	ginkgo.It("processes a message when saturation is low and quota is available", func() {
		msg := makeRequestMessage("composite-open", 5*time.Minute)
		msg.Metadata = map[string]string{"userid": "user-b"}
		enqueueMessage(ctx, rdb, compositeRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, compositeResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, compositeResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("composite-open"))
	})

	ginkgo.It("blocks when quota is exhausted even if saturation is low", func() {
		// Saturation is low (waiting-requests=0), but quota is full.
		rdb.Set(ctx, compositeQuotaPrefix+"userid:user-b", "1", 5*time.Minute) //nolint:errcheck

		msg := makeRequestMessage("composite-quota-blocked", 5*time.Minute)
		msg.Metadata = map[string]string{"userid": "user-b"}
		enqueueMessage(ctx, rdb, compositeRequestQueue, msg)

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, compositeResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Release quota → message should proceed.
		rdb.Del(ctx, compositeQuotaPrefix+"userid:user-b") //nolint:errcheck

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, compositeResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, compositeResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("composite-quota-blocked"))
	})

	ginkgo.It("blocks when saturation is high even if quota is available", func() {
		// Drive saturation above the composite gate's threshold (0.8).
		setSimWaitingRequests(simAdminURL, 10)
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v >= 0.8 })

		msg := makeRequestMessage("composite-sat-blocked", 5*time.Minute)
		msg.Metadata = map[string]string{"userid": "user-b"}
		enqueueMessage(ctx, rdb, compositeRequestQueue, msg)

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, compositeResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Lower saturation → gate reopens → message processed.
		setSimWaitingRequests(simAdminURL, 0)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, compositeResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, compositeResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("composite-sat-blocked"))
	})
})
