package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// These tests drive the endpoint-scrape gate by scraping the sim's /metrics
// endpoint directly (no Prometheus or EPP needed):
//
//	setSimWaitingRequests → sim reports vllm:num_requests_waiting on /metrics
//	  → async-processor scrapes /metrics, computes saturation, gate opens/closes
var _ = ginkgo.Describe("Endpoint Scrape Dispatch Gate E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, endpointScrapeRequestQueue) //nolint:errcheck
		rdb.Del(ctx, endpointScrapeResultQueue)  //nolint:errcheck
		setSimWaitingRequests(simAdminURL, 0)
	})

	ginkgo.It("processes a message when scrape metric is below max count", func() {
		setSimWaitingRequests(simAdminURL, 0)

		msg := makeRequestMessage("scrape-below-max", 5*time.Minute)
		enqueueMessage(ctx, rdb, endpointScrapeRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, endpointScrapeResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, endpointScrapeResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("scrape-below-max"))
	})

	ginkgo.It("pauses processing when scrape metric reaches max count", func() {
		// waiting=5, max_count_per_pod=5 → saturation=1.0 → gate closed
		setSimWaitingRequests(simAdminURL, 5)

		msg := makeRequestMessage("scrape-at-max", 5*time.Minute)
		enqueueMessage(ctx, rdb, endpointScrapeRequestQueue, msg)

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, endpointScrapeResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Drop waiting → saturation falls → gate reopens → message processed.
		setSimWaitingRequests(simAdminURL, 0)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, endpointScrapeResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, endpointScrapeResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("scrape-at-max"))
	})

	ginkgo.It("resumes processing for multiple messages when metric drops", func() {
		setSimWaitingRequests(simAdminURL, 5)

		for i := 1; i <= 3; i++ {
			msg := makeRequestMessage(fmt.Sprintf("scrape-resume-%d", i), 5*time.Minute)
			enqueueMessage(ctx, rdb, endpointScrapeRequestQueue, msg)
		}

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, endpointScrapeResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		setSimWaitingRequests(simAdminURL, 0)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, endpointScrapeResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))
	})
})
