package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	queryRequestQueue = "query-request-sortedset"
	queryResultQueue  = "query-result-list"
)

// The prometheus-query gate evaluates an arbitrary PromQL expression as the
// dispatch budget. The e2e Helm values configure it with the same saturation
// metric used by the saturation gate tests:
//
//	1 - inference_extension_flow_control_pool_saturation{inference_pool="e2e-pool"}
//
// Because threshold=0 the raw PromQL result is used directly as the budget.
var _ = ginkgo.Describe("Prometheus Query Dispatch Gate E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeAll(func() {
		redeployEPPWithFlowControl()
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, queryRequestQueue) //nolint:errcheck
		rdb.Del(ctx, queryResultQueue)  //nolint:errcheck
		setSimWaitingRequests(simAdminURL, 0)
	})

	ginkgo.It("processes a message when the query returns a positive budget", func() {
		setSimWaitingRequests(simAdminURL, 0)

		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v < 0.5 })

		msg := makeRequestMessage("query-positive", 5*time.Minute)
		enqueueMessage(ctx, rdb, queryRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, queryResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, queryResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("query-positive"))
	})

	ginkgo.It("pauses processing when the query returns zero budget", func() {
		setSimWaitingRequests(simAdminURL, 10)

		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v >= 0.7 })

		msg := makeRequestMessage("query-blocked", 5*time.Minute)
		enqueueMessage(ctx, rdb, queryRequestQueue, msg)

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, queryResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		setSimWaitingRequests(simAdminURL, 0)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, queryResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, queryResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("query-blocked"))
	})

	ginkgo.It("resumes processing when the query budget recovers", func() {
		setSimWaitingRequests(simAdminURL, 10)
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v >= 0.7 })

		for i := 1; i <= 3; i++ {
			msg := makeRequestMessage(fmt.Sprintf("query-resume-%d", i), 5*time.Minute)
			enqueueMessage(ctx, rdb, queryRequestQueue, msg)
		}

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, queryResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		setSimWaitingRequests(simAdminURL, 0)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, queryResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))
	})
})
