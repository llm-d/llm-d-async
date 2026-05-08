package e2e

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	budgetRequestQueue = "budget-request-sortedset"
	budgetResultQueue  = "budget-result-list"
)

// Budget Cascade tests run while EPP is deployed WITHOUT flow control.
// The primary metric (inference_extension_flow_control_queue_size) is never
// recorded, so the CascadeMetricSource falls back to vLLM metrics.
var _ = ginkgo.Describe("Budget Cascade E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, budgetRequestQueue) //nolint:errcheck
		rdb.Del(ctx, budgetResultQueue)  //nolint:errcheck
		setSimWaitingRequests(simAdminURL, 0)
	})

	ginkgo.It("falls back to vLLM metrics when primary metric is unavailable", func() {
		// EPP was deployed without flow control by BeforeSuite, so
		// inference_extension_flow_control_queue_size is never recorded.
		// The cascade falls back to vLLM metrics:
		//   D = 1 - (running_requests / (ready_pods * max_concurrency))
		// With sim at 0 waiting/running requests, budget is positive.
		setSimWaitingRequests(simAdminURL, 0)

		msg := makeRequestMessage("budget-cascade", 5*time.Minute)
		enqueueMessage(ctx, rdb, budgetRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, budgetResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("budget-cascade"))
	})
})

// Budget Metric Dispatch Gate tests run after EPP has been redeployed WITH
// flow control. The budget is derived from:
//
//	D = 1 - (queue_size / (ready_pods * max_concurrency))
//
// where queue_size is inference_extension_flow_control_queue_size (EPP's
// internal flow control admission queue depth).
//
// To drive queue_size > 0, the tests flood EPP with concurrent probe
// requests while saturation is high. EPP throttles them in its admission
// queue, making queue_size visible in Prometheus. With max_concurrency=1,
// a single queued request closes the gate.
var _ = ginkgo.Describe("Budget Metric Dispatch Gate E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeAll(func() {
		redeployEPPWithFlowControl()
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, budgetRequestQueue) //nolint:errcheck
		rdb.Del(ctx, budgetResultQueue)  //nolint:errcheck
		// Reset saturation and wait for EPP's flow control queue to drain.
		// Previous tests may have left probes queued.
		setSimWaitingRequests(simAdminURL, 0)
		gomega.Eventually(func() bool {
			v := queryProm(promURL, budgetPromQL)
			return !math.IsNaN(v) && v > 0.5
		}, 60*time.Second, 2*time.Second).Should(gomega.BeTrue(), "waiting for budget to recover between tests")
	})

	ginkgo.It("processes a message when dispatch budget is positive", func() {

		msg := makeRequestMessage("budget-positive", 5*time.Minute)
		enqueueMessage(ctx, rdb, budgetRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, budgetResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("budget-positive"))
	})

	ginkgo.It("blocks messages when dispatch budget is zero", func() {
		// Drive saturation high.
		setSimWaitingRequests(simAdminURL, 100)

		// Start flooding immediately — the flood goroutines send probes
		// that trigger EPP's admission layer and fill the flow control
		// queue once EPP detects saturation and starts throttling.
		stopFlood := floodProbes(envoyURL, 20)
		defer stopFlood()

		// Wait for the budget to drop. The propagation chain is:
		// sim fake_metrics → EPP scrapes sim → EPP detects saturation →
		// EPP throttles flood probes → queue_size rises → Prometheus scrapes.
		gomega.Eventually(func() bool {
			v := queryProm(promURL, budgetPromQL)
			return !math.IsNaN(v) && v < 0.05
		}, 120*time.Second, 2*time.Second).Should(gomega.BeTrue(), "waiting for budget to reach zero")

		msg := makeRequestMessage("budget-zero", 5*time.Minute)
		enqueueMessage(ctx, rdb, budgetRequestQueue, msg)

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Stop flooding and restore budget.
		stopFlood()
		setSimWaitingRequests(simAdminURL, 0)
		waitForBudget(promURL, envoyURL, func(b float64) bool { return b > 0.5 })

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, budgetResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("budget-zero"))
	})

	ginkgo.It("resumes processing when dispatch budget is restored", func() {
		setSimWaitingRequests(simAdminURL, 100)

		stopFlood := floodProbes(envoyURL, 20)
		defer stopFlood()

		gomega.Eventually(func() bool {
			v := queryProm(promURL, budgetPromQL)
			return !math.IsNaN(v) && v < 0.05
		}, 120*time.Second, 2*time.Second).Should(gomega.BeTrue(), "waiting for budget to reach zero")

		for i := 1; i <= 3; i++ {
			msg := makeRequestMessage(fmt.Sprintf("budget-resume-%d", i), 5*time.Minute)
			enqueueMessage(ctx, rdb, budgetRequestQueue, msg)
		}

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		stopFlood()
		setSimWaitingRequests(simAdminURL, 0)
		waitForBudget(promURL, envoyURL, func(b float64) bool { return b > 0.5 })

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, budgetResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))
	})
})
