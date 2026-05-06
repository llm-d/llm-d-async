package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// These tests drive the saturation gate through the full observability pipeline:
//
//	setSimWaitingRequests → sim reports vllm:num_requests_waiting
//	  → EPP scrapes sim, computes inference_extension_flow_control_pool_saturation
//	    (saturation = Max(WaitingQueue/QueueDepthThreshold, KVCache/KVCacheThreshold))
//	  → Prometheus scrapes EPP
//	  → async-processor queries Prometheus, gate opens/closes
//
// Probe requests through Envoy → EPP are needed to trigger the flow control
// admission layer that records the saturation metric.
var _ = ginkgo.Describe("Saturation Metric Dispatch Gate E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeAll(func() {
		redeployEPPWithFlowControl()
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, saturationRequestQueue) //nolint:errcheck
		rdb.Del(ctx, saturationResultQueue)  //nolint:errcheck
		setSimWaitingRequests(simAdminURL, 0)
	})

	ginkgo.It("processes a message when saturation is below threshold", func() {
		setSimWaitingRequests(simAdminURL, 0)

		// Wait for EPP to reflect low saturation in Prometheus.
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v < 0.5 })

		msg := makeRequestMessage("sat-below-threshold", 5*time.Minute)
		enqueueMessage(ctx, rdb, saturationRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, saturationResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("sat-below-threshold"))
	})

	ginkgo.It("pauses processing when saturation is at or above threshold", func() {
		// Drive waiting requests high → EPP saturation >> threshold (0.7) → gate closed.
		setSimWaitingRequests(simAdminURL, 10)

		// Wait for saturation to propagate through EPP → Prometheus.
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v >= 0.7 })

		msg := makeRequestMessage("sat-above-threshold", 5*time.Minute)
		enqueueMessage(ctx, rdb, saturationRequestQueue, msg)

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Drop waiting requests → saturation falls → gate reopens → message processed.
		setSimWaitingRequests(simAdminURL, 0)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, saturationResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("sat-above-threshold"))
	})

	ginkgo.It("resumes processing when saturation drops below threshold", func() {
		setSimWaitingRequests(simAdminURL, 10)
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v >= 0.7 })

		for i := 1; i <= 3; i++ {
			msg := makeRequestMessage(fmt.Sprintf("sat-resume-%d", i), 5*time.Minute)
			enqueueMessage(ctx, rdb, saturationRequestQueue, msg)
		}

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		setSimWaitingRequests(simAdminURL, 0)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, saturationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))
	})
})
