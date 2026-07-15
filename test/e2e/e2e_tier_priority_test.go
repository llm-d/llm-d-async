package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/llm-d/llm-d-async/api"
)

var _ = ginkgo.Describe("Tier Priority Admission and Merge Policy E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeAll(func() {
		redeployEPPWithFlowControl()
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		setSimWaitingRequests(simAdminURL, 0)
		rdb.Del(ctx, tierPriorityInteractiveQueue) //nolint:errcheck
		rdb.Del(ctx, tierPriorityAsyncQueue)       //nolint:errcheck
		rdb.Del(ctx, tierPriorityResultQueue)      //nolint:errcheck

		// Ensure the queues are completely empty before starting the test.
		gomega.Eventually(func() int64 {
			return rdb.ZCard(ctx, tierPriorityInteractiveQueue).Val() +
				rdb.ZCard(ctx, tierPriorityAsyncQueue).Val() +
				rdb.LLen(ctx, tierPriorityResultQueue).Val()
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))
	})

	ginkgo.It("processes messages from both interactive and async queues when not saturated", func() {
		setSimWaitingRequests(simAdminURL, 0)
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v < 0.5 })

		// Enqueue an interactive overflow request.
		routingInt := api.InternalRouting{
			RequestQueueName: tierPriorityInteractiveQueue,
			Labels: map[string]string{
				"tier": "interactive",
			},
		}
		routingInt.SetClassification(api.ClassificationOverflow)
		msgInt := makeRequestMessage("tp-interactive-ok", 5*time.Minute)
		enqueueMessageWithRouting(ctx, rdb, tierPriorityInteractiveQueue, msgInt, routingInt)

		// Enqueue an async overflow request.
		routingAsync := api.InternalRouting{
			RequestQueueName: tierPriorityAsyncQueue,
			Labels: map[string]string{
				"tier": "async",
			},
		}
		routingAsync.SetClassification(api.ClassificationOverflow)
		msgAsync := makeRequestMessage("tp-async-ok", 5*time.Minute)
		enqueueMessageWithRouting(ctx, rdb, tierPriorityAsyncQueue, msgAsync, routingAsync)

		// Wait for both to be processed successfully.
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, tierPriorityResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 2))

		results := make(map[string]*api.ResultMessage)
		for i := 0; i < 2; i++ {
			res := popResult(ctx, rdb, tierPriorityResultQueue)
			gomega.Expect(res).NotTo(gomega.BeNil())
			results[res.ID] = res
		}

		gomega.Expect(results).To(gomega.HaveKey("tp-interactive-ok"))
		gomega.Expect(results).To(gomega.HaveKey("tp-async-ok"))
		gomega.Expect(results["tp-interactive-ok"].Payload).NotTo(gomega.ContainSubstring("Too Many Requests"))
		gomega.Expect(results["tp-async-ok"].Payload).NotTo(gomega.ContainSubstring("Too Many Requests"))
	})

	ginkgo.It("enforces tier-priority admission rules when saturated", func() {
		// Drive saturation high.
		setSimWaitingRequests(simAdminURL, 10)
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v >= 0.7 })

		// 1. Enqueue interactive overflow -> should be immediately dropped with 429.
		routingIntOF := api.InternalRouting{
			RequestQueueName: tierPriorityInteractiveQueue,
			Labels: map[string]string{
				"tier": "interactive",
			},
		}
		routingIntOF.SetClassification(api.ClassificationOverflow)
		msgIntOF := makeRequestMessage("tp-interactive-of", 5*time.Minute)
		enqueueMessageWithRouting(ctx, rdb, tierPriorityInteractiveQueue, msgIntOF, routingIntOF)

		// 2. Enqueue async overflow -> should be refused (remains/retried in queue).
		routingAsyncOF := api.InternalRouting{
			RequestQueueName: tierPriorityAsyncQueue,
			Labels: map[string]string{
				"tier": "async",
			},
		}
		routingAsyncOF.SetClassification(api.ClassificationOverflow)
		msgAsyncOF := makeRequestMessage("tp-async-of", 5*time.Minute)
		enqueueMessageWithRouting(ctx, rdb, tierPriorityAsyncQueue, msgAsyncOF, routingAsyncOF)

		// 3. Enqueue interactive reserved -> should wait (remain in queue).
		routingIntRes := api.InternalRouting{
			RequestQueueName: tierPriorityInteractiveQueue,
			Labels: map[string]string{
				"tier": "interactive",
			},
		}
		routingIntRes.SetClassification(api.ClassificationReserved)
		msgIntRes := makeRequestMessage("tp-interactive-res", 5*time.Minute)
		enqueueMessageWithRouting(ctx, rdb, tierPriorityInteractiveQueue, msgIntRes, routingIntRes)

		// Eventually, only the dropped interactive overflow message should generate a result (429).
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, tierPriorityResultQueue)
		}, 45*time.Second, 1*time.Second).Should(gomega.Equal(int64(1)))

		dropResult := popResult(ctx, rdb, tierPriorityResultQueue)
		gomega.Expect(dropResult).NotTo(gomega.BeNil())
		gomega.Expect(dropResult.ID).To(gomega.Equal("tp-interactive-of"))
		gomega.Expect(dropResult.Payload).To(gomega.ContainSubstring("Too Many Requests"))

		// Ensure no other results are produced while remaining saturated.
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, tierPriorityResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Restore saturation to low.
		setSimWaitingRequests(simAdminURL, 0)
		waitForSaturation(promURL, envoyURL, func(v float64) bool { return v < 0.5 })

		// Once saturation clears, the remaining async-overflow and interactive-reserved requests
		// should be processed successfully.
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, tierPriorityResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 2))

		results := make(map[string]*api.ResultMessage)
		for i := 0; i < 2; i++ {
			res := popResult(ctx, rdb, tierPriorityResultQueue)
			gomega.Expect(res).NotTo(gomega.BeNil())
			results[res.ID] = res
		}

		gomega.Expect(results).To(gomega.HaveKey("tp-async-of"))
		gomega.Expect(results).To(gomega.HaveKey("tp-interactive-res"))
	})
})
