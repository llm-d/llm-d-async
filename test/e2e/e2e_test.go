package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("General Integration", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		setSimWaitingRequests(simAdminURL, 0)
		setEnvoyFaultAbort(envoyAdminURL, 0)
		// Drain queues and wait for any in-flight requests to settle.
		// Delete, pause for in-flight results, delete again.
		rdb.Del(ctx, integrationRequestQueue) //nolint:errcheck
		rdb.Del(ctx, integrationResultQueue)  //nolint:errcheck
		gomega.Consistently(func() int64 {
			return rdb.LLen(ctx, integrationResultQueue).Val() +
				rdb.ZCard(ctx, integrationRequestQueue).Val()
		}, 3*time.Second, 500*time.Millisecond).Should(gomega.Equal(int64(0)))
	})

	ginkgo.AfterEach(func() {
		setEnvoyFaultAbort(envoyAdminURL, 0)
	})

	ginkgo.It("processes a message end-to-end", func() {
		msg := makeRequestMessage("e2e-basic-1", 5*time.Minute)
		enqueueMessage(ctx, rdb, integrationRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, integrationResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("e2e-basic-1"))
	})

	ginkgo.It("processes messages in deadline order", func() {
		now := time.Now()

		// Enqueue 3 messages with different deadlines (out of order).
		msg1 := makeRequestMessage("deadline-300", 300*time.Second)
		msg1.Deadline = now.Add(300 * time.Second).Unix()

		msg2 := makeRequestMessage("deadline-100", 100*time.Second)
		msg2.Deadline = now.Add(100 * time.Second).Unix()

		msg3 := makeRequestMessage("deadline-200", 200*time.Second)
		msg3.Deadline = now.Add(200 * time.Second).Unix()

		enqueueMessages(ctx, rdb, integrationRequestQueue, msg1, msg2, msg3)

		// Wait for all 3 results. With concurrency=1 the result list preserves
		// processing order, which should match deadline order.
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))

		r1 := popResult(ctx, rdb, integrationResultQueue)
		r2 := popResult(ctx, rdb, integrationResultQueue)
		r3 := popResult(ctx, rdb, integrationResultQueue)
		gomega.Expect(r1).NotTo(gomega.BeNil())
		gomega.Expect(r2).NotTo(gomega.BeNil())
		gomega.Expect(r3).NotTo(gomega.BeNil())
		gomega.Expect(r1.ID).To(gomega.Equal("deadline-100"))
		gomega.Expect(r2.ID).To(gomega.Equal("deadline-200"))
		gomega.Expect(r3.ID).To(gomega.Equal("deadline-300"))
	})

	ginkgo.It("retries on 5xx from the inference backend", func() {
		// Enable 100% fault injection so the first attempt fails with 503.
		setEnvoyFaultAbort(envoyAdminURL, 100)

		msg := makeRequestMessage("retry-msg", 5*time.Minute)
		enqueueMessage(ctx, rdb, integrationRequestQueue, msg)

		// Message should not be delivered while faults are active.
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Disable fault injection so retries succeed.
		setEnvoyFaultAbort(envoyAdminURL, 0)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 120*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, integrationResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("retry-msg"))
	})

	ginkgo.It("drops expired messages and processes valid ones", func() {
		expiredMsg := makeRequestMessage("expired-msg", -100*time.Second)
		validMsg := makeRequestMessage("valid-msg", 5*time.Minute)

		enqueueMessage(ctx, rdb, integrationRequestQueue, expiredMsg)
		enqueueMessage(ctx, rdb, integrationRequestQueue, validMsg)

		// The expired message is silently dropped at dequeue time (deadline
		// already in the past). Only the valid message produces a result.
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, integrationResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("valid-msg"))

		// Verify the expired message was removed from the request queue
		// without producing a result.
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 3*time.Second, 500*time.Millisecond).Should(gomega.Equal(int64(0)))
	})

	ginkgo.It("collects all results from a batch of messages", func() {
		deadline := time.Now().Add(5 * time.Minute)
		ids := []string{"batch-1", "batch-2", "batch-3", "batch-4", "batch-5"}

		for _, id := range ids {
			msg := makeRequestMessage(id, 5*time.Minute)
			msg.Deadline = deadline.Unix()
			enqueueMessage(ctx, rdb, integrationRequestQueue, msg)
		}

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 5))

		collected := make(map[string]bool)
		for i := 0; i < 5; i++ {
			r := popResult(ctx, rdb, integrationResultQueue)
			gomega.Expect(r).NotTo(gomega.BeNil())
			collected[r.ID] = true
		}

		for _, id := range ids {
			gomega.Expect(collected).To(gomega.HaveKey(id))
		}
	})
})

var _ = ginkgo.Describe("Redis Dispatch Gate E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, redisGateRequestQueue) //nolint:errcheck
		rdb.Del(ctx, redisGateResultQueue)  //nolint:errcheck
		clearDispatchGateBudget(ctx, rdb)
	})

	ginkgo.AfterEach(func() {
		clearDispatchGateBudget(ctx, rdb)
	})

	ginkgo.It("pauses processing when budget is zero", func() {
		setDispatchGateBudget(ctx, rdb, "0.0")

		msg := makeRequestMessage("gated-pause", 5*time.Minute)
		enqueueMessage(ctx, rdb, redisGateRequestQueue, msg)

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, redisGateResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		setDispatchGateBudget(ctx, rdb, "1.0")

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, redisGateResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, redisGateResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("gated-pause"))
	})

	ginkgo.It("resumes processing when budget changes from zero to one", func() {
		setDispatchGateBudget(ctx, rdb, "0.0")

		for i := 1; i <= 3; i++ {
			msg := makeRequestMessage(fmt.Sprintf("resume-%d", i), 5*time.Minute)
			enqueueMessage(ctx, rdb, redisGateRequestQueue, msg)
		}

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, redisGateResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		setDispatchGateBudget(ctx, rdb, "1.0")

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, redisGateResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))
	})
})
