package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Redis Sorted Set E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		cleanupQueues(ctx, rdb)
		resetMock(adminURL)
	})

	ginkgo.It("processes a message end-to-end", func() {
		msg := makeRequestMessage("e2e-basic-1", 5*time.Minute)
		enqueueMessage(ctx, rdb, requestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, resultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("e2e-basic-1"))
	})

	ginkgo.It("processes messages in deadline order", func() {
		now := time.Now()

		// Enqueue 3 messages with different deadlines (out of order)
		msg1 := makeRequestMessage("deadline-300", 300*time.Second)
		msg1.Deadline = now.Add(300 * time.Second).Unix()

		msg2 := makeRequestMessage("deadline-100", 100*time.Second)
		msg2.Deadline = now.Add(100 * time.Second).Unix()

		msg3 := makeRequestMessage("deadline-200", 200*time.Second)
		msg3.Deadline = now.Add(200 * time.Second).Unix()

		enqueueMessages(ctx, rdb, requestQueue, msg1, msg2, msg3)

		// Wait for all 3 results
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))

		// Check the order in the mock's request log
		log := getRequestLog(adminURL)
		gomega.Expect(len(log)).To(gomega.BeNumerically(">=", 3))
		gomega.Expect(log[0]).To(gomega.Equal("deadline-100"))
		gomega.Expect(log[1]).To(gomega.Equal("deadline-200"))
		gomega.Expect(log[2]).To(gomega.Equal("deadline-300"))
	})

	ginkgo.It("skips expired messages", func() {
		// Enqueue an expired message
		expiredMsg := makeRequestMessage("expired-msg", -100*time.Second)
		// Enqueue a valid message
		validMsg := makeRequestMessage("valid-msg", 5*time.Minute)

		enqueueMessage(ctx, rdb, requestQueue, expiredMsg)
		enqueueMessage(ctx, rdb, requestQueue, validMsg)

		// Wait for the valid message to be processed
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		// Give extra time to ensure only valid messages appear
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.BeNumerically("<=", 1))

		result := popResult(ctx, rdb, resultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("valid-msg"))
	})

	ginkgo.It("retries on 5xx from inference gateway", func() {
		// Configure mock to fail the first request with 500
		setMockFailures(adminURL, 500, 1)

		msg := makeRequestMessage("retry-msg", 5*time.Minute)
		enqueueMessage(ctx, rdb, requestQueue, msg)

		// Eventually the result should appear after retry
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 120*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, resultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("retry-msg"))
	})

	ginkgo.It("collects all results from batch of messages", func() {
		// Enqueue 5 messages rapidly with the same priority
		deadline := time.Now().Add(5 * time.Minute)
		ids := []string{"fifo-1", "fifo-2", "fifo-3", "fifo-4", "fifo-5"}

		for _, id := range ids {
			msg := makeRequestMessage(id, 5*time.Minute)
			msg.Deadline = deadline.Unix()
			enqueueMessage(ctx, rdb, requestQueue, msg)
		}

		// Wait for all results
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 5))

		// Collect all results via RPOP and verify all IDs are present
		collected := make(map[string]bool)
		for i := 0; i < 5; i++ {
			r := popResult(ctx, rdb, resultQueue)
			gomega.Expect(r).NotTo(gomega.BeNil())
			collected[r.ID] = true
		}

		for _, id := range ids {
			gomega.Expect(collected).To(gomega.HaveKey(id))
		}
	})
})

var _ = ginkgo.Describe("Dispatch Gate E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		cleanupQueues(ctx, rdb)
		resetMock(adminURL)
		// Default to full capacity so other tests are unaffected
		clearDispatchGateBudget(ctx, rdb)
	})

	ginkgo.AfterEach(func() {
		// Restore full capacity
		clearDispatchGateBudget(ctx, rdb)
	})

	ginkgo.It("pauses processing when budget is zero", func() {
		// Close the gate
		setDispatchGateBudget(ctx, rdb, "0.0")

		msg := makeRequestMessage("gated-pause", 5*time.Minute)
		enqueueMessage(ctx, rdb, requestQueue, msg)

		// Verify the message is NOT processed while gate is closed
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Open the gate
		setDispatchGateBudget(ctx, rdb, "1.0")

		// Verify the message IS processed after gate opens
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, resultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("gated-pause"))
	})

	ginkgo.It("resumes processing when budget changes from zero to one", func() {
		// Start with gate closed
		setDispatchGateBudget(ctx, rdb, "0.0")

		// Enqueue multiple messages
		for i := 1; i <= 3; i++ {
			msg := makeRequestMessage(fmt.Sprintf("resume-%d", i), 5*time.Minute)
			enqueueMessage(ctx, rdb, requestQueue, msg)
		}

		// Nothing should be processed
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Open the gate
		setDispatchGateBudget(ctx, rdb, "1.0")

		// All 3 messages should eventually be processed
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, resultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 3))
	})
})
