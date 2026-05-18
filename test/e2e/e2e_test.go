package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
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
		setEnvoyFaultDelay(envoyAdminURL, 0)
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

	ginkgo.It("re-queues in-flight messages on pod termination", func() {
		// Add a 60s delay to 100% of requests so they stay in-flight in the Worker.
		setEnvoyFaultDelay(envoyAdminURL, 100)

		// Ensure the deployment is restored even if the test fails.
		ginkgo.DeferCleanup(func() {
			setEnvoyFaultDelay(envoyAdminURL, 0)
			cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
				"-n", nsName, "scale", "deployment/integration-async-processor",
				"--replicas=1", "--timeout=60s")
			session, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Eventually(session).WithTimeout(60 * time.Second).Should(gexec.Exit(0))

			cmd = exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
				"-n", nsName, "rollout", "status",
				"deployment/integration-async-processor", "--timeout=120s")
			session, err = gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Eventually(session).WithTimeout(180 * time.Second).Should(gexec.Exit(0))
		})

		ids := []string{"shutdown-1", "shutdown-2", "shutdown-3"}
		for _, id := range ids {
			enqueueMessage(ctx, rdb, integrationRequestQueue, makeRequestMessage(id, 5*time.Minute))
		}

		// Wait until the processor has popped messages from the request queue
		// (they are now in-flight, stuck in Envoy's delay).
		gomega.Eventually(func() int64 {
			return rdb.ZCard(ctx, integrationRequestQueue).Val()
		}, 30*time.Second, 500*time.Millisecond).Should(gomega.Equal(int64(0)))

		// Scale the deployment to 0 to trigger graceful shutdown. Using scale
		// instead of pod delete prevents a replacement pod from consuming the
		// re-queued messages before we can assert.
		cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "scale", "deployment/integration-async-processor",
			"--replicas=0", "--timeout=60s")
		session, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(60 * time.Second).Should(gexec.Exit(0))

		// Wait for the pod to fully terminate.
		cmd = exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "wait", "pod",
			"-l", "app.kubernetes.io/instance=integration",
			"--for=delete", "--timeout=60s")
		session, err = gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(90 * time.Second).Should(gexec.Exit(0))

		// The Worker should have re-queued the in-flight messages back to
		// Redis on shutdown instead of treating them as fatal errors.
		count := rdb.ZCard(ctx, integrationRequestQueue).Val()
		gomega.Expect(count).To(gomega.BeNumerically(">=", int64(len(ids))),
			"expected all in-flight messages re-queued after shutdown")
	})

	ginkgo.It("does not lose messages on pod termination", func() {
		// Enable 100% fault injection so messages fail and enter the retry loop.
		setEnvoyFaultAbort(envoyAdminURL, 100)

		ids := []string{"shutdown-noloss-1", "shutdown-noloss-2", "shutdown-noloss-3"}
		for _, id := range ids {
			enqueueMessage(ctx, rdb, integrationRequestQueue, makeRequestMessage(id, 5*time.Minute))
		}

		// Confirm no results appear while faults are active (messages are
		// cycling through the retry loop). This also gives the processor
		// enough time to attempt each message at least once.
		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 5*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Delete the processor pod with a short grace period to trigger shutdown.
		cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "delete", "pod",
			"-l", "app.kubernetes.io/instance=integration",
			"--grace-period=10")
		session, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(60 * time.Second).Should(gexec.Exit(0))

		// Disable fault injection so the replacement pod can process messages.
		setEnvoyFaultAbort(envoyAdminURL, 0)

		// Wait for the replacement pod to be ready.
		cmd = exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
			"-n", nsName, "rollout", "status",
			"deployment/integration-async-processor", "--timeout=120s")
		session, err = gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(180 * time.Second).Should(gexec.Exit(0))

		// All messages should eventually appear in the result queue.
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 120*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", int64(len(ids))))

		collected := make(map[string]bool)
		for range ids {
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
