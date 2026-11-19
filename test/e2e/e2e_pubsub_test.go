package e2e

import (
	"context"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("GCP PubSub Integration", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		setSimWaitingRequests(simAdminURL, 0)
		setEnvoyFaultAbort(envoyAdminURL, 0)
		recreatePubSubSubscription(ctx, pubsubClient, pubsubProjectID, pubsubResultSub, pubsubResultTopic)
	})

	ginkgo.AfterEach(func() {
		setEnvoyFaultAbort(envoyAdminURL, 0)
		setEnvoyFaultDelay(envoyAdminURL, 0)
	})

	ginkgo.It("processes a message end-to-end via GCP PubSub", func() {
		msg := makeRequestMessage("pubsub-e2e-1", 5*time.Minute)
		enqueuePubSubMessage(ctx, pubsubClient, pubsubRequestTopic, msg)

		var result *api.ResultMessage
		gomega.Eventually(func() *api.ResultMessage {
			result = popPubSubResult(ctx, pubsubClient, pubsubResultSub)
			return result
		}, 60*time.Second, 1*time.Second).ShouldNot(gomega.BeNil())

		gomega.Expect(result.ID).To(gomega.Equal("pubsub-e2e-1"))
	})

	ginkgo.It("processes multiple messages via GCP PubSub", func() {
		msg1 := makeRequestMessage("pubsub-batch-1", 5*time.Minute)
		msg2 := makeRequestMessage("pubsub-batch-2", 5*time.Minute)
		msg3 := makeRequestMessage("pubsub-batch-3", 5*time.Minute)
		enqueuePubSubMessages(ctx, pubsubClient, pubsubRequestTopic, msg1, msg2, msg3)

		ids := []string{"pubsub-batch-1", "pubsub-batch-2", "pubsub-batch-3"}

		collected := make(map[string]bool)
		for i := 0; i < len(ids); i++ {
			var result *api.ResultMessage
			gomega.Eventually(func() *api.ResultMessage {
				result = popPubSubResult(ctx, pubsubClient, pubsubResultSub)
				return result
			}, 60*time.Second, 1*time.Second).ShouldNot(gomega.BeNil())

			collected[result.ID] = true
		}

		for _, id := range ids {
			gomega.Expect(collected).To(gomega.HaveKey(id))
		}
	})

	ginkgo.It("accepts messages carrying caller attributes via GCP PubSub", func() {
		msg := makeRequestMessage("pubsub-attr-1", 5*time.Minute)
		msg.Metadata = map[string]string{
			"custom-key": "custom-value",
		}
		enqueuePubSubMessage(ctx, pubsubClient, pubsubRequestTopic, msg)

		var result *api.ResultMessage
		gomega.Eventually(func() *api.ResultMessage {
			result = popPubSubResult(ctx, pubsubClient, pubsubResultSub)
			return result
		}, 60*time.Second, 1*time.Second).ShouldNot(gomega.BeNil())

		gomega.Expect(result.ID).To(gomega.Equal("pubsub-attr-1"))
	})

	ginkgo.It("retries on 5xx from the inference backend via GCP PubSub", func() {
		// Enable 100% fault injection so the first attempt fails with 503.
		setEnvoyFaultAbort(envoyAdminURL, 100)

		msg := makeRequestMessage("pubsub-retry-msg", 5*time.Minute)
		enqueuePubSubMessage(ctx, pubsubClient, pubsubRequestTopic, msg)

		// Message should not be delivered while faults are active.
		gomega.Consistently(func() *api.ResultMessage {
			return popPubSubResult(ctx, pubsubClient, pubsubResultSub)
		}, 5*time.Second, 1*time.Second).Should(gomega.BeNil())

		// Disable fault injection so retries succeed.
		setEnvoyFaultAbort(envoyAdminURL, 0)

		var result *api.ResultMessage
		gomega.Eventually(func() *api.ResultMessage {
			result = popPubSubResult(ctx, pubsubClient, pubsubResultSub)
			return result
		}, 120*time.Second, 1*time.Second).ShouldNot(gomega.BeNil())

		gomega.Expect(result.ID).To(gomega.Equal("pubsub-retry-msg"))
	})
})
