package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	quotaRequestQueue = "quota-request-sortedset"
	quotaResultQueue  = "quota-result-list"
	quotaKeyPrefix    = "quota:"
)

var _ = ginkgo.Describe("Attribute Gating E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, quotaRequestQueue)              //nolint:errcheck
		rdb.Del(ctx, quotaResultQueue)               //nolint:errcheck
		rdb.Del(ctx, quotaKeyPrefix+"userid:user-a") //nolint:errcheck
	})

	ginkgo.It("enforces per-user concurrency limit", func() {
		// Simulate an in-flight request by setting the concurrency counter
		// directly. The redis-quota gate uses key "quota:userid:<value>" with
		// limit=1, so setting the counter to 1 closes the gate for user-a.
		rdb.Set(ctx, quotaKeyPrefix+"userid:user-a", "1", 5*time.Minute) //nolint:errcheck

		msg := makeRequestMessage("quota-blocked", 5*time.Minute)
		msg.Metadata = map[string]string{"userid": "user-a"}
		enqueueMessage(ctx, rdb, quotaRequestQueue, msg)

		gomega.Consistently(func() int64 {
			return getResultCount(ctx, rdb, quotaResultQueue)
		}, 10*time.Second, 1*time.Second).Should(gomega.Equal(int64(0)))

		// Release quota by clearing the key.
		rdb.Del(ctx, quotaKeyPrefix+"userid:user-a") //nolint:errcheck

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, quotaResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popResult(ctx, rdb, quotaResultQueue)
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.ID).To(gomega.Equal("quota-blocked"))
	})
})
