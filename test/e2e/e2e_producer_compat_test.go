package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/llm-d/llm-d-async/api"
	producerpkg "github.com/llm-d/llm-d-async/producer"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const retryQueue = "retry-sortedset"

var _ = ginkgo.Describe("Producer v0.10.0 with the current dispatcher", ginkgo.Ordered, func() {
	var (
		ctx context.Context
		bin string
	)

	ginkgo.BeforeAll(func() {
		bin = filepath.Join(ginkgo.GinkgoT().TempDir(), "oldproducer")
		build := exec.Command("go", "build", "-o", bin, ".")
		build.Dir = "oldproducer"
		build.Env = append(os.Environ(), "GOWORK=off")
		out, err := build.CombinedOutput()
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), string(out))
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		setEnvoyFaultAbort(envoyAdminURL, 0)
		clearDispatchGateBudget(ctx, rdb)
		rdb.Del(ctx, redisGateRequestQueue, redisGateResultQueue, integrationRequestQueue, integrationResultQueue) //nolint:errcheck
	})

	ginkgo.AfterEach(func() {
		setEnvoyFaultAbort(envoyAdminURL, 0)
		clearDispatchGateBudget(ctx, rdb)
	})

	oldProducer := func(command, requestQueue, resultQueue string, extra ...string) []byte {
		args := append([]string{command,
			"-redis-url", "redis://localhost:" + redisPort,
			"-request-queue", requestQueue,
			"-result-queue", resultQueue,
		}, extra...)
		cmd := exec.Command(bin, args...)
		cmd.Stderr = ginkgo.GinkgoWriter
		out, err := cmd.Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "oldproducer %s", strings.Join(args, " "))
		return out
	}

	submit := func(requestQueue, resultQueue, id string) {
		oldProducer("submit", requestQueue, resultQueue, "-id", id)
	}

	result := func(requestQueue, resultQueue string, timeout time.Duration) api.ResultMessage {
		out := oldProducer("result", requestQueue, resultQueue, "-timeout", timeout.String())
		var res api.ResultMessage
		gomega.Expect(json.Unmarshal(out, &res)).To(gomega.Succeed(), string(out))
		return res
	}

	expectInline := func(member string) {
		gomega.Expect(member).To(gomega.ContainSubstring(`"payload":{"model":`))
		gomega.Expect(member).NotTo(gomega.ContainSubstring("payload_ref"))
	}

	ginkgo.It("dispatches inline requests and returns results the old producer reads", func() {
		setDispatchGateBudget(ctx, rdb, "0.0")
		ids := []string{"compat-1", "compat-2", "compat-3"}
		for _, id := range ids {
			submit(redisGateRequestQueue, redisGateResultQueue, id)
		}

		members, err := rdb.ZRange(ctx, redisGateRequestQueue, 0, -1).Result()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(members).To(gomega.HaveLen(len(ids)))
		for _, m := range members {
			expectInline(m)
		}
		gomega.Expect(rdb.Keys(ctx, "request-payload:*").Val()).To(gomega.BeEmpty())

		setDispatchGateBudget(ctx, rdb, "1.0")

		var got []string
		for range ids {
			res := result(redisGateRequestQueue, redisGateResultQueue, 60*time.Second)
			gomega.Expect(res.StatusCode).To(gomega.Equal(200), "result %+v", res)
			gomega.Expect(res.ErrorCode).To(gomega.BeEmpty())
			got = append(got, res.ID)
		}
		gomega.Expect(got).To(gomega.ConsistOf(ids))
	})

	ginkgo.It("retries an inline request inline after a 5xx", func() {
		setEnvoyFaultAbort(envoyAdminURL, 100)
		submit(integrationRequestQueue, integrationResultQueue, "compat-retry")

		var retried string
		gomega.Eventually(func() string {
			members := append(rdb.ZRange(ctx, retryQueue, 0, -1).Val(), rdb.ZRange(ctx, integrationRequestQueue, 0, -1).Val()...)
			for _, m := range members {
				if strings.Contains(m, `"id":"compat-retry"`) && strings.Contains(m, `"retry_count"`) {
					retried = m
				}
			}
			return retried
		}, 30*time.Second, 200*time.Millisecond).ShouldNot(gomega.BeEmpty())
		expectInline(retried)

		setEnvoyFaultAbort(envoyAdminURL, 0)

		res := result(integrationRequestQueue, integrationResultQueue, 120*time.Second)
		gomega.Expect(res.ID).To(gomega.Equal("compat-retry"))
		gomega.Expect(res.StatusCode).To(gomega.Equal(200), "result %+v", res)
	})

	ginkgo.It("cancels an inline request", func() {
		setDispatchGateBudget(ctx, rdb, "0.0")
		submit(redisGateRequestQueue, redisGateResultQueue, "compat-cancel")
		oldProducer("cancel", redisGateRequestQueue, redisGateResultQueue, "-id", "compat-cancel")
		setDispatchGateBudget(ctx, rdb, "1.0")

		res := result(redisGateRequestQueue, redisGateResultQueue, 60*time.Second)
		gomega.Expect(res.ID).To(gomega.Equal("compat-cancel"))
		gomega.Expect(res.ErrorCode).To(gomega.Equal(api.ErrCodeCancelled))
	})
})

func newProducer(requestQueue, resultQueue string, opts ...producerpkg.ProducerOption) *producerpkg.RedisSortedSetProducer {
	p, err := producerpkg.NewRedisSortedSetProducer(producerpkg.RedisSortedSetConfig{
		RedisURL:         "redis://localhost:" + redisPort,
		RequestQueueName: requestQueue,
		ResultQueueName:  resultQueue,
	}, opts...)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(func() {
		gomega.Expect(p.Close()).To(gomega.Succeed())
	})
	return p
}

func getResult(ctx context.Context, p *producerpkg.RedisSortedSetProducer) *api.ResultMessage {
	resultCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	res, err := p.GetResult(resultCtx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return res
}

func compatRequest(id string) *api.RequestMessage {
	return &api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(time.Hour).Unix(),
		Payload:  testPayload(map[string]any{"model": "test-model", "prompt": id}),
		Model:    "test-model",
	}
}

var _ = ginkgo.Describe("Current producer with the v0.10.0 dispatcher", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		clearDispatchGateBudget(ctx, rdb)
		rdb.Del(ctx, oldDispatcherRequestQueue, oldDispatcherResultQueue) //nolint:errcheck
	})

	ginkgo.AfterEach(func() {
		clearDispatchGateBudget(ctx, rdb)
	})

	ginkgo.It("dispatches requests written inline by default", func() {
		p := newProducer(oldDispatcherRequestQueue, oldDispatcherResultQueue)
		setDispatchGateBudget(ctx, rdb, "0.0")
		ids := []string{"fwd-1", "fwd-2", "fwd-3"}
		for _, id := range ids {
			gomega.Expect(p.SubmitRequest(ctx, compatRequest(id))).To(gomega.Succeed())
		}

		members, err := rdb.ZRange(ctx, oldDispatcherRequestQueue, 0, -1).Result()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(members).To(gomega.HaveLen(len(ids)))
		for _, m := range members {
			gomega.Expect(m).To(gomega.ContainSubstring(`"payload":{"model":`))
			gomega.Expect(m).NotTo(gomega.ContainSubstring("payload_ref"))
		}

		setDispatchGateBudget(ctx, rdb, "1.0")

		var got []string
		for range ids {
			res := getResult(ctx, p)
			gomega.Expect(res.StatusCode).To(gomega.Equal(200), "result %+v", res)
			got = append(got, res.ID)
		}
		gomega.Expect(got).To(gomega.ConsistOf(ids))
	})

	ginkgo.It("cancels a request", func() {
		p := newProducer(oldDispatcherRequestQueue, oldDispatcherResultQueue)
		setDispatchGateBudget(ctx, rdb, "0.0")
		gomega.Expect(p.SubmitRequest(ctx, compatRequest("fwd-cancel"))).To(gomega.Succeed())
		gomega.Expect(p.CancelRequests(ctx, []string{"fwd-cancel"})).To(gomega.Succeed())
		setDispatchGateBudget(ctx, rdb, "1.0")

		res := getResult(ctx, p)
		gomega.Expect(res.ID).To(gomega.Equal("fwd-cancel"))
		gomega.Expect(res.ErrorCode).To(gomega.Equal(api.ErrCodeCancelled))
	})
})

var _ = ginkgo.Describe("Current producer with payload keys and the current dispatcher", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		clearDispatchGateBudget(ctx, rdb)
		rdb.Del(ctx, redisGateRequestQueue, redisGateResultQueue) //nolint:errcheck
	})

	ginkgo.AfterEach(func() {
		clearDispatchGateBudget(ctx, rdb)
	})

	ginkgo.It("dispatches a request whose payload is stored apart and deletes the key", func() {
		p := newProducer(redisGateRequestQueue, redisGateResultQueue, producerpkg.WithPayloadKeys())
		setDispatchGateBudget(ctx, rdb, "0.0")
		gomega.Expect(p.SubmitRequest(ctx, compatRequest("keyed"))).To(gomega.Succeed())

		members, err := rdb.ZRange(ctx, redisGateRequestQueue, 0, -1).Result()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(members).To(gomega.HaveLen(1))
		gomega.Expect(members[0]).NotTo(gomega.ContainSubstring(`"prompt"`))
		var queued api.InternalRequest
		gomega.Expect(json.Unmarshal([]byte(members[0]), &queued)).To(gomega.Succeed())
		gomega.Expect(queued.PayloadRef).NotTo(gomega.BeEmpty())
		gomega.Expect(rdb.Exists(ctx, queued.PayloadRef).Val()).To(gomega.Equal(int64(1)))

		setDispatchGateBudget(ctx, rdb, "1.0")

		res := getResult(ctx, p)
		gomega.Expect(res.ID).To(gomega.Equal("keyed"))
		gomega.Expect(res.StatusCode).To(gomega.Equal(200), "result %+v", res)
		gomega.Expect(rdb.Exists(ctx, queued.PayloadRef).Val()).To(gomega.Equal(int64(0)))
	})
})
