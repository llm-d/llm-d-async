package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
)

const (
	requestQueue = "request-sortedset"
	resultQueue  = "result-list"
)

func enqueueMessage(ctx context.Context, rdb *redis.Client, queue string, msg api.RequestMessage) {
	data, err := json.Marshal(msg)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	err = rdb.ZAdd(ctx, queue, redis.Z{
		Score:  parseDeadline(msg.DeadlineUnixSec),
		Member: string(data),
	}).Err()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
}

func parseDeadline(deadline string) float64 {
	var d float64
	_, err := fmt.Sscanf(deadline, "%f", &d)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return d
}

func getResultCount(ctx context.Context, rdb *redis.Client, queue string) int64 {
	n, err := rdb.LLen(ctx, queue).Result()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return n
}

func popResult(ctx context.Context, rdb *redis.Client, queue string) *api.ResultMessage {
	val, err := rdb.RPop(ctx, queue).Result()
	if err == redis.Nil {
		return nil
	}
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	var msg api.ResultMessage
	gomega.ExpectWithOffset(1, json.Unmarshal([]byte(val), &msg)).To(gomega.Succeed())
	return &msg
}

func cleanupQueues(ctx context.Context, rdb *redis.Client) {
	rdb.Del(ctx, requestQueue) //nolint:errcheck
	rdb.Del(ctx, resultQueue)  //nolint:errcheck
}

func resetMock(adminURL string) {
	req, err := http.NewRequest(http.MethodDelete, adminURL+"/admin/reset", nil)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	resp, err := http.DefaultClient.Do(req)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close() //nolint:errcheck
	gomega.ExpectWithOffset(1, resp.StatusCode).To(gomega.Equal(http.StatusOK))
}

func setMockFailures(adminURL string, status, count int) {
	body, _ := json.Marshal(map[string]int{"status": status, "count": count})
	resp, err := http.Post(adminURL+"/admin/fail-next", "application/json", bytes.NewReader(body))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close() //nolint:errcheck
	gomega.ExpectWithOffset(1, resp.StatusCode).To(gomega.Equal(http.StatusOK))
}

func getRequestLog(adminURL string) []string {
	resp, err := http.Get(adminURL + "/admin/request-log")
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())

	var log []string
	gomega.ExpectWithOffset(1, json.Unmarshal(body, &log)).To(gomega.Succeed())
	return log
}

const dispatchGateBudgetKey = "dispatch-gate-budget"

func setDispatchGateBudget(ctx context.Context, rdb *redis.Client, budget string) {
	gomega.ExpectWithOffset(1, rdb.Set(ctx, dispatchGateBudgetKey, budget, 0).Err()).NotTo(gomega.HaveOccurred())
}

func clearDispatchGateBudget(ctx context.Context, rdb *redis.Client) {
	rdb.Del(ctx, dispatchGateBudgetKey) //nolint:errcheck
}

func makeRequestMessage(id string, deadlineOffset time.Duration) api.RequestMessage {
	deadline := time.Now().Add(deadlineOffset)
	return api.RequestMessage{
		Id:              id,
		DeadlineUnixSec: fmt.Sprintf("%d", deadline.Unix()),
		Payload:         map[string]any{"model": id, "prompt": "test"},
	}
}
