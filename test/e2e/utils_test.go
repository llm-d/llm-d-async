package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"

	"github.com/llm-d-incubation/llm-d-async/api"
)

const (
	requestQueue = "request-sortedset"
	resultQueue  = "result-list"
)

var adminClient = &http.Client{Timeout: 10 * time.Second}

func enqueueMessage(ctx context.Context, rdb *redis.Client, queue string, msg api.RequestMessage) {
	ir := api.NewInternalRequest(api.InternalRouting{}, &msg)
	data, err := json.Marshal(ir)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	err = rdb.ZAdd(ctx, queue, redis.Z{
		Score:  float64(msg.Deadline),
		Member: string(data),
	}).Err()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
}

// enqueueMessages adds all messages to the sorted set in a single Redis
// pipeline so they become visible atomically. This prevents the processor
// from dequeuing early messages before the rest are enqueued.
func enqueueMessages(ctx context.Context, rdb *redis.Client, queue string, msgs ...api.RequestMessage) {
	pipe := rdb.Pipeline()
	for _, msg := range msgs {
		ir := api.NewInternalRequest(api.InternalRouting{}, &msg)
		data, err := json.Marshal(ir)
		gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
		pipe.ZAdd(ctx, queue, redis.Z{
			Score:  float64(msg.Deadline),
			Member: string(data),
		})
	}
	_, err := pipe.Exec(ctx)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
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
	resp, err := adminClient.Do(req)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close() //nolint:errcheck
	gomega.ExpectWithOffset(1, resp.StatusCode).To(gomega.Equal(http.StatusOK))
}

func setMockFailures(adminURL string, status, count int) {
	body, _ := json.Marshal(map[string]int{"status": status, "count": count})
	req, err := http.NewRequest(http.MethodPost, adminURL+"/admin/fail-next", bytes.NewReader(body))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := adminClient.Do(req)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close() //nolint:errcheck
	gomega.ExpectWithOffset(1, resp.StatusCode).To(gomega.Equal(http.StatusOK))
}

func getRequestLog(adminURL string) []string {
	req, err := http.NewRequest(http.MethodGet, adminURL+"/admin/request-log", nil)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	resp, err := adminClient.Do(req)
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

func setPromMockSaturation(promMockURL string, value string) {
	setPromMockValue(promMockURL+"/admin/saturation", value)
}

func setPromMockBudget(promMockURL string, value string) {
	setPromMockValue(promMockURL+"/admin/budget", value)
}

func setPromMockValue(url string, value string) {
	body, _ := json.Marshal(map[string]string{"value": value})
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := adminClient.Do(req)
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close() //nolint:errcheck
	gomega.ExpectWithOffset(2, resp.StatusCode).To(gomega.Equal(http.StatusOK))
}

func makeRequestMessage(id string, deadlineOffset time.Duration) api.RequestMessage {
	deadline := time.Now().Add(deadlineOffset)
	return api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: deadline.Unix(),
		Payload:  map[string]any{"model": id, "prompt": "test"},
	}
}
