package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"

	"github.com/llm-d-incubation/llm-d-async/api"
)

const (
	integrationRequestQueue = "integration-request-sortedset"
	integrationResultQueue  = "integration-result-list"

	saturationRequestQueue = "saturation-request-sortedset"
	saturationResultQueue  = "saturation-result-list"

	redisGateRequestQueue = "redis-gate-request-sortedset"
	redisGateResultQueue  = "redis-gate-result-list"
	dispatchGateBudgetKey = "dispatch-gate-budget"

	endpointScrapeRequestQueue = "endpoint-scrape-request-sortedset"
	endpointScrapeResultQueue  = "endpoint-scrape-result-list"

	shortDrainRequestQueue = "short-drain-request-sortedset"
	shortDrainResultQueue  = "short-drain-result-list"

	tierPriorityInteractiveQueue = "tier-priority-interactive"
	tierPriorityAsyncQueue       = "tier-priority-async"
	tierPriorityResultQueue      = "tier-priority-result-list"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

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

func enqueueMessageWithRouting(ctx context.Context, rdb *redis.Client, queue string, msg api.RequestMessage, routing api.InternalRouting) {
	ir := api.NewInternalRequest(routing, &msg)
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

func makeRequestMessage(id string, deadlineOffset time.Duration) api.RequestMessage {
	deadline := time.Now().Add(deadlineOffset)
	return api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: deadline.Unix(),
		Payload:  map[string]any{"model": id, "prompt": "test"},
	}
}

// setSimWaitingRequests drives the vLLM simulator's waiting request count.
// EPP computes per-pod saturation as Max(WaitingQueue/QueueDepthThreshold, KVCache/KVCacheThreshold).
// The default QueueDepthThreshold is 5, so waitingRequests >= 5 saturates the pod.
func setSimWaitingRequests(simAdminURL string, waitingRequests int) {
	body, _ := json.Marshal(map[string]any{
		"waiting-requests": waitingRequests,
	})
	req, err := http.NewRequest(http.MethodPost, simAdminURL+"/fake_metrics", bytes.NewReader(body))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close() //nolint:errcheck
	gomega.ExpectWithOffset(1, resp.StatusCode).To(gomega.BeElementOf(http.StatusOK, http.StatusNoContent))
}

// sendProbeRequest sends a minimal inference request through Envoy → EPP to trigger
// the EPP's flow control admission controller, which records the saturation metric.
func sendProbeRequest(envoyURL string) {
	body := []byte(`{"model":"test-model","prompt":"probe"}`)
	req, err := http.NewRequest(http.MethodPost, envoyURL+"/v1/completions", bytes.NewReader(body))
	if err != nil {
		ginkgo.GinkgoLogr.V(1).Info("probe request creation failed", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		ginkgo.GinkgoLogr.V(1).Info("probe request failed", "error", err)
		return
	}
	resp.Body.Close() //nolint:errcheck
}

// queryProm runs a PromQL instant query and returns the first result's scalar
// value. Returns NaN if the query fails or returns no results; callers must
// check with math.IsNaN. Do NOT use a numeric sentinel like -1: budget PromQL
// (1 - queue_size/capacity) legitimately returns negative values when queue_size
// exceeds capacity.
func queryProm(promURL, query string) float64 {
	resp, err := httpClient.Get(promURL + "/api/v1/query?query=" + url.QueryEscape(query))
	if err != nil {
		ginkgo.GinkgoLogr.V(1).Info("prom query failed", "error", err, "query", query)
		return math.NaN()
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		ginkgo.GinkgoLogr.V(1).Info("prom query returned non-200", "status", resp.StatusCode, "query", query)
		return math.NaN()
	}

	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return math.NaN()
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return math.NaN()
	}
	v, err := strconv.ParseFloat(result.Data.Result[0].Value[1].(string), 64)
	if err != nil {
		return math.NaN()
	}
	return v
}

// budgetPromQL is the same PromQL the budget gate's primary source uses
// (EPP flow control queue_size). The multiplier (* 1) is max_concurrency and
// must match the budget processor's gate-params in async-processor.yaml.
// We use max_concurrency=1 so a single queued request in EPP's admission
// layer is enough to drive the budget to zero.
const budgetPromQL = `1 - (sum by(inference_pool)(inference_extension_flow_control_queue_size{inference_pool="e2e-pool"}) / on() (inference_pool_ready_pods{name="e2e-pool"} * 1))`

// waitForBudget polls Prometheus using the same PromQL the budget gate uses.
// It sends probe requests through Envoy to trigger EPP metric recording, then
// queries the primary budget PromQL. pred receives the raw budget value D
// (before baseline subtraction — the gate subtracts baseline internally).
func waitForBudget(promURL, envoyURL string, pred func(float64) bool) {
	gomega.EventuallyWithOffset(1, func() bool {
		sendProbeRequest(envoyURL)
		v := queryProm(promURL, budgetPromQL)
		return !math.IsNaN(v) && pred(v)
	}, 60*time.Second, 2*time.Second).Should(gomega.BeTrue(), "waiting for budget to satisfy condition")
}

// waitForSaturation polls Prometheus until pred is satisfied or the timeout elapses.
// It sends probe requests through Envoy on each poll to trigger EPP's flow control
// admission controller, which records the saturation metric.
func waitForSaturation(promURL, envoyURL string, pred func(float64) bool) {
	gomega.EventuallyWithOffset(1, func() bool {
		sendProbeRequest(envoyURL)
		v := queryProm(promURL, `inference_extension_flow_control_pool_saturation{inference_pool="e2e-pool"}`)
		return !math.IsNaN(v) && pred(v)
	}, 60*time.Second, 2*time.Second).Should(gomega.BeTrue(), "waiting for saturation to satisfy condition")
}

// floodProbes continuously sends concurrent inference requests through Envoy
// to fill EPP's flow control admission queue. When EPP is saturated, these
// requests queue up, driving inference_extension_flow_control_queue_size > 0.
// Uses a long HTTP timeout so throttled requests stay in EPP's queue long
// enough for Prometheus to scrape the non-zero queue_size.
// Call the returned cancel function to stop the flood.
// A single shared Transport with MaxConnsPerHost is used to cap the number of
// TCP connections and avoid exhausting ephemeral ports, which would break
// subsequent tests that need to connect to other services.
func floodProbes(envoyURL string, concurrency int) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	tr := &http.Transport{
		MaxConnsPerHost: concurrency,
	}
	client := &http.Client{
		Timeout:   120 * time.Second,
		Transport: tr,
	}
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					body := []byte(`{"model":"test-model","prompt":"probe"}`)
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, envoyURL+"/v1/completions", bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					resp, err := client.Do(req)
					if err == nil {
						resp.Body.Close() //nolint:errcheck
					}
				}
			}
		}()
	}
	return func() {
		cancel()
		tr.CloseIdleConnections()
	}
}

// setEnvoyFaultAbort configures Envoy's fault injection filter via the admin
// runtime API. percent is 0–100 (percentage of requests that return 503).
func setEnvoyFaultAbort(envoyAdminURL string, percent int) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		body := fmt.Sprintf("fault.http.abort.abort_percent=%d", percent)
		req, err := http.NewRequest(http.MethodPost, envoyAdminURL+"/runtime_modify?"+body, nil)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		resp, err := httpClient.Do(req)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck
		g.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())
}

// setEnvoyFaultDelay configures Envoy's fault injection delay via the admin
// runtime API. percent is 0–100 (percentage of requests delayed by 60s,
// the duration configured in the static Envoy config).
func setEnvoyFaultDelay(envoyAdminURL string, percent int) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		body := fmt.Sprintf("fault.http.delay.fixed_delay_percent=%d", percent)
		req, err := http.NewRequest(http.MethodPost, envoyAdminURL+"/runtime_modify?"+body, nil)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		resp, err := httpClient.Do(req)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck
		g.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())
}

func setDispatchGateBudget(ctx context.Context, rdb *redis.Client, budget string) {
	gomega.ExpectWithOffset(1, rdb.Set(ctx, dispatchGateBudgetKey, budget, 0).Err()).NotTo(gomega.HaveOccurred())
}

func clearDispatchGateBudget(ctx context.Context, rdb *redis.Client) {
	rdb.Del(ctx, dispatchGateBudgetKey) //nolint:errcheck
}
