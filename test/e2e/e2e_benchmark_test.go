package e2e

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/llm-d/llm-d-async/api"
)

var _ = ginkgo.Describe("Async Processor Performance Benchmark E2E", ginkgo.Ordered, func() {
	var ctx context.Context

	ginkgo.BeforeAll(func() {
		redeployEPPWithFlowControl()

		// Patch the simulator to simulate realistic TTFT & ITL
		// TTFT: 10ms, ITL: 1ms, Max concurrency: 32.
		// Also increase max waiting queue length so EPP doesn't reject requests.
		ginkgo.By("Patching llm-sim to simulate realistic latencies")
		patchSimArgs([]string{
			"--model", "test-model",
			"--port", "8000",
			"--time-to-first-token", "10ms",
			"--inter-token-latency", "1ms",
			"--max-num-seqs", "32",
			"--max-waiting-queue-length", "2000",
			"--max-model-len", "4096",
			"--v", "4",
		})
	})

	ginkgo.AfterAll(func() {
		// Restore the default simulator setup (which uses fake metrics and responds instantly)
		ginkgo.By("Restoring default llm-sim configuration")
		patchSimArgs([]string{
			"--model", "test-model",
			"--port", "8000",
			"--fake-metrics", `{"kv-cache-usage": 0, "waiting-requests": 0, "running-requests": 0}`,
		})
	})

	ginkgo.BeforeEach(func() {
		ctx = context.Background()

		// Clean the benchmark queues
		rdb.Del(ctx, benchmarkRequestQueue)         //nolint:errcheck
		rdb.Del(ctx, benchmarkResultQueue)          //nolint:errcheck
		rdb.Del(ctx, benchmarkPoolGateRequestQueue) //nolint:errcheck
		rdb.Del(ctx, benchmarkPoolGateResultQueue)  //nolint:errcheck
	})

	ginkgo.It("measure clearing time and saturation under load with queue-level gate", func() {
		// Build a prompt containing 1000 tokens by repeating a word 1000 times
		prompt := strings.Repeat("word ", 1000)

		numRequests := 5000
		ginkgo.By(fmt.Sprintf("Bulk enqueuing %d inference requests (1000 input tokens, 500 output tokens)", numRequests))

		msgs := make([]api.RequestMessage, numRequests)
		now := time.Now().Unix()
		for i := 0; i < numRequests; i++ {
			msgs[i] = api.RequestMessage{
				ID:       fmt.Sprintf("bench-msg-%d", i),
				Created:  now,
				Deadline: now + 600, // 10 minutes deadline
				Payload: map[string]any{
					"model":      "test-model",
					"prompt":     prompt,
					"max_tokens": 500,
				},
			}
		}

		// Enqueue all messages atomically via pipeline
		enqueueMessages(ctx, rdb, benchmarkRequestQueue, msgs...)

		ginkgo.By("Starting performance monitoring loop")
		var saturationMetrics []float64
		monitorCtx, monitorCancel := context.WithCancel(ctx)
		var monitorWg sync.WaitGroup
		monitorWg.Add(1)

		// Start a goroutine to poll Prometheus for saturation metrics during the benchmark run
		go func() {
			defer monitorWg.Done()
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-monitorCtx.Done():
					return
				case <-ticker.C:
					// Send probe requests to drive flow control admission logic in EPP
					sendProbeRequest(envoyURL)
					satVal := queryProm(promURL, `inference_extension_flow_control_pool_saturation{inference_pool="e2e-pool"}`)
					if !math.IsNaN(satVal) {
						saturationMetrics = append(saturationMetrics, satVal)
					}
				}
			}
		}()

		startTime := time.Now()

		ginkgo.By("Waiting for all messages to be processed")
		// Wait for the result queue length to match the enqueued requests count
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, benchmarkResultQueue)
		}, 10*time.Minute, 5*time.Second).Should(gomega.Equal(int64(numRequests)))

		elapsedTime := time.Since(startTime)
		monitorCancel()
		monitorWg.Wait()

		// Calculate average saturation
		var avgSaturation float64
		if len(saturationMetrics) > 0 {
			var sum float64
			for _, v := range saturationMetrics {
				sum += v
			}
			avgSaturation = sum / float64(len(saturationMetrics))
		}

		ginkgo.GinkgoWriter.Printf("\n================ BENCHMARK RESULTS ================\n")
		ginkgo.GinkgoWriter.Printf("Total Requests: %d\n", numRequests)
		ginkgo.GinkgoWriter.Printf("Total Queue Clearing Time: %s\n", elapsedTime)
		ginkgo.GinkgoWriter.Printf("Average Saturation Level: %.4f\n", avgSaturation)
		ginkgo.GinkgoWriter.Printf("Saturation Metric Points Collected: %v\n", saturationMetrics)
		ginkgo.GinkgoWriter.Printf("===================================================\n\n")

		// Verify basic correctness: result queue contains all processed messages
		gomega.Expect(getResultCount(ctx, rdb, benchmarkResultQueue)).To(gomega.Equal(int64(numRequests)))
	})

	ginkgo.It("measure clearing time and saturation under load with pool-level gate", func() {
		// Build a prompt containing 1000 tokens by repeating a word 1000 times
		prompt := strings.Repeat("word ", 1000)

		numRequests := 5000
		ginkgo.By(fmt.Sprintf("Bulk enqueuing %d inference requests (1000 input tokens, 500 output tokens) to pool gate queue", numRequests))

		msgs := make([]api.RequestMessage, numRequests)
		now := time.Now().Unix()
		for i := 0; i < numRequests; i++ {
			msgs[i] = api.RequestMessage{
				ID:       fmt.Sprintf("bench-pool-msg-%d", i),
				Created:  now,
				Deadline: now + 600, // 10 minutes deadline
				Payload: map[string]any{
					"model":      "test-model",
					"prompt":     prompt,
					"max_tokens": 500,
				},
			}
		}

		// Enqueue all messages atomically via pipeline
		enqueueMessages(ctx, rdb, benchmarkPoolGateRequestQueue, msgs...)

		ginkgo.By("Starting performance monitoring loop for worker pool gate")
		var saturationMetrics []float64
		monitorCtx, monitorCancel := context.WithCancel(ctx)
		var monitorWg sync.WaitGroup
		monitorWg.Add(1)

		// Start a goroutine to poll Prometheus for saturation metrics during the benchmark run
		go func() {
			defer monitorWg.Done()
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-monitorCtx.Done():
					return
				case <-ticker.C:
					// Send probe requests to drive flow control admission logic in EPP
					sendProbeRequest(envoyURL)
					satVal := queryProm(promURL, `inference_extension_flow_control_pool_saturation{inference_pool="e2e-pool"}`)
					if !math.IsNaN(satVal) {
						saturationMetrics = append(saturationMetrics, satVal)
					}
				}
			}
		}()

		startTime := time.Now()

		ginkgo.By("Waiting for all messages to be processed by worker pool gate")
		// Wait for the result queue length to match the enqueued requests count
		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, benchmarkPoolGateResultQueue)
		}, 10*time.Minute, 5*time.Second).Should(gomega.Equal(int64(numRequests)))

		elapsedTime := time.Since(startTime)
		monitorCancel()
		monitorWg.Wait()

		// Calculate average saturation
		var avgSaturation float64
		if len(saturationMetrics) > 0 {
			var sum float64
			for _, v := range saturationMetrics {
				sum += v
			}
			avgSaturation = sum / float64(len(saturationMetrics))
		}

		ginkgo.GinkgoWriter.Printf("\n================ WORKER POOL GATE BENCHMARK RESULTS ================\n")
		ginkgo.GinkgoWriter.Printf("Total Requests: %d\n", numRequests)
		ginkgo.GinkgoWriter.Printf("Total Queue Clearing Time: %s\n", elapsedTime)
		ginkgo.GinkgoWriter.Printf("Average Saturation Level: %.4f\n", avgSaturation)
		ginkgo.GinkgoWriter.Printf("Saturation Metric Points Collected: %v\n", saturationMetrics)
		ginkgo.GinkgoWriter.Printf("====================================================================\n\n")

		// Verify basic correctness: result queue contains all processed messages
		gomega.Expect(getResultCount(ctx, rdb, benchmarkPoolGateResultQueue)).To(gomega.Equal(int64(numRequests)))
	})
})

func patchSimArgs(args []string) {
	// Construct the patch JSON
	// Since json.Marshal on string slices can be tricky inside a raw patch, we build it carefully.
	// E.g. {"spec":{"template":{"spec":{"containers":[{"name":"sim","args":["--model","test-model",...]}]}}}}
	var argsQuoted []string
	for _, arg := range args {
		// Escape quotes in args (specifically for the fake-metrics JSON string)
		escaped := strings.ReplaceAll(arg, `"`, `\"`)
		argsQuoted = append(argsQuoted, fmt.Sprintf(`"%s"`, escaped))
	}
	patchStr := fmt.Sprintf(`{"spec":{"template":{"spec":{"containers":[{"name":"sim","args":[%s]}]}}}}`, strings.Join(argsQuoted, ","))

	cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "patch", "deployment", "llm-sim",
		"--patch", patchStr)
	session, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(30 * time.Second).Should(gexec.Exit(0))

	// Wait for the rollout to complete
	cmd = exec.Command("kubectl", "--kubeconfig", kindKubeconfig,
		"-n", nsName, "rollout", "status", "deployment/llm-sim", "--timeout=120s")
	session, err = gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(120 * time.Second).Should(gexec.Exit(0))
}
