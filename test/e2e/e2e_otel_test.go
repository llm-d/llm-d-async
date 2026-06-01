package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("OpenTelemetry tracing", ginkgo.Ordered, func() {
	ginkgo.It("propagates producer trace context through the async pipeline", func() {
		ctx := context.Background()

		jaegerClient := &http.Client{Timeout: 5 * time.Second}
		checkResp, err := jaegerClient.Get(jaegerURL + "/")
		if err != nil {
			ginkgo.Skip("Jaeger not reachable at " + jaegerURL + ", skipping OTel trace verification")
		}
		checkResp.Body.Close() //nolint:errcheck

		// Simulate a producer injecting W3C trace context into request metadata.
		// This is what a real producer (e.g. batch-gateway) would do.
		knownTraceID := "a01b2c3d4e5f6a7b8c9d0e1f2a3b4c5d"
		traceparent := fmt.Sprintf("00-%s-1234567890abcdef-01", knownTraceID)

		msg := api.RequestMessage{
			ID:       "otel-propagation-test",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(2 * time.Minute).Unix(),
			Payload:  map[string]any{"model": "otel-propagation-test", "prompt": "test"},
			Metadata: map[string]string{
				"traceparent": traceparent,
			},
		}
		enqueueMessage(ctx, rdb, integrationRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		popResult(ctx, rdb, integrationResultQueue)

		// Give Jaeger time to index
		time.Sleep(3 * time.Second)

		// Query Jaeger for the specific trace ID we injected
		jaegerQueryURL := fmt.Sprintf("%s/api/traces/%s", jaegerURL, knownTraceID)
		resp, err := jaegerClient.Get(jaegerQueryURL)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck

		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Jaeger API returned %d: %s", resp.StatusCode, string(body))

		var result struct {
			Data []struct {
				TraceID string `json:"traceID"`
				Spans   []struct {
					OperationName string `json:"operationName"`
				} `json:"spans"`
			} `json:"data"`
		}
		gomega.Expect(json.Unmarshal(body, &result)).To(gomega.Succeed())
		gomega.Expect(result.Data).NotTo(gomega.BeEmpty(),
			"expected trace with ID %s in Jaeger — producer context was not propagated", knownTraceID)

		// Verify the process-request span exists in this trace
		var spanNames []string
		for _, span := range result.Data[0].Spans {
			spanNames = append(spanNames, span.OperationName)
		}
		gomega.Expect(spanNames).To(gomega.ContainElement("process-request"),
			"expected 'process-request' span in trace, got: %v", spanNames)

		ginkgo.GinkgoLogr.Info("Trace context propagation verified",
			"traceID", knownTraceID, "spans", spanNames)
	})

	ginkgo.It("exports traces to Jaeger after processing a message without producer context", func() {
		ctx := context.Background()

		jaegerClient := &http.Client{Timeout: 5 * time.Second}
		checkResp, err := jaegerClient.Get(jaegerURL + "/")
		if err != nil {
			ginkgo.Skip("Jaeger not reachable at " + jaegerURL + ", skipping OTel trace verification")
		}
		checkResp.Body.Close() //nolint:errcheck

		// Enqueue without metadata — processor should create its own root span
		msg := makeRequestMessage("otel-no-context-test", 2*time.Minute)
		enqueueMessage(ctx, rdb, integrationRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		popResult(ctx, rdb, integrationResultQueue)

		time.Sleep(3 * time.Second)

		// Query for any traces from the async-processor service
		jaegerQueryURL := jaegerURL + "/api/traces?service=async-processor&limit=5&lookback=1m"
		resp, err := jaegerClient.Get(jaegerQueryURL)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close() //nolint:errcheck

		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

		var result struct {
			Data []json.RawMessage `json:"data"`
		}
		gomega.Expect(json.Unmarshal(body, &result)).To(gomega.Succeed())
		gomega.Expect(result.Data).NotTo(gomega.BeEmpty(),
			"expected at least 1 trace from async-processor — OTel export is not working")

		ginkgo.GinkgoLogr.Info("OTel export verified (no producer context)",
			"traceCount", len(result.Data))
	})
})
