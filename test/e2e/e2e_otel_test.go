package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/llm-d/llm-d-async/api"
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

		// Poll Jaeger for the specific trace ID instead of a fixed sleep
		jaegerQueryURL := fmt.Sprintf("%s/api/traces/%s", jaegerURL, knownTraceID)
		var spanNames []string
		gomega.Eventually(func(g gomega.Gomega) {
			resp, err := jaegerClient.Get(jaegerQueryURL)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			defer resp.Body.Close() //nolint:errcheck

			body, err := io.ReadAll(resp.Body)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

			var result struct {
				Data []struct {
					TraceID string `json:"traceID"`
					Spans   []struct {
						OperationName string `json:"operationName"`
					} `json:"spans"`
				} `json:"data"`
			}
			g.Expect(json.Unmarshal(body, &result)).To(gomega.Succeed())
			g.Expect(result.Data).NotTo(gomega.BeEmpty(),
				"trace %s not yet indexed in Jaeger", knownTraceID)

			spanNames = nil
			for _, span := range result.Data[0].Spans {
				spanNames = append(spanNames, span.OperationName)
			}
			g.Expect(spanNames).To(gomega.ContainElement("process-request"))
		}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())

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

		msg := makeRequestMessage("otel-no-context-test", 2*time.Minute)
		enqueueMessage(ctx, rdb, integrationRequestQueue, msg)

		gomega.Eventually(func() int64 {
			return getResultCount(ctx, rdb, integrationResultQueue)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		popResult(ctx, rdb, integrationResultQueue)

		// Poll Jaeger for traces from async-processor instead of a fixed sleep
		jaegerQueryURL := jaegerURL + "/api/traces?service=async-processor&limit=5&lookback=1m"
		gomega.Eventually(func(g gomega.Gomega) {
			resp, err := jaegerClient.Get(jaegerQueryURL)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			defer resp.Body.Close() //nolint:errcheck

			body, err := io.ReadAll(resp.Body)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

			var result struct {
				Data []json.RawMessage `json:"data"`
			}
			g.Expect(json.Unmarshal(body, &result)).To(gomega.Succeed())
			g.Expect(result.Data).NotTo(gomega.BeEmpty(),
				"no traces from async-processor in Jaeger yet")
		}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())

		ginkgo.GinkgoLogr.Info("OTel export verified (no producer context)")
	})
})
