package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"k8s.io/client-go/rest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const (
	// openMetricsAccept is the Accept header Prometheus sends with its default
	// scrape_protocols, OpenMetrics first.
	openMetricsAccept = "application/openmetrics-text;version=1.0.0,text/plain;version=0.0.4;q=0.5"
	// classicAccept is what a scraper that doesn't understand OpenMetrics sends.
	classicAccept = "text/plain;version=0.0.4"
)

// The inference latency exemplar reaches a scrape of the metrics server
// startMetricsServer builds when the scrape asks for OpenMetrics; a classic
// scrape is unchanged.
func TestMetricsServerServesTraceExemplars(t *testing.T) {
	metrics.Register(metrics.GetAsyncProcessorCollectors(false)...)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("openmetrics-test").Start(context.Background(), "process-request")
	metrics.RecordInferenceLatency(ctx, 420, "openmetrics-qid", "openmetrics-queue", "openmetrics-pool")
	span.End()

	// The options startMetricsServer uses, with auth off and a loopback port.
	opts := metricsServerOptions(ServerConfig{MetricsEndpointAuth: false})
	opts.BindAddress = "127.0.0.1:0"
	srv, err := metricsserver.NewServer(opts, nil, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srvCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Start(srvCtx) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("metrics server: %v", err)
		}
	})
	url := "http://" + waitForBindAddr(t, srv) + metricsPath

	contentType, body := scrape(t, url, openMetricsAccept)
	if !strings.HasPrefix(contentType, "application/openmetrics-text") {
		t.Fatalf("Content-Type = %q, want OpenMetrics for a scraper that asks for it", contentType)
	}
	line := exemplarLine(body, `queue_id="openmetrics-qid"`)
	if line == "" {
		t.Fatal("no inference latency bucket carries an exemplar on the wire")
	}
	for _, want := range []string{
		`trace_id="` + span.SpanContext().TraceID().String() + `"`,
		`span_id="` + span.SpanContext().SpanID().String() + `"`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("exemplar line %q has no %s", line, want)
		}
	}

	contentType, body = scrape(t, url, classicAccept)
	if !strings.HasPrefix(contentType, "text/plain") {
		t.Errorf("Content-Type = %q, want the classic format for a classic scraper", contentType)
	}
	if !strings.Contains(body, `queue_id="openmetrics-qid"`) {
		t.Error("the classic scrape lost the inference latency series")
	}
	if strings.Contains(body, "# {") {
		t.Error("the classic format has no exemplars, but the scrape carries one")
	}
}

// controller-runtime runs the filter over every handler it mounts; only
// /metrics is served by the OpenMetrics handler.
func TestOpenMetricsFilterLeavesOtherPathsAlone(t *testing.T) {
	filter, err := openMetricsFilterProvider(false)(nil, nil)
	if err != nil {
		t.Fatalf("filter provider: %v", err)
	}
	handler, err := filter(logr.Discard(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "built by controller-runtime")
	}))
	if err != nil {
		t.Fatalf("filter: %v", err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/cmdline", nil))
	if got := rec.Body.String(); got != "built by controller-runtime" {
		t.Errorf("another path served %q, want the handler controller-runtime built", got)
	}
}

// With --metrics-endpoint-auth, the OpenMetrics handler stays behind
// authentication: a scrape without a token is refused before any metric is
// written. No API server is needed for that refusal.
func TestOpenMetricsFilterKeepsAuth(t *testing.T) {
	opts := metricsServerOptions(ServerConfig{MetricsEndpointAuth: true})
	filter, err := opts.FilterProvider(&rest.Config{Host: "https://127.0.0.1:1"}, http.DefaultClient)
	if err != nil {
		t.Fatalf("filter provider: %v", err)
	}
	handler, err := filter(logr.Discard(), http.NotFoundHandler())
	if err != nil {
		t.Fatalf("filter: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, metricsPath, nil)
	req.Header.Set("Accept", openMetricsAccept)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d for a scrape without a token", rec.Code, http.StatusUnauthorized)
	}
	if strings.Contains(rec.Body.String(), "# HELP") {
		t.Error("an unauthenticated scrape received metrics")
	}
}

// waitForBindAddr returns the address the metrics server listens on, which is
// only known once Start has created its listener.
func waitForBindAddr(t *testing.T, srv metricsserver.Server) string {
	t.Helper()
	withAddr, ok := srv.(interface{ GetBindAddr() string })
	if !ok {
		t.Fatal("the metrics server does not expose its bind address")
	}
	deadline := time.Now().Add(10 * time.Second)
	for withAddr.GetBindAddr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("the metrics server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return withAddr.GetBindAddr()
}

// scrape returns the Content-Type and body served at url to a scraper sending
// accept.
func scrape(t *testing.T, url, accept string) (string, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Accept", accept)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the scrape: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d: %s", url, resp.StatusCode, body)
	}
	return resp.Header.Get("Content-Type"), string(body)
}

// exemplarLine returns the first line of an OpenMetrics payload that contains
// series and carries an exemplar.
func exemplarLine(body, series string) string {
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, series) && strings.Contains(line, " # {") {
			return line
		}
	}
	return ""
}
