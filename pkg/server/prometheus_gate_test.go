package server

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/pipeline"
)

// The Prometheus options must reach the gates: a gate built from them has to
// read a metric from a TLS server that requires the configured bearer token.
func TestNewGateFactoryAppliesPrometheusAuth(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sa-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	tokenFile := filepath.Join(dir, "token")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenFile, []byte("sa-token"), 0o600); err != nil {
		t.Fatal(err)
	}

	factory, err := newGateFactory(PrometheusConfig{URL: srv.URL, CACertFile: caFile, BearerTokenFile: tokenFile}, logr.Discard())
	if err != nil {
		t.Fatalf("newGateFactory: %v", err)
	}
	gate, err := factory.CreateGate(pipeline.GateConfig{
		GateType:   "prometheus-query",
		GateParams: map[string]any{"query": "up"},
	})
	if err != nil {
		t.Fatalf("CreateGate: %v", err)
	}
	if got := gate.Budget(context.Background()); got != 1.0 {
		t.Fatalf("budget = %v, want 1 (query must carry the CA and bearer token from the options)", got)
	}
}
