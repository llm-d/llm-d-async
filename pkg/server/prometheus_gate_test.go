package server

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

// A CA certificate alone cannot authenticate GMP requests, so the startup
// path fails closed instead of running gates that always HTTP 401 and fall
// back.
func TestNewGateFactoryFailsOnCACertOnlyGMP(t *testing.T) {
	gmpURL := "https://monitoring.googleapis.com/v1/projects/p/location/global/prometheus"
	// A real PEM so the transport construction accepts the file.
	pemSrv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pemSrv.Certificate().Raw})
	pemSrv.Close()
	writeTempCert := func(name string) string {
		t.Helper()
		f, err := os.CreateTemp(t.TempDir(), name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(caPEM); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		return f.Name()
	}

	cases := []struct {
		name       string
		url        string
		caCertFile bool
		tokenFile  bool
		wantErr    bool
	}{
		{
			name:       "ca cert only on GMP fails",
			url:        gmpURL,
			caCertFile: true,
			wantErr:    true,
		},
		{
			name:       "ca cert with bearer token succeeds",
			url:        gmpURL,
			caCertFile: true,
			tokenFile:  true,
		},
		{
			name:       "ca cert only on a non-GMP URL succeeds",
			url:        "https://prom.example.com/query",
			caCertFile: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := PrometheusConfig{URL: tc.url}
			if tc.caCertFile {
				cfg.CACertFile = writeTempCert("ca")
			}
			if tc.tokenFile {
				cfg.BearerTokenFile = writeTempCert("token")
			}
			factory, err := newGateFactory(cfg, logr.Discard())
			if tc.wantErr {
				if err == nil {
					t.Fatal("newGateFactory succeeded, want error (CA cert alone cannot authenticate GMP)")
				}
				if !strings.Contains(err.Error(), "does not authenticate to Google Managed Prometheus") ||
					!strings.Contains(err.Error(), "prometheus-bearer-token-file") {
					t.Fatalf("error = %q, want it to name the misconfiguration and the bearer-token-file remedy", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("newGateFactory: %v", err)
			}
			if factory == nil {
				t.Fatal("factory is nil, want a working GateFactory")
			}
		})
	}
}
