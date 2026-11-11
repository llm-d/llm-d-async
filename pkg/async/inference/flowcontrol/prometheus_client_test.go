/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flowcontrol

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/stretchr/testify/require"
)

// newAuthenticatedPrometheus serves a Prometheus instant-query response of
// value 1 over TLS, and only to requests carrying the current bearer token.
func newAuthenticatedPrometheus(t *testing.T, token *atomic.Value) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token.Load().(string) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}`))
	}))
	t.Cleanup(srv.Close)

	caFile := filepath.Join(t.TempDir(), "ca.crt")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))
	return srv, caFile
}

func newQueryGate(t *testing.T, factory *GateFactory) pipeline.Gate {
	t.Helper()
	gate, err := factory.CreateGate(pipeline.GateConfig{
		GateType:   "prometheus-query",
		GateParams: map[string]any{"query": "up"},
	})
	require.NoError(t, err)
	return gate
}

func queryGateBudget(t *testing.T, factory *GateFactory) float64 {
	t.Helper()
	return newQueryGate(t, factory).Budget(context.Background())
}

func TestPrometheusGateAuthenticatesWithCAAndRotatedToken(t *testing.T) {
	var token atomic.Value
	token.Store("first-token")
	srv, caFile := newAuthenticatedPrometheus(t, &token)

	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("first-token\n"), 0o600))

	rt, err := NewPrometheusRoundTripper(caFile, tokenFile)
	require.NoError(t, err)
	gate := newQueryGate(t, NewGateFactoryWithCacheTTL(srv.URL, 0).WithPrometheusTransport(rt))

	require.Equal(t, 1.0, gate.Budget(context.Background()), "gate should read the metric through TLS with the bearer token")

	// The server now only accepts a new token; the same gate must pick it up
	// from the file, as with a rotated service account token.
	token.Store("second-token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("second-token"), 0o600))
	require.Equal(t, 1.0, gate.Budget(context.Background()), "gate should send the rotated token")
}

func TestPrometheusGateFailsClosedWithoutCredentials(t *testing.T) {
	var token atomic.Value
	token.Store("required-token")
	srv, caFile := newAuthenticatedPrometheus(t, &token)

	t.Run("no transport: server certificate is not trusted", func(t *testing.T) {
		factory := NewGateFactoryWithCacheTTL(srv.URL, 0)
		require.Equal(t, 0.0, queryGateBudget(t, factory))
	})

	t.Run("CA only: request is unauthorized", func(t *testing.T) {
		rt, err := NewPrometheusRoundTripper(caFile, "")
		require.NoError(t, err)
		factory := NewGateFactoryWithCacheTTL(srv.URL, 0).WithPrometheusTransport(rt)
		require.Equal(t, 0.0, queryGateBudget(t, factory))
	})
}

func TestNewPrometheusRoundTripper(t *testing.T) {
	t.Run("no files keeps the default transport", func(t *testing.T) {
		rt, err := NewPrometheusRoundTripper("", "")
		require.NoError(t, err)
		require.Nil(t, rt)
	})

	t.Run("missing CA file is rejected at startup", func(t *testing.T) {
		_, err := NewPrometheusRoundTripper(filepath.Join(t.TempDir(), "absent.crt"), "")
		require.Error(t, err)
	})

	t.Run("missing bearer token file is rejected at startup", func(t *testing.T) {
		_, err := NewPrometheusRoundTripper("", filepath.Join(t.TempDir(), "absent-token"))
		require.Error(t, err)
	})
}
