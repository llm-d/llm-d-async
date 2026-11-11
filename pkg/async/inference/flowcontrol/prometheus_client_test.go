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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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

// selfSignedServerCert returns a serving certificate for localhost that is its
// own CA, and that certificate PEM-encoded.
func selfSignedServerCert(t *testing.T, commonName string) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key},
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestPrometheusGateReloadsRotatedCA(t *testing.T) {
	oldCert, oldCA := selfSignedServerCert(t, "old-ca")
	newCert, newCA := selfSignedServerCert(t, "new-ca")

	var serving atomic.Pointer[tls.Certificate]
	serving.Store(&oldCert)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}`))
	}))
	srv.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return serving.Load(), nil
		},
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	// Address the server by name: without SNI it would serve httptest's own
	// certificate instead of calling GetCertificate.
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)
	url := "https://" + net.JoinHostPort("localhost", port)

	caFile := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(caFile, oldCA, 0o600))
	rt, err := NewPrometheusRoundTripper(caFile, "")
	require.NoError(t, err)
	gate := newQueryGate(t, NewGateFactoryWithCacheTTL(url, 0).WithPrometheusTransport(rt))
	require.Equal(t, 1.0, gate.Budget(context.Background()), "gate should trust the server through the CA file")

	// The server starts serving a certificate from a new CA. Dropping the open
	// connections forces the next query to handshake again.
	serving.Store(&newCert)
	srv.CloseClientConnections()
	require.Equal(t, 0.0, gate.Budget(context.Background()), "the old CA must not verify the new certificate")

	// Rotating the CA file is enough: the same gate picks it up.
	require.NoError(t, os.WriteFile(caFile, newCA, 0o600))
	require.Equal(t, 1.0, gate.Budget(context.Background()), "gate should reload the rotated CA file")
}
