package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func generateTestCert(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	caPath = filepath.Join(dir, "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	if err := os.WriteFile(caPath, caPEM, 0600); err != nil {
		t.Fatalf("write CA cert: %v", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Test Client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}

	certPath = filepath.Join(dir, "client.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	if err := os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatalf("write client cert: %v", err)
	}

	keyPath = filepath.Join(dir, "client-key.pem")
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatalf("write client key: %v", err)
	}

	return caPath, certPath, keyPath
}

func TestBuildTLSConfig_noFlags(t *testing.T) {
	cfg, err := buildTLSConfig(TLSConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config when no TLS flags are set")
	}
}

func TestBuildTLSConfig_mismatchedCertKey(t *testing.T) {
	tests := []struct {
		name string
		cert string
		key  string
	}{
		{"cert without key", "/some/cert.pem", ""},
		{"key without cert", "", "/some/key.pem"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildTLSConfig(TLSConfig{Cert: tt.cert, Key: tt.key})
			if err == nil {
				t.Fatal("expected error for mismatched cert/key")
			}
		})
	}
}

func TestBuildTLSConfig_caCertOnly(t *testing.T) {
	dir := t.TempDir()
	caPath, _, _ := generateTestCert(t, dir)

	cfg, err := buildTLSConfig(TLSConfig{CACert: caPath})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs to be populated")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %d, want %d", cfg.MinVersion, tls.VersionTLS12)
	}
	if len(cfg.Certificates) != 0 {
		t.Errorf("expected no client certificates, got %d", len(cfg.Certificates))
	}
}

func TestBuildTLSConfig_mTLS(t *testing.T) {
	dir := t.TempDir()
	_, certPath, keyPath := generateTestCert(t, dir)

	cfg, err := buildTLSConfig(TLSConfig{Cert: certPath, Key: keyPath})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(cfg.Certificates))
	}
}

func TestBuildTLSConfig_caPlusMTLS(t *testing.T) {
	dir := t.TempDir()
	caPath, certPath, keyPath := generateTestCert(t, dir)

	cfg, err := buildTLSConfig(TLSConfig{CACert: caPath, Cert: certPath, Key: keyPath})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs to be populated")
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(cfg.Certificates))
	}
}

func TestBuildTLSConfig_insecureSkipVerify(t *testing.T) {
	cfg, err := buildTLSConfig(TLSConfig{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be true")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %d, want %d", cfg.MinVersion, tls.VersionTLS12)
	}
}

func TestBuildTLSConfig_caCertFileNotFound(t *testing.T) {
	_, err := buildTLSConfig(TLSConfig{CACert: "/nonexistent/ca.pem"})
	if err == nil {
		t.Fatal("expected error for missing CA cert file")
	}
}

func TestBuildTLSConfig_malformedCACert(t *testing.T) {
	dir := t.TempDir()
	badCA := filepath.Join(dir, "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("not a valid PEM"), 0600); err != nil {
		t.Fatalf("write bad CA: %v", err)
	}

	_, err := buildTLSConfig(TLSConfig{CACert: badCA})
	if err == nil {
		t.Fatal("expected error for malformed CA cert")
	}
}

func TestBuildTLSConfig_invalidCertKeyPair(t *testing.T) {
	dir := t.TempDir()

	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, []byte("not a cert"), 0600); err != nil {
		t.Fatalf("write bad cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("not a key"), 0600); err != nil {
		t.Fatalf("write bad key: %v", err)
	}

	_, err := buildTLSConfig(TLSConfig{Cert: certPath, Key: keyPath})
	if err == nil {
		t.Fatal("expected error for invalid cert/key pair")
	}
}
