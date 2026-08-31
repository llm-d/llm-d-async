package server

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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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

type fakeBacklogReporter struct {
	mu    sync.Mutex
	stats []pipeline.QueueBacklogStat
	err   error
}

func (f *fakeBacklogReporter) QueueBacklog(context.Context) ([]pipeline.QueueBacklogStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats, f.err
}

func (f *fakeBacklogReporter) setStats(stats []pipeline.QueueBacklogStat) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats = stats
}

func histogramSampleCount(c prometheus.Collector) uint64 {
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)
	var total uint64
	for m := range ch {
		var dto dto.Metric
		// nolint:errcheck
		m.Write(&dto)
		total += dto.GetHistogram().GetSampleCount()
	}
	return total
}

func TestPollBacklogObservesDeadlineViews(t *testing.T) {
	metrics.DeadlineProximity.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cumulative counts for buckets 0, 1s, ..., 24h (see
	// metrics.DeadlineProximityBucketLabels). Every poll replaces the
	// snapshot, so the final state is deterministic regardless of tick count.
	labels := metrics.DeadlineProximityBucketLabels()
	cumulative := []int64{1, 2, 2, 2, 3, 3, 3, 4, 5, 6, 7, 7, 8, 9}
	counts := make([]pipeline.ExpiringCount, 0, len(labels))
	for i, l := range labels {
		counts = append(counts, pipeline.ExpiringCount{Window: l, Count: cumulative[i]})
	}
	reporter := &fakeBacklogReporter{stats: []pipeline.QueueBacklogStat{{
		QueueID:        "q1",
		QueueName:      "queue-1",
		PoolName:       "pool-a",
		Depth:          2,
		ExpiringCounts: counts,
	}}}
	go pollBacklog(ctx, reporter, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	if got := histogramSampleCount(metrics.DeadlineProximity); got != 9 {
		t.Fatalf("DeadlineProximity sample count = %d, want 9", got)
	}
	h := histogramFor(t, metrics.DeadlineProximity, map[string]string{
		"queue_id": "q1", "queue_name": "queue-1", "pool_name": "pool-a",
	})
	if got := h.GetSampleSum(); got != 72983000 {
		t.Errorf("sample sum = %v, want 72983000", got)
	}
	if len(h.GetBucket()) != 14 {
		t.Errorf("bucket count = %d, want 14", len(h.GetBucket()))
	}
}

func TestPollBacklogSkipsNilDeadlineViews(t *testing.T) {
	metrics.DeadlineProximity.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := &fakeBacklogReporter{stats: []pipeline.QueueBacklogStat{{
		QueueID:   "q1",
		QueueName: "queue-1",
		PoolName:  "pool-a",
		Depth:     5,
	}}}
	go pollBacklog(ctx, reporter, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	if got := histogramSampleCount(metrics.DeadlineProximity); got != 0 {
		t.Fatalf("DeadlineProximity sample count = %d, want 0", got)
	}
}

func TestPollBacklogRemovesDeletedQueueSnapshots(t *testing.T) {
	metrics.BrokerBacklog.Reset()
	metrics.BrokerBacklogSourceAvailable.Reset()
	metrics.DispatchBudget.Reset()
	metrics.DeadlineProximity.Reset()
	labels := pipeline.QueueBacklogStat{QueueID: "gone", QueueName: "queue-gone", PoolName: "pool-a", Depth: 3,
		ExpiringCounts: make([]pipeline.ExpiringCount, len(metrics.DeadlineProximityBuckets()))}
	reporter := &fakeBacklogReporter{stats: []pipeline.QueueBacklogStat{labels}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pollBacklog(ctx, reporter, 10*time.Millisecond)
	waitFor(t, "initial backlog snapshot", func() bool {
		return testutil.CollectAndCount(metrics.BrokerBacklog) == 1
	})
	reporter.setStats(nil)
	waitFor(t, "deleted backlog snapshot cleanup", func() bool {
		return testutil.CollectAndCount(metrics.BrokerBacklog) == 0
	})

	if got := testutil.CollectAndCount(metrics.BrokerBacklog); got != 0 {
		t.Fatalf("deleted queue backlog still exposes %d series", got)
	}
	if got := testutil.CollectAndCount(metrics.BrokerBacklogSourceAvailable); got != 0 {
		t.Fatalf("deleted queue backlog availability still exposes %d series", got)
	}
	if got := testutil.CollectAndCount(metrics.DeadlineProximity); got != 0 {
		t.Fatalf("deleted queue deadline snapshot still exposes %d series", got)
	}
}

// histogramFor gathers a collector and returns the histogram whose labels
// match wantLabels.
func histogramFor(t *testing.T, c prometheus.Collector, wantLabels map[string]string) *dto.Histogram {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)
	for m := range ch {
		var dto dto.Metric
		// nolint:errcheck
		m.Write(&dto)
		matches := true
		for _, lp := range dto.GetLabel() {
			if want, ok := wantLabels[lp.GetName()]; !ok || lp.GetValue() != want {
				matches = false
				break
			}
		}
		if matches {
			return dto.GetHistogram()
		}
	}
	t.Fatalf("no histogram matched labels %v", wantLabels)
	return nil
}
