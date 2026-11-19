package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

func healthyChecker(_ context.Context) error { return nil }

func unhealthyChecker(_ context.Context) error { return errors.New("backend unavailable") }

func slowChecker(ctx context.Context) error {
	select {
	case <-time.After(10 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func parseResponse(t *testing.T, rec *httptest.ResponseRecorder) response {
	t.Helper()
	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	return resp
}

func TestHealthz_AlwaysOK(t *testing.T) {
	s := NewServer(0, unhealthyChecker, logr.Discard())
	rec := httptest.NewRecorder()
	s.handleHealthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 regardless of checker, got %d", rec.Code)
	}
	resp := parseResponse(t, rec)
	if resp.Status != "ok" {
		t.Fatalf("expected status ok, got %s", resp.Status)
	}
}

func TestReadyz_NotReady(t *testing.T) {
	s := NewServer(0, nil, logr.Discard())
	rec := httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	resp := parseResponse(t, rec)
	if resp.Status != "not ready" {
		t.Fatalf("expected status 'not ready', got %s", resp.Status)
	}
}

func TestReadyz_Ready_NoChecker(t *testing.T) {
	s := NewServer(0, nil, logr.Discard())
	s.SetReady()
	rec := httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	resp := parseResponse(t, rec)
	if resp.Status != "ready" {
		t.Fatalf("expected status ready, got %s", resp.Status)
	}
}

func TestReadyz_Ready_HealthyChecker(t *testing.T) {
	s := NewServer(0, healthyChecker, logr.Discard())
	s.SetReady()
	rec := httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestReadyz_Ready_UnhealthyChecker(t *testing.T) {
	s := NewServer(0, unhealthyChecker, logr.Discard())
	s.SetReady()
	rec := httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	resp := parseResponse(t, rec)
	if resp.Error == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestReadyz_ReadyThenNotReady(t *testing.T) {
	s := NewServer(0, nil, logr.Discard())

	s.SetReady()
	rec := httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("after SetReady: expected 200, got %d", rec.Code)
	}

	s.SetNotReady()
	rec = httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("after SetNotReady: expected 503, got %d", rec.Code)
	}
}

func TestReadyz_CheckerTimeout(t *testing.T) {
	s := NewServer(0, slowChecker, logr.Discard())
	s.SetReady()
	rec := httptest.NewRecorder()

	start := time.Now()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("checker should have timed out within ~3s, took %v", elapsed)
	}
}

func TestServer_StartAndShutdown(t *testing.T) {
	s := NewServer(0, nil, logr.Discard())

	ln, err := s.ListenAndServe()
	if err != nil {
		t.Fatalf("failed to bind: %v", err)
	}
	addr := ln.Addr().String()

	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ln) }()

	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Get(fmt.Sprintf("http://%s/healthz", addr))
	if err != nil {
		t.Fatalf("health server not reachable: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Serve returned error: %v", err)
	}
}
