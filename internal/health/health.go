package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
)

const checkerTimeout = 3 * time.Second

// Checker returns nil when the backend is healthy.
type Checker func(ctx context.Context) error

// Server serves Kubernetes health probe endpoints.
type Server struct {
	ready   atomic.Bool
	checker Checker
	server  *http.Server
	logger  logr.Logger
}

type response struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func NewServer(port int, checker Checker, logger logr.Logger) *Server {
	s := &Server{
		checker: checker,
		logger:  logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)

	s.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) SetReady() {
	s.ready.Store(true)
	s.logger.Info("Readiness set to true")
}

func (s *Server) SetNotReady() {
	s.ready.Store(false)
	s.logger.Info("Readiness set to false")
}

func (s *Server) Start() error {
	s.logger.Info("Health server starting", "addr", s.server.Addr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, code int, resp response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if s.checker != nil {
		ctx, cancel := context.WithTimeout(r.Context(), checkerTimeout)
		defer cancel()
		if err := s.checker(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, response{Status: "error", Error: err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, response{Status: "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, response{Status: "not ready"})
		return
	}
	if s.checker != nil {
		ctx, cancel := context.WithTimeout(r.Context(), checkerTimeout)
		defer cancel()
		if err := s.checker(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, response{Status: "not ready", Error: err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, response{Status: "ready"})
}
