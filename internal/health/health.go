package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
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

// ListenAndServe binds the health port and returns the listener.
// The caller should invoke Serve in a goroutine. This split lets
// main() detect bind failures synchronously before proceeding.
func (s *Server) ListenAndServe() (net.Listener, error) {
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return nil, err
	}
	s.logger.Info("Health server listening", "addr", ln.Addr().String())
	return ln, nil
}

// Serve accepts connections on the listener. Blocks until shutdown.
func (s *Server) Serve(ln net.Listener) error {
	if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
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

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
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
