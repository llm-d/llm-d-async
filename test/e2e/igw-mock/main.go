package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
)

type failConfig struct {
	Status int `json:"status"`
	Count  int `json:"count"`
}

type server struct {
	mu         sync.Mutex
	requestLog []string
	fail       failConfig
}

func main() {
	s := &server{}

	inferenceMux := http.NewServeMux()
	inferenceMux.HandleFunc("/v1/completions", s.handleCompletions)

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/admin/fail-next", s.handleFailNext)
	adminMux.HandleFunc("/admin/request-log", s.handleRequestLog)
	adminMux.HandleFunc("/admin/reset", s.handleReset)

	go func() {
		log.Println("Starting admin server on :8081")
		if err := http.ListenAndServe(":8081", adminMux); err != nil {
			log.Fatalf("admin server failed: %v", err)
		}
	}()

	log.Println("Starting inference server on :8080")
	if err := http.ListenAndServe(":8080", inferenceMux); err != nil {
		log.Fatalf("inference server failed: %v", err)
	}
}

func (s *server) handleCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	// Extract request ID from the model field or generate one
	reqID := ""
	if id, ok := payload["id"].(string); ok {
		reqID = id
	} else if model, ok := payload["model"].(string); ok {
		reqID = model
	}

	s.mu.Lock()
	s.requestLog = append(s.requestLog, reqID)

	if s.fail.Count > 0 {
		status := s.fail.Status
		s.fail.Count--
		s.mu.Unlock()
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":"injected failure","status":%d}`, status)
		return
	}
	s.mu.Unlock()

	resp := map[string]any{
		"id":     reqID,
		"result": "ok",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *server) handleFailNext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var fc failConfig
	if err := json.NewDecoder(r.Body).Decode(&fc); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.fail = fc
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"ok"}`)
}

func (s *server) handleRequestLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	logCopy := make([]string, len(s.requestLog))
	copy(logCopy, s.requestLog)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logCopy)
}

func (s *server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	s.requestLog = nil
	s.fail = failConfig{}
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"ok"}`)
}
