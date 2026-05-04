package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

type server struct {
	mu              sync.Mutex
	saturation      string
	budget          string
	vllmBudget      string
	primaryDisabled bool
}

func main() {
	s := &server{saturation: "0", budget: "0", vllmBudget: "0"}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query", s.handleQuery)
	mux.HandleFunc("/admin/saturation", s.handleSetSaturation)
	mux.HandleFunc("/admin/budget", s.handleSetBudget)
	mux.HandleFunc("/admin/vllm-budget", s.handleSetVLLMBudget)
	mux.HandleFunc("/admin/disable-primary", s.handleDisablePrimary)
	mux.HandleFunc("/admin/reset", s.handleReset)

	log.Println("Starting prom-mock on :9090")
	if err := http.ListenAndServe(":9090", mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// handleQuery returns a single Prometheus vector result.
//
// Routing by query content:
//   - "inference_extension_flow_control_queue_size" → primary budget gate metric.
//     Returns empty result when primary is disabled (simulating EPP unavailability).
//   - "vllm:num_requests_running" → secondary vLLM fallback metric.
//   - anything else → saturation metric (1 - saturation).
func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form", http.StatusBadRequest)
		return
	}
	query := r.FormValue("query")

	s.mu.Lock()
	primaryDisabled := s.primaryDisabled
	budget := s.budget
	vllmBudget := s.vllmBudget
	saturation := s.saturation
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(query, "inference_extension_flow_control_queue_size"):
		if primaryDisabled {
			// Return empty result to trigger cascade to secondary
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
			return
		}
		writeVectorResult(w, budget)

	case strings.Contains(query, "vllm:num_requests_running"):
		writeVectorResult(w, vllmBudget)

	default:
		sat, _ := strconv.ParseFloat(saturation, 64)
		writeVectorResult(w, strconv.FormatFloat(1.0-sat, 'f', -1, 64))
	}
}

func writeVectorResult(w http.ResponseWriter, value string) {
	fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"inference_pool":"e2e-pool"},"value":[1234567890,"%s"]}]}}`, value)
}

func (s *server) handleSetSaturation(w http.ResponseWriter, r *http.Request) {
	s.handleSetValue(w, r, func(v string) { s.saturation = v }, func() string { return s.saturation })
}

func (s *server) handleSetBudget(w http.ResponseWriter, r *http.Request) {
	s.handleSetValue(w, r, func(v string) { s.budget = v }, func() string { return s.budget })
}

func (s *server) handleSetVLLMBudget(w http.ResponseWriter, r *http.Request) {
	s.handleSetValue(w, r, func(v string) { s.vllmBudget = v }, func() string { return s.vllmBudget })
}

func (s *server) handleDisablePrimary(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.mu.Lock()
		s.primaryDisabled = true
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	case http.MethodDelete:
		s.mu.Lock()
		s.primaryDisabled = false
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	s.saturation = "0"
	s.budget = "0"
	s.vllmBudget = "0"
	s.primaryDisabled = false
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"ok"}`)
}

func (s *server) handleSetValue(w http.ResponseWriter, r *http.Request, set func(string), get func() string) {
	switch r.Method {
	case http.MethodPost:
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if _, err := strconv.ParseFloat(body.Value, 64); err != nil {
			http.Error(w, "value must be a valid number", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		set(body.Value)
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	case http.MethodGet:
		s.mu.Lock()
		val := get()
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"value":"%s"}`, val)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
