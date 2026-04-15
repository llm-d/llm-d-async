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
	mu         sync.Mutex
	saturation string
	budget     string
}

func main() {
	s := &server{saturation: "0", budget: "0"}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query", s.handleQuery)
	mux.HandleFunc("/admin/saturation", s.handleSetSaturation)
	mux.HandleFunc("/admin/budget", s.handleSetBudget)

	log.Println("Starting prom-mock on :9090")
	if err := http.ListenAndServe(":9090", mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// handleQuery returns a single Prometheus vector result. If the query contains
// "queue_size" it is a budget gate query: the mock stores the dispatch budget D and
// returns 1 - D as the combined saturation (F_SYS + F_EPP + B) that the gate expects.
// Otherwise it returns the plain saturation value.
//
// The Prometheus client sends queries as POST form body, so we parse both
// URL params and form body to find the query expression.
func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
	// Prometheus client_golang sends the query as a POST form body.
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form", http.StatusBadRequest)
		return
	}
	query := r.FormValue("query")

	s.mu.Lock()
	var value string
	if strings.Contains(query, "inference_extension_flow_control_queue_size") {
		budget, _ := strconv.ParseFloat(s.budget, 64)
		value = strconv.FormatFloat(1.0-budget, 'f', -1, 64)
	} else {
		value = s.saturation
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"inference_pool":"e2e-pool"},"value":[1234567890,"%s"]}]}}`, value)
}

func (s *server) handleSetSaturation(w http.ResponseWriter, r *http.Request) {
	s.handleSetValue(w, r, func(v string) { s.saturation = v }, func() string { return s.saturation })
}

func (s *server) handleSetBudget(w http.ResponseWriter, r *http.Request) {
	s.handleSetValue(w, r, func(v string) { s.budget = v }, func() string { return s.budget })
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
