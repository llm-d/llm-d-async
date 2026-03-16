package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
)

type server struct {
	mu         sync.Mutex
	saturation string
}

func main() {
	s := &server{saturation: "0"}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query", s.handleQuery)
	mux.HandleFunc("/admin/saturation", s.handleSetSaturation)

	log.Println("Starting prom-mock on :9090")
	if err := http.ListenAndServe(":9090", mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	sat := s.saturation
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"inference_pool":"e2e-pool"},"value":[1234567890,"%s"]}]}}`, sat)
}

func (s *server) handleSetSaturation(w http.ResponseWriter, r *http.Request) {
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
		s.saturation = body.Value
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	case http.MethodGet:
		s.mu.Lock()
		sat := s.saturation
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"value":"%s"}`, sat)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
