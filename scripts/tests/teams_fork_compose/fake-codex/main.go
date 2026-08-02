package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type thread struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	ForkedFromID string    `json:"forkedFromId,omitempty"`
	LatestTurnID string    `json:"latestTurnId,omitempty"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type state struct {
	Scenario     string    `json:"scenario"`
	ForkRequests int       `json:"fork_requests"`
	Threads      []thread  `json:"threads"`
	Events       []string  `json:"events"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type server struct {
	mu       sync.Mutex
	path     string
	scenario string
	state    state
}

func main() {
	stateDir := firstNonEmpty(os.Getenv("STATE_DIR"), "/state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		fatal(err)
	}
	s := &server{
		path:     filepath.Join(stateDir, "fake-codex.json"),
		scenario: strings.TrimSpace(os.Getenv("SCENARIO")),
	}
	s.load()
	if s.state.Scenario == "" {
		s.state.Scenario = s.scenario
		_ = s.saveLocked()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/initialize", s.initialize)
	mux.HandleFunc("/thread/fork", s.fork)
	mux.HandleFunc("/thread/list", s.list)
	mux.HandleFunc("/thread/read", s.read)
	mux.HandleFunc("/thread/resume", s.resume)
	server := &http.Server{Addr: ":8080", Handler: mux}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fatal(err)
	}
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) initialize(w http.ResponseWriter, _ *http.Request) {
	s.record("initialize")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) resume(w http.ResponseWriter, _ *http.Request) {
	s.record("thread/resume")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) fork(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ParentThreadID string `json:"parentThreadId"`
		LastTurnID     string `json:"lastTurnId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.state.ForkRequests++
	s.state.Events = append(s.state.Events, "thread/fork")
	childID := "compose-child-codex"
	var child *thread
	for i := range s.state.Threads {
		candidate := &s.state.Threads[i]
		if candidate.ID == childID {
			child = candidate
			break
		}
	}
	if child == nil {
		now := time.Now().UTC()
		value := thread{
			ID:           childID,
			Name:         "Compose fork child",
			ForkedFromID: strings.TrimSpace(request.ParentThreadID),
			LatestTurnID: strings.TrimSpace(request.LastTurnID),
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		s.state.Threads = append(s.state.Threads, value)
		child = &s.state.Threads[len(s.state.Threads)-1]
	}
	requestNumber := s.state.ForkRequests
	if err := s.saveLocked(); err != nil {
		s.mu.Unlock()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.mu.Unlock()

	if s.scenario == "native-response-lost" && requestNumber == 1 {
		closeConnection(w)
		return
	}
	writeJSON(w, http.StatusOK, *child)
}

func (s *server) list(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Events = append(s.state.Events, "thread/list")
	_ = s.saveLocked()
	writeJSON(w, http.StatusOK, map[string]any{"threads": append([]thread(nil), s.state.Threads...)})
}

func (s *server) read(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ThreadID string `json:"threadId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Events = append(s.state.Events, "thread/read")
	_ = s.saveLocked()
	for _, candidate := range s.state.Threads {
		if candidate.ID == strings.TrimSpace(request.ThreadID) {
			writeJSON(w, http.StatusOK, candidate)
			return
		}
	}
	http.Error(w, "thread not found", http.StatusNotFound)
}

func (s *server) record(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Events = append(s.state.Events, event)
	_ = s.saveLocked()
}

func (s *server) load() {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &s.state)
}

func (s *server) saveLocked() error {
	s.state.UpdatedAt = time.Now().UTC()
	tmp := s.path + ".tmp"
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func closeConnection(w http.ResponseWriter) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hijacker.Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func fatal(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "fake-codex: %v\n", err)
	os.Exit(1)
}
