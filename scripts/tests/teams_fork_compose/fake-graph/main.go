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

type graphMessage struct {
	ID                   string    `json:"id"`
	ChatID               string    `json:"chatId"`
	Body                 string    `json:"body"`
	CreatedDateTime      time.Time `json:"createdDateTime"`
	LastModifiedDateTime time.Time `json:"lastModifiedDateTime"`
	FromID               string    `json:"from_id"`
}

type graphMeeting struct {
	ID      string `json:"id"`
	ChatID  string `json:"chat_id"`
	Subject string `json:"subject"`
	JoinURL string `json:"join_url"`
}

type state struct {
	Scenario      string                  `json:"scenario"`
	Meetings      map[string]graphMeeting `json:"meetings"`
	Messages      []graphMessage          `json:"messages"`
	Faults        map[string]int          `json:"faults"`
	NextMessageID int                     `json:"next_message_id"`
	UpdatedAt     time.Time               `json:"updated_at"`
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
		path:     filepath.Join(stateDir, "fake-graph.json"),
		scenario: strings.TrimSpace(os.Getenv("SCENARIO")),
	}
	s.load()
	if s.state.Scenario == "" {
		s.state.Scenario = s.scenario
	}
	if s.state.Meetings == nil {
		s.state.Meetings = make(map[string]graphMeeting)
	}
	if s.state.Faults == nil {
		s.state.Faults = make(map[string]int)
	}
	_ = s.saveLocked()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/me", s.me)
	mux.HandleFunc("/me/onlineMeetings/createOrGet", s.createMeeting)
	mux.HandleFunc("/chats/", s.chat)
	server := &http.Server{Addr: ":8081", Handler: mux}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fatal(err)
	}
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) me(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"id":                "compose-user",
		"displayName":       "Compose User",
		"userPrincipalName": "compose@example.test",
	})
}

func (s *server) createMeeting(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ExternalID string `json:"externalId"`
		Subject    string `json:"subject"`
	}
	_ = json.NewDecoder(r.Body).Decode(&request)
	externalID := strings.TrimSpace(request.ExternalID)
	if externalID == "" {
		http.Error(w, "externalId is required", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	meeting, ok := s.state.Meetings[externalID]
	if !ok {
		meeting = graphMeeting{
			ID:      "compose-meeting",
			ChatID:  "compose-child-chat",
			Subject: firstNonEmpty(request.Subject, "Compose fork child"),
			JoinURL: "https://teams.example/compose-child",
		}
		s.state.Meetings[externalID] = meeting
		if err := s.saveLocked(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":         meeting.ID,
		"subject":    meeting.Subject,
		"joinWebUrl": meeting.JoinURL,
		"chatInfo":   map[string]any{"threadId": meeting.ChatID},
	})
}

func (s *server) chat(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/chats/")
	path = strings.TrimSuffix(path, "/messages")
	chatID := strings.TrimSpace(path)
	if chatID == "" {
		http.Error(w, "chat id is required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.listMessages(w, chatID)
	case http.MethodPost:
		s.sendMessage(w, r, chatID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) listMessages(w http.ResponseWriter, chatID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make([]map[string]any, 0)
	for _, message := range s.state.Messages {
		if message.ChatID != chatID {
			continue
		}
		values = append(values, graphMessagePayload(message))
	}
	writeJSON(w, http.StatusOK, map[string]any{"value": values})
}

func (s *server) sendMessage(w http.ResponseWriter, r *http.Request, chatID string) {
	var request struct {
		Body struct {
			Content string `json:"content"`
		} `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.state.NextMessageID++
	now := time.Now().UTC()
	message := graphMessage{
		ID:                   fmt.Sprintf("compose-message-%06d", s.state.NextMessageID),
		ChatID:               chatID,
		Body:                 request.Body.Content,
		CreatedDateTime:      now,
		LastModifiedDateTime: now,
		FromID:               "compose-user",
	}
	s.state.Messages = append(s.state.Messages, message)
	faultKey := ""
	marker := outboxMarkerID(message.Body)
	if s.scenario == "graph-response-lost" && strings.HasPrefix(marker, "fork-outbox:") {
		faultKey = "graph-history-drop"
	}
	if s.scenario == "activated-restart" && strings.HasPrefix(marker, "fork-link:") {
		faultKey = "graph-link-drop"
	}
	drop := faultKey != "" && s.state.Faults[faultKey] == 0
	if faultKey != "" {
		s.state.Faults[faultKey]++
	}
	if err := s.saveLocked(); err != nil {
		s.mu.Unlock()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.mu.Unlock()
	if drop {
		closeConnection(w)
		return
	}
	writeJSON(w, http.StatusOK, graphMessagePayload(message))
}

func graphMessagePayload(message graphMessage) map[string]any {
	return map[string]any{
		"id":                   message.ID,
		"chatId":               message.ChatID,
		"messageType":          "message",
		"createdDateTime":      message.CreatedDateTime,
		"lastModifiedDateTime": message.LastModifiedDateTime,
		"from":                 map[string]any{"user": map[string]any{"id": message.FromID, "displayName": "Compose User"}},
		"body":                 map[string]any{"contentType": "html", "content": message.Body},
	}
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

func outboxMarkerID(body string) string {
	const prefix = "codex-helper-outbox:"
	start := strings.LastIndex(body, prefix)
	if start < 0 {
		return ""
	}
	start += len(prefix)
	end := strings.Index(body[start:], " -->")
	if end < 0 {
		return ""
	}
	return strings.TrimSpace(body[start : start+end])
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

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
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
	_, _ = fmt.Fprintf(os.Stderr, "fake-graph: %v\n", err)
	os.Exit(1)
}
