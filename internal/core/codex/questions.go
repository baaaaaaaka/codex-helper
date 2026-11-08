package codex

import (
	"encoding/json"
	"errors"
	"sync/atomic"
)

type AsyncQuestion struct {
	Title   string   `json:"title"`
	Options []string `json:"options"`
}

var ErrQuestionExpired = errors.New("the question request is no longer pending")

var questionBackends atomic.Uint64

// QuestionBackendID never reuses an identity within this helper process.
// Pointer addresses and OS PIDs can be reused after an idle backend is reaped.
func (s *Server) QuestionBackendID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.questionBackendID == 0 {
		s.questionBackendID = questionBackends.Add(1)
	}
	return s.questionBackendID
}

func (s *Server) HasQuestion(id json.RawMessage) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.serverRequests[string(id)]
	return ok && m.Method == "item/tool/requestUserInput"
}

// RespondQuestion only claims an existing question, never an approval. A write
// failure is ambiguous and must not be retried by the caller.
func (s *Server) RespondQuestion(id json.RawMessage, answers map[string][]string) error {
	s.mu.Lock()
	m, ok := s.serverRequests[string(id)]
	if ok && m.Method == "item/tool/requestUserInput" {
		delete(s.serverRequests, string(id))
	} else {
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		return ErrQuestionExpired
	}
	out := map[string]any{}
	for k, v := range answers {
		out[k] = map[string]any{"answers": v}
	}
	raw, err := json.Marshal(map[string]any{"answers": out})
	if err != nil {
		return err
	}
	return s.write(wireMsg{ID: id, Result: raw})
}
