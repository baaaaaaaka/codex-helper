package codex

import (
	"context"
	"encoding/json"
	"strings"
)

// Thread state the helper shows with Codex messages (owner request
// 2026-11-07): a thread's goal (Codex 0.16x `thread/goal/*`) and its
// effective model, reasoning effort and service tier. A server learns them
// from thread/start and thread/resume, from thread/goal/get and set, and
// from the thread/goal/updated, thread/goal/cleared and
// thread/settings/updated notifications. It also tracks the turn running
// in each thread, the helper's or one Codex started by itself (an active
// goal continues the thread whenever it goes idle), so that turn can be
// interrupted and its server is not reaped while it runs.

// Goal is a thread goal.
type Goal struct {
	Objective string `json:"objective"`
	Status    string `json:"status"` // active, paused, blocked, usageLimited, budgetLimited, complete
}

// Goal statuses the helper sets.
const (
	GoalActive = "active"
	GoalPaused = "paused"
)

// ThreadInfo is what a server knows of one loaded thread.
type ThreadInfo struct {
	Goal   *Goal // nil: no goal
	Model  string
	Effort string // "" = the model's default
	Tier   string // the selected service tier ("" or "default" = standard)
	Turn   string // the running turn ("" = idle)
}

type threadSettings struct {
	Model       string  `json:"model"`
	Effort      *string `json:"effort"`
	ServiceTier *string `json:"serviceTier"`
}

func (s *Server) threadLocked(id string) *ThreadInfo {
	if s.threads == nil {
		s.threads = map[string]*ThreadInfo{}
	}
	t := s.threads[id]
	if t == nil {
		t = &ThreadInfo{}
		s.threads[id] = t
	}
	return t
}

// Info returns what the server knows of threadID.
func (s *Server) Info(threadID string) ThreadInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.threads[threadID]
	if t == nil {
		return ThreadInfo{}
	}
	out := *t
	if t.Goal != nil {
		g := *t.Goal
		out.Goal = &g
	}
	return out
}

// ForgetTurn discards a stale task snapshot after a guarded steer was rejected.
// A newer task notification is retained. The next ordinary turn/start still
// makes its own atomic start-or-steer decision in the native server.
func (s *Server) ForgetTurn(thread, expected string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.threads[thread]; t != nil && t.Turn == expected {
		t.Turn = ""
	}
}

// Running reports whether a turn runs in any thread of the server.
func (s *Server) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.threads {
		if t.Turn != "" {
			return true
		}
	}
	return false
}

// track updates the thread state from a notification (reader goroutine).
func (s *Server) track(method, threadID string, params json.RawMessage) {
	if threadID == "" {
		return
	}
	switch method {
	case "thread/goal/updated":
		var m struct {
			Goal *Goal `json:"goal"`
		}
		if json.Unmarshal(params, &m) == nil && m.Goal != nil {
			s.mu.Lock()
			s.threadLocked(threadID).Goal = m.Goal
			s.mu.Unlock()
		}
	case "thread/goal/cleared":
		s.mu.Lock()
		s.threadLocked(threadID).Goal = nil
		s.mu.Unlock()
	case "thread/settings/updated":
		var m struct {
			ThreadSettings threadSettings `json:"threadSettings"`
		}
		if json.Unmarshal(params, &m) == nil {
			s.setSettings(threadID, m.ThreadSettings.Model, m.ThreadSettings.Effort, m.ThreadSettings.ServiceTier)
		}
	case "turn/started":
		var m struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if json.Unmarshal(params, &m) == nil && m.Turn.ID != "" {
			s.mu.Lock()
			s.threadLocked(threadID).Turn = m.Turn.ID
			s.mu.Unlock()
		}
	case "turn/completed":
		var m struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if json.Unmarshal(params, &m) == nil {
			s.mu.Lock()
			if t := s.threadLocked(threadID); m.Turn.ID == "" || t.Turn == m.Turn.ID {
				t.Turn = ""
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) setSettings(threadID, model string, effort, tier *string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.threadLocked(threadID)
	if model != "" {
		t.Model = model
	}
	if effort != nil {
		t.Effort = *effort
	}
	if tier != nil {
		t.Tier = *tier
	}
}

// loadedResponse records the settings of a thread/start or thread/resume
// response.
func (s *Server) loadedResponse(threadID string, raw json.RawMessage) {
	var m struct {
		Model           string  `json:"model"`
		ReasoningEffort *string `json:"reasoningEffort"`
		ServiceTier     *string `json:"serviceTier"`
	}
	if threadID == "" || json.Unmarshal(raw, &m) != nil {
		return
	}
	effort, tier := m.ReasoningEffort, m.ServiceTier
	empty := ""
	if effort == nil {
		effort = &empty
	}
	if tier == nil {
		tier = &empty
	}
	s.setSettings(threadID, m.Model, effort, tier)
}

// GoalGet reads the goal of a loaded thread (nil: none). A Codex without
// goals answers with an error.
func (s *Server) GoalGet(ctx context.Context, threadID string) (*Goal, error) {
	var res struct {
		Goal *Goal `json:"goal"`
	}
	if err := s.Call(ctx, "thread/goal/get", map[string]any{"threadId": threadID}, &res); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.threadLocked(threadID).Goal = res.Goal
	s.mu.Unlock()
	return res.Goal, nil
}

// GoalSet sets the goal of a loaded thread as the user (origin "user", as
// the TUI's /goal): a new objective ("" keeps it) and/or a status ("" keeps
// it; a new goal is active). An active goal on an idle thread makes Codex
// start working on it.
func (s *Server) GoalSet(ctx context.Context, threadID, objective, status string) (Goal, error) {
	params := map[string]any{"threadId": threadID, "origin": "user"}
	if strings.TrimSpace(objective) != "" {
		params["objective"] = objective
	}
	if status != "" {
		params["status"] = status
	}
	var res struct {
		Goal Goal `json:"goal"`
	}
	if err := s.Call(ctx, "thread/goal/set", params, &res); err != nil {
		return Goal{}, err
	}
	g := res.Goal
	s.mu.Lock()
	s.threadLocked(threadID).Goal = &g
	s.mu.Unlock()
	return g, nil
}

// GoalClear removes the goal of a loaded thread.
func (s *Server) GoalClear(ctx context.Context, threadID string) error {
	if err := s.Call(ctx, "thread/goal/clear", map[string]any{"threadId": threadID}, nil); err != nil {
		return err
	}
	s.mu.Lock()
	s.threadLocked(threadID).Goal = nil
	s.mu.Unlock()
	return nil
}
