package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/goal"
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
type Goal = goal.State

// Goal statuses the helper sets.
const (
	GoalActive = "active"
	GoalPaused = "paused"
)

// ThreadInfo is what a server knows of one loaded thread.
type ThreadInfo struct {
	Goal         *Goal // nil is absence only when GoalKnown
	GoalKnown    bool
	goalRevision uint64
	Model        string
	Effort       string // "" = the model's default
	Tier         string // the selected service tier ("" or "default" = standard)
	Turn         string // the running turn ("" = idle)
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
	if s.exitErr != nil {
		out.GoalKnown = false
	}
	if t.Goal != nil {
		g := t.Goal.Clone()
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
		err := json.Unmarshal(params, &m)
		s.mu.Lock()
		t := s.threadLocked(threadID)
		t.goalRevision++
		if err != nil || m.Goal == nil || m.Goal.Status == "" || strings.TrimSpace(m.Goal.Objective) == "" {
			t.GoalKnown = false
		} else {
			t.Goal, t.GoalKnown = m.Goal, true
		}
		s.mu.Unlock()
	case "thread/goal/cleared":
		s.mu.Lock()
		s.threadLocked(threadID).Goal = nil
		s.threadLocked(threadID).GoalKnown = true
		s.threadLocked(threadID).goalRevision++
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

// UpdateThreadSettings applies overrides to subsequent turns of a loaded thread.
func (s *Server) UpdateThreadSettings(ctx context.Context, threadID, model, effort string) error {
	params := map[string]any{"threadId": threadID}
	if model != "" {
		params["model"] = model
	}
	if effort != "" {
		params["effort"] = effort
	}
	if len(params) == 1 {
		return nil
	}
	return s.Call(ctx, "thread/settings/update", params, nil)
}

// ApplyThreadSettings applies settings and waits for their effective native
// snapshot. A successful RPC alone can omit a setting the server did not use.
func (s *Server) ApplyThreadSettings(ctx context.Context, threadID, model, effort string) error {
	matches := func(info ThreadInfo) bool {
		return (model == "" || info.Model == model) && (effort == "" || info.Effort == effort)
	}
	if matches(s.Info(threadID)) {
		return nil
	}
	updated := make(chan struct{}, 1)
	unsub := s.Subscribe(threadID, func(n Notification) {
		if n.Method == "thread/settings/updated" {
			select {
			case updated <- struct{}{}:
			default:
			}
		}
	})
	defer unsub()
	if err := s.UpdateThreadSettings(ctx, threadID, model, effort); err != nil {
		return err
	}
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		info := s.Info(threadID)
		if matches(info) {
			return nil
		}
		select {
		case <-updated:
		case <-s.Done():
			return ErrExited
		case <-wait.Done():
			return fmt.Errorf("Codex reported %s/%s, expected %s/%s: %w", info.Model, info.Effort, model, effort, wait.Err())
		}
	}
}

// GoalGet reads the goal without loading execution (nil: confirmed none). A Codex without
// goals answers with an error.
func (s *Server) GoalGet(ctx context.Context, threadID string) (*Goal, error) {
	var res struct {
		Goal *Goal `json:"goal"`
	}
	if err := s.Call(ctx, "thread/goal/get", map[string]any{"threadId": threadID}, &res); err != nil {
		return nil, err
	}
	i := s.Info(threadID)
	if !i.GoalKnown {
		return nil, fmt.Errorf("goal query returned no confirmed snapshot")
	}
	return i.Goal, nil
}

// GoalSet sets the goal as the user (origin "user", as
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
	i := s.Info(threadID)
	if !i.GoalKnown || i.Goal == nil {
		return Goal{}, fmt.Errorf("goal mutation returned no confirmed snapshot")
	}
	return *i.Goal, nil
}

// GoalClear removes the goal without loading execution.
func (s *Server) GoalClear(ctx context.Context, threadID string) error {
	if err := s.Call(ctx, "thread/goal/clear", map[string]any{"threadId": threadID, "origin": "user"}, nil); err != nil {
		return err
	}
	i := s.Info(threadID)
	if !i.GoalKnown || i.Goal != nil {
		return fmt.Errorf("goal clear was not confirmed")
	}
	return nil
}

// goalRPC is retained only while its request is pending. Results are applied by
// the reader before waking the caller, preserving notification/response order.
type goalRPC struct {
	Thread, Method string
	Revision       uint64
}

func goalRequest(method string, raw json.RawMessage) (goalRPC, bool) {
	switch method {
	case "thread/goal/get", "thread/goal/set", "thread/goal/clear":
		var p struct {
			Thread string `json:"threadId"`
		}
		if json.Unmarshal(raw, &p) == nil && p.Thread != "" {
			return goalRPC{Thread: p.Thread, Method: method}, true
		}
	}
	return goalRPC{}, false
}

// Called under s.mu. An RPC error cannot establish a goal snapshot.
func (s *Server) goalResult(r goalRPC, raw json.RawMessage, rpcErr *RPCError) {
	t := s.threadLocked(r.Thread)
	if t.goalRevision != r.Revision {
		return
	} // a notification superseded this in-flight request
	if rpcErr != nil {
		t.GoalKnown = false
		return
	}
	if r.Method == "thread/goal/clear" {
		var p struct {
			Cleared *bool `json:"cleared"`
		}
		if json.Unmarshal(raw, &p) != nil || p.Cleared == nil {
			t.GoalKnown = false
			return
		}
		t.Goal = nil
		t.GoalKnown = true
		return
	}
	var p struct {
		Goal *Goal `json:"goal"`
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &p) != nil || json.Unmarshal(raw, &fields) != nil {
		t.GoalKnown = false
		return
	}
	if _, ok := fields["goal"]; !ok {
		t.GoalKnown = false
		return
	}
	if p.Goal != nil && (p.Goal.Status == "" || strings.TrimSpace(p.Goal.Objective) == "") {
		t.GoalKnown = false
		return
	}
	if r.Method == "thread/goal/set" && p.Goal == nil {
		t.GoalKnown = false
		return
	}
	t.Goal = p.Goal
	t.GoalKnown = true
}

// IsGoalsUnavailable recognizes only definitive capability rejection. A timeout,
// ownership conflict or database error must never be treated as no active goal.
func IsGoalsUnavailable(err error) bool {
	var rpc *RPCError
	if !errors.As(err, &rpc) {
		return false
	}
	return rpc.Code == -32601 || (rpc.Code == -32600 && strings.TrimSpace(rpc.Message) == "goals feature is disabled")
}
