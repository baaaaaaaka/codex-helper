package codex

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

// Usage is the token usage of a turn (thread/tokenUsage/updated "last").
type Usage struct {
	Input     int64 `json:"inputTokens"`
	Cached    int64 `json:"cachedInputTokens"`
	Output    int64 `json:"outputTokens"`
	Reasoning int64 `json:"reasoningOutputTokens"`
	Total     int64 `json:"totalTokens"`
}

// TurnResult is the outcome of a turn.
type TurnResult struct {
	TurnID string
	Status string // completed | failed | interrupted
	Final  string
	Err    *TurnError
	Usage  *Usage
}

// TurnHooks observe a running turn. Each hook runs on its own call path
// and must not block for long.
type TurnHooks struct {
	// Started is called once turn/start was accepted.
	Started func(turnID string)
	// Progress receives commentary agent messages.
	Progress func(text string)
}

// ErrNotStarted wraps a turn/start rejection: the turn certainly did not
// start.
type ErrNotStarted struct{ Err error }

func (e *ErrNotStarted) Error() string { return "codex: turn not started: " + e.Err.Error() }
func (e *ErrNotStarted) Unwrap() error { return e.Err }

// RunTurn starts a turn on a loaded thread and waits for its end:
// turn/completed, or a poll of thread/turns/list every poll (Codex may
// lose the callback). It returns ErrExited when the server died, ctx.Err()
// when ctx ended, and *ErrNotStarted when turn/start was rejected. On
// ErrExited or a ctx error after turn/start was sent the outcome is
// unknown and must be reconciled with FindTurn by p.ClientID.
func (s *Server) RunTurn(ctx context.Context, p TurnStartParams, poll time.Duration, hooks TurnHooks) (TurnResult, error) {
	if poll <= 0 {
		poll = 5 * time.Second
	}
	var (
		mu        sync.Mutex
		turnID    string
		items     = map[string][]Item{} // by turn id
		usage     = map[string]*Usage{}
		completed = make(chan Turn, 4)
	)
	unsub := s.Subscribe(p.ThreadID, func(n Notification) {
		switch n.Method {
		case "item/completed":
			var m struct {
				TurnID string `json:"turnId"`
				Item   Item   `json:"item"`
			}
			if json.Unmarshal(n.Params, &m) != nil || m.Item.Type != "agentMessage" {
				return
			}
			mu.Lock()
			items[m.TurnID] = append(items[m.TurnID], m.Item)
			mine := m.TurnID == turnID
			mu.Unlock()
			if mine && m.Item.Phase != "final_answer" && hooks.Progress != nil && strings.TrimSpace(m.Item.Text) != "" {
				hooks.Progress(m.Item.Text)
			}
		case "thread/tokenUsage/updated":
			var m struct {
				TurnID     string `json:"turnId"`
				TokenUsage struct {
					Last *Usage `json:"last"`
				} `json:"tokenUsage"`
			}
			if json.Unmarshal(n.Params, &m) == nil && m.TokenUsage.Last != nil {
				mu.Lock()
				usage[m.TurnID] = m.TokenUsage.Last
				mu.Unlock()
			}
		case "turn/completed":
			var m struct {
				Turn Turn `json:"turn"`
			}
			if json.Unmarshal(n.Params, &m) == nil {
				select {
				case completed <- m.Turn:
				default:
				}
			}
		}
	})
	defer unsub()

	id, err := s.TurnStart(ctx, p)
	if err != nil {
		var re *RPCError
		if errors.As(err, &re) {
			return TurnResult{}, &ErrNotStarted{Err: err}
		}
		return TurnResult{}, err
	}
	mu.Lock()
	turnID = id
	early := append([]Item(nil), items[id]...)
	mu.Unlock()
	if hooks.Started != nil {
		hooks.Started(id)
	}
	if hooks.Progress != nil {
		for _, it := range early {
			if it.Phase != "final_answer" && strings.TrimSpace(it.Text) != "" {
				hooks.Progress(it.Text)
			}
		}
	}
	finish := func(t Turn) (TurnResult, error) {
		mu.Lock()
		got := Turn{ID: id, Items: append([]Item(nil), items[id]...)}
		u := usage[id]
		mu.Unlock()
		got.Items = append(got.Items, t.Items...)
		res := TurnResult{TurnID: id, Status: t.Status, Final: got.Final(), Err: t.Error, Usage: u}
		if res.Status == StatusCompleted && res.Final == "" {
			// turn/completed carries no items: read them back.
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if full, ok := s.turnByID(rctx, p.ThreadID, id); ok {
				res.Final = full.Final()
			}
		}
		return res, nil
	}
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		select {
		case t := <-completed:
			if t.ID != id || t.Status == StatusInProgress {
				continue
			}
			return finish(t)
		case <-tick.C:
			if t, ok := s.turnByID(ctx, p.ThreadID, id); ok && t.Status != StatusInProgress && t.Status != "" {
				return finish(t)
			}
		case <-s.Done():
			return TurnResult{TurnID: id}, ErrExited
		case <-ctx.Done():
			return TurnResult{TurnID: id}, ctx.Err()
		}
	}
}

func (s *Server) turnByID(ctx context.Context, threadID, turnID string) (Turn, bool) {
	turns, err := s.RecentTurns(ctx, threadID, 5)
	if err == nil {
		for _, t := range turns {
			if t.ID == turnID {
				return t, true
			}
		}
	}
	th, err := s.ThreadRead(ctx, threadID, true)
	if err != nil {
		return Turn{}, false
	}
	for _, t := range th.Turns {
		if t.ID == turnID {
			return t, true
		}
	}
	return Turn{}, false
}
