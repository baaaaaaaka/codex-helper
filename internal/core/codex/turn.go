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
	TurnChanged func() // start/completion notifications, not token deltas
	// Started is called once turn/start was accepted.
	Started func(turnID string)
	// Progress receives commentary agent messages.
	Progress func(text string)
	// Compacted is called once per context compaction of the turn (a
	// contextCompaction item, or the legacy thread/compacted notification;
	// both for one compaction count once).
	Compacted func()
	// Activity is called for every notification about the thread (any
	// sign of life: deltas, items, token counts), for silence tracking.
	Activity func()
	// Retrying is called for each error notification of the turn that
	// Codex will retry by itself (willRetry: a dropped stream, a
	// reconnect), with the composed, sanitized message and reason code.
	Retrying func(message, code string)
}

// compactPair is how close a contextCompaction item and a legacy
// thread/compacted notification must be to count as one compaction.
const compactPair = 5 * time.Second

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
	dispatched := make(chan string, 1)
	var (
		mu        sync.Mutex
		turnID    string
		candidate string
		items     = map[string][]Item{} // by turn id
		usage     = map[string]*Usage{}
		completed = make(chan Turn, 4)
		compacts  = map[string]bool{} // contextCompaction item ids seen
		// The last unpaired item and legacy notification: one of each
		// within compactPair is one compaction reported both ways.
		lastItem, lastLegacy time.Time
	)
	// Progress. Commentary is posted as it comes. An agent message without
	// a phase (Codex streams those for models without message phases) may
	// be the final answer, so it is held until a later message or a
	// compaction shows it was not (v0.1.20's one-message look-behind), and
	// dropped at the end if it is the final.
	var (
		pmu  sync.Mutex
		held string
	)
	progress := func(it Item) {
		if hooks.Progress == nil || it.Delivery == "async" || it.Phase == "final_answer" || strings.TrimSpace(it.Text) == "" {
			return
		}
		pmu.Lock()
		defer pmu.Unlock()
		if held != "" {
			hooks.Progress(held)
		}
		held = ""
		if it.Phase == "commentary" {
			hooks.Progress(it.Text)
		} else {
			held = it.Text
		}
	}
	flushHeld := func(final string) {
		pmu.Lock()
		defer pmu.Unlock()
		if held != "" && hooks.Progress != nil && strings.TrimSpace(held) != strings.TrimSpace(final) {
			hooks.Progress(held)
		}
		held = ""
	}
	// compacted reports one compaction of the running turn (item: the
	// contextCompaction item id, "" for thread/compacted).
	compacted := func(turn, item string) {
		mu.Lock()
		now := time.Now()
		dup := false
		switch {
		case p.Queue && (candidate == "" || (turn != "" && turn != candidate)):
			dup = true // a queued observer owns no earlier task
		case turn != "" && turnID != "" && turn != turnID:
			dup = true // another turn's (a compaction before turn/start answered is this turn's)
		case item != "" && compacts[item]:
			dup = true
		case item != "":
			compacts[item] = true
			if dup = !lastLegacy.IsZero() && now.Sub(lastLegacy) < compactPair; dup {
				lastLegacy = time.Time{}
			} else {
				lastItem = now
			}
		default:
			if dup = !lastItem.IsZero() && now.Sub(lastItem) < compactPair; dup {
				lastItem = time.Time{}
			} else {
				lastLegacy = now
			}
		}
		mu.Unlock()
		if !dup && hooks.Compacted != nil {
			flushHeld("")
			hooks.Compacted()
		}
	}
	unsub := s.Subscribe(p.ThreadID, func(n Notification) {
		if hooks.Activity != nil {
			hooks.Activity()
		}
		if (n.Method == "turn/started" || n.Method == "turn/completed") && hooks.TurnChanged != nil {
			hooks.TurnChanged()
		}
		switch n.Method {
		case "thread/compacted":
			var m struct {
				TurnID string `json:"turnId"`
			}
			if json.Unmarshal(n.Params, &m) == nil {
				compacted(m.TurnID, "")
			}
		case "item/started", "item/completed":
			var m struct {
				TurnID string `json:"turnId"`
				Item   Item   `json:"item"`
			}
			if json.Unmarshal(n.Params, &m) != nil {
				return
			}
			if m.Item.Type == "userMessage" && m.Item.ClientID == p.ClientID && p.ClientID != "" {
				mu.Lock()
				candidate = m.TurnID
				mu.Unlock()
				select {
				case dispatched <- m.TurnID:
				default:
				}
			}
			if n.Method != "item/completed" {
				return
			}
			mu.Lock()
			other := p.Queue && (candidate == "" || m.TurnID != candidate)
			mu.Unlock()
			if other {
				return
			}
			if m.Item.Type == "contextCompaction" {
				compacted(m.TurnID, m.Item.ID)
				return
			}
			if m.Item.Type != "agentMessage" {
				// Codex went on working: a held message was not the final
				// (v0.1.20 flushed on command events).
				switch m.Item.Type {
				case "commandExecution", "fileChange", "mcpToolCall":
					mu.Lock()
					mine := m.TurnID == turnID
					mu.Unlock()
					if mine {
						flushHeld("")
					}
				}
				return
			}
			mu.Lock()
			if turnID != "" && m.TurnID != turnID {
				mu.Unlock()
				return
			}
			items[m.TurnID] = append(items[m.TurnID], m.Item)
			mine := m.TurnID == turnID
			mu.Unlock()
			if mine {
				progress(m.Item)
			}
		case "error":
			if hooks.Retrying == nil {
				return
			}
			r, ok := parseRetry(n.Params)
			if !ok {
				return
			}
			mu.Lock()
			mine := (r.turnID == "" || turnID == "" || r.turnID == turnID) && (!p.Queue || (candidate != "" && (r.turnID == "" || r.turnID == candidate)))
			mu.Unlock()
			if mine {
				hooks.Retrying(r.message, r.code)
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
				if (!p.Queue || (candidate != "" && m.TurnID == candidate)) && (turnID == "" || turnID == m.TurnID) {
					usage[m.TurnID] = m.TokenUsage.Last
				}
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

	id, err := s.submitTurn(ctx, p, dispatched)
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
	for _, it := range early {
		progress(it)
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
		flushHeld(res.Final)
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
		case <-s.fault:
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

// ErrQueueCancelled means a queued input was removed before dispatch.
var ErrQueueCancelled = errors.New("codex: queued input cancelled")

func (s *Server) submitTurn(ctx context.Context, p TurnStartParams, dispatched <-chan string) (string, error) {
	if !p.Queue {
		return s.TurnStart(ctx, p)
	}
	// The queue uses the current session settings, as does native TUI Tab.
	settings := map[string]any{"threadId": p.ThreadID}
	if p.Model != "" {
		settings["model"] = p.Model
	}
	if p.Effort != "" {
		settings["effort"] = p.Effort
	}
	if len(settings) > 1 {
		if err := s.Call(ctx, "thread/settings/update", settings, nil); err != nil {
			return "", &ErrNotStarted{Err: err}
		}
	}
	id, err := s.QueueAdd(ctx, p)
	if err != nil {
		return "", err
	}
	if p.Queued != nil {
		p.Queued(id)
	}
	// add may already have dispatched it. start is an acceleration for an
	// idle thread; busy/not-found is harmless, and never means resend.
	if err := s.QueueStart(ctx, p.ThreadID, id); err != nil {
		var re *RPCError
		if !errors.As(err, &re) {
			return "", err
		}
	}
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case turn := <-dispatched:
			return turn, nil
		case <-p.QueueCancelled:
			return "", ErrQueueCancelled
		case <-ctx.Done():
			return "", ctx.Err()
		case <-s.fault:
			return "", ErrExited
		case <-tick.C:
			// While waiting, inspect only the queue, not previous tasks' large
			// tool results. History is needed only if dispatch wasn't observed.
			queued, err := s.queuedInput(ctx, p.ThreadID, p.ClientID)
			if err != nil {
				return "", err
			}
			if queued != "" {
				continue
			}
			// Recovery from a missed event uses a bounded recent window. Never
			// re-add an input because a response or notification was lost.
			turns, err := s.RecentTurns(ctx, p.ThreadID, 5)
			if err != nil {
				return "", err
			}
			for _, t := range turns {
				for _, it := range t.Items {
					if it.Type == "userMessage" && it.ClientID == p.ClientID {
						return t.ID, nil
					}
				}
			}
		}
	}
}
