package fakecodex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// TitleRequest records real thread/start and turn/start parameters, so tests
// check isolation on the wire rather than trusting the client's intent.
type TitleRequest struct {
	Start map[string]any `json:"start"`
	Turn  map[string]any `json:"turn"`
}
type TitleResult struct {
	Output string `json:"output"`
	Hold   bool   `json:"hold"`
}
type temporaryTitle struct {
	id     string
	start  map[string]any
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *server) startTemporaryTitle(p map[string]any) (any, *rpcError) {
	if p["sandbox"] != "read-only" || p["approvalPolicy"] != "never" {
		return nil, invalid("temporary title must be read-only and noninteractive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	tmp := &temporaryTitle{id: newID(), start: p, ctx: ctx, cancel: cancel}
	s.mu.Lock()
	s.temporary[tmp.id] = tmp
	s.mu.Unlock()
	return map[string]any{"thread": map[string]any{"id": tmp.id}, "sandbox": map[string]any{"type": "readOnly"}}, nil
}
func (s *server) runTemporaryTitle(tmp *temporaryTitle, p map[string]any) (any, *rpcError) {
	if p["outputSchema"] == nil {
		return nil, invalid("title requires output schema")
	}
	turn := newID()
	s.notify("turn/started", map[string]any{"threadId": tmp.id, "turn": map[string]any{"id": turn, "status": "inProgress"}})
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var result TitleResult
		err := s.post(tmp.ctx, "/title", TitleRequest{Start: tmp.start, Turn: p}, &result)
		if err != nil || tmp.ctx.Err() != nil {
			return
		}
		s.notify("item/completed", map[string]any{"threadId": tmp.id, "turnId": turn, "item": map[string]any{"type": "agentMessage", "id": newID(), "text": result.Output, "phase": "final_answer"}})
		s.notify("turn/completed", map[string]any{"threadId": tmp.id, "turn": map[string]any{"id": turn, "status": "completed"}})
	}()
	return map[string]any{"turn": map[string]any{"id": turn, "status": "inProgress"}}, nil
}
func (s *server) setName(id, name string) (any, *rpcError) {
	t, err := s.current(id)
	if err != nil {
		return nil, invalid("thread not found")
	}
	s.mu.Lock()
	t.Name = name
	err = s.cfg.Store.Save(t)
	s.mu.Unlock()
	if err != nil {
		return nil, invalid("save name: %v", err)
	}
	if s.cfg.CodexHome != "" {
		f, e := os.OpenFile(filepath.Join(s.cfg.CodexHome, "session_index.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			return nil, invalid("index: %v", e)
		}
		e = json.NewEncoder(f).Encode(map[string]any{"id": id, "thread_name": name, "updated_at": time.Now().UTC().Format(time.RFC3339Nano)})
		_ = f.Close()
		if e != nil {
			return nil, invalid("index: %v", e)
		}
	}
	s.notify("thread/name/updated", map[string]any{"threadId": id, "threadName": name})
	return map[string]any{}, nil
}
