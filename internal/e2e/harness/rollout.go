package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Rollout writes a Codex TUI rollout JSONL file the way a current Codex CLI
// (codex_cli_rs, rollout v2 with turn ids and message phases) does.
type Rollout struct {
	mu        sync.Mutex
	Path      string
	SessionID string
	Cwd       string
	turns     int
	seq       int
	ordinal   int
	clock     time.Time
}

// NewRollout creates sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl under
// env.SessionsDir with the session_meta and environment-context preamble.
func NewRollout(t testing.TB, env *Env, cwd string) *Rollout {
	t.Helper()
	now := time.Now().UTC()
	id := fmt.Sprintf("019e2e%02d-%04x-7%03x-8%03x-%012x", now.Second(), now.Nanosecond()&0xffff, (now.Nanosecond()>>16)&0xfff, now.Nanosecond()&0xfff, now.UnixNano()&0xffffffffffff)
	dir := filepath.Join(env.SessionsDir, now.Format("2006"), now.Format("01"), now.Format("02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir rollout dir: %v", err)
	}
	r := &Rollout{
		Path:      filepath.Join(dir, "rollout-"+now.Format("2006-01-02T15-04-05")+"-"+id+".jsonl"),
		SessionID: id,
		Cwd:       cwd,
		clock:     now,
	}
	ts := r.tick()
	r.append(t,
		line(ts, "session_meta", map[string]any{
			"id":             id,
			"timestamp":      ts,
			"cwd":            cwd,
			"originator":     "codex_cli_rs",
			"cli_version":    "0.58.0",
			"instructions":   nil,
			"source":         "cli",
			"model_provider": "openai",
			"git":            map[string]any{"commit_hash": "e2e0000", "branch": "main"},
		}),
		line(r.tick(), "response_item", map[string]any{
			"type": "message",
			"role": "user",
			"content": []any{map[string]any{
				"type": "input_text",
				"text": "<environment_context>\n  <cwd>" + cwd + "</cwd>\n  <approval_policy>on-request</approval_policy>\n  <sandbox_mode>workspace-write</sandbox_mode>\n  <network_access>restricted</network_access>\n  <shell>bash</shell>\n</environment_context>",
			}},
		}),
	)
	return r
}

func (r *Rollout) tick() string {
	r.clock = r.clock.Add(37 * time.Millisecond)
	if now := time.Now().UTC(); now.After(r.clock) {
		r.clock = now
	}
	return r.clock.Format("2006-01-02T15:04:05.000Z")
}

func line(ts, typ string, payload any) map[string]any {
	return map[string]any{"timestamp": ts, "type": typ, "payload": payload}
}

func (r *Rollout) append(t testing.TB, records ...map[string]any) {
	t.Helper()
	f, err := os.OpenFile(r.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open rollout: %v", err)
	}
	defer f.Close()
	for _, rec := range records {
		r.ordinal++
		rec["ordinal"] = r.ordinal
		data, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal rollout record: %v", err)
		}
		if _, err := f.Write(append(data, '\n')); err != nil {
			t.Fatalf("write rollout: %v", err)
		}
	}
}

// AppendTurn appends one complete user-triggered TUI turn in the current
// Codex rollout shape, modelled record-for-record on real 2026-11 rollouts:
//
//	event_msg task_started (turn_attribution trigger=user, root_turn_id)
//	turn_context
//	response_item message role=user (id + turn metadata, provenance user)
//	event_msg item_completed UserMessage
//	response_item reasoning (encrypted)
//	[event_msg item_completed AgentMessage + response_item message, phase commentary]
//	event_msg item_completed AgentMessage + response_item message, phase final_answer
//	event_msg token_count
//	event_msg task_complete
//
// Current Codex no longer writes user_message/agent_message event_msg
// duplicates; the item_completed events carry the same text instead.
func (r *Rollout) AppendTurn(t testing.TB, prompt, commentary, answer string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.turns++
	turnID := r.uuid()
	startedAt := r.clock
	passthrough := func(kind string) map[string]any {
		return map[string]any{"turn_id": turnID, "create_time": float64(r.clock.UnixNano()) / 1e9, "content_item_kinds": []any{kind}}
	}
	itemCompleted := func(item map[string]any) map[string]any {
		ms := r.clock.UnixMilli()
		return map[string]any{"type": "item_completed", "thread_id": r.SessionID, "turn_id": turnID, "item": item, "started_at_ms": ms, "completed_at_ms": ms}
	}
	agent := func(phase, text string) []map[string]any {
		id := "msg_" + r.hex(48)
		return []map[string]any{
			line(r.tick(), "event_msg", itemCompleted(map[string]any{
				"type": "AgentMessage", "id": id, "phase": phase,
				"content": []any{map[string]any{"type": "Text", "text": text}},
			})),
			line(r.tick(), "response_item", map[string]any{
				"type": "message", "status": "completed", "id": id, "role": "assistant",
				"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}, "logprobs": []any{}}},
				"phase":   phase, "internal_chat_message_metadata_passthrough": passthrough("unknown"),
			}),
		}
	}
	userID := "msg_" + r.uuid()
	userPassthrough := passthrough("user.text")
	userPassthrough["content_item_metadata"] = []any{map[string]any{"provenance": map[string]any{"type": "user"}}}
	recs := []map[string]any{
		line(r.tick(), "event_msg", map[string]any{
			"type": "task_started",
			"turn_attribution": map[string]any{
				"turn_id": turnID, "turn_trigger": "user", "parent_turn_id": nil,
				"initiating_agent_path": nil, "root_turn_id": turnID,
			},
			"turn_id": turnID, "root_turn_id": turnID, "started_at": startedAt.Unix(),
			"model_context_window": 258400, "collaboration_mode_kind": "default",
		}),
		line(r.tick(), "turn_context", map[string]any{
			"turn_id": turnID, "root_turn_id": turnID, "cwd": r.Cwd,
			"approval_policy": "on-request", "approvals_reviewer": "user",
			"sandbox_policy": map[string]any{"type": "workspace-write", "writable_roots": []any{}, "network_access": false},
			"model":          "gpt-5-codex", "effort": "medium", "summary": "auto",
		}),
		line(r.tick(), "response_item", map[string]any{
			"type": "message", "id": userID, "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": prompt}},
			"internal_chat_message_metadata_passthrough": userPassthrough,
		}),
		line(r.tick(), "event_msg", itemCompleted(map[string]any{
			"type": "UserMessage", "id": r.uuid(), "client_id": r.uuid(),
			"content": []any{map[string]any{"type": "text", "text": prompt, "text_elements": []any{}}},
		})),
		line(r.tick(), "response_item", map[string]any{
			"type": "reasoning", "id": "rs_" + r.hex(48), "summary": []any{},
			"encrypted_content": "gAAAAABe2e-not-real-reasoning",
		}),
	}
	if commentary != "" {
		recs = append(recs, agent("commentary", commentary)...)
	}
	recs = append(recs, agent("final_answer", answer)...)
	recs = append(recs,
		line(r.tick(), "event_msg", map[string]any{
			"type": "token_count",
			"info": map[string]any{
				"total_token_usage":    map[string]any{"input_tokens": 1200 * r.turns, "cached_input_tokens": 0, "output_tokens": 80 * r.turns, "reasoning_output_tokens": 20 * r.turns, "total_tokens": 1280 * r.turns},
				"last_token_usage":     map[string]any{"input_tokens": 1200, "cached_input_tokens": 0, "output_tokens": 80, "reasoning_output_tokens": 20, "total_tokens": 1280},
				"model_context_window": 258400,
			},
			"rate_limits": nil,
		}),
		line(r.tick(), "event_msg", map[string]any{
			"type": "task_complete", "root_turn_id": turnID, "turn_id": turnID, "last_agent_message": answer,
			"started_at": startedAt.Unix(), "completed_at": r.clock.Unix(),
			"duration_ms": r.clock.Sub(startedAt).Milliseconds(), "time_to_first_token_ms": 120,
		}),
	)
	r.append(t, recs...)
}

func (r *Rollout) uuid() string {
	r.seq++
	n := r.clock.UnixNano() + int64(r.seq)
	return fmt.Sprintf("019e2e%02x-%04x-7%03x-8%03x-%012x", r.seq&0xff, (n>>16)&0xffff, (n>>4)&0xfff, n&0xfff, n&0xffffffffffff)
}

func (r *Rollout) hex(n int) string {
	r.seq++
	const digits = "0123456789abcdef"
	x := uint64(r.clock.UnixNano()) ^ (uint64(r.seq) * 0x9e3779b97f4a7c15)
	b := make([]byte, n)
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = digits[x&15]
	}
	return string(b)
}
