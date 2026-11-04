// Package transcript reads Codex rollout files (sessions/YYYY/MM/DD/
// rollout-*.jsonl): record framing from a byte offset, the few record kinds
// the helper mirrors, rewrite detection and discovery classification.
//
// It never writes and has no dependency on the store: the engine persists
// offsets with store.AdvanceSource together with the outputs a Tracker
// produced.
package transcript

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

// Kind is the kind of a parsed record.
type Kind int

const (
	KindOther        Kind = iota // anything the mirror does not need
	KindSessionMeta              // first record: thread id, source, originator
	KindTurnStarted              // event_msg task_started
	KindUserMessage              // a user prompt (item_completed UserMessage or legacy user_message)
	KindAgentMessage             // an assistant message (item_completed AgentMessage or legacy agent_message)
	KindTurnComplete             // event_msg task_complete
	KindTurnAborted              // event_msg turn_aborted
)

func (k Kind) String() string {
	return [...]string{"other", "session_meta", "turn_started", "user_message", "agent_message", "turn_complete", "turn_aborted"}[k]
}

// SessionMeta is the session_meta payload.
type SessionMeta struct {
	ID         string
	Source     string // "cli", "vscode", "exec", "subagent" (object sources are reported as their key)
	Originator string
	Cwd        string
	ForkedFrom string
	Time       time.Time
}

// Event is one parsed record.
type Event struct {
	Kind     Kind
	Time     time.Time
	TurnID   string
	ThreadID string
	ClientID string // UserMessage client_id (the turn/start clientUserMessageId)
	Phase    string // AgentMessage phase: commentary | final_answer | ""
	Text     string // message text; TurnComplete: last_agent_message
	Legacy   bool   // legacy event_msg user_message / agent_message
	Meta     *SessionMeta
}

type envelope struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type eventPayload struct {
	Type             string          `json:"type"`
	TurnID           string          `json:"turn_id"`
	ThreadID         string          `json:"thread_id"`
	Message          string          `json:"message"`
	Phase            string          `json:"phase"`
	LastAgentMessage *string         `json:"last_agent_message"`
	Item             *itemPayload    `json:"item"`
	Kind             json.RawMessage `json:"kind"`
}

type itemPayload struct {
	Type     string `json:"type"`
	ClientID string `json:"client_id"`
	Phase    string `json:"phase"`
	Content  []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

type metaPayload struct {
	ID         string          `json:"id"`
	Timestamp  string          `json:"timestamp"`
	Cwd        string          `json:"cwd"`
	Originator string          `json:"originator"`
	Source     json.RawMessage `json:"source"`
	ForkedFrom string          `json:"forked_from_id"`
}

// topType returns the value of the first "type" key of a record line
// without decoding it, so large tool records are skipped cheaply. Codex
// writes timestamp (and ordinal) before type, so the first "type" key is
// the top-level one. ok is false when the line does not look like that.
func topType(line []byte) (string, bool) {
	head := line
	if len(head) > 256 {
		head = head[:256]
	}
	i := bytes.Index(head, []byte(`"type":`))
	if i < 0 {
		return "", false
	}
	if bytes.Count(head[:i], []byte("{")) != 1 {
		return "", false // the key belongs to a nested object
	}
	rest := bytes.TrimLeft(head[i+len(`"type":`):], " ")
	if len(rest) == 0 || rest[0] != '"' {
		return "", false
	}
	end := bytes.IndexByte(rest[1:], '"')
	if end < 0 {
		return "", false
	}
	return string(rest[1 : 1+end]), true
}

// Parse decodes one record line. Lines that are not JSON or not relevant
// are KindOther; Parse never fails.
func Parse(line []byte) Event {
	if t, ok := topType(line); ok && t != "event_msg" && t != "session_meta" {
		return Event{Kind: KindOther}
	}
	var env envelope
	if err := json.Unmarshal(line, &env); err != nil {
		return Event{Kind: KindOther}
	}
	ts, _ := time.Parse(time.RFC3339Nano, env.Timestamp)
	switch env.Type {
	case "session_meta":
		var m metaPayload
		if json.Unmarshal(env.Payload, &m) != nil || m.ID == "" {
			return Event{Kind: KindOther, Time: ts}
		}
		meta := &SessionMeta{ID: m.ID, Cwd: m.Cwd, Originator: m.Originator, Source: sourceName(m.Source), ForkedFrom: m.ForkedFrom}
		meta.Time, _ = time.Parse(time.RFC3339Nano, m.Timestamp)
		if meta.Time.IsZero() {
			meta.Time = ts
		}
		return Event{Kind: KindSessionMeta, Time: ts, ThreadID: m.ID, Meta: meta}
	case "event_msg":
	default:
		return Event{Kind: KindOther, Time: ts}
	}
	var p eventPayload
	if json.Unmarshal(env.Payload, &p) != nil {
		return Event{Kind: KindOther, Time: ts}
	}
	ev := Event{Time: ts, TurnID: p.TurnID, ThreadID: p.ThreadID}
	switch p.Type {
	case "task_started":
		ev.Kind = KindTurnStarted
	case "task_complete":
		ev.Kind = KindTurnComplete
		if p.LastAgentMessage != nil {
			ev.Text = *p.LastAgentMessage
		}
	case "turn_aborted":
		ev.Kind = KindTurnAborted
	case "user_message":
		ev.Kind, ev.Text, ev.Legacy = KindUserMessage, p.Message, true
	case "agent_message":
		ev.Kind, ev.Text, ev.Phase, ev.Legacy = KindAgentMessage, p.Message, p.Phase, true
	case "item_completed":
		if p.Item == nil {
			return Event{Kind: KindOther, Time: ts}
		}
		var b strings.Builder
		for _, c := range p.Item.Content {
			if strings.EqualFold(c.Type, "text") {
				b.WriteString(c.Text)
			}
		}
		switch p.Item.Type {
		case "UserMessage":
			ev.Kind, ev.ClientID = KindUserMessage, p.Item.ClientID
		case "AgentMessage":
			ev.Kind, ev.Phase = KindAgentMessage, p.Item.Phase
		default:
			return Event{Kind: KindOther, Time: ts}
		}
		ev.Text = b.String()
	default:
		ev.Kind = KindOther
	}
	return ev
}

// sourceName flattens session_meta.source: a string, or an object whose
// single key names the source ({"subagent": {...}}).
func sourceName(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) == nil {
		for k := range m {
			return k
		}
	}
	return ""
}

// Interactive reports whether a session was driven by a person (a TUI or
// IDE session) and is therefore mirrored when it is not already linked to a
// conversation: not a subagent or exec session, and not started by the
// helper itself.
func (m SessionMeta) Interactive() bool {
	src := strings.ToLower(m.Source)
	switch {
	case src == "exec", src == "subagent", strings.HasPrefix(src, "sub_agent"), src == "mcp":
		return false
	}
	orig := strings.ToLower(m.Originator)
	if orig == "codex-helper" || orig == "codex_helper" || orig == "codex_exec" {
		return false
	}
	return true
}

// IsBoilerplate reports user-role text Codex injects itself (environment
// context, instructions) that is not a person's prompt.
func IsBoilerplate(text string) bool {
	t := strings.TrimSpace(text)
	for _, p := range []string{"<environment_context>", "<user_instructions>", "<turn_aborted>", "# AGENTS.md instructions", "<INSTRUCTIONS>"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}
