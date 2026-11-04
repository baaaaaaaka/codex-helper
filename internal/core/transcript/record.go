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
	"strconv"
	"strings"
	"time"
)

// Kind is the kind of a parsed record.
type Kind int

const (
	KindOther        Kind = iota // anything the mirror does not need
	KindSessionMeta              // first record: thread id, source, originator
	KindTurnStarted              // event_msg task_started or turn_context: a turn begins
	KindUserMessage              // response_item message role=user: a prompt record
	KindUserItem                 // event_msg item_completed UserMessage: carries client_id
	KindAgentMessage             // response_item message role=assistant
	KindTurnComplete             // event_msg task_complete
	KindTurnAborted              // event_msg turn_aborted
	KindTurnRef                  // another event_msg naming the turn (item_completed AgentMessage, ...)
)

func (k Kind) String() string {
	return [...]string{"other", "session_meta", "turn_started", "user_message", "user_item", "agent_message", "turn_complete", "turn_aborted", "turn_ref"}[k]
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
//
// The mirror reads prompts and answers from response_item message records
// (like the converter, STORE-DESIGN §3.9) because they carry the record id
// used in mirror delivery keys; the event_msg duplicates only contribute
// turn boundaries and the client id of a prompt.
type Event struct {
	Kind     Kind
	Time     time.Time
	TurnID   string
	ThreadID string // item_completed thread id (a subagent's item has another)
	RecordID string // response_item id ("msg_…"), may be empty
	ClientID string // UserItem client_id (the turn/start clientUserMessageId)
	Phase    string // AgentMessage phase: commentary | final_answer | ""
	Text     string // message text; TurnComplete: last_agent_message
	HasLast  bool   // TurnComplete carried a non-null last_agent_message
	Meta     *SessionMeta
}

type envelope struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type payload struct {
	Type             string       `json:"type"`
	ID               string       `json:"id"`
	Role             string       `json:"role"`
	Phase            string       `json:"phase"`
	TurnID           string       `json:"turn_id"`
	ThreadID         string       `json:"thread_id"`
	LastAgentMessage *string      `json:"last_agent_message"`
	Item             *itemPayload `json:"item"`
	Content          []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

type itemPayload struct {
	Type     string `json:"type"`
	ClientID string `json:"client_id"`
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

var jMessage = []byte(`"type":"message"`)

// Parse decodes one record line. Lines that are not JSON or not relevant
// are KindOther; Parse never fails.
func Parse(line []byte) Event {
	if t, ok := topType(line); ok {
		switch t {
		case "event_msg", "session_meta", "turn_context":
		case "response_item":
			head := line
			if len(head) > 512 {
				head = head[:512]
			}
			if !bytes.Contains(head, jMessage) {
				return Event{Kind: KindOther} // tool calls and outputs, reasoning
			}
		default:
			return Event{Kind: KindOther}
		}
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
	case "event_msg", "turn_context", "response_item":
	default:
		return Event{Kind: KindOther, Time: ts}
	}
	var p payload
	if json.Unmarshal(env.Payload, &p) != nil {
		return Event{Kind: KindOther, Time: ts}
	}
	ev := Event{Time: ts, TurnID: p.TurnID, ThreadID: p.ThreadID}
	switch env.Type {
	case "turn_context":
		ev.Kind = KindTurnStarted
		return ev
	case "response_item":
		if p.Type != "message" {
			return Event{Kind: KindOther, Time: ts}
		}
		var b strings.Builder
		for _, c := range p.Content {
			b.WriteString(c.Text)
		}
		ev.Text, ev.RecordID, ev.Phase = b.String(), p.ID, p.Phase
		switch p.Role {
		case "user":
			ev.Kind = KindUserMessage
		case "assistant":
			ev.Kind = KindAgentMessage
		default:
			return Event{Kind: KindOther, Time: ts}
		}
		return ev
	}
	switch p.Type {
	case "task_started":
		ev.Kind = KindTurnStarted
	case "task_complete":
		ev.Kind = KindTurnComplete
		if p.LastAgentMessage != nil {
			ev.Text, ev.HasLast = *p.LastAgentMessage, true
		}
	case "turn_aborted":
		ev.Kind = KindTurnAborted
	case "item_completed":
		if p.Item != nil && p.Item.Type == "UserMessage" {
			ev.Kind, ev.ClientID = KindUserItem, p.Item.ClientID
		} else {
			ev.Kind = KindTurnRef
		}
	default:
		if p.TurnID == "" {
			return Event{Kind: KindOther, Time: ts}
		}
		ev.Kind = KindTurnRef
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

// Subagent reports a rollout that was not driven through a conversation:
// a subagent or exec session. Such a rollout is never tracked, even when it
// is linked to a conversation through its parent's session id (§9.4).
func (m SessionMeta) Subagent() bool {
	src := strings.ToLower(m.Source)
	return src == "exec" || src == "subagent" || strings.HasPrefix(src, "sub_agent") || src == "mcp"
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
// context, AGENTS.md, aborted-turn markers) that is not a person's prompt.
// Same rule as the converter (migrate.injectedContext).
func IsBoilerplate(text string) bool {
	t := strings.TrimSpace(text)
	return strings.HasPrefix(t, "<") || strings.HasPrefix(t, "# AGENTS.md instructions")
}

// LegacyTeamsPrompt recognises a prompt written by the legacy Teams helper
// by the wrapper it added (rc.59 artifact_handoff.go and history_watch.go;
// copied from migrate.LegacyTeamsPrompt, STORE-DESIGN §3.9). Legacy turns
// carry no client id, so this is the fallback after the client id. The
// markers include their line breaks so a local prompt that merely quotes
// one is not taken for a Teams turn.
func LegacyTeamsPrompt(text string) bool {
	for _, m := range legacyTeamsMarkers {
		if strings.Contains(text, m) {
			return true
		}
	}
	return strings.HasPrefix(text, "[codex-helper-control]") ||
		strings.HasPrefix(text, "You are handling an unrecognized message from the user's Microsoft Teams control chat for codex-helper.")
}

var legacyTeamsMarkers = []string{
	"\n\nTeams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.",
	"\n\nIf you need to return generated files or images to the Teams user, write them under this local directory:\n",
	"<codex-helper-control-context",
}

// MirrorKeyPrefix is the per-record prefix of a mirror delivery key
// (STORE-DESIGN §3.9): a record counts as delivered when any outbox key with
// this prefix exists. Identical to migrate.MirrorKeyPrefix.
func MirrorKeyPrefix(threadID, recordID string) string {
	return "mirror:" + threadID + ":" + recordID + ":"
}

// MirrorKey is the delivery key of one part (1-based) of a mirrored record.
func MirrorKey(threadID, recordID string, part int) string {
	return MirrorKeyPrefix(threadID, recordID) + strconv.Itoa(part)
}

// turnRecordID is the record id of a prompt or final without a
// response-item id: "t:<turn>:final" or "t:<turn>:prompt:<line>" (line is
// 1-based), as in the converter.
func turnRecordID(turn string, final bool, line int64) string {
	if turn == "" {
		turn = "?"
	}
	if final {
		return "t:" + turn + ":final"
	}
	return "t:" + turn + ":prompt:" + strconv.FormatInt(line, 10)
}
