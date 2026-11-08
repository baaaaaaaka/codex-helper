// Package rollout writes Codex rollout JSONL files the way a current Codex
// CLI / app-server does (rollout v2 with turn ids and message phases). It is
// shared by the harness (to play a local TUI user) and by the fake Codex
// app-server (which, like real Codex, records every thread it runs).
//
// It deliberately has no dependency on package testing so the fake
// app-server binary can use it.
package rollout

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Options configures a new rollout file.
type Options struct {
	// ID is the session/thread id. Empty generates a UUIDv7-shaped id.
	ID string
	// Start is the timestamp of the first record. Zero means now.
	Start time.Time
	// Source and Originator go into session_meta. Defaults: "cli",
	// "codex_cli_rs" (a TUI session).
	Source     string
	Originator string
}

// Writer appends records to one rollout file.
type Writer struct {
	mu        sync.Mutex
	Path      string
	SessionID string
	Cwd       string
	turns     int
	seq       int
	ordinal   int
	clock     time.Time
	realtime  bool
	written   []Written
	// chunks are the bytes this Writer appended, in order (hash and
	// length), so Intact can tell whether anyone rewrote them.
	chunks []chunk
}

type chunk struct {
	sum [sha256.Size]byte
	n   int
}

// New creates sessions/YYYY/MM/DD/rollout-<ts>-<id>.jsonl under sessionsDir
// with the session_meta and environment-context preamble.
func New(sessionsDir, cwd string, opts Options) (*Writer, error) {
	now := opts.Start.UTC()
	realtime := opts.Start.IsZero()
	if realtime {
		now = time.Now().UTC()
	}
	id := opts.ID
	if id == "" {
		id = fmt.Sprintf("019e2e%02d-%04x-7%03x-8%03x-%012x", now.Second(), now.Nanosecond()&0xffff, (now.Nanosecond()>>16)&0xfff, now.Nanosecond()&0xfff, now.UnixNano()&0xffffffffffff)
	}
	source := opts.Source
	if source == "" {
		source = "cli"
	}
	originator := opts.Originator
	if originator == "" {
		originator = "codex_cli_rs"
	}
	dir := filepath.Join(sessionsDir, now.Format("2006"), now.Format("01"), now.Format("02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &Writer{
		Path:      filepath.Join(dir, "rollout-"+now.Format("2006-01-02T15-04-05")+"-"+id+".jsonl"),
		SessionID: id,
		Cwd:       cwd,
		clock:     now,
		realtime:  realtime,
	}
	ts := w.tick()
	err := w.append(
		line(ts, "session_meta", map[string]any{
			"id":             id,
			"timestamp":      ts,
			"cwd":            cwd,
			"originator":     originator,
			"cli_version":    "0.58.0",
			"instructions":   nil,
			"source":         source,
			"model_provider": "openai",
			"git":            map[string]any{"commit_hash": "e2e0000", "branch": "main"},
		}),
		line(w.tick(), "response_item", map[string]any{
			"type": "message",
			"role": "user",
			"content": []any{map[string]any{
				"type": "input_text",
				"text": "<environment_context>\n  <cwd>" + cwd + "</cwd>\n  <approval_policy>on-request</approval_policy>\n  <sandbox_mode>workspace-write</sandbox_mode>\n  <network_access>restricted</network_access>\n  <shell>bash</shell>\n</environment_context>",
			}},
		}),
	)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Open re-opens an existing rollout for appending (used by the fake
// app-server after a restart). The ordinal counter restarts; Codex readers do
// not depend on it.
func Open(path, id, cwd string) *Writer {
	return &Writer{Path: path, SessionID: id, Cwd: cwd, clock: time.Now().UTC(), realtime: true}
}

func (w *Writer) tick() string {
	w.clock = w.clock.Add(37 * time.Millisecond)
	if w.realtime {
		if now := time.Now().UTC(); now.After(w.clock) {
			w.clock = now
		}
	}
	return w.clock.Format("2006-01-02T15:04:05.000Z")
}

func line(ts, typ string, payload any) map[string]any {
	return map[string]any{"timestamp": ts, "type": typ, "payload": payload}
}

func (w *Writer) append(records ...map[string]any) error {
	f, err := os.OpenFile(w.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, rec := range records {
		w.ordinal++
		rec["ordinal"] = w.ordinal
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		data = append(data, '\n')
		if _, err := f.Write(data); err != nil {
			return err
		}
		w.chunks = append(w.chunks, chunk{sum: sha256.Sum256(data), n: len(data)})
	}
	return nil
}

// AppendRecord appends one record of type typ ("response_item",
// "event_msg", ...) with payload, stamped with the Writer's clock: e.g. a
// tool call or a large tool output in the middle of a turn.
func (w *Writer) AppendRecord(typ string, payload map[string]any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.append(line(w.tick(), typ, payload))
}

// AppendRaw appends data verbatim: a malformed record, or the first part
// of a record whose newline is not written yet (a record Codex is still
// writing).
func (w *Writer) AppendRaw(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, err := os.OpenFile(w.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	w.chunks = append(w.chunks, chunk{sum: sha256.Sum256(data), n: len(data)})
	return nil
}

// Replace plays Codex replacing the rollout file (e.g. a legacy rollout
// rewritten as a paginated one): edit gets the current lines (without
// newlines) and returns the new file's lines, which are written to a new
// file renamed over the old path (a new inode). What this Writer appended
// before is forgotten: the new file is its content from now on.
func (w *Writer) Replace(edit func(lines [][]byte) [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := os.ReadFile(w.Path)
	if err != nil {
		return err
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	var out []byte
	for _, l := range edit(lines) {
		out = append(append(out, l...), '\n')
	}
	tmp := w.Path + ".replace"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.Path); err != nil {
		return err
	}
	w.chunks = []chunk{{sum: sha256.Sum256(out), n: len(out)}}
	return nil
}

// Intact reports whether everything this Writer appended (since it was
// created or last Replaced) is still in the file, byte for byte and in
// order. Records others appended in between (Codex continuing the same
// session) are allowed; a rewrite, truncation or edit of the local
// history is not (R-44).
func (w *Writer) Intact() error {
	w.mu.Lock()
	chunks := append([]chunk(nil), w.chunks...)
	w.mu.Unlock()
	if len(chunks) == 0 {
		return nil
	}
	data, err := os.ReadFile(w.Path)
	if err != nil {
		return err
	}
	off := 0
	for i, c := range chunks {
		found := false
		for off+c.n <= len(data) {
			if sha256.Sum256(data[off:off+c.n]) == c.sum {
				found = true
				break
			}
			// Others only append whole lines: resume at the next line.
			nl := bytes.IndexByte(data[off:], '\n')
			if nl < 0 {
				break
			}
			off += nl + 1
		}
		if !found {
			return fmt.Errorf("%s: write %d of %d (%d bytes) is no longer in the file (size %d)", filepath.Base(w.Path), i+1, len(chunks), c.n, len(data))
		}
		off += c.n
	}
	return nil
}

// Written is one text a Writer wrote, in order: what a local Codex user
// produced, for the harness's order and catch-up invariants.
type Written struct {
	At   time.Time // wall-clock time of the write
	Kind string    // "prompt", "commentary", "answer" or "aborted"
	Text string
}

func (w *Writer) logWritten(kind, text string) {
	w.written = append(w.written, Written{At: time.Now(), Kind: kind, Text: text})
}

// Written returns everything this Writer wrote so far.
func (w *Writer) Written() []Written {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]Written(nil), w.written...)
}

// Turn describes one turn to append.
type Turn struct {
	Unphased bool
	// ID is the Codex turn id. Empty generates one.
	ID string
	// ClientID is the turn/start clientUserMessageId, recorded as client_id
	// on the UserMessage item. Empty generates a random one (TUI turns).
	ClientID   string
	Prompt     string
	Commentary string
	Answer     string
	// Aborted writes the turn start and the user message, then a
	// turn_aborted event instead of an answer (an interrupted turn).
	Aborted bool
	// Images are data URLs pasted into the prompt (input_image items of
	// the user message, as the TUI records a pasted screenshot).
	Images []string
}

// AppendTurn appends one complete user-triggered turn in the current Codex
// rollout shape (BeginTurn followed by FinishTurn), modelled record-for-record
// on real 2026-11 rollouts:
//
//	event_msg task_started (turn_attribution trigger=user, root_turn_id)
//	turn_context
//	response_item message role=user (id + turn metadata, provenance user)
//	event_msg item_completed UserMessage (client_id = clientUserMessageId)
//	response_item reasoning (encrypted)
//	[event_msg item_completed AgentMessage + response_item message, phase commentary]
//	event_msg item_completed AgentMessage + response_item message, phase final_answer
//	event_msg token_count
//	event_msg task_complete
//
// Current Codex no longer writes user_message/agent_message event_msg
// duplicates; the item_completed events carry the same text instead.
func (w *Writer) AppendTurn(turn Turn) (string, error) {
	open, err := w.BeginTurn(turn)
	if err != nil {
		return open.ID, err
	}
	return open.ID, w.FinishTurn(open, turn)
}

// OpenTurn is a turn whose start records were written.
type OpenTurn struct {
	ID        string
	startedAt time.Time
}

// BeginTurn writes the records Codex appends when a turn starts (through the
// user message and the first reasoning item). A process killed mid-turn
// leaves exactly these.
func (w *Writer) BeginTurn(turn Turn) (OpenTurn, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.turns++
	turnID := turn.ID
	if turnID == "" {
		turnID = w.uuid()
	}
	startedAt := w.clock
	w.logWritten("prompt", turn.Prompt)
	userID := "msg_" + w.uuid()
	clientID := turn.ClientID
	if clientID == "" {
		clientID = w.uuid()
	}
	userPassthrough := w.passthrough(turnID, "user.text")
	userPassthrough["content_item_metadata"] = []any{map[string]any{"provenance": map[string]any{"type": "user"}}}
	content := []any{map[string]any{"type": "input_text", "text": turn.Prompt}}
	for _, img := range turn.Images {
		content = append(content, map[string]any{"type": "input_image", "image_url": img})
	}
	recs := []map[string]any{
		line(w.tick(), "event_msg", map[string]any{
			"type": "task_started",
			"turn_attribution": map[string]any{
				"turn_id": turnID, "turn_trigger": "user", "parent_turn_id": nil,
				"initiating_agent_path": nil, "root_turn_id": turnID,
			},
			"turn_id": turnID, "root_turn_id": turnID, "started_at": startedAt.Unix(),
			"model_context_window": 258400, "collaboration_mode_kind": "default",
		}),
		line(w.tick(), "turn_context", map[string]any{
			"turn_id": turnID, "root_turn_id": turnID, "cwd": w.Cwd,
			"approval_policy": "on-request", "approvals_reviewer": "user",
			"sandbox_policy": map[string]any{"type": "workspace-write", "writable_roots": []any{}, "network_access": false},
			"model":          "gpt-5-codex", "effort": "medium", "summary": "auto",
		}),
		line(w.tick(), "response_item", map[string]any{
			"type": "message", "id": userID, "role": "user",
			"content": content,
			"internal_chat_message_metadata_passthrough": userPassthrough,
		}),
		line(w.tick(), "event_msg", w.itemCompleted(turnID, map[string]any{
			"type": "UserMessage", "id": w.uuid(), "client_id": clientID,
			"content": []any{map[string]any{"type": "text", "text": turn.Prompt, "text_elements": []any{}}},
		})),
		line(w.tick(), "response_item", map[string]any{
			"type": "reasoning", "id": "rs_" + w.hex(48), "summary": []any{},
			"encrypted_content": "gAAAAABe2e-not-real-reasoning",
		}),
	}
	return OpenTurn{ID: turnID, startedAt: startedAt}, w.append(recs...)
}

// FinishTurn writes the rest of a turn begun with BeginTurn: the agent
// messages, token count and task_complete, or turn_aborted when
// turn.Aborted.
func (w *Writer) FinishTurn(open OpenTurn, turn Turn) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.append(w.finishRecords(open, turn)...)
}

// RenderFinish returns the bytes FinishTurn would append, without writing
// them, so a scenario can write them in pieces (AppendRaw) the way a
// record still being written looks to a reader. The turn counts as
// written now.
func (w *Writer) RenderFinish(open OpenTurn, turn Turn) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []byte
	for _, rec := range w.finishRecords(open, turn) {
		w.ordinal++
		rec["ordinal"] = w.ordinal
		data, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		out = append(append(out, data...), '\n')
	}
	return out, nil
}

func (w *Writer) finishRecords(open OpenTurn, turn Turn) []map[string]any {
	turnID := open.ID
	if turn.Aborted {
		w.logWritten("aborted", "")
		return []map[string]any{line(w.tick(), "event_msg", map[string]any{
			"type": "turn_aborted", "turn_id": turnID, "reason": "interrupted",
		})}
	}
	var recs []map[string]any
	if turn.Commentary != "" {
		w.logWritten("commentary", turn.Commentary)
		recs = append(recs, w.agent(turnID, "commentary", turn.Commentary)...)
	}
	w.logWritten("answer", turn.Answer)
	recs = append(recs, w.agent(turnID, finalPhase(turn.Unphased), turn.Answer)...)
	recs = append(recs,
		line(w.tick(), "event_msg", map[string]any{
			"type": "token_count",
			"info": map[string]any{
				"total_token_usage":    map[string]any{"input_tokens": 1200 * w.turns, "cached_input_tokens": 0, "output_tokens": 80 * w.turns, "reasoning_output_tokens": 20 * w.turns, "total_tokens": 1280 * w.turns},
				"last_token_usage":     map[string]any{"input_tokens": 1200, "cached_input_tokens": 0, "output_tokens": 80, "reasoning_output_tokens": 20, "total_tokens": 1280},
				"model_context_window": 258400,
			},
			"rate_limits": nil,
		}),
		line(w.tick(), "event_msg", map[string]any{
			"type": "task_complete", "root_turn_id": turnID, "turn_id": turnID, "last_agent_message": turn.Answer,
			"started_at": open.startedAt.Unix(), "completed_at": w.clock.Unix(),
			"duration_ms": w.clock.Sub(open.startedAt).Milliseconds(), "time_to_first_token_ms": 120,
		}),
	)
	return recs
}

func (w *Writer) passthrough(turnID, kind string) map[string]any {
	return map[string]any{"turn_id": turnID, "create_time": float64(w.clock.UnixNano()) / 1e9, "content_item_kinds": []any{kind}}
}

func (w *Writer) itemCompleted(turnID string, item map[string]any) map[string]any {
	ms := w.clock.UnixMilli()
	return map[string]any{"type": "item_completed", "thread_id": w.SessionID, "turn_id": turnID, "item": item, "started_at_ms": ms, "completed_at_ms": ms}
}

func (w *Writer) agent(turnID, phase, text string) []map[string]any {
	id := "msg_" + w.hex(48)
	return []map[string]any{
		line(w.tick(), "event_msg", w.itemCompleted(turnID, map[string]any{
			"type": "AgentMessage", "id": id, "phase": phase,
			"content": []any{map[string]any{"type": "Text", "text": text}},
		})),
		line(w.tick(), "response_item", map[string]any{
			"type": "message", "status": "completed", "id": id, "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}, "logprobs": []any{}}},
			"phase":   phase, "internal_chat_message_metadata_passthrough": w.passthrough(turnID, "unknown"),
		}),
	}
}

func (w *Writer) uuid() string {
	w.seq++
	n := w.clock.UnixNano() + int64(w.seq)
	return fmt.Sprintf("019e2e%02x-%04x-7%03x-8%03x-%012x", w.seq&0xff, (n>>16)&0xffff, (n>>4)&0xfff, n&0xfff, n&0xffffffffffff)
}

func (w *Writer) hex(n int) string {
	w.seq++
	const digits = "0123456789abcdef"
	x := uint64(w.clock.UnixNano()) ^ (uint64(w.seq) * 0x9e3779b97f4a7c15)
	b := make([]byte, n)
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = digits[x&15]
	}
	return string(b)
}

// SetModTime sets the file's mtime (and atime) to t, e.g. to make a session
// look last-updated in the past.
func (w *Writer) SetModTime(t time.Time) error {
	return os.Chtimes(w.Path, t, t)
}

func finalPhase(unphased bool) string {
	if unphased {
		return ""
	}
	return "final_answer"
}
