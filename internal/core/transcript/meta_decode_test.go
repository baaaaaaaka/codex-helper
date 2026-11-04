package transcript

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// parseTwoPass is Parse as it was before the one-pass session_meta decode
// (W1b), kept verbatim as the reference the new decode must match.
func parseTwoPass(line []byte) Event {
	if ev, ok := parseHead(line, len(line) > cheapRecordAt); ok {
		return ev
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

// metaLines are session_meta lines that vary in everything the decode
// could treat differently: key order, source shapes, missing or odd
// fields, duplicate and case-folded keys, escapes, invalid JSON.
func metaLines() []string {
	big := strings.Repeat("You are Codex, a coding agent. ", 270) // about 8 KB like the real one
	return []string{
		// Codex order (timestamp, type, payload) and the writer's (sorted keys).
		`{"timestamp":"2026-11-04T10:00:00.123Z","type":"session_meta","payload":{"id":"019a-1","timestamp":"2026-11-04T10:00:00.100Z","cwd":"/w","originator":"codex_cli_rs","cli_version":"0.58.0","source":"cli","base_instructions":{"text":"` + big + `"},"git":{"commit_hash":"abc","branch":"main"}}}`,
		`{"payload":{"cli_version":"0.58.0","cwd":"/w","git":{"branch":"main"},"id":"019a-2","instructions":null,"model_provider":"openai","originator":"codex_vscode","source":"vscode","timestamp":"2026-11-04T10:00:00Z"},"timestamp":"2026-11-04T10:00:00Z","type":"session_meta"}`,
		// Source shapes.
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"s1","source":"exec","originator":"codex_exec"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"s2","source":{"subagent":{"thread_spawn":{"parent_thread_id":"p","depth":1}}},"originator":"codex_cli_rs"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"s3","source":{"subagent":"review"}}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"s4","source":"mcp"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"s5","source":null}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"s6","source":42}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"s7","source":["cli"]}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"s8"}}`,
		// Forks and times.
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"f1","forked_from_id":"parent-1","source":"cli","timestamp":"2026-11-04T09:00:00Z"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"t1","timestamp":"yesterday"}}`,
		`{"timestamp":"not a time","type":"session_meta","payload":{"id":"t2"}}`,
		`{"type":"session_meta","payload":{"id":"t3"}}`,
		`{"timestamp":12,"type":"session_meta","payload":{"id":"t4"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"t5","timestamp":7}}`,
		// Missing or odd ids and payloads.
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":""}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"cwd":"/w"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":5}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":null}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":"id"}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":[{"id":"a"}]}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta"}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"x","cwd":["/w"]}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"x","cwd":null,"originator":null}}`,
		// Duplicate and case-folded keys.
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"a","cwd":"/first"},"payload":{"id":"b"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"a","cwd":"/first"},"Payload":{"id":"b"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","PAYLOAD":{"id":"c","cwd":"/upper"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"a","cwd":"/first"},"p\u0061yload":{"id":"b"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"a","id":"b","ID":"c"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"a","git":{"payload":{"x":1}}}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","timestamp":"2026-11-05T10:00:00Z","type":"session_meta","payload":{"id":"a"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","Type":"event_msg","payload":{"id":"a"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"event_msg","type":"session_meta","payload":{"id":"a","type":"task_started"}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","type":"event_msg","payload":{"id":"a","type":"task_started","turn_id":"t"}}`,
		// Another record naming session_meta inside.
		`{"timestamp":"2026-11-04T10:00:00Z","type":"response_item","payload":{"type":"message","role":"user","id":"m","content":[{"type":"input_text","text":"x"}],"extra":{"type":"session_meta"}}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"\"type\":\"session_meta\""}]}}`,
		// Escapes, unicode and whitespace.
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"u1","cwd":"/w/\u00e9t\u00e9","base_instructions":{"text":"tab\tline\n"}}}`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"u2","cwd":"/w/été/日本"}}`,
		`{ "timestamp" : "2026-11-04T10:00:00Z" , "type" : "session_meta" , "payload" : { "id" : "w1" , "cwd" : "/w" } }`,
		// Invalid JSON.
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"bad"}} trailing`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"cut"`,
		`{"timestamp":"2026-11-04T10:00:00Z","type":"session_meta","payload":{"id":"x",}}`,
		`"type":"session_meta"`,
		``,
	}
}

func checkSameAsTwoPass(t *testing.T, where string, line []byte) {
	t.Helper()
	got, want := Parse(line), parseTwoPass(line)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: one-pass decode differs\nline: %.300s\n got: %+v %+v\nwant: %+v %+v", where, line, got, got.Meta, want, want.Meta)
	}
}

// W1b: the one-pass session_meta decode classifies every line exactly as
// the two-pass decode did (kind, times, ids, source, fork, cwd).
func TestSessionMetaDecodedOnceAsBefore(t *testing.T) {
	lines := metaLines()
	for i, l := range lines {
		checkSameAsTwoPass(t, fmt.Sprintf("line %d", i), []byte(l))
	}
	// The usual lines take the one-pass decode; a possible second payload
	// key or a \u escape falls back to the two-pass decode.
	for i, want := range map[int]bool{0: true, 1: true, 2: true, 3: true, 25: false, 26: false, 28: false, 36: false} {
		if _, ok := parseMetaOnce([]byte(lines[i])); ok != want {
			t.Errorf("line %d one-pass = %v, want %v: %.120s", i, ok, want, lines[i])
		}
	}
}

// W1b: over the e2e rollout fixtures (the writer the harness and the fake
// app-server use, every source and originator they write, with turns),
// every record parses as before.
func TestRolloutFixturesParseAsBefore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sessions")
	opts := []rollout.Options{
		{},
		{Source: "vscode", Originator: "codex_vscode"},
		{Source: "exec", Originator: "codex_exec"},
		{Source: "mcp", Originator: "codex-helper"},
		{Source: "subagent"},
		{ID: "019e2e00-0000-7000-8000-000000000001", Start: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
	}
	lines := 0
	for i, o := range opts {
		w, err := rollout.New(root, "/work/é", o)
		if err != nil {
			t.Fatal(err)
		}
		for k := 0; k < 3; k++ {
			if _, err := w.AppendTurn(rollout.Turn{Prompt: "prompt", Commentary: "looking", Answer: "answer", Aborted: k == 2 && i%2 == 0}); err != nil {
				t.Fatal(err)
			}
		}
		f, err := os.Open(w.Path)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(nil, 1<<24)
		for sc.Scan() {
			line := bytes.Clone(sc.Bytes())
			checkSameAsTwoPass(t, filepath.Base(w.Path), line)
			lines++
		}
		f.Close()
		if err := sc.Err(); err != nil {
			t.Fatal(err)
		}
		meta, err := ReadMeta(w.Path)
		if err != nil || meta.ID != w.SessionID {
			t.Fatalf("ReadMeta(%s) = %+v, %v", w.Path, meta, err)
		}
	}
	if lines < 6*10 {
		t.Fatalf("only %d fixture lines", lines)
	}
}

// W1b: a session_meta line is decoded once. The two-pass decode copied
// the whole payload (about 8 KB in a real rollout) before decoding it
// again; the one-pass decode allocates only the fields it keeps.
func TestSessionMetaDecodeDoesNotCopyThePayload(t *testing.T) {
	line := []byte(metaLines()[0])
	if ev := Parse(line); ev.Kind != KindSessionMeta {
		t.Fatalf("kind %v", ev.Kind)
	}
	const runs = 200
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		Parse(line)
	}
	runtime.ReadMemStats(&after)
	if per := (after.TotalAlloc - before.TotalAlloc) / runs; per > uint64(len(line))/2 {
		t.Fatalf("%d bytes allocated per decode of a %d-byte session_meta line, want under %d", per, len(line), len(line)/2)
	}
}
