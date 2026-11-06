package transcript

import (
	"os"
	"strings"
	"testing"
)

// Selection rules ported from v0.1.20's transcript sync (status/commentary
// and compaction notices); scripts/fidelity/select_diff.py checks them
// against the owner's real data.

const selTS = `{"timestamp":"2026-09-01T00:00:00Z",`

func selTurnStart(turn string) string {
	return selTS + `"type":"event_msg","payload":{"type":"task_started","turn_id":"` + turn + `"}}` + "\n"
}

func selTurnContext(turn string) string {
	return selTS + `"type":"turn_context","payload":{"turn_id":"` + turn + `","cwd":"/w"}}` + "\n"
}

func selPrompt(id, text string) string {
	idf := ""
	if id != "" {
		idf = `"id":"` + id + `",`
	}
	return selTS + `"type":"response_item","payload":{"type":"message",` + idf + `"role":"user","content":[{"type":"input_text","text":"` + text + `"}]}}` + "\n"
}

func selUserEvent(text string) string {
	return selTS + `"type":"event_msg","payload":{"type":"user_message","message":"` + text + `","images":[]}}` + "\n"
}

func selUserItem(turn, clientID string) string {
	return selTS + `"type":"event_msg","payload":{"type":"item_completed","turn_id":"` + turn + `","item":{"type":"UserMessage","client_id":"` + clientID + `"}}}` + "\n"
}

func selAgent(id, phase, text string) string {
	idf := ""
	if id != "" {
		idf = `"id":"` + id + `",`
	}
	return selTS + `"type":"response_item","payload":{"type":"message",` + idf + `"role":"assistant","phase":"` + phase + `","content":[{"type":"output_text","text":"` + text + `"}]}}` + "\n"
}

func selAgentEvent(phase, text string) string {
	return selTS + `"type":"event_msg","payload":{"type":"agent_message","message":"` + text + `","phase":"` + phase + `"}}` + "\n"
}

func selAgentItem(turn, phase, text string) string {
	return selTS + `"type":"event_msg","payload":{"type":"item_completed","turn_id":"` + turn + `","item":{"type":"AgentMessage","id":"x","phase":"` + phase + `","content":[{"type":"Text","text":"` + text + `"}]}}}` + "\n"
}

func selComplete(turn, last string) string {
	return selTS + `"type":"event_msg","payload":{"type":"task_complete","turn_id":"` + turn + `","last_agent_message":"` + last + `"}}` + "\n"
}

func selCompacted(history string) string {
	return selTS + `"type":"compacted","payload":{"message":"","replacement_history":[{"type":"message","role":"user","content":[{"type":"input_text","text":"` + history + `"}],"internal_chat_message_metadata_passthrough":{"turn_id":"old-turn"}}]}}` + "\n"
}

var (
	selContextCompacted = selTS + `"type":"event_msg","payload":{"type":"context_compacted"}}` + "\n"
	selWorldState       = selTS + `"type":"world_state","payload":{"full":true}}` + "\n"
	selTokenCount       = selTS + `"type":"event_msg","payload":{"type":"token_count","info":null}}` + "\n"
	selReasoning        = selTS + `"type":"response_item","payload":{"type":"reasoning","summary":[],"encrypted_content":"x"}}` + "\n"
	selToolCall         = selTS + `"type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{}","call_id":"c1"}}` + "\n"
)

func selRead(t *testing.T, skip Skip, lines ...string) []Output {
	t.Helper()
	return readAll(t, writeLines(t, lines...), NewTracker("th", 0, 0, skip))
}

func selIDs(out []Output) string {
	var s []string
	for _, o := range out {
		s = append(s, o.RecordID)
	}
	return strings.Join(s, " ")
}

// Commentary is mirrored as status, once per message: the event_msg
// agent_message (older rollouts) and item_completed AgentMessage (current
// rollouts) twins of the response_item repeat it, and v0.1.20 posted each
// pair once. The prompt's user_message twin and the final's agent_message
// twin are not posted either.
func TestTrackerMirrorsCommentaryOnceAsStatus(t *testing.T) {
	out := selRead(t, nil,
		selTurnStart("t1"),
		selPrompt("", "question"),
		selUserEvent("question"),
		selAgentEvent("commentary", "looking"), // older rollouts: event first
		selAgent("msg_c1", "commentary", "looking"),
		selAgentItem("t1", "commentary", "checking"), // current rollouts
		selAgent("msg_c2", "commentary", "checking"),
		selAgent("", "commentary", "no id"),
		selAgent("msg_e", "commentary", "   "), // empty: nothing
		selAgentEvent("final_answer", "done"),
		selAgent("msg_f", "final_answer", "done"),
		selComplete("t1", "done"),
	)
	if got := keys(out); got != "U:question | S:looking | S:checking | S:no id | F:done" {
		t.Fatalf("outputs %q", got)
	}
	if got := selIDs(out); got != "t:t1:prompt:2 msg_c1 msg_c2 t:t1:status:8 msg_f" {
		t.Fatalf("record ids %q", got)
	}
	for _, o := range out {
		if o.TurnID != "t1" {
			t.Fatalf("turn of %+v", o)
		}
	}
}

// The commentary of a turn the helper ran is not mirrored, also when it is
// read before the item that carries the client id (held with the prompt),
// and by turn id.
func TestTrackerSkipsStatusOfHelperTurns(t *testing.T) {
	skip := func(clientID, turn string) bool { return strings.HasPrefix(clientID, "cxp-") || turn == "t3" }
	out := selRead(t, skip,
		selTurnStart("t1"),
		selPrompt("", "from teams"),
		selAgent("msg_a", "commentary", "teams status before the item"),
		selUserItem("t1", "cxp-1"),
		selAgent("msg_b", "commentary", "teams status"),
		selAgent("msg_c", "final_answer", "teams answer"),
		selComplete("t1", "teams answer"),
		selTurnStart("t2"),
		selPrompt("", "local"),
		selAgent("msg_d", "commentary", "held until decided"),
		selUserItem("t2", ""),
		selAgent("msg_e", "commentary", "local status"),
		selAgent("msg_f", "final_answer", "local answer"),
		selComplete("t2", "local answer"),
		selTurnStart("t3"),
		selPrompt("", "run by an old helper"),
		selAgent("msg_g", "commentary", "old helper status"),
		selCompacted("h"),
		selContextCompacted,
		selComplete("t3", ""),
	)
	if got := keys(out); got != "U:local | S:held until decided | S:local status | F:local answer" {
		t.Fatalf("outputs %q", got)
	}
}

// A compaction is mirrored as the fixed v0.1.20 notice. A compacted record
// followed by context_compacted (only non-record lines between: world
// state, turn context, token counts, reasoning without summary) is one
// compaction, posted at the second record; any other compacted or
// context_compacted record is a notice of its own.
func TestTrackerCompactionNotices(t *testing.T) {
	cases := []struct {
		name  string
		lines []string
		want  string
		ids   string
	}{
		{"pair", []string{selCompacted("a"), selWorldState, selTurnContext("t1"), selTokenCount, selReasoning, selContextCompacted, selAgent("m1", "commentary", "go on")},
			"C:compacted | S:go on", "t:t1:compact:9 m1"},
		{"compacted alone", []string{selCompacted("a"), selWorldState, selTurnContext("t1"), selTokenCount, selAgent("m1", "commentary", "go on")},
			"C:compacted | S:go on", "t:t1:compact:4 m1"},
		{"context_compacted alone", []string{selContextCompacted, selAgent("m1", "commentary", "go on")},
			"C:compacted | S:go on", "t:t1:compact:4 m1"},
		{"two compacted", []string{selCompacted("a"), selCompacted("b"), selContextCompacted},
			"C:compacted | C:compacted", "t:t1:compact:4 t:t1:compact:6"},
		{"record between", []string{selCompacted("a"), selToolCall, selContextCompacted},
			"C:compacted | C:compacted", "t:t1:compact:4 t:t1:compact:6"},
		{"message between", []string{selCompacted("a"), selAgent("m1", "commentary", "x"), selContextCompacted},
			"C:compacted | S:x | C:compacted", "t:t1:compact:4 m1 t:t1:compact:6"},
		{"twice in a row", []string{selContextCompacted, selContextCompacted},
			"C:compacted | C:compacted", "t:t1:compact:4 t:t1:compact:5"},
		{"injected context between", []string{selCompacted("a"), selPrompt("", "<environment_context>x</environment_context>"), selContextCompacted},
			"C:compacted", "t:t1:compact:6"},
		{"turn ends between", []string{selCompacted("a"), selComplete("t1", ""), selTurnStart("t2"), selContextCompacted, selUserItem("t2", "")},
			"C:compacted", "t:t2:compact:7"},
	}
	for _, c := range cases {
		lines := append([]string{selTurnStart("t1"), selPrompt("u1", "q"), selUserItem("t1", "")}, c.lines...)
		out := selRead(t, nil, lines...)
		if got := keys(out[1:]); got != c.want {
			t.Errorf("%s: outputs %q, want %q", c.name, got, c.want)
			continue
		}
		if got := selIDs(out[1:]); got != c.ids {
			t.Errorf("%s: ids %q, want %q", c.name, got, c.ids)
		}
		for _, o := range out[1:] {
			if o.Kind == OutputCompact && (o.Text != CompactText || !strings.HasPrefix(o.RecordID, "t:"+o.TurnID+":")) {
				t.Errorf("%s: compaction output %+v", c.name, o)
			}
		}
	}
	// A large compacted record (the replacement history) is classified
	// from its head, never decoded.
	big := selCompacted(strings.Repeat("h", 64<<10))
	if ev := Parse([]byte(strings.TrimSuffix(big, "\n"))); ev.Kind != KindCompact || ev.Phase != compactedType || ev.Time.IsZero() {
		t.Fatalf("large compacted record parsed as %+v", ev)
	}
	bigEvent := selTS + `"type":"event_msg","payload":{"type":"context_compacted","pad":"` + strings.Repeat("p", 8<<10) + `"}}`
	if ev := Parse([]byte(bigEvent)); ev.Kind != KindCompact || ev.Phase != contextCompactedType {
		t.Fatalf("large context_compacted record parsed as %+v", ev)
	}
	out := selRead(t, nil, selTurnStart("t1"), selUserItem("t1", ""), big, selAgent("m1", "commentary", "x"))
	if keys(out) != "C:compacted | S:x" {
		t.Fatalf("large compacted record: %q", keys(out))
	}
	// A compaction in a helper turn is not mirrored; one read between
	// turns belongs to the turn before it (v0.1.20: the last turn id).
	helper := func(_, turn string) bool { return turn == "t1" }
	lines := []string{selTurnStart("t1"), selUserItem("t1", ""), selCompacted("a"), selContextCompacted, selCompacted("b"), selToolCall,
		selComplete("t1", ""), selCompacted("c"), selToolCall,
		selTurnStart("t2"), selUserItem("t2", ""), selComplete("t2", ""), selContextCompacted}
	out = selRead(t, helper, lines...)
	if keys(out) != "C:compacted" || selIDs(out) != "t:t2:compact:13" || out[0].TurnID != "t2" {
		t.Fatalf("helper turn compactions: %q %q", keys(out), selIDs(out))
	}
	if out := selRead(t, nil, lines...); selIDs(out) != "t:t1:compact:4 t:t1:compact:5 t:t1:compact:8 t:t2:compact:13" {
		t.Fatalf("local turn compactions: %q", selIDs(out))
	}
}

// A restart re-reads the open turn from its start (Resume) and produces the
// same record ids for status and compaction notices, also for a compacted
// record still held when the pass ended, and for one held while its turn
// ended.
func TestTrackerStatusAndCompactIDsStableAcrossRestart(t *testing.T) {
	all := []string{
		selTS + `"type":"session_meta","payload":{"id":"th","timestamp":"2026-09-01T00:00:00Z","cwd":"/w","originator":"codex_cli_rs","source":"cli"}}` + "\n",
		selTurnStart("t1"),
		selPrompt("", "q1"),
		selAgent("", "commentary", "s1"),
		selCompacted("a"), // held at the cut below
		selTurnContext("t1"),
		selContextCompacted,
		selAgent("", "commentary", "s2"),
		selCompacted("b"),
		selComplete("t1", "answer one"), // ends the turn with the compaction held
		selTurnStart("t2"),
		selPrompt("", "q2"),
		selAgent("", "final_answer", "answer two"),
		selComplete("t2", "answer two"),
	}
	full := selRead(t, nil, all...)
	if got := keys(full); got != "U:q1 | S:s1 | C:compacted | S:s2 | F:answer one | C:compacted | U:q2 | F:answer two" {
		t.Fatalf("outputs %q", got)
	}
	for _, cut := range []int{4, 5, 6, 9, 10, 11, 12} {
		p := writeLines(t, all[:cut]...)
		tr := NewTracker("th", 0, 0, nil)
		first := readAll(t, p, tr)
		off, line := tr.Resume()
		if err := os.WriteFile(p, []byte(strings.Join(all, "")), 0o644); err != nil {
			t.Fatal(err)
		}
		second := readAll(t, p, NewTracker("th", off, line, nil))
		// The restart re-delivers what it re-reads (the engine drops the
		// delivered keys); the union in order equals one full read.
		seen := map[string]bool{}
		var got []Output
		for _, o := range append(first, second...) {
			if !seen[o.RecordID] {
				seen[o.RecordID] = true
				got = append(got, o)
			}
		}
		if selIDs(got) != selIDs(full) || keys(got) != keys(full) {
			t.Fatalf("cut %d: ids %q / %q, want %q / %q", cut, selIDs(got), keys(got), selIDs(full), keys(full))
		}
	}
}
