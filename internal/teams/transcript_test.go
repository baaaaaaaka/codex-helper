package teams

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestParseCodexTranscriptPreservesUserAssistantOrder(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"session-1","cwd":"/work","source":"cli"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}`,
		`{"timestamp":"2026-01-01T00:02:00Z","type":"response_item","payload":{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if got.ThreadID != "session-1" {
		t.Fatalf("ThreadID = %q, want session-1", got.ThreadID)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	if got.Records[0].ThreadID != "session-1" || got.Records[1].ThreadID != "session-1" {
		t.Fatalf("record thread ids = %q/%q, want session-1", got.Records[0].ThreadID, got.Records[1].ThreadID)
	}
	assertTranscriptRecord(t, got.Records[0], "user-1", "source:user-1", TranscriptKindUser, "hello", 2)
	assertTranscriptRecord(t, got.Records[1], "assistant-1", "source:assistant-1", TranscriptKindAssistant, "hi", 3)
}

func TestParseCodexTranscriptSupportsExecJSONLItemCompleted(t *testing.T) {
	input := strings.Join([]string{
		"Reading additional input from stdin...",
		`{"type":"thread.started","thread_id":"thread-123"}`,
		`{"type":"turn.started","turn_id":"turn-1"}`,
		`{"type":"item.completed","item":{"id":"item-1","type":"agent_message","content":[{"type":"output_text","text":"done"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "exec.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Diagnostics) != 0 {
		t.Fatalf("diagnostics = %#v, want non-JSON prelude skipped", got.Diagnostics)
	}
	if len(got.Records) != 1 {
		t.Fatalf("records = %#v, want 1", got.Records)
	}
	record := got.Records[0]
	if record.ThreadID != "thread-123" || record.TurnID != "turn-1" {
		t.Fatalf("record ids = thread %q turn %q", record.ThreadID, record.TurnID)
	}
	assertTranscriptRecord(t, record, "item-1", "source:item-1", TranscriptKindAssistant, "done", 4)
}

func TestParseCodexTranscriptClassifiesEventCommentaryAsStatus(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"agent_message","id":"commentary-1","message":"working","phase":"commentary"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"final-1","message":"done","phase":"final_answer"}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "commentary-1", "source:commentary-1", TranscriptKindStatus, "working", 1)
	assertTranscriptRecord(t, got.Records[1], "final-1", "source:final-1", TranscriptKindAssistant, "done", 2)
}

func TestParseCodexTranscriptHidesInternalResponseItemAgentMessages(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"id":"child-message","type":"agent_message","author":"/root/child","recipient":"/root","content":[{"type":"input_text","text":"Message Type: MESSAGE\nTask name: /root/child\nPayload: encrypted"}],"encrypted_content":"sealed","internal_chat_message_metadata_passthrough":{"turn_id":"child-turn"}}}`,
		`{"type":"response_item","payload":{"id":"child-final","type":"agent_message","content":[{"type":"input_text","text":"Message Type: FINAL_ANSWER\nTask name: /root/child\nPayload: private answer"}],"internal_chat_message_metadata_passthrough":{}}}`,
		`{"type":"event_msg","payload":{"id":"visible-status","type":"agent_message","phase":"commentary","message":"working"}}`,
		`{"type":"response_item","payload":{"id":"visible-agent","type":"agent_message","content":[{"type":"output_text","text":"legacy visible agent message"}]}}`,
		`{"type":"response_item","payload":{"id":"visible-final","type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"real final answer"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 5 {
		t.Fatalf("records = %#v, want five source records including hidden collaboration records", got.Records)
	}
	for _, index := range []int{0, 1} {
		record := got.Records[index]
		if !record.Internal {
			t.Fatalf("record %d = %#v, want Internal=true", index, record)
		}
		if record.Kind != TranscriptKindAssistant || record.Text != "" || record.SourceType != "agent_message" {
			t.Fatalf("internal record %d = %#v, want empty hidden assistant agent_message", index, record)
		}
	}
	assertTranscriptRecord(t, got.Records[2], "visible-status", "source:visible-status", TranscriptKindStatus, "working", 3)
	assertTranscriptRecord(t, got.Records[3], "visible-agent", "source:visible-agent", TranscriptKindAssistant, "legacy visible agent message", 4)
	assertTranscriptRecord(t, got.Records[4], "visible-final", "source:visible-final", TranscriptKindAssistant, "real final answer", 5)
}

func TestParseCodexTranscriptHidesChatGPTAppInternalEvents(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"collect the local IP"}]}}`,
		`{"type":"event_msg","payload":{"type":"agent_reasoning","id":"event-reasoning","message":"Planning macOS IP logging script"}}`,
		`{"type":"response_item","payload":{"id":"response-reasoning","type":"reasoning","summary":[{"type":"summary_text","text":"Designing macOS launchd IP logging script"}]}}`,
		`{"type":"response_item","payload":{"id":"response-reasoning-raw","type":"reasoning","encrypted_content":"private reasoning","summary":[]}}`,
		`{"type":"event_msg","payload":{"type":"token_count","id":"event-token-empty","info":{"total_token_usage":{"input_tokens":12,"output_tokens":3}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","id":"event-token-text","message":"private token accounting","info":{"total_token_usage":{"input_tokens":12,"output_tokens":3}}}}`,
		`{"type":"agent_reasoning","id":"generic-reasoning","text":"Planning the next internal step"}`,
		`{"type":"patch_apply_end","id":"generic-patch","message":"Applied private patch details"}`,
		`{"type":"future_internal_event","id":"generic-unknown","message":"Private future event details"}`,
		`{"type":"item.completed","item":{"id":"completed-patch","type":"patch_apply_end","text":"Completed private patch details"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"visible-status","phase":"commentary","message":"working"}}`,
		`{"type":"response_item","payload":{"id":"visible-final","type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"IP logging is configured."}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "chatgpt-app-session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}

	type expectedRecord struct {
		internal   bool
		kind       TranscriptKind
		text       string
		sourceType string
	}
	want := map[string]expectedRecord{
		"user-1": {
			kind:       TranscriptKindUser,
			text:       "collect the local IP",
			sourceType: "message",
		},
		"event-reasoning": {
			internal:   true,
			sourceType: "agent_reasoning",
		},
		"response-reasoning": {
			internal:   true,
			sourceType: "reasoning",
		},
		"response-reasoning-raw": {
			internal:   true,
			sourceType: "reasoning",
		},
		"event-token-empty": {
			internal:   true,
			sourceType: "token_count",
		},
		"event-token-text": {
			internal:   true,
			sourceType: "token_count",
		},
		"generic-reasoning": {
			internal:   true,
			sourceType: "agent_reasoning",
		},
		"generic-patch": {
			internal:   true,
			sourceType: "patch_apply_end",
		},
		"generic-unknown": {
			internal:   true,
			sourceType: "future_internal_event",
		},
		"completed-patch": {
			internal:   true,
			sourceType: "patch_apply_end",
		},
		"visible-status": {
			kind:       TranscriptKindStatus,
			text:       "working",
			sourceType: "agent_message",
		},
		"visible-final": {
			kind:       TranscriptKindAssistant,
			text:       "IP logging is configured.",
			sourceType: "message",
		},
	}

	if len(got.Records) != len(want) {
		t.Errorf("records = %d, want %d; got %#v", len(got.Records), len(want), got.Records)
	}
	byID := make(map[string]TranscriptRecord, len(got.Records))
	for _, record := range got.Records {
		byID[record.ItemID] = record
	}
	for id, expected := range want {
		record, ok := byID[id]
		if !ok {
			t.Errorf("record %q is missing; parsed records = %#v", id, got.Records)
			continue
		}
		if record.Internal != expected.internal {
			t.Errorf("record %q Internal = %v, want %v: %#v", id, record.Internal, expected.internal, record)
		}
		if expected.internal && strings.TrimSpace(record.Text) != "" {
			t.Errorf("internal record %q exposed text %q", id, record.Text)
		}
		if !expected.internal && (record.Kind != expected.kind || record.Text != expected.text) {
			t.Errorf("visible record %q = %#v, want kind=%q text=%q", id, record, expected.kind, expected.text)
		}
		if record.SourceType != expected.sourceType {
			t.Errorf("record %q SourceType = %q, want %q", id, record.SourceType, expected.sourceType)
		}
	}
	for id, record := range byID {
		if _, ok := want[id]; !ok {
			t.Errorf("unexpected parsed record %q: %#v", id, record)
		}
	}
}

func TestParseCodexTranscriptFailsClosedAcrossInternalAgentEnvelopes(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"id":"response-internal-agent","type":"agent_message","author":"/root/child","recipient":"/root","content":[{"type":"input_text","text":"private response item"}]}}`,
		`{"type":"item.completed","item":{"id":"completed-internal-agent","type":"agent_message","author":"/root/child","recipient":"/root","text":"private completed item"}}`,
		`{"type":"event_msg","payload":{"id":"event-internal-agent","type":"agent_message","author":"/root/child","recipient":"/root","message":"private event message"}}`,
		`{"id":"generic-internal-agent","type":"agent_message","author":"/root/child","recipient":"/root","message":"private generic message"}`,
		`{"type":"response_item","payload":{"id":"response-unknown-assistant","type":"future_event","role":"assistant","text":"private future response"}}`,
		`{"type":"item.completed","item":{"id":"completed-unknown-assistant","type":"future_event","role":"assistant","text":"private future completed"}}`,
		`{"type":"event_msg","payload":{"id":"event-unknown-assistant","type":"future_event","role":"assistant","message":"private future event"}}`,
		`{"id":"generic-unknown-assistant","type":"future_event","role":"assistant","message":"private future generic"}`,
		`{"type":"turn_context","payload":{"model":"gpt-test"}}`,
		`{"id":"legacy-assistant","role":"assistant","text":"legacy visible assistant"}`,
		`{"type":"item.completed","item":{"id":"visible-completed-agent","type":"agent_message","text":"visible completed message"}}`,
		`{"type":"response_item","payload":{"id":"visible-final","type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"visible final answer"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 12 {
		t.Fatalf("records = %#v, want all source lines represented", got.Records)
	}

	internalIDs := map[string]bool{
		"response-internal-agent":     true,
		"completed-internal-agent":    true,
		"event-internal-agent":        true,
		"generic-internal-agent":      true,
		"response-unknown-assistant":  true,
		"completed-unknown-assistant": true,
		"event-unknown-assistant":     true,
		"generic-unknown-assistant":   true,
	}
	seenInternal := make(map[string]bool)
	for _, record := range got.Records {
		if internalIDs[record.ItemID] {
			seenInternal[record.ItemID] = true
			if !record.Internal || strings.TrimSpace(record.Text) != "" {
				t.Errorf("internal record %q = %#v, want hidden empty record", record.ItemID, record)
			}
		}
		if record.SourceType == "turn_context" {
			if !record.Internal || strings.TrimSpace(record.Text) != "" || strings.TrimSpace(transcriptRecordCheckpointKey(record)) == "" {
				t.Errorf("turn_context record = %#v, want hidden record with checkpoint identity", record)
			}
		}
	}
	if len(seenInternal) != len(internalIDs) {
		t.Errorf("hidden internal records = %#v, want %#v", seenInternal, internalIDs)
	}

	for _, want := range []struct {
		id   string
		kind TranscriptKind
		text string
	}{
		{id: "legacy-assistant", kind: TranscriptKindAssistant, text: "legacy visible assistant"},
		{id: "visible-completed-agent", kind: TranscriptKindAssistant, text: "visible completed message"},
		{id: "visible-final", kind: TranscriptKindAssistant, text: "visible final answer"},
	} {
		var found TranscriptRecord
		for _, record := range got.Records {
			if record.ItemID == want.id {
				found = record
				break
			}
		}
		if found.ItemID != want.id || found.Internal || found.Kind != want.kind || found.Text != want.text {
			t.Errorf("visible record %q = %#v, want kind=%q text=%q", want.id, found, want.kind, want.text)
		}
	}
}

func TestParseCodexTranscriptTurnContextCheckpointKeySurvivesTailRead(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"turn-context-session"}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-context-1","model":"gpt-test"}}`,
		`{"type":"response_item","payload":{"id":"final","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}`,
	}, "\n") + "\n"

	first, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(first.Records) != 2 {
		t.Fatalf("records = %#v, want turn_context and final", first.Records)
	}
	turnContext := first.Records[0]
	if turnContext.SourceType != "turn_context" || turnContext.TurnID != "turn-context-1" || !turnContext.Internal || transcriptRecordCheckpointKey(turnContext) == "" {
		t.Fatalf("turn_context = %#v, want checkpointable internal record", turnContext)
	}
	if first.Records[1].TurnID != "turn-context-1" {
		t.Fatalf("record after turn_context has TurnID %q, want turn-context-1", first.Records[1].TurnID)
	}

	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	tail, err := ReadSessionTranscriptSince(path, transcriptRecordCheckpointKey(turnContext))
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSinceWithOptions error: %v", err)
	}
	if len(tail.Records) != 1 || tail.Records[0].ItemID != "final" {
		t.Fatalf("tail records = %#v, want final after turn_context checkpoint", tail.Records)
	}
}

func TestParseCodexTranscriptAcceptsCaseInsensitiveKnownResponseItemTypes(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"id":"user-upper","type":"MESSAGE","role":"user","content":[{"type":"input_text","text":"prompt"}]}}`,
		`{"type":"response_item","payload":{"id":"assistant-mixed","type":"Message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "case-insensitive.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want both known message records", got.Records)
	}
	if got.Records[0].Kind != TranscriptKindUser || got.Records[0].Text != "prompt" {
		t.Fatalf("user record = %#v, want visible user prompt", got.Records[0])
	}
	if got.Records[1].Kind != TranscriptKindAssistant || got.Records[1].Text != "answer" {
		t.Fatalf("assistant record = %#v, want visible assistant answer", got.Records[1])
	}
}

func TestParseCodexTranscriptReadsResponseItemTurnFromMetadata(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"id":"user-1","type":"message","role":"user","internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"},"content":[{"type":"input_text","text":"prompt"}]}}`,
		`{"type":"response_item","payload":{"id":"assistant-1","type":"message","role":"assistant","phase":"final_answer","internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"},"content":[{"type":"output_text","text":"answer"}]}}`,
		`{"type":"response_item","payload":{"id":"assistant-direct","type":"message","role":"assistant","phase":"final_answer","turn_id":"turn-direct","internal_chat_message_metadata_passthrough":{"turn_id":"turn-ignored"},"content":[{"type":"output_text","text":"direct answer"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 3 {
		t.Fatalf("records = %#v, want 3", got.Records)
	}
	wantTurns := map[string]string{
		"user-1":           "turn-1",
		"assistant-1":      "turn-1",
		"assistant-direct": "turn-direct",
	}
	for _, record := range got.Records {
		if want, ok := wantTurns[record.ItemID]; !ok || record.TurnID != want {
			t.Fatalf("record %q TurnID = %q, want %q", record.ItemID, record.TurnID, want)
		}
	}
}

func TestParseCodexTranscriptClassifiesResponseItemCommentaryAsStatus(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"id":"commentary-1","type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"working"}]}}`,
		`{"type":"response_item","payload":{"id":"final-1","type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"done"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "commentary-1", "source:commentary-1", TranscriptKindStatus, "working", 1)
	if got.Records[0].Phase != "commentary" {
		t.Fatalf("commentary phase = %q, want commentary", got.Records[0].Phase)
	}
	assertTranscriptRecord(t, got.Records[1], "final-1", "source:final-1", TranscriptKindAssistant, "done", 2)
	if got.Records[1].Phase != "final_answer" {
		t.Fatalf("final phase = %q, want final_answer", got.Records[1].Phase)
	}
}

func TestParseCodexTranscriptSkipsRawReasoningWithoutVisibleSummary(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"id":"reasoning-raw","type":"reasoning","encrypted_content":"private thinking","content":[{"type":"reasoning_text","text":"private thinking"}],"summary":[]}}`,
		`{"type":"response_item","payload":{"id":"reasoning-summary","type":"reasoning","summary":[{"type":"summary_text","text":"visible summary"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want both reasoning records as internal checkpoints", got.Records)
	}
	for _, record := range got.Records {
		if !record.Internal || strings.TrimSpace(record.Text) != "" || record.Kind != TranscriptKindUnknown {
			t.Fatalf("reasoning record leaked or lost internal classification: %#v", record)
		}
	}
}

func TestParseCodexTranscriptKeepsResponseItemAssistantPrefixRecords(t *testing.T) {
	first := "long assistant response_item body that is a legitimate first message and should stay"
	second := first + " with additional text in a separate response item"
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"` + first + `"}]}}`,
		`{"type":"response_item","payload":{"id":"assistant-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"` + second + `"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want both response_item assistant records", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "assistant-1", "source:assistant-1", TranscriptKindAssistant, first, 1)
	assertTranscriptRecord(t, got.Records[1], "assistant-2", "source:assistant-2", TranscriptKindAssistant, second, 2)
}

func TestParseCodexTranscriptDropsStreamingPrefixAfterIntermediateAssistant(t *testing.T) {
	partial := "streamed final prefix that should be removed once the full response item appears `"
	intermediate := "different assistant record in the same turn that is not a prefix and should stay"
	full := partial + "<oai-mem-citation> literal tag explanation plus the rest of the complete answer"
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"stream-1","type":"agent_message","turn_id":"turn-1","phase":"final_answer","message":` + strconv.Quote(partial) + `}}`,
		`{"type":"event_msg","payload":{"id":"stream-2","type":"agent_message","turn_id":"turn-1","phase":"final_answer","message":` + strconv.Quote(intermediate) + `}}`,
		`{"type":"response_item","payload":{"id":"assistant-full","type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":` + strconv.Quote(full) + `}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want intermediate and full records only", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "stream-2", "source:stream-2", TranscriptKindAssistant, intermediate, 2)
	assertTranscriptRecord(t, got.Records[1], "assistant-full", "source:assistant-full", TranscriptKindAssistant, full, 3)
}

func TestParseCodexTranscriptRecordsContextCompactEvent(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-compact"}}`,
		`{"timestamp":"2026-05-20T03:30:42.693Z","type":"compacted","payload":{"message":"","replacement_history":[{"type":"message","role":"user","content":[{"type":"input_text","text":"old prompt"}]}]}}`,
		`{"timestamp":"2026-05-20T03:30:42.694Z","type":"turn_context","payload":{"turn_id":"turn-1"}}`,
		`{"timestamp":"2026-05-20T03:30:42.695Z","type":"event_msg","payload":{"type":"context_compacted"}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 3 {
		t.Fatalf("records = %#v, want visible context compact records plus an internal turn_context checkpoint", got.Records)
	}
	firstID := "fallback:session:thread-compact:line:2:kind:compact"
	assertTranscriptRecord(t, got.Records[0], firstID, firstID, TranscriptKindCompact, transcriptContextCompactMessage, 2)
	if got.Records[0].SourceType != "compacted" {
		t.Fatalf("first SourceType = %q, want compacted", got.Records[0].SourceType)
	}
	if got.Records[1].SourceType != "turn_context" || !got.Records[1].Internal || got.Records[1].Kind != TranscriptKindUnknown || strings.TrimSpace(got.Records[1].Text) != "" {
		t.Fatalf("turn_context record = %#v, want empty internal checkpoint record", got.Records[1])
	}
	lastID := "fallback:session:thread-compact:line:4:kind:compact"
	assertTranscriptRecord(t, got.Records[2], lastID, lastID, TranscriptKindCompact, transcriptContextCompactMessage, 4)
	if got.Records[2].SourceType != "context_compacted" {
		t.Fatalf("last SourceType = %q, want context_compacted", got.Records[2].SourceType)
	}
}

func TestParseCodexTranscriptRecordsCompactOnlyLine(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-compact"}}`,
		`{"timestamp":"2026-05-20T03:30:42.693Z","type":"compacted","payload":{"message":"","replacement_history":[{"type":"message","role":"user","content":[{"type":"input_text","text":"old prompt"}]}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 1 {
		t.Fatalf("records = %#v, want one compact-only record", got.Records)
	}
	wantID := "fallback:session:thread-compact:line:2:kind:compact"
	assertTranscriptRecord(t, got.Records[0], wantID, wantID, TranscriptKindCompact, transcriptContextCompactMessage, 2)
	if got.Records[0].SourceType != "compacted" {
		t.Fatalf("SourceType = %q, want compacted", got.Records[0].SourceType)
	}
	if strings.Contains(got.Records[0].Text, "old prompt") {
		t.Fatalf("compact record leaked replacement history: %q", got.Records[0].Text)
	}
}

func TestParseCodexTranscriptRecordsContextCompactCompletedItem(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-123"}`,
		`{"type":"turn.started","turn_id":"turn-1"}`,
		`{"type":"item.completed","item":{"id":"compact-1","type":"context_compaction"}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "exec.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 1 {
		t.Fatalf("records = %#v, want one context compact item", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "compact-1", "source:compact-1", TranscriptKindCompact, transcriptContextCompactMessage, 3)
	if got.Records[0].ThreadID != "thread-123" || got.Records[0].TurnID != "turn-1" {
		t.Fatalf("record ids = thread %q turn %q", got.Records[0].ThreadID, got.Records[0].TurnID)
	}
}

func TestParseCodexTranscriptBadTailIsDiagnosticOnly(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"session-1","cwd":"/work","source":"cli"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"complete"}]}}`,
		`{"timestamp":"2026-01-01T00:02:00Z","type":"response_item","payload":{"id":"assistant-tail","type":"message","role":"assistant","content":[{"type":"output_text","text":"partial"}]}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 1 {
		t.Fatalf("records = %#v, want only the complete record", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "user-1", "source:user-1", TranscriptKindUser, "complete", 2)
	if len(got.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %#v, want 1", got.Diagnostics)
	}
	if got.Diagnostics[0].Kind != "invalid_json" || got.Diagnostics[0].SourceLine != 3 {
		t.Fatalf("diagnostic = %#v, want invalid_json on line 3", got.Diagnostics[0])
	}
}

func TestParseCodexTranscriptMissingIDUsesStableFallback(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"session-1","cwd":"/work","source":"cli"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"no id"}]}}`,
	}, "\n")

	got1, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript #1 error: %v", err)
	}
	got2, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript #2 error: %v", err)
	}
	if len(got1.Records) != 1 || len(got2.Records) != 1 {
		t.Fatalf("records got1=%#v got2=%#v", got1.Records, got2.Records)
	}
	wantID := "fallback:session:session-1:line:2:kind:user"
	if got1.FileFingerprint != "session:session-1" {
		t.Fatalf("FileFingerprint = %q, want session:session-1", got1.FileFingerprint)
	}
	if got1.Records[0].ItemID != wantID || got1.Records[0].DedupeKey != wantID {
		t.Fatalf("fallback identity = item %q dedupe %q, want %q", got1.Records[0].ItemID, got1.Records[0].DedupeKey, wantID)
	}
	if got2.Records[0].ItemID != got1.Records[0].ItemID {
		t.Fatalf("fallback ItemID changed across parses: %q vs %q", got1.Records[0].ItemID, got2.Records[0].ItemID)
	}
}

func TestParseCodexTranscriptDuplicateSourceIDKeepsOrderedRecordsAndReportsDiagnostic(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"session-1"}}`,
		`{"type":"response_item","payload":{"id":"dup","type":"message","role":"user","content":[{"type":"input_text","text":"first"}]}}`,
		`{"type":"response_item","payload":{"id":"dup","type":"message","role":"assistant","content":[{"type":"output_text","text":"second"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "dup", "source:dup", TranscriptKindUser, "first", 2)
	assertTranscriptRecord(t, got.Records[1], "dup#line:3", "source:dup", TranscriptKindAssistant, "second", 3)
	if len(got.Diagnostics) != 1 || got.Diagnostics[0].Kind != "duplicate_item_id" || got.Diagnostics[0].SourceLine != 3 {
		t.Fatalf("diagnostics = %#v, want duplicate_item_id on line 3", got.Diagnostics)
	}
}

func TestParseCodexTranscriptUsesJSONLOrderBeforeTimestamp(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:10:00Z","type":"response_item","payload":{"id":"first","type":"message","role":"user","content":[{"type":"input_text","text":"first in file"}]}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"id":"second","type":"message","role":"assistant","content":[{"type":"output_text","text":"second in file"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	if got.Records[0].Text != "first in file" || got.Records[1].Text != "second in file" {
		t.Fatalf("records reordered by timestamp: %#v", got.Records)
	}
	if !got.Records[0].CreatedAt.After(got.Records[1].CreatedAt) {
		t.Fatalf("test fixture should have reversed timestamps: %#v", got.Records)
	}
}

func TestReadSessionTranscriptSinceRefusesMissingCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	input := `{"type":"session_meta","payload":{"id":"session-1"}}` + "\n" +
		`{"type":"response_item","payload":{"id":"first","type":"message","role":"user","content":[{"type":"input_text","text":"first"}]}}` + "\n" +
		`{"type":"response_item","payload":{"id":"second","type":"message","role":"assistant","content":[{"type":"output_text","text":"second"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := ReadSessionTranscriptSince(path, "source:first")
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSince error: %v", err)
	}
	if len(got.Records) != 1 || got.Records[0].ItemID != "second" {
		t.Fatalf("records after checkpoint = %#v", got.Records)
	}

	got, err = ReadSessionTranscriptSince(path, "missing")
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSince missing error: %v", err)
	}
	if len(got.Records) != 0 {
		t.Fatalf("records for missing checkpoint = %#v, want none", got.Records)
	}
	if len(got.Diagnostics) == 0 || got.Diagnostics[len(got.Diagnostics)-1].Kind != "checkpoint_not_found" {
		t.Fatalf("diagnostics = %#v, want checkpoint_not_found", got.Diagnostics)
	}
}

func TestReadSessionTranscriptSinceMatchesFullTailForSourceCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"session-1"}}`,
		`{"type":"response_item","payload":{"id":"first","type":"message","role":"user","content":[{"type":"input_text","text":"first"}]}}`,
		`{"type":"event_msg","payload":{"id":"status-1","type":"agent_message","phase":"commentary","message":"working"}}`,
		`{"type":"response_item","payload":{"id":"second","type":"message","role":"assistant","content":[{"type":"output_text","text":"second"}]}}`,
		`{"type":"response_item","payload":{"id":"third","type":"message","role":"assistant","content":[{"type":"output_text","text":"third"}]}}`,
		``,
	}, "\n")
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}

	full, err := ReadSessionTranscript(path)
	if err != nil {
		t.Fatalf("ReadSessionTranscript error: %v", err)
	}
	got, err := ReadSessionTranscriptSince(path, "second")
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSince error: %v", err)
	}
	if len(full.Records) != 4 || len(got.Records) != 1 {
		t.Fatalf("records full=%#v since=%#v", full.Records, got.Records)
	}
	if got.Records[0] != full.Records[3] {
		t.Fatalf("since tail = %#v, want full tail %#v", got.Records[0], full.Records[3])
	}
}

func TestReadSessionTranscriptSinceMatchesFullTailForFallbackCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"session-1"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"fallback checkpoint"}]}}`,
		`{"type":"response_item","payload":{"id":"after","type":"message","role":"assistant","content":[{"type":"output_text","text":"after fallback"}]}}`,
		``,
	}, "\n")
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	full, err := ReadSessionTranscript(path)
	if err != nil {
		t.Fatalf("ReadSessionTranscript error: %v", err)
	}
	if len(full.Records) != 2 {
		t.Fatalf("full records = %#v, want 2", full.Records)
	}
	got, err := ReadSessionTranscriptSince(path, full.Records[0].ItemID)
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSince error: %v", err)
	}
	if len(got.Records) != 1 || got.Records[0] != full.Records[1] {
		t.Fatalf("since fallback tail = %#v, want %#v", got.Records, full.Records[1:])
	}
}

func TestFindTranscriptCheckpointPositionSupportsFallbackCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"session-1"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"fallback checkpoint"}]}}`,
		`{"type":"response_item","payload":{"id":"after","type":"message","role":"assistant","content":[{"type":"output_text","text":"after fallback"}]}}`,
		``,
	}, "\n")
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	full, err := ReadSessionTranscript(path)
	if err != nil {
		t.Fatalf("ReadSessionTranscript error: %v", err)
	}
	if len(full.Records) != 2 {
		t.Fatalf("full records = %#v, want 2", full.Records)
	}
	position, ok, err := findTranscriptCheckpointPosition(path, full.Records[0].ItemID)
	if err != nil {
		t.Fatalf("findTranscriptCheckpointPosition error: %v", err)
	}
	if !ok {
		t.Fatal("fallback checkpoint position was not found")
	}
	if position.Line != full.Records[0].SourceLine || position.Offset != full.Records[0].SourceOffset {
		t.Fatalf("position = line %d offset %d, want line %d offset %d", position.Line, position.Offset, full.Records[0].SourceLine, full.Records[0].SourceOffset)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat transcript: %v", err)
	}
	if position.SourceSize != info.Size() || position.SourceModTime.IsZero() {
		t.Fatalf("source position metadata = size %d mod %v, want size %d and mod time", position.SourceSize, position.SourceModTime, info.Size())
	}
}

func TestReadSessionTranscriptSinceRefusesInvalidLineLikeCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"session-1"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"fallback checkpoint"}]}}`,
		`{"type":"response_item","payload":{"id":"after","type":"message","role":"assistant","content":[{"type":"output_text","text":"after fallback"}]}}`,
		``,
	}, "\n")
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadSessionTranscriptSince(path, "fallback:session:other-session:line:2:kind:user")
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSince error: %v", err)
	}
	if len(got.Records) != 0 {
		t.Fatalf("records for invalid line-like checkpoint = %#v, want none", got.Records)
	}
	if len(got.Diagnostics) == 0 || got.Diagnostics[len(got.Diagnostics)-1].Kind != "checkpoint_not_found" {
		t.Fatalf("diagnostics = %#v, want checkpoint_not_found", got.Diagnostics)
	}
}

func TestReadSessionTranscriptSinceFallsBackForPrefixDuplicateSourceIDs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"session-1"}}`,
		`{"type":"response_item","payload":{"id":"checkpoint","type":"message","role":"user","content":[{"type":"input_text","text":"checkpoint"}]}}`,
		`{"type":"response_item","payload":{"id":"dup","type":"message","role":"assistant","content":[{"type":"output_text","text":"before dup"}]}}`,
		`{"type":"response_item","payload":{"id":"after","type":"message","role":"assistant","content":[{"type":"output_text","text":"after"}]}}`,
		`{"type":"response_item","payload":{"id":"dup","type":"message","role":"assistant","content":[{"type":"output_text","text":"after dup"}]}}`,
		``,
	}, "\n")
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	full, err := ReadSessionTranscript(path)
	if err != nil {
		t.Fatalf("ReadSessionTranscript error: %v", err)
	}
	got, err := ReadSessionTranscriptSince(path, "checkpoint")
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSince error: %v", err)
	}
	want := full.Records[1:]
	if len(got.Records) != len(want) {
		t.Fatalf("records = %#v, want %#v", got.Records, want)
	}
	for i := range want {
		if got.Records[i] != want[i] {
			t.Fatalf("record %d = %#v, want %#v", i, got.Records[i], want[i])
		}
	}
}

func TestReadSessionTranscriptSinceAvoidsFullParseWorkForTail(t *testing.T) {
	path, checkpoints := writeBenchmarkTranscript(t, 800, 24)
	afterKey := checkpoints[len(checkpoints)-20]

	var fullErr error
	var fullRecords int
	fullAllocs := testing.AllocsPerRun(3, func() {
		var transcript Transcript
		transcript, fullErr = ReadSessionTranscript(path)
		fullRecords = len(transcript.Records)
	})
	if fullErr != nil {
		t.Fatalf("ReadSessionTranscript error: %v", fullErr)
	}
	if fullRecords == 0 {
		t.Fatal("expected full transcript records")
	}

	var tailErr error
	var tailRecords int
	tailAllocs := testing.AllocsPerRun(5, func() {
		var transcript Transcript
		transcript, tailErr = ReadSessionTranscriptSince(path, afterKey)
		tailRecords = len(transcript.Records)
	})
	if tailErr != nil {
		t.Fatalf("ReadSessionTranscriptSince error: %v", tailErr)
	}
	if tailRecords == 0 || tailRecords > 40 {
		t.Fatalf("tail records = %d, want small non-empty tail", tailRecords)
	}
	if tailAllocs >= fullAllocs*0.30 {
		t.Fatalf("tail read allocations = %.0f, full read allocations = %.0f; tail path appears to be doing full-parse work", tailAllocs, fullAllocs)
	}
}

func assertTranscriptRecord(t *testing.T, record TranscriptRecord, wantItemID string, wantDedupeKey string, wantKind TranscriptKind, wantText string, wantLine int) {
	t.Helper()
	if record.ItemID != wantItemID {
		t.Fatalf("ItemID = %q, want %q in %#v", record.ItemID, wantItemID, record)
	}
	if record.DedupeKey != wantDedupeKey {
		t.Fatalf("DedupeKey = %q, want %q in %#v", record.DedupeKey, wantDedupeKey, record)
	}
	if record.Kind != wantKind {
		t.Fatalf("Kind = %q, want %q in %#v", record.Kind, wantKind, record)
	}
	if record.Text != wantText {
		t.Fatalf("Text = %q, want %q in %#v", record.Text, wantText, record)
	}
	if record.SourceLine != wantLine {
		t.Fatalf("SourceLine = %d, want %d in %#v", record.SourceLine, wantLine, record)
	}
}
