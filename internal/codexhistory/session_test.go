package codexhistory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// appendMessage
// ---------------------------------------------------------------------------

func TestAppendMessage_UnderLimit(t *testing.T) {
	var ring []Message
	appendMessage(&ring, Message{Role: "user", Content: "a"}, 5)
	appendMessage(&ring, Message{Role: "assistant", Content: "b"}, 5)
	if len(ring) != 2 {
		t.Fatalf("len = %d, want 2", len(ring))
	}
}

func TestAppendMessage_AtLimit(t *testing.T) {
	var ring []Message
	appendMessage(&ring, Message{Role: "user", Content: "1"}, 2)
	appendMessage(&ring, Message{Role: "user", Content: "2"}, 2)
	appendMessage(&ring, Message{Role: "user", Content: "3"}, 2)
	if len(ring) != 2 {
		t.Fatalf("len = %d, want 2", len(ring))
	}
	if ring[0].Content != "2" || ring[1].Content != "3" {
		t.Errorf("ring = [%q, %q], want [2, 3]", ring[0].Content, ring[1].Content)
	}
}

func TestAppendMessage_ZeroLimitUnlimited(t *testing.T) {
	var ring []Message
	for i := 0; i < 100; i++ {
		appendMessage(&ring, Message{Role: "user", Content: "x"}, 0)
	}
	if len(ring) != 100 {
		t.Fatalf("len = %d, want 100", len(ring))
	}
}

// ---------------------------------------------------------------------------
// parseLineMessages
// ---------------------------------------------------------------------------

func TestParseLineMessages_InvalidJSON(t *testing.T) {
	msgs := parseLineMessages([]byte(`{broken`))
	if msgs != nil {
		t.Errorf("expected nil for invalid JSON, got %v", msgs)
	}
}

func TestParseLineMessages_UnknownType(t *testing.T) {
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"unknown_type","payload":{}}`
	msgs := parseLineMessages([]byte(line))
	if msgs != nil {
		t.Errorf("expected nil for unknown type, got %v", msgs)
	}
}

func TestParseLineMessages_ResponseItem(t *testing.T) {
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}`
	msgs := parseLineMessages([]byte(line))
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Role != "user" || msgs[0].Content != "hello" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

func TestParseLineMessages_EventMsg(t *testing.T) {
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"event_msg","payload":{"type":"user_message","content":"hi there"}}`
	msgs := parseLineMessages([]byte(line))
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Role != "user" || msgs[0].Content != "hi there" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

// ---------------------------------------------------------------------------
// parseResponseItem
// ---------------------------------------------------------------------------

func TestParseResponseItem_InvalidJSON(t *testing.T) {
	msgs := parseResponseItem([]byte(`{broken`), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil")
	}
}

func TestParseResponseItem_UnknownType(t *testing.T) {
	msgs := parseResponseItem([]byte(`{"type":"something_new"}`), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for unknown type")
	}
}

func TestParseResponseItem_Message(t *testing.T) {
	raw := `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}`
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "assistant" || msgs[0].Content != "answer" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

func TestParseResponseItem_FunctionCall(t *testing.T) {
	raw := `{"type":"function_call","name":"read_file","arguments":"{\"path\":\"/tmp/x\"}"}`
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "tool" {
		t.Errorf("role = %q", msgs[0].Role)
	}
	if !strings.Contains(msgs[0].Content, "read_file") {
		t.Errorf("content missing function name: %q", msgs[0].Content)
	}
}

func TestParseResponseItem_FunctionCallOutput(t *testing.T) {
	raw := `{"type":"function_call_output","output":"file contents here"}`
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "tool_result" {
		t.Errorf("role = %q", msgs[0].Role)
	}
	if msgs[0].Content != "file contents here" {
		t.Errorf("content = %q", msgs[0].Content)
	}
}

func TestParseResponseItem_CustomToolCall(t *testing.T) {
	raw := `{"type":"custom_tool_call","name":"my_tool","content":[{"type":"input_text","text":"arg data"}]}`
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "tool" {
		t.Errorf("role = %q", msgs[0].Role)
	}
	if !strings.Contains(msgs[0].Content, "my_tool") {
		t.Errorf("content = %q", msgs[0].Content)
	}
}

func TestParseResponseItem_CustomToolCallOutput(t *testing.T) {
	raw := `{"type":"custom_tool_call_output","content":[{"type":"output_text","text":"result data"}]}`
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "tool_result" || msgs[0].Content != "result data" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

func TestParseResponseItem_Reasoning(t *testing.T) {
	raw := `{"type":"reasoning","summary":[{"type":"summary_text","text":"thinking about it"}]}`
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "thinking" || msgs[0].Content != "thinking about it" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

func TestParseResponseItem_MessageBadPayload(t *testing.T) {
	raw := `{"type":"message","role":123}` // role is not a string in payload
	msgs := parseResponseItem([]byte(raw), fixedTime())
	// json.Unmarshal into codexResponsePayload should fail
	if msgs != nil {
		t.Errorf("expected nil for bad payload")
	}
}

func TestParseResponseItem_CustomToolCallBadPayload(t *testing.T) {
	raw := `{"type":"custom_tool_call","name":123}` // name is not string
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for bad payload")
	}
}

func TestParseResponseItem_CustomToolCallOutputBadPayload(t *testing.T) {
	raw := `{"type":"custom_tool_call_output","content":123}`
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for bad payload")
	}
}

// ---------------------------------------------------------------------------
// parseMessagePayload
// ---------------------------------------------------------------------------

func TestParseMessagePayload_UserMessage(t *testing.T) {
	p := codexResponsePayload{
		Type:    "message",
		Role:    "user",
		Content: []byte(`[{"type":"input_text","text":"hello world"}]`),
	}
	msgs := parseMessagePayload(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "user" || msgs[0].Content != "hello world" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

func TestParseMessagePayload_AssistantMessage(t *testing.T) {
	p := codexResponsePayload{
		Type:    "message",
		Role:    "assistant",
		Content: []byte(`[{"type":"output_text","text":"response"}]`),
	}
	msgs := parseMessagePayload(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "assistant" {
		t.Errorf("role = %q", msgs[0].Role)
	}
}

func TestParseMessagePayload_AssistantCommentary(t *testing.T) {
	p := codexResponsePayload{
		Type:    "message",
		Role:    "assistant",
		Phase:   "commentary",
		Content: []byte(`[{"type":"output_text","text":"update"}]`),
	}
	msgs := parseMessagePayload(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "assistant_commentary" {
		t.Errorf("role = %q, want assistant_commentary", msgs[0].Role)
	}
}

func TestParseMessagePayload_DeveloperRoleSkipped(t *testing.T) {
	p := codexResponsePayload{
		Type:    "message",
		Role:    "developer",
		Content: []byte(`[{"type":"input_text","text":"system"}]`),
	}
	msgs := parseMessagePayload(p, fixedTime())
	if msgs != nil {
		t.Errorf("developer role should be skipped")
	}
}

func TestParseMessagePayload_SystemRoleSkipped(t *testing.T) {
	p := codexResponsePayload{
		Type:    "message",
		Role:    "system",
		Content: []byte(`[{"type":"input_text","text":"system"}]`),
	}
	msgs := parseMessagePayload(p, fixedTime())
	if msgs != nil {
		t.Errorf("system role should be skipped")
	}
}

func TestParseMessagePayload_EmptyContent(t *testing.T) {
	p := codexResponsePayload{
		Type:    "message",
		Role:    "user",
		Content: []byte(`[]`),
	}
	msgs := parseMessagePayload(p, fixedTime())
	if msgs != nil {
		t.Errorf("empty content should return nil")
	}
}

func TestParseMessagePayload_SystemInjectedSkipped(t *testing.T) {
	p := codexResponsePayload{
		Type:    "message",
		Role:    "user",
		Content: []byte(`[{"type":"input_text","text":"# AGENTS.md\nsome skill instructions"}]`),
	}
	msgs := parseMessagePayload(p, fixedTime())
	if msgs != nil {
		t.Errorf("system-injected user message should be skipped")
	}
}

func TestParseMessagePayload_WhitespaceContent(t *testing.T) {
	p := codexResponsePayload{
		Type:    "message",
		Role:    "user",
		Content: []byte(`[{"type":"input_text","text":"   \n  "}]`),
	}
	msgs := parseMessagePayload(p, fixedTime())
	if msgs != nil {
		t.Errorf("whitespace-only content should return nil")
	}
}

// ---------------------------------------------------------------------------
// parseFunctionCall
// ---------------------------------------------------------------------------

func TestParseFunctionCall_ValidJSONArgs(t *testing.T) {
	raw := `{"type":"function_call","name":"write_file","arguments":"{\"path\":\"/tmp/x\",\"content\":\"data\"}"}`
	msgs := parseFunctionCall([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if !strings.HasPrefix(msgs[0].Content, "Tool: write_file") {
		t.Errorf("content = %q", msgs[0].Content)
	}
	// JSON should be pretty-printed
	if !strings.Contains(msgs[0].Content, "\"path\"") {
		t.Errorf("expected formatted JSON in content: %q", msgs[0].Content)
	}
}

func TestParseFunctionCall_InvalidJSONArgs(t *testing.T) {
	raw := `{"type":"function_call","name":"tool","arguments":"not valid json"}`
	msgs := parseFunctionCall([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if !strings.Contains(msgs[0].Content, "not valid json") {
		t.Errorf("invalid JSON args should be included raw: %q", msgs[0].Content)
	}
}

func TestParseFunctionCall_EmptyArgs(t *testing.T) {
	raw := `{"type":"function_call","name":"no_args","arguments":""}`
	msgs := parseFunctionCall([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Content != "Tool: no_args" {
		t.Errorf("content = %q", msgs[0].Content)
	}
}

func TestParseFunctionCall_EmptyName(t *testing.T) {
	raw := `{"type":"function_call","name":"","arguments":""}`
	msgs := parseFunctionCall([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Content != "Tool: function_call" {
		t.Errorf("content = %q, want 'Tool: function_call'", msgs[0].Content)
	}
}

func TestParseFunctionCall_InvalidJSON(t *testing.T) {
	msgs := parseFunctionCall([]byte(`{broken`), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil")
	}
}

// ---------------------------------------------------------------------------
// parseFunctionCallOutput
// ---------------------------------------------------------------------------

func TestParseFunctionCallOutput_Normal(t *testing.T) {
	raw := `{"type":"function_call_output","output":"result text"}`
	msgs := parseFunctionCallOutput([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "tool_result" || msgs[0].Content != "result text" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

func TestParseFunctionCallOutput_EmptyOutput(t *testing.T) {
	raw := `{"type":"function_call_output","output":""}`
	msgs := parseFunctionCallOutput([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for empty output")
	}
}

func TestParseFunctionCallOutput_WhitespaceOutput(t *testing.T) {
	raw := `{"type":"function_call_output","output":"   \n  "}`
	msgs := parseFunctionCallOutput([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for whitespace-only output")
	}
}

func TestParseFunctionCallOutput_InvalidJSON(t *testing.T) {
	msgs := parseFunctionCallOutput([]byte(`{broken`), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil")
	}
}

// ---------------------------------------------------------------------------
// parseCustomToolCall
// ---------------------------------------------------------------------------

func TestParseCustomToolCall_NamedWithContent(t *testing.T) {
	p := codexResponsePayload{
		Name:    "bash",
		Content: []byte(`[{"type":"input_text","text":"ls -la"}]`),
	}
	msgs := parseCustomToolCall(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if !strings.HasPrefix(msgs[0].Content, "Tool: bash") {
		t.Errorf("content = %q", msgs[0].Content)
	}
	if !strings.Contains(msgs[0].Content, "ls -la") {
		t.Errorf("content should contain args: %q", msgs[0].Content)
	}
}

func TestParseCustomToolCall_EmptyName(t *testing.T) {
	p := codexResponsePayload{
		Name:    "",
		Content: []byte(`[{"type":"input_text","text":"data"}]`),
	}
	msgs := parseCustomToolCall(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if !strings.HasPrefix(msgs[0].Content, "Tool: custom_tool") {
		t.Errorf("content = %q", msgs[0].Content)
	}
}

func TestParseCustomToolCall_EmptyContent(t *testing.T) {
	p := codexResponsePayload{
		Name:    "tool",
		Content: []byte(`[]`),
	}
	msgs := parseCustomToolCall(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Content != "Tool: tool" {
		t.Errorf("content = %q", msgs[0].Content)
	}
}

// ---------------------------------------------------------------------------
// parseCustomToolCallOutput
// ---------------------------------------------------------------------------

func TestParseCustomToolCallOutput_Normal(t *testing.T) {
	p := codexResponsePayload{
		Content: []byte(`[{"type":"output_text","text":"result data"}]`),
	}
	msgs := parseCustomToolCallOutput(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "tool_result" || msgs[0].Content != "result data" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

func TestParseCustomToolCallOutput_EmptyContent(t *testing.T) {
	p := codexResponsePayload{
		Content: []byte(`[]`),
	}
	msgs := parseCustomToolCallOutput(p, fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for empty content")
	}
}

// ---------------------------------------------------------------------------
// parseReasoning
// ---------------------------------------------------------------------------

func TestParseReasoning_Normal(t *testing.T) {
	raw := `{"type":"reasoning","summary":[{"type":"summary_text","text":"step 1"},{"type":"summary_text","text":"step 2"}]}`
	msgs := parseReasoning([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "thinking" {
		t.Errorf("role = %q", msgs[0].Role)
	}
	if msgs[0].Content != "step 1\nstep 2" {
		t.Errorf("content = %q", msgs[0].Content)
	}
}

func TestParseReasoning_EmptySummary(t *testing.T) {
	raw := `{"type":"reasoning","summary":[]}`
	msgs := parseReasoning([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for empty summary")
	}
}

func TestParseReasoning_WhitespaceOnlySummary(t *testing.T) {
	raw := `{"type":"reasoning","summary":[{"type":"summary_text","text":"  "}]}`
	msgs := parseReasoning([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for whitespace-only summary")
	}
}

func TestParseReasoning_InvalidJSON(t *testing.T) {
	msgs := parseReasoning([]byte(`{broken`), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil")
	}
}

func TestParseReasoning_NilSummary(t *testing.T) {
	raw := `{"type":"reasoning"}`
	msgs := parseReasoning([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for missing summary")
	}
}

// ---------------------------------------------------------------------------
// parseEventMsg
// ---------------------------------------------------------------------------

func TestParseEventMsg_UserMessage(t *testing.T) {
	raw := `{"type":"user_message","content":"hello from event"}`
	msgs := parseEventMsg([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "user" || msgs[0].Content != "hello from event" {
		t.Errorf("msg = %+v", msgs[0])
	}
}

func TestParseEventMsg_NonUserMessage(t *testing.T) {
	raw := `{"type":"agent_status","content":"working"}`
	msgs := parseEventMsg([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for non-user_message event")
	}
}

func TestParseEventMsg_EmptyContent(t *testing.T) {
	raw := `{"type":"user_message","content":""}`
	msgs := parseEventMsg([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for empty content")
	}
}

func TestParseEventMsg_WhitespaceContent(t *testing.T) {
	raw := `{"type":"user_message","content":"  \n "}`
	msgs := parseEventMsg([]byte(raw), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil for whitespace content")
	}
}

func TestParseEventMsg_InvalidJSON(t *testing.T) {
	msgs := parseEventMsg([]byte(`{broken`), fixedTime())
	if msgs != nil {
		t.Errorf("expected nil")
	}
}

// ---------------------------------------------------------------------------
// ReadSessionMessages — integration
// ---------------------------------------------------------------------------

func TestReadSessionMessages_EmptyFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "empty.jsonl")
	if err := os.WriteFile(f, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, err := ReadSessionMessages(f, 50)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
}

func TestReadSessionMessages_FileNotFound(t *testing.T) {
	_, err := ReadSessionMessages("/nonexistent/path.jsonl", 50)
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestReadSessionMessages_MixedTypes(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"s1","cwd":"/tmp","source":"cli"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}`,
		`{"timestamp":"2026-01-01T00:02:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi there"}]}}`,
		`{"timestamp":"2026-01-01T00:03:00Z","type":"response_item","payload":{"type":"function_call","name":"read","arguments":"{}"}}`,
		`{"timestamp":"2026-01-01T00:04:00Z","type":"response_item","payload":{"type":"function_call_output","output":"file data"}}`,
		`{"timestamp":"2026-01-01T00:05:00Z","type":"response_item","payload":{"type":"reasoning","summary":[{"type":"summary_text","text":"thinking"}]}}`,
		`{invalid json line}`,
		`{"timestamp":"2026-01-01T00:06:00Z","type":"event_msg","payload":{"type":"user_message","content":"event hello"}}`,
	}
	f := filepath.Join(t.TempDir(), "mixed.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, err := ReadSessionMessages(f, 0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	// session_meta → 0 msgs, user → 1, assistant → 1, function_call → 1,
	// function_call_output → 1, reasoning → 1, invalid → 0, event_msg → 1 = 6
	if len(msgs) != 6 {
		t.Fatalf("expected 6 messages, got %d", len(msgs))
	}
	wantRoles := []string{"user", "assistant", "tool", "tool_result", "thinking", "user"}
	for i, want := range wantRoles {
		if msgs[i].Role != want {
			t.Errorf("msgs[%d].Role = %q, want %q", i, msgs[i].Role, want)
		}
	}
}

func TestReadSessionMessages_MaxMessagesRingBuffer(t *testing.T) {
	var lines []string
	for i := 0; i < 10; i++ {
		lines = append(lines, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"msg`+string(rune('A'+i))+`"}]}}`)
	}
	f := filepath.Join(t.TempDir(), "ring.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, err := ReadSessionMessages(f, 3)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	// Should keep last 3: msgH, msgI, msgJ
	if msgs[0].Content != "msgH" || msgs[1].Content != "msgI" || msgs[2].Content != "msgJ" {
		t.Errorf("ring buffer: [%q, %q, %q]", msgs[0].Content, msgs[1].Content, msgs[2].Content)
	}
}

func TestReadSessionMessages_SystemInjectedSkipped(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"# AGENTS.md\nskill stuff"}]}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"real question"}]}}`,
	}
	f := filepath.Join(t.TempDir(), "skip.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, err := ReadSessionMessages(f, 0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (system injected skipped), got %d", len(msgs))
	}
	if msgs[0].Content != "real question" {
		t.Errorf("content = %q", msgs[0].Content)
	}
}

func TestReadSessionMessages_CommentaryPhase(t *testing.T) {
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"status update"}]}}`
	f := filepath.Join(t.TempDir(), "commentary.jsonl")
	if err := os.WriteFile(f, []byte(line+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, err := ReadSessionMessages(f, 0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "assistant_commentary" {
		t.Errorf("role = %q, want assistant_commentary", msgs[0].Role)
	}
}

func TestReadSessionMessages_CustomToolCalls(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"custom_tool_call","name":"my_tool","content":[{"type":"input_text","text":"input"}]}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"custom_tool_call_output","content":[{"type":"output_text","text":"output"}]}}`,
	}
	f := filepath.Join(t.TempDir(), "custom.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, err := ReadSessionMessages(f, 0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2, got %d", len(msgs))
	}
	if msgs[0].Role != "tool" {
		t.Errorf("msgs[0].Role = %q", msgs[0].Role)
	}
	if msgs[1].Role != "tool_result" {
		t.Errorf("msgs[1].Role = %q", msgs[1].Role)
	}
}
