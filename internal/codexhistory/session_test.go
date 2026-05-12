package codexhistory

import (
	"os"
	"path/filepath"
	"strconv"
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

func TestParseResponseItem_AgentMessageTextContentArray(t *testing.T) {
	raw := `{"id":"agent-raw-1","type":"agent_message","text":[{"type":"output_text","text":"raw array answer"}]}`
	msgs := parseResponseItem([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "assistant" || msgs[0].Content != "raw array answer" {
		t.Fatalf("msg = %#v", msgs[0])
	}
	if msgs[0].sourceID != "agent_message:agent-raw-1" {
		t.Fatalf("sourceID = %q", msgs[0].sourceID)
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
// parseAgentMessagePayload
// ---------------------------------------------------------------------------

func TestParseAgentMessagePayload_TextContentArray(t *testing.T) {
	p := codexResponsePayload{
		ID:   "agent-1",
		Type: "agent_message",
		Text: []byte(`[{"type":"output_text","text":"array final answer"}]`),
	}
	msgs := parseAgentMessagePayload(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "assistant" || msgs[0].Content != "array final answer" {
		t.Fatalf("msg = %#v", msgs[0])
	}
	if msgs[0].sourceID != "agent_message:agent-1" {
		t.Fatalf("sourceID = %q", msgs[0].sourceID)
	}
}

func TestParseAgentMessagePayload_MessageString(t *testing.T) {
	p := codexResponsePayload{
		Type:    "agent_message",
		Message: []byte(`"string final answer"`),
	}
	msgs := parseAgentMessagePayload(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Content != "string final answer" {
		t.Fatalf("content = %q", msgs[0].Content)
	}
}

func TestParseAgentMessagePayload_CommentaryPhase(t *testing.T) {
	p := codexResponsePayload{
		Type:    "agent_message",
		Phase:   "commentary",
		Content: []byte(`[{"type":"output_text","text":"working"}]`),
	}
	msgs := parseAgentMessagePayload(p, fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if msgs[0].Role != "assistant_commentary" {
		t.Fatalf("role = %q", msgs[0].Role)
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

func TestParseFunctionCall_ObjectArgs(t *testing.T) {
	raw := `{"type":"function_call","name":"write_file","arguments":{"path":"/tmp/x","content":"data"}}`
	msgs := parseFunctionCall([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if !strings.Contains(msgs[0].Content, `"path"`) || !strings.Contains(msgs[0].Content, `/tmp/x`) {
		t.Fatalf("object arguments were not rendered: %q", msgs[0].Content)
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

func TestParseFunctionCallOutput_ObjectOutput(t *testing.T) {
	raw := `{"type":"function_call_output","output":{"stdout":"ok","exit_code":0}}`
	msgs := parseFunctionCallOutput([]byte(raw), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if !strings.Contains(msgs[0].Content, `"stdout"`) || !strings.Contains(msgs[0].Content, `"ok"`) {
		t.Fatalf("object output was not rendered: %q", msgs[0].Content)
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

func TestParseEventMsg_AgentMessagePhases(t *testing.T) {
	commentary := `{"type":"agent_message","phase":"commentary","message":"working"}`
	msgs := parseEventMsg([]byte(commentary), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("commentary messages = %#v, want 1", msgs)
	}
	if msgs[0].Role != "assistant_commentary" || msgs[0].Content != "working" {
		t.Fatalf("commentary msg = %#v", msgs[0])
	}

	final := `{"type":"agent_message","phase":"final_answer","message":"done"}`
	msgs = parseEventMsg([]byte(final), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("final messages = %#v, want 1", msgs)
	}
	if msgs[0].Role != "assistant" || msgs[0].Content != "done" {
		t.Fatalf("final msg = %#v", msgs[0])
	}

	arrayContent := `{"type":"agent_message","phase":"final_answer","content":[{"type":"output_text","text":"array done"}]}`
	msgs = parseEventMsg([]byte(arrayContent), fixedTime())
	if len(msgs) != 1 {
		t.Fatalf("array content messages = %#v, want 1", msgs)
	}
	if msgs[0].Role != "assistant" || msgs[0].Content != "array done" {
		t.Fatalf("array content msg = %#v", msgs[0])
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

func TestReadSessionPreviewMessagesFiltersToCodexStatusAndAnswer(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"s1","cwd":"/tmp","source":"cli"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"user prompt"}]}}`,
		`{"timestamp":"2026-01-01T00:02:00Z","type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"visible status"}}`,
		`{"timestamp":"2026-01-01T00:03:00Z","type":"response_item","payload":{"type":"function_call","name":"read","arguments":"{}"}}`,
		`{"timestamp":"2026-01-01T00:04:00Z","type":"response_item","payload":{"type":"function_call_output","output":"tool output"}}`,
		`{"timestamp":"2026-01-01T00:05:00Z","type":"response_item","payload":{"type":"reasoning","summary":[{"type":"summary_text","text":"hidden thinking"}]}}`,
		`{"timestamp":"2026-01-01T00:06:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"visible answer 1"}]}}`,
		`{"timestamp":"2026-01-01T00:07:00Z","type":"turn.failed","error":{"message":"visible failure"}}`,
		`{"timestamp":"2026-01-01T00:08:00Z","type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"visible answer 2"}}`,
	}
	f := filepath.Join(t.TempDir(), "preview.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	msgs, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages: %v", err)
	}
	if len(msgs) != 4 {
		t.Fatalf("messages = %#v, want 4 visible messages", msgs)
	}
	want := []struct {
		role string
		text string
	}{
		{"assistant_commentary", "visible status"},
		{"assistant", "visible answer 1"},
		{"assistant_commentary", "visible failure"},
		{"assistant", "visible answer 2"},
	}
	for i := range want {
		if msgs[i].Role != want[i].role || msgs[i].Content != want[i].text {
			t.Fatalf("msg[%d] = %#v, want role=%q text=%q", i, msgs[i], want[i].role, want[i].text)
		}
	}
}

func TestReadSessionPreviewMessagesParsesTurnFailedMethodRetryStatus(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","method":"turn/failed","params":{"turnId":"turn-1","willRetry":true}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","method":"turn/failed","params":{"turn":{"id":"turn-2"},"error":{"additionalDetails":"stream reset"}}}`,
	}
	f := filepath.Join(t.TempDir(), "preview-failed-method.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	msgs, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages = %#v, want 2 failed-turn statuses", msgs)
	}
	if msgs[0].Role != "assistant_commentary" || msgs[0].Content != "Codex stream disconnected; reconnecting" {
		t.Fatalf("retry status = %#v", msgs[0])
	}
	if msgs[1].Role != "assistant_commentary" || msgs[1].Content != "stream reset" {
		t.Fatalf("failure status = %#v", msgs[1])
	}
}

func TestReadSessionPreviewMessagesLimitCountsOnlyVisibleMessages(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"old visible"}]}}`,
	}
	for i := 0; i < 20; i++ {
		lines = append(lines, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","output":"hidden tool output"}}`)
	}
	lines = append(lines,
		`{"timestamp":"2026-01-01T00:00:02Z","type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"new status"}}`,
		`{"timestamp":"2026-01-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"new answer"}]}}`,
	)
	f := filepath.Join(t.TempDir(), "preview-limit.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	msgs, err := ReadSessionPreviewMessages(f, 2)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages: %v", err)
	}
	if got := len(msgs); got != 2 {
		t.Fatalf("len = %d, want 2", got)
	}
	if msgs[0].Content != "new status" || msgs[1].Content != "new answer" {
		t.Fatalf("messages = %#v, want last two visible messages", msgs)
	}
}

func TestReadSessionPreviewMessagesLimitExpandsTailWindow(t *testing.T) {
	prevTailBytes := sessionPreviewTailInitialBytes
	sessionPreviewTailInitialBytes = 64
	t.Cleanup(func() { sessionPreviewTailInitialBytes = prevTailBytes })

	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"old visible"}]}}`,
	}
	for i := 0; i < 20; i++ {
		lines = append(lines, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","output":"`+strings.Repeat("hidden", 20)+`"}}`)
	}
	lines = append(lines,
		`{"timestamp":"2026-01-01T00:00:02Z","type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"new status"}}`,
		`{"timestamp":"2026-01-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"new answer"}]}}`,
	)
	f := filepath.Join(t.TempDir(), "preview-tail-expand.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	msgs, err := ReadSessionPreviewMessages(f, 2)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages: %v", err)
	}
	if got := len(msgs); got != 2 {
		t.Fatalf("len = %d, want 2", got)
	}
	if msgs[0].Content != "new status" || msgs[1].Content != "new answer" {
		t.Fatalf("messages = %#v, want recent visible messages from expanded tail", msgs)
	}
}

func TestReadSessionPreviewMessagesDefaultKeepsCompleteCachedHistory(t *testing.T) {
	setTestUserCacheDir(t)
	f := filepath.Join(t.TempDir(), "preview-complete-cache.jsonl")
	initial := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"old","type":"message","role":"assistant","content":[{"type":"output_text","text":"old visible"}]}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","output":"hidden tool output"}}`,
		`{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"id":"middle","type":"message","role":"assistant","content":[{"type":"output_text","text":"middle visible"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(f, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}

	first, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages initial: %v", err)
	}
	if len(first) != 2 || first[0].Content != "old visible" || first[1].Content != "middle visible" {
		t.Fatalf("initial messages = %#v, want complete visible history", first)
	}

	appendText := `{"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"id":"status","type":"agent_message","phase":"commentary","message":"new status"}}` + "\n"
	file, err := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err := file.WriteString(appendText); err != nil {
		_ = file.Close()
		t.Fatalf("append: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close append: %v", err)
	}

	second, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages append: %v", err)
	}
	if len(second) != 3 || second[0].Content != "old visible" || second[1].Content != "middle visible" || second[2].Content != "new status" {
		t.Fatalf("appended messages = %#v, want cached full history plus appended tail", second)
	}
}

func TestReadSessionPreviewTextUsesFormattedCacheAndAppendsTail(t *testing.T) {
	setTestUserCacheDir(t)
	cachePath, err := sessionPreviewCacheFile()
	if err != nil {
		t.Fatalf("sessionPreviewCacheFile: %v", err)
	}
	var writes int
	prevHook := persistentCacheWriteHook
	persistentCacheWriteHook = func(path string) {
		if filepath.Clean(path) == filepath.Clean(cachePath) {
			writes++
		}
	}
	t.Cleanup(func() { persistentCacheWriteHook = prevHook })

	f := filepath.Join(t.TempDir(), "preview-text-cache.jsonl")
	initial := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"old","type":"message","role":"assistant","content":[{"type":"output_text","text":"old answer"}]}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","type":"event_msg","payload":{"id":"status","type":"agent_message","phase":"commentary","message":"middle status"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(f, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}

	first, err := ReadSessionPreviewText(f, 0, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewText initial: %v", err)
	}
	if !strings.Contains(first, "old answer") || !strings.Contains(first, "middle status") {
		t.Fatalf("initial text = %q, want complete formatted preview", first)
	}
	if writes != 1 {
		t.Fatalf("cache writes after initial read = %d, want 1", writes)
	}

	second, err := ReadSessionPreviewText(f, 0, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewText cached: %v", err)
	}
	if second != first {
		t.Fatalf("cached text changed: got %q want %q", second, first)
	}
	if writes != 1 {
		t.Fatalf("cache writes after exact hit = %d, want still 1", writes)
	}

	file, err := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err := file.WriteString(`{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"id":"new","type":"message","role":"assistant","content":[{"type":"output_text","text":"new answer"}]}}` + "\n"); err != nil {
		_ = file.Close()
		t.Fatalf("append: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close append: %v", err)
	}

	third, err := ReadSessionPreviewText(f, 0, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewText append: %v", err)
	}
	if !strings.Contains(third, "old answer") || !strings.Contains(third, "middle status") || !strings.Contains(third, "new answer") {
		t.Fatalf("appended text = %q, want cached text plus appended tail", third)
	}
	if writes != 2 {
		t.Fatalf("cache writes after append = %d, want 2", writes)
	}
}

func TestReadSessionPreviewMessagesCacheRewriteDoesNotKeepStaleHistory(t *testing.T) {
	setTestUserCacheDir(t)
	f := filepath.Join(t.TempDir(), "preview-rewrite-cache.jsonl")
	initial := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"old","type":"message","role":"assistant","content":[{"type":"output_text","text":"old visible"}]}}` + "\n"
	if err := os.WriteFile(f, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewMessages(f, 0); err != nil {
		t.Fatalf("ReadSessionPreviewMessages initial: %v", err)
	}

	rewrite := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"function_call_output","output":"` + strings.Repeat("hidden", 80) + `"}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"new","type":"message","role":"assistant","content":[{"type":"output_text","text":"new visible"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(f, []byte(rewrite), 0o644); err != nil {
		t.Fatal(err)
	}

	msgs, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages rewrite: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Content != "new visible" {
		t.Fatalf("rewrite messages = %#v, want stale cached message discarded", msgs)
	}
}

func TestReadSessionPreviewMessagesCacheDoesNotAdvancePastPartialTail(t *testing.T) {
	setTestUserCacheDir(t)
	f := filepath.Join(t.TempDir(), "preview-partial-cache.jsonl")
	initial := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"old","type":"message","role":"assistant","content":[{"type":"output_text","text":"old visible"}]}}` + "\n"
	if err := os.WriteFile(f, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages initial: %v", err)
	}
	if len(first) != 1 || first[0].Content != "old visible" {
		t.Fatalf("initial messages = %#v, want old visible", first)
	}

	partial := `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"new","type":"message","role":"assistant","content":[{"type":"output_text","text":"new`
	file, err := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open partial append: %v", err)
	}
	if _, err := file.WriteString(partial); err != nil {
		_ = file.Close()
		t.Fatalf("append partial: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close partial append: %v", err)
	}

	duringWrite, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages partial: %v", err)
	}
	if len(duringWrite) != 1 || duringWrite[0].Content != "old visible" {
		t.Fatalf("partial messages = %#v, want cache to keep only complete lines", duringWrite)
	}

	file, err = os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open completion append: %v", err)
	}
	if _, err := file.WriteString(` visible"}]}}` + "\n"); err != nil {
		_ = file.Close()
		t.Fatalf("append completion: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close completion append: %v", err)
	}

	complete, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages complete: %v", err)
	}
	if len(complete) != 2 || complete[0].Content != "old visible" || complete[1].Content != "new visible" {
		t.Fatalf("complete messages = %#v, want completed tail to be picked up", complete)
	}
}

func TestReadSessionPreviewMessagesDefaultKeepsUnterminatedCompleteLastLine(t *testing.T) {
	setTestUserCacheDir(t)
	f := filepath.Join(t.TempDir(), "preview-unterminated.jsonl")
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer","type":"message","role":"assistant","content":[{"type":"output_text","text":"visible without final newline"}]}}`
	if err := os.WriteFile(f, []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}

	msgs, err := ReadSessionPreviewMessages(f, 0)
	if err != nil {
		t.Fatalf("ReadSessionPreviewMessages: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Content != "visible without final newline" {
		t.Fatalf("messages = %#v, want unterminated complete last line to remain visible", msgs)
	}
}

func BenchmarkReadSessionPreviewMessagesTailLargeHiddenPrefix(b *testing.B) {
	dir := b.TempDir()
	f := filepath.Join(dir, "large-preview.jsonl")
	var body strings.Builder
	body.WriteString(`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"old visible"}]}}` + "\n")
	hiddenLine := `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","output":"` + strings.Repeat("hidden", 200) + `"}}` + "\n"
	for body.Len() < 8*1024*1024 {
		body.WriteString(hiddenLine)
	}
	body.WriteString(`{"timestamp":"2026-01-01T00:00:02Z","type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"new status"}}` + "\n")
	body.WriteString(`{"timestamp":"2026-01-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"new answer"}]}}` + "\n")
	if err := os.WriteFile(f, []byte(body.String()), 0o644); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msgs, err := ReadSessionPreviewMessages(f, 2)
		if err != nil {
			b.Fatalf("ReadSessionPreviewMessages: %v", err)
		}
		if len(msgs) != 2 || msgs[0].Content != "new status" || msgs[1].Content != "new answer" {
			b.Fatalf("messages = %#v, want recent status and answer", msgs)
		}
	}
}

func BenchmarkReadSessionPreviewMessagesCachedCompleteLarge(b *testing.B) {
	cacheDir := b.TempDir()
	b.Setenv("XDG_CACHE_HOME", cacheDir)
	b.Setenv("HOME", cacheDir)
	b.Setenv("LOCALAPPDATA", cacheDir)
	resetPersistentCacheStatesForTest()

	dir := b.TempDir()
	f := filepath.Join(dir, "large-complete-preview.jsonl")
	var body strings.Builder
	visibleLine := func(i int) string {
		return `{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"id":"answer-` + strconv.Itoa(i) + `","type":"message","role":"assistant","content":[{"type":"output_text","text":"visible answer ` + strconv.Itoa(i) + `"}]}}` + "\n"
	}
	hiddenLine := `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","output":"` + strings.Repeat("hidden", 200) + `"}}` + "\n"
	for i := 0; body.Len() < 8*1024*1024; i++ {
		body.WriteString(hiddenLine)
		if i%250 == 0 {
			body.WriteString(visibleLine(i))
		}
	}
	if err := os.WriteFile(f, []byte(body.String()), 0o644); err != nil {
		b.Fatal(err)
	}
	if msgs, err := ReadSessionPreviewMessages(f, 0); err != nil {
		b.Fatalf("warm cache: %v", err)
	} else if len(msgs) == 0 {
		b.Fatalf("warm cache returned no visible messages")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, err := ReadSessionPreviewMessages(f, 0)
		if err != nil {
			b.Fatalf("ReadSessionPreviewMessages: %v", err)
		}
		if len(msgs) == 0 {
			b.Fatalf("cached complete preview returned no visible messages")
		}
	}
}

func BenchmarkReadSessionPreviewTextCachedCompleteLarge(b *testing.B) {
	cacheDir := b.TempDir()
	b.Setenv("XDG_CACHE_HOME", cacheDir)
	b.Setenv("HOME", cacheDir)
	b.Setenv("LOCALAPPDATA", cacheDir)
	resetPersistentCacheStatesForTest()

	dir := b.TempDir()
	f := filepath.Join(dir, "large-complete-preview-text.jsonl")
	var body strings.Builder
	visibleLine := func(i int) string {
		return `{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"id":"answer-` + strconv.Itoa(i) + `","type":"message","role":"assistant","content":[{"type":"output_text","text":"visible answer ` + strconv.Itoa(i) + `"}]}}` + "\n"
	}
	hiddenLine := `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","output":"` + strings.Repeat("hidden", 200) + `"}}` + "\n"
	for i := 0; body.Len() < 8*1024*1024; i++ {
		body.WriteString(hiddenLine)
		if i%250 == 0 {
			body.WriteString(visibleLine(i))
		}
	}
	if err := os.WriteFile(f, []byte(body.String()), 0o644); err != nil {
		b.Fatal(err)
	}
	if text, err := ReadSessionPreviewText(f, 0, 0); err != nil {
		b.Fatalf("warm cache: %v", err)
	} else if text == "" {
		b.Fatalf("warm cache returned no visible text")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		text, err := ReadSessionPreviewText(f, 0, 0)
		if err != nil {
			b.Fatalf("ReadSessionPreviewText: %v", err)
		}
		if text == "" {
			b.Fatalf("cached complete preview returned no visible text")
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

func TestReadSessionMessages_DoesNotDedupeDifferentItemTypesWithSameCallID(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"function_call","call_id":"call-1","name":"read","arguments":"{}"}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"file data"}}`,
	}
	f := filepath.Join(t.TempDir(), "tool.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, err := ReadSessionMessages(f, 0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages = %#v, want tool call and tool result", msgs)
	}
	if msgs[0].Role != "tool" || msgs[1].Role != "tool_result" {
		t.Fatalf("roles = %q/%q, want tool/tool_result", msgs[0].Role, msgs[1].Role)
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

func TestReadSessionMessages_CompletedEventShapes(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"working"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"item.completed","item":{"id":"item-1","type":"agent_message","text":"item done"}}`,
		`{"timestamp":"2026-01-01T00:02:00Z","method":"item/completed","params":{"item":{"id":"item-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"method item done"}]}}}`,
		`{"timestamp":"2026-01-01T00:02:30Z","type":"response_item","payload":{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"question"}]}}`,
		`{"timestamp":"2026-01-01T00:02:31Z","type":"response_item","payload":{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}}`,
		`{"timestamp":"2026-01-01T00:03:00Z","method":"turn/completed","params":{"turn":{"items":[{"id":"sys","type":"message","role":"user","content":[{"type":"input_text","text":"# AGENTS.md\nskip me"}]},{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"question"}]},{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}}}`,
	}
	f := filepath.Join(t.TempDir(), "completed.jsonl")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, err := ReadSessionMessages(f, 0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("messages = %#v, want 5", msgs)
	}
	want := []struct {
		role string
		text string
	}{
		{"assistant_commentary", "working"},
		{"assistant", "item done"},
		{"assistant", "method item done"},
		{"user", "question"},
		{"assistant", "answer"},
	}
	for i, item := range want {
		if msgs[i].Role != item.role || msgs[i].Content != item.text {
			t.Fatalf("msgs[%d] = %#v, want role=%q text=%q", i, msgs[i], item.role, item.text)
		}
	}
}
