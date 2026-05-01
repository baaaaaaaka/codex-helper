package codexrunner

import (
	"strings"
	"testing"
)

func TestParseJSONLExtractsThreadTurnMessageFailureAndCachedTokens(t *testing.T) {
	input := strings.Join([]string{
		"Reading additional input from stdin...",
		`{"type":"thread.started","thread_id":"thread-123"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.completed","item":{"id":"item-1","type":"agent_message","text":"first"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":12,"total_tokens":112,"cached_input_tokens":34}}`,
		`{"type":"item.completed","item":{"id":"item-2","type":"agent_message","text":"final"}}`,
		`{"type":"turn.failed","turn_id":"turn-2","error":{"code":"tool_error","message":"tool failed"},"usage":{"cached_input_tokens":55}}`,
	}, "\n")

	got, err := ParseJSONL(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseJSONL error: %v", err)
	}
	if got.ThreadID != "thread-123" {
		t.Fatalf("thread id = %q", got.ThreadID)
	}
	if got.TurnID != "turn-2" {
		t.Fatalf("turn id = %q", got.TurnID)
	}
	if got.FinalAgentMessage != "final" {
		t.Fatalf("final message = %q", got.FinalAgentMessage)
	}
	if got.Status != TurnStatusFailed {
		t.Fatalf("status = %q", got.Status)
	}
	if got.Failure == nil || got.Failure.Code != "tool_error" || got.Failure.Message != "tool failed" {
		t.Fatalf("failure = %#v", got.Failure)
	}
	if got.Usage.CachedInputTokens != 55 {
		t.Fatalf("cached input tokens = %d", got.Usage.CachedInputTokens)
	}
}

func TestParseJSONLAllowsOfficialExecEventsWithoutTurnID(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-123"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.completed","item":{"id":"item-1","type":"agent_message","content":[{"type":"output_text","text":"done"}]}}`,
		`{"type":"turn.completed","usage":{"cached_input_tokens":34}}`,
	}, "\n")

	got, err := ParseJSONL(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseJSONL error: %v", err)
	}
	if got.TurnID != "" {
		t.Fatalf("turn id = %q, want empty for official exec JSON", got.TurnID)
	}
	if got.Status != TurnStatusCompleted || got.FinalAgentMessage != "done" || got.Usage.CachedInputTokens != 34 {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestParseJSONLReturnsParseFailureForInvalidJSONEvent(t *testing.T) {
	_, err := ParseJSONL(strings.NewReader("{bad json}\n"))
	if !IsKind(err, ErrorParse) {
		t.Fatalf("expected parse failure, got %v", err)
	}
}
