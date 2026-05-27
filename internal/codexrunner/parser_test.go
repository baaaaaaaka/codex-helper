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
		`{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":12,"reasoning_output_tokens":5,"total_tokens":112,"cached_input_tokens":34}}`,
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
	if got.Usage.ReasoningOutputTokens != 5 {
		t.Fatalf("reasoning output tokens = %d", got.Usage.ReasoningOutputTokens)
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

func TestParseJSONLExtractsThreadNameUpdates(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"thread.started","thread":{"id":"thread-123","name":"Initial title"}}`,
		`{"type":"thread.name.updated","threadId":"thread-123","threadName":"Generated title"}`,
		`{"type":"item.completed","item":{"id":"item-1","type":"agent_message","text":"done"}}`,
		`{"type":"turn.completed"}`,
	}, "\n")

	got, err := ParseJSONL(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseJSONL error: %v", err)
	}
	if got.ThreadID != "thread-123" {
		t.Fatalf("thread id = %q", got.ThreadID)
	}
	if got.ThreadName != "Generated title" {
		t.Fatalf("thread name = %q, want Generated title", got.ThreadName)
	}
}

func TestParseJSONLExtractsFinalAnswerEventMessage(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-session"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"final-1","turn_id":"turn-final","phase":"final_answer","message":"final from event msg"},"usage":{"cached_input_tokens":17}}`,
	}, "\n")

	got, err := ParseJSONL(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseJSONL error: %v", err)
	}
	if got.ThreadID != "thread-session" || got.TurnID != "turn-final" || got.Status != TurnStatusCompleted || got.FinalAgentMessage != "final from event msg" {
		t.Fatalf("result = %#v, want completed final from event_msg", got)
	}
	if got.Usage.CachedInputTokens != 17 {
		t.Fatalf("usage = %#v, want usage preserved from final event", got.Usage)
	}
	if len(got.RawCompletedMessage) == 0 {
		t.Fatal("RawCompletedMessage was not captured")
	}
}

func TestParseJSONLExtractsFinalAnswerResponseItem(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-1"}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"response "},{"type":"output_text","text":"final"}]}}`,
	}, "\n")

	got, err := ParseJSONL(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseJSONL error: %v", err)
	}
	if got.Status != TurnStatusCompleted || got.FinalAgentMessage != "response final" {
		t.Fatalf("result = %#v, want completed final from response_item", got)
	}
}

func TestParseJSONLExtractsTaskCompleteLastAgentMessage(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-1"}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1","last_agent_message":"final from task complete"}}`,
	}, "\n")

	got, err := ParseJSONL(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseJSONL error: %v", err)
	}
	if got.Status != TurnStatusCompleted || got.TurnID != "turn-1" || got.FinalAgentMessage != "final from task complete" {
		t.Fatalf("result = %#v, want completed task_complete", got)
	}
}

func TestParseJSONLDoesNotTreatLiteralFinalAnswerAsCompletion(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"the literal word final_answer is not terminal"}]}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","message":"phase final_answer appears in text only"}}`,
	}, "\n")

	got, err := ParseJSONL(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseJSONL error: %v", err)
	}
	if got.Status != TurnStatusUnknown || got.FinalAgentMessage != "" {
		t.Fatalf("result = %#v, want no completion", got)
	}
}

func TestParseJSONLFailureAfterFinalAnswerStillWins(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"final before failure"}}`,
		`{"type":"turn.failed","turn_id":"turn-1","error":{"code":"tool_error","message":"tool failed"}}`,
	}, "\n")

	got, err := ParseJSONL(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseJSONL error: %v", err)
	}
	if got.Status != TurnStatusFailed || got.Failure == nil || got.Failure.Message != "tool failed" {
		t.Fatalf("result = %#v, want later failure to win", got)
	}
	if got.FinalAgentMessage != "final before failure" {
		t.Fatalf("final message = %q", got.FinalAgentMessage)
	}
}

func TestParseJSONLReturnsParseFailureForInvalidJSONEvent(t *testing.T) {
	_, err := ParseJSONL(strings.NewReader("{bad json}\n"))
	if !IsKind(err, ErrorParse) {
		t.Fatalf("expected parse failure, got %v", err)
	}
}

func TestParseJSONLValidatesCommandExecutionEvents(t *testing.T) {
	_, err := ParseJSONL(strings.NewReader(`{"type":"item.completed","item":{"type":"command_execution","aggregated_output":"unterminated` + "\n"))
	if !IsKind(err, ErrorParse) {
		t.Fatalf("expected parse failure, got %v", err)
	}
}
