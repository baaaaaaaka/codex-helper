package responsesadapter

import (
	"encoding/json"
	"testing"
)

func TestParseInputCollapsesAssistantToolCallTurn(t *testing.T) {
	parsed, err := parseInput(json.RawMessage(`[
		{"type":"message","role":"user","content":[{"type":"input_text","text":"do both"}]},
		{"type":"message","role":"assistant","content":[{"type":"output_text","text":"I will use tools"}]},
		{"type":"function_call","call_id":"call_a","name":"tool_a","arguments":"{\"a\":1}"},
		{"type":"function_call","call_id":"call_b","name":"tool_b","arguments":"{\"b\":2}"},
		{"type":"function_call_output","call_id":"call_a","output":"A"},
		{"type":"function_call_output","call_id":"call_b","output":"B"}
	]`))
	if err != nil {
		t.Fatalf("parseInput error: %v", err)
	}
	if len(parsed.Messages) != 4 {
		t.Fatalf("messages = %#v", parsed.Messages)
	}
	assistant := parsed.Messages[1]
	if assistant.Role != "assistant" || assistant.Content != "I will use tools" || len(assistant.ToolCalls) != 2 {
		t.Fatalf("assistant message = %#v", assistant)
	}
	if assistant.ToolCalls[0].ID != "call_a" || assistant.ToolCalls[1].ID != "call_b" {
		t.Fatalf("tool calls = %#v", assistant.ToolCalls)
	}
	if parsed.Messages[2].Role != "tool" || parsed.Messages[2].ToolCallID != "call_a" || parsed.Messages[3].ToolCallID != "call_b" {
		t.Fatalf("tool outputs = %#v", parsed.Messages[2:])
	}
}

func TestParseInputFoldsReasoningIntoAssistantToolCallTurn(t *testing.T) {
	parsed, err := parseInput(json.RawMessage(`[
		{"type":"message","role":"user","content":"run"},
		{"type":"function_call","call_id":"call_a","name":"tool_a","arguments":"{}"},
		{"type":"reasoning","summary":[{"type":"summary_text","text":"after deciding"}]},
		{"type":"function_call_output","call_id":"call_a","output":"ok"}
	]`))
	if err != nil {
		t.Fatalf("parseInput error: %v", err)
	}
	if len(parsed.Messages) != 3 {
		t.Fatalf("messages = %#v", parsed.Messages)
	}
	assistant := parsed.Messages[1]
	if assistant.Role != "assistant" || assistant.ReasoningContent != "after deciding" || len(assistant.ToolCalls) != 1 {
		t.Fatalf("assistant = %#v", assistant)
	}
	if parsed.Messages[2].Role != "tool" || parsed.Messages[2].ToolCallID != "call_a" {
		t.Fatalf("tool = %#v", parsed.Messages[2])
	}
}

func TestParseInputDropsStandaloneReasoningItems(t *testing.T) {
	parsed, err := parseInput(json.RawMessage(`[
		{"type":"message","role":"user","content":"first turn"},
		{"type":"reasoning","summary":[{"type":"summary_text","text":"orphan reasoning"}]},
		{"type":"message","role":"user","content":"second turn"}
	]`))
	if err != nil {
		t.Fatalf("parseInput error: %v", err)
	}
	if len(parsed.Messages) != 2 {
		t.Fatalf("messages = %#v", parsed.Messages)
	}
	if parsed.Messages[0].Role != "user" || parsed.Messages[0].Content != "first turn" {
		t.Fatalf("first = %#v", parsed.Messages[0])
	}
	if parsed.Messages[1].Role != "user" || parsed.Messages[1].Content != "second turn" {
		t.Fatalf("second = %#v", parsed.Messages[1])
	}
	if parsed.Text != "first turn\nsecond turn" {
		t.Fatalf("text = %q", parsed.Text)
	}
}

func TestParseInputSynthesizesMissingToolOutputPlaceholder(t *testing.T) {
	parsed, err := parseInput(json.RawMessage(`[
		{"type":"message","role":"user","content":"run"},
		{"type":"function_call","call_id":"call_a","name":"tool_a","arguments":"{}"},
		{"type":"function_call","call_id":"call_b","name":"tool_b","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_a","output":"A"},
		{"type":"message","role":"user","content":"next"}
	]`))
	if err != nil {
		t.Fatalf("parseInput error: %v", err)
	}
	if len(parsed.Messages) != 5 {
		t.Fatalf("messages = %#v", parsed.Messages)
	}
	if parsed.Messages[3].Role != "tool" || parsed.Messages[3].ToolCallID != "call_b" || parsed.Messages[3].Content != "Tool output missing." {
		t.Fatalf("placeholder = %#v", parsed.Messages[3])
	}
	if parsed.Messages[4].Role != "user" || parsed.Messages[4].Content != "next" {
		t.Fatalf("tail message = %#v", parsed.Messages[4])
	}
}

func TestParseInputExtractsCodexToolOutputPayloadShapes(t *testing.T) {
	parsed, err := parseInput(json.RawMessage(`[
		{"type":"function_call","call_id":"call_string","name":"tool","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_string","output":"plain text"},
		{"type":"function_call","call_id":"call_content_items","name":"tool","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_content_items","output":[{"type":"input_text","text":"content item text"}]},
		{"type":"function_call","call_id":"call_object","name":"tool","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_object","output":{"content":"object text","success":true}},
		{"type":"function_call","call_id":"call_object_items","name":"tool","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_object_items","output":{"content":[{"type":"input_text","text":"object content item text"}],"success":false}}
	]`))
	if err != nil {
		t.Fatalf("parseInput error: %v", err)
	}
	if len(parsed.Messages) != 8 {
		t.Fatalf("messages = %#v", parsed.Messages)
	}
	want := map[string]string{
		"call_string":        "plain text",
		"call_content_items": "content item text",
		"call_object":        "object text",
		"call_object_items":  "object content item text",
	}
	for _, message := range parsed.Messages {
		if message.Role != "tool" {
			continue
		}
		if got, ok := want[message.ToolCallID]; !ok || message.Content != got {
			t.Fatalf("tool output %q = %q, want %q; messages=%#v", message.ToolCallID, message.Content, got, parsed.Messages)
		}
		delete(want, message.ToolCallID)
	}
	if len(want) != 0 {
		t.Fatalf("missing tool outputs: %#v", want)
	}
}

func TestParseInputSalvagesHistoricalFunctionCallArguments(t *testing.T) {
	parsed, err := parseInput(json.RawMessage(`[
		{"type":"function_call","call_id":"valid","name":"tool","arguments":"{\"cmd\":\"ls\"}"},
		{"type":"function_call_output","call_id":"valid","output":"ok"},
		{"type":"function_call","call_id":"empty","name":"tool","arguments":""},
		{"type":"function_call_output","call_id":"empty","output":"ok"},
		{"type":"function_call","call_id":"truncated","name":"tool","arguments":"{\"cmd\":"},
		{"type":"function_call_output","call_id":"truncated","output":"ok"}
	]`))
	if err != nil {
		t.Fatalf("parseInput error: %v", err)
	}
	want := map[string]string{
		"valid":     `{"cmd":"ls"}`,
		"empty":     "{}",
		"truncated": "{}",
	}
	for _, message := range parsed.Messages {
		for _, call := range message.ToolCalls {
			if got := call.Arguments; got != want[call.ID] {
				t.Fatalf("call %s arguments = %q, want %q; messages=%#v", call.ID, got, want[call.ID], parsed.Messages)
			}
			delete(want, call.ID)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing calls: %#v", want)
	}
}

func TestParseInputKeepsImageOnlyMessageVisibleAsTextPlaceholder(t *testing.T) {
	parsed, err := parseInput(json.RawMessage(`[
		{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,abc"}]}
	]`))
	if err != nil {
		t.Fatalf("parseInput error: %v", err)
	}
	if len(parsed.Messages) != 1 {
		t.Fatalf("messages = %#v", parsed.Messages)
	}
	if parsed.Messages[0].Content != "image attachment omitted" || parsed.Text != "image attachment omitted" {
		t.Fatalf("parsed = %#v", parsed)
	}
}
