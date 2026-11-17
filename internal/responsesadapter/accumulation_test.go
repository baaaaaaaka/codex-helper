package responsesadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResponseAccumulationPreservesFragmentsAndSnapshots(t *testing.T) {
	text, reasoning := "你好\nanswer", "thinking\n思考"
	stream := make(chan ProviderEvent, 100)
	for _, r := range text {
		stream <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: string(r)}
	}
	for _, r := range reasoning {
		stream <- ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: string(r)}
	}
	stream <- ProviderEvent{Kind: ProviderEventDone}
	f := &Facade{}
	result, ok := f.collectProviderResult(context.Background(), stream, nil)
	if !ok || result.text != text || result.reasoningText != reasoning {
		t.Fatalf("collected=%+v ok=%v", result, ok)
	}
	a := newToolCallAccumulator("test")
	index := 0
	call, _, err := a.apply(&ProviderToolCallDelta{Index: 0, Name: "tool", ID: "call", ArgumentsDelta: `{"x":"`}, &index)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := call.record("in_progress")
	if _, _, err := a.apply(&ProviderToolCallDelta{Index: 0, ArgumentsDelta: `你\n好"}`}, &index); err != nil {
		t.Fatal(err)
	}
	if snapshot.Arguments != `{"x":"` || call.record("completed").Arguments != `{"x":"你\n好"}` {
		t.Fatal("argument snapshot changed or fragments lost")
	}
	if _, _, err := a.apply(&ProviderToolCallDelta{Index: 0, ID: "different"}, &index); err == nil {
		t.Fatal("accepted changed call id")
	}
}

func TestResponseAccumulationInterleavesCallsAndTerminalStates(t *testing.T) {
	for _, terminal := range []ProviderEvent{{Kind: ProviderEventDone, FinishReason: "length"}, {Kind: ProviderEventError, Err: errors.New("provider failed")}} {
		stream := make(chan ProviderEvent, 10)
		stream <- ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: "r"}
		stream <- ProviderEvent{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 1, ID: "second", Name: "tool", ArgumentsDelta: `{"b":`}}
		stream <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: "answer"}
		stream <- ProviderEvent{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "first", Name: "tool", ArgumentsDelta: `{"a":1}`}}
		stream <- ProviderEvent{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 1, ArgumentsDelta: `2}`}}
		stream <- terminal
		got, ok := (&Facade{}).collectProviderResult(context.Background(), stream, nil)
		if got.text != "answer" || got.reasoningText != "r" || ok != (terminal.Kind == ProviderEventDone) {
			t.Fatalf("%+v %v", got, ok)
		}
		if ok && (got.finishReason != "length" || len(got.toolCalls) != 2 || got.toolCalls[0].ID != "second" || got.toolCalls[0].Arguments != `{"b":2}` || got.toolCalls[1].Arguments != `{"a":1}`) {
			t.Fatalf("order/arguments: %+v", got)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := (&Facade{}).collectProviderResult(ctx, make(chan ProviderEvent), nil); ok {
		t.Fatal("cancelled request completed")
	}
	closed := make(chan ProviderEvent)
	close(closed)
	if _, ok := (&Facade{}).collectProviderResult(context.Background(), closed, nil); ok {
		t.Fatal("closed stream completed")
	}
}

func TestResponseAccumulationStreamingMatchesCollectedText(t *testing.T) {
	stream := make(chan ProviderEvent, 8)
	for _, piece := range []string{"hello", "\n", "你好"} {
		stream <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: piece}
	}
	stream <- ProviderEvent{Kind: ProviderEventDone}
	w := httptest.NewRecorder()
	(&Facade{}).streamResponse(w, context.Background(), "response", ResponsesRequest{Model: "test"}, ProviderRequest{}, stream)
	var text strings.Builder
	var events []string
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event map[string]json.RawMessage
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		var kind string
		_ = json.Unmarshal(event["type"], &kind)
		events = append(events, kind)
		if kind == "response.output_text.delta" {
			var delta string
			_ = json.Unmarshal(event["delta"], &delta)
			text.WriteString(delta)
		}
	}
	if text.String() != "hello\n你好" || len(events) == 0 || events[0] != "response.created" || events[len(events)-1] != "response.completed" {
		t.Fatalf("text=%q events=%v body=%s", text.String(), events, w.Body.String())
	}
}

func TestResponseAccumulationAllocationsDoNotFollowFragmentCount(t *testing.T) {
	stream := make(chan ProviderEvent, 1001)
	f := &Facade{}
	allocs := testing.AllocsPerRun(5, func() {
		for range 1000 {
			stream <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: "abcdefgh"}
		}
		stream <- ProviderEvent{Kind: ProviderEventDone}
		result, ok := f.collectProviderResult(context.Background(), stream, nil)
		if !ok || len(result.text) != 8000 {
			t.Fatal("lost fragments")
		}
	})
	if allocs > 100 {
		t.Fatalf("fragment-sized allocations returned: %.0f", allocs)
	}
}
