package codexrunner

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"testing"
)

func TestParseStreamEventJSONLCommandExecution(t *testing.T) {
	line := []byte(`{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"/usr/bin/zsh -lc pwd","aggregated_output":"/tmp/work\n","exit_code":0,"status":"completed"}}`)
	event, ok, err := ParseStreamEventJSONL(line)
	if err != nil {
		t.Fatalf("ParseStreamEventJSONL error: %v", err)
	}
	if !ok {
		t.Fatal("event was not recognized")
	}
	if event.Kind != StreamEventCommandCompleted || event.ItemID != "item_1" || event.Command == "" || event.AggregatedOutput != "/tmp/work\n" {
		t.Fatalf("unexpected command event: %#v", event)
	}
	if event.ExitCode == nil || *event.ExitCode != 0 {
		t.Fatalf("exit code = %#v, want 0", event.ExitCode)
	}
}

func TestParseStreamEventJSONLRetryableStreamError(t *testing.T) {
	line := []byte(`{"type":"error","threadId":"thread-1","turn_id":"turn-1","willRetry":true,"error":{"message":"Reconnecting... 2/5","codexErrorInfo":{"responseStreamDisconnected":{"httpStatusCode":null}}}}`)
	event, ok, err := ParseStreamEventJSONL(line)
	if err != nil {
		t.Fatalf("ParseStreamEventJSONL error: %v", err)
	}
	if !ok {
		t.Fatal("event was not recognized")
	}
	if event.Kind != StreamEventStreamRetry || !event.WillRetry {
		t.Fatalf("event kind/retry = %q/%v, want %q/true", event.Kind, event.WillRetry, StreamEventStreamRetry)
	}
	if event.ThreadID != "thread-1" || event.TurnID != "turn-1" {
		t.Fatalf("event ids = %q/%q", event.ThreadID, event.TurnID)
	}
	if event.Failure == nil || event.Failure.Code != "responseStreamDisconnected" || event.Failure.Message != "Reconnecting... 2/5" {
		t.Fatalf("failure = %#v", event.Failure)
	}
}

func TestParseStreamEventJSONLContextCompacted(t *testing.T) {
	lines := [][]byte{
		[]byte(`{"type":"context_compacted","thread_id":"thread-1","turn_id":"turn-1"}`),
		[]byte(`{"type":"event_msg","payload":{"type":"context_compacted","thread_id":"thread-2","turn_id":"turn-2"}}`),
		[]byte(`{"type":"response_item","payload":{"type":"context_compaction","threadId":"thread-3","turnId":"turn-3"}}`),
	}
	for i, line := range lines {
		event, ok, err := ParseStreamEventJSONL(line)
		if err != nil {
			t.Fatalf("case %d ParseStreamEventJSONL error: %v", i, err)
		}
		if !ok {
			t.Fatalf("case %d event was not recognized", i)
		}
		if event.Kind != StreamEventContextCompacted {
			t.Fatalf("case %d kind = %q, want %q", i, event.Kind, StreamEventContextCompacted)
		}
	}
}

func TestParseStreamEventJSONLFinalAnswerAndTaskComplete(t *testing.T) {
	lines := []struct {
		line     []byte
		wantKind StreamEventKind
		wantText string
		wantTurn string
	}{
		{
			line:     []byte(`{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","turn_id":"turn-1","message":"done from event"}}`),
			wantKind: StreamEventAgentMessage,
			wantText: "done from event",
			wantTurn: "turn-1",
		},
		{
			line:     []byte(`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","turnId":"turn-2","content":[{"type":"output_text","text":"done from response"}]}}`),
			wantKind: StreamEventAgentMessage,
			wantText: "done from response",
			wantTurn: "turn-2",
		},
		{
			line:     []byte(`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-3","last_agent_message":"done from task"}}`),
			wantKind: StreamEventTurnCompleted,
			wantText: "done from task",
			wantTurn: "turn-3",
		},
	}
	for i, tc := range lines {
		event, ok, err := ParseStreamEventJSONL(tc.line)
		if err != nil {
			t.Fatalf("case %d ParseStreamEventJSONL error: %v", i, err)
		}
		if !ok {
			t.Fatalf("case %d event was not recognized", i)
		}
		if event.Kind != tc.wantKind || event.Text != tc.wantText || event.TurnID != tc.wantTurn {
			t.Fatalf("case %d event = %#v", i, event)
		}
	}
}

func TestParseStreamEventJSONLDoesNotTreatLiteralFinalAnswerAsCompletion(t *testing.T) {
	line := []byte(`{"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"literal final_answer only"}]}}`)
	event, ok, err := ParseStreamEventJSONL(line)
	if err != nil {
		t.Fatalf("ParseStreamEventJSONL error: %v", err)
	}
	if ok {
		t.Fatalf("event = %#v, want unrecognized literal text", event)
	}
}

func TestEventStreamWriterEmitsJSONEventsAcrossWrites(t *testing.T) {
	var dst bytes.Buffer
	var events []StreamEvent
	writer := NewEventStreamWriter(&dst, func(event StreamEvent) {
		events = append(events, event)
	})

	_, _ = writer.Write([]byte(`{"type":"thread.started","thread_id":"thread-1"}` + "\n" + `{"type":"item.completed","item":{"type":"agent_message","text":"hel`))
	_, _ = writer.Write([]byte(`lo"}}` + "\n"))

	if !strings.Contains(dst.String(), "thread.started") || !strings.Contains(dst.String(), "agent_message") {
		t.Fatalf("writer did not preserve output: %q", dst.String())
	}
	if len(events) != 2 {
		t.Fatalf("events = %#v, want 2", events)
	}
	if events[0].Kind != StreamEventThreadStarted || events[0].ThreadID != "thread-1" {
		t.Fatalf("thread event = %#v", events[0])
	}
	if events[1].Kind != StreamEventAgentMessage || events[1].Text != "hello" {
		t.Fatalf("agent event = %#v", events[1])
	}
}

func TestDirectLauncherStreamsEventsWhileCapturingStdout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell command uses POSIX sh")
	}
	var events []StreamEvent
	result, err := DirectLauncher{}.Launch(context.Background(), LaunchRequest{
		Command: "/bin/sh",
		Args: []string{"-c", strings.Join([]string{
			`printf '%s\n' '{"type":"thread.started","thread_id":"thread-direct"}'`,
			`printf '%s\n' '{"type":"item.completed","item":{"type":"agent_message","text":"done"}}'`,
		}, "; ")},
		EventHandler: func(event StreamEvent) {
			events = append(events, event)
		},
	})
	if err != nil {
		t.Fatalf("Launch error: %v", err)
	}
	if !strings.Contains(string(result.Stdout), "thread-direct") {
		t.Fatalf("stdout was not captured: %s", string(result.Stdout))
	}
	if len(events) != 2 || events[0].ThreadID != "thread-direct" || events[1].Text != "done" {
		t.Fatalf("streamed events = %#v", events)
	}
}
