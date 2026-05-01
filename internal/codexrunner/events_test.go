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
