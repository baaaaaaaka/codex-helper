package codexrunner

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestLaunchOutputRecorderCapsStdoutAndParsesResult(t *testing.T) {
	recorder := NewLaunchOutputRecorder(nil)
	writer := recorder.StdoutWriter()
	if _, err := writer.Write([]byte(strings.Repeat("x", defaultLaunchStdoutCaptureBytes+1024) + "\n")); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	if _, err := writer.Write([]byte(strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-capped"}`,
		`{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`,
		`{"type":"turn.completed"}`,
	}, "\n"))); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	result := recorder.LaunchResult(nil, 0)
	if !result.StdoutTruncated {
		t.Fatal("StdoutTruncated = false, want true")
	}
	if len(result.Stdout) > defaultLaunchStdoutCaptureBytes {
		t.Fatalf("stdout len = %d, want <= %d", len(result.Stdout), defaultLaunchStdoutCaptureBytes)
	}
	if result.ParsedResult == nil {
		t.Fatal("ParsedResult is nil")
	}
	if got := result.ParsedResult.FinalAgentMessage; got != "done" {
		t.Fatalf("final message = %q, want done", got)
	}
	if got := result.ParsedResult.ThreadID; got != "thread-capped" {
		t.Fatalf("thread id = %q, want thread-capped", got)
	}
}

func TestLaunchOutputRecorderCanOmitCommandOutputForTeamsStreaming(t *testing.T) {
	var events []StreamEvent
	recorder := NewLaunchOutputRecorderWithOptions(func(event StreamEvent) {
		events = append(events, event)
	}, LaunchOutputOptions{IncludeCommandOutput: false})
	_, err := recorder.StdoutWriter().Write([]byte(`{"type":"item.completed","thread_id":"thread-light","turn_id":"turn-light","item":{"id":"cmd-1","type":"command_execution","command":"go test ./...","aggregated_output":"very large output","exit_code":0,"status":"completed"}}` + "\n"))
	if err != nil {
		t.Fatalf("write command event: %v", err)
	}
	result := recorder.LaunchResult(nil, 0)
	if result.ParseErr != nil {
		t.Fatalf("parse error: %v", result.ParseErr)
	}
	if len(events) != 1 || events[0].Kind != StreamEventCommandCompleted {
		t.Fatalf("events = %#v, want command completed", events)
	}
	if events[0].ThreadID != "thread-light" || events[0].TurnID != "turn-light" {
		t.Fatalf("command event ids = %q/%q, want thread-light/turn-light", events[0].ThreadID, events[0].TurnID)
	}
	if events[0].AggregatedOutput != "" {
		t.Fatalf("command event = %#v, want output omitted", events[0])
	}
	if len(events[0].Raw) != 0 {
		t.Fatalf("raw event len = %d, want omitted", len(events[0].Raw))
	}
}

func TestLaunchOutputRecorderOmitCommandOutputStillReportsInvalidJSON(t *testing.T) {
	recorder := NewLaunchOutputRecorderWithOptions(nil, LaunchOutputOptions{IncludeCommandOutput: false})
	_, err := recorder.StdoutWriter().Write([]byte(`{"type":"item.completed","item":{"type":"command_execution","aggregated_output":"unterminated` + "\n"))
	if err != nil {
		t.Fatalf("write command event: %v", err)
	}
	result := recorder.LaunchResult(nil, 0)
	if !IsKind(result.ParseErr, ErrorParse) {
		t.Fatalf("ParseErr = %v, want parse failure", result.ParseErr)
	}
}

func TestLaunchOutputRecorderRecoverParseErrorsContinuesToRecoveredFinal(t *testing.T) {
	var events []StreamEvent
	recorder := NewLaunchOutputRecorderWithOptions(func(event StreamEvent) {
		events = append(events, event)
	}, LaunchOutputOptions{IncludeCommandOutput: false, RecoverParseErrors: true})
	_, err := recorder.StdoutWriter().Write([]byte(strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-recover"}`,
		`{"type":"item.completed","item":{"type":"command_execution","aggregated_output":"unterminated`,
		`{"type":"event_msg","thread_id":"thread-recover","payload":{"type":"agent_message","phase":"commentary","message":"working","turn_id":"turn-recover"}}`,
		`{"type":"event_msg","thread_id":"thread-recover","payload":{"type":"agent_message","phase":"final_answer","message":"done","turn_id":"turn-recover"}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("write stream: %v", err)
	}
	result := recorder.LaunchResult(nil, 0)
	if result.ParseErr != nil {
		t.Fatalf("ParseErr = %v, want recovered success", result.ParseErr)
	}
	if result.ParsedResult == nil {
		t.Fatal("ParsedResult is nil")
	}
	if got := result.ParsedResult.FinalAgentMessage; got != "done" {
		t.Fatalf("final message = %q, want done", got)
	}
	if got := result.ParsedResult.Status; got != TurnStatusCompleted {
		t.Fatalf("status = %q, want completed", got)
	}
	var agentMessages []string
	for _, event := range events {
		if event.Kind == StreamEventAgentMessage {
			agentMessages = append(agentMessages, event.Text)
		}
	}
	if strings.Join(agentMessages, "|") != "working|done" {
		t.Fatalf("agent messages = %#v, want working and done", agentMessages)
	}
}

func TestLaunchOutputRecorderRecoverParseErrorsWithoutTerminalReturnsFirstError(t *testing.T) {
	recorder := NewLaunchOutputRecorderWithOptions(nil, LaunchOutputOptions{IncludeCommandOutput: false, RecoverParseErrors: true})
	_, err := recorder.StdoutWriter().Write([]byte(strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-partial"}`,
		`{"type":"item.completed","item":{"type":"command_execution","aggregated_output":"unterminated`,
		`{"type":"event_msg","thread_id":"thread-partial","payload":{"type":"agent_message","phase":"commentary","message":"working","turn_id":"turn-partial"}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("write stream: %v", err)
	}
	result := recorder.LaunchResult(nil, 0)
	if !IsKind(result.ParseErr, ErrorParse) {
		t.Fatalf("ParseErr = %v, want parse failure", result.ParseErr)
	}
	if result.ParsedResult == nil || result.ParsedResult.ThreadID != "thread-partial" {
		t.Fatalf("parsed result = %#v, want partial thread id preserved", result.ParsedResult)
	}
}

func TestLaunchOutputRecorderOmitCommandOutputStillParsesFinalEvents(t *testing.T) {
	recorder := NewLaunchOutputRecorderWithOptions(nil, LaunchOutputOptions{IncludeCommandOutput: false})
	_, err := recorder.StdoutWriter().Write([]byte(strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-light"}`,
		`{"type":"item.completed","item":{"type":"agent_message","text":"final text"}}`,
		`{"type":"turn.completed","usage":{"cached_input_tokens":42}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("write stream: %v", err)
	}
	result := recorder.LaunchResult(nil, 0)
	if result.ParseErr != nil {
		t.Fatalf("parse error: %v", result.ParseErr)
	}
	if result.ParsedResult == nil || result.ParsedResult.ThreadID != "thread-light" || result.ParsedResult.FinalAgentMessage != "final text" || result.ParsedResult.Usage.CachedInputTokens != 42 {
		t.Fatalf("parsed result = %#v", result.ParsedResult)
	}
}

func TestExecRunnerPrefersParsedLaunchResult(t *testing.T) {
	parsed := TurnResult{
		ThreadID:          "thread-parsed",
		Status:            TurnStatusCompleted,
		FinalAgentMessage: "parsed result",
	}
	launcher := &recordingLauncher{
		result: LaunchResult{
			Stdout:       []byte("{bad json that would fail if reparsed}\n"),
			ParsedResult: &parsed,
		},
	}

	got, err := NewExecRunner(launcher).StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.ThreadID != "thread-parsed" || got.FinalAgentMessage != "parsed result" {
		t.Fatalf("result = %#v, want parsed launch result", got)
	}
}

func TestExecRunnerReturnsStreamingParseError(t *testing.T) {
	partial := TurnResult{ThreadID: "thread-partial"}
	launcher := &recordingLauncher{
		result: LaunchResult{
			ParsedResult: &partial,
			ParseErr:     newJSONLParseError(3, fmt.Errorf("bad json")),
		},
	}

	got, err := NewExecRunner(launcher).StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorParse) {
		t.Fatalf("StartThread error = %v, want parse failure", err)
	}
	if got.ThreadID != "thread-partial" {
		t.Fatalf("partial result = %#v, want parsed prefix preserved", got)
	}
}

func BenchmarkLaunchOutputRecorderLargeJSONL(b *testing.B) {
	benchmarkLaunchOutputRecorderLargeJSONL(b, LaunchOutputOptions{IncludeCommandOutput: true, IncludeRawEvent: true})
}

func BenchmarkLaunchOutputRecorderLargeJSONLOmitCommandOutput(b *testing.B) {
	benchmarkLaunchOutputRecorderLargeJSONL(b, LaunchOutputOptions{IncludeCommandOutput: false})
}

func benchmarkLaunchOutputRecorderLargeJSONL(b *testing.B, options LaunchOutputOptions) {
	payload := largeCodexJSONLForBenchmark(256, 64*1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events := 0
		recorder := NewLaunchOutputRecorderWithOptions(func(StreamEvent) {
			events++
		}, options)
		writer := recorder.StdoutWriter()
		for start := 0; start < len(payload); {
			end := start + 32*1024
			if end > len(payload) {
				end = len(payload)
			}
			if _, err := writer.Write(payload[start:end]); err != nil {
				b.Fatalf("write stream: %v", err)
			}
			start = end
		}
		result := recorder.LaunchResult(nil, 0)
		if result.ParseErr != nil {
			b.Fatalf("parse error: %v", result.ParseErr)
		}
		if result.ParsedResult == nil || result.ParsedResult.FinalAgentMessage != "done" {
			b.Fatalf("parsed result = %#v", result.ParsedResult)
		}
		if events == 0 {
			b.Fatal("stream handler did not receive events")
		}
	}
}

func largeCodexJSONLForBenchmark(lines int, payloadBytes int) []byte {
	var builder strings.Builder
	chunk := strings.Repeat("x", payloadBytes)
	builder.WriteString(`{"type":"thread.started","thread_id":"thread-large"}` + "\n")
	for i := 0; i < lines; i++ {
		builder.WriteString(fmt.Sprintf(`{"type":"item.completed","item":{"id":"cmd-%d","type":"command_execution","command":"printf x","aggregated_output":%q,"exit_code":0,"status":"completed"}}`+"\n", i, chunk))
	}
	builder.WriteString(`{"type":"item.completed","item":{"id":"final","type":"agent_message","text":"done"}}` + "\n")
	builder.WriteString(`{"type":"turn.completed"}` + "\n")
	return []byte(builder.String())
}
