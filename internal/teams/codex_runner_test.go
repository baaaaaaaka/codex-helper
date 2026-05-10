package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

func TestParseCodexJSONLExtractsThreadAndFinalAgentMessage(t *testing.T) {
	output := strings.Join([]string{
		"Reading additional input from stdin...",
		`{"type":"thread.started","thread_id":"019ddc51-618d-75c1-b508-8150cd20fb96"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"first"}}`,
		`{"type":"item.completed","item":{"id":"item_1","type":"agent_message","content":[{"type":"output_text","text":"final"}]}}`,
		`{"type":"turn.completed"}`,
	}, "\n")

	got := parseCodexJSONL(output)
	if got.CodexThreadID != "019ddc51-618d-75c1-b508-8150cd20fb96" {
		t.Fatalf("unexpected thread id %q", got.CodexThreadID)
	}
	if got.Text != "final" {
		t.Fatalf("unexpected final text %q", got.Text)
	}
	if got.CodexTurnID != "" {
		t.Fatalf("unexpected turn id %q for official exec JSON", got.CodexTurnID)
	}
}

func TestCodexExecutorDoesNotDuplicateExtraArgs(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("shell stub test uses POSIX script")
	}
	argsFile := filepath.Join(t.TempDir(), "args.txt")
	codexPath := filepath.Join(t.TempDir(), "codex")
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$@" > %q
cat >/dev/null
printf '%%s\n' '{"type":"thread.started","thread_id":"thread-new"}' '{"type":"turn.started"}' '{"type":"item.completed","item":{"type":"agent_message","text":"done"}}' '{"type":"turn.completed"}'
`, argsFile)
	if err := os.WriteFile(codexPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write codex stub: %v", err)
	}

	reasoningArg := CodexReasoningEffortConfigArg(DefaultControlFallbackReasoningEffort)
	executor := CodexExecutor{
		CodexPath: codexPath,
		WorkDir:   t.TempDir(),
		ExtraArgs: []string{"-c", reasoningArg},
	}
	got, err := executor.RunInput(context.Background(), &Session{}, ExecutionInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("RunInput error: %v", err)
	}
	if got.Text != "done" {
		t.Fatalf("result text = %q", got.Text)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	if count := strings.Count(string(args), reasoningArg); count != 1 {
		t.Fatalf("reasoning arg count = %d, want 1; args:\n%s", count, string(args))
	}
}

func TestRunnerExecutorDoesNotTreatExistingThreadIDErrorAsAccepted(t *testing.T) {
	runner := &fakeCodexRunner{
		result: codexrunner.TurnResult{ThreadID: "thread-existing"},
		err:    fmt.Errorf("codex_failure: Error: Failed to load cloud requirements (workspace-managed policies)."),
	}
	executor := RunnerExecutor{Runner: runner}
	got, err := executor.Run(context.Background(), &Session{CodexThreadID: "thread-existing"}, "continue")
	if err == nil {
		t.Fatal("Run error = nil, want failure")
	}
	if IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, should not be ambiguous when only the existing thread id is known", err)
	}
	if got.CodexThreadID != "thread-existing" || got.CodexTurnID != "" {
		t.Fatalf("unexpected execution result: %#v", got)
	}
}

func TestRunnerExecutorTreatsStartedTurnErrorAsAmbiguous(t *testing.T) {
	runner := &fakeCodexRunner{
		result: codexrunner.TurnResult{
			ThreadID:   "thread-existing",
			ThreadName: "Existing thread title",
			TurnID:     "turn-started",
			Status:     codexrunner.TurnStatusInProgress,
		},
		err: fmt.Errorf("stream disconnected before completion"),
	}
	executor := RunnerExecutor{Runner: runner}
	got, err := executor.Run(context.Background(), &Session{CodexThreadID: "thread-existing"}, "continue")
	if !IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, want ambiguous", err)
	}
	if got.CodexThreadID != "thread-existing" || got.CodexTurnID != "turn-started" {
		t.Fatalf("unexpected execution result: %#v", got)
	}
	if got.CodexThreadTitle != "Existing thread title" {
		t.Fatalf("thread title = %q", got.CodexThreadTitle)
	}
}

func TestRunnerExecutorDoesNotTreatTerminalFailedTurnAsAmbiguous(t *testing.T) {
	runner := &fakeCodexRunner{
		result: codexrunner.TurnResult{
			ThreadID: "thread-existing",
			TurnID:   "turn-failed",
			Status:   codexrunner.TurnStatusFailed,
			Failure:  &codexrunner.TurnFailure{Message: "model policy failed"},
		},
		err: fmt.Errorf("codex_failure: model policy failed"),
	}
	executor := RunnerExecutor{Runner: runner}
	got, err := executor.Run(context.Background(), &Session{CodexThreadID: "thread-existing"}, "continue")
	if err == nil {
		t.Fatal("Run error = nil, want failure")
	}
	if IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, should not be ambiguous for terminal failed turn", err)
	}
	if got.CodexThreadID != "thread-existing" || got.CodexTurnID != "turn-failed" {
		t.Fatalf("unexpected execution result: %#v", got)
	}
}

func TestRunnerExecutorPassesImageInputToCodexRunner(t *testing.T) {
	runner := &fakeCodexRunner{
		result: codexrunner.TurnResult{
			ThreadID:          "thread-new",
			TurnID:            "turn-1",
			Status:            codexrunner.TurnStatusCompleted,
			FinalAgentMessage: "saw image",
		},
	}
	executor := RunnerExecutor{Runner: runner}
	got, err := executor.RunInput(context.Background(), &Session{}, ExecutionInput{
		Prompt:     "inspect",
		ImagePaths: []string{"/tmp/a.png", "/tmp/b.webp"},
	})
	if err != nil {
		t.Fatalf("RunInput error: %v", err)
	}
	if got.Text != "saw image" {
		t.Fatalf("result = %#v", got)
	}
	if runner.input.Prompt != "inspect" || strings.Join(runner.input.ImagePaths, ",") != "/tmp/a.png,/tmp/b.webp" {
		t.Fatalf("runner input = %#v", runner.input)
	}
}

func TestSplitTextChunks(t *testing.T) {
	got := splitTextChunks("one two three four", 8)
	if len(got) != 3 {
		t.Fatalf("expected 3 chunks, got %#v", got)
	}
	if strings.Join(got, " ") != "one two three four" {
		t.Fatalf("unexpected chunks %#v", got)
	}
}

type fakeCodexRunner struct {
	result codexrunner.TurnResult
	err    error
	input  codexrunner.StartTurnInput
}

func (r *fakeCodexRunner) StartThread(context.Context, codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	return r.result, r.err
}

func (r *fakeCodexRunner) ResumeThread(context.Context, string, codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	return r.result, r.err
}

func (r *fakeCodexRunner) StartTurn(_ context.Context, input codexrunner.StartTurnInput) (codexrunner.TurnResult, error) {
	r.input = input
	return r.result, r.err
}

func (r *fakeCodexRunner) InterruptTurn(context.Context, codexrunner.TurnRef) error {
	return nil
}

func (r *fakeCodexRunner) ReadThread(context.Context, string) (codexrunner.Thread, error) {
	return codexrunner.Thread{}, nil
}

func (r *fakeCodexRunner) ListThreads(context.Context, codexrunner.ListThreadsOptions) ([]codexrunner.Thread, error) {
	return nil, nil
}

func TestSplitTextChunksForHTMLMessageUsesRenderedHTMLBytes(t *testing.T) {
	text := strings.Repeat("<&>", 200)
	chunks := splitTextChunksForHTMLMessage("Codex", text, 512)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %#v", chunks)
	}
	for _, chunk := range chunks {
		if got := len(HTMLMessage("Codex", chunk)); got > 512 {
			t.Fatalf("chunk rendered to %d HTML bytes, want <= 512", got)
		}
	}
	if strings.Join(chunks, "") != text {
		t.Fatalf("chunks did not preserve text")
	}
}

func TestTeamsChunkLimitLeavesRoomForPartLabels(t *testing.T) {
	text := strings.Repeat("&", 30000)
	chunks := splitTextChunksForHTMLMessage("Codex", text, teamsChunkHTMLContentBytes)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		body := chunk
		if len(chunks) > 1 {
			body = "part label headroom\n" + body
		}
		if got := len(HTMLMessage("Codex", body)); got > safeTeamsHTMLContentBytes {
			t.Fatalf("chunk %d rendered to %d HTML bytes, want <= %d", i, got, safeTeamsHTMLContentBytes)
		}
	}
}
