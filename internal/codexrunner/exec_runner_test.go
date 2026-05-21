package codexrunner

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type recordingLauncher struct {
	req    LaunchRequest
	result LaunchResult
	err    error
}

func (l *recordingLauncher) Launch(_ context.Context, req LaunchRequest) (LaunchResult, error) {
	l.req = req
	return l.result, l.err
}

func TestExecRunnerStartThreadBuildsJSONCommandAndParsesResult(t *testing.T) {
	launcher := &recordingLauncher{
		result: LaunchResult{Stdout: []byte(strings.Join([]string{
			`{"type":"thread.started","thread_id":"thread-new"}`,
			`{"type":"turn.started"}`,
			`{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`,
			`{"type":"turn.completed","usage":{"cached_input_tokens":9}}`,
		}, "\n"))},
	}
	runner := &ExecRunner{
		Launcher:   launcher,
		Command:    "/managed/codex",
		ExtraArgs:  []string{"--model", "gpt-5"},
		WorkingDir: "/work",
		Timeout:    time.Minute,
	}

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	wantArgs := []string{"exec", "--json", "--model", "gpt-5", "-C", "/work", "-"}
	if !reflect.DeepEqual(launcher.req.Args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", launcher.req.Args, wantArgs)
	}
	if launcher.req.Command != "/managed/codex" {
		t.Fatalf("command = %q", launcher.req.Command)
	}
	if launcher.req.Stdin != "hello" {
		t.Fatalf("stdin = %q", launcher.req.Stdin)
	}
	if got.ThreadID != "thread-new" || got.FinalAgentMessage != "done" || got.Usage.CachedInputTokens != 9 {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestExecRunnerResumeUsesExactThreadIDAndNeverLast(t *testing.T) {
	launcher := &recordingLauncher{
		result: LaunchResult{Stdout: []byte(strings.Join([]string{
			`{"type":"turn.started"}`,
			`{"type":"item.completed","item":{"type":"agent_message","text":"resumed"}}`,
			`{"type":"turn.completed"}`,
		}, "\n"))},
	}
	runner := NewExecRunner(launcher)

	got, err := runner.ResumeThread(context.Background(), "thread-exact", TurnInput{
		Prompt:    "continue",
		ExtraArgs: []string{"--model", "gpt-5"},
	})
	if err != nil {
		t.Fatalf("ResumeThread error: %v", err)
	}
	wantArgs := []string{"exec", "resume", "--json", "--model", "gpt-5", "thread-exact", "-"}
	if !reflect.DeepEqual(launcher.req.Args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", launcher.req.Args, wantArgs)
	}
	for _, arg := range launcher.req.Args {
		if arg == "--last" {
			t.Fatalf("resume args included --last: %#v", launcher.req.Args)
		}
	}
	if got.ThreadID != "thread-exact" {
		t.Fatalf("thread id = %q", got.ThreadID)
	}
}

func TestExecRunnerAttachesImagesToStartAndResume(t *testing.T) {
	launcher := &recordingLauncher{
		result: LaunchResult{Stdout: []byte(strings.Join([]string{
			`{"type":"thread.started","thread_id":"thread-new"}`,
			`{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`,
			`{"type":"turn.completed"}`,
		}, "\n"))},
	}
	runner := NewExecRunner(launcher)

	_, err := runner.StartThread(context.Background(), TurnInput{
		Prompt:     "inspect these",
		ImagePaths: []string{"/tmp/a.png", "  ", "/tmp/b.jpg"},
	})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	wantStartArgs := []string{"exec", "--json", "--image", "/tmp/a.png", "--image", "/tmp/b.jpg", "-"}
	if !reflect.DeepEqual(launcher.req.Args, wantStartArgs) {
		t.Fatalf("start args = %#v, want %#v", launcher.req.Args, wantStartArgs)
	}
	if launcher.req.Stdin != "inspect these" {
		t.Fatalf("start stdin = %q", launcher.req.Stdin)
	}

	launcher.result = LaunchResult{Stdout: []byte(strings.Join([]string{
		`{"type":"item.completed","item":{"type":"agent_message","text":"resumed"}}`,
		`{"type":"turn.completed"}`,
	}, "\n"))}
	_, err = runner.ResumeThread(context.Background(), "thread-existing", TurnInput{
		Prompt:     "continue",
		ImagePaths: []string{"/tmp/c.webp"},
	})
	if err != nil {
		t.Fatalf("ResumeThread error: %v", err)
	}
	wantResumeArgs := []string{"exec", "resume", "--json", "--image", "/tmp/c.webp", "thread-existing", "-"}
	if !reflect.DeepEqual(launcher.req.Args, wantResumeArgs) {
		t.Fatalf("resume args = %#v, want %#v", launcher.req.Args, wantResumeArgs)
	}
}

func TestExecRunnerResumeThreadFailureKeepsExistingThreadIDOnly(t *testing.T) {
	launcher := &recordingLauncher{
		result: LaunchResult{
			Stderr:   []byte("Error: Failed to load cloud requirements (workspace-managed policies)."),
			ExitCode: 1,
		},
	}
	runner := NewExecRunner(launcher)

	got, err := runner.ResumeThread(context.Background(), "thread-existing", TurnInput{Prompt: "continue"})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("ResumeThread error = %v, want codex failure", err)
	}
	if got.ThreadID != "thread-existing" || got.TurnID != "" || got.Status != TurnStatusUnknown {
		t.Fatalf("unexpected result: %#v", got)
	}
	if !strings.Contains(err.Error(), "Failed to load cloud requirements") {
		t.Fatalf("error did not preserve stderr: %v", err)
	}
}

func TestExecRunnerResumeTranslatesSandboxArgForCurrentCodexCLI(t *testing.T) {
	launcher := &recordingLauncher{
		result: LaunchResult{Stdout: []byte(strings.Join([]string{
			`{"type":"turn.started"}`,
			`{"type":"item.completed","item":{"type":"agent_message","text":"resumed"}}`,
			`{"type":"turn.completed"}`,
		}, "\n"))},
	}
	runner := &ExecRunner{
		Launcher:  launcher,
		ExtraArgs: []string{"--model", "gpt-5", "--sandbox", "workspace-write", "--skip-git-repo-check"},
	}

	_, err := runner.ResumeThread(context.Background(), "thread-exact", TurnInput{Prompt: "continue"})
	if err != nil {
		t.Fatalf("ResumeThread error: %v", err)
	}
	wantArgs := []string{"exec", "resume", "--json", "--model", "gpt-5", "-c", `sandbox_mode="workspace-write"`, "--skip-git-repo-check", "thread-exact", "-"}
	if !reflect.DeepEqual(launcher.req.Args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", launcher.req.Args, wantArgs)
	}
}

func TestExecRunnerRejectsLastInResumeArgs(t *testing.T) {
	runner := &ExecRunner{Launcher: &recordingLauncher{}, ExtraArgs: []string{"--last"}}
	_, err := runner.ResumeThread(context.Background(), "thread-exact", TurnInput{Prompt: "continue"})
	if !IsKind(err, ErrorInvalidRequest) {
		t.Fatalf("expected invalid request, got %v", err)
	}

	runner = &ExecRunner{Launcher: &recordingLauncher{}, ExtraArgs: []string{"--last=true"}}
	_, err = runner.ResumeThread(context.Background(), "thread-exact", TurnInput{Prompt: "continue"})
	if !IsKind(err, ErrorInvalidRequest) {
		t.Fatalf("expected invalid request for --last=true, got %v", err)
	}
}

func TestExecRunnerSurfacesTimeoutAndCancelDistinctly(t *testing.T) {
	timeoutLauncher := &recordingLauncher{err: context.DeadlineExceeded}
	runner := NewExecRunner(timeoutLauncher)
	_, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorTimeout) {
		t.Fatalf("expected timeout, got %v", err)
	}

	cancelLauncher := &recordingLauncher{err: context.Canceled}
	runner = NewExecRunner(cancelLauncher)
	_, err = runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorCanceled) {
		t.Fatalf("expected cancel, got %v", err)
	}
}

func TestExecRunnerTreatsCompletedTurnWithCanceledLaunchAsSuccess(t *testing.T) {
	launcher := &recordingLauncher{
		result: LaunchResult{Stdout: []byte(strings.Join([]string{
			`{"type":"thread.started","thread_id":"thread-done"}`,
			`{"type":"turn.started","turn_id":"turn-done"}`,
			`{"type":"item.completed","item":{"id":"item-final","type":"agent_message","text":"done despite canceled context"}}`,
			`{"type":"turn.completed"}`,
		}, "\n"))},
		err: context.Canceled,
	}
	runner := NewExecRunner(launcher)
	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error = %v, want completed turn success", err)
	}
	if got.ThreadID != "thread-done" || got.TurnID != "turn-done" || got.FinalAgentMessage != "done despite canceled context" {
		t.Fatalf("result = %#v, want completed turn metadata and final text", got)
	}
}

func TestExecRunnerTreatsFinalAnswerWithCanceledLaunchAsSuccess(t *testing.T) {
	launcher := &recordingLauncher{
		result: LaunchResult{Stdout: []byte(strings.Join([]string{
			`{"type":"session_meta","payload":{"id":"thread-done"}}`,
			`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"turn-done","phase":"final_answer","message":"done before canceled context"}}`,
			`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-done","last_agent_message":"done before canceled context"}}`,
		}, "\n"))},
		err: context.Canceled,
	}
	runner := NewExecRunner(launcher)
	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error = %v, want final_answer success", err)
	}
	if got.ThreadID != "thread-done" || got.TurnID != "turn-done" || got.FinalAgentMessage != "done before canceled context" {
		t.Fatalf("result = %#v, want final_answer metadata and final text", got)
	}
}

func TestExecRunnerDoesNotTreatFinalAnswerWithNonZeroExitAsSuccess(t *testing.T) {
	launcher := &recordingLauncher{result: LaunchResult{
		Stdout: []byte(strings.Join([]string{
			`{"type":"session_meta","payload":{"id":"thread-done"}}`,
			`{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"final before bad exit"}}`,
		}, "\n")),
		Stderr:   []byte("codex exited badly"),
		ExitCode: 1,
	}}
	runner := NewExecRunner(launcher)
	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("StartThread error = %v, want codex failure", err)
	}
	if got.Status != TurnStatusCompleted || got.FinalAgentMessage != "final before bad exit" {
		t.Fatalf("result = %#v, want parsed final metadata despite failure", got)
	}
}

func TestExecRunnerDistinguishesLaunchAndCodexFailures(t *testing.T) {
	launchErr := errors.New("fork failed")
	runner := NewExecRunner(&recordingLauncher{err: launchErr})
	_, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorLaunch) {
		t.Fatalf("expected launch error, got %v", err)
	}

	runner = NewExecRunner(&recordingLauncher{result: LaunchResult{
		Stdout:   []byte(`{"type":"turn.failed","error":{"message":"model failed"}}` + "\n"),
		ExitCode: 1,
	}})
	_, err = runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("expected codex error, got %v", err)
	}
}
