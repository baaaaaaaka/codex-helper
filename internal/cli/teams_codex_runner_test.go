package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// Legacy direct-exec launcher tests were replaced by app-server contract tests below.

func TestTeamsCodexChildEnvExposesHelperCLIPathAndDirWithoutMutatingPATH(t *testing.T) {
	prevExecutablePath := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = prevExecutablePath })

	dir := filepath.Join(t.TempDir(), "helper bin")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir helper dir: %v", err)
	}
	exe := filepath.Join(dir, "cxp")
	teamsChildExecutablePath = func() (string, error) { return exe, nil }
	t.Setenv("PATH", "/usr/bin:/bin")

	got := teamsCodexChildEnv()
	if !hasEnvValue(got, envTeamsHelperCLIPath, exe) {
		t.Fatalf("expected helper CLI path env: %#v", got)
	}
	if !hasEnvValue(got, envTeamsHelperCLIDir, dir) {
		t.Fatalf("expected helper CLI dir env: %#v", got)
	}
	if _, ok := sliceEnvValue(got, "PATH"); ok {
		t.Fatalf("child environment must not override the user's PATH: %#v", got)
	}
}

func TestTeamsCodexChildEnvDoesNotCopyExistingPATH(t *testing.T) {
	prevExecutablePath := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = prevExecutablePath })

	dir := t.TempDir()
	teamsChildExecutablePath = func() (string, error) { return filepath.Join(dir, "codex-proxy"), nil }
	t.Setenv("PATH", dir+string(os.PathListSeparator)+"/usr/bin")

	got := teamsCodexChildEnv()
	if _, ok := sliceEnvValue(got, "PATH"); ok {
		t.Fatalf("child environment copied PATH: %#v", got)
	}
}

func TestTeamsCodexChildEnvDoesNotExposeTransientHelperPath(t *testing.T) {
	prevExecutablePath := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = prevExecutablePath })

	dir := t.TempDir()
	running := filepath.Join(dir, ".nfs802014de01c482a800000492")
	teamsChildExecutablePath = func() (string, error) { return running, nil }
	t.Setenv("PATH", "/usr/bin:/bin")

	got := teamsCodexChildEnv()
	if _, ok := sliceEnvValue(got, envTeamsHelperCLIPath); ok {
		t.Fatalf("transient helper path should not be exposed: %#v", got)
	}
	if _, ok := sliceEnvValue(got, envTeamsHelperCLIDir); ok {
		t.Fatalf("transient helper dir should not be exposed: %#v", got)
	}
	if path := envValue(got, "PATH"); strings.Contains(path, dir) {
		t.Fatalf("PATH should not include transient helper dir, got %q", path)
	}
}

func TestTeamsCodexChildEnvExposesRecoveredStableHelperPath(t *testing.T) {
	prevExecutablePath := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = prevExecutablePath })

	dir := t.TempDir()
	name := "codex-proxy"
	if runtime.GOOS == "windows" {
		name = "codex-proxy.exe"
	}
	stable := filepath.Join(dir, name)
	if err := os.WriteFile(stable, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write stable helper: %v", err)
	}
	running := filepath.Join(dir, ".nfs802014de01c482a800000492")
	teamsChildExecutablePath = func() (string, error) { return running, nil }
	t.Setenv("PATH", "/usr/bin:/bin")

	got := teamsCodexChildEnv()
	if !hasEnvValue(got, envTeamsHelperCLIPath, stable) {
		t.Fatalf("expected recovered stable helper path %q: %#v", stable, got)
	}
	if !hasEnvValue(got, envTeamsHelperCLIDir, dir) {
		t.Fatalf("expected recovered helper dir %q: %#v", dir, got)
	}
}

func TestTeamsCodexChildEnvAbsoluteHelperPathRemainsRunnable(t *testing.T) {
	prevExecutablePath := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = prevExecutablePath })
	if os.PathSeparator != '/' {
		t.Skip("shell PATH lookup test uses POSIX shell")
	}

	dir := t.TempDir()
	exe := filepath.Join(dir, "cxp")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write helper stub: %v", err)
	}
	teamsChildExecutablePath = func() (string, error) { return exe, nil }
	t.Setenv("PATH", "/usr/bin:/bin")

	helperPath := envValue(teamsCodexChildEnv(), envTeamsHelperCLIPath)
	cmd := exec.Command(helperPath)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run explicit helper path: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "" {
		t.Fatalf("helper output = %q, want empty", got)
	}
}

func hasEnvValue(env []string, key string, want string) bool {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimPrefix(item, prefix) == want
		}
	}
	return false
}

func TestTeamsCodexExecutorResumesExistingSession(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:          "thread-existing",
		ThreadName:        "Existing thread title",
		TurnID:            "turn-existing",
		FinalAgentMessage: "final",
	}}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{
		CodexThreadID:   "thread-existing",
		ModelProfile:    modelprofile.Snapshot{Provider: modelprofile.DefaultProvider, Model: "gpt-next"},
		ReasoningEffort: "high",
	}, "continue")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !runner.resumed || runner.threadID != "thread-existing" {
		t.Fatalf("expected resume with exact thread id, runner=%#v", runner)
	}
	if !runner.input.BackfillThreadName {
		t.Fatal("auto-title session should request thread name backfill")
	}
	if runner.input.ReasoningEffort != "high" {
		t.Fatalf("reasoning effort = %q, want high", runner.input.ReasoningEffort)
	}
	if runner.input.Model != "gpt-next" {
		t.Fatalf("turn model = %q, want gpt-next", runner.input.Model)
	}
	if got.Text != "final" || got.CodexThreadID != "thread-existing" || got.CodexTurnID != "turn-existing" {
		t.Fatalf("unexpected result: %#v", got)
	}
	if got.CodexThreadTitle != "Existing thread title" {
		t.Fatalf("thread title = %q", got.CodexThreadTitle)
	}
}

func TestTeamsCodexExecutorUsesSessionCwdForNewThread(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:          "thread-new",
		TurnID:            "turn-new",
		FinalAgentMessage: "final",
	}}
	executor := teamsCodexExecutor{runner: runner, workDir: "/helper/default"}
	_, err := executor.Run(context.Background(), &teams.Session{
		Cwd:          "  /workspace/project  ",
		ModelProfile: modelprofile.Snapshot{Provider: modelprofile.DefaultProvider, Model: "gpt-new-thread"},
	}, "start")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if runner.resumed {
		t.Fatal("new session should start a new thread, not resume")
	}
	if runner.input.WorkingDir != "/workspace/project" {
		t.Fatalf("working dir = %q, want session cwd", runner.input.WorkingDir)
	}
	if runner.input.Model != "gpt-new-thread" {
		t.Fatalf("new-thread model = %q, want gpt-new-thread", runner.input.Model)
	}
}

func TestTeamsCodexExecutorReasoningEffortCatalogUsesPinnedModelAndPreservesOptions(t *testing.T) {
	runner := &fakeTeamsRunner{models: []codexrunner.ModelInfo{
		{ID: "default", Model: "gpt-default", IsDefault: true, DefaultReasoningEffort: "medium"},
		{ID: "pinned", Model: "provider/pinned", DisplayName: "Pinned", DefaultReasoningEffort: "high", ReasoningEfforts: []codexrunner.ReasoningEffortOption{
			{Effort: "low", Description: "fast"},
			{Effort: "xhigh", Description: "deep"},
		}},
	}}
	executor := teamsCodexExecutor{runner: runner}
	catalog, err := executor.ReasoningEffortCatalog(context.Background(), &teams.Session{ModelProfile: modelprofile.Snapshot{Model: "provider/pinned"}})
	if err != nil {
		t.Fatalf("ReasoningEffortCatalog: %v", err)
	}
	if catalog.Model != "provider/pinned" || catalog.DefaultEffort != "high" || len(catalog.Options) != 2 {
		t.Fatalf("catalog = %#v", catalog)
	}
	if catalog.Options[0].Effort != "low" || catalog.Options[1].Effort != "xhigh" {
		t.Fatalf("option order = %#v", catalog.Options)
	}
}

func TestTeamsCodexExecutorReasoningEffortCatalogRejectsMissingPinnedModel(t *testing.T) {
	runner := &fakeTeamsRunner{models: []codexrunner.ModelInfo{{
		ID: "default", Model: "gpt-default", IsDefault: true,
		ReasoningEfforts: []codexrunner.ReasoningEffortOption{{Effort: "low"}},
	}}}
	executor := teamsCodexExecutor{runner: runner}
	_, err := executor.ReasoningEffortCatalog(context.Background(), &teams.Session{ModelProfile: modelprofile.Snapshot{Model: "provider/missing"}})
	if err == nil || !strings.Contains(err.Error(), `configured model "provider/missing"`) {
		t.Fatalf("missing pinned model error = %v", err)
	}
}

func TestTeamsCodexExecutorUsesSessionCwdForExistingThread(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:          "thread-existing",
		TurnID:            "turn-existing",
		FinalAgentMessage: "final",
	}}
	executor := teamsCodexExecutor{runner: runner, workDir: "/helper/default"}
	_, err := executor.Run(context.Background(), &teams.Session{
		CodexThreadID: "thread-existing",
		Cwd:           "/workspace/project",
	}, "continue")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !runner.resumed || runner.threadID != "thread-existing" {
		t.Fatalf("expected resume with exact thread id, runner=%#v", runner)
	}
	if runner.input.WorkingDir != "/workspace/project" {
		t.Fatalf("working dir = %q, want session cwd", runner.input.WorkingDir)
	}
}

func TestTeamsCodexExecutorRejectsResumeThreadMismatch(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:          "thread-other",
		TurnID:            "turn-other",
		FinalAgentMessage: "final from wrong thread",
	}}
	executor := teamsCodexExecutor{runner: runner, workDir: "/helper/default"}
	got, err := executor.Run(context.Background(), &teams.Session{
		CodexThreadID: "thread-existing",
		Cwd:           "/workspace/project",
	}, "continue")
	if err == nil {
		t.Fatal("Run error = nil, want resume thread mismatch")
	}
	if !strings.Contains(err.Error(), "expected \"thread-existing\"") || !strings.Contains(err.Error(), "thread-other") {
		t.Fatalf("Run error = %v, want mismatch detail", err)
	}
	if got.CodexThreadID != "thread-other" || got.CodexTurnID != "turn-other" {
		t.Fatalf("result = %#v, want observed wrong thread for recovery path", got)
	}
	if !runner.resumed || runner.threadID != "thread-existing" {
		t.Fatalf("runner = %#v, want resume attempted with expected thread", runner)
	}
}

func TestTeamsCodexExecutorFallsBackToDefaultWorkDirWhenSessionCwdEmpty(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:          "thread-new",
		TurnID:            "turn-new",
		FinalAgentMessage: "final",
	}}
	executor := teamsCodexExecutor{runner: runner, workDir: "  /helper/default  "}
	_, err := executor.Run(context.Background(), &teams.Session{}, "start")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if runner.input.WorkingDir != "/helper/default" {
		t.Fatalf("working dir = %q, want fallback workdir", runner.input.WorkingDir)
	}
}

func TestTeamsCodexExecutorSkipsThreadNameBackfillForUserTitle(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:          "thread-existing",
		TurnID:            "turn-existing",
		FinalAgentMessage: "final",
	}}
	executor := teamsCodexExecutor{runner: runner}
	_, err := executor.Run(context.Background(), &teams.Session{
		CodexThreadID: "thread-existing",
		UserTitle:     "manual room",
		TitleSource:   "user",
	}, "continue")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if runner.input.BackfillThreadName {
		t.Fatal("user-titled session should not request thread name backfill")
	}
}

func TestTeamsCodexExecutorDoesNotTreatExistingThreadIDErrorAsAccepted(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{ThreadID: "thread-existing"},
		err:    fmt.Errorf("codex_failure: Error: Failed to load cloud requirements (workspace-managed policies)."),
	}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if err == nil {
		t.Fatal("Run error = nil, want failure")
	}
	if teams.IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, should not be ambiguous when only the existing thread id is known", err)
	}
	if got.CodexThreadID != "thread-existing" || got.CodexTurnID != "" {
		t.Fatalf("unexpected execution result: %#v", got)
	}
}

func TestTeamsCodexExecutorTreatsStartedTurnErrorAsAmbiguous(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{
			ThreadID: "thread-existing",
			TurnID:   "turn-started",
			Status:   codexrunner.TurnStatusInProgress,
		},
		err: fmt.Errorf("stream disconnected before completion"),
	}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if !teams.IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, want ambiguous", err)
	}
	if got.CodexThreadID != "thread-existing" || got.CodexTurnID != "turn-started" {
		t.Fatalf("unexpected execution result: %#v", got)
	}
}

func TestTeamsCodexExecutorTreatsUnidentifiedStartedTurnAsAmbiguous(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{
			ThreadID: "thread-existing",
			Status:   codexrunner.TurnStatusStarted,
		},
		err: fmt.Errorf("turn/start response was not confirmed"),
	}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if !teams.IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, want unidentified started turn to be ambiguous", err)
	}
	if got.CodexThreadID != "thread-existing" || got.CodexTurnID != "" {
		t.Fatalf("unexpected execution result: %#v", got)
	}
}

func TestTeamsCodexExecutorDoesNotTreatTerminalFailedTurnAsAmbiguous(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{
			ThreadID: "thread-existing",
			TurnID:   "turn-failed",
			Status:   codexrunner.TurnStatusFailed,
			Failure:  &codexrunner.TurnFailure{Message: "model policy failed"},
		},
		err: fmt.Errorf("codex_failure: model policy failed"),
	}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if err == nil {
		t.Fatal("Run error = nil, want failure")
	}
	if teams.IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, should not be ambiguous for terminal failed turn", err)
	}
	if got.CodexThreadID != "thread-existing" || got.CodexTurnID != "turn-failed" {
		t.Fatalf("unexpected execution result: %#v", got)
	}
}

func TestTeamsCodexExecutorTreatsCompletedTurnWithCanceledContextAsSuccess(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{
			ThreadID:                  "thread-existing",
			ThreadName:                "Existing thread title",
			TurnID:                    "turn-completed",
			Status:                    codexrunner.TurnStatusCompleted,
			FinalAgentMessage:         "final answer",
			FinalAgentMessageComplete: true,
		},
		err: context.Canceled,
	}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if err != nil {
		t.Fatalf("Run error = %v, want completed turn success", err)
	}
	if got.Text != "final answer" || got.CodexThreadID != "thread-existing" || got.CodexTurnID != "turn-completed" || got.CodexThreadTitle != "Existing thread title" {
		t.Fatalf("unexpected execution result: %#v", got)
	}
}

func TestTeamsCodexExecutorRejectsIncompleteCompletedTurnWithCanceledContext(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{
			ThreadID:          "thread-existing",
			TurnID:            "turn-completed",
			Status:            codexrunner.TurnStatusCompleted,
			FinalAgentMessage: "partial delta",
		},
		err: context.Canceled,
	}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if !teams.IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, want incomplete canceled turn to require recovery", err)
	}
	if got.Text != "partial delta" || got.CodexTurnID != "turn-completed" {
		t.Fatalf("result = %#v, want partial diagnostic evidence and turn id", got)
	}
}

func TestTeamsCodexExecutorRejectsCompletedTurnWithoutFinalMessage(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID: "thread-existing",
		TurnID:   "turn-completed",
		Status:   codexrunner.TurnStatusCompleted,
	}}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if err == nil || !strings.Contains(err.Error(), "without a final agent message") {
		t.Fatalf("Run error = %v, want missing final failure", err)
	}
	if !teams.IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, want read-only recovery classification", err)
	}
	if got.CodexThreadID != "thread-existing" || got.CodexTurnID != "turn-completed" || got.Text != "" {
		t.Fatalf("result = %#v, want recovery ids without placeholder", got)
	}
}

func TestTeamsCodexExecutorTreatsIncompleteCompletedTurnAsAmbiguous(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{
			ThreadID:          "thread-existing",
			TurnID:            "turn-completed",
			Status:            codexrunner.TurnStatusCompleted,
			FinalAgentMessage: "partial delta",
		},
		err: &codexrunner.Error{Kind: codexrunner.ErrorParse, Message: "completed without a complete final"},
	}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if !teams.IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, want read-only recovery classification", err)
	}
	if got.Text != "partial delta" || got.CodexTurnID != "turn-completed" {
		t.Fatalf("result = %#v, want partial diagnostic evidence and turn id", got)
	}
}

func TestTeamsCodexExecutorRejectsSuccessfulPartialCompletedTurn(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:          "thread-existing",
		TurnID:            "turn-completed",
		Status:            codexrunner.TurnStatusCompleted,
		FinalAgentMessage: "partial delta",
	}}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if !teams.IsAmbiguousExecutionError(err) {
		t.Fatalf("Run error = %v, want read-only recovery classification", err)
	}
	if got.Text != "partial delta" || got.CodexTurnID != "turn-completed" {
		t.Fatalf("result = %#v, want partial diagnostic evidence and turn id", got)
	}
}

func TestTeamsCodexExecutorPassesImageInputToRunner(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{
			ThreadID:                  "thread-new",
			TurnID:                    "turn-1",
			Status:                    codexrunner.TurnStatusCompleted,
			FinalAgentMessage:         "saw image",
			FinalAgentMessageComplete: true,
		},
	}
	executor := teamsCodexExecutor{runner: runner, workDir: "/work"}
	got, err := executor.RunInput(context.Background(), &teams.Session{}, teams.ExecutionInput{
		Prompt:     "inspect",
		ImagePaths: []string{"/tmp/a.png", "/tmp/b.jpg"},
	})
	if err != nil {
		t.Fatalf("RunInput error: %v", err)
	}
	if got.Text != "saw image" {
		t.Fatalf("result = %#v", got)
	}
	if runner.input.Prompt != "inspect" || strings.Join(runner.input.ImagePaths, ",") != "/tmp/a.png,/tmp/b.jpg" {
		t.Fatalf("runner input = %#v", runner.input)
	}
	if runner.input.WorkingDir != "/work" {
		t.Fatalf("working dir = %q", runner.input.WorkingDir)
	}
}

func TestNewManagedTeamsCodexExecutorUsesStandardAppServerRunner(t *testing.T) {
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{}, "appserver", "/tmp/codex", "/work", []string{"--model", "gpt-test"}, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newManagedTeamsCodexExecutor appserver error: %v", err)
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		t.Fatalf("executor type = %T, want teamsCodexExecutor", executor)
	}
	runner, ok := teamsExecutor.runner.(*codexrunner.AppServerRunner)
	if !ok {
		t.Fatalf("runner type = %T, want AppServerRunner", teamsExecutor.runner)
	}
	if runner.Starter == nil {
		t.Fatalf("appserver runner missing policy starter: %#v", runner)
	}
	if runner.ApprovalMode != codexrunner.ApprovalModeAutomatic {
		t.Fatalf("Teams runner approval mode = %v, want explicit automatic mode", runner.ApprovalMode)
	}
	if runner.Command != "/tmp/codex" || runner.WorkingDir != "/work" || runner.Timeout != time.Minute {
		t.Fatalf("appserver runner config mismatch: %#v", runner)
	}
	wantArgs := []string{"--analytics-default-enabled", "-c", `model="gpt-test"`}
	if !reflect.DeepEqual(runner.AppServerArgs, wantArgs) {
		t.Fatalf("appserver args = %#v, want %#v", runner.AppServerArgs, wantArgs)
	}
	if runner.BackfillThreadName {
		t.Fatal("Teams appserver runner should request thread name backfill per turn, not globally")
	}
	if !runner.MetadataOnlyResume || !runner.RequireCompleteFinal {
		t.Fatalf("Teams appserver safety flags are disabled: %#v", runner)
	}
}

func TestTeamsStandardRuntimeRestoresSavedProxyAndCodexHome(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX app-server fixture")
	}
	lockCLITestHooks(t)
	rootDir := t.TempDir()
	codexHome := filepath.Join(rootDir, "codex-home")
	setTestCodexHomeEnv(t, codexHome)
	store, err := config.NewStore(filepath.Join(rootDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	enabled := true
	if err := store.Save(config.Config{
		Version:           config.CurrentVersion,
		RuntimeGeneration: currentRuntimeGeneration,
		ProxyEnabled:      &enabled,
		Profiles:          []config.Profile{{ID: "proxy-1", Name: "proxy", Host: "host", Port: 22, User: "user"}},
	}); err != nil {
		t.Fatal(err)
	}
	previousProxy := codexAppEnsureProxyURLFn
	t.Cleanup(func() { codexAppEnsureProxyURLFn = previousProxy })
	codexAppEnsureProxyURLFn = func(context.Context, *config.Store, config.Profile, []config.Instance, io.Writer) (string, error) {
		return "http://127.0.0.1:18080", nil
	}
	envPath := filepath.Join(rootDir, "child.env")
	codexPath := filepath.Join(rootDir, "codex")
	script := fmt.Sprintf(`#!/bin/sh
case "${1:-}" in
  --version) echo 'codex-cli 0.133.0'; exit 0 ;;
  --help) echo 'Options: --remote <ADDR> --remote-auth-token-env <ENV_VAR>'; exit 0 ;;
  app-server)
    printf '%%s|%%s\n' "${HTTP_PROXY:-}" "${CODEX_HOME:-}" > %s
    while IFS= read -r line; do
      id=$(printf %%s "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
      case "$line" in
        *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{}}\n' "$id" ;;
        *'"method":"thread/list"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{"data":[]}}\n' "$id" ;;
		*'"method":"thread/start"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{"thread":{"id":"thread-teams"}}}\n' "$id" ;;
		*'"method":"thread/read"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{"thread":{"id":"thread-teams","name":"Teams thread","turns":[{"id":"turn-teams","status":"completed","items":[{"type":"agentMessage","text":"ok"}]}]}}}\n' "$id" ;;
        *'"method":"turn/start"'*)
          printf '{"jsonrpc":"2.0","id":%%s,"result":{"turn":{"id":"turn-teams","status":"inProgress","items":[]}}}\n' "$id"
          printf '%%s\n' '{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-teams","turnId":"turn-teams","item":{"id":"final","type":"agentMessage","text":"ok"}}}'
          printf '%%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-teams","turn":{"id":"turn-teams","status":"completed","items":[]}}}' ;;
      esac
    done ;;
  *) exit 64 ;;
esac
`, shellSingleQuoteForBeaconCLITest(envPath))
	if err := os.WriteFile(codexPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{configPath: store.Path()}, "appserver", codexPath, rootDir, nil, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.(teamsCodexExecutor).Close()
	result, err := executor.Run(context.Background(), &teams.Session{Cwd: rootDir}, "reply ok")
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "ok" {
		t.Fatalf("result = %#v", result)
	}
	raw, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != "http://127.0.0.1:18080|"+codexHome {
		t.Fatalf("child runtime env = %q", got)
	}
}

func TestTeamsStandardRuntimeRefreshesManagedNodePATHAfterExecutorCreation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX env-node wrapper fixture")
	}
	lockCLITestHooks(t)
	rootDir := t.TempDir()
	home := filepath.Join(rootDir, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	setTestCodexHomeEnv(t, filepath.Join(home, ".codex"))
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(rootDir, "cache"))
	t.Setenv("CODEX_NODE_INSTALL_ROOT", filepath.Join(home, ".cache", "codex-proxy", "node"))

	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		t.Skip("unsupported managed-node architecture")
	}
	nodeBin := filepath.Join(home, ".cache", "codex-proxy", "node", "v22-"+runtime.GOOS+"-"+arch, "bin")
	if err := os.MkdirAll(nodeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	testBinary, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	nodeScript := `#!/bin/sh
shift
case "${1:-}" in
  --version) echo 'codex-cli 0.142.3'; exit 0 ;;
  --help) echo 'Options: --remote <ADDR> --remote-auth-token-env <ENV_VAR>'; exit 0 ;;
  app-server) CXP_TEAMS_MANAGED_NODE_APP_SERVER=1 exec ` + shellSingleQuoteForBeaconCLITest(testBinary) + ` -test.run '^TestTeamsManagedNodeAppServerHelperProcess$' -- ;;
  *) exit 64 ;;
esac
`
	if err := os.WriteFile(filepath.Join(nodeBin, "node"), []byte(nodeScript), 0o700); err != nil {
		t.Fatal(err)
	}
	codexPath := filepath.Join(rootDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	// Snapshot the runner environment before managed Node is added to the
	// process PATH. The app-server cold start must refresh PATH after resolving
	// the runtime instead of replaying this stale value.
	emptyPath := filepath.Join(rootDir, "empty-path")
	if err := os.MkdirAll(emptyPath, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", emptyPath)
	store, err := config.NewStore(filepath.Join(rootDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, RuntimeGeneration: currentRuntimeGeneration}); err != nil {
		t.Fatal(err)
	}
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{configPath: store.Path()}, "appserver", codexPath, rootDir, nil, "", 5*time.Second, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	managed := executor.(teamsCodexExecutor)
	defer managed.Close()

	result, err := managed.Run(context.Background(), &teams.Session{Cwd: rootDir}, "managed node turn")
	if err != nil {
		t.Fatalf("managed-node Teams turn: %v", err)
	}
	if result.Text != "managed node ok" || result.CodexThreadID != "thread-managed-node" {
		t.Fatalf("result = %#v", result)
	}
	runner := managed.runner.(*codexrunner.AppServerRunner)
	if err := runner.Close(); err != nil {
		t.Fatalf("close first app-server: %v", err)
	}
	secondEmptyPath := filepath.Join(rootDir, "second-empty-path")
	if err := os.MkdirAll(secondEmptyPath, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", secondEmptyPath)
	result, err = managed.Run(context.Background(), &teams.Session{Cwd: rootDir}, "managed node cold restart")
	if err != nil {
		t.Fatalf("managed-node Teams cold restart: %v", err)
	}
	if result.Text != "managed node ok" {
		t.Fatalf("cold restart result = %#v", result)
	}
}

func TestTeamsManagedNodeAppServerHelperProcess(t *testing.T) {
	if os.Getenv("CXP_TEAMS_MANAGED_NODE_APP_SERVER") != "1" {
		t.Skip("helper process only")
	}
	type message struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	scanner := bufio.NewScanner(os.Stdin)
	write := func(format string, args ...any) {
		_, _ = fmt.Fprintf(os.Stdout, format+"\n", args...)
	}
	for scanner.Scan() {
		var request message
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			os.Exit(2)
		}
		if request.Method == "" {
			continue
		}
		var id int64
		_ = json.Unmarshal(request.ID, &id)
		switch request.Method {
		case "initialized":
		case "initialize":
			write(`{"jsonrpc":"2.0","id":%d,"result":{}}`, id)
		case "thread/list":
			write(`{"jsonrpc":"2.0","id":%d,"result":{"data":[]}}`, id)
		case "thread/start":
			write(`{"jsonrpc":"2.0","id":%d,"result":{"thread":{"id":"thread-managed-node"}}}`, id)
		case "thread/read":
			write(`{"jsonrpc":"2.0","id":%d,"result":{"thread":{"id":"thread-managed-node","name":"managed node"}}}`, id)
		case "turn/start":
			write(`{"jsonrpc":"2.0","id":%d,"result":{"turn":{"id":"turn-managed-node","status":"inProgress","items":[]}}}`, id)
			write(`{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-managed-node","turnId":"turn-managed-node","item":{"id":"final","type":"agentMessage","text":"managed node ok"}}}`)
			write(`{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-managed-node","turn":{"id":"turn-managed-node","status":"completed","items":[]}}}`)
		default:
			os.Exit(3)
		}
	}
	os.Exit(0)
}

func TestTeamsStandardRuntimeRunsTwoSessionsConcurrentlyOnSharedProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX test-binary app-server wrapper")
	}
	lockCLITestHooks(t)
	rootDir := t.TempDir()
	setTestCodexHomeEnv(t, filepath.Join(rootDir, "codex-home"))
	store, err := config.NewStore(filepath.Join(rootDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, RuntimeGeneration: currentRuntimeGeneration}); err != nil {
		t.Fatal(err)
	}
	testBinary, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	codexPath := filepath.Join(rootDir, "codex")
	wrapper := `#!/bin/sh
case "${1:-}" in
  --version) echo 'codex-cli 0.142.3'; exit 0 ;;
  --help) echo 'Options: --remote <ADDR> --remote-auth-token-env <ENV_VAR>'; exit 0 ;;
  app-server) CXP_TEAMS_PARALLEL_APP_SERVER=1 exec ` + shellSingleQuoteForBeaconCLITest(testBinary) + ` -test.run '^TestTeamsParallelAppServerHelperProcess$' -- ;;
  *) exit 64 ;;
esac
`
	if err := os.WriteFile(codexPath, []byte(wrapper), 0o700); err != nil {
		t.Fatal(err)
	}
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{configPath: store.Path()}, "appserver", codexPath, rootDir, nil, "", 5*time.Second, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	managed := executor.(teamsCodexExecutor)
	defer managed.Close()

	type outcome struct {
		result teams.ExecutionResult
		err    error
	}
	results := make(chan outcome, 2)
	for index := range 2 {
		index := index
		go func() {
			result, runErr := managed.Run(context.Background(), &teams.Session{Cwd: rootDir}, fmt.Sprintf("session %d", index+1))
			results <- outcome{result: result, err: runErr}
		}()
	}
	seen := map[string]bool{}
	for range 2 {
		out := <-results
		if out.err != nil {
			t.Fatalf("parallel Teams session failed: result=%#v err=%v", out.result, out.err)
		}
		seen[out.result.CodexThreadID] = strings.HasPrefix(out.result.Text, "done thread-")
	}
	if len(seen) != 2 || !seen["thread-1"] || !seen["thread-2"] {
		t.Fatalf("parallel Teams results = %#v", seen)
	}
}

func TestTeamsParallelAppServerHelperProcess(t *testing.T) {
	if os.Getenv("CXP_TEAMS_PARALLEL_APP_SERVER") != "1" {
		t.Skip("helper process only")
	}
	type message struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	type turn struct {
		requestID int64
		threadID  string
		turnID    string
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	threadSequence := 0
	turns := make([]turn, 0, 2)
	approvalResponses := 0
	write := func(format string, args ...any) {
		_, _ = fmt.Fprintf(os.Stdout, format+"\n", args...)
	}
	for scanner.Scan() {
		var request message
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			os.Exit(2)
		}
		if request.Method == "" {
			approvalResponses++
			if approvalResponses == 2 {
				for _, turn := range turns {
					write(`{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":%q,"turnId":%q,"item":{"id":"final","type":"agentMessage","text":%q}}}`, turn.threadID, turn.turnID, "done "+turn.threadID)
					write(`{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":%q,"turn":{"id":%q,"status":"completed","items":[]}}}`, turn.threadID, turn.turnID)
				}
			}
			continue
		}
		var id int64
		if len(request.ID) > 0 {
			_ = json.Unmarshal(request.ID, &id)
		}
		switch request.Method {
		case "initialized":
		case "initialize":
			write(`{"jsonrpc":"2.0","id":%d,"result":{}}`, id)
		case "thread/list":
			write(`{"jsonrpc":"2.0","id":%d,"result":{"data":[]}}`, id)
		case "thread/start":
			threadSequence++
			write(`{"jsonrpc":"2.0","id":%d,"result":{"thread":{"id":%q}}}`, id, "thread-"+strconv.Itoa(threadSequence))
		case "thread/read":
			var params struct {
				ThreadID string `json:"threadId"`
			}
			_ = json.Unmarshal(request.Params, &params)
			write(`{"jsonrpc":"2.0","id":%d,"result":{"thread":{"id":%q,"name":%q}}}`, id, params.ThreadID, "Teams "+params.ThreadID)
		case "turn/start":
			var params struct {
				ThreadID string `json:"threadId"`
			}
			_ = json.Unmarshal(request.Params, &params)
			turns = append(turns, turn{requestID: id, threadID: params.ThreadID, turnID: "turn-" + params.ThreadID})
			if len(turns) == 2 {
				for index := len(turns) - 1; index >= 0; index-- {
					turn := turns[index]
					write(`{"jsonrpc":"2.0","id":%d,"result":{"turn":{"id":%q,"status":"inProgress","items":[]}}}`, turn.requestID, turn.turnID)
				}
				for index, turn := range turns {
					write(`{"jsonrpc":"2.0","id":%d,"method":"item/commandExecution/requestApproval","params":{"threadId":%q,"turnId":%q}}`, 900+index, turn.threadID, turn.turnID)
				}
			}
		default:
			os.Exit(3)
		}
	}
	os.Exit(0)
}

func TestNewManagedTeamsCodexExecutorConfiguresThirdPartyModelProfileForAppServer(t *testing.T) {
	t.Skip("family adapter launch is covered by external catalog integration tests")
	lockCLITestHooks(t)
	previousProbe := codexLoginStatusProbeFn
	t.Cleanup(func() { codexLoginStatusProbeFn = previousProbe })
	codexLoginStatusProbeFn = func(_ context.Context, path string, _ []string, _ io.Writer) bool {
		if path != "/tmp/codex" {
			t.Fatalf("login probe path = %q, want runner Codex path", path)
		}
		return true
	}
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {Provider: "mimo", APIKeyRef: "env:MIMO_API_KEY", Revision: 1},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("MIMO_API_KEY", "sk-test")
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{configPath: store.Path()}, "appserver", "/tmp/codex", "/work", nil, "mimo25", time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newManagedTeamsCodexExecutor: %v", err)
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		t.Fatalf("executor type = %T, want teamsCodexExecutor", executor)
	}
	runner, ok := teamsExecutor.runner.(*codexrunner.AppServerRunner)
	if !ok {
		t.Fatalf("runner type = %T, want AppServerRunner", teamsExecutor.runner)
	}
	joinedArgs := strings.Join(runner.AppServerArgs, "\n")
	for _, want := range []string{
		`model_provider="cxp-unified"`,
		`model="mimo/mimo-v2.5"`,
		`model_providers.cxp-unified.wire_api="responses"`,
		`model_providers.cxp-unified.requires_openai_auth=true`,
	} {
		if !strings.Contains(joinedArgs, want) {
			t.Fatalf("appserver args missing %q:\n%v", want, runner.AppServerArgs)
		}
	}
	if !slices.ContainsFunc(runner.ExtraEnv, func(entry string) bool {
		return strings.HasPrefix(entry, envCXPUnifiedGatewayKey+"=")
	}) {
		t.Fatalf("appserver extra env missing proxy key: %v", runner.ExtraEnv)
	}
}

func TestTeamsCodexExecutorRoutesSessionModelProfileSnapshot(t *testing.T) {
	lockCLITestHooks(t)
	previousPrepare := prepareTeamsAppServerModelProfileForRunner
	defer func() { prepareTeamsAppServerModelProfileForRunner = previousPrepare }()
	var capturedSnapshot modelprofile.Snapshot
	prepareTeamsAppServerModelProfileForRunner = func(_ context.Context, _ *rootOptions, _ string, snapshot modelprofile.Snapshot, _ io.Writer) ([]string, []string, func(), error) {
		capturedSnapshot = snapshot
		return []string{"-c", `model="snapshot-model"`}, []string{"SNAPSHOT_PROFILE=1"}, nil, nil
	}
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {Provider: "mimo", APIKeyRef: "env:NEW_MIMO_KEY", Revision: 9},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	baseRunner := &fakeTeamsRunner{}
	executor := teamsCodexExecutor{
		runner:           baseRunner,
		root:             &rootOptions{configPath: store.Path()},
		runnerName:       "exec",
		codexPath:        "/tmp/codex",
		workDir:          "/work",
		runnerCacheMu:    &sync.Mutex{},
		runnersByProfile: map[string]codexrunner.Runner{},
	}
	oldSnapshot := modelprofile.Snapshot{
		Name:      "mimo25",
		Provider:  "mimo",
		APIKeyRef: "env:OLD_MIMO_KEY",
		Revision:  3,
	}
	runner, err := executor.runnerForSessionProfile(context.Background(), &teams.Session{ModelProfile: oldSnapshot})
	if err != nil {
		t.Fatalf("runnerForSessionProfile: %v", err)
	}
	appServerRunner, ok := runner.(*codexrunner.AppServerRunner)
	if !ok {
		t.Fatalf("runner type = %T, want AppServerRunner", runner)
	}
	if capturedSnapshot.APIKeyRef != "env:OLD_MIMO_KEY" || capturedSnapshot.Revision != 3 {
		t.Fatalf("prepared snapshot = %#v, want old pinned key/revision", capturedSnapshot)
	}
	if !slices.Contains(appServerRunner.ExtraEnv, "SNAPSHOT_PROFILE=1") {
		t.Fatalf("snapshot runtime env missing: %#v", appServerRunner.ExtraEnv)
	}
	again, err := executor.runnerForSessionProfile(context.Background(), &teams.Session{ModelProfile: oldSnapshot})
	if err != nil {
		t.Fatalf("runnerForSessionProfile cached: %v", err)
	}
	if again != runner {
		t.Fatalf("profile runner cache missed: first=%p second=%p", runner, again)
	}
	base, err := executor.runnerForSessionProfile(context.Background(), &teams.Session{})
	if err != nil {
		t.Fatalf("runnerForSessionProfile default: %v", err)
	}
	if base != baseRunner {
		t.Fatalf("default session runner = %T/%p, want base %p", base, base, baseRunner)
	}
}

func TestTeamsCodexExecutorSwitchesProfileRunnerWithoutChangingThread(t *testing.T) {
	baseSnapshot := modelprofile.Snapshot{Name: "gpt-origin", Provider: modelprofile.DefaultProvider, Model: "gpt-origin", Revision: 1}
	targetSnapshot := modelprofile.Snapshot{Name: "ih-target", Provider: "chat-compatible", Model: "nvidia/nvidia/eccn-nemotron-3-ultra", Revision: 2}
	baseRunner := &fakeTeamsRunner{}
	targetRunner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:                  "thread-shared",
		TurnID:                    "turn-target",
		Status:                    codexrunner.TurnStatusCompleted,
		FinalAgentMessage:         "target reply",
		FinalAgentMessageComplete: true,
	}}
	executor := teamsCodexExecutor{
		runner:               baseRunner,
		modelProfileSnapshot: baseSnapshot,
		runnerCacheMu:        &sync.Mutex{},
		runnersByProfile: map[string]codexrunner.Runner{
			modelProfileRunnerSessionCacheKey(&teams.Session{ID: "session-switch", ModelProfile: targetSnapshot, ModelGeneration: 1}): targetRunner,
		},
		runnerKeyBySession: map[string]string{},
	}

	result, err := executor.Run(context.Background(), &teams.Session{
		ID:              "session-switch",
		CodexThreadID:   "thread-shared",
		ModelProfile:    targetSnapshot,
		ModelGeneration: 1,
	}, "continue after switch")
	if err != nil {
		t.Fatal(err)
	}
	if baseRunner.resumed {
		t.Fatal("model switch incorrectly resumed through the origin profile runner")
	}
	if !targetRunner.resumed || targetRunner.threadID != "thread-shared" {
		t.Fatalf("target runner resume = resumed:%t thread:%q", targetRunner.resumed, targetRunner.threadID)
	}
	if targetRunner.input.Model != targetSnapshot.Model {
		t.Fatalf("target runner turn model = %q, want %q", targetRunner.input.Model, targetSnapshot.Model)
	}
	if result.CodexThreadID != "thread-shared" || result.Text != "target reply" {
		t.Fatalf("execution result = %#v", result)
	}
}

func TestTeamsCodexExecutorReturnToBaseProfileUsesFreshGenerationRunner(t *testing.T) {
	baseSnapshot := modelprofile.Snapshot{Name: "gpt-origin", Provider: modelprofile.DefaultProvider, Model: "gpt-origin", Revision: 1}
	baseRunner := &fakeTeamsRunner{}
	freshRunner := &fakeTeamsRunner{}
	executor := teamsCodexExecutor{
		runner:               baseRunner,
		modelProfileSnapshot: baseSnapshot,
		runnerCacheMu:        &sync.Mutex{},
		runnersByProfile: map[string]codexrunner.Runner{
			modelProfileRunnerSessionCacheKey(&teams.Session{ID: "session-return", ModelProfile: baseSnapshot, ModelGeneration: 2}): freshRunner,
		},
		runnerKeyBySession: map[string]string{},
	}

	initial, err := executor.runnerForSessionProfile(context.Background(), &teams.Session{ModelProfile: baseSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	if initial != baseRunner {
		t.Fatalf("initial profile runner = %p, want base %p", initial, baseRunner)
	}
	returned, err := executor.runnerForSessionProfile(context.Background(), &teams.Session{
		ID:              "session-return",
		ModelProfile:    baseSnapshot,
		ModelGeneration: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if returned != freshRunner {
		t.Fatalf("return-to-profile runner = %p, want fresh generation runner %p", returned, freshRunner)
	}
}

func TestTeamsCodexExecutorReclaimsPreviousSessionGenerationRunner(t *testing.T) {
	sessionID := "session-reclaim"
	oldSnapshot := modelprofile.Snapshot{Name: "old", Provider: "chat-compatible", Model: "nvidia/old", Revision: 1}
	newSnapshot := modelprofile.Snapshot{Name: "new", Provider: modelprofile.DefaultProvider, Model: "gpt-new", Revision: 1}
	oldKey := modelProfileRunnerSessionCacheKey(&teams.Session{ID: sessionID, ModelProfile: oldSnapshot, ModelGeneration: 1})
	newKey := modelProfileRunnerSessionCacheKey(&teams.Session{ID: sessionID, ModelProfile: newSnapshot, ModelGeneration: 2})
	closed := 0
	oldRunner := &codexrunner.AppServerRunner{CloseHook: func() { closed++ }}
	newRunner := &fakeTeamsRunner{}
	executor := teamsCodexExecutor{
		runner:             &fakeTeamsRunner{},
		runnerCacheMu:      &sync.Mutex{},
		runnerKeyBySession: map[string]string{sessionID: oldKey},
		runnersByProfile: map[string]codexrunner.Runner{
			oldKey: oldRunner,
			newKey: newRunner,
		},
	}

	got, err := executor.runnerForSessionProfile(context.Background(), &teams.Session{
		ID:              sessionID,
		ModelProfile:    newSnapshot,
		ModelGeneration: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != newRunner || closed != 1 {
		t.Fatalf("runner=%p want=%p previous closes=%d want=1", got, newRunner, closed)
	}
	if _, exists := executor.runnersByProfile[oldKey]; exists {
		t.Fatal("previous session generation runner remains cached")
	}
	if executor.runnerKeyBySession[sessionID] != newKey {
		t.Fatalf("session runner key = %q, want %q", executor.runnerKeyBySession[sessionID], newKey)
	}
}

func TestTeamsModelProfileResolverAcceptsListedOfficialModelSlug(t *testing.T) {
	lockCLITestHooks(t)
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	previous := listTeamsOfficialModelsFn
	t.Cleanup(func() { listTeamsOfficialModelsFn = previous })
	listTeamsOfficialModelsFn = func(context.Context, string) ([]teamsOfficialModel, error) {
		return []teamsOfficialModel{{Slug: "gpt-5.6-luna", DisplayName: "GPT 5.6 Luna", DefaultReasoningLevel: "high"}}, nil
	}
	snapshot, err := newTeamsModelProfileResolver(&rootOptions{configPath: store.Path()}, "/test/codex")(context.Background(), "gpt-5.6-luna")
	if err != nil {
		t.Fatalf("resolve official model: %v", err)
	}
	if snapshot.Provider != modelprofile.DefaultProvider || snapshot.Model != "gpt-5.6-luna" || snapshot.DefaultReasoningEffort != "high" {
		t.Fatalf("official snapshot = %#v", snapshot)
	}
	if _, err := modelprofile.ResolveSnapshot(config.Config{Version: config.CurrentVersion}, snapshot); err != nil {
		t.Fatalf("official snapshot is not launch-resolvable: %v", err)
	}
}

func TestTeamsModelProfileResolverRejectsUnavailableOfficialSlugWhenCatalogIsHealthy(t *testing.T) {
	lockCLITestHooks(t)
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	previous := listTeamsOfficialModelsFn
	t.Cleanup(func() { listTeamsOfficialModelsFn = previous })
	listTeamsOfficialModelsFn = func(context.Context, string) ([]teamsOfficialModel, error) {
		return []teamsOfficialModel{{Slug: "gpt-available", IsDefault: true}}, nil
	}
	_, err = newTeamsModelProfileResolver(&rootOptions{configPath: store.Path()}, "/test/codex")(context.Background(), "gpt-typo")
	if err == nil || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("unavailable official model error = %v", err)
	}
}

func TestTeamsModelProfileResolverRejectsUnverifiedThirdPartyProfile(t *testing.T) {
	t.Skip("legacy sourced family profile path was removed; catalog provider verification covers this")
	previousVerify := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previousVerify })
	verifyConfiguredModelAuthenticationFn = func(context.Context, modelprofile.Resolved, string) error { return fmt.Errorf("unauthorized") }
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion,
		ModelSources: map[string]config.ModelSource{"repo": {URL: "https://example.invalid/repo.git", Revision: "abc123"}},
		ModelProfiles: map[string]config.ModelProfile{
			"unverified": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:TEST_UNVERIFIED_KEY", Revision: 1, Source: "repo"},
		}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_UNVERIFIED_KEY", "not-a-real-key")
	_, err = newTeamsModelProfileResolver(&rootOptions{configPath: store.Path()})(context.Background(), "unverified")
	if err == nil || !strings.Contains(err.Error(), "automatic authentication verification failed") {
		t.Fatalf("unverified resolver error = %v", err)
	}
}

func TestTeamsModelProfileResolverSilentlyReverifiesSourcedProfile(t *testing.T) {
	t.Skip("legacy sourced family profile path was removed; catalog provider verification covers this")
	previousVerify := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previousVerify })
	verifyConfiguredModelAuthenticationFn = func(context.Context, modelprofile.Resolved, string) error { return nil }
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion,
		ModelSources:  map[string]config.ModelSource{"repo": {URL: "https://example.invalid/repo.git", Revision: "abc123"}},
		ModelProfiles: map[string]config.ModelProfile{"sourced": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:TEST_SOURCE_KEY", Revision: 1, Source: "repo"}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_SOURCE_KEY", "valid-test-key")
	snapshot, err := newTeamsModelProfileResolver(&rootOptions{configPath: store.Path()})(context.Background(), "sourced")
	if err != nil || snapshot.Name != "sourced" {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModelProfiles["sourced"].VerificationFingerprint == "" || cfg.ModelProfiles["sourced"].VerifiedAt.IsZero() {
		t.Fatalf("automatic verification was not persisted: %#v", cfg.ModelProfiles["sourced"])
	}
}

func TestTeamsModelProfileResolverKeepsLegacyLocalProfileCompatible(t *testing.T) {
	t.Skip("forward compatibility for removed family profiles is intentionally unsupported")
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"legacy": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:TEST_LEGACY_KEY", Revision: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_LEGACY_KEY", "legacy-key")
	snapshot, err := newTeamsModelProfileResolver(&rootOptions{configPath: store.Path()})(context.Background(), "profile:legacy")
	if err != nil || snapshot.Name != "legacy" {
		t.Fatalf("legacy snapshot=%#v err=%v", snapshot, err)
	}
}

func TestTeamsCodexExecutorSessionProfilePrepareUsesTurnContextCI(t *testing.T) {
	lockCLITestHooks(t)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	enabled := true
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &enabled,
		Profiles:     []config.Profile{{ID: "p1", Name: "dev", Host: "host", Port: 22, User: "me"}},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("DEEPSEEK_API_KEY", "sk-test")

	oldTimeout := teamsAppServerModelProfilePrepareTimeout
	teamsAppServerModelProfilePrepareTimeout = time.Hour
	t.Cleanup(func() { teamsAppServerModelProfilePrepareTimeout = oldTimeout })

	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	t.Cleanup(func() { codexAppEnsureProxyURLFn = prevEnsureProxyURL })
	codexAppEnsureProxyURLFn = func(ctx context.Context, _ *config.Store, _ config.Profile, _ []config.Instance, _ io.Writer) (string, error) {
		return "", waitForProxyPrepareContext(ctx)
	}

	executor, err := newManagedTeamsCodexExecutor(&rootOptions{configPath: store.Path()}, "appserver", "/tmp/codex", "/work", nil, "", time.Hour, io.Discard)
	if err != nil {
		t.Fatalf("newManagedTeamsCodexExecutor: %v", err)
	}
	snapshot := modelprofile.Snapshot{
		Name:      "deepseek-pro",
		Provider:  "deepseek",
		Model:     "deepseek/deepseek-v4-pro",
		APIKeyRef: "env:DEEPSEEK_API_KEY",
		Revision:  1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = executor.(teamsCodexExecutor).RunInput(ctx, &teams.Session{ModelProfile: snapshot}, teams.ExecutionInput{Prompt: "say ok"})
	if err == nil {
		t.Fatal("RunInput error = nil, want cancellation")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("RunInput error = %v, want canceled", err)
	}
}

func TestModelProfileSnapshotKeyIncludesRuntimeIdentity(t *testing.T) {
	base := modelprofile.Snapshot{
		Name:           "mimo25",
		Provider:       "mimo",
		APIKeyRef:      "secret:model-profile/mimo25/api-key",
		Revision:       1,
		KeyFingerprint: "key:one",
		BaseURLHash:    "url:one",
	}
	changedKey := base
	changedKey.KeyFingerprint = "key:two"
	if modelProfileSnapshotKey(base) == modelProfileSnapshotKey(changedKey) {
		t.Fatal("snapshot cache key should include key fingerprint")
	}
	changedURL := base
	changedURL.BaseURLHash = "url:two"
	if modelProfileSnapshotKey(base) == modelProfileSnapshotKey(changedURL) {
		t.Fatal("snapshot cache key should include base URL hash")
	}
}

func TestTeamsCodexExecutorProfileRunnerCacheIsConcurrentAndSnapshotScoped(t *testing.T) {
	lockCLITestHooks(t)
	previousPrepare := prepareTeamsAppServerModelProfileForRunner
	defer func() { prepareTeamsAppServerModelProfileForRunner = previousPrepare }()
	prepareTeamsAppServerModelProfileForRunner = func(_ context.Context, _ *rootOptions, _ string, snapshot modelprofile.Snapshot, _ io.Writer) ([]string, []string, func(), error) {
		return []string{"-c", fmt.Sprintf("snapshot_key=%q", modelProfileSnapshotKey(snapshot))}, nil, nil, nil
	}
	baseRunner := &fakeTeamsRunner{}
	executor := teamsCodexExecutor{
		runner:           baseRunner,
		runnerName:       "exec",
		codexPath:        "/tmp/codex",
		workDir:          "/work",
		runnerCacheMu:    &sync.Mutex{},
		runnersByProfile: map[string]codexrunner.Runner{},
	}
	snapshots := []modelprofile.Snapshot{
		{Name: "mimo25", Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:MIMO_KEY_A", Revision: 1},
		{Name: "mimo25", Provider: "mimo", Model: "mimo/mimo-v2.5-pro", APIKeyRef: "env:MIMO_KEY_A", Revision: 1},
		{Name: "mimo25", Provider: "mimo", Model: "mimo/mimo-v2.5-pro", APIKeyRef: "env:MIMO_KEY_B", Revision: 1},
		{Name: "deepseek", Provider: "deepseek", Model: "deepseek/deepseek-v4-pro", APIKeyRef: "env:DEEPSEEK_KEY", SSHProxy: "jump-a", Revision: 2},
	}

	var wg sync.WaitGroup
	errs := make(chan error, 120)
	for i := 0; i < 120; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshot := snapshots[i%len(snapshots)]
			runner, err := executor.runnerForSessionProfile(context.Background(), &teams.Session{ModelProfile: snapshot})
			if err != nil {
				errs <- err
				return
			}
			appServerRunner, ok := runner.(*codexrunner.AppServerRunner)
			if !ok {
				errs <- fmt.Errorf("runner type = %T, want AppServerRunner", runner)
				return
			}
			if !strings.Contains(strings.Join(appServerRunner.AppServerArgs, "\n"), fmt.Sprintf("%q", modelProfileSnapshotKey(snapshot))) {
				errs <- fmt.Errorf("app-server args %#v do not contain snapshot %#v", appServerRunner.AppServerArgs, snapshot)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(executor.runnersByProfile) != len(snapshots) {
		t.Fatalf("cached profile runners = %d, want %d", len(executor.runnersByProfile), len(snapshots))
	}
	for _, snapshot := range snapshots {
		key := modelProfileSnapshotKey(snapshot)
		runner, ok := executor.runnersByProfile[key]
		if !ok {
			t.Fatalf("missing cached runner for %#v", snapshot)
		}
		again, err := executor.runnerForSessionProfile(context.Background(), &teams.Session{ModelProfile: snapshot})
		if err != nil {
			t.Fatalf("runnerForSessionProfile cached: %v", err)
		}
		if again != runner {
			t.Fatalf("runner cache miss for %#v: first=%p second=%p", snapshot, runner, again)
		}
	}
}

func TestTeamsExecutorUsesStandardAppServerPolicyArgs(t *testing.T) {
	tests := []struct {
		name       string
		runnerName string
		control    bool
		args       []string
		model      string
		want       []string
	}{
		{name: "legacy runner alias", runnerName: "exec", args: []string{"--model", "gpt-5", "--sandbox", "workspace-write"}, want: []string{`model="gpt-5"`, teams.CodexReasoningEffortConfigArg(teams.DefaultSessionReasoningEffort)}},
		{name: "native runner name", runnerName: "appserver", want: []string{teams.CodexReasoningEffortConfigArg(teams.DefaultSessionReasoningEffort)}},
		{name: "explicit effort", runnerName: "exec", args: []string{"-c", `model_reasoning_effort="medium"`}, want: []string{`model_reasoning_effort="medium"`}},
		{name: "control defaults", runnerName: "exec", control: true, args: []string{"--model", "gpt-5", "--sandbox", "workspace-write", "-c", `model_reasoning_effort="xhigh"`}, want: []string{teams.CodexReasoningEffortConfigArg(teams.DefaultControlFallbackReasoningEffort)}},
		{name: "control model", runnerName: "appserver", control: true, model: "gpt-control", want: []string{`model="gpt-control"`, teams.CodexReasoningEffortConfigArg(teams.DefaultControlFallbackReasoningEffort)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var executor teams.Executor
			var err error
			if test.control {
				executor, err = newTeamsControlFallbackExecutor(&rootOptions{}, test.runnerName, "/tmp/codex", "/work", test.args, "", test.model, time.Minute, io.Discard)
			} else {
				executor, err = newTeamsExecutor(&rootOptions{}, "codex", test.runnerName, "/tmp/codex", "/work", test.args, "", time.Minute, io.Discard)
			}
			if err != nil {
				t.Fatal(err)
			}
			runner, ok := executor.(teamsCodexExecutor).runner.(*codexrunner.AppServerRunner)
			if !ok {
				t.Fatalf("runner type = %T, want AppServerRunner", executor.(teamsCodexExecutor).runner)
			}
			joined := strings.Join(runner.AppServerArgs, "\n")
			if !strings.Contains(joined, "--analytics-default-enabled") {
				t.Fatalf("analytics was not enabled: %#v", runner.AppServerArgs)
			}
			for _, want := range test.want {
				if !strings.Contains(joined, want) {
					t.Fatalf("app-server args missing %q: %#v", want, runner.AppServerArgs)
				}
			}
			for _, forbidden := range []string{"workspace-write", "danger-full-access", "approval_policy=never"} {
				if strings.Contains(joined, forbidden) {
					t.Fatalf("app-server args retained execution override %q: %#v", forbidden, runner.AppServerArgs)
				}
			}
		})
	}
}

func TestNewManagedTeamsCodexExecutorRemovesLegacyExecutionArgs(t *testing.T) {
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{}, "exec", "/tmp/codex", "/work", []string{
		"--model", "gpt-test",
		"--sandbox=workspace-write",
		"--ask-for-approval", "on-request",
		"-s", "read-only",
	}, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	runner := executor.(teamsCodexExecutor).runner.(*codexrunner.AppServerRunner)
	joined := strings.Join(runner.AppServerArgs, "\n")
	if !strings.Contains(joined, `model="gpt-test"`) || strings.Contains(joined, "workspace-write") || strings.Contains(joined, "read-only") {
		t.Fatalf("translated app-server args = %#v", runner.AppServerArgs)
	}
}

func TestTeamsCodexArgsPreserveTurnScopedExecOptions(t *testing.T) {
	dir := t.TempDir()
	schema := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(schema, []byte(`{"type":"object"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{}, "appserver", "/tmp/codex", "/work", []string{
		"--ephemeral", "--add-dir=/data", "--image", "/tmp/input.png", "--output-schema", schema,
	}, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	managed := executor.(teamsCodexExecutor)
	if !managed.ephemeral || !reflect.DeepEqual(managed.additionalDirs, []string{"/data"}) || !reflect.DeepEqual(managed.staticImages, []string{"/tmp/input.png"}) || !json.Valid(managed.outputSchema) {
		t.Fatalf("turn options were not preserved: %#v", managed)
	}
}

func TestTeamsCodexArgsRejectLoaderOnlyOptionsInsteadOfSilentlyChangingSemantics(t *testing.T) {
	for _, args := range [][]string{
		{"--profile", "work"},
		{"--profile-v2", "work"},
		{"--ignore-user-config"},
		{"--ignore-rules"},
	} {
		if _, err := translateTeamsCodexArgsToAppServer(args); err == nil {
			t.Fatalf("translateTeamsCodexArgsToAppServer(%#v) unexpectedly succeeded", args)
		}
	}
}

func TestCodexArgsWithModelReplacesExistingModelForms(t *testing.T) {
	got := codexArgsWithModel([]string{"--model", "gpt-5", "--model=gpt-5.2", "-m", "mini", "-m=old", "--sandbox", "read-only"}, "spark")
	want := []string{"--sandbox", "read-only", "--model", "spark"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("codexArgsWithModel = %#v, want %#v", got, want)
	}
}

func TestCodexArgsWithReasoningEffortReplacesExistingConfigForms(t *testing.T) {
	got := codexArgsWithReasoningEffort([]string{
		"-c", `model_reasoning_effort="medium"`,
		"--config", `sandbox_mode="read-only"`,
		"--config=model_reasoning_effort=\"high\"",
		"-c=model_reasoning_effort=\"xhigh\"",
		"--model", "gpt-5",
	}, "low")
	want := []string{
		"--config", `sandbox_mode="read-only"`,
		"--model", "gpt-5",
		"-c", teams.CodexReasoningEffortConfigArg("low"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("codexArgsWithReasoningEffort = %#v, want %#v", got, want)
	}
}

func TestCodexReasoningEffortFromArgsPreservesExplicitLastOverride(t *testing.T) {
	args := []string{
		"-c", `model_reasoning_effort="low"`,
		"--config=unrelated=true",
		`-c=model_reasoning_effort='medium'`,
		"--config", `model_reasoning_effort="xhigh"`,
	}
	if got := codexReasoningEffortFromArgs(args); got != "xhigh" {
		t.Fatalf("codexReasoningEffortFromArgs = %q, want xhigh", got)
	}
	executor, err := newTeamsExecutor(&rootOptions{}, "codex", "appserver", "/tmp/codex", "/work", []string{"-c", `model_reasoning_effort="medium"`}, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	provider, ok := executor.(teams.ReasoningEffortDefaultProvider)
	if !ok {
		t.Fatalf("executor %T does not expose its launch effort default", executor)
	}
	if got := provider.DefaultReasoningEffort(); got != "medium" {
		t.Fatalf("executor default effort = %q, want medium", got)
	}
}

func TestNewManagedTeamsCodexExecutorRejectsUnknownRunner(t *testing.T) {
	_, err := newManagedTeamsCodexExecutor(&rootOptions{}, "unknown", "", "", nil, "", 0, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unknown Teams codex runner") {
		t.Fatalf("expected unknown runner error, got %v", err)
	}
}

func TestRunTeamsUpgradeCodexOnceUsesExistingUpgradePath(t *testing.T) {
	lockCLITestHooks(t)
	stubTeamsCodexUpgradePath(t, "/target-account/bin/codex")

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cfgPath := filepath.Join(tmp, "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatalf("Save config: %v", err)
	}

	prevUpgrade := upgradeCodexInstalledForTeamsRun
	t.Cleanup(func() { upgradeCodexInstalledForTeamsRun = prevUpgrade })
	called := false
	upgradeCodexInstalledForTeamsRun = func(_ context.Context, _ io.Writer, opts codexInstallOptions) (string, error) {
		called = true
		if !opts.upgradeCodex {
			t.Fatal("expected upgradeCodex install option")
		}
		if opts.upgradeCodexPath != "/target-account/bin/codex" {
			t.Fatalf("upgrade Codex path = %q", opts.upgradeCodexPath)
		}
		if opts.withInstallerEnv != nil {
			t.Fatal("did not expect proxy installer env when proxy is disabled")
		}
		return "/managed/codex", nil
	}

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	var out strings.Builder
	cmd.SetOut(&out)
	err = runTeamsUpgradeCodexOnce(cmd, &rootOptions{configPath: cfgPath}, "")
	if err != nil {
		t.Fatalf("runTeamsUpgradeCodexOnce error: %v", err)
	}
	if !called {
		t.Fatal("upgrade function was not called")
	}
	if !strings.Contains(out.String(), "Codex upgraded before Teams listen: /managed/codex") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestRunTeamsUpgradeCodexOnceSkipsIncompleteProxyPreferenceCI(t *testing.T) {
	lockCLITestHooks(t)
	stubTeamsCodexUpgradePath(t, "/target-account/bin/codex")

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cfgPath := filepath.Join(tmp, "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(true)}); err != nil {
		t.Fatalf("Save config: %v", err)
	}

	prevEnsureProfile := ensureProfileRunFn
	prevUpgrade := upgradeCodexInstalledForTeamsRun
	t.Cleanup(func() {
		ensureProfileRunFn = prevEnsureProfile
		upgradeCodexInstalledForTeamsRun = prevUpgrade
	})
	ensureProfileRunFn = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		t.Fatal("incomplete proxy preference must not start interactive profile setup during Teams upgrade")
		return config.Profile{}, config.Config{}, nil
	}
	called := false
	upgradeCodexInstalledForTeamsRun = func(_ context.Context, _ io.Writer, opts codexInstallOptions) (string, error) {
		called = true
		if !opts.upgradeCodex {
			t.Fatal("expected upgradeCodex install option")
		}
		if opts.withInstallerEnv != nil {
			t.Fatal("did not expect proxy installer env for incomplete proxy preference")
		}
		return "/managed/codex", nil
	}

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := runTeamsUpgradeCodexOnce(cmd, &rootOptions{configPath: cfgPath}, ""); err != nil {
		t.Fatalf("runTeamsUpgradeCodexOnce error: %v", err)
	}
	if !called {
		t.Fatal("upgrade function was not called")
	}
}

func TestRunTeamsUpgradeCodexOnceRejectsLiveTeamsOwnerBeforeUpgrade(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	prevUpgrade := upgradeCodexInstalledForTeamsRun
	t.Cleanup(func() { upgradeCodexInstalledForTeamsRun = prevUpgrade })
	upgradeCodexInstalledForTeamsRun = func(context.Context, io.Writer, codexInstallOptions) (string, error) {
		t.Fatal("upgrade should not run while a Teams bridge owner is live")
		return "", nil
	}
	_ = seedLiveTeamsOwnerForUpgradeTest(t)

	cmd := newRootCmd()
	err := runTeamsUpgradeCodexOnce(cmd, &rootOptions{}, "")
	if err == nil || !strings.Contains(err.Error(), "Teams bridge is already running") {
		t.Fatalf("expected live owner error, got %v", err)
	}
}

func TestRunTeamsUpgradeCodexOnceRejectsUnfinishedTeamsWorkWithoutOwner(t *testing.T) {
	lockCLITestHooks(t)

	cases := []struct {
		name string
		seed func(t *testing.T, st *teamstore.Store)
	}{
		{
			name: "queued turn",
			seed: func(t *testing.T, st *teamstore.Store) {
				t.Helper()
				if _, _, err := st.CreateSession(context.Background(), teamstore.SessionContext{ID: "s1", Status: teamstore.SessionStatusActive, TeamsChatID: "chat-1"}); err != nil {
					t.Fatalf("CreateSession: %v", err)
				}
				if _, _, err := st.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-queued", SessionID: "s1", Status: teamstore.TurnStatusQueued}); err != nil {
					t.Fatalf("QueueTurn: %v", err)
				}
			},
		},
		{
			name: "running turn",
			seed: func(t *testing.T, st *teamstore.Store) {
				t.Helper()
				if _, _, err := st.CreateSession(context.Background(), teamstore.SessionContext{ID: "s1", Status: teamstore.SessionStatusActive, TeamsChatID: "chat-1"}); err != nil {
					t.Fatalf("CreateSession: %v", err)
				}
				if _, _, err := st.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-running", SessionID: "s1", Status: teamstore.TurnStatusRunning}); err != nil {
					t.Fatalf("QueueTurn: %v", err)
				}
			},
		},
		{
			name: "blocking outbox",
			seed: func(t *testing.T, st *teamstore.Store) {
				t.Helper()
				if _, _, err := st.CreateSession(context.Background(), teamstore.SessionContext{ID: "s1", Status: teamstore.SessionStatusActive, TeamsChatID: "chat-1"}); err != nil {
					t.Fatalf("CreateSession: %v", err)
				}
				if _, _, err := st.QueueOutbox(context.Background(), teamstore.OutboxMessage{ID: "outbox-1", SessionID: "s1", TeamsChatID: "chat-1", Body: "pending"}); err != nil {
					t.Fatalf("QueueOutbox: %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			prevUpgrade := upgradeCodexInstalledForTeamsRun
			t.Cleanup(func() { upgradeCodexInstalledForTeamsRun = prevUpgrade })
			upgradeCodexInstalledForTeamsRun = func(context.Context, io.Writer, codexInstallOptions) (string, error) {
				t.Fatal("upgrade should not run while Teams work is upgrade-blocking")
				return "", nil
			}
			st, err := openTeamsStore()
			if err != nil {
				t.Fatalf("openTeamsStore: %v", err)
			}
			tc.seed(t, st)

			cmd := newRootCmd()
			err = runTeamsUpgradeCodexOnce(cmd, &rootOptions{}, "")
			if err == nil || !strings.Contains(err.Error(), "upgrade-blocking work") {
				t.Fatalf("expected upgrade-blocking work error, got %v", err)
			}
			if !strings.Contains(err.Error(), "status=") {
				t.Fatalf("upgrade-blocking error should name concrete blocker status, got %v", err)
			}
		})
	}
}

func TestRunTeamsUpgradeCodexOnceRejectsBeaconTargetWork(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	seedBeaconStateForUpgradeTest(t, func(st *beacon.State) {
		st.Conversations["conv-1"] = beacon.Conversation{
			ID: "conv-1",
			Queued: []beacon.QueuedTurn{{
				ID:       "turn-gpu",
				Snapshot: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu", Signature: "sig-gpu"},
			}},
		}
	})

	prevUpgrade := upgradeCodexInstalledForTeamsRun
	t.Cleanup(func() { upgradeCodexInstalledForTeamsRun = prevUpgrade })
	upgradeCodexInstalledForTeamsRun = func(context.Context, io.Writer, codexInstallOptions) (string, error) {
		t.Fatal("upgrade should not run while beacon target work is queued")
		return "", nil
	}

	cmd := newRootCmd()
	err := runTeamsUpgradeCodexOnce(cmd, &rootOptions{}, "")
	if err == nil || !strings.Contains(err.Error(), "Beacon state has upgrade-blocking work") || !strings.Contains(err.Error(), "beacon_queued_turn conv-1 turn-gpu") {
		t.Fatalf("expected beacon queued turn blocker, got %v", err)
	}
}

func TestRunTeamsUpgradeCodexOnceRejectsCodexPath(t *testing.T) {
	cmd := newRootCmd()
	err := runTeamsUpgradeCodexOnce(cmd, &rootOptions{}, "/custom/codex")
	if err == nil || !strings.Contains(err.Error(), "--upgrade-codex cannot be used with --codex-path") {
		t.Fatalf("expected codex-path conflict, got %v", err)
	}
}

func TestRunTeamsCodexUpgradeFromBridgeUsesExistingUpgradePath(t *testing.T) {
	stubTeamsCodexUpgradePath(t, "/target-account/bin/codex")
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatalf("Save config: %v", err)
	}
	prevUpgrade := upgradeCodexInstalledForTeamsRun
	t.Cleanup(func() { upgradeCodexInstalledForTeamsRun = prevUpgrade })
	called := false
	upgradeCodexInstalledForTeamsRun = func(_ context.Context, _ io.Writer, opts codexInstallOptions) (string, error) {
		called = true
		if !opts.upgradeCodex {
			t.Fatal("expected upgradeCodex install option")
		}
		if opts.upgradeCodexPath != "/target-account/bin/codex" {
			t.Fatalf("upgrade Codex path = %q", opts.upgradeCodexPath)
		}
		return "/managed/codex", nil
	}

	got, err := runTeamsCodexUpgradeFromBridge(context.Background(), &rootOptions{configPath: cfgPath}, io.Discard, "")
	if err != nil {
		t.Fatalf("runTeamsCodexUpgradeFromBridge error: %v", err)
	}
	if !called || got.Path != "/managed/codex" {
		t.Fatalf("upgrade called=%v result=%#v", called, got)
	}
}

func TestRunTeamsCodexUpgradeFromBridgeSkipsIncompleteProxyPreferenceCI(t *testing.T) {
	lockCLITestHooks(t)
	stubTeamsCodexUpgradePath(t, "/target-account/bin/codex")

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(true)}); err != nil {
		t.Fatalf("Save config: %v", err)
	}

	prevEnsureProfile := ensureProfileRunFn
	prevUpgrade := upgradeCodexInstalledForTeamsRun
	t.Cleanup(func() {
		ensureProfileRunFn = prevEnsureProfile
		upgradeCodexInstalledForTeamsRun = prevUpgrade
	})
	ensureProfileRunFn = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		t.Fatal("incomplete proxy preference must not start interactive profile setup during bridge upgrade")
		return config.Profile{}, config.Config{}, nil
	}
	called := false
	upgradeCodexInstalledForTeamsRun = func(_ context.Context, _ io.Writer, opts codexInstallOptions) (string, error) {
		called = true
		if !opts.upgradeCodex {
			t.Fatal("expected upgradeCodex install option")
		}
		if opts.withInstallerEnv != nil {
			t.Fatal("did not expect proxy installer env for incomplete proxy preference")
		}
		return "/managed/codex", nil
	}

	got, err := runTeamsCodexUpgradeFromBridge(context.Background(), &rootOptions{configPath: cfgPath}, io.Discard, "")
	if err != nil {
		t.Fatalf("runTeamsCodexUpgradeFromBridge error: %v", err)
	}
	if !called || got.Path != "/managed/codex" {
		t.Fatalf("upgrade called=%v result=%#v", called, got)
	}
}

func stubTeamsCodexUpgradePath(t *testing.T, path string) {
	t.Helper()
	previous := resolveTeamsCodexUpgradeTargetForRun
	resolveTeamsCodexUpgradeTargetForRun = func(context.Context, config.Config, effectivePaths) (teamsCodexUpgradeTarget, error) {
		return teamsCodexUpgradeTarget{path: path, environment: []string{"PATH=/target-account/bin:/usr/bin"}}, nil
	}
	t.Cleanup(func() { resolveTeamsCodexUpgradeTargetForRun = previous })
}

func TestTeamsCodexUpgradeProxyEnvironmentPreservesTargetPATH(t *testing.T) {
	got := teamsCodexUpgradeProxyEnvironment(
		[]string{"PATH=/target/bin:/usr/bin", "HOME=/home/target", "HTTP_PROXY=http://stale"},
		[]string{"PATH=/service/bin", "HOME=/service", "HTTP_PROXY=http://127.0.0.1:18080"},
	)
	if path := envValue(got, "PATH"); path != "/target/bin:/usr/bin" {
		t.Fatalf("PATH = %q", path)
	}
	if home := envValue(got, "HOME"); home != "/home/target" {
		t.Fatalf("HOME = %q", home)
	}
	if proxy := envValue(got, "HTTP_PROXY"); proxy != "http://127.0.0.1:18080" {
		t.Fatalf("HTTP_PROXY = %q", proxy)
	}
}

func TestTeamsCodexUpgraderForRunRejectsCustomCodexPathBeforeDrain(t *testing.T) {
	if got := teamsCodexUpgraderForRun(&rootOptions{}, io.Discard, " /custom/codex "); got != nil {
		t.Fatal("custom --codex-path must disable control-chat Codex updates")
	}
	if got := teamsCodexUpgraderForRun(&rootOptions{}, io.Discard, ""); got == nil {
		t.Fatal("default Codex discovery should enable control-chat Codex updates")
	}
}

func TestUpgradeOrInstallManagedTeamsCodexMigratesWhenManagedInstallIsMissing(t *testing.T) {
	previousEnsure := ensureCodexInstalledForTeamsRun
	previousUpgrade := upgradeCodexInstalledForTeamsRun
	t.Cleanup(func() {
		ensureCodexInstalledForTeamsRun = previousEnsure
		upgradeCodexInstalledForTeamsRun = previousUpgrade
	})
	upgradeCodexInstalledForTeamsRun = func(context.Context, io.Writer, codexInstallOptions) (string, error) {
		t.Fatal("missing managed install must not upgrade a PATH or cached Codex")
		return "", nil
	}
	ensureCodexInstalledForTeamsRun = func(_ context.Context, path string, _ io.Writer, opts codexInstallOptions) (string, error) {
		if path != "" || opts.upgradeCodex || !opts.requireManaged || envValue(opts.installerEnv, "PATH") != "/service/bin" {
			t.Fatalf("managed migration path=%q opts=%#v", path, opts)
		}
		return "/managed/bin/codex", nil
	}

	got, err := upgradeOrInstallManagedTeamsCodex(context.Background(), io.Discard, teamsCodexUpgradeTarget{
		environment: []string{"PATH=/service/bin"},
	}, codexInstallOptions{upgradeCodex: true, installerEnv: []string{"PATH=/service/bin"}, requireManaged: true})
	if err != nil {
		t.Fatal(err)
	}
	if got != "/managed/bin/codex" {
		t.Fatalf("managed migration result = %q", got)
	}
}

type restartTrackingTeamsExecutor struct {
	restarts int
}

func (e *restartTrackingTeamsExecutor) Run(context.Context, *teams.Session, string) (teams.ExecutionResult, error) {
	return teams.ExecutionResult{}, nil
}

func (e *restartTrackingTeamsExecutor) RestartCodexRunners() error {
	e.restarts++
	return nil
}

func TestTeamsCodexUpgraderForRunRestartsCachedRunnersAfterSuccess(t *testing.T) {
	lockCLITestHooks(t)
	stubTeamsCodexUpgradePath(t, "/target-account/bin/codex")
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatal(err)
	}
	previousUpgrade := upgradeCodexInstalledForTeamsRun
	upgradeCodexInstalledForTeamsRun = func(context.Context, io.Writer, codexInstallOptions) (string, error) {
		return "/target-account/bin/codex", nil
	}
	t.Cleanup(func() { upgradeCodexInstalledForTeamsRun = previousUpgrade })

	executor := &restartTrackingTeamsExecutor{}
	upgrader := teamsCodexUpgraderForRun(&rootOptions{configPath: cfgPath}, io.Discard, "", executor)
	result, err := upgrader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != "/target-account/bin/codex" || executor.restarts != 1 {
		t.Fatalf("result=%#v runner restarts=%d", result, executor.restarts)
	}
}

type fakeTeamsRunner struct {
	result   codexrunner.TurnResult
	err      error
	resumed  bool
	threadID string
	input    codexrunner.TurnInput
	models   []codexrunner.ModelInfo
}

func (r *fakeTeamsRunner) StartThread(_ context.Context, input codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	r.input = input
	return r.result, r.err
}

func (r *fakeTeamsRunner) ResumeThread(_ context.Context, threadID string, input codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	r.resumed = true
	r.threadID = threadID
	r.input = input
	return r.result, r.err
}

func (r *fakeTeamsRunner) StartTurn(context.Context, codexrunner.StartTurnInput) (codexrunner.TurnResult, error) {
	return r.result, r.err
}

func (r *fakeTeamsRunner) InterruptTurn(context.Context, codexrunner.TurnRef) error {
	return nil
}

func (r *fakeTeamsRunner) ReadThread(context.Context, string) (codexrunner.Thread, error) {
	return codexrunner.Thread{}, nil
}

func (r *fakeTeamsRunner) ListThreads(context.Context, codexrunner.ListThreadsOptions) ([]codexrunner.Thread, error) {
	return nil, nil
}

func (r *fakeTeamsRunner) ListModels(context.Context) ([]codexrunner.ModelInfo, error) {
	return append([]codexrunner.ModelInfo(nil), r.models...), nil
}
