package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

func TestTeamsCodexLauncherUsesManagedRunPathHeadlessly(t *testing.T) {
	lockCLITestHooks(t)
	if os.PathSeparator != '/' {
		t.Skip("shell stub test uses POSIX script")
	}

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false), YoloEnabled: boolPtr(false)}); err != nil {
		t.Fatalf("Save config: %v", err)
	}

	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\ncase \"$1\" in --version) exit 0 ;; --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; *) exit 0 ;; esac\n"), 0o700); err != nil {
		t.Fatalf("write codex stub: %v", err)
	}

	prevRun := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() { runTargetWithFallbackWithOptionsFn = prevRun })
	var gotArgs []string
	var gotOpts runTargetOptions
	runTargetWithFallbackWithOptionsFn = func(_ context.Context, cmdArgs []string, _ string, _ func() error, _ <-chan error, opts runTargetOptions) error {
		gotArgs = append([]string{}, cmdArgs...)
		gotOpts = opts
		stdin, err := io.ReadAll(opts.Stdin)
		if err != nil {
			t.Fatalf("read stdin: %v", err)
		}
		if string(stdin) != "prompt text" {
			t.Fatalf("stdin = %q", string(stdin))
		}
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"thread.started","thread_id":"thread-managed"}`)
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`)
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"turn.completed"}`)
		return nil
	}

	launcher := teamsCodexLauncher{root: &rootOptions{configPath: cfgPath}, log: io.Discard}
	result, err := launcher.Launch(context.Background(), codexrunner.LaunchRequest{
		Command: codexPath,
		Args:    []string{"exec", "--json", "-"},
		Dir:     t.TempDir(),
		Stdin:   "prompt text",
	})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if !reflect.DeepEqual(gotArgs, []string{codexPath, "--dangerously-bypass-approvals-and-sandbox", "exec", "--json", "-"}) {
		t.Fatalf("cmd args = %#v", gotArgs)
	}
	if gotOpts.UseProxy {
		t.Fatal("expected direct run options")
	}
	if gotOpts.PreserveTTY {
		t.Fatal("Teams launcher must not preserve TTY")
	}
	if !gotOpts.YoloEnabled {
		t.Fatal("Teams launcher should force yolo mode even when global config has yolo disabled")
	}
	if !gotOpts.RequireYolo {
		t.Fatal("Teams launcher must not fall back to sandbox mode when yolo launch is rejected")
	}
	if gotOpts.Stdout == nil || gotOpts.Stderr == nil || gotOpts.Stdin == nil {
		t.Fatalf("headless IO not configured: %#v", gotOpts)
	}
	if !hasExplicitCodexHomeEnv(gotOpts.ExtraEnv) {
		t.Fatalf("expected Codex home env in launch options: %#v", gotOpts.ExtraEnv)
	}
	if !strings.Contains(string(result.Stdout), "thread-managed") {
		t.Fatalf("stdout was not captured: %s", string(result.Stdout))
	}
}

func TestTeamsCodexExecutorResumesExistingSession(t *testing.T) {
	runner := &fakeTeamsRunner{result: codexrunner.TurnResult{
		ThreadID:          "thread-existing",
		TurnID:            "turn-existing",
		FinalAgentMessage: "final",
	}}
	executor := teamsCodexExecutor{runner: runner}
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !runner.resumed || runner.threadID != "thread-existing" {
		t.Fatalf("expected resume with exact thread id, runner=%#v", runner)
	}
	if got.Text != "final" || got.CodexThreadID != "thread-existing" || got.CodexTurnID != "turn-existing" {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestNewManagedTeamsCodexExecutorCanUseExperimentalAppServerRunner(t *testing.T) {
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{}, "appserver", "/tmp/codex", "/work", []string{"--model", "gpt-test"}, time.Minute, io.Discard)
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
	if runner.Starter == nil || runner.Fallback == nil {
		t.Fatalf("appserver runner missing starter or fallback: %#v", runner)
	}
	if runner.Command != "/tmp/codex" || runner.WorkingDir != "/work" || runner.Timeout != time.Minute {
		t.Fatalf("appserver runner config mismatch: %#v", runner)
	}
	if !reflect.DeepEqual(runner.ExtraArgs, []string{"--model", "gpt-test"}) {
		t.Fatalf("appserver extra args = %#v", runner.ExtraArgs)
	}
	if len(runner.AppServerArgs) != 0 {
		t.Fatalf("appserver process args should be separate from turn args: %#v", runner.AppServerArgs)
	}
}

func TestNewTeamsControlFallbackExecutorForcesSparkModel(t *testing.T) {
	executor, err := newTeamsControlFallbackExecutor(&rootOptions{}, "exec", "/tmp/codex", "/work", []string{"--model", "gpt-5", "--sandbox", "workspace-write"}, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newTeamsControlFallbackExecutor error: %v", err)
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		t.Fatalf("executor type = %T, want teamsCodexExecutor", executor)
	}
	runner, ok := teamsExecutor.runner.(*codexrunner.ExecRunner)
	if !ok {
		t.Fatalf("runner type = %T, want ExecRunner", teamsExecutor.runner)
	}
	want := []string{"--model", teams.DefaultControlFallbackModel}
	if !reflect.DeepEqual(runner.ExtraArgs, want) {
		t.Fatalf("fallback extra args = %#v, want %#v", runner.ExtraArgs, want)
	}
}

func TestNewManagedTeamsCodexExecutorStripsSandboxArgs(t *testing.T) {
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{}, "exec", "/tmp/codex", "/work", []string{
		"--model", "gpt-test",
		"--sandbox=workspace-write",
		"--ask-for-approval", "on-request",
		"-s", "read-only",
	}, time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newManagedTeamsCodexExecutor error: %v", err)
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		t.Fatalf("executor type = %T, want teamsCodexExecutor", executor)
	}
	runner, ok := teamsExecutor.runner.(*codexrunner.ExecRunner)
	if !ok {
		t.Fatalf("runner type = %T, want ExecRunner", teamsExecutor.runner)
	}
	want := []string{"--model", "gpt-test"}
	if !reflect.DeepEqual(runner.ExtraArgs, want) {
		t.Fatalf("runner extra args = %#v, want %#v", runner.ExtraArgs, want)
	}
}

func TestCodexArgsWithModelReplacesExistingModelForms(t *testing.T) {
	got := codexArgsWithModel([]string{"--model", "gpt-5", "--model=gpt-5.2", "-m", "mini", "-m=old", "--sandbox", "read-only"}, "spark")
	want := []string{"--sandbox", "read-only", "--model", "spark"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("codexArgsWithModel = %#v, want %#v", got, want)
	}
}

func TestNewManagedTeamsCodexExecutorRejectsUnknownRunner(t *testing.T) {
	_, err := newManagedTeamsCodexExecutor(&rootOptions{}, "unknown", "", "", nil, 0, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unknown Teams codex runner") {
		t.Fatalf("expected unknown runner error, got %v", err)
	}
}

func TestRunTeamsUpgradeCodexOnceUsesExistingUpgradePath(t *testing.T) {
	lockCLITestHooks(t)

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

func TestRunTeamsUpgradeCodexOnceRejectsCodexPath(t *testing.T) {
	cmd := newRootCmd()
	err := runTeamsUpgradeCodexOnce(cmd, &rootOptions{}, "/custom/codex")
	if err == nil || !strings.Contains(err.Error(), "--upgrade-codex cannot be used with --codex-path") {
		t.Fatalf("expected codex-path conflict, got %v", err)
	}
}

type fakeTeamsRunner struct {
	result   codexrunner.TurnResult
	resumed  bool
	threadID string
}

func (r *fakeTeamsRunner) StartThread(context.Context, codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	return r.result, nil
}

func (r *fakeTeamsRunner) ResumeThread(_ context.Context, threadID string, _ codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	r.resumed = true
	r.threadID = threadID
	return r.result, nil
}

func (r *fakeTeamsRunner) StartTurn(context.Context, codexrunner.StartTurnInput) (codexrunner.TurnResult, error) {
	return r.result, nil
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
