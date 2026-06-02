package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
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
	codexDir := t.TempDir()
	setTestCodexHomeEnv(t, codexDir)
	writeFakeCache(t, codexDir)
	originalAuth := writeTestAuthJSON(t, codexDir, true)

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
		if !cacheExists(t, codexDir) {
			t.Fatal("Teams yolo launch should install a cloud requirements bypass cache before Codex starts")
		}
		cache := readTestCloudRequirementsCache(t, codexDir)
		if cache.Signature == "" {
			t.Fatal("Teams yolo launch should sign the cloud requirements bypass cache")
		}
		if cache.SignedPayload.ChatGPTUserID != "user_test" || cache.SignedPayload.AccountID != "org_test" {
			t.Fatalf("cache identity = %#v", cache.SignedPayload)
		}
		if cache.SignedPayload.Contents != nil {
			t.Fatalf("cache contents = %#v, want nil", *cache.SignedPayload.Contents)
		}
		authDuringRun, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
		if err != nil {
			t.Fatalf("read auth during run: %v", err)
		}
		if authJSONHasPlanClaim(t, authDuringRun) {
			t.Fatal("Teams yolo launch should mask workspace plan auth before Codex starts")
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
	if !reflect.DeepEqual(gotArgs, []string{codexPath, "-c", `cli_auth_credentials_store="file"`, "--dangerously-bypass-approvals-and-sandbox", "exec", "--json", "-"}) {
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
	if !hasEnvValue(gotOpts.ExtraEnv, envTeamsCodexChild, "1") {
		t.Fatalf("expected Teams child marker env in launch options: %#v", gotOpts.ExtraEnv)
	}
	if !hasEnvValue(gotOpts.ExtraEnv, envTeamsCodexParentPID, fmt.Sprint(os.Getpid())) {
		t.Fatalf("expected Teams parent pid env in launch options: %#v", gotOpts.ExtraEnv)
	}
	if envValue(gotOpts.ExtraEnv, envTeamsHelperCLIPath) == "" {
		t.Fatalf("expected Teams helper CLI path env in launch options: %#v", gotOpts.ExtraEnv)
	}
	if !strings.Contains(string(result.Stdout), "thread-managed") {
		t.Fatalf("stdout was not captured: %s", string(result.Stdout))
	}
	authAfterRun, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read auth after run: %v", err)
	}
	if !reflect.DeepEqual(authAfterRun, originalAuth) {
		t.Fatal("Teams yolo launch should restore auth after Codex exits")
	}
	if cacheExists(t, codexDir) {
		t.Fatal("Teams yolo launch should remove the bypass cache after Codex exits")
	}
}

func TestTeamsCodexLauncherModelProfileWithoutSSHRespectsDisabledProxyPreferenceCI(t *testing.T) {
	lockCLITestHooks(t)
	if os.PathSeparator != '/' {
		t.Skip("shell stub test uses POSIX script")
	}

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	disabled := false
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &disabled,
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-live": {
				Provider:  "deepseek",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  1,
			},
		},
	}); err != nil {
		t.Fatalf("Save config: %v", err)
	}
	t.Setenv("DEEPSEEK_API_KEY", "sk-ci-deepseek")

	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\ncase \"$1\" in --version) exit 0 ;; --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; *) exit 0 ;; esac\n"), 0o700); err != nil {
		t.Fatalf("write codex stub: %v", err)
	}
	setTestCodexHomeEnv(t, t.TempDir())

	prevEnsureProxy := ensureProxyPreferenceRunFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() {
		ensureProxyPreferenceRunFn = prevEnsureProxy
		runTargetWithFallbackWithOptionsFn = prevRunTarget
	})
	ensureProxyPreferenceRunFn = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion, ProxyEnabled: &disabled}, nil
	}

	var gotArgs []string
	var gotOpts runTargetOptions
	runTargetWithFallbackWithOptionsFn = func(_ context.Context, cmdArgs []string, _ string, _ func() error, _ <-chan error, opts runTargetOptions) error {
		gotArgs = append([]string{}, cmdArgs...)
		gotOpts = opts
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"thread.started","thread_id":"thread-model-profile"}`)
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`)
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"turn.completed"}`)
		return nil
	}

	launcher := teamsCodexLauncher{root: &rootOptions{configPath: cfgPath}, log: io.Discard, modelProfileRef: "deepseek-live"}
	result, err := launcher.Launch(context.Background(), codexrunner.LaunchRequest{
		Command: codexPath,
		Args:    []string{"exec", "--json", "-"},
		Dir:     t.TempDir(),
		Stdin:   "prompt text",
	})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d", result.ExitCode)
	}
	if gotOpts.UseProxy {
		t.Fatalf("UseProxy = true, want false when global proxy preference is disabled")
	}
	if envValue(gotOpts.ExtraEnv, envCXPResponsesProxyKey) == "" {
		t.Fatalf("missing model profile proxy key env: %v", gotOpts.ExtraEnv)
	}
	if !strings.Contains(strings.Join(gotArgs, "\n"), `model_providers.`+cxpCodexModelProviderID+`.requires_openai_auth=false`) {
		t.Fatalf("missing third-party codex config override: %v", gotArgs)
	}
}

func TestTeamsCodexLauncherModelProfileWithoutSSHServiceDefaultsDirectWhenProxyUnsetCI(t *testing.T) {
	lockCLITestHooks(t)
	if os.PathSeparator != '/' {
		t.Skip("shell stub test uses POSIX script")
	}

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-live": {
				Provider:  "deepseek",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  1,
			},
		},
	}); err != nil {
		t.Fatalf("Save config: %v", err)
	}
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	t.Setenv("DEEPSEEK_API_KEY", "sk-ci-deepseek")

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = prevStdin
		_ = reader.Close()
		_ = writer.Close()
	})

	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\ncase \"$1\" in --version) exit 0 ;; --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; *) exit 0 ;; esac\n"), 0o700); err != nil {
		t.Fatalf("write codex stub: %v", err)
	}
	setTestCodexHomeEnv(t, t.TempDir())

	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() { runTargetWithFallbackWithOptionsFn = prevRunTarget })

	var gotArgs []string
	var gotOpts runTargetOptions
	runTargetWithFallbackWithOptionsFn = func(_ context.Context, cmdArgs []string, _ string, _ func() error, _ <-chan error, opts runTargetOptions) error {
		gotArgs = append([]string{}, cmdArgs...)
		gotOpts = opts
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"thread.started","thread_id":"thread-model-profile-default-direct"}`)
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`)
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"turn.completed"}`)
		return nil
	}

	type launchResult struct {
		result codexrunner.LaunchResult
		err    error
	}
	done := make(chan launchResult, 1)
	launchDir := t.TempDir()
	go func() {
		launcher := teamsCodexLauncher{root: &rootOptions{configPath: cfgPath}, log: io.Discard, modelProfileRef: "deepseek-live"}
		result, err := launcher.Launch(context.Background(), codexrunner.LaunchRequest{
			Command: codexPath,
			Args:    []string{"exec", "--json", "-"},
			Dir:     launchDir,
			Stdin:   "prompt text",
		})
		done <- launchResult{result: result, err: err}
	}()

	var result codexrunner.LaunchResult
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Launch: %v", got.err)
		}
		result = got.result
	case <-time.After(5 * time.Second):
		t.Fatal("Teams model-profile launch blocked on stdin")
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d", result.ExitCode)
	}
	if gotOpts.UseProxy {
		t.Fatalf("UseProxy = true, want service-mode default direct when proxy preference is unset")
	}
	if envValue(gotOpts.ExtraEnv, envCXPResponsesProxyKey) == "" {
		t.Fatalf("missing model profile proxy key env: %v", gotOpts.ExtraEnv)
	}
	if !strings.Contains(strings.Join(gotArgs, "\n"), `model_providers.`+cxpCodexModelProviderID+`.requires_openai_auth=false`) {
		t.Fatalf("missing third-party codex config override: %v", gotArgs)
	}
	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled == nil || *updated.ProxyEnabled {
		t.Fatalf("expected service launch to persist ProxyEnabled=false, got %v", updated.ProxyEnabled)
	}
}

func TestTeamsCodexLauncherModelProfileWithoutSSHUsesGlobalProxyPreferenceCI(t *testing.T) {
	lockCLITestHooks(t)

	for _, tc := range []struct {
		name     string
		snapshot modelprofile.Snapshot
	}{
		{
			name:     "default",
			snapshot: modelprofile.Snapshot{Name: "default", Provider: "default", Revision: 1},
		},
		{
			name:     "third-party",
			snapshot: modelprofile.Snapshot{Name: "mimo25", Provider: "mimo", APIKeyRef: "env:MIMO_KEY", Revision: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfgPath := filepath.Join(t.TempDir(), "config.json")
			store, err := config.NewStore(cfgPath)
			if err != nil {
				t.Fatalf("NewStore: %v", err)
			}
			enabled := true
			profile := config.Profile{ID: "p1", Name: "primary", Host: "example.com", User: "coder", CreatedAt: time.Now()}
			if err := store.Save(config.Config{
				Version:      config.CurrentVersion,
				ProxyEnabled: &enabled,
				Profiles:     []config.Profile{profile},
			}); err != nil {
				t.Fatalf("Save config: %v", err)
			}

			prevEnsureProxy := ensureProxyPreferenceRunFn
			prevEnsureProfile := ensureProfileRunFn
			t.Cleanup(func() {
				ensureProxyPreferenceRunFn = prevEnsureProxy
				ensureProfileRunFn = prevEnsureProfile
			})
			ensureProxyPreferenceRunFn = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
				return true, config.Config{Version: config.CurrentVersion, ProxyEnabled: &enabled, Profiles: []config.Profile{profile}}, nil
			}
			wantErr := errors.New("stop after proxy selection")
			var gotProfileRef string
			ensureProfileRunFn = func(_ context.Context, _ *config.Store, profileRef string, _ bool, _ io.Writer) (config.Profile, config.Config, error) {
				gotProfileRef = profileRef
				return config.Profile{}, config.Config{}, wantErr
			}

			launcher := teamsCodexLauncher{root: &rootOptions{configPath: cfgPath}, log: io.Discard, modelProfileSnapshot: tc.snapshot}
			_, err = launcher.Launch(context.Background(), codexrunner.LaunchRequest{
				Command: "codex",
				Args:    []string{"exec", "--json", "-"},
				Dir:     t.TempDir(),
				Stdin:   "prompt text",
			})
			if !errors.Is(err, wantErr) {
				t.Fatalf("Launch error = %v, want %v", err, wantErr)
			}
			if gotProfileRef != "" {
				t.Fatalf("profile ref = %q, want generic global proxy profile selection", gotProfileRef)
			}
		})
	}
}

func TestTeamsCodexChildEnvExposesHelperCLIPathAndDir(t *testing.T) {
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
	path := envValue(got, "PATH")
	if !strings.HasPrefix(path, dir+string(os.PathListSeparator)) {
		t.Fatalf("PATH = %q, want helper dir prepended", path)
	}
}

func TestTeamsCodexChildEnvDoesNotDuplicateHelperDirInPATH(t *testing.T) {
	prevExecutablePath := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = prevExecutablePath })

	dir := t.TempDir()
	teamsChildExecutablePath = func() (string, error) { return filepath.Join(dir, "codex-proxy"), nil }
	t.Setenv("PATH", dir+string(os.PathListSeparator)+"/usr/bin")

	got := teamsCodexChildEnv()
	if path := envValue(got, "PATH"); path != dir+string(os.PathListSeparator)+"/usr/bin" {
		t.Fatalf("PATH = %q, want unchanged when helper dir is already present", path)
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

func TestTeamsCodexChildEnvMakesHelperDirDiscoverableOnPATH(t *testing.T) {
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

	cmd := exec.Command("/bin/sh", "-c", "command -v cxp")
	cmd.Env = []string{"PATH=" + envValue(teamsCodexChildEnv(), "PATH")}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("command -v cxp: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != exe {
		t.Fatalf("command -v cxp = %q, want %q", got, exe)
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
	got, err := executor.Run(context.Background(), &teams.Session{CodexThreadID: "thread-existing"}, "continue")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !runner.resumed || runner.threadID != "thread-existing" {
		t.Fatalf("expected resume with exact thread id, runner=%#v", runner)
	}
	if !runner.input.BackfillThreadName {
		t.Fatal("auto-title session should request thread name backfill")
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
	_, err := executor.Run(context.Background(), &teams.Session{Cwd: "  /workspace/project  "}, "start")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if runner.resumed {
		t.Fatal("new session should start a new thread, not resume")
	}
	if runner.input.WorkingDir != "/workspace/project" {
		t.Fatalf("working dir = %q, want session cwd", runner.input.WorkingDir)
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
			ThreadID:          "thread-existing",
			ThreadName:        "Existing thread title",
			TurnID:            "turn-completed",
			Status:            codexrunner.TurnStatusCompleted,
			FinalAgentMessage: "final answer",
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

func TestTeamsCodexExecutorPassesImageInputToRunner(t *testing.T) {
	runner := &fakeTeamsRunner{
		result: codexrunner.TurnResult{
			ThreadID:          "thread-new",
			TurnID:            "turn-1",
			Status:            codexrunner.TurnStatusCompleted,
			FinalAgentMessage: "saw image",
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

func TestNewManagedTeamsCodexExecutorCanUseExperimentalAppServerRunner(t *testing.T) {
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
	if runner.BackfillThreadName {
		t.Fatal("Teams appserver runner should request thread name backfill per turn, not globally")
	}
}

func TestNewManagedTeamsCodexExecutorConfiguresThirdPartyModelProfileForAppServer(t *testing.T) {
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
		`model_provider="cxp-thirdparty"`,
		`model="mimo/mimo-v2.5"`,
		`model_catalog_json="`,
		`model_providers.cxp-thirdparty.wire_api="responses"`,
	} {
		if !strings.Contains(joinedArgs, want) {
			t.Fatalf("appserver args missing %q:\n%v", want, runner.AppServerArgs)
		}
	}
	if !slices.ContainsFunc(runner.ExtraEnv, func(entry string) bool {
		return strings.HasPrefix(entry, envCXPResponsesProxyKey+"=")
	}) {
		t.Fatalf("appserver extra env missing proxy key: %v", runner.ExtraEnv)
	}
}

func TestTeamsCodexExecutorRoutesSessionModelProfileSnapshot(t *testing.T) {
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
	execRunner, ok := runner.(*codexrunner.ExecRunner)
	if !ok {
		t.Fatalf("runner type = %T, want ExecRunner", runner)
	}
	launcher, ok := execRunner.Launcher.(teamsCodexLauncher)
	if !ok {
		t.Fatalf("launcher type = %T, want teamsCodexLauncher", execRunner.Launcher)
	}
	if launcher.modelProfileSnapshot.APIKeyRef != "env:OLD_MIMO_KEY" || launcher.modelProfileSnapshot.Revision != 3 {
		t.Fatalf("launcher snapshot = %#v, want old pinned key/revision", launcher.modelProfileSnapshot)
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
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-pro": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  1,
			},
		},
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

func TestTeamsCodexExecutorSessionModelProfileLaunchInjectsAdapterCI(t *testing.T) {
	lockCLITestHooks(t)
	if os.PathSeparator != '/' {
		t.Skip("shell stub test uses POSIX script")
	}

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	disabled := false
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &disabled}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("MIMO_KEY_PINNED", "sk-ci-mimo")
	setTestCodexHomeEnv(t, t.TempDir())

	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\ncase \"$1\" in --version) exit 0 ;; --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; *) exit 0 ;; esac\n"), 0o700); err != nil {
		t.Fatalf("write codex stub: %v", err)
	}

	prevEnsureProxy := ensureProxyPreferenceRunFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() {
		ensureProxyPreferenceRunFn = prevEnsureProxy
		runTargetWithFallbackWithOptionsFn = prevRunTarget
	})
	ensureProxyPreferenceRunFn = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion, ProxyEnabled: &disabled}, nil
	}

	var gotArgs []string
	var gotOpts runTargetOptions
	runTargetWithFallbackWithOptionsFn = func(_ context.Context, cmdArgs []string, _ string, _ func() error, _ <-chan error, opts runTargetOptions) error {
		gotArgs = append([]string{}, cmdArgs...)
		gotOpts = opts
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"thread.started","thread_id":"thread-session-profile"}`)
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`)
		_, _ = fmt.Fprintln(opts.Stdout, `{"type":"turn.completed"}`)
		return nil
	}

	executor, err := newManagedTeamsCodexExecutor(&rootOptions{configPath: store.Path()}, "exec", codexPath, "/work", []string{"--skip-git-repo-check", "-c", `model_reasoning_effort="high"`}, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newManagedTeamsCodexExecutor: %v", err)
	}
	session := &teams.Session{
		ID:  "s001",
		Cwd: t.TempDir(),
		ModelProfile: modelprofile.Snapshot{
			Name:      "mimo25-live",
			Provider:  "mimo",
			APIKeyRef: "env:MIMO_KEY_PINNED",
			Revision:  7,
		},
	}
	result, err := executor.(teamsCodexExecutor).RunInput(context.Background(), session, teams.ExecutionInput{Prompt: "prompt text"})
	if err != nil {
		t.Fatalf("RunInput: %v", err)
	}
	if result.CodexThreadID != "thread-session-profile" {
		t.Fatalf("thread id = %q", result.CodexThreadID)
	}
	if gotOpts.UseProxy {
		t.Fatalf("UseProxy = true, want false when global proxy preference is disabled")
	}
	if gotOpts.ModelProfileRef != "" || gotOpts.ModelProfileSnapshot.Provider != "mimo" || gotOpts.ModelProfileSnapshot.APIKeyRef != "env:MIMO_KEY_PINNED" {
		t.Fatalf("model profile opts = ref %q snapshot %#v", gotOpts.ModelProfileRef, gotOpts.ModelProfileSnapshot)
	}
	if envValue(gotOpts.ExtraEnv, envCXPResponsesProxyKey) == "" {
		t.Fatalf("missing model profile proxy key env: %v", gotOpts.ExtraEnv)
	}
	execIndex := slices.Index(gotArgs, "exec")
	providerIndex := codexConfigPairIndex(gotArgs, `model_provider="`+cxpCodexModelProviderID+`"`)
	if execIndex < 0 || providerIndex < 0 || providerIndex <= execIndex {
		t.Fatalf("model profile config must be injected after codex exec, exec=%d provider=%d:\n%v", execIndex, providerIndex, gotArgs)
	}
	reasoningIndex := codexConfigPairIndex(gotArgs, `model_reasoning_effort="high"`)
	if reasoningIndex < 0 || providerIndex >= reasoningIndex {
		t.Fatalf("model profile config should share exec config scope before reasoning override, provider=%d reasoning=%d:\n%v", providerIndex, reasoningIndex, gotArgs)
	}
	joined := strings.Join(gotArgs, "\n")
	for _, want := range []string{
		`model_provider="` + cxpCodexModelProviderID + `"`,
		`model="mimo/mimo-v2.5"`,
		`model_catalog_json="`,
		`model_providers.` + cxpCodexModelProviderID + `.wire_api="responses"`,
		`model_providers.` + cxpCodexModelProviderID + `.requires_openai_auth=false`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("session model profile codex args missing %q:\n%v", want, gotArgs)
		}
	}
}

func TestTeamsCodexExecutorProfileRunnerCacheIsConcurrentAndSnapshotScoped(t *testing.T) {
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
			execRunner, ok := runner.(*codexrunner.ExecRunner)
			if !ok {
				errs <- fmt.Errorf("runner type = %T, want ExecRunner", runner)
				return
			}
			launcher, ok := execRunner.Launcher.(teamsCodexLauncher)
			if !ok {
				errs <- fmt.Errorf("launcher type = %T, want teamsCodexLauncher", execRunner.Launcher)
				return
			}
			if modelProfileSnapshotKey(launcher.modelProfileSnapshot) != modelProfileSnapshotKey(snapshot) {
				errs <- fmt.Errorf("launcher snapshot = %#v, want %#v", launcher.modelProfileSnapshot, snapshot)
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

func TestNewTeamsExecutorDefaultsSessionReasoningEffortToXHigh(t *testing.T) {
	executor, err := newTeamsExecutor(&rootOptions{}, "codex", "exec", "/tmp/codex", "/work", []string{"--model", "gpt-5", "--sandbox", "workspace-write"}, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newTeamsExecutor error: %v", err)
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		t.Fatalf("executor type = %T, want teamsCodexExecutor", executor)
	}
	runner, ok := teamsExecutor.runner.(*codexrunner.ExecRunner)
	if !ok {
		t.Fatalf("runner type = %T, want ExecRunner", teamsExecutor.runner)
	}
	want := []string{"--model", "gpt-5", "-c", teams.CodexReasoningEffortConfigArg(teams.DefaultSessionReasoningEffort)}
	if !reflect.DeepEqual(runner.ExtraArgs, want) {
		t.Fatalf("session extra args = %#v, want %#v", runner.ExtraArgs, want)
	}
}

func TestNewTeamsExecutorDefaultsSessionReasoningEffortToXHighForAppServer(t *testing.T) {
	executor, err := newTeamsExecutor(&rootOptions{}, "codex", "appserver", "/tmp/codex", "/work", nil, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newTeamsExecutor error: %v", err)
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		t.Fatalf("executor type = %T, want teamsCodexExecutor", executor)
	}
	runner, ok := teamsExecutor.runner.(*codexrunner.AppServerRunner)
	if !ok {
		t.Fatalf("runner type = %T, want AppServerRunner", teamsExecutor.runner)
	}
	want := []string{"-c", teams.CodexReasoningEffortConfigArg(teams.DefaultSessionReasoningEffort)}
	if !reflect.DeepEqual(runner.ExtraArgs, want) {
		t.Fatalf("appserver session extra args = %#v, want %#v", runner.ExtraArgs, want)
	}
	fallback, ok := runner.Fallback.(*codexrunner.ExecRunner)
	if !ok {
		t.Fatalf("fallback type = %T, want ExecRunner", runner.Fallback)
	}
	if !reflect.DeepEqual(fallback.ExtraArgs, want) {
		t.Fatalf("appserver fallback extra args = %#v, want %#v", fallback.ExtraArgs, want)
	}
}

func TestNewTeamsExecutorPreservesExplicitSessionReasoningEffort(t *testing.T) {
	executor, err := newTeamsExecutor(&rootOptions{}, "codex", "exec", "/tmp/codex", "/work", []string{"-c", `model_reasoning_effort="medium"`}, "", time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newTeamsExecutor error: %v", err)
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		t.Fatalf("executor type = %T, want teamsCodexExecutor", executor)
	}
	runner, ok := teamsExecutor.runner.(*codexrunner.ExecRunner)
	if !ok {
		t.Fatalf("runner type = %T, want ExecRunner", teamsExecutor.runner)
	}
	want := []string{"-c", `model_reasoning_effort="medium"`}
	if !reflect.DeepEqual(runner.ExtraArgs, want) {
		t.Fatalf("session extra args = %#v, want %#v", runner.ExtraArgs, want)
	}
}

func TestNewTeamsControlFallbackExecutorUsesLowEffortWithoutDefaultModel(t *testing.T) {
	executor, err := newTeamsControlFallbackExecutor(&rootOptions{}, "exec", "/tmp/codex", "/work", []string{"--model", "gpt-5", "--sandbox", "workspace-write", "-c", `model_reasoning_effort="xhigh"`}, "", "", time.Minute, io.Discard)
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
	want := []string{"-c", teams.CodexReasoningEffortConfigArg(teams.DefaultControlFallbackReasoningEffort)}
	if !reflect.DeepEqual(runner.ExtraArgs, want) {
		t.Fatalf("fallback extra args = %#v, want %#v", runner.ExtraArgs, want)
	}
}

func TestNewTeamsControlFallbackExecutorUsesLowEffortForAppServer(t *testing.T) {
	executor, err := newTeamsControlFallbackExecutor(&rootOptions{}, "appserver", "/tmp/codex", "/work", []string{"--model", "gpt-5", "-c", `model_reasoning_effort="xhigh"`}, "", "", time.Minute, io.Discard)
	if err != nil {
		t.Fatalf("newTeamsControlFallbackExecutor error: %v", err)
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		t.Fatalf("executor type = %T, want teamsCodexExecutor", executor)
	}
	runner, ok := teamsExecutor.runner.(*codexrunner.AppServerRunner)
	if !ok {
		t.Fatalf("runner type = %T, want AppServerRunner", teamsExecutor.runner)
	}
	want := []string{"-c", teams.CodexReasoningEffortConfigArg(teams.DefaultControlFallbackReasoningEffort)}
	if !reflect.DeepEqual(runner.ExtraArgs, want) {
		t.Fatalf("appserver control fallback extra args = %#v, want %#v", runner.ExtraArgs, want)
	}
	fallback, ok := runner.Fallback.(*codexrunner.ExecRunner)
	if !ok {
		t.Fatalf("fallback type = %T, want ExecRunner", runner.Fallback)
	}
	if !reflect.DeepEqual(fallback.ExtraArgs, want) {
		t.Fatalf("appserver fallback extra args = %#v, want %#v", fallback.ExtraArgs, want)
	}
}

func TestNewTeamsControlFallbackExecutorHonorsExplicitFallbackModel(t *testing.T) {
	executor, err := newTeamsControlFallbackExecutor(&rootOptions{}, "exec", "/tmp/codex", "/work", nil, "", "gpt-control", time.Minute, io.Discard)
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
	want := []string{"--model", "gpt-control", "-c", teams.CodexReasoningEffortConfigArg(teams.DefaultControlFallbackReasoningEffort)}
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
	}, "", time.Minute, io.Discard)
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

func TestNewManagedTeamsCodexExecutorRejectsUnknownRunner(t *testing.T) {
	_, err := newManagedTeamsCodexExecutor(&rootOptions{}, "unknown", "", "", nil, "", 0, io.Discard)
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

func TestRunTeamsUpgradeCodexOnceSkipsIncompleteProxyPreferenceCI(t *testing.T) {
	lockCLITestHooks(t)

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

type fakeTeamsRunner struct {
	result   codexrunner.TurnResult
	err      error
	resumed  bool
	threadID string
	input    codexrunner.TurnInput
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
