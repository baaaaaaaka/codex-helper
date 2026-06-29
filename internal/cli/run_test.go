package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
	"github.com/spf13/cobra"
)

func TestSelectProfile(t *testing.T) {
	cfg := config.Config{
		Profiles: []config.Profile{
			{ID: "one", Name: "first"},
			{ID: "two", Name: "second"},
		},
	}

	if _, err := selectProfile(cfg, "one"); err != nil {
		t.Fatalf("expected profile by ID, got error %v", err)
	}
	if _, err := selectProfile(cfg, "second"); err != nil {
		t.Fatalf("expected profile by name, got error %v", err)
	}
	if _, err := selectProfile(cfg, "missing"); err == nil {
		t.Fatalf("expected missing profile error")
	}
	if _, err := selectProfile(cfg, ""); err == nil {
		t.Fatalf("expected error when multiple profiles exist without ref")
	}
}

func TestSelectProfileSingleDefault(t *testing.T) {
	cfg := config.Config{
		Profiles: []config.Profile{
			{ID: "only", Name: "only-profile"},
		},
	}
	p, err := selectProfile(cfg, "")
	if err != nil {
		t.Fatalf("expected single profile to be selected by default, got error %v", err)
	}
	if p.ID != "only" {
		t.Fatalf("expected profile ID %q, got %q", "only", p.ID)
	}
}

func TestRunTargetSupervisedSuccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "ok.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	if err := runTargetSupervised(context.Background(), []string{script}, "", nil, nil); err != nil {
		t.Fatalf("runTargetSupervised error: %v", err)
	}
}

func TestRunTargetOnceWithOptionsNoProxyKeepsEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	outFile := filepath.Join(dir, "env.txt")
	script := filepath.Join(dir, "print.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf \"%s\" \"$HTTP_PROXY\" > \"$OUT_FILE\"\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	t.Setenv("HTTP_PROXY", "http://example.com")
	opts := runTargetOptions{
		ExtraEnv: []string{"OUT_FILE=" + outFile},
		UseProxy: false,
	}

	if err := runTargetOnceWithOptions(context.Background(), []string{script}, "http://127.0.0.1:9999", nil, nil, &bytes.Buffer{}, &bytes.Buffer{}, opts); err != nil {
		t.Fatalf("runTargetOnceWithOptions error: %v", err)
	}
	content, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read env file: %v", err)
	}
	if got := string(content); got != "http://example.com" {
		t.Fatalf("expected HTTP_PROXY preserved, got %q", got)
	}
}

func TestTerminateProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip process signal test on windows")
	}
	cmd := exec.Command("sleep", "5")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	if err := terminateProcess(cmd.Process, 100*time.Millisecond); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("terminateProcess error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected process to exit")
	}
}

func TestRunTargetCancelTerminatesChildProcessGroup(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("skip process group signal test on windows")
	}
	dir := t.TempDir()
	childFile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "spawn-child.sh")
	content := "#!/bin/sh\n" +
		"sleep 30 &\n" +
		"echo $! > \"$CHILD_FILE\"\n" +
		"wait\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runTargetOnceWithOptions(ctx, []string{script}, "", nil, nil, &bytes.Buffer{}, &bytes.Buffer{}, runTargetOptions{
			UseProxy: false,
			ExtraEnv: []string{"CHILD_FILE=" + childFile},
		})
	}()

	var childPID int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(childFile); err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if parseErr != nil {
				t.Fatalf("parse child pid: %v", parseErr)
			}
			childPID = pid
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if childPID <= 0 {
		cancel()
		t.Fatal("child pid was not written")
	}
	if !proc.IsAlive(childPID) {
		cancel()
		t.Fatalf("child process %d exited before cancellation", childPID)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runTargetOnceWithOptions error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runTargetOnceWithOptions did not return after cancellation")
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !proc.IsAlive(childPID) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("child process %d survived target cancellation", childPID)
}

func TestRunTargetHealthFailureTerminatesChildProcessGroup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip process group signal test on windows")
	}
	oldInterval := runTargetHealthCheckInterval
	runTargetHealthCheckInterval = 20 * time.Millisecond
	t.Cleanup(func() { runTargetHealthCheckInterval = oldInterval })

	dir := t.TempDir()
	childFile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "spawn-child.sh")
	content := "#!/bin/sh\n" +
		"sleep 30 &\n" +
		"echo $! > \"$CHILD_FILE\"\n" +
		"wait\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- runTargetOnceWithOptions(context.Background(), []string{script}, "", func() error {
			if _, err := os.Stat(childFile); err != nil {
				return nil
			}
			return errors.New("synthetic health failure")
		}, nil, &bytes.Buffer{}, &bytes.Buffer{}, runTargetOptions{
			UseProxy: false,
			ExtraEnv: []string{"CHILD_FILE=" + childFile},
		})
	}()

	var childPID int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(childFile); err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if parseErr != nil {
				t.Fatalf("parse child pid: %v", parseErr)
			}
			childPID = pid
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if childPID <= 0 {
		t.Fatal("child pid was not written")
	}

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "proxy unhealthy") {
			t.Fatalf("runTargetOnceWithOptions error = %v, want proxy unhealthy", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runTargetOnceWithOptions did not return after repeated health failures")
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !proc.IsAlive(childPID) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("child process %d survived health-failure target termination", childPID)
}

func TestRunTargetHealthSuccessResetsFailureCount(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based signal test on windows")
	}
	oldInterval := runTargetHealthCheckInterval
	runTargetHealthCheckInterval = 20 * time.Millisecond
	t.Cleanup(func() { runTargetHealthCheckInterval = oldInterval })

	script := filepath.Join(t.TempDir(), "long-running.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := 0
	healthCheck := func() error {
		checks++
		if checks >= 5 {
			cancel()
		}
		switch checks {
		case 1, 2, 4, 5:
			return errors.New("synthetic intermittent health failure")
		default:
			return nil
		}
	}

	err := runTargetOnceWithOptions(ctx, []string{script}, "", healthCheck, nil, &bytes.Buffer{}, &bytes.Buffer{}, runTargetOptions{
		UseProxy: false,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runTargetOnceWithOptions error = %v, want context.Canceled after health reset", err)
	}
}

func TestIsTerminalFileRejectsCharacterDeviceThatIsNotTTY(t *testing.T) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	defer f.Close()
	if isTerminalFile(f) {
		t.Fatalf("%s is a character device but should not be treated as a TTY", os.DevNull)
	}
}

func TestCodexExecutionContextForRunRequiresRunnableIdentityForForeignHome(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	if runtime.GOOS == "windows" {
		t.Skip("windows does not enforce exec identity requirements")
	}

	currentHome := t.TempDir()
	candidateHome := t.TempDir()
	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, candidateHome)
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsExecIdentityHome = func(string) (*execIdentity, error) { return nil, nil }

	envVars, identity, err := codexExecutionContextForRun("")
	if err == nil {
		t.Fatal("expected error when foreign home has no runnable identity")
	}
	var targetErr *execIdentityRequired
	if !errors.As(err, &targetErr) {
		t.Fatalf("expected execIdentityRequired, got %T %v", err, err)
	}
	if envVars != nil {
		t.Fatalf("expected env to be nil on error, got %v", envVars)
	}
	if identity != nil {
		t.Fatalf("expected identity to be nil on error, got %+v", identity)
	}
}

func TestCodexExecutionContextForRunUsesResolvedForeignHomeAndExecIdentity(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)

	currentHome := t.TempDir()
	candidateHome := t.TempDir()
	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, candidateHome)
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsExecIdentityHome = func(home string) (*execIdentity, error) {
		if home != candidateHome {
			t.Fatalf("unexpected identity lookup home: %q", home)
		}
		return &execIdentity{
			UID:         1000,
			GID:         1001,
			Groups:      []uint32{1002},
			GroupsKnown: true,
			Username:    "alice",
			Home:        candidateHome,
		}, nil
	}

	envVars, identity, err := codexExecutionContextForRun("")
	if err != nil {
		t.Fatalf("codexExecutionContextForRun: %v", err)
	}
	wantHome := filepath.Join(candidateHome, ".codex")
	if !slices.Contains(envVars, codexhistory.EnvCodexDir+"="+wantHome) {
		t.Fatalf("expected %s env for %q, got %v", codexhistory.EnvCodexDir, wantHome, envVars)
	}
	if !slices.Contains(envVars, envCodexHome+"="+wantHome) {
		t.Fatalf("expected %s env for %q, got %v", envCodexHome, wantHome, envVars)
	}
	if identity == nil {
		t.Fatal("expected exec identity")
	}
	if identity.UID != 1000 || identity.GID != 1001 || !identity.GroupsKnown {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func TestLimitedBufferWrite(t *testing.T) {
	buf := &limitedBuffer{max: 5}
	if _, err := buf.Write([]byte("abc")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	if got := buf.String(); got != "abc" {
		t.Fatalf("expected %q, got %q", "abc", got)
	}
	if _, err := buf.Write([]byte("def")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	if got := buf.String(); got != "bcdef" {
		t.Fatalf("expected %q, got %q", "bcdef", got)
	}
	if !buf.Truncated() {
		t.Fatal("expected buffer to report truncation after overflow")
	}
	if got := string(buf.Bytes()); got != "bcdef" {
		t.Fatalf("Bytes() = %q, want %q", got, "bcdef")
	}

	buf = &limitedBuffer{max: 5}
	_, _ = buf.Write([]byte("0123456789"))
	if got := buf.String(); got != "56789" {
		t.Fatalf("expected %q, got %q", "56789", got)
	}
	if !buf.Truncated() {
		t.Fatal("expected buffer to report truncation after oversized write")
	}

	buf = &limitedBuffer{max: 0}
	_, _ = buf.Write([]byte("abc"))
	if got := buf.String(); got != "" {
		t.Fatalf("expected empty buffer, got %q", got)
	}
	if !buf.Truncated() {
		t.Fatal("expected zero-size buffer to report truncation after write")
	}
}

func TestRunLikeRejectsMultipleProfiles(t *testing.T) {
	cmd := &cobra.Command{}
	if err := cmd.Flags().Parse([]string{"a", "b"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	root := &rootOptions{}
	if err := runLike(cmd, root, false); err == nil {
		t.Fatalf("expected error for multiple profile args")
	}
}

func TestRunLikeUsesDirectModeWhenProxyDisabled(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	disabled := false
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &disabled,
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevRunWithProfile := runWithProfileFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runTargetWithFallbackWithOptionsFn = prevRunTarget
	})

	runWithProfileFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string) error {
		t.Fatal("runWithProfile should not be called when proxy preference is disabled")
		return nil
	}

	var gotCmdArgs []string
	var gotOpts runTargetOptions
	runTargetWithFallbackWithOptionsFn = func(ctx context.Context, cmdArgs []string, proxyURL string, healthCheck func() error, fatalCh <-chan error, opts runTargetOptions) error {
		gotCmdArgs = append([]string(nil), cmdArgs...)
		gotOpts = opts
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{"--", "echo", "ok"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	root := &rootOptions{configPath: store.Path()}
	if err := runLike(cmd, root, false); err != nil {
		t.Fatalf("runLike: %v", err)
	}

	if gotOpts.UseProxy {
		t.Fatal("expected direct mode to disable proxy env injection")
	}
	wantCmdArgs := []string{"echo", "ok"}
	if !reflect.DeepEqual(gotCmdArgs, wantCmdArgs) {
		t.Fatalf("expected direct command %v, got %v", wantCmdArgs, gotCmdArgs)
	}
}

func TestRunLikeAfterProxyResetPromptsForProxyPreference(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	enabled := true
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &enabled,
		Profiles:     []config.Profile{{ID: "p1", Name: "dev"}},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}
	resetCmd := newProxyResetCmd(&rootOptions{configPath: store.Path()})
	resetCmd.SetOut(io.Discard)
	resetCmd.SetArgs([]string{})
	if err := resetCmd.Execute(); err != nil {
		t.Fatalf("proxy reset: %v", err)
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = prevStdin
		_ = reader.Close()
	})
	if _, err := writer.Write([]byte("n\n")); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	_ = writer.Close()

	prevRunWithProfile := runWithProfileFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runTargetWithFallbackWithOptionsFn = prevRunTarget
	})

	runWithProfileFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string) error {
		t.Fatal("runWithProfile should not be called when reset state is answered with direct mode")
		return nil
	}

	var gotCmdArgs []string
	var gotOpts runTargetOptions
	runTargetWithFallbackWithOptionsFn = func(ctx context.Context, cmdArgs []string, proxyURL string, healthCheck func() error, fatalCh <-chan error, opts runTargetOptions) error {
		gotCmdArgs = append([]string(nil), cmdArgs...)
		gotOpts = opts
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{"--", "echo", "ok"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	root := &rootOptions{configPath: store.Path()}
	if err := runLike(cmd, root, false); err != nil {
		t.Fatalf("runLike: %v", err)
	}

	if gotOpts.UseProxy {
		t.Fatal("expected direct mode after answering reset prompt with no")
	}
	wantCmdArgs := []string{"echo", "ok"}
	if !reflect.DeepEqual(gotCmdArgs, wantCmdArgs) {
		t.Fatalf("expected direct command %v, got %v", wantCmdArgs, gotCmdArgs)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ProxyEnabled == nil || *cfg.ProxyEnabled {
		t.Fatalf("expected answer 'no' to persist ProxyEnabled=false, got %v", cfg.ProxyEnabled)
	}
}

func TestRunLikeUsesProxyPreferenceWhenEnabled(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	enabled := true
	profile := config.Profile{ID: "p1", Name: "primary", Host: "example.com", User: "coder", CreatedAt: time.Now()}
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &enabled,
		Profiles:     []config.Profile{profile},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevRunWithProfile := runWithProfileFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runTargetWithFallbackWithOptionsFn = prevRunTarget
	})

	runTargetWithFallbackWithOptionsFn = func(context.Context, []string, string, func() error, <-chan error, runTargetOptions) error {
		t.Fatal("direct runner should not be used when proxy preference is enabled")
		return nil
	}

	var gotProfile config.Profile
	var gotCmdArgs []string
	runWithProfileFn = func(ctx context.Context, _ *config.Store, prof config.Profile, _ []config.Instance, cmdArgs []string) error {
		gotProfile = prof
		gotCmdArgs = append([]string(nil), cmdArgs...)
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{"--", "echo", "ok"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	root := &rootOptions{configPath: store.Path()}
	if err := runLike(cmd, root, false); err != nil {
		t.Fatalf("runLike: %v", err)
	}

	if gotProfile.ID != profile.ID {
		t.Fatalf("expected profile %q, got %q", profile.ID, gotProfile.ID)
	}
	wantCmdArgs := []string{"echo", "ok"}
	if !reflect.DeepEqual(gotCmdArgs, wantCmdArgs) {
		t.Fatalf("expected proxy command %v, got %v", wantCmdArgs, gotCmdArgs)
	}
}

func TestRunLikeUsesDefaultCodexCommandInDirectMode(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	disabled := false
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &disabled}); err != nil {
		t.Fatal(err)
	}

	previous := runCodexCLIInvocationFn
	t.Cleanup(func() { runCodexCLIInvocationFn = previous })
	called := false
	runCodexCLIInvocationFn = func(_ context.Context, _ *rootOptions, gotStore *config.Store, profile *config.Profile, _ []config.Instance, args []string, useProxy bool, opts runTargetOptions) error {
		called = true
		if gotStore.Path() != store.Path() || profile != nil || useProxy || opts.UseProxy {
			t.Fatalf("direct broker dispatch store=%q profile=%#v useProxy=%v opts.UseProxy=%v", gotStore.Path(), profile, useProxy, opts.UseProxy)
		}
		if !reflect.DeepEqual(args, []string{"codex"}) {
			t.Fatalf("direct broker args = %#v", args)
		}
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse(nil); err != nil {
		t.Fatal(err)
	}
	if err := runLike(cmd, &rootOptions{configPath: store.Path()}, false); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("default Codex command did not use the standard broker dispatch")
	}
}

func TestRunLikeCodexWithProxyUsesStandardBrokerInsteadOfLegacyRunner(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	enabled := true
	profile := config.Profile{ID: "p1", Name: "primary", Host: "example.com", User: "coder", CreatedAt: time.Now()}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &enabled, Profiles: []config.Profile{profile}}); err != nil {
		t.Fatal(err)
	}

	previousBroker := runCodexCLIInvocationFn
	previousLegacy := runWithProfileFn
	t.Cleanup(func() {
		runCodexCLIInvocationFn = previousBroker
		runWithProfileFn = previousLegacy
	})
	runWithProfileFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string) error {
		t.Fatal("Codex launch used the legacy generic profile runner")
		return nil
	}
	called := false
	runCodexCLIInvocationFn = func(_ context.Context, _ *rootOptions, _ *config.Store, gotProfile *config.Profile, _ []config.Instance, args []string, useProxy bool, _ runTargetOptions) error {
		called = true
		if gotProfile == nil || gotProfile.ID != profile.ID || !useProxy {
			t.Fatalf("proxy broker dispatch profile=%#v useProxy=%v", gotProfile, useProxy)
		}
		if !reflect.DeepEqual(args, []string{"codex", "exec", "hello"}) {
			t.Fatalf("proxy broker args = %#v", args)
		}
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{"--", "codex", "exec", "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := runLike(cmd, &rootOptions{configPath: store.Path()}, false); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("proxy Codex command did not use the standard broker dispatch")
	}
}

func TestRunLikeExplicitProfileForcesProxy(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	disabled := false
	profile := config.Profile{ID: "p1", Name: "primary", Host: "example.com", User: "coder", CreatedAt: time.Now()}
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &disabled,
		Profiles:     []config.Profile{profile},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevRunWithProfile := runWithProfileFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runTargetWithFallbackWithOptionsFn = prevRunTarget
	})

	runTargetWithFallbackWithOptionsFn = func(context.Context, []string, string, func() error, <-chan error, runTargetOptions) error {
		t.Fatal("explicit profile should keep the proxy execution path")
		return nil
	}

	var gotProfile config.Profile
	runWithProfileFn = func(ctx context.Context, _ *config.Store, prof config.Profile, _ []config.Instance, _ []string) error {
		gotProfile = prof
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{"primary", "--", "echo", "ok"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	root := &rootOptions{configPath: store.Path()}
	if err := runLike(cmd, root, false); err != nil {
		t.Fatalf("runLike: %v", err)
	}

	if gotProfile.ID != profile.ID {
		t.Fatalf("expected explicit profile %q, got %q", profile.ID, gotProfile.ID)
	}

	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled == nil || *updated.ProxyEnabled {
		t.Fatalf("expected explicit profile run to preserve ProxyEnabled=false, got %v", updated.ProxyEnabled)
	}
}

func TestRunLikeRejectsModelProfileForNonCodexCommand(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatalf("save config: %v", err)
	}
	root := &rootOptions{configPath: store.Path()}
	cmd := newRunCmd(root)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--model-profile", "mimo25", "--", "bash", "-lc", "true"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--model-profile only applies to Codex launches") {
		t.Fatalf("run command error = %v, want model-profile non-Codex rejection", err)
	}
}

func TestRunLikeRejectsAAAForNonCodexCommand(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	cmd := newRunCmd(&rootOptions{configPath: store.Path()})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--aaa", "--", "bash", "-lc", "true"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--aaa only applies to Codex launches") {
		t.Fatalf("run command error = %v, want AAA non-Codex rejection", err)
	}
}

func TestRunLikeAAAIsPerInvocationAndReachesOnlyCodexBroker(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	disabled := false
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &disabled}); err != nil {
		t.Fatal(err)
	}
	previous := runCodexCLIInvocationFn
	t.Cleanup(func() { runCodexCLIInvocationFn = previous })
	called := false
	runCodexCLIInvocationFn = func(_ context.Context, _ *rootOptions, _ *config.Store, _ *config.Profile, _ []config.Instance, args []string, _ bool, opts runTargetOptions) error {
		called = true
		if !opts.AgentAutoApprove {
			t.Fatal("AAA flag did not reach the Codex broker options")
		}
		if !reflect.DeepEqual(args, []string{"codex", "exec", "hello"}) {
			t.Fatalf("AAA leaked into Codex args: %#v", args)
		}
		return nil
	}
	cmd := newRunCmd(&rootOptions{configPath: store.Path()})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--aaa", "--", "codex", "exec", "hello"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("Codex broker was not called")
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AgentAutoApproveEnabled != nil {
		t.Fatalf("per-invocation --aaa persisted a preference: %#v", cfg.AgentAutoApproveEnabled)
	}
}

func codexOverrideValue(t *testing.T, args []string, key string) string {
	t.Helper()
	prefix := key + "="
	for i := 0; i+1 < len(args); i++ {
		if args[i] != "-c" {
			continue
		}
		value, ok := strings.CutPrefix(args[i+1], prefix)
		if !ok {
			continue
		}
		return strings.Trim(value, `"`)
	}
	t.Fatalf("missing codex override %q in args: %v", key, args)
	return ""
}

func TestRunLikePersistsProxyEnabledAfterProfileSetup(t *testing.T) {
	lockCLITestHooks(t)

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	prevRunWithProfile := runWithProfileFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	prevEnsureProxy := ensureProxyPreferenceRunFn
	prevEnsureProfile := ensureProfileRunFn
	prevPersist := persistProxyPreferenceRunFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runTargetWithFallbackWithOptionsFn = prevRunTarget
		ensureProxyPreferenceRunFn = prevEnsureProxy
		ensureProfileRunFn = prevEnsureProfile
		persistProxyPreferenceRunFn = prevPersist
	})

	runTargetWithFallbackWithOptionsFn = func(context.Context, []string, string, func() error, <-chan error, runTargetOptions) error {
		t.Fatal("direct runner should not be used when proxy preference is enabled")
		return nil
	}

	ensureProxyPreferenceRunFn = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return true, config.Config{Version: config.CurrentVersion}, nil
	}

	profile := config.Profile{ID: "p1", Name: "primary"}
	ensureProfileRunFn = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		return profile, config.Config{
			Version:  config.CurrentVersion,
			Profiles: []config.Profile{profile},
		}, nil
	}

	persistCalls := 0
	persistProxyPreferenceRunFn = func(s *config.Store, enabled bool) error {
		persistCalls++
		if !enabled {
			t.Fatalf("expected persist true after profile setup")
		}
		return persistProxyPreference(s, enabled)
	}

	runWithProfileFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string) error {
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{"--", "echo", "ok"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	root := &rootOptions{configPath: cfgPath}
	if err := runLike(cmd, root, true); err != nil {
		t.Fatalf("runLike: %v", err)
	}

	if persistCalls != 1 {
		t.Fatalf("expected 1 persist call, got %d", persistCalls)
	}

	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled == nil || !*updated.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=true persisted, got %v", updated.ProxyEnabled)
	}
}

func TestRunLikeExplicitProfilePersistsProxyEnabledAfterProfileSetup(t *testing.T) {
	lockCLITestHooks(t)

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	prevRunWithProfile := runWithProfileFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	prevEnsureProfile := ensureProfileRunFn
	prevPersist := persistProxyPreferenceRunFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runTargetWithFallbackWithOptionsFn = prevRunTarget
		ensureProfileRunFn = prevEnsureProfile
		persistProxyPreferenceRunFn = prevPersist
	})

	runTargetWithFallbackWithOptionsFn = func(context.Context, []string, string, func() error, <-chan error, runTargetOptions) error {
		t.Fatal("direct runner should not be used for explicit profile runs")
		return nil
	}

	profile := config.Profile{ID: "p1", Name: "primary"}
	ensureProfileRunFn = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		return profile, config.Config{
			Version:  config.CurrentVersion,
			Profiles: []config.Profile{profile},
		}, nil
	}

	persistCalls := 0
	persistProxyPreferenceRunFn = func(s *config.Store, enabled bool) error {
		persistCalls++
		if !enabled {
			t.Fatalf("expected persist true after explicit profile setup")
		}
		return persistProxyPreference(s, enabled)
	}

	runWithProfileFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string) error {
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{"primary", "--", "echo", "ok"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	root := &rootOptions{configPath: cfgPath}
	if err := runLike(cmd, root, true); err != nil {
		t.Fatalf("runLike: %v", err)
	}

	if persistCalls != 1 {
		t.Fatalf("expected 1 persist call, got %d", persistCalls)
	}

	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled == nil || !*updated.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=true persisted, got %v", updated.ProxyEnabled)
	}
}

// startHealthServer starts an HTTP server that responds to the codex-proxy
// health endpoint for the given instanceID. Returns the port and a cleanup function.
func startHealthServer(t *testing.T, instanceID string) int {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_codex_proxy/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"instanceId": instanceID,
		})
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().(*net.TCPAddr).Port
}

func TestRunWithProfileOptionsUsesSnapshotFirst(t *testing.T) {
	lockCLITestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	instanceID := "test-inst-snapshot"
	httpPort := startHealthServer(t, instanceID)

	// Fresh store with NO instances on disk.
	store := newTempStore(t)

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		t.Fatal("stackStart should not be called when snapshot already has instance")
		return nil, nil
	}

	dir := t.TempDir()
	script := filepath.Join(dir, "ok.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	now := time.Now()
	instances := []config.Instance{{
		ID:         instanceID,
		ProfileID:  "prof-1",
		Kind:       config.InstanceKindDaemon,
		HTTPPort:   httpPort,
		SocksPort:  0,
		DaemonPID:  os.Getpid(),
		StartedAt:  now,
		LastSeenAt: now,
	}}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		instances,
		[]string{script},
		defaultRunTargetOptions(),
	)
	if err != nil {
		t.Fatalf("runWithProfileOptions error: %v", err)
	}
}

func TestRunWithProfileOptionsCreatesNewStack(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	sentinel := errors.New("mock: stackStart called")
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	// Both snapshot and disk are empty → must fall through to new stack.
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		nil,
		[]string{"true"},
		defaultRunTargetOptions(),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel from stackStart, got: %v", err)
	}
}

func TestRunWithProfileOptionsLoadErrorFallsThrough(t *testing.T) {
	lockCLITestHooks(t)
	// Create a store backed by corrupt JSON so store.Load() fails.
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte("not valid json"), 0o600); err != nil {
		t.Fatalf("write corrupt config: %v", err)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	sentinel := errors.New("mock: stackStart after load error")
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	// Snapshot empty, store.Load fails → should still fall through to new stack.
	err = runWithProfileOptions(
		context.Background(),
		store,
		profile,
		nil,
		[]string{"true"},
		defaultRunTargetOptions(),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel from stackStart, got: %v", err)
	}
}

func TestRunWithProfileOptionsSkipsWrongProfile(t *testing.T) {
	lockCLITestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	// Health server for a different profile's instance.
	instanceID := "inst-wrong-prof"
	httpPort := startHealthServer(t, instanceID)

	store := newTempStore(t)
	now := time.Now()
	// Write instance with profileID "other" to disk.
	if err := store.Update(func(cfg *config.Config) error {
		cfg.UpsertInstance(config.Instance{
			ID:         instanceID,
			ProfileID:  "other",
			Kind:       config.InstanceKindDaemon,
			HTTPPort:   httpPort,
			SocksPort:  0,
			DaemonPID:  os.Getpid(),
			StartedAt:  now,
			LastSeenAt: now,
		})
		return nil
	}); err != nil {
		t.Fatalf("record instance: %v", err)
	}

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	sentinel := errors.New("mock: stackStart for correct profile")
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	// Request profile "prof-1" — neither snapshot nor disk has a match.
	profile := config.Profile{ID: "prof-1", Name: "test"}
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		nil,
		[]string{"true"},
		defaultRunTargetOptions(),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel (wrong profile should not match), got: %v", err)
	}
}

func TestRunWithProfileOptionsRefreshesInstances(t *testing.T) {
	lockCLITestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	instanceID := "test-inst-refresh"
	httpPort := startHealthServer(t, instanceID)

	// Create a store and record the instance on disk.
	store := newTempStore(t)
	now := time.Now()
	inst := config.Instance{
		ID:         instanceID,
		ProfileID:  "prof-1",
		Kind:       config.InstanceKindDaemon,
		HTTPPort:   httpPort,
		SocksPort:  0,
		DaemonPID:  os.Getpid(),
		StartedAt:  now,
		LastSeenAt: now,
	}
	if err := store.Update(func(cfg *config.Config) error {
		cfg.UpsertInstance(inst)
		return nil
	}); err != nil {
		t.Fatalf("record instance: %v", err)
	}

	// Override stackStart so we can detect if it's called (it shouldn't be).
	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		t.Fatal("stackStart should not be called when refresh finds an instance")
		return nil, nil
	}

	// Create a simple script that exits 0.
	dir := t.TempDir()
	script := filepath.Join(dir, "ok.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	// Pass nil instances (simulating a stale/empty snapshot).
	// runWithProfileOptions should reload from disk and find the instance.
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		nil,
		[]string{script},
		defaultRunTargetOptions(),
	)
	if err != nil {
		t.Fatalf("runWithProfileOptions error: %v", err)
	}
}

func TestRunWithProfileOptionsSkipsLegacyInstance(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	sentinel := errors.New("mock: stackStart for legacy instance")
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	now := time.Now()
	instances := []config.Instance{{
		ID:         "legacy-inst",
		ProfileID:  "prof-1",
		HTTPPort:   18080,
		SocksPort:  0,
		DaemonPID:  os.Getpid(),
		StartedAt:  now,
		LastSeenAt: now,
	}}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		instances,
		[]string{"true"},
		defaultRunTargetOptions(),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel from stackStart, got: %v", err)
	}
}
