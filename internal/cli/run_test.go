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

func TestRunTargetOnceWithOptionsUsesPatchInfoForSelfUpdateGuard(t *testing.T) {
	lockCLITestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	prevDetect := codexSelfUpdateDetectSource
	prevLookPath := codexSelfUpdateLookPath
	prevExecutable := codexSelfUpdateExecutable
	t.Cleanup(func() {
		codexSelfUpdateDetectSource = prevDetect
		codexSelfUpdateLookPath = prevLookPath
		codexSelfUpdateExecutable = prevExecutable
	})

	origCodexPath := "/tmp/managed/bin/codex"
	privateNpmPath := "/tmp/codex-proxy/node/v22-linux-x64/bin/npm"
	codexSelfUpdateDetectSource = func(context.Context, string, []string) (codexUpgradeSource, error) {
		return codexUpgradeSource{
			origin:    codexInstallOriginManaged,
			codexPath: origCodexPath,
			npmPrefix: "/tmp/managed",
		}, nil
	}
	codexSelfUpdateLookPath = func(string) (string, error) {
		return privateNpmPath, nil
	}
	codexSelfUpdateExecutable = func() (string, error) {
		return "/usr/bin/codex-proxy", nil
	}

	dir := t.TempDir()
	outFile := filepath.Join(dir, "guard-env.txt")
	script := filepath.Join(dir, "patched-binary")
	content := "#!/bin/sh\n" +
		"printf 'UPDATE=%s\\nREAL=%s\\n' \"$CODEX_PROXY_UPDATE_CODEX_PATH\" \"$CODEX_PROXY_REAL_NPM\" > \"$OUT_FILE\"\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	opts := runTargetOptions{
		ExtraEnv: []string{"OUT_FILE=" + outFile},
		UseProxy: false,
		PatchInfo: &patchRunInfo{
			OrigBinaryPath: origCodexPath,
		},
	}

	if err := runTargetOnceWithOptions(context.Background(), []string{script}, "", nil, nil, &bytes.Buffer{}, &bytes.Buffer{}, opts); err != nil {
		t.Fatalf("runTargetOnceWithOptions error: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read guard env file: %v", err)
	}
	want := "UPDATE=/tmp/managed/bin/codex\nREAL=/tmp/codex-proxy/node/v22-linux-x64/bin/npm\n"
	if string(got) != want {
		t.Fatalf("expected guard env %q, got %q", want, string(got))
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

func TestRunTargetWithFallbackDisablesYolo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "yolo.sh")
	content := "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = \"--yolo\" ]; then\n    echo \"unknown flag: --yolo\" >&2\n    exit 2\n  fi\ndone\nexit 0\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	disabled := false
	opts := runTargetOptions{
		UseProxy:    false,
		PreserveTTY: false,
		YoloEnabled: true,
		OnYoloFallback: func() error {
			disabled = true
			return nil
		},
	}
	cmdArgs := []string{script, "--yolo"}
	if err := runTargetWithFallbackWithOptions(context.Background(), cmdArgs, "", nil, nil, opts); err != nil {
		t.Fatalf("runTargetWithFallbackWithOptions error: %v", err)
	}
	if !disabled {
		t.Fatalf("expected yolo to be disabled on failure")
	}
}

func TestRunTargetWithFallbackPreserveTTYStillCapturesYoloError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "yolo.sh")
	content := "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = \"--yolo\" ]; then\n    echo \"unknown flag: --yolo\" >&2\n    exit 2\n  fi\ndone\nexit 0\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	disabled := false
	opts := runTargetOptions{
		UseProxy:    false,
		PreserveTTY: true,
		YoloEnabled: true,
		Stdout:      io.Discard,
		Stderr:      io.Discard,
		OnYoloFallback: func() error {
			disabled = true
			return nil
		},
	}
	cmdArgs := []string{script, "--yolo"}
	if err := runTargetWithFallbackWithOptions(context.Background(), cmdArgs, "", nil, nil, opts); err != nil {
		t.Fatalf("runTargetWithFallbackWithOptions error: %v", err)
	}
	if !disabled {
		t.Fatalf("expected yolo to be disabled on PreserveTTY stderr failure")
	}
}

func TestRunTargetWithFallbackRequiresYolo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "yolo.sh")
	content := "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = \"--yolo\" ]; then\n    echo \"unknown flag: --yolo\" >&2\n    exit 2\n  fi\ndone\nexit 0\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	disabled := false
	opts := runTargetOptions{
		UseProxy:    false,
		PreserveTTY: false,
		YoloEnabled: true,
		RequireYolo: true,
		OnYoloFallback: func() error {
			disabled = true
			return nil
		},
	}
	err := runTargetWithFallbackWithOptions(context.Background(), []string{script, "--yolo"}, "", nil, nil, opts)
	if err == nil || !strings.Contains(err.Error(), "yolo mode is required") {
		t.Fatalf("runTargetWithFallbackWithOptions error = %v, want required-yolo error", err)
	}
	if disabled {
		t.Fatal("required yolo should not call fallback disable hook")
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

	buf = &limitedBuffer{max: 5}
	_, _ = buf.Write([]byte("0123456789"))
	if got := buf.String(); got != "56789" {
		t.Fatalf("expected %q, got %q", "56789", got)
	}

	buf = &limitedBuffer{max: 0}
	_, _ = buf.Write([]byte("abc"))
	if got := buf.String(); got != "" {
		t.Fatalf("expected empty buffer, got %q", got)
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

func TestRunLikeUsesDefaultCodexCommandInDirectMode(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	disabled := false
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &disabled,
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	codexDir := t.TempDir()
	_ = writeProbeableCodex(t, codexDir, true)
	t.Setenv("PATH", codexDir+string(os.PathListSeparator)+os.Getenv("PATH"))

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
	runTargetWithFallbackWithOptionsFn = func(ctx context.Context, cmdArgs []string, proxyURL string, healthCheck func() error, fatalCh <-chan error, opts runTargetOptions) error {
		gotCmdArgs = append([]string(nil), cmdArgs...)
		if opts.UseProxy {
			t.Fatal("expected default direct command to disable proxy env injection")
		}
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	root := &rootOptions{configPath: store.Path()}
	if err := runLike(cmd, root, false); err != nil {
		t.Fatalf("runLike: %v", err)
	}

	if len(gotCmdArgs) != 1 {
		t.Fatalf("expected a single resolved default command, got %v", gotCmdArgs)
	}
	base := strings.ToLower(filepath.Base(gotCmdArgs[0]))
	if base != "codex" && base != "codex.cmd" && base != "codex.exe" {
		t.Fatalf("expected resolved default command to target codex, got %q", gotCmdArgs[0])
	}
}

func TestRunLikeYoloFlagEnablesManagedYoloInDirectMode(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(false),
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	codexDir := t.TempDir()
	writeStub(t, codexDir, "codex",
		"#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; --version) echo 'codex 0.0.0' ;; *) exit 0 ;; esac\n",
		"@echo off\r\nif \"%~1\"==\"--help\" (\r\n  echo usage codex --dangerously-bypass-approvals-and-sandbox\r\n  exit /b 0\r\n)\r\nif \"%~1\"==\"--version\" (\r\n  echo codex 0.0.0\r\n  exit /b 0\r\n)\r\nexit /b 0\r\n")
	t.Setenv("PATH", codexDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CODEX_HOME", t.TempDir())

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

	root := &rootOptions{configPath: store.Path()}
	cmd := newRunCmd(root)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--yolo"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("run command: %v", err)
	}
	if !gotOpts.YoloEnabled || !gotOpts.RequireYolo {
		t.Fatalf("expected explicit run --yolo to require yolo, got opts=%+v", gotOpts)
	}
	if gotOpts.UseProxy {
		t.Fatal("expected direct yolo run to keep proxy disabled")
	}
	if !slices.Contains(gotCmdArgs, "--dangerously-bypass-approvals-and-sandbox") {
		t.Fatalf("expected managed yolo launch arg in %v", gotCmdArgs)
	}
}

type testCloudRequirementsCacheFile struct {
	Signature     string `json:"signature"`
	SignedPayload struct {
		ChatGPTUserID string  `json:"chatgpt_user_id"`
		AccountID     string  `json:"account_id"`
		Contents      *string `json:"contents"`
	} `json:"signed_payload"`
}

func readTestCloudRequirementsCache(t *testing.T, codexDir string) testCloudRequirementsCacheFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(codexDir, "cloud-requirements-cache.json"))
	if err != nil {
		t.Fatalf("read cloud requirements cache: %v", err)
	}
	var cache testCloudRequirementsCacheFile
	if err := json.Unmarshal(data, &cache); err != nil {
		t.Fatalf("parse cloud requirements cache: %v", err)
	}
	return cache
}

func TestRunLikeYoloFlagInstallsCloudRequirementsBypassInDirectMode(t *testing.T) {
	lockCLITestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(false),
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	codexBinDir := t.TempDir()
	codexPath := filepath.Join(codexBinDir, "codex")
	script := "#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; --version) echo 'codex 0.0.0' ;; *) exit 0 ;; esac\n"
	if err := os.WriteFile(codexPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write codex stub: %v", err)
	}
	t.Setenv("PATH", codexBinDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	codexHome := t.TempDir()
	setTestCodexHomeEnv(t, codexHome)
	writeFakeCache(t, codexHome)
	originalAuth := writeTestAuthJSON(t, codexHome, true)

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
	runTargetWithFallbackWithOptionsFn = func(ctx context.Context, cmdArgs []string, proxyURL string, healthCheck func() error, fatalCh <-chan error, opts runTargetOptions) error {
		gotCmdArgs = append([]string(nil), cmdArgs...)
		if opts.UseProxy {
			t.Fatal("expected direct yolo run to keep proxy disabled")
		}
		if !cacheExists(t, codexHome) {
			t.Fatal("cloud requirements bypass cache should exist while Codex starts")
		}
		cache := readTestCloudRequirementsCache(t, codexHome)
		if cache.Signature == "" {
			t.Fatal("cloud requirements bypass cache should be signed")
		}
		if cache.SignedPayload.ChatGPTUserID != "user_test" || cache.SignedPayload.AccountID != "org_test" {
			t.Fatalf("cache identity = %#v", cache.SignedPayload)
		}
		if cache.SignedPayload.Contents != nil {
			t.Fatalf("cache contents = %#v, want nil", *cache.SignedPayload.Contents)
		}
		authDuringRun, err := os.ReadFile(filepath.Join(codexHome, "auth.json"))
		if err != nil {
			t.Fatalf("read auth during run: %v", err)
		}
		if authJSONHasPlanClaim(t, authDuringRun) {
			t.Fatal("yolo run should mask workspace plan auth before Codex starts")
		}
		return nil
	}

	root := &rootOptions{configPath: store.Path()}
	cmd := newRunCmd(root)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--yolo"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("run command: %v", err)
	}
	if !reflect.DeepEqual(gotCmdArgs, []string{codexPath, "-c", `cli_auth_credentials_store="file"`, "--dangerously-bypass-approvals-and-sandbox"}) {
		t.Fatalf("cmd args = %#v", gotCmdArgs)
	}
	if cacheExists(t, codexHome) {
		t.Fatal("cloud requirements bypass cache should be removed after Codex exits")
	}
	authAfterRun, err := os.ReadFile(filepath.Join(codexHome, "auth.json"))
	if err != nil {
		t.Fatalf("read auth after run: %v", err)
	}
	if !reflect.DeepEqual(authAfterRun, originalAuth) {
		t.Fatal("yolo run should restore auth after Codex exits")
	}
}

func TestPrepareYoloCodexCommandForRunAddsFileAuthWhenYoloAlreadyPresent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	store := newTempStore(t)
	codexBinDir := t.TempDir()
	codexPath := filepath.Join(codexBinDir, "codex")
	script := "#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; --version) echo 'codex 0.0.0' ;; *) exit 0 ;; esac\n"
	if err := os.WriteFile(codexPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write codex stub: %v", err)
	}
	codexHome := t.TempDir()
	writeTestAuthJSON(t, codexHome, true)

	opts := runTargetOptions{
		YoloEnabled: true,
		ExtraEnv:    []string{envCodexHome + "=" + codexHome},
		Log:         io.Discard,
	}
	cmdArgs, cleanup := prepareYoloCodexCommandForRun(store, []string{codexPath, "--dangerously-bypass-approvals-and-sandbox", "exec", "-"}, &opts)
	if cleanup == nil {
		t.Fatal("expected cleanup for yolo auth/cache overrides")
	}
	defer cleanup()

	wantPrefix := []string{codexPath, "-c", `cli_auth_credentials_store="file"`, "--dangerously-bypass-approvals-and-sandbox", "exec", "-"}
	if !reflect.DeepEqual(cmdArgs, wantPrefix) {
		t.Fatalf("cmd args = %#v, want %#v", cmdArgs, wantPrefix)
	}
	if !cacheExists(t, codexHome) {
		t.Fatal("cloud requirements bypass cache should be installed")
	}
}

func TestRunLikeYoloFlagFailsWhenCodexHasNoYoloLaunchFlag(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(false),
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	codexDir := t.TempDir()
	writeStub(t, codexDir, "codex",
		"#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex' ;; --version) echo 'codex 0.0.0' ;; *) exit 0 ;; esac\n",
		"@echo off\r\nif \"%~1\"==\"--help\" (\r\n  echo usage codex\r\n  exit /b 0\r\n)\r\nif \"%~1\"==\"--version\" (\r\n  echo codex 0.0.0\r\n  exit /b 0\r\n)\r\nexit /b 0\r\n")
	t.Setenv("PATH", codexDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CODEX_HOME", t.TempDir())

	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() { runTargetWithFallbackWithOptionsFn = prevRunTarget })
	runTargetWithFallbackWithOptionsFn = func(context.Context, []string, string, func() error, <-chan error, runTargetOptions) error {
		t.Fatal("run target should not start when explicit --yolo cannot be satisfied")
		return nil
	}

	root := &rootOptions{configPath: store.Path()}
	cmd := newRunCmd(root)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--yolo"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "no supported Codex yolo launch flag") {
		t.Fatalf("run command error = %v, want missing yolo flag", err)
	}
}

func TestRunLikeYoloFlagPassesOptionsThroughProxyMode(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	prevRunWithProfile := runWithProfileFn
	prevRunWithProfileOptions := runWithProfileOptionsFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	prevEnsureProxy := ensureProxyPreferenceRunFn
	prevEnsureProfile := ensureProfileRunFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runWithProfileOptionsFn = prevRunWithProfileOptions
		runTargetWithFallbackWithOptionsFn = prevRunTarget
		ensureProxyPreferenceRunFn = prevEnsureProxy
		ensureProfileRunFn = prevEnsureProfile
	})

	runWithProfileFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string) error {
		t.Fatal("runWithProfile should not be used for explicit run --yolo")
		return nil
	}
	runTargetWithFallbackWithOptionsFn = func(context.Context, []string, string, func() error, <-chan error, runTargetOptions) error {
		t.Fatal("direct runner should not be used when proxy preference is enabled")
		return nil
	}
	ensureProxyPreferenceRunFn = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return true, config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(true)}, nil
	}
	profile := config.Profile{ID: "p1", Name: "primary"}
	ensureProfileRunFn = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		return profile, config.Config{
			Version:      config.CurrentVersion,
			ProxyEnabled: boolPtr(true),
			Profiles:     []config.Profile{profile},
		}, nil
	}

	var gotCmdArgs []string
	var gotOpts runTargetOptions
	runWithProfileOptionsFn = func(ctx context.Context, store *config.Store, prof config.Profile, instances []config.Instance, cmdArgs []string, opts runTargetOptions) error {
		gotCmdArgs = append([]string(nil), cmdArgs...)
		gotOpts = opts
		return nil
	}

	root := &rootOptions{configPath: store.Path()}
	cmd := newRunCmd(root)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--yolo", "--", "codex", "exec", "-"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("run command: %v", err)
	}
	if !reflect.DeepEqual(gotCmdArgs, []string{"codex", "exec", "-"}) {
		t.Fatalf("cmd args = %v, want codex exec -", gotCmdArgs)
	}
	if !gotOpts.YoloEnabled || !gotOpts.RequireYolo {
		t.Fatalf("expected proxy run --yolo to pass required yolo opts, got %+v", gotOpts)
	}
	if !gotOpts.UseProxy {
		t.Fatal("expected proxy run to keep proxy enabled")
	}
}

func TestRunLikePropagatesCodexHomeEnv(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	disabled := false
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &disabled,
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	codexDir := t.TempDir()
	codexPath := writeProbeableCodex(t, t.TempDir(), true)
	t.Setenv("PATH", filepath.Dir(codexPath)+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CODEX_HOME", codexDir)
	t.Setenv("CODEX_DIR", "")

	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() { runTargetWithFallbackWithOptionsFn = prevRunTarget })

	var gotEnv []string
	runTargetWithFallbackWithOptionsFn = func(ctx context.Context, cmdArgs []string, proxyURL string, healthCheck func() error, fatalCh <-chan error, opts runTargetOptions) error {
		gotEnv = append([]string(nil), opts.ExtraEnv...)
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Parse([]string{}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	if err := runLike(cmd, &rootOptions{configPath: store.Path()}, false); err != nil {
		t.Fatalf("runLike: %v", err)
	}

	if !slices.Contains(gotEnv, "CODEX_HOME="+codexDir) {
		t.Fatalf("expected CODEX_HOME in env, got %v", gotEnv)
	}
	if !slices.Contains(gotEnv, "CODEX_DIR="+codexDir) {
		t.Fatalf("expected CODEX_DIR in env, got %v", gotEnv)
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

func TestRunLikeModelProfileSSHProxyPassesModelProfileOptions(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	disabled := false
	profile := config.Profile{ID: "p1", Name: "primary", Host: "example.com", User: "coder", CreatedAt: time.Now()}
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &disabled,
		Profiles:     []config.Profile{profile},
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-ssh": {
				Provider:  "deepseek",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				SSHProxy:  "primary",
				Revision:  1,
			},
		},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevRunWithProfile := runWithProfileFn
	prevRunWithProfileOptions := runWithProfileOptionsFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runWithProfileOptionsFn = prevRunWithProfileOptions
		runTargetWithFallbackWithOptionsFn = prevRunTarget
	})

	runWithProfileFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string) error {
		t.Fatal("model profile runs through an SSH proxy must preserve model-profile options")
		return nil
	}
	runTargetWithFallbackWithOptionsFn = func(context.Context, []string, string, func() error, <-chan error, runTargetOptions) error {
		t.Fatal("model profile SSH proxy run should use the proxy execution path")
		return nil
	}

	var gotProfile config.Profile
	var gotCmdArgs []string
	var gotOpts runTargetOptions
	runWithProfileOptionsFn = func(ctx context.Context, _ *config.Store, prof config.Profile, _ []config.Instance, cmdArgs []string, opts runTargetOptions) error {
		gotProfile = prof
		gotCmdArgs = append([]string(nil), cmdArgs...)
		gotOpts = opts
		return nil
	}

	root := &rootOptions{configPath: store.Path()}
	cmd := newRunCmd(root)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--model-profile", "deepseek-ssh", "--", "codex", "exec", "-"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("run command: %v", err)
	}
	if gotProfile.ID != profile.ID {
		t.Fatalf("profile = %#v, want %#v", gotProfile, profile)
	}
	if !reflect.DeepEqual(gotCmdArgs, []string{"codex", "exec", "-"}) {
		t.Fatalf("cmd args = %v", gotCmdArgs)
	}
	if gotOpts.ModelProfileRef != "deepseek-ssh" || !gotOpts.UseProxy {
		t.Fatalf("opts = %+v, want model profile and proxy enabled", gotOpts)
	}
}

func TestRunLikeDirectModelProfileSimulatesCodexLaunch(t *testing.T) {
	lockCLITestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("fake codex launch script uses Unix shell")
	}

	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-live": {
				Provider:  "deepseek",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  7,
			},
		},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}
	t.Setenv("DEEPSEEK_API_KEY", "sk-ci-deepseek")
	t.Setenv(envCodexHome, t.TempDir())

	binDir := t.TempDir()
	fakeCodex := filepath.Join(binDir, "codex")
	if err := os.WriteFile(fakeCodex, []byte("#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo codex 0.0.0; exit 0; fi\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write fake codex: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	prevRunWithProfile := runWithProfileFn
	prevRunWithProfileOptions := runWithProfileOptionsFn
	prevRunTarget := runTargetWithFallbackWithOptionsFn
	prevEnsureProxy := ensureProxyPreferenceRunFn
	t.Cleanup(func() {
		runWithProfileFn = prevRunWithProfile
		runWithProfileOptionsFn = prevRunWithProfileOptions
		runTargetWithFallbackWithOptionsFn = prevRunTarget
		ensureProxyPreferenceRunFn = prevEnsureProxy
	})

	runWithProfileFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string) error {
		t.Fatal("direct model-profile run should not use SSH profile path")
		return nil
	}
	runWithProfileOptionsFn = func(context.Context, *config.Store, config.Profile, []config.Instance, []string, runTargetOptions) error {
		t.Fatal("direct model-profile run should not use SSH profile options path")
		return nil
	}
	ensureProxyPreferenceRunFn = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		t.Fatal("direct model-profile run without an ssh proxy should not prompt for generic proxy preference")
		return false, config.Config{}, nil
	}

	var gotCmdArgs []string
	var gotOpts runTargetOptions
	runTargetWithFallbackWithOptionsFn = func(ctx context.Context, cmdArgs []string, proxyURL string, healthCheck func() error, fatalCh <-chan error, opts runTargetOptions) error {
		gotCmdArgs = append([]string(nil), cmdArgs...)
		gotOpts = opts
		if proxyURL != "" {
			t.Fatalf("proxyURL = %q, want direct mode", proxyURL)
		}
		if healthCheck != nil || fatalCh != nil {
			t.Fatalf("direct mode should not pass proxy health/fatal hooks")
		}
		baseURL := codexOverrideValue(t, cmdArgs, `model_providers.`+cxpCodexModelProviderID+`.base_url`)
		proxyKey := envValue(opts.ExtraEnv, envCXPResponsesProxyKey)
		if proxyKey == "" {
			t.Fatalf("missing %s in ExtraEnv: %v", envCXPResponsesProxyKey, opts.ExtraEnv)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(baseURL, "/")+"/models", nil)
		if err != nil {
			t.Fatalf("new /models request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+proxyKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET adapter /models: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET adapter /models status = %d", resp.StatusCode)
		}
		return nil
	}

	root := &rootOptions{configPath: store.Path()}
	cmd := newRunCmd(root)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--model-profile", "deepseek-live", "--", "codex", "exec", "-"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("run command: %v", err)
	}
	if len(gotCmdArgs) == 0 || filepath.Base(gotCmdArgs[0]) != "codex" {
		t.Fatalf("cmd args = %v, want resolved codex command", gotCmdArgs)
	}
	joined := strings.Join(gotCmdArgs, "\n")
	for _, want := range []string{
		`model_provider="` + cxpCodexModelProviderID + `"`,
		`model="deepseek/deepseek-v4-flash"`,
		`model_catalog_json="`,
		`model_providers.` + cxpCodexModelProviderID + `.wire_api="responses"`,
		`model_providers.` + cxpCodexModelProviderID + `.requires_openai_auth=false`,
		"exec\n-",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("codex args missing %q:\n%v", want, gotCmdArgs)
		}
	}
	if gotOpts.UseProxy || gotOpts.ModelProfileRef != "deepseek-live" {
		t.Fatalf("opts = %+v, want direct model profile run", gotOpts)
	}
	if got := envValue(gotOpts.ExtraEnv, envCodexHome); got == "" {
		t.Fatalf("missing %s in ExtraEnv: %v", envCodexHome, gotOpts.ExtraEnv)
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
