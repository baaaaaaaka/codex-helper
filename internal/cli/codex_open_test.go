package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestRunCodexNewSessionUsesOriginalTUIAndPolicyAppServer(t *testing.T) {
	fixture := writeCodexTUIBrokerFixture(t)
	store := newCodexOpenTestStore(t)
	before, err := hashFileSHA256(fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := runCodexNewSession(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil, fixture.workDir, fixture.path, "", false, io.Discard); err != nil {
		t.Fatalf("runCodexNewSession: %v", err)
	}
	after, err := hashFileSHA256(fixture.path)
	if err != nil || after != before {
		t.Fatalf("original Codex fixture changed: before=%s after=%s err=%v", before, after, err)
	}
	assertStandardBrokerLaunch(t, fixture)
}

func TestRunCodexSessionPreservesResumeExperience(t *testing.T) {
	fixture := writeCodexTUIBrokerFixture(t)
	store := newCodexOpenTestStore(t)
	session := codexhistory.Session{SessionID: "session-existing", ProjectPath: fixture.workDir}
	if err := runCodexSession(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil, session, codexhistory.Project{Path: fixture.workDir}, fixture.path, "", false, io.Discard); err != nil {
		t.Fatalf("runCodexSession: %v", err)
	}
	tuiArgs := readArgLines(t, fixture.tuiArgs)
	if len(tuiArgs) != 8 || tuiArgs[0] != "-c" || tuiArgs[1] != codexRemoteTUIFeatureConfig || tuiArgs[2] != "--remote" || !strings.HasPrefix(tuiArgs[3], "ws://127.0.0.1:") || strings.Contains(strings.TrimPrefix(tuiArgs[3], "ws://"), "/") || tuiArgs[4] != "--remote-auth-token-env" || tuiArgs[5] != codexrunner.RemoteBrokerAuthTokenEnv || tuiArgs[6] != "resume" || tuiArgs[7] != "session-existing" {
		t.Fatalf("TUI args = %#v", tuiArgs)
	}
	assertBrokerCapabilityToken(t, fixture)
}

func TestNormalizeWorkingDirRejectsMissingDirectory(t *testing.T) {
	if _, err := normalizeWorkingDir(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("normalizeWorkingDir accepted a missing directory")
	}
}

func TestNormalizeWorkingDirRejectsRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := normalizeWorkingDir(path); err == nil {
		t.Fatal("normalizeWorkingDir accepted a regular file")
	}
}

func TestNormalizeWorkingDirResolvesRelativeDirectory(t *testing.T) {
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(workingDir, ".normalize-working-dir-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	want := filepath.Join(root, "work")
	if err := os.Mkdir(want, 0o700); err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(workingDir, want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := normalizeWorkingDir(relative)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("normalizeWorkingDir = %q, want %q", got, want)
	}
}

type codexTUIBrokerFixture struct {
	path          string
	workDir       string
	tuiArgs       string
	tuiAuthToken  string
	tuiSQLiteHome string
	appServerArgs string
	appSQLiteHome string
}

func writeCodexTUIBrokerFixture(t *testing.T) codexTUIBrokerFixture {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX app-server fixture")
	}
	dir := t.TempDir()
	workDir := filepath.Join(dir, "work")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "codex")
	tuiArgs := filepath.Join(dir, "tui.args")
	tuiAuthToken := filepath.Join(dir, "tui.auth-token")
	tuiSQLiteHome := filepath.Join(dir, "tui.sqlite-home")
	appServerArgs := filepath.Join(dir, "app-server.args")
	appSQLiteHome := filepath.Join(dir, "app-server.sqlite-home")
	t.Setenv(codexrunner.RemoteBrokerAuthTokenEnv, "poisoned-inherited-token")
	script := fmt.Sprintf(`#!/bin/sh
set -eu
case "${1:-}" in
  --version)
    echo 'codex-cli 0.133.0'
    exit 0
    ;;
  --help)
    echo 'Options: --remote <ADDR> --remote-auth-token-env <ENV_VAR>'
    exit 0
    ;;
  app-server)
    printf '%%s\n' "$@" > %s
    printf '%%s\n' "${%s:-}" > %s
    while IFS= read -r line; do
      id=$(printf %%s "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
      case "$line" in
        *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{}}\n' "$id" ;;
        *'"method":"thread/list"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{"data":[]}}\n' "$id" ;;
      esac
    done
    exit 0
    ;;
  *)
    printf '%%s\n' "$@" > %s
    printf '%%s\n' "${%s:-}" > %s
    printf '%%s\n' "${%s:-}" > %s
    exit 0
    ;;
esac
`, shellSingleQuoteForBeaconCLITest(appServerArgs), envCodexSQLiteHome, shellSingleQuoteForBeaconCLITest(appSQLiteHome), shellSingleQuoteForBeaconCLITest(tuiArgs), codexrunner.RemoteBrokerAuthTokenEnv, shellSingleQuoteForBeaconCLITest(tuiAuthToken), envCodexSQLiteHome, shellSingleQuoteForBeaconCLITest(tuiSQLiteHome))
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return codexTUIBrokerFixture{path: path, workDir: workDir, tuiArgs: tuiArgs, tuiAuthToken: tuiAuthToken, tuiSQLiteHome: tuiSQLiteHome, appServerArgs: appServerArgs, appSQLiteHome: appSQLiteHome}
}

func newCodexOpenTestStore(t *testing.T) *config.Store {
	t.Helper()
	root := t.TempDir()
	setTestCodexHomeEnv(t, filepath.Join(root, "codex-home"))
	store, err := config.NewStore(filepath.Join(root, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, RuntimeGeneration: currentRuntimeGeneration}); err != nil {
		t.Fatal(err)
	}
	return store
}

func assertStandardBrokerLaunch(t *testing.T, fixture codexTUIBrokerFixture) {
	t.Helper()
	tuiArgs := readArgLines(t, fixture.tuiArgs)
	if len(tuiArgs) != 6 || tuiArgs[0] != "-c" || tuiArgs[1] != codexRemoteTUIFeatureConfig || tuiArgs[2] != "--remote" || !strings.HasPrefix(tuiArgs[3], "ws://127.0.0.1:") || strings.Contains(strings.TrimPrefix(tuiArgs[3], "ws://"), "/") || tuiArgs[4] != "--remote-auth-token-env" || tuiArgs[5] != codexrunner.RemoteBrokerAuthTokenEnv {
		t.Fatalf("TUI args = %#v", tuiArgs)
	}
	assertBrokerCapabilityToken(t, fixture)
	assertRemoteTUISQLiteIsolation(t, fixture)
	appArgs := strings.Join(readArgLines(t, fixture.appServerArgs), "\n")
	for _, want := range []string{"app-server", "--analytics-default-enabled", `approval_policy="on-request"`, `approvals_reviewer="user"`, `sandbox_mode="read-only"`} {
		if !strings.Contains(appArgs, want) {
			t.Fatalf("app-server args missing %q:\n%s", want, appArgs)
		}
	}
	for _, forbidden := range []string{"--yolo", "dangerously-bypass", "danger-full-access", "approval_policy=\"never\""} {
		if strings.Contains(appArgs, forbidden) || strings.Contains(strings.Join(tuiArgs, "\n"), forbidden) {
			t.Fatalf("launch retained forbidden execution signal %q", forbidden)
		}
	}
}

func assertRemoteTUISQLiteIsolation(t *testing.T, fixture codexTUIBrokerFixture) {
	t.Helper()
	raw, err := os.ReadFile(fixture.tuiSQLiteHome)
	if err != nil {
		t.Fatal(err)
	}
	home := strings.TrimSpace(string(raw))
	if home == "" || !strings.HasPrefix(filepath.Base(home), ".cxp-remote-tui-sqlite-") {
		t.Fatalf("remote TUI sqlite home was not isolated: %q", home)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("temporary remote TUI sqlite home was not cleaned: %q err=%v", home, err)
	}
	appRaw, err := os.ReadFile(fixture.appSQLiteHome)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(appRaw)); got != "" {
		t.Fatalf("app-server unexpectedly inherited remote TUI sqlite home %q", got)
	}
}

func assertBrokerCapabilityToken(t *testing.T, fixture codexTUIBrokerFixture) {
	t.Helper()
	raw, err := os.ReadFile(fixture.tuiAuthToken)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" || token == "poisoned-inherited-token" {
		t.Fatalf("TUI broker capability token was not injected safely: %q", token)
	}
}

func readArgLines(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(strings.TrimSpace(string(raw)))
}
