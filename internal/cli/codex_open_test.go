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
	if len(tuiArgs) != 6 || tuiArgs[0] != "-c" || tuiArgs[1] != codexRemoteTUIFeatureConfig || tuiArgs[2] != "--remote" || !strings.HasPrefix(tuiArgs[3], "ws://127.0.0.1:") || tuiArgs[4] != "resume" || tuiArgs[5] != "session-existing" {
		t.Fatalf("TUI args = %#v", tuiArgs)
	}
}

func TestNormalizeWorkingDirRejectsMissingDirectory(t *testing.T) {
	if _, err := normalizeWorkingDir(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("normalizeWorkingDir accepted a missing directory")
	}
}

type codexTUIBrokerFixture struct {
	path          string
	workDir       string
	tuiArgs       string
	appServerArgs string
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
	appServerArgs := filepath.Join(dir, "app-server.args")
	script := fmt.Sprintf(`#!/bin/sh
set -eu
case "${1:-}" in
  --version)
    echo 'codex-cli 0.0-test'
    exit 0
    ;;
  app-server)
    printf '%%s\n' "$@" > %s
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
    exit 0
    ;;
esac
`, shellSingleQuoteForBeaconCLITest(appServerArgs), shellSingleQuoteForBeaconCLITest(tuiArgs))
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return codexTUIBrokerFixture{path: path, workDir: workDir, tuiArgs: tuiArgs, appServerArgs: appServerArgs}
}

func newCodexOpenTestStore(t *testing.T) *config.Store {
	t.Helper()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	return store
}

func assertStandardBrokerLaunch(t *testing.T, fixture codexTUIBrokerFixture) {
	t.Helper()
	tuiArgs := readArgLines(t, fixture.tuiArgs)
	if len(tuiArgs) != 4 || tuiArgs[0] != "-c" || tuiArgs[1] != codexRemoteTUIFeatureConfig || tuiArgs[2] != "--remote" || !strings.HasPrefix(tuiArgs[3], "ws://127.0.0.1:") || !strings.Contains(tuiArgs[3], "/_cxp/") {
		t.Fatalf("TUI args = %#v", tuiArgs)
	}
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

func readArgLines(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(strings.TrimSpace(string(raw)))
}
