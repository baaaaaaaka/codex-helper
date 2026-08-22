package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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
	assertNoCodexRolloutMigration(t, fixture)
}

func TestRunCodexNewSessionUsesManagedNodeWithoutSystemNode(t *testing.T) {
	fixture := writeCodexTUIBrokerFixture(t)
	wrapper := writeManagedNodeCodexWrapperFixture(t, fixture.path)
	store := newCodexOpenTestStore(t)
	if err := runCodexNewSession(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil, fixture.workDir, wrapper, "", false, io.Discard); err != nil {
		t.Fatalf("runCodexNewSession through managed Node: %v", err)
	}
	assertStandardBrokerLaunch(t, fixture)
	assertNoCodexRolloutMigration(t, fixture)
}

func TestRunCodexSessionPreservesResumeExperience(t *testing.T) {
	fixture := writeCodexTUIBrokerFixture(t)
	store := newCodexOpenTestStore(t)
	threadID := "11111111-2222-3333-4444-555555555555"
	session := codexhistory.Session{SessionID: threadID, ProjectPath: fixture.workDir}
	if err := runCodexSession(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil, session, codexhistory.Project{Path: fixture.workDir}, fixture.path, "", false, io.Discard); err != nil {
		t.Fatalf("runCodexSession: %v", err)
	}
	tuiArgs := readArgLines(t, fixture.tuiArgs)
	if len(tuiArgs) != 8 || tuiArgs[0] != "-c" || tuiArgs[1] != codexRemoteTUIFeatureConfig || tuiArgs[2] != "--remote" || !strings.HasPrefix(tuiArgs[3], "ws://127.0.0.1:") || strings.Contains(strings.TrimPrefix(tuiArgs[3], "ws://"), "/") || tuiArgs[4] != "--remote-auth-token-env" || tuiArgs[5] != codexrunner.RemoteBrokerAuthTokenEnv || tuiArgs[6] != "resume" || tuiArgs[7] != threadID {
		t.Fatalf("TUI args = %#v", tuiArgs)
	}
	assertCodexRolloutMigration(t, fixture, threadID)
	assertBrokerCapabilityToken(t, fixture)
}

func TestRunCodexSessionBlocksBrokerWhenRolloutMigrationFails(t *testing.T) {
	fixture := writeCodexTUIBrokerFixture(t)
	store := newCodexOpenTestStore(t)
	threadID := "11111111-2222-3333-4444-555555555555"
	t.Setenv("CXP_TEST_MIGRATION_STATUS", "failed")
	err := runCodexSession(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil,
		codexhistory.Session{SessionID: threadID, ProjectPath: fixture.workDir}, codexhistory.Project{Path: fixture.workDir}, fixture.path, "", false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "migrate legacy Codex rollout") {
		t.Fatalf("runCodexSession error = %v, want migration failure", err)
	}
	if _, statErr := os.Stat(fixture.tuiArgs); !os.IsNotExist(statErr) {
		t.Fatalf("TUI started after migration failure: stat err=%v", statErr)
	}
	if _, statErr := os.Stat(fixture.appServerArgs); !os.IsNotExist(statErr) {
		t.Fatalf("app-server started after migration failure: stat err=%v", statErr)
	}
	events, err := os.ReadFile(fixture.events)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(string(events)); !reflect.DeepEqual(got, []string{"migration-start"}) {
		t.Fatalf("lifecycle events = %#v, want only migration-start", got)
	}
}

func TestRunCodexSessionBlocksBrokerWhenMigrationReportIsInvalid(t *testing.T) {
	fixture := writeCodexTUIBrokerFixture(t)
	store := newCodexOpenTestStore(t)
	threadID := "11111111-2222-3333-4444-555555555555"
	t.Setenv("CXP_TEST_MIGRATION_STATUS", "bad-json")
	err := runCodexSession(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil,
		codexhistory.Session{SessionID: threadID, ProjectPath: fixture.workDir}, codexhistory.Project{Path: fixture.workDir}, fixture.path, "", false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "parse JSON report") {
		t.Fatalf("runCodexSession error = %v, want invalid report error", err)
	}
	if _, statErr := os.Stat(fixture.migrationArgs); statErr != nil {
		t.Fatalf("migration did not run before invalid report was rejected: %v", statErr)
	}
	if _, statErr := os.Stat(fixture.tuiArgs); !os.IsNotExist(statErr) {
		t.Fatalf("TUI started after invalid migration report: stat err=%v", statErr)
	}
}

func TestRunCodexForkSessionForksBeforeResumingChildThroughRemoteTUI(t *testing.T) {
	fixture := writeCodexTUIBrokerFixture(t)
	store := newCodexOpenTestStore(t)
	session := codexhistory.Session{SessionID: "parent-thread", ProjectPath: fixture.workDir}
	if err := runCodexForkSession(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil, session, codexhistory.Project{Path: fixture.workDir}, fixture.path, "", false, io.Discard); err != nil {
		t.Fatalf("runCodexForkSession: %v", err)
	}
	tuiArgs := readArgLines(t, fixture.tuiArgs)
	if len(tuiArgs) != 8 || tuiArgs[6] != "resume" || tuiArgs[7] != "child-thread" {
		t.Fatalf("TUI args = %#v, want resume child-thread", tuiArgs)
	}
	rawEvents, err := os.ReadFile(fixture.events)
	if err != nil {
		t.Fatal(err)
	}
	events := strings.Fields(string(rawEvents))
	forkIndex := indexOfString(events, "fork")
	tuiIndex := indexOfString(events, "tui-start")
	if forkIndex < 0 || tuiIndex < 0 || forkIndex >= tuiIndex {
		t.Fatalf("fork lifecycle events = %#v, want fork before remote TUI", events)
	}
	assertNoCodexRolloutMigration(t, fixture)
}

func indexOfString(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
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
	migrationArgs string
	migrationEnv  string
	events        string
}

func writeCodexTUIBrokerFixture(t *testing.T) codexTUIBrokerFixture {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX app-server fixture")
	}
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("sleep is required by the asynchronous app-server fixture")
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
	migrationArgs := filepath.Join(dir, "migration.args")
	migrationEnv := filepath.Join(dir, "migration.env")
	events := filepath.Join(dir, "events")
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
	  migrate-rollouts)
	    printf 'migration-start\n' >> %s
	    printf '%%s\n' "$@" > %s
	    printf '%%s\n' "${%s:-}" > %s
	    printf '%%s\n' "${%s:-}" >> %s
	    printf '%%s\n' "${%s:-}" >> %s
	    migration_thread_id=
	    previous=
	    for arg in "$@"; do
	      if [ "$previous" = '--thread' ]; then
	        migration_thread_id="$arg"
	      fi
	      previous="$arg"
	    done
	    status="${CXP_TEST_MIGRATION_STATUS:-migrated}"
	    if [ "$status" = 'process-failure' ]; then
	      echo 'migration process failed' >&2
	      exit 17
	    fi
	    if [ "$status" = 'bad-json' ]; then
	      printf 'not-json\n'
	      exit 0
	    fi
	    printf '{"outcomes":[{"thread_id":"%%s","status":"%%s"}]}\n' "$migration_thread_id" "$status"
	    exit 0
	    ;;
	  app-server)
	    printf 'app-server-start\n' >> %s
	    printf '%%s\n' "$@" > %s
    printf '%%s\n' "${%s:-}" > %s
    while IFS= read -r line; do
      id=$(printf %%s "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
      case "$line" in
        *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{}}\n' "$id" ;;
	        *'"method":"thread/list"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{"data":[]}}\n' "$id" ;;
	        *'"method":"thread/read"'*) printf '{"jsonrpc":"2.0","id":%%s,"result":{"thread":{"id":"parent-thread","latestTurnId":"turn-7","turns":[{"id":"turn-7","status":"completed"}]}}}\n' "$id" ;;
	        *'"method":"thread/fork"'*) printf 'fork\n' >> %s; printf '{"jsonrpc":"2.0","id":%%s,"result":{"thread":{"id":"child-thread","forkedFromId":"parent-thread","forkedFromTurnId":"turn-7"}}}\n' "$id" ;;
      esac
    done
    exit 0
    ;;
	  *)
	    printf 'tui-start\n' >> %s
	    printf '%%s\n' "$@" > %s
    printf '%%s\n' "${%s:-}" > %s
    printf '%%s\n' "${%s:-}" > %s
    attempt=0
    while [ ! -f %s ]; do
      attempt=$((attempt + 1))
      if [ "$attempt" -ge 200 ]; then
        echo 'app-server fixture did not start before remote TUI exit' >&2
        exit 91
      fi
      %s 0.01
    done
    exit 0
    ;;
esac
	`, shellSingleQuoteForBeaconCLITest(events), shellSingleQuoteForBeaconCLITest(migrationArgs), envCodexHome, shellSingleQuoteForBeaconCLITest(migrationEnv), codexrunner.RemoteBrokerAuthTokenEnv, shellSingleQuoteForBeaconCLITest(migrationEnv), envCodexSQLiteHome, shellSingleQuoteForBeaconCLITest(migrationEnv), shellSingleQuoteForBeaconCLITest(events), shellSingleQuoteForBeaconCLITest(appServerArgs), envCodexSQLiteHome, shellSingleQuoteForBeaconCLITest(appSQLiteHome), shellSingleQuoteForBeaconCLITest(events), shellSingleQuoteForBeaconCLITest(events), shellSingleQuoteForBeaconCLITest(tuiArgs), codexrunner.RemoteBrokerAuthTokenEnv, shellSingleQuoteForBeaconCLITest(tuiAuthToken), envCodexSQLiteHome, shellSingleQuoteForBeaconCLITest(tuiSQLiteHome), shellSingleQuoteForBeaconCLITest(appSQLiteHome), shellSingleQuoteForBeaconCLITest(sleepPath))
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return codexTUIBrokerFixture{path: path, workDir: workDir, tuiArgs: tuiArgs, tuiAuthToken: tuiAuthToken, tuiSQLiteHome: tuiSQLiteHome, appServerArgs: appServerArgs, appSQLiteHome: appSQLiteHome, migrationArgs: migrationArgs, migrationEnv: migrationEnv, events: events}
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
	for _, want := range []string{"app-server", "--analytics-default-enabled", `approval_policy="on-request"`, `approvals_reviewer="user"`} {
		if !strings.Contains(appArgs, want) {
			t.Fatalf("app-server args missing %q:\n%s", want, appArgs)
		}
	}
	if strings.Contains(appArgs, "sandbox_mode=") {
		t.Fatalf("app-server args must leave sandbox_mode to Codex configuration:\n%s", appArgs)
	}
	for _, forbidden := range []string{"--aaa", "agent_auto_approve", "auto_approve", "--yolo", "dangerously-bypass", "danger-full-access", "approval_policy=\"never\""} {
		if strings.Contains(appArgs, forbidden) || strings.Contains(strings.Join(tuiArgs, "\n"), forbidden) {
			t.Fatalf("launch retained forbidden execution signal %q", forbidden)
		}
	}
}

func assertCodexRolloutMigration(t *testing.T, fixture codexTUIBrokerFixture, threadID string) {
	t.Helper()
	wantArgs := []string{"migrate-rollouts", "--apply", "--thread", threadID, "--json"}
	if got := readArgLines(t, fixture.migrationArgs); !reflect.DeepEqual(got, wantArgs) {
		t.Fatalf("migration args = %#v, want %#v", got, wantArgs)
	}
	envRaw, err := os.ReadFile(fixture.migrationEnv)
	if err != nil {
		t.Fatal(err)
	}
	envLines := strings.Split(strings.TrimSuffix(string(envRaw), "\n"), "\n")
	if len(envLines) != 3 || strings.TrimSpace(envLines[0]) == "" || envLines[1] != "" || envLines[2] != "" {
		t.Fatalf("migration environment = %#v, want Codex home plus no broker token/SQLite home", envLines)
	}
	eventsRaw, err := os.ReadFile(fixture.events)
	if err != nil {
		t.Fatal(err)
	}
	events := strings.Fields(string(eventsRaw))
	migrationIndex := indexOfString(events, "migration-start")
	appServerIndex := indexOfString(events, "app-server-start")
	tuiIndex := indexOfString(events, "tui-start")
	if migrationIndex < 0 || appServerIndex < 0 || tuiIndex < 0 || migrationIndex >= appServerIndex || appServerIndex >= tuiIndex {
		t.Fatalf("lifecycle events = %#v, want migration before app-server before TUI", events)
	}
}

func assertNoCodexRolloutMigration(t *testing.T, fixture codexTUIBrokerFixture) {
	t.Helper()
	if _, err := os.Stat(fixture.migrationArgs); !os.IsNotExist(err) {
		t.Fatalf("unexpected rollout migration invocation: stat err=%v", err)
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
