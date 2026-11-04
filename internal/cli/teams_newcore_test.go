package cli

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// The store the new core runs on: the converted store when there is one,
// a fresh one only when no older helper ever ran (an older store is
// converted at the first start: teams_newcore_migrate_test.go).
func TestTeamsCoreStoreResolution(t *testing.T) {
	t.Setenv(envTeamsCoreStore, "")
	t.Run("fresh install", func(t *testing.T) {
		root := t.TempDir()
		got, err := resolveTeamsCoreStorePath(root)
		if err != nil || got != filepath.Join(root, "teams", migrate.TargetName) {
			t.Fatalf("got %q, %v", got, err)
		}
	})
	t.Run("intermediate release store is refused", func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, filepath.Join(root, "teams", "scopes", "s1", "state.json"), "{}")
		_, err := resolveTeamsCoreStorePath(root)
		if err == nil || !strings.Contains(err.Error(), "run v0.1.22-rc.59 once") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("converted in place", func(t *testing.T) {
		root := t.TempDir()
		dir := filepath.Join(root, "teams", "scopes", "s1")
		writeFile(t, filepath.Join(dir, "state.json"), "{}")
		writeFile(t, filepath.Join(dir, migrate.TargetName), "")
		// A stale scope that was never converted does not block it.
		writeFile(t, filepath.Join(root, "teams", "scopes", "s0", "state.json"), "{}")
		got, err := resolveTeamsCoreStorePath(root)
		if err != nil || got != filepath.Join(dir, migrate.TargetName) {
			t.Fatalf("got %q, %v", got, err)
		}
	})
	t.Run("converted to another directory", func(t *testing.T) {
		root := t.TempDir()
		dir := filepath.Join(root, "teams")
		target := filepath.Join(t.TempDir(), "out dir", migrate.TargetName)
		writeFile(t, filepath.Join(dir, "state.json"), "{}")
		writeFile(t, target, "")
		writeFile(t, filepath.Join(dir, migrate.MarkerName), fmt.Sprintf("migrated to %s at 2026-11-04T10:00:00Z by cxp teams migrate (v1)\n", target))
		got, err := resolveTeamsCoreStorePath(root)
		if err != nil || got != target {
			t.Fatalf("got %q, %v", got, err)
		}
	})
	t.Run("several converted stores", func(t *testing.T) {
		root := t.TempDir()
		for _, s := range []string{"s1", "s2"} {
			dir := filepath.Join(root, "teams", "scopes", s)
			writeFile(t, filepath.Join(dir, "state.json"), "{}")
			writeFile(t, filepath.Join(dir, migrate.TargetName), "")
		}
		if _, err := resolveTeamsCoreStorePath(root); err == nil || !strings.Contains(err.Error(), envTeamsCoreStore) {
			t.Fatalf("err = %v", err)
		}
		pick := filepath.Join(root, "teams", "scopes", "s2", migrate.TargetName)
		t.Setenv(envTeamsCoreStore, pick)
		if got, err := resolveTeamsCoreStorePath(root); err != nil || got != pick {
			t.Fatalf("with %s: %q, %v", envTeamsCoreStore, got, err)
		}
	})
}

// `cxp teams run` on a store of an intermediate release stops before
// touching it and says to run rc.59 once first.
func TestTeamsRunNewCoreRefusesUnconvertedStore(t *testing.T) {
	state := t.TempDir()
	t.Setenv("CODEX_HELPER_STATE_DIR", state)
	t.Setenv("HOME", t.TempDir())
	t.Setenv(envTeamsCoreStore, "")
	writeFile(t, filepath.Join(state, "teams", "state.json"), "{}")
	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "run", "--auto-service=false", "--auto-update=false"})
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "run v0.1.22-rc.59 once") {
		t.Fatalf("err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(state, "teams", migrate.TargetName)); !os.IsNotExist(err) {
		t.Fatalf("core.sqlite was created next to the old store: %v", err)
	}
}

func TestTeamsNewCoreWorkflowFromStore(t *testing.T) {
	dir := t.TempDir()
	st, err := corestore.Open(filepath.Join(dir, migrate.TargetName))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Acquire(corestore.Ident{Host: "h", PID: 1}); err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	set := func(kv ...string) {
		if err := st.Update(func(tx *corestore.Tx) error {
			for i := 0; i < len(kv); i += 2 {
				if err := tx.SetMeta(kv[i], kv[i+1]); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if url, err := teamsNewCoreWorkflowURL(st, dir); url != "" || err != nil {
		t.Fatalf("no config: %q, %v", url, err)
	}
	secret := filepath.Join(dir, "workflow-webhook-url")
	writeFile(t, secret, "https://example.invalid/hook\n")
	set(migrate.MetaWorkflowEnabled, "false", migrate.MetaWorkflowSecretFile, secret)
	if url, err := teamsNewCoreWorkflowURL(st, dir); url != "" || err != nil {
		t.Fatalf("disabled: %q, %v", url, err)
	}
	set(migrate.MetaWorkflowEnabled, "true")
	if url, err := teamsNewCoreWorkflowURL(st, dir); url != "https://example.invalid/hook" || err != nil {
		t.Fatalf("enabled: %q, %v", url, err)
	}
	// A relative path (migrate --out was relative) is found next to the store.
	set(migrate.MetaWorkflowSecretFile, filepath.Join("rel", "workflow-webhook-url"))
	if url, err := teamsNewCoreWorkflowURL(st, dir); url != "https://example.invalid/hook" || err != nil {
		t.Fatalf("relative: %q, %v", url, err)
	}
	set(migrate.MetaWorkflowSecretFile, filepath.Join(dir, "missing"))
	if _, err := teamsNewCoreWorkflowURL(st, dir); err == nil {
		t.Fatal("missing secret: no error")
	}
}

func TestExitCodeForRestart(t *testing.T) {
	restart := &exitCodeError{code: teamsRestartExitCode, err: errors.New("restart")}
	if got := exitCodeFor(fmt.Errorf("wrapped: %w", restart)); got != 75 {
		t.Fatalf("restart: %d", got)
	}
	if got := exitCodeFor(errors.New("x")); got != 1 {
		t.Fatalf("error: %d", got)
	}
	if got := exitCodeFor(nil); got != 0 {
		t.Fatalf("nil: %d", got)
	}
}

func TestLocalSupervisorRestartsPromptlyAfterExit75(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh")
	}
	exitWith := func(code int) error {
		return exec.Command("sh", "-c", fmt.Sprintf("exit %d", code)).Run()
	}
	if got := teamsServiceLocalSupervisorRestartDelayAfter(exitWith(75)); got != teamsServiceLocalSupervisorRestartAfterExit75 {
		t.Fatalf("exit 75: delay %v", got)
	}
	if teamsServiceLocalSupervisorRestartAfterExit75 >= teamsServiceLocalSupervisorRestartDelay {
		t.Fatal("exit 75 is not restarted sooner than a failure")
	}
	for _, err := range []error{exitWith(1), nil, errors.New("start failed")} {
		if got := teamsServiceLocalSupervisorRestartDelayAfter(err); got != teamsServiceLocalSupervisorRestartDelay {
			t.Fatalf("%v: delay %v", err, got)
		}
	}
}

// Exit 75 (restart on the same state) restarts the helper under every
// supervisor: launchd keeps a non-zero exit alive and on Windows the watchdog
// task starts the stopped task (systemd and the local supervisor are covered
// above). The WSL Startup entry is not a supervisor: it only runs
// `cxp teams service start` once, and the local supervisor restarts.
func TestEverySupervisorRestartsAfterExit75(t *testing.T) {
	spec := teamsServiceSpec{Executable: "/usr/local/bin/cxp", WorkingDir: "/w"}
	plist := buildTeamsServiceLaunchAgentPlist(spec)
	if !strings.Contains(plist, "<key>SuccessfulExit</key>\n\t\t<false/>") {
		t.Fatalf("launchd plist does not restart a non-zero exit:\n%s", plist)
	}
	entry := buildTeamsServiceWSLStartupKickVBS(buildTeamsServiceWSLStartupKickArguments(spec))
	if !strings.Contains(entry, "teams service start") || strings.Contains(entry, "teams run") || !strings.HasSuffix(entry, ", 0, False\r\n") {
		t.Fatalf("WSL Startup entry is not a one-shot service start:\n%s", entry)
	}
	d := evaluateTeamsServiceWatchdog(teamsServiceWatchdogSnapshot{Installed: true, Active: false},
		teamsServiceWatchdogState{LastActionAt: time.Now()}, teamsServiceWatchdogOptions{})
	if d.Action != teamsServiceWatchdogActionStart {
		t.Fatalf("watchdog does not start a stopped service: %+v", d)
	}
}

func TestServiceUnitRestartsOnExit75(t *testing.T) {
	spec := teamsServiceSpec{Executable: "/usr/bin/cxp", WorkingDir: "/w",
		Environment: map[string]string{"CODEX_HELPER_TEAMS_SERVICE": "1"}}
	unit := buildTeamsServiceUnit(spec)
	for _, want := range []string{"Restart=always\n", "RestartForceExitStatus=75\n", "RestartSec=10s\n"} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit lacks %q:\n%s", want, unit)
		}
	}
	if strings.Contains(unit, "Restart=on-failure") || strings.Contains(unit, "CODEX_HELPER_TEAMS_CORE=") {
		t.Fatalf("unit keeps an old setting:\n%s", unit)
	}
	if !teamsServiceEnvironmentNameAllowed(envTeamsCoreStore) {
		t.Fatal("the store override is not carried into the service environment")
	}
}

// The app-server PID file is per host: a helper taking over a shared home
// from another host never reads (and kills) that host's pids.
func TestTeamsCorePIDFileIsPerHost(t *testing.T) {
	dir := t.TempDir()
	a, b := teamsCorePIDFile(dir, "login-01"), teamsCorePIDFile(dir, "compute-7.cluster")
	if a == b || filepath.Dir(a) != dir || filepath.Dir(b) != dir {
		t.Fatalf("pid files %q and %q", a, b)
	}
	if got := teamsCorePIDFile(dir, "../x/y"); filepath.Dir(got) != dir {
		t.Fatalf("host name escapes the state dir: %q", got)
	}
	if got := teamsCorePIDFile(dir, ""); filepath.Base(got) != "core-app-servers.pid" {
		t.Fatalf("no host: %q", got)
	}
}
