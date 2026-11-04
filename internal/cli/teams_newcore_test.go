package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

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

func TestTeamsNewCoreSelection(t *testing.T) {
	for _, c := range []struct {
		flag, env string
		want      bool
		err       bool
	}{
		{"", "", false, false},
		{"old", "new", false, false},
		{"new", "", true, false},
		{"", "new", true, false},
		{"", "NEW", true, false},
		{"", "old", false, false},
		{"bogus", "", false, true},
	} {
		t.Setenv(envTeamsCore, c.env)
		got, err := teamsNewCoreSelected(c.flag)
		if got != c.want || (err != nil) != c.err {
			t.Errorf("flag %q env %q: %v, %v; want %v, error %v", c.flag, c.env, got, err, c.want, c.err)
		}
	}
}

// The store the new core runs on: the converted store when there is one,
// a fresh one only when no older helper ever ran, and never an automatic
// conversion of an old store.
func TestTeamsCoreStoreResolution(t *testing.T) {
	t.Setenv(envTeamsCoreStore, "")
	t.Run("fresh install", func(t *testing.T) {
		root := t.TempDir()
		got, err := resolveTeamsCoreStorePath(root)
		if err != nil || got != filepath.Join(root, "teams", migrate.TargetName) {
			t.Fatalf("got %q, %v", got, err)
		}
	})
	t.Run("old store not converted is refused", func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, filepath.Join(root, "teams", "scopes", "s1", "state.json"), "{}")
		_, err := resolveTeamsCoreStorePath(root)
		if err == nil || !strings.Contains(err.Error(), "`cxp teams migrate --source "+filepath.Join(root, "teams", "scopes", "s1")+"`") {
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

// `cxp teams run --core=new` on an old store stops before touching it.
func TestTeamsRunNewCoreRefusesUnconvertedStore(t *testing.T) {
	state := t.TempDir()
	t.Setenv("CODEX_HELPER_STATE_DIR", state)
	t.Setenv("HOME", t.TempDir())
	t.Setenv(envTeamsCore, "")
	t.Setenv(envTeamsCoreStore, "")
	writeFile(t, filepath.Join(state, "teams", "state.json"), "{}")
	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "run", "--core=new", "--auto-service=false", "--auto-update=false"})
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "cxp teams migrate") {
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

func TestNewCoreServiceUnitRestartsOnExit75(t *testing.T) {
	spec := teamsServiceSpec{Executable: "/usr/bin/cxp", WorkingDir: "/w", RegistryPath: "/r.json",
		Environment: map[string]string{"CODEX_HELPER_TEAMS_SERVICE": "1"}}
	old := buildTeamsServiceUnit(spec)
	if !strings.Contains(old, "Restart=on-failure\n") || strings.Contains(old, "RestartForceExitStatus") {
		t.Fatalf("old core unit changed:\n%s", old)
	}
	spec.Environment[envTeamsCore] = "new"
	unit := buildTeamsServiceUnit(spec)
	for _, want := range []string{"Restart=always\n", "RestartForceExitStatus=75\n", "Environment=" + envTeamsCore + "=new\n"} {
		if !strings.Contains(unit, want) {
			t.Fatalf("new core unit lacks %q:\n%s", want, unit)
		}
	}
	if strings.Contains(unit, "Restart=on-failure") {
		t.Fatalf("new core unit keeps Restart=on-failure:\n%s", unit)
	}
	if !teamsServiceEnvironmentNameAllowed(envTeamsCore) || !teamsServiceEnvironmentNameAllowed(envTeamsCoreStore) {
		t.Fatal("the core selection is not carried into the service environment")
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

// Under the new core neither the local supervisor nor the watchdog judges
// the helper from the old store's heartbeats (which would only look stale
// and restart a healthy helper).
func TestNewCoreNotJudgedFromOldStore(t *testing.T) {
	prevCheck, prevCollect := teamsServiceLocalSupervisorCheckChildHealth, teamsServiceWatchdogCollectSnapshot
	t.Cleanup(func() {
		teamsServiceLocalSupervisorCheckChildHealth, teamsServiceWatchdogCollectSnapshot = prevCheck, prevCollect
	})
	checked, collected := 0, 0
	teamsServiceLocalSupervisorCheckChildHealth = func(context.Context, *teamsServiceWatchdogState) (teamsServiceWatchdogDecision, error) {
		checked++
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionRestart, Reason: "helper owner heartbeat is stale"}, nil
	}
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		collected++
		return teamsServiceWatchdogSnapshot{}, errors.New("old store read")
	}

	var state teamsServiceWatchdogState
	cfg := teamsServiceLocalSupervisorConfig{Spec: teamsServiceSpec{Environment: map[string]string{envTeamsCore: "new"}}}
	d, err := teamsServiceLocalSupervisorChildHealth(context.Background(), cfg, &state)
	if err != nil || d.Action != teamsServiceWatchdogActionNoop || checked != 0 {
		t.Fatalf("new core: %+v, %v, old check ran %d times", d, err, checked)
	}
	cfg.Spec.Environment = map[string]string{}
	if d, _ := teamsServiceLocalSupervisorChildHealth(context.Background(), cfg, &state); d.Action != teamsServiceWatchdogActionRestart || checked != 1 {
		t.Fatalf("old core: %+v, checked %d", d, checked)
	}

	t.Setenv(envTeamsCore, "new")
	res, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{})
	if err != nil || res.Decision.Action != teamsServiceWatchdogActionNoop || collected != 0 {
		t.Fatalf("watchdog under the new core: %+v, %v, old store read %d times", res.Decision, err, collected)
	}
}
