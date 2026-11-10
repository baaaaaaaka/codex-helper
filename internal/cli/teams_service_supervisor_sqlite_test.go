package cli

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSupervisorSQLiteLegacyIdentityAndConfig(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	configRoot, _ := isolateTeamsUserDirsForTest(t, tmp)
	legacyConfig := filepath.Join(configRoot, "codex-helper", "teams", "local-supervisor.json")
	raw := `{"version":1,"enabled":true,"spec":{"Executable":"/old/cxp","WorkingDir":"/old/work","RegistryPath":"/retired/registry.json","Environment":{"A":"中文","B":"two"},"WindowsTaskLauncherPath":"C:\\task.cmd","WindowsWatchdogLauncherPath":"C:\\watch.cmd"},"updated_at":"2026-11-10T02:03:04.123456789+08:00"}`
	writeFile(t, legacyConfig, raw)
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(configPath)
	if err != nil || cfg.Spec.Executable != "/old/cxp" || cfg.Spec.Environment["A"] != "中文" || cfg.UpdatedAt.Nanosecond() != 123456789 {
		t.Fatalf("config %#v/%v", cfg, err)
	}
	if b, err := os.ReadFile(legacyConfig + ".migrated"); err != nil || string(b) != raw {
		t.Fatalf("config archive %v", err)
	}
	dir, err := teamsServiceLocalSupervisorRuntimeDir()
	if err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(dir, "local-supervisor-status.json")
	writeFile(t, legacy, `{"version":1,"supervisor_pid":9007199254740993,"supervisor_pgid":18,"supervisor_identity":{"executable":"/old/cxp","args":["teams","teams","--config","/old.json"],"environment":{"A":"one"},"proc_start_time":"start-exact"},"child_pid":20,"child_identity":{"executable":"/old/child","args":[],"environment":{}},"state":"cleanup_failed","enabled":true,"last_error":"barrier","restart_count":7,"updated_at":"2026-11-10T02:03:04.000000001+08:00"}`)
	activation := filepath.Join(dir, "local-supervisor-activation.json")
	writeFile(t, activation, `{"version":1,"status":"pending","target_version":"v9.9.9","old_supervisor_pid":9007199254740993,"old_child_pid":20,"deadline_at":"2026-11-10T02:08:04.000000001+08:00"}`)
	status, ok, err := readTeamsServiceLocalSupervisorStatus()
	if err != nil || !ok || status.SupervisorPID != 9007199254740993 || status.State != "cleanup_failed" || status.RestartCount != 7 || status.SupervisorIdentity == nil || status.SupervisorIdentity.ProcStartTime != "start-exact" || len(status.SupervisorIdentity.Args) != 4 || status.SupervisorIdentity.Args[1] != "teams" || status.SupervisorIdentity.Environment["A"] != "one" || status.UpdatedAt.Nanosecond() != 1 {
		t.Fatalf("status %#v/%v/%v", status, ok, err)
	}
	decision, ok, err := readTeamsServiceLocalSupervisorActivation()
	if err != nil || !ok || decision.TargetVersion != "v9.9.9" || decision.OldSupervisorPID != 9007199254740993 || decision.DeadlineAt.Nanosecond() != 1 {
		t.Fatalf("activation %#v/%v/%v", decision, ok, err)
	}
	if err = os.WriteFile(legacy, []byte("poisoned"), 0600); err != nil {
		t.Fatal(err)
	}
	status.ChildPID = 21
	if err = writeTeamsServiceLocalSupervisorStatus(status); err != nil {
		t.Fatal(err)
	}
	actual, ok, err := readTeamsServiceLocalSupervisorStatus()
	if err != nil || !ok || actual.ChildPID != 21 {
		t.Fatal("legacy source overrode native status")
	}
}

func TestSupervisorSQLiteQuietAndChildRollback(t *testing.T) {
	lockCLITestHooks(t)
	isolateTeamsUserDirsForTest(t, t.TempDir())
	st, err := openTeamsWatchdogState()
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	status := teamsServiceLocalSupervisorStatus{Version: 1, SupervisorPID: 10, ChildPID: 20, State: "cleanup_failed", UpdatedAt: time.Now(), ChildIdentity: &teamsServiceLocalSupervisorProcessIdentity{Executable: "/child", Args: []string{"one", "two"}, Environment: map[string]string{"A": "value"}}}
	if err = st.SaveSupervisorStatus(status); err != nil {
		t.Fatal(err)
	}
	decision := teamsServiceLocalSupervisorActivation{Version: 1, Status: "pending", TargetVersion: "v9.9.9"}
	if err = st.SaveActivation(decision); err != nil {
		t.Fatal(err)
	}
	changes := func() int {
		var n int
		if err = st.db.QueryRow(`SELECT total_changes()`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	c := changes()
	for range 20 {
		if err = st.SaveSupervisorStatus(status); err != nil {
			t.Fatal(err)
		}
		if err = st.SaveActivation(decision); err != nil {
			t.Fatal(err)
		}
		if _, _, err = st.SupervisorStatus(); err != nil {
			t.Fatal(err)
		}
	}
	if c != changes() {
		t.Fatal("unchanged supervisor records wrote")
	}
	if _, err = st.db.Exec(`CREATE TRIGGER reject_supervisor_arg BEFORE INSERT ON service_process_args WHEN NEW.value='reject' BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	status.ChildPID = 99
	status.ChildIdentity.Args = []string{"reject"}
	if err = st.SaveSupervisorStatus(status); err == nil {
		t.Fatal("child insert failure accepted")
	}
	actual, ok, err := st.SupervisorStatus()
	if err != nil || !ok || actual.ChildPID != 20 || len(actual.ChildIdentity.Args) != 2 || actual.ChildIdentity.Args[0] != "one" {
		t.Fatalf("partial parent/child write %#v/%v", actual, err)
	}
	if err = st.SaveWatchdog(teamsServiceWatchdogState{ConsecutiveStale: 7}); err != nil {
		t.Fatal(err)
	}
	if err = st.DeleteSupervisorStatus(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = st.SupervisorStatus(); err != nil || ok {
		t.Fatalf("status not deleted: %v/%v", ok, err)
	}
	if watch, err := st.Watchdog(); err != nil || watch.ConsecutiveStale != 7 {
		t.Fatal("status deletion affected watchdog")
	}
	if value, ok, err := st.Activation(); err != nil || !ok || value.Status != "pending" {
		t.Fatal("status deletion affected activation")
	}
}

func TestSupervisorSQLiteConfigChildRollbackAndQuiet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "supervisor.sqlite")
	st, err := openTeamsSupervisorConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.db.Close()
	cfg := teamsServiceLocalSupervisorConfig{Version: 1, Enabled: true, Spec: teamsServiceSpec{Executable: "/old", Environment: map[string]string{"A": "keep"}}}
	if err = st.Save(cfg); err != nil {
		t.Fatal(err)
	}
	var before, after int
	if err = st.db.QueryRow(`SELECT total_changes()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if err = st.Save(cfg); err != nil {
			t.Fatal(err)
		}
		if _, err = st.Config(); err != nil {
			t.Fatal(err)
		}
	}
	if err = st.db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil || before != after {
		t.Fatal("unchanged config wrote")
	}
	if _, err = st.db.Exec(`CREATE TRIGGER reject_supervisor_env BEFORE INSERT ON supervisor_config_environment WHEN NEW.name='reject' BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	cfg.Spec.Executable = "/new"
	cfg.Spec.Environment = map[string]string{"reject": "bad"}
	if err = st.Save(cfg); err == nil {
		t.Fatal("environment failure accepted")
	}
	actual, err := st.Config()
	if err != nil || actual.Spec.Executable != "/old" || actual.Spec.Environment["A"] != "keep" {
		t.Fatal("partial config update")
	}
	if err = st.Delete(); err != nil {
		t.Fatal(err)
	}
	if _, err = st.Config(); !os.IsNotExist(err) {
		t.Fatalf("config not deleted: %v", err)
	}
}

func TestSupervisorSQLiteReadRetriesFailedMigrationAndRejectsSealedLoss(t *testing.T) {
	for _, kind := range []string{"config", "status"} {
		t.Run(kind, func(t *testing.T) {
			lockCLITestHooks(t)
			configRoot, _ := isolateTeamsUserDirsForTest(t, t.TempDir())
			var legacy, native, valid string
			var read func() error
			if kind == "config" {
				legacy = filepath.Join(configRoot, "codex-helper", "teams", "local-supervisor.json")
				var err error
				native, err = teamsServiceLocalSupervisorConfigPath()
				if err != nil {
					t.Fatal(err)
				}
				valid = `{"version":1,"enabled":true,"spec":{"Executable":"/old/cxp"}}`
				read = func() error {
					v, err := readTeamsServiceLocalSupervisorConfig(native)
					if err == nil && v.Spec.Executable != "/old/cxp" {
						t.Fatal("lost imported config")
					}
					return err
				}
			} else {
				dir, err := teamsServiceLocalSupervisorRuntimeDir()
				if err != nil {
					t.Fatal(err)
				}
				legacy = filepath.Join(dir, "local-supervisor-status.json")
				native, err = defaultTeamsServiceWatchdogStatePath()
				if err != nil {
					t.Fatal(err)
				}
				valid = `{"version":1,"supervisor_pid":123,"state":"cleanup_failed"}`
				read = func() error {
					v, ok, err := readTeamsServiceLocalSupervisorStatus()
					if err == nil && (!ok || v.SupervisorPID != 123) {
						t.Fatal("lost imported cleanup evidence")
					}
					return err
				}
			}
			writeFile(t, legacy, "{broken")
			if err := read(); err == nil {
				t.Fatal("bad migration source accepted")
			}
			writeFile(t, legacy, valid)
			if err := read(); err != nil {
				t.Fatalf("normal read cannot retry failed migration: %v", err)
			}
			if err := os.Remove(native); err != nil {
				t.Fatal(err)
			}
			// Even a complete legacy source must not restore a published lost store.
			writeFile(t, legacy, valid)
			if err := read(); err == nil || os.IsNotExist(err) {
				t.Fatalf("sealed loss treated as empty/new: %v", err)
			}
			if _, err := os.Stat(native); !os.IsNotExist(err) {
				t.Fatal("remigrated lost store", err)
			}
		})
	}
}
