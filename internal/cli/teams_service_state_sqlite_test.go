package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

func TestServiceSQLiteLegacyMigrationAndQuietWrites(t *testing.T) {
	lockCLITestHooks(t)
	dir := t.TempDir()
	legacy := filepath.Join(dir, "service-restarts.host-a.json")
	raw := `{"streak":7,"last_start":"2026-11-10T00:00:00.000000001+08:00","wait_until":"2026-11-10T00:05:00+08:00"}`
	if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	watch := filepath.Join(dir, "watchdog.json")
	if err := os.WriteFile(watch, []byte(`{"consecutive_stale":3,"last_reason":"故障","last_action":"restart","last_action_at":"2026-11-10T00:00:00.123456789Z"}`), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "service-state.sqlite")
	st, err := openTeamsServiceState(path, []string{watch})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rec, err := st.Restart("host-a")
	if err != nil || rec.Streak != 7 || rec.LastStart.Nanosecond() != 1 || rec.WaitUntil.Sub(rec.LastStart) != 5*time.Minute-time.Nanosecond {
		t.Fatalf("restart %#v/%v", rec, err)
	}
	state, err := st.Watchdog()
	if err != nil || state.LastReason != "故障" || state.LastActionAt.Nanosecond() != 123456789 {
		t.Fatalf("watchdog %#v/%v", state, err)
	}
	if archived, err := os.ReadFile(legacy + ".migrated"); err != nil || string(archived) != raw {
		t.Fatalf("archive %q/%v", archived, err)
	}
	observer, err := sqlitestate.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	changes := func() int {
		var n int
		if err = st.db.QueryRow(`SELECT total_changes()`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	version := func() int {
		var n int
		if err = observer.QueryRow(`PRAGMA data_version`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	c, d := changes(), version()
	for range 20 {
		if err = st.SaveRestart("host-a", rec); err != nil {
			t.Fatal(err)
		}
		if err = st.SaveWatchdog(state); err != nil {
			t.Fatal(err)
		}
		if _, err = st.Restart("host-a"); err != nil {
			t.Fatal(err)
		}
		if _, err = st.Watchdog(); err != nil {
			t.Fatal(err)
		}
	}
	if changes() != c || version() != d {
		t.Fatal("unchanged service state wrote")
	}
	if err = st.SaveRestart("host-b", teamsServiceRestartRecord{Streak: 11}); err != nil {
		t.Fatal(err)
	}
	rec, err = st.Restart("host-a")
	if err != nil || rec.Streak != 7 {
		t.Fatal("host isolation lost")
	}
	if err = os.WriteFile(legacy, []byte("poisoned"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(watch, []byte("poisoned"), 0600); err != nil {
		t.Fatal(err)
	}
	again, err := openTeamsServiceState(path, []string{watch})
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if rec, err = again.Restart("host-b"); err != nil || rec.Streak != 11 {
		t.Fatalf("native authority lost %#v/%v", rec, err)
	}
}

func TestServiceSQLiteMigrationRollbackAndCorruption(t *testing.T) {
	lockCLITestHooks(t)
	for _, raw := range []string{`null`, `{`, `{} trailing`, `{"streak":-1}`, `{"last_start":"bad"}`} {
		t.Run(raw, func(t *testing.T) {
			dir := t.TempDir()
			legacy := filepath.Join(dir, "service-restarts.host-a.json")
			path := filepath.Join(dir, "service-state.sqlite")
			if err := os.WriteFile(legacy, []byte(`{"streak":4}`), 0600); err != nil {
				t.Fatal(err)
			}
			bad := filepath.Join(dir, "service-restarts.host-z.json")
			if err := os.WriteFile(bad, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			if st, err := openTeamsServiceState(path, nil); err == nil {
				st.Close()
				t.Fatal("bad source accepted")
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var n, v int
			if err = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != 0 || n != 0 {
				t.Fatalf("partial migration tables=%d version=%d: %v", n, v, err)
			}
			if _, err = os.Stat(legacy); err != nil {
				t.Fatal(err)
			}
		})
	}
	dir := t.TempDir()
	loc := teamsServiceRestartRecordPath(dir, "host")
	if err := os.WriteFile(loc.Database, []byte("broken database"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readTeamsServiceRestartRecord(loc); err == nil {
		t.Fatal("corrupt restart state reset")
	}
	if err := teamsServiceStartBackoff(context.Background(), loc, os.Stderr); err == nil {
		t.Fatal("corrupt restart allowed start")
	}
}

func TestServiceSQLiteReadonlyDoesNotCreateState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.sqlite")
	if _, err := loadTeamsServiceWatchdogNative(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("readonly watchdog created state")
	}
	loc := teamsServiceRestartRecordPath(dir, "host")
	if _, err := readTeamsServiceRestartRecord(loc); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(loc.Database); !os.IsNotExist(err) {
		t.Fatal("readonly backoff created state")
	}
}
