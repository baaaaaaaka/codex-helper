package codex

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPIDSQLiteMigrationAndQuietWrites(t *testing.T) {
	legacy := filepath.Join(t.TempDir(), "servers.pid")
	raw := `[{"pid":9007199254740993,"start":"exact/start","key":"会话","boot":"boot","pidns":"ns"}]`
	if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openPIDDatabase(legacy)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	recs, err := readPIDPage(db, 0)
	if err != nil || len(recs) != 1 || recs[0].PID != 9007199254740993 || recs[0].Key != "会话" {
		t.Fatalf("literal records %#v/%v", recs, err)
	}
	if b, err := os.ReadFile(legacy + ".migrated"); err != nil || string(b) != raw {
		t.Fatal("archive", err)
	}
	var before, after int
	if err = db.QueryRow(`SELECT total_changes()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	r := recs[0]
	for range 20 {
		if _, err = db.Exec(pidPut, r.PID, r.Start, r.Key, r.Boot, r.NS); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil || before != after {
		t.Fatal("unchanged process wrote")
	}
	if err = os.WriteFile(legacy, []byte("poison"), 0600); err != nil {
		t.Fatal(err)
	}
	other, err := openPIDDatabase(legacy)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
	if _, err = db.Exec(`PRAGMA user_version=999`); err != nil {
		t.Fatal(err)
	}
	if other, err := openPIDDatabase(legacy); err == nil {
		other.Close()
		t.Fatal("future process state accepted")
	}
}

func TestPIDSQLiteMigrationRollsBack(t *testing.T) {
	for _, bad := range []string{`[{"pid":3,"key":"first"},{"pid":`, `[{"pid":3},{"pid":3}]`, `null`, `[] {}`} {
		legacy := filepath.Join(t.TempDir(), "servers.pid")
		if err := os.WriteFile(legacy, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if db, err := openPIDDatabase(legacy); err == nil {
			db.Close()
			t.Fatalf("accepted %q", bad)
		}
		if _, err := os.Stat(legacy); err != nil {
			t.Fatal("source archived before commit")
		}
		if err := os.WriteFile(legacy, []byte(`[{"pid":3,"key":"fixed"}]`), 0600); err != nil {
			t.Fatal(err)
		}
		db, err := openPIDDatabase(legacy)
		if err != nil {
			t.Fatal(err)
		}
		recs, err := readPIDPage(db, 0)
		db.Close()
		if err != nil || len(recs) != 1 || recs[0].Key != "fixed" {
			t.Fatalf("rollback/retry %#v/%v", recs, err)
		}
	}
}

func TestPIDSQLiteKeyTransactionRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "servers.sqlite")
	db, err := openPIDDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(pidPut, 20, "old", "one", "boot", "ns"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(pidPut, 21, "old", "two", "boot", "ns"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TRIGGER reject_pid_delete BEFORE DELETE ON app_servers WHEN OLD.server_key='two' BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	p := NewPool(PoolConfig{PIDFile: path})
	p.pidStore = db
	if err = p.savePIDKeys([]string{"one", "two"}); err == nil {
		t.Fatal("failed batch accepted")
	}
	recs, err := readPIDPage(db, 0)
	if err != nil || len(recs) != 2 {
		t.Fatal("partial process batch committed")
	}
	p.Close(0)
}
