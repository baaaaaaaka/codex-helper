package managedinstall

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestInstallSQLiteMigrationAndQuiet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "install.sqlite")
	legacy := legacyRecordPath(path)
	raw := `{"schema_version":1,"target_path":"/主机/codex-proxy","version":"v9.9.9","shims":["first","first","中"],"updated_at":"2026-11-10T01:02:03.123456789Z"}`
	if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRecord(path)
	if err != nil || r.TargetPath != "/主机/codex-proxy" || len(r.Shims) != 3 || r.Shims[0] != r.Shims[1] || r.UpdatedAt != "2026-11-10T01:02:03.123456789Z" {
		t.Fatalf("literal record %#v/%v", r, err)
	}
	if body, err := os.ReadFile(legacy + ".migrated"); err != nil || string(body) != raw {
		t.Fatal("archive", err)
	}
	db, err := openRecord(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var before, after int
	db.QueryRow(`SELECT total_changes()`).Scan(&before)
	for range 20 {
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		old, err := readRecord(tx)
		if err != nil {
			t.Fatal(err)
		}
		if err = putRecord(tx, r, &old); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil || before != after {
		t.Fatal("unchanged install record wrote")
	}
	if _, err = db.Exec(`CREATE TRIGGER reject_shim BEFORE INSERT ON install_shims WHEN NEW.path='reject' BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	next := r
	next.TargetPath = "changed"
	next.Shims = []string{"reject"}
	if err = SaveRecord(path, next); err == nil {
		t.Fatal("child failure accepted")
	}
	actual, err := LoadRecord(path)
	if err != nil || actual.TargetPath != r.TargetPath || len(actual.Shims) != 3 {
		t.Fatal("partial install update", err)
	}
	if err = os.WriteFile(legacy, []byte("poison"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadRecord(path); err != nil {
		t.Fatal("legacy overrode native", err)
	}
}

func TestInstallSQLiteRejectsBadSources(t *testing.T) {
	for _, raw := range []string{`null`, `{"schema_version":999,"target_path":"future"}`, `{"schema_version":1,"shims":["a"],"target_path":`, `{} {}`} {
		dir := t.TempDir()
		path := filepath.Join(dir, "install.sqlite")
		legacy := legacyRecordPath(path)
		if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadRecord(path); err == nil {
			t.Fatalf("accepted %s", raw)
		}
		if _, err := os.Stat(legacy); err != nil {
			t.Fatal("bad source archived", err)
		}
		if err := os.WriteFile(legacy, []byte(`{"schema_version":1,"target_path":"fixed"}`), 0600); err != nil {
			t.Fatal(err)
		}

		r, err := LoadRecord(path)
		if err != nil || r.TargetPath != "fixed" {
			t.Fatal("migration retry", err)
		}
	}
	path := filepath.Join(t.TempDir(), "install.sqlite")
	if _, err := LoadRecord(path); !os.IsNotExist(err) {
		t.Fatalf("missing read %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("missing read created database")
	}
	if err := SaveRecord(path, Record{TargetPath: "old"}); err != nil {
		t.Fatal(err)
	}
	db, err := openRecord(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE install_record SET schema_version=999`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err = SaveRecord(path, Record{TargetPath: "new"}); err == nil {
		t.Fatal("future record overwritten")
	}
}

func TestInstallSQLiteInstallerEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "install.sqlite")
	var out, stderr bytes.Buffer
	code, handled := HandleInstallerCommand([]string{"cxp", "--cxp-record-install", "--record-path", path, "--target", "/custom/codex-proxy", "--shim", "/custom/cxp", "--version", "v2.0.0"}, &out, &stderr)
	if !handled || code != 0 {
		t.Fatalf("entry %d/%v: %s", code, handled, stderr.String())
	}
	r, err := LoadRecord(path)
	if err != nil || r.TargetPath != "/custom/codex-proxy" || len(r.Shims) != 1 || r.TargetSource != "installer" {
		t.Fatalf("installer record %#v/%v", r, err)
	}
}
