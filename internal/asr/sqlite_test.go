package asr

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestASRSQLiteServerRecords(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "run")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(dir, "old.json")
	raw := `{"host":"主机","pid":9007199254740993,"id":"exact/start","owner_pid":42,"owner_id":"owner/start"}`
	if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openServerRecords(root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.page("主机", 0)
	if err != nil || len(rows) != 1 || rows[0].PID != 9007199254740993 || rows[0].ID != "exact/start" {
		t.Fatalf("records %#v/%v", rows, err)
	}
	if b, err := os.ReadFile(legacy + ".migrated"); err != nil || string(b) != raw {
		t.Fatalf("archive: %v", err)
	}
	var before, after int
	if err = db.QueryRow(`SELECT total_changes()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if err = db.put(rows[0]); err != nil {
			t.Fatal(err)
		}
		if _, err = db.page("主机", 0); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil || before != after {
		t.Fatal("unchanged process wrote")
	}
	old := rows[0]
	replacement := old
	replacement.ID = "replacement"
	if err = db.put(replacement); err != nil {
		t.Fatal(err)
	}
	if err = db.remove(old); err != nil {
		t.Fatal(err)
	}
	actual, err := db.page("主机", 0)
	if err != nil || len(actual) != 1 || actual[0].ID != "replacement" {
		t.Fatal("stale cleanup removed new process")
	}
	if records, err := db.page("another-host", 0); err != nil || len(records) != 0 {
		t.Fatal("cross-host records")
	}
	if err = os.WriteFile(legacy, []byte("poison"), 0600); err != nil {
		t.Fatal(err)
	}
	other, err := openServerRecords(root)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
}

func TestASRSQLiteServerMigrationRollback(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "run")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"a.json": `{"host":"h","pid":4,"id":"x","owner_pid":3,"owner_id":"y"}`, "z.json": `{"pid":`} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if db, err := openServerRecords(root); err == nil {
		db.Close()
		t.Fatal("bad record accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "a.json")); err != nil {
		t.Fatal("source archived before commit")
	}
	if err := os.WriteFile(filepath.Join(dir, "z.json"), []byte(`{"host":"h","pid":5,"id":"z","owner_pid":3,"owner_id":"y"}`), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openServerRecords(root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.page("h", 0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rollback/retry %#v/%v", rows, err)
	}
}

func TestASRSQLiteInstallManifest(t *testing.T) {
	dir := t.TempDir()
	body := []byte("synthetic asset 中文")
	if err := os.WriteFile(filepath.Join(dir, "asset"), body, 0600); err != nil {
		t.Fatal(err)
	}
	sum, err := fileSHA256(filepath.Join(dir, "asset"))
	if err != nil {
		t.Fatal(err)
	}
	a := Asset{Name: "asset", SHA256: sum, Format: FormatFile}
	if err = writeManifest(dir, a); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if err = verifyInstall(dir, a); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Stat(filepath.Join(dir, manifestName))
	if err != nil || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("manifest verification wrote")
	}
	if err = os.WriteFile(filepath.Join(dir, ".asr-install.json"), []byte("poison"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = verifyInstall(dir, a); err != nil {
		t.Fatal("native read legacy", err)
	}
	if err = os.WriteFile(filepath.Join(dir, "asset"), []byte(strings.Repeat("x", len(body))), 0600); err != nil {
		t.Fatal(err)
	}
	if err = verifyInstall(dir, a); err == nil {
		t.Fatal("changed file accepted")
	}
	if err = os.WriteFile(filepath.Join(dir, manifestName), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = verifyInstall(dir, a); err == nil {
		t.Fatal("corrupt native accepted")
	}
}

func TestASRSQLiteInstallMigration(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, ".asr-install.json")
	raw := `{"asset":"中文","sha256":"pin","installed":"2026-11-10T01:02:03.123456789Z","files":[{"path":"huge","size":9007199254740993,"sha256":"sum"},{"path":"second","size":1,"sha256":"x"}]}`
	if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openInstallManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var name, installed string
	var size int64
	if err = db.QueryRow(`SELECT asset,installed FROM install_manifest`).Scan(&name, &installed); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT size FROM install_files WHERE position=0`).Scan(&size); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if name != "中文" || installed != "2026-11-10T01:02:03.123456789Z" || size != 9007199254740993 {
		t.Fatalf("literal values %s %s %d", name, installed, size)
	}
	if body, err := os.ReadFile(legacy + ".migrated"); err != nil || string(body) != raw {
		t.Fatal("manifest archive", err)
	}
	for i, bad := range []string{`null`, `{"files":[{"path":"a","size":1,"sha256":"s"}],"asset":`, strings.Replace(raw, `"second"`, `"huge"`, 1), strings.Replace(raw, `"huge"`, `"../escape"`, 1), raw + ` {}`} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, ".asr-install.json"), []byte(bad), 0600); err != nil {
				t.Fatal(err)
			}
			if db, err := openInstallManifest(dir); err == nil {
				db.Close()
				t.Fatal("bad manifest accepted")
			}
			if _, err := os.Stat(filepath.Join(dir, manifestName)); !os.IsNotExist(err) {
				t.Fatalf("partial manifest published: %v", err)
			}
		})
	}
}
