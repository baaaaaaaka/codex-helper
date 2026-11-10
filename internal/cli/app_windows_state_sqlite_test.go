package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsAppSQLiteMigrationAndQuiet(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "current.json")
	raw := `{"packageName":"OpenAI.ChatGPT-Desktop","packageVersion":"26.721.4979.0","architecture":"x64","publisher":"CN=中文","packageSha256":"package","executableSha256":"exe","runtime":"versions/26.721.4979.0","installedAt":"2026-11-10T01:02:03.123456789Z"}`
	if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	s, found, err := loadWindowsAppState(root)
	if err != nil || !found || s.Publisher != "CN=中文" || s.InstalledAt != "2026-11-10T01:02:03.123456789Z" {
		t.Fatalf("app state %#v/%v/%v", s, found, err)
	}
	if body, err := os.ReadFile(legacy + ".migrated"); err != nil || string(body) != raw {
		t.Fatal("archive", err)
	}
	db, err := openWindowsAppState(root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var before, after int
	db.QueryRow(`SELECT total_changes()`).Scan(&before)
	for range 20 {
		if _, err = db.Exec(windowsAppStatePut, windowsAppStateArgs(s)...); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil || before != after {
		t.Fatal("unchanged app state wrote")
	}
	if err = os.WriteFile(legacy, []byte("poison"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = loadWindowsAppState(root); err != nil {
		t.Fatal("native read legacy", err)
	}
	if _, err = db.Exec(`PRAGMA user_version=999`); err != nil {
		t.Fatal(err)
	}
	if err = writeCodexWindowsManagedState(root, s); err == nil {
		t.Fatal("future app state overwritten")
	}
}

func TestWindowsAppSQLiteMigrationRollback(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "current.json")
	if err := os.WriteFile(legacy, []byte(`{"packageName":`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadWindowsAppState(root); err == nil {
		t.Fatal("bad app source accepted")
	}
	if _, err := os.Stat(legacy); err != nil {
		t.Fatal("source archived prematurely")
	}
	if err := os.WriteFile(legacy, []byte(`{"packageName":"fixed"}`), 0600); err != nil {
		t.Fatal(err)
	}

	s, ok, err := loadWindowsAppState(root)
	if err != nil || !ok || s.PackageName != "fixed" {
		t.Fatalf("retry %#v/%v/%v", s, ok, err)
	}
}
