package teams

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFontSQLiteMigrationAndReadOnly(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "font.json")
	raw := `{"kind":"codex-helper-teams-math-font","id":"中文","version":"1","filename":"font.otf","size":9007199254740993,"sha256":"literal","installed_at":"2026-11-10T01:02:03.123456789Z"}`
	if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := readFontMarker(dir)
	if err != nil || m.ID != "中文" || m.Size != 9007199254740993 || m.Installed != "2026-11-10T01:02:03.123456789Z" {
		t.Fatalf("font %#v/%v", m, err)
	}
	if body, err := os.ReadFile(legacy + ".migrated"); err != nil || string(body) != raw {
		t.Fatal("font archive", err)
	}
	path := filepath.Join(dir, fontDatabaseName)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(legacy, []byte("poison"), 0600); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if _, err = readFontMarker(dir); err != nil {
			t.Fatal("native read", err)
		}
	}
	after, err := os.Stat(path)
	if err != nil || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("font query wrote")
	}
	if err = os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = readFontMarker(dir); err == nil {
		t.Fatal("corrupt native replaced from legacy")
	}
}

func TestFontSQLiteMigrationRejectsBadInputs(t *testing.T) {
	for _, raw := range []string{`null`, `{"size":-1}`, `{"filename":`, `{} {}`} {
		dir := t.TempDir()
		legacy := filepath.Join(dir, "font.json")
		if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readFontMarker(dir); err == nil {
			t.Fatalf("accepted %s", raw)
		}
		if _, err := os.Stat(filepath.Join(dir, fontDatabaseName)); !os.IsNotExist(err) {
			t.Fatal("partial font database published")
		}
		if _, err := os.Stat(legacy); err != nil {
			t.Fatal("bad font source archived")
		}
	}
}
