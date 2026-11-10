package config

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestConfigSQLiteReadOnlyRequiresMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	raw := []byte(`{"version":6,"profiles":[{"id":"p","name":"preserved"}]}`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPathReadOnly(path); err == nil {
		t.Fatal("unmigrated legacy config appeared empty")
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, raw) {
		t.Fatal("read-only access changed the migration source", err)
	}
	native := filepath.Join(filepath.Dir(path), "config.sqlite")
	if _, err := os.Stat(native); !os.IsNotExist(err) {
		t.Fatal("read-only access initialized SQLite", err)
	}
	s, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Load(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	cfg, err := LoadPathReadOnly(path)
	if err != nil || len(cfg.Profiles) != 1 || cfg.Profiles[0].Name != "preserved" {
		t.Fatal("native config did not retain the profile", cfg, err)
	}
}

func TestConfigSQLiteMissingPublishedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.sqlite")
	s, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Load(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err = os.Rename(path, path+".lost"); err != nil {
		t.Fatal(err)
	}
	// A residual legacy file must not cause re-import after native state loss.
	if err = os.WriteFile(filepath.Join(filepath.Dir(path), "config.json"), []byte(`{"version":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path)
	if err == nil {
		defer s.Close()
		_, err = s.Load()
	}
	if err == nil || os.IsNotExist(err) {
		t.Fatalf("published loss treated as new config: %v", err)
	}
	if _, err := LoadPathReadOnly(path); err == nil || os.IsNotExist(err) {
		t.Fatalf("read-only published loss treated as defaults: %v", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("replacement config database created")
	}
}
