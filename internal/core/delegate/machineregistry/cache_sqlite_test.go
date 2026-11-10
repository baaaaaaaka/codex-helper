package machineregistry

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestRegistryCacheSQLiteMigrationAndQuietReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.sqlite")
	legacy := filepath.Join(filepath.Dir(path), "registry.json")
	if err := os.WriteFile(legacy, []byte(`{"schema_version":1,"registry_key":"原键","registry_generation":"generation","inbox_generation":"inbox-generation","slot_message_id":"slot","inbox_next_refresh_at":"2026-11-10T04:00:00.000000001+08:00","capability_fingerprint":"fp"}`), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenCacheDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, err := db.Load()
	if err != nil {
		t.Fatal(err)
	}
	if v.RegistryKey != "原键" || v.RegistryGeneration != "generation" || v.InboxGeneration != "inbox-generation" || v.SlotMessageID != "slot" || v.InboxNextRefreshAt.Nanosecond() != 1 || v.CapabilityFingerprint != "fp" {
		t.Fatal(v)
	}
	if _, err = os.Stat(legacy + ".migrated"); err != nil {
		t.Fatal(err)
	}
	observer, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	observer.SetMaxOpenConns(1)
	probe := func() (int64, int64, int64) {
		var c, v int64
		if err := db.db.QueryRow(`SELECT total_changes()`).Scan(&c); err != nil {
			t.Fatal(err)
		}
		if err := observer.QueryRow(`PRAGMA data_version`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(path + "-wal")
		if os.IsNotExist(err) {
			return c, v, 0
		}
		if err != nil {
			t.Fatal(err)
		}
		return c, v, st.Size()
	}
	c0, v0, w0 := probe()
	for range 30 {
		if changed, err := db.Save(v); err != nil || changed {
			t.Fatalf("same value changed=%v err=%v", changed, err)
		}
		if _, err := db.Load(); err != nil {
			t.Fatal(err)
		}
	}
	c1, v1, w1 := probe()
	if c0 != c1 || v0 != v1 || w0 != w1 {
		t.Fatalf("unchanged I/O changes %d/%d version %d/%d WAL %d/%d", c0, c1, v0, v1, w0, w1)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(legacy, []byte("broken residual"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err = OpenCacheDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := db.Load()
	if err != nil || got.RegistryGeneration != "generation" {
		t.Fatalf("reopen %+v %v", got, err)
	}
}

func TestRegistryCacheSQLiteRejectsBadInputs(t *testing.T) {
	for _, raw := range []string{`null`, `{`, `{"schema_version":999}`, `{} trailing`} {
		t.Run(raw, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "registry.sqlite")
			if err := os.WriteFile(filepath.Join(filepath.Dir(path), "registry.json"), []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			if db, err := OpenCacheDB(path); err == nil {
				db.Close()
				t.Fatal("bad cache migrated")
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var n, version int
			if err = db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='registry_cache'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if n != 0 || version != 0 {
				t.Fatalf("partial import: tables=%d version=%d", n, version)
			}
		})
	}
}
