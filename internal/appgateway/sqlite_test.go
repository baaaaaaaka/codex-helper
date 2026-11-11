package appgateway

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestGatewaySQLiteConcurrentInitialization(t *testing.T) {
	dir := t.TempDir()
	start := make(chan struct{})
	errs := make(chan error, 12)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := NewRegistry(dir)
			if err == nil {
				_, err = r.Ensure(context.Background(), "p", "fp")
				r.Close()
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGatewaySQLiteMigrationAndQuietWrites(t *testing.T) {
	dir := t.TempDir()
	name := profileFilePart("p") + ".json"
	raw := `{"schema":1,"id":"` + profileFilePart("p") + `","profileId":"p","httpPort":3804,"legacyPorts":[1234,4567],"state":"blocked","ownerGeneration":"original","ownerPid":123,"ownerStartedAt":"2026-11-01T02:03:04.123456789+08:00","recoveryAttempts":7,"recoveryBlocked":true,"lastError":"原始故障"}`
	if err := os.WriteFile(filepath.Join(dir, name), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, profileFilePart("p")+".jsonl"), []byte(`{"at":"2026-11-01T02:03:04Z","profileId":"p","event":"blocked","details":"原始故障"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := newTestRegistry(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	v, err := r.Load("p")
	if err != nil {
		t.Fatal(err)
	}
	if v.RecoveryAttempts != 7 || !v.RecoveryBlocked || v.OwnerGeneration != "original" || len(v.LegacyPorts) != 2 || v.LegacyPorts[1] != 4567 || gatewayTime(v.OwnerStartedAt) != "2026-11-01T02:03:04.123456789+08:00" {
		t.Fatalf("lost migration evidence %#v", v)
	}
	if _, err = os.Stat(filepath.Join(dir, name+".migrated")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, name), []byte("poisoned"), 0600); err != nil {
		t.Fatal(err)
	}
	other, err := newTestRegistry(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	changes := func() int {
		var n int
		if err = r.db.QueryRow(`SELECT total_changes()`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	version := func() int {
		var n int
		if err = other.db.QueryRow(`PRAGMA data_version`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	c, d := changes(), version()
	for i := 0; i < 20; i++ {
		if _, err = r.Load("p"); err != nil {
			t.Fatal(err)
		}
		if err = r.Save(v); err != nil {
			t.Fatal(err)
		}
	}
	if c != changes() || d != version() {
		t.Fatalf("unchanged registration wrote %d/%d %d/%d", c, changes(), d, version())
	}
	if _, err = r.db.Exec(`CREATE TRIGGER reject_port BEFORE INSERT ON legacy_ports WHEN NEW.port=5555 BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	v.LastError = "should roll back"
	v.LegacyPorts = append(v.LegacyPorts, 5555)
	if err = r.Save(v); err == nil {
		t.Fatal("accepted injected child failure")
	}
	got, err := r.Load("p")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastError != "原始故障" || len(got.LegacyPorts) != 2 {
		t.Fatalf("partial registration commit %#v", got)
	}
}

// Old releases only appended to the event logs; an interrupted append left a
// torn line that must not stop the helper from starting after an update.
func TestGatewaySQLiteMigrationSkipsTornEventLines(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, profileFilePart("p")+".jsonl")
	previous := `{"at":"2026-08-23T00:42:38Z","profileId":"p","event":"before"}` + "\n" +
		`{"at":"2026-08-23T00:42:39.06582513Z","registrationId":"5651{"at":"2026-08-23T00:42:53Z","profileId":"p","event":"torn"}` + "\n" +
		`{"at":"2026-08-23T00:42:54Z","profileId":"p","event":"after"}` + "\n"
	if err := os.WriteFile(log+".1", []byte(previous), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(log, []byte(`{"at":"2026-11-10T00:00:00Z","profileId":"p","event":"current"}`+"\n"+`{"at":"2026-11-10T00:00:01Z","prof`), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := newTestRegistry(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.db.Query(`SELECT event_name FROM gateway_events ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		got = append(got, name)
	}
	if strings.Join(got, ",") != "before,after,current" {
		t.Fatalf("events %v", got)
	}
	if b, err := os.ReadFile(log + ".1.migrated"); err != nil || string(b) != previous {
		t.Fatalf("archive %q/%v", b, err)
	}
}

func TestGatewaySQLiteMigrationRollsBackAndRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	for _, profile := range []string{"a", "b"} {
		raw := `{"schema":1,"id":"` + profileFilePart(profile) + `","profileId":"` + profile + `","httpPort":3804,"state":"pending"}`
		if profile == "b" {
			raw = "broken"
		}
		if err := os.WriteFile(filepath.Join(dir, profileFilePart(profile)+".json"), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := NewRegistry(dir); err == nil {
		r.Close()
		t.Fatal("accepted malformed legacy registration")
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "gateway.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	var version, tables int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if version != 0 || tables != 0 {
		t.Fatalf("partial migration version=%d tables=%d", version, tables)
	}
	native := t.TempDir()
	r, err := NewRegistry(native)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Ensure(context.Background(), "p", "fp"); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(native, "gateway.sqlite"), []byte("corrupt database"), 0600); err != nil {
		t.Fatal(err)
	}
	if r, err = NewRegistry(native); err == nil {
		r.Close()
		t.Fatal("silently recreated a corrupt native database")
	}
}
