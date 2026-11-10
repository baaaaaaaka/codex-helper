package appgateway

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

func TestRegistryEnsurePersistsStablePortOutsideLegacyConfig(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, err := r.Ensure(context.Background(), "profile-a", "fingerprint-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.HTTPPort <= 0 || first.State != StatePending {
		t.Fatalf("first registration = %#v", first)
	}
	second, err := r.Ensure(context.Background(), "profile-a", "fingerprint-2")
	if err != nil {
		t.Fatal(err)
	}
	if second.HTTPPort != first.HTTPPort {
		t.Fatalf("stable port changed: first=%d second=%d", first.HTTPPort, second.HTTPPort)
	}
	if second.ProfileFingerprint != "fingerprint-2" {
		t.Fatalf("fingerprint = %q", second.ProfileFingerprint)
	}
	if _, err := os.Stat(filepath.Join(r.Dir(), "config.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("registry unexpectedly used legacy config path: %v", err)
	}
}

func TestRegistryRejectsCorruptAndInvalidState(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Load("missing"); !errors.Is(err, ErrRegistrationNotFound) {
		t.Fatalf("missing load error = %v", err)
	}
	if err := r.Save(Registration{Schema: CurrentSchema, ID: "id", ProfileID: "p", HTTPPort: 0, State: StatePending}); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("invalid save error = %v", err)
	}
}

func TestRegistryLoadReadOnlyDoesNotCreateStateOrWaitForLock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	if _, err := LoadReadOnly(dir, "p1"); !errors.Is(err, ErrRegistrationNotFound) {
		t.Fatalf("missing read error = %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read created state directory: %v", err)
	}
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reg, err := r.Ensure(context.Background(), "p1", "fp")
	if err != nil {
		t.Fatal(err)
	}
	lock := flock.New(r.lockPath("p1"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	before, err := os.Stat(r.path("p1"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := LoadReadOnly(r.Dir(), "p1")
	if err != nil || got.ID != reg.ID {
		t.Fatalf("snapshot = %+v/%v", got, err)
	}
	after, err := os.Stat(r.path("p1"))
	if err != nil || !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("read rewrote state: %v", err)
	}
}

func TestRegistryLoadReadOnlyValidatesStoredScope(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reg, err := r.Ensure(context.Background(), "p1", "fp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.db.Exec(`UPDATE registrations SET id='invalid-id' WHERE profile_id=?`, reg.ProfileID); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadReadOnly(r.Dir(), "p1"); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("invalid native scope accepted: %v", err)
	}
	if _, err = LoadReadOnly(r.Dir(), "other"); !errors.Is(err, ErrRegistrationNotFound) {
		t.Fatalf("wrong scope: %v", err)
	}
	var id string
	if err = r.db.QueryRow(`SELECT id FROM registrations WHERE profile_id='p1'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != "invalid-id" {
		t.Fatal("diagnostic repaired state")
	}
}

func TestRegistryFirstMigrationMayAdoptLegacyPortButReadyPortIsImmutable(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, err := r.EnsureWithPort(context.Background(), "profile-a", "fp", 3804)
	if err != nil {
		t.Fatal(err)
	}
	if first.HTTPPort != 3804 {
		t.Fatalf("initial adopted port = %d", first.HTTPPort)
	}
	if first.MigrationStage != "legacy-port-adopted" {
		t.Fatalf("migration stage = %q", first.MigrationStage)
	}
	first.State = StateReady
	first.LastReadyAt = time.Now()
	if err := r.Save(first); err != nil {
		t.Fatal(err)
	}
	second, err := r.EnsureWithPort(context.Background(), "profile-a", "fp", 2743)
	if err != nil {
		t.Fatal(err)
	}
	if second.HTTPPort != 3804 {
		t.Fatalf("ready port changed during migration: %d", second.HTTPPort)
	}
	if len(second.LegacyPorts) != 0 {
		t.Fatalf("unexpected legacy ports after immutable ready registration: %#v", second.LegacyPorts)
	}
}

func TestRegistryLeaseIsIndependentFromStateWrites(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reg, err := r.Ensure(context.Background(), "profile-a", "fp")
	if err != nil {
		t.Fatal(err)
	}
	release, err := r.AcquireLease(context.Background(), reg.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	reg.LastError = "written while daemon lease is held"
	// The assertion is about lock independence, not sub-second filesystem
	// latency. Windows CI may spend more than one second in the durable
	// fsync/atomic-replace sequence while the full repository suite is active.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Save(reg) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("state write was blocked by the daemon lease")
	}
}

func TestRegistryRejectsSecondLifetimeLease(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Ensure(context.Background(), "profile-a", "fp"); err != nil {
		t.Fatal(err)
	}
	release, err := r.AcquireLease(context.Background(), "profile-a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := r.AcquireLease(ctx, "profile-a"); !errors.Is(err, ErrRegistrationLeaseHeld) {
		t.Fatalf("second lease error = %v, want %v", err, ErrRegistrationLeaseHeld)
	}
}

func TestRegistryAppendEventIsTimestampedAndDurable(t *testing.T) {
	dir := t.TempDir()
	r, err := newTestRegistry(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.AppendEvent(Event{ProfileID: "profile-a", Event: "frontend-start", HTTPPort: 3804, State: StatePending}); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := newTestRegistry(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	var event, at string
	var port int
	if err = reopened.db.QueryRow(`SELECT event_name,at,http_port FROM gateway_events WHERE profile_id='profile-a'`).Scan(&event, &at, &port); err != nil {
		t.Fatal(err)
	}
	if event != "frontend-start" || at == "" || port != 3804 {
		t.Fatalf("event %q %q %d", event, at, port)
	}
}

func TestRegistryMigratesLegacyBackupAfterPrimaryCorruption(t *testing.T) {
	dir := t.TempDir()
	profile := "profile-a"
	path := filepath.Join(dir, profileFilePart(profile)+".json")
	v := Registration{Schema: 1, ID: profileFilePart(profile), ProfileID: profile, HTTPPort: 3804, State: StateReady, LastError: "original backup"}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+".bak", raw, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := newTestRegistry(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Load(profile)
	if err != nil || got.LastError != "original backup" {
		t.Fatalf("migration %#v %v", got, err)
	}
	if err = os.WriteFile(path, []byte("poisoned leftover"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Load(profile); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path + ".migrated"); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryRejectsWhenPrimaryAndBackupAreBothCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, profileFilePart("p")+".json")
	for _, suffix := range []string{"", ".bak"} {
		if err := os.WriteFile(path+suffix, []byte("broken"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := NewRegistry(dir); err == nil {
		r.Close()
		t.Fatal("accepted corrupt legacy state")
	}
}

func TestRegistryEventLogRotatesAndRemainsBounded(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	large := strings.Repeat("x", eventLogMaxBytes/2)
	for i := 0; i < 3; i++ {
		if err = r.AppendEvent(Event{ProfileID: "profile-a", Event: "recovery-attempt", Details: large}); err != nil {
			t.Fatal(err)
		}
	}
	var rows, batches int
	var bytes int64
	if err = r.db.QueryRow(`SELECT count(*),count(DISTINCT batch),sum(length(details)+128) FROM gateway_events`).Scan(&rows, &batches, &bytes); err != nil {
		t.Fatal(err)
	}
	if rows != 2 || batches != 2 || bytes > 2*eventLogMaxBytes {
		t.Fatalf("unbounded events rows=%d batches=%d bytes=%d", rows, batches, bytes)
	}
}

func TestRegistryConcurrentStateWritesRemainReadable(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reg, err := r.Ensure(context.Background(), "profile-a", "fp")
	if err != nil {
		t.Fatal(err)
	}
	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			copy := reg
			copy.LastError = "writer-" + strconv.Itoa(index)
			errs <- r.Save(copy)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent registry save: %v", err)
		}
	}
	if got, err := r.Load(reg.ProfileID); err != nil || got.HTTPPort != reg.HTTPPort {
		t.Fatalf("concurrent registry load = %#v/%v", got, err)
	}
}

func TestRegistryLoadHonorsRegistrationLock(t *testing.T) {
	dir := t.TempDir()
	writer, err := newTestRegistry(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := writer.Ensure(context.Background(), "profile-a", "fp")
	if err != nil {
		t.Fatal(err)
	}
	lock := flock.New(writer.lockPath(reg.ProfileID))
	locked, err := lock.TryLock()
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("failed to acquire registration lock for test")
	}
	defer func() { _ = lock.Unlock() }()

	reader, err := newTestRegistry(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, loadErr := reader.Load(reg.ProfileID)
		done <- loadErr
	}()
	<-started
	select {
	case loadErr := <-done:
		t.Fatalf("Load bypassed registration lock: %v", loadErr)
	case <-time.After(250 * time.Millisecond):
	}

	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	select {
	case loadErr := <-done:
		if loadErr != nil {
			t.Fatalf("Load after registration lock release: %v", loadErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Load did not resume after registration lock release")
	}
}

func TestRegistryLeaseCanBeReacquiredAfterRelease(t *testing.T) {
	r, err := newTestRegistry(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Ensure(context.Background(), "profile-a", "fp"); err != nil {
		t.Fatal(err)
	}
	release, err := r.AcquireLease(context.Background(), "profile-a")
	if err != nil {
		t.Fatal(err)
	}
	release()
	reacquired, err := r.AcquireLease(context.Background(), "profile-a")
	if err != nil {
		t.Fatalf("reacquire lease: %v", err)
	}
	reacquired()
}

func newTestRegistry(t *testing.T, dir string) (*Registry, error) {
	t.Helper()
	r, err := NewRegistry(dir)
	if err == nil {
		t.Cleanup(func() { r.Close() })
	}
	return r, err
}
