package appgateway

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRegistryEnsurePersistsStablePortOutsideLegacyConfig(t *testing.T) {
	r, err := NewRegistry(t.TempDir())
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
	r, err := NewRegistry(t.TempDir())
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

func TestRegistryFirstMigrationMayAdoptLegacyPortButReadyPortIsImmutable(t *testing.T) {
	r, err := NewRegistry(t.TempDir())
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
	r, err := NewRegistry(t.TempDir())
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
	r, err := NewRegistry(t.TempDir())
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
	r, err := NewRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AppendEvent(Event{ProfileID: "profile-a", Event: "frontend-start", HTTPPort: 3804, State: StatePending}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(r.Dir(), profileFilePart("profile-a")+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 || !bytes.Contains(b, []byte(`"event":"frontend-start"`)) || !bytes.Contains(b, []byte(`"at":`)) {
		t.Fatalf("event log = %s", b)
	}
}

func TestRegistryLoadsLastBackupAfterPrimaryCorruption(t *testing.T) {
	r, err := NewRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reg, err := r.Ensure(context.Background(), "profile-a", "fp")
	if err != nil {
		t.Fatal(err)
	}
	reg.LastError = "committed"
	if err := r.Save(reg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.path(reg.ProfileID), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered, err := r.Load(reg.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.LastError != "" {
		t.Fatalf("recovered registration unexpectedly used corrupt replacement: %#v", recovered)
	}
	if _, err := os.Stat(r.path(reg.ProfileID) + ".bak"); err != nil {
		t.Fatalf("registration backup missing: %v", err)
	}
}

func TestRegistryRejectsWhenPrimaryAndBackupAreBothCorrupt(t *testing.T) {
	r, err := NewRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reg, err := r.Ensure(context.Background(), "profile-a", "fp")
	if err != nil {
		t.Fatal(err)
	}
	reg.LastError = "committed"
	if err := r.Save(reg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.path(reg.ProfileID), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.path(reg.ProfileID)+".bak", []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Load(reg.ProfileID); err == nil {
		t.Fatal("corrupt primary and backup were accepted")
	}
}

func TestRegistryEventLogRotatesAndRemainsBounded(t *testing.T) {
	r, err := NewRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	large := strings.Repeat("x", eventLogMaxBytes/2)
	for i := 0; i < 3; i++ {
		if err := r.AppendEvent(Event{ProfileID: "profile-a", Event: "recovery-attempt", Details: large}); err != nil {
			t.Fatal(err)
		}
	}
	current, err := os.Stat(r.eventPath("profile-a"))
	if err != nil {
		t.Fatal(err)
	}
	backup, err := os.Stat(r.eventPath("profile-a") + ".1")
	if err != nil {
		t.Fatal(err)
	}
	if current.Size() > int64(eventLogMaxBytes) || backup.Size() > int64(eventLogMaxBytes) {
		t.Fatalf("event log exceeded bound: current=%d backup=%d", current.Size(), backup.Size())
	}
}

func TestRegistryConcurrentStateWritesRemainReadable(t *testing.T) {
	r, err := NewRegistry(t.TempDir())
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

func TestRegistryLeaseCanBeReacquiredAfterRelease(t *testing.T) {
	r, err := NewRegistry(t.TempDir())
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
