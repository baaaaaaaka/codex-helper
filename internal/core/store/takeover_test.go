package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
)

type fakeLease struct {
	check, verify   error
	checks, verifys int
}

func (l *fakeLease) Check() error  { l.checks++; return l.check }
func (l *fakeLease) Verify() error { l.verifys++; return l.verify }

func TestLeaseGatesWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.sqlite")
	l := &fakeLease{}
	s, err := OpenWith(path, Options{Now: newClock().now, Lease: l})
	mustT(t, err)
	defer s.Close()
	_, err = s.Acquire(testIdent(100))
	mustT(t, err)
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("machine_label", "1") }))
	if l.checks != 2 {
		t.Fatalf("checks %d, want one per write transaction", l.checks)
	}
	// Heartbeat checks the lease and writes nothing.
	var before int64
	s.View(func(q Querier) error {
		return q.QueryRowContext(t.Context(), `SELECT heartbeat_at FROM owner`).Scan(&before)
	})
	mustT(t, s.Heartbeat())
	var after int64
	s.View(func(q Querier) error {
		return q.QueryRowContext(t.Context(), `SELECT heartbeat_at FROM owner`).Scan(&after)
	})
	if after != before {
		t.Fatal("heartbeat wrote the owner row under a lease")
	}
	mustT(t, s.Fence())
	if l.verifys != 1 {
		t.Fatalf("Fence must verify the lease (%d)", l.verifys)
	}
	l.check = lease.ErrFenced
	wantErr(t, s.Update(func(tx *Tx) error { return tx.SetMeta("machine_label", "2") }), ErrFenced)
	if s.Update(func(*Tx) error { return nil }) != ErrFenced {
		t.Fatal("the engine compares fence errors with ==")
	}
	wantErr(t, s.Heartbeat(), ErrFenced)
	l.check, l.verify = lease.ErrDisabled, lease.ErrDisabled
	if s.VacuumStep(1) != ErrDisabled {
		t.Fatal("vacuum must check the lease")
	}
	l.check, l.verify = nil, lease.ErrFenced
	if s.Fence() != ErrFenced {
		t.Fatal("fence")
	}
	if v, _, _ := s.Meta("machine_label"); v != "1" {
		t.Fatalf("meta %q: a fenced write landed", v)
	}
}

// A frozen owner's store, copied raw while it still has it open with
// committed transactions only in the WAL: the copy has every commit.
func TestCopySnapshotOfAnOpenStore(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "core.sqlite")
	s, err := OpenWith(src, Options{Now: newClock().now})
	mustT(t, err)
	defer s.Close()
	_, err = s.Acquire(testIdent(1))
	mustT(t, err)
	for i := range 50 {
		mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("machine_label", string(rune('a'+i%26))) }))
	}
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("engine_control_user_title", "yes") }))
	if fi, err := os.Stat(src + "-wal"); err != nil || fi.Size() == 0 {
		t.Fatalf("the test needs commits in the WAL: %v", err)
	}
	dst := filepath.Join(dir, "core.g2.sqlite")
	mustT(t, CopySnapshot(src, dst))
	for _, sfx := range []string{"-wal", "-shm", ".tmp", ".tmp-wal"} {
		if _, err := os.Stat(dst + sfx); err == nil {
			t.Fatalf("%s left behind", dst+sfx)
		}
	}
	c, err := OpenWith(dst, Options{Now: newClock().now})
	mustT(t, err)
	defer c.Close()
	if v, ok, err := c.Meta("engine_control_user_title"); err != nil || !ok || v != "yes" {
		t.Fatalf("copy lost a commit: %q %v %v", v, ok, err)
	}
}

// A WAL whose tail never reached the server (the old host died): the copy
// keeps the commits before the damage and drops the rest, and is a valid
// store.
func TestCopySnapshotOfATornWAL(t *testing.T) {
	dir := t.TempDir()
	live := filepath.Join(dir, "live.sqlite")
	s, err := OpenWith(live, Options{Now: newClock().now})
	mustT(t, err)
	_, err = s.Acquire(testIdent(1))
	mustT(t, err)
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("machine_id", "1") }))
	fi, _ := os.Stat(live + "-wal")
	keep := fi.Size()
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("scope_id", "2") }))
	// The host's view of the files at the moment it died.
	src := filepath.Join(dir, "core.sqlite")
	for _, sfx := range []string{"", "-wal"} {
		data, err := os.ReadFile(live + sfx)
		mustT(t, err)
		if sfx == "-wal" {
			data = data[:keep+100] // part of the second commit's first frame
		}
		mustT(t, os.WriteFile(src+sfx, data, 0o600))
	}
	s.Close()
	dst := filepath.Join(dir, "core.g2.sqlite")
	mustT(t, CopySnapshot(src, dst))
	c, err := OpenWith(dst, Options{Now: newClock().now})
	mustT(t, err)
	defer c.Close()
	if v, ok, _ := c.Meta("machine_id"); !ok || v != "1" {
		t.Fatal("lost a complete commit")
	}
	if _, ok, _ := c.Meta("scope_id"); ok {
		t.Fatal("kept a torn commit")
	}
}

func TestCopySnapshotRefusesAMissingSource(t *testing.T) {
	dir := t.TempDir()
	err := CopySnapshot(filepath.Join(dir, "core.sqlite"), filepath.Join(dir, "core.g2.sqlite"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "core.g2.sqlite")); err == nil {
		t.Fatal("created an empty store")
	}
}
