package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

var bg = context.Background()

// clock is a settable test clock.
type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }
func newClock() *clock               { return &clock{t: time.UnixMilli(1_800_000_000_000)} }
func testIdent(pid int) Ident {
	return Ident{Host: "h1", InstallHost: "h1", PID: pid, BootID: "boot-1"}
}
func alwaysAlive(int) bool { return true }

func mustT(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("got error %v, want %v", err, target)
	}
}

// openAt opens (without acquiring) a store at path with clock c.
func openAt(t *testing.T, path string, c *clock) *Store {
	t.Helper()
	s, err := OpenWith(path, Options{Now: c.now, PIDAlive: alwaysAlive})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// openTest opens a fresh store in a temp dir and acquires it.
func openTest(t *testing.T) (*Store, *clock) {
	t.Helper()
	c := newClock()
	s := openAt(t, filepath.Join(t.TempDir(), "core.sqlite"), c)
	if _, err := s.Acquire(testIdent(100)); err != nil {
		t.Fatal(err)
	}
	return s, c
}

// newWork creates an active work conversation with a binding.
func newWork(t *testing.T, s *Store, label string) int64 {
	t.Helper()
	var id int64
	mustT(t, s.Update(func(tx *Tx) error {
		var err error
		id, err = tx.CreateConversation(NewConversation{Label: label, Kind: KindWork, Channel: "teams", ExtChatID: "chat-" + label})
		if err != nil {
			return err
		}
		return tx.Bind(Thread{ConvID: id, ThreadID: "th-" + label, Cwd: "/w", Model: "m1", Effort: "high"})
	}))
	return id
}

func prompt(id string, ts int64) InboundMsg {
	return InboundMsg{ExtMsgID: id, ExtTime: ts, Sender: SenderOwner, Kind: InPrompt, Body: "hi " + id}
}

func out(key, kind string) OutMsg { return OutMsg{DeliveryKey: key, Kind: kind, Body: "body " + key} }

// countWhere runs a scalar count query on the read pool.
func countWhere(t *testing.T, s *Store, q string, args ...any) int {
	t.Helper()
	var n int
	mustT(t, s.r.QueryRow(q, args...).Scan(&n))
	return n
}
