package store

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A standby facing a write lock held by a stopped owner (frozen inside a
// write transaction) is refused, not failed: ErrOwnerLive while the
// owner's heartbeat is fresh, then ErrLocked naming the owner. The lock
// cannot be broken; once the owner resumes and commits (or exits), the
// standby takes over by the ordinary heartbeat rule.
func TestAcquireAgainstAStalledWriterIsARefusal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.sqlite")
	c := newClock()
	a := openAt(t, path, c)
	_, err := a.Acquire(Ident{Host: "host-a", PID: 100, BootID: "boot-a"})
	mustT(t, err)

	inTx, resume := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- a.Update(func(tx *Tx) error {
			close(inTx)
			<-resume // the owner is frozen here, holding the write lock
			return tx.SetMeta("k", "v")
		})
	}()
	<-inTx

	b := openAt(t, path, c) // opening needs no write lock
	_, err = b.Acquire(Ident{Host: "host-b", PID: 200, BootID: "boot-b"})
	wantErr(t, err, ErrOwnerLive)
	if !strings.Contains(err.Error(), "busy") {
		t.Fatalf("busy refusal %q does not say the store is busy", err)
	}
	c.add(OwnerStaleAfter + time.Second)
	_, err = b.Acquire(Ident{Host: "host-b", PID: 200, BootID: "boot-b"})
	wantErr(t, err, ErrLocked)
	if !strings.Contains(err.Error(), "host-a pid 100") || !strings.Contains(err.Error(), "1m31s") {
		t.Fatalf("locked refusal %q does not name the owner and its heartbeat age", err)
	}

	close(resume) // the owner resumes: its transaction commits
	mustT(t, <-done)
	gen, err := b.Acquire(Ident{Host: "host-b", PID: 200, BootID: "boot-b"})
	mustT(t, err)
	if gen != 2 {
		t.Fatalf("standby generation %d, want 2", gen)
	}
	wantErr(t, a.Fence(), ErrFenced)
}

// The owner's fence for writes outside the store (Graph). Within the lease
// it needs no write; past it, a renewal either proves ownership (no
// takeover: the owner carries on, however long it was stopped) or fences
// it for good. At no time can the owner pass the fence while another
// process could have acquired.
func TestFenceLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.sqlite")
	c := newClock()
	a := openAt(t, path, c)
	_, err := a.Acquire(Ident{Host: "host-a", PID: 100, BootID: "boot-a"})
	mustT(t, err)
	b := openAt(t, path, c)
	beat := func() int64 {
		o, _, err := a.OwnerInfo()
		mustT(t, err)
		return o.HeartbeatAt
	}

	start := beat()
	c.add(ownerLease - time.Second)
	mustT(t, a.Fence())
	if beat() != start {
		t.Fatal("a fence check within the lease wrote the heartbeat")
	}
	_, err = b.Acquire(Ident{Host: "host-b", PID: 200, BootID: "boot-b"})
	wantErr(t, err, ErrOwnerLive)

	// Stopped well past the stale window, but nobody took over: the
	// renewal succeeds and the owner is still the owner.
	c.add(10 * time.Minute)
	mustT(t, a.Fence())
	if beat() != c.now().UnixMilli() {
		t.Fatal("the fence past the lease did not renew the heartbeat")
	}
	_, err = b.Acquire(Ident{Host: "host-b", PID: 200, BootID: "boot-b"})
	wantErr(t, err, ErrOwnerLive)

	// Stopped again; this time the standby takes over.
	c.add(OwnerStaleAfter)
	_, err = b.Acquire(Ident{Host: "host-b", PID: 200, BootID: "boot-b"})
	mustT(t, err)
	wantErr(t, a.Fence(), ErrFenced)
	wantErr(t, a.Fence(), ErrFenced) // for good, without another write
	wantErr(t, a.Heartbeat(), ErrFenced)
	mustT(t, b.Fence())

	// Between the end of the lease and the stale window the fence renews,
	// so a standby is still refused.
	for _, d := range []time.Duration{ownerLease, OwnerStaleAfter - time.Second} {
		c.add(d)
		mustT(t, b.Fence())
		_, err = a.Acquire(Ident{Host: "host-a", PID: 100, BootID: "boot-a"})
		if !errors.Is(err, ErrOwnerLive) {
			t.Fatalf("acquire %s after the owner's fence: %v, want ErrOwnerLive", d, err)
		}
	}
	mustT(t, b.Release())
	wantErr(t, b.Fence(), ErrNotAcquired)
}
