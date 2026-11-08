//go:build !windows

package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestShutdownRecordFailureResumesWork(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permissions to deny creation")
	}
	r := newRig(t)
	ctl := r.control()
	work := r.newWork(t.TempDir())
	dir := filepath.Dir(r.path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	r.ch.PostUser(ctl, "shutdown")
	r.wait("shutdown failure reply", func() bool { return len(r.own(ctl, "Shutdown failed")) == 1 })
	if _, exists, err := store.ReadStop(dir); err != nil || exists {
		t.Fatalf("failed shutdown published a stop: exists=%v err=%v", exists, err)
	}
	if r.e.stopping.Load() {
		t.Fatal("failed shutdown left the engine stopping")
	}
	r.e.mu.Lock()
	prefix := r.e.stopReply
	r.e.mu.Unlock()
	if prefix != "" {
		t.Fatalf("failed shutdown still protects reply %q", prefix)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	r.ch.PostUser(work, "work after failed shutdown")
	r.wait("work resumes", func() bool { return len(r.own(work, "ANSWER to work after failed shutdown")) == 1 })
}
