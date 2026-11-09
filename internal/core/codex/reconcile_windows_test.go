//go:build windows

package codex

import (
	"os"
	"os/exec"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"golang.org/x/sys/windows"
)

// A retained handle keeps the PID openable after exit. Reconciliation must
// inspect liveness rather than treating FindProcess success as a live owner.
func TestReconcileExitedOwnerWithRetainedWindowsHandle(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestExitedOwnerFixtureChild$")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	pid := cmd.Process.Pid
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(h)
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	store := fakecodex.Store{Dir: t.TempDir()}
	if err := store.Save(&fakecodex.Thread{ID: "exited-owner", Turns: []*fakecodex.Turn{{
		ID: "turn", Status: fakecodex.StatusInProgress, OwnerPID: pid,
	}}}); err != nil {
		t.Fatal(err)
	}
	thread, err := store.Load("exited-owner")
	if err != nil {
		t.Fatal(err)
	}
	if got := thread.Turns[0].Status; got != fakecodex.StatusInterrupted {
		t.Fatalf("exited owner with retained handle: status=%s, want interrupted", got)
	}
}

func TestExitedOwnerFixtureChild(t *testing.T) {}
