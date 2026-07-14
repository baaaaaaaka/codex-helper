package cli

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

// This test exercises the same Wait ownership used by proxy start and the
// desktop-app proxy launch without requiring a real SSH profile. The child
// exits immediately; the parent must still observe and reap it instead of
// abandoning the Wait call.
func TestStartDetachedProcessWaitsForExitedChild(t *testing.T) {
	if os.Getenv("CODEX_HELPER_DETACHED_WAIT_CHILD") == "1" {
		os.Exit(0)
	}
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestStartDetachedProcessWaitsForExitedChild$")
	cmd.Env = append(os.Environ(), "CODEX_HELPER_DETACHED_WAIT_CHILD=1")
	detached, err := startDetachedProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := detached.wait(3 * time.Second); err != nil {
		t.Fatalf("detached child was not reaped: %v", err)
	}
}
