//go:build cxp_boot_e2e && !windows

package helperruntime

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
)

func assertBootPIDStopped(t *testing.T, pid int) {
	t.Helper()
	awaitBoot(t, "boot descendant exits", func() bool {
		if err := syscall.Kill(pid, 0); err != nil {
			return true
		}
		// Linux orphan zombies may await init reaping; they cannot run.
		b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		return strings.Contains(string(b), ") Z")
	})
}
func assertBootKindStopped(t *testing.T, events, kind string) {
	for _, line := range strings.Split(events, "\n") {
		var got, v string
		var pid int
		if _, err := fmt.Sscanf(line, "%s %s %d", &got, &v, &pid); err == nil && got == kind && v == "v2.0.0" {
			assertBootPIDStopped(t, pid)
		}
	}
}
func assertBootDescendantsStopped(t *testing.T, events string) {
	assertBootKindStopped(t, events, "descendant")
}
func assertBootCandidatesStopped(t *testing.T, events string) {
	assertBootKindStopped(t, events, "entered")
}
