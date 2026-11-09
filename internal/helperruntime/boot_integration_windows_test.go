//go:build cxp_boot_e2e && windows

package helperruntime

import (
	"fmt"
	"golang.org/x/sys/windows"
	"strings"
	"testing"
)

func assertBootKindStopped(t *testing.T, events, kind string) {
	for _, line := range strings.Split(events, "\n") {
		var got, v string
		var pid int
		if _, err := fmt.Sscanf(line, "%s %s %d", &got, &v, &pid); err == nil && got == kind && v == "v2.0.0" {
			awaitBoot(t, "boot descendant exits", func() bool {
				h, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
				if err != nil {
					return true
				}
				defer windows.CloseHandle(h)
				status, _ := windows.WaitForSingleObject(h, 0)
				return status == windows.WAIT_OBJECT_0
			})
		}
	}
}
func assertBootDescendantsStopped(t *testing.T, events string) {
	assertBootKindStopped(t, events, "descendant")
}
func assertBootCandidatesStopped(t *testing.T, events string) {
	assertBootKindStopped(t, events, "entered")
}
