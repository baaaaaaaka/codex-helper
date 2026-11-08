package cli

import (
	"fmt"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func TestTeamsServiceWatchdogRuntimeRefreshRecoversSharingViolation(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			testTeamsServiceWatchdogRuntimeRefreshRecovery(t, func(root string) func() {
				if legacy {
					t.Setenv(helperruntime.EnvRuntimeVersion, "")
				}
				path, err := windows.UTF16PtrFromString(filepath.Join(root, "active"))
				if err != nil {
					t.Fatal(err)
				}
				// A real exclusive handle makes ReadActive fail with a sharing violation.
				h, err := windows.CreateFile(path, windows.GENERIC_READ, 0, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
				if err != nil {
					t.Fatal(err)
				}
				return func() {
					if err := windows.CloseHandle(h); err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}
