//go:build cxp_boot_e2e && windows

package candidateupdate

import (
	"golang.org/x/sys/windows"
	"testing"
)

func holdNativeCandidateEntry(t *testing.T, path string) func() {
	t.Helper()
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	// An executing old service can hold read sharing without delete sharing.
	h, err := windows.CreateFile(p, windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	close := func() {
		if !closed {
			closed = true
			windows.CloseHandle(h)
		}
	}
	t.Cleanup(close)
	return close
}
