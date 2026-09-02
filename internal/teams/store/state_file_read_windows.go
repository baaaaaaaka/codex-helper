//go:build windows

package store

import (
	"os"

	"golang.org/x/sys/windows"
)

// openRuntimeStateFile opens the small JSON pointer with delete sharing. The
// liveness path deliberately does not take Store.mu, so a concurrent normal
// writer may atomically replace state.json while this read is in progress.
// Go's default Windows file open does not guarantee the delete sharing needed
// by MoveFileEx; use an explicit read-only handle for this narrow path.
func openRuntimeStateFile(path string) (*os.File, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, os.ErrInvalid
	}
	return file, nil
}
