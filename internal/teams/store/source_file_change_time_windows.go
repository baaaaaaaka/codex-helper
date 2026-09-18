//go:build windows

package store

import (
	"os"
	"syscall"
)

// Windows exposes the writable-file revision as LastWriteTime rather than a
// Ctimespec field on FileInfo.Sys. Keep this adapter next to the other
// platform-specific file-stamp code so the source-rewrite proof uses the same
// revision as stateFileStampRevision.
func sourceFileChangeTimeFromFileInfo(info os.FileInfo) int64 {
	if info == nil || info.Sys() == nil {
		return 0
	}
	native, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok || native == nil {
		return 0
	}
	return native.LastWriteTime.Nanoseconds()
}
