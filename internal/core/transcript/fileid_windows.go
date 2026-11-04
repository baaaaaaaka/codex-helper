//go:build windows

package transcript

import (
	"os"
	"syscall"
)

// fileID is the NTFS file index (GetFileInformationByHandle), the Windows
// equivalent of an inode: a file replaced by rename gets a new one. The
// handle is opened without access rights and with full sharing, so Codex
// can keep writing, renaming or deleting the file.
func fileID(path string, _ os.FileInfo) int64 {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0
	}
	h, err := syscall.CreateFile(p, 0, syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return 0
	}
	defer syscall.CloseHandle(h)
	var d syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(h, &d); err != nil {
		return 0
	}
	return int64(d.FileIndexHigh)<<32 | int64(d.FileIndexLow)
}
