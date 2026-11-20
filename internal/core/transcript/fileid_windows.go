//go:build windows

package transcript

import (
	"os"
	"syscall"
)

func fileIdentity(f *os.File, _ os.FileInfo) (device, inode int64) {
	var d syscall.ByHandleFileInformation
	if f == nil || syscall.GetFileInformationByHandle(syscall.Handle(f.Fd()), &d) != nil {
		return 0, 0
	}
	return int64(d.VolumeSerialNumber), int64(d.FileIndexHigh)<<32 | int64(d.FileIndexLow)
}

func statIdentity(path string, st os.FileInfo) (device, inode int64) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0
	}
	h, err := syscall.CreateFile(p, 0, syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return 0, 0
	}
	defer syscall.CloseHandle(h)
	var d syscall.ByHandleFileInformation
	if syscall.GetFileInformationByHandle(h, &d) != nil {
		return 0, 0
	}
	return int64(d.VolumeSerialNumber), int64(d.FileIndexHigh)<<32 | int64(d.FileIndexLow)
}
