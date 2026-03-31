//go:build windows

package codexhistory

import (
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/windows"
)

func populatePlatformFileCacheKey(path string, info os.FileInfo, key *fileCacheKey) {
	if key == nil {
		return
	}
	if attr, ok := info.Sys().(*syscall.Win32FileAttributeData); ok {
		key.HasCtime = true
		key.CtimeUnixNano = attr.CreationTime.Nanoseconds()
	}

	cleanPath := filepath.Clean(path)
	p, err := windows.UTF16PtrFromString(cleanPath)
	if err != nil {
		return
	}
	handle, err := windows.CreateFile(
		p,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return
	}
	defer windows.CloseHandle(handle)

	var infoByHandle windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &infoByHandle); err != nil {
		return
	}
	key.HasFileID = true
	key.Dev = uint64(infoByHandle.VolumeSerialNumber)
	key.Ino = uint64(infoByHandle.FileIndexHigh)<<32 | uint64(infoByHandle.FileIndexLow)
}
