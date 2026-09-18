//go:build windows

package store

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows FileInfo.Sys exposes LastWriteTime, but not the stronger
// FILE_BASIC_INFO.ChangeTime needed by the source-rewrite proof. Query the
// native handle record here. If the handle query is unavailable, return zero
// so callers that require a stable rewrite proof fail closed.
func sourceFileChangeTime(path string, _ os.FileInfo) int64 {
	if path != "" {
		if pathPtr, err := windows.UTF16PtrFromString(path); err == nil {
			handle, err := windows.CreateFile(
				pathPtr,
				windows.FILE_READ_ATTRIBUTES,
				windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
				nil,
				windows.OPEN_EXISTING,
				windows.FILE_ATTRIBUTE_NORMAL,
				0,
			)
			if err == nil {
				defer windows.CloseHandle(handle)
				var basic windowsFileBasicInfo
				if err := windows.GetFileInformationByHandleEx(
					handle,
					windows.FileBasicInfo,
					(*byte)(unsafe.Pointer(&basic)),
					uint32(unsafe.Sizeof(basic)),
				); err == nil && basic.ChangeTime != 0 {
					return basic.ChangeTime
				}
			}
		}
	}
	return 0
}

// windowsFileBasicInfo mirrors FILE_BASIC_INFO. The trailing padding is part
// of the Windows ABI's 8-byte alignment and keeps the buffer size exact for
// GetFileInformationByHandleEx.
type windowsFileBasicInfo struct {
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	ChangeTime     int64
	FileAttributes uint32
	_              uint32
}
