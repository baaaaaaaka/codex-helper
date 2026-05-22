//go:build windows

package store

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsFileBasicInfo struct {
	CreationTime   windows.Filetime
	LastAccessTime windows.Filetime
	LastWriteTime  windows.Filetime
	ChangeTime     windows.Filetime
	FileAttributes uint32
	_              uint32
}

const windowsFileBasicInfoSize = 40

var (
	_ [windowsFileBasicInfoSize - int(unsafe.Sizeof(windowsFileBasicInfo{}))]byte
	_ [int(unsafe.Sizeof(windowsFileBasicInfo{})) - windowsFileBasicInfoSize]byte
)

func stateFileStampRevision(path string, _ os.FileInfo) (stateFileRevision, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return stateFileRevision{}, err
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return stateFileRevision{}, err
	}
	defer windows.CloseHandle(handle)

	var handleInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &handleInfo); err != nil {
		return stateFileRevision{}, err
	}
	var basicInfo windowsFileBasicInfo
	if err := windows.GetFileInformationByHandleEx(
		handle,
		windows.FileBasicInfo,
		(*byte)(unsafe.Pointer(&basicInfo)),
		uint32(unsafe.Sizeof(basicInfo)),
	); err != nil {
		return stateFileRevision{}, err
	}
	return stateFileRevision{
		Valid:             true,
		VolumeSerial:      handleInfo.VolumeSerialNumber,
		FileIndexHigh:     handleInfo.FileIndexHigh,
		FileIndexLow:      handleInfo.FileIndexLow,
		CreationTimeNanos: basicInfo.CreationTime.Nanoseconds(),
		ChangeTimeNanos:   basicInfo.ChangeTime.Nanoseconds(),
	}, nil
}
