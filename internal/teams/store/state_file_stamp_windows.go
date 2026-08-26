//go:build windows

package store

import (
	"os"

	"golang.org/x/sys/windows"
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
	return stateFileRevision{
		Valid:             true,
		VolumeSerial:      handleInfo.VolumeSerialNumber,
		FileIndexHigh:     handleInfo.FileIndexHigh,
		FileIndexLow:      handleInfo.FileIndexLow,
		CreationTimeNanos: handleInfo.CreationTime.Nanoseconds(),
		ChangeTimeNanos:   handleInfo.LastWriteTime.Nanoseconds(),
	}, nil
}
