//go:build windows

package store

import (
	"encoding/binary"
	"os"

	"golang.org/x/sys/windows"
)

// Windows FileInfo.Sys exposes only LastWriteTime. Query the per-file USN
// revision from a native handle instead: unlike mtime/basic-info timestamps,
// the USN changes for an in-place data rewrite even when a writer restores the
// visible timestamps afterwards. If the volume has no usable USN journal,
// return zero so callers that require a stable rewrite proof fail closed.
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
				return windowsFileUSN(handle)
			}
		}
	}
	return 0
}

// windowsFileUSN reads the current USN_RECORD_V2/V3 revision. The USN offset
// differs because V3 uses 128-bit file IDs; validate the record shape before
// reading it so a filesystem-specific response cannot become a false source
// revision.
func windowsFileUSN(handle windows.Handle) int64 {
	const (
		usnRecordV2HeaderBytes = 60
		usnRecordV3HeaderBytes = 76
		usnV2Offset            = 24
		usnV3Offset            = 40
	)
	// Ask for both the classic 64-bit-file-ID record and the ReFS-compatible
	// v3 record. An omitted input defaults to v2, which would make this proof
	// unavailable on a volume that uses 128-bit file IDs.
	input := [4]byte{2, 0, 3, 0}
	buffer := make([]byte, 64*1024)
	var returned uint32
	if err := windows.DeviceIoControl(
		handle,
		windows.FSCTL_READ_FILE_USN_DATA,
		&input[0],
		uint32(len(input)),
		&buffer[0],
		uint32(len(buffer)),
		&returned,
		nil,
	); err != nil || returned < usnRecordV2HeaderBytes || returned > uint32(len(buffer)) {
		return 0
	}
	recordLength := binary.LittleEndian.Uint32(buffer[0:4])
	majorVersion := binary.LittleEndian.Uint16(buffer[4:6])
	minRecordLength := uint32(usnRecordV2HeaderBytes)
	usnOffset := usnV2Offset
	if majorVersion == 3 {
		minRecordLength = usnRecordV3HeaderBytes
		usnOffset = usnV3Offset
	}
	if recordLength < minRecordLength || recordLength > returned || (majorVersion != 2 && majorVersion != 3) {
		return 0
	}
	usn := int64(binary.LittleEndian.Uint64(buffer[usnOffset : usnOffset+8]))
	if usn <= 0 {
		return 0
	}
	return usn
}
