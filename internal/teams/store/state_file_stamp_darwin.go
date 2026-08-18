//go:build darwin

package store

import (
	"os"
	"syscall"
)

// Darwin does not expose the Linux statx fields used by the other identity
// implementation, but (device, inode) is a stable identity for the open
// source file and is sufficient to reject an atomic path replacement.
func stateFileStampRevision(path string, info os.FileInfo) (stateFileRevision, error) {
	var stat syscall.Stat_t
	if native, ok := info.Sys().(*syscall.Stat_t); ok {
		stat = *native
	} else if err := syscall.Stat(path, &stat); err != nil {
		return stateFileRevision{}, err
	}
	dev := uint64(stat.Dev)
	ino := uint64(stat.Ino)
	return stateFileRevision{
		Valid:         true,
		VolumeSerial:  uint32(dev),
		FileIndexHigh: uint32(ino >> 32),
		FileIndexLow:  uint32(ino),
	}, nil
}
