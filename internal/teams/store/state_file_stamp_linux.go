//go:build linux

package store

import (
	"os"
	"syscall"
)

func stateFileStampRevision(path string, _ os.FileInfo) (stateFileRevision, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return stateFileRevision{}, err
	}
	dev := uint64(stat.Dev)
	ino := uint64(stat.Ino)
	return stateFileRevision{
		Valid:             true,
		VolumeSerial:      uint32(dev),
		FileIndexHigh:     uint32(ino >> 32),
		FileIndexLow:      uint32(ino),
		CreationTimeNanos: int64(dev >> 32),
		ChangeTimeNanos:   stat.Ctim.Sec*1_000_000_000 + stat.Ctim.Nsec,
	}, nil
}
