//go:build !windows

package transcript

import (
	"os"
	"syscall"
)

func fileIdentity(_ *os.File, st os.FileInfo) (device, inode int64) {
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		return int64(sys.Dev), int64(sys.Ino)
	}
	return 0, 0
}

func statIdentity(_ string, st os.FileInfo) (device, inode int64) {
	return fileIdentity(nil, st)
}
