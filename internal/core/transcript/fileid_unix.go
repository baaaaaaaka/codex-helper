//go:build !windows

package transcript

import (
	"os"
	"syscall"
)

func fileID(_ string, st os.FileInfo) int64 {
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		return int64(sys.Ino)
	}
	return 0
}
