//go:build unix

package migrate

import (
	"os"
	"syscall"
)

func inodeOf(st os.FileInfo) int64 {
	if s, ok := st.Sys().(*syscall.Stat_t); ok {
		return int64(s.Ino)
	}
	return 0
}
