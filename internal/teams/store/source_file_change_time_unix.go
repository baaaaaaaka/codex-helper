//go:build !windows

package store

import "os"

// Unix platforms with a usable ctime are handled by
// fileInfoChangeTimeUnixNano. This fallback keeps the platform adapter
// complete for filesystems and operating systems that do not expose one.
func sourceFileChangeTimeFromFileInfo(_ os.FileInfo) int64 {
	return 0
}
