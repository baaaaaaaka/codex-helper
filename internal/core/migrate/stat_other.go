//go:build !unix

package migrate

import "os"

func inodeOf(os.FileInfo) int64 { return 0 }
