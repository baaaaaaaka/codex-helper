//go:build !windows

package codexhistory

import "os"

func populatePlatformFileCacheKey(_ string, _ os.FileInfo, _ *fileCacheKey) {}
