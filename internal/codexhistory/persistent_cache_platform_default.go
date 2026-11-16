//go:build !windows && !linux

package codexhistory

import "os"

func populatePlatformFileCacheKey(_ string, _ os.FileInfo, _ *fileCacheKey) {}
