//go:build !windows

package codexhistory

import "os"

func replacePersistentCacheFilePlatform(src, dst string) error {
	return os.Rename(src, dst)
}
