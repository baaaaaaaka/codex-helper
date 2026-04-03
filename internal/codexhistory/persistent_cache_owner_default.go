//go:build !windows

package codexhistory

import "os"

func populatePlatformPersistentCacheOwnerID(_ string, _ os.FileInfo) (string, bool) {
	return "", false
}
