package machineregistry

import (
	"os"
	"path/filepath"
	"time"
)

var cacheReplaceFile = defaultCacheReplaceFile
var cacheReplaceSleep = time.Sleep

const cacheReplaceAttempts = 8

func writeCacheFileAtomically(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := cacheReplaceFile(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return os.Chmod(path, perm)
}

func cacheReplaceFileWithRetry(src string, dst string, replace func(string, string) error, retryable func(error) bool) error {
	var lastErr error
	for attempt := 0; attempt < cacheReplaceAttempts; attempt++ {
		if err := replace(src, dst); err != nil {
			lastErr = err
			if retryable == nil || !retryable(err) || attempt == cacheReplaceAttempts-1 {
				return err
			}
			cacheReplaceSleep(time.Duration(attempt+1) * 25 * time.Millisecond)
			continue
		}
		return nil
	}
	return lastErr
}

func syncCacheParentDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
