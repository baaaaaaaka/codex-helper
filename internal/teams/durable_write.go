package teams

import (
	"os"
	"path/filepath"
	"time"
)

var durableReplaceFile = defaultDurableReplaceFile
var durableReplaceSleep = time.Sleep

const durableReplaceAttempts = 8

func durableWriteFile(path string, data []byte, perm os.FileMode) error {
	if err := ensurePrivateDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
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
	if err := durableReplaceFile(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return os.Chmod(path, perm)
}

func replaceFileWithRetry(src string, dst string, replace func(string, string) error, retryable func(error) bool) error {
	var lastErr error
	for attempt := 0; attempt < durableReplaceAttempts; attempt++ {
		if err := replace(src, dst); err != nil {
			lastErr = err
			if retryable == nil || !retryable(err) || attempt == durableReplaceAttempts-1 {
				return err
			}
			durableReplaceSleep(time.Duration(attempt+1) * 25 * time.Millisecond)
			continue
		}
		return nil
	}
	return lastErr
}

func syncParentDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
