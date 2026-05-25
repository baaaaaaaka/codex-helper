package store

import (
	"os"
	"path/filepath"
	"time"
)

var durableReplaceFile = defaultDurableReplaceFile
var durableReplaceSleep = time.Sleep

const durableReplaceAttempts = 8

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
