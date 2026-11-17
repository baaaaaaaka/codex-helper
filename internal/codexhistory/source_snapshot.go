package codexhistory

import (
	"errors"
	"os"
)

// A cached checkpoint must describe the same snapshot that was parsed. The
// descriptor pins file identity across rename; stat also catches in-place
// writes or truncation while a bounded window was being read.
func unchangedJSONLSource(f *os.File, path string, initial os.FileInfo) error {
	current, err := f.Stat()
	if err != nil {
		return err
	}
	named, err := os.Stat(path)
	if err != nil {
		return err
	}
	key := newFileCacheKey(path, initial)
	if newFileCacheKey(path, current) != key || newFileCacheKey(path, named) != key {
		return errors.New("JSONL source changed during cache preparation")
	}
	return nil
}

func unchangedJSONLPath(path string, initial os.FileInfo) bool {
	current, err := os.Stat(path)
	return err == nil && newFileCacheKey(path, current) == newFileCacheKey(path, initial)
}
