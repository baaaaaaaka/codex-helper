//go:build !windows

package teams

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func syncRuntimeStoreRenameParents(source string, destination string) error {
	parents := []string{filepath.Dir(source), filepath.Dir(destination)}
	seen := make(map[string]struct{}, len(parents))
	for _, parent := range parents {
		parent = filepath.Clean(parent)
		if _, ok := seen[parent]; ok {
			continue
		}
		seen[parent] = struct{}{}
		dir, err := os.Open(parent)
		if err != nil {
			return fmt.Errorf("open migration parent %s: %w", parent, err)
		}
		syncErr := dir.Sync()
		closeErr := dir.Close()
		if syncErr != nil &&
			!errors.Is(syncErr, syscall.EINVAL) &&
			!errors.Is(syncErr, syscall.ENOTSUP) {
			return fmt.Errorf("sync migration parent %s: %w", parent, syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close migration parent %s: %w", parent, closeErr)
		}
	}
	return nil
}
