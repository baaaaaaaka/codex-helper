//go:build linux

package candidateupdate

import (
	"fmt"
	"os"
	"path/filepath"
)

func directParentExecutable() (string, error) {
	path := fmt.Sprintf("/proc/%d/exe", os.Getppid())
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

func replaceLegacyStableEntry(candidate string, entry string, runningRuntime string, root string) error {
	target, expectedHash, err := stableReplacementTarget(entry, runningRuntime, root)
	if err != nil {
		return err
	}
	if err := copyExecutableAtomically(candidate, target, expectedHash); err != nil {
		return fmt.Errorf("replace legacy stable entry %s: %w", target, err)
	}
	return nil
}
