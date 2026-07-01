//go:build darwin

package candidateupdate

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func directParentExecutable() (string, error) {
	raw, err := unix.SysctlRaw("kern.procargs2", os.Getppid())
	if err != nil {
		return "", err
	}
	if len(raw) <= 4 {
		return "", fmt.Errorf("kern.procargs2 returned %d bytes", len(raw))
	}
	offset := 4
	for offset < len(raw) && raw[offset] != 0 {
		offset++
	}
	if offset == 4 {
		return "", fmt.Errorf("kern.procargs2 parent executable path is empty")
	}
	return filepath.Clean(string(raw[4:offset])), nil
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
