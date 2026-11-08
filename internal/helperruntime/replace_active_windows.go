//go:build windows

package helperruntime

import (
	"fmt"
	"os"
	"path/filepath"
)

func replaceActiveFile(from string, to string) error {
	dir := filepath.Dir(from)
	name, err := filepath.Rel(dir, to)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	// Root.Rename uses Windows POSIX replacement semantics, so a reader that
	// shares delete access can keep reading the old file after activation.
	if err := root.Rename(filepath.Base(from), name); err != nil {
		return fmt.Errorf("replace active runtime pointer: %w", err)
	}
	return nil
}
