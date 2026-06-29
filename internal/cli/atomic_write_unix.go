//go:build !windows

package cli

import (
	"fmt"
	"os"
	"path/filepath"
)

func writeFileAtomically(path string, data []byte, perm os.FileMode) error {
	return writeFileAtomicallyForIdentity(path, data, perm, nil)
}

func writeFileAtomicallyForIdentity(path string, data []byte, perm os.FileMode, identity *execIdentity) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()

	if err := f.Chmod(perm); err != nil {
		_ = f.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := ensurePathOwnedByIdentity(tmp, identity); err != nil {
		_ = f.Close()
		return fmt.Errorf("chown temp file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := replaceStagedFile(tmp, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}

func replaceStagedFile(fromPath string, toPath string) error {
	return os.Rename(fromPath, toPath)
}
