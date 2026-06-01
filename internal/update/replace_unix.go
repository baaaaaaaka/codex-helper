//go:build !windows

package update

import (
	"io"
	"os"
)

func replaceBinary(tmpPath, destPath string, _ replaceOptions) (replaceResult, error) {
	// Before overwriting, keep a copy of the current binary as a rollback
	// target (<dest>.prev). Best-effort: a missing dest (fresh install) is fine,
	// and a failed backup must not block the update.
	backupPreviousBinary(destPath)
	if err := os.Rename(tmpPath, destPath); err != nil {
		return replaceResult{}, err
	}
	return replaceResult{restartRequired: false}, nil
}

// PreviousBinaryPath returns the path of the retained previous binary for a
// given install path, the rollback target written by backupPreviousBinary.
func PreviousBinaryPath(destPath string) string {
	return destPath + ".prev"
}

// backupPreviousBinary copies destPath to <dest>.prev so the prior version can
// be restored if the new one misbehaves. It is best-effort and never errors.
func backupPreviousBinary(destPath string) {
	info, err := os.Stat(destPath)
	if err != nil || info.IsDir() {
		return
	}
	src, err := os.Open(destPath)
	if err != nil {
		return
	}
	defer func() { _ = src.Close() }()

	backup := PreviousBinaryPath(destPath)
	tmp := backup + ".tmp"
	dst, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmp)
		return
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(tmp)
		return
	}
	_ = os.Chmod(tmp, info.Mode().Perm())
	// Atomic publish so a crash mid-copy never leaves a truncated .prev.
	if err := os.Rename(tmp, backup); err != nil {
		_ = os.Remove(tmp)
	}
}
