package helperruntime

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// publishProcessIdentityReady writes the child PID to a temporary file and
// publishes it with a same-directory rename. The parent must never observe a
// partially written readiness marker, which is especially important on the
// Windows hosted runner where a visible file can otherwise be read before its
// contents are complete.
func publishProcessIdentityReady(path string, pid int) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	tmp, err := os.CreateTemp(dir, base+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := fmt.Fprintf(tmp, "%s\n", strconv.Itoa(pid)); err != nil {
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
	return os.Rename(tmpPath, path)
}

func TestPublishProcessIdentityReadyAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready")
	const pid = 12345
	if err := publishProcessIdentityReady(path, pid); err != nil {
		t.Fatalf("publish readiness marker: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read readiness marker: %v", err)
	}
	if got := string(data); got != "12345\n" {
		t.Fatalf("readiness marker = %q, want %q", got, "12345\n")
	}
}
