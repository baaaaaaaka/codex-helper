//go:build windows

package store

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestWindowsReplaceRetryableErrors(t *testing.T) {
	for _, errno := range []syscall.Errno{5, 32, 33} {
		if !windowsReplaceRetryable(errno) {
			t.Fatalf("windowsReplaceRetryable(%v) = false, want true", errno)
		}
	}
	if windowsReplaceRetryable(syscall.Errno(2)) {
		t.Fatal("windowsReplaceRetryable(ERROR_FILE_NOT_FOUND) = true, want false")
	}
	if windowsReplaceRetryable(errors.New("access denied")) {
		t.Fatal("windowsReplaceRetryable(non-errno) = true, want false")
	}
}

func TestWindowsAtomicWriteFileRetriesLockedTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	handle, err := syscall.CreateFile(
		syscall.StringToUTF16Ptr(path),
		syscall.GENERIC_READ,
		0,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatalf("lock target: %v", err)
	}
	released := make(chan struct{})
	go func() {
		time.Sleep(75 * time.Millisecond)
		_ = syscall.CloseHandle(handle)
		close(released)
	}()
	err = atomicWriteFile(path, []byte("new"), 0o600)
	<-released
	if err != nil {
		t.Fatalf("atomicWriteFile locked target: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("target data = %q, want new", data)
	}
}
