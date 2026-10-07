package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsSQLiteOptionalSidecarDeletePendingRechecksAbsence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.sqlite-shm")
	if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(name, windows.FILE_READ_ATTRIBUTES|windows.DELETE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if handle != windows.InvalidHandle {
			_ = windows.CloseHandle(handle)
		}
	}()
	disposition := [1]byte{1}
	if err := windows.SetFileInformationByHandle(handle, windows.FileDispositionInfo, &disposition[0], uint32(len(disposition))); err != nil {
		t.Fatal(err)
	}
	_, err = stateFileStampRevision(path, info)
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("identity of delete-pending sidecar: %T: %v", err, err)
	}
	t.Logf("identity of delete-pending sidecar: %T: %v", err, err)
	persistentReads := 0
	_, err = readOptionalSQLiteSidecarIdentity(path, func(currentPath string) (sqliteReadOnlyFileIdentity, error) {
		persistentReads++
		_, identityError := stateFileStampRevision(currentPath, info)
		return sqliteReadOnlyFileIdentity{}, identityError
	})
	if persistentReads != 3 || !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("persistent delete-pending sidecar: reads=%d err=%v", persistentReads, err)
	}
	reads := 0
	identity, err := readOptionalSQLiteSidecarIdentity(path, func(currentPath string) (sqliteReadOnlyFileIdentity, error) {
		reads++
		if reads == 1 {
			_, identityError := stateFileStampRevision(currentPath, info)
			if closeError := windows.CloseHandle(handle); closeError != nil {
				t.Fatal(closeError)
			}
			handle = windows.InvalidHandle
			return sqliteReadOnlyFileIdentity{}, identityError
		}
		return sqliteReadOnlyFileIdentityForPath(currentPath)
	})
	if err != nil || identity.Exists || reads != 2 {
		t.Fatalf("sidecar after final handle close: reads=%d identity=%+v err=%v", reads, identity, err)
	}
}
