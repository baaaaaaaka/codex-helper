package store

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSQLiteOptionalSidecarIdentityRechecksTransientMetadataFailure(t *testing.T) {
	reparseError := errors.New("sqlite store path is not a regular file")
	replacement := sqliteReadOnlyFileIdentity{Exists: true, Size: 64, Revision: "replacement"}
	for _, testCase := range []struct {
		name     string
		initial  error
		identity sqliteReadOnlyFileIdentity
		final    error
	}{
		{name: "confirmed absent", initial: os.ErrNotExist},
		{name: "replacement revalidated", initial: os.ErrNotExist, identity: replacement},
		{name: "replacement reparse rejected", initial: os.ErrNotExist, final: reparseError},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reads := 0
			identity, err := readOptionalSQLiteSidecarIdentity("store.sqlite-shm", func(path string) (sqliteReadOnlyFileIdentity, error) {
				if path != "store.sqlite-shm" {
					t.Fatalf("unexpected sidecar path %q", path)
				}
				reads++
				if reads == 1 {
					return sqliteReadOnlyFileIdentity{}, testCase.initial
				}
				return testCase.identity, testCase.final
			})
			if reads != 2 || identity != testCase.identity || !errors.Is(err, testCase.final) {
				t.Fatalf("sidecar result: reads=%d identity=%+v err=%v", reads, identity, err)
			}
		})
	}
}

func TestSQLiteOptionalSidecarIdentityDoesNotSuppressPersistentErrors(t *testing.T) {
	unexpectedError := errors.New("unexpected metadata error")
	permissionReads := 1
	if runtime.GOOS == "windows" {
		permissionReads = 3
	}
	for _, testCase := range []struct {
		name  string
		err   error
		reads int
	}{
		{name: "repeated disappearance", err: os.ErrNotExist, reads: 3},
		{name: "unrelated failure", err: unexpectedError, reads: 1},
		{name: "access denied", err: os.ErrPermission, reads: permissionReads},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reads := 0
			identity, err := readOptionalSQLiteSidecarIdentity("store.sqlite-wal", func(string) (sqliteReadOnlyFileIdentity, error) {
				reads++
				return sqliteReadOnlyFileIdentity{}, testCase.err
			})
			if reads != testCase.reads || identity.Exists || !errors.Is(err, testCase.err) {
				t.Fatalf("persistent sidecar error: reads=%d identity=%+v err=%v", reads, identity, err)
			}
		})
	}
}

func TestSQLiteOptionalSidecarValidationStillRequiresMainDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.sqlite")
	if err := validateExistingSQLiteStorePath(path); err == nil {
		t.Fatal("missing main database accepted")
	}
	if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateExistingSQLiteStorePath(path); err != nil {
		t.Fatalf("absent optional sidecars rejected: %v", err)
	}
}
