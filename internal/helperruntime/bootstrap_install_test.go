package helperruntime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSQLiteBootstrapPublishedLossIsNotFreshInstall(t *testing.T) {
	root := t.TempDir()
	if _, err := ReadBoot(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh install: %v", err)
	}
	if err := Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, BootFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBoot(root); !errors.Is(err, ErrBootstrapState) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lost publication could admit fresh-install fallback: %v", err)
	}
}

func TestSQLiteBootstrapExplicitMigrationDropsIncompatibleRollback(t *testing.T) {
	root := t.TempDir()
	// Frozen protocol-one evidence: do not generate this using the new writer.
	legacy := []byte(`{"schema":1,"generation":9007199254740993,"good":"v1.0.0","previous":"v0.9.0","candidate":{"version":"v2.0.0","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","attempt":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}`)
	oldPath := filepath.Join(root, "boot.json")
	if err := os.WriteFile(oldPath, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBoot(root); !errors.Is(err, ErrBootstrapState) {
		t.Fatalf("legacy runtime admission: %v", err)
	}
	source := filepath.Join(t.TempDir(), "verified")
	if err := os.WriteFile(source, []byte("verified new executable"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := installNativeBootstrap(context.Background(), root, source, "v3.0.0"); err != nil {
		t.Fatal(err)
	}
	got, err := ReadBoot(root)
	if err != nil || got.Schema != BootProtocol || got.Generation != 9007199254740994 || got.Good != "v3.0.0" || got.Previous != "" || got.Candidate != nil {
		t.Fatalf("native state %+v: %v", got, err)
	}
	if archived, err := os.ReadFile(oldPath + ".migrated"); err != nil || !bytes.Equal(archived, legacy) {
		t.Fatalf("lost original trial evidence: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("legacy not archived: %v", err)
	}
	path := filepath.Join(root, BootFile)
	before, _ := os.ReadFile(path)
	if err := installNativeBootstrap(context.Background(), root, source, "v3.0.0"); err != nil {
		t.Fatal(err)
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatal("same install rewrote native boot")
	}
	// A lost published database must never reset to the legacy version.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldPath, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	if err := installNativeBootstrap(context.Background(), root, source, "v3.0.0"); err == nil {
		t.Fatal("lost native database silently remigrated")
	}
}

func TestSQLiteBootstrapMigrationFailurePreservesLegacy(t *testing.T) {
	root := t.TempDir()
	legacy := []byte(`{"schema":1,"generation":7,"good":"v1.0.0"}`)
	path := filepath.Join(root, "boot.json")
	if err := os.WriteFile(path, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), BinaryName(runtime.GOOS))
	if err := os.WriteFile(source, []byte("verified new executable"), 0755); err != nil {
		t.Fatal(err)
	}
	old := commitBootFile
	commitBootFile = func(string, string) error { return errors.New("injected commit failure") }
	t.Cleanup(func() { commitBootFile = old })
	if err := installNativeBootstrap(context.Background(), root, source, "v3.0.0"); err == nil {
		t.Fatal("ignored failed commit")
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, legacy) {
		t.Fatal("changed legacy after failed migration")
	}
	if _, err := os.Stat(filepath.Join(root, BootFile)); !os.IsNotExist(err) {
		t.Fatalf("published failed migration: %v", err)
	}
	commitBootFile = old
	if err := installNativeBootstrap(context.Background(), root, source, "v3.0.0"); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestSQLiteBootstrapAutomaticUpdateRequiresExplicitMigration(t *testing.T) {
	c, source, digest := bootFixture(t)
	if err := os.Remove(filepath.Join(c.Root, BootFile)); err != nil {
		t.Fatal(err)
	}
	// Absence before first native publication, unlike sealed loss, is a legacy
	// installation. Neither case may arm an automatic trial.
	if err := os.Remove(filepath.Join(c.Root, BootFile) + ".published"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.Root, "active"), []byte("v1.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := StageBootCandidate(context.Background(), c, "v1.0.0", 0, source, "v2.0.0", digest); !errors.Is(err, ErrBootstrapState) {
		t.Fatalf("legacy automatic admission: %v", err)
	}
	if _, err := os.Stat(VersionPath(c.Root, "v2.0.0", runtime.GOOS)); !os.IsNotExist(err) {
		t.Fatalf("candidate published before admission: %v", err)
	}
	if _, _, err := BootSnapshot(c.Root); err == nil {
		t.Fatal("legacy download baseline admitted")
	}
	if err := Activate(c.Root, "v2.0.0"); !errors.Is(err, ErrBootstrapState) {
		t.Fatalf("legacy activation bypass: %v", err)
	}
}

func TestSQLiteBootstrapRetryAfterCommittedBeforeArchive(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(t.TempDir(), "verified")
	if err := os.WriteFile(source, []byte("new verified binary"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := installNativeBootstrap(context.Background(), root, source, "v3.0.0"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, BootFile)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the two post-commit crash windows independently of import.
	if err := os.Remove(path + ".published"); err != nil {
		t.Fatal(err)
	}
	legacy := []byte(`{"schema":1,"generation":5,"good":"v1.0.0"}`)
	if err := os.WriteFile(filepath.Join(root, "boot.json"), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	if err := installNativeBootstrap(context.Background(), root, source, "v3.0.0"); err != nil {
		t.Fatal(err)
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatal("retry changed committed selection")
	}
	if _, err := os.Stat(path + ".published"); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "boot.json.migrated")); err != nil || !bytes.Equal(got, legacy) {
		t.Fatal("lost unarchived legacy evidence", err)
	}
}
