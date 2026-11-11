package cli

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRepairHelperInstallSelfLoopPreservesPermissionError(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires POSIX unprivileged permission checks")
	}
	dir := t.TempDir()
	locked := filepath.Join(dir, "locked")
	if err := os.Mkdir(locked, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(locked, "cxp")
	if err := os.WriteFile(target, []byte("binary"), 0755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "codex-proxy")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(locked, 0700)
	repaired, err := repairHelperInstallSelfLoopFromPrevious(alias, runtime.GOOS)
	if repaired || !errors.Is(err, os.ErrPermission) {
		t.Fatalf("permission failure misclassified: %v %v", repaired, err)
	}
	if got, err := os.Readlink(alias); err != nil || got != target {
		t.Fatalf("alias changed: %s %v", got, err)
	}
}

func TestRepairHelperInstallSelfLoopBoundaries(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlink layout")
	}
	for _, kind := range []string{"dangling", "self", "two-links", "unusable-previous", "runnable-previous"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "codex-proxy")
			target := path
			if kind == "dangling" {
				target = path + ".missing"
			}
			if kind == "two-links" {
				target = path + ".other"
				if err := os.Symlink(path, target); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
			if kind == "unusable-previous" || kind == "runnable-previous" {
				body := "not executable"
				if kind == "runnable-previous" {
					body = upgradeCXPShimTestScript("1.2.4")
				}
				if err := os.WriteFile(path+".prev", []byte(body), 0755); err != nil {
					t.Fatal(err)
				}
			}
			repaired, err := repairHelperInstallSelfLoopFromPrevious(path, runtime.GOOS)
			if kind == "runnable-previous" {
				if !repaired || err != nil {
					t.Fatalf("repair: %v %v", repaired, err)
				}
				if _, err := os.Readlink(path); err == nil {
					t.Fatal("cycle not replaced")
				}
				return
			}
			if kind == "dangling" {
				if repaired || err != nil {
					t.Fatalf("dangling link: %v %v", repaired, err)
				}
			} else if !repaired || err == nil || !strings.Contains(err.Error(), "previous binary") {
				t.Fatalf("cycle without usable previous: %v %v", repaired, err)
			}
			if got, err := os.Readlink(path); err != nil || got != target {
				t.Fatalf("changed link: %q %v", got, err)
			}
		})
	}
}

// A healthy alias produced by entrypoint repair must not need a .prev file.
func TestRepairHelperInstallSelfLoopPreservesValidSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlink layout")
	}
	for _, relative := range []bool{false, true} {
		name := "absolute"
		if relative {
			name = "relative"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			entry := filepath.Join(dir, "cxp")
			alias := filepath.Join(dir, "codex-proxy")
			if err := os.WriteFile(entry, []byte("regular stable entry fixture"), 0755); err != nil {
				t.Fatal(err)
			}
			target := entry
			if relative {
				target = "cxp"
			}
			if err := os.Symlink(target, alias); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(alias); err != nil {
				t.Fatalf("fixture must resolve: %v", err)
			}
			repaired, err := repairHelperInstallSelfLoopFromPrevious(alias, runtime.GOOS)
			if repaired || err != nil {
				t.Fatalf("healthy symlink treated as loop: repaired=%v err=%v", repaired, err)
			}
			if got, err := os.Readlink(alias); err != nil || got != target {
				t.Fatalf("alias changed: %q %v", got, err)
			}
		})
	}
}

// The producer and consumer must agree on a layout made by our own repair.
func TestRepairHelperInstallAcceptsGeneratedAlias(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlink layout")
	}
	dir := t.TempDir()
	entry, alias := filepath.Join(dir, "cxp"), filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(entry, []byte(upgradeCXPShimTestScript("1.2.4")), 0755); err != nil {
		t.Fatal(err)
	}
	if err := repairHelperEntrypointAlias(entry, helperEntrypointAlias{path: alias, create: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Readlink(alias); err != nil {
		t.Fatalf("producer did not make an alias: %v", err)
	}
	repaired, err := repairHelperInstallSelfLoopFromPrevious(alias, runtime.GOOS)
	if repaired || err != nil {
		t.Fatalf("consumer rejected generated alias: repaired=%v err=%v", repaired, err)
	}
}

func TestRepairHelperInstallDoesNotReplaceHealthyAliasFromPrevious(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlink layout")
	}
	dir := t.TempDir()
	entry, alias := filepath.Join(dir, "cxp"), filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(entry, []byte(upgradeCXPShimTestScript("1.2.4")), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(entry, alias); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(alias+".prev", []byte(upgradeCXPShimTestScript("1.2.3")), 0755); err != nil {
		t.Fatal(err)
	}
	repaired, err := repairHelperInstallSelfLoopFromPrevious(alias, runtime.GOOS)
	_, readErr := os.Readlink(alias)
	if repaired || err != nil || readErr != nil {
		t.Fatalf("healthy alias was replaced from .prev: repaired=%v err=%v readlink=%v", repaired, err, readErr)
	}
}
