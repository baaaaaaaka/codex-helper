package update

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestFindPendingReplacementsForPlatformSortsHighestAndParsesWindowsAssets(t *testing.T) {
	dir := t.TempDir()
	installPath := filepath.Join(dir, "codex-proxy.exe")
	files := []string{
		".codex-proxy_0.1.0-rc.70_windows_amd64.exe.3939715129",
		".codex-proxy_0.1.0-rc.73_windows_amd64.exe.347704575",
		".codex-proxy_0.1.0-rc.72_windows_amd64.exe.3261172515",
		".codex-proxy_0.1.0-rc.71_linux_amd64.ignored",
		"codex-proxy.exe",
	}
	for i, name := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		mod := time.Date(2026, 5, 13, 8+i, 0, 0, 0, time.UTC)
		if strings.Contains(name, "rc.73") {
			mod = mod.Add(24 * time.Hour)
		}
		if strings.Contains(name, "rc.72") {
			mod = mod.Add(48 * time.Hour)
		}
		if err := os.Chtimes(path, mod, mod); err != nil {
			t.Fatalf("chtimes %s: %v", name, err)
		}
	}

	got, err := FindPendingReplacementsForPlatform(installPath, "windows", "amd64")
	if err != nil {
		t.Fatalf("FindPendingReplacementsForPlatform error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("pending count = %d, want 3: %#v", len(got), got)
	}
	if got[0].Version != "0.1.0-rc.73" || got[1].Version != "0.1.0-rc.72" || got[2].Version != "0.1.0-rc.70" {
		t.Fatalf("pending order = %#v, want highest comparable versions first", got)
	}
}

func TestProbeBinaryVersionParsesCodexProxyVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script probe fixture is POSIX-only")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho 'codex-proxy version 0.1.0-rc.73 (abc) 2026-05-13T00:00:00Z'\n"), 0o700); err != nil {
		t.Fatalf("write probe fixture: %v", err)
	}
	got, err := ProbeBinaryVersion(context.Background(), path, time.Second)
	if err != nil {
		t.Fatalf("ProbeBinaryVersion error: %v", err)
	}
	if got.Version != "0.1.0-rc.73" {
		t.Fatalf("version = %q, want rc73; output=%q", got.Version, got.Output)
	}
}
