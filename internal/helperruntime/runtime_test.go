package helperruntime

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
)

func TestNormalizeVersion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   string
		want string
		ok   bool
	}{
		{in: "0.1.13", want: "v0.1.13", ok: true},
		{in: "v0.1.13-rc.28 (commit)", want: "v0.1.13-rc.28", ok: true},
		{in: "dev", ok: false},
		{in: "../escape", ok: false},
	} {
		got, ok := NormalizeVersion(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("NormalizeVersion(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestLauncherEnvironmentRemovesRuntimeMarkersCaseInsensitively(t *testing.T) {
	t.Parallel()
	got := LauncherEnvironment([]string{
		"PATH=/bin",
		"CXP_RUNTIME=1",
		"cxp_runtime_root=/tmp/runtime",
		"Cxp_Runtime_Version=v1.2.3",
		"Cxp_Entry_Path=/tmp/cxp",
		"KEEP=yes",
	})
	want := []string{"PATH=/bin", "KEEP=yes"}
	if !slices.Equal(got, want) {
		t.Fatalf("LauncherEnvironment = %#v, want %#v", got, want)
	}
}

func TestCurrentKeepsLaunchedVersionAfterConcurrentActivation(t *testing.T) {
	root := t.TempDir()
	if err := Activate(root, "v2.0.0"); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvRuntime, "1")
	t.Setenv(EnvRuntimeRoot, root)
	t.Setenv(EnvRuntimeVersion, "v1.0.0")
	t.Setenv(EnvEntryPath, filepath.Join(filepath.Dir(root), BinaryName(runtime.GOOS)))

	current, ok := Current()
	if !ok {
		t.Fatal("Current rejected a pinned runtime context")
	}
	if current.Version != "v1.0.0" {
		t.Fatalf("current version = %q, want the launched v1.0.0 rather than active v2.0.0", current.Version)
	}
	wantPath := VersionPath(root, "v1.0.0", runtime.GOOS)
	if current.RuntimePath != wantPath {
		t.Fatalf("current runtime path = %q, want %q", current.RuntimePath, wantPath)
	}
}

func TestInstallVersionAndActivate(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	source := filepath.Join(root, "legacy-entry")
	writeExecutable(t, source, "v1")

	target, err := InstallVersion(filepath.Join(root, ".cxp-runtime"), source, "v1.2.3", runtime.GOOS, runtime.GOOS != "windows")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(target) != BinaryName(runtime.GOOS) {
		t.Fatalf("target = %q", target)
	}
	if err := Activate(filepath.Join(root, ".cxp-runtime"), "v1.2.3"); err != nil {
		t.Fatal(err)
	}
	active, err := ReadActive(filepath.Join(root, ".cxp-runtime"))
	if err != nil || active != "v1.2.3" {
		t.Fatalf("ReadActive = %q, %v", active, err)
	}
	if runtime.GOOS != "windows" {
		sourceInfo, _ := os.Stat(source)
		targetInfo, _ := os.Stat(target)
		if !os.SameFile(sourceInfo, targetInfo) {
			t.Fatal("initial Unix runtime should use a zero-copy hardlink")
		}
	}
}

func TestInstallVersionRejectsDifferentImmutableContent(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), ".cxp-runtime")
	first := filepath.Join(t.TempDir(), "first")
	second := filepath.Join(t.TempDir(), "second")
	writeExecutable(t, first, "first")
	writeExecutable(t, second, "second")
	if _, err := InstallVersion(root, first, "v1.2.3", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallVersion(root, second, "v1.2.3", runtime.GOOS, false); err == nil {
		t.Fatal("expected immutable version conflict")
	}
}

func TestPublishDownloadedSwitchesOnlyAfterPublish(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), ".cxp-runtime")
	v1 := filepath.Join(t.TempDir(), "v1")
	writeExecutable(t, v1, "v1")
	if _, err := InstallVersion(root, v1, "v1.0.0", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(root, "candidate")
	writeExecutable(t, candidate, "v2")
	target, err := PublishDownloaded(root, candidate, "v2.0.0", runtime.GOOS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(candidate); !os.IsNotExist(err) {
		t.Fatalf("candidate still exists: %v", err)
	}
	active, err := ReadActive(root)
	if err != nil || active != "v2.0.0" {
		t.Fatalf("ReadActive = %q, %v", active, err)
	}
}

func TestEnsureStableEntryPreservesExistingUserSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics are covered by native Windows CI")
	}
	t.Parallel()
	dir := t.TempDir()
	source := filepath.Join(dir, "legacy")
	entry := filepath.Join(dir, "cxp")
	writeExecutable(t, source, "source")
	if err := os.Symlink("legacy", entry); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Readlink(entry)
	if err := EnsureStableEntry(source, entry, runtime.GOOS); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Readlink(entry)
	if before != after {
		t.Fatalf("user symlink changed: %q -> %q", before, after)
	}
}

func TestEnsureStableEntryPreservesMultiHopUserSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics are covered by native Windows CI")
	}
	t.Parallel()
	dir := t.TempDir()
	physical := filepath.Join(dir, "physical")
	hopDir := filepath.Join(dir, "hop")
	if err := os.MkdirAll(physical, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(hopDir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(physical, "legacy")
	writeExecutable(t, source, "source")
	secondHop := filepath.Join(hopDir, "second")
	entry := filepath.Join(dir, "cxp")
	if err := os.Symlink(source, secondHop); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("hop", "second"), entry); err != nil {
		t.Fatal(err)
	}
	entryBefore, _ := os.Readlink(entry)
	hopBefore, _ := os.Readlink(secondHop)
	if err := EnsureStableEntry(source, entry, runtime.GOOS); err != nil {
		t.Fatal(err)
	}
	entryAfter, _ := os.Readlink(entry)
	hopAfter, _ := os.Readlink(secondHop)
	if entryBefore != entryAfter || hopBefore != hopAfter {
		t.Fatalf("multi-hop user topology changed: entry %q -> %q, hop %q -> %q", entryBefore, entryAfter, hopBefore, hopAfter)
	}
}

func TestPublishDownloadedConcurrentSameVersionIsIdempotent(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), ".cxp-runtime")
	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		candidate := filepath.Join(t.TempDir(), fmt.Sprintf("candidate-%02d", i))
		writeExecutable(t, candidate, "identical-runtime")
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			_, err := PublishDownloaded(root, path, "v1.2.3", runtime.GOOS)
			errs <- err
		}(candidate)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	active, err := ReadActive(root)
	if err != nil || active != "v1.2.3" {
		t.Fatalf("ReadActive = %q, %v", active, err)
	}
	target := VersionPath(root, active, runtime.GOOS)
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "identical-runtime" {
		t.Fatalf("published runtime = %q", data)
	}
}

func TestPublishDownloadedKeepsPreviousRuntimeAvailable(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), ".cxp-runtime")
	v1 := filepath.Join(t.TempDir(), "v1")
	v2 := filepath.Join(t.TempDir(), "v2")
	writeExecutable(t, v1, "v1")
	writeExecutable(t, v2, "v2")
	v1Target, err := PublishDownloaded(root, v1, "v1.0.0", runtime.GOOS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PublishDownloaded(root, v2, "v2.0.0", runtime.GOOS); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(v1Target)
	if err != nil {
		t.Fatalf("previous runtime disappeared after activation: %v", err)
	}
	if string(data) != "v1" {
		t.Fatalf("previous runtime changed after activation: %q", data)
	}
}

func TestCurrentRejectsUnsafeActive(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "active"), []byte("../escape\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvRuntime, "1")
	t.Setenv(EnvRuntimeRoot, root)
	t.Setenv(EnvRuntimeVersion, "")
	t.Setenv(EnvEntryPath, filepath.Join(root, BinaryName(runtime.GOOS)))
	if _, ok := Current(); ok {
		t.Fatal("unsafe active pointer was accepted")
	}
}

func writeExecutable(t *testing.T, path string, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}
