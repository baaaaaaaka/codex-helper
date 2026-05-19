package helperpath

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func hostTestOptions() Options {
	return Options{GOOS: runtime.GOOS}
}

func hostTestHelperBinaryName() string {
	return BinaryName(runtime.GOOS)
}

func hostTestCXPBinaryName() string {
	if runtime.GOOS == "windows" {
		return "cxp.exe"
	}
	return "cxp"
}

func TestClassifyTransientHelperPaths(t *testing.T) {
	tests := []struct {
		name string
		path string
		kind Kind
	}{
		{name: "nfs silly rename", path: "/home/me/bin/.nfs802014de01c482a800000492", kind: KindNFSSilly},
		{name: "deleted", path: "/home/me/bin/codex-proxy (deleted)", kind: KindDeleted},
		{name: "reload backup", path: "/home/me/bin/codex-proxy.reload-backup-123-456", kind: KindReloadBackup},
		{name: "go build", path: "/tmp/go-build123/b001/exe/codex-proxy", kind: KindGoBuildTemp},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.path)
			if got.Kind != tc.kind || !got.Transient {
				t.Fatalf("Classify(%q) = %#v, want transient %s", tc.path, got, tc.kind)
			}
		})
	}
}

func TestClassifyInvalidNFSSillyNameAsStable(t *testing.T) {
	got := ClassifyPath("/home/me/bin/.nfs-not-hex")
	if got.Kind != KindStable || got.Transient {
		t.Fatalf("invalid .nfs name classified as %#v, want stable", got)
	}
}

func TestStableInstallTargetRules(t *testing.T) {
	dir := t.TempDir()
	opts := hostTestOptions()
	stable := filepath.Join(dir, hostTestHelperBinaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	runningNFS := filepath.Join(dir, ".nfs802014de01c482a800000492")

	got, err := StableInstallTarget("", "", runningNFS, opts)
	if err != nil {
		t.Fatalf("StableInstallTarget os executable: %v", err)
	}
	if got.Path != stable || !got.Recovered {
		t.Fatalf("resolved = %#v, want recovered stable %q", got, stable)
	}

	if _, err := StableInstallTarget(runningNFS, "", "", opts); err == nil {
		t.Fatal("explicit .nfs install path should fail closed")
	}

	got, err = StableInstallTarget(dir, "", "", opts)
	if err != nil {
		t.Fatalf("StableInstallTarget dir: %v", err)
	}
	if got.Path != stable {
		t.Fatalf("dir install target = %q, want %q", got.Path, stable)
	}

	custom := filepath.Join(dir, "codex-proxy-canary")
	got, err = StableInstallTarget(custom, "", "", opts)
	if err != nil {
		t.Fatalf("custom file target: %v", err)
	}
	if got.Path != custom {
		t.Fatalf("custom target = %q, want %q", got.Path, custom)
	}
}

func TestProbePathSeparatesClassificationFromFilesystem(t *testing.T) {
	dir := t.TempDir()
	opts := hostTestOptions()
	stable := filepath.Join(dir, hostTestHelperBinaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	probe := ProbePath(stable, opts)
	if probe.Kind != KindStable || !probe.Exists || probe.IsDir || !probe.Executable || !probe.PlausibleHelperEntry {
		t.Fatalf("stable probe = %#v", probe)
	}

	nfs := ProbePath(filepath.Join(dir, ".nfs802014de01c482a800000492"), opts)
	if nfs.Kind != KindNFSSilly || !nfs.Transient || nfs.Exists {
		t.Fatalf("nfs probe = %#v, want transient without implicit sibling probing", nfs)
	}

	custom := filepath.Join(dir, "codex-proxy-canary")
	if err := os.WriteFile(custom, []byte("custom"), 0o755); err != nil {
		t.Fatalf("write custom: %v", err)
	}
	customProbe := ProbePath(custom, opts)
	if !customProbe.Exists || !customProbe.Executable || customProbe.PlausibleHelperEntry {
		t.Fatalf("custom probe = %#v, want executable but not known entry basename", customProbe)
	}
}

func TestStableInstallTargetSourcePolicy(t *testing.T) {
	dir := t.TempDir()
	opts := hostTestOptions()
	stable := filepath.Join(dir, hostTestHelperBinaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	runningNFS := filepath.Join(dir, ".nfs802014de01c482a800000492")

	if _, err := StableInstallTargetFromSources(runningNFS, "", stable, stable, opts); err == nil {
		t.Fatal("explicit transient install path should fail closed without argv0 fallback")
	}

	got, err := StableInstallTargetFromSources("", dir, runningNFS, "", opts)
	if err != nil {
		t.Fatalf("env dir target: %v", err)
	}
	if got.Source != SourceEnv || got.Path != stable {
		t.Fatalf("env dir target = %#v, want %s from env", got, stable)
	}

	got, err = StableInstallTargetFromSources("", "", filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492"), stable, opts)
	if err != nil {
		t.Fatalf("argv0 fallback: %v", err)
	}
	if got.Source != SourceArgv0 || got.Path != stable {
		t.Fatalf("argv0 fallback = %#v, want %s", got, stable)
	}

	if _, err := StableInstallTargetFromSources("", "", filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492"), filepath.Base(stable), opts); err == nil {
		t.Fatal("relative argv0 fallback should be rejected")
	}

	badBase := filepath.Join(dir, "not-codex")
	if err := os.WriteFile(badBase, []byte("bad"), 0o755); err != nil {
		t.Fatalf("write bad base: %v", err)
	}
	if _, err := StableInstallTargetFromSources("", "", filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492"), badBase, opts); err == nil {
		t.Fatal("argv0 fallback with an unknown helper basename should be rejected")
	}
}

func TestStableRunnablePathSourcePolicy(t *testing.T) {
	dir := t.TempDir()
	opts := hostTestOptions()
	stable := filepath.Join(dir, hostTestCXPBinaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	got, err := StableRunnablePathFromSources(filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492"), stable, opts)
	if err != nil {
		t.Fatalf("argv0 runnable fallback: %v", err)
	}
	if got.Source != SourceArgv0 || got.Path != stable {
		t.Fatalf("runnable fallback = %#v, want argv0 %s", got, stable)
	}
}

func TestWindowsExecutableProbeIgnoresModeBits(t *testing.T) {
	dir := t.TempDir()
	stable := filepath.Join(dir, "codex-proxy.exe")
	if err := os.WriteFile(stable, []byte("stable"), 0o644); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	probe := ProbePath(stable, Options{GOOS: "windows"})
	if !probe.Exists || !probe.Executable || !probe.PlausibleHelperEntry {
		t.Fatalf("windows probe = %#v, want executable despite unix mode bits", probe)
	}
}

func TestStableRunnablePathFailsClosedForUnrecoverableTransient(t *testing.T) {
	_, err := StableRunnablePath(filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492"), hostTestOptions())
	if err == nil || !strings.Contains(err.Error(), "cannot recover") {
		t.Fatalf("StableRunnablePath error = %v, want unrecoverable transient", err)
	}
}

func TestStableRunnablePathRejectsNonExecutableRecoveredSibling(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("unix executable bits are not meaningful on this platform")
	}
	dir := t.TempDir()
	stable := filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(stable, []byte("stable"), 0o644); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	_, err := StableRunnablePath(filepath.Join(dir, ".nfs802014de01c482a800000492"), Options{GOOS: "linux"})
	if err == nil || !strings.Contains(err.Error(), "cannot recover") {
		t.Fatalf("StableRunnablePath error = %v, want non-executable sibling rejected", err)
	}
}

func TestStableRunnablePathAcceptsSymlinkToHelper(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("symlink test is Unix-focused")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(target, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(dir, "cxp")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	got, err := StableRunnablePath(link, Options{GOOS: "linux"})
	if err != nil {
		t.Fatalf("StableRunnablePath symlink: %v", err)
	}
	if got.Path != link {
		t.Fatalf("symlink runnable path = %q, want link path %q", got.Path, link)
	}
}

func TestStableRunnablePathRecoversDeletedExecutable(t *testing.T) {
	dir := t.TempDir()
	opts := hostTestOptions()
	stable := filepath.Join(dir, hostTestHelperBinaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	got, err := StableRunnablePath(stable+" (deleted)", opts)
	if err != nil {
		t.Fatalf("StableRunnablePath deleted: %v", err)
	}
	if got.Path != stable || !got.Recovered {
		t.Fatalf("resolved = %#v, want recovered stable %q", got, stable)
	}
}

func TestCanonicalOwnerExecutableRecoversNFSSibling(t *testing.T) {
	dir := t.TempDir()
	opts := hostTestOptions()
	stable := filepath.Join(dir, hostTestHelperBinaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	running := filepath.Join(dir, ".nfs802014de01c482a800000492")
	if got := CanonicalOwnerExecutable(running, opts); got != stable {
		t.Fatalf("CanonicalOwnerExecutable = %q, want %q", got, stable)
	}
}
