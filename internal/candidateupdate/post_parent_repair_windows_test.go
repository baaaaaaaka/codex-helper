//go:build windows

package candidateupdate

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
)

func TestMain(m *testing.M) {
	if exitCode, handled := HandlePostParentRepairCommand(os.Args, os.Stdout, os.Stderr); handled {
		os.Exit(exitCode)
	}
	os.Exit(m.Run())
}

func TestScheduleLegacyDirectSelfUpdateRepairUsesPublishedRuntime(t *testing.T) {
	clearPostParentRuntimeEnvironment(t)
	dir := t.TempDir()
	installDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(installDir, ".codex-proxy_0.1.13-rc.54_windows_amd64.exe.123")
	stable := filepath.Join(installDir, "cxp.exe")
	managed := filepath.Join(installDir, "codex-proxy.exe")
	writePostParentTestFile(t, candidate, []byte("new-candidate"))
	writePostParentTestFile(t, stable, []byte("old-launcher"))
	writePostParentTestFile(t, managed, []byte("old-launcher"))
	writePostParentTestFile(t, filepath.Join(installDir, "cxp.cmd"), []byte("@echo off\r\n\"%~dp0cxp.exe\" %*\r\n"))

	runtimeRoot := filepath.Join(installDir, ".cxp-runtime")
	runtimePath, err := helperruntime.InstallVersion(runtimeRoot, candidate, "v0.1.13-rc.54", "windows", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(runtimeRoot, "v0.1.13-rc.54"); err != nil {
		t.Fatal(err)
	}
	writeManagedPostParentRecord(t, dir, managed)

	previousExecutable := legacyDirectExecutablePath
	previousParent := legacyDirectParentExecutable
	previousVersion := legacyDirectParentVersion
	previousSchedule := legacyDirectScheduleRepair
	t.Cleanup(func() {
		legacyDirectExecutablePath = previousExecutable
		legacyDirectParentExecutable = previousParent
		legacyDirectParentVersion = previousVersion
		legacyDirectScheduleRepair = previousSchedule
	})
	legacyDirectExecutablePath = func() (string, error) { return candidate, nil }
	legacyDirectParentExecutable = func() (string, error) { return stable, nil }
	legacyDirectParentVersion = func(string) (string, error) { return "v0.1.12", nil }
	var gotSource string
	var gotRequest postParentRepairRequest
	legacyDirectScheduleRepair = func(source string, request postParentRepairRequest) error {
		gotSource = source
		gotRequest = request
		return nil
	}

	if err := ScheduleLegacyDirectSelfUpdateRepair([]string{candidate, "--version"}, "0.1.13-rc.54"); err != nil {
		t.Fatal(err)
	}
	if !samePath(gotSource, runtimePath) {
		t.Fatalf("repair source = %q, want %q", gotSource, runtimePath)
	}
	if !samePath(gotRequest.TargetPath, stable) || gotRequest.ParentPID != os.Getppid() {
		t.Fatalf("repair request = %#v", gotRequest)
	}
	if gotRequest.ExpectedCurrentSHA256 == "" || gotRequest.SourceSHA256 == "" || gotRequest.ExpectedCurrentSHA256 == gotRequest.SourceSHA256 {
		t.Fatalf("repair hashes = %#v", gotRequest)
	}
}

func TestScheduleLegacyDirectSelfUpdateRepairRequiresVerifiedAffectedParent(t *testing.T) {
	clearPostParentRuntimeEnvironment(t)
	previousExecutable := legacyDirectExecutablePath
	previousParent := legacyDirectParentExecutable
	previousVersion := legacyDirectParentVersion
	previousSchedule := legacyDirectScheduleRepair
	t.Cleanup(func() {
		legacyDirectExecutablePath = previousExecutable
		legacyDirectParentExecutable = previousParent
		legacyDirectParentVersion = previousVersion
		legacyDirectScheduleRepair = previousSchedule
	})
	dir := t.TempDir()
	installDir := filepath.Join(dir, "bin")
	candidate := filepath.Join(installDir, ".codex-proxy_0.1.13-rc.54_windows_amd64.exe.123")
	stable := filepath.Join(installDir, "cxp.exe")
	managed := filepath.Join(installDir, "codex-proxy.exe")
	writePostParentTestFile(t, candidate, []byte("new-candidate"))
	writePostParentTestFile(t, stable, []byte("old-launcher"))
	writePostParentTestFile(t, managed, []byte("old-launcher"))
	writePostParentTestFile(t, filepath.Join(installDir, "cxp.cmd"), []byte("@echo off\r\n\"%~dp0cxp.exe\" %*\r\n"))
	writeManagedPostParentRecord(t, dir, managed)
	legacyDirectExecutablePath = func() (string, error) { return candidate, nil }
	legacyDirectParentExecutable = func() (string, error) { return stable, nil }
	legacyDirectScheduleRepair = func(string, postParentRepairRequest) error {
		t.Fatal("repair scheduled for an unsupported parent version")
		return nil
	}
	legacyDirectParentVersion = func(string) (string, error) { return "", errors.New("probe failed") }
	if err := ScheduleLegacyDirectSelfUpdateRepair([]string{candidate, "--version"}, "0.1.13-rc.54"); err == nil || !strings.Contains(err.Error(), "verify managed legacy cxp parent") {
		t.Fatalf("managed parent probe error = %v", err)
	}
	legacyDirectParentVersion = func(string) (string, error) { return "v0.1.11", nil }
	if err := ScheduleLegacyDirectSelfUpdateRepair([]string{candidate, "--version"}, "0.1.13-rc.54"); err != nil {
		t.Fatal(err)
	}
}

func TestManagedLegacyWindowsEntryRejectsCustomCommand(t *testing.T) {
	dir := t.TempDir()
	installDir := filepath.Join(dir, "bin")
	stable := filepath.Join(installDir, "cxp.exe")
	managed := filepath.Join(installDir, "codex-proxy.exe")
	writePostParentTestFile(t, stable, []byte("old-launcher"))
	writePostParentTestFile(t, managed, []byte("old-launcher"))
	writePostParentTestFile(t, filepath.Join(installDir, "cxp.cmd"), []byte("@echo off\r\ncustom-helper.exe %*\r\n"))
	writeManagedPostParentRecord(t, dir, managed)
	owned, err := managedLegacyWindowsEntry(stable)
	if err != nil {
		t.Fatal(err)
	}
	if owned {
		t.Fatal("custom cxp.cmd was treated as a managed entry")
	}
}

func TestRequireManagedTargetCommitRejectsUncommittedCandidate(t *testing.T) {
	dir := t.TempDir()
	stable := filepath.Join(dir, "cxp.exe")
	managed := filepath.Join(dir, "codex-proxy.exe")
	writePostParentTestFile(t, stable, []byte("old"))
	writePostParentTestFile(t, managed, []byte("old"))
	source := filepath.Join(dir, "candidate.exe")
	writePostParentTestFile(t, source, []byte("new"))
	sourceHash, err := fileSHA256(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := requireManagedTargetCommit(stable, sourceHash); err == nil || !strings.Contains(err.Error(), "did not commit the candidate") {
		t.Fatalf("uncommitted managed target error = %v", err)
	}
	copyPostParentTestFile(t, source, managed)
	if err := requireManagedTargetCommit(stable, sourceHash); err != nil {
		t.Fatalf("committed managed target rejected: %v", err)
	}
}

func TestRepairPostParentEntryWaitsForLockedParentThenReplaces(t *testing.T) {
	if os.Getenv("CXP_POST_PARENT_LOCK_HOLDER") == "1" {
		ready := os.Getenv("CXP_POST_PARENT_READY")
		release := os.Getenv("CXP_POST_PARENT_RELEASE")
		if err := os.WriteFile(ready, []byte("ready"), 0o600); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(release); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("timed out waiting for release")
	}

	dir := t.TempDir()
	installDir := filepath.Join(dir, "bin")
	runtimeRoot := filepath.Join(installDir, ".cxp-runtime")
	versionDir := filepath.Join(runtimeRoot, "versions", "v0.1.13-rc.54")
	if err := os.MkdirAll(versionDir, 0o755); err != nil {
		t.Fatal(err)
	}
	testExecutable, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(installDir, "cxp.exe")
	source := filepath.Join(versionDir, "cxp.exe")
	copyPostParentTestFile(t, testExecutable, target)
	copyPostParentTestFile(t, testExecutable, source)
	file, err := os.OpenFile(source, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("new-runtime-overlay")); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	writePostParentTestFile(t, filepath.Join(installDir, "cxp.cmd"), []byte("@echo off\r\n\"%~dp0cxp.exe\" %*\r\n"))
	managed := filepath.Join(installDir, "codex-proxy.exe")
	copyPostParentTestFile(t, testExecutable, managed)
	if err := helperruntime.Activate(runtimeRoot, "v0.1.13-rc.54"); err != nil {
		t.Fatal(err)
	}
	writeManagedPostParentRecord(t, dir, managed)

	ready := filepath.Join(dir, "ready")
	release := filepath.Join(dir, "release")
	parent := exec.Command(target, "-test.run=^TestRepairPostParentEntryWaitsForLockedParentThenReplaces$")
	parent.Env = append(os.Environ(),
		"CXP_POST_PARENT_LOCK_HOLDER=1",
		"CXP_POST_PARENT_READY="+ready,
		"CXP_POST_PARENT_RELEASE="+release,
	)
	if err := parent.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if parent.ProcessState == nil {
			_ = parent.Process.Kill()
			_, _ = parent.Process.Wait()
		}
	})
	waitForPostParentTestPath(t, ready, 5*time.Second)
	sourceHash, err := fileSHA256(source)
	if err != nil {
		t.Fatal(err)
	}
	targetHash, err := fileSHA256(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := schedulePostParentRepair(source, postParentRepairRequest{
		ParentPID:             parent.Process.Pid,
		TargetPath:            target,
		ExpectedCurrentSHA256: targetHash,
		SourceSHA256:          sourceHash,
	}); err != nil {
		t.Fatal(err)
	}
	if markers, err := filepath.Glob(filepath.Join(runtimeRoot, ".post-parent-ready-*")); err != nil {
		t.Fatal(err)
	} else if len(markers) != 0 {
		t.Fatalf("readiness marker leaked after worker armed: %v", markers)
	}
	// schedulePostParentRepair returns only after the worker has validated and
	// opened the still-running parent. Releasing immediately exercises that
	// readiness contract instead of hiding a launch race behind a sleep.
	copyPostParentTestFile(t, source, managed)
	writePostParentTestFile(t, release, []byte("release"))
	if err := parent.Wait(); err != nil {
		t.Fatal(err)
	}
	waitForPostParentTestHash(t, target, sourceHash, 10*time.Second)
	if got, err := fileSHA256(target); err != nil || !strings.EqualFold(got, sourceHash) {
		t.Fatalf("target hash = %q, %v; want %q", got, err, sourceHash)
	}
	waitForPostParentTestLog(t, target+".update.log", "post-parent entry repair completed", 10*time.Second)
}

func writeManagedPostParentRecord(t *testing.T, dir string, managed string) {
	t.Helper()
	appData := filepath.Join(dir, "appdata")
	t.Setenv("APPDATA", appData)
	recordPath, err := managedinstall.DefaultRecordPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := managedinstall.SaveRecord(recordPath, managedinstall.Record{
		TargetPath:   managed,
		TargetSource: string(managedinstall.SourceRecord),
		TargetState:  string(managedinstall.StateManaged),
		Version:      "0.1.12",
		GOOS:         "windows",
		GOARCH:       "amd64",
	}); err != nil {
		t.Fatal(err)
	}
}

func copyPostParentTestFile(t *testing.T, source string, target string) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func writePostParentTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o700); err != nil {
		t.Fatal(err)
	}
}

func waitForPostParentTestPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitForPostParentTestHash(t *testing.T, path string, expected string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hash, err := fileSHA256(path); err == nil && strings.EqualFold(hash, expected) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to reach hash %s", path, expected)
}

func waitForPostParentTestLog(t *testing.T, path string, expected string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			last = string(data)
			if strings.Contains(last, expected) {
				return
			}
		} else if !os.IsNotExist(err) {
			t.Fatalf("read %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to contain %q; last contents:\n%s", path, expected, last)
}

func clearPostParentRuntimeEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		helperruntime.EnvRuntime,
		helperruntime.EnvRuntimeRoot,
		helperruntime.EnvRuntimeVersion,
		helperruntime.EnvEntryPath,
		helperruntime.EnvDisable,
		helperruntime.EnvForce,
	} {
		t.Setenv(name, "")
	}
}
