//go:build windows

package candidateupdate

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"golang.org/x/mod/semver"
	"golang.org/x/sys/windows"
)

const legacyDirectSelfUpdateParentVersion = "v0.1.12"

var (
	legacyDirectExecutablePath   = helperpath.RawExecutable
	legacyDirectParentExecutable = directParentExecutable
	legacyDirectParentVersion    = verifiedPhysicalVersion
	legacyDirectScheduleRepair   = schedulePostParentRepair
)

// ScheduleLegacyDirectSelfUpdateRepair recognizes only the cross-generation
// layout created when the current installer installs v0.1.12: a managed
// codex-proxy.exe target, a canonical cxp.cmd, and a v0.1.12 cxp.exe parent
// validating a downloaded .codex-proxy_* candidate. The new candidate has
// already published and activated its immutable runtime before this function
// runs. A worker from that immutable runtime waits for the locked parent to
// exit, then converges the physical cxp.exe without user intervention.
func ScheduleLegacyDirectSelfUpdateRepair(args []string, buildVersion string) error {
	if len(args) != 2 || args[1] != "--version" {
		return nil
	}
	if _, ok := helperruntime.Current(); ok {
		return nil
	}
	targetVersion, ok := helperruntime.NormalizeVersion(buildVersion)
	if !ok || semver.Compare(targetVersion, legacyDirectSelfUpdateParentVersion) <= 0 {
		return nil
	}
	executable, err := legacyDirectExecutablePath()
	if err != nil {
		return nil
	}
	executable, err = filepath.Abs(filepath.Clean(executable))
	if err != nil || !strings.HasPrefix(strings.ToLower(filepath.Base(executable)), ".codex-proxy_") {
		return nil
	}
	installDir := filepath.Dir(executable)
	stableEntry := filepath.Join(installDir, helperruntime.BinaryName("windows"))
	parent, err := legacyDirectParentExecutable()
	if err != nil || !samePath(parent, stableEntry) {
		return nil
	}
	owned, err := managedLegacyWindowsEntry(stableEntry)
	if err != nil || !owned {
		return nil
	}
	parentVersion, err := legacyDirectParentVersion(stableEntry)
	if err != nil {
		return fmt.Errorf("verify managed legacy cxp parent: %w", err)
	}
	if parentVersion != legacyDirectSelfUpdateParentVersion {
		return nil
	}
	runtimeRoot := filepath.Join(installDir, ".cxp-runtime")
	active, err := helperruntime.ReadActive(runtimeRoot)
	if err != nil {
		return fmt.Errorf("read active runtime before scheduling legacy entry repair: %w", err)
	}
	if active != targetVersion {
		return fmt.Errorf("active runtime is %s after candidate validation, want %s", active, targetVersion)
	}
	source := helperruntime.VersionPath(runtimeRoot, targetVersion, "windows")
	if same, err := sameFileContent(executable, source); err != nil || !same {
		if err != nil {
			return fmt.Errorf("verify published repair source: %w", err)
		}
		return fmt.Errorf("published repair source does not match the downloaded candidate")
	}
	sourceHash, err := fileSHA256(source)
	if err != nil {
		return fmt.Errorf("hash published repair source: %w", err)
	}
	currentHash, err := fileSHA256(stableEntry)
	if err != nil {
		return fmt.Errorf("hash locked stable entry: %w", err)
	}
	if strings.EqualFold(sourceHash, currentHash) {
		return nil
	}
	return legacyDirectScheduleRepair(source, postParentRepairRequest{
		ParentPID:             os.Getppid(),
		TargetPath:            stableEntry,
		ExpectedCurrentSHA256: currentHash,
		SourceSHA256:          sourceHash,
	})
}

var (
	managedStableEntryParentPID      = findStableEntryParentPID
	managedStableEntryScheduleRepair = schedulePostParentRepair
)

// scheduleDeferredStableEntryRepair arms a worker only when the stable
// launcher is an ancestor of the current candidate/runtime process. A caller
// that directly executes an immutable runtime has no stable launcher to repair;
// active runtime dispatch remains correct, so that case is intentionally a
// no-op instead of turning a successful update into an error.
func scheduleDeferredStableEntryRepair(result Result) error {
	if result.stableEntryRepair == nil {
		return nil
	}
	parentPID, err := managedStableEntryParentPID(result.stableEntryRepair.TargetPath)
	if err != nil {
		return fmt.Errorf("find stable cxp launcher for deferred repair: %w", err)
	}
	if parentPID <= 0 {
		return nil
	}
	return managedStableEntryScheduleRepair(result.stableEntryRepair.SourcePath, postParentRepairRequest{
		ParentPID:                     parentPID,
		TargetPath:                    result.stableEntryRepair.TargetPath,
		ExpectedCurrentSHA256:         result.stableEntryRepair.ExpectedCurrentSHA256,
		SourceSHA256:                  result.stableEntryRepair.SourceSHA256,
		AllowUncommittedManagedTarget: true,
	})
}

func shouldDeferStableEntryRepair(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) ||
		errors.Is(err, windows.ERROR_SHARING_VIOLATION) ||
		errors.Is(err, windows.ERROR_LOCK_VIOLATION)
}

func findStableEntryParentPID(target string) (int, error) {
	target, err := filepath.Abs(filepath.Clean(target))
	if err != nil {
		return 0, err
	}
	pid := uint32(os.Getppid())
	seen := map[uint32]bool{}
	for depth := 0; pid != 0 && depth < 64; depth++ {
		if seen[pid] {
			return 0, nil
		}
		seen[pid] = true
		if path, pathErr := processPathForPID(pid); pathErr == nil && samePath(path, target) {
			return int(pid), nil
		}
		parent, parentErr := parentProcessID(pid)
		if parentErr != nil || parent == pid {
			return 0, parentErr
		}
		pid = parent
	}
	return 0, nil
}

func processPathForPID(pid uint32) (string, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(handle)
	return processExecutableFromHandle(handle)
}

func parentProcessID(pid uint32) (uint32, error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(snapshot)
	var process windows.ProcessEntry32
	process.Size = uint32(unsafe.Sizeof(process))
	if err := windows.Process32First(snapshot, &process); err != nil {
		return 0, err
	}
	for {
		if process.ProcessID == pid {
			return process.ParentProcessID, nil
		}
		if err := windows.Process32Next(snapshot, &process); err != nil {
			return 0, err
		}
	}
}

func managedLegacyWindowsEntry(stableEntry string) (bool, error) {
	stableEntry, err := filepath.Abs(filepath.Clean(stableEntry))
	if err != nil || !strings.EqualFold(filepath.Base(stableEntry), helperruntime.BinaryName("windows")) {
		return false, err
	}
	installDir := filepath.Dir(stableEntry)
	shim, err := os.ReadFile(filepath.Join(installDir, "cxp.cmd"))
	if err != nil || !isManagedWindowsCXPCommand(shim) {
		return false, err
	}
	recordPath, err := managedinstall.DefaultRecordPath()
	if err != nil {
		return false, err
	}
	record, err := managedinstall.LoadRecord(recordPath)
	if err != nil {
		return false, err
	}
	managedTarget := filepath.Join(installDir, helperpath.BinaryName("windows"))
	if !samePath(record.TargetPath, managedTarget) || record.TargetState != string(managedinstall.StateManaged) {
		return false, nil
	}
	return true, nil
}

func isManagedWindowsCXPCommand(data []byte) bool {
	content := strings.TrimPrefix(string(data), "\xef\xbb\xbf")
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ToLower(strings.TrimSpace(content))
	return content == "@echo off\n\"%~dp0cxp.exe\" %*" || content == "@echo off\n\"%~dp0codex-proxy.exe\" %*"
}

func schedulePostParentRepair(source string, request postParentRepairRequest) error {
	runtimeRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Clean(source))))
	readyFile, err := os.CreateTemp(runtimeRoot, ".post-parent-ready-")
	if err != nil {
		return fmt.Errorf("reserve post-parent repair readiness marker: %w", err)
	}
	readyPath := readyFile.Name()
	if err := readyFile.Close(); err != nil {
		_ = os.Remove(readyPath)
		return fmt.Errorf("close post-parent repair readiness marker: %w", err)
	}
	if err := os.Remove(readyPath); err != nil {
		return fmt.Errorf("prepare post-parent repair readiness marker: %w", err)
	}
	defer os.Remove(readyPath)
	request.ReadyPath = readyPath

	logPath := request.TargetPath + ".update.log"
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open post-parent repair log: %w", err)
	}
	_, _ = fmt.Fprintf(logFile, "%s scheduling post-parent repair source=%s target=%s parent=%d\n", time.Now().UTC().Format(time.RFC3339Nano), source, request.TargetPath, request.ParentPID)
	args := []string{
		PostParentRepairCommand,
		"--parent-pid=" + strconv.Itoa(request.ParentPID),
		"--target-path=" + request.TargetPath,
		"--ready-path=" + request.ReadyPath,
		"--expected-current-sha256=" + request.ExpectedCurrentSHA256,
		"--source-sha256=" + request.SourceSHA256,
	}
	if request.AllowUncommittedManagedTarget {
		args = append(args, "--allow-uncommitted-managed-target=true")
	}
	cmd := exec.Command(source, args...)
	cmd.Env = append(helperruntime.LauncherEnvironment(os.Environ()), helperruntime.EnvDisable+"=1")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start post-parent entry repair: %w", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, readErr := os.ReadFile(readyPath)
		if readErr == nil {
			if string(data) != "ready\n" {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
				_ = logFile.Close()
				return fmt.Errorf("post-parent entry repair wrote an invalid readiness marker")
			}
			break
		}
		// A same-directory atomic rename makes the marker visible only after
		// the worker closes its writer, but Windows security/indexing filters
		// can still briefly hold the newly visible file. Keep the existing
		// bounded handshake window for those transient sharing errors; treating
		// the first one as fatal turns a successful worker launch into a false
		// update failure.
		if !os.IsNotExist(readErr) &&
			!errors.Is(readErr, windows.ERROR_SHARING_VIOLATION) &&
			!errors.Is(readErr, windows.ERROR_LOCK_VIOLATION) {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			_ = logFile.Close()
			return fmt.Errorf("read post-parent repair readiness marker: %w", readErr)
		}
		if time.Now().After(deadline) {
			if !os.IsNotExist(readErr) {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
				_ = logFile.Close()
				return fmt.Errorf("read post-parent repair readiness marker: %w", readErr)
			}
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			_ = logFile.Close()
			return fmt.Errorf("post-parent entry repair did not become ready within 5s; see %s", logPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.Remove(readyPath); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = logFile.Close()
		return fmt.Errorf("remove post-parent repair readiness marker: %w", err)
	}
	if err := cmd.Process.Release(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("release post-parent entry repair process: %w", err)
	}
	return logFile.Close()
}

func repairPostParentEntry(source string, request postParentRepairRequest, out io.Writer) error {
	source, err := filepath.Abs(filepath.Clean(source))
	if err != nil {
		return fmt.Errorf("resolve worker source: %w", err)
	}
	if !strings.EqualFold(filepath.Base(source), helperruntime.BinaryName("windows")) {
		return fmt.Errorf("worker source is not an immutable cxp executable: %s", source)
	}
	versionDir := filepath.Dir(source)
	versionsRoot := filepath.Dir(versionDir)
	runtimeRoot := filepath.Dir(versionsRoot)
	if !strings.EqualFold(filepath.Base(versionsRoot), "versions") || !strings.EqualFold(filepath.Base(runtimeRoot), ".cxp-runtime") {
		return fmt.Errorf("worker source is outside an immutable runtime: %s", source)
	}
	readyPath, err := filepath.Abs(filepath.Clean(request.ReadyPath))
	if err != nil || !samePath(filepath.Dir(readyPath), runtimeRoot) || !strings.HasPrefix(filepath.Base(readyPath), ".post-parent-ready-") {
		return fmt.Errorf("repair readiness marker is outside %s", runtimeRoot)
	}
	target, err := filepath.Abs(filepath.Clean(request.TargetPath))
	if err != nil {
		return fmt.Errorf("resolve repair target: %w", err)
	}
	if !samePath(target, filepath.Join(filepath.Dir(runtimeRoot), helperruntime.BinaryName("windows"))) {
		return fmt.Errorf("repair target is not the stable entry owned by %s", runtimeRoot)
	}
	active, err := helperruntime.ReadActive(runtimeRoot)
	if err != nil {
		return fmt.Errorf("read active runtime: %w", err)
	}
	sourceVersion, ok := helperruntime.NormalizeVersion(filepath.Base(versionDir))
	if !ok || active != sourceVersion {
		return fmt.Errorf("worker runtime %s is not active (%s)", sourceVersion, active)
	}
	owned, err := managedLegacyWindowsEntry(target)
	if err != nil {
		return fmt.Errorf("inspect managed cxp entry: %w", err)
	}
	if !owned {
		return fmt.Errorf("repair target is not a managed cxp entry")
	}
	sourceHash, err := fileSHA256(source)
	if err != nil {
		return fmt.Errorf("hash worker source: %w", err)
	}
	if !strings.EqualFold(sourceHash, request.SourceSHA256) {
		return fmt.Errorf("worker source hash changed: got %s, want %s", sourceHash, request.SourceSHA256)
	}
	currentHash, err := fileSHA256(target)
	if err != nil {
		return fmt.Errorf("hash stable entry before waiting for parent: %w", err)
	}
	if !strings.EqualFold(currentHash, request.ExpectedCurrentSHA256) {
		return fmt.Errorf("stable entry changed before waiting for parent: got %s, want %s", currentHash, request.ExpectedCurrentSHA256)
	}

	parentHandle, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(request.ParentPID))
	if err != nil {
		return fmt.Errorf("open legacy updater parent %d: %w", request.ParentPID, err)
	}
	defer windows.CloseHandle(parentHandle)
	parentPath, err := processExecutableFromHandle(parentHandle)
	if err != nil {
		return fmt.Errorf("inspect legacy updater parent: %w", err)
	}
	if !samePath(parentPath, target) {
		return fmt.Errorf("legacy updater parent is %s, want %s", parentPath, target)
	}
	if err := publishPostParentReady(readyPath, runtimeRoot); err != nil {
		return err
	}
	if status, err := windows.WaitForSingleObject(parentHandle, uint32((2*time.Minute)/time.Millisecond)); err != nil {
		return fmt.Errorf("wait for legacy updater parent: %w", err)
	} else if status == uint32(windows.WAIT_TIMEOUT) {
		return fmt.Errorf("timed out waiting for legacy updater parent %d", request.ParentPID)
	} else if status != uint32(windows.WAIT_OBJECT_0) {
		return fmt.Errorf("unexpected parent wait status %d", status)
	}
	active, err = helperruntime.ReadActive(runtimeRoot)
	if err != nil {
		return fmt.Errorf("re-read active runtime after parent exit: %w", err)
	}
	if active != sourceVersion {
		return fmt.Errorf("active runtime changed from repair source %s to %s while waiting for parent exit", sourceVersion, active)
	}
	owned, err = managedLegacyWindowsEntry(target)
	if err != nil {
		return fmt.Errorf("re-check managed cxp entry after parent exit: %w", err)
	}
	if !owned {
		return fmt.Errorf("repair target stopped being a managed cxp entry while waiting for parent exit")
	}
	if !request.AllowUncommittedManagedTarget {
		if err := requireManagedTargetCommit(target, request.SourceSHA256); err != nil {
			return err
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		currentHash, hashErr := fileSHA256(target)
		if hashErr == nil && strings.EqualFold(currentHash, request.SourceSHA256) {
			return nil
		}
		if hashErr == nil && !strings.EqualFold(currentHash, request.ExpectedCurrentSHA256) {
			return fmt.Errorf("stable entry changed after scheduling: got %s, want %s", currentHash, request.ExpectedCurrentSHA256)
		}
		if hashErr != nil {
			lastErr = hashErr
		} else {
			lastErr = copyExecutableAtomically(source, target, request.ExpectedCurrentSHA256)
			if lastErr == nil {
				if currentHash, verifyErr := fileSHA256(target); verifyErr == nil && strings.EqualFold(currentHash, request.SourceSHA256) {
					_, _ = fmt.Fprintf(out, "replaced %s after legacy parent exit on attempt %d\n", target, attempt)
					return nil
				} else {
					lastErr = fmt.Errorf("verify replacement hash: got %s: %v", currentHash, verifyErr)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("replace stable entry after parent exit: %w", lastErr)
}

func requireManagedTargetCommit(stableEntry string, expectedSHA256 string) error {
	managedTarget := filepath.Join(filepath.Dir(stableEntry), helperpath.BinaryName("windows"))
	managedHash, err := fileSHA256(managedTarget)
	if err != nil {
		return fmt.Errorf("hash managed helper target after parent exit: %w", err)
	}
	if !strings.EqualFold(managedHash, expectedSHA256) {
		return fmt.Errorf("managed helper target did not commit the candidate: got %s, want %s", managedHash, expectedSHA256)
	}
	return nil
}

func publishPostParentReady(readyPath string, runtimeRoot string) error {
	readyFile, err := os.CreateTemp(runtimeRoot, ".post-parent-arming-")
	if err != nil {
		return fmt.Errorf("create post-parent repair readiness: %w", err)
	}
	tmpPath := readyFile.Name()
	cleanup := true
	defer func() {
		_ = readyFile.Close()
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := io.WriteString(readyFile, "ready\n"); err != nil {
		return fmt.Errorf("write post-parent repair readiness: %w", err)
	}
	if err := readyFile.Sync(); err != nil {
		return fmt.Errorf("sync post-parent repair readiness: %w", err)
	}
	if err := readyFile.Close(); err != nil {
		return fmt.Errorf("close post-parent repair readiness: %w", err)
	}
	// Rename only after close so observing ReadyPath also proves that Windows
	// no longer has a writer handle that can block the scheduler's cleanup.
	if err := os.Rename(tmpPath, readyPath); err != nil {
		return fmt.Errorf("publish post-parent repair readiness: %w", err)
	}
	cleanup = false
	return nil
}
