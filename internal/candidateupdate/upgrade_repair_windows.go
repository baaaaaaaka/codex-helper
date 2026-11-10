//go:build windows

package candidateupdate

import (
	"bufio"
	"context"
	"crypto/sha256"
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

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
	"golang.org/x/sys/windows"
)

func deferUpgradeEntry(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION) || errors.Is(err, windows.ERROR_LOCK_VIOLATION)
}

func upgradeProcessPath(h windows.Handle) (string, error) {
	b := make([]uint16, 32768)
	n := uint32(len(b))
	if err := windows.QueryFullProcessImageName(h, 0, &b[0], &n); err != nil {
		return "", err
	}
	return windows.UTF16ToString(b[:n]), nil
}

// Find the actual mapped launcher, rather than assuming the immediate parent
// is cxp.exe (services and immutable runtimes add intermediate processes).
func upgradeLauncherPID(entry string) (uint32, error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(snapshot)
	parents := map[uint32]uint32{}
	var p windows.ProcessEntry32
	p.Size = uint32(unsafe.Sizeof(p))
	for err = windows.Process32First(snapshot, &p); err == nil; err = windows.Process32Next(snapshot, &p) {
		parents[p.ProcessID] = p.ParentProcessID
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return 0, err
	}
	pid := uint32(os.Getppid())
	for depth := 0; pid != 0 && depth < 64; depth++ {
		h, openErr := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
		if openErr == nil {
			path, pathErr := upgradeProcessPath(h)
			windows.CloseHandle(h)
			if pathErr == nil && samePath(path, entry) {
				return pid, nil
			}
		}
		pid = parents[pid]
	}
	return 0, nil
}

func upgradeEntryAvailable(entry string) error {
	pid, err := upgradeLauncherPID(entry)
	if err != nil {
		return err
	}
	if pid != 0 {
		return windows.ERROR_SHARING_VIOLATION
	}
	return nil
}

// The event is a lifetime token, not persisted migration state. The staging
// caller retains it until the worker acknowledges its own handle. This keeps
// repeated invocations from spawning workers while an old service maps entry.
func upgradeRepairToken(p updatestate.Pending) (windows.Handle, error) {
	digest := sha256.Sum256([]byte(strings.ToLower(filepath.Clean(p.RuntimeRoot)) + "\n" + p.RequestID))
	name, err := windows.UTF16PtrFromString(fmt.Sprintf(`Local\CXP-SQLite-Upgrade-%x`, digest))
	if err != nil {
		return 0, err
	}
	return windows.CreateEvent(nil, 1, 0, name)
}

func scheduleUpgradeRepair(p updatestate.Pending) error {
	token, tokenErr := upgradeRepairToken(p)
	if token != 0 {
		defer windows.CloseHandle(token)
	}
	if errors.Is(tokenErr, windows.ERROR_ALREADY_EXISTS) {
		return nil
	}
	if tokenErr != nil {
		return tokenErr
	}

	var pid uint32
	for _, entry := range upgradeEntries(p) {
		var err error
		pid, err = upgradeLauncherPID(entry)
		if err != nil {
			return err
		}
		if pid != 0 {
			break
		}
	}
	if pid == 0 {
		// Another terminal may still hold the image. The fixed migration
		// forwarder stays valid; its next invocation retries entry repair.
		return nil
	}
	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	defer r.Close()
	log, err := os.OpenFile(filepath.Join(p.RuntimeRoot, "bootstrap-migration.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		w.Close()
		return err
	}
	defer log.Close()
	cmd := exec.Command(p.TargetActive, upgradeRepairCommand, "--root="+p.RuntimeRoot, "--request-id="+p.RequestID, "--parent-pid="+strconv.FormatUint(uint64(pid), 10))
	cmd.Env = append(helperruntime.LauncherEnvironment(os.Environ()), helperruntime.EnvDisable+"=1")
	cmd.Stdout, cmd.Stderr = w, log
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
	if err = cmd.Start(); err != nil {
		w.Close()
		return err
	}
	w.Close()
	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(io.LimitReader(r, 64)).ReadString('\n')
		if err == nil && line != "ready\n" {
			err = fmt.Errorf("invalid bootstrap repair readiness")
		}
		ready <- err
	}()
	select {
	case err = <-ready:
	case <-time.After(5 * time.Second):
		err = fmt.Errorf("bootstrap repair readiness timed out")
	}
	if err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		return err
	}
	return cmd.Process.Release()
}

func runUpgradeRepair(root, id string, pid int, stdout io.Writer) error {
	p, err := migrationPending(root)
	if err != nil {
		return err
	}
	if p.RequestID != id || !samePath(root, p.RuntimeRoot) {
		return fmt.Errorf("migration owner changed")
	}
	if err = validateMigration(p); err != nil {
		return err
	}
	token, tokenErr := upgradeRepairToken(p)
	if token != 0 {
		defer windows.CloseHandle(token)
	}
	if tokenErr != nil && !errors.Is(tokenErr, windows.ERROR_ALREADY_EXISTS) {
		return tokenErr
	}
	raw, err := candidateExecutablePath()
	if err != nil || !samePath(raw, p.TargetActive) {
		return fmt.Errorf("worker is not the verified immutable migration runtime")
	}
	h, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(h)
	path, err := upgradeProcessPath(h)
	if err != nil || !ownsUpgradeEntry(p, path) {
		return fmt.Errorf("repair parent is not the installation launcher")
	}
	// Retain the kernel handle before acknowledging readiness: PID reuse can
	// never cause this worker to wait for a different process.
	if _, err = io.WriteString(stdout, "ready\n"); err != nil {
		return err
	}
	if f, ok := stdout.(*os.File); ok {
		f.Close()
	}
	if status, err := windows.WaitForSingleObject(h, windows.INFINITE); err != nil || status != windows.WAIT_OBJECT_0 {
		return fmt.Errorf("wait for old launcher: status=%d: %v", status, err)
	}
	// A restarted service can map the old entry before the first parent exits.
	// Wait on the actual image handles without polling or copying. This single
	// worker survives that race; the kernel token disappears if it is stopped.
	for transient := 0; transient < 3; {
		mapped, err := mappedUpgradeEntry(p)
		if err != nil {
			return err
		}
		if mapped != 0 {
			status, waitErr := windows.WaitForSingleObject(mapped, windows.INFINITE)
			windows.CloseHandle(mapped)
			if waitErr != nil || status != windows.WAIT_OBJECT_0 {
				return fmt.Errorf("wait for mapped entry: status=%d: %v", status, waitErr)
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = repairUpgradeEntries(ctx, p)
		cancel()
		if !deferUpgradeEntry(err) {
			return err
		}
		transient++
		time.Sleep(time.Duration(transient) * time.Second)
	}
	return fmt.Errorf("bootstrap entry remains unavailable after launcher exit")
}

// Called only after a mapped launcher exits, never on a timer or normal CLI
// path. Retaining the returned process handle makes PID reuse harmless.
func mappedUpgradeEntry(p updatestate.Pending) (windows.Handle, error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(snapshot)
	var process windows.ProcessEntry32
	process.Size = uint32(unsafe.Sizeof(process))
	for err = windows.Process32First(snapshot, &process); err == nil; err = windows.Process32Next(snapshot, &process) {
		h, openErr := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, process.ProcessID)
		if openErr != nil {
			continue
		}
		path, pathErr := upgradeProcessPath(h)
		if pathErr == nil && ownsUpgradeEntry(p, path) {
			return h, nil
		}
		windows.CloseHandle(h)
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return 0, err
	}
	return 0, nil
}
