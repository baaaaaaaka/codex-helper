//go:build windows

package cli

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"golang.org/x/sys/windows"
)

const (
	windowsDetachedLifecycleModeEnv   = "CODEX_HELPER_WINDOWS_DETACHED_LIFECYCLE_MODE"
	windowsDetachedLifecycleReadyEnv  = "CODEX_HELPER_WINDOWS_DETACHED_LIFECYCLE_READY"
	windowsDetachedLifecycleParentEnv = "CODEX_HELPER_WINDOWS_DETACHED_LIFECYCLE_PARENT"
)

var (
	windowsKernel32              = windows.NewLazySystemDLL("kernel32.dll")
	windowsAttachConsole         = windowsKernel32.NewProc("AttachConsole")
	windowsFreeConsole           = windowsKernel32.NewProc("FreeConsole")
	windowsSetConsoleCtrlHandler = windowsKernel32.NewProc("SetConsoleCtrlHandler")
)

// TestDetachedDaemonSurvivesConsoleControlEvent models closing the Windows
// console that launched cxp app. The launcher receives CTRL_BREAK_EVENT and
// exits; a child started with CREATE_NO_WINDOW must remain reachable.
func TestDetachedDaemonSurvivesConsoleControlEvent(t *testing.T) {
	switch os.Getenv(windowsDetachedLifecycleModeEnv) {
	case "child":
		runWindowsDetachedLifecycleChild()
		return
	case "parent":
		runWindowsDetachedLifecycleParent()
		return
	case "signal":
		runWindowsDetachedLifecycleSignal()
		return
	}

	root := t.TempDir()
	readyPath := filepath.Join(root, "ready")
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	launcher := exec.Command(exe, "-test.run=^TestDetachedDaemonSurvivesConsoleControlEvent$")
	launcher.Env = append(os.Environ(),
		windowsDetachedLifecycleModeEnv+"=parent",
		windowsDetachedLifecycleReadyEnv+"="+readyPath,
	)
	launcher.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: windows.CREATE_NEW_CONSOLE | windows.CREATE_NEW_PROCESS_GROUP,
		HideWindow:    true,
	}
	if err := launcher.Start(); err != nil {
		t.Fatalf("start console lifecycle launcher: %v", err)
	}

	pid, port, err := waitForWindowsDetachedLifecycleReady(readyPath, 10*time.Second)
	if err != nil {
		_ = launcher.Process.Kill()
		_ = launcher.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if process, err := os.FindProcess(pid); err == nil {
			_ = process.Kill()
		}
	})

	signalHelper := exec.Command(exe, "-test.run=^TestDetachedDaemonSurvivesConsoleControlEvent$")
	signalHelper.Env = append(os.Environ(),
		windowsDetachedLifecycleModeEnv+"=signal",
		windowsDetachedLifecycleParentEnv+"="+strconv.Itoa(launcher.Process.Pid),
	)
	signalHelper.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NO_WINDOW}
	if err := signalHelper.Run(); err != nil {
		_ = launcher.Process.Kill()
		_ = launcher.Wait()
		t.Fatalf("send console control event: %v", err)
	}
	if err := launcher.Wait(); err == nil {
		t.Fatal("console launcher survived CTRL_BREAK_EVENT")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := checkWindowsDetachedLifecycleHealth(port); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("detached daemon pid %d on port %d did not survive console control event", pid, port)
}

func runWindowsDetachedLifecycleParent() {
	readyPath := strings.TrimSpace(os.Getenv(windowsDetachedLifecycleReadyEnv))
	if readyPath == "" {
		windowsLifecycleHelperFatal("ready path is required")
	}
	exe, err := helperpath.RawExecutable()
	if err != nil {
		windowsLifecycleHelperFatal("resolve helper executable: %v", err)
	}
	child := exec.Command(exe, "-test.run=^TestDetachedDaemonSurvivesConsoleControlEvent$")
	child.Env = append(os.Environ(),
		windowsDetachedLifecycleModeEnv+"=child",
		windowsDetachedLifecycleReadyEnv+"="+readyPath,
	)
	configureTeamsServiceDetachedCommand(child)
	if _, err := startDetachedProcess(child); err != nil {
		windowsLifecycleHelperFatal("start detached child: %v", err)
	}
	if _, _, err := waitForWindowsDetachedLifecycleReady(readyPath, 10*time.Second); err != nil {
		windowsLifecycleHelperFatal("wait for detached child: %v", err)
	}
	time.Sleep(10 * time.Second)
	windowsLifecycleHelperFatal("console launcher ignored control event")
}

func runWindowsDetachedLifecycleSignal() {
	rawPID := strings.TrimSpace(os.Getenv(windowsDetachedLifecycleParentEnv))
	pid, err := strconv.ParseUint(rawPID, 10, 32)
	if err != nil || pid == 0 {
		windowsLifecycleHelperFatal("invalid parent PID %q", rawPID)
	}
	if result, _, callErr := windowsFreeConsole.Call(); result == 0 && callErr != windows.ERROR_INVALID_HANDLE {
		windowsLifecycleHelperFatal("FreeConsole: %v", callErr)
	}
	if result, _, callErr := windowsSetConsoleCtrlHandler.Call(0, 1); result == 0 {
		windowsLifecycleHelperFatal("SetConsoleCtrlHandler: %v", callErr)
	}
	if result, _, callErr := windowsAttachConsole.Call(uintptr(pid)); result == 0 {
		windowsLifecycleHelperFatal("AttachConsole(%d): %v", pid, callErr)
	}
	if err := windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(pid)); err != nil {
		windowsLifecycleHelperFatal("GenerateConsoleCtrlEvent: %v", err)
	}
	_, _, _ = windowsFreeConsole.Call()
}

func runWindowsDetachedLifecycleChild() {
	readyPath := strings.TrimSpace(os.Getenv(windowsDetachedLifecycleReadyEnv))
	if readyPath == "" {
		windowsLifecycleHelperFatal("ready path is required")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		windowsLifecycleHelperFatal("listen health endpoint: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := os.WriteFile(readyPath, []byte(strconv.Itoa(os.Getpid())+"\n"+strconv.Itoa(port)+"\n"), 0o600); err != nil {
		_ = listener.Close()
		windowsLifecycleHelperFatal("write readiness: %v", err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})}
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		windowsLifecycleHelperFatal("serve health endpoint: %v", err)
	}
}

func waitForWindowsDetachedLifecycleReady(path string, timeout time.Duration) (int, int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			fields := strings.Fields(string(data))
			if len(fields) == 2 {
				pid, pidErr := strconv.Atoi(fields[0])
				port, portErr := strconv.Atoi(fields[1])
				if pidErr == nil && portErr == nil && pid > 0 && port > 0 {
					return pid, port, nil
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return 0, 0, fmt.Errorf("timed out waiting for Windows detached lifecycle readiness at %s", path)
}

func checkWindowsDetachedLifecycleHealth(port int) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health status = %s", resp.Status)
	}
	return nil
}

func windowsLifecycleHelperFatal(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
