//go:build windows

package helperruntime

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
	"golang.org/x/sys/windows"
)

func TestRuntimeProcessIdentityWindows(t *testing.T) {
	if os.Getenv("CXP_PROCESS_IDENTITY_HELPER") == "1" {
		if code, handled, err := Launch("v1.2.3", os.Args); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		} else if handled {
			os.Exit(code)
		}
		if _, ok := Current(); !ok {
			os.Exit(3)
		}
		if err := os.WriteFile(os.Getenv("CXP_TEST_READY"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			os.Exit(4)
		}
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}

	dir, err := os.MkdirTemp("", "cxp-process-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	entry := filepath.Join(dir, "cxp.exe")
	self, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	copyWindowsTestExecutable(t, self, entry)
	ready := filepath.Join(dir, "ready")
	cmd := exec.Command(entry, "-test.run=TestRuntimeProcessIdentityWindows")
	cmd.Dir = dir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"SystemRoot=" + os.Getenv("SystemRoot"),
		"HOME=" + dir,
		"CXP_PROCESS_IDENTITY_HELPER=1",
		"CXP_TEST_READY=" + ready,
		EnvForce + "=1",
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	childPID := waitForWindowsReady(t, ready, &stderr)
	for _, pid := range []int{cmd.Process.Pid, childPID} {
		exe := windowsProcessImage(t, pid)
		command, err := proc.CommandLine(pid)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(strings.ToLower(exe+"\n"+command), "codex") {
			t.Fatalf("runtime process metadata contains forbidden keyword: exe=%q command=%q", exe, command)
		}
		if !strings.EqualFold(filepath.Base(exe), "cxp.exe") {
			t.Fatalf("process image = %q, want cxp.exe", exe)
		}
	}
}

func windowsProcessImage(t *testing.T, pid int) string {
	t.Helper()
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(handle)
	buffer := make([]uint16, 32768)
	size := uint32(len(buffer))
	if err := windows.QueryFullProcessImageName(handle, 0, &buffer[0], &size); err != nil {
		t.Fatal(err)
	}
	return windows.UTF16ToString(buffer[:size])
}

func copyWindowsTestExecutable(t *testing.T, source string, target string) {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, data, 0o755); err != nil {
		t.Fatal(err)
	}
}

func waitForWindowsReady(t *testing.T, ready string, stderr *bytes.Buffer) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if data, err := os.ReadFile(ready); err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				t.Fatal(err)
			}
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime did not become ready: %s", stderr.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
}
