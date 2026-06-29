//go:build darwin

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
)

func TestRuntimeProcessIdentityDarwin(t *testing.T) {
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

	for _, entryName := range []string{"cxp", "codex-proxy"} {
		t.Run(entryName, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "cxp-process-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			entry := filepath.Join(dir, entryName)
			self, err := helperpath.RawExecutable()
			if err != nil {
				t.Fatal(err)
			}
			copyDarwinTestExecutable(t, self, entry)
			ready := filepath.Join(dir, "ready")
			cmd := exec.Command(entry, "-test.run=TestRuntimeProcessIdentityDarwin")
			cmd.Dir = dir
			cmd.Env = []string{
				"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
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
			waitForDarwinReady(t, ready, &stderr)
			pid := strconv.Itoa(cmd.Process.Pid)
			comm, err := exec.Command("ps", "-p", pid, "-o", "comm=").CombinedOutput()
			if err != nil {
				t.Fatalf("ps comm: %v: %s", err, comm)
			}
			command, err := exec.Command("ps", "-p", pid, "-o", "command=").CombinedOutput()
			if err != nil {
				t.Fatalf("ps command: %v: %s", err, command)
			}
			openFiles, err := exec.Command("lsof", "-a", "-p", pid, "-d", "txt", "-Fn").CombinedOutput()
			if err != nil {
				t.Fatalf("lsof executable: %v: %s", err, openFiles)
			}
			for _, value := range [][]byte{comm, command, openFiles} {
				if strings.Contains(strings.ToLower(string(value)), "codex") {
					t.Fatalf("runtime process metadata contains forbidden keyword: %q", value)
				}
			}
			if !strings.Contains(string(comm), "/.cxp-runtime/versions/v1.2.3/cxp") {
				t.Fatalf("comm = %q", comm)
			}
		})
	}
}

func copyDarwinTestExecutable(t *testing.T, source string, target string) {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, data, 0o755); err != nil {
		t.Fatal(err)
	}
}

func waitForDarwinReady(t *testing.T, ready string, stderr *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime did not become ready: %s", stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
