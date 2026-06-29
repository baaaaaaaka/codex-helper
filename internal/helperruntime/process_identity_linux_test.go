//go:build linux

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

func TestRuntimeProcessIdentityLinux(t *testing.T) {
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

	for _, tc := range []struct {
		name     string
		entry    string
		multiHop bool
		preseed  bool
	}{
		{name: "cxp", entry: "cxp"},
		{name: "newer-stable-entry", entry: "cxp", preseed: true},
		{name: "legacy-entry", entry: "codex-proxy"},
		{name: "multi-hop-legacy-entry", entry: "cxp-link", multiHop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "cxp-process-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			entry := filepath.Join(dir, tc.entry)
			self, err := helperpath.RawExecutable()
			if err != nil {
				t.Fatal(err)
			}
			if tc.multiHop {
				physicalDir := filepath.Join(dir, "physical")
				hopDir := filepath.Join(dir, "links")
				if err := os.MkdirAll(physicalDir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(hopDir, 0o755); err != nil {
					t.Fatal(err)
				}
				physical := filepath.Join(physicalDir, "codex-proxy")
				second := filepath.Join(hopDir, "second")
				copyTestExecutable(t, self, physical)
				if err := os.Symlink(physical, second); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join("links", "second"), entry); err != nil {
					t.Fatal(err)
				}
			} else {
				copyTestExecutable(t, self, entry)
			}
			if tc.preseed {
				root := filepath.Join(dir, ".cxp-runtime")
				if _, err := InstallVersion(root, entry, "v1.0.0", "linux", true); err != nil {
					t.Fatal(err)
				}
				if err := Activate(root, "v1.0.0"); err != nil {
					t.Fatal(err)
				}
			}
			ready := filepath.Join(dir, "ready")
			cmd := exec.Command(entry, "-test.run=TestRuntimeProcessIdentityLinux")
			cmd.Dir = dir
			cmd.Env = []string{
				"PATH=/usr/bin:/bin",
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
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("runtime did not become ready: %s", stderr.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			pid := cmd.Process.Pid
			observed := []string{
				readProcessFile(t, pid, "comm"),
				readProcessLink(t, pid, "exe"),
				strings.ReplaceAll(readProcessFile(t, pid, "cmdline"), "\x00", " "),
				mappedExecutablePaths(t, pid),
			}
			for _, value := range observed {
				if strings.Contains(strings.ToLower(value), "codex") {
					t.Fatalf("runtime process metadata contains forbidden keyword: %q", value)
				}
			}
			if got := strings.TrimSpace(observed[0]); got != "cxp" {
				t.Fatalf("comm = %q, want cxp", got)
			}
			if !strings.HasSuffix(observed[1], "/.cxp-runtime/versions/v1.2.3/cxp") {
				t.Fatalf("exe = %q", observed[1])
			}
			runtimeRoot := filepath.Clean(filepath.Join(filepath.Dir(observed[1]), "..", ".."))
			if active, err := ReadActive(runtimeRoot); err != nil || active != "v1.2.3" {
				t.Fatalf("active runtime = %q, %v; want v1.2.3", active, err)
			}
			if tc.multiHop {
				if got, err := os.Readlink(entry); err != nil || got != filepath.Join("links", "second") {
					t.Fatalf("first user link changed: %q, %v", got, err)
				}
			}
		})
	}
}

func copyTestExecutable(t *testing.T, source string, target string) {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, data, 0o755); err != nil {
		t.Fatal(err)
	}
}

func readProcessFile(t *testing.T, pid int, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func readProcessLink(t *testing.T, pid int, name string) string {
	t.Helper()
	value, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mappedExecutablePaths(t *testing.T, pid int) string {
	t.Helper()
	data := readProcessFile(t, pid, "maps")
	var paths []string
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 6 && strings.HasPrefix(fields[len(fields)-1], "/") {
			paths = append(paths, fields[len(fields)-1])
		}
	}
	return strings.Join(paths, "\n")
}
