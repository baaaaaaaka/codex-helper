//go:build cxp_boot_e2e && windows

package helperruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

// A real ConPTY, including the release-built executable and its child launcher.
// No desktop, external model, SSH profile or installed state is needed.
type bootConsole struct {
	process, console windows.Handle
	input, output    *os.File
	mu               sync.Mutex
	text             bytes.Buffer
	done             chan struct{}
	code             uint32
}

func (p *bootConsole) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.text.Write(b)
}
func (p *bootConsole) Output() string { p.mu.Lock(); defer p.mu.Unlock(); return p.text.String() }

func startBootConsole(t *testing.T, binary string, args, env []string) *bootConsole {
	t.Helper()
	var inR, inW, outR, outW windows.Handle
	security := &windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), InheritHandle: 1}
	if err := windows.CreatePipe(&inR, &inW, security, 0); err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(inR)
	if err := windows.CreatePipe(&outR, &outW, security, 0); err != nil {
		_ = windows.CloseHandle(inW)
		t.Fatal(err)
	}
	defer windows.CloseHandle(outW)
	var console windows.Handle
	if err := windows.CreatePseudoConsole(windows.Coord{X: 100, Y: 24}, inR, outW, 0, &console); err != nil {
		_ = windows.CloseHandle(inW)
		_ = windows.CloseHandle(outR)
		t.Fatal(err)
	}
	p := &bootConsole{console: console, input: os.NewFile(uintptr(inW), "console-input"), output: os.NewFile(uintptr(outR), "console-output"), done: make(chan struct{})}
	t.Cleanup(func() {
		if p.process != 0 {
			_ = windows.TerminateProcess(p.process, 1)
			<-p.done
		}
		windows.ClosePseudoConsole(p.console)
		_ = p.input.Close()
		_ = p.output.Close()
		if p.process != 0 {
			_ = windows.CloseHandle(p.process)
		}
	})
	attributes, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		t.Fatal(err)
	}
	defer attributes.Delete()
	// HPCON is passed as a pointer-sized value (not a pointer to the handle).
	var consoleValue unsafe.Pointer
	*(*uintptr)(unsafe.Pointer(&consoleValue)) = uintptr(console)
	if err := attributes.Update(windows.PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, consoleValue, unsafe.Sizeof(console)); err != nil {
		t.Fatal(err)
	}
	app, err := windows.UTF16PtrFromString(binary)
	if err != nil {
		t.Fatal(err)
	}
	line, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(append([]string{binary}, args...)))
	if err != nil {
		t.Fatal(err)
	}
	// Deduplicate case-insensitively, keeping scoped overrides last.
	values := map[string]string{}
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			values[strings.ToUpper(key)] = entry
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]string, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, values[key])
	}
	block := utf16.Encode([]rune(strings.Join(entries, "\x00") + "\x00\x00"))
	startup := windows.StartupInfoEx{StartupInfo: windows.StartupInfo{Cb: uint32(unsafe.Sizeof(windows.StartupInfoEx{}))}, ProcThreadAttributeList: attributes.List()}
	var info windows.ProcessInformation
	if err := windows.CreateProcess(app, line, nil, nil, false, windows.EXTENDED_STARTUPINFO_PRESENT|windows.CREATE_UNICODE_ENVIRONMENT, &block[0], nil, &startup.StartupInfo, &info); err != nil {
		t.Fatal(err)
	}
	p.process = info.Process
	_ = windows.CloseHandle(info.Thread)
	go func() { _, _ = io.Copy(p, p.output) }()
	go func() {
		_, _ = windows.WaitForSingleObject(p.process, windows.INFINITE)
		_ = windows.GetExitCodeProcess(p.process, &p.code)
		close(p.done)
	}()
	return p
}

func (p *bootConsole) quit(t *testing.T) {
	t.Helper()
	if _, err := p.input.Write([]byte("q")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
		if p.code != 0 {
			t.Fatalf("console exit %d: %s", p.code, p.Output())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("console did not exit: %s", p.Output())
	}
}

func TestBootE2ERealConsolePromotesAndRecoversPackagedTUI(t *testing.T) {
	sut := os.Getenv("CXP_TUI_SUT_BINARY")
	if sut == "" {
		sut = filepath.Join(t.TempDir(), "cxp.exe")
		if out, err := exec.Command("go", "build", "-o", sut, "../../cmd/codex-proxy").CombinedOutput(); err != nil {
			t.Fatalf("build CLI: %v %s", err, out)
		}
	}
	bytes, err := os.ReadFile(sut)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"healthy", "crash"} {
		t.Run(mode, func(t *testing.T) {
			c, dir := bootProcessFixture(t)
			for _, path := range []string{c.EntryPath, c.RuntimePath} {
				if err := os.WriteFile(path, bytes, 0755); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "healthy" {
				if err := os.WriteFile(VersionPath(c.Root, "v2.0.0", "windows"), bytes, 0755); err != nil {
					t.Fatal(err)
				}
				s, _ := ReadBoot(c.Root)
				h := sha256.Sum256(bytes)
				s.Candidate.SHA256 = hex.EncodeToString(h[:])
				if err := writeBoot(c.Root, s); err != nil {
					t.Fatal(err)
				}
			}
			cfg := filepath.Join(dir, "config.json")
			if err := os.WriteFile(cfg, []byte(`{"proxyEnabled":false}`), 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"--config", cfg, "tui", "--codex-dir", filepath.Join(dir, "codex")}
			env := append(WithoutRuntimeMarkers(os.Environ()), EnvForce+"=1", "CXP_AUTO_UPDATE=1", "CODEX_PROXY_VERSION=latest", "CXP_BOOT_TEST_DIR="+dir, "CXP_BOOT_TEST_MODE="+mode,
				"HOME="+dir, "USERPROFILE="+dir, "CODEX_HOME="+filepath.Join(dir, "codex"), "CODEX_DIR="+filepath.Join(dir, "codex"), "CODEX_HELPER_STATE_DIR="+filepath.Join(dir, "state"), "HTTP_PROXY=http://127.0.0.1:1", "HTTPS_PROXY=http://127.0.0.1:1")
			first := startBootConsole(t, c.EntryPath, args, env)
			awaitBoot(t, "real ConPTY first frame", func() bool {
				if mode == "healthy" {
					s, _ := ReadBoot(c.Root)
					return s.Good == "v2.0.0"
				}
				return strings.Contains(first.Output(), "Projects")
			})
			first.quit(t)
			s, _ := ReadBoot(c.Root)
			want := "v2.0.0"
			if mode == "crash" {
				want = "v1.0.0"
				if s.Candidate.Attempt == "" {
					t.Fatal("no durable failed attempt")
				}
			}
			if s.Good != want {
				t.Fatal(s)
			}
			next := startBootConsole(t, c.EntryPath, args, env)
			awaitBoot(t, "fresh ConPTY after update", func() bool { return strings.Contains(next.Output(), "Projects") })
			next.quit(t)
			if mode == "crash" && strings.Count(bootEvents(t, dir), "entered v2.0.0") != 1 {
				t.Fatal("failed candidate retried")
			}
		})
	}
}
