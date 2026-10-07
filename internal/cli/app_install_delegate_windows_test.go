package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"golang.org/x/sys/windows"
)

func TestWindowsInstallNativeNormalTokenProbe(t *testing.T) {
	if os.Getenv("CXP_TEST_WINDOWS_NORMAL_TOKEN_PROBE") != "1" {
		t.Skip("inert child process probe only")
	}
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	defer token.Close()
	identity, err := windowsInstallTokenIdentity(token)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("CXP_NORMAL_TOKEN_PROBE=%s\n", data)
	if fixture := os.Getenv("CXP_TEST_WINDOWS_INSTALL_FIXTURE"); fixture != "" {
		t.Setenv("USERPROFILE", identity.Profile)
		t.Setenv("LOCALAPPDATA", identity.Cache)
		codexAppDownloadPackageFn = func(ctx context.Context, opts codexAppDownloadOptions) error {
			if opts.ProxyURL != "http://127.0.0.1:8608" {
				return fmt.Errorf("worker lost installation proxy: %s", opts.ProxyURL)
			}
			if marker := os.Getenv("CXP_TEST_WINDOWS_INSTALL_BLOCK"); marker != "" {
				if err := os.WriteFile(marker, []byte("normal worker reached download"), 0o600); err != nil {
					return err
				}
				<-ctx.Done()
				return ctx.Err()
			}
			contents, err := os.ReadFile(fixture)
			if err != nil {
				return err
			}
			return os.WriteFile(opts.Path, contents, 0o600)
		}
		codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
			return []byte("CN=TestPublisher\n"), nil
		}
		var result bytes.Buffer
		if err := runWindowsAppInstallWorker(os.Stdin, &result, os.Stderr); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("CXP_NORMAL_INSTALL_RESULT=%s", result.Bytes())
	}
}

func TestWindowsInstallNativeUACProcessCreation(t *testing.T) {
	if os.Getenv("CXP_TEST_WINDOWS_UAC_ACCEPTANCE") != "1" {
		t.Skip("requires explicit native split-token UAC acceptance")
	}
	current, err := windows.OpenCurrentProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	identity, err := windowsInstallTokenIdentity(current)
	if err != nil || !identity.Elevated {
		t.Fatalf("requires a genuinely elevated token: identity=%+v err=%v", identity, err)
	}
	t.Setenv("USERPROFILE", identity.Profile)
	t.Setenv("LOCALAPPDATA", identity.Cache)
	t.Setenv("CXP_TEST_WINDOWS_NORMAL_TOKEN_PROBE", "1")
	fixture := filepath.Join(t.TempDir(), "fixture.msix")
	writeTestCodexWindowsManagedMSIX(t, fixture, "CN=TestPublisher", "app/ChatGPT.exe", []byte("inert fixture"))
	root := filepath.Join(t.TempDir(), "managed")
	t.Setenv("CXP_TEST_WINDOWS_INSTALL_FIXTURE", fixture)
	t.Setenv("CXP_WINDOWS_MANAGED_ROOT", root)
	parent, expected, err := normalWindowsInstallParent()
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(parent)
	executable, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for index, operation := range []struct{ refresh, changed bool }{{false, true}, {true, false}, {true, true}, {true, true}} {
		if index == 2 {
			writeTestCodexWindowsManagedMSIX(t, fixture, "CN=TestPublisher", "app/ChatGPT.exe", []byte("inert updated fixture"))
		}
		if index == 3 {
			legacyState, valid, err := readValidCodexWindowsManagedState(root)
			if err != nil || !valid {
				t.Fatalf("missing source state for admin-created cache: %v", err)
			}
			root = filepath.Join(t.TempDir(), "admin-created-managed")
			t.Setenv("CXP_WINDOWS_MANAGED_ROOT", root)
			executablePath := filepath.Join(root, "versions", "legacy", "app", codexDesktopWindowsCurrentExecutable)
			if err := os.MkdirAll(filepath.Dir(executablePath), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(executablePath, []byte("inert admin-created fixture"), 0o700); err != nil {
				t.Fatal(err)
			}
			executableHash, err := sha256File(executablePath)
			if err != nil {
				t.Fatal(err)
			}
			legacyState.RuntimeRelative = "versions/legacy"
			legacyState.ExecutableSHA256 = executableHash
			if err := writeCodexWindowsManagedState(root, legacyState); err != nil {
				t.Fatal(err)
			}
			if _, valid, err := readValidCodexWindowsManagedState(root); err != nil || !valid {
				t.Fatalf("admin-created cache was not valid before normal-worker update: %v", err)
			}
			writeTestCodexWindowsManagedMSIX(t, fixture, "CN=TestPublisher", "app/ChatGPT.exe", []byte("inert update over admin-created cache"))
		}
		request, err := json.Marshal(windowsInstallRequest{Identity: expected, Root: root, ProxyURL: "http://127.0.0.1:8608", Refresh: operation.refresh})
		if err != nil {
			t.Fatal(err)
		}
		command := windowsInstallWorkerCommand(ctx, executable, parent, identity.Profile)
		command.Args = []string{executable, "-test.run=^TestWindowsInstallNativeNormalTokenProbe$", "-test.count=1"}
		command.Stdin = bytes.NewReader(request)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("genuine same-user process creation/worker failed: %v\n%s", err, output)
		}
		identityVerified, stateVerified := false, false
		for _, line := range strings.Split(string(output), "\n") {
			if encoded, found := strings.CutPrefix(line, "CXP_NORMAL_TOKEN_PROBE="); found {
				var actual windowsInstallIdentity
				if err := json.Unmarshal([]byte(encoded), &actual); err != nil {
					t.Fatal(err)
				}
				if err := validateWindowsInstallIdentity(expected, actual); err != nil {
					t.Fatal(err)
				}
				identityVerified = true
			}
			if encoded, found := strings.CutPrefix(line, "CXP_NORMAL_INSTALL_RESULT="); found {
				var result windowsInstallResult
				if err := json.Unmarshal([]byte(encoded), &result); err != nil {
					t.Fatal(err)
				}
				state, valid, err := readValidCodexWindowsManagedState(root)
				if err != nil || !valid || state != result.State || result.Changed != operation.changed {
					t.Fatalf("native worker published unexpected state: result=%+v valid=%t err=%v", result, valid, err)
				}
				stateVerified = true
			}
		}
		if !identityVerified || !stateVerified {
			t.Fatalf("native child did not return verified identity/install state: %s", output)
		}
	}
	before, valid, err := readValidCodexWindowsManagedState(root)
	if err != nil || !valid {
		t.Fatal("missing verified state before cancellation")
	}
	marker := filepath.Join(t.TempDir(), "normal-download-started")
	t.Setenv("CXP_TEST_WINDOWS_INSTALL_BLOCK", marker)
	request, err := json.Marshal(windowsInstallRequest{Identity: expected, Root: root, ProxyURL: "http://127.0.0.1:8608", Refresh: true})
	if err != nil {
		t.Fatal(err)
	}
	cancelContext, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	command := windowsInstallWorkerCommand(cancelContext, executable, parent, identity.Profile)
	command.Args = []string{executable, "-test.run=^TestWindowsInstallNativeNormalTokenProbe$", "-test.count=1"}
	command.Stdin = bytes.NewReader(request)
	done := make(chan error, 1)
	go func() { done <- command.Run() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("normal worker did not reach the inert download before cancellation")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancelWorker()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("interrupted installation reported success")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker was not terminated after cancellation")
	}
	after, valid, err := readValidCodexWindowsManagedState(root)
	if err != nil || !valid || before != after {
		t.Fatalf("interrupted worker damaged the existing valid installation: %v", err)
	}
	t.Log("real elevated parent delegates inert install/update, including an admin-created cache, to the same normal identity; cancellation preserves the existing installation")
}

func TestWindowsInstallWorkerCommandPreservesEnvironmentAndToken(t *testing.T) {
	t.Setenv("CXP_WINDOWS_MANAGED_ROOT", `C:\custom-cache\chatgpt`)
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:8608")
	command := windowsInstallWorkerCommand(context.Background(), "cxp.exe", windows.Handle(123), `C:\Users\same-user`)
	if command.Dir != `C:\Users\same-user` || command.SysProcAttr.ParentProcess != 123 || command.SysProcAttr.Token != 0 || !command.SysProcAttr.HideWindow || command.SysProcAttr.CreationFlags&windowsCreateNoWindow == 0 {
		t.Fatalf("worker command changed token/profile/window settings: %+v", command.SysProcAttr)
	}
	for _, required := range []string{`CXP_WINDOWS_MANAGED_ROOT=C:\custom-cache\chatgpt`, "HTTP_PROXY=http://127.0.0.1:8608"} {
		if !slices.Contains(command.Env, required) {
			t.Fatalf("worker lost required installation environment: %s", required)
		}
	}
	if command.Cancel == nil || command.WaitDelay <= 0 || !slices.Equal(command.Args, []string{"cxp.exe", windowsInstallWorkerArgument}) {
		t.Fatal("worker lost cancellation or internal dispatch configuration")
	}
}

func TestWindowsInstallNativeTokenQueries(t *testing.T) {
	current, err := windows.OpenCurrentProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	identity, err := windowsInstallTokenIdentity(current)
	if err != nil {
		t.Fatal(err)
	}
	elevated, err := currentWindowsTokenElevated()
	if err != nil || identity.Elevated != elevated {
		t.Fatalf("inconsistent native token queries: elevated=%t err=%v", elevated, err)
	}
	if _, err := currentWindowsInstallIdentity(); err == nil {
		t.Fatal("isolated test profile was accepted as the native token profile")
	}
	if !identity.Elevated {
		t.Log("standard native token verified; genuine UAC delegation acceptance requires an elevated split-token session")
		return
	}
	t.Log("elevated native token verified; normal-parent process creation is covered only by explicit UAC acceptance")
}
