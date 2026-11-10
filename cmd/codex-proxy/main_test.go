package main

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/candidateupdate"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func stubEntrypoint(t *testing.T) {
	t.Helper()
	launch, execute := launchHelperRuntime, executeCLI
	t.Cleanup(func() { launchHelperRuntime, executeCLI = launch, execute })
	launchHelperRuntime = func(string, []string) (int, bool, error) {
		t.Fatal("unexpected runtime dispatch")
		return 0, false, nil
	}
	executeCLI = func() int { t.Fatal("unexpected CLI initialization"); return 0 }
}

func TestRunMainHandlesBootProtocolBeforeRuntimeDispatch(t *testing.T) {
	stubEntrypoint(t)
	if code := runMain([]string{"cxp", helperruntime.ProtocolProbeFlag}); code != 0 {
		t.Fatal(code)
	}
}

func TestRunMainHandlesCandidateUpdateBeforeRuntimeDispatch(t *testing.T) {
	stubEntrypoint(t)
	if code := runMain([]string{"candidate", candidateupdate.InternalCommand, "--unsupported"}); code != 2 {
		t.Fatal(code)
	}
}

func TestRunMainHandlesAppInstallWorkerBeforeRuntimeDispatch(t *testing.T) {
	stubEntrypoint(t)
	previousInput := os.Stdin
	t.Cleanup(func() { os.Stdin = previousInput })
	input, err := os.CreateTemp(t.TempDir(), "invalid-request")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	os.Stdin = input
	if code := runMain([]string{"cxp", "--cxp-internal-windows-app-install"}); code != 1 {
		t.Fatal(code)
	}
}

func TestRunMainReturnsHandledRuntimeExitCode(t *testing.T) {
	stubEntrypoint(t)
	launchHelperRuntime = func(string, []string) (int, bool, error) { return 9, true, nil }
	if code := runMain([]string{"cxp", "--version"}); code != 9 {
		t.Fatal(code)
	}
}

func TestRunMainBootstrapFailureStopsBeforeCLI(t *testing.T) {
	stubEntrypoint(t)
	launchHelperRuntime = func(string, []string) (int, bool, error) {
		return 0, false, errors.Join(helperruntime.ErrBootstrapState, errors.New("corrupt database"))
	}
	if code := runMain([]string{"cxp", "--version"}); code != 1 {
		t.Fatal(code)
	}
}

func TestRunMainRejectsLegacyDownloadBeforeRuntimeDispatch(t *testing.T) {
	stubEntrypoint(t)
	// Old updaters already used runtime-disable. That must not bypass refusal.
	t.Setenv(helperruntime.EnvDisable, "1")
	t.Setenv(helperruntime.EnvProbeProtocol, "")
	if code := runMain([]string{".codex-proxy_0.1.99_linux_amd64", "--version"}); code != 1 {
		t.Fatal(code)
	}
}

func TestMainVersionExitZero(t *testing.T) {
	if os.Getenv("CODEX_PROXY_HELPER") == "1" {
		os.Args = []string{"codex-proxy", "--version"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestMainVersionExitZero")
	cmd.Env = append(os.Environ(), "CODEX_PROXY_HELPER=1")
	if err := cmd.Run(); err != nil {
		t.Fatalf("expected exit 0, got error: %v", err)
	}
}

func TestMainInvalidArgsExitOne(t *testing.T) {
	if os.Getenv("CODEX_PROXY_HELPER_INVALID") == "1" {
		os.Args = []string{"codex-proxy", "--not-a-flag"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestMainInvalidArgsExitOne")
	cmd.Env = append(os.Environ(), "CODEX_PROXY_HELPER_INVALID=1")
	err := cmd.Run()
	if err == nil {
		t.Fatalf("expected non-zero exit, got nil error")
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got %T: %v", err, err)
	}
	if exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}
