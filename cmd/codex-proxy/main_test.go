package main

import (
	"errors"
	"os"
	"os/exec"
	"reflect"
	"testing"
)

func TestRunMainRunsLegacyPreflightBeforeRuntimeDispatch(t *testing.T) {
	previousPreflight := runLegacyUpdaterVersionPreflight
	previousLaunch := launchHelperRuntime
	previousExecute := executeCLI
	t.Cleanup(func() {
		runLegacyUpdaterVersionPreflight = previousPreflight
		launchHelperRuntime = previousLaunch
		executeCLI = previousExecute
	})

	var events []string
	runLegacyUpdaterVersionPreflight = func() error {
		events = append(events, "preflight")
		return nil
	}
	launchHelperRuntime = func(_ string, args []string) (int, bool, error) {
		events = append(events, "runtime:"+args[0])
		return 0, false, nil
	}
	executeCLI = func() int {
		events = append(events, "cli")
		return 17
	}

	if got := runMain([]string{"candidate", "--version"}); got != 17 {
		t.Fatalf("runMain exit code = %d, want 17", got)
	}
	want := []string{"preflight", "runtime:candidate", "cli"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestRunMainPreflightFailureStopsBeforeRuntimeDispatch(t *testing.T) {
	previousPreflight := runLegacyUpdaterVersionPreflight
	previousLaunch := launchHelperRuntime
	previousExecute := executeCLI
	t.Cleanup(func() {
		runLegacyUpdaterVersionPreflight = previousPreflight
		launchHelperRuntime = previousLaunch
		executeCLI = previousExecute
	})

	runLegacyUpdaterVersionPreflight = func() error { return errors.New("unsafe legacy state") }
	launchHelperRuntime = func(string, []string) (int, bool, error) {
		t.Fatal("runtime dispatch ran after a failed compatibility preflight")
		return 0, false, nil
	}
	executeCLI = func() int {
		t.Fatal("CLI ran after a failed compatibility preflight")
		return 0
	}

	if got := runMain([]string{"candidate", "--version"}); got != 1 {
		t.Fatalf("runMain exit code = %d, want 1", got)
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
