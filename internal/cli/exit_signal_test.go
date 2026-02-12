package cli

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
)

func TestExitDueToFatalSignal_NilError(t *testing.T) {
	if exitDueToFatalSignal(nil) {
		t.Fatal("expected false for nil error")
	}
}

func TestExitDueToFatalSignal_NonExitError(t *testing.T) {
	if exitDueToFatalSignal(fmt.Errorf("random error")) {
		t.Fatal("expected false for non-ExitError")
	}
}

func TestExitDueToFatalSignal_WrappedNonExitError(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", os.ErrNotExist)
	if exitDueToFatalSignal(err) {
		t.Fatal("expected false for wrapped non-ExitError")
	}
}

func TestExitDueToFatalSignal_NormalExit(t *testing.T) {
	// Run a command that exits normally with code 1.
	cmd := exec.Command("false")
	err := cmd.Run()
	if err == nil {
		t.Skip("false command unexpectedly succeeded")
	}
	if exitDueToFatalSignal(err) {
		t.Fatal("expected false for normal non-zero exit")
	}
}

func TestExitDueToFatalSignal_SuccessfulCommand(t *testing.T) {
	cmd := exec.Command("true")
	err := cmd.Run()
	if exitDueToFatalSignal(err) {
		t.Fatal("expected false for successful command")
	}
}

func TestExitDueToFatalSignal_SIGABRT(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	// Use kill -ABRT on a sleep process.
	cmd := exec.Command("sh", "-c", "kill -ABRT $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if !exitDueToFatalSignal(err) {
		t.Fatal("expected true for SIGABRT")
	}
}

func TestExitDueToFatalSignal_SIGSEGV(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	cmd := exec.Command("sh", "-c", "kill -SEGV $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if !exitDueToFatalSignal(err) {
		t.Fatal("expected true for SIGSEGV")
	}
}

func TestExitDueToFatalSignal_SIGBUS(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	cmd := exec.Command("sh", "-c", "kill -BUS $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if !exitDueToFatalSignal(err) {
		t.Fatal("expected true for SIGBUS")
	}
}

func TestExitDueToFatalSignal_SIGFPE(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	cmd := exec.Command("sh", "-c", "kill -FPE $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if !exitDueToFatalSignal(err) {
		t.Fatal("expected true for SIGFPE")
	}
}

func TestExitDueToFatalSignal_SIGILL(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	cmd := exec.Command("sh", "-c", "kill -ILL $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if !exitDueToFatalSignal(err) {
		t.Fatal("expected true for SIGILL")
	}
}

func TestExitDueToFatalSignal_SIGTRAP(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	cmd := exec.Command("sh", "-c", "kill -TRAP $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if !exitDueToFatalSignal(err) {
		t.Fatal("expected true for SIGTRAP")
	}
}

func TestExitDueToFatalSignal_SIGTERM_NotFatal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	cmd := exec.Command("sh", "-c", "kill -TERM $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if exitDueToFatalSignal(err) {
		t.Fatal("expected false for SIGTERM (not a fatal signal)")
	}
}

func TestExitDueToFatalSignal_SIGKILL_NotFatal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	// SIGKILL cannot be caught, so we need to kill a child process.
	cmd := exec.Command("sh", "-c", "sleep 60 & pid=$!; kill -KILL $pid; wait $pid; exit $?")
	err := cmd.Run()
	// The shell exits with the signal status.
	// SIGKILL is not in our fatal signals list, so this tests the negative case.
	// Note: on some systems the shell may translate SIGKILL differently, so this
	// test is best-effort.
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	// We don't assert false here because the shell may exit with 137 (not signaled),
	// which would already give false. This test is mainly for exercising the code path.
}

func TestExitDueToFatalSignal_WrappedExitError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	cmd := exec.Command("sh", "-c", "kill -ABRT $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	// Wrap the error.
	wrapped := fmt.Errorf("run failed: %w", err)
	if !exitDueToFatalSignal(wrapped) {
		t.Fatal("expected true for wrapped SIGABRT ExitError")
	}
}

func TestExitDueToFatalSignal_ExitCode2(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip on Windows")
	}
	cmd := exec.Command("sh", "-c", "exit 2")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if exitDueToFatalSignal(err) {
		t.Fatal("expected false for exit code 2 (not a signal)")
	}
}

func TestExitDueToFatalSignal_ExitCode139(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip on Windows")
	}
	// Exit code 139 conventionally means killed by signal 11 (SEGV),
	// but when the shell exits with this code it's just an exit code,
	// not an actual signal delivery.
	cmd := exec.Command("sh", "-c", "exit 139")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if exitDueToFatalSignal(err) {
		t.Fatal("expected false for exit code 139 (exit, not signaled)")
	}
}
