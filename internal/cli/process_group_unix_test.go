//go:build !windows

package cli

import (
	"context"
	"io"
	"os/exec"
	"syscall"
	"testing"
)

func TestConfigureTargetProcessGroupPreservesExistingSysProcAttr(t *testing.T) {
	credential := &syscall.Credential{Uid: 1000, Gid: 1001, Groups: []uint32{1002}}
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: credential}

	configureTargetProcessGroup(cmd)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr was cleared")
	}
	if cmd.SysProcAttr.Credential != credential {
		t.Fatal("existing command credential was not preserved")
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Fatal("Setpgid was not enabled")
	}
}

func TestRunTargetOncePreserveTTYDoesNotCreateBackgroundProcessGroup(t *testing.T) {
	lockCLITestHooks(t)

	shell := requireShell(t)
	prevRunTargetCommand := runTargetCommand
	t.Cleanup(func() { runTargetCommand = prevRunTargetCommand })
	var gotCmd *exec.Cmd
	runTargetCommand = func(name string, args ...string) *exec.Cmd {
		gotCmd = exec.Command(name, args...)
		return gotCmd
	}

	err := runTargetOnceWithOptions(
		context.Background(),
		[]string{shell, "-c", "exit 0"},
		"",
		nil,
		nil,
		nil,
		nil,
		runTargetOptions{UseProxy: false, PreserveTTY: true, Stdout: io.Discard, Stderr: io.Discard},
	)
	if err != nil {
		t.Fatalf("runTargetOnceWithOptions error: %v", err)
	}
	if gotCmd == nil {
		t.Fatal("runTargetCommand was not called")
	}
	if gotCmd.SysProcAttr != nil && gotCmd.SysProcAttr.Setpgid {
		t.Fatal("interactive PreserveTTY command was moved to a background process group")
	}
}

func TestRunTargetOnceHeadlessCreatesProcessGroup(t *testing.T) {
	lockCLITestHooks(t)

	shell := requireShell(t)
	prevRunTargetCommand := runTargetCommand
	t.Cleanup(func() { runTargetCommand = prevRunTargetCommand })
	var gotCmd *exec.Cmd
	runTargetCommand = func(name string, args ...string) *exec.Cmd {
		gotCmd = exec.Command(name, args...)
		return gotCmd
	}

	err := runTargetOnceWithOptions(
		context.Background(),
		[]string{shell, "-c", "exit 0"},
		"",
		nil,
		nil,
		nil,
		nil,
		runTargetOptions{UseProxy: false, PreserveTTY: false, Stdout: io.Discard, Stderr: io.Discard},
	)
	if err != nil {
		t.Fatalf("runTargetOnceWithOptions error: %v", err)
	}
	if gotCmd == nil || gotCmd.SysProcAttr == nil || !gotCmd.SysProcAttr.Setpgid {
		t.Fatal("headless command was not put in an isolated process group")
	}
}
