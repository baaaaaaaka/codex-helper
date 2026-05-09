//go:build linux

package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRunTargetPreserveTTYAllowsFakeCodexTUIInPTY(t *testing.T) {
	output := runTTYProcessGroupHarness(t, "preserve-tty")
	if !strings.Contains(output, "fake-codex-tui-ready") {
		t.Fatalf("fake Codex TUI did not start in foreground:\n%s", output)
	}
}

func TestRunTargetAutoPreservesInteractiveTTYInPTY(t *testing.T) {
	output := runTTYProcessGroupHarness(t, "auto-tty")
	if !strings.Contains(output, "fake-codex-tui-ready") {
		t.Fatalf("auto TTY detection did not keep fake Codex TUI in foreground:\n%s", output)
	}
}

func TestRunTargetOldProcessGroupBlocksFakeCodexTUIInPTY(t *testing.T) {
	output := runTTYProcessGroupHarness(t, "old-setpgid")
	if !strings.Contains(output, "old-mode-blocked") {
		t.Fatalf("old process-group behavior did not block the fake TUI as expected:\n%s", output)
	}
}

func runTTYProcessGroupHarness(t *testing.T, mode string) string {
	t.Helper()
	master, slave := openLinuxPTY(t)
	defer master.Close()
	defer slave.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTTYProcessGroupHarnessHelper$")
	cmd.Env = append(os.Environ(),
		"CODEX_HELPER_TTY_PROCESS_GROUP_HELPER=1",
		"CODEX_HELPER_TTY_PROCESS_GROUP_MODE="+mode,
	)
	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid:  true,
		Setctty: true,
		Ctty:    0,
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("start pty harness: %v", err)
	}
	_ = slave.Close()

	var output bytes.Buffer
	copyDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(&output, master)
		close(copyDone)
	}()

	err := cmd.Wait()
	_ = master.Close()
	<-copyDone
	if ctx.Err() != nil {
		t.Fatalf("pty harness timed out in mode %s:\n%s", mode, output.String())
	}
	if err != nil {
		t.Fatalf("pty harness failed in mode %s: %v\n%s", mode, err, output.String())
	}
	return output.String()
}

func openLinuxPTY(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	masterFD, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Skipf("open /dev/ptmx: %v", err)
	}
	if err := unix.IoctlSetPointerInt(masterFD, unix.TIOCSPTLCK, 0); err != nil {
		_ = unix.Close(masterFD)
		t.Skipf("unlock pty: %v", err)
	}
	ptn, err := unix.IoctlGetInt(masterFD, unix.TIOCGPTN)
	if err != nil {
		_ = unix.Close(masterFD)
		t.Skipf("get pty number: %v", err)
	}
	slaveName := fmt.Sprintf("/dev/pts/%d", ptn)
	slaveFD, err := unix.Open(slaveName, unix.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		_ = unix.Close(masterFD)
		t.Skipf("open pty slave %s: %v", slaveName, err)
	}
	return os.NewFile(uintptr(masterFD), "/dev/ptmx"), os.NewFile(uintptr(slaveFD), slaveName)
}

func TestTTYProcessGroupHarnessHelper(t *testing.T) {
	if os.Getenv("CODEX_HELPER_TTY_PROCESS_GROUP_HELPER") != "1" {
		return
	}
	mode := os.Getenv("CODEX_HELPER_TTY_PROCESS_GROUP_MODE")
	preserveTTY := mode == "preserve-tty"
	opts := runTargetOptions{
		UseProxy:    false,
		PreserveTTY: preserveTTY,
	}
	// Simulate the old behavior by making the command look headless to the
	// new auto-TTY detector while still attaching it to the same pty.
	if mode == "old-setpgid" {
		opts.Stdout = os.Stdout
		opts.Stderr = os.Stderr
	}
	err := runTargetWithFallbackWithOptions(
		context.Background(),
		[]string{os.Args[0], "-test.run=^TestTTYProcessGroupFakeCodexTUI$"},
		"",
		nil,
		nil,
		opts,
	)
	switch mode {
	case "preserve-tty", "auto-tty":
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s failed: %v\n", mode, err)
			os.Exit(1)
		}
	case "old-setpgid":
		if err == nil {
			fmt.Fprintln(os.Stderr, "old process-group behavior unexpectedly allowed fake TUI")
			os.Exit(1)
		}
		fmt.Fprintln(os.Stdout, "old-mode-blocked")
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode: %s\n", mode)
		os.Exit(1)
	}
	os.Exit(0)
}

func TestTTYProcessGroupFakeCodexTUI(t *testing.T) {
	if os.Getenv("CODEX_HELPER_TTY_PROCESS_GROUP_HELPER") != "1" {
		return
	}
	pgid := unix.Getpgrp()
	for _, stream := range []struct {
		name string
		file *os.File
	}{
		{name: "stdin", file: os.Stdin},
		{name: "stdout", file: os.Stdout},
	} {
		fg, err := unix.IoctlGetInt(int(stream.file.Fd()), unix.TIOCGPGRP)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fake TUI %s is not a terminal: %v\n", stream.name, err)
			os.Exit(42)
		}
		if fg != pgid {
			fmt.Fprintf(os.Stderr, "fake TUI %s is not foreground: foreground=%d pgid=%d\n", stream.name, fg, pgid)
			os.Exit(42)
		}
	}
	fmt.Fprintln(os.Stdout, "fake-codex-tui-ready")
	os.Exit(0)
}
