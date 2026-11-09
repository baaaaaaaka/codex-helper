// A native, deliberately faulty CLI used only by cxp_boot_e2e tests.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func event(kind, version string) {
	f, err := os.OpenFile(filepath.Join(os.Getenv("CXP_BOOT_TEST_DIR"), "events"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	_, _ = fmt.Fprintf(f, "%s %s %d\n", kind, version, os.Getpid())
	_ = f.Close()
}

func waitRelease() {
	for {
		if _, err := os.Stat(filepath.Join(os.Getenv("CXP_BOOT_TEST_DIR"), "release")); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	if code, handled := helperruntime.HandleProtocolProbe(os.Args, os.Stdout); handled {
		os.Exit(code)
	}
	if len(os.Args) > 1 && os.Args[1] == "--probe-flood" {
		fmt.Print(strings.Repeat("x", 100<<10))
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "--probe-descendant" {
		event("entered", "v2.0.0")
		cmd := exec.Command(os.Args[0], "--descendant")
		cmd.Env, cmd.Stdout, cmd.Stderr = os.Environ(), os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			panic(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	// Inert download probe. Like the real CLI this cannot enter an installed
	// runtime or execute operations when CXP_RUNTIME_DISABLE is set.
	if len(os.Args) == 2 && os.Args[1] == "--version" && os.Getenv(helperruntime.EnvDisable) == "1" {
		fmt.Println("cxp version 2.0.0")
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "--descendant" {
		event("descendant", "v2.0.0")
		for {
			time.Sleep(time.Hour)
		}
	}
	if code, handled, err := helperruntime.Launch("v1.0.0", os.Args); handled || err != nil {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(code)
	}
	if err := helperruntime.PrepareBoot(); err != nil {
		os.Exit(1)
	}
	v := "base"
	if c, ok := helperruntime.Current(); ok {
		v = c.Version
	}
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("cxp version", strings.TrimPrefix(v, "v"))
		return
	}
	event("entered", v)
	mode := os.Getenv("CXP_BOOT_TEST_MODE")
	if v == "v2.0.0" {
		switch mode {
		case "crash":
			os.Exit(42)
		case "hang":
			for {
				time.Sleep(time.Hour)
			}
		case "cancel":
			cancelSelf()
		case "handled-cancel":
			handledCancel()
		case "descendant":
			cmd := exec.Command(os.Args[0], "--descendant")
			cmd.Env, cmd.Stdout, cmd.Stderr = os.Environ(), os.Stdout, os.Stderr
			if err := cmd.Start(); err != nil {
				panic(err)
			}
			for {
				time.Sleep(time.Hour)
			}
		case "setup":
			if err := helperruntime.BeforeInteractiveSetup(); err != nil {
				panic(err)
			}
			event("setup", v)
			waitRelease()
		}
	}
	// Exercise the native startup protocol without a physical tty. Separate
	// production-CLI PTY tests exercise the real first frame and terminal.
	if err := helperruntime.Ready(); err != nil {
		panic(err)
	}
	event("input", v)
	if mode == "hold" {
		waitRelease()
	}
	if v == "v2.0.0" && mode == "post-ready-crash" {
		os.Exit(43)
	}
}
