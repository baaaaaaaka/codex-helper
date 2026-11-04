// Package sut selects the system under test for the scenario packages.
package sut

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut/oldcore"
)

// EnvSUT names the adapter to use:
//
//	oldcore         (default) the current core as a subprocess; real SIGKILL,
//	                real codexrunner.AppServerRunner + fake app-server binary
//	oldcore-inproc  the M1 in-process adapter (fake teams.Executor)
const EnvSUT = "E2E_SUT"

// Run is the TestMain body of every scenario package: it selects the SUT,
// builds the binaries it needs (once per test binary) and runs the tests.
func Run(m *testing.M) int {
	switch name := os.Getenv(EnvSUT); name {
	case "", "oldcore":
		dir, err := os.MkdirTemp("", "e2e-bin-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		defer os.RemoveAll(dir)
		bins := oldcore.Binaries{
			SUT:   filepath.Join(dir, "oldcore-sut"),
			Codex: filepath.Join(dir, "codex"),
		}
		start := time.Now()
		if err := goBuild(bins.SUT, "./internal/e2e/sut/oldcore/cmd/oldcore-sut"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if err := goBuild(bins.Codex, "./internal/e2e/fakecodex/cmd/fakecodex"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		fmt.Fprintf(os.Stderr, "e2e: built SUT binaries in %v\n", time.Since(start).Round(time.Millisecond))
		harness.DefaultFactory = oldcore.ProcessFactory(bins)
		harness.SetProcessEnv = false
	case "oldcore-inproc":
		harness.DefaultFactory = oldcore.Factory
	default:
		fmt.Fprintf(os.Stderr, "unknown %s %q (known: oldcore, oldcore-inproc)\n", EnvSUT, name)
		return 2
	}
	return m.Run()
}

// goBuild builds pkg (relative to the module root) into out.
func goBuild(out, pkg string) error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = root
	if msg, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("go build %s: %v\n%s", pkg, err, msg)
	}
	return nil
}

func moduleRoot() (string, error) {
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMOD: %w", err)
	}
	mod := strings.TrimSpace(string(out))
	if mod == "" || mod == os.DevNull {
		return "", fmt.Errorf("not inside a Go module")
	}
	return filepath.Dir(mod), nil
}
