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
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut/newcore"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut/oldcore"
)

// EnvSUT names the adapter to use:
//
//	oldcore         (default) the current core as a subprocess; real SIGKILL,
//	                real codexrunner.AppServerRunner + fake app-server binary
//	oldcore-inproc  the M1 in-process adapter (fake teams.Executor)
//	newcore         the rebuilt core (internal/core/engine) as a subprocess:
//	                real channel/teams adapter, codexagent + fake app-server
//	cxp             the new core through the production entry point:
//	                `cxp teams run --core=new` (e2e build tag)
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
	case "newcore":
		dir, err := os.MkdirTemp("", "e2e-bin-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		defer os.RemoveAll(dir)
		bins := newcore.Binaries{
			SUT:   filepath.Join(dir, "newcore-sut"),
			Codex: filepath.Join(dir, "codex"),
		}
		start := time.Now()
		if err := goBuild(bins.SUT, "./internal/e2e/sut/newcore/cmd/newcore-sut"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if err := goBuild(bins.Codex, "./internal/e2e/fakecodex/cmd/fakecodex"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		fmt.Fprintf(os.Stderr, "e2e: built SUT binaries in %v\n", time.Since(start).Round(time.Millisecond))
		harness.DefaultFactory = newcore.ProcessFactory(bins)
		harness.SetProcessEnv = false
	case "cxp":
		dir, err := os.MkdirTemp("", "e2e-bin-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		defer os.RemoveAll(dir)
		bins := newcore.Binaries{
			SUT:   filepath.Join(dir, "cxp"),
			Codex: filepath.Join(dir, "codex"),
		}
		start := time.Now()
		// The real cxp entry point; the e2e tag only adds the sandbox
		// transport and the scenario's timing (internal/cli/teams_newcore_e2e.go).
		if err := goBuild(bins.SUT, "./cmd/codex-proxy", "-tags", "e2e"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if err := goBuild(bins.Codex, "./internal/e2e/fakecodex/cmd/fakecodex"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		fmt.Fprintf(os.Stderr, "e2e: built SUT binaries in %v\n", time.Since(start).Round(time.Millisecond))
		harness.DefaultFactory = newcore.CXPFactory(bins)
		harness.SetProcessEnv = false
	default:
		fmt.Fprintf(os.Stderr, "unknown %s %q (known: oldcore, oldcore-inproc, newcore, cxp)\n", EnvSUT, name)
		return 2
	}
	return m.Run()
}

// goBuild builds pkg (relative to the module root) into out.
func goBuild(out, pkg string, flags ...string) error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	cmd := exec.Command("go", append(append([]string{"build", "-o", out}, flags...), pkg)...)
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
