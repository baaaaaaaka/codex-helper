// Package sut runs the system under test of the scenario packages: the
// release `cxp` binary (./cmd/codex-proxy, built without build tags),
// started as a user starts the Teams helper (`cxp teams run`) and reached
// only through its external interfaces (see internal/e2e/doc.go).
package sut

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// EnvSUT may name the SUT; "cxp" (the default) is the only one.
const EnvSUT = "E2E_SUT"

// Run is the TestMain body of every scenario package: it builds the cxp
// binary and the fake Codex (once per test binary) and runs the tests.
func Run(m *testing.M) int {
	if name := os.Getenv(EnvSUT); name != "" && name != "cxp" {
		fmt.Fprintf(os.Stderr, "unknown %s %q (the only SUT is cxp)\n", EnvSUT, name)
		return 2
	}
	dir, err := os.MkdirTemp("", "e2e-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer os.RemoveAll(dir)
	binDir = dir
	cxp, codex := filepath.Join(dir, "cxp"), filepath.Join(dir, "codex")
	start := time.Now()
	if err := goBuild(cxp, "./cmd/codex-proxy"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if err := goBuild(codex, "./internal/e2e/fakecodex/cmd/fakecodex"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Fprintf(os.Stderr, "e2e: built cxp and the fake Codex in %v\n", time.Since(start).Round(time.Millisecond))
	harness.CLIBinary = cxp
	fakeCodex = codex
	harness.DefaultFactory = Factory(cxp, codex)
	return harness.RunAndCheckLeaks(m)
}

var (
	binDir     string
	fakeCodex  string
	versionsMu sync.Mutex
	versions   = map[string]string{}
)

// VersionedCLI returns a cxp binary built like the SUT but reporting
// version v (a newer release installed while the helper runs the
// SUT's), built once per test binary.
func VersionedCLI(v string) (string, error) {
	versionsMu.Lock()
	defer versionsMu.Unlock()
	if p, ok := versions[v]; ok {
		return p, nil
	}
	if binDir == "" {
		return "", fmt.Errorf("sut: VersionedCLI outside sut.Run")
	}
	p := filepath.Join(binDir, "cxp-"+strings.Map(func(r rune) rune {
		if r == '.' || r == '-' || ('0' <= r && r <= '9') || ('a' <= r && r <= 'z') {
			return r
		}
		return '_'
	}, v))
	if err := goBuild(p, "./cmd/codex-proxy", "-ldflags", "-X github.com/baaaaaaaka/codex-helper/internal/cli.version="+v); err != nil {
		return "", err
	}
	versions[v] = p
	return p, nil
}

// Factory returns the SUT factory for the cxp binary.
//
// Lifecycle (harness.ProcessSUT):
//   - Start runs `cxp teams run --codex-path <fake codex> ...` with the
//     scenario's environment (HOME, XDG dirs, CODEX_HOME, token caches) and
//     the test seam (CXP_TEST_ONLY_SEAM: the fake Graph endpoint, the
//     instance tag, the timing profile and the host name).
//   - Stop sends SIGTERM and waits; Crash sends SIGKILL to the helper only
//     (its app-servers see their stdin close).
//   - Restart is Stop + Start on the same state.
//   - An exit with harness.ServiceRestartExitCode is followed by a new
//     instance on the same state, as the service manager does.
func Factory(cxp, codex string) harness.SUTFactory {
	return factory(cxp, codex)
}

// FakeCodexBinary is the fake Codex executable of the test binary, for a
// scenario that installs it where a user's Codex lives (with
// InstalledCodexFactory).
func FakeCodexBinary() string { return fakeCodex }

// InstalledCodexFactory is Factory without --codex-path: the helper finds
// Codex the way it does on a user's machine (the CXP-managed install under
// HOME, else installs one; never the network here). The scenario puts
// FakeCodexBinary where it should be found before the SUT starts.
func InstalledCodexFactory() harness.SUTFactory {
	if harness.CLIBinary == "" {
		panic("sut: InstalledCodexFactory outside sut.Run")
	}
	return factory(harness.CLIBinary, "")
}

func factory(cxp, codex string) harness.SUTFactory {
	return func(cfg harness.SUTConfig) (harness.SUT, error) {
		args := []string{"teams", "run"}
		if codex != "" {
			args = append(args, "--codex-path", codex)
		}
		args = append(args, "--machine-registry=false", "--auto-update=false", "--auto-service=false")
		args = append(args, cfg.Args...)
		name := "cxp"
		if cfg.Peer != "" {
			name += "-" + cfg.Peer
		}
		env := func(tag string) []string {
			vars := map[string]string{}
			for k, v := range cfg.Env.Vars {
				vars[k] = v
			}
			vars["PATH"] = os.Getenv("PATH")
			vars["TMPDIR"] = os.Getenv("TMPDIR")
			vars["GOTRACEBACK"] = "all"
			// The fake Codex reads its own settings from the environment it
			// inherits from cxp, like a real Codex reads CODEX_HOME.
			vars[fakecodex.EnvBrainURL] = cfg.Codex.BrainURL()
			vars[fakecodex.EnvKind] = "work"
			vars[fakecodex.EnvInstance] = tag
			for k, v := range cfg.UserEnv {
				vars[k] = v
			}
			vars[harness.EnvSeam] = cfg.Seam.Value(cfg.Graph.URL(), tag, cfg.Interval)
			out := make([]string, 0, len(vars))
			for k, v := range vars {
				if v != "" || k == "CODEX_DIR" {
					out = append(out, k+"="+v)
				}
			}
			sort.Strings(out)
			return out
		}
		return harness.NewProcessSUT(cfg, harness.ProcessSpec{Name: name, Binary: cxp, Args: args, Env: env}), nil
	}
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
