package harness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// NewPeer builds a second SUT on this scenario's state (same HOME, Graph
// and Codex), as another host sharing the home would run it. extraEnv is
// added to the scenario's SUTEnv. The peer is not started; the scenario
// stops it at cleanup.
func (s *Scenario) NewPeer(peer string, extraEnv map[string]string) SUT {
	s.T.Helper()
	env := map[string]string{}
	for k, v := range s.Opts.SUTEnv {
		env[k] = v
	}
	for k, v := range extraEnv {
		env[k] = v
	}
	p, err := s.Opts.Factory(SUTConfig{Env: s.Env, Graph: s.Graph, Codex: s.Codex, Interval: s.Opts.Interval, ExtraEnv: env,
		WorkflowWebhookURL: s.WorkflowWebhookURL, Peer: peer, Logf: s.T.Logf})
	if err != nil {
		s.T.Fatalf("build peer SUT: %v", err)
	}
	s.T.Cleanup(func() {
		Thaw(p)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if p.Running() {
			_ = p.Stop(ctx)
		}
	})
	return p
}

// Freeze stops a subprocess SUT's helper process with SIGSTOP (a hung or
// partitioned host: no heartbeat, no request), Thaw resumes it. Both are
// no-ops for an SUT without a process.
func Freeze(sut SUT) bool { return signalPID(sut, syscall.SIGSTOP) }

// Thaw resumes a frozen SUT (SIGCONT).
func Thaw(sut SUT) bool { return signalPID(sut, syscall.SIGCONT) }

func signalPID(sut SUT, sig syscall.Signal) bool {
	p, ok := sut.(interface{ Pid() int })
	if !ok || p.Pid() == 0 {
		return false
	}
	return syscall.Kill(p.Pid(), sig) == nil
}

// StorePath finds the SUT's store database under the scenario root
// (cxp: <state>/…/core.sqlite; newcore-sut: <state>/core/core.db), "" when
// it does not exist yet.
func (s *Scenario) StorePath() string {
	var found []string
	_ = filepath.WalkDir(s.Env.Root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && (d.Name() == "core.sqlite" || d.Name() == "core.db") {
			found = append(found, path)
		}
		return nil
	})
	sort.Strings(found)
	if len(found) == 0 {
		return ""
	}
	return found[0]
}

// CLIBinary is the cxp binary scenarios run CLI commands with (set by
// sut.Run when E2E_SUT=cxp; "" otherwise).
var CLIBinary string

// RunCLI runs `cxp <args>` with the scenario's environment (HOME, state,
// token caches; never the host's) and returns its combined output and exit
// code. It skips the scenario when no cxp binary is available.
func (s *Scenario) RunCLI(timeout time.Duration, args ...string) (string, int) {
	s.T.Helper()
	if CLIBinary == "" {
		s.T.Skip("needs the cxp binary (E2E_SUT=cxp)")
	}
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, CLIBinary, args...)
	env := []string{"PATH=" + os.Getenv("PATH"), "TMPDIR=" + os.Getenv("TMPDIR"), "CXP_RUNTIME_DISABLE=1"}
	for k, v := range s.Env.Vars {
		env = append(env, k+"="+v)
	}
	cmd.Env = env
	cmd.Dir = s.Env.Home
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		code = -1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
	text := string(out)
	s.T.Logf("cxp %s -> exit %d\n%s", strings.Join(args, " "), code, clipTail(text, 2000))
	return text, code
}

func clipTail(s string, n int) string {
	if len(s) > n {
		return fmt.Sprintf("…%s", s[len(s)-n:])
	}
	return s
}
