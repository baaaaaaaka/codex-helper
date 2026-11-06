package harness

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// PeerOptions configures a second helper on the scenario's state.
type PeerOptions struct {
	// Seam is merged over the scenario's seam (e.g. another Hostname).
	Seam Seam
	// Args are extra `cxp teams run` flags (e.g. --managed-service-child).
	Args []string
	// UserEnv are merged over the scenario's UserEnv: what differs on the
	// other host (e.g. SLURM_JOB_ID on a batch-job node). Test switches are
	// refused as for Options.UserEnv.
	UserEnv map[string]string
	// Env runs the peer in another environment, e.g. Env.MovedTo for a
	// machine whose home is a copy (nil: the scenario's).
	Env *Env
}

// NewPeer builds a second SUT on this scenario's state (same HOME, Graph
// and Codex), as another host sharing the home would run it. The peer is
// not started; the scenario stops it at teardown and checks it like the
// main SUT.
func (s *Scenario) NewPeer(peer string, opts PeerOptions) SUT {
	s.T.Helper()
	cfg := s.sutConfig(peer, opts.Seam, opts.Args)
	if opts.Env != nil {
		cfg.Env = opts.Env
	}
	if len(opts.UserEnv) > 0 {
		env := map[string]string{}
		for k, v := range s.Opts.UserEnv {
			env[k] = v
		}
		for k, v := range opts.UserEnv {
			if strings.HasPrefix(k, "E2E_") || strings.HasPrefix(k, "CXP_TEST") {
				s.T.Fatalf("harness: peer UserEnv %s is a test switch; use PeerOptions.Seam", k)
			}
			env[k] = v
		}
		cfg.UserEnv = env
	}
	p, err := s.Opts.Factory(cfg)
	if err != nil {
		s.T.Fatalf("build peer SUT: %v", err)
	}
	s.mu.Lock()
	s.suts = append(s.suts, p)
	s.mu.Unlock()
	return p
}

// Freeze stops a SUT's helper process with SIGSTOP (a hung or partitioned
// host: no heartbeat, no request); Thaw resumes it.
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

// StorePath is the helper's store database as `cxp teams status` reports
// it ("Store: <path>"), "" when status names none.
func (s *Scenario) StorePath() string {
	s.T.Helper()
	res := s.runCLI(20*time.Second, false, "teams", "status")
	for _, line := range strings.Split(res.Stdout+"\n"+res.Stderr, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "Store:"); ok {
			path := strings.TrimSpace(rest)
			if i := strings.Index(path, " ("); i >= 0 {
				path = path[:i]
			}
			if _, err := os.Stat(path); err == nil {
				return path
			}
		}
	}
	return ""
}

// CLIBinary is the cxp binary scenarios run CLI commands with (sut.Run).
var CLIBinary string

// NeedsCLI is kept for the scenarios that drive the cxp command line; the
// SUT is always the cxp binary, so it only checks the binary was built.
func NeedsCLI(t interface {
	Helper()
	Fatal(...any)
}) {
	t.Helper()
	if CLIBinary == "" {
		t.Fatal("harness: no cxp binary (run the scenarios through sut.Run)")
	}
}

// CLIRun is one `cxp` command run by the scenario, as the user would see it.
type CLIRun struct {
	Args   []string
	Stdout string
	Stderr string
	Code   int
}

// RunCLI runs `cxp <args>` with the scenario's environment (HOME, state,
// token caches; never the host's) and returns its combined output and exit
// code. The run is part of the transcript golden.
func (s *Scenario) RunCLI(timeout time.Duration, args ...string) (string, int) {
	s.T.Helper()
	res := s.runCLI(timeout, true, args...)
	return res.Combined, res.Code
}

// CLIAs describes who runs a cxp command: by default the scenario's user on
// the scenario's machine.
type CLIAs struct {
	// Env overrides user environment variables (e.g. another HOME: another
	// machine of the same user).
	Env map[string]string
	// Dir is the working directory ("" = HOME).
	Dir string
	// Hostname is the host name the command believes it runs on.
	Hostname string
	// Poll marks one try of a wait loop: how many tries a wait takes is
	// timing, so the run is not part of the golden. The scenario records
	// the settled result with one more run without Poll.
	Poll bool
	// Binary runs another cxp build ("" = CLIBinary), e.g. a newer
	// version installed while the helper still runs the older one.
	Binary string
}

// RunCLIAs is RunCLI run as as; the run is part of the golden unless
// as.Poll is set.
func (s *Scenario) RunCLIAs(timeout time.Duration, as CLIAs, args ...string) CLIResult {
	s.T.Helper()
	return s.runCLIWith(timeout, !as.Poll, as, args...)
}

// CLIResult is the outcome of one cxp command.
type CLIResult struct {
	Stdout, Stderr, Combined string
	Code                     int
}

func (s *Scenario) runCLI(timeout time.Duration, record bool, args ...string) CLIResult {
	return s.runCLIWith(timeout, record, CLIAs{}, args...)
}

func (s *Scenario) runCLIWith(timeout time.Duration, record bool, as CLIAs, args ...string) CLIResult {
	s.T.Helper()
	NeedsCLI(s.T)
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	defer cancel()
	bin := CLIBinary
	if as.Binary != "" {
		bin = as.Binary
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	vars := map[string]string{"PATH": os.Getenv("PATH"), "TMPDIR": os.Getenv("TMPDIR")}
	for k, v := range s.Env.Vars {
		vars[k] = v
	}
	for k, v := range s.Opts.UserEnv {
		vars[k] = v
	}
	// CLI commands that talk to Teams (workflow test, delegate) use the
	// same seam as the helper, tagged as the CLI.
	for k, v := range as.Env {
		if strings.HasPrefix(k, "E2E_") || strings.HasPrefix(k, "CXP_TEST") {
			s.T.Fatalf("harness: CLI env %s is a test switch", k)
		}
		vars[k] = v
	}
	vars[EnvSeam] = s.Opts.Seam.With(Seam{Hostname: as.Hostname}).Value(s.Graph.URL(), "cxp-cli", s.Opts.Interval)
	for k, v := range vars {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Dir = s.Env.Home
	if h := as.Env["HOME"]; h != "" {
		cmd.Dir = h
	}
	if as.Dir != "" {
		cmd.Dir = as.Dir
	}
	var stdout, stderr, combined bytes.Buffer
	var mu lockedWriter
	mu.w = &combined
	cmd.Stdout = io.MultiWriter(&stdout, &mu)
	cmd.Stderr = io.MultiWriter(&stderr, &mu)
	err := cmd.Run()
	code := 0
	if err != nil {
		code = -1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
	res := CLIResult{Stdout: stdout.String(), Stderr: stderr.String(), Combined: combined.String(), Code: code}
	s.T.Logf("cxp %s -> exit %d\n%s", strings.Join(args, " "), code, clipTail(res.Combined, 2000))
	if record {
		s.mu.Lock()
		s.cli = append(s.cli, CLIRun{Args: append([]string(nil), args...), Stdout: res.Stdout, Stderr: res.Stderr, Code: code})
		s.mu.Unlock()
	}
	return res
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func clipTail(s string, n int) string {
	if len(s) > n {
		return fmt.Sprintf("…%s", s[len(s)-n:])
	}
	return s
}

// Ctx is the scenario context (cancelled at cleanup).
func (s *Scenario) Ctx() context.Context { return s.ctx }
