package newcore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// Binaries are the executables the process adapter runs.
type Binaries struct {
	SUT   string // internal/e2e/sut/newcore/cmd/newcore-sut
	Codex string // fakecodex/cmd/fakecodex
}

// ExitRestart is the exit code with which the SUT asks to be started again
// on the same state (`helper restart`), like a service supervisor would.
const ExitRestart = 75

// ProcessFactory returns a factory for the new-core subprocess adapter.
//
// Lifecycle semantics (same as the oldcore process adapter):
//
//   - Start launches newcore-sut with only the scenario's environment
//     plus PATH. It talks to the fake Graph through the sandbox transport
//     with the real internal/channel/teams adapter, and runs Codex turns
//     through internal/core/codexagent against the fake app-server binary.
//   - Stop sends SIGTERM and waits for the process to exit.
//   - Crash sends SIGKILL to the helper process only. Its app-servers live
//     in their own process groups and see stdin EOF.
//   - Restart is Stop + Start on the same HOME / store.
//   - An exit with code 75 (ExitRestart) that the harness did not cause is
//     a restart request: a new instance starts on the same state.
func ProcessFactory(bins Binaries) harness.SUTFactory {
	return func(cfg harness.SUTConfig) (harness.SUT, error) {
		return &ProcSUT{cfg: cfg, bins: bins}, nil
	}
}

// ProcSUT runs the new core as a child process.
type ProcSUT struct {
	cfg  harness.SUTConfig
	bins Binaries

	mu         sync.Mutex
	out        syncBuffer
	generation int
	cur        *procInstance
}

type procInstance struct {
	tag      string
	cmd      *exec.Cmd
	done     chan struct{}
	err      error
	signaled atomic.Bool // stopped or killed by the harness on purpose
}

// Name implements harness.SUT.
func (s *ProcSUT) Name() string { return "newcore" }

// Instance implements harness.SUT.
func (s *ProcSUT) Instance() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil {
		return ""
	}
	return s.cur.tag
}

// Output implements harness.SUT.
func (s *ProcSUT) Output() string { return s.out.String() }

// Running implements harness.SUT.
func (s *ProcSUT) Running() bool {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	if cur == nil {
		return false
	}
	select {
	case <-cur.done:
		return false
	default:
		return true
	}
}

// ExitErr implements harness.SUT. Exits caused by Stop or Crash are not
// errors.
func (s *ProcSUT) ExitErr() error {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	if cur == nil {
		return nil
	}
	select {
	case <-cur.done:
		if cur.signaled.Load() {
			return nil
		}
		return cur.err
	default:
		return nil
	}
}

// Pid is the current helper process id (0 when not running).
func (s *ProcSUT) Pid() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil || s.cur.cmd.Process == nil {
		return 0
	}
	return s.cur.cmd.Process.Pid
}

// CPUTime is the user+system CPU time of the current helper process: read
// from /proc while it runs (Linux), from its exit status once it exited.
func (s *ProcSUT) CPUTime() time.Duration {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	if cur == nil || cur.cmd.Process == nil {
		return 0
	}
	select {
	case <-cur.done:
		return cur.cmd.ProcessState.UserTime() + cur.cmd.ProcessState.SystemTime()
	default:
	}
	return procCPUTime(cur.cmd.Process.Pid)
}

// procCPUTime reads utime+stime from /proc/<pid>/stat (USER_HZ = 100 on
// every Linux the suite runs on).
func procCPUTime(pid int) time.Duration {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	// Fields after the parenthesised command name; utime and stime are
	// fields 14 and 15 overall, i.e. 12 and 13 after the name.
	rest := string(data)
	if i := strings.LastIndexByte(rest, ')'); i >= 0 {
		rest = rest[i+1:]
	}
	f := strings.Fields(rest)
	if len(f) < 13 {
		return 0
	}
	ut, _ := strconv.ParseInt(f[11], 10, 64)
	st, _ := strconv.ParseInt(f[12], 10, 64)
	return time.Duration(ut+st) * 10 * time.Millisecond
}

func (s *ProcSUT) childEnv(tag string) []string {
	vars := map[string]string{}
	for k, v := range s.cfg.Env.Vars {
		vars[k] = v
	}
	vars["PATH"] = os.Getenv("PATH")
	vars["TMPDIR"] = os.Getenv("TMPDIR")
	vars["GOTRACEBACK"] = "all"
	vars["E2E_SUT_GRAPH_URL"] = s.cfg.Graph.URL()
	vars["E2E_SUT_INSTANCE"] = tag
	vars["E2E_SUT_INTERVAL"] = s.cfg.Interval.String()
	vars["E2E_SUT_CODEX_BIN"] = s.bins.Codex
	vars[fakecodex.EnvBrainURL] = s.cfg.Codex.BrainURL()
	for k, v := range s.cfg.ExtraEnv {
		vars[k] = v
	}
	env := make([]string, 0, len(vars))
	for k, v := range vars {
		if v != "" || k == "CODEX_DIR" {
			env = append(env, k+"="+v)
		}
	}
	sort.Strings(env)
	return env
}

// Start implements harness.SUT.
func (s *ProcSUT) Start(ctx context.Context) error {
	if s.Running() && !s.restarting() {
		return errors.New("newcore: already running")
	}
	s.mu.Lock()
	s.generation++
	tag := fmt.Sprintf("newcore-%d", s.generation)
	s.mu.Unlock()
	fmt.Fprintf(&s.out, "=== e2e: starting %s ===\n", tag)

	cmd := exec.Command(s.bins.SUT)
	cmd.Env = s.childEnv(tag)
	cmd.Dir = s.cfg.Env.Home
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		return fmt.Errorf("start %s: %w", s.bins.SUT, err)
	}
	inst := &procInstance{tag: tag, cmd: cmd, done: make(chan struct{})}
	s.mu.Lock()
	s.cur = inst
	s.mu.Unlock()
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		s.scan(pr, tag)
	}()
	go func() {
		err := cmd.Wait()
		_ = pw.Close()
		<-scanned
		inst.err = err
		fmt.Fprintf(&s.out, "=== e2e: %s exited: %v ===\n", tag, err)
		restart := !inst.signaled.Load() && cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == ExitRestart
		if restart {
			// Like a service supervisor: start again on the same state.
			// The new instance is current before done closes, so Running
			// stays true across the restart.
			if serr := s.Start(context.Background()); serr != nil {
				fmt.Fprintf(&s.out, "=== e2e: restart after exit %d failed: %v ===\n", ExitRestart, serr)
			}
		}
		close(inst.done)
	}()
	return nil
}

func (s *ProcSUT) scan(r io.Reader, tag string) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		line := sc.Text()
		if rest, ok := strings.CutPrefix(line, BlockedPrefix); ok {
			s.cfg.Graph.RecordBlocked(fakegraph.BlockedRequest{At: time.Now(), Instance: tag, URL: rest})
		}
		fmt.Fprintln(&s.out, line)
	}
	_, _ = io.Copy(io.Discard, r)
}

// BlockedPrefix starts the line the SUT main prints for a refused foreign
// request (see cmd/newcore-sut).
const BlockedPrefix = "E2E-BLOCKED "

// Stop implements harness.SUT.
func (s *ProcSUT) Stop(ctx context.Context) error { return s.signal(ctx, syscall.SIGTERM) }

// Crash implements harness.SUT.
func (s *ProcSUT) Crash(ctx context.Context) error { return s.signal(ctx, syscall.SIGKILL) }

func (s *ProcSUT) signal(ctx context.Context, sig syscall.Signal) error {
	s.mu.Lock()
	inst := s.cur
	s.mu.Unlock()
	if inst == nil {
		return nil
	}
	select {
	case <-inst.done:
		return nil
	default:
	}
	verb := "stopping"
	if sig == syscall.SIGKILL {
		verb = "crashing (SIGKILL)"
	}
	fmt.Fprintf(&s.out, "=== e2e: %s %s ===\n", verb, inst.tag)
	inst.signaled.Store(true)
	if err := inst.cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-inst.done:
	case <-ctx.Done():
		_ = inst.cmd.Process.Kill()
		<-inst.done
		return fmt.Errorf("newcore: %s did not exit after %v: %w", inst.tag, sig, ctx.Err())
	}
	return nil
}

// DumpStacks makes the SUT print every goroutine (SIGQUIT) and exit, and
// returns what it wrote from then on. It is a debugging aid: the SUT is
// gone afterwards.
func (s *ProcSUT) DumpStacks(ctx context.Context) string {
	mark := len(s.out.String())
	if err := s.signal(ctx, syscall.SIGQUIT); err != nil {
		return err.Error()
	}
	return s.out.String()[mark:]
}

// Restart implements harness.SUT.
func (s *ProcSUT) Restart(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	return s.Start(ctx)
}

// restarting reports that the current instance exited asking for a restart
// (its done channel is not closed yet).
func (s *ProcSUT) restarting() bool {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	return cur != nil && cur.cmd.ProcessState != nil && cur.cmd.ProcessState.ExitCode() == ExitRestart && !cur.signaled.Load()
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Clone(b.buf.String())
}
