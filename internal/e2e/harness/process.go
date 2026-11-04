package harness

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// BlockedLinePrefix starts the output line by which a subprocess SUT reports
// a request its sandbox transport refused (see sandbox.New); ProcessSUT
// records it on the fake Graph as a blocked request.
const BlockedLinePrefix = "E2E-BLOCKED "

// ServiceRestartDelay is how long ProcessSUT, acting as the SUT's service
// supervisor, waits before starting the replacement instance after an exit
// with ServiceRestartExitCode (like systemd's RestartSec).
const ServiceRestartDelay = 500 * time.Millisecond

// ProcessSpec describes a subprocess SUT for ProcessSUT.
type ProcessSpec struct {
	// Name is the adapter name; instance tags are "<Name>-<n>".
	Name string
	// Binary is the executable to run, with Args.
	Binary string
	Args   []string
	// Dir is the working directory (default: the scenario HOME).
	Dir string
	// Env returns the complete environment of the instance tagged tag.
	Env func(tag string) []string
}

// ProcessSUT is the harness-side runner shared by subprocess adapters. It
// implements the SUT lifecycle for one executable:
//
//   - Start runs a new instance (tag "<Name>-<n>") in its own process group
//     and captures its combined output.
//   - Stop sends SIGTERM and waits for the exit; Crash sends SIGKILL to the
//     instance process only (its children see their pipes close).
//   - Restart is Stop + Start.
//   - Service restarts: an instance that exits on its own with
//     ServiceRestartExitCode is replaced, after ServiceRestartDelay, by a new
//     instance on the same state, unless the harness started, stopped or
//     crashed the SUT in the meantime. While the replacement is pending the
//     SUT counts as running, and that exit is not an ExitErr.
//
// Adapters embed it and add their own environment and extras.
type ProcessSUT struct {
	spec ProcessSpec
	cfg  SUTConfig

	mu         sync.Mutex
	out        lockedBuffer
	generation int
	intent     int // bumped by every harness Start/Stop/Crash
	pending    bool
	restarts   int
	cur        *procInstance
}

type procInstance struct {
	tag        string
	cmd        *exec.Cmd
	done       chan struct{}
	err        error
	signaled   atomic.Bool // stopped or killed by the harness on purpose
	supervised atomic.Bool // exited asking for a service restart
}

// NewProcessSUT returns a runner for spec.
func NewProcessSUT(cfg SUTConfig, spec ProcessSpec) *ProcessSUT {
	if spec.Dir == "" && cfg.Env != nil {
		spec.Dir = cfg.Env.Home
	}
	return &ProcessSUT{spec: spec, cfg: cfg}
}

// Name implements SUT.
func (s *ProcessSUT) Name() string { return s.spec.Name }

// Instance implements SUT.
func (s *ProcessSUT) Instance() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil {
		return ""
	}
	return s.cur.tag
}

// Output implements SUT.
func (s *ProcessSUT) Output() string { return s.out.String() }

// ServiceRestarts is the number of service restarts the runner performed.
func (s *ProcessSUT) ServiceRestarts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restarts
}

// Running implements SUT. A pending service restart counts as running.
func (s *ProcessSUT) Running() bool {
	s.mu.Lock()
	cur, pending := s.cur, s.pending
	s.mu.Unlock()
	if pending {
		return true
	}
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

// ExitErr implements SUT. Exits caused by Stop or Crash, and service-restart
// exits, are not errors.
func (s *ProcessSUT) ExitErr() error {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	if cur == nil {
		return nil
	}
	select {
	case <-cur.done:
		if cur.signaled.Load() || cur.supervised.Load() {
			return nil
		}
		return cur.err
	default:
		return nil
	}
}

// Pid is the current instance's process id (0 when not running).
func (s *ProcessSUT) Pid() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil || s.cur.cmd.Process == nil {
		return 0
	}
	return s.cur.cmd.Process.Pid
}

// CPUTime is the user+system CPU time of the current instance: read from
// /proc while it runs (Linux), from its exit status once it exited.
func (s *ProcessSUT) CPUTime() time.Duration {
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

// Start implements SUT.
func (s *ProcessSUT) Start(ctx context.Context) error {
	s.mu.Lock()
	s.intent++
	s.pending = false
	s.mu.Unlock()
	return s.start(-1)
}

// start launches a new instance. With wantIntent >= 0 (a service restart)
// it does nothing unless the harness intent is still wantIntent.
func (s *ProcessSUT) start(wantIntent int) error {
	s.mu.Lock()
	if wantIntent >= 0 {
		if s.intent != wantIntent || !s.pending {
			s.mu.Unlock()
			return nil
		}
		s.pending = false
		s.restarts++
	}
	if cur := s.cur; cur != nil {
		select {
		case <-cur.done:
		default:
			s.mu.Unlock()
			return fmt.Errorf("%s: already running", s.spec.Name)
		}
	}
	s.generation++
	tag := fmt.Sprintf("%s-%d", s.spec.Name, s.generation)
	s.mu.Unlock()
	fmt.Fprintf(&s.out, "=== e2e: starting %s ===\n", tag)

	cmd := exec.Command(s.spec.Binary, s.spec.Args...)
	if s.spec.Env != nil {
		cmd.Env = s.spec.Env(tag)
	}
	cmd.Dir = s.spec.Dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		return fmt.Errorf("start %s: %w", s.spec.Binary, err)
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
		var exitErr *exec.ExitError
		restart := !inst.signaled.Load() && errors.As(err, &exitErr) && exitErr.ExitCode() == ServiceRestartExitCode
		want := -1
		if restart {
			inst.supervised.Store(true)
			s.mu.Lock()
			if s.cur == inst {
				s.pending = true
				want = s.intent
			}
			s.mu.Unlock()
		}
		fmt.Fprintf(&s.out, "=== e2e: %s exited: %v ===\n", tag, err)
		close(inst.done)
		if want >= 0 {
			fmt.Fprintf(&s.out, "=== e2e: %s asked for a service restart ===\n", tag)
			time.Sleep(ServiceRestartDelay)
			if err := s.start(want); err != nil {
				fmt.Fprintf(&s.out, "=== e2e: service restart after %s failed: %v ===\n", tag, err)
			}
		}
	}()
	return nil
}

func (s *ProcessSUT) scan(r io.Reader, tag string) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		line := sc.Text()
		if rest, ok := strings.CutPrefix(line, BlockedLinePrefix); ok && s.cfg.Graph != nil {
			s.cfg.Graph.RecordBlocked(fakegraph.BlockedRequest{At: time.Now(), Instance: tag, URL: rest})
		}
		fmt.Fprintln(&s.out, line)
	}
	_, _ = io.Copy(io.Discard, r)
}

// Stop implements SUT.
func (s *ProcessSUT) Stop(ctx context.Context) error { return s.signal(ctx, syscall.SIGTERM) }

// Crash implements SUT.
func (s *ProcessSUT) Crash(ctx context.Context) error { return s.signal(ctx, syscall.SIGKILL) }

func (s *ProcessSUT) signal(ctx context.Context, sig syscall.Signal) error {
	s.mu.Lock()
	s.intent++ // cancels a pending service restart
	s.pending = false
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
	switch sig {
	case syscall.SIGKILL:
		verb = "crashing (SIGKILL)"
	case syscall.SIGQUIT:
		verb = "dumping stacks (SIGQUIT)"
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
		return fmt.Errorf("%s: %s did not exit after %v: %w", s.spec.Name, inst.tag, sig, ctx.Err())
	}
	return nil
}

// DumpStacks makes the instance print every goroutine (SIGQUIT) and exit,
// and returns what it wrote from then on. It is a debugging aid: the SUT is
// gone afterwards.
func (s *ProcessSUT) DumpStacks(ctx context.Context) string {
	mark := len(s.out.String())
	if err := s.signal(ctx, syscall.SIGQUIT); err != nil {
		return err.Error()
	}
	return s.out.String()[mark:]
}

// Restart implements SUT.
func (s *ProcessSUT) Restart(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	return s.Start(ctx)
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Clone(b.buf.String())
}
