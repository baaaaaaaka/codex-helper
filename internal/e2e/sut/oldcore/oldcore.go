// Package oldcore adapts the current Teams core (internal/teams) to the E2E
// harness SUT interface, using only its exported API:
//
//   - teams.DefaultEffectiveAuthConfig / NewAuthManagerWithHTTPClient
//   - teams.NewBridgeWithHTTPClient
//   - (*teams.Bridge).Listen with teams.BridgeOptions
//   - the teams.Executor interface (implemented by a recording fake)
//
// Lifecycle semantics:
//
//   - Start constructs a new Bridge (new AuthManager, new sandbox transport
//     with a fresh instance tag) on the scenario's HOME and runs Listen in a
//     goroutine, exactly like `cxp teams run` minus service management,
//     auto-update, machine registry and model catalogs.
//   - Stop cancels the Listen context and waits for Listen to return. This is
//     the in-process equivalent of SIGTERM to `cxp teams run`.
//   - Crash first cuts the instance off the network (every further Graph
//     request fails with a transport error, idle connections are closed),
//     makes any in-flight Codex call of that instance fail, then cancels
//     Listen and waits for it to return. This is the closest in-process
//     equivalent of SIGKILL: no further Teams-visible effect is possible.
//     Unlike SIGKILL, deferred cleanups still run, so local store writes
//     during shutdown (lease release, turn bookkeeping) are NOT prevented.
//     A process-level crash needs a subprocess adapter (M2).
//   - Restart is Stop + Start on the same HOME / store.
package oldcore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

// Factory builds the old-core SUT.
func Factory(cfg harness.SUTConfig) (harness.SUT, error) {
	return &SUT{cfg: cfg}, nil
}

// SUT runs internal/teams in-process.
type SUT struct {
	cfg harness.SUTConfig

	mu         sync.Mutex
	out        syncBuffer
	generation int
	cur        *instance
	lastErr    error
}

type instance struct {
	tag       string
	transport *harness.Transport
	cancel    context.CancelFunc
	done      chan struct{}
	err       error
	killed    chan struct{}
}

// Name implements harness.SUT.
func (s *SUT) Name() string { return "oldcore" }

// Instance implements harness.SUT.
func (s *SUT) Instance() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil {
		return ""
	}
	return s.cur.tag
}

// Output implements harness.SUT.
func (s *SUT) Output() string { return s.out.String() }

// Running implements harness.SUT.
func (s *SUT) Running() bool {
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

// ExitErr implements harness.SUT.
func (s *SUT) ExitErr() error {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	if cur == nil {
		return s.lastErr
	}
	select {
	case <-cur.done:
		return cur.err
	default:
		return nil
	}
}

// Start implements harness.SUT.
func (s *SUT) Start(ctx context.Context) error {
	if s.Running() {
		return errors.New("oldcore: already running")
	}
	s.mu.Lock()
	s.generation++
	tag := fmt.Sprintf("oldcore-%d", s.generation)
	s.mu.Unlock()

	transport := harness.NewTransport(s.cfg.Graph, tag)
	client := transport.Client()
	fmt.Fprintf(&s.out, "=== e2e: starting %s ===\n", tag)

	authCfg, err := teams.DefaultEffectiveAuthConfig()
	if err != nil {
		return fmt.Errorf("auth config: %w", err)
	}
	auth := teams.NewAuthManagerWithHTTPClient(authCfg, client)
	bridge, err := teams.NewBridgeWithHTTPClient(ctx, auth, "", &s.out, client)
	if err != nil {
		return fmt.Errorf("NewBridgeWithHTTPClient: %w", err)
	}
	inst := &instance{tag: tag, transport: transport, done: make(chan struct{}), killed: make(chan struct{})}
	exec := &executor{codex: s.cfg.Codex, kind: "work", inst: inst}
	control := &executor{codex: s.cfg.Codex, kind: "control", inst: inst}
	listenCtx, cancel := context.WithCancel(context.Background())
	inst.cancel = cancel
	s.mu.Lock()
	s.cur = inst
	s.mu.Unlock()
	go func() {
		defer close(inst.done)
		err := bridge.Listen(listenCtx, teams.BridgeOptions{
			HelperVersion:           "e2e-oldcore",
			Interval:                s.cfg.Interval,
			Executor:                exec,
			ControlFallbackExecutor: control,
			TranscriptSyncInterval:  s.cfg.Interval,
			AsyncTurnShutdownGrace:  2 * time.Second,
		})
		if err != nil && errors.Is(err, context.Canceled) {
			err = nil
		}
		inst.err = err
		fmt.Fprintf(&s.out, "=== e2e: %s Listen returned: %v ===\n", tag, err)
	}()
	return nil
}

// Stop implements harness.SUT.
func (s *SUT) Stop(ctx context.Context) error {
	return s.stop(ctx, false)
}

// Crash implements harness.SUT.
func (s *SUT) Crash(ctx context.Context) error {
	return s.stop(ctx, true)
}

func (s *SUT) stop(ctx context.Context, crash bool) error {
	s.mu.Lock()
	inst := s.cur
	s.mu.Unlock()
	if inst == nil {
		return nil
	}
	if crash {
		fmt.Fprintf(&s.out, "=== e2e: crashing %s ===\n", inst.tag)
		inst.transport.Kill()
		close(inst.killed)
	} else {
		fmt.Fprintf(&s.out, "=== e2e: stopping %s ===\n", inst.tag)
	}
	inst.cancel()
	select {
	case <-inst.done:
	case <-ctx.Done():
		return fmt.Errorf("oldcore: %s did not stop: %w", inst.tag, ctx.Err())
	}
	if !crash {
		// Graceful stop: the process would exit now, so nothing of this
		// instance may reach Graph any more either.
		inst.transport.Kill()
	}
	s.mu.Lock()
	s.lastErr = inst.err
	s.mu.Unlock()
	return nil
}

// Restart implements harness.SUT.
func (s *SUT) Restart(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	return s.Start(ctx)
}

// executor adapts the harness fake Codex to teams.Executor.
type executor struct {
	codex *harness.FakeCodex
	kind  string
	inst  *instance
}

func (e *executor) Run(ctx context.Context, session *teams.Session, prompt string) (teams.ExecutionResult, error) {
	call := harness.CodexCall{Instance: e.inst.tag, Kind: e.kind, Prompt: prompt}
	if session != nil {
		call.SessionID = session.ID
		call.ChatID = session.ChatID
		call.Cwd = session.Cwd
		call.ThreadID = session.CodexThreadID
	}
	if call.ThreadID == "" {
		call.ThreadID = fakeThreadID(call.SessionID + call.ChatID + e.kind)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-e.inst.killed:
			cancel()
		case <-runCtx.Done():
		}
	}()
	res, err := e.codex.Invoke(runCtx, call)
	if err != nil {
		return teams.ExecutionResult{}, err
	}
	return teams.ExecutionResult{
		Text:          res.Answer,
		CodexThreadID: call.ThreadID,
		CodexTurnID:   fmt.Sprintf("e2e-turn-%d", res.Seq),
	}, nil
}

// fakeThreadID returns a stable UUID-shaped id.
func fakeThreadID(seed string) string {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(seed); i++ {
		h ^= uint64(seed[i])
		h *= 1099511628211
	}
	return fmt.Sprintf("019e2e00-%04x-7%03x-8%03x-%012x", (h>>48)&0xffff, (h>>36)&0xfff, (h>>24)&0xfff, h&0xffffffffffff)
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
