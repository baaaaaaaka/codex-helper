// Package codexagent implements engine.Agent over internal/core/codex: one
// app-server per running conversation from a warm pool, thread/resume once
// per server, the turn key sent as clientUserMessageId and reconciliation
// by that key.
package codexagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

// Config configures the agent.
type Config struct {
	Server  codex.Config // template for every app-server (Bin, Args, Env, Log)
	PIDFile string       // app-servers of this helper (KillStale at Prepare)
	Idle    time.Duration
	Poll    time.Duration // turn/completed safety poll
	Log     io.Writer
}

// Agent is the Codex app-server agent.
type Agent struct {
	cfg  Config
	pool *codex.Pool

	mu      sync.Mutex
	running map[int64]*runningTurn
}

type runningTurn struct {
	srv      *codex.Server
	threadID string
	turnID   string
}

var _ engine.Agent = (*Agent)(nil)

// New returns an agent; nothing starts until a turn runs.
func New(cfg Config) *Agent {
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	if cfg.Server.Log == nil {
		cfg.Server.Log = cfg.Log
	}
	return &Agent{
		cfg:     cfg,
		pool:    codex.NewPool(codex.PoolConfig{Idle: cfg.Idle, PIDFile: cfg.PIDFile, Log: cfg.Log}),
		running: map[int64]*runningTurn{},
	}
}

func key(conv int64) string { return fmt.Sprintf("conv-%d", conv) }

// Prepare kills app-servers a previous helper process left behind.
func (a *Agent) Prepare(ctx context.Context) error {
	if a.cfg.PIDFile == "" {
		return nil
	}
	n, err := codex.KillStale(a.cfg.PIDFile, 5*time.Second)
	if n > 0 {
		fmt.Fprintf(a.cfg.Log, "codexagent: killed %d app-server(s) left by the previous helper\n", n)
	}
	return err
}

func (a *Agent) serverConfig(cwd string) codex.Config {
	c := a.cfg.Server
	c.Env = append([]string(nil), c.Env...)
	if cwd != "" {
		c.Dir = cwd
	}
	return c
}

// Run executes one turn.
func (a *Agent) Run(ctx context.Context, req engine.TurnRequest, hooks engine.TurnHooks) (engine.TurnOutcome, error) {
	lease, err := a.pool.Acquire(ctx, key(req.ConvID), a.serverConfig(req.Cwd))
	if err != nil {
		if ctx.Err() != nil {
			return engine.TurnOutcome{}, ctx.Err()
		}
		return engine.TurnOutcome{NotStarted: true, Status: engine.StatusFailed, Error: "Codex could not be started: " + err.Error()}, nil
	}
	defer lease.Release()
	srv := lease.Server()
	threadID := req.ThreadID
	if threadID == "" {
		th, err := srv.ThreadStart(ctx, codex.ThreadStartParams{Cwd: req.Cwd, Model: req.Model})
		if err != nil {
			return a.notStarted(ctx, err)
		}
		lease.Loaded(th.ID)
		threadID = th.ID
		if hooks.ThreadStarted != nil {
			if err := hooks.ThreadStarted(threadID); err != nil {
				return engine.TurnOutcome{NotStarted: true, Status: engine.StatusFailed, Error: "binding the new thread failed: " + err.Error()}, nil
			}
		}
	} else if err := lease.Load(ctx, threadID); err != nil {
		return a.notStarted(ctx, err)
	}
	in := []codex.Input{}
	if strings.TrimSpace(req.Text) != "" {
		in = append(in, codex.Input{Text: req.Text})
	}
	for _, img := range req.Images {
		in = append(in, codex.Input{Image: img})
	}
	rt := &runningTurn{srv: srv, threadID: threadID}
	a.mu.Lock()
	a.running[req.ConvID] = rt
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		if a.running[req.ConvID] == rt {
			delete(a.running, req.ConvID)
		}
		a.mu.Unlock()
	}()
	res, err := srv.RunTurn(ctx, codex.TurnStartParams{
		ThreadID: threadID, Input: in, ClientID: req.TurnKey, Cwd: req.Cwd, Model: req.Model, Effort: req.Effort,
	}, a.cfg.Poll, codex.TurnHooks{
		Started: func(id string) {
			a.mu.Lock()
			rt.turnID = id
			a.mu.Unlock()
			if hooks.Started != nil {
				hooks.Started(id)
			}
		},
		Progress: hooks.Progress,
	})
	var ns *codex.ErrNotStarted
	switch {
	case errors.As(err, &ns):
		return a.notStarted(ctx, ns.Err)
	case err != nil && ctx.Err() != nil:
		return engine.TurnOutcome{CodexTurnID: res.TurnID}, ctx.Err()
	case errors.Is(err, codex.ErrExited):
		a.pool.Drop(key(req.ConvID))
		return engine.TurnOutcome{CodexTurnID: res.TurnID}, engine.ErrUnknownOutcome
	case err != nil:
		return engine.TurnOutcome{CodexTurnID: res.TurnID}, fmt.Errorf("%w: %v", engine.ErrUnknownOutcome, err)
	}
	return outcome(res.TurnID, res.Status, res.Final, res.Err, res.Usage), nil
}

func (a *Agent) notStarted(ctx context.Context, err error) (engine.TurnOutcome, error) {
	if ctx.Err() != nil {
		return engine.TurnOutcome{}, ctx.Err()
	}
	o := engine.TurnOutcome{NotStarted: true, Status: engine.StatusFailed, Error: err.Error()}
	if codex.IsActiveWriter(err) {
		o.ActiveWriter = true
	}
	if te := (&codex.TurnError{Message: err.Error()}); te.Capacity() {
		o.Capacity = true
	}
	return o, nil
}

func outcome(turnID, status, final string, terr *codex.TurnError, u *codex.Usage) engine.TurnOutcome {
	o := engine.TurnOutcome{CodexTurnID: turnID, Final: final}
	switch status {
	case codex.StatusCompleted:
		o.Status = engine.StatusCompleted
	case codex.StatusInterrupted:
		o.Status = engine.StatusInterrupted
	case codex.StatusInProgress:
		o.Status = engine.StatusInProgress
	default:
		o.Status = engine.StatusFailed
	}
	if terr != nil {
		o.Error = terr.Message
		o.Capacity = terr.Capacity()
	}
	if u != nil {
		o.Usage = engine.Usage{Input: u.Input, Cached: u.Cached, Output: u.Output, Reasoning: u.Reasoning, Total: u.Total}
	}
	return o
}

// Lookup finds the turn carrying turnKey on threadID.
func (a *Agent) Lookup(ctx context.Context, convID int64, threadID, turnKey string) (engine.TurnOutcome, bool, error) {
	if threadID == "" {
		return engine.TurnOutcome{}, false, nil
	}
	lease, err := a.pool.Acquire(ctx, key(convID), a.serverConfig(""))
	if err != nil {
		return engine.TurnOutcome{}, false, err
	}
	defer lease.Release()
	t, ok, err := lease.Server().FindTurn(ctx, threadID, turnKey)
	if err != nil || !ok {
		return engine.TurnOutcome{}, ok, err
	}
	return outcome(t.ID, t.Status, t.Final(), t.Error, nil), true, nil
}

// Interrupt asks Codex to stop the running turn of convID.
func (a *Agent) Interrupt(ctx context.Context, convID int64) error {
	a.mu.Lock()
	rt := a.running[convID]
	var turnID string
	if rt != nil {
		turnID = rt.turnID
	}
	a.mu.Unlock()
	if rt == nil {
		return nil
	}
	if turnID == "" {
		return errors.New("codexagent: the turn has not started yet")
	}
	return rt.srv.TurnInterrupt(ctx, rt.threadID, turnID)
}

// Kill ends the app-server of convID.
func (a *Agent) Kill(convID int64) { a.pool.Drop(key(convID)) }

// Fork forks threadID after lastTurnID into a new thread.
func (a *Agent) Fork(ctx context.Context, convID int64, threadID, lastTurnID, cwd string) (string, error) {
	lease, err := a.pool.Acquire(ctx, key(convID), a.serverConfig(cwd))
	if err != nil {
		return "", err
	}
	defer lease.Release()
	th, err := lease.Server().ThreadFork(ctx, threadID, lastTurnID, cwd)
	if err != nil {
		return "", err
	}
	return th.ID, nil
}

// Models lists the models Codex offers (any running or a fresh server).
func (a *Agent) Models(ctx context.Context) ([]engine.Model, error) {
	lease, err := a.pool.Acquire(ctx, "models", a.serverConfig(""))
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	ms, err := lease.Server().Models(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]engine.Model, 0, len(ms))
	for _, m := range ms {
		em := engine.Model{ID: m.ID, DisplayName: m.DisplayName, Default: m.IsDefault, Effort: m.DefaultEffort}
		for _, e := range m.Efforts {
			em.Efforts = append(em.Efforts, e.Effort)
		}
		out = append(out, em)
	}
	return out, nil
}

// Maintain reaps idle app-servers.
func (a *Agent) Maintain() { a.pool.Reap() }

// Close ends every app-server.
func (a *Agent) Close() { a.pool.Close(2 * time.Second) }
