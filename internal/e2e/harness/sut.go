package harness

import (
	"context"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// SUT is a Teams helper core under test. Adapters exist per implementation
// (sut/oldcore for the current core; a future adapter for the rebuilt core).
//
// Lifecycle semantics every adapter must document:
//   - Start begins listening and returns once the listener is launched. It
//     does not wait for any Teams-visible effect; scenarios wait on those.
//   - Stop is a graceful shutdown and waits for the listener to exit.
//   - Crash is the most abrupt stop the adapter can produce. After Crash
//     returns, the instance must have no further externally visible effect.
//   - Restart is Stop followed by Start on the same state.
//   - Service restart (contract added in M3): a SUT process that exits with
//     ServiceRestartExitCode (75) asks to be restarted on the same state, as
//     its service supervisor would do in production (the owner's `helper
//     restart`). The adapter must then start a new instance on the same
//     state without the scenario's involvement, unless the harness started,
//     stopped or crashed the SUT meanwhile; Instance() then reports the new
//     instance and the restart exit is not an ExitErr. Subprocess adapters
//     get this from ProcessSUT; an in-process adapter that supports the
//     SUT's own restart must emulate it.
type SUT interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Crash(ctx context.Context) error
	Restart(ctx context.Context) error
	Running() bool
	// Output is everything the SUT wrote to its stdout/stderr equivalent.
	Output() string
	// ExitErr is the listener's exit error once it stopped on its own.
	ExitErr() error
	// Instance is the tag of the currently (or last) running instance; the
	// fake records it on every request.
	Instance() string
}

// ServiceRestartExitCode is the exit status by which a SUT process asks its
// supervisor for a restart on the same state ("restart me"; EX_TEMPFAIL).
const ServiceRestartExitCode = 75

// SUTConfig is what the harness hands to an adapter factory.
type SUTConfig struct {
	Env      *Env
	Graph    *fakegraph.Server
	Codex    *FakeCodex
	Interval time.Duration
	// ExtraEnv is added to a subprocess SUT's environment (scenario
	// switches such as E2E_SUT_MACHINE_REGISTRY=1). In-process adapters
	// ignore it.
	ExtraEnv map[string]string
	// WorkflowWebhookURL, if set, is the owner's Teams Workflow webhook. The
	// SUT must be configured with it before it starts, the way its own CLI
	// or chat command would do it (old core: `cxp teams workflow enable`).
	WorkflowWebhookURL string
	Logf               func(format string, args ...any)
}

// SUTFactory builds a SUT for a scenario.
type SUTFactory func(cfg SUTConfig) (SUT, error)
