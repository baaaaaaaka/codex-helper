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

// SUTConfig is what the harness hands to an adapter factory.
type SUTConfig struct {
	Env      *Env
	Graph    *fakegraph.Server
	Codex    *FakeCodex
	Interval time.Duration
	Logf     func(format string, args ...any)
}

// SUTFactory builds a SUT for a scenario.
type SUTFactory func(cfg SUTConfig) (SUT, error)
