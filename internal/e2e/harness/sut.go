package harness

import (
	"context"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// SUT is the Teams helper under test: the cxp binary run as a process
// (ProcessSUT; see package sut for how it is started).
//
// Lifecycle:
//   - Start begins listening and returns once the process is launched. It
//     does not wait for any Teams-visible effect; scenarios wait on those.
//   - Stop is a graceful shutdown (SIGTERM) and waits for the exit.
//   - Crash is SIGKILL. After Crash returns, the instance has no further
//     externally visible effect.
//   - Restart is Stop followed by Start on the same state.
//   - Service restart: a process that exits with ServiceRestartExitCode (75)
//     asks to be restarted on the same state, as its service supervisor
//     would do in production (the owner's `helper restart`). A new instance
//     is started on the same state unless the harness started, stopped or
//     crashed the SUT meanwhile; Instance() then reports the new instance
//     and the restart exit is not an ExitErr.
type SUT interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Crash(ctx context.Context) error
	Restart(ctx context.Context) error
	Running() bool
	// Output is everything the SUT wrote to its stdout and stderr.
	Output() string
	// ExitErr is the process's exit error once it stopped on its own.
	ExitErr() error
	// Instance is the tag of the currently (or last) running instance; the
	// fake records it on every request.
	Instance() string
}

// ServiceRestartExitCode is the exit status by which a SUT process asks its
// supervisor for a restart on the same state ("restart me"; EX_TEMPFAIL).
const ServiceRestartExitCode = 75

// SUTConfig is what the harness hands to the SUT factory.
type SUTConfig struct {
	Env      *Env
	Graph    *fakegraph.Server
	Codex    *FakeCodex
	Interval time.Duration
	// Seam is the scenario's test-seam setting (timing, host name).
	Seam Seam
	// Args are extra `cxp teams run` flags (e.g. --machine-registry).
	Args []string
	// UserEnv are extra environment variables a user could set (e.g.
	// CODEX_HELPER_TEAMS_ASR_COMMAND, PATH).
	UserEnv map[string]string
	// Peer, when set, names a second helper on the same state (another
	// host sharing the home): its instances are tagged "cxp-<Peer>-<n>".
	Peer string
	Logf func(format string, args ...any)
}

// SUTFactory builds a SUT for a scenario.
type SUTFactory func(cfg SUTConfig) (SUT, error)
