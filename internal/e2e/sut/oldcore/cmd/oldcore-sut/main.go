// Command oldcore-sut runs the current Teams core (internal/teams) as a
// standalone process for the E2E harness. It is the process-level
// equivalent of `cxp teams run`, built only from the exported API:
//
//   - teams.DefaultEffectiveAuthConfig / NewAuthManagerWithHTTPClient
//   - teams.NewBridgeWithHTTPClient and (*teams.Bridge).Listen
//   - codexrunner.AppServerRunner + AppServerProcessStarter, i.e. the real
//     app-server client, pointed at the fake Codex binary
//   - optionally teams.NewMachineRegistryGraphAdapter (machine registry and
//     delegation worker, on by default in `cxp teams run`)
//
// Differences from `cxp teams run` (documented in the M2 report): no service
// manager, auto-update, model-profile manager, ASR or Codex upgrader; the
// executor below mirrors cli.teamsCodexExecutor (resume an existing thread,
// start a new one otherwise) without model profiles.
//
// Configuration comes from the environment (see the E2E_SUT_* constants);
// all Graph and login traffic goes through the sandbox transport to the fake
// Graph. SIGTERM/SIGINT stop gracefully; the harness uses SIGKILL to crash.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sandbox"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

// Environment variables.
const (
	EnvGraphURL        = "E2E_SUT_GRAPH_URL"
	EnvInstance        = "E2E_SUT_INSTANCE"
	EnvInterval        = "E2E_SUT_INTERVAL"
	EnvCodexBin        = "E2E_SUT_CODEX_BIN"
	EnvMachineRegistry = "E2E_SUT_MACHINE_REGISTRY"
)

// BlockedPrefix starts the stderr line reporting a refused foreign request.
const BlockedPrefix = "E2E-BLOCKED "

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "oldcore-sut: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	graphURL := os.Getenv(EnvGraphURL)
	instance := os.Getenv(EnvInstance)
	codexBin := os.Getenv(EnvCodexBin)
	if graphURL == "" || instance == "" || codexBin == "" {
		return fmt.Errorf("%s, %s and %s are required", EnvGraphURL, EnvInstance, EnvCodexBin)
	}
	interval := 250 * time.Millisecond
	if v := os.Getenv(EnvInterval); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvInterval, err)
		}
		interval = d
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var blockedMu sync.Mutex
	transport, err := sandbox.New(graphURL, instance, func(b fakegraph.BlockedRequest) {
		blockedMu.Lock()
		defer blockedMu.Unlock()
		fmt.Fprintf(os.Stderr, "%s%s\n", BlockedPrefix, b.URL)
	})
	if err != nil {
		return err
	}
	client := transport.Client()

	authCfg, err := teams.DefaultEffectiveAuthConfig()
	if err != nil {
		return fmt.Errorf("auth config: %w", err)
	}
	auth := teams.NewAuthManagerWithHTTPClient(authCfg, client)
	bridge, err := teams.NewBridgeWithHTTPClient(ctx, auth, "", os.Stdout, client)
	if err != nil {
		return fmt.Errorf("NewBridgeWithHTTPClient: %w", err)
	}

	codexHome := os.Getenv("CODEX_HOME")
	work := newExecutor(codexBin, codexHome, "work", instance)
	defer work.Close()
	control := newExecutor(codexBin, codexHome, "control", instance)
	defer control.Close()

	opts := teams.BridgeOptions{
		HelperVersion:           "e2e-oldcore",
		Interval:                interval,
		Executor:                work,
		ControlFallbackExecutor: control,
		TranscriptSyncInterval:  interval,
		AsyncTurnShutdownGrace:  2 * time.Second,
	}
	if os.Getenv(EnvMachineRegistry) == "1" {
		registryAuth := teams.NewAuthManagerWithHTTPClient(authCfg, client)
		graph := teams.NewGraphClientWithHTTPClient(registryAuth, io.Discard, client)
		opts.MachineRegistryEnabled = true
		opts.MachineRegistryGraph = teams.NewMachineRegistryGraphAdapter(graph)
		opts.MachineDelegationClaimRecheckDelay = teams.DefaultMachineDelegationClaimRecheckDelay
	}
	fmt.Fprintf(os.Stdout, "=== e2e: %s listening (pid %d) ===\n", instance, os.Getpid())
	err = bridge.Listen(ctx, opts)
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	fmt.Fprintf(os.Stdout, "=== e2e: %s Listen returned: %v ===\n", instance, err)
	return err
}

// executor mirrors cli.teamsCodexExecutor for the default model profile: one
// app-server runner, ResumeThread when the session already has a Codex
// thread, StartThread otherwise, and the same result/error classification.
type executor struct {
	runner *codexrunner.AppServerRunner
	mu     sync.Mutex
	used   bool
}

func newExecutor(codexBin, codexHome, kind, instance string) *executor {
	return &executor{runner: &codexrunner.AppServerRunner{
		Starter:       codexrunner.AppServerProcessStarter{},
		ApprovalMode:  codexrunner.ApprovalModeAutomatic,
		Command:       codexBin,
		AppServerArgs: []string{"--analytics-default-enabled"},
		ExtraEnv: []string{
			fakecodex.EnvBrainURL + "=" + os.Getenv(fakecodex.EnvBrainURL),
			fakecodex.EnvKind + "=" + kind,
			fakecodex.EnvInstance + "=" + instance,
			"CODEX_HOME=" + codexHome,
		},
		CodexHome:            codexHome,
		MetadataOnlyResume:   true,
		RequireCompleteFinal: true,
	}}
}

func (e *executor) Close() error { return e.runner.Close() }

func (e *executor) Run(ctx context.Context, session *teams.Session, prompt string) (teams.ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, teams.ExecutionInput{Prompt: prompt}, nil)
}

func (e *executor) RunWithEventHandler(ctx context.Context, session *teams.Session, prompt string, handler codexrunner.EventHandler) (teams.ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, teams.ExecutionInput{Prompt: prompt}, handler)
}

func (e *executor) RunInput(ctx context.Context, session *teams.Session, input teams.ExecutionInput) (teams.ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, input, nil)
}

func (e *executor) ReconcileExecutionFence(ctx context.Context, session *teams.Session) (bool, error) {
	if session == nil || strings.TrimSpace(session.CodexThreadID) == "" {
		return false, nil
	}
	// Like production: only a runner that was already used can hold an
	// in-memory fence; a fresh process has none to reconcile.
	e.mu.Lock()
	used := e.used
	e.mu.Unlock()
	if !used {
		return false, nil
	}
	return e.runner.ReconcileTurnCancelFence(ctx, strings.TrimSpace(session.CodexThreadID))
}

func (e *executor) ForkThread(ctx context.Context, session *teams.Session, cutoffCodexTurnID string) (teams.ForkResult, error) {
	if session == nil || strings.TrimSpace(session.CodexThreadID) == "" {
		return teams.ForkResult{}, fmt.Errorf("Codex parent thread is required for fork")
	}
	thread, err := e.runner.ForkThread(ctx, codexrunner.ThreadForkParams{
		ThreadID:   strings.TrimSpace(session.CodexThreadID),
		LastTurnID: strings.TrimSpace(cutoffCodexTurnID),
		WorkingDir: strings.TrimSpace(session.Cwd),
	})
	if err != nil {
		return teams.ForkResult{}, err
	}
	return teams.ForkResult{CodexThreadID: thread.ID, CodexThreadTitle: thread.Name}, nil
}

func (e *executor) RunInputWithEventHandler(ctx context.Context, session *teams.Session, input teams.ExecutionInput, handler codexrunner.EventHandler) (teams.ExecutionResult, error) {
	e.mu.Lock()
	e.used = true
	e.mu.Unlock()
	workDir := ""
	if session != nil {
		workDir = strings.TrimSpace(session.Cwd)
	}
	turnInput := codexrunner.TurnInput{
		Prompt:          input.Prompt,
		ImagePaths:      append([]string{}, input.ImagePaths...),
		WorkingDir:      workDir,
		EventHandler:    handler,
		BeforeFirstTurn: input.BeforeFirstTurn,
	}
	if session != nil {
		turnInput.ReasoningEffort = strings.TrimSpace(session.ReasoningEffort)
		if teams.SessionAllowsAutoTitleUpdate(*session) {
			turnInput.BackfillThreadName = true
		}
	}
	expected := ""
	if session != nil {
		expected = strings.TrimSpace(session.CodexThreadID)
	}
	var result codexrunner.TurnResult
	var err error
	if expected != "" {
		result, err = e.runner.ResumeThread(ctx, expected, turnInput)
	} else {
		result, err = e.runner.StartThread(ctx, turnInput)
	}
	out := teams.ExecutionResult{
		Text:             strings.TrimSpace(result.FinalAgentMessage),
		CodexThreadID:    result.ThreadID,
		CodexThreadTitle: strings.TrimSpace(result.ThreadName),
		CodexTurnID:      result.TurnID,
	}
	if err != nil {
		completedDespiteCancel := (errors.Is(err, context.Canceled) || codexrunner.IsKind(err, codexrunner.ErrorCanceled)) &&
			result.Status == codexrunner.TurnStatusCompleted && result.FinalAgentMessageComplete && result.Failure == nil
		if completedDespiteCancel {
			if out.Text == "" {
				return out, missingFinal(result, err)
			}
			if expected != "" && out.CodexThreadID != "" && out.CodexThreadID != expected {
				return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, expected)
			}
			return out, nil
		}
		if result.Status == codexrunner.TurnStatusCompleted && result.Failure == nil && !result.FinalAgentMessageComplete {
			return out, missingFinal(result, err)
		}
		if mayStillBeRunning(result) {
			return out, &teams.AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: err}
		}
		return out, err
	}
	if out.Text == "" || (result.Status == codexrunner.TurnStatusCompleted && !result.FinalAgentMessageComplete) {
		return out, missingFinal(result, nil)
	}
	if expected != "" && out.CodexThreadID != "" && out.CodexThreadID != expected {
		return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, expected)
	}
	return out, nil
}

func mayStillBeRunning(result codexrunner.TurnResult) bool {
	switch result.Status {
	case codexrunner.TurnStatusStarted, codexrunner.TurnStatusInProgress:
		return true
	case codexrunner.TurnStatusUnknown:
		return strings.TrimSpace(result.TurnID) != ""
	default:
		return false
	}
}

func missingFinal(result codexrunner.TurnResult, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("Codex turn completed without a final agent message")
	} else {
		cause = fmt.Errorf("Codex turn completed without a final agent message: %w", cause)
	}
	if strings.TrimSpace(result.TurnID) != "" {
		return &teams.AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: cause}
	}
	return cause
}
