// Command newcore-sut runs the rebuilt core (internal/core/engine) as a
// standalone process for the E2E harness (E2E_SUT=newcore):
//
//   - internal/channel/teams against the fake Graph, through the sandbox
//     transport (delegated auth from the scenario's token caches)
//   - internal/core/store under $CODEX_HELPER_STATE_DIR/core
//   - internal/core/codexagent driving the fake Codex app-server binary
//
// Configuration comes from the same E2E_SUT_* environment as oldcore-sut.
// SIGTERM/SIGINT stop gracefully; the harness uses SIGKILL to crash. Exit
// code 75 asks the harness to start a new instance on the same state
// (`helper restart`).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/codexagent"
	"github.com/baaaaaaaka/codex-helper/internal/core/delegate"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/skillcmd"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sandbox"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut/override"
)

// Environment variables.
const (
	EnvGraphURL = "E2E_SUT_GRAPH_URL"
	EnvInstance = "E2E_SUT_INSTANCE"
	EnvInterval = "E2E_SUT_INTERVAL"
	EnvCodexBin = "E2E_SUT_CODEX_BIN"
	// EnvProductionTiming=1 keeps production timing (cost scenarios).
	EnvProductionTiming = "E2E_SUT_PRODUCTION_TIMING"
	// EnvWorkflowWebhook is the Teams Workflow webhook URL ("" = none).
	EnvWorkflowWebhook = "E2E_SUT_WORKFLOW_WEBHOOK_URL"
)

// BlockedPrefix starts the stderr line reporting a refused foreign request.
const BlockedPrefix = "E2E-BLOCKED "

// exitRestart is harness.ServiceRestartExitCode: the harness (like a
// service supervisor) starts a new instance on the same state.
const exitRestart = 75

func main() {
	err := run()
	switch {
	case errors.Is(err, engine.ErrRestart):
		fmt.Fprintln(os.Stdout, "newcore-sut: restart requested")
		os.Exit(exitRestart)
	case err != nil:
		fmt.Fprintf(os.Stderr, "newcore-sut: %v\n", err)
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

	authCfg, err := teams.ResolveAuthConfig("")
	if err != nil {
		return fmt.Errorf("auth config: %w", err)
	}
	ch, err := teams.New(teams.Config{HTTP: client, Tokens: teams.NewDelegatedAuth(authCfg, client), TenantID: authCfg.TenantID})
	if err != nil {
		return err
	}

	stateDir := os.Getenv("CODEX_HELPER_STATE_DIR")
	if stateDir == "" {
		return errors.New("CODEX_HELPER_STATE_DIR is required")
	}
	dir := filepath.Join(stateDir, "core")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	st, err := store.Open(filepath.Join(dir, "core.db"))
	if err != nil {
		return err
	}
	defer st.Close()
	host, _ := os.Hostname()
	host = override.Hostname(host)
	boot, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if _, err := st.Acquire(store.Ident{Host: host, PID: os.Getpid(), BootID: strings.TrimSpace(string(boot))}); err != nil {
		return fmt.Errorf("acquire store: %w", err)
	}
	defer st.Release()

	codexHome := os.Getenv("CODEX_HOME")
	ag := codexagent.New(codexagent.Config{
		Server: codex.Config{
			Bin:  codexBin,
			Args: []string{"--analytics-default-enabled"},
			Env: []string{
				fakecodex.EnvBrainURL + "=" + os.Getenv(fakecodex.EnvBrainURL),
				fakecodex.EnvKind + "=work",
				fakecodex.EnvInstance + "=" + instance,
				"CODEX_HOME=" + codexHome,
			},
		},
		PIDFile: filepath.Join(dir, "app-servers.pid"),
		Idle:    10 * time.Minute,
		Poll:    2 * time.Second,
		Log:     os.Stdout,
	})
	timing := engine.FastTiming(interval)
	// Fast timing polls interval/1s times faster than production; the
	// account read budget (production 4/s) scales with it.
	readRate := 4 * float64(time.Second) / float64(interval)
	if os.Getenv(EnvProductionTiming) == "1" {
		timing, readRate = engine.ProductionTiming(), 0
	}
	if err := override.Timing(&timing); err != nil {
		return err
	}
	machine := os.Getenv("CODEX_HELPER_TEAMS_MACHINE_LABEL")
	if machine == "" {
		machine = host
	}
	cfg := engine.Config{
		Store: st, Channel: ch, ChannelName: "teams", Agent: ag,
		Log: os.Stdout, Timing: timing, ReadRate: readRate, Machine: machine, Version: "e2e-newcore",
		FilesDir: filepath.Join(dir, "files"), ActivityScan: true,
		Registry: os.Getenv("E2E_SUT_MACHINE_REGISTRY") == "1",
		// Skill subscriptions live in the scenario's sandbox only.
		Skills: skillcmd.New(skillcmd.Options{
			ConfigDir: filepath.Join(dir, "skills", "config"),
			CacheDir:  filepath.Join(dir, "skills", "cache"),
			CodexDir:  codexHome,
			HomeDir:   os.Getenv("HOME"),
		}),
	}
	if codexHome != "" {
		cfg.SessionsDir = filepath.Join(codexHome, "sessions")
	}
	if cfg.Registry {
		// The worker side of cross-machine delegation and the machine
		// card, as in production (cxp teams run).
		if err := delegate.Configure(ctx, &cfg, ch, authCfg.TenantID, host, "", ""); err != nil {
			return fmt.Errorf("machine registry: %w", err)
		}
	}
	if url := os.Getenv(EnvWorkflowWebhook); url != "" {
		cfg.Notifier = teams.NewWorkflowNotifier(url, client)
	}
	e, err := engine.New(cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "=== e2e: %s listening (pid %d) ===\n", instance, os.Getpid())
	err = e.Run(ctx)
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	fmt.Fprintf(os.Stdout, "=== e2e: %s Run returned: %v ===\n", instance, err)
	return err
}
