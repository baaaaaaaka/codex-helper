package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
	"github.com/baaaaaaaka/codex-helper/internal/proxysupervisor"
)

const (
	proxySupervisorRestartDelay  = 5 * time.Second
	proxySupervisorRestartWindow = time.Minute
	proxySupervisorRestartBurst  = 3
)

// ErrProxyRecoveryBlocked is a durable circuit-breaker result. A native
// supervisor must exit successfully for this state so systemd/LaunchAgent/
// Task Scheduler do not turn a deliberate block into an external restart loop.
var ErrProxyRecoveryBlocked = errors.New("proxy recovery is durably blocked")

var (
	proxySupervisorCommand = exec.Command
	proxySupervisorNow     = time.Now
	proxySupervisorWait    = waitProxySupervisor
)

func newProxySupervisorCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "supervisor",
		Short: "Run or render the bounded proxy supervisor",
	}
	cmd.AddCommand(newProxySupervisorRunCmd(root), newProxySupervisorRenderCmd(root))
	return cmd
}

func newProxySupervisorRunCmd(root *rootOptions) *cobra.Command {
	var instanceID, ownerToken string
	cmd := &cobra.Command{
		Use:    "run --instance-id <id> --owner-token <token>",
		Short:  "Run the bounded proxy child supervisor (internal)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			return runProxySupervisor(cmd.Context(), store, instanceID, ownerToken)
		},
	}
	cmd.Flags().StringVar(&instanceID, "instance-id", "", "Instance id")
	cmd.Flags().StringVar(&ownerToken, "owner-token", "", "Proxy owner lease token")
	_ = cmd.MarkFlagRequired("instance-id")
	_ = cmd.MarkFlagRequired("owner-token")
	return cmd
}

func newProxySupervisorRenderCmd(root *rootOptions) *cobra.Command {
	var instanceID, platform, output string
	cmd := &cobra.Command{
		Use:   "render [instance-id]",
		Short: "Render a native proxy supervisor definition without installing it",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			if instanceID == "" && len(args) > 0 {
				instanceID = args[0]
			}
			cfg, err := store.Load()
			if err != nil {
				return err
			}
			inst, err := proxyInstanceFromConfig(cfg, instanceID)
			if err != nil {
				return err
			}
			exe, err := proxySupervisorExecutable()
			if err != nil {
				return err
			}
			name, data, err := proxysupervisor.Render(proxysupervisor.Spec{
				Platform:      proxysupervisor.Platform(platform),
				Executable:    exe,
				ConfigPath:    store.Path(),
				InstanceID:    inst.ID,
				OwnerToken:    inst.OwnerToken,
				WorkingDir:    filepath.Dir(store.Path()),
				RestartDelay:  proxySupervisorRestartDelay,
				RestartWindow: proxySupervisorRestartWindow,
				RestartBurst:  proxySupervisorRestartBurst,
			})
			if err != nil {
				return err
			}
			path := output
			if strings.TrimSpace(path) == "" {
				path = filepath.Join(filepath.Dir(store.Path()), "proxy-supervisors", name)
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return fmt.Errorf("create proxy supervisor directory: %w", err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				return fmt.Errorf("write proxy supervisor definition: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Rendered %s supervisor: %s\n", platform, path)
			return nil
		},
	}
	cmd.Flags().StringVar(&instanceID, "instance-id", "", "Instance id")
	cmd.Flags().StringVar(&platform, "platform", runtime.GOOS, "Target platform: linux, darwin, or windows")
	cmd.Flags().StringVar(&output, "output", "", "Output file path")
	return cmd
}

func proxyInstanceFromConfig(cfg config.Config, instanceID string) (config.Instance, error) {
	if strings.TrimSpace(instanceID) == "" {
		if len(cfg.Instances) == 1 {
			return cfg.Instances[0], nil
		}
		return config.Instance{}, errors.New("proxy supervisor instance id is required when multiple instances exist")
	}
	for _, inst := range cfg.Instances {
		if inst.ID == instanceID {
			if strings.TrimSpace(inst.OwnerToken) == "" {
				return config.Instance{}, fmt.Errorf("proxy instance %q has no owner token; start it with the current helper first", instanceID)
			}
			return inst, nil
		}
	}
	return config.Instance{}, fmt.Errorf("proxy instance %q not found", instanceID)
}

func proxySupervisorExecutable() (string, error) {
	exe, err := proxyExecutable()
	if err != nil {
		return "", err
	}
	resolved, err := helperpath.StableRunnablePathFromSources(exe, restartArgv0(), helperpath.Options{})
	if err != nil {
		return "", err
	}
	return resolved.Path, nil
}

func runProxySupervisor(ctx context.Context, store *config.Store, instanceID, ownerToken string) error {
	if store == nil {
		return errors.New("proxy supervisor: nil config store")
	}
	if strings.TrimSpace(instanceID) == "" || strings.TrimSpace(ownerToken) == "" {
		return errors.New("proxy supervisor requires instance id and owner token")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// A native supervisor terminates the helper process directly. Convert that
	// signal into the same cancellation path used by an explicit context so the
	// managed daemon is stopped before the supervisor exits.
	ctx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	exe, err := proxySupervisorExecutable()
	if err != nil {
		return err
	}
	if err := proxySupervisorOwnerCurrent(store, instanceID, ownerToken); err != nil {
		if errors.Is(err, ErrProxyRecoveryBlocked) {
			return nil
		}
		return err
	}
	logPath := filepath.Join(filepath.Dir(store.Path()), "instances", instanceID+"-supervisor.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return fmt.Errorf("create proxy supervisor log directory: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open proxy supervisor log: %w", err)
	}
	defer logFile.Close()

	attempts := make([]time.Time, 0, proxySupervisorRestartBurst)
	for {
		if ctx.Err() != nil {
			return nil
		}
		now := proxySupervisorNow()
		kept := attempts[:0]
		for _, at := range attempts {
			if now.Sub(at) <= proxySupervisorRestartWindow {
				kept = append(kept, at)
			}
		}
		attempts = kept
		if len(attempts) >= proxySupervisorRestartBurst {
			// The durable record is normally blocked by the third child exit
			// below. Keep this guard fail-closed for an old record or a legacy
			// child path that reached the local limit without persisting first.
			if err := markProxySupervisorBlocked(store, instanceID, ownerToken, now, fmt.Sprintf("proxy supervisor restart budget exceeded for instance %s", instanceID)); err != nil {
				return err
			}
			return nil
		}
		if err := proxySupervisorOwnerCurrent(store, instanceID, ownerToken); err != nil {
			if errors.Is(err, ErrProxyRecoveryBlocked) {
				return nil
			}
			return err
		}
		// A supervisor may be restarted while its managed daemon is still
		// healthy. Adopt that daemon by waiting for it to exit instead of
		// starting a second listener/tunnel generation. Ignore the supervisor's
		// own PID, which is recorded by detached `proxy start` before this
		// process gets to run.
		if err := waitForExistingProxyDaemon(ctx, store, instanceID, ownerToken); err != nil {
			return err
		}

		child := proxySupervisorCommand(exe, (&proxysupervisor.Spec{
			ConfigPath: store.Path(), InstanceID: instanceID, OwnerToken: ownerToken,
		}).ChildArgs()...)
		child.Stdout = logFile
		child.Stderr = logFile
		if err := child.Start(); err != nil {
			attempts = append(attempts, now)
			blocked, recordErr := recordProxySupervisorAttempt(store, instanceID, ownerToken, now, err)
			if recordErr != nil {
				return recordErr
			}
			if blocked {
				return nil
			}
			if err := proxySupervisorWait(ctx, proxySupervisorRestartDelay); err != nil {
				return err
			}
			continue
		}
		waitCh := make(chan error, 1)
		go func() { waitCh <- child.Wait() }()
		select {
		case <-ctx.Done():
			if child.Process != nil {
				_ = proxyTerminate(child.Process, 2*time.Second)
			}
			<-waitCh
			return nil
		case childErr := <-waitCh:
			if err := proxySupervisorOwnerCurrent(store, instanceID, ownerToken); err != nil {
				if errors.Is(err, ErrProxyRecoveryBlocked) {
					return nil
				}
				return err
			}
			attempts = append(attempts, now)
			blocked, recordErr := recordProxySupervisorAttempt(store, instanceID, ownerToken, now, childErr)
			if recordErr != nil {
				return recordErr
			}
			if blocked {
				return nil
			}
			if childErr != nil {
				_, _ = fmt.Fprintf(logFile, "proxy daemon exited: %v\n", childErr)
			}
			if err := proxySupervisorWait(ctx, proxySupervisorRestartDelay); err != nil {
				return err
			}
		}
	}
}

func waitForExistingProxyDaemon(ctx context.Context, store *config.Store, instanceID, ownerToken string) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := proxySupervisorOwnerCurrent(store, instanceID, ownerToken); err != nil {
			return err
		}
		cfg, err := store.Load()
		if err != nil {
			return err
		}
		alive := false
		for _, inst := range cfg.Instances {
			if inst.ID == instanceID && inst.DaemonPID > 0 && inst.DaemonPID != os.Getpid() {
				alive = proxyProcessAlive(inst.DaemonPID)
				if alive {
					// A persisted PID can be reused after a crash. Do not adopt an
					// unrelated process merely because kill(pid, 0) succeeds.
					looksLike, lookErr := proxyLooksLikeProxyDaemon(inst.DaemonPID)
					alive = lookErr == nil && looksLike
				}
				break
			}
		}
		if !alive {
			return nil
		}
		if err := proxySupervisorWait(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
}

func proxySupervisorOwnerCurrent(store *config.Store, instanceID, ownerToken string) error {
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	for _, inst := range cfg.Instances {
		if inst.ID != instanceID {
			continue
		}
		if inst.OwnerToken != ownerToken {
			return manager.ErrInstanceOwnershipLost
		}
		if inst.RecoveryBudget.Blocked {
			return ErrProxyRecoveryBlocked
		}
		return nil
	}
	return fmt.Errorf("proxy instance %q no longer exists", instanceID)
}

func recordProxySupervisorAttempt(store *config.Store, instanceID, ownerToken string, now time.Time, cause error) (bool, error) {
	if store == nil {
		return false, errors.New("proxy supervisor: nil config store")
	}
	blocked := false
	err := store.Update(func(cfg *config.Config) error {
		for i := range cfg.Instances {
			inst := &cfg.Instances[i]
			if inst.ID != instanceID {
				continue
			}
			if inst.OwnerToken != ownerToken {
				return manager.ErrInstanceOwnershipLost
			}
			budget := inst.RecoveryBudget
			if budget.RestartWindowStartedAt.IsZero() || now.Before(budget.RestartWindowStartedAt) || now.Sub(budget.RestartWindowStartedAt) > proxySupervisorRestartWindow {
				budget.RestartWindowStartedAt = now
				budget.RestartAttempts = 0
			}
			budget.RestartAttempts++
			if budget.RestartAttempts >= proxySupervisorRestartBurst {
				budget.Blocked = true
				budget.BlockedAt = now
				budget.LastReason = fmt.Sprintf("proxy supervisor restart budget exceeded: cause=%v", cause)
				blocked = true
			}
			inst.RecoveryBudget = budget
			return nil
		}
		return fmt.Errorf("proxy instance %q no longer exists", instanceID)
	})
	return blocked, err
}

func markProxySupervisorBlocked(store *config.Store, instanceID, ownerToken string, now time.Time, reason string) error {
	return store.Update(func(cfg *config.Config) error {
		for i := range cfg.Instances {
			inst := &cfg.Instances[i]
			if inst.ID != instanceID {
				continue
			}
			if inst.OwnerToken != ownerToken {
				return manager.ErrInstanceOwnershipLost
			}
			inst.RecoveryBudget.Blocked = true
			inst.RecoveryBudget.BlockedAt = now
			inst.RecoveryBudget.LastReason = reason
			return nil
		}
		return fmt.Errorf("proxy instance %q no longer exists", instanceID)
	})
}

func waitProxySupervisor(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil
	case <-timer.C:
		return nil
	}
}
