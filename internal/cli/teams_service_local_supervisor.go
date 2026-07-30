package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/gofrs/flock"
	"github.com/spf13/cobra"
)

const (
	teamsServiceLocalSupervisorID                = "local-supervisor"
	teamsServiceLocalSupervisorConfigVersion     = 1
	teamsServiceLocalSupervisorStatusVersion     = 1
	teamsServiceLocalSupervisorActivationVersion = 1
	teamsServiceLocalSupervisorStatusFreshness   = 30 * time.Second
	teamsServiceLocalSupervisorRetireTimeout     = 3 * time.Second
	teamsServiceLocalSupervisorMaxLogBytes       = 5 * 1024 * 1024
	teamsServiceLocalSupervisorLogBackups        = 3
)

type teamsServiceLocalSupervisorBackend struct{}

type teamsServiceLocalSupervisorConfig struct {
	Version   int              `json:"version"`
	Enabled   bool             `json:"enabled"`
	Spec      teamsServiceSpec `json:"spec"`
	UpdatedAt time.Time        `json:"updated_at,omitempty"`
}

type teamsServiceLocalSupervisorProcessIdentity struct {
	Executable    string            `json:"executable,omitempty"`
	Args          []string          `json:"args,omitempty"`
	Environment   map[string]string `json:"environment,omitempty"`
	ProcStartTime string            `json:"proc_start_time,omitempty"`
}

type teamsServiceLocalSupervisorStatus struct {
	Version            int                                         `json:"version"`
	ConfigPath         string                                      `json:"config_path,omitempty"`
	LogPath            string                                      `json:"log_path,omitempty"`
	SupervisorPID      int                                         `json:"supervisor_pid,omitempty"`
	SupervisorPGID     int                                         `json:"supervisor_pgid,omitempty"`
	SupervisorIdentity *teamsServiceLocalSupervisorProcessIdentity `json:"supervisor_identity,omitempty"`
	ChildPID           int                                         `json:"child_pid,omitempty"`
	ChildPGID          int                                         `json:"child_pgid,omitempty"`
	ChildIdentity      *teamsServiceLocalSupervisorProcessIdentity `json:"child_identity,omitempty"`
	State              string                                      `json:"state,omitempty"`
	Enabled            bool                                        `json:"enabled,omitempty"`
	StartedAt          time.Time                                   `json:"started_at,omitempty"`
	UpdatedAt          time.Time                                   `json:"updated_at,omitempty"`
	LastChildStartAt   time.Time                                   `json:"last_child_start_at,omitempty"`
	LastChildExitAt    time.Time                                   `json:"last_child_exit_at,omitempty"`
	LastExitCode       int                                         `json:"last_exit_code,omitempty"`
	LastError          string                                      `json:"last_error,omitempty"`
	LastHealthCheckAt  time.Time                                   `json:"last_health_check_at,omitempty"`
	LastHealthReason   string                                      `json:"last_health_reason,omitempty"`
	LastHealthAction   string                                      `json:"last_health_action,omitempty"`
	RestartCount       int                                         `json:"restart_count,omitempty"`
}

type teamsServiceLocalSupervisorActivation struct {
	Version               int       `json:"version"`
	Status                string    `json:"status,omitempty"`
	TargetVersion         string    `json:"target_version,omitempty"`
	ObservedSupervisorEnv string    `json:"observed_supervisor_env,omitempty"`
	OldSupervisorPID      int       `json:"old_supervisor_pid,omitempty"`
	OldChildPID           int       `json:"old_child_pid,omitempty"`
	Message               string    `json:"message,omitempty"`
	ScheduledAt           time.Time `json:"scheduled_at,omitempty"`
	DeadlineAt            time.Time `json:"deadline_at,omitempty"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
}

var (
	teamsServiceLocalSupervisorStartDetached    = defaultTeamsServiceLocalSupervisorStartDetached
	teamsServiceLocalSupervisorRestartDelay     = time.Duration(teamsServiceTaskRestartInterval) * time.Second
	teamsServiceLocalSupervisorHeartbeatEvery   = 5 * time.Second
	teamsServiceLocalSupervisorTerminationWait  = 5 * time.Second
	teamsServiceLocalSupervisorReadyTimeout     = 5 * time.Second
	teamsServiceLocalSupervisorReleaseWait      = 2 * time.Second
	teamsServiceLocalSupervisorReleaseProcess   = func(process *os.Process) error { return process.Release() }
	teamsServiceLocalSupervisorCheckChildHealth = defaultTeamsServiceLocalSupervisorCheckChildHealth
	teamsServiceLocalSupervisorTerminateTarget  = terminateTargetCommand
	teamsLocalSupervisorVerifyProcessIdentity   = defaultTeamsLocalSupervisorVerifyProcessIdentity
	teamsLocalSupervisorVerifyChildIdentity     = defaultTeamsLocalSupervisorVerifyChildIdentity
	teamsLocalSupervisorProcessStartTime        = defaultTeamsLocalSupervisorProcessStartTime
	teamsLocalSupervisorProcessArgs             = defaultTeamsLocalSupervisorProcessArgs
	teamsLocalSupervisorProcessEnvironment      = defaultTeamsLocalSupervisorProcessEnvironment
)

var errTeamsServiceLocalSupervisorStatusMalformed = errors.New("local supervisor status is malformed")

func newTeamsServiceLocalSupervisorCmd() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:    "local-supervisor",
		Short:  "Run the local Teams service supervisor",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTeamsServiceLocalSupervisor(cmd.Context(), configPath)
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "Local supervisor config path")
	return cmd
}

func (teamsServiceLocalSupervisorBackend) ID() string {
	return teamsServiceLocalSupervisorID
}

func (teamsServiceLocalSupervisorBackend) Name() string {
	return "Codex Helper Teams local supervisor"
}

func (teamsServiceLocalSupervisorBackend) Path() (string, error) {
	return teamsServiceLocalSupervisorConfigPath()
}

func (b teamsServiceLocalSupervisorBackend) Install(ctx context.Context, spec teamsServiceSpec) (string, error) {
	path, err := b.Path()
	if err != nil {
		return "", err
	}
	cfg := teamsServiceLocalSupervisorConfig{
		Version:   teamsServiceLocalSupervisorConfigVersion,
		Enabled:   false,
		Spec:      spec,
		UpdatedAt: time.Now(),
	}
	if err := writeTeamsServiceLocalSupervisorConfig(path, cfg); err != nil {
		return "", err
	}
	return path, nil
}

func (b teamsServiceLocalSupervisorBackend) Repair(ctx context.Context, spec teamsServiceSpec, opts teamsServiceRepairOptions) (string, error) {
	path, err := b.Path()
	if err != nil {
		return "", err
	}
	enabled := false
	if oldCfg, readErr := readTeamsServiceLocalSupervisorConfig(path); readErr == nil {
		enabled = oldCfg.Enabled
	}
	if opts.Enable {
		enabled = true
	}
	active := false
	if opts.Start {
		active, err = b.Active(ctx)
		if err != nil {
			return "", err
		}
		if active {
			if _, err := b.stop(ctx); err != nil {
				return "", err
			}
		}
	}
	cfg := teamsServiceLocalSupervisorConfig{
		Version:   teamsServiceLocalSupervisorConfigVersion,
		Enabled:   enabled,
		Spec:      spec,
		UpdatedAt: time.Now(),
	}
	if err := writeTeamsServiceLocalSupervisorConfig(path, cfg); err != nil {
		return "", err
	}
	if opts.Enable {
		if _, err := b.Run(ctx, "enable"); err != nil {
			return "", err
		}
	}
	if opts.Start {
		if _, err := b.Run(ctx, "start"); err != nil {
			return "", err
		}
	}
	return path, nil
}

func (b teamsServiceLocalSupervisorBackend) Uninstall(ctx context.Context) (string, error) {
	path, err := b.Path()
	if err != nil {
		return "", err
	}
	if _, err := b.Run(ctx, "stop"); err != nil {
		return "", err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	statusPath, _ := teamsServiceLocalSupervisorStatusPath()
	if strings.TrimSpace(statusPath) != "" {
		_ = os.Remove(statusPath)
	}
	return path, nil
}

func (b teamsServiceLocalSupervisorBackend) Run(ctx context.Context, action string) ([]byte, error) {
	path, err := b.Path()
	if err != nil {
		return nil, err
	}
	switch action {
	case "enable":
		cfg, err := readTeamsServiceLocalSupervisorConfig(path)
		if err != nil {
			return nil, err
		}
		cfg.Enabled = true
		cfg.UpdatedAt = time.Now()
		if err := writeTeamsServiceLocalSupervisorConfig(path, cfg); err != nil {
			return nil, err
		}
		return []byte("Local supervisor configured. This fallback does not provide machine/container reboot autostart.\n"), nil
	case "disable":
		cfg, err := readTeamsServiceLocalSupervisorConfig(path)
		if err != nil {
			return nil, err
		}
		cfg.Enabled = false
		cfg.UpdatedAt = time.Now()
		if err := writeTeamsServiceLocalSupervisorConfig(path, cfg); err != nil {
			return nil, err
		}
		return []byte("Local supervisor disabled. Running processes are not stopped by disable; use stop for that.\n"), nil
	case "start":
		return b.start(ctx, path)
	case "restart":
		out, stopErr := b.Run(ctx, "stop")
		if stopErr != nil {
			return out, stopErr
		}
		startOut, startErr := b.start(ctx, path)
		return appendLaunchctlOutput(out, startOut), startErr
	case "stop":
		return b.stop(ctx)
	case "status":
		return b.status(ctx)
	default:
		return nil, fmt.Errorf("unsupported Teams service action for local supervisor: %s", action)
	}
}

func (b teamsServiceLocalSupervisorBackend) Installed() (bool, error) {
	path, err := b.Path()
	if err != nil {
		return false, err
	}
	return teamsServiceFileExists(path)
}

func (b teamsServiceLocalSupervisorBackend) Active(_ context.Context) (bool, error) {
	status, ok, err := readTeamsServiceLocalSupervisorStatus()
	if err != nil || !ok {
		return false, err
	}
	return teamsServiceLocalSupervisorStatusActive(status, time.Now()), nil
}

func (b teamsServiceLocalSupervisorBackend) start(ctx context.Context, configPath string) ([]byte, error) {
	if active, err := b.Active(ctx); err != nil {
		return nil, err
	} else if active {
		status, _, _ := readTeamsServiceLocalSupervisorStatus()
		return []byte(fmt.Sprintf("Local supervisor already running: pid=%d\n", status.SupervisorPID)), nil
	}
	if held, err := teamsServiceLocalSupervisorLockHeld(); err != nil {
		return nil, err
	} else if held {
		if waitOut, waitErr := waitTeamsServiceLocalSupervisorLockOwnerReady(ctx, teamsServiceLocalSupervisorReadyTimeout); waitErr != nil {
			return waitOut, waitErr
		} else if len(waitOut) > 0 {
			return waitOut, nil
		}
		return nil, fmt.Errorf("local supervisor lock is held but no verified supervisor status became ready")
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(configPath)
	if err != nil {
		return nil, err
	}
	if err := ensureTeamsServiceLocalSupervisorDir(filepath.Dir(configPath)); err != nil {
		return nil, err
	}
	logPath, err := teamsServiceLocalSupervisorLogPath()
	if err != nil {
		return nil, err
	}
	logFile, err := openTeamsServiceLocalSupervisorLog(logPath)
	if err != nil {
		return nil, err
	}
	if err := logFile.Close(); err != nil {
		return nil, err
	}
	if _, err := teamsServiceRetireLocalBridgeProcesses(ctx, cfg.Spec); err != nil {
		return nil, fmt.Errorf("could not stop old local Teams helper process(es) before local-supervisor start: %w", err)
	}
	if err := retireTeamsServiceConflictingBackendsForLocalSupervisor(ctx, cfg.Spec); err != nil {
		return nil, err
	}
	pid, err := teamsServiceLocalSupervisorStartDetached(ctx, configPath, logPath, cfg.Spec)
	if err != nil {
		return nil, err
	}
	if err := waitTeamsServiceLocalSupervisorReady(ctx, pid, configPath, teamsServiceLocalSupervisorReadyTimeout); err != nil {
		_ = terminateStartedTeamsServiceLocalSupervisor(pid, configPath)
		return nil, err
	}
	return []byte(fmt.Sprintf("Started local supervisor: pid=%d\nLog: %s\n", pid, logPath)), nil
}

func (b teamsServiceLocalSupervisorBackend) stop(ctx context.Context) ([]byte, error) {
	status, ok, err := readTeamsServiceLocalSupervisorStatus()
	if err != nil {
		if errors.Is(err, errTeamsServiceLocalSupervisorStatusMalformed) {
			if held, heldErr := teamsServiceLocalSupervisorLockHeld(); heldErr != nil {
				return nil, errors.Join(err, heldErr)
			} else if !held {
				return []byte("Local supervisor is not running; ignoring malformed status because the lock is not held.\n"), nil
			}
		}
		return nil, err
	}
	if !ok || status.SupervisorPID <= 0 {
		return []byte("Local supervisor is not running.\n"), nil
	}
	if !teamsLocalSupervisorProcessAlive(status.SupervisorPID) {
		if err := stopTeamsServiceLocalSupervisorChildFromStatus(ctx, status, true); err != nil {
			return nil, err
		}
		status.State = "stopped"
		status.SupervisorPID = 0
		status.SupervisorPGID = 0
		status.ChildPID = 0
		status.ChildPGID = 0
		status.UpdatedAt = time.Now()
		_ = writeTeamsServiceLocalSupervisorStatus(status)
		return []byte("Local supervisor is not running.\n"), nil
	}
	configPath := strings.TrimSpace(status.ConfigPath)
	if configPath == "" {
		configPath, _ = b.Path()
	}
	if err := teamsLocalSupervisorVerifyProcessIdentity(status.SupervisorPID, configPath); err != nil {
		return nil, err
	}
	if err := teamsServiceLocalSupervisorVerifyRecordedIdentity(status.SupervisorPID, status.SupervisorIdentity, "local supervisor"); err != nil {
		return nil, err
	}
	pgid, err := teamsLocalSupervisorVerifiedProcessGroupID(status.SupervisorPID, status.SupervisorPGID)
	if err != nil {
		return nil, err
	}
	if current := teamsLocalSupervisorCurrentProcessGroupID(); current > 0 && pgid == current {
		return nil, fmt.Errorf("refusing to stop local supervisor process group %d because it matches the current process group", pgid)
	}
	if status.ChildPGID > 0 && status.ChildPGID != pgid {
		if current := teamsLocalSupervisorCurrentProcessGroupID(); current > 0 && status.ChildPGID == current {
			return nil, fmt.Errorf("refusing to stop local supervisor child process group %d because it matches the current process group", status.ChildPGID)
		}
		if err := stopTeamsServiceLocalSupervisorChildFromStatus(ctx, status, true); err != nil {
			return nil, err
		}
	}
	if err := teamsLocalSupervisorTerminateProcessGroup(pgid, status.SupervisorPID, teamsServiceLocalSupervisorTerminationWait); err != nil {
		return nil, err
	}
	stoppedPID := status.SupervisorPID
	status.State = "stopped"
	status.SupervisorPID = 0
	status.SupervisorPGID = 0
	status.SupervisorIdentity = nil
	status.ChildPID = 0
	status.ChildPGID = 0
	status.ChildIdentity = nil
	status.UpdatedAt = time.Now()
	_ = writeTeamsServiceLocalSupervisorStatus(status)
	return []byte(fmt.Sprintf("Stopped local supervisor: pid=%d pgid=%d\n", stoppedPID, pgid)), nil
}

func (b teamsServiceLocalSupervisorBackend) status(_ context.Context) ([]byte, error) {
	path, err := b.Path()
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	fmt.Fprintf(&out, "Local supervisor config: %s\n", path)
	if installed, err := b.Installed(); err != nil {
		fmt.Fprintf(&out, "Installed: unknown (%v)\n", err)
	} else {
		fmt.Fprintf(&out, "Installed: %t\n", installed)
	}
	statusPath, _ := teamsServiceLocalSupervisorStatusPath()
	if strings.TrimSpace(statusPath) != "" {
		fmt.Fprintf(&out, "Local supervisor status: %s\n", statusPath)
	}
	status, ok, err := readTeamsServiceLocalSupervisorStatus()
	if err != nil {
		return out.Bytes(), err
	}
	if !ok {
		fmt.Fprintln(&out, "Active: false")
		writeTeamsServiceLocalSupervisorActivationSummary(&out)
		return out.Bytes(), nil
	}
	active := teamsServiceLocalSupervisorStatusActive(status, time.Now())
	fmt.Fprintf(&out, "Active: %t\n", active)
	fmt.Fprintf(&out, "SupervisorPID: %d\n", status.SupervisorPID)
	fmt.Fprintf(&out, "SupervisorPGID: %d\n", status.SupervisorPGID)
	fmt.Fprintf(&out, "ChildPID: %d\n", status.ChildPID)
	fmt.Fprintf(&out, "State: %s\n", firstNonEmptyCLI(status.State, "unknown"))
	if !status.UpdatedAt.IsZero() {
		fmt.Fprintf(&out, "UpdatedAt: %s\n", status.UpdatedAt.Format(time.RFC3339Nano))
	}
	if status.RestartCount > 0 {
		fmt.Fprintf(&out, "RestartCount: %d\n", status.RestartCount)
	}
	if status.LastError != "" {
		fmt.Fprintf(&out, "LastError: %s\n", status.LastError)
	}
	if status.LastHealthReason != "" {
		fmt.Fprintf(&out, "LastHealth: %s\n", status.LastHealthReason)
	}
	if status.LogPath != "" {
		fmt.Fprintf(&out, "Log: %s\n", status.LogPath)
	}
	writeTeamsServiceLocalSupervisorActivationSummary(&out)
	fmt.Fprintln(&out, "Autostart: not guaranteed after machine/container reboot")
	return out.Bytes(), nil
}

func writeTeamsServiceLocalSupervisorActivationSummary(out *bytes.Buffer) {
	if out == nil {
		return
	}
	if activation, ok, activationErr := readTeamsServiceLocalSupervisorActivation(); activationErr != nil {
		fmt.Fprintf(out, "Activation: unknown (%v)\n", activationErr)
	} else if ok {
		fmt.Fprintf(out, "Activation: %s\n", formatTeamsServiceLocalSupervisorActivation(activation))
	}
}

func formatTeamsServiceLocalSupervisorActivation(activation teamsServiceLocalSupervisorActivation) string {
	status := firstNonEmptyCLI(strings.TrimSpace(activation.Status), "unknown")
	var parts []string
	parts = append(parts, status)
	if target := strings.TrimSpace(activation.TargetVersion); target != "" {
		parts = append(parts, "target="+target)
	}
	if observed := strings.TrimSpace(activation.ObservedSupervisorEnv); observed != "" {
		parts = append(parts, "observed="+observed)
	}
	if activation.OldSupervisorPID > 0 {
		parts = append(parts, fmt.Sprintf("old_supervisor_pid=%d", activation.OldSupervisorPID))
	}
	if !activation.ScheduledAt.IsZero() {
		parts = append(parts, "scheduled="+activation.ScheduledAt.Format(time.RFC3339))
	}
	if !activation.DeadlineAt.IsZero() && (status == "scheduled" || status == "expired") {
		parts = append(parts, "deadline="+activation.DeadlineAt.Format(time.RFC3339))
	}
	if msg := strings.TrimSpace(activation.Message); msg != "" {
		parts = append(parts, "message="+msg)
	}
	return strings.Join(parts, ", ")
}

func defaultTeamsServiceLocalSupervisorStartDetached(_ context.Context, configPath string, logPath string, spec teamsServiceSpec) (int, error) {
	if strings.TrimSpace(configPath) == "" {
		return 0, fmt.Errorf("local supervisor config path is required")
	}
	logFile, err := openTeamsServiceLocalSupervisorLog(logPath)
	if err != nil {
		return 0, err
	}
	defer logFile.Close()
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return 0, err
	}
	defer devNull.Close()
	cmd := exec.Command(spec.Executable, "teams", "service", "local-supervisor", "--config", configPath)
	cmd.Dir = spec.WorkingDir
	cmd.Env = teamsServiceLocalSupervisorProcessEnv(spec.Environment)
	cmd.Stdin = devNull
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	configureTeamsLocalSupervisorDetachedCommand(cmd)
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	if err := teamsServiceLocalSupervisorReleaseProcess(cmd.Process); err != nil {
		return 0, handleTeamsServiceLocalSupervisorReleaseFailure(pid, configPath, err, cmd.Wait)
	}
	return pid, nil
}

func handleTeamsServiceLocalSupervisorReleaseFailure(pid int, configPath string, releaseErr error, wait func() error) error {
	terminateErr := terminateStartedTeamsServiceLocalSupervisor(pid, configPath)
	if terminateErr != nil {
		return fmt.Errorf("release detached local supervisor pid %d: %w; cleanup failed: %v", pid, releaseErr, terminateErr)
	}
	if wait != nil {
		done := make(chan struct{})
		go func() {
			_ = wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(teamsServiceLocalSupervisorReleaseWait):
			return fmt.Errorf("release detached local supervisor pid %d: %w; cleanup wait timed out", pid, releaseErr)
		}
	}
	return releaseErr
}

func runTeamsServiceLocalSupervisor(ctx context.Context, configPath string) error {
	if strings.TrimSpace(configPath) == "" {
		var err error
		configPath, err = teamsServiceLocalSupervisorConfigPath()
		if err != nil {
			return err
		}
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(configPath)
	if err != nil {
		return err
	}
	lockPath, err := teamsServiceLocalSupervisorLockPath()
	if err != nil {
		return err
	}
	if err := ensureTeamsServiceLocalSupervisorDir(filepath.Dir(lockPath)); err != nil {
		return err
	}
	lock := flock.New(lockPath)
	locked, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("local supervisor is already running")
	}
	defer func() { _ = lock.Unlock() }()

	logPath, err := teamsServiceLocalSupervisorLogPath()
	if err != nil {
		return err
	}
	logWriter, err := openTeamsServiceLocalSupervisorLogWriter(logPath)
	if err != nil {
		return err
	}
	defer func() { _ = logWriter.Close() }()

	supervisorCtx, stop := teamsLocalSupervisorNotifyContext(ctx)
	defer stop()

	supervisorExecutable := cfg.Spec.Executable
	if exe, err := helperpath.RawExecutable(); err == nil && strings.TrimSpace(exe) != "" {
		supervisorExecutable = exe
	}
	status := teamsServiceLocalSupervisorStatus{
		Version:            teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:         configPath,
		LogPath:            logPath,
		SupervisorPID:      os.Getpid(),
		SupervisorPGID:     teamsLocalSupervisorCurrentProcessGroupID(),
		SupervisorIdentity: teamsServiceLocalSupervisorIdentityForProcess(os.Getpid(), supervisorExecutable, []string{"teams", "service", "local-supervisor", "--config", configPath}, cfg.Spec.Environment),
		State:              "starting",
		Enabled:            cfg.Enabled,
		StartedAt:          time.Now(),
		UpdatedAt:          time.Now(),
	}
	_ = writeTeamsServiceLocalSupervisorStatus(status)
	defer func() {
		status.State = "stopped"
		status.SupervisorPID = 0
		status.SupervisorPGID = 0
		status.SupervisorIdentity = nil
		status.ChildPID = 0
		status.ChildPGID = 0
		status.ChildIdentity = nil
		status.UpdatedAt = time.Now()
		_ = writeTeamsServiceLocalSupervisorStatus(status)
	}()

	for {
		if err := supervisorCtx.Err(); err != nil {
			return nil
		}
		if runErr := runTeamsServiceLocalSupervisorChild(supervisorCtx, cfg, &status, logWriter); runErr != nil && !errors.Is(runErr, context.Canceled) {
			status.LastError = runErr.Error()
			status.UpdatedAt = time.Now()
			_ = writeTeamsServiceLocalSupervisorStatus(status)
		}
		if err := logWriter.RotateIfNeeded(); err != nil {
			status.LastError = "local supervisor log rotation failed: " + err.Error()
			status.UpdatedAt = time.Now()
			_ = writeTeamsServiceLocalSupervisorStatus(status)
		}
		if err := sleepContext(supervisorCtx, teamsServiceLocalSupervisorRestartDelay); err != nil {
			return nil
		}
		status.RestartCount++
	}
}

func runTeamsServiceLocalSupervisorChild(ctx context.Context, cfg teamsServiceLocalSupervisorConfig, status *teamsServiceLocalSupervisorStatus, logFile io.Writer) error {
	args := buildTeamsServiceRunArgs(cfg.Spec)
	cmd := exec.Command(cfg.Spec.Executable, args...)
	cmd.Dir = cfg.Spec.WorkingDir
	cmd.Env = teamsServiceLocalSupervisorChildProcessEnv(cfg.Spec.Environment)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	configureTargetProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	childPGID, _ := teamsLocalSupervisorProcessGroupID(cmd.Process.Pid)
	status.State = "running"
	status.Enabled = cfg.Enabled
	status.ChildPID = cmd.Process.Pid
	status.ChildPGID = childPGID
	status.ChildIdentity = teamsServiceLocalSupervisorIdentityForProcess(cmd.Process.Pid, cfg.Spec.Executable, args, cfg.Spec.Environment)
	status.LastChildStartAt = time.Now()
	status.UpdatedAt = status.LastChildStartAt
	status.LastError = ""
	_ = writeTeamsServiceLocalSupervisorStatus(*status)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ticker := time.NewTicker(teamsServiceLocalSupervisorHeartbeatEvery)
	defer ticker.Stop()
	var healthState teamsServiceWatchdogState
	for {
		select {
		case <-ctx.Done():
			_ = teamsServiceLocalSupervisorTerminateTarget(cmd, teamsServiceLocalSupervisorTerminationWait)
			return ctx.Err()
		case <-ticker.C:
			if decision, err := teamsServiceLocalSupervisorCheckChildHealth(ctx, &healthState); err != nil {
				status.LastHealthCheckAt = time.Now()
				status.LastHealthReason = "health check unavailable: " + err.Error()
				status.LastHealthAction = ""
			} else {
				status.LastHealthCheckAt = time.Now()
				status.LastHealthReason = decision.Reason
				status.LastHealthAction = firstNonEmptyCLI(decision.Action, teamsServiceWatchdogActionNoop)
				if decision.Action == teamsServiceWatchdogActionRestart {
					status.LastError = "local supervisor health check restarting child: " + decision.Reason
					status.UpdatedAt = time.Now()
					_ = writeTeamsServiceLocalSupervisorStatus(*status)
					if err := teamsServiceLocalSupervisorTerminateTarget(cmd, teamsServiceLocalSupervisorTerminationWait); err != nil {
						status.LastError = "local supervisor health check could not terminate child: " + err.Error()
						status.UpdatedAt = time.Now()
						_ = writeTeamsServiceLocalSupervisorStatus(*status)
						return errors.New(status.LastError)
					}
					select {
					case err := <-done:
						status.LastChildExitAt = time.Now()
						status.UpdatedAt = status.LastChildExitAt
						status.ChildPID = 0
						status.ChildPGID = 0
						status.ChildIdentity = nil
						status.State = "waiting"
						status.LastExitCode = teamsServiceLocalSupervisorExitCode(err)
						_ = writeTeamsServiceLocalSupervisorStatus(*status)
						return fmt.Errorf("local supervisor health check restarted child: %s", decision.Reason)
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			status.UpdatedAt = time.Now()
			_ = writeTeamsServiceLocalSupervisorStatus(*status)
		case err := <-done:
			status.LastChildExitAt = time.Now()
			status.UpdatedAt = status.LastChildExitAt
			status.ChildPID = 0
			status.ChildPGID = 0
			status.ChildIdentity = nil
			status.State = "waiting"
			status.LastExitCode = teamsServiceLocalSupervisorExitCode(err)
			if err != nil {
				status.LastError = err.Error()
			}
			_ = writeTeamsServiceLocalSupervisorStatus(*status)
			return err
		}
	}
}

func defaultTeamsServiceLocalSupervisorCheckChildHealth(ctx context.Context, state *teamsServiceWatchdogState) (teamsServiceWatchdogDecision, error) {
	if state == nil {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "health state unavailable"}, nil
	}
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: time.Now()})
	snapshot, err := collectTeamsServiceWatchdogSnapshot(ctx, opts)
	if err != nil {
		return teamsServiceWatchdogDecision{}, err
	}
	decision := evaluateTeamsServiceWatchdog(snapshot, *state, opts)
	*state = nextTeamsServiceWatchdogState(*state, decision, opts.Now)
	return decision, nil
}

func teamsServiceLocalSupervisorChildProcessEnv(env map[string]string) []string {
	childEnv := make(map[string]string, len(env)+1)
	for key, value := range env {
		childEnv[key] = value
	}
	childEnv[envTeamsLocalSupervisorVersion] = buildVersion()
	return teamsServiceLocalSupervisorProcessEnv(childEnv)
}

func teamsServiceLocalSupervisorProcessEnv(env map[string]string) []string {
	base := map[string]string{}
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || strings.TrimSpace(key) == "" {
			continue
		}
		if teamsServiceLocalSupervisorBaseEnvAllowed(key) {
			base[key] = value
		}
	}
	for _, key := range sortedEnvironmentKeys(env) {
		base[key] = env[key]
	}
	out := make([]string, 0, len(base))
	for _, key := range sortedEnvironmentKeys(base) {
		out = append(out, key+"="+base[key])
	}
	return out
}

func teamsServiceLocalSupervisorIdentityForProcess(pid int, executable string, args []string, env map[string]string) *teamsServiceLocalSupervisorProcessIdentity {
	identity := &teamsServiceLocalSupervisorProcessIdentity{
		Executable:  strings.TrimSpace(executable),
		Args:        append([]string{}, args...),
		Environment: teamsServiceLocalSupervisorIdentityEnvironment(env),
	}
	if startTime, err := teamsLocalSupervisorProcessStartTime(pid); err == nil {
		identity.ProcStartTime = startTime
	}
	if identity.Executable == "" && len(identity.Args) == 0 && len(identity.Environment) == 0 && identity.ProcStartTime == "" {
		return nil
	}
	return identity
}

func teamsServiceLocalSupervisorIdentityEnvironment(env map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range env {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if key == appdirs.EnvStateDir || key == "CODEX_PROXY_INSTALL_DIR" || key == "CODEX_PROXY_INSTALL_PATH" || strings.HasPrefix(key, "CODEX_HELPER_TEAMS_") {
			out[key] = value
		}
	}
	return out
}

func teamsServiceLocalSupervisorVerifyRecordedIdentity(pid int, identity *teamsServiceLocalSupervisorProcessIdentity, label string) error {
	if identity == nil || pid <= 0 {
		return nil
	}
	var liveArgs []string
	if len(identity.Args) > 0 || strings.TrimSpace(identity.Executable) != "" {
		args, err := teamsLocalSupervisorProcessArgs(pid)
		if err != nil {
			return fmt.Errorf("could not verify %s pid %d args: %w", label, pid, err)
		}
		liveArgs = args
	}
	if len(identity.Args) > 0 {
		if !teamsServiceLocalSupervisorArgsContainSequence(liveArgs, identity.Args) {
			return fmt.Errorf("refusing to manage %s pid %d because process args do not match recorded service identity", label, pid)
		}
	}
	if strings.TrimSpace(identity.ProcStartTime) != "" {
		got, err := teamsLocalSupervisorProcessStartTime(pid)
		if err != nil {
			return fmt.Errorf("could not verify %s pid %d start time: %w", label, pid, err)
		}
		if got != identity.ProcStartTime {
			return fmt.Errorf("refusing to manage %s pid %d because process start time changed from %s to %s", label, pid, identity.ProcStartTime, got)
		}
	}
	if strings.TrimSpace(identity.Executable) != "" {
		if err := teamsLocalSupervisorExecutableMatches(pid, identity.Executable, liveArgs); err != nil {
			return err
		}
	}
	if len(identity.Environment) > 0 {
		env, err := teamsLocalSupervisorProcessEnvironment(pid)
		if err != nil {
			return fmt.Errorf("could not verify %s pid %d environment: %w", label, pid, err)
		}
		for _, key := range sortedEnvironmentKeys(identity.Environment) {
			if env[key] != identity.Environment[key] {
				return fmt.Errorf("refusing to manage %s pid %d because environment %s does not match recorded service identity", label, pid, key)
			}
		}
	}
	return nil
}

func teamsServiceLocalSupervisorArgsContainSequence(args []string, want []string) bool {
	if len(want) == 0 {
		return true
	}
	for i := 0; i+len(want) <= len(args); i++ {
		matched := true
		for j := range want {
			if args[i+j] != want[j] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func teamsServiceLocalSupervisorBaseEnvAllowed(key string) bool {
	switch key {
	case "HOME", "USER", "LOGNAME", "SHELL", "PATH", "LANG", "TMPDIR", "TEMP", "TMP",
		"XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME":
		return true
	}
	return strings.HasPrefix(key, "LC_")
}

func teamsServiceLocalSupervisorExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func teamsServiceLocalSupervisorStatusActive(status teamsServiceLocalSupervisorStatus, now time.Time) bool {
	if status.SupervisorPID <= 0 || !teamsLocalSupervisorProcessAlive(status.SupervisorPID) {
		return false
	}
	if strings.TrimSpace(status.ConfigPath) == "" {
		return false
	}
	if err := teamsLocalSupervisorVerifyProcessIdentity(status.SupervisorPID, status.ConfigPath); err != nil {
		return false
	}
	if err := teamsServiceLocalSupervisorVerifyRecordedIdentity(status.SupervisorPID, status.SupervisorIdentity, "local supervisor"); err != nil {
		return false
	}
	if status.UpdatedAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return now.Sub(status.UpdatedAt) <= teamsServiceLocalSupervisorStatusFreshness
}

func waitTeamsServiceLocalSupervisorReady(ctx context.Context, pid int, configPath string, timeout time.Duration) error {
	if pid <= 0 {
		return fmt.Errorf("local supervisor did not report a valid pid")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = teamsServiceLocalSupervisorReadyTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		if !teamsLocalSupervisorProcessAlive(pid) {
			if lastErr != nil {
				return fmt.Errorf("local supervisor pid %d exited before reporting ready: %w", pid, lastErr)
			}
			return fmt.Errorf("local supervisor pid %d exited before reporting ready", pid)
		}
		status, ok, err := readTeamsServiceLocalSupervisorStatus()
		if err != nil {
			lastErr = err
		} else if ok && status.SupervisorPID == pid && teamsServiceLocalSupervisorSamePath(status.ConfigPath, configPath) && teamsServiceLocalSupervisorStatusActive(status, time.Now()) {
			if err := teamsLocalSupervisorVerifyProcessIdentity(pid, configPath); err != nil {
				lastErr = err
			} else {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			if lastErr != nil {
				return fmt.Errorf("local supervisor pid %d did not report ready within %s: %w", pid, timeout, lastErr)
			}
			return fmt.Errorf("local supervisor pid %d did not report ready within %s", pid, timeout)
		case <-ticker.C:
		}
	}
}

func waitTeamsServiceLocalSupervisorLockOwnerReady(ctx context.Context, timeout time.Duration) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = teamsServiceLocalSupervisorReadyTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		status, ok, err := readTeamsServiceLocalSupervisorStatus()
		if err != nil {
			lastErr = err
		} else if ok && teamsServiceLocalSupervisorStatusActive(status, time.Now()) {
			return []byte(fmt.Sprintf("Local supervisor already running: pid=%d\n", status.SupervisorPID)), nil
		}
		select {
		case <-waitCtx.Done():
			if lastErr != nil {
				return nil, fmt.Errorf("local supervisor lock is held but no verified supervisor status became ready within %s: %w", timeout, lastErr)
			}
			return nil, fmt.Errorf("local supervisor lock is held but no verified supervisor status became ready within %s", timeout)
		case <-ticker.C:
		}
	}
}

func terminateStartedTeamsServiceLocalSupervisor(pid int, configPath string) error {
	if pid <= 0 || !teamsLocalSupervisorProcessAlive(pid) {
		return nil
	}
	if err := teamsLocalSupervisorVerifyProcessIdentity(pid, configPath); err != nil {
		return err
	}
	pgid, err := teamsLocalSupervisorVerifiedProcessGroupID(pid, 0)
	if err != nil {
		return err
	}
	if current := teamsLocalSupervisorCurrentProcessGroupID(); current > 0 && pgid == current {
		return fmt.Errorf("refusing to terminate started local supervisor process group %d because it matches the current process group", pgid)
	}
	return teamsLocalSupervisorTerminateProcessGroup(pgid, pid, teamsServiceLocalSupervisorTerminationWait)
}

func stopTeamsServiceLocalSupervisorChildFromStatus(ctx context.Context, status teamsServiceLocalSupervisorStatus, verify bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if status.ChildPID <= 0 && status.ChildPGID <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if status.ChildPID <= 0 && status.ChildPGID > 0 {
		return fmt.Errorf("refusing to stop local supervisor child process group %d because status has no child pid to verify", status.ChildPGID)
	}
	if status.ChildPID > 0 && !teamsLocalSupervisorProcessAlive(status.ChildPID) {
		return nil
	}
	if verify && status.ChildPID > 0 {
		if status.ChildIdentity != nil {
			if err := teamsServiceLocalSupervisorVerifyRecordedIdentity(status.ChildPID, status.ChildIdentity, "local supervisor child"); err != nil {
				return err
			}
		} else {
			spec, err := teamsServiceLocalSupervisorChildSpecForStatus(status)
			if err != nil {
				return err
			}
			if err := teamsLocalSupervisorVerifyChildIdentity(status.ChildPID, spec); err != nil {
				return err
			}
		}
	}
	livePGID, err := teamsLocalSupervisorVerifiedProcessGroupID(status.ChildPID, status.ChildPGID)
	if err != nil {
		return err
	}
	status.ChildPGID = livePGID
	if current := teamsLocalSupervisorCurrentProcessGroupID(); current > 0 && status.ChildPGID == current {
		return fmt.Errorf("refusing to stop local supervisor child process group %d because it matches the current process group", status.ChildPGID)
	}
	return teamsLocalSupervisorTerminateProcessGroup(status.ChildPGID, status.ChildPID, teamsServiceLocalSupervisorTerminationWait)
}

func teamsServiceLocalSupervisorChildSpecForStatus(status teamsServiceLocalSupervisorStatus) (teamsServiceSpec, error) {
	configPath := strings.TrimSpace(status.ConfigPath)
	if configPath == "" {
		return teamsServiceSpec{}, fmt.Errorf("refusing to stop local supervisor child pid %d because status has no config path", status.ChildPID)
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(configPath)
	if err != nil {
		return teamsServiceSpec{}, fmt.Errorf("refusing to stop local supervisor child pid %d because config could not be read: %w", status.ChildPID, err)
	}
	return cfg.Spec, nil
}

func retireTeamsServiceConflictingBackendsForLocalSupervisor(ctx context.Context, _ teamsServiceSpec) error {
	if teamsServiceGOOS() == "linux" && teamsServiceIsWSL() {
		if err := (teamsServiceWSLWindowsTaskBackend{}).RetireScheduledTasks(ctx); err != nil {
			return fmt.Errorf("disable old WSL Scheduled Tasks before local-supervisor start: %w", err)
		}
		return nil
	}
	return retireTeamsServiceSystemdUserArtifactsForLocalSupervisor(ctx)
}

func retireTeamsServiceSystemdUserArtifactsForLocalSupervisor(ctx context.Context) error {
	if teamsServiceGOOS() != "linux" || teamsServiceIsWSL() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	retireCtx, cancel := context.WithTimeout(ctx, teamsServiceLocalSupervisorRetireTimeout)
	defer cancel()
	systemdAvailable, availabilityErr := teamsServiceSystemdUserAvailable(retireCtx)
	if availabilityErr != nil {
		if !teamsServiceSystemdUserUnavailableError(availabilityErr) {
			return fmt.Errorf("verify systemd --user is unavailable before local-supervisor fallback: %w", availabilityErr)
		}
		systemdAvailable = false
	}
	if systemdAvailable {
		if data, err := teamsServiceSystemdStop(retireCtx); err != nil && !teamsServiceSystemdUnitMissingError(err, data) && !teamsServiceSystemdUserUnavailableCommandError(err, data) {
			return fmt.Errorf("stop old systemd Teams service before local-supervisor fallback: %w", err)
		}
		if data, err := teamsServiceRunSystemctl(retireCtx, "disable", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName); err != nil && !teamsServiceSystemdUnitMissingError(err, data) && !teamsServiceSystemdUserUnavailableCommandError(err, data) {
			return fmt.Errorf("disable old systemd Teams service before local-supervisor fallback: %w", err)
		}
	}
	var errs []error
	for _, resolve := range []func() (string, error){
		teamsServiceUnitPath,
		teamsServiceWatchdogUnitPath,
		teamsServiceWatchdogTimerPath,
	} {
		path, err := resolve()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	if systemdAvailable {
		if data, err := teamsServiceRunSystemctl(retireCtx, "daemon-reload"); err != nil && !teamsServiceSystemdUserUnavailableCommandError(err, data) {
			errs = append(errs, fmt.Errorf("reload systemd after retiring old Teams service units: %w", err))
		}
	}
	return errors.Join(errs...)
}

func teamsServiceSystemdUserUnavailableCommandError(err error, data []byte) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(err.Error() + "\n" + string(data)))
	for _, marker := range []string{
		"failed to connect to bus",
		"failed to get d-bus connection",
		"failed to get environment",
		"org.freedesktop.systemd1 exited",
		"org.freedesktop.dbus.error.namehasnoowner",
		"the name org.freedesktop.systemd1 was not provided",
		"dbus-org.freedesktop.systemd1.service not found",
		"cannot autolaunch d-bus",
		"no medium found",
		"host is down",
		"connection refused",
		"connection reset by peer",
		"no route to host",
		"transport endpoint is not connected",
		"transport endpoint not connected",
		"xdg_runtime_dir",
		"not booted with systemd",
		"system has not been booted with systemd",
		"executable file not found",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func teamsServiceSystemdUserUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{
		"failed to connect to bus",
		"failed to get d-bus connection",
		"failed to get environment",
		"org.freedesktop.systemd1 exited",
		"org.freedesktop.dbus.error.namehasnoowner",
		"the name org.freedesktop.systemd1 was not provided",
		"dbus-org.freedesktop.systemd1.service not found",
		"cannot autolaunch d-bus",
		"operation not permitted",
		"permission denied",
		"no medium found",
		"host is down",
		"connection refused",
		"connection reset by peer",
		"no route to host",
		"transport endpoint is not connected",
		"transport endpoint not connected",
		"xdg_runtime_dir",
		"context deadline exceeded",
		"deadline exceeded",
		"timed out",
		"timeout",
		"signal: killed",
		"not booted with systemd",
		"system has not been booted with systemd",
		"no such file or directory",
		"executable file not found",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func teamsServiceLocalSupervisorSamePath(a string, b string) bool {
	a = teamsServiceCleanRegistryPath(a)
	b = teamsServiceCleanRegistryPath(b)
	return a != "" && b != "" && a == b
}

func teamsServiceLocalSupervisorSticky() bool {
	path, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		return false
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(path)
	if err == nil && cfg.Enabled {
		return true
	}
	if status, ok, err := readTeamsServiceLocalSupervisorStatus(); err == nil && ok && teamsServiceLocalSupervisorStatusActive(status, time.Now()) {
		return true
	}
	return false
}

func teamsServiceDisabledLocalSupervisorConfigPath() (string, bool) {
	path, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		return "", false
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(path)
	if err != nil || cfg.Enabled {
		return "", false
	}
	if status, ok, err := readTeamsServiceLocalSupervisorStatus(); err == nil && ok && teamsServiceLocalSupervisorStatusActive(status, time.Now()) {
		return "", false
	}
	if held, err := teamsServiceLocalSupervisorLockHeldExisting(); err == nil && held {
		return "", false
	}
	return path, true
}

func removeDisabledTeamsServiceLocalSupervisorConfigIfInactive() error {
	path, ok := teamsServiceDisabledLocalSupervisorConfigPath()
	if !ok {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	statusPath, _ := teamsServiceLocalSupervisorStatusPath()
	if strings.TrimSpace(statusPath) != "" {
		_ = os.Remove(statusPath)
	}
	return nil
}

func teamsServiceLocalSupervisorLockHeld() (bool, error) {
	lockPath, err := teamsServiceLocalSupervisorLockPath()
	if err != nil {
		return false, err
	}
	return teamsServiceLocalSupervisorLockHeldAtPath(lockPath, true)
}

func teamsServiceLocalSupervisorLockHeldExisting() (bool, error) {
	lockPath, err := teamsServiceLocalSupervisorLockPath()
	if err != nil {
		return false, err
	}
	return teamsServiceLocalSupervisorLockHeldAtPath(lockPath, false)
}

func teamsServiceLocalSupervisorLockHeldAtPath(lockPath string, create bool) (bool, error) {
	dir := filepath.Dir(lockPath)
	if create {
		if err := ensureTeamsServiceLocalSupervisorDir(dir); err != nil {
			return false, err
		}
	} else if _, err := os.Lstat(dir); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	} else if err := validateTeamsServiceLocalSupervisorExistingDir(dir); err != nil {
		return false, err
	}
	if !create {
		if _, err := os.Lstat(lockPath); err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
	}
	if err := validateTeamsServiceLocalSupervisorRegularFile(lockPath, "local supervisor lock", true); err != nil {
		return false, err
	}
	lock := flock.New(lockPath)
	locked, err := lock.TryLock()
	if err != nil {
		return false, err
	}
	if !locked {
		return true, nil
	}
	if err := lock.Unlock(); err != nil {
		return false, err
	}
	return false, nil
}

func ensureTeamsServiceLocalSupervisorDir(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("local supervisor directory path is required")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return validateTeamsServiceLocalSupervisorDir(path)
}

func validateTeamsServiceLocalSupervisorRegularFile(path string, label string, allowMissing bool) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("%s path is required", label)
	}
	info, err := os.Lstat(path)
	if err != nil {
		if allowMissing && os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s must not be a symlink: %s", label, path)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular file: %s", label, path)
	}
	return nil
}

func renderTeamsServiceLocalSupervisorConfig(spec teamsServiceSpec, enabled bool) string {
	cfg := teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: enabled,
		Spec:    spec,
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Sprintf("error: %v\n", err)
	}
	return string(data) + "\n"
}

func readTeamsServiceLocalSupervisorConfig(path string) (teamsServiceLocalSupervisorConfig, error) {
	if err := validateTeamsServiceLocalSupervisorRegularFile(path, "local supervisor config", false); err != nil {
		return teamsServiceLocalSupervisorConfig{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return teamsServiceLocalSupervisorConfig{}, err
	}
	var cfg teamsServiceLocalSupervisorConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return teamsServiceLocalSupervisorConfig{}, err
	}
	if cfg.Version == 0 {
		cfg.Version = teamsServiceLocalSupervisorConfigVersion
	}
	if strings.TrimSpace(cfg.Spec.Executable) == "" {
		return teamsServiceLocalSupervisorConfig{}, fmt.Errorf("local supervisor config missing executable: %s", path)
	}
	return cfg, nil
}

func writeTeamsServiceLocalSupervisorConfig(path string, cfg teamsServiceLocalSupervisorConfig) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("local supervisor config path is required")
	}
	if err := ensureTeamsServiceLocalSupervisorDir(filepath.Dir(path)); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeTeamsServiceLocalSupervisorFileAtomic(path, data, 0o600)
}

func readTeamsServiceLocalSupervisorStatus() (teamsServiceLocalSupervisorStatus, bool, error) {
	path, err := teamsServiceLocalSupervisorStatusPath()
	if err != nil {
		return teamsServiceLocalSupervisorStatus{}, false, err
	}
	if err := validateTeamsServiceLocalSupervisorRegularFile(path, "local supervisor status", true); err != nil {
		if os.IsNotExist(err) {
			return teamsServiceLocalSupervisorStatus{}, false, nil
		}
		return teamsServiceLocalSupervisorStatus{}, false, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return teamsServiceLocalSupervisorStatus{}, false, nil
		}
		return teamsServiceLocalSupervisorStatus{}, false, err
	}
	var status teamsServiceLocalSupervisorStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return teamsServiceLocalSupervisorStatus{}, false, fmt.Errorf("%w: %s: %v", errTeamsServiceLocalSupervisorStatusMalformed, path, err)
	}
	return status, true, nil
}

func writeTeamsServiceLocalSupervisorStatus(status teamsServiceLocalSupervisorStatus) error {
	path, err := teamsServiceLocalSupervisorStatusPath()
	if err != nil {
		return err
	}
	if err := ensureTeamsServiceLocalSupervisorDir(filepath.Dir(path)); err != nil {
		return err
	}
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeTeamsServiceLocalSupervisorFileAtomic(path, data, 0o600)
}

func readTeamsServiceLocalSupervisorActivation() (teamsServiceLocalSupervisorActivation, bool, error) {
	path, err := teamsServiceLocalSupervisorActivationPath()
	if err != nil {
		return teamsServiceLocalSupervisorActivation{}, false, err
	}
	if err := validateTeamsServiceLocalSupervisorRegularFile(path, "local supervisor activation", true); err != nil {
		if os.IsNotExist(err) {
			return teamsServiceLocalSupervisorActivation{}, false, nil
		}
		return teamsServiceLocalSupervisorActivation{}, false, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return teamsServiceLocalSupervisorActivation{}, false, nil
		}
		return teamsServiceLocalSupervisorActivation{}, false, err
	}
	var activation teamsServiceLocalSupervisorActivation
	if err := json.Unmarshal(data, &activation); err != nil {
		return teamsServiceLocalSupervisorActivation{}, false, fmt.Errorf("local supervisor activation is malformed: %s: %v", path, err)
	}
	return activation, true, nil
}

func writeTeamsServiceLocalSupervisorActivation(activation teamsServiceLocalSupervisorActivation) error {
	path, err := teamsServiceLocalSupervisorActivationPath()
	if err != nil {
		return err
	}
	if err := ensureTeamsServiceLocalSupervisorDir(filepath.Dir(path)); err != nil {
		return err
	}
	if activation.Version == 0 {
		activation.Version = teamsServiceLocalSupervisorActivationVersion
	}
	activation.Status = strings.TrimSpace(activation.Status)
	activation.TargetVersion = strings.TrimSpace(activation.TargetVersion)
	activation.ObservedSupervisorEnv = strings.TrimSpace(activation.ObservedSupervisorEnv)
	activation.Message = strings.TrimSpace(activation.Message)
	if activation.UpdatedAt.IsZero() {
		activation.UpdatedAt = time.Now()
	}
	data, err := json.MarshalIndent(activation, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeTeamsServiceLocalSupervisorFileAtomic(path, data, 0o600)
}

func writeTeamsServiceLocalSupervisorFileAtomic(path string, data []byte, perm os.FileMode) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("local supervisor file path is required")
	}
	dir := filepath.Dir(path)
	if err := ensureTeamsServiceLocalSupervisorDir(dir); err != nil {
		return err
	}
	if err := validateTeamsServiceLocalSupervisorRegularFile(path, "local supervisor file", true); err != nil {
		return err
	}
	if err := writeFileAtomically(path, data, perm); err != nil {
		return err
	}
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	return nil
}

func openTeamsServiceLocalSupervisorLog(path string) (*os.File, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("local supervisor log path is required")
	}
	if err := ensureTeamsServiceLocalSupervisorDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if err := validateTeamsServiceLocalSupervisorRegularFile(path, "local supervisor log", true); err != nil {
		return nil, err
	}
	if err := rotateTeamsServiceLocalSupervisorLogIfNeeded(path); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
}

type teamsServiceLocalSupervisorLogWriter struct {
	mu   sync.Mutex
	path string
	file *os.File
}

func openTeamsServiceLocalSupervisorLogWriter(path string) (*teamsServiceLocalSupervisorLogWriter, error) {
	file, err := openTeamsServiceLocalSupervisorLog(path)
	if err != nil {
		return nil, err
	}
	return &teamsServiceLocalSupervisorLogWriter{path: path, file: file}, nil
}

func (w *teamsServiceLocalSupervisorLogWriter) Write(p []byte) (int, error) {
	if w == nil {
		return 0, fmt.Errorf("local supervisor log writer is nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotateLocked(); err != nil {
		return 0, err
	}
	return w.file.Write(p)
}

func (w *teamsServiceLocalSupervisorLogWriter) RotateIfNeeded() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

func (w *teamsServiceLocalSupervisorLogWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func (w *teamsServiceLocalSupervisorLogWriter) rotateLocked() error {
	next, err := reopenTeamsServiceLocalSupervisorLogIfNeeded(w.path, w.file)
	if err != nil {
		return err
	}
	w.file = next
	return nil
}

func reopenTeamsServiceLocalSupervisorLogIfNeeded(path string, current *os.File) (*os.File, error) {
	if current == nil {
		return openTeamsServiceLocalSupervisorLog(path)
	}
	info, err := current.Stat()
	if err != nil {
		return current, err
	}
	if info.Size() <= teamsServiceLocalSupervisorMaxLogBytes {
		return current, nil
	}
	next, err := openTeamsServiceLocalSupervisorLog(path)
	if err != nil {
		return current, err
	}
	if err := current.Close(); err != nil {
		_ = next.Close()
		return current, err
	}
	return next, nil
}

func rotateTeamsServiceLocalSupervisorLogIfNeeded(path string) error {
	if err := validateTeamsServiceLocalSupervisorRegularFile(path, "local supervisor log", true); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Size() <= teamsServiceLocalSupervisorMaxLogBytes {
		return nil
	}
	oldest := fmt.Sprintf("%s.%d", path, teamsServiceLocalSupervisorLogBackups)
	if err := validateTeamsServiceLocalSupervisorRegularFile(oldest, "local supervisor log backup", true); err != nil {
		return err
	}
	_ = os.Remove(oldest)
	for i := teamsServiceLocalSupervisorLogBackups - 1; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", path, i)
		dst := fmt.Sprintf("%s.%d", path, i+1)
		if err := validateTeamsServiceLocalSupervisorRegularFile(src, "local supervisor log backup", true); err != nil {
			return err
		}
		if _, err := os.Stat(src); err == nil {
			if err := os.Rename(src, dst); err != nil {
				return err
			}
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.Rename(path, path+".1")
}

func teamsServiceLocalSupervisorConfigPath() (string, error) {
	dir, err := teamsServiceLocalSupervisorConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceLocalSupervisorConfigName), nil
}

func teamsServiceLocalSupervisorStatusPath() (string, error) {
	dir, err := teamsServiceLocalSupervisorRuntimeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceLocalSupervisorStatusName), nil
}

func teamsServiceLocalSupervisorActivationPath() (string, error) {
	dir, err := teamsServiceLocalSupervisorRuntimeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceLocalSupervisorActivationName), nil
}

func teamsServiceLocalSupervisorLockPath() (string, error) {
	dir, err := teamsServiceLocalSupervisorRuntimeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceLocalSupervisorLockName), nil
}

func teamsServiceLocalSupervisorLogPath() (string, error) {
	dir, err := teamsServiceLocalSupervisorLogDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceLocalSupervisorLogName), nil
}

func teamsServiceLocalSupervisorConfigDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams"), nil
}

func teamsServiceLocalSupervisorRuntimeDir() (string, error) {
	dir, err := appdirs.StatePath("teams", "service", "local-supervisor", "run")
	if err != nil {
		return "", err
	}
	configBase, configErr := teamsServiceLocalSupervisorConfigDir()
	if configErr != nil {
		return dir, nil
	}
	legacyDir := filepath.Join(configBase, "run", "local-supervisor")
	resolvedDir, err := appdirs.ResolveMigratedDirWithRequired(dir, legacyDir, teamsServiceLocalSupervisorStatusName)
	if err != nil {
		return "", err
	}
	statusPath := filepath.Join(resolvedDir, teamsServiceLocalSupervisorStatusName)
	legacyStatusPath := filepath.Join(legacyDir, teamsServiceLocalSupervisorStatusName)
	if filepath.Clean(resolvedDir) == filepath.Clean(dir) && filepath.Clean(dir) != filepath.Clean(legacyDir) && !teamsServiceLocalSupervisorStatusFileValid(statusPath) && teamsServiceLocalSupervisorStatusFileValid(legacyStatusPath) {
		if err := appdirs.CopyFileReplacing(statusPath, legacyStatusPath); err != nil {
			return legacyDir, nil
		}
	}
	return resolvedDir, nil
}

func teamsServiceLocalSupervisorStatusFileValid(path string) bool {
	if err := validateTeamsServiceLocalSupervisorRegularFile(path, "local supervisor status", true); err != nil {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var status teamsServiceLocalSupervisorStatus
	return json.Unmarshal(data, &status) == nil
}

func teamsServiceLocalSupervisorLogDir() (string, error) {
	dir, err := appdirs.StatePath("teams", "service", "local-supervisor", "logs")
	if err != nil {
		return teamsServiceLocalSupervisorRuntimeDir()
	}
	legacyDir, legacyErr := appdirs.LegacyCachePath("teams", "local-supervisor")
	if legacyErr != nil {
		return dir, nil
	}
	return appdirs.ResolveMigratedDir(dir, legacyDir)
}
