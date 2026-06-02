package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

const (
	teamsServiceUnitName                      = "codex-helper-teams.service"
	teamsServiceWatchdogUnitName              = "codex-helper-teams-watchdog.service"
	teamsServiceWatchdogTimerName             = "codex-helper-teams-watchdog.timer"
	teamsServiceLaunchAgentLabel              = "com.codex-helper.teams"
	teamsServiceLaunchAgentPlistName          = teamsServiceLaunchAgentLabel + ".plist"
	teamsServiceLaunchAgentWatchdogLabel      = teamsServiceLaunchAgentLabel + ".watchdog"
	teamsServiceLaunchAgentWatchdogPlistName  = teamsServiceLaunchAgentWatchdogLabel + ".plist"
	teamsServiceWindowsTaskName               = "Codex Helper Teams Bridge"
	teamsServiceWindowsWatchdogTaskName       = "Codex Helper Teams Watchdog"
	teamsServiceWindowsTaskXMLName            = "codex-helper-teams-task.xml"
	teamsServiceWindowsWatchdogTaskXMLName    = "codex-helper-teams-watchdog-task.xml"
	teamsServiceWSLTaskConfigName             = "codex-helper-teams-wsl-task.txt"
	teamsServiceLocalSupervisorConfigName     = "local-supervisor.json"
	teamsServiceLocalSupervisorStatusName     = "local-supervisor-status.json"
	teamsServiceLocalSupervisorActivationName = "local-supervisor-activation.json"
	teamsServiceLocalSupervisorLockName       = "local-supervisor.lock"
	teamsServiceLocalSupervisorLogName        = "local-supervisor.log"
	teamsServiceTaskRestartCount              = 999
	teamsServiceTaskRestartInterval           = 10
	teamsServiceTaskSchedulerRestartMinutes   = 1
	teamsServiceWatchdogMinutes               = 1
	teamsServiceWatchdogDays                  = 3650
	teamsServiceRunOwnerStaleAfter            = 90 * time.Second
	teamsServiceExternalWatchdogInterval      = 10 * time.Second
	teamsServiceExternalWatchdogCheckTimeout  = 45 * time.Second
	teamsServiceExternalWatchdogSeconds       = int(teamsServiceExternalWatchdogInterval / time.Second)
	teamsServiceExternalWatchdogMinutes       = 1
)

type teamsServiceCommandRunner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

type teamsServiceExecRunner struct{}

func (teamsServiceExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
}

var (
	teamsServiceGOOS       = func() string { return runtime.GOOS }
	teamsServiceExecutable = helperpath.RawExecutable
	teamsServiceArgv0      = func() string {
		if len(os.Args) == 0 {
			return ""
		}
		return os.Args[0]
	}
	teamsServiceGetwd                                                 = os.Getwd
	teamsServiceSystemdUserDir                                        = defaultTeamsServiceSystemdUserDir
	teamsServiceLaunchAgentDir                                        = defaultTeamsServiceLaunchAgentDir
	teamsServiceWindowsTaskXMLDir                                     = defaultTeamsServiceWindowsTaskXMLDir
	teamsServiceUserID                                                = defaultTeamsServiceUserID
	teamsServiceIsWSL                                                 = defaultTeamsServiceIsWSL
	teamsServiceWSLDistroName                                         = defaultTeamsServiceWSLDistroName
	teamsServiceWSLLinuxUserName                                      = defaultTeamsServiceWSLLinuxUserName
	teamsServicePowerShellExecutable                                  = defaultTeamsServicePowerShellExecutable
	teamsServiceSystemctl                   teamsServiceCommandRunner = teamsServiceExecRunner{}
	teamsServiceSystemdUserAvailable                                  = defaultTeamsServiceSystemdUserAvailable
	teamsServiceAuthPreflight                                         = defaultTeamsServiceAuthPreflight
	teamsServiceBootstrapControlChat                                  = defaultTeamsServiceBootstrapControlChat
	teamsServiceOpenURL                                               = defaultTeamsServiceOpenURL
	teamsServiceStartupFallbackRestartDelay                           = 2 * time.Second
)

func newTeamsServiceCmd(root *rootOptions, registryPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "service",
		Short: "Manage the Teams bridge background service",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(
		newTeamsServiceInstallCmd(registryPath),
		newTeamsServiceBootstrapCmd(root, registryPath),
		newTeamsServiceUninstallCmd(),
		newTeamsServiceEnableCmd(),
		newTeamsServiceDisableCmd(),
		newTeamsServiceStatusCmd(),
		newTeamsServiceStartCmd(),
		newTeamsServiceStopCmd(),
		newTeamsServiceRestartCmd(),
		newTeamsServiceWatchdogCmd(),
		newTeamsServiceDoctorCmd(),
		newTeamsServiceLocalSupervisorCmd(),
	)
	return cmd
}

func newTeamsServiceBootstrapCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var yes bool
	var noUAC bool
	var fallbackOnly bool
	var noOpenControl bool
	var noStart bool
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Install or repair the Teams bridge background service",
		Long:  "Install or repair the Teams bridge background service. Bootstrap prepares the Teams control chat, prints the link in this terminal, and tries to open it automatically. On WSL, this first tries a current-user Windows Scheduled Task. If Windows blocks that path, it can ask before opening a UAC prompt and then falls back to a current-user Startup watchdog.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("bootstrap the Teams service", "helper reload now"); err != nil {
				return err
			}
			if dryRun {
				return printTeamsServiceDryRun(cmd.Context(), cmd.OutOrStdout(), registryPath)
			}
			if noStart {
				result, err := bootstrapTeamsService(cmd.Context(), registryPath, teamsServiceBootstrapOptions{
					NoStart: true,
					In:      cmd.InOrStdin(),
					Out:     cmd.OutOrStdout(),
				})
				if err != nil {
					return err
				}
				printTeamsServiceBootstrapReady(cmd.OutOrStdout(), result)
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Service was repaired but not started.")
				return nil
			}
			if err := teamsServiceAuthPreflight(); err != nil {
				return err
			}
			spec, err := buildTeamsServiceSpec(registryPath)
			if err != nil {
				return err
			}
			control, controlErr := bootstrapControlChatWithServiceSpec(cmd.Context(), root, registryPath, spec, !noOpenControl, io.Discard)
			result, err := bootstrapTeamsService(cmd.Context(), registryPath, teamsServiceBootstrapOptions{
				AssumeYes:    yes,
				NoUAC:        noUAC,
				FallbackOnly: fallbackOnly,
				In:           cmd.InOrStdin(),
				Out:          cmd.OutOrStdout(),
			})
			if err != nil {
				return err
			}
			printTeamsServiceBootstrapReady(cmd.OutOrStdout(), result)
			if controlErr != nil {
				printTeamsServiceBootstrapControlChatUnavailable(cmd.OutOrStdout(), controlErr)
			} else {
				printTeamsServiceBootstrapControlChat(cmd.OutOrStdout(), control)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Approve the Windows UAC prompt if Scheduled Task setup needs elevation")
	cmd.Flags().BoolVar(&noUAC, "no-uac", false, "Do not open a Windows UAC prompt if Scheduled Task setup needs elevation")
	cmd.Flags().BoolVar(&fallbackOnly, "fallback-only", false, "Install the current-user Startup watchdog instead of trying Windows Task Scheduler")
	cmd.Flags().BoolVar(&noOpenControl, "no-open-control", false, "Do not try to open the Teams control chat link automatically")
	cmd.Flags().BoolVar(&noStart, "no-start", false, "Repair the service config and enable it without starting the service or preparing the control chat")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Render the service config that would be written without modifying services, tasks, or Teams state")
	return cmd
}

func newTeamsServiceInstallCmd(registryPath *string) *cobra.Command {
	var yes bool
	var noUAC bool
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install the Teams bridge user service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("install the Teams service", "helper reload now"); err != nil {
				return err
			}
			path, err := installTeamsService(cmd.Context(), registryPath, teamsServiceBootstrapOptions{
				AssumeYes: yes,
				NoUAC:     noUAC,
				In:        cmd.InOrStdin(),
				Out:       cmd.OutOrStdout(),
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Installed Teams service config: %s\n", path)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Service was not enabled or started automatically.\n")
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Approve the Windows UAC prompt if Scheduled Task setup needs elevation")
	cmd.Flags().BoolVar(&noUAC, "no-uac", false, "Do not open a Windows UAC prompt if Scheduled Task setup needs elevation")
	return cmd
}

func newTeamsServiceUninstallCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "uninstall",
		Short: "Remove the Teams bridge user service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("uninstall the Teams service", "helper restart now"); err != nil {
				return err
			}
			path, err := uninstallTeamsService(cmd.Context())
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed Teams service config: %s\n", path)
			return nil
		},
	}
}

func newTeamsServiceEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Enable the Teams bridge user service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("enable the Teams service", "helper restart now"); err != nil {
				return err
			}
			if err := teamsServiceAuthPreflight(); err != nil {
				return err
			}
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				return err
			}
			if err := runTeamsServiceCommand(cmd.Context(), cmd.OutOrStdout(), backend, "enable"); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Enabled Teams service: %s\n", backend.Name())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Service was not started automatically.\n")
			return nil
		},
	}
}

func newTeamsServiceDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Disable the Teams bridge user service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("disable the Teams service", "helper restart now"); err != nil {
				return err
			}
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				return err
			}
			if err := runTeamsServiceCommand(cmd.Context(), cmd.OutOrStdout(), backend, "disable"); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Disabled Teams service: %s\n", backend.Name())
			return nil
		},
	}
}

func newTeamsServiceStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the Teams bridge service status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				return err
			}
			return runTeamsServiceCommand(cmd.Context(), cmd.OutOrStdout(), backend, "status")
		},
	}
}

func newTeamsServiceStartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the Teams bridge service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("start the Teams service", "helper restart now"); err != nil {
				return err
			}
			if err := teamsServiceAuthPreflight(); err != nil {
				return err
			}
			if scheduled, err := schedulePendingTeamsServiceActivationBeforeStart(cmd.Context(), cmd.OutOrStdout(), "start"); err != nil {
				return err
			} else if scheduled {
				return nil
			}
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				return err
			}
			if err := startTeamsServiceWithOutput(cmd.Context(), false, cmd.OutOrStdout()); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Started Teams service: %s\n", backend.Name())
			return nil
		},
	}
}

func newTeamsServiceStopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the Teams bridge service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("stop the Teams service", "helper restart now"); err != nil {
				return err
			}
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				return err
			}
			if err := stopTeamsServiceWithOutput(cmd.Context(), cmd.OutOrStdout()); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Stopped Teams service: %s\n", backend.Name())
			return nil
		},
	}
}

func newTeamsServiceRestartCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "restart",
		Short: "Restart the Teams bridge service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("restart the Teams service", "helper restart now"); err != nil {
				return err
			}
			if err := teamsServiceAuthPreflight(); err != nil {
				return err
			}
			if force {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Force recovering Teams state before service restart...")
				summary, err := recoverTeamsStores(cmd.Context(), true, 0)
				if err != nil {
					return fmt.Errorf("force recover Teams state before service restart: %w", err)
				}
				printTeamsRecoverSummary(cmd.OutOrStdout(), summary)
			}
			if scheduled, err := schedulePendingTeamsServiceActivationBeforeStart(cmd.Context(), cmd.OutOrStdout(), "restart"); err != nil {
				return err
			} else if scheduled {
				return nil
			}
			if err := startTeamsServiceWithOutput(cmd.Context(), true, cmd.OutOrStdout()); err != nil {
				return err
			}
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Restarted Teams service: %s\n", backend.Name())
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Recover active Teams state before restarting; may interrupt running work")
	return cmd
}

func schedulePendingTeamsServiceActivationBeforeStart(ctx context.Context, out io.Writer, action string) (bool, error) {
	if teamsServiceGOOS() != "windows" {
		return false, nil
	}
	installPath, err := teamsServiceExecutable()
	if err != nil {
		return false, err
	}
	resolved, resolveErr := helperpath.StableRunnablePathFromSources(installPath, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()})
	if resolveErr != nil {
		return false, resolveErr
	}
	installPath = resolved.Path
	activation, ok, err := discoverTeamsPendingHelperActivation(ctx, installPath, "")
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := scheduleTeamsPendingHelperActivation(ctx, activation); err != nil {
		return false, err
	}
	if out != nil {
		verb := "start"
		if strings.EqualFold(action, "restart") {
			verb = "restart"
		}
		_, _ = fmt.Fprintf(out, "Scheduled Teams service %s after activating pending helper v%s.\n", verb, activation.Version)
	}
	return true, nil
}

func newTeamsServiceDoctorCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Check local Teams service supervisor support",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				return err
			}
			path, err := backend.Path()
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service backend: %s\n", backend.ID())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service name: %s\n", backend.Name())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service config: %s\n", path)
			if exe, err := teamsServiceExecutable(); err != nil {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service executable: unavailable (%v)\n", err)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service raw executable: %s\n", exe)
				if resolved, resolveErr := helperpath.StableRunnablePathFromSources(exe, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()}); resolveErr != nil {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service stable executable: unresolved (%v)\n", resolveErr)
				} else if err := validateTeamsServiceExecutable(resolved.Path); err != nil {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service stable executable: not installable (%v)\n", err)
				} else {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service stable executable: %s\n", resolved.Path)
					if resolved.Recovered || resolved.Source == helperpath.SourceArgv0 {
						_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service path resolution: source=%s reason=%s\n", resolved.Source, resolved.Reason)
					}
				}
			}
			if err := teamsServiceAuthPreflight(); err != nil {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service auth: not ready (%v)\n", err)
			} else {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams service auth: ok")
			}
			if teamsServiceGOOS() == "linux" && teamsServiceIsWSL() {
				if backend.ID() == "wsl-windows-task-scheduler" {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "WSL: detected. Teams service will use a per-user Windows Scheduled Task that launches wsl.exe, so it can survive closing the terminal without root.")
					if err := runTeamsServiceWSLReadinessCheck(cmd.Context(), cmd.OutOrStdout()); err != nil {
						return err
					}
				} else if backend.ID() == "local-supervisor" {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "WSL: local-supervisor was explicitly selected. It survives terminal close and helper crashes inside the running WSL instance, but Windows-login autostart still requires the Windows Scheduled Task backend.")
				} else {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "WSL: detected. systemd --user requires WSL systemd and a running user manager; for Windows-login autostart, unset CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND=systemd.")
				}
			} else if teamsServiceGOOS() == "linux" && backend.ID() == "local-supervisor" {
				if teamsServiceLocalSupervisorSticky() {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Linux: local-supervisor is selected because an enabled local-supervisor config or a verified active local-supervisor status is present.")
				} else {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Linux: systemd --user is not available, so Teams service will use the local supervisor fallback.")
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Linux: local-supervisor survives terminal close and helper crashes, but it cannot guarantee restart after a machine or container reboot.")
			} else if teamsServiceGOOS() == "linux" && backend.ID() == "systemd-user" {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Linux: systemd --user keeps the Teams bridge independent of the terminal while the user manager is alive.")
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Linux: if lingering is disabled, a full logout may stop the user manager; without root/admin policy changes, no user service can guarantee survival after that boundary.")
				if path, ok := teamsServiceDisabledLocalSupervisorConfigPath(); ok {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Linux: disabled local-supervisor config exists but is ignored while systemd --user is selected: %s\n", path)
				}
			}
			return nil
		},
	}
}

func installTeamsService(ctx context.Context, registryPath *string, opts teamsServiceBootstrapOptions) (string, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return "", err
	}
	spec, err := buildTeamsServiceSpec(registryPath)
	if err != nil {
		return "", err
	}
	if err := cleanupTeamsServiceBeforeTaskRewrite(ctx, backend); err != nil {
		if windowsBackend, ok := backend.(teamsServiceWindowsTaskBackend); ok {
			return repairWindowsTaskBackendWithUAC(ctx, windowsBackend, spec, teamsServiceRepairOptions{}, err, opts)
		}
		return "", err
	}
	path, err := backend.Install(ctx, spec)
	if err != nil {
		if windowsBackend, ok := backend.(teamsServiceWindowsTaskBackend); ok {
			return repairWindowsTaskBackendWithUAC(ctx, windowsBackend, spec, teamsServiceRepairOptions{}, err, opts)
		}
		return "", err
	}
	return path, nil
}

func repairTeamsService(ctx context.Context, registryPath *string, opts teamsServiceRepairOptions, buildOptions ...teamsServiceSpecBuildOption) (string, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return "", err
	}
	spec, err := buildTeamsServiceSpec(registryPath, buildOptions...)
	if err != nil {
		return "", err
	}
	if err := cleanupTeamsServiceBeforeTaskRewrite(ctx, backend); err != nil {
		return "", err
	}
	if repairer, ok := backend.(teamsServiceRepairBackend); ok {
		return repairer.Repair(ctx, spec, opts)
	}
	path, err := backend.Install(ctx, spec)
	if err != nil {
		return "", err
	}
	if opts.Enable {
		if err := runTeamsServiceCommand(ctx, io.Discard, backend, "enable"); err != nil {
			return "", err
		}
	}
	if opts.Start {
		if err := runTeamsServiceCommand(ctx, io.Discard, backend, "start"); err != nil {
			return "", err
		}
	}
	return path, nil
}

func cleanupTeamsServiceBeforeTaskRewrite(ctx context.Context, backend teamsServiceBackend) error {
	if _, ok := backend.(teamsServiceWindowsTaskBackend); !ok {
		return nil
	}
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	if _, err := teamsServiceRunPowerShell(ctx, teamsServiceWindowsStopBridgeChildrenPowerShell(task)); err != nil {
		return fmt.Errorf("cleanup old Teams bridge process(es) before Task Scheduler rewrite: %w", err)
	}
	return nil
}

func printTeamsServiceDryRun(ctx context.Context, out io.Writer, registryPath *string) error {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	spec, err := buildTeamsServiceSpec(registryPath)
	if err != nil {
		return err
	}
	path, err := backend.Path()
	if err != nil {
		return err
	}
	if out == nil {
		out = io.Discard
	}
	_, _ = fmt.Fprintf(out, "Teams service dry-run: backend=%s name=%s path=%s\n", backend.ID(), backend.Name(), path)
	_, _ = fmt.Fprintf(out, "Executable: %s\n", spec.Executable)
	_, _ = fmt.Fprintf(out, "WorkingDirectory: %s\n", spec.WorkingDir)
	if spec.RegistryPath != "" {
		_, _ = fmt.Fprintf(out, "Registry: %s\n", spec.RegistryPath)
	}
	switch typed := backend.(type) {
	case teamsServiceSystemdBackend:
		_, _ = fmt.Fprintln(out, "--- systemd main unit ---")
		_, _ = fmt.Fprint(out, buildTeamsServiceUnit(spec))
		_, _ = fmt.Fprintln(out, "--- systemd watchdog unit ---")
		_, _ = fmt.Fprint(out, buildTeamsServiceWatchdogUnit(spec))
	case teamsServiceLaunchAgentBackend:
		_, _ = fmt.Fprintln(out, "--- launchd main plist ---")
		_, _ = fmt.Fprint(out, buildTeamsServiceLaunchAgentPlist(spec))
		_, _ = fmt.Fprintln(out, "--- launchd watchdog plist ---")
		_, _ = fmt.Fprint(out, buildTeamsServiceWatchdogLaunchAgentPlist(spec))
	case teamsServiceWindowsTaskBackend:
		_, _ = fmt.Fprintln(out, "--- windows task xml ---")
		_, _ = fmt.Fprint(out, buildTeamsServiceWindowsTaskXML(spec))
		_, _ = fmt.Fprintln(out, "--- windows watchdog task xml ---")
		_, _ = fmt.Fprint(out, buildTeamsServiceWindowsWatchdogTaskXML(spec))
	case teamsServiceWSLWindowsTaskBackend:
		args := buildTeamsServiceWSLArguments(spec)
		watchdogArgs := buildTeamsServiceWSLWatchdogArguments(spec)
		_, _ = fmt.Fprintln(out, "--- wsl task config ---")
		_, _ = fmt.Fprint(out, buildTeamsServiceWSLTaskConfig(typed.Name(), args))
		_, _ = fmt.Fprintln(out, "--- wsl watchdog task config ---")
		_, _ = fmt.Fprint(out, buildTeamsServiceWSLTaskConfig(typed.watchdogName(), watchdogArgs))
	case teamsServiceLocalSupervisorBackend:
		_, _ = fmt.Fprintln(out, "--- local supervisor config ---")
		_, _ = fmt.Fprint(out, renderTeamsServiceLocalSupervisorConfig(spec, false))
	default:
		_, _ = fmt.Fprintln(out, "Rendered config preview is not available for this backend.")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type teamsServiceBootstrapOptions struct {
	AssumeYes    bool
	NoUAC        bool
	FallbackOnly bool
	NoStart      bool
	In           io.Reader
	Out          io.Writer
}

type teamsServiceBootstrapResult struct {
	Mode string
	Path string
}

type teamsServiceBootstrapControlChatResult struct {
	URL     string
	Topic   string
	ChatID  string
	Opened  bool
	OpenErr error
}

func defaultTeamsServiceBootstrapControlChat(ctx context.Context, root *rootOptions, registryPath *string, openControl bool, errOut io.Writer) (teamsServiceBootstrapControlChatResult, error) {
	httpClient, err := newTeamsGraphHTTPClientLease(ctx, root, errOut)
	if err != nil {
		return teamsServiceBootstrapControlChatResult{}, err
	}
	defer func() { _ = httpClient.Close(context.Background()) }()
	auth, err := newTeamsAuthManagerWithHTTPClient(httpClient.Client)
	if err != nil {
		return teamsServiceBootstrapControlChatResult{}, err
	}
	var registry string
	if registryPath != nil {
		registry = *registryPath
	}
	bridge, err := teams.NewBridgeWithHTTPClient(ctx, auth, registry, io.Discard, httpClient.Client)
	if err != nil {
		return teamsServiceBootstrapControlChatResult{}, err
	}
	httpClient.RetireSuspects(ctx, errOut)
	chat, err := bridge.EnsureControlChat(ctx)
	if err != nil {
		return teamsServiceBootstrapControlChatResult{}, err
	}
	if err := bridge.Save(); err != nil {
		return teamsServiceBootstrapControlChatResult{}, err
	}
	result := teamsServiceBootstrapControlChatResult{
		URL:    strings.TrimSpace(chat.WebURL),
		Topic:  strings.TrimSpace(chat.Topic),
		ChatID: strings.TrimSpace(chat.ID),
	}
	if openControl && result.URL != "" {
		if err := teamsServiceOpenURL(ctx, result.URL); err != nil {
			result.OpenErr = err
		} else {
			result.Opened = true
		}
	}
	return result, nil
}

func bootstrapControlChatWithServiceSpec(ctx context.Context, root *rootOptions, registryPath *string, spec teamsServiceSpec, openControl bool, errOut io.Writer) (teamsServiceBootstrapControlChatResult, error) {
	controlRegistryPath := registryPath
	if strings.TrimSpace(spec.RegistryPath) != "" {
		registry := spec.RegistryPath
		controlRegistryPath = &registry
	}
	return withTeamsServiceSpecEnvironment(spec.Environment, func() (teamsServiceBootstrapControlChatResult, error) {
		return teamsServiceBootstrapControlChat(ctx, root, controlRegistryPath, openControl, errOut)
	})
}

type teamsServiceEnvValue struct {
	value string
	ok    bool
}

func withTeamsServiceSpecEnvironment(env map[string]string, fn func() (teamsServiceBootstrapControlChatResult, error)) (teamsServiceBootstrapControlChatResult, error) {
	keys := sortedEnvironmentKeys(env)
	if len(keys) == 0 {
		return fn()
	}
	previous := make(map[string]teamsServiceEnvValue, len(keys))
	for _, key := range keys {
		value, ok := os.LookupEnv(key)
		previous[key] = teamsServiceEnvValue{value: value, ok: ok}
		if err := os.Setenv(key, env[key]); err != nil {
			restoreTeamsServiceEnvironment(previous)
			return teamsServiceBootstrapControlChatResult{}, fmt.Errorf("prepare Teams service environment for control chat: %w", err)
		}
	}
	defer restoreTeamsServiceEnvironment(previous)
	return fn()
}

func restoreTeamsServiceEnvironment(previous map[string]teamsServiceEnvValue) {
	for key, value := range previous {
		if value.ok {
			_ = os.Setenv(key, value.value)
		} else {
			_ = os.Unsetenv(key)
		}
	}
}

func printTeamsServiceBootstrapControlChat(out io.Writer, result teamsServiceBootstrapControlChatResult) {
	if out == nil || strings.TrimSpace(firstNonEmptyCLI(result.URL, result.ChatID)) == "" {
		return
	}
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, "NEXT STEP: OPEN THE TEAMS CONTROL CHAT")
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "Open:")
	_, _ = fmt.Fprintln(out, firstNonEmptyCLI(result.URL, result.ChatID))
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "Then send:")
	_, _ = fmt.Fprintln(out, "help")
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out)
}

func printTeamsServiceBootstrapControlChatUnavailable(out io.Writer, err error) {
	if out == nil {
		return
	}
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, "NEXT STEP: PRINT THE TEAMS CONTROL CHAT LINK")
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, "The service was repaired, but the control chat link was not ready.")
	_, _ = fmt.Fprintln(out, "Reason: "+teamsServiceBootstrapErrorSummary(err))
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "Run:")
	_, _ = fmt.Fprintln(out, "codex-proxy teams control")
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "Then open the printed link and send:")
	_, _ = fmt.Fprintln(out, "help")
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out)
}

func printTeamsServiceBootstrapReady(out io.Writer, result teamsServiceBootstrapResult) {
	if out == nil {
		return
	}
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, "BOOTSTRAP COMPLETE")
	_, _ = fmt.Fprintln(out, "============================================================")
	if result.Mode == "windows-pending-helper-activation" {
		_, _ = fmt.Fprintln(out, "Teams helper update activation scheduled.")
		_, _ = fmt.Fprintln(out, "The service will start after the staged helper replaces the old entry.")
	} else {
		_, _ = fmt.Fprintf(out, "Teams service bootstrap ready: %s\n", result.Mode)
		if strings.HasPrefix(result.Mode, teamsServiceLocalSupervisorID) {
			_, _ = fmt.Fprintln(out, "Linux local-supervisor survives terminal close and helper crashes, but it cannot guarantee restart after a machine or container reboot.")
		}
	}
	if strings.TrimSpace(result.Path) != "" {
		_, _ = fmt.Fprintf(out, "Teams service config: %s\n", result.Path)
	}
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out)
}

func repairWindowsTaskBackendWithUACForBootstrap(ctx context.Context, backend teamsServiceWindowsTaskBackend, spec teamsServiceSpec, repairOpts teamsServiceRepairOptions, failure error, opts teamsServiceBootstrapOptions) (string, bool, error) {
	path, err := repairWindowsTaskBackendWithUAC(ctx, backend, spec, repairOpts, failure, opts)
	return path, err == nil, err
}

func repairWindowsTaskBackendWithUAC(ctx context.Context, backend teamsServiceWindowsTaskBackend, spec teamsServiceSpec, repairOpts teamsServiceRepairOptions, failure error, opts teamsServiceBootstrapOptions) (string, error) {
	if !isTeamsServiceWindowsAccessDeniedError(failure) {
		return "", failure
	}
	if opts.NoUAC {
		return "", fmt.Errorf("Windows Scheduled Task setup failed: Windows denied permission to create or repair the Scheduled Task, and UAC is disabled by --no-uac")
	}
	out := opts.Out
	if out == nil {
		out = io.Discard
	}
	if !confirmTeamsServiceUACPrompt(opts.In, out, opts.AssumeYes) {
		return "", fmt.Errorf("Windows Scheduled Task setup failed: %s", teamsServiceBootstrapErrorSummary(failure))
	}
	principalUser, userErr := teamsServiceCurrentWindowsUser(ctx)
	if userErr != nil {
		return "", fmt.Errorf("could not identify the current Windows user for UAC setup: %w", userErr)
	}
	path, elevatedErr := backend.RepairElevated(ctx, spec, repairOpts, principalUser)
	if elevatedErr != nil {
		return "", fmt.Errorf("UAC Scheduled Task setup failed: %s", teamsServiceBootstrapErrorSummary(elevatedErr))
	}
	return path, nil
}

func bootstrapTeamsService(ctx context.Context, registryPath *string, opts teamsServiceBootstrapOptions) (teamsServiceBootstrapResult, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return teamsServiceBootstrapResult{}, err
	}
	spec, err := buildTeamsServiceSpec(registryPath)
	if err != nil {
		return teamsServiceBootstrapResult{}, err
	}
	if activation, ok, err := discoverTeamsPendingHelperActivation(ctx, spec.Executable, ""); err != nil {
		return teamsServiceBootstrapResult{}, fmt.Errorf("check pending helper activation: %w", err)
	} else if ok {
		usedUAC := false
		path, err := repairTeamsService(ctx, registryPath, teamsServiceRepairOptions{Enable: true, Start: false})
		if err != nil {
			if windowsBackend, ok := backend.(teamsServiceWindowsTaskBackend); ok {
				path, usedUAC, err = repairWindowsTaskBackendWithUACForBootstrap(ctx, windowsBackend, spec, teamsServiceRepairOptions{Enable: true, Start: false}, err, opts)
			}
			if err != nil {
				return teamsServiceBootstrapResult{}, fmt.Errorf("repair Teams service before pending helper activation: %w", err)
			}
		}
		if opts.NoStart {
			mode := backend.ID() + "-no-start"
			if usedUAC {
				mode = "windows-task-scheduler-uac-no-start"
			}
			return teamsServiceBootstrapResult{Mode: mode, Path: path}, nil
		}
		if err := scheduleTeamsPendingHelperActivation(ctx, activation); err != nil {
			return teamsServiceBootstrapResult{}, fmt.Errorf("schedule pending helper activation: %w", err)
		}
		mode := "windows-pending-helper-activation"
		if usedUAC {
			mode = "windows-pending-helper-activation-uac"
		}
		return teamsServiceBootstrapResult{Mode: mode, Path: path}, nil
	}
	if opts.NoStart {
		path, err := repairTeamsService(ctx, registryPath, teamsServiceRepairOptions{Enable: true, Start: false})
		if err != nil {
			if windowsBackend, ok := backend.(teamsServiceWindowsTaskBackend); ok {
				if elevatedPath, _, elevatedErr := repairWindowsTaskBackendWithUACForBootstrap(ctx, windowsBackend, spec, teamsServiceRepairOptions{Enable: true, Start: false}, err, opts); elevatedErr == nil {
					return teamsServiceBootstrapResult{Mode: "windows-task-scheduler-uac-no-start", Path: elevatedPath}, nil
				} else {
					err = elevatedErr
				}
			}
		}
		return teamsServiceBootstrapResult{Mode: backend.ID() + "-no-start", Path: path}, err
	}
	if _, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
		if _, err := teamsServiceRetireLocalDuplicateProcesses(ctx, spec); err != nil {
			return teamsServiceBootstrapResult{}, fmt.Errorf("could not stop old local Teams helper process(es) before bootstrap: %w", err)
		}
	}
	if opts.FallbackOnly {
		if wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
			if err := wslBackend.RetireScheduledTasks(ctx); err != nil {
				if !wslBackend.canSkipScheduledTaskRetireForStartupFallback(err) {
					return teamsServiceBootstrapResult{}, fmt.Errorf("--fallback-only cannot safely start the Windows Startup watchdog because old WSL Scheduled Tasks could not be disabled: %w", err)
				}
			}
			path, err := wslBackend.InstallStartupFallback(ctx, spec, true)
			return teamsServiceBootstrapResult{Mode: "wsl-startup-watchdog", Path: path}, err
		}
		return teamsServiceBootstrapResult{}, fmt.Errorf("--fallback-only is only supported by the WSL Windows service backend")
	}
	path, err := repairTeamsService(ctx, registryPath, teamsServiceRepairOptions{Enable: true, Start: true})
	if err == nil {
		if wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
			if postRepairErr := wslBackend.VerifyAndCleanAfterRepair(ctx, spec); postRepairErr != nil {
				err = postRepairErr
			} else {
				return teamsServiceBootstrapResult{Mode: backend.ID(), Path: path}, nil
			}
		} else {
			return teamsServiceBootstrapResult{Mode: backend.ID(), Path: path}, nil
		}
	}
	if windowsBackend, ok := backend.(teamsServiceWindowsTaskBackend); ok {
		path, _, elevatedErr := repairWindowsTaskBackendWithUACForBootstrap(ctx, windowsBackend, spec, teamsServiceRepairOptions{Enable: true, Start: true}, err, opts)
		if elevatedErr != nil {
			return teamsServiceBootstrapResult{}, elevatedErr
		}
		return teamsServiceBootstrapResult{Mode: "windows-task-scheduler-uac", Path: path}, nil
	}
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok {
		return teamsServiceBootstrapResult{}, err
	}
	accessDenied := isTeamsServiceWindowsAccessDeniedError(err)
	if !accessDenied && !opts.NoUAC {
		if isTeamsServiceWindowsScheduledTasksUnavailableError(err) && wslBackend.canSkipScheduledTaskRetireForStartupFallback(err) {
			fallbackReason := "Windows Scheduled Task setup could not be completed because Windows Scheduled Task cmdlets are unavailable: " + teamsServiceBootstrapErrorSummary(err)
			printTeamsServiceBootstrapTaskFallback(opts.Out, fallbackReason)
			path, fallbackErr := wslBackend.InstallStartupFallback(ctx, spec, true)
			if fallbackErr != nil {
				return teamsServiceBootstrapResult{}, fmt.Errorf("Windows Startup watchdog fallback failed after Scheduled Task setup failed (%s): %s", teamsServiceBootstrapErrorSummary(err), teamsServiceBootstrapErrorSummary(fallbackErr))
			}
			return teamsServiceBootstrapResult{Mode: "wsl-startup-watchdog", Path: path}, nil
		}
		return teamsServiceBootstrapResult{}, fmt.Errorf("Windows Scheduled Task setup failed: %s", teamsServiceBootstrapErrorSummary(err))
	}
	out := opts.Out
	if out == nil {
		out = io.Discard
	}
	fallbackReason := ""
	uacConfirmed := false
	if accessDenied && !opts.NoUAC && confirmTeamsServiceUACPrompt(opts.In, out, opts.AssumeYes) {
		uacConfirmed = true
		principalUser, userErr := teamsServiceCurrentWindowsUser(ctx)
		if userErr != nil {
			fallbackReason = "Could not identify the current Windows user for UAC setup: " + teamsServiceBootstrapErrorSummary(userErr)
		} else {
			if path, elevateErr := wslBackend.RepairElevated(ctx, spec, teamsServiceRepairOptions{Enable: true, Start: true}, principalUser); elevateErr == nil {
				if postRepairErr := wslBackend.VerifyAndCleanAfterRepair(ctx, spec); postRepairErr != nil {
					return teamsServiceBootstrapResult{}, fmt.Errorf("elevated Windows Scheduled Task setup completed, but cleanup or verification failed: %w", postRepairErr)
				}
				return teamsServiceBootstrapResult{Mode: "wsl-windows-task-scheduler-uac", Path: path}, nil
			} else {
				fallbackReason = "UAC Scheduled Task setup failed: " + teamsServiceBootstrapErrorSummary(elevateErr)
			}
		}
	} else {
		fallbackReason = "Windows Scheduled Task setup could not be completed: " + teamsServiceBootstrapErrorSummary(err)
		if accessDenied && opts.NoUAC {
			fallbackReason = "Windows Scheduled Task setup could not be completed: Windows denied permission to create or repair the Scheduled Task, and UAC is disabled by --no-uac."
		}
	}
	if retireErr := wslBackend.RetireScheduledTasks(ctx); retireErr != nil {
		if isTeamsServiceWindowsScheduledTasksUnavailableError(err) && wslBackend.canSkipScheduledTaskRetireForStartupFallback(retireErr) {
			if strings.TrimSpace(fallbackReason) != "" {
				fallbackReason += " Windows Scheduled Task cmdlets are unavailable and no Teams Scheduled Task config was found, so installing the Startup fallback without task retirement."
			}
		} else if uacConfirmed {
			if elevatedRetireErr := wslBackend.RetireScheduledTasksElevated(ctx); elevatedRetireErr != nil {
				return teamsServiceBootstrapResult{}, fmt.Errorf("Windows Startup watchdog fallback is unsafe because old WSL Scheduled Tasks could not be disabled after Scheduled Task setup failed (%s): normal cleanup failed: %s; elevated cleanup failed: %s", teamsServiceBootstrapErrorSummary(err), teamsServiceBootstrapErrorSummary(retireErr), teamsServiceBootstrapErrorSummary(elevatedRetireErr))
			}
			if strings.TrimSpace(fallbackReason) != "" {
				fallbackReason += " Old WSL Scheduled Tasks were disabled using Windows permission before installing the fallback."
			}
		} else {
			return teamsServiceBootstrapResult{}, fmt.Errorf("Windows Startup watchdog fallback is unsafe because old WSL Scheduled Tasks could not be disabled after Scheduled Task setup failed (%s): %s", teamsServiceBootstrapErrorSummary(err), teamsServiceBootstrapErrorSummary(retireErr))
		}
	}
	printTeamsServiceBootstrapTaskFallback(out, fallbackReason)
	path, fallbackErr := wslBackend.InstallStartupFallback(ctx, spec, true)
	if fallbackErr == nil {
		_ = wslBackend.removeTaskConfig()
	}
	if fallbackErr != nil {
		return teamsServiceBootstrapResult{}, fmt.Errorf("Windows Startup watchdog fallback failed after Scheduled Task setup failed (%s): %s", teamsServiceBootstrapErrorSummary(err), teamsServiceBootstrapErrorSummary(fallbackErr))
	}
	return teamsServiceBootstrapResult{Mode: "wsl-startup-watchdog", Path: path}, nil
}

func uninstallTeamsService(ctx context.Context) (string, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return "", err
	}
	path, err := backend.Uninstall(ctx)
	if err != nil {
		return "", err
	}
	if backend.ID() != teamsServiceLocalSupervisorID {
		if cleanupErr := removeDisabledTeamsServiceLocalSupervisorConfigIfInactive(); cleanupErr != nil {
			return "", cleanupErr
		}
	}
	return path, nil
}

type teamsServiceSpec struct {
	Executable                  string
	WorkingDir                  string
	RegistryPath                string
	Environment                 map[string]string
	WindowsTaskLauncherPath     string
	WindowsWatchdogLauncherPath string
}

type teamsServiceSpecBuildOptions struct {
	Environment map[string]string
}

type teamsServiceSpecBuildOption func(*teamsServiceSpecBuildOptions)

func teamsServiceSpecEnvironmentOverrides(env map[string]string) teamsServiceSpecBuildOption {
	return func(opts *teamsServiceSpecBuildOptions) {
		if len(env) == 0 {
			return
		}
		if opts.Environment == nil {
			opts.Environment = make(map[string]string, len(env))
		}
		for key, value := range env {
			opts.Environment[key] = value
		}
	}
}

type teamsServiceBackend interface {
	ID() string
	Name() string
	Path() (string, error)
	Install(ctx context.Context, spec teamsServiceSpec) (string, error)
	Uninstall(ctx context.Context) (string, error)
	Run(ctx context.Context, action string) ([]byte, error)
	Installed() (bool, error)
	Active(ctx context.Context) (bool, error)
}

type teamsServiceRepairOptions struct {
	Enable bool
	Start  bool
}

type teamsServiceRepairBackend interface {
	Repair(ctx context.Context, spec teamsServiceSpec, opts teamsServiceRepairOptions) (string, error)
}

func teamsServiceBackendForCurrentPlatform() (teamsServiceBackend, error) {
	switch teamsServiceGOOS() {
	case "linux":
		if teamsServiceIsWSL() {
			mode, source := teamsServiceWSLBackendMode()
			switch mode {
			case "", "auto":
				if teamsServiceLocalSupervisorSticky() {
					return teamsServiceLocalSupervisorBackend{}, nil
				}
				return teamsServiceWSLWindowsTaskBackend{}, nil
			case "windows", "windows-task", "windows-task-scheduler", "wsl-windows-task-scheduler":
				return teamsServiceWSLWindowsTaskBackend{}, nil
			case "systemd", "systemd-user":
				return teamsServiceSystemdBackend{}, nil
			case "local", "local-supervisor":
				return teamsServiceLocalSupervisorBackend{}, nil
			default:
				if source == "" {
					source = "CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND"
				}
				return nil, fmt.Errorf("unsupported WSL Teams service backend %q from %s: use auto, windows-task, systemd, or local-supervisor", mode, source)
			}
		}
		switch teamsServiceLinuxBackendMode() {
		case "", "auto":
			if teamsServiceLocalSupervisorSticky() {
				return teamsServiceLocalSupervisorBackend{}, nil
			}
			available, err := teamsServiceSystemdUserAvailable(context.Background())
			if err == nil && available {
				return teamsServiceSystemdBackend{}, nil
			}
			if err != nil && !teamsServiceSystemdUserUnavailableError(err) {
				return nil, fmt.Errorf("verify systemd --user availability for Linux Teams service auto backend: %w", err)
			}
			return teamsServiceLocalSupervisorBackend{}, nil
		case "systemd", "systemd-user":
			return teamsServiceSystemdBackend{}, nil
		case "local", "local-supervisor":
			return teamsServiceLocalSupervisorBackend{}, nil
		default:
			return nil, fmt.Errorf("unsupported Linux Teams service backend %q: use systemd, local-supervisor, or auto", teamsServiceLinuxBackendMode())
		}
	case "darwin":
		return teamsServiceLaunchAgentBackend{}, nil
	case "windows":
		return teamsServiceWindowsTaskBackend{}, nil
	default:
		return nil, fmt.Errorf("unsupported platform %q: Teams service management supports Linux systemd --user, macOS LaunchAgent, and Windows per-user Task Scheduler", teamsServiceGOOS())
	}
}

func teamsServiceWSLBackendMode() (string, string) {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND")))
	if mode != "" {
		return mode, "CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND"
	}
	if linuxMode := teamsServiceLinuxBackendMode(); linuxMode != "" {
		return linuxMode, "CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND"
	}
	return "", ""
}

func teamsServiceLinuxBackendMode() string {
	return strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND")))
}

func defaultTeamsServiceSystemdUserAvailable(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if data, err := teamsServiceRunSystemctl(checkCtx, "show-environment"); err != nil {
		if detail := strings.TrimSpace(string(data)); detail != "" {
			return false, fmt.Errorf("%w: %s", err, detail)
		}
		return false, err
	}
	return true, nil
}

func runTeamsServiceCommand(ctx context.Context, out io.Writer, backend teamsServiceBackend, action string) error {
	data, err := backend.Run(ctx, action)
	if len(data) > 0 {
		_, _ = out.Write(data)
		if !bytes.HasSuffix(data, []byte("\n")) {
			_, _ = fmt.Fprintln(out)
		}
	}
	return err
}

func runTeamsServiceWSLReadinessCheck(ctx context.Context, out io.Writer) error {
	command := "Get-Command wsl.exe -ErrorAction Stop | Out-Null; Get-Command Get-ScheduledTask -ErrorAction Stop | Out-Null"
	if _, err := teamsServiceRunPowerShell(ctx, command); err != nil {
		return fmt.Errorf("WSL Windows Scheduled Task readiness check failed: %w", err)
	}
	if out != nil {
		_, _ = fmt.Fprintln(out, "WSL supervisor readiness: powershell.exe, wsl.exe, and ScheduledTask cmdlets are available.")
	}
	return nil
}

func defaultTeamsServiceAuthPreflight() error {
	readCfg, err := teams.DefaultEffectiveReadAuthConfig()
	if err != nil {
		return err
	}
	readStatus, err := teams.TokenCacheStatus(readCfg.CachePath)
	if err != nil {
		return err
	}
	switch readStatus {
	case "missing", "empty", "present, access token expired":
		return fmt.Errorf("Teams read auth cache is not ready for background service (%s at %s); run `%s` in a foreground terminal first", readStatus, readCfg.CachePath, teamsAuthCommandForCache(readCfg.CachePath, "codex-proxy teams auth read"))
	}
	writeCfg, err := teams.DefaultEffectiveAuthConfig()
	if err != nil {
		return err
	}
	writeStatus, err := teams.TokenCacheStatus(writeCfg.CachePath)
	if err != nil {
		return err
	}
	switch writeStatus {
	case "missing", "empty", "present, access token expired":
		return fmt.Errorf("Teams write auth cache is not ready for background service (%s at %s); run `%s` in a foreground terminal first", writeStatus, writeCfg.CachePath, teamsAuthCommandForCache(writeCfg.CachePath, "codex-proxy teams auth"))
	}
	return nil
}

func ensureTeamsServiceForRun(ctx context.Context, registryPath *string, buildOptions ...teamsServiceSpecBuildOption) error {
	if runningInsideTeamsCodexChild() {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_AUTO_SERVICE")), "0") ||
		strings.EqualFold(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_AUTO_SERVICE")), "false") {
		return nil
	}
	if teamsServiceGOOS() != "linux" || !teamsServiceIsWSL() {
		return nil
	}
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	if backend.ID() != "wsl-windows-task-scheduler" {
		return nil
	}
	if err := teamsServiceAuthPreflight(); err != nil {
		return err
	}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) != "" {
		return nil
	}
	if wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
		if installed, err := wslBackend.StartupFallbackMarkerExists(); err == nil && installed {
			return nil
		}
	}
	start := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) == ""
	_, err = repairTeamsService(ctx, registryPath, teamsServiceRepairOptions{Enable: true, Start: start}, buildOptions...)
	if err != nil {
		wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
		if ok {
			spec, specErr := buildTeamsServiceSpec(registryPath, buildOptions...)
			if specErr != nil {
				return specErr
			}
			if retireErr := wslBackend.RetireScheduledTasks(ctx); retireErr != nil {
				if !isTeamsServiceWindowsScheduledTasksUnavailableError(err) || !wslBackend.canSkipScheduledTaskRetireForStartupFallback(retireErr) {
					return fmt.Errorf("WSL Scheduled Task setup failed (%v), and Startup fallback is unsafe because old WSL Scheduled Tasks could not be disabled: %w", err, retireErr)
				}
			}
			if _, fallbackErr := wslBackend.InstallStartupFallback(ctx, spec, false); fallbackErr != nil {
				return fmt.Errorf("WSL Scheduled Task setup was blocked (%v), and Startup fallback setup failed: %w", err, fallbackErr)
			}
			_ = wslBackend.removeTaskConfig()
			if isTeamsServiceWindowsAccessDeniedError(err) {
				return fmt.Errorf("WSL Scheduled Task setup was blocked by Windows policy; installed the current-user Startup watchdog fallback")
			}
			return fmt.Errorf("WSL Scheduled Task setup failed (%v); installed the current-user Startup watchdog fallback", err)
		}
	}
	return err
}

func teamsServiceInstalled() (bool, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return false, err
	}
	return backend.Installed()
}

func teamsServiceActive(ctx context.Context) (bool, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return false, err
	}
	return backend.Active(ctx)
}

func stopTeamsService(ctx context.Context) error {
	return stopTeamsServiceWithOutput(ctx, io.Discard)
}

func stopTeamsServiceWithOutput(ctx context.Context, out io.Writer) error {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	if err := stopTeamsServiceRawWithOutput(ctx, backend, out); err != nil {
		return err
	}
	if _, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
		return retireWSLTeamsBridgeProcesses(ctx, nil)
	}
	return nil
}

func stopTeamsServiceRaw(ctx context.Context, backend teamsServiceBackend) error {
	return stopTeamsServiceRawWithOutput(ctx, backend, io.Discard)
}

func stopTeamsServiceRawWithOutput(ctx context.Context, backend teamsServiceBackend, out io.Writer) error {
	if backend == nil {
		return fmt.Errorf("Teams service backend is required")
	}
	if out == nil {
		out = io.Discard
	}
	return runTeamsServiceCommand(ctx, out, backend, "stop")
}

func startTeamsService(ctx context.Context, restart bool) error {
	return startTeamsServiceWithOutput(ctx, restart, io.Discard)
}

func startTeamsServiceWithOutput(ctx context.Context, restart bool, out io.Writer) error {
	if out == nil {
		out = io.Discard
	}
	action := "start"
	if restart {
		action = "restart"
	}
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	if _, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
		if restart {
			if err := runTeamsServiceCommand(ctx, out, backend, "stop"); err != nil {
				return err
			}
			if err := retireWSLTeamsBridgeProcesses(ctx, nil); err != nil {
				return err
			}
			return runTeamsServiceCommand(ctx, out, backend, "start")
		}
		active, activeErr := backend.Active(ctx)
		if activeErr != nil {
			return activeErr
		}
		if active {
			if teamsServiceWSLStartupFallbackInstalled(backend) {
				return nil
			}
		} else {
			if err := retireWSLTeamsBridgeProcesses(ctx, nil); err != nil {
				return err
			}
		}
	}
	return runTeamsServiceCommand(ctx, out, backend, action)
}

type teamsServicePrimaryRunner interface {
	RunPrimary(ctx context.Context, action string) ([]byte, error)
}

func startTeamsPrimaryService(ctx context.Context, restart bool) error {
	action := "start"
	if restart {
		action = "restart"
	}
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	if primary, ok := backend.(teamsServicePrimaryRunner); ok {
		if _, wsl := backend.(teamsServiceWSLWindowsTaskBackend); wsl {
			if restart {
				if _, err := primary.RunPrimary(ctx, "stop"); err != nil {
					return err
				}
				if err := retireWSLTeamsBridgeProcesses(ctx, nil); err != nil {
					return err
				}
				_, err := primary.RunPrimary(ctx, "start")
				return err
			}
			active, activeErr := backend.Active(ctx)
			if activeErr != nil {
				return activeErr
			}
			if active {
				if teamsServiceWSLStartupFallbackInstalled(backend) {
					return nil
				}
			} else {
				if err := retireWSLTeamsBridgeProcesses(ctx, nil); err != nil {
					return err
				}
			}
		}
		_, err := primary.RunPrimary(ctx, action)
		return err
	}
	return runTeamsServiceCommand(ctx, io.Discard, backend, action)
}

func teamsServiceWSLStartupFallbackInstalled(backend teamsServiceBackend) bool {
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok {
		return false
	}
	installed, err := wslBackend.startupFallbackMarkerInstalled()
	return err == nil && installed
}

func retireWSLTeamsBridgeProcesses(ctx context.Context, registryPath *string) error {
	spec, err := buildTeamsServiceSpec(registryPath)
	if err != nil {
		return err
	}
	if _, err := teamsServiceRetireLocalBridgeProcesses(ctx, spec); err != nil {
		return fmt.Errorf("could not stop old local Teams bridge process(es): %w", err)
	}
	return nil
}

type teamsServiceSystemdBackend struct{}

func (teamsServiceSystemdBackend) ID() string {
	return "systemd-user"
}

func (teamsServiceSystemdBackend) Name() string {
	return teamsServiceUnitName
}

func (teamsServiceSystemdBackend) Path() (string, error) {
	return teamsServiceUnitPath()
}

func (b teamsServiceSystemdBackend) Install(ctx context.Context, spec teamsServiceSpec) (string, error) {
	unitPath, err := b.Path()
	if err != nil {
		return "", err
	}
	unit := buildTeamsServiceUnit(spec)
	if err := os.MkdirAll(filepath.Dir(unitPath), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(unitPath, []byte(unit), 0o600); err != nil {
		return "", err
	}
	watchdogUnitPath, err := teamsServiceWatchdogUnitPath()
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(watchdogUnitPath, []byte(buildTeamsServiceWatchdogUnit(spec)), 0o600); err != nil {
		return "", err
	}
	watchdogTimerPath, err := teamsServiceWatchdogTimerPath()
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(watchdogTimerPath, []byte(buildTeamsServiceWatchdogTimer()), 0o600); err != nil {
		return "", err
	}
	if _, err := teamsServiceRunSystemctl(ctx, "daemon-reload"); err != nil {
		return "", err
	}
	return unitPath, nil
}

func (b teamsServiceSystemdBackend) Uninstall(ctx context.Context) (string, error) {
	unitPath, err := b.Path()
	if err != nil {
		return "", err
	}
	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if watchdogUnitPath, err := teamsServiceWatchdogUnitPath(); err == nil {
		if err := os.Remove(watchdogUnitPath); err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}
	if watchdogTimerPath, err := teamsServiceWatchdogTimerPath(); err == nil {
		if err := os.Remove(watchdogTimerPath); err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}
	if _, err := teamsServiceRunSystemctl(ctx, "daemon-reload"); err != nil {
		return "", err
	}
	return unitPath, nil
}

func (teamsServiceSystemdBackend) Run(ctx context.Context, action string) ([]byte, error) {
	switch action {
	case "enable", "disable", "start", "restart":
		return teamsServiceRunSystemctl(ctx, action, teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName)
	case "stop":
		return teamsServiceSystemdStop(ctx)
	case "status":
		main, err := teamsServiceRunSystemctl(ctx, "status", "--no-pager", teamsServiceUnitName)
		watchdog, watchdogErr := teamsServiceRunSystemctl(ctx, "status", "--no-pager", teamsServiceWatchdogUnitName)
		timer, timerErr := teamsServiceRunSystemctl(ctx, "status", "--no-pager", teamsServiceWatchdogTimerName)
		out := append([]byte{}, main...)
		if len(out) > 0 && !bytes.HasSuffix(out, []byte("\n")) {
			out = append(out, '\n')
		}
		out = append(out, watchdog...)
		if len(out) > 0 && !bytes.HasSuffix(out, []byte("\n")) {
			out = append(out, '\n')
		}
		out = append(out, timer...)
		if err != nil {
			return out, err
		}
		if watchdogErr != nil {
			return out, watchdogErr
		}
		return out, timerErr
	default:
		return nil, fmt.Errorf("unsupported Teams service action for systemd: %s", action)
	}
}

func teamsServiceSystemdStop(ctx context.Context) ([]byte, error) {
	var out []byte
	for _, unit := range []string{teamsServiceWatchdogTimerName, teamsServiceWatchdogUnitName} {
		data, err := teamsServiceRunSystemctl(ctx, "stop", unit)
		if err != nil {
			if teamsServiceSystemdUnitMissingError(err, data) {
				continue
			}
			return appendLaunchctlOutput(out, data), err
		}
		out = appendLaunchctlOutput(out, data)
	}
	data, err := teamsServiceRunSystemctl(ctx, "stop", teamsServiceUnitName)
	return appendLaunchctlOutput(out, data), err
}

func teamsServiceSystemdUnitMissingError(err error, data []byte) bool {
	if err == nil {
		return false
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 5 {
		return true
	}
	text := strings.ToLower(string(data) + "\n" + err.Error())
	return strings.Contains(text, "exit status 5") ||
		strings.Contains(text, "unit not found") ||
		strings.Contains(text, "could not be found") ||
		strings.Contains(text, "not loaded")
}

func (teamsServiceSystemdBackend) RunPrimary(ctx context.Context, action string) ([]byte, error) {
	switch action {
	case "start", "restart", "stop", "enable", "disable":
		return teamsServiceRunSystemctl(ctx, action, teamsServiceUnitName)
	case "status":
		return teamsServiceRunSystemctl(ctx, "status", "--no-pager", teamsServiceUnitName)
	default:
		return nil, fmt.Errorf("unsupported primary Teams service action for systemd: %s", action)
	}
}

func (b teamsServiceSystemdBackend) Installed() (bool, error) {
	unitPath, err := b.Path()
	if err != nil {
		return false, err
	}
	return teamsServiceFileExists(unitPath)
}

func (b teamsServiceSystemdBackend) Active(ctx context.Context) (bool, error) {
	installed, err := b.Installed()
	if err != nil || !installed {
		return false, err
	}
	_, err = teamsServiceRunSystemctl(ctx, "is-active", "--quiet", teamsServiceUnitName)
	if err != nil {
		return false, nil
	}
	return true, nil
}

type teamsServiceLaunchAgentBackend struct{}

func (teamsServiceLaunchAgentBackend) ID() string {
	return "launchagent"
}

func (teamsServiceLaunchAgentBackend) Name() string {
	return teamsServiceLaunchAgentLabel
}

func (teamsServiceLaunchAgentBackend) Path() (string, error) {
	dir, err := teamsServiceLaunchAgentDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceLaunchAgentPlistName), nil
}

func (b teamsServiceLaunchAgentBackend) Install(_ context.Context, spec teamsServiceSpec) (string, error) {
	plistPath, err := b.Path()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(plistPath, []byte(buildTeamsServiceLaunchAgentPlist(spec)), 0o600); err != nil {
		return "", err
	}
	watchdogPath, err := teamsServiceLaunchAgentWatchdogPath()
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(watchdogPath, []byte(buildTeamsServiceWatchdogLaunchAgentPlist(spec)), 0o600); err != nil {
		return "", err
	}
	return plistPath, nil
}

func (b teamsServiceLaunchAgentBackend) Uninstall(ctx context.Context) (string, error) {
	plistPath, err := b.Path()
	if err != nil {
		return "", err
	}
	_, _ = teamsServiceRunLaunchctl(ctx, "bootout", teamsServiceLaunchctlServiceTarget())
	_, _ = teamsServiceRunLaunchctl(ctx, "bootout", teamsServiceLaunchctlWatchdogTarget())
	if err := os.Remove(plistPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if watchdogPath, err := teamsServiceLaunchAgentWatchdogPath(); err == nil {
		if err := os.Remove(watchdogPath); err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}
	return plistPath, nil
}

func (b teamsServiceLaunchAgentBackend) Run(ctx context.Context, action string) ([]byte, error) {
	switch action {
	case "enable":
		main, err := teamsServiceRunLaunchctl(ctx, "enable", teamsServiceLaunchctlServiceTarget())
		watchdog, watchdogErr := teamsServiceRunLaunchctl(ctx, "enable", teamsServiceLaunchctlWatchdogTarget())
		if watchdogErr != nil {
			watchdog = nil
		}
		return appendLaunchctlOutput(main, watchdog), err
	case "disable":
		watchdog, watchdogErr := teamsServiceRunLaunchctl(ctx, "disable", teamsServiceLaunchctlWatchdogTarget())
		if watchdogErr != nil {
			watchdog = nil
		}
		main, err := teamsServiceRunLaunchctl(ctx, "disable", teamsServiceLaunchctlServiceTarget())
		return appendLaunchctlOutput(watchdog, main), err
	case "status":
		main, err := teamsServiceRunLaunchctl(ctx, "print", teamsServiceLaunchctlServiceTarget())
		watchdog, watchdogErr := teamsServiceRunLaunchctl(ctx, "print", teamsServiceLaunchctlWatchdogTarget())
		if watchdogErr != nil {
			watchdog = nil
		}
		return appendLaunchctlOutput(main, watchdog), err
	case "start":
		path, err := b.Path()
		if err != nil {
			return nil, err
		}
		main, err := teamsServiceRunLaunchctl(ctx, "bootstrap", teamsServiceLaunchctlUserTarget(), path)
		watchdogPath, pathErr := teamsServiceLaunchAgentWatchdogPath()
		if pathErr == nil {
			if exists, existsErr := teamsServiceFileExists(watchdogPath); existsErr == nil && exists {
				watchdog, watchdogErr := teamsServiceRunLaunchctl(ctx, "bootstrap", teamsServiceLaunchctlUserTarget(), watchdogPath)
				if watchdogErr != nil {
					watchdog = nil
				}
				return appendLaunchctlOutput(main, watchdog), err
			}
		}
		return main, err
	case "stop":
		watchdog, watchdogErr := teamsServiceRunLaunchctl(ctx, "bootout", teamsServiceLaunchctlWatchdogTarget())
		if watchdogErr != nil {
			watchdog = nil
		}
		main, err := teamsServiceRunLaunchctl(ctx, "bootout", teamsServiceLaunchctlServiceTarget())
		return appendLaunchctlOutput(watchdog, main), err
	case "restart":
		main, err := teamsServiceRunLaunchctl(ctx, "kickstart", "-k", teamsServiceLaunchctlServiceTarget())
		watchdog, watchdogErr := teamsServiceRunLaunchctl(ctx, "kickstart", "-k", teamsServiceLaunchctlWatchdogTarget())
		if watchdogErr != nil {
			watchdog = nil
		}
		return appendLaunchctlOutput(main, watchdog), err
	default:
		return nil, fmt.Errorf("unsupported Teams service action for LaunchAgent: %s", action)
	}
}

func (b teamsServiceLaunchAgentBackend) Installed() (bool, error) {
	plistPath, err := b.Path()
	if err != nil {
		return false, err
	}
	return teamsServiceFileExists(plistPath)
}

func (b teamsServiceLaunchAgentBackend) Active(ctx context.Context) (bool, error) {
	_, err := teamsServiceRunLaunchctl(ctx, "print", teamsServiceLaunchctlServiceTarget())
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (b teamsServiceLaunchAgentBackend) RunPrimary(ctx context.Context, action string) ([]byte, error) {
	switch action {
	case "enable":
		return teamsServiceRunLaunchctl(ctx, "enable", teamsServiceLaunchctlServiceTarget())
	case "disable":
		return teamsServiceRunLaunchctl(ctx, "disable", teamsServiceLaunchctlServiceTarget())
	case "status":
		return teamsServiceRunLaunchctl(ctx, "print", teamsServiceLaunchctlServiceTarget())
	case "start":
		path, err := b.Path()
		if err != nil {
			return nil, err
		}
		return teamsServiceRunLaunchctl(ctx, "bootstrap", teamsServiceLaunchctlUserTarget(), path)
	case "stop":
		return teamsServiceRunLaunchctl(ctx, "bootout", teamsServiceLaunchctlServiceTarget())
	case "restart":
		return teamsServiceRunLaunchctl(ctx, "kickstart", "-k", teamsServiceLaunchctlServiceTarget())
	default:
		return nil, fmt.Errorf("unsupported primary Teams service action for LaunchAgent: %s", action)
	}
}

type teamsServiceWindowsTaskBackend struct{}

func (teamsServiceWindowsTaskBackend) ID() string {
	return "windows-task-scheduler"
}

func (teamsServiceWindowsTaskBackend) Name() string {
	return teamsServiceWindowsTaskName
}

func (teamsServiceWindowsTaskBackend) Path() (string, error) {
	dir, err := teamsServiceWindowsTaskXMLDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceWindowsTaskXMLName), nil
}

func teamsServiceWindowsWatchdogTaskXMLPath() (string, error) {
	dir, err := teamsServiceWindowsTaskXMLDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceWindowsWatchdogTaskXMLName), nil
}

func (b teamsServiceWindowsTaskBackend) writeTaskFiles(spec teamsServiceSpec, principalUser string) (string, string, error) {
	xmlPath, err := b.Path()
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(filepath.Dir(xmlPath), 0o700); err != nil {
		return "", "", err
	}
	watchdogXMLPath, err := teamsServiceWindowsWatchdogTaskXMLPath()
	if err != nil {
		return "", "", err
	}
	spec = teamsServiceSpecWithWindowsTaskLaunchers(spec, xmlPath, watchdogXMLPath)
	if err := writeTeamsServiceWindowsTaskLauncherFiles(spec.WindowsTaskLauncherPath, spec, buildTeamsServiceRunArgs(spec)); err != nil {
		return "", "", err
	}
	if err := writeTeamsServiceWindowsTaskLauncherFiles(spec.WindowsWatchdogLauncherPath, spec, buildTeamsServiceWatchdogArgs()); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(xmlPath, []byte(buildTeamsServiceWindowsTaskXMLWithPrincipalUser(spec, principalUser)), 0o600); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(watchdogXMLPath, []byte(buildTeamsServiceWindowsWatchdogTaskXMLWithPrincipalUser(spec, principalUser)), 0o600); err != nil {
		return "", "", err
	}
	return xmlPath, watchdogXMLPath, nil
}

func buildTeamsServiceWindowsTaskRegisterCommand(xmlPath string, watchdogXMLPath string) string {
	cmd := "$xml = Get-Content -LiteralPath " + powershellSingleQuote(xmlPath) + " -Raw; Register-ScheduledTask -TaskName " + powershellSingleQuote(teamsServiceWindowsTaskName) + " -Xml $xml -Force | Out-Null"
	cmd += "; $watchdogXml = Get-Content -LiteralPath " + powershellSingleQuote(watchdogXMLPath) + " -Raw; Register-ScheduledTask -TaskName " + powershellSingleQuote(teamsServiceWindowsWatchdogTaskName) + " -Xml $watchdogXml -Force | Out-Null"
	return cmd
}

func (b teamsServiceWindowsTaskBackend) Install(ctx context.Context, spec teamsServiceSpec) (string, error) {
	xmlPath, watchdogXMLPath, err := b.writeTaskFiles(spec, "")
	if err != nil {
		return "", err
	}
	if _, err := teamsServiceRunPowerShell(ctx, buildTeamsServiceWindowsTaskRegisterCommand(xmlPath, watchdogXMLPath)); err != nil {
		return "", err
	}
	return xmlPath, nil
}

func (b teamsServiceWindowsTaskBackend) RepairElevated(ctx context.Context, spec teamsServiceSpec, opts teamsServiceRepairOptions, principalUser string) (string, error) {
	xmlPath, watchdogXMLPath, err := b.writeTaskFiles(spec, principalUser)
	if err != nil {
		return "", err
	}
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	cmd := teamsServiceWindowsStopBridgeChildrenPowerShell(task) + "; " + buildTeamsServiceWindowsTaskRegisterCommand(xmlPath, watchdogXMLPath)
	switch {
	case opts.Start:
		cmd += "; " + teamsServiceWindowsStartTasksPowerShell()
	case opts.Enable:
		cmd += "; " + teamsServiceWindowsEnableTasksPowerShell()
	}
	if _, err := teamsServiceRunPowerShell(ctx, buildTeamsServiceWindowsElevatedCommand(cmd)); err != nil {
		return "", err
	}
	return xmlPath, nil
}

func (b teamsServiceWindowsTaskBackend) Uninstall(ctx context.Context) (string, error) {
	xmlPath, err := b.Path()
	if err != nil {
		return "", err
	}
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	watchdogTask := powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)
	cmd := "if (Get-ScheduledTask -TaskName " + watchdogTask + " -ErrorAction SilentlyContinue) { Unregister-ScheduledTask -TaskName " + watchdogTask + " -Confirm:$false }; "
	cmd += "if (Get-ScheduledTask -TaskName " + task + " -ErrorAction SilentlyContinue) { Unregister-ScheduledTask -TaskName " + task + " -Confirm:$false }"
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		return "", err
	}
	if err := os.Remove(xmlPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if watchdogXMLPath, err := teamsServiceWindowsWatchdogTaskXMLPath(); err == nil {
		if err := os.Remove(watchdogXMLPath); err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}
	removeTeamsServiceWindowsTaskLauncherFiles()
	return xmlPath, nil
}

func (teamsServiceWindowsTaskBackend) Run(ctx context.Context, action string) ([]byte, error) {
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	watchdogTask := powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)
	switch action {
	case "enable":
		return teamsServiceRunPowerShell(ctx, teamsServiceWindowsEnableTasksPowerShell())
	case "disable":
		resolveWatchdog := teamsServiceWindowsResolveWatchdogTaskPowerShell()
		return teamsServiceRunPowerShell(ctx, resolveWatchdog+"if ($null -ne $watchdogTask) { Disable-ScheduledTask -TaskName "+watchdogTask+" | Out-Null }; Disable-ScheduledTask -TaskName "+task+" | Out-Null")
	case "status":
		resolveWatchdog := teamsServiceWindowsResolveWatchdogTaskPowerShell()
		return teamsServiceRunPowerShell(ctx, "$task = Get-ScheduledTask -TaskName "+task+"; $info = Get-ScheduledTaskInfo -TaskName "+task+"; $task | Format-List TaskName,State; $info | Format-List LastRunTime,LastTaskResult,NextRunTime; "+resolveWatchdog+"if ($null -ne $watchdogTask) { $watchdogInfo = Get-ScheduledTaskInfo -TaskName "+watchdogTask+"; $watchdogTask | Format-List TaskName,State; $watchdogInfo | Format-List LastRunTime,LastTaskResult,NextRunTime } else { Write-Output 'Watchdog task not installed' }")
	case "start":
		return teamsServiceRunPowerShell(ctx, teamsServiceWindowsStartTasksPowerShell())
	case "stop":
		cleanupBridgeChildren := teamsServiceWindowsStopBridgeChildrenPowerShell(task)
		return teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+watchdogTask+" -ErrorAction SilentlyContinue; Stop-ScheduledTask -TaskName "+task+" -ErrorAction SilentlyContinue; "+cleanupBridgeChildren)
	case "restart":
		resolveWatchdog := teamsServiceWindowsResolveWatchdogTaskPowerShell()
		cleanupBridgeChildren := teamsServiceWindowsStopBridgeChildrenPowerShell(task)
		return teamsServiceRunPowerShell(ctx, teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell()+teamsServiceWaitScheduledTaskStoppedFunctionPowerShell()+"Stop-ScheduledTask -TaskName "+task+" -ErrorAction SilentlyContinue; "+cleanupBridgeChildren+"Enable-ScheduledTask -TaskName "+task+" | Out-Null; if (-not (Wait-CodexHelperScheduledTaskStopped "+task+" 20)) { throw 'Teams bridge Scheduled Task did not stop before restart' }; "+resolveWatchdog+"if ($null -ne $watchdogTask) { Enable-ScheduledTask -TaskName "+watchdogTask+" | Out-Null }; Start-CodexHelperScheduledTaskIfStopped "+task+"; if ($null -ne $watchdogTask) { Start-CodexHelperScheduledTaskIfStopped "+watchdogTask+" }")
	default:
		return nil, fmt.Errorf("unsupported Teams service action for Task Scheduler: %s", action)
	}
}

func (teamsServiceWindowsTaskBackend) RunPrimary(ctx context.Context, action string) ([]byte, error) {
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	cleanupBridgeChildren := teamsServiceWindowsStopBridgeChildrenPowerShell(task)
	switch action {
	case "enable":
		return teamsServiceRunPowerShell(ctx, "Enable-ScheduledTask -TaskName "+task+" | Out-Null")
	case "disable":
		return teamsServiceRunPowerShell(ctx, "Disable-ScheduledTask -TaskName "+task+" | Out-Null")
	case "status":
		return teamsServiceRunPowerShell(ctx, "$task = Get-ScheduledTask -TaskName "+task+"; $info = Get-ScheduledTaskInfo -TaskName "+task+"; $task | Format-List TaskName,State; $info | Format-List LastRunTime,LastTaskResult,NextRunTime")
	case "start":
		return teamsServiceRunPowerShell(ctx, teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell()+"Enable-ScheduledTask -TaskName "+task+" | Out-Null; $bridgeTask = Get-ScheduledTask -TaskName "+task+"; if ($bridgeTask.State -ne 'Running') { "+cleanupBridgeChildren+"Start-CodexHelperScheduledTaskIfStopped "+task+" }")
	case "stop":
		return teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+task+" -ErrorAction SilentlyContinue; "+cleanupBridgeChildren)
	case "restart":
		return teamsServiceRunPowerShell(ctx, teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell()+teamsServiceWaitScheduledTaskStoppedFunctionPowerShell()+"Stop-ScheduledTask -TaskName "+task+" -ErrorAction SilentlyContinue; "+cleanupBridgeChildren+"Enable-ScheduledTask -TaskName "+task+" | Out-Null; if (-not (Wait-CodexHelperScheduledTaskStopped "+task+" 20)) { throw 'Teams bridge Scheduled Task did not stop before restart' }; Start-CodexHelperScheduledTaskIfStopped "+task)
	default:
		return nil, fmt.Errorf("unsupported primary Teams service action for Task Scheduler: %s", action)
	}
}

func teamsServiceWindowsResolveWatchdogTaskPowerShell() string {
	watchdogTask := powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)
	return "$watchdogTask = Get-ScheduledTask -TaskName " + watchdogTask + " -ErrorAction SilentlyContinue; "
}

func teamsServiceWindowsEnableTasksPowerShell() string {
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	watchdogTask := powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)
	resolveWatchdog := teamsServiceWindowsResolveWatchdogTaskPowerShell()
	return "Enable-ScheduledTask -TaskName " + task + " | Out-Null; " + resolveWatchdog + "if ($null -ne $watchdogTask) { Enable-ScheduledTask -TaskName " + watchdogTask + " | Out-Null }"
}

func teamsServiceWindowsStartTasksPowerShell() string {
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	watchdogTask := powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)
	resolveWatchdog := teamsServiceWindowsResolveWatchdogTaskPowerShell()
	cleanupBridgeChildren := teamsServiceWindowsStopBridgeChildrenPowerShell(task)
	return teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() +
		"Enable-ScheduledTask -TaskName " + task + " | Out-Null; " +
		resolveWatchdog +
		"if ($null -ne $watchdogTask) { Enable-ScheduledTask -TaskName " + watchdogTask + " | Out-Null }; " +
		"$bridgeTask = Get-ScheduledTask -TaskName " + task + "; " +
		"if ($bridgeTask.State -ne 'Running') { " + cleanupBridgeChildren + "Start-CodexHelperScheduledTaskIfStopped " + task + " }; " +
		"if ($null -ne $watchdogTask) { Start-CodexHelperScheduledTaskIfStopped " + watchdogTask + " }"
}

func teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() string {
	return "function Test-CodexHelperScheduledTaskRunning([string]$taskName, [int]$attempts) { " +
		"for ($i = 0; $i -lt $attempts; $i++) { " +
		"$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; " +
		"if ($null -eq $task) { return $false }; " +
		"if ($task.State -eq 'Running') { return $true }; " +
		"if ($i -lt ($attempts - 1)) { Start-Sleep -Milliseconds 250 } " +
		"}; " +
		"return $false " +
		"}; " +
		"function Start-CodexHelperScheduledTaskIfStopped([string]$taskName) { " +
		"if ([string]::IsNullOrWhiteSpace($taskName)) { return }; " +
		"$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; " +
		"if ($null -eq $task) { return }; " +
		"if ($task.State -eq 'Running' -and (Test-CodexHelperScheduledTaskRunning $taskName 3)) { return }; " +
		"try { Start-ScheduledTask -TaskName $taskName -ErrorAction Stop | Out-Null } catch { " +
		"if (-not (Test-CodexHelperScheduledTaskRunning $taskName 10)) { throw } " +
		"} " +
		"}; "
}

func teamsServiceWaitScheduledTaskStoppedFunctionPowerShell() string {
	return "function Wait-CodexHelperScheduledTaskStopped([string]$taskName, [int]$attempts) { " +
		"for ($i = 0; $i -lt $attempts; $i++) { " +
		"$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; " +
		"if ($null -eq $task -or $task.State -ne 'Running') { return $true }; " +
		"if ($i -lt ($attempts - 1)) { Start-Sleep -Milliseconds 250 } " +
		"}; " +
		"return $false " +
		"}; "
}

func teamsServiceWindowsStopBridgeChildrenPowerShell(taskNameExpr string) string {
	taskNameExpr = strings.TrimSpace(taskNameExpr)
	if taskNameExpr == "" {
		taskNameExpr = "$null"
	}
	parts := []string{
		`function Convert-CodexHelperSingleQuotedPowerShellLiteral([string]$value) { if ($null -eq $value) { return '' }; return $value.Replace("''", "'") };`,
		`function Convert-CodexHelperCommandForMatch([string]$value) { if ([string]::IsNullOrWhiteSpace($value)) { return '' }; $norm = $value.Replace([char]34, ' ').Replace([char]39, ' '); $norm = [regex]::Replace($norm, '\s+', ' ').Trim(); return ' ' + $norm + ' ' };`,
		`function Test-CodexHelperTeamsBridgeCommand([string]$cmd, [string]$expectedArgs) { if ([string]::IsNullOrWhiteSpace($cmd) -or [string]::IsNullOrWhiteSpace($expectedArgs)) { return $false }; $norm = Convert-CodexHelperCommandForMatch $cmd; $expected = Convert-CodexHelperCommandForMatch $expectedArgs; return ($norm -match '(?i)\steams\s+(run|listen)\s' -and $norm -notmatch '(?i)\s--once\s' -and $norm.IndexOf($expected, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) };`,
		`function Set-CodexHelperCleanupExeCandidate($identity, [string]$candidate) { if ([string]::IsNullOrWhiteSpace($candidate) -or -not [string]::IsNullOrWhiteSpace([string]$identity['Exe'])) { return }; try { $full = [IO.Path]::GetFullPath($candidate); $leaf = [IO.Path]::GetFileName($full); if (($leaf -ieq 'codex-proxy.exe' -or $leaf -ieq 'cxp.exe') -and (Test-Path -LiteralPath $full)) { $identity['Exe'] = $full } } catch { } };`,
		`function Add-CodexHelperCleanupIdentity($identity, [string]$text) { if ([string]::IsNullOrWhiteSpace($text)) { return }; foreach ($pattern in @("Start-Process\s+-FilePath\s+'((?:''|[^'])*)'", "&\s+'((?:''|[^'])*)'")) { foreach ($m in [regex]::Matches($text, $pattern, 'IgnoreCase')) { Set-CodexHelperCleanupExeCandidate $identity (Convert-CodexHelperSingleQuotedPowerShellLiteral $m.Groups[1].Value) } }; foreach ($m in [regex]::Matches($text, ([string][char]36 + "argumentLine\s*=\s+'((?:''|[^'])*)'"), 'IgnoreCase')) { $identity['Args'] = Convert-CodexHelperSingleQuotedPowerShellLiteral $m.Groups[1].Value; break } };`,
		`function Get-CodexHelperBridgeTaskIdentity([string]$taskName) { $identity = @{ Exe = ''; Args = '' }; try { $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; if ($null -eq $task) { return $identity }; foreach ($action in @($task.Actions)) { Set-CodexHelperCleanupExeCandidate $identity ([string]$action.Execute); $arguments = [string]$action.Arguments; Add-CodexHelperCleanupIdentity $identity $arguments; foreach ($m in [regex]::Matches($arguments, '"([^"]+\.vbs)"')) { $vbs = $m.Groups[1].Value; try { $ps1 = [IO.Path]::ChangeExtension($vbs, '.ps1'); if (Test-Path -LiteralPath $ps1) { Add-CodexHelperCleanupIdentity $identity (Get-Content -LiteralPath $ps1 -Raw -ErrorAction SilentlyContinue) } } catch { } } } } catch { }; return $identity };`,
		`$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity $bridgeTaskNameForCleanup; $expectedBridgeExeForCleanup = [string]$bridgeIdentityForCleanup['Exe']; $expectedBridgeArgsForCleanup = [string]$bridgeIdentityForCleanup['Args'];`,
		`if (-not [string]::IsNullOrWhiteSpace($expectedBridgeExeForCleanup) -and -not [string]::IsNullOrWhiteSpace($expectedBridgeArgsForCleanup)) { try { Get-CimInstance Win32_Process -ErrorAction Stop | ForEach-Object { $proc = $_; $cmd = [string]$proc.CommandLine; if ($proc.ProcessId -ne $PID -and (Test-CodexHelperTeamsBridgeCommand $cmd $expectedBridgeArgsForCleanup)) { $matchesBridgeExeForCleanup = $false; try { $matchesBridgeExeForCleanup = ([IO.Path]::GetFullPath([string]$proc.ExecutablePath) -ieq $expectedBridgeExeForCleanup) } catch { $matchesBridgeExeForCleanup = $false }; if ($matchesBridgeExeForCleanup) { Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue } } } } catch { } };`,
	}
	return "$bridgeTaskNameForCleanup = " + taskNameExpr + "; " + strings.Join(parts, " ")
}

func (b teamsServiceWindowsTaskBackend) Installed() (bool, error) {
	_, err := teamsServiceRunPowerShell(context.Background(), "Get-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName)+" -ErrorAction Stop | Out-Null")
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (b teamsServiceWindowsTaskBackend) Active(ctx context.Context) (bool, error) {
	_, err := teamsServiceRunPowerShell(ctx, "$task = Get-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName)+"; if ($task.State -ne 'Running') { exit 3 }")
	if err != nil {
		return false, nil
	}
	return true, nil
}

type teamsServiceWSLWindowsTaskBackend struct{}

func (teamsServiceWSLWindowsTaskBackend) ID() string {
	return "wsl-windows-task-scheduler"
}

func (b teamsServiceWSLWindowsTaskBackend) Name() string {
	identity := teamsServiceWSLTaskIdentity()
	return "Codex Helper Teams Bridge (WSL " + identity.Display + " " + identity.Suffix + ")"
}

func (b teamsServiceWSLWindowsTaskBackend) watchdogName() string {
	identity := teamsServiceWSLTaskIdentity()
	return "Codex Helper Teams Watchdog (WSL " + identity.Display + " " + identity.Suffix + ")"
}

func (teamsServiceWSLWindowsTaskBackend) Path() (string, error) {
	dir, err := teamsServiceWindowsTaskXMLDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "codex-helper-teams-wsl-task-"+teamsServiceWSLTaskIdentity().Suffix+".txt"), nil
}

func (teamsServiceWSLWindowsTaskBackend) startupFallbackMarkerPath() (string, error) {
	dir, err := teamsServiceWindowsTaskXMLDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "codex-helper-teams-wsl-startup-"+teamsServiceWSLTaskIdentity().Suffix+".txt"), nil
}

func (b teamsServiceWSLWindowsTaskBackend) startupFallbackMarkerPathsForCurrentTaskPrefix() ([]string, error) {
	current, err := b.startupFallbackMarkerPath()
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(current)
	matches, err := filepath.Glob(filepath.Join(dir, "codex-helper-teams-wsl-startup-*.txt"))
	if err != nil {
		return nil, err
	}
	prefix := teamsServiceWSLTaskNamePrefix(b.Name())
	seen := map[string]bool{}
	paths := make([]string, 0, len(matches)+1)
	add := func(path string) {
		if !seen[path] {
			seen[path] = true
			paths = append(paths, path)
		}
	}
	add(current)
	for _, path := range matches {
		if path == current {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if teamsServiceWSLStartupFallbackConfigMatchesTaskPrefix(string(data), prefix) {
			add(path)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func teamsServiceWSLStartupFallbackConfigMatchesTaskPrefix(config, prefix string) bool {
	for _, line := range strings.Split(config, "\n") {
		taskName, ok := strings.CutPrefix(strings.TrimSpace(line), "TaskName=")
		if ok {
			return strings.HasPrefix(strings.TrimSpace(taskName), prefix)
		}
	}
	return false
}

func teamsServiceWSLStartupFallbackSuffixFromMarkerPath(path string) (string, bool) {
	base := filepath.Base(path)
	const markerPrefix = "codex-helper-teams-wsl-startup-"
	if !strings.HasPrefix(base, markerPrefix) || !strings.HasSuffix(base, ".txt") {
		return "", false
	}
	suffix := strings.TrimSuffix(strings.TrimPrefix(base, markerPrefix), ".txt")
	if suffix == "" {
		return "", false
	}
	return suffix, true
}

func teamsServiceWSLStartupFallbackStopPath(markerPath string) string {
	return strings.TrimSuffix(markerPath, ".txt") + ".stop"
}

func (b teamsServiceWSLWindowsTaskBackend) writeTaskConfig(args []string) (string, error) {
	configPath, err := b.Path()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(configPath, []byte(buildTeamsServiceWSLTaskConfig(b.Name(), args)), 0o600); err != nil {
		return "", err
	}
	return configPath, nil
}

func (b teamsServiceWSLWindowsTaskBackend) removeTaskConfig() error {
	configPath, err := b.Path()
	if err != nil {
		return err
	}
	if err := os.Remove(configPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b teamsServiceWSLWindowsTaskBackend) Install(ctx context.Context, spec teamsServiceSpec) (string, error) {
	args := buildTeamsServiceWSLArguments(spec)
	cmd := buildTeamsServiceWSLRegisterCommand(b.Name(), args, teamsServiceWSLRegisterOptions{ForceDisabled: true})
	watchdogArgs := buildTeamsServiceWSLWatchdogArguments(spec)
	cmd += "; " + buildTeamsServiceWSLRegisterCommand(b.watchdogName(), watchdogArgs, teamsServiceWSLRegisterOptions{ForceDisabled: true})
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		return "", err
	}
	return b.writeTaskConfig(args)
}

func (b teamsServiceWSLWindowsTaskBackend) Repair(ctx context.Context, spec teamsServiceSpec, opts teamsServiceRepairOptions) (string, error) {
	args := buildTeamsServiceWSLArguments(spec)
	cmd := buildTeamsServiceWSLRegisterCommand(b.Name(), args, teamsServiceWSLRegisterOptions{
		Enable:          opts.Enable,
		Start:           opts.Start,
		PreserveEnabled: !opts.Enable,
		PreserveRunning: !opts.Start,
		CleanLegacy:     true,
	})
	watchdogArgs := buildTeamsServiceWSLWatchdogArguments(spec)
	cmd += "; " + buildTeamsServiceWSLRegisterCommand(b.watchdogName(), watchdogArgs, teamsServiceWSLRegisterOptions{
		Enable:          opts.Enable,
		Start:           opts.Start,
		PreserveEnabled: !opts.Enable,
		PreserveRunning: !opts.Start,
		CleanLegacy:     true,
	})
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		return "", err
	}
	return b.writeTaskConfig(args)
}

func (b teamsServiceWSLWindowsTaskBackend) RepairElevated(ctx context.Context, spec teamsServiceSpec, opts teamsServiceRepairOptions, principalUser string) (string, error) {
	args := buildTeamsServiceWSLArguments(spec)
	cmd := buildTeamsServiceWSLRegisterCommand(b.Name(), args, teamsServiceWSLRegisterOptions{
		Enable:          opts.Enable,
		Start:           opts.Start,
		PreserveEnabled: !opts.Enable,
		PreserveRunning: !opts.Start,
		PrincipalUser:   principalUser,
		CleanLegacy:     true,
	})
	watchdogArgs := buildTeamsServiceWSLWatchdogArguments(spec)
	cmd += "; " + buildTeamsServiceWSLRegisterCommand(b.watchdogName(), watchdogArgs, teamsServiceWSLRegisterOptions{
		Enable:          opts.Enable,
		Start:           opts.Start,
		PreserveEnabled: !opts.Enable,
		PreserveRunning: !opts.Start,
		PrincipalUser:   principalUser,
		CleanLegacy:     true,
	})
	if _, err := teamsServiceRunPowerShell(ctx, buildTeamsServiceWSLElevatedCommand(cmd)); err != nil {
		return "", err
	}
	return b.writeTaskConfig(args)
}

func (b teamsServiceWSLWindowsTaskBackend) TaskConfigMatches(ctx context.Context, spec teamsServiceSpec) (bool, error) {
	args := buildTeamsServiceWSLArguments(spec)
	watchdogArgs := buildTeamsServiceWSLWatchdogArguments(spec)
	cmd := teamsServiceWSLTaskConfigMatchHelpersPowerShell() +
		"$allMatched = $true; " +
		buildTeamsServiceWSLTaskConfigMatchesCommand(b.Name(), args, teamsServiceWSLTaskConfigMatchOptions{}) + "; " +
		buildTeamsServiceWSLTaskConfigMatchesCommand(b.watchdogName(), watchdogArgs, teamsServiceWSLTaskConfigMatchOptions{}) + "; " +
		"if (-not $allMatched) { exit 3 }"
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, nil
	}
	return true, nil
}

func (b teamsServiceWSLWindowsTaskBackend) VerifyAndCleanAfterRepair(ctx context.Context, spec teamsServiceSpec) error {
	matches, matchErr := b.TaskConfigMatches(ctx, spec)
	if matchErr != nil {
		return fmt.Errorf("Windows Scheduled Task setup completed but verification failed: %w", matchErr)
	}
	if !matches {
		return fmt.Errorf("Windows Scheduled Task setup completed but the registered task still does not match the current helper configuration")
	}
	if err := b.RetireLegacyScheduledTasks(ctx); err != nil {
		return fmt.Errorf("Windows Scheduled Task setup completed, but old WSL Scheduled Tasks could not be disabled: %w", err)
	}
	if cleanupErr := b.RemoveStartupFallbackMarker(); cleanupErr != nil {
		return fmt.Errorf("Windows Scheduled Task setup completed, but old Startup watchdog cleanup failed: %w", cleanupErr)
	}
	return nil
}

func (b teamsServiceWSLWindowsTaskBackend) RetireScheduledTasks(ctx context.Context) error {
	cmd := buildTeamsServiceWSLRetireTaskCommand(b.Name(), true) + "; " + buildTeamsServiceWSLRetireTaskCommand(b.watchdogName(), true)
	_, err := teamsServiceRunPowerShell(ctx, cmd)
	return err
}

func (b teamsServiceWSLWindowsTaskBackend) RetireScheduledTasksElevated(ctx context.Context) error {
	cmd := buildTeamsServiceWSLRetireTaskCommand(b.Name(), true) + "; " + buildTeamsServiceWSLRetireTaskCommand(b.watchdogName(), true)
	_, err := teamsServiceRunPowerShell(ctx, buildTeamsServiceWSLElevatedCommand(cmd))
	return err
}

func (b teamsServiceWSLWindowsTaskBackend) RetireLegacyScheduledTasks(ctx context.Context) error {
	cmd := buildTeamsServiceWSLRetireTaskCommand(b.Name(), false) + "; " + buildTeamsServiceWSLRetireTaskCommand(b.watchdogName(), false)
	_, err := teamsServiceRunPowerShell(ctx, cmd)
	return err
}

func (b teamsServiceWSLWindowsTaskBackend) InstallStartupFallback(ctx context.Context, spec teamsServiceSpec, start bool) (string, error) {
	markerPath, err := b.startupFallbackMarkerPath()
	if err != nil {
		return "", err
	}
	stopPath := teamsServiceWSLStartupFallbackStopPath(markerPath)
	fallbackSpec := buildTeamsServiceWSLStartupFallbackSpec(spec, stopPath)
	args := buildTeamsServiceWSLArguments(fallbackSpec)
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		return "", err
	}
	if err := os.Remove(stopPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	command := buildTeamsServiceWSLStartupFallbackCommand(b.Name(), args, start)
	if _, err := teamsServiceRunPowerShell(ctx, command); err != nil {
		return "", err
	}
	if err := os.WriteFile(markerPath, []byte(buildTeamsServiceWSLStartupFallbackConfig(b.Name(), args)), 0o600); err != nil {
		return "", err
	}
	return markerPath, nil
}

func buildTeamsServiceWSLStartupFallbackSpec(spec teamsServiceSpec, stopPath string) teamsServiceSpec {
	fallbackSpec := spec
	fallbackSpec.Environment = make(map[string]string, len(spec.Environment)+2)
	for key, value := range spec.Environment {
		fallbackSpec.Environment[key] = value
	}
	fallbackSpec.Environment["CODEX_HELPER_TEAMS_STARTUP_FALLBACK"] = "1"
	fallbackSpec.Environment["CODEX_HELPER_TEAMS_STARTUP_FALLBACK_STOP_FILE"] = stopPath
	return fallbackSpec
}

func (b teamsServiceWSLWindowsTaskBackend) StartupFallbackMarkerExists() (bool, error) {
	markerPath, err := b.startupFallbackMarkerPath()
	if err != nil {
		return false, err
	}
	installed, err := teamsServiceFileExists(markerPath)
	if err != nil || !installed {
		return installed, err
	}
	stopped, err := teamsServiceFileExists(teamsServiceWSLStartupFallbackStopPath(markerPath))
	if err != nil {
		return false, err
	}
	return !stopped, nil
}

func (b teamsServiceWSLWindowsTaskBackend) startupFallbackMarkerInstalled() (bool, error) {
	markerPath, err := b.startupFallbackMarkerPath()
	if err != nil {
		return false, err
	}
	return teamsServiceFileExists(markerPath)
}

func (b teamsServiceWSLWindowsTaskBackend) startupFallbackSuffix() (string, string, error) {
	markerPath, err := b.startupFallbackMarkerPath()
	if err != nil {
		return "", "", err
	}
	suffix, ok := teamsServiceWSLStartupFallbackSuffixFromMarkerPath(markerPath)
	if !ok {
		return "", "", fmt.Errorf("invalid Teams WSL Startup fallback marker path: %s", markerPath)
	}
	return markerPath, suffix, nil
}

type teamsServiceWSLStartupFallbackConfig struct {
	TaskName  string
	Arguments string
}

func readTeamsServiceWSLStartupFallbackConfig(path string) (teamsServiceWSLStartupFallbackConfig, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return teamsServiceWSLStartupFallbackConfig{}, false
	}
	var cfg teamsServiceWSLStartupFallbackConfig
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if value, ok := strings.CutPrefix(line, "TaskName="); ok {
			cfg.TaskName = strings.TrimSpace(value)
		}
		if value, ok := strings.CutPrefix(line, "Arguments="); ok {
			cfg.Arguments = strings.TrimSpace(value)
		}
	}
	return cfg, cfg.TaskName != "" || cfg.Arguments != ""
}

func filterTeamsServiceWSLStartupFallbackArgs(args []string) []string {
	out := args[:0]
	for _, arg := range args {
		if strings.HasPrefix(arg, "CODEX_HELPER_TEAMS_EXIT_ON_STANDBY=") {
			continue
		}
		out = append(out, arg)
	}
	return out
}

func (b teamsServiceWSLWindowsTaskBackend) startStartupFallback(ctx context.Context) ([]byte, error) {
	markerPath, suffix, err := b.startupFallbackSuffix()
	if err != nil {
		return nil, err
	}
	installed, err := teamsServiceFileExists(markerPath)
	if err != nil {
		return nil, err
	}
	if !installed {
		return nil, fmt.Errorf("Teams WSL Startup fallback marker not found: %s", markerPath)
	}
	if err := os.Remove(teamsServiceWSLStartupFallbackStopPath(markerPath)); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	command := buildTeamsServiceWSLStartExistingStartupFallbackCommand(suffix)
	if config, ok := readTeamsServiceWSLStartupFallbackConfig(markerPath); ok && strings.TrimSpace(config.Arguments) != "" {
		if args, err := splitWindowsCommandLine(config.Arguments); err == nil {
			args = filterTeamsServiceWSLStartupFallbackArgs(args)
			taskName := strings.TrimSpace(config.TaskName)
			if taskName == "" {
				taskName = b.Name()
			}
			command = buildTeamsServiceWSLStartupFallbackCommandWithArgumentLine(taskName, windowsCommandLine(args), suffix, true)
		}
	}
	data, err := teamsServiceRunPowerShell(ctx, command)
	if err != nil {
		return data, err
	}
	return appendLaunchctlOutput(data, []byte("Startup watchdog fallback: started\nStartup watchdog config: "+markerPath+"\n")), nil
}

func (b teamsServiceWSLWindowsTaskBackend) stopStartupFallback(ctx context.Context) ([]byte, error) {
	markerPath, suffix, err := b.startupFallbackSuffix()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(teamsServiceWSLStartupFallbackStopPath(markerPath), []byte("stop\n"), 0o600); err != nil {
		return nil, err
	}
	data, err := teamsServiceRunPowerShell(ctx, buildTeamsServiceWSLStopStartupFallbackCommand([]string{suffix}))
	if err != nil {
		return data, err
	}
	return appendLaunchctlOutput(data, []byte("Startup watchdog fallback: stopped\nStartup watchdog config: "+markerPath+"\n")), nil
}

func (b teamsServiceWSLWindowsTaskBackend) restartStartupFallback(ctx context.Context) ([]byte, error) {
	stopData, err := b.stopStartupFallback(ctx)
	if err != nil {
		return stopData, err
	}
	if teamsServiceStartupFallbackRestartDelay > 0 {
		timer := time.NewTimer(teamsServiceStartupFallbackRestartDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return stopData, ctx.Err()
		}
	}
	startData, err := b.startStartupFallback(ctx)
	return appendLaunchctlOutput(stopData, startData), err
}

func (b teamsServiceWSLWindowsTaskBackend) RemoveStartupFallbackMarker() error {
	markerPaths, err := b.startupFallbackMarkerPathsForCurrentTaskPrefix()
	if err != nil {
		return err
	}
	var suffixes []string
	var installedMarkers []string
	for _, markerPath := range markerPaths {
		installed, err := teamsServiceFileExists(markerPath)
		if err != nil {
			return err
		}
		if suffix, ok := teamsServiceWSLStartupFallbackSuffixFromMarkerPath(markerPath); ok {
			suffixes = append(suffixes, suffix)
		}
		if !installed {
			continue
		}
		if err := os.WriteFile(teamsServiceWSLStartupFallbackStopPath(markerPath), []byte("stop\n"), 0o600); err != nil {
			return err
		}
		installedMarkers = append(installedMarkers, markerPath)
	}
	if _, err = teamsServiceRunPowerShell(context.Background(), buildTeamsServiceWSLRemoveStartupFallbackCommand(b.Name(), suffixes)); err != nil {
		return err
	}
	for _, markerPath := range installedMarkers {
		if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (b teamsServiceWSLWindowsTaskBackend) Uninstall(ctx context.Context) (string, error) {
	configPath, err := b.Path()
	if err != nil {
		return "", err
	}
	task := powershellSingleQuote(b.Name())
	watchdogTask := powershellSingleQuote(b.watchdogName())
	cmd := "if (Get-ScheduledTask -TaskName " + watchdogTask + " -ErrorAction SilentlyContinue) { Unregister-ScheduledTask -TaskName " + watchdogTask + " -Confirm:$false }; "
	cmd += "if (Get-ScheduledTask -TaskName " + task + " -ErrorAction SilentlyContinue) { Unregister-ScheduledTask -TaskName " + task + " -Confirm:$false }"
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		return "", err
	}
	if err := os.Remove(configPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	_ = b.RemoveStartupFallbackMarker()
	return configPath, nil
}

func (b teamsServiceWSLWindowsTaskBackend) Run(ctx context.Context, action string) ([]byte, error) {
	if installed, err := b.startupFallbackMarkerInstalled(); err == nil && installed {
		switch action {
		case "enable", "start":
			return b.startStartupFallback(ctx)
		case "disable", "stop":
			return b.stopStartupFallback(ctx)
		case "restart":
			return b.restartStartupFallback(ctx)
		}
	}
	resolve := teamsServiceWSLResolveTaskPowerShell(b.Name())
	resolveWatchdog := teamsServiceWSLResolveOptionalTaskPowerShell(b.watchdogName())
	switch action {
	case "enable":
		return teamsServiceRunPowerShell(ctx, resolve+"Enable-ScheduledTask -TaskName $taskName | Out-Null; "+resolveWatchdog+"if ($null -ne $task) { Enable-ScheduledTask -TaskName $taskName | Out-Null }")
	case "disable":
		return teamsServiceRunPowerShell(ctx, resolveWatchdog+"if ($null -ne $task) { Disable-ScheduledTask -TaskName $taskName | Out-Null }; "+resolve+"Disable-ScheduledTask -TaskName $taskName | Out-Null")
	case "status":
		runLogChild := powershellSingleQuote("codex-helper\\teams\\" + teamsServiceWSLTaskRunLogName(b.Name()))
		watchdogLogChild := powershellSingleQuote("codex-helper\\teams\\" + teamsServiceWSLTaskRunLogName(b.watchdogName()))
		data, err := teamsServiceRunPowerShell(ctx, resolve+"$info = Get-ScheduledTaskInfo -TaskName $taskName; $runLog = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "+runLogChild+"; if ($task.TaskName -ne "+powershellSingleQuote(b.Name())+") { 'ResolvedLegacyTaskName : ' + $task.TaskName }; $task | Format-List TaskName,State; $info | Format-List LastRunTime,LastTaskResult,NextRunTime; 'RunLog : ' + $runLog; "+resolveWatchdog+"if ($null -ne $task) { $watchdogInfo = Get-ScheduledTaskInfo -TaskName $taskName; $watchdogRunLog = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "+watchdogLogChild+"; $task | Format-List TaskName,State; $watchdogInfo | Format-List LastRunTime,LastTaskResult,NextRunTime; 'WatchdogRunLog : ' + $watchdogRunLog } else { 'WatchdogScheduledTask : not registered' }")
		if err == nil {
			if installed, markerErr := b.startupFallbackMarkerInstalled(); markerErr == nil && installed {
				markerPath, _ := b.startupFallbackMarkerPath()
				status := "installed"
				if active, activeErr := b.StartupFallbackMarkerExists(); activeErr == nil && !active {
					status = "stopped"
				}
				return appendLaunchctlOutput(data, []byte("Startup watchdog fallback: "+status+"\nStartup watchdog config: "+markerPath+"\n")), nil
			}
			return data, nil
		}
		if installed, markerErr := b.startupFallbackMarkerInstalled(); markerErr == nil && installed {
			markerPath, _ := b.startupFallbackMarkerPath()
			status := "installed"
			if active, activeErr := b.StartupFallbackMarkerExists(); activeErr == nil && !active {
				status = "stopped"
			}
			return []byte("Scheduled Task: not registered\nStartup watchdog fallback: " + status + "\nStartup watchdog config: " + markerPath + "\n"), nil
		}
		return data, err
	case "start":
		return teamsServiceRunPowerShell(ctx, resolve+teamsServiceWSLEnableStartTaskAndVerifyPowerShell()+"; "+resolveWatchdog+"if ($null -ne $task) { "+teamsServiceWSLEnableStartTaskIfStoppedAndVerifyPowerShell()+" }")
	case "stop":
		return teamsServiceRunPowerShell(ctx, resolveWatchdog+"if ($null -ne $task) { Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue }; "+resolve+"Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue")
	case "restart":
		return teamsServiceRunPowerShell(ctx, resolve+"Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; "+teamsServiceWSLEnableStartTaskAndVerifyPowerShell()+"; "+resolveWatchdog+"if ($null -ne $task) { "+teamsServiceWSLEnableStartTaskIfStoppedAndVerifyPowerShell()+" }")
	default:
		return nil, fmt.Errorf("unsupported Teams service action for WSL Task Scheduler: %s", action)
	}
}

func (b teamsServiceWSLWindowsTaskBackend) Installed() (bool, error) {
	_, err := teamsServiceRunPowerShell(context.Background(), teamsServiceWSLResolveTaskPowerShell(b.Name())+"$task | Out-Null")
	if err != nil {
		if installed, markerErr := b.startupFallbackMarkerInstalled(); markerErr == nil && installed {
			return true, nil
		}
		return false, nil
	}
	return true, nil
}

func (b teamsServiceWSLWindowsTaskBackend) Active(ctx context.Context) (bool, error) {
	_, err := teamsServiceRunPowerShell(ctx, teamsServiceWSLResolveTaskPowerShell(b.Name())+"if ($task.State -ne 'Running') { exit 3 }")
	if err != nil {
		if active, markerErr := b.StartupFallbackMarkerExists(); markerErr == nil && active {
			return true, nil
		}
		return false, nil
	}
	return true, nil
}

func (b teamsServiceWSLWindowsTaskBackend) RunPrimary(ctx context.Context, action string) ([]byte, error) {
	resolve := teamsServiceWSLResolveTaskPowerShell(b.Name())
	switch action {
	case "enable":
		return teamsServiceRunPowerShell(ctx, resolve+"Enable-ScheduledTask -TaskName $taskName | Out-Null")
	case "disable":
		return teamsServiceRunPowerShell(ctx, resolve+"Disable-ScheduledTask -TaskName $taskName | Out-Null")
	case "status":
		return teamsServiceRunPowerShell(ctx, resolve+"$info = Get-ScheduledTaskInfo -TaskName $taskName; $task | Format-List TaskName,State; $info | Format-List LastRunTime,LastTaskResult,NextRunTime")
	case "start":
		return teamsServiceRunPowerShell(ctx, resolve+teamsServiceWSLEnableStartTaskAndVerifyPowerShell())
	case "stop":
		return teamsServiceRunPowerShell(ctx, resolve+"Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue")
	case "restart":
		return teamsServiceRunPowerShell(ctx, resolve+"Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; "+teamsServiceWSLEnableStartTaskAndVerifyPowerShell())
	default:
		return nil, fmt.Errorf("unsupported primary Teams service action for WSL Task Scheduler: %s", action)
	}
}

func teamsServiceRunSystemctl(ctx context.Context, args ...string) ([]byte, error) {
	fullArgs := append([]string{"--user"}, args...)
	data, err := teamsServiceRunCommandDirect(ctx, "systemctl", fullArgs...)
	if err != nil {
		if len(data) > 0 {
			return data, fmt.Errorf("systemctl %s failed: %w", strings.Join(fullArgs, " "), err)
		}
		return nil, fmt.Errorf("systemctl %s failed: %w", strings.Join(fullArgs, " "), err)
	}
	return data, nil
}

func teamsServiceRunLaunchctl(ctx context.Context, args ...string) ([]byte, error) {
	data, err := teamsServiceRunCommandDirect(ctx, "launchctl", args...)
	if err != nil {
		if len(data) > 0 {
			return data, fmt.Errorf("launchctl %s failed: %w", strings.Join(args, " "), err)
		}
		return nil, fmt.Errorf("launchctl %s failed: %w", strings.Join(args, " "), err)
	}
	return data, nil
}

func appendLaunchctlOutput(chunks ...[]byte) []byte {
	var out []byte
	for _, data := range chunks {
		if len(data) == 0 {
			continue
		}
		out = append(out, data...)
		if !bytes.HasSuffix(out, []byte("\n")) {
			out = append(out, '\n')
		}
	}
	return out
}

func teamsServiceRunPowerShell(ctx context.Context, command string) ([]byte, error) {
	args := []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", command}
	name := teamsServicePowerShellExecutable()
	data, err := teamsServiceRunCommandDirect(ctx, name, args...)
	if err != nil {
		detail := strings.TrimSpace(string(data))
		if detail != "" {
			return data, fmt.Errorf("%s %s failed: %w\n%s", name, strings.Join(args, " "), err, detail)
		}
		return nil, fmt.Errorf("%s %s failed: %w", name, strings.Join(args, " "), err)
	}
	return data, nil
}

func teamsServiceCurrentWindowsUser(ctx context.Context) (string, error) {
	data, err := teamsServiceRunPowerShell(ctx, "[System.Security.Principal.WindowsIdentity]::GetCurrent().Name")
	if err != nil {
		return "", err
	}
	user := strings.TrimSpace(string(data))
	if user == "" {
		return "", fmt.Errorf("current Windows user is empty")
	}
	return user, nil
}

func isTeamsServiceWindowsAccessDeniedError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(teamsServiceCommandFailureDetail(err))
	for _, needle := range []string{
		"access is denied",
		"0x80070005",
		"unauthorizedaccessexception",
		"permission denied",
		"permissiondenied",
		"accessdenied",
		"e_accessdenied",
		"拒绝访问",
		"存取被拒",
		"zugriff verweigert",
		"acceso denegado",
		"accès refusé",
		"accesso negato",
	} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func isTeamsServiceWindowsScheduledTasksUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(teamsServiceCommandFailureDetail(err))
	for _, needle := range []string{
		"register-scheduledtask' is not recognized",
		"get-scheduledtask' is not recognized",
		"scheduledtasks module could not be loaded",
		"module 'scheduledtasks' could not be loaded",
		"no valid module file was found",
		"not recognized as the name of a cmdlet",
	} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func teamsServiceCommandFailureDetail(err error) string {
	if err == nil {
		return ""
	}
	text := strings.TrimSpace(err.Error())
	if idx := strings.Index(text, " failed: "); idx >= 0 {
		if detail := strings.TrimSpace(text[idx+len(" failed: "):]); detail != "" {
			if firstNewline := strings.Index(detail, "\n"); firstNewline >= 0 {
				if commandOutput := strings.TrimSpace(detail[firstNewline+1:]); commandOutput != "" {
					return commandOutput
				}
			}
			return detail
		}
	}
	return text
}

func (b teamsServiceWSLWindowsTaskBackend) canSkipScheduledTaskRetireForStartupFallback(err error) bool {
	if !isTeamsServiceWindowsScheduledTasksUnavailableError(err) {
		return false
	}
	path, pathErr := b.Path()
	if pathErr != nil {
		return false
	}
	_, statErr := os.Stat(path)
	return os.IsNotExist(statErr)
}

func teamsServiceBootstrapErrorSummary(err error) string {
	if err == nil {
		return "unknown error"
	}
	if isTeamsServiceWindowsAccessDeniedError(err) {
		return "Windows denied permission to create or repair the current-user Scheduled Task."
	}
	summary := strings.TrimSpace(err.Error())
	summary = strings.Join(strings.Fields(summary), " ")
	if summary == "" {
		return "unknown error"
	}
	const maxLen = 320
	if len(summary) <= maxLen {
		return summary
	}
	return strings.TrimSpace(summary[:maxLen]) + "..."
}

func printTeamsServiceBootstrapTaskFallback(out io.Writer, reason string) {
	if out == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "Windows Scheduled Task setup could not be completed."
	}
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, "NOTICE: USING STARTUP WATCHDOG FALLBACK")
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, reason)
	_, _ = fmt.Fprintln(out, "Installing the current-user Windows Startup watchdog fallback instead.")
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out)
}

func confirmTeamsServiceUACPrompt(in io.Reader, out io.Writer, assumeYes bool) bool {
	if out == nil {
		out = io.Discard
	}
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, "NEXT STEP: TYPE yes TO CONTINUE")
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, "Windows needs permission to create or repair the current-user Scheduled Task.")
	_, _ = fmt.Fprintln(out, "The task targets only the current Windows user and uses least privilege.")
	if assumeYes {
		_, _ = fmt.Fprintln(out, "UAC prompt approved by --yes.")
		_, _ = fmt.Fprintln(out, "============================================================")
		_, _ = fmt.Fprintln(out)
		return true
	}
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprint(out, "Type yes and press Enter: ")
	if in == nil {
		in = os.Stdin
	}
	if !teamsServiceUACPromptInputAllowed(in) {
		_, _ = fmt.Fprintln(out)
		_, _ = fmt.Fprintln(out, "UAC prompt was not confirmed because stdin is non-interactive.")
		_, _ = fmt.Fprintln(out, "============================================================")
		_, _ = fmt.Fprintln(out)
		return false
	}
	var answer string
	if _, err := fmt.Fscan(in, &answer); err != nil {
		_, _ = fmt.Fprintln(out)
		_, _ = fmt.Fprintln(out, "UAC prompt was not confirmed.")
		_, _ = fmt.Fprintln(out, "============================================================")
		_, _ = fmt.Fprintln(out)
		return false
	}
	_, _ = fmt.Fprintln(out)
	confirmed := strings.EqualFold(strings.TrimSpace(answer), "yes") || strings.EqualFold(strings.TrimSpace(answer), "y")
	if !confirmed {
		_, _ = fmt.Fprintln(out, "UAC prompt was not confirmed.")
	}
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out)
	return confirmed
}

func teamsServiceUACPromptInputAllowed(in io.Reader) bool {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) != "" {
		return false
	}
	if f, ok := in.(*os.File); ok {
		return isTerminalFile(f)
	}
	return true
}

func defaultTeamsServicePowerShellExecutable() string {
	if path, err := exec.LookPath("powershell.exe"); err == nil && strings.TrimSpace(path) != "" {
		return path
	}
	if teamsServiceGOOS() == "linux" && teamsServiceIsWSL() {
		const windowsPowerShell = "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
		if info, err := os.Stat(windowsPowerShell); err == nil && !info.IsDir() {
			return windowsPowerShell
		}
	}
	return "powershell.exe"
}

func defaultTeamsServiceOpenURL(ctx context.Context, rawURL string) error {
	name, args, err := teamsServiceOpenURLCommand(rawURL)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, name, args...)
	if err := cmd.Start(); err != nil {
		return err
	}
	if cmd.Process != nil {
		if err := cmd.Process.Release(); err != nil {
			return err
		}
	}
	return nil
}

func teamsServiceOpenURLCommand(rawURL string) (string, []string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", nil, fmt.Errorf("Teams control chat URL is empty")
	}
	switch teamsServiceGOOS() {
	case "windows":
		return "rundll32.exe", []string{"url.dll,FileProtocolHandler", rawURL}, nil
	case "darwin":
		return "open", []string{rawURL}, nil
	case "linux":
		if teamsServiceIsWSL() {
			if _, err := os.Stat("/mnt/c/Windows/System32/cmd.exe"); err == nil {
				return "/mnt/c/Windows/System32/cmd.exe", []string{"/C", "start", "", rawURL}, nil
			}
			return "cmd.exe", []string{"/C", "start", "", rawURL}, nil
		}
		return "xdg-open", []string{rawURL}, nil
	default:
		return "", nil, fmt.Errorf("automatic URL opening is not supported on %s", teamsServiceGOOS())
	}
}

func teamsServiceRunCommandDirect(ctx context.Context, name string, args ...string) ([]byte, error) {
	runner := teamsServiceSystemctl
	if runner == nil {
		runner = teamsServiceExecRunner{}
	}
	return runner.Run(ctx, name, args...)
}

func buildTeamsServiceSpec(registryPath *string, buildOptions ...teamsServiceSpecBuildOption) (teamsServiceSpec, error) {
	exe, err := teamsServiceExecutable()
	if err != nil {
		return teamsServiceSpec{}, err
	}
	resolvedExe, err := helperpath.StableRunnablePathFromSources(exe, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()})
	if err != nil {
		return teamsServiceSpec{}, err
	}
	exe = resolvedExe.Path
	if err := validateTeamsServiceExecutable(exe); err != nil {
		return teamsServiceSpec{}, err
	}
	exe, err = filepath.Abs(exe)
	if err != nil {
		return teamsServiceSpec{}, err
	}
	cwd, err := teamsServiceGetwd()
	if err != nil {
		return teamsServiceSpec{}, err
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return teamsServiceSpec{}, err
	}
	var resolvedRegistryPath string
	if registryPath != nil && strings.TrimSpace(*registryPath) != "" {
		resolvedRegistryPath = strings.TrimSpace(*registryPath)
		if !filepath.IsAbs(resolvedRegistryPath) {
			resolvedRegistryPath = filepath.Join(cwd, resolvedRegistryPath)
		}
	}
	env, err := teamsServiceEnvironmentForWorkingDir(cwd)
	if err != nil {
		return teamsServiceSpec{}, err
	}
	var opts teamsServiceSpecBuildOptions
	for _, apply := range buildOptions {
		if apply != nil {
			apply(&opts)
		}
	}
	for name, value := range opts.Environment {
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if name == "" || value == "" || !teamsServiceEnvironmentValueAllowed(name, value) {
			continue
		}
		env[name] = value
	}
	env[update.EnvInstallDir] = exe
	return teamsServiceSpec{
		Executable:   exe,
		WorkingDir:   cwd,
		RegistryPath: resolvedRegistryPath,
		Environment:  env,
	}, nil
}

func validateTeamsServiceExecutable(exe string) error {
	clean := filepath.Clean(strings.TrimSpace(exe))
	if clean == "" {
		return fmt.Errorf("cannot install Teams service: executable path is empty")
	}
	parts := strings.FieldsFunc(clean, func(r rune) bool {
		return r == filepath.Separator || r == '/' || r == '\\'
	})
	for _, part := range parts {
		if strings.HasPrefix(part, "go-build") {
			return fmt.Errorf("cannot install Teams service from a temporary go run binary (%s); install codex-proxy or pass a stable binary on PATH, then run `codex-proxy teams service install`", exe)
		}
	}
	return nil
}

func teamsServiceEnvironment() map[string]string {
	env, err := teamsServiceEnvironmentForWorkingDir("")
	if err != nil {
		return map[string]string{
			"NO_COLOR":                        "1",
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
		}
	}
	return env
}

func teamsServiceEnvironmentForWorkingDir(workingDir string) (map[string]string, error) {
	env := map[string]string{
		"NO_COLOR":                        "1",
		"CODEX_HELPER_TEAMS_SERVICE":      "1",
		"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
	}
	for _, name := range teamsServiceEnvironmentAllowlist() {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			if !teamsServiceEnvironmentValueAllowed(name, value) {
				continue
			}
			env[name] = value
		}
	}
	for name, value := range teamsServiceExistingEnvironment() {
		if strings.TrimSpace(env[name]) != "" || !teamsServiceEnvironmentValueAllowed(name, value) {
			continue
		}
		env[name] = value
	}
	codexHomeEnv := strings.TrimSpace(env[envCodexHome])
	codexDirEnv := strings.TrimSpace(env["CODEX_DIR"])
	switch {
	case codexHomeEnv != "" || codexDirEnv != "":
		if codexHomeEnv == "" {
			codexHomeEnv = codexDirEnv
		}
		if codexDirEnv == "" {
			codexDirEnv = codexHomeEnv
		}
		env[envCodexHome] = normalizeTeamsServiceCodexHomeEnv(codexHomeEnv, workingDir)
		env["CODEX_DIR"] = normalizeTeamsServiceCodexHomeEnv(codexDirEnv, workingDir)
	case codexHomeEnv == "" && codexDirEnv == "":
		codexHome, err := resolveCodexHome("", workingDir)
		if err == nil && strings.TrimSpace(codexHome) != "" {
			codexHome = strings.TrimSpace(codexHome)
			env[envCodexHome] = codexHome
			env["CODEX_DIR"] = codexHome
		}
	}
	return env, nil
}

func normalizeTeamsServiceCodexHomeEnv(value string, workingDir string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if !filepath.IsAbs(value) && strings.TrimSpace(workingDir) != "" {
		value = filepath.Join(workingDir, value)
	}
	if abs, err := filepath.Abs(value); err == nil {
		value = abs
	}
	return value
}

func teamsServiceExistingEnvironment() map[string]string {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return nil
	}
	path, err := backend.Path()
	if err != nil {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	switch backend.(type) {
	case teamsServiceSystemdBackend:
		return parseTeamsServiceSystemdEnvironment(string(data))
	case teamsServiceLaunchAgentBackend:
		return parseTeamsServiceLaunchAgentEnvironment(string(data))
	case teamsServiceLocalSupervisorBackend:
		cfg, err := readTeamsServiceLocalSupervisorConfig(path)
		if err != nil {
			return nil
		}
		return cfg.Spec.Environment
	default:
		return nil
	}
}

func teamsServiceEnvironmentValueAllowed(name string, value string) bool {
	if !teamsServiceEnvironmentNameAllowed(name) {
		return false
	}
	return !teamsServiceShouldDropProxyEnv(name, value)
}

func teamsServiceEnvironmentNameAllowed(name string) bool {
	for _, allowed := range teamsServiceEnvironmentAllowlist() {
		if name == allowed {
			return true
		}
	}
	return false
}

func teamsServiceShouldDropProxyEnv(name string, value string) bool {
	if !teamsServiceProxyEnvName(name) {
		return false
	}
	if teamsServiceProxyHasCredentials(value) {
		return true
	}
	if !teamsServiceDropLocalProxyEnv() {
		return false
	}
	return teamsServiceProxyIsLoopback(value)
}

func parseTeamsServiceSystemdEnvironment(data string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Environment=") {
			continue
		}
		item := strings.TrimSpace(strings.TrimPrefix(line, "Environment="))
		if unquoted, err := strconv.Unquote(item); err == nil {
			item = unquoted
		}
		key, value, ok := strings.Cut(item, "=")
		if !ok || strings.TrimSpace(key) == "" {
			continue
		}
		out[strings.TrimSpace(key)] = value
	}
	return out
}

func parseTeamsServiceLaunchAgentEnvironment(data string) map[string]string {
	out := make(map[string]string)
	inEnv := false
	var pendingKey string
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "<key>EnvironmentVariables</key>") {
			inEnv = true
			pendingKey = ""
			continue
		}
		if !inEnv {
			continue
		}
		if strings.Contains(line, "</dict>") {
			break
		}
		if strings.HasPrefix(line, "<key>") && strings.Contains(line, "</key>") {
			pendingKey = html.UnescapeString(strings.TrimSuffix(strings.TrimPrefix(line, "<key>"), "</key>"))
			continue
		}
		if pendingKey != "" && strings.HasPrefix(line, "<string>") && strings.Contains(line, "</string>") {
			out[pendingKey] = html.UnescapeString(strings.TrimSuffix(strings.TrimPrefix(line, "<string>"), "</string>"))
			pendingKey = ""
		}
	}
	return out
}

func teamsServiceProxyEnvName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "http_proxy", "https_proxy", "all_proxy":
		return true
	default:
		return false
	}
}

func teamsServiceKeepLocalProxyEnv() bool {
	return !teamsServiceDropLocalProxyEnv()
}

func teamsServiceDropLocalProxyEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_KEEP_LOCAL_PROXY"))) {
	case "1", "true", "yes", "on":
		return false
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_DROP_LOCAL_PROXY"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func teamsServiceProxyIsLoopback(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	host := value
	if parsed, err := url.Parse(value); err == nil && parsed.Host != "" {
		host = parsed.Hostname()
	}
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func teamsServiceProxyHasCredentials(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.User == nil {
		if strings.Contains(value, "://") {
			return false
		}
		head := value
		if idx := strings.IndexAny(head, "/?#"); idx >= 0 {
			head = head[:idx]
		}
		userInfo, _, hasUserInfo := strings.Cut(head, "@")
		return hasUserInfo && strings.TrimSpace(userInfo) != ""
	}
	if parsed.User.Username() != "" {
		return true
	}
	_, hasPassword := parsed.User.Password()
	return hasPassword
}

func teamsServiceEnvironmentAllowlist() []string {
	names := []string{
		"CODEX_HOME",
		"CODEX_DIR",
		"CODEX_HELPER_CONFIG",
		"CODEX_HELPER_TEAMS_AUTH_CONFIG",
		"CODEX_HELPER_TEAMS_PROFILE",
		"CODEX_HELPER_TEAMS_AUTH_PROFILE",
		"CODEX_HELPER_TEAMS_MACHINE_ID",
		"CODEX_HELPER_TEAMS_MACHINE_LABEL",
		"CODEX_HELPER_TEAMS_MACHINE_KIND",
		"CODEX_HELPER_TEAMS_MACHINE_PRIORITY",
		envTeamsASRCommand,
		envTeamsASRArgsJSON,
		envTeamsASRBackend,
		envTeamsASRLlamaBinary,
		envTeamsASRLlamaModel,
		envTeamsASRLlamaMMProj,
		envTeamsASRLlamaDevice,
		envTeamsASRFFmpeg,
		"CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS",
		"CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES",
		"CODEX_HELPER_TEAMS_READ_TOKEN_CACHE",
		"CODEX_HELPER_TEAMS_READ_TENANT_ID",
		"CODEX_HELPER_TEAMS_READ_CLIENT_ID",
		"CODEX_HELPER_TEAMS_READ_SCOPES",
		"CODEX_HELPER_TEAMS_TOKEN_CACHE",
		"CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE",
		"CODEX_HELPER_TEAMS_TENANT_ID",
		"CODEX_HELPER_TEAMS_CLIENT_ID",
		"CODEX_HELPER_TEAMS_SCOPES",
		"CODEX_HELPER_TEAMS_FILE_WRITE_TENANT_ID",
		"CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID",
		"CODEX_HELPER_TEAMS_FILE_WRITE_SCOPES",
		"CODEX_HELPER_TEAMS_FULL_TENANT_ID",
		"CODEX_HELPER_TEAMS_FULL_CLIENT_ID",
		"CODEX_HELPER_TEAMS_FULL_SCOPES",
		"CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE",
		"CODEX_HELPER_BEACON_STORE",
		beacon.BeaconSlurmQueryCommandEnv,
		beacon.BeaconSlurmSubmitCommandEnv,
		beacon.BeaconSlurmCancelCommandEnv,
		beacon.BeaconSlurmRenewCommandEnv,
		beacon.BeaconLSFQueryCommandEnv,
		beacon.BeaconLSFSubmitCommandEnv,
		beacon.BeaconLSFCancelCommandEnv,
		beacon.BeaconLSFRenewCommandEnv,
		beacon.BeaconProviderShellModeEnv,
		update.EnvRepo,
		update.EnvUpdateIndexURL,
		"HTTP_PROXY",
		"HTTPS_PROXY",
		"ALL_PROXY",
		"NO_PROXY",
		"http_proxy",
		"https_proxy",
		"all_proxy",
		"no_proxy",
	}
	for _, providerID := range modelprofile.ProviderIDs() {
		spec, ok := modelprofile.LookupProvider(providerID)
		if ok && strings.TrimSpace(spec.RecommendedEnv) != "" {
			names = append(names, strings.TrimSpace(spec.RecommendedEnv))
		}
	}
	return names
}

func sortedEnvironmentKeys(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key, value := range env {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func buildTeamsServiceRunArgs(spec teamsServiceSpec) []string {
	args := []string{
		"teams",
		"run",
		"--owner-stale-after",
		teamsServiceRunOwnerStaleAfter.String(),
		"--auto-service=false",
	}
	if spec.RegistryPath != "" {
		args = append(args, "--registry", spec.RegistryPath)
	}
	return args
}

func buildTeamsServiceWatchdogArgs() []string {
	return []string{
		"teams",
		"service",
		"watchdog",
		"--loop",
		"--interval",
		teamsServiceExternalWatchdogInterval.String(),
		"--quiet",
	}
}

func buildTeamsServiceUnit(spec teamsServiceSpec) string {
	args := []string{systemdQuoteArg(spec.Executable)}
	for _, arg := range buildTeamsServiceRunArgs(spec) {
		args = append(args, systemdQuoteArg(arg))
	}
	execStart := strings.Join(args, " ")

	var b strings.Builder
	b.WriteString("[Unit]\n")
	b.WriteString("Description=Codex Helper Teams bridge\n")
	b.WriteString("\n")
	b.WriteString("[Service]\n")
	b.WriteString("Type=simple\n")
	b.WriteString("WorkingDirectory=")
	b.WriteString(systemdQuoteArg(spec.WorkingDir))
	b.WriteString("\n")
	b.WriteString("ExecStart=")
	b.WriteString(execStart)
	b.WriteString("\n")
	b.WriteString("Restart=on-failure\n")
	b.WriteString("RestartSec=10s\n")
	for _, key := range sortedEnvironmentKeys(spec.Environment) {
		b.WriteString("Environment=")
		b.WriteString(systemdQuoteArg(key + "=" + spec.Environment[key]))
		b.WriteString("\n")
	}
	b.WriteString("\n")
	b.WriteString("[Install]\n")
	b.WriteString("WantedBy=default.target\n")
	return b.String()
}

func buildTeamsServiceWatchdogUnit(spec teamsServiceSpec) string {
	args := []string{systemdQuoteArg(spec.Executable)}
	for _, arg := range buildTeamsServiceWatchdogArgs() {
		args = append(args, systemdQuoteArg(arg))
	}
	execStart := strings.Join(args, " ")
	var b strings.Builder
	b.WriteString("[Unit]\n")
	b.WriteString("Description=Codex Helper Teams service watchdog\n")
	b.WriteString("\n")
	b.WriteString("[Service]\n")
	b.WriteString("Type=simple\n")
	b.WriteString("WorkingDirectory=")
	b.WriteString(systemdQuoteArg(spec.WorkingDir))
	b.WriteString("\n")
	b.WriteString("ExecStart=")
	b.WriteString(execStart)
	b.WriteString("\n")
	b.WriteString("Restart=on-failure\n")
	b.WriteString("RestartSec=10s\n")
	for _, key := range sortedEnvironmentKeys(spec.Environment) {
		b.WriteString("Environment=")
		b.WriteString(systemdQuoteArg(key + "=" + spec.Environment[key]))
		b.WriteString("\n")
	}
	b.WriteString("\n")
	b.WriteString("[Install]\n")
	b.WriteString("WantedBy=default.target\n")
	return b.String()
}

func buildTeamsServiceWatchdogTimer() string {
	var b strings.Builder
	b.WriteString("[Unit]\n")
	b.WriteString("Description=Codex Helper Teams service watchdog timer\n")
	b.WriteString("\n")
	b.WriteString("[Timer]\n")
	b.WriteString("OnBootSec=30s\n")
	b.WriteString("OnUnitActiveSec=1min\n")
	b.WriteString("AccuracySec=30s\n")
	b.WriteString("Persistent=true\n")
	b.WriteString("Unit=" + teamsServiceWatchdogUnitName + "\n")
	b.WriteString("\n")
	b.WriteString("[Install]\n")
	b.WriteString("WantedBy=timers.target\n")
	return b.String()
}

func buildTeamsServiceLaunchAgentPlist(spec teamsServiceSpec) string {
	args := append([]string{spec.Executable}, buildTeamsServiceRunArgs(spec)...)
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	b.WriteString(`<plist version="1.0">` + "\n")
	b.WriteString("<dict>\n")
	b.WriteString("\t<key>Label</key>\n\t<string>" + xmlEscape(teamsServiceLaunchAgentLabel) + "</string>\n")
	b.WriteString("\t<key>Disabled</key>\n\t<true/>\n")
	b.WriteString("\t<key>ProgramArguments</key>\n\t<array>\n")
	for _, arg := range args {
		b.WriteString("\t\t<string>" + xmlEscape(arg) + "</string>\n")
	}
	b.WriteString("\t</array>\n")
	b.WriteString("\t<key>WorkingDirectory</key>\n\t<string>" + xmlEscape(spec.WorkingDir) + "</string>\n")
	b.WriteString("\t<key>RunAtLoad</key>\n\t<true/>\n")
	b.WriteString("\t<key>KeepAlive</key>\n\t<dict>\n")
	b.WriteString("\t\t<key>SuccessfulExit</key>\n\t\t<false/>\n")
	b.WriteString("\t</dict>\n")
	b.WriteString("\t<key>EnvironmentVariables</key>\n\t<dict>\n")
	for _, key := range sortedEnvironmentKeys(spec.Environment) {
		b.WriteString("\t\t<key>" + xmlEscape(key) + "</key>\n\t\t<string>" + xmlEscape(spec.Environment[key]) + "</string>\n")
	}
	b.WriteString("\t</dict>\n")
	b.WriteString("</dict>\n")
	b.WriteString("</plist>\n")
	return b.String()
}

func buildTeamsServiceWatchdogLaunchAgentPlist(spec teamsServiceSpec) string {
	args := append([]string{spec.Executable}, buildTeamsServiceWatchdogArgs()...)
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	b.WriteString(`<plist version="1.0">` + "\n")
	b.WriteString("<dict>\n")
	b.WriteString("\t<key>Label</key>\n\t<string>" + xmlEscape(teamsServiceLaunchAgentWatchdogLabel) + "</string>\n")
	b.WriteString("\t<key>Disabled</key>\n\t<true/>\n")
	b.WriteString("\t<key>ProgramArguments</key>\n\t<array>\n")
	for _, arg := range args {
		b.WriteString("\t\t<string>" + xmlEscape(arg) + "</string>\n")
	}
	b.WriteString("\t</array>\n")
	b.WriteString("\t<key>WorkingDirectory</key>\n\t<string>" + xmlEscape(spec.WorkingDir) + "</string>\n")
	b.WriteString("\t<key>RunAtLoad</key>\n\t<true/>\n")
	b.WriteString("\t<key>KeepAlive</key>\n\t<dict>\n")
	b.WriteString("\t\t<key>SuccessfulExit</key>\n\t\t<false/>\n")
	b.WriteString("\t</dict>\n")
	b.WriteString("\t<key>EnvironmentVariables</key>\n\t<dict>\n")
	for _, key := range sortedEnvironmentKeys(spec.Environment) {
		b.WriteString("\t\t<key>" + xmlEscape(key) + "</key>\n\t\t<string>" + xmlEscape(spec.Environment[key]) + "</string>\n")
	}
	b.WriteString("\t</dict>\n")
	b.WriteString("</dict>\n")
	b.WriteString("</plist>\n")
	return b.String()
}

func buildTeamsServiceWindowsTaskXML(spec teamsServiceSpec) string {
	return buildTeamsServiceWindowsTaskXMLWithPrincipalUser(spec, "")
}

func buildTeamsServiceWindowsTaskXMLWithPrincipalUser(spec teamsServiceSpec, principalUser string) string {
	args := buildTeamsServiceRunArgs(spec)
	command := spec.Executable
	arguments := windowsCommandLine(args)
	if launcher := strings.TrimSpace(spec.WindowsTaskLauncherPath); launcher != "" {
		command = "wscript.exe"
		arguments = windowsCommandLine([]string{"//B", "//Nologo", launcher})
	} else if len(spec.Environment) > 0 {
		command = "powershell.exe"
		arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -Command " + windowsQuoteArg(buildTeamsServiceWindowsPowerShell(spec, args))
	}
	var b strings.Builder
	// Register-ScheduledTask -Xml receives this as a PowerShell/.NET string.
	// An explicit encoding declaration makes Task Scheduler try to switch away
	// from that string encoding and fail with "unable to switch the encoding".
	b.WriteString(`<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">` + "\n")
	b.WriteString("  <RegistrationInfo>\n")
	b.WriteString("    <Description>Codex Helper Teams bridge</Description>\n")
	b.WriteString("  </RegistrationInfo>\n")
	b.WriteString("  <Triggers>\n")
	b.WriteString("    <LogonTrigger>\n")
	b.WriteString("      <Enabled>true</Enabled>\n")
	b.WriteString("    </LogonTrigger>\n")
	b.WriteString("  </Triggers>\n")
	b.WriteString("  <Principals>\n")
	b.WriteString("    <Principal id=\"Author\">\n")
	if principalUser = strings.TrimSpace(principalUser); principalUser != "" {
		b.WriteString("      <UserId>" + xmlEscape(principalUser) + "</UserId>\n")
	}
	b.WriteString("      <LogonType>InteractiveToken</LogonType>\n")
	b.WriteString("      <RunLevel>LeastPrivilege</RunLevel>\n")
	b.WriteString("    </Principal>\n")
	b.WriteString("  </Principals>\n")
	b.WriteString("  <Settings>\n")
	b.WriteString("    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n")
	b.WriteString("    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n")
	b.WriteString("    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n")
	b.WriteString("    <AllowHardTerminate>true</AllowHardTerminate>\n")
	b.WriteString("    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>\n")
	b.WriteString("    <StartWhenAvailable>true</StartWhenAvailable>\n")
	b.WriteString("    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>\n")
	b.WriteString("    <Hidden>true</Hidden>\n")
	b.WriteString("    <Enabled>false</Enabled>\n")
	b.WriteString("    <RestartOnFailure>\n")
	b.WriteString("      <Interval>PT" + strconv.Itoa(teamsServiceTaskSchedulerRestartMinutes) + "M</Interval>\n")
	b.WriteString("      <Count>" + strconv.Itoa(teamsServiceTaskRestartCount) + "</Count>\n")
	b.WriteString("    </RestartOnFailure>\n")
	b.WriteString("  </Settings>\n")
	b.WriteString("  <Actions Context=\"Author\">\n")
	b.WriteString("    <Exec>\n")
	b.WriteString("      <Command>" + xmlEscape(command) + "</Command>\n")
	b.WriteString("      <Arguments>" + xmlEscape(arguments) + "</Arguments>\n")
	b.WriteString("      <WorkingDirectory>" + xmlEscape(spec.WorkingDir) + "</WorkingDirectory>\n")
	b.WriteString("    </Exec>\n")
	b.WriteString("  </Actions>\n")
	b.WriteString("</Task>\n")
	return b.String()
}

func buildTeamsServiceWindowsWatchdogTaskXML(spec teamsServiceSpec) string {
	return buildTeamsServiceWindowsWatchdogTaskXMLWithPrincipalUser(spec, "")
}

func buildTeamsServiceWindowsWatchdogTaskXMLWithPrincipalUser(spec teamsServiceSpec, principalUser string) string {
	args := buildTeamsServiceWatchdogArgs()
	command := spec.Executable
	arguments := windowsCommandLine(args)
	if launcher := strings.TrimSpace(spec.WindowsWatchdogLauncherPath); launcher != "" {
		command = "wscript.exe"
		arguments = windowsCommandLine([]string{"//B", "//Nologo", launcher})
	} else if len(spec.Environment) > 0 {
		command = "powershell.exe"
		arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -Command " + windowsQuoteArg(buildTeamsServiceWindowsPowerShell(spec, args))
	}
	var b strings.Builder
	b.WriteString(`<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">` + "\n")
	b.WriteString("  <RegistrationInfo>\n")
	b.WriteString("    <Description>Codex Helper Teams service watchdog</Description>\n")
	b.WriteString("  </RegistrationInfo>\n")
	b.WriteString("  <Triggers>\n")
	b.WriteString("    <LogonTrigger>\n")
	b.WriteString("      <Enabled>true</Enabled>\n")
	b.WriteString("    </LogonTrigger>\n")
	b.WriteString("    <CalendarTrigger>\n")
	b.WriteString("      <Repetition>\n")
	b.WriteString("        <Interval>PT" + strconv.Itoa(teamsServiceExternalWatchdogMinutes) + "M</Interval>\n")
	b.WriteString("        <Duration>P" + strconv.Itoa(teamsServiceWatchdogDays) + "D</Duration>\n")
	b.WriteString("        <StopAtDurationEnd>false</StopAtDurationEnd>\n")
	b.WriteString("      </Repetition>\n")
	b.WriteString("      <StartBoundary>2026-01-01T00:00:00</StartBoundary>\n")
	b.WriteString("      <Enabled>true</Enabled>\n")
	b.WriteString("      <ScheduleByDay>\n")
	b.WriteString("        <DaysInterval>1</DaysInterval>\n")
	b.WriteString("      </ScheduleByDay>\n")
	b.WriteString("    </CalendarTrigger>\n")
	b.WriteString("  </Triggers>\n")
	b.WriteString("  <Principals>\n")
	b.WriteString("    <Principal id=\"Author\">\n")
	if principalUser = strings.TrimSpace(principalUser); principalUser != "" {
		b.WriteString("      <UserId>" + xmlEscape(principalUser) + "</UserId>\n")
	}
	b.WriteString("      <LogonType>InteractiveToken</LogonType>\n")
	b.WriteString("      <RunLevel>LeastPrivilege</RunLevel>\n")
	b.WriteString("    </Principal>\n")
	b.WriteString("  </Principals>\n")
	b.WriteString("  <Settings>\n")
	b.WriteString("    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n")
	b.WriteString("    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n")
	b.WriteString("    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n")
	b.WriteString("    <AllowHardTerminate>true</AllowHardTerminate>\n")
	b.WriteString("    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>\n")
	b.WriteString("    <StartWhenAvailable>true</StartWhenAvailable>\n")
	b.WriteString("    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>\n")
	b.WriteString("    <Hidden>true</Hidden>\n")
	b.WriteString("    <Enabled>false</Enabled>\n")
	b.WriteString("    <RestartOnFailure>\n")
	b.WriteString("      <Interval>PT" + strconv.Itoa(teamsServiceTaskSchedulerRestartMinutes) + "M</Interval>\n")
	b.WriteString("      <Count>" + strconv.Itoa(teamsServiceTaskRestartCount) + "</Count>\n")
	b.WriteString("    </RestartOnFailure>\n")
	b.WriteString("  </Settings>\n")
	b.WriteString("  <Actions Context=\"Author\">\n")
	b.WriteString("    <Exec>\n")
	b.WriteString("      <Command>" + xmlEscape(command) + "</Command>\n")
	b.WriteString("      <Arguments>" + xmlEscape(arguments) + "</Arguments>\n")
	b.WriteString("      <WorkingDirectory>" + xmlEscape(spec.WorkingDir) + "</WorkingDirectory>\n")
	b.WriteString("    </Exec>\n")
	b.WriteString("  </Actions>\n")
	b.WriteString("</Task>\n")
	return b.String()
}

func buildTeamsServiceWSLArguments(spec teamsServiceSpec) []string {
	var args []string
	if distro := strings.TrimSpace(teamsServiceWSLDistroName()); distro != "" {
		args = append(args, "-d", distro)
	}
	if linuxUser := strings.TrimSpace(teamsServiceWSLLinuxUserName()); linuxUser != "" {
		args = append(args, "-u", linuxUser)
	}
	if strings.TrimSpace(spec.WorkingDir) != "" {
		args = append(args, "--cd", spec.WorkingDir)
	}
	// Use --exec so WSL runs env directly instead of passing the command line
	// through the user's login shell. zsh NOMATCH would otherwise reject
	// unquoted proxy globs such as NO_PROXY=*.example.com before helper starts.
	args = append(args, "--exec", "env")
	for _, key := range sortedEnvironmentKeys(spec.Environment) {
		args = append(args, key+"="+spec.Environment[key])
	}
	args = append(args, spec.Executable, "teams", "run", "--owner-stale-after", teamsServiceRunOwnerStaleAfter.String(), "--auto-service=false")
	if spec.RegistryPath != "" {
		args = append(args, "--registry", spec.RegistryPath)
	}
	return args
}

func teamsServiceSpecWithWindowsTaskLaunchers(spec teamsServiceSpec, bridgeXMLPath string, watchdogXMLPath string) teamsServiceSpec {
	if strings.TrimSpace(spec.WindowsTaskLauncherPath) == "" {
		spec.WindowsTaskLauncherPath = strings.TrimSuffix(bridgeXMLPath, filepath.Ext(bridgeXMLPath)) + ".vbs"
	}
	if strings.TrimSpace(spec.WindowsWatchdogLauncherPath) == "" {
		spec.WindowsWatchdogLauncherPath = strings.TrimSuffix(watchdogXMLPath, filepath.Ext(watchdogXMLPath)) + ".vbs"
	}
	return spec
}

func writeTeamsServiceWindowsTaskLauncherFiles(vbsPath string, spec teamsServiceSpec, args []string) error {
	vbsPath = strings.TrimSpace(vbsPath)
	if vbsPath == "" {
		return nil
	}
	psPath := strings.TrimSuffix(vbsPath, filepath.Ext(vbsPath)) + ".ps1"
	if err := os.MkdirAll(filepath.Dir(vbsPath), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(psPath, []byte(buildTeamsServiceWindowsPowerShell(spec, args)), 0o600); err != nil {
		return err
	}
	return os.WriteFile(vbsPath, []byte(buildTeamsServiceWindowsTaskLauncherVBS(psPath)), 0o600)
}

func removeTeamsServiceWindowsTaskLauncherFiles() {
	dir, err := teamsServiceWindowsTaskXMLDir()
	if err != nil {
		return
	}
	for _, name := range []string{
		"codex-helper-teams-task.ps1",
		"codex-helper-teams-task.vbs",
		"codex-helper-teams-watchdog-task.ps1",
		"codex-helper-teams-watchdog-task.vbs",
	} {
		_ = os.Remove(filepath.Join(dir, name))
	}
}

func buildTeamsServiceWindowsTaskLauncherVBS(psPath string) string {
	psPath = strings.ReplaceAll(psPath, `"`, `""`)
	lines := []string{
		"Function Q(s)",
		"  Q = Chr(34) & s & Chr(34)",
		"End Function",
		"Set shell = CreateObject(\"WScript.Shell\")",
		"ps = shell.ExpandEnvironmentStrings(\"%SystemRoot%\\System32\\WindowsPowerShell\\v1.0\\powershell.exe\")",
		"cmd = Q(ps) & \" -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File \" & Q(\"" + psPath + "\")",
		"code = shell.Run(cmd, 0, True)",
		"WScript.Quit code",
	}
	return strings.Join(lines, "\r\n")
}

func buildTeamsServiceWSLWatchdogArguments(spec teamsServiceSpec) []string {
	args := buildTeamsServiceWSLArguments(spec)
	for i := 0; i+2 < len(args); i++ {
		if args[i] == spec.Executable && args[i+1] == "teams" && args[i+2] == "run" {
			out := append([]string{}, args[:i]...)
			out = append(out, spec.Executable)
			out = append(out, buildTeamsServiceWatchdogArgs()...)
			return out
		}
	}
	out := append([]string{}, args...)
	out = append(out, buildTeamsServiceWatchdogArgs()...)
	return out
}

type teamsServiceWSLRegisterOptions struct {
	ForceDisabled   bool
	Enable          bool
	Start           bool
	PreserveEnabled bool
	PreserveRunning bool
	PrincipalUser   string
	CleanLegacy     bool
}

func buildTeamsServiceWSLRegisterCommand(taskName string, args []string, opts teamsServiceWSLRegisterOptions) string {
	task := powershellSingleQuote(taskName)
	principalUser := "[System.Security.Principal.WindowsIdentity]::GetCurrent().Name"
	if strings.TrimSpace(opts.PrincipalUser) != "" {
		principalUser = powershellSingleQuote(strings.TrimSpace(opts.PrincipalUser))
	}
	verifyAction := "$registered = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop; " +
		"$registeredTasks = @($registered); " +
		"$registered = $registeredTasks[0]; " +
		"$actualActions = @($registered.Actions); " +
		"$actualAction = if ($actualActions.Count -gt 0) { $actualActions[0] } else { $null }; " +
		"$actionMatches = ($null -ne $actualAction -and $actualAction.Execute -eq $expectedActionExecute -and $actualAction.Arguments -eq $expectedActionArgument); "
	cmd := "$taskName = " + task + "; "
	if opts.CleanLegacy {
		prefix := teamsServiceWSLTaskNamePrefix(taskName)
		cmd += "$legacyPrefix = " + powershellSingleQuote(prefix) + "; " +
			"Get-ScheduledTask | Where-Object { $_.TaskName -like ($legacyPrefix + '*') -and $_.TaskName -ne $taskName } | ForEach-Object { Stop-ScheduledTask -TaskName $_.TaskName -ErrorAction SilentlyContinue; Unregister-ScheduledTask -TaskName $_.TaskName -Confirm:$false -ErrorAction SilentlyContinue }; "
	}
	cmd +=
		"$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; " +
			"$wasEnabled = $false; $wasRunning = $false; " +
			"if ($null -ne $existing) { $wasEnabled = $existing.State -ne 'Disabled'; $wasRunning = $existing.State -eq 'Running' }; " +
			buildTeamsServiceWSLTaskLauncherSetupPowerShell(taskName, args) +
			"$action = New-ScheduledTaskAction -Execute $expectedActionExecute -Argument $expectedActionArgument; " +
			"$logon = New-ScheduledTaskTrigger -AtLogOn; " +
			"$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -Hidden -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -RestartCount " + strconv.Itoa(teamsServiceTaskRestartCount) + " -RestartInterval (New-TimeSpan -Minutes " + strconv.Itoa(teamsServiceTaskSchedulerRestartMinutes) + "); " +
			"$principalUser = " + principalUser + "; " +
			"$principal = New-ScheduledTaskPrincipal -UserId $principalUser -LogonType Interactive -RunLevel Limited; " +
			"Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $logon -Settings $settings -Principal $principal -Force | Out-Null; " +
			verifyAction +
			"if (-not $actionMatches) { Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue; Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $logon -Settings $settings -Principal $principal -Force | Out-Null; " +
			verifyAction +
			"}; " +
			"if (-not $actionMatches) { throw 'Teams WSL Scheduled Task action did not refresh; access is denied or task is protected' }; "
	switch {
	case opts.ForceDisabled:
		cmd += "Disable-ScheduledTask -TaskName $taskName | Out-Null; "
	case opts.Enable:
		cmd += "Enable-ScheduledTask -TaskName $taskName | Out-Null; "
	case opts.PreserveEnabled:
		cmd += "if ($wasEnabled) { Enable-ScheduledTask -TaskName $taskName | Out-Null } else { Disable-ScheduledTask -TaskName $taskName | Out-Null }; "
	}
	switch {
	case opts.Start:
		cmd += teamsServiceWSLStartTaskAndVerifyPowerShell()
	case opts.PreserveRunning:
		cmd += "if ($wasRunning) { " + teamsServiceWSLStartTaskAndVerifyPowerShell() + " }"
	}
	return strings.TrimSpace(cmd)
}

type teamsServiceWSLTaskConfigMatchOptions struct{}

func teamsServiceWSLTaskConfigMatchHelpersPowerShell() string {
	return "function Test-CodexHelperTaskDurationMinutes { param($value, [double]$expectedMinutes) " +
		"if ($null -eq $value) { return $false }; " +
		"try { if ($value -is [TimeSpan]) { return [Math]::Abs($value.TotalMinutes - $expectedMinutes) -lt 0.01 } } catch { }; " +
		"$text = [string]$value; " +
		"if ([string]::IsNullOrWhiteSpace($text)) { return $false }; " +
		"if ($text -match '^PT(?<minutes>[0-9]+(?:\\.[0-9]+)?)M$') { return [Math]::Abs(([double]$Matches['minutes']) - $expectedMinutes) -lt 0.01 }; " +
		"if ($text -match '^PT(?<seconds>[0-9]+(?:\\.[0-9]+)?)S$') { return [Math]::Abs((([double]$Matches['seconds']) / 60.0) - $expectedMinutes) -lt 0.01 }; " +
		"try { $duration = [TimeSpan]::Parse($text); return [Math]::Abs($duration.TotalMinutes - $expectedMinutes) -lt 0.01 } catch { return $false } " +
		"}; "
}

func buildTeamsServiceWSLTaskConfigMatchesCommand(taskName string, args []string, opts teamsServiceWSLTaskConfigMatchOptions) string {
	return "$taskName = " + powershellSingleQuote(taskName) + "; " +
		buildTeamsServiceWSLTaskLauncherExpectedPowerShell(taskName, args) +
		"$expectedPrincipalUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name; " +
		"$tasks = @(Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue); " +
		"$task = if ($tasks.Count -gt 0) { $tasks[0] } else { $null }; " +
		"if ($null -eq $task) { $allMatched = $false } else { " +
		"$launcherPowerShellMatches = $false; " +
		"if (Test-Path -LiteralPath $launcherPowerShellPath) { $launcherPowerShellMatches = ((Get-Content -LiteralPath $launcherPowerShellPath -Raw) -eq $expectedLauncherPowerShell) }; " +
		"$launcherVbsMatches = $false; " +
		"if (Test-Path -LiteralPath $launcherVbsPath) { $launcherVbsMatches = ((Get-Content -LiteralPath $launcherVbsPath -Raw) -eq $expectedLauncherVbs) }; " +
		"if (-not $launcherPowerShellMatches -or -not $launcherVbsMatches) { $allMatched = $false }; " +
		"$actions = @($task.Actions); " +
		"$action = if ($actions.Count -gt 0) { $actions[0] } else { $null }; " +
		"if ($null -eq $action -or $action.Execute -ne $expectedActionExecute" +
		" -or $action.Arguments -ne $expectedActionArgument" +
		" -or $task.State -eq 'Disabled') { $allMatched = $false }; " +
		"if ($null -eq $task.Principal -or $task.Principal.UserId -ne $expectedPrincipalUser -or $task.Principal.LogonType -ne 'Interactive' -or $task.Principal.RunLevel -ne 'Limited') { $allMatched = $false }; " +
		"$settings = $task.Settings; " +
		"if ($null -eq $settings -or $settings.MultipleInstances -ne 'IgnoreNew' -or $settings.RestartCount -ne " + strconv.Itoa(teamsServiceTaskRestartCount) +
		" -or -not (Test-CodexHelperTaskDurationMinutes $settings.RestartInterval " + strconv.Itoa(teamsServiceTaskSchedulerRestartMinutes) + ")" +
		" -or -not (Test-CodexHelperTaskDurationMinutes $settings.ExecutionTimeLimit 0)" +
		" -or $settings.Hidden -ne $true) { $allMatched = $false }; " +
		"$hasLogonTrigger = $false; $hasRepeatingTrigger = $false; " +
		"foreach ($trigger in @($task.Triggers)) { " +
		"$className = ''; if ($null -ne $trigger.CimClass) { $className = [string]$trigger.CimClass.CimClassName }; " +
		"if ($trigger.Enabled -ne $false -and $className -like '*LogonTrigger*') { $hasLogonTrigger = $true }; " +
		"$repetition = $trigger.Repetition; " +
		"if ($trigger.Enabled -ne $false -and $null -ne $repetition -and -not [string]::IsNullOrWhiteSpace([string]$repetition.Interval)) { $hasRepeatingTrigger = $true } " +
		"}; " +
		"if (-not $hasLogonTrigger -or $hasRepeatingTrigger) { $allMatched = $false } " +
		"}"
}

func buildTeamsServiceWSLRetireTaskCommand(taskName string, includeCurrent bool) string {
	prefix := teamsServiceWSLTaskNamePrefix(taskName)
	skipCurrent := ""
	if !includeCurrent {
		skipCurrent = "if ($t.TaskName -eq $taskName) { continue }; "
	}
	return "$taskName = " + powershellSingleQuote(taskName) + "; " +
		"$legacyPrefix = " + powershellSingleQuote(prefix) + "; " +
		"$tasks = @(Get-ScheduledTask | Where-Object { $_.TaskName -like ($legacyPrefix + '*') }); " +
		"foreach ($t in $tasks) { " +
		skipCurrent +
		"Stop-ScheduledTask -TaskPath $t.TaskPath -TaskName $t.TaskName -ErrorAction SilentlyContinue; " +
		"Disable-ScheduledTask -TaskPath $t.TaskPath -TaskName $t.TaskName -ErrorAction Stop | Out-Null " +
		"}"
}

func teamsServiceWSLStartTaskAndVerifyPowerShell() string {
	return teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() + "Start-CodexHelperScheduledTaskIfStopped $taskName; " + teamsServiceWSLVerifyTaskRunningPowerShell()
}

func teamsServiceWSLStartTaskIfStoppedAndVerifyPowerShell() string {
	return teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() + "if ($task.State -ne 'Running') { Start-CodexHelperScheduledTaskIfStopped $taskName }; " + teamsServiceWSLVerifyTaskRunningPowerShell()
}

func teamsServiceWSLEnableStartTaskAndVerifyPowerShell() string {
	return "Enable-ScheduledTask -TaskName $taskName | Out-Null; " + teamsServiceWSLStartTaskAndVerifyPowerShell()
}

func teamsServiceWSLEnableStartTaskIfStoppedAndVerifyPowerShell() string {
	return "if ($task.State -eq 'Disabled') { Enable-ScheduledTask -TaskName $taskName | Out-Null }; " + teamsServiceWSLStartTaskIfStoppedAndVerifyPowerShell()
}

func teamsServiceWSLVerifyTaskRunningPowerShell() string {
	return "Start-Sleep -Seconds 2; " +
		"$startedTask = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop; " +
		"if ($startedTask.State -ne 'Running') { " +
		"$startedInfo = Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction SilentlyContinue; " +
		"$lastResult = if ($null -ne $startedInfo) { $startedInfo.LastTaskResult } else { 'unknown' }; " +
		"throw ('Teams WSL Scheduled Task did not stay running after start; state=' + $startedTask.State + ' last_result=' + $lastResult) " +
		"}"
}

func teamsServiceWSLTaskRunLogName(taskName string) string {
	sum := sha256.Sum256([]byte(taskName))
	return "codex-helper-teams-wsl-task-" + safeWindowsTaskNamePart(taskName, 56) + "-" + hex.EncodeToString(sum[:])[:12] + ".log"
}

func teamsServiceWindowsTaskRunLogName(args []string) string {
	label := "teams"
	if len(args) >= 3 && args[0] == "teams" && args[1] == "service" && args[2] == "watchdog" {
		label = "teams-watchdog"
	} else if len(args) >= 2 && args[0] == "teams" && args[1] == "run" {
		label = "teams-run"
	}
	seed := strings.Join(args, "\x00")
	sum := sha256.Sum256([]byte(label + "\x00" + seed))
	return "codex-helper-teams-windows-task-" + safeWindowsTaskNamePart(label, 40) + "-" + hex.EncodeToString(sum[:])[:12] + ".log"
}

func teamsServiceWSLTaskLauncherStem(taskName string, args []string) string {
	var b strings.Builder
	b.WriteString(taskName)
	for _, arg := range args {
		b.WriteByte(0)
		b.WriteString(arg)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "codex-helper-teams-wsl-task-" + safeWindowsTaskNamePart(taskName, 48) + "-" + hex.EncodeToString(sum[:])[:12]
}

func buildTeamsServiceWSLTaskPowerShellScript(taskName string, args []string) string {
	runLogName := teamsServiceWSLTaskRunLogName(taskName)
	wslArgumentLine := windowsCommandLine(args)
	var b strings.Builder
	b.WriteString("$ErrorActionPreference = 'Continue'\r\n")
	b.WriteString("$logDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\teams'\r\n")
	b.WriteString("New-Item -ItemType Directory -Force -Path $logDir | Out-Null\r\n")
	b.WriteString("$runLog = Join-Path $logDir " + powershellSingleQuote(runLogName) + "\r\n")
	b.WriteString("$stdoutLog = $runLog + '.stdout.log'\r\n")
	b.WriteString("$stderrLog = $runLog + '.stderr.log'\r\n")
	b.WriteString("$wslArgumentLine = " + powershellSingleQuote(wslArgumentLine) + "\r\n")
	b.WriteString("Add-Content -LiteralPath $runLog -Value ((Get-Date).ToString('o') + ' starting ' + " + powershellSingleQuote(taskName) + ")\r\n")
	b.WriteString("Add-Content -LiteralPath $runLog -Value ('stdout ' + $stdoutLog + ' stderr ' + $stderrLog)\r\n")
	b.WriteString("Remove-Item -LiteralPath $stdoutLog,$stderrLog -Force -ErrorAction SilentlyContinue\r\n")
	b.WriteString("$p = Start-Process -FilePath 'wsl.exe' -ArgumentList $wslArgumentLine -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog\r\n")
	b.WriteString("$code = if ($null -ne $p) { $p.ExitCode } else { 1 }\r\n")
	b.WriteString("if ($null -eq $code) { $code = 1 }\r\n")
	b.WriteString("Add-Content -LiteralPath $runLog -Value ((Get-Date).ToString('o') + ' exited ' + $code)\r\n")
	b.WriteString("exit $code\r\n")
	return b.String()
}

func buildTeamsServiceWSLTaskLauncherSetupPowerShell(taskName string, args []string) string {
	return buildTeamsServiceWSLTaskLauncherPowerShell(taskName, args, true)
}

func buildTeamsServiceWSLTaskLauncherExpectedPowerShell(taskName string, args []string) string {
	return buildTeamsServiceWSLTaskLauncherPowerShell(taskName, args, false)
}

func buildTeamsServiceWSLTaskLauncherPowerShell(taskName string, args []string, writeFiles bool) string {
	stem := teamsServiceWSLTaskLauncherStem(taskName, args)
	psName := stem + ".ps1"
	vbsName := stem + ".vbs"
	psScript := buildTeamsServiceWSLTaskPowerShellScript(taskName, args)
	cmd := "$appDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\teams'; " +
		"$launcherPowerShellPath = Join-Path $appDir " + powershellSingleQuote(psName) + "; " +
		"$launcherVbsPath = Join-Path $appDir " + powershellSingleQuote(vbsName) + "; " +
		"$expectedLauncherPowerShell = " + powershellSingleQuote(psScript) + "; " +
		"$launcherPowerShellExe = Join-Path $env:SystemRoot 'System32\\WindowsPowerShell\\v1.0\\powershell.exe'; " +
		"$launcherPowerShellExeVbs = $launcherPowerShellExe.Replace('\"', '\"\"'); " +
		"$launcherPowerShellPathVbs = $launcherPowerShellPath.Replace('\"', '\"\"'); " +
		"$expectedLauncherVbs = @(" +
		powershellSingleQuote("Function Q(s)") + ", " +
		powershellSingleQuote("  Q = Chr(34) & s & Chr(34)") + ", " +
		powershellSingleQuote("End Function") + ", " +
		powershellSingleQuote("Set shell = CreateObject(\"WScript.Shell\")") + ", " +
		"('cmd = Q(\"' + $launcherPowerShellExeVbs + '\") & \" -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File \" & Q(\"' + $launcherPowerShellPathVbs + '\")'), " +
		powershellSingleQuote("code = shell.Run(cmd, 0, True)") + ", " +
		powershellSingleQuote("WScript.Quit code") +
		") -join \"`r`n\"; "
	if writeFiles {
		cmd +=
			"New-Item -ItemType Directory -Force -Path $appDir | Out-Null; " +
				"Set-Content -LiteralPath $launcherPowerShellPath -Value $expectedLauncherPowerShell -Encoding UTF8 -NoNewline; " +
				"Set-Content -LiteralPath $launcherVbsPath -Value $expectedLauncherVbs -Encoding ASCII -NoNewline; "
	}
	cmd += "$expectedActionExecute = 'wscript.exe'; " +
		"$expectedActionArgument = '//B //Nologo \"' + $launcherVbsPath + '\"'; "
	return cmd
}

func teamsServiceWSLTaskNamePrefix(taskName string) string {
	taskName = strings.TrimSpace(taskName)
	idx := strings.LastIndex(taskName, " ")
	if idx < 0 {
		return taskName
	}
	return taskName[:idx+1]
}

func teamsServiceWSLResolveTaskPowerShell(taskName string) string {
	return teamsServiceWSLResolveTaskPowerShellRequired(taskName, true)
}

func teamsServiceWSLResolveOptionalTaskPowerShell(taskName string) string {
	return teamsServiceWSLResolveTaskPowerShellRequired(taskName, false)
}

func teamsServiceWSLResolveTaskPowerShellRequired(taskName string, required bool) string {
	prefix := teamsServiceWSLTaskNamePrefix(taskName)
	cmd := "$taskName = " + powershellSingleQuote(taskName) + "; " +
		"$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; " +
		"if ($null -eq $task) { " +
		"$legacyPrefix = " + powershellSingleQuote(prefix) + "; " +
		"$matches = @(Get-ScheduledTask | Where-Object { $_.TaskName -like ($legacyPrefix + '*') } | Sort-Object TaskName); " +
		"if ($matches.Count -gt 0) { " +
		"$running = @($matches | Where-Object { $_.State -eq 'Running' }); " +
		"if ($running.Count -gt 0) { $task = $running[0] } else { $task = $matches[0] }; " +
		"$taskName = $task.TaskName " +
		"} " +
		"}; "
	if required {
		cmd += "if ($null -eq $task) { throw ('Teams WSL Scheduled Task not found: ' + $taskName) }; "
	}
	return cmd
}

func buildTeamsServiceWSLElevatedCommand(command string) string {
	return buildTeamsServiceWindowsElevatedCommand(command)
}

func buildTeamsServiceWindowsElevatedCommand(command string) string {
	inner := "$ErrorActionPreference = 'Stop'; " + strings.TrimSpace(command)
	args := "@('-NoProfile','-ExecutionPolicy','Bypass','-Command'," + powershellSingleQuote(inner) + ")"
	return "$uacArgs = " + args + "; " +
		"$p = Start-Process -FilePath 'powershell.exe' -ArgumentList $uacArgs -Verb RunAs -Wait -PassThru; " +
		"if ($null -eq $p) { throw 'Windows UAC process did not start' }; " +
		"if ($p.ExitCode -ne 0) { throw ('elevated Teams service bootstrap failed with exit code ' + $p.ExitCode) }"
}

func buildTeamsServiceWSLTaskConfig(taskName string, args []string) string {
	var b strings.Builder
	b.WriteString("TaskName=")
	b.WriteString(taskName)
	b.WriteString("\nCommand=wsl.exe\nArguments=")
	b.WriteString(windowsCommandLine(args))
	b.WriteString("\n")
	return b.String()
}

func buildTeamsServiceWSLStartupFallbackCommand(taskName string, args []string, start bool) string {
	identity := teamsServiceWSLTaskIdentity()
	return buildTeamsServiceWSLStartupFallbackCommandWithArgumentLine(taskName, windowsCommandLine(args), identity.Suffix, start)
}

func buildTeamsServiceWSLStartupFallbackCommandWithArgumentLine(taskName string, wslArgumentLine string, suffix string, start bool) string {
	scriptName := "codex-helper-teams-wsl-" + suffix + ".ps1"
	launcherName := "codex-helper-teams-wsl-" + suffix + ".vbs"
	legacyCmdLauncherName := "codex-helper-teams-wsl-" + suffix + ".cmd"
	stopName := "codex-helper-teams-wsl-stop-" + suffix + ".signal"
	script := buildTeamsServiceWSLStartupWatchdogScriptWithArgumentLine(taskName, wslArgumentLine, suffix)
	cleanup := buildTeamsServiceWSLRemoveStartupFallbackCommand(taskName, []string{suffix})
	ps := cleanup + "; " +
		"$startup = [Environment]::GetFolderPath('Startup'); " +
		"if ([string]::IsNullOrWhiteSpace($startup)) { throw 'Windows Startup folder is unavailable' }; " +
		"$appDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\teams'; " +
		"New-Item -ItemType Directory -Force -Path $appDir | Out-Null; " +
		"$scriptPath = Join-Path $appDir " + powershellSingleQuote(scriptName) + "; " +
		"$launcherPath = Join-Path $startup " + powershellSingleQuote(launcherName) + "; " +
		"$legacyCmdLauncherPath = Join-Path $startup " + powershellSingleQuote(legacyCmdLauncherName) + "; " +
		"$stopPath = Join-Path $appDir " + powershellSingleQuote(stopName) + "; " +
		"Remove-Item -LiteralPath $stopPath -Force -ErrorAction SilentlyContinue; " +
		"Remove-Item -LiteralPath $legacyCmdLauncherPath -Force -ErrorAction SilentlyContinue; " +
		"Set-Content -LiteralPath $scriptPath -Value " + powershellSingleQuote(script) + " -Encoding UTF8; " +
		"$launcherPowerShellExe = Join-Path $env:SystemRoot 'System32\\WindowsPowerShell\\v1.0\\powershell.exe'; " +
		"$launcherPowerShellExeVbs = $launcherPowerShellExe.Replace('\"', '\"\"'); " +
		"$scriptPathVbs = $scriptPath.Replace('\"', '\"\"'); " +
		"$launcherVbs = @(" +
		powershellSingleQuote("Function Q(s)") + ", " +
		powershellSingleQuote("  Q = Chr(34) & s & Chr(34)") + ", " +
		powershellSingleQuote("End Function") + ", " +
		powershellSingleQuote("Set shell = CreateObject(\"WScript.Shell\")") + ", " +
		"('cmd = Q(\"' + $launcherPowerShellExeVbs + '\") & \" -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File \" & Q(\"' + $scriptPathVbs + '\")'), " +
		powershellSingleQuote("code = shell.Run(cmd, 0, True)") + ", " +
		powershellSingleQuote("WScript.Quit code") +
		") -join \"`r`n\"; " +
		"Set-Content -LiteralPath $launcherPath -Value $launcherVbs -Encoding ASCII"
	if start {
		ps += "; Start-Process -FilePath 'wscript.exe' -ArgumentList ('//B //Nologo \"' + $launcherPath + '\"') -WindowStyle Hidden | Out-Null"
	}
	return ps
}

func buildTeamsServiceWSLStartExistingStartupFallbackCommand(suffix string) string {
	launcherName := "codex-helper-teams-wsl-" + suffix + ".vbs"
	scriptName := "codex-helper-teams-wsl-" + suffix + ".ps1"
	legacyCmdLauncherName := "codex-helper-teams-wsl-" + suffix + ".cmd"
	stopName := "codex-helper-teams-wsl-stop-" + suffix + ".signal"
	return "$startup = [Environment]::GetFolderPath('Startup'); " +
		"if ([string]::IsNullOrWhiteSpace($startup)) { throw 'Windows Startup folder is unavailable' }; " +
		"$appDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\teams'; " +
		"New-Item -ItemType Directory -Force -Path $appDir | Out-Null; " +
		"$launcherPath = Join-Path $startup " + powershellSingleQuote(launcherName) + "; " +
		"$scriptPath = Join-Path $appDir " + powershellSingleQuote(scriptName) + "; " +
		"$legacyCmdLauncherPath = Join-Path $startup " + powershellSingleQuote(legacyCmdLauncherName) + "; " +
		"$stopPath = Join-Path $appDir " + powershellSingleQuote(stopName) + "; " +
		"Remove-Item -LiteralPath $stopPath -Force -ErrorAction SilentlyContinue; " +
		"if (-not (Test-Path -LiteralPath $launcherPath)) { throw ('Teams WSL Startup watchdog launcher not found: ' + $launcherPath) }; " +
		teamsServiceWSLStopStartupFallbackProcessesPowerShell("$scriptPath", "$launcherPath", "$legacyCmdLauncherPath") +
		"Start-Process -FilePath 'wscript.exe' -ArgumentList ('//B //Nologo \"' + $launcherPath + '\"') -WindowStyle Hidden | Out-Null"
}

func buildTeamsServiceWSLStopStartupFallbackCommand(suffixes []string) string {
	return "$appDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\teams'; " +
		"New-Item -ItemType Directory -Force -Path $appDir | Out-Null; " +
		"$startup = [Environment]::GetFolderPath('Startup'); " +
		"$scriptPrefix = 'codex-helper-teams-wsl-'; " +
		"$suffixes = " + powershellArrayLiteral(uniqueNonEmptyStrings(suffixes)) + "; " +
		"foreach ($suffix in $suffixes) { " +
		"if ([string]::IsNullOrWhiteSpace($suffix)) { continue }; " +
		"$stopPath = Join-Path $appDir ('codex-helper-teams-wsl-stop-' + $suffix + '.signal'); " +
		"Set-Content -LiteralPath $stopPath -Value 'stop' -Encoding ASCII; " +
		"$scriptPath = Join-Path $appDir ($scriptPrefix + $suffix + '.ps1'); " +
		"$launcherPath = if (-not [string]::IsNullOrWhiteSpace($startup)) { Join-Path $startup ($scriptPrefix + $suffix + '.vbs') } else { '' }; " +
		"$legacyCmdLauncherPath = if (-not [string]::IsNullOrWhiteSpace($startup)) { Join-Path $startup ($scriptPrefix + $suffix + '.cmd') } else { '' }; " +
		teamsServiceWSLStopStartupFallbackProcessesPowerShell("$scriptPath", "$launcherPath", "$legacyCmdLauncherPath") +
		"}"
}

func buildTeamsServiceWSLRemoveStartupFallbackCommand(taskName string, suffixes []string) string {
	prefix := teamsServiceWSLTaskNamePrefix(taskName)
	return "$startup = [Environment]::GetFolderPath('Startup'); " +
		"$appDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\teams'; " +
		"New-Item -ItemType Directory -Force -Path $appDir | Out-Null; " +
		"$suffixes = " + powershellArrayLiteral(uniqueNonEmptyStrings(suffixes)) + "; " +
		"$legacyPrefix = " + powershellSingleQuote(prefix) + "; " +
		"$scriptPrefix = 'codex-helper-teams-wsl-'; " +
		"$allSuffixes = New-Object System.Collections.Generic.List[string]; " +
		"foreach ($suffix in $suffixes) { if (-not [string]::IsNullOrWhiteSpace($suffix)) { $allSuffixes.Add($suffix) } }; " +
		"if (Test-Path -LiteralPath $appDir) { " +
		"Get-ChildItem -LiteralPath $appDir -Filter 'codex-helper-teams-wsl-*.ps1' -File -ErrorAction SilentlyContinue | ForEach-Object { " +
		"$content = Get-Content -LiteralPath $_.FullName -Raw -ErrorAction SilentlyContinue; " +
		"if ($null -ne $content -and ($content.Contains('starting ' + $legacyPrefix) -or $content.Contains($legacyPrefix))) { " +
		"$suffix = $_.BaseName -replace '^codex-helper-teams-wsl-', ''; " +
		"if (-not [string]::IsNullOrWhiteSpace($suffix) -and -not $allSuffixes.Contains($suffix)) { $allSuffixes.Add($suffix) } " +
		"} } }; " +
		"if (-not [string]::IsNullOrWhiteSpace($startup) -and (Test-Path -LiteralPath $startup)) { " +
		"Get-ChildItem -LiteralPath $startup -Filter 'codex-helper-teams-wsl-*.cmd' -File -ErrorAction SilentlyContinue | ForEach-Object { " +
		"$suffix = $_.BaseName -replace '^codex-helper-teams-wsl-', ''; " +
		"$content = Get-Content -LiteralPath $_.FullName -Raw -ErrorAction SilentlyContinue; " +
		"$scriptPath = Join-Path $appDir ($scriptPrefix + $suffix + '.ps1'); " +
		"$scriptContent = if (Test-Path -LiteralPath $scriptPath) { Get-Content -LiteralPath $scriptPath -Raw -ErrorAction SilentlyContinue } else { $null }; " +
		"if (-not [string]::IsNullOrWhiteSpace($suffix) -and -not $allSuffixes.Contains($suffix) -and (($null -ne $content -and $content.Contains($legacyPrefix)) -or ($null -ne $scriptContent -and ($scriptContent.Contains('starting ' + $legacyPrefix) -or $scriptContent.Contains($legacyPrefix))))) { $allSuffixes.Add($suffix) } " +
		"}; " +
		"Get-ChildItem -LiteralPath $startup -Filter 'codex-helper-teams-wsl-*.vbs' -File -ErrorAction SilentlyContinue | ForEach-Object { " +
		"$suffix = $_.BaseName -replace '^codex-helper-teams-wsl-', ''; " +
		"$content = Get-Content -LiteralPath $_.FullName -Raw -ErrorAction SilentlyContinue; " +
		"$scriptPath = Join-Path $appDir ($scriptPrefix + $suffix + '.ps1'); " +
		"$scriptContent = if (Test-Path -LiteralPath $scriptPath) { Get-Content -LiteralPath $scriptPath -Raw -ErrorAction SilentlyContinue } else { $null }; " +
		"if (-not [string]::IsNullOrWhiteSpace($suffix) -and -not $allSuffixes.Contains($suffix) -and (($null -ne $content -and $content.Contains($legacyPrefix)) -or ($null -ne $scriptContent -and ($scriptContent.Contains('starting ' + $legacyPrefix) -or $scriptContent.Contains($legacyPrefix))))) { $allSuffixes.Add($suffix) } " +
		"} }; " +
		"foreach ($suffix in $allSuffixes) { " +
		"$stopPath = Join-Path $appDir ('codex-helper-teams-wsl-stop-' + $suffix + '.signal'); " +
		"Set-Content -LiteralPath $stopPath -Value 'stop' -Encoding ASCII; " +
		"$paths = @(); " +
		"if (-not [string]::IsNullOrWhiteSpace($startup)) { $paths += (Join-Path $startup ($scriptPrefix + $suffix + '.cmd')); $paths += (Join-Path $startup ($scriptPrefix + $suffix + '.vbs')) }; " +
		"$paths += (Join-Path $appDir ($scriptPrefix + $suffix + '.ps1')); " +
		"$paths += (Join-Path $appDir ($scriptPrefix + $suffix + '.vbs')); " +
		"$scriptPath = Join-Path $appDir ($scriptPrefix + $suffix + '.ps1'); " +
		"$launcherPath = if (-not [string]::IsNullOrWhiteSpace($startup)) { Join-Path $startup ($scriptPrefix + $suffix + '.vbs') } else { '' }; " +
		"$legacyCmdLauncherPath = if (-not [string]::IsNullOrWhiteSpace($startup)) { Join-Path $startup ($scriptPrefix + $suffix + '.cmd') } else { '' }; " +
		teamsServiceWSLStopStartupFallbackProcessesPowerShell("$scriptPath", "$launcherPath", "$legacyCmdLauncherPath") +
		"foreach ($path in $paths) { Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue } " +
		"}"
}

func teamsServiceWSLStopStartupFallbackProcessesPowerShell(pathExpressions ...string) string {
	paths := make([]string, 0, len(pathExpressions))
	for _, expr := range pathExpressions {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}
		paths = append(paths, expr)
	}
	if len(paths) == 0 {
		return ""
	}
	return "$watchdogNeedles = @(" + strings.Join(paths, ", ") + ") | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }; " +
		"try { Get-CimInstance Win32_Process -ErrorAction Stop | ForEach-Object { " +
		"$proc = $_; $cmd = [string]$proc.CommandLine; " +
		"if ($proc.ProcessId -ne $PID -and -not [string]::IsNullOrWhiteSpace($cmd)) { " +
		"foreach ($needle in $watchdogNeedles) { if ($cmd.IndexOf($needle, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) { Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue; break } } " +
		"} " +
		"} } catch { }; "
}

func buildTeamsServiceWSLStartupWatchdogScript(taskName string, args []string, suffix string) string {
	return buildTeamsServiceWSLStartupWatchdogScriptWithArgumentLine(taskName, windowsCommandLine(args), suffix)
}

func buildTeamsServiceWSLStartupWatchdogScriptWithArgumentLine(taskName string, wslArgumentLine string, suffix string) string {
	mutexName := "Local\\CodexHelperTeamsWSLWatchdog-" + safeWindowsTaskNamePart(suffix, 32)
	runLogName := "codex-helper-teams-wsl-run-" + safeWindowsTaskNamePart(suffix, 32) + ".log"
	watchdogLogName := "codex-helper-teams-wsl-watchdog-" + safeWindowsTaskNamePart(suffix, 32) + ".log"
	stopName := "codex-helper-teams-wsl-stop-" + safeWindowsTaskNamePart(suffix, 32) + ".signal"
	var b strings.Builder
	b.WriteString("$ErrorActionPreference = 'Continue'\r\n")
	b.WriteString("$created = $false\r\n")
	b.WriteString("$mutex = $null\r\n")
	b.WriteString("try {\r\n")
	b.WriteString("  $deadline = (Get-Date).AddSeconds(60)\r\n")
	b.WriteString("  while (-not $created) {\r\n")
	b.WriteString("    $mutex = New-Object System.Threading.Mutex($true, " + powershellSingleQuote(mutexName) + ", [ref]$created)\r\n")
	b.WriteString("    if ($created) { break }\r\n")
	b.WriteString("    if ($null -ne $mutex) { $mutex.Dispose(); $mutex = $null }\r\n")
	b.WriteString("    if ((Get-Date) -ge $deadline) { exit 0 }\r\n")
	b.WriteString("    Start-Sleep -Seconds 2\r\n")
	b.WriteString("  }\r\n")
	b.WriteString("  $logDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\teams'\r\n")
	b.WriteString("  New-Item -ItemType Directory -Force -Path $logDir | Out-Null\r\n")
	b.WriteString("  $runLog = Join-Path $logDir " + powershellSingleQuote(runLogName) + "\r\n")
	b.WriteString("  $watchdogLog = Join-Path $logDir " + powershellSingleQuote(watchdogLogName) + "\r\n")
	b.WriteString("  $stopPath = Join-Path $logDir " + powershellSingleQuote(stopName) + "\r\n")
	b.WriteString("  $stdoutLog = $runLog + '.stdout.log'\r\n")
	b.WriteString("  $stderrLog = $runLog + '.stderr.log'\r\n")
	b.WriteString("  $wslArgumentLine = " + powershellSingleQuote(wslArgumentLine) + "\r\n")
	b.WriteString("  Add-Content -LiteralPath $watchdogLog -Value ((Get-Date).ToString('o') + ' starting " + strings.ReplaceAll(taskName, "'", "''") + "')\r\n")
	b.WriteString("  while ($true) {\r\n")
	b.WriteString("    if (Test-Path -LiteralPath $stopPath) { Add-Content -LiteralPath $watchdogLog -Value ((Get-Date).ToString('o') + ' stop requested; exiting'); break }\r\n")
	b.WriteString("    Add-Content -LiteralPath $runLog -Value ((Get-Date).ToString('o') + ' starting hidden wsl.exe; stdout ' + $stdoutLog + ' stderr ' + $stderrLog)\r\n")
	b.WriteString("    Remove-Item -LiteralPath $stdoutLog,$stderrLog -Force -ErrorAction SilentlyContinue\r\n")
	b.WriteString("    $p = Start-Process -FilePath 'wsl.exe' -ArgumentList $wslArgumentLine -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog\r\n")
	b.WriteString("    $code = if ($null -ne $p) { $p.ExitCode } else { 1 }\r\n")
	b.WriteString("    if (Test-Path -LiteralPath $stopPath) { Add-Content -LiteralPath $watchdogLog -Value ((Get-Date).ToString('o') + ' stop requested after wsl.exe exited ' + $code); break }\r\n")
	b.WriteString("    Add-Content -LiteralPath $watchdogLog -Value ((Get-Date).ToString('o') + ' wsl.exe exited ' + $code + '; restarting in " + strconv.Itoa(teamsServiceTaskRestartInterval) + "s')\r\n")
	b.WriteString("    Start-Sleep -Seconds " + strconv.Itoa(teamsServiceTaskRestartInterval) + "\r\n")
	b.WriteString("  }\r\n")
	b.WriteString("} finally {\r\n")
	b.WriteString("  if ($created -and $null -ne $mutex) { $mutex.ReleaseMutex(); $mutex.Dispose() } elseif ($null -ne $mutex) { $mutex.Dispose() }\r\n")
	b.WriteString("}\r\n")
	return b.String()
}

func buildTeamsServiceWSLStartupFallbackConfig(taskName string, args []string) string {
	var b strings.Builder
	b.WriteString("Fallback=Windows Startup watchdog\n")
	b.WriteString("TaskName=")
	b.WriteString(taskName)
	b.WriteString("\nCommand=wsl.exe\nArguments=")
	b.WriteString(windowsCommandLine(args))
	b.WriteString("\n")
	return b.String()
}

func buildTeamsServiceWindowsPowerShell(spec teamsServiceSpec, args []string) string {
	var parts []string
	for _, key := range sortedEnvironmentKeys(spec.Environment) {
		parts = append(parts, "$env:"+key+" = "+powershellSingleQuote(spec.Environment[key]))
	}
	runLogName := teamsServiceWindowsTaskRunLogName(args)
	argumentLine := windowsCommandLine(args)
	parts = append(parts,
		"$logBase = [Environment]::GetFolderPath('LocalApplicationData')",
		"if ([string]::IsNullOrWhiteSpace($logBase)) { $logBase = [IO.Path]::GetTempPath() }",
		"$logDir = Join-Path $logBase 'codex-helper\\teams'",
		"New-Item -ItemType Directory -Force -Path $logDir | Out-Null",
		"$runLog = Join-Path $logDir "+powershellSingleQuote(runLogName),
		"$stdoutLog = $runLog + '.stdout.log'",
		"$stderrLog = $runLog + '.stderr.log'",
		"Add-Content -LiteralPath $runLog -Value ((Get-Date).ToString('o') + ' starting hidden Teams helper process; stdout ' + $stdoutLog + ' stderr ' + $stderrLog)",
		"Remove-Item -LiteralPath $stdoutLog,$stderrLog -Force -ErrorAction SilentlyContinue",
		"$argumentLine = "+powershellSingleQuote(argumentLine),
	)
	directCall := "& " + powershellSingleQuote(spec.Executable)
	for _, arg := range args {
		directCall += " " + powershellSingleQuote(arg)
	}
	parts = append(parts,
		"if ($env:OS -eq 'Windows_NT') { $p = Start-Process -FilePath "+powershellSingleQuote(spec.Executable)+" -ArgumentList $argumentLine -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog; if ($null -ne $p) { $code = $p.ExitCode } else { $code = 1 } } else { "+directCall+" > $stdoutLog 2> $stderrLog; $code = $LASTEXITCODE }",
	)
	parts = append(parts, "if ($null -eq $code) { $code = 1 }")
	parts = append(parts, "Add-Content -LiteralPath $runLog -Value ((Get-Date).ToString('o') + ' exited ' + $code)")
	parts = append(parts, "exit $code")
	return strings.Join(parts, "; ")
}

func teamsServiceUnitPath() (string, error) {
	dir, err := teamsServiceSystemdUserDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceUnitName), nil
}

func teamsServiceWatchdogUnitPath() (string, error) {
	dir, err := teamsServiceSystemdUserDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceWatchdogUnitName), nil
}

func teamsServiceWatchdogTimerPath() (string, error) {
	dir, err := teamsServiceSystemdUserDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceWatchdogTimerName), nil
}

func defaultTeamsServiceSystemdUserDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "systemd", "user"), nil
}

func defaultTeamsServiceLaunchAgentDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents"), nil
}

func teamsServiceLaunchAgentWatchdogPath() (string, error) {
	dir, err := teamsServiceLaunchAgentDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceLaunchAgentWatchdogPlistName), nil
}

func defaultTeamsServiceWindowsTaskXMLDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams"), nil
}

func defaultTeamsServiceWSLDistroName() string {
	return strings.TrimSpace(os.Getenv("WSL_DISTRO_NAME"))
}

func defaultTeamsServiceWSLLinuxUserName() string {
	for _, name := range []string{"USER", "LOGNAME", "USERNAME"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		if base := strings.TrimSpace(filepath.Base(home)); base != "" && base != "." && base != string(filepath.Separator) {
			return base
		}
	}
	return ""
}

type teamsServiceWSLTaskIdentityInfo struct {
	Display string
	Suffix  string
}

func teamsServiceWSLTaskIdentity() teamsServiceWSLTaskIdentityInfo {
	distro := strings.TrimSpace(teamsServiceWSLDistroName())
	if distro == "" {
		distro = "default"
	}
	linuxUser := strings.TrimSpace(teamsServiceWSLLinuxUserName())
	if linuxUser == "" {
		linuxUser = "user"
	}
	profile := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_PROFILE"))
	if profile == "" {
		profile = "default"
	}
	machineID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_MACHINE_ID"))
	raw := strings.Join([]string{distro, linuxUser, profile, machineID}, "\x00")
	sum := sha256.Sum256([]byte(raw))
	displayParts := []string{
		safeWindowsTaskNamePart(distro, 28),
		safeWindowsTaskNamePart(linuxUser, 20),
		safeWindowsTaskNamePart(profile, 20),
	}
	return teamsServiceWSLTaskIdentityInfo{
		Display: strings.Join(displayParts, " "),
		Suffix:  hex.EncodeToString(sum[:])[:12],
	}
}

func firstNonEmptyTeamsServiceString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func safeWindowsTaskNamePart(value string, max int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "default"
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		ok := r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			r == '.' || r == '_' || r == '-' || r == '@'
		if ok {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		out = "default"
	}
	if max > 0 && len(out) > max {
		out = out[:max]
		out = strings.TrimRight(out, "_")
		if out == "" {
			out = "default"
		}
	}
	return out
}

func teamsServiceFileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func teamsServiceLaunchctlUserTarget() string {
	return "gui/" + strings.TrimSpace(teamsServiceUserID())
}

func teamsServiceLaunchctlServiceTarget() string {
	return teamsServiceLaunchctlUserTarget() + "/" + teamsServiceLaunchAgentLabel
}

func teamsServiceLaunchctlWatchdogTarget() string {
	return teamsServiceLaunchctlUserTarget() + "/" + teamsServiceLaunchAgentWatchdogLabel
}

func defaultTeamsServiceIsWSL() bool {
	if teamsServiceGOOS() != "linux" {
		return false
	}
	if strings.TrimSpace(os.Getenv("WSL_DISTRO_NAME")) != "" || strings.TrimSpace(os.Getenv("WSL_INTEROP")) != "" {
		return true
	}
	version := ""
	data, err := os.ReadFile("/proc/version")
	if err == nil {
		version = string(data)
	}
	return teamsServiceIsWSLFromSignals(teamsServiceGOOS(), version, teamsServiceWSLInteropAvailable())
}

func teamsServiceIsWSLFromSignals(goos string, procVersion string, interopAvailable bool) bool {
	if goos != "linux" {
		return false
	}
	version := strings.ToLower(procVersion)
	if !strings.Contains(version, "microsoft") && !strings.Contains(version, "wsl") {
		return false
	}
	return interopAvailable
}

func teamsServiceWSLInteropAvailable() bool {
	info, err := os.Stat("/proc/sys/fs/binfmt_misc/WSLInterop")
	return err == nil && !info.IsDir()
}

func systemdQuoteArg(s string) string {
	if s == "" {
		return `""`
	}
	if strings.IndexFunc(s, func(r rune) bool {
		return r <= ' ' || r == '"' || r == '\\' || r == '\'' || r == ';'
	}) == -1 {
		return s
	}
	return strconv.Quote(s)
}

func windowsQuoteArg(s string) string {
	if s == "" {
		return `""`
	}
	if strings.IndexFunc(s, func(r rune) bool {
		return r <= ' ' || r == '"'
	}) == -1 {
		return s
	}
	var b strings.Builder
	b.WriteByte('"')
	backslashes := 0
	for _, r := range s {
		switch r {
		case '\\':
			backslashes++
		case '"':
			b.WriteString(strings.Repeat(`\`, backslashes*2+1))
			b.WriteRune(r)
			backslashes = 0
		default:
			if backslashes > 0 {
				b.WriteString(strings.Repeat(`\`, backslashes))
				backslashes = 0
			}
			b.WriteRune(r)
		}
	}
	if backslashes > 0 {
		b.WriteString(strings.Repeat(`\`, backslashes*2))
	}
	b.WriteByte('"')
	return b.String()
}

func windowsCommandLine(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, windowsQuoteArg(arg))
	}
	return strings.Join(quoted, " ")
}

func splitWindowsCommandLine(line string) ([]string, error) {
	var args []string
	var b strings.Builder
	inQuotes := false
	haveArg := false
	backslashes := 0
	flushBackslashes := func(n int) {
		if n > 0 {
			b.WriteString(strings.Repeat(`\`, n))
		}
	}
	flushArg := func() {
		args = append(args, b.String())
		b.Reset()
		haveArg = false
	}
	for _, r := range line {
		switch r {
		case '\\':
			backslashes++
			haveArg = true
		case '"':
			flushBackslashes(backslashes / 2)
			if backslashes%2 == 0 {
				inQuotes = !inQuotes
				haveArg = true
			} else {
				b.WriteRune('"')
				haveArg = true
			}
			backslashes = 0
		case ' ', '\t', '\r', '\n':
			flushBackslashes(backslashes)
			backslashes = 0
			if inQuotes {
				b.WriteRune(r)
				haveArg = true
				continue
			}
			if haveArg {
				flushArg()
			}
		default:
			flushBackslashes(backslashes)
			backslashes = 0
			b.WriteRune(r)
			haveArg = true
		}
	}
	flushBackslashes(backslashes)
	if inQuotes {
		return nil, fmt.Errorf("unterminated quote in Windows command line")
	}
	if haveArg {
		flushArg()
	}
	return args, nil
}

func powershellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func powershellArrayLiteral(values []string) string {
	if len(values) == 0 {
		return "@()"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, powershellSingleQuote(value))
	}
	return "@(" + strings.Join(parts, ", ") + ")"
}

func uniqueNonEmptyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func xmlEscape(s string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return replacer.Replace(s)
}
