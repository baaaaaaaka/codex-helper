package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

const (
	teamsServiceUnitName                     = "codex-helper-teams.service"
	teamsServiceWatchdogUnitName             = "codex-helper-teams-watchdog.service"
	teamsServiceWatchdogTimerName            = "codex-helper-teams-watchdog.timer"
	teamsServiceLaunchAgentLabel             = "com.codex-helper.teams"
	teamsServiceLaunchAgentPlistName         = teamsServiceLaunchAgentLabel + ".plist"
	teamsServiceLaunchAgentWatchdogLabel     = teamsServiceLaunchAgentLabel + ".watchdog"
	teamsServiceLaunchAgentWatchdogPlistName = teamsServiceLaunchAgentWatchdogLabel + ".plist"
	teamsServiceWindowsTaskName              = "Codex Helper Teams Bridge"
	teamsServiceWindowsWatchdogTaskName      = "Codex Helper Teams Watchdog"
	teamsServiceWindowsTaskXMLName           = "codex-helper-teams-task.xml"
	teamsServiceWindowsWatchdogTaskXMLName   = "codex-helper-teams-watchdog-task.xml"
	teamsServiceWSLTaskConfigName            = "codex-helper-teams-wsl-task.txt"
	teamsServiceTaskRestartCount             = 999
	teamsServiceTaskRestartInterval          = 10
	teamsServiceTaskSchedulerRestartMinutes  = 1
	teamsServiceWatchdogMinutes              = 1
	teamsServiceWatchdogDays                 = 3650
	teamsServiceRunOwnerStaleAfter           = 18 * time.Second
	teamsServiceExternalWatchdogInterval     = 10 * time.Second
	teamsServiceExternalWatchdogCheckTimeout = 20 * time.Second
	teamsServiceExternalWatchdogSeconds      = int(teamsServiceExternalWatchdogInterval / time.Second)
	teamsServiceExternalWatchdogMinutes      = 1
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
	teamsServiceGOOS                                           = func() string { return runtime.GOOS }
	teamsServiceExecutable                                     = os.Executable
	teamsServiceGetwd                                          = os.Getwd
	teamsServiceSystemdUserDir                                 = defaultTeamsServiceSystemdUserDir
	teamsServiceLaunchAgentDir                                 = defaultTeamsServiceLaunchAgentDir
	teamsServiceWindowsTaskXMLDir                              = defaultTeamsServiceWindowsTaskXMLDir
	teamsServiceUserID                                         = defaultTeamsServiceUserID
	teamsServiceIsWSL                                          = defaultTeamsServiceIsWSL
	teamsServiceWSLDistroName                                  = defaultTeamsServiceWSLDistroName
	teamsServiceWSLLinuxUserName                               = defaultTeamsServiceWSLLinuxUserName
	teamsServicePowerShellExecutable                           = defaultTeamsServicePowerShellExecutable
	teamsServiceSystemctl            teamsServiceCommandRunner = teamsServiceExecRunner{}
	teamsServiceAuthPreflight                                  = defaultTeamsServiceAuthPreflight
	teamsServiceBootstrapControlChat                           = defaultTeamsServiceBootstrapControlChat
	teamsServiceOpenURL                                        = defaultTeamsServiceOpenURL
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
	)
	return cmd
}

func newTeamsServiceBootstrapCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var yes bool
	var noUAC bool
	var fallbackOnly bool
	var noOpenControl bool
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Install or repair the Teams bridge background service",
		Long:  "Install or repair the Teams bridge background service. Bootstrap prepares the Teams control chat, prints the link in this terminal, and tries to open it automatically. On WSL, this first tries a current-user Windows Scheduled Task. If Windows blocks that path, it can ask before opening a UAC prompt and then falls back to a current-user Startup watchdog.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("bootstrap the Teams service", "helper reload now"); err != nil {
				return err
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
	cmd.Flags().BoolVar(&yes, "yes", false, "Approve the Windows UAC prompt if the WSL Scheduled Task needs elevation")
	cmd.Flags().BoolVar(&noUAC, "no-uac", false, "Do not open a Windows UAC prompt; use the current-user Startup fallback if needed")
	cmd.Flags().BoolVar(&fallbackOnly, "fallback-only", false, "Install the current-user Startup watchdog instead of trying Windows Task Scheduler")
	cmd.Flags().BoolVar(&noOpenControl, "no-open-control", false, "Do not try to open the Teams control chat link automatically")
	return cmd
}

func newTeamsServiceInstallCmd(registryPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "install",
		Short: "Install the Teams bridge user service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("install the Teams service", "helper reload now"); err != nil {
				return err
			}
			path, err := installTeamsService(cmd.Context(), registryPath)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Installed Teams service config: %s\n", path)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Service was not enabled or started automatically.\n")
			return nil
		},
	}
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
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				return err
			}
			if err := runTeamsServiceCommand(cmd.Context(), cmd.OutOrStdout(), backend, "start"); err != nil {
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
			if err := runTeamsServiceCommand(cmd.Context(), cmd.OutOrStdout(), backend, "stop"); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Stopped Teams service: %s\n", backend.Name())
			return nil
		},
	}
}

func newTeamsServiceRestartCmd() *cobra.Command {
	return &cobra.Command{
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
			if err := startTeamsService(cmd.Context(), true); err != nil {
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
			} else if err := validateTeamsServiceExecutable(exe); err != nil {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service executable: not installable (%v)\n", err)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service executable: %s\n", exe)
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
				} else {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "WSL: detected. systemd --user requires WSL systemd and a running user manager; for Windows-login autostart, unset CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND=systemd.")
				}
			} else if teamsServiceGOOS() == "linux" && backend.ID() == "systemd-user" {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Linux: systemd --user keeps the Teams bridge independent of the terminal while the user manager is alive.")
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Linux: if lingering is disabled, a full logout may stop the user manager; without root/admin policy changes, no user service can guarantee survival after that boundary.")
			}
			return nil
		},
	}
}

func installTeamsService(ctx context.Context, registryPath *string) (string, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return "", err
	}
	spec, err := buildTeamsServiceSpec(registryPath)
	if err != nil {
		return "", err
	}
	return backend.Install(ctx, spec)
}

func repairTeamsService(ctx context.Context, registryPath *string, opts teamsServiceRepairOptions) (string, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return "", err
	}
	spec, err := buildTeamsServiceSpec(registryPath)
	if err != nil {
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

type teamsServiceBootstrapOptions struct {
	AssumeYes    bool
	NoUAC        bool
	FallbackOnly bool
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
	_, _ = fmt.Fprintf(out, "Teams service bootstrap ready: %s\n", result.Mode)
	if strings.TrimSpace(result.Path) != "" {
		_, _ = fmt.Fprintf(out, "Teams service config: %s\n", result.Path)
	}
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out)
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
	if _, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
		if _, err := teamsServiceRetireLocalDuplicateProcesses(ctx, spec); err != nil {
			return teamsServiceBootstrapResult{}, fmt.Errorf("could not stop old local Teams helper process(es) before bootstrap: %w", err)
		}
	}
	if opts.FallbackOnly {
		if wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
			if err := wslBackend.RetireScheduledTasks(ctx); err != nil {
				return teamsServiceBootstrapResult{}, fmt.Errorf("--fallback-only cannot safely start the Windows Startup watchdog because old WSL Scheduled Tasks could not be disabled: %w", err)
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
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok {
		return teamsServiceBootstrapResult{}, err
	}
	accessDenied := isTeamsServiceWindowsAccessDeniedError(err)
	if !accessDenied && !opts.NoUAC {
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
		if uacConfirmed {
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
	return backend.Uninstall(ctx)
}

type teamsServiceSpec struct {
	Executable   string
	WorkingDir   string
	RegistryPath string
	Environment  map[string]string
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
		if teamsServiceIsWSL() && strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND"))) != "systemd" {
			return teamsServiceWSLWindowsTaskBackend{}, nil
		}
		return teamsServiceSystemdBackend{}, nil
	case "darwin":
		return teamsServiceLaunchAgentBackend{}, nil
	case "windows":
		return teamsServiceWindowsTaskBackend{}, nil
	default:
		return nil, fmt.Errorf("unsupported platform %q: Teams service management supports Linux systemd --user, macOS LaunchAgent, and Windows per-user Task Scheduler", teamsServiceGOOS())
	}
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

func ensureTeamsServiceForRun(ctx context.Context, registryPath *string) error {
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
	_, err = repairTeamsService(ctx, registryPath, teamsServiceRepairOptions{Enable: true, Start: start})
	if err != nil {
		wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
		if ok {
			spec, specErr := buildTeamsServiceSpec(registryPath)
			if specErr != nil {
				return specErr
			}
			if retireErr := wslBackend.RetireScheduledTasks(ctx); retireErr != nil {
				return fmt.Errorf("WSL Scheduled Task setup failed (%v), and Startup fallback is unsafe because old WSL Scheduled Tasks could not be disabled: %w", err, retireErr)
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
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	return runTeamsServiceCommand(ctx, io.Discard, backend, "stop")
}

func startTeamsService(ctx context.Context, restart bool) error {
	action := "start"
	if restart {
		action = "restart"
	}
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	return runTeamsServiceCommand(ctx, io.Discard, backend, action)
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
		_, err := primary.RunPrimary(ctx, action)
		return err
	}
	return runTeamsServiceCommand(ctx, io.Discard, backend, action)
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
	case "enable", "disable", "start", "stop", "restart":
		return teamsServiceRunSystemctl(ctx, action, teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName)
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

func (b teamsServiceWindowsTaskBackend) Install(ctx context.Context, spec teamsServiceSpec) (string, error) {
	xmlPath, err := b.Path()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(xmlPath), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(xmlPath, []byte(buildTeamsServiceWindowsTaskXML(spec)), 0o600); err != nil {
		return "", err
	}
	watchdogXMLPath, err := teamsServiceWindowsWatchdogTaskXMLPath()
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(watchdogXMLPath, []byte(buildTeamsServiceWindowsWatchdogTaskXML(spec)), 0o600); err != nil {
		return "", err
	}
	cmd := "$xml = Get-Content -LiteralPath " + powershellSingleQuote(xmlPath) + " -Raw; Register-ScheduledTask -TaskName " + powershellSingleQuote(teamsServiceWindowsTaskName) + " -Xml $xml -Force | Out-Null"
	cmd += "; $watchdogXml = Get-Content -LiteralPath " + powershellSingleQuote(watchdogXMLPath) + " -Raw; Register-ScheduledTask -TaskName " + powershellSingleQuote(teamsServiceWindowsWatchdogTaskName) + " -Xml $watchdogXml -Force | Out-Null"
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
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
	return xmlPath, nil
}

func (teamsServiceWindowsTaskBackend) Run(ctx context.Context, action string) ([]byte, error) {
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	watchdogTask := powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)
	switch action {
	case "enable":
		return teamsServiceRunPowerShell(ctx, "Enable-ScheduledTask -TaskName "+task+" | Out-Null; Enable-ScheduledTask -TaskName "+watchdogTask+" | Out-Null")
	case "disable":
		return teamsServiceRunPowerShell(ctx, "Disable-ScheduledTask -TaskName "+watchdogTask+" | Out-Null; Disable-ScheduledTask -TaskName "+task+" | Out-Null")
	case "status":
		return teamsServiceRunPowerShell(ctx, "$task = Get-ScheduledTask -TaskName "+task+"; $info = Get-ScheduledTaskInfo -TaskName "+task+"; $task | Format-List TaskName,State; $info | Format-List LastRunTime,LastTaskResult,NextRunTime; $watchdog = Get-ScheduledTask -TaskName "+watchdogTask+"; $watchdogInfo = Get-ScheduledTaskInfo -TaskName "+watchdogTask+"; $watchdog | Format-List TaskName,State; $watchdogInfo | Format-List LastRunTime,LastTaskResult,NextRunTime")
	case "start":
		return teamsServiceRunPowerShell(ctx, "Start-ScheduledTask -TaskName "+task+"; Start-ScheduledTask -TaskName "+watchdogTask)
	case "stop":
		return teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+watchdogTask+" -ErrorAction SilentlyContinue; Stop-ScheduledTask -TaskName "+task)
	case "restart":
		return teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+task+" -ErrorAction SilentlyContinue; Start-ScheduledTask -TaskName "+task+"; Start-ScheduledTask -TaskName "+watchdogTask)
	default:
		return nil, fmt.Errorf("unsupported Teams service action for Task Scheduler: %s", action)
	}
}

func (teamsServiceWindowsTaskBackend) RunPrimary(ctx context.Context, action string) ([]byte, error) {
	task := powershellSingleQuote(teamsServiceWindowsTaskName)
	switch action {
	case "enable":
		return teamsServiceRunPowerShell(ctx, "Enable-ScheduledTask -TaskName "+task+" | Out-Null")
	case "disable":
		return teamsServiceRunPowerShell(ctx, "Disable-ScheduledTask -TaskName "+task+" | Out-Null")
	case "status":
		return teamsServiceRunPowerShell(ctx, "$task = Get-ScheduledTask -TaskName "+task+"; $info = Get-ScheduledTaskInfo -TaskName "+task+"; $task | Format-List TaskName,State; $info | Format-List LastRunTime,LastTaskResult,NextRunTime")
	case "start":
		return teamsServiceRunPowerShell(ctx, "Start-ScheduledTask -TaskName "+task)
	case "stop":
		return teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+task)
	case "restart":
		return teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+task+" -ErrorAction SilentlyContinue; Start-ScheduledTask -TaskName "+task)
	default:
		return nil, fmt.Errorf("unsupported primary Teams service action for Task Scheduler: %s", action)
	}
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
	fallbackSpec.Environment = make(map[string]string, len(spec.Environment)+3)
	for key, value := range spec.Environment {
		fallbackSpec.Environment[key] = value
	}
	fallbackSpec.Environment["CODEX_HELPER_TEAMS_STARTUP_FALLBACK"] = "1"
	fallbackSpec.Environment["CODEX_HELPER_TEAMS_STARTUP_FALLBACK_STOP_FILE"] = stopPath
	fallbackSpec.Environment["CODEX_HELPER_TEAMS_EXIT_ON_STANDBY"] = "1"
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
			return data, nil
		}
		if installed, markerErr := b.StartupFallbackMarkerExists(); markerErr == nil && installed {
			markerPath, _ := b.startupFallbackMarkerPath()
			return []byte("Scheduled Task: not registered\nStartup watchdog fallback: installed\nStartup watchdog config: " + markerPath + "\n"), nil
		}
		return data, err
	case "start":
		return teamsServiceRunPowerShell(ctx, resolve+teamsServiceWSLStartTaskAndVerifyPowerShell()+"; "+resolveWatchdog+"if ($null -ne $task) { "+teamsServiceWSLStartTaskIfStoppedAndVerifyPowerShell()+" }")
	case "stop":
		return teamsServiceRunPowerShell(ctx, resolveWatchdog+"if ($null -ne $task) { Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue }; "+resolve+"Stop-ScheduledTask -TaskName $taskName")
	case "restart":
		return teamsServiceRunPowerShell(ctx, resolve+"Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; "+teamsServiceWSLStartTaskAndVerifyPowerShell()+"; "+resolveWatchdog+"if ($null -ne $task) { "+teamsServiceWSLStartTaskIfStoppedAndVerifyPowerShell()+" }")
	default:
		return nil, fmt.Errorf("unsupported Teams service action for WSL Task Scheduler: %s", action)
	}
}

func (b teamsServiceWSLWindowsTaskBackend) Installed() (bool, error) {
	_, err := teamsServiceRunPowerShell(context.Background(), teamsServiceWSLResolveTaskPowerShell(b.Name())+"$task | Out-Null")
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (b teamsServiceWSLWindowsTaskBackend) Active(ctx context.Context) (bool, error) {
	_, err := teamsServiceRunPowerShell(ctx, teamsServiceWSLResolveTaskPowerShell(b.Name())+"if ($task.State -ne 'Running') { exit 3 }")
	if err != nil {
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
		return teamsServiceRunPowerShell(ctx, resolve+teamsServiceWSLStartTaskAndVerifyPowerShell())
	case "stop":
		return teamsServiceRunPowerShell(ctx, resolve+"Stop-ScheduledTask -TaskName $taskName")
	case "restart":
		return teamsServiceRunPowerShell(ctx, resolve+"Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; "+teamsServiceWSLStartTaskAndVerifyPowerShell())
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
	args := []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", command}
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
	lower := strings.ToLower(err.Error())
	for _, needle := range []string{
		"access is denied",
		"0x80070005",
		"unauthorizedaccessexception",
		"accessdenied",
		"e_accessdenied",
	} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
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

func buildTeamsServiceSpec(registryPath *string) (teamsServiceSpec, error) {
	exe, err := teamsServiceExecutable()
	if err != nil {
		return teamsServiceSpec{}, err
	}
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
			if teamsServiceShouldDropProxyEnv(name, value) {
				continue
			}
			env[name] = value
		}
	}
	if strings.TrimSpace(env[envCodexHome]) == "" || strings.TrimSpace(env["CODEX_DIR"]) == "" {
		codexHome, err := resolveCodexHome("", workingDir)
		if err == nil && strings.TrimSpace(codexHome) != "" {
			codexHome = strings.TrimSpace(codexHome)
			env[envCodexHome] = codexHome
			env["CODEX_DIR"] = codexHome
		}
	}
	return env, nil
}

func teamsServiceShouldDropProxyEnv(name string, value string) bool {
	if !teamsServiceProxyEnvName(name) || !teamsServiceDropLocalProxyEnv() {
		return false
	}
	return teamsServiceProxyIsLoopback(value)
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

func teamsServiceEnvironmentAllowlist() []string {
	return []string{
		"CODEX_HOME",
		"CODEX_DIR",
		"CODEX_HELPER_CONFIG",
		"CODEX_HELPER_TEAMS_PROFILE",
		"CODEX_HELPER_TEAMS_AUTH_PROFILE",
		"CODEX_HELPER_TEAMS_MACHINE_ID",
		"CODEX_HELPER_TEAMS_MACHINE_LABEL",
		"CODEX_HELPER_TEAMS_MACHINE_KIND",
		"CODEX_HELPER_TEAMS_MACHINE_PRIORITY",
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
		"HTTP_PROXY",
		"HTTPS_PROXY",
		"ALL_PROXY",
		"NO_PROXY",
		"http_proxy",
		"https_proxy",
		"all_proxy",
		"no_proxy",
	}
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
	args := buildTeamsServiceRunArgs(spec)
	command := spec.Executable
	arguments := windowsCommandLine(args)
	if len(spec.Environment) > 0 {
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
	args := buildTeamsServiceWatchdogArgs()
	command := spec.Executable
	arguments := windowsCommandLine(args)
	if len(spec.Environment) > 0 {
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
			"$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -RestartCount " + strconv.Itoa(teamsServiceTaskRestartCount) + " -RestartInterval (New-TimeSpan -Minutes " + strconv.Itoa(teamsServiceTaskSchedulerRestartMinutes) + "); " +
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
		" -or -not (Test-CodexHelperTaskDurationMinutes $settings.ExecutionTimeLimit 0)) { $allMatched = $false }; " +
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
	return "Start-ScheduledTask -TaskName $taskName | Out-Null; " + teamsServiceWSLVerifyTaskRunningPowerShell()
}

func teamsServiceWSLStartTaskIfStoppedAndVerifyPowerShell() string {
	return "if ($task.State -ne 'Running') { Start-ScheduledTask -TaskName $taskName | Out-Null }; " + teamsServiceWSLVerifyTaskRunningPowerShell()
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
	scriptName := "codex-helper-teams-wsl-" + identity.Suffix + ".ps1"
	launcherName := "codex-helper-teams-wsl-" + identity.Suffix + ".vbs"
	legacyCmdLauncherName := "codex-helper-teams-wsl-" + identity.Suffix + ".cmd"
	stopName := "codex-helper-teams-wsl-stop-" + identity.Suffix + ".signal"
	script := buildTeamsServiceWSLStartupWatchdogScript(taskName, args, identity.Suffix)
	cleanup := buildTeamsServiceWSLRemoveStartupFallbackCommand(taskName, []string{identity.Suffix})
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
		"foreach ($path in $paths) { Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue } " +
		"}"
}

func buildTeamsServiceWSLStartupWatchdogScript(taskName string, args []string, suffix string) string {
	mutexName := "Local\\CodexHelperTeamsWSLWatchdog-" + safeWindowsTaskNamePart(suffix, 32)
	runLogName := "codex-helper-teams-wsl-run-" + safeWindowsTaskNamePart(suffix, 32) + ".log"
	watchdogLogName := "codex-helper-teams-wsl-watchdog-" + safeWindowsTaskNamePart(suffix, 32) + ".log"
	stopName := "codex-helper-teams-wsl-stop-" + safeWindowsTaskNamePart(suffix, 32) + ".signal"
	wslArgumentLine := windowsCommandLine(args)
	var b strings.Builder
	b.WriteString("$ErrorActionPreference = 'Continue'\r\n")
	b.WriteString("$created = $false\r\n")
	b.WriteString("$mutex = New-Object System.Threading.Mutex($true, " + powershellSingleQuote(mutexName) + ", [ref]$created)\r\n")
	b.WriteString("if (-not $created) { exit 0 }\r\n")
	b.WriteString("try {\r\n")
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
	b.WriteString("    if ($code -eq 0) { Add-Content -LiteralPath $watchdogLog -Value ((Get-Date).ToString('o') + ' wsl.exe exited 0; exiting watchdog'); break }\r\n")
	b.WriteString("    Add-Content -LiteralPath $watchdogLog -Value ((Get-Date).ToString('o') + ' wsl.exe exited ' + $code + '; restarting in " + strconv.Itoa(teamsServiceTaskRestartInterval) + "s')\r\n")
	b.WriteString("    Start-Sleep -Seconds " + strconv.Itoa(teamsServiceTaskRestartInterval) + "\r\n")
	b.WriteString("  }\r\n")
	b.WriteString("} finally {\r\n")
	b.WriteString("  if ($null -ne $mutex) { $mutex.ReleaseMutex(); $mutex.Dispose() }\r\n")
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
	call := "& " + powershellSingleQuote(spec.Executable)
	for _, arg := range args {
		call += " " + powershellSingleQuote(arg)
	}
	parts = append(parts, call)
	parts = append(parts, "$code = $LASTEXITCODE")
	parts = append(parts, "if ($null -eq $code) { $code = 1 }")
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
	data, err := os.ReadFile("/proc/version")
	if err != nil {
		return false
	}
	version := strings.ToLower(string(data))
	return strings.Contains(version, "microsoft") || strings.Contains(version, "wsl")
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
