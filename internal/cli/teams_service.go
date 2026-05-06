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

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

const (
	teamsServiceUnitName             = "codex-helper-teams.service"
	teamsServiceLaunchAgentLabel     = "com.codex-helper.teams"
	teamsServiceLaunchAgentPlistName = teamsServiceLaunchAgentLabel + ".plist"
	teamsServiceWindowsTaskName      = "Codex Helper Teams Bridge"
	teamsServiceWindowsTaskXMLName   = "codex-helper-teams-task.xml"
	teamsServiceWSLTaskConfigName    = "codex-helper-teams-wsl-task.txt"
	teamsServiceTaskRestartCount     = 999
	teamsServiceTaskRestartInterval  = 60
	teamsServiceWatchdogMinutes      = 30
	teamsServiceWatchdogDays         = 3650
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
)

func newTeamsServiceCmd(root *rootOptions, registryPath *string) *cobra.Command {
	_ = root
	cmd := &cobra.Command{
		Use:   "service",
		Short: "Manage the Teams bridge background service",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(
		newTeamsServiceInstallCmd(registryPath),
		newTeamsServiceBootstrapCmd(registryPath),
		newTeamsServiceUninstallCmd(),
		newTeamsServiceEnableCmd(),
		newTeamsServiceDisableCmd(),
		newTeamsServiceStatusCmd(),
		newTeamsServiceStartCmd(),
		newTeamsServiceStopCmd(),
		newTeamsServiceRestartCmd(),
		newTeamsServiceDoctorCmd(),
	)
	return cmd
}

func newTeamsServiceBootstrapCmd(registryPath *string) *cobra.Command {
	var yes bool
	var noUAC bool
	var fallbackOnly bool
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Install or repair the Teams bridge background service",
		Long:  "Install or repair the Teams bridge background service. On WSL, this first tries a current-user Windows Scheduled Task. If Windows blocks that path, it can ask before opening a UAC prompt and then falls back to a current-user Startup watchdog.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("bootstrap the Teams service", "helper reload now"); err != nil {
				return err
			}
			if err := teamsServiceAuthPreflight(); err != nil {
				return err
			}
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
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service bootstrap ready: %s\n", result.Mode)
			if strings.TrimSpace(result.Path) != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams service config: %s\n", result.Path)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Approve the Windows UAC prompt if the WSL Scheduled Task needs elevation")
	cmd.Flags().BoolVar(&noUAC, "no-uac", false, "Do not open a Windows UAC prompt; use the current-user Startup fallback if needed")
	cmd.Flags().BoolVar(&fallbackOnly, "fallback-only", false, "Install the current-user Startup watchdog instead of trying Windows Task Scheduler")
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

func bootstrapTeamsService(ctx context.Context, registryPath *string, opts teamsServiceBootstrapOptions) (teamsServiceBootstrapResult, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return teamsServiceBootstrapResult{}, err
	}
	spec, err := buildTeamsServiceSpec(registryPath)
	if err != nil {
		return teamsServiceBootstrapResult{}, err
	}
	if opts.FallbackOnly {
		if wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
			path, err := wslBackend.InstallStartupFallback(ctx, spec, true)
			return teamsServiceBootstrapResult{Mode: "wsl-startup-watchdog", Path: path}, err
		}
		return teamsServiceBootstrapResult{}, fmt.Errorf("--fallback-only is only supported by the WSL Windows service backend")
	}
	path, err := repairTeamsService(ctx, registryPath, teamsServiceRepairOptions{Enable: true, Start: true})
	if err == nil {
		if wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
			_ = wslBackend.RemoveStartupFallbackMarker()
		}
		return teamsServiceBootstrapResult{Mode: backend.ID(), Path: path}, nil
	}
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok {
		return teamsServiceBootstrapResult{}, err
	}
	accessDenied := isTeamsServiceWindowsAccessDeniedError(err)
	if !accessDenied && !opts.NoUAC {
		return teamsServiceBootstrapResult{}, err
	}
	out := opts.Out
	if out == nil {
		out = io.Discard
	}
	if accessDenied {
		_, _ = fmt.Fprintf(out, "Windows blocked automatic Scheduled Task setup: %v\n", err)
	} else {
		_, _ = fmt.Fprintf(out, "Windows Scheduled Task setup failed: %v\n", err)
	}
	if accessDenied && !opts.NoUAC && confirmTeamsServiceUACPrompt(opts.In, out, opts.AssumeYes) {
		principalUser, userErr := teamsServiceCurrentWindowsUser(ctx)
		if userErr != nil {
			_, _ = fmt.Fprintf(out, "Could not identify the current Windows user for the elevated task: %v\n", userErr)
		} else {
			if path, elevateErr := wslBackend.RepairElevated(ctx, spec, teamsServiceRepairOptions{Enable: true, Start: true}, principalUser); elevateErr == nil {
				_ = wslBackend.RemoveStartupFallbackMarker()
				return teamsServiceBootstrapResult{Mode: "wsl-windows-task-scheduler-uac", Path: path}, nil
			} else {
				_, _ = fmt.Fprintf(out, "Elevated Scheduled Task setup failed: %v\n", elevateErr)
			}
		}
	}
	_, _ = fmt.Fprintln(out, "Installing the current-user Windows Startup watchdog fallback instead.")
	path, fallbackErr := wslBackend.InstallStartupFallback(ctx, spec, true)
	return teamsServiceBootstrapResult{Mode: "wsl-startup-watchdog", Path: path}, fallbackErr
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
			if _, fallbackErr := wslBackend.InstallStartupFallback(ctx, spec, false); fallbackErr != nil {
				return fmt.Errorf("WSL Scheduled Task setup was blocked (%v), and Startup fallback setup failed: %w", err, fallbackErr)
			}
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
	if _, err := teamsServiceRunSystemctl(ctx, "daemon-reload"); err != nil {
		return "", err
	}
	return unitPath, nil
}

func (teamsServiceSystemdBackend) Run(ctx context.Context, action string) ([]byte, error) {
	args := []string{action, teamsServiceUnitName}
	if action == "status" {
		args = []string{"status", "--no-pager", teamsServiceUnitName}
	}
	return teamsServiceRunSystemctl(ctx, args...)
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
	return plistPath, nil
}

func (b teamsServiceLaunchAgentBackend) Uninstall(ctx context.Context) (string, error) {
	plistPath, err := b.Path()
	if err != nil {
		return "", err
	}
	_, _ = teamsServiceRunLaunchctl(ctx, "bootout", teamsServiceLaunchctlServiceTarget())
	if err := os.Remove(plistPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return plistPath, nil
}

func (b teamsServiceLaunchAgentBackend) Run(ctx context.Context, action string) ([]byte, error) {
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
	cmd := "$xml = Get-Content -LiteralPath " + powershellSingleQuote(xmlPath) + " -Raw; Register-ScheduledTask -TaskName " + powershellSingleQuote(teamsServiceWindowsTaskName) + " -Xml $xml -Force | Out-Null"
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
	cmd := "if (Get-ScheduledTask -TaskName " + task + " -ErrorAction SilentlyContinue) { Unregister-ScheduledTask -TaskName " + task + " -Confirm:$false }"
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		return "", err
	}
	if err := os.Remove(xmlPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return xmlPath, nil
}

func (teamsServiceWindowsTaskBackend) Run(ctx context.Context, action string) ([]byte, error) {
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
		return nil, fmt.Errorf("unsupported Teams service action for Task Scheduler: %s", action)
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

func (b teamsServiceWSLWindowsTaskBackend) writeTaskConfig(spec teamsServiceSpec) (string, []string, error) {
	configPath, err := b.Path()
	if err != nil {
		return "", nil, err
	}
	args := buildTeamsServiceWSLArguments(spec)
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return "", nil, err
	}
	if err := os.WriteFile(configPath, []byte(buildTeamsServiceWSLTaskConfig(b.Name(), args)), 0o600); err != nil {
		return "", nil, err
	}
	return configPath, args, nil
}

func (b teamsServiceWSLWindowsTaskBackend) Install(ctx context.Context, spec teamsServiceSpec) (string, error) {
	configPath, args, err := b.writeTaskConfig(spec)
	if err != nil {
		return "", err
	}
	cmd := buildTeamsServiceWSLRegisterCommand(b.Name(), args, teamsServiceWSLRegisterOptions{ForceDisabled: true})
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		return "", err
	}
	return configPath, nil
}

func (b teamsServiceWSLWindowsTaskBackend) Repair(ctx context.Context, spec teamsServiceSpec, opts teamsServiceRepairOptions) (string, error) {
	configPath, args, err := b.writeTaskConfig(spec)
	if err != nil {
		return "", err
	}
	cmd := buildTeamsServiceWSLRegisterCommand(b.Name(), args, teamsServiceWSLRegisterOptions{
		Enable:          opts.Enable,
		Start:           opts.Start,
		PreserveEnabled: !opts.Enable,
		PreserveRunning: !opts.Start,
	})
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		return "", err
	}
	return configPath, nil
}

func (b teamsServiceWSLWindowsTaskBackend) RepairElevated(ctx context.Context, spec teamsServiceSpec, opts teamsServiceRepairOptions, principalUser string) (string, error) {
	configPath, args, err := b.writeTaskConfig(spec)
	if err != nil {
		return "", err
	}
	cmd := buildTeamsServiceWSLRegisterCommand(b.Name(), args, teamsServiceWSLRegisterOptions{
		Enable:          opts.Enable,
		Start:           opts.Start,
		PreserveEnabled: !opts.Enable,
		PreserveRunning: !opts.Start,
		PrincipalUser:   principalUser,
	})
	if _, err := teamsServiceRunPowerShell(ctx, buildTeamsServiceWSLElevatedCommand(cmd)); err != nil {
		return "", err
	}
	return configPath, nil
}

func (b teamsServiceWSLWindowsTaskBackend) InstallStartupFallback(ctx context.Context, spec teamsServiceSpec, start bool) (string, error) {
	markerPath, err := b.startupFallbackMarkerPath()
	if err != nil {
		return "", err
	}
	args := buildTeamsServiceWSLArguments(spec)
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
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

func (b teamsServiceWSLWindowsTaskBackend) StartupFallbackMarkerExists() (bool, error) {
	markerPath, err := b.startupFallbackMarkerPath()
	if err != nil {
		return false, err
	}
	return teamsServiceFileExists(markerPath)
}

func (b teamsServiceWSLWindowsTaskBackend) RemoveStartupFallbackMarker() error {
	markerPath, err := b.startupFallbackMarkerPath()
	if err != nil {
		return err
	}
	if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b teamsServiceWSLWindowsTaskBackend) Uninstall(ctx context.Context) (string, error) {
	configPath, err := b.Path()
	if err != nil {
		return "", err
	}
	task := powershellSingleQuote(b.Name())
	cmd := "if (Get-ScheduledTask -TaskName " + task + " -ErrorAction SilentlyContinue) { Unregister-ScheduledTask -TaskName " + task + " -Confirm:$false }"
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
	task := powershellSingleQuote(b.Name())
	switch action {
	case "enable":
		return teamsServiceRunPowerShell(ctx, "Enable-ScheduledTask -TaskName "+task+" | Out-Null")
	case "disable":
		return teamsServiceRunPowerShell(ctx, "Disable-ScheduledTask -TaskName "+task+" | Out-Null")
	case "status":
		data, err := teamsServiceRunPowerShell(ctx, "$task = Get-ScheduledTask -TaskName "+task+" -ErrorAction Stop; $info = Get-ScheduledTaskInfo -TaskName "+task+"; $task | Format-List TaskName,State; $info | Format-List LastRunTime,LastTaskResult,NextRunTime")
		if err == nil {
			return data, nil
		}
		if installed, markerErr := b.StartupFallbackMarkerExists(); markerErr == nil && installed {
			markerPath, _ := b.startupFallbackMarkerPath()
			return []byte("Scheduled Task: not registered\nStartup watchdog fallback: installed\nStartup watchdog config: " + markerPath + "\n"), nil
		}
		return data, err
	case "start":
		return teamsServiceRunPowerShell(ctx, "Start-ScheduledTask -TaskName "+task)
	case "stop":
		return teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+task)
	case "restart":
		return teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+task+" -ErrorAction SilentlyContinue; Start-ScheduledTask -TaskName "+task)
	default:
		return nil, fmt.Errorf("unsupported Teams service action for WSL Task Scheduler: %s", action)
	}
}

func (b teamsServiceWSLWindowsTaskBackend) Installed() (bool, error) {
	_, err := teamsServiceRunPowerShell(context.Background(), "Get-ScheduledTask -TaskName "+powershellSingleQuote(b.Name())+" -ErrorAction Stop | Out-Null")
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (b teamsServiceWSLWindowsTaskBackend) Active(ctx context.Context) (bool, error) {
	_, err := teamsServiceRunPowerShell(ctx, "$task = Get-ScheduledTask -TaskName "+powershellSingleQuote(b.Name())+"; if ($task.State -ne 'Running') { exit 3 }")
	if err != nil {
		return false, nil
	}
	return true, nil
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

func teamsServiceRunPowerShell(ctx context.Context, command string) ([]byte, error) {
	args := []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", command}
	name := teamsServicePowerShellExecutable()
	data, err := teamsServiceRunCommandDirect(ctx, name, args...)
	if err != nil {
		if len(data) > 0 {
			return data, fmt.Errorf("%s %s failed: %w", name, strings.Join(args, " "), err)
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

func confirmTeamsServiceUACPrompt(in io.Reader, out io.Writer, assumeYes bool) bool {
	if out == nil {
		out = io.Discard
	}
	_, _ = fmt.Fprintln(out, "A Windows UAC prompt is about to appear to install a current-user Teams helper Scheduled Task.")
	_, _ = fmt.Fprintln(out, "The task targets only the current Windows user and uses least privilege.")
	if assumeYes {
		_, _ = fmt.Fprintln(out, "UAC prompt approved by --yes.")
		return true
	}
	_, _ = fmt.Fprint(out, "Show the UAC prompt now? Type yes to continue: ")
	if in == nil {
		in = os.Stdin
	}
	var answer string
	if _, err := fmt.Fscan(in, &answer); err != nil {
		_, _ = fmt.Fprintln(out)
		_, _ = fmt.Fprintln(out, "UAC prompt was not confirmed.")
		return false
	}
	confirmed := strings.EqualFold(strings.TrimSpace(answer), "yes") || strings.EqualFold(strings.TrimSpace(answer), "y")
	if !confirmed {
		_, _ = fmt.Fprintln(out, "UAC prompt was not confirmed.")
	}
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
	return teamsServiceSpec{
		Executable:   exe,
		WorkingDir:   cwd,
		RegistryPath: resolvedRegistryPath,
		Environment:  teamsServiceEnvironment(),
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
	return env
}

func teamsServiceShouldDropProxyEnv(name string, value string) bool {
	if !teamsServiceProxyEnvName(name) || teamsServiceKeepLocalProxyEnv() {
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
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_KEEP_LOCAL_PROXY"))) {
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

func buildTeamsServiceUnit(spec teamsServiceSpec) string {
	args := []string{
		systemdQuoteArg(spec.Executable),
		"teams",
		"run",
	}
	if spec.RegistryPath != "" {
		args = append(args, "--registry", systemdQuoteArg(spec.RegistryPath))
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

func buildTeamsServiceLaunchAgentPlist(spec teamsServiceSpec) string {
	args := []string{spec.Executable, "teams", "run"}
	if spec.RegistryPath != "" {
		args = append(args, "--registry", spec.RegistryPath)
	}
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

func buildTeamsServiceWindowsTaskXML(spec teamsServiceSpec) string {
	args := []string{"teams", "run"}
	if spec.RegistryPath != "" {
		args = append(args, "--registry", spec.RegistryPath)
	}
	command := spec.Executable
	arguments := windowsCommandLine(args)
	if len(spec.Environment) > 0 {
		command = "powershell.exe"
		arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -Command " + windowsQuoteArg(buildTeamsServiceWindowsPowerShell(spec, args))
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
	b.WriteString("    <StartWhenAvailable>true</StartWhenAvailable>\n")
	b.WriteString("    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>\n")
	b.WriteString("    <Enabled>false</Enabled>\n")
	b.WriteString("    <RestartOnFailure>\n")
	b.WriteString("      <Interval>PT" + strconv.Itoa(teamsServiceTaskRestartInterval) + "S</Interval>\n")
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
	args = append(args, "--", "env")
	for _, key := range sortedEnvironmentKeys(spec.Environment) {
		args = append(args, key+"="+spec.Environment[key])
	}
	args = append(args, spec.Executable, "teams", "run", "--auto-service=false")
	if spec.RegistryPath != "" {
		args = append(args, "--registry", spec.RegistryPath)
	}
	return args
}

type teamsServiceWSLRegisterOptions struct {
	ForceDisabled   bool
	Enable          bool
	Start           bool
	PreserveEnabled bool
	PreserveRunning bool
	PrincipalUser   string
}

func buildTeamsServiceWSLRegisterCommand(taskName string, args []string, opts teamsServiceWSLRegisterOptions) string {
	task := powershellSingleQuote(taskName)
	arg := powershellSingleQuote(windowsCommandLine(args))
	principalUser := "[System.Security.Principal.WindowsIdentity]::GetCurrent().Name"
	if strings.TrimSpace(opts.PrincipalUser) != "" {
		principalUser = powershellSingleQuote(strings.TrimSpace(opts.PrincipalUser))
	}
	cmd := "$taskName = " + task + "; " +
		"$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; " +
		"$wasEnabled = $false; $wasRunning = $false; " +
		"if ($null -ne $existing) { $wasEnabled = $existing.State -ne 'Disabled'; $wasRunning = $existing.State -eq 'Running' }; " +
		"$action = New-ScheduledTaskAction -Execute 'wsl.exe' -Argument " + arg + "; " +
		"$logon = New-ScheduledTaskTrigger -AtLogOn; " +
		"$watchdog = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes " + strconv.Itoa(teamsServiceWatchdogMinutes) + ") -RepetitionDuration (New-TimeSpan -Days " + strconv.Itoa(teamsServiceWatchdogDays) + "); " +
		"$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -RestartCount " + strconv.Itoa(teamsServiceTaskRestartCount) + " -RestartInterval (New-TimeSpan -Seconds " + strconv.Itoa(teamsServiceTaskRestartInterval) + "); " +
		"$principalUser = " + principalUser + "; " +
		"$principal = New-ScheduledTaskPrincipal -UserId $principalUser -LogonType Interactive -RunLevel Limited; " +
		"Register-ScheduledTask -TaskName $taskName -Action $action -Trigger @($logon, $watchdog) -Settings $settings -Principal $principal -Force | Out-Null; "
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
		cmd += "Start-ScheduledTask -TaskName $taskName | Out-Null"
	case opts.PreserveRunning:
		cmd += "if ($wasRunning) { Start-ScheduledTask -TaskName $taskName | Out-Null }"
	}
	return strings.TrimSpace(cmd)
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
	cmdName := "codex-helper-teams-wsl-" + identity.Suffix + ".cmd"
	script := buildTeamsServiceWSLStartupWatchdogScript(taskName, args, identity.Suffix)
	cmdScript := "@echo off\r\npowershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File \"%LOCALAPPDATA%\\codex-helper\\teams\\" + scriptName + "\"\r\n"
	ps := "$startup = [Environment]::GetFolderPath('Startup'); " +
		"if ([string]::IsNullOrWhiteSpace($startup)) { throw 'Windows Startup folder is unavailable' }; " +
		"$appDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\teams'; " +
		"New-Item -ItemType Directory -Force -Path $appDir | Out-Null; " +
		"$scriptPath = Join-Path $appDir " + powershellSingleQuote(scriptName) + "; " +
		"$cmdPath = Join-Path $startup " + powershellSingleQuote(cmdName) + "; " +
		"Set-Content -LiteralPath $scriptPath -Value " + powershellSingleQuote(script) + " -Encoding UTF8; " +
		"Set-Content -LiteralPath $cmdPath -Value " + powershellSingleQuote(cmdScript) + " -Encoding ASCII"
	if start {
		ps += "; Start-Process -FilePath $cmdPath -WindowStyle Hidden | Out-Null"
	}
	return ps
}

func buildTeamsServiceWSLStartupWatchdogScript(taskName string, args []string, suffix string) string {
	mutexName := "Local\\CodexHelperTeamsWSLWatchdog-" + safeWindowsTaskNamePart(suffix, 32)
	runLogName := "codex-helper-teams-wsl-run-" + safeWindowsTaskNamePart(suffix, 32) + ".log"
	watchdogLogName := "codex-helper-teams-wsl-watchdog-" + safeWindowsTaskNamePart(suffix, 32) + ".log"
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
	b.WriteString("  $wslArgs = " + powershellArrayLiteral(args) + "\r\n")
	b.WriteString("  Add-Content -LiteralPath $watchdogLog -Value ((Get-Date).ToString('o') + ' starting " + strings.ReplaceAll(taskName, "'", "''") + "')\r\n")
	b.WriteString("  while ($true) {\r\n")
	b.WriteString("    & wsl.exe @wslArgs *>> $runLog\r\n")
	b.WriteString("    $code = $LASTEXITCODE\r\n")
	b.WriteString("    Add-Content -LiteralPath $watchdogLog -Value ((Get-Date).ToString('o') + ' wsl.exe exited ' + $code + '; restarting in 30s')\r\n")
	b.WriteString("    Start-Sleep -Seconds 30\r\n")
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
	return strings.Join(parts, "; ")
}

func teamsServiceUnitPath() (string, error) {
	dir, err := teamsServiceSystemdUserDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, teamsServiceUnitName), nil
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
	configPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_CONFIG"))
	codexHome := strings.TrimSpace(firstNonEmptyTeamsServiceString(os.Getenv("CODEX_HOME"), os.Getenv("CODEX_DIR")))
	raw := strings.Join([]string{distro, linuxUser, profile, machineID, configPath, codexHome}, "\x00")
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
