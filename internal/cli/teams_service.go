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

func newTeamsServiceInstallCmd(registryPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "install",
		Short: "Install the Teams bridge user service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
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
	readCfg, err := teams.DefaultReadAuthConfig()
	if err != nil {
		return err
	}
	readStatus, err := teams.TokenCacheStatus(readCfg.CachePath)
	if err != nil {
		return err
	}
	switch readStatus {
	case "missing", "empty", "present, access token expired":
		return fmt.Errorf("Teams read auth cache is not ready for background service (%s at %s); run `codex-proxy teams auth read` in a foreground terminal first", readStatus, readCfg.CachePath)
	}
	writeCfg, err := teams.DefaultAuthConfig()
	if err != nil {
		return err
	}
	writeStatus, err := teams.TokenCacheStatus(writeCfg.CachePath)
	if err != nil {
		return err
	}
	switch writeStatus {
	case "missing", "empty", "present, access token expired":
		return fmt.Errorf("Teams write auth cache is not ready for background service (%s at %s); run `codex-proxy teams auth` in a foreground terminal first", writeStatus, writeCfg.CachePath)
	}
	return nil
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
	xmlPath, err := b.Path()
	if err != nil {
		return false, err
	}
	return teamsServiceFileExists(xmlPath)
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

func (b teamsServiceWSLWindowsTaskBackend) Install(ctx context.Context, spec teamsServiceSpec) (string, error) {
	configPath, err := b.Path()
	if err != nil {
		return "", err
	}
	args := buildTeamsServiceWSLArguments(spec)
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(configPath, []byte(buildTeamsServiceWSLTaskConfig(b.Name(), args)), 0o600); err != nil {
		return "", err
	}
	task := powershellSingleQuote(b.Name())
	arg := powershellSingleQuote(windowsCommandLine(args))
	cmd := "$action = New-ScheduledTaskAction -Execute 'wsl.exe' -Argument " + arg + "; " +
		"$trigger = New-ScheduledTaskTrigger -AtLogOn; " +
		"$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount " + strconv.Itoa(teamsServiceTaskRestartCount) + " -RestartInterval (New-TimeSpan -Seconds " + strconv.Itoa(teamsServiceTaskRestartInterval) + "); " +
		"$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited; " +
		"Register-ScheduledTask -TaskName " + task + " -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null; " +
		"Disable-ScheduledTask -TaskName " + task + " | Out-Null"
	if _, err := teamsServiceRunPowerShell(ctx, cmd); err != nil {
		return "", err
	}
	return configPath, nil
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
		return teamsServiceRunPowerShell(ctx, "$task = Get-ScheduledTask -TaskName "+task+"; $info = Get-ScheduledTaskInfo -TaskName "+task+"; $task | Format-List TaskName,State; $info | Format-List LastRunTime,LastTaskResult,NextRunTime")
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
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
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
	args = append(args, spec.Executable, "teams", "run")
	if spec.RegistryPath != "" {
		args = append(args, "--registry", spec.RegistryPath)
	}
	return args
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
