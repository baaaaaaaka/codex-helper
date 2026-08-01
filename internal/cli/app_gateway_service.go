package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/appgateway"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

var (
	appGatewayServiceGOOS       = func() string { return runtime.GOOS }
	appGatewayServiceIsWSL      = func() bool { return teamsServiceIsWSL() }
	appGatewayServiceRunCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, name, args...).CombinedOutput()
	}
	appGatewayServiceUserConfigDir = os.UserConfigDir
	appGatewayServiceUserHome      = os.UserHomeDir
)

func appGatewayServiceName(reg appgateway.Registration) string {
	return "codex-helper-app-gateway-" + reg.ID
}

func ensureAppGatewayService(ctx context.Context, store *config.Store, profile config.Profile, reg appgateway.Registration, stateDir string) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("app gateway service config store is required")
	}
	exe, err := proxyExecutable()
	if err != nil {
		return false, err
	}
	resolved, err := helperpath.StableRunnablePathFromSources(exe, restartArgv0(), helperpath.Options{})
	if err != nil {
		return false, err
	}
	spec := appgateway.ServiceSpec{
		Name:       appGatewayServiceName(reg),
		Executable: resolved.Path,
		ConfigPath: store.Path(),
		ProfileID:  profile.ID,
		StateDir:   stateDir,
	}
	switch appGatewayServiceGOOS() {
	case "windows":
		spec.Platform = appgateway.ServiceWindows
		return ensureAppGatewayWindowsTask(ctx, spec)
	case "darwin":
		spec.Platform = appgateway.ServiceLaunchd
		return ensureAppGatewayLaunchAgent(ctx, spec)
	case "linux":
		if appGatewayServiceIsWSL() {
			return false, fmt.Errorf("WSL requires a Windows-side App Gateway companion")
		}
		spec.Platform = appgateway.ServiceSystemd
		return ensureAppGatewaySystemd(ctx, spec)
	default:
		return false, fmt.Errorf("unsupported app gateway service platform %q", appGatewayServiceGOOS())
	}
}

func ensureAppGatewaySystemd(ctx context.Context, spec appgateway.ServiceSpec) (bool, error) {
	configDir, err := appGatewayServiceUserConfigDir()
	if err != nil {
		return false, err
	}
	unitDir := filepath.Join(configDir, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		return false, fmt.Errorf("create systemd user unit directory: %w", err)
	}
	rendered, err := spec.Render()
	if err != nil {
		return false, err
	}
	unitPath := filepath.Join(unitDir, spec.UnitName()+".service")
	if err := writeAppGatewayPrivateFile(unitPath, rendered, 0o600); err != nil {
		return false, fmt.Errorf("write app gateway systemd unit: %w", err)
	}
	if _, err := appGatewayServiceRunCommand(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return false, fmt.Errorf("reload systemd user units: %w", err)
	}
	if _, err := appGatewayServiceRunCommand(ctx, "systemctl", "--user", "enable", "--now", spec.UnitName()+".service"); err != nil {
		return false, fmt.Errorf("enable app gateway systemd unit: %w", err)
	}
	return true, nil
}

func ensureAppGatewayLaunchAgent(ctx context.Context, spec appgateway.ServiceSpec) (bool, error) {
	home, err := appGatewayServiceUserHome()
	if err != nil {
		return false, err
	}
	launchDir := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(launchDir, 0o700); err != nil {
		return false, fmt.Errorf("create LaunchAgents directory: %w", err)
	}
	rendered, err := spec.Render()
	if err != nil {
		return false, err
	}
	plistPath := filepath.Join(launchDir, spec.UnitName()+".plist")
	if err := writeAppGatewayPrivateFile(plistPath, rendered, 0o600); err != nil {
		return false, fmt.Errorf("write app gateway LaunchAgent: %w", err)
	}
	if _, err := appGatewayServiceRunCommand(ctx, "launchctl", "load", "-w", plistPath); err != nil {
		return false, fmt.Errorf("load app gateway LaunchAgent: %w", err)
	}
	return true, nil
}

func ensureAppGatewayWindowsTask(ctx context.Context, spec appgateway.ServiceSpec) (bool, error) {
	stateDir := strings.TrimSpace(spec.StateDir)
	if stateDir == "" {
		return false, fmt.Errorf("Windows app gateway state directory is required")
	}
	taskDir := filepath.Join(stateDir, "tasks")
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		return false, fmt.Errorf("create Windows app gateway task directory: %w", err)
	}
	rendered, err := spec.Render()
	if err != nil {
		return false, err
	}
	xmlPath := filepath.Join(taskDir, spec.UnitName()+".xml")
	if err := writeAppGatewayPrivateFile(xmlPath, rendered, 0o600); err != nil {
		return false, fmt.Errorf("write Windows app gateway task: %w", err)
	}
	if _, err := appGatewayServiceRunCommand(ctx, "schtasks.exe", "/Create", "/TN", spec.UnitName(), "/XML", xmlPath, "/F"); err != nil {
		return false, fmt.Errorf("register Windows app gateway task: %w", err)
	}
	if _, err := appGatewayServiceRunCommand(ctx, "schtasks.exe", "/Run", "/TN", spec.UnitName()); err != nil {
		return false, fmt.Errorf("start Windows app gateway task: %w", err)
	}
	if err := ensureAppGatewayWindowsLauncher(spec); err != nil {
		// The task is already registered and running; a launcher permission
		// failure must not make the caller start a second detached daemon.
		return true, err
	}
	return true, nil
}

// ensureAppGatewayWindowsLauncher creates the user-visible one-time entry
// point promised by `cxp app`. The stock Store icon cannot carry Chromium's
// proxy argument; this Start Menu command reruns the idempotent CXP app path,
// which reuses the same registration and starts the stable frontdoor.
func ensureAppGatewayWindowsLauncher(spec appgateway.ServiceSpec) error {
	configDir, err := appGatewayServiceUserConfigDir()
	if err != nil {
		return err
	}
	launcherDir := filepath.Join(configDir, "Microsoft", "Windows", "Start Menu", "Programs")
	if err := os.MkdirAll(launcherDir, 0o700); err != nil {
		return fmt.Errorf("create CXP Start Menu directory: %w", err)
	}
	if strings.ContainsAny(spec.ProfileID, "\r\n\"") || strings.ContainsAny(spec.Executable, "\r\n\"") || strings.ContainsAny(spec.ConfigPath, "\r\n\"") {
		return fmt.Errorf("unsafe Windows App Gateway launcher path or profile")
	}
	name := "ChatGPT via CXP - " + spec.Name + ".cmd"
	content := "@echo off\r\nsetlocal\r\n" + windowsBatchQuote(spec.Executable) + " --config " + windowsBatchQuote(spec.ConfigPath) + " app --profile " + windowsBatchQuote(spec.ProfileID) + "\r\nendlocal\r\n"
	path := filepath.Join(launcherDir, name)
	if err := writeAppGatewayPrivateFile(path, []byte(content), 0o700); err != nil {
		return fmt.Errorf("write CXP Start Menu launcher: %w", err)
	}
	return nil
}

func windowsBatchQuote(value string) string {
	return `"` + value + `"`
}

func writeAppGatewayPrivateFile(path string, data []byte, mode os.FileMode) error {
	if existing, err := os.ReadFile(path); err == nil && string(existing) == string(data) {
		return nil
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".app-gateway-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
