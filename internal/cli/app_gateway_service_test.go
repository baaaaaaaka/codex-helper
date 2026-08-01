package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/appgateway"
)

func serviceTestSpec(t *testing.T, platform appgateway.ServicePlatform, stateDir string) appgateway.ServiceSpec {
	t.Helper()
	return appgateway.ServiceSpec{
		Platform:   platform,
		Name:       "codex-helper-app-gateway-test",
		Executable: filepath.Join(stateDir, "bin", "cxp"),
		ConfigPath: filepath.Join(stateDir, "config.json"),
		ProfileID:  "profile-a",
		StateDir:   stateDir,
	}
}

func TestEnsureAppGatewaySystemdIsIdempotentAndUserScoped(t *testing.T) {
	stateDir := t.TempDir()
	configDir := t.TempDir()
	oldConfigDir := appGatewayServiceUserConfigDir
	oldRun := appGatewayServiceRunCommand
	t.Cleanup(func() {
		appGatewayServiceUserConfigDir = oldConfigDir
		appGatewayServiceRunCommand = oldRun
	})
	appGatewayServiceUserConfigDir = func() (string, error) { return configDir, nil }
	var calls [][]string
	appGatewayServiceRunCommand = func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		calls = append(calls, append([]string{name}, arguments...))
		return nil, nil
	}
	spec := serviceTestSpec(t, appgateway.ServiceSystemd, stateDir)
	installed, err := ensureAppGatewaySystemd(context.Background(), spec)
	if err != nil || !installed {
		t.Fatalf("ensure systemd = %v/%v", installed, err)
	}
	unitPath := filepath.Join(configDir, "systemd", "user", spec.UnitName()+".service")
	b, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "Restart=on-failure") || !strings.Contains(string(b), "--profile-id profile-a") {
		t.Fatalf("unit does not contain bounded restart and profile: %s", b)
	}
	if len(calls) != 2 || calls[0][0] != "systemctl" || calls[1][0] != "systemctl" {
		t.Fatalf("systemd calls = %#v", calls)
	}
}

func TestEnsureAppGatewayLaunchdWritesKeepAliveAgent(t *testing.T) {
	stateDir := t.TempDir()
	home := t.TempDir()
	oldHome := appGatewayServiceUserHome
	oldRun := appGatewayServiceRunCommand
	t.Cleanup(func() {
		appGatewayServiceUserHome = oldHome
		appGatewayServiceRunCommand = oldRun
	})
	appGatewayServiceUserHome = func() (string, error) { return home, nil }
	var command string
	appGatewayServiceRunCommand = func(_ context.Context, name string, args ...string) ([]byte, error) {
		command = name + " " + strings.Join(args, " ")
		return nil, nil
	}
	spec := serviceTestSpec(t, appgateway.ServiceLaunchd, stateDir)
	installed, err := ensureAppGatewayLaunchAgent(context.Background(), spec)
	if err != nil || !installed {
		t.Fatalf("ensure launchd = %v/%v", installed, err)
	}
	plistPath := filepath.Join(home, "Library", "LaunchAgents", spec.UnitName()+".plist")
	b, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "<key>KeepAlive</key><true/>") || !strings.Contains(command, "launchctl load") {
		t.Fatalf("launch agent = %s; command = %s", b, command)
	}
}

func TestEnsureAppGatewayWindowsTaskCreatesLeastPrivilegeTask(t *testing.T) {
	stateDir := t.TempDir()
	oldRun := appGatewayServiceRunCommand
	oldConfigDir := appGatewayServiceUserConfigDir
	t.Cleanup(func() {
		appGatewayServiceRunCommand = oldRun
		appGatewayServiceUserConfigDir = oldConfigDir
	})
	configDir := t.TempDir()
	appGatewayServiceUserConfigDir = func() (string, error) { return configDir, nil }
	var calls [][]string
	appGatewayServiceRunCommand = func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		calls = append(calls, append([]string{name}, arguments...))
		return nil, nil
	}
	spec := serviceTestSpec(t, appgateway.ServiceWindows, stateDir)
	installed, err := ensureAppGatewayWindowsTask(context.Background(), spec)
	if err != nil || !installed {
		t.Fatalf("ensure Windows task = %v/%v", installed, err)
	}
	xmlPath := filepath.Join(stateDir, "tasks", spec.UnitName()+".xml")
	b, err := os.ReadFile(xmlPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "<RunLevel>LeastPrivilege</RunLevel>") || !strings.Contains(string(b), "<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>") {
		t.Fatalf("task XML = %s", b)
	}
	if len(calls) != 2 || calls[0][0] != "schtasks.exe" || calls[1][0] != "schtasks.exe" {
		t.Fatalf("Windows calls = %#v", calls)
	}
	launcherPath := filepath.Join(configDir, "Microsoft", "Windows", "Start Menu", "Programs", "ChatGPT via CXP - "+spec.Name+".cmd")
	launcher, err := os.ReadFile(launcherPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(launcher), " app --profile \"profile-a\"") || !strings.Contains(string(launcher), "--config \"") {
		t.Fatalf("launcher = %s", launcher)
	}
}

func TestEnsureAppGatewaySystemdFailureDoesNotReportInstalled(t *testing.T) {
	stateDir := t.TempDir()
	configDir := t.TempDir()
	oldConfigDir := appGatewayServiceUserConfigDir
	oldRun := appGatewayServiceRunCommand
	t.Cleanup(func() {
		appGatewayServiceUserConfigDir = oldConfigDir
		appGatewayServiceRunCommand = oldRun
	})
	appGatewayServiceUserConfigDir = func() (string, error) { return configDir, nil }
	var calls [][]string
	appGatewayServiceRunCommand = func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		calls = append(calls, append([]string{name}, arguments...))
		if len(calls) == 1 {
			return nil, os.ErrPermission
		}
		return nil, nil
	}
	installed, err := ensureAppGatewaySystemd(context.Background(), serviceTestSpec(t, appgateway.ServiceSystemd, stateDir))
	if installed || err == nil {
		t.Fatalf("systemd reload failure = installed %t/error %v", installed, err)
	}
	if len(calls) != 1 || calls[0][0] != "systemctl" {
		t.Fatalf("systemd failure calls = %#v", calls)
	}
}

func TestEnsureAppGatewayWindowsLauncherFailureReportsRunningTask(t *testing.T) {
	stateDir := t.TempDir()
	configDir := t.TempDir()
	// Make the parent of the launcher directory a file. The Task Scheduler XML
	// and /Run command still succeed, but the user-visible launcher cannot be
	// written; callers must not start a second detached process in response.
	blockedParent := filepath.Join(configDir, "Microsoft", "Windows", "Start Menu")
	if err := os.MkdirAll(filepath.Dir(blockedParent), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blockedParent, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldRun := appGatewayServiceRunCommand
	oldConfigDir := appGatewayServiceUserConfigDir
	t.Cleanup(func() {
		appGatewayServiceRunCommand = oldRun
		appGatewayServiceUserConfigDir = oldConfigDir
	})
	appGatewayServiceUserConfigDir = func() (string, error) { return configDir, nil }
	var calls [][]string
	appGatewayServiceRunCommand = func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		calls = append(calls, append([]string{name}, arguments...))
		return nil, nil
	}
	installed, err := ensureAppGatewayWindowsTask(context.Background(), serviceTestSpec(t, appgateway.ServiceWindows, stateDir))
	if !installed || err == nil {
		t.Fatalf("launcher failure = installed %t/error %v", installed, err)
	}
	if len(calls) != 2 || calls[0][0] != "schtasks.exe" || calls[1][0] != "schtasks.exe" {
		t.Fatalf("Windows task calls = %#v", calls)
	}
}
