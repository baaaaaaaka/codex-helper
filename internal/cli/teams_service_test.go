package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func isolateTeamsUserDirsForTest(t *testing.T, tmp string) (string, string) {
	t.Helper()
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)
	t.Setenv("APPDATA", filepath.Join(tmp, "AppData", "Roaming"))
	t.Setenv("LOCALAPPDATA", filepath.Join(tmp, "AppData", "Local"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	configBase, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("os.UserConfigDir: %v", err)
	}
	cacheBase, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("os.UserCacheDir: %v", err)
	}
	return configBase, cacheBase
}

func teamsServiceTestAbsPath(t *testing.T, path string) string {
	t.Helper()
	out, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("filepath.Abs(%q): %v", path, err)
	}
	return out
}

func teamsServiceTestRegistryPath(cwd string, registryPath string) string {
	registryPath = strings.TrimSpace(registryPath)
	if filepath.IsAbs(registryPath) {
		return registryPath
	}
	return filepath.Join(cwd, registryPath)
}

func TestTeamsServiceInstallWritesSystemdUserUnitWithoutEnabling(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd user")
	exePath := filepath.Join(tmp, "bin", "codex proxy")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     cwd,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	unitPath := filepath.Join(unitDir, teamsServiceUnitName)
	data, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"[Unit]",
		"Description=Codex Helper Teams bridge",
		"WorkingDirectory=" + strconv.Quote(cwd),
		"ExecStart=" + strconv.Quote(exePath) + " teams run --registry " + strconv.Quote(registryPath),
		"Restart=on-failure",
		"RestartSec=10s",
		"Environment=NO_COLOR=1",
		"Environment=CODEX_HELPER_TEAMS_SERVICE=1",
		"[Install]",
		"WantedBy=default.target",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing %q:\n%s", want, unit)
		}
	}

	wantCalls := []teamsServiceCommandCall{{
		name: "systemctl",
		args: []string{"--user", "daemon-reload"},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("systemctl calls = %#v, want %#v", runner.calls, wantCalls)
	}
	for _, call := range runner.calls {
		joined := strings.Join(call.args, " ")
		if strings.Contains(joined, "enable") || strings.Contains(joined, "start") {
			t.Fatalf("install should not enable or start service, calls=%#v", runner.calls)
		}
	}
	if !strings.Contains(out.String(), "not enabled or started automatically") {
		t.Fatalf("install output should state no auto enable/start:\n%s", out.String())
	}
}

func TestTeamsServiceInstallWithoutRegistryLetsBridgeUseScopedDefaults(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	cwd := filepath.Join(tmp, "work")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     cwd,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	if strings.Contains(unit, "--registry") {
		t.Fatalf("default service unit should not force a shared legacy registry:\n%s", unit)
	}
	if !strings.Contains(unit, "ExecStart="+exePath+" teams run") {
		t.Fatalf("unit missing teams run ExecStart:\n%s", unit)
	}
}

func TestTeamsServiceInstallRejectsGoRunTemporaryExecutable(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "go-build123", "b001", "exe", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "temporary go run binary") {
		t.Fatalf("expected go run executable rejection, got %v", err)
	}
}

func TestTeamsServiceAuthPreflightRequiresForegroundLogin(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	err := defaultTeamsServiceAuthPreflight()
	if err == nil || !strings.Contains(err.Error(), "teams auth") {
		t.Fatalf("expected missing auth preflight error, got %v", err)
	}
}

func TestTeamsServiceStartRequiresAuthPreflight(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("started\n")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	teamsServiceAuthPreflight = func() error { return errors.New("auth missing") }

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"start"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "auth missing") {
		t.Fatalf("expected auth preflight error, got %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("service start should not reach supervisor when auth preflight fails: %#v", runner.calls)
	}
}

func TestTeamsServiceDoctorReportsAuthAndExecutableWithoutFailingFast(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "go-build123", "b001", "exe", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	teamsServiceAuthPreflight = func() error { return errors.New("auth missing") }

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("service doctor should report auth/executable issues without failing fast: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Teams service backend: systemd-user",
		"Teams service executable: not installable",
		"temporary go run binary",
		"Teams service auth: not ready",
		"auth missing",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
}

func TestTeamsServiceInstallPreservesScopedEnvironment(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex home"))
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "work-profile")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "1")
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", filepath.Join(tmp, "read-token.json"))
	t.Setenv("HTTPS_PROXY", "http://proxy.example.test:8080")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     tmp,
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		`Environment="CODEX_HOME=` + filepath.Join(tmp, "codex home") + `"`,
		"Environment=CODEX_HELPER_TEAMS_PROFILE=work-profile",
		"Environment=CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1",
		"Environment=CODEX_HELPER_TEAMS_READ_TOKEN_CACHE=" + filepath.Join(tmp, "read-token.json"),
		"Environment=HTTPS_PROXY=http://proxy.example.test:8080",
		"Environment=CODEX_HELPER_TEAMS_SERVICE_MODE=background",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing preserved env %q:\n%s", want, unit)
		}
	}
}

func TestTeamsServiceSystemctlSubcommandsUseUserService(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("active\n")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "enable", args: []string{"--user", "enable", teamsServiceUnitName}},
		{name: "disable", args: []string{"--user", "disable", teamsServiceUnitName}},
		{name: "status", args: []string{"--user", "status", "--no-pager", teamsServiceUnitName}},
		{name: "start", args: []string{"--user", "start", teamsServiceUnitName}},
		{name: "stop", args: []string{"--user", "stop", teamsServiceUnitName}},
		{name: "restart", args: []string{"--user", "restart", teamsServiceUnitName}},
	} {
		runner.calls = nil
		cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
		cmd.SetArgs([]string{tc.name})
		var out bytes.Buffer
		cmd.SetOut(&out)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("execute service %s: %v", tc.name, err)
		}
		wantCalls := []teamsServiceCommandCall{{name: "systemctl", args: tc.args}}
		if !reflect.DeepEqual(runner.calls, wantCalls) {
			t.Fatalf("%s calls = %#v, want %#v", tc.name, runner.calls, wantCalls)
		}
		if tc.name == "status" && !strings.Contains(out.String(), "active") {
			t.Fatalf("status should print systemctl output:\n%s", out.String())
		}
	}
}

func TestTeamsServiceUninstallRemovesUnitAndReloads(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	unitPath := filepath.Join(unitDir, teamsServiceUnitName)
	if err := os.WriteFile(unitPath, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale unit: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"uninstall"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service uninstall: %v", err)
	}
	if _, err := os.Stat(unitPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected unit to be removed, stat err=%v", err)
	}
	wantCalls := []teamsServiceCommandCall{{
		name: "systemctl",
		args: []string{"--user", "daemon-reload"},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("systemctl calls = %#v, want %#v", runner.calls, wantCalls)
	}
}

func TestTeamsServiceUnsupportedPlatformInjection(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "freebsd",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"install"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected unsupported platform error")
	}
	if !strings.Contains(err.Error(), `unsupported platform "freebsd"`) ||
		!strings.Contains(err.Error(), "Linux systemd --user, macOS LaunchAgent, and Windows per-user Task Scheduler") {
		t.Fatalf("unexpected unsupported error: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("unsupported platform should not invoke systemctl, calls=%#v", runner.calls)
	}
}

func TestTeamsServiceInstallWritesMacOSLaunchAgentPlist(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	agentDir := filepath.Join(tmp, "LaunchAgents")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "darwin",
		exe:            exePath,
		cwd:            cwd,
		launchAgentDir: agentDir,
		userID:         "501",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	plistPath := filepath.Join(agentDir, teamsServiceLaunchAgentPlistName)
	data, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	plist := string(data)
	for _, want := range []string{
		"<string>" + teamsServiceLaunchAgentLabel + "</string>",
		"<key>Disabled</key>",
		"<key>ProgramArguments</key>",
		"<string>" + exePath + "</string>",
		"<string>teams</string>",
		"<string>run</string>",
		"<string>--registry</string>",
		"<string>" + registryPath + "</string>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<false/>",
		"<key>CODEX_HELPER_TEAMS_SERVICE</key>",
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("plist missing %q:\n%s", want, plist)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatalf("install should only write plist, calls=%#v", runner.calls)
	}

	runner.calls = nil
	cmd = newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"start"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service start: %v", err)
	}
	wantCalls := []teamsServiceCommandCall{{
		name: "launchctl",
		args: []string{"bootstrap", "gui/501", plistPath},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("launchctl start calls = %#v, want %#v", runner.calls, wantCalls)
	}
}

func TestTeamsServiceInstallWritesWindowsTaskXMLAndRegistersTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	xmlDir := filepath.Join(tmp, "config")
	exePath := filepath.Join(tmp, "bin", "codex-proxy.exe")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            exePath,
		cwd:            cwd,
		windowsTaskDir: xmlDir,
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	xmlPath := filepath.Join(xmlDir, teamsServiceWindowsTaskXMLName)
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		t.Fatalf("read task xml: %v", err)
	}
	taskXML := string(data)
	for _, want := range []string{
		"<LogonType>InteractiveToken</LogonType>",
		"<RunLevel>LeastPrivilege</RunLevel>",
		"<Enabled>false</Enabled>",
		"<Command>powershell.exe</Command>",
		"<WorkingDirectory>" + cwd + "</WorkingDirectory>",
		`&amp; &apos;` + exePath + `&apos; &apos;teams&apos; &apos;run&apos; &apos;--registry&apos; &apos;` + registryPath + `&apos;`,
		`$env:CODEX_HELPER_TEAMS_SERVICE = &apos;1&apos;`,
		"<RestartOnFailure>",
		"<Count>999</Count>",
	} {
		if !strings.Contains(taskXML, want) {
			t.Fatalf("task xml missing %q:\n%s", want, taskXML)
		}
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Register-ScheduledTask") {
		t.Fatalf("install should register task with powershell, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Start-ScheduledTask", "Enable-ScheduledTask")

	runner.calls = nil
	cmd = newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"enable"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service enable: %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Enable-ScheduledTask") {
		t.Fatalf("enable should call powershell scheduled task command, calls=%#v", runner.calls)
	}
}

func TestTeamsServiceDoctorReportsWSLHint(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		unitDir:        filepath.Join(tmp, "systemd", "user"),
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service doctor: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Teams service backend: wsl-windows-task-scheduler",
		"Codex Helper Teams Bridge (WSL Ubuntu alice default ",
		"WSL: detected",
		"wsl.exe",
		"WSL supervisor readiness",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Get-Command wsl.exe") || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Get-ScheduledTask") {
		t.Fatalf("doctor should run a non-destructive WSL readiness probe, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Register-ScheduledTask", "Start-ScheduledTask", "Enable-ScheduledTask", "Disable-ScheduledTask")
}

func TestTeamsServiceDoctorReportsWSLReadinessFailure(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{err: errors.New("powershell missing")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		unitDir:        filepath.Join(tmp, "systemd", "user"),
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "WSL Windows Scheduled Task readiness check failed") {
		t.Fatalf("doctor error = %v, want WSL readiness failure", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
		t.Fatalf("doctor readiness calls=%#v", runner.calls)
	}
}

func TestTeamsServiceRunPowerShellCanUseExplicitExecutablePath(t *testing.T) {
	lockCLITestHooks(t)

	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  "/home/alice/bin/codex-proxy",
		cwd:                  "/home/alice/work",
		isWSL:                true,
		runner:               runner,
		powerShellExecutable: "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
	})

	if _, err := teamsServiceRunPowerShell(context.Background(), "Get-Command wsl.exe"); err != nil {
		t.Fatalf("teamsServiceRunPowerShell error: %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe" {
		t.Fatalf("powershell executable call = %#v", runner.calls)
	}
}

func TestTeamsServiceInstallWritesWSLWindowsTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex-home"))
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu-22.04",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/registry.json"))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(tmp, "wsl-task", "codex-helper-teams-wsl-task-*.txt"))
	if err != nil {
		t.Fatalf("glob WSL task config: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("WSL task config files = %#v, want one", files)
	}
	configPath := files[0]
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read WSL task config: %v", err)
	}
	config := string(data)
	wantCWD := teamsServiceTestAbsPath(t, "/home/alice/work dir")
	wantExe := teamsServiceTestAbsPath(t, "/home/alice/bin/codex-proxy")
	wantRegistry := teamsServiceTestRegistryPath(wantCWD, "/home/alice/registry.json")
	for _, want := range []string{
		"TaskName=Codex Helper Teams Bridge (WSL Ubuntu-22.04 alice default ",
		"Command=wsl.exe",
		"-d Ubuntu-22.04",
		"-u alice",
		"--cd ",
		wantCWD,
		"CODEX_HOME=" + filepath.Join(tmp, "codex-home"),
		wantExe + " teams run --registry",
		wantRegistry,
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("WSL config missing %q:\n%s", want, config)
		}
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Register-ScheduledTask") || !strings.Contains(strings.Join(runner.calls[0].args, " "), "wsl.exe") {
		t.Fatalf("install should register WSL scheduled task, calls=%#v", runner.calls)
	}
	if !strings.Contains(strings.Join(runner.calls[0].args, " "), "RestartCount 999") {
		t.Fatalf("WSL task should use high restart count for background keepalive, calls=%#v", runner.calls)
	}
	if !strings.Contains(strings.Join(runner.calls[0].args, " "), "Disable-ScheduledTask") {
		t.Fatalf("install should leave WSL task disabled, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Start-ScheduledTask", "Enable-ScheduledTask")
}

func TestTeamsServiceWSLTaskNameSeparatesUsersAndProfiles(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "research/profile")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu/Dev",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	aliceResearch := teamsServiceWSLWindowsTaskBackend{}.Name()

	teamsServiceWSLLinuxUserName = func() string { return "bob" }
	bobResearch := teamsServiceWSLWindowsTaskBackend{}.Name()
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "default")
	bobDefault := teamsServiceWSLWindowsTaskBackend{}.Name()

	if aliceResearch == bobResearch || bobResearch == bobDefault || aliceResearch == bobDefault {
		t.Fatalf("WSL task names should be isolated by Linux user and profile: alice=%q bob=%q default=%q", aliceResearch, bobResearch, bobDefault)
	}
	for _, name := range []string{aliceResearch, bobResearch, bobDefault} {
		if strings.ContainsAny(name, `\/:*?"<>|`) {
			t.Fatalf("WSL task name contains Windows-unsafe character: %q", name)
		}
	}
}

func TestTeamsServiceActiveQueriesSupervisorWithoutGeneratedConfig(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	for _, tc := range []struct {
		name     string
		goos     string
		runner   string
		wantName string
	}{
		{name: "darwin", goos: "darwin", runner: "launchctl", wantName: "print"},
		{name: "windows", goos: "windows", runner: "powershell.exe", wantName: "Get-ScheduledTask"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &recordingTeamsServiceRunner{}
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:           tc.goos,
				exe:            filepath.Join(tmp, "codex-proxy"),
				cwd:            tmp,
				launchAgentDir: filepath.Join(tmp, "missing-launch-agents"),
				windowsTaskDir: filepath.Join(tmp, "missing-task-xml"),
				userID:         "501",
				runner:         runner,
			})
			active, err := teamsServiceActive(context.Background())
			if err != nil {
				t.Fatalf("teamsServiceActive error: %v", err)
			}
			if !active {
				t.Fatal("teamsServiceActive = false, want true from supervisor")
			}
			if len(runner.calls) != 1 || runner.calls[0].name != tc.runner || !strings.Contains(strings.Join(runner.calls[0].args, " "), tc.wantName) {
				t.Fatalf("unexpected supervisor calls: %#v", runner.calls)
			}
		})
	}
}

type teamsServiceCommandCall struct {
	name string
	args []string
}

type recordingTeamsServiceRunner struct {
	calls  []teamsServiceCommandCall
	output []byte
	err    error
}

func (r *recordingTeamsServiceRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, teamsServiceCommandCall{
		name: name,
		args: append([]string{}, args...),
	})
	return append([]byte{}, r.output...), r.err
}

func teamsServiceCallSeen(calls []teamsServiceCommandCall, action string) bool {
	for _, call := range calls {
		for _, arg := range call.args {
			if arg == action {
				return true
			}
		}
	}
	return false
}

func assertTeamsServiceCallsDoNotContain(t *testing.T, calls []teamsServiceCommandCall, forbidden ...string) {
	t.Helper()
	for _, call := range calls {
		joined := call.name + " " + strings.Join(call.args, " ")
		for _, value := range forbidden {
			if strings.Contains(joined, value) {
				t.Fatalf("service command contained forbidden %q: %#v", value, calls)
			}
		}
	}
}

type teamsServiceTestHooks struct {
	goos                 string
	exe                  string
	cwd                  string
	unitDir              string
	launchAgentDir       string
	windowsTaskDir       string
	userID               string
	isWSL                bool
	wslDistro            string
	wslLinuxUser         string
	powerShellExecutable string
	runner               teamsServiceCommandRunner
}

func withTeamsServiceTestHooks(t *testing.T, hooks teamsServiceTestHooks) {
	t.Helper()
	prevGOOS := teamsServiceGOOS
	prevExecutable := teamsServiceExecutable
	prevGetwd := teamsServiceGetwd
	prevSystemdUserDir := teamsServiceSystemdUserDir
	prevLaunchAgentDir := teamsServiceLaunchAgentDir
	prevWindowsTaskXMLDir := teamsServiceWindowsTaskXMLDir
	prevUserID := teamsServiceUserID
	prevIsWSL := teamsServiceIsWSL
	prevWSLDistroName := teamsServiceWSLDistroName
	prevWSLLinuxUserName := teamsServiceWSLLinuxUserName
	prevPowerShellExecutable := teamsServicePowerShellExecutable
	prevSystemctl := teamsServiceSystemctl
	prevAuthPreflight := teamsServiceAuthPreflight
	teamsServiceGOOS = func() string { return hooks.goos }
	teamsServiceExecutable = func() (string, error) { return hooks.exe, nil }
	teamsServiceGetwd = func() (string, error) { return hooks.cwd, nil }
	teamsServiceSystemdUserDir = func() (string, error) { return hooks.unitDir, nil }
	teamsServiceLaunchAgentDir = func() (string, error) { return hooks.launchAgentDir, nil }
	teamsServiceWindowsTaskXMLDir = func() (string, error) { return hooks.windowsTaskDir, nil }
	teamsServiceUserID = func() string { return hooks.userID }
	teamsServiceIsWSL = func() bool { return hooks.isWSL }
	teamsServiceWSLDistroName = func() string { return hooks.wslDistro }
	teamsServiceWSLLinuxUserName = func() string { return hooks.wslLinuxUser }
	teamsServicePowerShellExecutable = func() string {
		if hooks.powerShellExecutable != "" {
			return hooks.powerShellExecutable
		}
		return "powershell.exe"
	}
	teamsServiceSystemctl = hooks.runner
	teamsServiceAuthPreflight = func() error { return nil }
	t.Cleanup(func() {
		teamsServiceGOOS = prevGOOS
		teamsServiceExecutable = prevExecutable
		teamsServiceGetwd = prevGetwd
		teamsServiceSystemdUserDir = prevSystemdUserDir
		teamsServiceLaunchAgentDir = prevLaunchAgentDir
		teamsServiceWindowsTaskXMLDir = prevWindowsTaskXMLDir
		teamsServiceUserID = prevUserID
		teamsServiceIsWSL = prevIsWSL
		teamsServiceWSLDistroName = prevWSLDistroName
		teamsServiceWSLLinuxUserName = prevWSLLinuxUserName
		teamsServicePowerShellExecutable = prevPowerShellExecutable
		teamsServiceSystemctl = prevSystemctl
		teamsServiceAuthPreflight = prevAuthPreflight
	})
}

func stringPtr(s string) *string {
	return &s
}
