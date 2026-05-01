package cli

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestTeamsBackgroundKeepaliveBackendSelectionCI(t *testing.T) {
	lockCLITestHooks(t)

	tests := []struct {
		name      string
		goos      string
		isWSL     bool
		wslMode   string
		wantID    string
		wantName  string
		wantError string
	}{
		{name: "linux systemd user", goos: "linux", wantID: "systemd-user", wantName: teamsServiceUnitName},
		{name: "wsl defaults to windows task", goos: "linux", isWSL: true, wantID: "wsl-windows-task-scheduler", wantName: "Codex Helper Teams Bridge (WSL Ubuntu alice"},
		{name: "wsl explicit systemd", goos: "linux", isWSL: true, wslMode: "systemd", wantID: "systemd-user", wantName: teamsServiceUnitName},
		{name: "macos launchagent", goos: "darwin", wantID: "launchagent", wantName: teamsServiceLaunchAgentLabel},
		{name: "windows task scheduler", goos: "windows", wantID: "windows-task-scheduler", wantName: teamsServiceWindowsTaskName},
		{name: "unsupported platform", goos: "freebsd", wantError: "unsupported platform"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND", tc.wslMode)
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:           tc.goos,
				exe:            filepath.Join(tmp, "codex-proxy"),
				cwd:            tmp,
				unitDir:        filepath.Join(tmp, "systemd", "user"),
				launchAgentDir: filepath.Join(tmp, "LaunchAgents"),
				windowsTaskDir: filepath.Join(tmp, "windows-task"),
				userID:         "501",
				isWSL:          tc.isWSL,
				wslDistro:      "Ubuntu",
				wslLinuxUser:   "alice",
				runner:         &recordingTeamsServiceRunner{},
			})
			backend, err := teamsServiceBackendForCurrentPlatform()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("backend error = %v, want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
			}
			if backend.ID() != tc.wantID {
				t.Fatalf("backend ID = %q, want %q", backend.ID(), tc.wantID)
			}
			if !strings.Contains(backend.Name(), tc.wantName) {
				t.Fatalf("backend name = %q, want containing %q", backend.Name(), tc.wantName)
			}
		})
	}
}

func TestTeamsBackgroundKeepaliveSupervisorConfigMatrixCI(t *testing.T) {
	spec := teamsServiceSpec{
		Executable:   "/home/alice/bin/codex-proxy",
		WorkingDir:   "/home/alice/work dir",
		RegistryPath: "/home/alice/.config/codex-helper/teams registry.json",
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
			"CODEX_HOME":                      "/home/alice/.codex",
			"NO_COLOR":                        "1",
		},
	}

	unit := buildTeamsServiceUnit(spec)
	for _, want := range []string{
		"Type=simple",
		"WorkingDirectory=" + strconv.Quote(spec.WorkingDir),
		"ExecStart=" + spec.Executable + " teams run --registry " + strconv.Quote(spec.RegistryPath),
		"Restart=on-failure",
		"RestartSec=10s",
		"WantedBy=default.target",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("systemd unit missing %q:\n%s", want, unit)
		}
	}
	for _, forbidden := range []string{"Restart=always", "User=root", "sudo", "StartLimitBurst=0"} {
		if strings.Contains(unit, forbidden) {
			t.Fatalf("systemd unit should not contain %q:\n%s", forbidden, unit)
		}
	}

	plist := buildTeamsServiceLaunchAgentPlist(spec)
	for _, want := range []string{
		"<key>RunAtLoad</key>",
		"<true/>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<false/>",
		"<key>WorkingDirectory</key>",
		"<string>" + spec.WorkingDir + "</string>",
		"<key>CODEX_HELPER_TEAMS_SERVICE_MODE</key>",
		"<string>background</string>",
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("LaunchAgent plist missing %q:\n%s", want, plist)
		}
	}

	taskXML := buildTeamsServiceWindowsTaskXML(spec)
	for _, want := range []string{
		"<LogonTrigger>",
		"<LogonType>InteractiveToken</LogonType>",
		"<RunLevel>LeastPrivilege</RunLevel>",
		"<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"<StartWhenAvailable>true</StartWhenAvailable>",
		"<RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>",
		"<Enabled>false</Enabled>",
		"<RestartOnFailure>",
		"<Interval>PT10S</Interval>",
		"<Count>999</Count>",
		"<WorkingDirectory>" + spec.WorkingDir + "</WorkingDirectory>",
	} {
		if !strings.Contains(taskXML, want) {
			t.Fatalf("Windows task XML missing %q:\n%s", want, taskXML)
		}
	}
	for _, forbidden := range []string{"HighestAvailable", "RunLevel>Highest", "S4U"} {
		if strings.Contains(taskXML, forbidden) {
			t.Fatalf("Windows task XML should not contain %q:\n%s", forbidden, taskXML)
		}
	}
}

func TestTeamsBackgroundKeepaliveWSLTaskConfigCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex home"))
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "research/profile")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu/Dev",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute WSL service install: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("WSL install calls = %#v, want one Register-ScheduledTask call", runner.calls)
	}
	command := runner.calls[0].name + " " + strings.Join(runner.calls[0].args, " ")
	for _, want := range []string{
		"New-ScheduledTaskAction",
		"wsl.exe",
		"New-ScheduledTaskTrigger -AtLogOn",
		"New-ScheduledTaskSettingsSet",
		"RestartCount 999",
		"Interactive",
		"RunLevel Limited",
		"Disable-ScheduledTask",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("WSL scheduled task command missing %q:\n%s", want, command)
		}
	}
	for _, forbidden := range []string{"Start-ScheduledTask", "Enable-ScheduledTask", "RunLevel Highest"} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("WSL install command should not contain %q:\n%s", forbidden, command)
		}
	}

	files, err := filepath.Glob(filepath.Join(tmp, "wsl-task", "codex-helper-teams-wsl-task-*.txt"))
	if err != nil {
		t.Fatalf("glob WSL task config: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("WSL task config files = %#v, want one", files)
	}
	configData, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("read WSL task config: %v", err)
	}
	config := string(configData)
	wantCWD := teamsServiceTestAbsPath(t, "/home/alice/work dir")
	wantExe := teamsServiceTestAbsPath(t, "/home/alice/bin/codex-proxy")
	wantRegistry := teamsServiceTestRegistryPath(wantCWD, "/home/alice/teams registry.json")
	for _, want := range []string{
		"Command=wsl.exe",
		"-d Ubuntu/Dev",
		"-u alice",
		"--cd ",
		wantCWD,
		"CODEX_HOME=" + filepath.Join(tmp, "codex home"),
		wantExe + " teams run --registry",
		wantRegistry,
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("WSL config missing %q:\n%s", want, config)
		}
	}
}

func TestTeamsBackgroundKeepaliveDelayedRestartCommandsCI(t *testing.T) {
	lockCLITestHooks(t)

	tests := []struct {
		name        string
		goos        string
		isWSL       bool
		powerShell  string
		wantName    string
		wantSnippet string
	}{
		{name: "linux systemd", goos: "linux", wantName: "sh", wantSnippet: "systemctl --user start '" + teamsServiceUnitName + "'"},
		{name: "macos launchagent", goos: "darwin", wantName: "sh", wantSnippet: "launchctl kickstart -k 'gui/501/" + teamsServiceLaunchAgentLabel + "'"},
		{name: "windows task", goos: "windows", wantName: "powershell.exe", wantSnippet: "Start-ScheduledTask -TaskName '" + teamsServiceWindowsTaskName + "'"},
		{name: "wsl windows task", goos: "linux", isWSL: true, powerShell: "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe", wantName: "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe", wantSnippet: "Start-ScheduledTask -TaskName 'Codex Helper Teams Bridge (WSL Ubuntu alice default "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:                 tc.goos,
				exe:                  filepath.Join(tmp, "codex-proxy"),
				cwd:                  tmp,
				unitDir:              filepath.Join(tmp, "systemd", "user"),
				launchAgentDir:       filepath.Join(tmp, "LaunchAgents"),
				windowsTaskDir:       filepath.Join(tmp, "windows-task"),
				userID:               "501",
				isWSL:                tc.isWSL,
				wslDistro:            "Ubuntu",
				wslLinuxUser:         "alice",
				powerShellExecutable: tc.powerShell,
				runner:               &recordingTeamsServiceRunner{},
			})
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				t.Fatalf("backend error: %v", err)
			}
			name, args, err := delayedTeamsServiceStartCommand(backend)
			if err != nil {
				t.Fatalf("delayedTeamsServiceStartCommand error: %v", err)
			}
			if name != tc.wantName {
				t.Fatalf("restart command name = %q, want %q", name, tc.wantName)
			}
			if joined := strings.Join(args, " "); !strings.Contains(joined, tc.wantSnippet) {
				t.Fatalf("restart command args missing %q:\n%#v", tc.wantSnippet, args)
			}
		})
	}
}

func TestTeamsBackgroundKeepaliveServiceActionsCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	for _, action := range []string{"enable", "status", "stop", "restart", "disable"} {
		runner.calls = nil
		cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
		cmd.SetArgs([]string{action})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("service %s error: %v", action, err)
		}
		if len(runner.calls) != 1 || runner.calls[0].name != "systemctl" || runner.calls[0].args[0] != "--user" {
			t.Fatalf("service %s calls = %#v, want one systemctl --user call", action, runner.calls)
		}
	}

	runner.calls = nil
	teamsServiceAuthPreflight = func() error { return errTeamsKeepaliveAuthMissingForTest{} }
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"start"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "keepalive auth missing") {
		t.Fatalf("service start error = %v, want auth preflight error", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("service start should not reach supervisor when auth preflight fails: %#v", runner.calls)
	}
}

type errTeamsKeepaliveAuthMissingForTest struct{}

func (errTeamsKeepaliveAuthMissingForTest) Error() string { return "keepalive auth missing" }
