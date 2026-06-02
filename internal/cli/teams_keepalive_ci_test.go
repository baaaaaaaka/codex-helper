package cli

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

func requireSubstringsInOrder(t *testing.T, text string, wants ...string) {
	t.Helper()
	offset := 0
	for _, want := range wants {
		idx := strings.Index(text[offset:], want)
		if idx < 0 {
			t.Fatalf("missing %q after offset %d:\n%s", want, offset, text)
		}
		offset += idx + len(want)
	}
}

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
			"HTTP_PROXY":                      "http://127.0.0.1:38471",
			"NO_COLOR":                        "1",
		},
	}

	unit := buildTeamsServiceUnit(spec)
	for _, want := range []string{
		"Type=simple",
		"WorkingDirectory=" + strconv.Quote(spec.WorkingDir),
		"ExecStart=" + spec.Executable + " teams run --owner-stale-after 1m30s --auto-service=false --registry " + strconv.Quote(spec.RegistryPath),
		"Environment=CODEX_HELPER_TEAMS_SERVICE=1",
		"Environment=CODEX_HELPER_TEAMS_SERVICE_MODE=background",
		"Environment=HTTP_PROXY=http://127.0.0.1:38471",
		"Restart=on-failure",
		"RestartSec=10s",
		"WantedBy=default.target",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("systemd unit missing %q:\n%s", want, unit)
		}
	}
	for _, forbidden := range []string{
		"Restart=always",
		"User=root",
		"sudo",
		"StartLimitBurst=0",
		"StandardInput=tty",
		"TTYPath=",
		"PAMName=",
		"BindsTo=",
		"PartOf=",
		"Requires=graphical-session.target",
		"After=graphical-session.target",
	} {
		if strings.Contains(unit, forbidden) {
			t.Fatalf("systemd unit should not contain %q:\n%s", forbidden, unit)
		}
	}
	watchdogUnit := buildTeamsServiceWatchdogUnit(spec)
	for _, want := range []string{
		"Type=simple",
		"WorkingDirectory=" + strconv.Quote(spec.WorkingDir),
		"ExecStart=" + spec.Executable + " teams service watchdog --loop --interval 10s --quiet",
		"Restart=on-failure",
		"RestartSec=10s",
		"Environment=CODEX_HELPER_TEAMS_SERVICE=1",
		"WantedBy=default.target",
	} {
		if !strings.Contains(watchdogUnit, want) {
			t.Fatalf("systemd watchdog unit missing %q:\n%s", want, watchdogUnit)
		}
	}
	watchdogTimer := buildTeamsServiceWatchdogTimer()
	for _, want := range []string{
		"OnBootSec=30s",
		"OnUnitActiveSec=1min",
		"AccuracySec=30s",
		"Unit=" + teamsServiceWatchdogUnitName,
		"WantedBy=timers.target",
	} {
		if !strings.Contains(watchdogTimer, want) {
			t.Fatalf("systemd watchdog timer missing %q:\n%s", want, watchdogTimer)
		}
	}

	plist := buildTeamsServiceLaunchAgentPlist(spec)
	for _, want := range []string{
		"<key>Disabled</key>",
		"<key>RunAtLoad</key>",
		"<true/>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<false/>",
		"<string>" + spec.Executable + "</string>",
		"<string>teams</string>",
		"<string>run</string>",
		"<string>--owner-stale-after</string>",
		"<string>1m30s</string>",
		"<string>--auto-service=false</string>",
		"<string>--registry</string>",
		"<string>" + spec.RegistryPath + "</string>",
		"<key>WorkingDirectory</key>",
		"<string>" + spec.WorkingDir + "</string>",
		"<key>CODEX_HELPER_TEAMS_SERVICE</key>",
		"<string>1</string>",
		"<key>CODEX_HELPER_TEAMS_SERVICE_MODE</key>",
		"<string>background</string>",
		"<key>HTTP_PROXY</key>",
		"<string>http://127.0.0.1:38471</string>",
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("LaunchAgent plist missing %q:\n%s", want, plist)
		}
	}
	if !strings.Contains(plist, "<key>Disabled</key>\n\t<true/>") {
		t.Fatalf("LaunchAgent should install disabled until explicitly started:\n%s", plist)
	}
	requireSubstringsInOrder(t, plist,
		"<key>RunAtLoad</key>",
		"<true/>",
		"<key>KeepAlive</key>",
		"<dict>",
		"<key>SuccessfulExit</key>",
		"<false/>",
		"</dict>",
	)
	for _, forbidden := range []string{"LimitLoadToSessionType", "Aqua", "StandardInPath"} {
		if strings.Contains(plist, forbidden) {
			t.Fatalf("LaunchAgent plist should not contain %q:\n%s", forbidden, plist)
		}
	}
	watchdogPlist := buildTeamsServiceWatchdogLaunchAgentPlist(spec)
	for _, want := range []string{
		"<string>" + teamsServiceLaunchAgentWatchdogLabel + "</string>",
		"<string>" + spec.Executable + "</string>",
		"<string>teams</string>",
		"<string>service</string>",
		"<string>watchdog</string>",
		"<string>--loop</string>",
		"<string>--interval</string>",
		"<string>10s</string>",
		"<string>--quiet</string>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<false/>",
		"<key>CODEX_HELPER_TEAMS_SERVICE</key>",
	} {
		if !strings.Contains(watchdogPlist, want) {
			t.Fatalf("LaunchAgent watchdog plist missing %q:\n%s", want, watchdogPlist)
		}
	}

	taskXML := buildTeamsServiceWindowsTaskXML(spec)
	if strings.Contains(taskXML, "<?xml") || strings.Contains(strings.ToLower(taskXML), "encoding=") {
		t.Fatalf("Windows Task Scheduler XML is passed through Register-ScheduledTask -Xml and must not declare a conflicting encoding:\n%s", taskXML)
	}
	for _, want := range []string{
		"<LogonTrigger>",
		"<LogonType>InteractiveToken</LogonType>",
		"<RunLevel>LeastPrivilege</RunLevel>",
		"<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
		"<StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
		"<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"<StartWhenAvailable>true</StartWhenAvailable>",
		"<RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>",
		"<Hidden>true</Hidden>",
		"<Enabled>false</Enabled>",
		"<RestartOnFailure>",
		"<Interval>PT1M</Interval>",
		"<Count>999</Count>",
		"CODEX_HELPER_TEAMS_SERVICE",
		"CODEX_HELPER_TEAMS_SERVICE_MODE",
		"HTTP_PROXY",
		"http://127.0.0.1:38471",
		spec.Executable,
		"teams",
		"run",
		"--owner-stale-after",
		"1m30s",
		"--auto-service=false",
		"--registry",
		"<WorkingDirectory>" + spec.WorkingDir + "</WorkingDirectory>",
	} {
		if !strings.Contains(taskXML, want) {
			t.Fatalf("Windows task XML missing %q:\n%s", want, taskXML)
		}
	}
	if strings.Contains(taskXML, "PT10S") {
		t.Fatalf("Windows task XML must not use Task Scheduler sub-minute restart intervals:\n%s", taskXML)
	}
	for _, forbidden := range []string{"HighestAvailable", "RunLevel>Highest", "S4U", "RunOnlyIfIdle", "IdleSettings"} {
		if strings.Contains(taskXML, forbidden) {
			t.Fatalf("Windows task XML should not contain %q:\n%s", forbidden, taskXML)
		}
	}
	requireSubstringsInOrder(t, taskXML,
		"<LogonTrigger>",
		"<Enabled>true</Enabled>",
		"</LogonTrigger>",
		"<Principals>",
		"<LogonType>InteractiveToken</LogonType>",
		"<RunLevel>LeastPrivilege</RunLevel>",
		"</Principals>",
		"<Settings>",
		"<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"<Enabled>false</Enabled>",
		"<RestartOnFailure>",
	)
	watchdogTaskXML := buildTeamsServiceWindowsWatchdogTaskXML(spec)
	for _, want := range []string{
		"<Description>Codex Helper Teams service watchdog</Description>",
		"<CalendarTrigger>",
		"<Interval>PT1M</Interval>",
		"<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"<RestartOnFailure>",
		"<Interval>PT1M</Interval>",
		"<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"<Hidden>true</Hidden>",
		spec.Executable,
		"teams",
		"service",
		"watchdog",
		"--loop",
		"--interval",
		"10s",
		"--quiet",
	} {
		if !strings.Contains(watchdogTaskXML, want) {
			t.Fatalf("Windows watchdog task XML missing %q:\n%s", want, watchdogTaskXML)
		}
	}
	if strings.Contains(watchdogTaskXML, "PT10S") {
		t.Fatalf("Windows watchdog task XML must not use Task Scheduler sub-minute restart intervals:\n%s", watchdogTaskXML)
	}
	requireSubstringsInOrder(t, watchdogTaskXML,
		"<CalendarTrigger>",
		"<Repetition>",
		"<Interval>PT1M</Interval>",
		"</Repetition>",
		"<StartBoundary>",
		"<Enabled>true</Enabled>",
		"<ScheduleByDay>",
		"</CalendarTrigger>",
	)
}

func TestTeamsServiceSystemdUnitVerifiesWithSystemdAnalyzeCI(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("systemd-analyze verification is Linux-only")
	}
	if os.Getenv("CODEX_HELPER_SYSTEMD_ANALYZE_TEST") != "1" {
		t.Skip("set CODEX_HELPER_SYSTEMD_ANALYZE_TEST=1 to run native systemd unit verification")
	}
	if _, err := exec.LookPath("systemd-analyze"); err != nil {
		t.Skipf("systemd-analyze not available: %v", err)
	}
	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}
	spec := teamsServiceSpec{
		Executable:   exe,
		WorkingDir:   tmp,
		RegistryPath: filepath.Join(tmp, "teams registry.json"),
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
		},
	}
	unitPath := filepath.Join(tmp, "codex-helper-teams.service")
	if err := os.WriteFile(unitPath, []byte(buildTeamsServiceUnit(spec)), 0o600); err != nil {
		t.Fatalf("write systemd unit: %v", err)
	}
	watchdogUnitPath := filepath.Join(tmp, "codex-helper-teams-watchdog.service")
	if err := os.WriteFile(watchdogUnitPath, []byte(buildTeamsServiceWatchdogUnit(spec)), 0o600); err != nil {
		t.Fatalf("write systemd watchdog unit: %v", err)
	}
	watchdogTimerPath := filepath.Join(tmp, "codex-helper-teams-watchdog.timer")
	if err := os.WriteFile(watchdogTimerPath, []byte(buildTeamsServiceWatchdogTimer()), 0o600); err != nil {
		t.Fatalf("write systemd watchdog timer: %v", err)
	}
	cmd := exec.Command("systemd-analyze", "verify", unitPath, watchdogUnitPath, watchdogTimerPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("systemd-analyze verify failed: %v\n%s", err, out)
	}
}

func TestTeamsServiceLaunchAgentPlistLintsWithPlutilCI(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("plutil LaunchAgent verification is macOS-only")
	}
	if os.Getenv("CODEX_HELPER_PLUTIL_TEST") != "1" {
		t.Skip("set CODEX_HELPER_PLUTIL_TEST=1 to run native LaunchAgent plist verification")
	}
	if _, err := exec.LookPath("plutil"); err != nil {
		t.Skipf("plutil not available: %v", err)
	}
	tmp := t.TempDir()
	spec := teamsServiceSpec{
		Executable:   filepath.Join(tmp, "codex-proxy"),
		WorkingDir:   tmp,
		RegistryPath: filepath.Join(tmp, "teams registry.json"),
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
		},
	}
	plistPath := filepath.Join(tmp, "com.codex-helper.teams.plist")
	if err := os.WriteFile(plistPath, []byte(buildTeamsServiceLaunchAgentPlist(spec)), 0o600); err != nil {
		t.Fatalf("write LaunchAgent plist: %v", err)
	}
	watchdogPlistPath := filepath.Join(tmp, "com.codex-helper.teams.watchdog.plist")
	if err := os.WriteFile(watchdogPlistPath, []byte(buildTeamsServiceWatchdogLaunchAgentPlist(spec)), 0o600); err != nil {
		t.Fatalf("write LaunchAgent watchdog plist: %v", err)
	}
	cmd := exec.Command("plutil", "-lint", plistPath, watchdogPlistPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("plutil lint failed: %v\n%s", err, out)
	}
}

func TestTeamsServiceWindowsTaskXMLRegistersWithTaskSchedulerCI(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Task Scheduler registration is Windows-only")
	}
	if os.Getenv("CODEX_HELPER_WINDOWS_TASK_REGISTER_TEST") != "1" {
		t.Skip("set CODEX_HELPER_WINDOWS_TASK_REGISTER_TEST=1 to run native Task Scheduler registration")
	}
	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	testExe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	testExeData, err := os.ReadFile(testExe)
	if err != nil {
		t.Fatalf("read test executable: %v", err)
	}
	if err := os.WriteFile(exe, testExeData, 0o755); err != nil {
		t.Fatalf("write fake codex-proxy executable: %v", err)
	}
	spec := teamsServiceSpec{
		Executable:   exe,
		WorkingDir:   tmp,
		RegistryPath: filepath.Join(tmp, "teams registry.json"),
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
		},
	}
	xmlPath := filepath.Join(tmp, "codex-helper-teams-task.xml")
	watchdogXMLPath := filepath.Join(tmp, "codex-helper-teams-watchdog-task.xml")
	spec = teamsServiceSpecWithWindowsTaskLaunchers(spec, xmlPath, watchdogXMLPath)
	if err := writeTeamsServiceWindowsTaskLauncherFiles(spec.WindowsTaskLauncherPath, spec, buildTeamsServiceRunArgs(spec)); err != nil {
		t.Fatalf("write Windows task launcher files: %v", err)
	}
	if err := writeTeamsServiceWindowsTaskLauncherFiles(spec.WindowsWatchdogLauncherPath, spec, buildTeamsServiceWatchdogArgs()); err != nil {
		t.Fatalf("write Windows watchdog launcher files: %v", err)
	}
	xml := buildTeamsServiceWindowsTaskXML(spec)
	if strings.Contains(xml, "<?xml") || strings.Contains(strings.ToLower(xml), "encoding=") {
		t.Fatalf("Windows Task Scheduler XML is passed as a PowerShell string and must not declare an encoding:\n%s", xml)
	}
	if !strings.Contains(xml, "<Hidden>true</Hidden>") || !strings.Contains(xml, "<Command>wscript.exe</Command>") || !strings.Contains(xml, "//B //Nologo") {
		t.Fatalf("Windows Task Scheduler XML should use hidden wscript launcher:\n%s", xml)
	}
	if strings.Contains(xml, "<Command>powershell.exe</Command>") || strings.Contains(xml, "-WindowStyle Hidden -Command") {
		t.Fatalf("Windows Task Scheduler XML should not run PowerShell directly:\n%s", xml)
	}
	watchdogXML := buildTeamsServiceWindowsWatchdogTaskXML(spec)
	if strings.Contains(watchdogXML, "<?xml") || strings.Contains(strings.ToLower(watchdogXML), "encoding=") {
		t.Fatalf("Windows watchdog Task Scheduler XML is passed as a PowerShell string and must not declare an encoding:\n%s", watchdogXML)
	}
	if !strings.Contains(watchdogXML, "<Hidden>true</Hidden>") || !strings.Contains(watchdogXML, "<Command>wscript.exe</Command>") || !strings.Contains(watchdogXML, "//B //Nologo") {
		t.Fatalf("Windows watchdog Task Scheduler XML should use hidden wscript launcher:\n%s", watchdogXML)
	}
	if strings.Contains(watchdogXML, "<Command>powershell.exe</Command>") || strings.Contains(watchdogXML, "-WindowStyle Hidden -Command") {
		t.Fatalf("Windows watchdog Task Scheduler XML should not run PowerShell directly:\n%s", watchdogXML)
	}
	if err := os.WriteFile(xmlPath, []byte(xml), 0o600); err != nil {
		t.Fatalf("write Windows task XML: %v", err)
	}
	if err := os.WriteFile(watchdogXMLPath, []byte(watchdogXML), 0o600); err != nil {
		t.Fatalf("write Windows watchdog task XML: %v", err)
	}
	taskName := "Codex Helper Teams Bridge CI " + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	watchdogTaskName := "Codex Helper Teams Watchdog CI " + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	helperArgs := []string{
		"-test.run=TestTeamsServiceWindowsTaskCleanupHelperProcess",
		"--",
		"teams",
		"run",
		"--owner-stale-after",
		"1m30s",
		"--auto-service=false",
		"--registry",
		spec.RegistryPath,
	}
	cleanupBridgeChildren := teamsServiceWindowsStopBridgeChildrenPowerShell("$task")
	script := "$ErrorActionPreference = 'Stop'; " +
		"$task = " + powershellSingleQuote(taskName) + "; " +
		"$watchdogTask = " + powershellSingleQuote(watchdogTaskName) + "; " +
		"$xmlPath = " + powershellSingleQuote(xmlPath) + "; " +
		"$watchdogXmlPath = " + powershellSingleQuote(watchdogXMLPath) + "; " +
		"$helperExe = " + powershellSingleQuote(exe) + "; " +
		"$helperArgLine = " + powershellSingleQuote(windowsCommandLine(helperArgs)) + "; " +
		"try { " +
		"$xml = Get-Content -LiteralPath $xmlPath -Raw; " +
		"Register-ScheduledTask -TaskName $task -Xml $xml -Force -ErrorAction Stop | Out-Null; " +
		"$watchdogXml = Get-Content -LiteralPath $watchdogXmlPath -Raw; " +
		"Register-ScheduledTask -TaskName $watchdogTask -Xml $watchdogXml -Force -ErrorAction Stop | Out-Null; " +
		"$registered = Get-ScheduledTask -TaskName $task -ErrorAction Stop; " +
		"$registeredWatchdog = Get-ScheduledTask -TaskName $watchdogTask -ErrorAction Stop; " +
		"if ($registered.TaskName -ne $task) { throw 'registered task name mismatch' }; " +
		"if ($registeredWatchdog.TaskName -ne $watchdogTask) { throw 'registered watchdog task name mismatch' }; " +
		"if ($registered.Settings.Hidden -ne $true) { throw 'registered bridge task is not hidden' }; " +
		"if ($registeredWatchdog.Settings.Hidden -ne $true) { throw 'registered watchdog task is not hidden' }; " +
		"$action = @($registered.Actions)[0]; " +
		"$watchdogAction = @($registeredWatchdog.Actions)[0]; " +
		"if ($action.Execute -ne 'wscript.exe') { throw ('bridge action execute mismatch: ' + $action.Execute) }; " +
		"if ($watchdogAction.Execute -ne 'wscript.exe') { throw ('watchdog action execute mismatch: ' + $watchdogAction.Execute) }; " +
		"if ($action.Arguments -notlike '*//B*//Nologo*codex-helper-teams-task.vbs*') { throw ('bridge action arguments mismatch: ' + $action.Arguments) }; " +
		"if ($watchdogAction.Arguments -notlike '*//B*//Nologo*codex-helper-teams-watchdog-task.vbs*') { throw ('watchdog action arguments mismatch: ' + $watchdogAction.Arguments) }; " +
		"$env:CODEX_HELPER_WINDOWS_CLEANUP_HELPER_PROCESS = '1'; " +
		"$child = Start-Process -FilePath $helperExe -ArgumentList $helperArgLine -PassThru -WindowStyle Hidden; " +
		"try { " +
		"Start-Sleep -Seconds 1; " +
		cleanupBridgeChildren +
		"$deadline = (Get-Date).AddSeconds(10); " +
		"while ((Get-Date) -lt $deadline -and $null -ne (Get-Process -Id $child.Id -ErrorAction SilentlyContinue)) { Start-Sleep -Milliseconds 200 }; " +
		"if ($null -ne (Get-Process -Id $child.Id -ErrorAction SilentlyContinue)) { throw 'native bridge cleanup did not stop matching task child process' } " +
		"} finally { " +
		"if ($null -ne $child -and $null -ne (Get-Process -Id $child.Id -ErrorAction SilentlyContinue)) { Stop-Process -Id $child.Id -Force -ErrorAction SilentlyContinue }; " +
		"Remove-Item Env:CODEX_HELPER_WINDOWS_CLEANUP_HELPER_PROCESS -ErrorAction SilentlyContinue " +
		"} " +
		"} finally { " +
		"Unregister-ScheduledTask -TaskName $watchdogTask -Confirm:$false -ErrorAction SilentlyContinue | Out-Null; " +
		"Unregister-ScheduledTask -TaskName $task -Confirm:$false -ErrorAction SilentlyContinue | Out-Null " +
		"}"
	cmd := exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Register-ScheduledTask smoke failed: %v\n%s", err, out)
	}
}

func TestTeamsServiceWindowsTaskStartIfStoppedUsesTaskSchedulerCI(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Task Scheduler start is Windows-only")
	}
	if os.Getenv("CODEX_HELPER_WINDOWS_TASK_REGISTER_TEST") != "1" {
		t.Skip("set CODEX_HELPER_WINDOWS_TASK_REGISTER_TEST=1 to run native Task Scheduler start verification")
	}
	tmp := t.TempDir()
	markerPath := filepath.Join(tmp, "task-starts.txt")
	taskScriptPath := filepath.Join(tmp, "task-action.ps1")
	taskScript := "$ErrorActionPreference = 'Stop'\r\n" +
		"Add-Content -LiteralPath " + powershellSingleQuote(markerPath) + " -Value ([System.Diagnostics.Process]::GetCurrentProcess().Id.ToString())\r\n" +
		"Start-Sleep -Seconds 8\r\n"
	if err := os.WriteFile(taskScriptPath, []byte(taskScript), 0o600); err != nil {
		t.Fatalf("write task script: %v", err)
	}
	taskName := "Codex Helper Teams Start If Stopped CI " + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	taskArgs := windowsCommandLine([]string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", taskScriptPath})
	script := "$ErrorActionPreference = 'Stop'; " +
		"$taskName = " + powershellSingleQuote(taskName) + "; " +
		"$marker = " + powershellSingleQuote(markerPath) + "; " +
		"Remove-Item -LiteralPath $marker -Force -ErrorAction SilentlyContinue; " +
		"$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument " + powershellSingleQuote(taskArgs) + "; " +
		"$trigger = New-ScheduledTaskTrigger -Once -At ((Get-Date).AddYears(1)); " +
		"$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew; " +
		teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() +
		"try { " +
		"Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Force -ErrorAction Stop | Out-Null; " +
		"Start-CodexHelperScheduledTaskIfStopped $taskName; " +
		"Start-CodexHelperScheduledTaskIfStopped $taskName; " +
		"$deadline = (Get-Date).AddSeconds(10); " +
		"while ((Get-Date) -lt $deadline -and -not (Test-Path -LiteralPath $marker)) { Start-Sleep -Milliseconds 200 }; " +
		"if (-not (Test-Path -LiteralPath $marker)) { throw 'scheduled task action did not run' }; " +
		"Start-CodexHelperScheduledTaskIfStopped $taskName; " +
		"Start-Sleep -Milliseconds 500; " +
		"$lines = @(Get-Content -LiteralPath $marker -ErrorAction Stop); " +
		"if ($lines.Count -ne 1) { throw ('expected one scheduled task action run, got ' + $lines.Count) } " +
		"} finally { " +
		"Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue | Out-Null; " +
		"Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null " +
		"}"
	cmd := exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Start-CodexHelperScheduledTaskIfStopped Task Scheduler smoke failed: %v\n%s", err, out)
	}
}

func TestTeamsServiceWindowsTaskXMLWithPrincipalRegistersWithTaskSchedulerCI(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Task Scheduler registration is Windows-only")
	}
	if os.Getenv("CODEX_HELPER_WINDOWS_TASK_REGISTER_TEST") != "1" {
		t.Skip("set CODEX_HELPER_WINDOWS_TASK_REGISTER_TEST=1 to run native Task Scheduler registration")
	}
	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	testExe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	testExeData, err := os.ReadFile(testExe)
	if err != nil {
		t.Fatalf("read test executable: %v", err)
	}
	if err := os.WriteFile(exe, testExeData, 0o755); err != nil {
		t.Fatalf("write fake codex-proxy executable: %v", err)
	}
	spec := teamsServiceSpec{
		Executable:   exe,
		WorkingDir:   tmp,
		RegistryPath: filepath.Join(tmp, "teams registry.json"),
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
		},
	}
	xmlPath := filepath.Join(tmp, "codex-helper-teams-task-principal.xml")
	watchdogXMLPath := filepath.Join(tmp, "codex-helper-teams-watchdog-task-principal.xml")
	spec = teamsServiceSpecWithWindowsTaskLaunchers(spec, xmlPath, watchdogXMLPath)
	if err := writeTeamsServiceWindowsTaskLauncherFiles(spec.WindowsTaskLauncherPath, spec, buildTeamsServiceRunArgs(spec)); err != nil {
		t.Fatalf("write Windows task launcher files: %v", err)
	}
	if err := writeTeamsServiceWindowsTaskLauncherFiles(spec.WindowsWatchdogLauncherPath, spec, buildTeamsServiceWatchdogArgs()); err != nil {
		t.Fatalf("write Windows watchdog task launcher files: %v", err)
	}
	principalUser, err := teamsServiceCurrentWindowsUser(context.Background())
	if err != nil {
		t.Fatalf("resolve current Windows user: %v", err)
	}
	xml := buildTeamsServiceWindowsTaskXMLWithPrincipalUser(spec, principalUser)
	watchdogXML := buildTeamsServiceWindowsWatchdogTaskXMLWithPrincipalUser(spec, principalUser)
	for label, data := range map[string]string{"bridge": xml, "watchdog": watchdogXML} {
		for _, want := range []string{
			"<UserId>" + xmlEscape(principalUser) + "</UserId>",
			"<LogonType>InteractiveToken</LogonType>",
			"<RunLevel>LeastPrivilege</RunLevel>",
			"<Command>wscript.exe</Command>",
			"//B //Nologo",
		} {
			if !strings.Contains(data, want) {
				t.Fatalf("%s task XML missing %q:\n%s", label, want, data)
			}
		}
		for _, forbidden := range []string{"HighestAvailable", "RunLevel>Highest", "LogonType>Password", "NT AUTHORITY\\SYSTEM"} {
			if strings.Contains(data, forbidden) {
				t.Fatalf("%s task XML must not contain %q:\n%s", label, forbidden, data)
			}
		}
	}
	if err := os.WriteFile(xmlPath, []byte(xml), 0o600); err != nil {
		t.Fatalf("write Windows task XML: %v", err)
	}
	if err := os.WriteFile(watchdogXMLPath, []byte(watchdogXML), 0o600); err != nil {
		t.Fatalf("write Windows watchdog task XML: %v", err)
	}
	taskName := "Codex Helper Teams Bridge Principal CI " + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	watchdogTaskName := "Codex Helper Teams Watchdog Principal CI " + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	script := "$ErrorActionPreference = 'Stop'; " +
		"$task = " + powershellSingleQuote(taskName) + "; " +
		"$watchdogTask = " + powershellSingleQuote(watchdogTaskName) + "; " +
		"$xmlPath = " + powershellSingleQuote(xmlPath) + "; " +
		"$watchdogXmlPath = " + powershellSingleQuote(watchdogXMLPath) + "; " +
		"$expectedUser = " + powershellSingleQuote(principalUser) + "; " +
		"$expectedShortUser = if ($expectedUser -like '*\\*') { ($expectedUser -split '\\\\')[-1] } else { $expectedUser }; " +
		"try { " +
		"$xml = Get-Content -LiteralPath $xmlPath -Raw; " +
		"Register-ScheduledTask -TaskName $task -Xml $xml -Force -ErrorAction Stop | Out-Null; " +
		"$watchdogXml = Get-Content -LiteralPath $watchdogXmlPath -Raw; " +
		"Register-ScheduledTask -TaskName $watchdogTask -Xml $watchdogXml -Force -ErrorAction Stop | Out-Null; " +
		"$registered = Get-ScheduledTask -TaskName $task -ErrorAction Stop; " +
		"$registeredWatchdog = Get-ScheduledTask -TaskName $watchdogTask -ErrorAction Stop; " +
		"if (($registered.Principal.UserId -ine $expectedUser) -and ($registered.Principal.UserId -ine $expectedShortUser)) { throw ('bridge principal user mismatch: ' + $registered.Principal.UserId + ' expected ' + $expectedUser) }; " +
		"if (($registeredWatchdog.Principal.UserId -ine $expectedUser) -and ($registeredWatchdog.Principal.UserId -ine $expectedShortUser)) { throw ('watchdog principal user mismatch: ' + $registeredWatchdog.Principal.UserId + ' expected ' + $expectedUser) }; " +
		"if ($registered.Principal.LogonType.ToString() -ne 'Interactive') { throw ('bridge logon type mismatch: ' + $registered.Principal.LogonType) }; " +
		"if ($registeredWatchdog.Principal.LogonType.ToString() -ne 'Interactive') { throw ('watchdog logon type mismatch: ' + $registeredWatchdog.Principal.LogonType) }; " +
		"if ($registered.Principal.RunLevel.ToString() -ne 'Limited') { throw ('bridge run level mismatch: ' + $registered.Principal.RunLevel) }; " +
		"if ($registeredWatchdog.Principal.RunLevel.ToString() -ne 'Limited') { throw ('watchdog run level mismatch: ' + $registeredWatchdog.Principal.RunLevel) }; " +
		"$action = @($registered.Actions)[0]; " +
		"$watchdogAction = @($registeredWatchdog.Actions)[0]; " +
		"if ($action.Execute -ne 'wscript.exe') { throw ('bridge action execute mismatch: ' + $action.Execute) }; " +
		"if ($watchdogAction.Execute -ne 'wscript.exe') { throw ('watchdog action execute mismatch: ' + $watchdogAction.Execute) } " +
		"} finally { " +
		"Unregister-ScheduledTask -TaskName $watchdogTask -Confirm:$false -ErrorAction SilentlyContinue | Out-Null; " +
		"Unregister-ScheduledTask -TaskName $task -Confirm:$false -ErrorAction SilentlyContinue | Out-Null " +
		"}"
	cmd := exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Register-ScheduledTask principal smoke failed: %v\n%s", err, out)
	}
}

func TestTeamsServiceWindowsTaskCleanupHelperProcess(t *testing.T) {
	if os.Getenv("CODEX_HELPER_WINDOWS_CLEANUP_HELPER_PROCESS") != "1" || teamsServiceLocalProcessKind(os.Args) != "run" {
		return
	}
	for {
		time.Sleep(time.Second)
	}
}

func TestTeamsBackgroundKeepaliveStartupFallbackWatchdogRestartsWSLLoopCI(t *testing.T) {
	args := []string{
		"-d", "Ubuntu",
		"-u", "alice",
		"--exec",
		"env",
		"CODEX_HELPER_TEAMS_SERVICE=1",
		"CODEX_HELPER_TEAMS_SERVICE_MODE=background",
		"/home/alice/bin/codex-proxy",
		"teams",
		"run",
		"--auto-service=false",
		"--registry",
		"/home/alice/.config/codex-helper/teams registry.json",
	}
	script := buildTeamsServiceWSLStartupWatchdogScript("Codex Helper Teams Bridge (WSL Ubuntu alice abc)", args, "abc")
	for _, want := range []string{
		"System.Threading.Mutex",
		"$deadline = (Get-Date).AddSeconds(60)",
		"Start-Sleep -Seconds 2",
		"while ($true)",
		"codex-helper-teams-wsl-stop-abc.signal",
		"Test-Path -LiteralPath $stopPath",
		"stop requested",
		"$wslArgumentLine",
		"Start-Process -FilePath",
		"-WindowStyle Hidden",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
		"wsl.exe exited",
		"restarting in 10s",
		"Start-Sleep -Seconds 10",
		"$mutex.ReleaseMutex(); $mutex.Dispose()",
		"-d Ubuntu",
		"/home/alice/bin/codex-proxy",
		"--auto-service=false",
		`"/home/alice/.config/codex-helper/teams registry.json"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("WSL Startup watchdog script missing %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "& wsl.exe @wslArgs") {
		t.Fatalf("WSL Startup watchdog script must not launch wsl.exe directly in a visible console path:\n%s", script)
	}
	if strings.Contains(script, "wsl.exe exited 0; exiting watchdog") {
		t.Fatalf("WSL Startup watchdog must keep watching after a clean WSL exit unless a stop signal is present:\n%s", script)
	}

	startCommand := buildTeamsServiceWSLStartupFallbackCommand("Codex Helper Teams Bridge (WSL Ubuntu alice abc)", args, true)
	for _, want := range []string{
		"[Environment]::GetFolderPath('Startup')",
		"codex-helper\\teams",
		".vbs",
		".ps1",
		"$launcherPath = Join-Path $startup",
		"$legacyCmdLauncherPath = Join-Path $startup",
		".cmd",
		"Get-ChildItem -LiteralPath $startup -Filter 'codex-helper-teams-wsl-*.cmd'",
		"Get-ChildItem -LiteralPath $startup -Filter 'codex-helper-teams-wsl-*.vbs'",
		"$content.Contains($legacyPrefix)",
		"$stopPath = Join-Path $appDir",
		"Remove-Item -LiteralPath $stopPath",
		"Remove-Item -LiteralPath $legacyCmdLauncherPath",
		"Set-Content -LiteralPath $scriptPath",
		"Set-Content -LiteralPath $launcherPath",
		"WScript.Shell",
		"shell.Run(cmd, 0, True)",
		"WScript.Quit code",
		"WindowStyle Hidden",
		"Start-Process -FilePath 'wscript.exe'",
		"//B //Nologo",
	} {
		if !strings.Contains(startCommand, want) {
			t.Fatalf("WSL Startup fallback command missing %q:\n%s", want, startCommand)
		}
	}
	for _, forbidden := range []string{
		"Set-Content -LiteralPath $legacyCmdLauncherPath",
		"Start-Process -FilePath 'cmd.exe'",
		"cmd.exe /",
	} {
		if strings.Contains(startCommand, forbidden) {
			t.Fatalf("WSL Startup fallback must not create or start a console .cmd launcher, found %q:\n%s", forbidden, startCommand)
		}
	}

	installOnlyCommand := buildTeamsServiceWSLStartupFallbackCommand("Codex Helper Teams Bridge (WSL Ubuntu alice abc)", args, false)
	if strings.Contains(installOnlyCommand, "Start-Process -FilePath 'wscript.exe'") {
		t.Fatalf("install-only WSL Startup fallback should not start watchdog immediately:\n%s", installOnlyCommand)
	}
}

func TestTeamsBackgroundKeepaliveWindowsTaskXMLLogonAndSelfRecoveryCI(t *testing.T) {
	spec := teamsServiceSpec{
		Executable:   `C:\Users\alice\AppData\Local\codex-helper\codex-proxy.exe`,
		WorkingDir:   `C:\Users\alice\work dir`,
		RegistryPath: `C:\Users\alice\AppData\Roaming\codex-helper\teams registry.json`,
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
		},
	}

	taskXML := buildTeamsServiceWindowsTaskXML(spec)
	for _, want := range []string{
		"<LogonTrigger>",
		"<Enabled>true</Enabled>",
		"<LogonType>InteractiveToken</LogonType>",
		"<RunLevel>LeastPrivilege</RunLevel>",
		"<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
		"<StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
		"<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"<StartWhenAvailable>true</StartWhenAvailable>",
		"<RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>",
		"<Hidden>true</Hidden>",
		"<Enabled>false</Enabled>",
		"<RestartOnFailure>",
		"<Interval>PT1M</Interval>",
		"<Count>999</Count>",
		"<Command>powershell.exe</Command>",
		"-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -Command",
		"codex-helper\\teams",
		"starting hidden Teams helper process",
		"Start-Process -FilePath",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
		"CODEX_HELPER_TEAMS_SERVICE",
		"CODEX_HELPER_TEAMS_SERVICE_MODE",
		"$code = $LASTEXITCODE",
		"exit $code",
		"teams",
		"run",
		"--registry",
	} {
		if !strings.Contains(taskXML, want) {
			t.Fatalf("Windows task XML missing %q:\n%s", want, taskXML)
		}
	}
	for _, forbidden := range []string{"S4U", "Password", "RunLevel>Highest", "HighestAvailable", "IdleSettings", "RunOnlyIfIdle"} {
		if strings.Contains(taskXML, forbidden) {
			t.Fatalf("Windows task XML should not contain %q:\n%s", forbidden, taskXML)
		}
	}
	requireSubstringsInOrder(t, taskXML,
		"<LogonTrigger>",
		"<Enabled>true</Enabled>",
		"</LogonTrigger>",
		"<Principals>",
		"<LogonType>InteractiveToken</LogonType>",
		"<RunLevel>LeastPrivilege</RunLevel>",
		"</Principals>",
		"<Settings>",
		"<Enabled>false</Enabled>",
		"<RestartOnFailure>",
	)
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
	spec, err := buildTeamsServiceSpec(stringPtr("/home/alice/teams registry.json"))
	if err != nil {
		t.Fatalf("buildTeamsServiceSpec error: %v", err)
	}
	taskName := teamsServiceWSLWindowsTaskBackend{}.Name()
	actionExecute := "wscript.exe"
	watchdogTaskName := teamsServiceWSLWindowsTaskBackend{}.watchdogName()
	watchdogExecute := "wscript.exe"
	runLogName := teamsServiceWSLTaskRunLogName(taskName)
	watchdogRunLogName := teamsServiceWSLTaskRunLogName(watchdogTaskName)
	launcherStem := teamsServiceWSLTaskLauncherStem(taskName, buildTeamsServiceWSLArguments(spec))
	watchdogLauncherStem := teamsServiceWSLTaskLauncherStem(watchdogTaskName, buildTeamsServiceWSLWatchdogArguments(spec))
	for _, want := range []string{
		"New-ScheduledTaskAction",
		"$expectedActionExecute = " + powershellSingleQuote(actionExecute),
		"$expectedActionExecute = " + powershellSingleQuote(watchdogExecute),
		"New-ScheduledTaskAction -Execute $expectedActionExecute -Argument $expectedActionArgument",
		"-Hidden",
		"$expectedActionArgument = '//B //Nologo",
		"wscript.exe",
		launcherStem + ".ps1",
		launcherStem + ".vbs",
		watchdogLauncherStem + ".ps1",
		watchdogLauncherStem + ".vbs",
		"$expectedLauncherPowerShell",
		"$expectedLauncherVbs",
		"-NoNewline",
		"WScript.Shell",
		"shell.Run(cmd, 0, True)",
		"WScript.Quit code",
		runLogName,
		watchdogRunLogName,
		"Add-Content -LiteralPath $runLog",
		"$wslArgumentLine",
		"Start-Process -FilePath ''wsl.exe''",
		"-WindowStyle Hidden",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
		"New-ScheduledTaskTrigger -AtLogOn",
		"New-ScheduledTaskSettingsSet",
		"-Hidden",
		"MultipleInstances IgnoreNew",
		"ExecutionTimeLimit (New-TimeSpan -Seconds 0)",
		"RestartCount 999",
		"RestartInterval (New-TimeSpan -Minutes 1)",
		"[System.Security.Principal.WindowsIdentity]::GetCurrent().Name",
		"Interactive",
		"RunLevel Limited",
		"Trigger $logon",
		"Disable-ScheduledTask",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("WSL scheduled task command missing %q:\n%s", want, command)
		}
	}
	for _, forbidden := range []string{
		"RepetitionInterval",
		"RepetitionDuration",
		"Trigger @($logon, $watchdog)",
		"$watchdog = New-ScheduledTaskTrigger",
	} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("WSL scheduled task command must not contain repeating triggers %q:\n%s", forbidden, command)
		}
	}
	if strings.Contains(command, "& wsl.exe @wslArgs") {
		t.Fatalf("WSL scheduled task command must not launch wsl.exe directly in a visible console path:\n%s", command)
	}
	if strings.Contains(command, "RestartInterval (New-TimeSpan -Seconds 10)") {
		t.Fatalf("WSL scheduled task command must not use Task Scheduler sub-minute restart intervals:\n%s", command)
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
		"--exec env",
		wantCWD,
		"CODEX_HOME=" + filepath.Join(tmp, "codex home"),
		wantExe + " teams run --owner-stale-after 1m30s --auto-service=false --registry",
		wantRegistry,
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("WSL config missing %q:\n%s", want, config)
		}
	}
}

func TestTeamsBackgroundKeepaliveWSLRepairEnablesStartsAndPreservesTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	if _, err := repairTeamsService(context.Background(), stringPtr("/home/alice/teams registry.json"), teamsServiceRepairOptions{Enable: true, Start: true}); err != nil {
		t.Fatalf("repairTeamsService error: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("WSL repair calls = %#v, want one Register-ScheduledTask call", runner.calls)
	}
	command := runner.calls[0].name + " " + strings.Join(runner.calls[0].args, " ")
	spec, err := buildTeamsServiceSpec(stringPtr("/home/alice/teams registry.json"))
	if err != nil {
		t.Fatalf("buildTeamsServiceSpec error: %v", err)
	}
	taskName := teamsServiceWSLWindowsTaskBackend{}.Name()
	actionExecute := "wscript.exe"
	watchdogTaskName := teamsServiceWSLWindowsTaskBackend{}.watchdogName()
	watchdogExecute := "wscript.exe"
	runLogName := teamsServiceWSLTaskRunLogName(taskName)
	watchdogRunLogName := teamsServiceWSLTaskRunLogName(watchdogTaskName)
	launcherStem := teamsServiceWSLTaskLauncherStem(taskName, buildTeamsServiceWSLArguments(spec))
	watchdogLauncherStem := teamsServiceWSLTaskLauncherStem(watchdogTaskName, buildTeamsServiceWSLWatchdogArguments(spec))
	for _, want := range []string{
		"Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue",
		"$legacyPrefix",
		"Unregister-ScheduledTask -TaskName $_.TaskName",
		"$expectedActionExecute = " + powershellSingleQuote(actionExecute),
		"$expectedActionExecute = " + powershellSingleQuote(watchdogExecute),
		"New-ScheduledTaskAction -Execute $expectedActionExecute -Argument $expectedActionArgument",
		"$expectedActionArgument = '//B //Nologo",
		launcherStem + ".ps1",
		launcherStem + ".vbs",
		watchdogLauncherStem + ".ps1",
		watchdogLauncherStem + ".vbs",
		"$expectedLauncherPowerShell",
		"$expectedLauncherVbs",
		"-NoNewline",
		"WScript.Shell",
		"shell.Run(cmd, 0, True)",
		"WScript.Quit code",
		"$actionMatches = ($null -ne $actualAction",
		"Teams WSL Scheduled Task action did not refresh; access is denied or task is protected",
		runLogName,
		watchdogRunLogName,
		"exited",
		"$wslArgumentLine",
		"Start-Process -FilePath ''wsl.exe''",
		"-WindowStyle Hidden",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
		"Register-ScheduledTask",
		"Enable-ScheduledTask -TaskName $taskName",
		"Start-CodexHelperScheduledTaskIfStopped $taskName",
		"Teams WSL Scheduled Task did not stay running after start",
		"Get-ScheduledTaskInfo -TaskName $taskName",
		"MultipleInstances IgnoreNew",
		"ExecutionTimeLimit (New-TimeSpan -Seconds 0)",
		"[System.Security.Principal.WindowsIdentity]::GetCurrent().Name",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("WSL repair command missing %q:\n%s", want, command)
		}
	}
	for _, forbidden := range []string{
		"RepetitionInterval",
		"RepetitionDuration",
		"Trigger @($logon, $watchdog)",
		"$watchdog = New-ScheduledTaskTrigger",
	} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("WSL repair command must not contain repeating triggers %q:\n%s", forbidden, command)
		}
	}
	if strings.Contains(command, "& wsl.exe @wslArgs") {
		t.Fatalf("WSL repair command must not launch wsl.exe directly in a visible console path:\n%s", command)
	}
	if strings.Contains(command, "Disable-ScheduledTask -TaskName $taskName") {
		t.Fatalf("WSL repair enable command should not disable task:\n%s", command)
	}
	if got := strings.Count(command, "Teams WSL Scheduled Task did not stay running after start"); got != 2 {
		t.Fatalf("WSL repair should verify both bridge and watchdog tasks, got %d verifications:\n%s", got, command)
	}
}

func TestTeamsBackgroundKeepaliveWSLRepairReplacesStaleRegisteredActionCI(t *testing.T) {
	lockCLITestHooks(t)

	taskName := "Codex Helper Teams Bridge (WSL Ubuntu alice abc)"
	args := []string{
		"-d", "Ubuntu",
		"-u", "alice",
		"--cd", "/home/alice",
		"--exec", "env",
		"CODEX_HELPER_TEAMS_SERVICE=1",
		"/home/alice/bin/codex-proxy",
		"teams", "run", "--auto-service=false",
	}
	command := buildTeamsServiceWSLRegisterCommand(taskName, args, teamsServiceWSLRegisterOptions{
		Enable:      true,
		Start:       true,
		CleanLegacy: true,
	})

	firstRegister := strings.Index(command, "Register-ScheduledTask -TaskName $taskName")
	reRegister := strings.LastIndex(command, "Register-ScheduledTask -TaskName $taskName")
	if firstRegister < 0 || reRegister <= firstRegister {
		t.Fatalf("WSL repair command should register once, verify, then re-register after stale action mismatch:\n%s", command)
	}
	for _, want := range []string{
		"$actualAction.Execute -eq $expectedActionExecute",
		"$actualAction.Arguments -eq $expectedActionArgument",
		"if (-not $actionMatches) { Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue; Register-ScheduledTask -TaskName $taskName",
		"if (-not $actionMatches) { throw 'Teams WSL Scheduled Task action did not refresh; access is denied or task is protected' }",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("WSL repair stale-action hardening missing %q:\n%s", want, command)
		}
	}
	if enableIdx := strings.Index(command, "Enable-ScheduledTask -TaskName $taskName"); enableIdx < 0 || enableIdx < reRegister {
		t.Fatalf("WSL repair should only enable after stale-action verification/re-register:\n%s", command)
	}
	if startIdx := strings.Index(command, "Start-CodexHelperScheduledTaskIfStopped $taskName"); startIdx < 0 || startIdx < reRegister {
		t.Fatalf("WSL repair should only start after stale-action verification/re-register:\n%s", command)
	}
}

func TestTeamsBackgroundKeepaliveWSLTaskConfigMatchChecksFullTaskShapeCI(t *testing.T) {
	lockCLITestHooks(t)

	taskName := "Codex Helper Teams Bridge (WSL Ubuntu alice abc)"
	args := []string{
		"-d", "Ubuntu",
		"-u", "alice",
		"--cd", "/home/alice",
		"--exec", "env",
		"CODEX_HELPER_TEAMS_SERVICE=1",
		"/home/alice/bin/codex-proxy",
		"teams", "run", "--auto-service=false",
	}
	actionExecute := "wscript.exe"
	launcherStem := teamsServiceWSLTaskLauncherStem(taskName, args)
	command := teamsServiceWSLTaskConfigMatchHelpersPowerShell() +
		buildTeamsServiceWSLTaskConfigMatchesCommand(taskName, args, teamsServiceWSLTaskConfigMatchOptions{})
	for _, want := range []string{
		"Test-CodexHelperTaskDurationMinutes",
		"Test-Path -LiteralPath $launcherPowerShellPath",
		"Get-Content -LiteralPath $launcherPowerShellPath -Raw",
		"Get-Content -LiteralPath $launcherVbsPath -Raw",
		"$expectedLauncherPowerShell",
		"$expectedLauncherVbs",
		"$launcherPowerShellMatches",
		"$launcherVbsMatches",
		launcherStem + ".ps1",
		launcherStem + ".vbs",
		"Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue",
		"$expectedActionExecute = " + powershellSingleQuote(actionExecute),
		"$expectedActionArgument = '//B //Nologo",
		"$task.State -eq 'Disabled'",
		"$expectedPrincipalUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name",
		"$task.Principal.UserId -ne $expectedPrincipalUser",
		"$task.Principal.LogonType -ne 'Interactive'",
		"$task.Principal.RunLevel -ne 'Limited'",
		"$settings.MultipleInstances -ne 'IgnoreNew'",
		"$settings.RestartCount -ne 999",
		"$settings.RestartInterval 1",
		"$settings.ExecutionTimeLimit 0",
		"$settings.Hidden -ne $true",
		"$hasLogonTrigger",
		"$className -like '*LogonTrigger*'",
		"$hasRepeatingTrigger",
		"$hasRepeatingTrigger) { $allMatched = $false",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("WSL task match probe missing %q:\n%s", want, command)
		}
	}
	for _, forbidden := range []string{
		"Register-ScheduledTask",
		"Start-ScheduledTask",
		"Stop-ScheduledTask",
		"Unregister-ScheduledTask",
		"Set-Content",
		"New-ScheduledTaskTrigger -Once",
	} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("WSL task match probe should be read-only and not contain %q:\n%s", forbidden, command)
		}
	}
}

func TestTeamsBackgroundKeepaliveWSLStatusPrintsTaskRunLogPathCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	backend := teamsServiceWSLWindowsTaskBackend{}
	if _, err := backend.Run(context.Background(), "status"); err != nil {
		t.Fatalf("WSL status command error: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("WSL status calls = %#v, want one PowerShell call", runner.calls)
	}
	command := strings.Join(runner.calls[0].args, " ")
	for _, want := range []string{
		"Get-ScheduledTaskInfo -TaskName $taskName",
		"[Environment]::GetFolderPath('LocalApplicationData')",
		"RunLog : ",
		teamsServiceWSLTaskRunLogName(backend.Name()),
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("WSL status command missing %q:\n%s", want, command)
		}
	}
}

func TestTeamsBackgroundKeepaliveAutoEnsureWSLRepairsAndStartsFromForeground(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	if err := ensureTeamsServiceForRun(context.Background(), stringPtr("/home/alice/teams registry.json")); err != nil {
		t.Fatalf("ensureTeamsServiceForRun error: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("auto ensure calls = %#v, want one repair call", runner.calls)
	}
	command := strings.Join(runner.calls[0].args, " ")
	if !strings.Contains(command, "Enable-ScheduledTask -TaskName $taskName") || !strings.Contains(command, "Start-CodexHelperScheduledTaskIfStopped $taskName") {
		t.Fatalf("auto ensure did not enable and start foreground WSL task:\n%s", command)
	}
}

func TestTeamsBackgroundKeepaliveAutoEnsureWSLServiceModeDoesNotRegisterItself(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")

	if err := ensureTeamsServiceForRun(context.Background(), stringPtr("/home/alice/teams registry.json")); err != nil {
		t.Fatalf("ensureTeamsServiceForRun service mode error: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("auto ensure service mode should not re-register its own WSL supervisor task: %#v", runner.calls)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapDirectRepairSuccessCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	taskConfigPath, err := backend.Path()
	if err != nil {
		t.Fatalf("task config path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(taskConfigPath), 0o700); err != nil {
		t.Fatalf("mkdir task config dir: %v", err)
	}
	if err := os.WriteFile(taskConfigPath, []byte("stale scheduled task config"), 0o600); err != nil {
		t.Fatalf("write stale task config: %v", err)
	}

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap direct repair error: %v", err)
	}
	if len(runner.calls) != 4 {
		t.Fatalf("bootstrap direct repair calls = %#v, want repair, verification, stale task retirement, and stale fallback cleanup", runner.calls)
	}
	command := strings.Join(runner.calls[0].args, " ")
	for _, want := range []string{
		"Register-ScheduledTask",
		"Enable-ScheduledTask -TaskName $taskName",
		"Start-CodexHelperScheduledTaskIfStopped $taskName",
		"RunLevel Limited",
		"--auto-service=false",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("bootstrap direct command missing %q:\n%s", want, command)
		}
	}
	if got := out.String(); !strings.Contains(got, "Teams service bootstrap ready: wsl-windows-task-scheduler") {
		t.Fatalf("bootstrap output missing success mode:\n%s", got)
	}
	retireLegacy := strings.Join(runner.calls[2].args, " ")
	if !strings.Contains(retireLegacy, "Disable-ScheduledTask") || !strings.Contains(retireLegacy, "continue") || strings.Contains(retireLegacy, "Unregister-ScheduledTask") {
		t.Fatalf("direct bootstrap should disable legacy WSL tasks without deleting the current task:\n%s", retireLegacy)
	}
	configData, err := os.ReadFile(taskConfigPath)
	if err != nil {
		t.Fatalf("read refreshed task config: %v", err)
	}
	if config := string(configData); strings.Contains(config, "stale scheduled task config") || !strings.Contains(config, "--exec env") {
		t.Fatalf("direct repair should refresh stale task config, got:\n%s", config)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapSuccessCleansStartupFallbackCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startupFallbackMarkerPath error: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker dir: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("legacy fallback"), 0o600); err != nil {
		t.Fatalf("write fallback marker: %v", err)
	}
	legacyMarkerPath := filepath.Join(filepath.Dir(markerPath), "codex-helper-teams-wsl-startup-legacy123.txt")
	legacyTaskName := teamsServiceWSLTaskNamePrefix(backend.Name()) + "legacy123)"
	if err := os.WriteFile(legacyMarkerPath, []byte("TaskName="+legacyTaskName+"\n"), 0o600); err != nil {
		t.Fatalf("write legacy fallback marker: %v", err)
	}
	otherMarkerPath := filepath.Join(filepath.Dir(markerPath), "codex-helper-teams-wsl-startup-other999.txt")
	if err := os.WriteFile(otherMarkerPath, []byte("TaskName=Codex Helper Teams Bridge (WSL Other user default other999)\n"), 0o600); err != nil {
		t.Fatalf("write unrelated fallback marker: %v", err)
	}

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap direct repair error: %v", err)
	}
	if len(runner.calls) != 4 {
		t.Fatalf("bootstrap with fallback cleanup calls = %#v, want repair, verification, stale task retirement, and cleanup", runner.calls)
	}
	cleanup := strings.Join(runner.calls[3].args, " ")
	for _, want := range []string{
		"GetFolderPath('Startup')",
		"Remove-Item",
		"codex-helper-teams-wsl-",
		".cmd",
		".vbs",
		".ps1",
		"legacy123",
		"Set-Content -LiteralPath $stopPath",
		"Get-CimInstance Win32_Process",
		"OrdinalIgnoreCase",
		"Stop-Process -Id $proc.ProcessId -Force",
		"$content.Contains('starting ' + $legacyPrefix)",
		"Get-ChildItem -LiteralPath $startup -Filter 'codex-helper-teams-wsl-*.cmd'",
		"Get-ChildItem -LiteralPath $startup -Filter 'codex-helper-teams-wsl-*.vbs'",
	} {
		if !strings.Contains(cleanup, want) {
			t.Fatalf("startup fallback cleanup command missing %q:\n%s", want, cleanup)
		}
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("startup fallback marker was not removed, err=%v", err)
	}
	if _, err := os.Stat(legacyMarkerPath); !os.IsNotExist(err) {
		t.Fatalf("legacy startup fallback marker was not removed, err=%v", err)
	}
	if _, err := os.Stat(teamsServiceWSLStartupFallbackStopPath(markerPath)); err != nil {
		t.Fatalf("startup fallback stop file was not written: %v", err)
	}
	if _, err := os.Stat(teamsServiceWSLStartupFallbackStopPath(legacyMarkerPath)); err != nil {
		t.Fatalf("legacy startup fallback stop file was not written: %v", err)
	}
	if _, err := os.Stat(otherMarkerPath); err != nil {
		t.Fatalf("unrelated startup fallback marker should remain: %v", err)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapAccessDeniedConfirmsBeforeUACCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("NVIDIA\\jason\n"),
			nil,
		},
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	taskConfigPath, err := backend.Path()
	if err != nil {
		t.Fatalf("task config path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(taskConfigPath), 0o700); err != nil {
		t.Fatalf("mkdir task config dir: %v", err)
	}
	if err := os.WriteFile(taskConfigPath, []byte("stale scheduled task config"), 0o600); err != nil {
		t.Fatalf("write stale task config: %v", err)
	}

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetIn(strings.NewReader("yes\n"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap UAC repair error: %v\noutput:\n%s", err, out.String())
	}
	if len(runner.calls) != 7 {
		t.Fatalf("bootstrap UAC calls = %#v, want direct, current-user query, elevated repair, normal start, verification, stale task retirement, and cleanup", runner.calls)
	}
	gotOut := out.String()
	if !strings.Contains(gotOut, "NEXT STEP: TYPE yes TO CONTINUE") || !strings.Contains(gotOut, "Type yes and press Enter") {
		t.Fatalf("bootstrap did not clearly prompt before UAC:\n%s", gotOut)
	}
	if strings.Contains(gotOut, "Show the UAC prompt now?") {
		t.Fatalf("bootstrap UAC prompt should be concise and action-only:\n%s", gotOut)
	}
	if strings.Contains(gotOut, "Windows blocked automatic Scheduled Task setup") || strings.Contains(gotOut, "Register-ScheduledTask failed") {
		t.Fatalf("bootstrap should not print raw access-denied repair errors before UAC:\n%s", gotOut)
	}
	elevated := strings.Join(runner.calls[2].args, " ")
	wantCWD := teamsServiceTestAbsPath(t, "/home/alice/work dir")
	wantExe := teamsServiceTestAbsPath(t, "/home/alice/bin/codex-proxy")
	for _, want := range []string{
		"Start-Process",
		"-Verb RunAs",
		"-Wait",
		"-PassThru",
		"New-ScheduledTaskAction",
		"$expectedActionExecute = ''wscript.exe''",
		"$expectedActionArgument = ''//B //Nologo",
		"WScript.Shell",
		"-WindowStyle Hidden",
		"wsl.exe",
		"wslArgumentLine",
		"Ubuntu",
		"alice",
		wantCWD,
		wantExe,
		"Register-ScheduledTask",
		"Enable-ScheduledTask",
		"--auto-service=false",
		"RunLevel Limited",
		"NVIDIA\\jason",
	} {
		if !strings.Contains(elevated, want) {
			t.Fatalf("elevated command missing %q:\n%s", want, elevated)
		}
	}
	if strings.Contains(elevated, "Start-ScheduledTask") {
		t.Fatalf("elevated repair should not start WSL tasks before returning to current-user context:\n%s", elevated)
	}
	start := strings.Join(runner.calls[3].args, " ")
	for _, want := range []string{
		"Start-CodexHelperScheduledTaskIfStopped $taskName",
		"Enable-ScheduledTask -TaskName $taskName",
		teamsServiceWSLVerifyTaskRunningPowerShell(),
	} {
		if !strings.Contains(start, want) {
			t.Fatalf("post-UAC WSL start command missing %q:\n%s", want, start)
		}
	}
	for _, forbidden := range []string{"RunLevel Highest", "HighestAvailable", "NT AUTHORITY\\SYSTEM", "-UserId 'SYSTEM'", "LogonType Password"} {
		if strings.Contains(elevated, forbidden) || strings.Contains(start, forbidden) {
			t.Fatalf("UAC repair/start must stay current-user least-privilege, found %q:\nelevated=%s\nstart=%s", forbidden, elevated, start)
		}
	}
	if got := out.String(); !strings.Contains(got, "Teams service bootstrap ready: wsl-windows-task-scheduler-uac") {
		t.Fatalf("bootstrap output missing UAC success mode:\n%s", got)
	}
	configData, err := os.ReadFile(taskConfigPath)
	if err != nil {
		t.Fatalf("read refreshed task config: %v", err)
	}
	if config := string(configData); strings.Contains(config, "stale scheduled task config") || !strings.Contains(config, "--exec env") {
		t.Fatalf("UAC repair should refresh stale task config, got:\n%s", config)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapDeclineUACInstallsStartupFallbackCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	fallbackBackend := teamsServiceWSLWindowsTaskBackend{}
	fallbackTaskConfigPath, err := fallbackBackend.Path()
	if err != nil {
		t.Fatalf("fallback task config path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(fallbackTaskConfigPath), 0o700); err != nil {
		t.Fatalf("mkdir fallback task config dir: %v", err)
	}
	if err := os.WriteFile(fallbackTaskConfigPath, []byte("stale scheduled task config"), 0o600); err != nil {
		t.Fatalf("write stale fallback task config: %v", err)
	}

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetIn(strings.NewReader("no\n"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap fallback error: %v\noutput:\n%s", err, out.String())
	}
	if len(runner.calls) != 3 {
		t.Fatalf("bootstrap fallback calls = %#v, want direct repair, stale task retirement, then Startup fallback", runner.calls)
	}
	retire := strings.Join(runner.calls[1].args, " ")
	if !strings.Contains(retire, "Disable-ScheduledTask") || !strings.Contains(retire, "Codex Helper Teams Watchdog") {
		t.Fatalf("fallback path should disable stale scheduled tasks before installing Startup fallback:\n%s", retire)
	}
	fallback := strings.Join(runner.calls[2].args, " ")
	for _, want := range []string{
		"GetFolderPath('Startup')",
		"codex-helper\\teams",
		"System.Threading.Mutex",
		"wsl.exe",
		"--auto-service=false",
		"CODEX_HELPER_TEAMS_STARTUP_FALLBACK_STOP_FILE=",
		"Remove-Item -LiteralPath $stopPath",
		"Remove-Item -LiteralPath $legacyCmdLauncherPath",
		"Get-CimInstance Win32_Process",
		"$proc.ProcessId -ne $PID",
		"Stop-Process -Id $proc.ProcessId -Force",
		"$launcherPath = Join-Path $startup",
		"Set-Content -LiteralPath $launcherPath",
		"WScript.Shell",
		"shell.Run(cmd, 0, True)",
		"Start-Process -FilePath 'wscript.exe'",
		"//B //Nologo",
	} {
		if !strings.Contains(fallback, want) {
			t.Fatalf("fallback command missing %q:\n%s", want, fallback)
		}
	}
	if strings.Contains(fallback, "Set-Content -LiteralPath $legacyCmdLauncherPath") || strings.Contains(fallback, "Start-Process -FilePath 'cmd.exe'") {
		t.Fatalf("Startup fallback should not create or start a console .cmd launcher:\n%s", fallback)
	}
	if strings.Contains(fallback, "CODEX_HELPER_TEAMS_EXIT_ON_STANDBY=1") {
		t.Fatalf("Startup fallback must not retire itself when another helper temporarily owns the shared home:\n%s", fallback)
	}
	if strings.Contains(fallback, "-Verb RunAs") || strings.Contains(fallback, "Register-ScheduledTask") {
		t.Fatalf("Startup fallback should not use UAC or Task Scheduler:\n%s", fallback)
	}
	files, err := filepath.Glob(filepath.Join(tmp, "wsl-task", "codex-helper-teams-wsl-startup-*.txt"))
	if err != nil {
		t.Fatalf("glob Startup fallback marker: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("Startup fallback marker files = %#v, want one", files)
	}
	if _, err := os.Stat(fallbackTaskConfigPath); !os.IsNotExist(err) {
		t.Fatalf("stale Scheduled Task config should be removed after fallback install, err=%v", err)
	}
	if got := out.String(); !strings.Contains(got, "UAC prompt was not confirmed") || !strings.Contains(got, "NOTICE: USING STARTUP WATCHDOG FALLBACK") || !strings.Contains(got, "Teams service bootstrap ready: wsl-startup-watchdog") {
		t.Fatalf("bootstrap fallback output missing confirmation and mode:\n%s", got)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapPreservesTaskConfigWhenFallbackFailsCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			errTeamsKeepaliveScheduledTaskFailureForTest{},
			nil,
			errTeamsKeepaliveScheduledTaskFailureForTest{},
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	taskConfigPath, err := backend.Path()
	if err != nil {
		t.Fatalf("task config path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(taskConfigPath), 0o700); err != nil {
		t.Fatalf("mkdir task config dir: %v", err)
	}
	if err := os.WriteFile(taskConfigPath, []byte("stale scheduled task config"), 0o600); err != nil {
		t.Fatalf("write stale task config: %v", err)
	}

	var out strings.Builder
	var errOut strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"bootstrap", "--no-uac"})
	if err := cmd.Execute(); err == nil {
		t.Fatalf("bootstrap should fail when both Scheduled Task and Startup fallback fail")
	}
	configData, err := os.ReadFile(taskConfigPath)
	if err != nil {
		t.Fatalf("stale Scheduled Task config should be preserved when fallback fails: %v", err)
	}
	if string(configData) != "stale scheduled task config" {
		t.Fatalf("fallback failure should not rewrite stale task config, got:\n%s", configData)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapRefusesFallbackWhenStaleTasksCannotRetireCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			errTeamsKeepaliveScheduledTaskFailureForTest{},
			errTeamsKeepaliveAccessDeniedForTest{},
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-uac"})
	err := cmd.Execute()
	if err == nil {
		t.Fatalf("bootstrap should refuse Startup fallback when old Scheduled Tasks cannot be disabled")
	}
	if !strings.Contains(err.Error(), "fallback is unsafe") || !strings.Contains(err.Error(), "old WSL Scheduled Tasks could not be disabled") {
		t.Fatalf("bootstrap error should explain unsafe fallback, got: %v", err)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("bootstrap calls = %#v, want direct repair and stale task retirement only", runner.calls)
	}
	retire := strings.Join(runner.calls[1].args, " ")
	if !strings.Contains(retire, "Disable-ScheduledTask") || strings.Contains(retire, "Start-Process -FilePath 'wscript.exe'") {
		t.Fatalf("second call should only retire stale tasks:\n%s", retire)
	}
	if strings.Contains(out.String(), "Teams service bootstrap ready") {
		t.Fatalf("bootstrap should not report success when stale task retirement failed:\n%s", out.String())
	}
	if strings.Contains(out.String(), "NOTICE: USING STARTUP WATCHDOG FALLBACK") {
		t.Fatalf("bootstrap should not announce Startup fallback before stale tasks are safely disabled:\n%s", out.String())
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapUsesElevatedRetireBeforeFallbackCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("NVIDIA\\jason\n"),
			nil,
			nil,
			nil,
			nil,
		},
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			errTeamsKeepaliveAccessDeniedForTest{},
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetIn(strings.NewReader("yes\n"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap fallback with elevated retire error: %v\noutput:\n%s", err, out.String())
	}
	if len(runner.calls) != 6 {
		t.Fatalf("bootstrap calls = %#v, want direct repair, user query, elevated repair, normal retire, elevated retire, fallback", runner.calls)
	}
	elevatedRetire := strings.Join(runner.calls[4].args, " ")
	for _, want := range []string{
		"Start-Process",
		"-Verb RunAs",
		"Disable-ScheduledTask",
		"Codex Helper Teams Bridge",
		"Codex Helper Teams Watchdog",
	} {
		if !strings.Contains(elevatedRetire, want) {
			t.Fatalf("elevated retire command missing %q:\n%s", want, elevatedRetire)
		}
	}
	if strings.Contains(elevatedRetire, "Register-ScheduledTask") {
		t.Fatalf("elevated retire must not try to create or repair tasks:\n%s", elevatedRetire)
	}
	fallback := strings.Join(runner.calls[5].args, " ")
	if !strings.Contains(fallback, "Start-Process -FilePath 'wscript.exe'") || !strings.Contains(fallback, "Remove-Item -LiteralPath $legacyCmdLauncherPath") {
		t.Fatalf("fallback install should use hidden wscript launcher and remove old .cmd launcher:\n%s", fallback)
	}
	got := out.String()
	if !strings.Contains(got, "NOTICE: USING STARTUP WATCHDOG FALLBACK") || !strings.Contains(got, "Old WSL Scheduled Tasks were disabled using Windows permission") {
		t.Fatalf("bootstrap output missing elevated cleanup fallback explanation:\n%s", got)
	}
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackStopFileRetiresMarkerCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         &recordingTeamsServiceRunner{},
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startupFallbackMarkerPath error: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker dir: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("Fallback=Windows Startup watchdog\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	installed, err := backend.StartupFallbackMarkerExists()
	if err != nil || !installed {
		t.Fatalf("fresh Startup fallback marker should be active, installed=%v err=%v", installed, err)
	}
	if err := os.WriteFile(teamsServiceWSLStartupFallbackStopPath(markerPath), []byte("stop\n"), 0o600); err != nil {
		t.Fatalf("write stop file: %v", err)
	}
	installed, err = backend.StartupFallbackMarkerExists()
	if err != nil {
		t.Fatalf("stopped Startup fallback marker check error: %v", err)
	}
	if installed {
		t.Fatalf("Startup fallback marker with a stop file should not block Scheduled Task repair")
	}
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackServiceStartNoopsWhenActiveCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{err: errTeamsKeepaliveScheduledTaskFailureForTest{}}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startupFallbackMarkerPath error: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker dir: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("Fallback=Windows Startup watchdog\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	if err := startTeamsService(context.Background(), false); err != nil {
		t.Fatalf("startTeamsService start: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("startup fallback start calls = %#v, want only the Scheduled Task active probe", runner.calls)
	}
	joined := strings.Join(runner.calls[0].args, " ")
	if !strings.Contains(joined, "if ($task.State -ne 'Running') { exit 3 }") {
		t.Fatalf("startup fallback active start should only probe active state:\n%s", joined)
	}
	for _, forbidden := range []string{"Start-Process", "Start-ScheduledTask", "Enable-ScheduledTask"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("startup fallback active start should not start another wrapper via %q:\n%s", forbidden, joined)
		}
	}

	runner.calls = nil
	if err := startTeamsPrimaryService(context.Background(), false); err != nil {
		t.Fatalf("startTeamsPrimaryService start: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("startup fallback primary start calls = %#v, want only the Scheduled Task active probe", runner.calls)
	}
	joined = strings.Join(runner.calls[0].args, " ")
	if !strings.Contains(joined, "if ($task.State -ne 'Running') { exit 3 }") {
		t.Fatalf("startup fallback active primary start should only probe active state:\n%s", joined)
	}
	for _, forbidden := range []string{"Start-Process", "Start-ScheduledTask", "Enable-ScheduledTask"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("startup fallback active primary start should not start another wrapper via %q:\n%s", forbidden, joined)
		}
	}
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackServiceStartPreservesOutputCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{nil, []byte("hidden launcher started\n")},
		errs:    []error{errTeamsKeepaliveScheduledTaskFailureForTest{}, nil},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) {
		return nil, nil
	}
	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startupFallbackMarkerPath error: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker dir: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("Fallback=Windows Startup watchdog\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	if err := os.WriteFile(teamsServiceWSLStartupFallbackStopPath(markerPath), []byte("stop\n"), 0o600); err != nil {
		t.Fatalf("write stop marker: %v", err)
	}

	var out strings.Builder
	if err := startTeamsServiceWithOutput(context.Background(), false, &out); err != nil {
		t.Fatalf("startTeamsServiceWithOutput start: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "hidden launcher started") ||
		!strings.Contains(got, "Startup watchdog fallback: started") ||
		!strings.Contains(got, markerPath) {
		t.Fatalf("startup fallback start output was not preserved:\n%s", got)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("startup fallback start calls = %#v, want active probe then fallback start", runner.calls)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapNoUACFallsBackOnTaskFailureCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			errTeamsKeepaliveScheduledTaskFailureForTest{},
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-uac"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap --no-uac fallback error: %v\noutput:\n%s", err, out.String())
	}
	if len(runner.calls) != 3 {
		t.Fatalf("bootstrap --no-uac calls = %#v, want direct repair, stale task retirement, then Startup fallback", runner.calls)
	}
	if got := out.String(); !strings.Contains(got, "NOTICE: USING STARTUP WATCHDOG FALLBACK") || !strings.Contains(got, "Windows Scheduled Task setup could not be completed") || !strings.Contains(got, "Teams service bootstrap ready: wsl-startup-watchdog") {
		t.Fatalf("bootstrap --no-uac output missing protected-task fallback:\n%s", got)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapClassifiesAccessDeniedOutputCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			[]byte("Register-ScheduledTask : Access is denied.\n"),
			nil,
		},
		errs: []error{
			errTeamsKeepaliveScheduledTaskFailureForTest{},
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-uac"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap fallback error: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	if !strings.Contains(got, "NOTICE: USING STARTUP WATCHDOG FALLBACK") || !strings.Contains(got, "Windows Scheduled Task setup could not be completed") || !strings.Contains(got, "Windows denied permission") {
		t.Fatalf("bootstrap did not classify access denied PowerShell output:\n%s", got)
	}
	if strings.Contains(got, "Register-ScheduledTask : Access is denied") {
		t.Fatalf("bootstrap should summarize access denied output instead of printing raw PowerShell noise:\n%s", got)
	}
	if !strings.Contains(got, "Teams service bootstrap ready: wsl-startup-watchdog") {
		t.Fatalf("bootstrap output missing fallback mode:\n%s", got)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("bootstrap calls = %#v, want direct repair, stale task retirement, then Startup fallback", runner.calls)
	}
	if joined := strings.Join(runner.calls[2].args, " "); !strings.Contains(joined, "Start-Process -FilePath 'wscript.exe'") || !strings.Contains(joined, "WScript.Shell") || strings.Contains(joined, "Register-ScheduledTask") {
		t.Fatalf("fallback call should use Startup watchdog without re-registering task:\n%s", joined)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapFallsBackWhenScheduledTasksCmdletsUnavailableCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			[]byte("Register-ScheduledTask : The term 'Register-ScheduledTask' is not recognized as the name of a cmdlet.\n"),
		},
		errs: []error{
			errors.New("exit status 1"),
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap fallback error: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	if !strings.Contains(got, "NOTICE: USING STARTUP WATCHDOG FALLBACK") ||
		!strings.Contains(got, "Scheduled Task cmdlets are unavailable") ||
		!strings.Contains(got, "Teams service bootstrap ready: wsl-startup-watchdog") {
		t.Fatalf("bootstrap output missing ScheduledTasks-unavailable fallback:\n%s", got)
	}
	if strings.Contains(got, "NEXT STEP: TYPE yes TO CONTINUE") {
		t.Fatalf("ScheduledTasks-unavailable fallback should not ask for UAC:\n%s", got)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("bootstrap calls = %#v, want direct repair then Startup fallback", runner.calls)
	}
	fallback := strings.Join(runner.calls[1].args, " ")
	if !strings.Contains(fallback, "Start-Process -FilePath 'wscript.exe'") || strings.Contains(fallback, "Register-ScheduledTask") {
		t.Fatalf("fallback call should use Startup watchdog without ScheduledTasks cmdlets:\n%s", fallback)
	}
}

func TestTeamsBackgroundKeepaliveWSLBootstrapDoesNotSkipRetireWhenTaskConfigExistsCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			[]byte("Register-ScheduledTask : The term 'Register-ScheduledTask' is not recognized as the name of a cmdlet.\n"),
		},
		errs: []error{
			errors.New("exit status 1"),
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	taskConfigPath, err := backend.Path()
	if err != nil {
		t.Fatalf("task config path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(taskConfigPath), 0o700); err != nil {
		t.Fatalf("mkdir task config dir: %v", err)
	}
	if err := os.WriteFile(taskConfigPath, []byte("stale scheduled task config"), 0o600); err != nil {
		t.Fatalf("write stale task config: %v", err)
	}

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/teams registry.json"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	err = cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "Windows Scheduled Task setup failed") {
		t.Fatalf("bootstrap error = %v, want hard failure while stale task config exists\noutput:\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "USING STARTUP WATCHDOG FALLBACK") {
		t.Fatalf("bootstrap must not install fallback while stale task config exists:\n%s", out.String())
	}
	if len(runner.calls) != 1 {
		t.Fatalf("bootstrap calls = %#v, want only direct repair", runner.calls)
	}
}

func TestTeamsBackgroundKeepaliveAutoEnsureWSLAccessDeniedInstallsStartupFallbackOnceCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	err := ensureTeamsServiceForRun(context.Background(), stringPtr("/home/alice/teams registry.json"))
	if err == nil || !strings.Contains(err.Error(), "Startup watchdog fallback") {
		t.Fatalf("auto ensure access denied error = %v, want Startup fallback diagnostic", err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("auto ensure calls = %#v, want direct repair, stale task retirement, plus fallback install", runner.calls)
	}
	retire := strings.Join(runner.calls[1].args, " ")
	if !strings.Contains(retire, "Disable-ScheduledTask") {
		t.Fatalf("auto ensure should retire stale WSL tasks before fallback:\n%s", retire)
	}
	fallback := strings.Join(runner.calls[2].args, " ")
	if strings.Contains(fallback, "Start-Process -FilePath $cmdPath") {
		t.Fatalf("auto ensure fallback should not start a background watchdog while foreground run is active:\n%s", fallback)
	}
	if !strings.Contains(fallback, "GetFolderPath('Startup')") || !strings.Contains(fallback, "--auto-service=false") {
		t.Fatalf("auto ensure fallback command missing Startup watchdog setup:\n%s", fallback)
	}

	runner.calls = nil
	runner.errs = nil
	if err := ensureTeamsServiceForRun(context.Background(), stringPtr("/home/alice/teams registry.json")); err != nil {
		t.Fatalf("auto ensure with fallback marker should skip retry, got: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("auto ensure should not retry blocked Scheduled Task after fallback marker: %#v", runner.calls)
	}
}

func TestTeamsBackgroundKeepaliveAutoEnsureWSLScheduledTasksUnavailableSkipsRetireWithoutTaskConfigCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unavailable := errors.New("Register-ScheduledTask : The term 'Register-ScheduledTask' is not recognized as the name of a cmdlet")
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			unavailable,
			unavailable,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	err := ensureTeamsServiceForRun(context.Background(), stringPtr("/home/alice/teams registry.json"))
	if err == nil || !strings.Contains(err.Error(), "Startup watchdog fallback") {
		t.Fatalf("auto ensure ScheduledTasks-unavailable error = %v, want Startup fallback diagnostic", err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("auto ensure calls = %#v, want direct repair, failed retire probe, plus fallback install", runner.calls)
	}
	retire := strings.Join(runner.calls[1].args, " ")
	if !strings.Contains(retire, "Disable-ScheduledTask") {
		t.Fatalf("auto ensure should still attempt stale WSL task retirement before safe skip:\n%s", retire)
	}
	fallback := strings.Join(runner.calls[2].args, " ")
	if !strings.Contains(fallback, "GetFolderPath('Startup')") || strings.Contains(fallback, "Register-ScheduledTask") {
		t.Fatalf("auto ensure fallback command should use Startup watchdog without ScheduledTasks cmdlets:\n%s", fallback)
	}
}

func TestTeamsBackgroundKeepaliveAutoEnsureWSLTaskFailureInstallsStartupFallbackOnceCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			[]byte("Register-ScheduledTask : Access is denied.\n"),
		},
		errs: []error{
			errTeamsKeepaliveScheduledTaskFailureForTest{},
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	err := ensureTeamsServiceForRun(context.Background(), stringPtr("/home/alice/teams registry.json"))
	if err == nil || !strings.Contains(err.Error(), "Startup watchdog fallback") || !strings.Contains(err.Error(), "blocked by Windows policy") {
		t.Fatalf("auto ensure task failure error = %v, want protected-task Startup fallback diagnostic", err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("auto ensure calls = %#v, want direct repair, stale task retirement, plus fallback install", runner.calls)
	}

	runner.calls = nil
	runner.errs = nil
	if err := ensureTeamsServiceForRun(context.Background(), stringPtr("/home/alice/teams registry.json")); err != nil {
		t.Fatalf("auto ensure with fallback marker should skip retry, got: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("auto ensure should not retry failed Scheduled Task after fallback marker: %#v", runner.calls)
	}
}

func TestTeamsBackgroundKeepaliveWSLStatusReportsStartupFallbackCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		errs: []error{errTeamsKeepaliveScheduledTaskFailureForTest{}},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startupFallbackMarkerPath error: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker dir: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("fallback"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"status"})
	var out strings.Builder
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("service status with fallback marker error: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "Startup watchdog fallback: installed") || !strings.Contains(got, markerPath) {
		t.Fatalf("service status did not report fallback marker:\n%s", got)
	}
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackServiceActionsAvoidScheduledTaskCI(t *testing.T) {
	lockCLITestHooks(t)

	prevDelay := teamsServiceStartupFallbackRestartDelay
	teamsServiceStartupFallbackRestartDelay = 0
	t.Cleanup(func() { teamsServiceStartupFallbackRestartDelay = prevDelay })

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startupFallbackMarkerPath error: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker dir: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("Fallback=Windows Startup watchdog\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	stopPath := teamsServiceWSLStartupFallbackStopPath(markerPath)
	suffix, ok := teamsServiceWSLStartupFallbackSuffixFromMarkerPath(markerPath)
	if !ok {
		t.Fatalf("could not resolve startup fallback suffix from %s", markerPath)
	}

	for _, action := range []string{"stop", "start", "restart"} {
		runner.calls = nil
		if _, err := backend.Run(context.Background(), action); err != nil {
			t.Fatalf("fallback service %s error: %v", action, err)
		}
		if len(runner.calls) == 0 {
			t.Fatalf("fallback service %s made no PowerShell calls", action)
		}
		assertTeamsServiceCallsDoNotContain(t, runner.calls, "Get-ScheduledTask", "Enable-ScheduledTask", "Start-ScheduledTask", "Stop-ScheduledTask")
		joined := strings.Join(runner.calls[len(runner.calls)-1].args, " ")
		switch action {
		case "stop":
			if !strings.Contains(joined, "Set-Content -LiteralPath $stopPath") || !strings.Contains(joined, suffix) || !strings.Contains(joined, ".signal") {
				t.Fatalf("fallback stop should write the Startup watchdog stop signal:\n%s", joined)
			}
			for _, want := range []string{
				"Get-CimInstance Win32_Process",
				"$proc.ProcessId -ne $PID",
				"Stop-Process -Id $proc.ProcessId -Force",
				"OrdinalIgnoreCase",
				"$scriptPrefix + $suffix + '.ps1",
				"$scriptPrefix + $suffix + '.vbs",
				"$scriptPrefix + $suffix + '.cmd",
			} {
				if !strings.Contains(joined, want) {
					t.Fatalf("fallback stop should retire stale watchdog wrapper processes; missing %q:\n%s", want, joined)
				}
			}
			if _, err := os.Stat(stopPath); err != nil {
				t.Fatalf("fallback stop should write Linux stop marker: %v", err)
			}
		case "start":
			if !strings.Contains(joined, "Start-Process -FilePath 'wscript.exe'") || !strings.Contains(joined, "codex-helper-teams-wsl-"+suffix+".vbs") {
				t.Fatalf("fallback start should launch existing hidden Startup watchdog:\n%s", joined)
			}
			if !strings.Contains(joined, "Get-CimInstance Win32_Process") || !strings.Contains(joined, "$proc.ProcessId -ne $PID") {
				t.Fatalf("fallback start should retire stale existing watchdog wrapper before relaunch:\n%s", joined)
			}
			if !strings.Contains(joined, "OrdinalIgnoreCase") {
				t.Fatalf("fallback start should match stale wrapper command lines case-insensitively:\n%s", joined)
			}
			if _, err := os.Stat(stopPath); !os.IsNotExist(err) {
				t.Fatalf("fallback start should clear Linux stop marker, stat err=%v", err)
			}
		case "restart":
			if len(runner.calls) != 2 {
				t.Fatalf("fallback restart calls = %#v, want stop signal then hidden launcher", runner.calls)
			}
			first := strings.Join(runner.calls[0].args, " ")
			second := strings.Join(runner.calls[1].args, " ")
			if !strings.Contains(first, "Set-Content -LiteralPath $stopPath") || !strings.Contains(first, "Get-CimInstance Win32_Process") || !strings.Contains(second, "Start-Process -FilePath 'wscript.exe'") {
				t.Fatalf("fallback restart should stop then start via Startup watchdog:\nfirst=%s\nsecond=%s", first, second)
			}
			if _, err := os.Stat(stopPath); !os.IsNotExist(err) {
				t.Fatalf("fallback restart should clear Linux stop marker before starting, stat err=%v", err)
			}
		}
	}
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackProcessCleanupPowerShellCI(t *testing.T) {
	command := "$scriptPath = 'C:\\Users\\Alice\\AppData\\Local\\codex-helper\\teams\\codex-helper-teams-wsl-AbC.ps1'; " +
		"$launcherPath = 'C:\\Users\\Alice\\AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\codex-helper-teams-wsl-AbC.vbs'; " +
		"$legacyCmdLauncherPath = 'C:\\Users\\Alice\\AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\codex-helper-teams-wsl-AbC.cmd'; " +
		teamsServiceWSLStopStartupFallbackProcessesPowerShell("$scriptPath", "$launcherPath", "$legacyCmdLauncherPath")
	for _, want := range []string{
		"Get-CimInstance Win32_Process",
		"$proc.ProcessId -ne $PID",
		"OrdinalIgnoreCase",
		"Stop-Process -Id $proc.ProcessId -Force",
		"catch { }",
		"$scriptPath",
		"$launcherPath",
		"$legacyCmdLauncherPath",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("startup fallback process cleanup PowerShell missing %q:\n%s", want, command)
		}
	}
	if strings.Contains(command, "$cmd.Contains(") {
		t.Fatalf("startup fallback cleanup should not use case-sensitive Contains matching:\n%s", command)
	}
}

type simulatedTeamsStartupFallbackProcess struct {
	pid         int
	commandLine string
	current     bool
	stopped     bool
}

func simulateTeamsStartupFallbackCleanup(processes []simulatedTeamsStartupFallbackProcess, needles ...string) []simulatedTeamsStartupFallbackProcess {
	out := append([]simulatedTeamsStartupFallbackProcess(nil), processes...)
	lowerNeedles := make([]string, 0, len(needles))
	for _, needle := range needles {
		needle = strings.TrimSpace(needle)
		if needle != "" {
			lowerNeedles = append(lowerNeedles, strings.ToLower(needle))
		}
	}
	for i := range out {
		if out[i].current || strings.TrimSpace(out[i].commandLine) == "" {
			continue
		}
		lowerCommand := strings.ToLower(out[i].commandLine)
		for _, needle := range lowerNeedles {
			if strings.Contains(lowerCommand, needle) {
				out[i].stopped = true
				break
			}
		}
	}
	return out
}

func simulatedStoppedStartupFallbackPIDs(processes []simulatedTeamsStartupFallbackProcess) []int {
	var pids []int
	for _, process := range processes {
		if process.stopped {
			pids = append(pids, process.pid)
		}
	}
	return pids
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackCleanupSimulationCI(t *testing.T) {
	scriptPath := `C:\Users\Alice\AppData\Local\codex-helper\teams\codex-helper-teams-wsl-AbC.ps1`
	launcherPath := `C:\Users\Alice\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup\codex-helper-teams-wsl-AbC.vbs`
	legacyCmdLauncherPath := `C:\Users\Alice\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup\codex-helper-teams-wsl-AbC.cmd`
	processes := []simulatedTeamsStartupFallbackProcess{
		{pid: 101, commandLine: `powershell.exe -NoProfile -File "` + strings.ToUpper(scriptPath) + `"`},
		{pid: 102, commandLine: `wscript.exe //B //Nologo "` + launcherPath + `"`},
		{pid: 103, commandLine: `cmd.exe /c "` + legacyCmdLauncherPath + `"`},
		{pid: 104, commandLine: `powershell.exe -NoProfile -File "C:\Temp\codex-helper-teams-wsl-AbC.ps1"`},
		{pid: 105, commandLine: `powershell.exe -Command cleanup "` + scriptPath + `"`, current: true},
		{pid: 106},
	}

	cleaned := simulateTeamsStartupFallbackCleanup(processes, scriptPath, launcherPath, legacyCmdLauncherPath)
	if got, want := simulatedStoppedStartupFallbackPIDs(cleaned), []int{101, 102, 103}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stopped pids = %#v, want %#v; processes=%#v", got, want, cleaned)
	}
}

type simulatedTeamsStartupFallbackSupervisor struct {
	wrapperAlive bool
	childAlive   bool
	listenerHeld bool
	starts       int
	cleanups     int
}

func (s *simulatedTeamsStartupFallbackSupervisor) cleanupStaleWrapper() {
	s.cleanups++
	s.wrapperAlive = false
}

func (s *simulatedTeamsStartupFallbackSupervisor) startAfterCleanup() string {
	s.cleanupStaleWrapper()
	if s.listenerHeld {
		return "listener-held-by-existing-process"
	}
	s.wrapperAlive = true
	s.childAlive = true
	s.listenerHeld = true
	s.starts++
	return ""
}

func (s *simulatedTeamsStartupFallbackSupervisor) childExit() {
	s.childAlive = false
	s.listenerHeld = false
}

func (s simulatedTeamsStartupFallbackSupervisor) health() string {
	switch {
	case s.wrapperAlive && s.childAlive && s.listenerHeld:
		return "healthy"
	case !s.wrapperAlive && s.childAlive && s.listenerHeld:
		return "orphan-listener"
	case s.wrapperAlive && !s.listenerHeld:
		return "wrapper-without-listener"
	default:
		return "stopped"
	}
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackOrphanChildSimulationCI(t *testing.T) {
	supervisor := simulatedTeamsStartupFallbackSupervisor{
		wrapperAlive: true,
		childAlive:   true,
		listenerHeld: true,
	}
	supervisor.cleanupStaleWrapper()
	if got := supervisor.health(); got != "orphan-listener" {
		t.Fatalf("health after wrapper cleanup = %q, want orphan-listener", got)
	}
	if got := supervisor.startAfterCleanup(); got != "listener-held-by-existing-process" {
		t.Fatalf("start while orphan child holds listener = %q, want listener-held-by-existing-process", got)
	}
	if supervisor.starts != 0 {
		t.Fatalf("start count while listener is occupied = %d, want 0", supervisor.starts)
	}

	supervisor.childExit()
	if got := supervisor.health(); got != "stopped" {
		t.Fatalf("health after orphan child exits = %q, want stopped", got)
	}
	if got := supervisor.startAfterCleanup(); got != "" {
		t.Fatalf("start after orphan child exits error = %q, want success", got)
	}
	if got := supervisor.health(); got != "healthy" {
		t.Fatalf("health after clean restart = %q, want healthy", got)
	}
	if supervisor.starts != 1 || supervisor.cleanups != 3 {
		t.Fatalf("supervisor counters = starts:%d cleanups:%d, want starts:1 cleanups:3", supervisor.starts, supervisor.cleanups)
	}
}

type simulatedTeamsServiceHealthInput struct {
	windowsTaskState       string
	startupFallbackRunning bool
	listenerAlive          bool
	ownerPresent           bool
	ownerHeartbeatAge      time.Duration
	lastPollAge            time.Duration
	queuedOutbox           int
}

func simulatedTeamsServiceHealth(input simulatedTeamsServiceHealthInput, ownerStaleAfter time.Duration, pollStaleAfter time.Duration) string {
	supervisorRunning := strings.EqualFold(input.windowsTaskState, "running") || input.startupFallbackRunning
	if !input.listenerAlive {
		if input.queuedOutbox > 0 {
			return "upgrade-blocked-listener-stopped"
		}
		if supervisorRunning {
			return "supervisor-running-listener-stopped"
		}
		return "listener-stopped"
	}
	if !input.ownerPresent {
		return "owner-missing"
	}
	if ownerStaleAfter > 0 && input.ownerHeartbeatAge > ownerStaleAfter {
		return "owner-stale"
	}
	if pollStaleAfter > 0 && input.lastPollAge > pollStaleAfter {
		return "poll-stale"
	}
	if !supervisorRunning {
		return "listener-unsupervised"
	}
	if strings.EqualFold(input.windowsTaskState, "disabled") && input.startupFallbackRunning {
		return "healthy-fallback"
	}
	return "healthy"
}

func TestTeamsBackgroundKeepaliveSimulatedStatusRequiresListenerOwnerAndPollFreshCI(t *testing.T) {
	const ownerStaleAfter = time.Minute
	const pollStaleAfter = 2 * time.Minute
	tests := []struct {
		name  string
		input simulatedTeamsServiceHealthInput
		want  string
	}{
		{
			name: "disabled task can be healthy when startup fallback owns a fresh listener",
			input: simulatedTeamsServiceHealthInput{
				windowsTaskState:       "Disabled",
				startupFallbackRunning: true,
				listenerAlive:          true,
				ownerPresent:           true,
				ownerHeartbeatAge:      10 * time.Second,
				lastPollAge:            20 * time.Second,
			},
			want: "healthy-fallback",
		},
		{
			name: "running scheduler task alone is not healthy",
			input: simulatedTeamsServiceHealthInput{
				windowsTaskState: "Running",
				listenerAlive:    false,
				ownerPresent:     true,
			},
			want: "supervisor-running-listener-stopped",
		},
		{
			name: "queued outbox without listener remains an upgrade blocker",
			input: simulatedTeamsServiceHealthInput{
				windowsTaskState: "Disabled",
				listenerAlive:    false,
				queuedOutbox:     3,
			},
			want: "upgrade-blocked-listener-stopped",
		},
		{
			name: "listener with stale owner heartbeat is unhealthy",
			input: simulatedTeamsServiceHealthInput{
				windowsTaskState:       "Disabled",
				startupFallbackRunning: true,
				listenerAlive:          true,
				ownerPresent:           true,
				ownerHeartbeatAge:      90 * time.Second,
				lastPollAge:            20 * time.Second,
			},
			want: "owner-stale",
		},
		{
			name: "listener with stale poll is unhealthy",
			input: simulatedTeamsServiceHealthInput{
				windowsTaskState:       "Disabled",
				startupFallbackRunning: true,
				listenerAlive:          true,
				ownerPresent:           true,
				ownerHeartbeatAge:      10 * time.Second,
				lastPollAge:            5 * time.Minute,
			},
			want: "poll-stale",
		},
		{
			name: "listener without any supervisor is a restart risk",
			input: simulatedTeamsServiceHealthInput{
				listenerAlive:     true,
				ownerPresent:      true,
				ownerHeartbeatAge: 10 * time.Second,
				lastPollAge:       10 * time.Second,
			},
			want: "listener-unsupervised",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := simulatedTeamsServiceHealth(tc.input, ownerStaleAfter, pollStaleAfter); got != tc.want {
				t.Fatalf("health = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackProcessCleanupNativeWindowsCI(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("native PowerShell process cleanup is Windows-only")
	}
	powershellExe, err := exec.LookPath("powershell.exe")
	if err != nil {
		t.Fatalf("powershell.exe not found: %v", err)
	}

	tmp := t.TempDir()
	scriptPath := filepath.Join(tmp, "codex-helper-teams-wsl-CiMixed.ps1")
	launcherPath := filepath.Join(tmp, "codex-helper-teams-wsl-CiMixed.vbs")
	legacyCmdLauncherPath := filepath.Join(tmp, "codex-helper-teams-wsl-CiMixed.cmd")
	for _, path := range []string{scriptPath, launcherPath, legacyCmdLauncherPath} {
		if err := os.WriteFile(path, []byte("placeholder"), 0o600); err != nil {
			t.Fatalf("write placeholder %s: %v", path, err)
		}
	}

	child := exec.Command(powershellExe,
		"-NoProfile",
		"-NonInteractive",
		"-ExecutionPolicy",
		"Bypass",
		"-Command",
		"$watchdogScript = "+powershellSingleQuote(scriptPath)+"; Start-Sleep -Seconds 300",
	)
	if err := child.Start(); err != nil {
		t.Fatalf("start stale watchdog stand-in: %v", err)
	}
	exited := make(chan struct{})
	go func() {
		_ = child.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		select {
		case <-exited:
		default:
			_ = child.Process.Kill()
			<-exited
		}
	})

	waitForWindowsProcessCommandLineContains(t, powershellExe, child.Process.Pid, scriptPath)

	cleanupCommand := "$scriptPath = " + powershellSingleQuote(strings.ToUpper(scriptPath)) + "; " +
		"$launcherPath = " + powershellSingleQuote(strings.ToUpper(launcherPath)) + "; " +
		"$legacyCmdLauncherPath = " + powershellSingleQuote(strings.ToUpper(legacyCmdLauncherPath)) + "; " +
		teamsServiceWSLStopStartupFallbackProcessesPowerShell("$scriptPath", "$launcherPath", "$legacyCmdLauncherPath")
	out, err := exec.Command(powershellExe,
		"-NoProfile",
		"-NonInteractive",
		"-ExecutionPolicy",
		"Bypass",
		"-Command",
		cleanupCommand,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("cleanup stale watchdog stand-in failed: %v\n%s", err, out)
	}

	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatalf("stale watchdog stand-in pid %d was not stopped by cleanup PowerShell", child.Process.Pid)
	}
}

func waitForWindowsProcessCommandLineContains(t *testing.T, powershellExe string, pid int, needle string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastOut []byte
	for time.Now().Before(deadline) {
		query := "$p = Get-CimInstance Win32_Process -Filter " + powershellSingleQuote("ProcessId = "+strconv.Itoa(pid)) + "; " +
			"if ($null -eq $p) { exit 2 }; " +
			"$cmd = [string]$p.CommandLine; " +
			"if ($cmd.IndexOf(" + powershellSingleQuote(needle) + ", [System.StringComparison]::OrdinalIgnoreCase) -ge 0) { exit 0 }; " +
			"Write-Output $cmd; exit 3"
		out, err := exec.Command(powershellExe,
			"-NoProfile",
			"-NonInteractive",
			"-ExecutionPolicy",
			"Bypass",
			"-Command",
			query,
		).CombinedOutput()
		if err == nil {
			return
		}
		lastOut = out
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("process %d command line did not contain %q; last output:\n%s", pid, needle, lastOut)
}

func TestTeamsBackgroundKeepaliveWSLStartupFallbackStartRewritesLegacyConfigCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startupFallbackMarkerPath error: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker dir: %v", err)
	}
	legacyArgs := []string{
		"-d", "Ubuntu",
		"-u", "alice",
		"--exec",
		"env",
		"CODEX_HELPER_TEAMS_STARTUP_FALLBACK=1",
		"CODEX_HELPER_TEAMS_EXIT_ON_STANDBY=1",
		"/home/alice/bin/codex-proxy",
		"teams",
		"run",
		"--auto-service=false",
	}
	if err := os.WriteFile(markerPath, []byte(buildTeamsServiceWSLStartupFallbackConfig(backend.Name(), legacyArgs)), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	if _, err := backend.Run(context.Background(), "start"); err != nil {
		t.Fatalf("fallback start error: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("fallback start calls = %#v, want one PowerShell call", runner.calls)
	}
	command := strings.Join(runner.calls[0].args, " ")
	for _, want := range []string{
		"Set-Content -LiteralPath $scriptPath",
		"Start-Process -FilePath 'wscript.exe'",
		"$deadline = (Get-Date).AddSeconds(60)",
		"CODEX_HELPER_TEAMS_STARTUP_FALLBACK=1",
		"/home/alice/bin/codex-proxy",
		"Get-CimInstance Win32_Process",
		"OrdinalIgnoreCase",
		"Stop-Process -Id $proc.ProcessId -Force",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("fallback start rewrite command missing %q:\n%s", want, command)
		}
	}
	if strings.Contains(command, "CODEX_HELPER_TEAMS_EXIT_ON_STANDBY=1") {
		t.Fatalf("fallback start should strip legacy standby-exit env from rewritten watchdog:\n%s", command)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Enable-ScheduledTask", "Start-ScheduledTask", "Stop-ScheduledTask")
}

func TestTeamsBackgroundKeepaliveWindowsCommandLineRoundTripCI(t *testing.T) {
	args := []string{
		"-d",
		"Ubuntu",
		"--cd",
		"/home/alice/work dir",
		"--exec",
		"env",
		`NO_PROXY=*.example.com`,
		`QUOTED=a "quote" and slash\`,
		"",
		"/home/alice/bin/codex-proxy",
	}
	line := windowsCommandLine(args)
	got, err := splitWindowsCommandLine(line)
	if err != nil {
		t.Fatalf("splitWindowsCommandLine error: %v", err)
	}
	if !reflect.DeepEqual(got, args) {
		t.Fatalf("splitWindowsCommandLine = %#v, want %#v\nline=%s", got, args, line)
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
		{name: "windows task", goos: "windows", wantName: "powershell.exe", wantSnippet: "Enable-ScheduledTask -TaskName '" + teamsServiceWindowsTaskName + "'"},
		{name: "wsl windows task", goos: "linux", isWSL: true, powerShell: "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe", wantName: "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe", wantSnippet: "Enable-ScheduledTask -TaskName $taskName"},
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
			name, args, err := delayedTeamsServiceStartCommand(backend, "")
			if err != nil {
				t.Fatalf("delayedTeamsServiceStartCommand error: %v", err)
			}
			if name != tc.wantName {
				t.Fatalf("restart command name = %q, want %q", name, tc.wantName)
			}
			joined := strings.Join(args, " ")
			if !strings.Contains(joined, tc.wantSnippet) {
				t.Fatalf("restart command args missing %q:\n%#v", tc.wantSnippet, args)
			}
			if tc.isWSL && !strings.Contains(joined, "Codex Helper Teams Watchdog (WSL Ubuntu alice default ") {
				t.Fatalf("WSL delayed restart should start watchdog too:\n%s", joined)
			}
			if tc.isWSL && (!strings.Contains(joined, "$task.State -eq 'Disabled'") || !strings.Contains(joined, "Start-CodexHelperScheduledTaskIfStopped $taskName")) {
				t.Fatalf("WSL delayed restart should enable disabled watchdog tasks and delegate running-state refresh to the guarded starter:\n%s", joined)
			}
			if tc.goos == "windows" && !strings.Contains(joined, "Start-CodexHelperScheduledTaskIfStopped "+powershellSingleQuote(teamsServiceWindowsTaskName)) {
				t.Fatalf("Windows delayed restart should use the guarded scheduled task starter:\n%s", joined)
			}
		})
	}
	t.Run("linux local supervisor", func(t *testing.T) {
		tmp := t.TempDir()
		isolateTeamsUserDirsForTest(t, tmp)
		exe := filepath.Join(tmp, "codex-proxy")
		withTeamsServiceTestHooks(t, teamsServiceTestHooks{
			goos: "linux",
			exe:  exe,
			cwd:  tmp,
		})
		t.Setenv(envTeamsLinuxServiceBackend, "local-supervisor")
		configPath, err := teamsServiceLocalSupervisorConfigPath()
		if err != nil {
			t.Fatalf("local supervisor config path: %v", err)
		}
		if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
			Version: teamsServiceLocalSupervisorConfigVersion,
			Enabled: true,
			Spec: teamsServiceSpec{
				Executable: exe,
				WorkingDir: tmp,
			},
		}); err != nil {
			t.Fatalf("write local supervisor config: %v", err)
		}
		backend, err := teamsServiceBackendForCurrentPlatform()
		if err != nil {
			t.Fatalf("backend error: %v", err)
		}
		name, args, err := delayedTeamsServiceStartCommand(backend, "")
		if err != nil {
			t.Fatalf("delayedTeamsServiceStartCommand error: %v", err)
		}
		joined := strings.Join(args, " ")
		if name != "sh" ||
			!strings.Contains(joined, shellQuote(exe)+" teams service start") ||
			!strings.Contains(joined, envTeamsLinuxServiceBackend+"=local-supervisor") ||
			strings.Contains(joined, "systemctl") {
			t.Fatalf("restart command = %q %#v, want local-supervisor start", name, args)
		}
	})
	t.Run("windows waits for pending replacement file", func(t *testing.T) {
		tmp := t.TempDir()
		withTeamsServiceTestHooks(t, teamsServiceTestHooks{
			goos:           "windows",
			exe:            filepath.Join(tmp, "codex-proxy.exe"),
			cwd:            tmp,
			windowsTaskDir: filepath.Join(tmp, "windows-task"),
			runner:         &recordingTeamsServiceRunner{},
		})
		backend, err := teamsServiceBackendForCurrentPlatform()
		if err != nil {
			t.Fatalf("backend error: %v", err)
		}
		pending := filepath.Join(tmp, ".codex-proxy_1.2.3_windows_amd64.exe.tmp")
		name, args, err := delayedTeamsServiceStartCommand(backend, pending)
		if err != nil {
			t.Fatalf("delayedTeamsServiceStartCommand error: %v", err)
		}
		if name != "powershell.exe" {
			t.Fatalf("restart command name = %q, want powershell.exe", name)
		}
		joined := strings.Join(args, " ")
		if !strings.Contains(joined, "Test-Path -LiteralPath $pendingReplace") || !strings.Contains(joined, powershellSingleQuote(pending)) {
			t.Fatalf("windows pending restart command must wait for staged replacement:\n%s", joined)
		}
	})
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
		if len(runner.calls) == 0 {
			t.Fatalf("service %s made no systemctl calls", action)
		}
		for _, call := range runner.calls {
			if call.name != "systemctl" || len(call.args) == 0 || call.args[0] != "--user" {
				t.Fatalf("service %s calls = %#v, want systemctl --user calls", action, runner.calls)
			}
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

func TestTeamsBackgroundKeepaliveWindowsTaskStartRestartEnablesDisabledTasksCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "windows-task"),
		runner:         runner,
	})
	backend := teamsServiceWindowsTaskBackend{}
	requireNativeBridgeCleanup := func(t *testing.T, action string, joined string) {
		t.Helper()
		for _, want := range []string{
			"Get-CodexHelperBridgeTaskIdentity",
			"$expectedBridgeExeForCleanup",
			"$expectedBridgeArgsForCleanup",
			"Test-CodexHelperTeamsBridgeCommand",
			"$proc.ProcessId -ne $PID",
			"[string]$proc.ExecutablePath",
			"--once",
			"Stop-Process -Id $proc.ProcessId -Force",
			"if (-not [string]::IsNullOrWhiteSpace($expectedBridgeExeForCleanup) -and -not [string]::IsNullOrWhiteSpace($expectedBridgeArgsForCleanup))",
		} {
			if !strings.Contains(joined, want) {
				t.Fatalf("%s should include native bridge cleanup fragment %q:\n%s", action, want, joined)
			}
		}
		if strings.Contains(joined, "$name -ieq 'codex-proxy.exe'") || strings.Contains(joined, "$name -ieq 'cxp.exe'") {
			t.Fatalf("%s should fail closed instead of falling back to process-name-only cleanup:\n%s", action, joined)
		}
	}
	for _, action := range []string{"start", "restart"} {
		t.Run(action, func(t *testing.T) {
			runner.calls = nil
			if _, err := backend.Run(context.Background(), action); err != nil {
				t.Fatalf("Run(%s) error: %v", action, err)
			}
			if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
				t.Fatalf("Run(%s) calls = %#v, want powershell.exe", action, runner.calls)
			}
			joined := strings.Join(runner.calls[0].args, " ")
			if action == "start" {
				requireSubstringsInOrder(t, joined,
					"function Start-CodexHelperScheduledTaskIfStopped",
					"Enable-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
					"$watchdogTask = Get-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)+" -ErrorAction SilentlyContinue",
					"Enable-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName),
					"Start-CodexHelperScheduledTaskIfStopped "+powershellSingleQuote(teamsServiceWindowsTaskName),
					"Start-CodexHelperScheduledTaskIfStopped "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName),
				)
				if !strings.Contains(joined, "if ($null -ne $watchdogTask) { Enable-ScheduledTask") ||
					!strings.Contains(joined, "if ($null -ne $watchdogTask) { Start-CodexHelperScheduledTaskIfStopped") {
					t.Fatalf("Run(%s) should treat the watchdog task as optional for old installs:\n%s", action, joined)
				}
			} else {
				requireSubstringsInOrder(t, joined,
					"function Start-CodexHelperScheduledTaskIfStopped",
					"function Wait-CodexHelperScheduledTaskStopped",
					"Stop-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
					"Enable-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
					"Wait-CodexHelperScheduledTaskStopped "+powershellSingleQuote(teamsServiceWindowsTaskName)+" 20",
					"$watchdogTask = Get-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)+" -ErrorAction SilentlyContinue",
					"Enable-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName),
					"Start-CodexHelperScheduledTaskIfStopped "+powershellSingleQuote(teamsServiceWindowsTaskName),
					"Start-CodexHelperScheduledTaskIfStopped "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName),
				)
				if !strings.Contains(joined, "if ($null -ne $watchdogTask) { Enable-ScheduledTask") ||
					!strings.Contains(joined, "if ($null -ne $watchdogTask) { Start-CodexHelperScheduledTaskIfStopped") {
					t.Fatalf("Run(%s) should treat the watchdog task as optional for old installs:\n%s", action, joined)
				}
			}
			requireNativeBridgeCleanup(t, "Run("+action+")", joined)
			bridgeStart := "Start-CodexHelperScheduledTaskIfStopped " + powershellSingleQuote(teamsServiceWindowsTaskName)
			if strings.Index(joined, "$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity") > strings.Index(joined, bridgeStart) {
				t.Fatalf("Run(%s) should clean stale bridge child processes before starting:\n%s", action, joined)
			}
		})
	}
	t.Run("stop", func(t *testing.T) {
		runner.calls = nil
		if _, err := backend.Run(context.Background(), "stop"); err != nil {
			t.Fatalf("Run(stop) error: %v", err)
		}
		if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
			t.Fatalf("Run(stop) calls = %#v, want powershell.exe", runner.calls)
		}
		joined := strings.Join(runner.calls[0].args, " ")
		requireSubstringsInOrder(t, joined,
			"Stop-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName),
			"Stop-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
			"$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity",
		)
		requireNativeBridgeCleanup(t, "Run(stop)", joined)
	})
	for _, action := range []string{"start", "restart"} {
		t.Run("primary_"+action, func(t *testing.T) {
			runner.calls = nil
			if _, err := backend.RunPrimary(context.Background(), action); err != nil {
				t.Fatalf("RunPrimary(%s) error: %v", action, err)
			}
			if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
				t.Fatalf("RunPrimary(%s) calls = %#v, want powershell.exe", action, runner.calls)
			}
			joined := strings.Join(runner.calls[0].args, " ")
			bridgeStart := "Start-CodexHelperScheduledTaskIfStopped " + powershellSingleQuote(teamsServiceWindowsTaskName)
			if action == "restart" {
				requireSubstringsInOrder(t, joined,
					"function Start-CodexHelperScheduledTaskIfStopped",
					"function Wait-CodexHelperScheduledTaskStopped",
					"Stop-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
					"Enable-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
					"Wait-CodexHelperScheduledTaskStopped "+powershellSingleQuote(teamsServiceWindowsTaskName)+" 20",
					bridgeStart,
				)
			} else {
				requireSubstringsInOrder(t, joined,
					"Enable-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
					bridgeStart,
				)
			}
			if strings.Contains(joined, teamsServiceWindowsWatchdogTaskName) {
				t.Fatalf("RunPrimary(%s) should only target the bridge task:\n%s", action, joined)
			}
			requireNativeBridgeCleanup(t, "RunPrimary("+action+")", joined)
			if strings.Index(joined, "$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity") > strings.Index(joined, bridgeStart) {
				t.Fatalf("RunPrimary(%s) should clean stale bridge child processes before starting:\n%s", action, joined)
			}
		})
	}
	t.Run("primary_stop", func(t *testing.T) {
		runner.calls = nil
		if _, err := backend.RunPrimary(context.Background(), "stop"); err != nil {
			t.Fatalf("RunPrimary(stop) error: %v", err)
		}
		if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
			t.Fatalf("RunPrimary(stop) calls = %#v, want powershell.exe", runner.calls)
		}
		joined := strings.Join(runner.calls[0].args, " ")
		if strings.Contains(joined, teamsServiceWindowsWatchdogTaskName) {
			t.Fatalf("RunPrimary(stop) should only target the bridge task:\n%s", joined)
		}
		requireSubstringsInOrder(t, joined,
			"Stop-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
			"$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity",
		)
		requireNativeBridgeCleanup(t, "RunPrimary(stop)", joined)
	})
}

func TestTeamsBackgroundKeepaliveWindowsTaskOptionalWatchdogActionsCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "windows-task"),
		runner:         runner,
	})
	backend := teamsServiceWindowsTaskBackend{}
	for _, action := range []string{"enable", "disable", "status"} {
		t.Run(action, func(t *testing.T) {
			runner.calls = nil
			if _, err := backend.Run(context.Background(), action); err != nil {
				t.Fatalf("Run(%s) error: %v", action, err)
			}
			if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
				t.Fatalf("Run(%s) calls = %#v, want powershell.exe", action, runner.calls)
			}
			joined := strings.Join(runner.calls[0].args, " ")
			if !strings.Contains(joined, "$watchdogTask = Get-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName)+" -ErrorAction SilentlyContinue") ||
				!strings.Contains(joined, "if ($null -ne $watchdogTask)") {
				t.Fatalf("Run(%s) should treat the watchdog task as optional for old installs:\n%s", action, joined)
			}
		})
	}
}

func TestTeamsBackgroundKeepaliveWSLServiceActionsCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "windows-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	tests := []struct {
		action string
		want   string
	}{
		{action: "enable", want: "Enable-ScheduledTask -TaskName $taskName"},
		{action: "status", want: "ResolvedLegacyTaskName"},
		{action: "start", want: "Start-CodexHelperScheduledTaskIfStopped $taskName"},
		{action: "stop", want: "Stop-ScheduledTask -TaskName $taskName"},
		{action: "restart", want: "Stop-ScheduledTask -TaskName $taskName"},
		{action: "disable", want: "Disable-ScheduledTask -TaskName $taskName"},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			runner.calls = nil
			cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
			cmd.SetArgs([]string{tt.action})
			if err := cmd.Execute(); err != nil {
				t.Fatalf("service %s error: %v", tt.action, err)
			}
			wantCalls := 1
			if tt.action == "start" || tt.action == "restart" {
				wantCalls = 2
			}
			if len(runner.calls) != wantCalls || runner.calls[0].name != "powershell.exe" {
				t.Fatalf("service %s calls = %#v, want %d powershell.exe call(s)", tt.action, runner.calls, wantCalls)
			}
			joined := strings.Join(runner.calls[0].args, " ")
			allJoined := teamsServiceJoinedCalls(runner.calls)
			commandForAction := joined
			if tt.action == "start" || tt.action == "restart" {
				commandForAction = allJoined
			}
			if !strings.Contains(commandForAction, tt.want) || !strings.Contains(commandForAction, "$legacyPrefix") {
				t.Fatalf("service %s command missing %q:\n%s", tt.action, tt.want, commandForAction)
			} else if tt.action == "restart" {
				requireSubstringsInOrder(t, allJoined,
					"Stop-ScheduledTask -TaskName $taskName",
					"-ErrorAction SilentlyContinue",
					"Enable-ScheduledTask -TaskName $taskName",
					"Start-CodexHelperScheduledTaskIfStopped $taskName",
					"Teams WSL Scheduled Task did not stay running after start",
				)
				if strings.Count(allJoined, "Teams WSL Scheduled Task did not stay running after start") != 2 {
					t.Fatalf("service restart should verify both bridge and watchdog tasks:\n%s", allJoined)
				}
				if !strings.Contains(allJoined, "if ($null -ne $task) { if ($task.State -eq 'Disabled') { Enable-ScheduledTask -TaskName $taskName") ||
					!strings.Contains(allJoined, "if ($task.State -ne 'Running') { Start-CodexHelperScheduledTaskIfStopped $taskName") {
					t.Fatalf("service restart should enable disabled watchdog tasks without re-starting an already running watchdog task:\n%s", allJoined)
				}
			} else if tt.action == "start" {
				requireSubstringsInOrder(t, allJoined,
					"if ($task.State -ne 'Running') { exit 3 }",
					"Enable-ScheduledTask -TaskName $taskName",
					"Start-CodexHelperScheduledTaskIfStopped $taskName",
					"Start-Sleep -Seconds 2",
					"Teams WSL Scheduled Task did not stay running after start",
				)
				if strings.Count(allJoined, "Teams WSL Scheduled Task did not stay running after start") != 2 {
					t.Fatalf("service start should verify both bridge and watchdog tasks:\n%s", allJoined)
				}
				if !strings.Contains(allJoined, "if ($null -ne $task) { if ($task.State -eq 'Disabled') { Enable-ScheduledTask -TaskName $taskName") ||
					!strings.Contains(allJoined, "if ($task.State -ne 'Running') { Start-CodexHelperScheduledTaskIfStopped $taskName") {
					t.Fatalf("service start should enable disabled watchdog tasks without re-starting an already running watchdog task:\n%s", allJoined)
				}
			}
		})
	}
}

func TestTeamsBackgroundKeepaliveWSLStopSkipsMissingWatchdogTaskCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "windows-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	backend := teamsServiceWSLWindowsTaskBackend{}
	if _, err := backend.Run(context.Background(), "stop"); err != nil {
		t.Fatalf("WSL stop error: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("WSL stop calls = %#v, want one PowerShell call", runner.calls)
	}
	command := strings.Join(runner.calls[0].args, " ")
	watchdogIdx := strings.Index(command, "Codex Helper Teams Watchdog")
	bridgeIdx := strings.Index(command, "Codex Helper Teams Bridge")
	if watchdogIdx < 0 || bridgeIdx < 0 || bridgeIdx <= watchdogIdx {
		t.Fatalf("WSL stop should resolve optional watchdog before required bridge:\n%s", command)
	}
	watchdogSegment := command[watchdogIdx:bridgeIdx]
	if strings.Contains(watchdogSegment, "Teams WSL Scheduled Task not found") {
		t.Fatalf("missing WSL watchdog task must not fail stop:\n%s", command)
	}
	if !strings.Contains(watchdogSegment, "if ($null -ne $task) { Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue }") {
		t.Fatalf("WSL stop should stop watchdog only when it exists:\n%s", command)
	}
	if !strings.Contains(command[bridgeIdx:], "Teams WSL Scheduled Task not found") {
		t.Fatalf("WSL stop should still require the primary bridge task:\n%s", command)
	}
}

func TestTeamsBackgroundKeepaliveWSLPrimaryStartRestartEnablesDisabledTasksCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "windows-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	backend := teamsServiceWSLWindowsTaskBackend{}
	for _, action := range []string{"start", "restart"} {
		t.Run(action, func(t *testing.T) {
			runner.calls = nil
			if _, err := backend.RunPrimary(context.Background(), action); err != nil {
				t.Fatalf("RunPrimary(%s) error: %v", action, err)
			}
			if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
				t.Fatalf("RunPrimary(%s) calls = %#v, want one powershell.exe call", action, runner.calls)
			}
			joined := strings.Join(runner.calls[0].args, " ")
			requireSubstringsInOrder(t, joined,
				"$legacyPrefix",
				"Enable-ScheduledTask -TaskName $taskName",
				"Start-CodexHelperScheduledTaskIfStopped $taskName",
				"Teams WSL Scheduled Task did not stay running after start",
			)
			if strings.Contains(joined, "Codex Helper Teams Watchdog") {
				t.Fatalf("RunPrimary(%s) should only target the bridge task:\n%s", action, joined)
			}
		})
	}
	t.Run("stop", func(t *testing.T) {
		runner.calls = nil
		if _, err := backend.RunPrimary(context.Background(), "stop"); err != nil {
			t.Fatalf("RunPrimary(stop) error: %v", err)
		}
		if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
			t.Fatalf("RunPrimary(stop) calls = %#v, want one powershell.exe call", runner.calls)
		}
		joined := strings.Join(runner.calls[0].args, " ")
		if !strings.Contains(joined, "Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue") {
			t.Fatalf("RunPrimary(stop) should tolerate an already stopped bridge task:\n%s", joined)
		}
		if strings.Contains(joined, "Codex Helper Teams Watchdog") {
			t.Fatalf("RunPrimary(stop) should only target the bridge task:\n%s", joined)
		}
	})
}

func TestTeamsBackgroundKeepaliveWSLInstalledActiveProbeUsesTaskSchedulerCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "windows-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	installed, err := teamsServiceInstalled()
	if err != nil {
		t.Fatalf("teamsServiceInstalled error: %v", err)
	}
	if !installed {
		t.Fatal("teamsServiceInstalled = false, want true from mocked Task Scheduler")
	}
	active, err := teamsServiceActive(context.Background())
	if err != nil {
		t.Fatalf("teamsServiceActive error: %v", err)
	}
	if !active {
		t.Fatal("teamsServiceActive = false, want true from mocked Task Scheduler")
	}
	if len(runner.calls) != 2 {
		t.Fatalf("WSL probe calls = %#v, want installed and active checks", runner.calls)
	}
	for i, call := range runner.calls {
		joined := strings.Join(call.args, " ")
		if !strings.Contains(joined, "Get-ScheduledTask -TaskName $taskName") || !strings.Contains(joined, "$legacyPrefix") {
			t.Fatalf("WSL probe command missing task lookup:\n%s", joined)
		}
		if strings.Contains(joined, "Register-ScheduledTask") || strings.Contains(joined, "systemctl") {
			t.Fatalf("WSL probe command should be read-only Task Scheduler lookup:\n%s", joined)
		}
		if i == 1 {
			requireSubstringsInOrder(t, joined,
				"Get-ScheduledTask -TaskName",
				"$task.State -ne 'Running'",
				"exit 3",
			)
		}
	}

	runner.calls = nil
	runner.err = errTeamsKeepaliveScheduledTaskFailureForTest{}
	active, err = teamsServiceActive(context.Background())
	if err != nil {
		t.Fatalf("teamsServiceActive stopped-task error: %v", err)
	}
	if active {
		t.Fatal("teamsServiceActive = true when Task Scheduler reports non-running task")
	}
}

func TestTeamsBackgroundKeepaliveWSLTaskSchedulerRealWindowsRoundTripCI(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("real Windows Scheduled Task roundtrip only runs on Windows CI")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex-home"))
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "ci-"+safeWindowsTaskNamePart(filepath.Base(tmp), 16))
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "codex-proxy"),
		cwd:                  filepath.Join(tmp, "work"),
		windowsTaskDir:       filepath.Join(tmp, "wsl-task"),
		isWSL:                true,
		wslDistro:            "CodexHelperCI",
		wslLinuxUser:         "runner",
		powerShellExecutable: "powershell.exe",
		runner:               teamsServiceExecRunner{},
	})

	backend := teamsServiceWSLWindowsTaskBackend{}
	spec := teamsServiceSpec{
		Executable:   "/home/runner/bin/codex-proxy",
		WorkingDir:   "/home/runner/work",
		RegistryPath: "/home/runner/.config/codex-helper/teams-registry.json",
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
			"CODEX_HOME":                      "/home/runner/.codex",
			"NO_COLOR":                        "1",
		},
	}
	if _, err := backend.Install(context.Background(), spec); err != nil {
		t.Fatalf("real WSL-shaped Scheduled Task install failed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = backend.Uninstall(context.Background())
	})

	inspect := "$task = Get-ScheduledTask -TaskName " + powershellSingleQuote(backend.Name()) + "; " +
		"$watchdog = Get-ScheduledTask -TaskName " + powershellSingleQuote(backend.watchdogName()) + "; " +
		"'bridge'; $task.State; $task.Actions | Format-List Execute,Arguments; $task.Triggers | ForEach-Object { $_.CimClass.CimClassName + ' ' + [string]$_.Repetition.Interval }; " +
		"'watchdog'; $watchdog.State; $watchdog.Actions | Format-List Execute,Arguments; $watchdog.Triggers | ForEach-Object { $_.CimClass.CimClassName + ' ' + [string]$_.Repetition.Interval }"
	data, err := teamsServiceRunPowerShell(context.Background(), inspect)
	if err != nil {
		t.Fatalf("inspect real WSL-shaped task: %v", err)
	}
	got := string(data)
	for _, want := range []string{
		"Disabled",
		"wscript.exe",
		"//B //Nologo",
		".vbs",
		"MSFT_TaskLogonTrigger",
		"bridge",
		"watchdog",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("real WSL-shaped task missing %q:\n%s", want, got)
		}
	}
	for _, forbidden := range []string{
		"powershell.exe",
		"PT1M",
		"MSFT_TaskTimeTrigger",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("real WSL-shaped task should not contain %q:\n%s", forbidden, got)
		}
	}
	if _, err := backend.Run(context.Background(), "enable"); err != nil {
		t.Fatalf("enable real WSL-shaped task: %v", err)
	}
	if _, err := backend.Run(context.Background(), "disable"); err != nil {
		t.Fatalf("disable real WSL-shaped task: %v", err)
	}
}

type errTeamsKeepaliveAuthMissingForTest struct{}

func (errTeamsKeepaliveAuthMissingForTest) Error() string { return "keepalive auth missing" }

type errTeamsKeepaliveAccessDeniedForTest struct{}

func (errTeamsKeepaliveAccessDeniedForTest) Error() string {
	return "Register-ScheduledTask : Access is denied. (Exception from HRESULT: 0x80070005 (E_ACCESSDENIED))"
}

type errTeamsKeepaliveScheduledTaskFailureForTest struct{}

func (errTeamsKeepaliveScheduledTaskFailureForTest) Error() string {
	return "Register-ScheduledTask failed: exit status 1"
}

type scriptedTeamsServiceRunner struct {
	calls   []teamsServiceCommandCall
	outputs [][]byte
	errs    []error
	onRun   func(index int, name string, args []string)
}

func (r *scriptedTeamsServiceRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	idx := len(r.calls)
	r.calls = append(r.calls, teamsServiceCommandCall{
		name: name,
		args: append([]string{}, args...),
	})
	if r.onRun != nil {
		r.onRun(idx, name, append([]string{}, args...))
	}
	var out []byte
	if idx < len(r.outputs) {
		out = append([]byte{}, r.outputs[idx]...)
	}
	var err error
	if idx < len(r.errs) {
		err = r.errs[idx]
	}
	return out, err
}
