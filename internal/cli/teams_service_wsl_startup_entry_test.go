package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// buildTeamsServiceWSLStartupFallbackConfig renders the marker an older
// looping Startup watchdog left behind; tests use it to set up legacy state.
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

func isolateTeamsServiceLocalSupervisorDirsForTest(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	// os.UserConfigDir uses HOME on macOS and APPDATA on Windows. XDG alone
	// does not isolate the supervisor config on those native runners.
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)
	t.Setenv("APPDATA", filepath.Join(tmp, "config"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "state"))
}

func withWSLStartupEntryTestHooks(t *testing.T, runner teamsServiceCommandRunner) {
	t.Helper()
	lockCLITestHooks(t)
	isolateTeamsServiceLocalSupervisorDirsForTest(t)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/cxp",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(t.TempDir(), "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
}

func wslStartupEntryTestSpec() teamsServiceSpec {
	return teamsServiceSpec{
		Executable: "/home/alice/bin/cxp",
		WorkingDir: "/home/alice/work dir",
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_SERVICE_MODE": "background",
			"CODEX_HOME":                      "/home/alice/.codex",
			"NO_COLOR":                        "1",
		},
	}
}

// The Windows Startup entry is one hidden wsl.exe run of
// `cxp teams service start`; it does not wait and does not loop.
func TestWSLStartupEntryIsOneShotServiceStartGolden(t *testing.T) {
	withWSLStartupEntryTestHooks(t, &recordingTeamsServiceRunner{})
	got := buildTeamsServiceWSLStartupKickVBS(buildTeamsServiceWSLStartupKickArguments(wslStartupEntryTestSpec()))
	want := `CreateObject("WScript.Shell").Run "wsl.exe -d Ubuntu -u alice --cd ""/home/alice/work dir"" --exec env ` +
		`CODEX_HELPER_TEAMS_STARTUP_KICK=1 CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND=local-supervisor CODEX_HOME=/home/alice/.codex NO_COLOR=1 ` +
		`/home/alice/bin/cxp teams service start", 0, False` + "\r\n"
	if got != want {
		t.Fatalf("Startup entry:\n got %q\nwant %q", got, want)
	}
}

func TestWSLStartupFallbackInstallsOneShotEntryForLocalSupervisor(t *testing.T) {
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withWSLStartupEntryTestHooks(t, runner)
	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := buildTeamsServiceWSLStartupFallbackConfig(backend.Name(), []string{"-d", "Ubuntu", "--exec", "env", "/home/alice/bin/cxp", "teams", "run"})
	if err := os.WriteFile(markerPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}

	spec := wslStartupEntryTestSpec()
	path, err := backend.InstallStartupFallback(context.Background(), spec, true)
	if err != nil {
		t.Fatalf("InstallStartupFallback: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("calls = %d, want one PowerShell call", len(runner.calls))
	}
	command := strings.Join(runner.calls[0].args, " ")
	legacyCleanup := buildTeamsServiceWSLRemoveStartupFallbackCommand(backend.Name(), []string{teamsServiceWSLTaskIdentity().Suffix})
	if !strings.Contains(command, legacyCleanup) {
		t.Fatalf("the older looping watchdog is not retired first:\n%s", command)
	}
	entry := command[strings.Index(command, legacyCleanup)+len(legacyCleanup):]
	for _, want := range []string{
		"codex-helper-teams-start-" + teamsServiceWSLTaskIdentity().Suffix + ".vbs",
		"Set-Content -LiteralPath $kickPath",
		"-Encoding Unicode",
		"teams service start",
		"CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND=local-supervisor",
		"Start-Process -FilePath 'wscript.exe'",
	} {
		if !strings.Contains(entry, want) {
			t.Fatalf("Startup entry command lacks %q:\n%s", want, entry)
		}
	}
	for _, forbidden := range []string{"while", "Start-Sleep", "Mutex", ".ps1", ".signal", "teams run", "-Wait", "STOP_FILE"} {
		if strings.Contains(entry, forbidden) {
			t.Fatalf("Startup entry command contains %q; it must be a one-shot start:\n%s", forbidden, entry)
		}
	}
	for _, forbidden := range []string{"while ($true)", "Start-Sleep -Seconds 10", "System.Threading.Mutex", "CODEX_HELPER_TEAMS_STARTUP_FALLBACK"} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("fallback command still carries the looping watchdog (%q):\n%s", forbidden, command)
		}
	}

	cfg, err := readTeamsServiceLocalSupervisorConfig(path)
	if err != nil {
		t.Fatalf("local-supervisor config: %v", err)
	}
	if !cfg.Enabled || cfg.Spec.Executable != spec.Executable {
		t.Fatalf("local-supervisor config = %+v, want enabled with the service spec", cfg)
	}
	selected, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatal(err)
	}
	if selected.ID() != teamsServiceLocalSupervisorID {
		t.Fatalf("backend after fallback = %s, want the local supervisor", selected.ID())
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("legacy watchdog marker survived: %v", err)
	}

	runner.calls = nil
	if _, err := backend.InstallStartupFallback(context.Background(), spec, false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(runner.calls[0].args, " "), "wscript.exe") {
		t.Fatal("install-only fallback runs the Startup entry")
	}
}

// The local supervisor retires older launchers when it starts; the one-shot
// entry that started it must survive, while other backends remove it.
func TestWSLStartupEntrySurvivesLocalSupervisorStartCleanup(t *testing.T) {
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withWSLStartupEntryTestHooks(t, runner)
	backend := teamsServiceWSLWindowsTaskBackend{}
	entry := teamsServiceWSLStartupKickName(teamsServiceWSLTaskIdentity().Suffix)

	if err := backend.removeLegacyStartupWatchdogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(runner.calls[0].args, " "); strings.Contains(got, entry) {
		t.Fatalf("local-supervisor cleanup removes its own Startup entry:\n%s", got)
	}
	runner.calls = nil
	if err := backend.removeStartupFallbackMarker(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(runner.calls[0].args, " "); !strings.Contains(got, entry) || !strings.Contains(got, "Remove-Item -LiteralPath $kickPath") {
		t.Fatalf("Scheduled Task cleanup keeps the Startup entry:\n%s", got)
	}
}

func TestLocalSupervisorLoginStartRespectsDisabledConfig(t *testing.T) {
	runner := &recordingTeamsServiceRunner{output: []byte("ok")}
	withWSLStartupEntryTestHooks(t, runner)
	local := teamsServiceLocalSupervisorBackend{}
	if _, err := local.Install(context.Background(), wslStartupEntryTestSpec()); err != nil {
		t.Fatal(err)
	}
	t.Setenv(teamsServiceStartupKickEnv, "1")
	started := false
	prev := teamsServiceLocalSupervisorStartDetached
	teamsServiceLocalSupervisorStartDetached = func(context.Context, string, string, teamsServiceSpec) (int, error) {
		started = true
		return 0, os.ErrInvalid
	}
	t.Cleanup(func() { teamsServiceLocalSupervisorStartDetached = prev })
	out, err := local.Run(context.Background(), "start")
	if err != nil {
		t.Fatalf("login start of a disabled supervisor: %v", err)
	}
	if started || !strings.Contains(string(out), "disabled") {
		t.Fatalf("login start ignored the disabled config: started=%v out=%q", started, out)
	}
}
