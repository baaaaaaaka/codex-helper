package cli

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestRootCommandWiresExpectedSubcommandsAndFlags(t *testing.T) {
	cmd := newRootCmd()

	var names []string
	for _, sub := range cmd.Commands() {
		names = append(names, sub.Name())
	}
	sort.Strings(names)

	want := []string{"__internal-npm-wrapper", "history", "init", "proxy", "run", "teams", "tui", "upgrade"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected root subcommands\n got: %#v\nwant: %#v", names, want)
	}
	if cmd.PersistentFlags().Lookup("config") == nil {
		t.Fatal("expected persistent --config flag")
	}
	if cmd.Flags().Lookup("upgrade-codex") == nil {
		t.Fatal("expected --upgrade-codex flag")
	}
}

func TestTeamsCommandWiresPlannedSubcommands(t *testing.T) {
	cmd := newRootCmd()
	teamsCmd, _, err := cmd.Find([]string{"teams"})
	if err != nil {
		t.Fatalf("find teams command: %v", err)
	}

	var names []string
	for _, sub := range teamsCmd.Commands() {
		names = append(names, sub.Name())
	}
	sort.Strings(names)

	want := []string{"auth", "chat", "control", "doctor", "drain", "pause", "probe-chat", "recover", "resume", "run", "send-file", "service", "setup", "status"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected teams subcommands\n got: %#v\nwant: %#v", names, want)
	}

	runCmd, _, err := teamsCmd.Find([]string{"listen"})
	if err != nil {
		t.Fatalf("find teams listen alias: %v", err)
	}
	if runCmd.Name() != "run" {
		t.Fatalf("teams listen should resolve to run, got %q", runCmd.Name())
	}
	if runCmd.Flags().Lookup("control-fallback-model") == nil {
		t.Fatal("teams run should expose --control-fallback-model")
	}

	authCmd, _, err := teamsCmd.Find([]string{"auth"})
	if err != nil {
		t.Fatalf("find teams auth command: %v", err)
	}
	var authNames []string
	for _, sub := range authCmd.Commands() {
		authNames = append(authNames, sub.Name())
	}
	sort.Strings(authNames)
	if want := []string{"config", "file-write", "file-write-logout", "file-write-status", "logout", "read", "read-logout", "read-status", "status"}; !reflect.DeepEqual(authNames, want) {
		t.Fatalf("unexpected teams auth subcommands\n got: %#v\nwant: %#v", authNames, want)
	}
}

func TestRestartTeamsHelperFromTeamsWindowsServiceSchedulesTaskStart(t *testing.T) {
	prevGOOS := teamsServiceGOOS
	prevDetached := teamsServiceStartDetached
	prevExit := exitFunc
	prevPowerShell := teamsServicePowerShellExecutable
	t.Cleanup(func() {
		teamsServiceGOOS = prevGOOS
		teamsServiceStartDetached = prevDetached
		exitFunc = prevExit
		teamsServicePowerShellExecutable = prevPowerShell
	})
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	teamsServiceGOOS = func() string { return "windows" }
	teamsServicePowerShellExecutable = func() string { return "powershell.exe" }
	var gotName string
	var gotArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		gotName = name
		gotArgs = append([]string(nil), args...)
		return nil
	}
	var exitCode int
	var exited bool
	exitFunc = func(code int) {
		exited = true
		exitCode = code
	}

	if err := restartTeamsHelperFromTeams(context.Background()); err != nil {
		t.Fatalf("restartTeamsHelperFromTeams error: %v", err)
	}
	if !exited || exitCode != 0 {
		t.Fatalf("exit = %v/%d, want true/0", exited, exitCode)
	}
	if gotName != "powershell.exe" || !strings.Contains(strings.Join(gotArgs, " "), "Start-ScheduledTask") {
		t.Fatalf("detached restart command = %q %#v, want scheduled task start", gotName, gotArgs)
	}
}

func TestInternalNpmWrapperCommandPassesArgsThroughRoot(t *testing.T) {
	lockCLITestHooks(t)

	prevExit := internalNpmWrapperExit
	prevRun := codexSelfUpdateRunRealNpm
	prevCleanup := codexSelfUpdateCleanupStale
	t.Cleanup(func() {
		internalNpmWrapperExit = prevExit
		codexSelfUpdateRunRealNpm = prevRun
		codexSelfUpdateCleanupStale = prevCleanup
	})

	t.Setenv(envCodexProxyRealNPM, "/usr/bin/npm")
	t.Setenv(envCodexProxyOriginalPath, "/usr/bin:/bin")

	var exitCode int
	internalNpmWrapperExit = func(code int) {
		exitCode = code
	}
	var gotArgs []string
	codexSelfUpdateRunRealNpm = func(_ context.Context, npmPath string, args []string, env []string) error {
		if npmPath != "/usr/bin/npm" {
			t.Fatalf("expected real npm path, got %q", npmPath)
		}
		gotArgs = append([]string{}, args...)
		if got := envValue(env, "PATH"); got != "/usr/bin:/bin" {
			t.Fatalf("expected sanitized PATH, got %q", got)
		}
		return nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"__internal-npm-wrapper", "install", "--", "-g", "lodash"})
	var errOut bytes.Buffer
	cmd.SetErr(&errOut)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute internal wrapper: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d, stderr=%q", exitCode, errOut.String())
	}
	if strings.Join(gotArgs, " ") != "install -- -g lodash" {
		t.Fatalf("unexpected forwarded args: %#v", gotArgs)
	}
}

func TestInternalNpmWrapperCommandReportsFailureExit(t *testing.T) {
	lockCLITestHooks(t)

	prevExit := internalNpmWrapperExit
	t.Cleanup(func() { internalNpmWrapperExit = prevExit })

	var exitCode int
	internalNpmWrapperExit = func(code int) {
		exitCode = code
	}
	t.Setenv(envCodexProxyRealNPM, "")

	cmd := newRootCmd()
	cmd.SetArgs([]string{"__internal-npm-wrapper", "install", "-g", "@openai/codex"})
	var errOut bytes.Buffer
	cmd.SetErr(&errOut)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute internal wrapper: %v", err)
	}
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
	if !strings.Contains(errOut.String(), "missing real npm path") {
		t.Fatalf("expected missing npm message, got %q", errOut.String())
	}
}
