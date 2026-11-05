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

	want := []string{"__internal-npm-wrapper", "__user-path-probe", "app", "delegate", "history", "init", "proxy", "run", "selftest", "skills", "teams", "tui", "upgrade"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected root subcommands\n got: %#v\nwant: %#v", names, want)
	}
	if cmd.PersistentFlags().Lookup("config") == nil {
		t.Fatal("expected persistent --config flag")
	}
	if cmd.Flags().Lookup("upgrade-codex") == nil {
		t.Fatal("expected --upgrade-codex flag")
	}
	if cmd.Flags().Lookup("upgrade-codex-app") == nil {
		t.Fatal("expected --upgrade-codex-app flag")
	}
	if cmd.Flags().Lookup("upgrade-codex-path") == nil {
		t.Fatal("expected --upgrade-codex-path flag")
	}
	appCmd, _, err := cmd.Find([]string{"app"})
	if err != nil {
		t.Fatalf("find app command: %v", err)
	}
	if appCmd.Flags().Lookup("model-profile") != nil {
		t.Fatal("app command still exposes the retired --model-profile flag")
	}
}

func TestSkillsCommandWiresPlannedSubcommands(t *testing.T) {
	cmd := newRootCmd()
	skillsCmd, _, err := cmd.Find([]string{"skills"})
	if err != nil {
		t.Fatalf("find skills command: %v", err)
	}

	var names []string
	for _, sub := range skillsCmd.Commands() {
		names = append(names, sub.Name())
	}
	sort.Strings(names)

	want := []string{"add", "doctor", "install-builtin", "list", "migrate", "push", "remove", "sync"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected skills subcommands\n got: %#v\nwant: %#v", names, want)
	}
	if skillsCmd.PersistentFlags().Lookup("codex-dir") == nil {
		t.Fatal("skills command should expose --codex-dir")
	}
	syncCmd, _, err := skillsCmd.Find([]string{"sync"})
	if err != nil {
		t.Fatalf("find skills sync: %v", err)
	}
	if syncCmd.Use != "sync [name]" {
		t.Fatalf("skills sync use = %q", syncCmd.Use)
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

	want := []string{"auth", "control", "doctor", "migrate", "path", "run", "service", "setup", "start", "status", "stop", "workflow"}
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
	if flag := runCmd.Flags().Lookup("owner-stale-after"); flag == nil || !flag.Hidden {
		t.Fatal("teams run should accept the hidden --owner-stale-after of earlier service units")
	}
	if flag := runCmd.Flag("registry"); flag == nil || !flag.Hidden {
		t.Fatal("teams run should accept the hidden --registry of earlier service units")
	}
	if runCmd.Flags().Lookup("auto-update") == nil || runCmd.Flags().Lookup("auto-update-repo") == nil || runCmd.Flags().Lookup("auto-update-prerelease") == nil {
		t.Fatal("teams run should expose Teams helper auto-update flags")
	}
	if runCmd.Flags().Lookup("auto-service") == nil {
		t.Fatal("teams run should expose Teams helper background service auto-ensure flag")
	}
	if flag := runCmd.Flags().Lookup("machine-registry"); flag == nil {
		t.Fatal("teams run should expose --machine-registry")
	} else if flag.DefValue != "true" {
		t.Fatalf("teams run --machine-registry default = %q, want true", flag.DefValue)
	}
	for _, removed := range []string{"model-profile", "control-fallback-model", "max-work-chat-polls", "codex-timeout"} {
		if runCmd.Flags().Lookup(removed) != nil {
			t.Fatalf("teams run still exposes removed --%s", removed)
		}
	}
	if flag := runCmd.Flags().Lookup("asr-command"); flag == nil {
		t.Fatal("teams run should keep developer ASR override wired")
	} else if !flag.Hidden {
		t.Fatal("teams run should hide developer ASR override from user help")
	}
	if flag := runCmd.Flags().Lookup("asr-arg"); flag == nil {
		t.Fatal("teams run should keep developer ASR arg override wired")
	} else if !flag.Hidden {
		t.Fatal("teams run should hide developer ASR args from user help")
	}
	runHelp := runCmd.UsageString()
	if strings.Contains(runHelp, "--asr-command") || strings.Contains(runHelp, "CODEX_HELPER_TEAMS_ASR") {
		t.Fatalf("teams run help leaked ASR implementation details:\n%s", runHelp)
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
	if want := []string{"config", "login", "logout", "status"}; !reflect.DeepEqual(authNames, want) {
		t.Fatalf("unexpected teams auth subcommands\n got: %#v\nwant: %#v", authNames, want)
	}
}

func TestTeamsServiceRestartRefusesTeamsCodexChild(t *testing.T) {
	t.Setenv(envTeamsCodexChild, "1")
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"teams", "service", "restart"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("teams service restart error = nil, want refusal")
	}
	if !strings.Contains(err.Error(), "helper restart now") || !strings.Contains(err.Error(), "launched by Teams helper") {
		t.Fatalf("teams service restart error = %v, want Teams child refusal", err)
	}
}

func TestTeamsServiceStopRefusesTeamsCodexChild(t *testing.T) {
	t.Setenv(envTeamsCodexChild, "1")
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"teams", "service", "stop"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("teams service stop error = nil, want refusal")
	}
	if !strings.Contains(err.Error(), "helper restart now") || !strings.Contains(err.Error(), "launched by Teams helper") {
		t.Fatalf("teams service stop error = %v, want Teams child refusal", err)
	}
}

func TestTeamsServiceMutationsRefuseTeamsCodexChild(t *testing.T) {
	localAdvice := map[string]string{
		"teams service install":   "run `cxp teams service install` in a local terminal",
		"teams service bootstrap": "run `cxp teams service bootstrap` in a local terminal",
	}
	for _, args := range [][]string{
		{"teams", "service", "install"},
		{"teams", "service", "bootstrap"},
		{"teams", "service", "enable"},
		{"teams", "service", "disable"},
		{"teams", "service", "start"},
		{"teams", "service", "uninstall"},
		{"upgrade"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Setenv(envTeamsCodexChild, "1")
			cmd := newRootCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(args)
			err := cmd.Execute()
			if err == nil {
				t.Fatalf("%v error = nil, want Teams child refusal", args)
			}
			if !strings.Contains(err.Error(), "launched by Teams helper") {
				t.Fatalf("%v error = %v, want Teams child refusal", args, err)
			}
			if want := localAdvice[strings.Join(args, " ")]; want != "" && !strings.Contains(err.Error(), want) {
				t.Fatalf("%v error = %v, want local terminal advice %q", args, err, want)
			}
		})
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

// A removed Teams command must fail, not print the group help and exit 0
// (decision: `cxp teams recover` and `probe-chat` are deleted). A bare
// group still shows its help.
func TestTeamsUnknownSubcommandFails(t *testing.T) {
	for _, args := range [][]string{
		{"teams", "recover"},
		{"teams", "probe-chat"},
		{"teams", "service", "reload"},
		{"teams", "path", "capture-terminal"},
		{"teams", "workflow", "probe"},
	} {
		cmd := newRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs(args)
		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "unknown command") {
			t.Fatalf("%v: error = %v, want unknown command (output %q)", args, err, out.String())
		}
	}
	for _, args := range [][]string{{"teams"}, {"teams", "service"}} {
		cmd := newRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil || !strings.Contains(out.String(), "Available Commands") {
			t.Fatalf("%v: error = %v, output %q, want help", args, err, out.String())
		}
	}
}
