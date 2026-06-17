package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

func TestRootCommandWiresExpectedSubcommandsAndFlags(t *testing.T) {
	cmd := newRootCmd()

	var names []string
	for _, sub := range cmd.Commands() {
		names = append(names, sub.Name())
	}
	sort.Strings(names)

	want := []string{"__internal-npm-wrapper", "app", "beacon", "history", "init", "model", "model-profile", "proxy", "responses", "run", "selftest", "skills", "teams", "tui", "upgrade"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected root subcommands\n got: %#v\nwant: %#v", names, want)
	}
	if cmd.PersistentFlags().Lookup("config") == nil {
		t.Fatal("expected persistent --config flag")
	}
	if cmd.Flags().Lookup("upgrade-codex") == nil {
		t.Fatal("expected --upgrade-codex flag")
	}
	appCmd, _, err := cmd.Find([]string{"app"})
	if err != nil {
		t.Fatalf("find app command: %v", err)
	}
	if appCmd.Flags().Lookup("model-profile") == nil {
		t.Fatal("app command should expose --model-profile")
	}
}

func TestModelCommandWiresSimpleSubcommands(t *testing.T) {
	cmd := newRootCmd()
	modelCmd, _, err := cmd.Find([]string{"model"})
	if err != nil {
		t.Fatalf("find model command: %v", err)
	}
	var names []string
	for _, sub := range modelCmd.Commands() {
		names = append(names, sub.Name())
	}
	sort.Strings(names)
	want := []string{"doctor", "list", "setup", "use"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected model subcommands\n got: %#v\nwant: %#v", names, want)
	}
	setupCmd, _, err := modelCmd.Find([]string{"setup"})
	if err != nil {
		t.Fatalf("find model setup: %v", err)
	}
	for _, name := range []string{"api-key-env", "api-key-stdin", "ssh-proxy", "no-default", "no-doctor"} {
		if setupCmd.Flags().Lookup(name) == nil {
			t.Fatalf("model setup should expose --%s", name)
		}
	}
}

func TestModelProfileCommandWiresPlannedSubcommands(t *testing.T) {
	cmd := newRootCmd()
	modelCmd, _, err := cmd.Find([]string{"model-profile"})
	if err != nil {
		t.Fatalf("find model-profile command: %v", err)
	}
	var names []string
	for _, sub := range modelCmd.Commands() {
		names = append(names, sub.Name())
	}
	sort.Strings(names)
	want := []string{"delete", "doctor", "list", "set-default", "setup"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected model-profile subcommands\n got: %#v\nwant: %#v", names, want)
	}
	setupCmd, _, err := modelCmd.Find([]string{"setup"})
	if err != nil {
		t.Fatalf("find model-profile setup: %v", err)
	}
	for _, name := range []string{"provider", "api-key-env", "api-key-stdin", "ssh-proxy", "set-default", "no-doctor"} {
		if setupCmd.Flags().Lookup(name) == nil {
			t.Fatalf("model-profile setup should expose --%s", name)
		}
	}
}

func TestResponsesCommandWiresServeFlags(t *testing.T) {
	cmd := newRootCmd()
	serveCmd, _, err := cmd.Find([]string{"responses", "serve"})
	if err != nil {
		t.Fatalf("find responses serve command: %v", err)
	}
	for _, name := range []string{"listen", "base-url", "api-key", "api-key-env", "model", "provider", "store-path", "providers-json", "proxy-keys", "scope-salt"} {
		if serveCmd.Flags().Lookup(name) == nil {
			t.Fatalf("responses serve should expose --%s", name)
		}
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

	want := []string{"auth", "chat", "control", "doctor", "drain", "pause", "probe-chat", "recover", "resume", "run", "send-file", "service", "setup", "status", "workflow"}
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
	if runCmd.Flags().Lookup("owner-stale-after") == nil || runCmd.Flags().Lookup("max-work-chat-polls") == nil {
		t.Fatal("teams run should expose Teams helper reliability tuning flags")
	}
	if runCmd.Flags().Lookup("auto-update") == nil || runCmd.Flags().Lookup("auto-update-repo") == nil || runCmd.Flags().Lookup("auto-update-prerelease") == nil {
		t.Fatal("teams run should expose Teams helper auto-update flags")
	}
	if runCmd.Flags().Lookup("auto-service") == nil {
		t.Fatal("teams run should expose Teams helper background service auto-ensure flag")
	}
	if flag := runCmd.Flags().Lookup("codex-timeout"); flag == nil {
		t.Fatal("teams run should expose --codex-timeout")
	} else if flag.DefValue != "0s" {
		t.Fatalf("teams run --codex-timeout default = %q, want 0s", flag.DefValue)
	}
	if runCmd.Flags().Lookup("model-profile") == nil {
		t.Fatal("teams run should expose --model-profile")
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
	if want := []string{"config", "file-write", "file-write-logout", "file-write-status", "full", "full-logout", "full-status", "logout", "read", "read-logout", "read-status", "status"}; !reflect.DeepEqual(authNames, want) {
		t.Fatalf("unexpected teams auth subcommands\n got: %#v\nwant: %#v", authNames, want)
	}
}

func TestTeamsASRTranscriberDefaultsToManagedQwenRuntime(t *testing.T) {
	transcriber := teamsASRTranscriberFromConfig("", nil)
	managed, ok := transcriber.(*teams.ManagedASRTranscriber)
	if !ok {
		t.Fatalf("default ASR transcriber = %T, want managed Qwen runtime", transcriber)
	}
	if managed == nil || managed.Config.ModelID != "" {
		t.Fatalf("managed ASR config = %#v, want default model resolved lazily", managed)
	}
	if managed.Config.LlamaDevice != "cpu" || managed.Config.AllowTransformersFallback {
		t.Fatalf("default managed ASR config = %#v, want CPU llama without transformers fallback opt-in", managed.Config)
	}

	override := teamsASRTranscriberFromConfig("/tmp/custom-asr", []string{"--input={input}"})
	command, ok := override.(*teams.CommandASRTranscriber)
	if !ok {
		t.Fatalf("developer ASR override = %T, want command transcriber", override)
	}
	if command.Command != "/tmp/custom-asr" || strings.Join(command.Args, "\n") != "--input={input}" {
		t.Fatalf("developer ASR override not preserved: %#v", command)
	}
}

func TestTeamsASRTranscriberReadsManagedLlamaEnv(t *testing.T) {
	t.Setenv(envTeamsASRBackend, "llama")
	t.Setenv(envTeamsASRLlamaBinary, "/opt/llama/llama-mtmd-cli")
	t.Setenv(envTeamsASRLlamaModel, "/models/qwen.gguf")
	t.Setenv(envTeamsASRLlamaMMProj, "/models/mmproj.gguf")
	t.Setenv(envTeamsASRLlamaDevice, "auto")
	t.Setenv(envTeamsASRFFmpeg, "/opt/ffmpeg")
	t.Setenv(envTeamsASRAllowTransformersFallback, "yes")

	transcriber := teamsASRTranscriberFromConfig("", nil)
	managed, ok := transcriber.(*teams.ManagedASRTranscriber)
	if !ok {
		t.Fatalf("ASR transcriber = %T, want managed Qwen runtime", transcriber)
	}
	if managed.Config.Backend != "llama" ||
		managed.Config.LlamaBinaryPath != "/opt/llama/llama-mtmd-cli" ||
		managed.Config.LlamaModelPath != "/models/qwen.gguf" ||
		managed.Config.LlamaMMProjPath != "/models/mmproj.gguf" ||
		managed.Config.LlamaDevice != "auto" ||
		managed.Config.FFmpegPath != "/opt/ffmpeg" ||
		!managed.Config.AllowTransformersFallback {
		t.Fatalf("managed ASR config from env = %#v", managed.Config)
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
	t.Setenv(envTeamsCodexChild, "")
	t.Setenv(envTeamsCodexParentPID, "")
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	t.Setenv("CODEX_HELPER_BEACON_STORE", t.TempDir()+"/beacon.json")
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

func TestRestartTeamsHelperFromTeamsLinuxLocalSupervisorSchedulesServiceRestart(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  exe,
		cwd:  tmp,
	})
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(tmp, "beacon.json"))
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

	prevDetached := teamsServiceStartDetached
	prevExit := exitFunc
	prevExec := execSelf
	prevStart := startSelf
	t.Cleanup(func() {
		teamsServiceStartDetached = prevDetached
		exitFunc = prevExit
		execSelf = prevExec
		startSelf = prevStart
	})
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
	execSelf = func(string, []string, []string) error {
		t.Fatal("local-supervisor service restart must not exec the current helper process")
		return nil
	}
	startSelf = func(string, []string) error {
		t.Fatal("local-supervisor service restart must not spawn teams run directly")
		return nil
	}

	if err := restartTeamsHelperFromTeams(context.Background()); err != nil {
		t.Fatalf("restartTeamsHelperFromTeams error: %v", err)
	}
	if !exited || exitCode != 0 {
		t.Fatalf("exit = %v/%d, want true/0", exited, exitCode)
	}
	joined := strings.Join(gotArgs, " ")
	if gotName != "sh" ||
		!strings.Contains(joined, envTeamsLinuxServiceBackend+"=local-supervisor") ||
		!strings.Contains(joined, envTeamsWSLServiceBackend+"=local-supervisor") ||
		!strings.Contains(joined, shellQuote(exe)+" teams service restart") ||
		strings.Contains(joined, "systemctl") {
		t.Fatalf("detached restart command = %q %#v, want local-supervisor service restart", gotName, gotArgs)
	}
}

func TestRestartTeamsHelperFromTeamsBlocksOnBeaconJob(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	seedBeaconStateForUpgradeTest(t, func(st *beacon.State) {
		st.Machines["gpu-a"] = beacon.Machine{
			ID:            "gpu-a",
			LeaseID:       "lease-gpu-a",
			ProviderJobID: "slurm-123",
			Profile:       "gpu",
			State:         "accepting",
			Jobs:          []string{"job-1"},
		}
	})

	prevExec := execSelf
	prevStart := startSelf
	prevGOOS := teamsServiceGOOS
	t.Cleanup(func() {
		execSelf = prevExec
		startSelf = prevStart
		teamsServiceGOOS = prevGOOS
	})
	teamsServiceGOOS = func() string { return "linux" }
	execSelf = func(string, []string, []string) error {
		t.Fatal("helper restart must not exec while beacon work is active")
		return nil
	}
	startSelf = func(string, []string) error {
		t.Fatal("helper restart must not spawn while beacon work is active")
		return nil
	}

	err := restartTeamsHelperFromTeams(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Beacon state has upgrade-blocking work") || !strings.Contains(err.Error(), "beacon_job job-1") {
		t.Fatalf("restartTeamsHelperFromTeams error = %v, want beacon blocker", err)
	}
}

func TestRestartTeamsHelperFromTeamsRefusesTeamsCodexChild(t *testing.T) {
	t.Setenv(envTeamsCodexChild, "1")
	err := restartTeamsHelperFromTeams(context.Background())
	if err == nil {
		t.Fatal("restartTeamsHelperFromTeams error = nil, want refusal")
	}
	if !strings.Contains(err.Error(), "helper restart now") || !strings.Contains(err.Error(), "launched by Teams helper") {
		t.Fatalf("restartTeamsHelperFromTeams error = %v, want Teams child refusal", err)
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
