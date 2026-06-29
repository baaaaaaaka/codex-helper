package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/env"
	"github.com/baaaaaaaka/codex-helper/internal/migration"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

type codexCLIInvocation struct {
	GlobalArgs     []string
	Command        string
	Args           []string
	WorkingDir     string
	AdditionalDirs []string
	ImagePaths     []string
}

var codexSubcommands = map[string]bool{
	"exec": true, "e": true, "resume": true, "fork": true, "review": true,
	"login": true, "logout": true, "mcp": true, "mcp-server": true,
	"app-server": true, "completion": true, "cloud": true, "features": true,
	"debug": true, "apply": true, "sandbox": true, "execpolicy": true,
	"stdio-to-uds": true, "responses-api-proxy": true, "plugin": true,
	"remote-control": true, "update": true, "doctor": true,
	"exec-server": true, "help": true, "a": true,
}

func splitCodexCLIInvocation(args []string) (codexCLIInvocation, error) {
	args = expandCodexOptionEquals(migration.RemoveLegacyCodexExecutionOverrides(args))
	var invocation codexCLIInvocation
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		if codexSubcommands[arg] {
			invocation.Command = arg
			invocation.Args = append([]string(nil), args[index+1:]...)
			return invocation, nil
		}
		if arg == "--" {
			if index+1 < len(args) {
				invocation.Command = strings.TrimSpace(args[index+1])
				invocation.Args = append([]string(nil), args[index+2:]...)
			}
			return invocation, nil
		}
		if !strings.HasPrefix(arg, "-") {
			// Without a recognized subcommand, Codex treats the remaining
			// positional arguments as the interactive TUI's initial prompt.
			invocation.Args = append([]string(nil), args[index:]...)
			return invocation, nil
		}
		invocation.GlobalArgs = append(invocation.GlobalArgs, args[index])
		if codexGlobalOptionTakesValue(arg) && !strings.Contains(arg, "=") {
			if index+1 >= len(args) {
				return invocation, fmt.Errorf("Codex option %s requires a value", arg)
			}
			index++
			invocation.GlobalArgs = append(invocation.GlobalArgs, args[index])
			switch arg {
			case "-C", "--cd":
				invocation.WorkingDir = args[index]
			case "--add-dir":
				invocation.AdditionalDirs = append(invocation.AdditionalDirs, args[index])
			case "-i", "--image":
				invocation.ImagePaths = append(invocation.ImagePaths, args[index])
			}
		}
	}
	return invocation, nil
}

func expandCodexOptionEquals(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		key, value, ok := strings.Cut(arg, "=")
		if !ok {
			out = append(out, arg)
			continue
		}
		switch key {
		case "--config", "--enable", "--disable", "--model", "--profile", "--profile-v2", "--local-provider", "--cd", "--add-dir", "--image", "--color", "--output-schema", "--output-last-message", "--remote", "--remote-auth-token-env":
			out = append(out, key, value)
		default:
			out = append(out, arg)
		}
	}
	return out
}

func codexGlobalOptionTakesValue(arg string) bool {
	switch arg {
	case "-c", "--config", "--enable", "--disable", "-m", "--model", "-p", "--profile", "--profile-v2", "--local-provider", "-C", "--cd", "--add-dir", "-i", "--image", "--remote", "--remote-auth-token-env":
		return true
	default:
		return false
	}
}

func translateCodexGlobalArgsToAppServer(args []string) ([]string, error) {
	out := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		switch arg {
		case "-c", "--config", "--enable", "--disable":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			out = append(out, arg, args[index+1])
			index++
		case "-m", "--model":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a model", arg)
			}
			out = append(out, "-c", `model="`+tomlEscapeString(args[index+1])+`"`)
			index++
		case "--search":
			out = append(out, "-c", `web_search="live"`)
		case "--oss":
			out = append(out, "-c", `model_provider="ollama"`)
		case "--local-provider":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("--local-provider requires a value")
			}
			out = append(out, "-c", `model_provider="`+tomlEscapeString(args[index+1])+`"`)
			index++
		case "--strict-config":
			out = append(out, arg)
		case "-p", "--profile", "--profile-v2":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a profile", arg)
			}
			// The remote TUI resolves its selected profile and sends the effective
			// thread configuration to app-server. Exec profile calls use the native
			// CLI because profile selection is a loader-level operation.
			index++
		case "--dangerously-bypass-hook-trust":
			out = append(out, "-c", "bypass_hook_trust=true")
		case "--no-alt-screen":
			// This is a TUI-only presentation option.
		case "-C", "--cd", "--add-dir", "-i", "--image":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a directory", arg)
			}
			index++
		default:
			return nil, fmt.Errorf("Codex option %q cannot be applied to app-server", arg)
		}
	}
	return out, nil
}

func runCodexCLIInvocation(
	ctx context.Context,
	root *rootOptions,
	store *config.Store,
	profile *config.Profile,
	instances []config.Instance,
	cmdArgs []string,
	useProxy bool,
	opts runTargetOptions,
) error {
	if len(cmdArgs) == 0 {
		return fmt.Errorf("missing Codex command")
	}
	invocation, err := splitCodexCLIInvocation(cmdArgs[1:])
	if err != nil {
		return err
	}
	if invocation.Command == "" && len(invocation.Args) > 0 {
		if discovered := discoverCodexTopLevelCommands(ctx, cmdArgs[0]); discovered[invocation.Args[0]] {
			invocation.Command = invocation.Args[0]
			invocation.Args = invocation.Args[1:]
		}
	}
	if codexInvocationUsesNativeCLI(invocation, cmdArgs[1:]) {
		if opts.AgentAutoApprove {
			return fmt.Errorf("--aaa is not supported for this native Codex subcommand")
		}
		return runCodexNativeInvocation(ctx, store, profile, instances, cmdArgs, useProxy, opts)
	}
	appServerArgs, err := translateCodexGlobalArgsToAppServer(invocation.GlobalArgs)
	if err != nil {
		return err
	}
	cwd := strings.TrimSpace(opts.Cwd)
	if strings.TrimSpace(invocation.WorkingDir) != "" {
		cwd = invocation.WorkingDir
	}
	if cwd == "" {
		cwd, err = os.Getwd()
		if err != nil {
			return err
		}
	}

	switch invocation.Command {
	case "", "resume", "fork":
		tail := append([]string(nil), invocation.Args...)
		if invocation.Command != "" {
			tail = append([]string{invocation.Command}, invocation.Args...)
		}
		return runCodexTUIInvocationViaBroker(ctx, root, store, profile, instances, cwd, cmdArgs[0], "", useProxy, opts.AgentAutoApprove, opts.ModelProfileRef, invocation.GlobalArgs, tail, appServerArgs, opts.Log)
	case "exec", "e":
		execArgs := codexFacadeArgsWithGlobalInputs(invocation, invocation.Args)
		return runCodexExecFacade(ctx, root, store, profile, instances, cmdArgs[0], cwd, useProxy, opts, appServerArgs, execArgs)
	case "review":
		reviewArgs, err := codexReviewArgsToExecArgs(invocation.Args)
		if err != nil {
			return err
		}
		reviewArgs = codexFacadeArgsWithGlobalInputs(invocation, reviewArgs)
		return runCodexExecFacade(ctx, root, store, profile, instances, cmdArgs[0], cwd, useProxy, opts, appServerArgs, reviewArgs)
	default:
		return runCodexNativeInvocation(ctx, store, profile, instances, cmdArgs, useProxy, opts)
	}
}

func codexFacadeArgsWithGlobalInputs(invocation codexCLIInvocation, tail []string) []string {
	capacity := len(tail) + 2*len(invocation.ImagePaths) + 2*len(invocation.AdditionalDirs)
	args := make([]string, 0, capacity)
	for _, image := range invocation.ImagePaths {
		args = append(args, "--image", image)
	}
	for _, dir := range invocation.AdditionalDirs {
		args = append(args, "--add-dir", dir)
	}
	return append(args, tail...)
}

func codexInvocationUsesNativeCLI(invocation codexCLIInvocation, rawArgs []string) bool {
	for _, arg := range rawArgs {
		switch strings.TrimSpace(arg) {
		case "-h", "--help", "-V", "--version":
			return true
		}
		if strings.HasPrefix(strings.TrimSpace(arg), "--remote") {
			return true
		}
	}
	switch invocation.Command {
	case "", "resume", "fork", "exec", "e", "review":
	default:
		return true
	}
	if invocation.Command == "exec" || invocation.Command == "e" {
		for _, arg := range append(append([]string{}, invocation.GlobalArgs...), invocation.Args...) {
			switch strings.TrimSpace(arg) {
			case "--ignore-user-config", "--ignore-rules", "-p", "--profile", "--profile-v2":
				return true
			}
		}
	}
	return false
}

func runCodexNativeInvocation(ctx context.Context, store *config.Store, profile *config.Profile, instances []config.Instance, cmdArgs []string, useProxy bool, opts runTargetOptions) error {
	installOptions := codexInstallOptions{}
	if useProxy {
		if profile == nil {
			return fmt.Errorf("proxy mode enabled but no profile configured")
		}
		installOptions.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, *profile, instances, runInstall)
		}
	}
	lookup := cmdArgs[0]
	if codexPathAllowsAutomaticUpgrade(lookup) {
		lookup = ""
	}
	if err := applyDefaultCodexExecutionContext(&opts); err != nil {
		return err
	}
	runtimeContract, err := resolveCodexInstalledRuntimeForLaunch(ctx, lookup, opts.Log, installOptions, opts.ExecIdentity, opts.ExtraEnv)
	if err != nil {
		return err
	}
	cmdArgs = append([]string{runtimeContract.Command}, migration.RemoveLegacyCodexExecutionOverrides(cmdArgs[1:])...)
	opts.ExtraEnv = codexRuntimeEnvironmentOverlay(opts.ExtraEnv, runtimeContract.Environment)
	proxyURL := ""
	if useProxy {
		proxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *profile, instances, opts.Log)
		if err != nil {
			return err
		}
	}
	cmdArgs, cleanup, err := prepareCodexModelProfileForRun(ctx, store, cmdArgs, &opts, proxyURL)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	opts.UseProxy = useProxy
	return runTargetWithFallbackWithOptions(ctx, cmdArgs, proxyURL, nil, nil, opts)
}

func discoverCodexTopLevelCommands(ctx context.Context, codexPath string) map[string]bool {
	commands := make(map[string]bool, len(codexSubcommands))
	for command := range codexSubcommands {
		commands[command] = true
	}
	probePath := codexPath
	if codexPathAllowsAutomaticUpgrade(probePath) {
		if resolved, err := exec.LookPath("codex"); err == nil {
			probePath = resolved
		} else if resolved, err := findInstalledCodexWithoutProbe(); err == nil {
			probePath = resolved
		}
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(probeCtx, probePath, "--help").Output()
	if err != nil {
		return commands
	}
	inCommands := false
	for _, line := range strings.Split(string(output), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "Commands:" {
			inCommands = true
			continue
		}
		if !inCommands {
			continue
		}
		if trimmed == "Arguments:" || trimmed == "Options:" {
			break
		}
		// Clap renders command rows with exactly two leading spaces. Wrapped
		// descriptions are indented further and must not become fake commands.
		if !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "   ") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) == 0 || strings.HasPrefix(fields[0], "[") {
			continue
		}
		commands[fields[0]] = true
		if marker := strings.Index(trimmed, "[aliases:"); marker >= 0 {
			aliases := strings.TrimSuffix(strings.TrimSpace(trimmed[marker+len("[aliases:"):]), "]")
			for _, alias := range strings.Split(aliases, ",") {
				commands[strings.TrimSpace(alias)] = true
			}
		}
	}
	return commands
}

type codexExecFacadeOptions struct {
	JSONOutput     bool
	Resume         bool
	ThreadID       string
	ResumeLast     bool
	ResumeAll      bool
	Prompt         string
	ImagePaths     []string
	WorkingDir     string
	OutputLast     string
	OutputSchema   json.RawMessage
	AdditionalDirs []string
	AppServerArgs  []string
	SkipRepository bool
	Ephemeral      bool
}

func parseCodexExecFacadeArgs(args []string, defaultCwd string) (codexExecFacadeOptions, error) {
	args = expandCodexOptionEquals(migration.RemoveLegacyCodexExecutionOverrides(args))
	options := codexExecFacadeOptions{WorkingDir: defaultCwd}
	if len(args) > 0 && strings.TrimSpace(args[0]) == "resume" {
		options.Resume = true
		args = args[1:]
	}
	var prompt []string
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		switch arg {
		case "--json":
			options.JSONOutput = true
		case "--skip-git-repo-check":
			options.SkipRepository = true
		case "--ephemeral":
			options.Ephemeral = true
		case "--last":
			if !options.Resume {
				return options, fmt.Errorf("--last requires exec resume")
			}
			options.ResumeLast = true
		case "--all":
			if !options.Resume {
				return options, fmt.Errorf("--all requires exec resume")
			}
			options.ResumeAll = true
		case "-i", "--image":
			if index+1 >= len(args) {
				return options, fmt.Errorf("%s requires a path", arg)
			}
			index++
			options.ImagePaths = append(options.ImagePaths, args[index])
		case "-C", "--cd":
			if index+1 >= len(args) {
				return options, fmt.Errorf("%s requires a directory", arg)
			}
			index++
			options.WorkingDir = args[index]
		case "--add-dir":
			if index+1 >= len(args) {
				return options, fmt.Errorf("--add-dir requires a directory")
			}
			index++
			options.AdditionalDirs = append(options.AdditionalDirs, args[index])
		case "-o", "--output-last-message":
			if index+1 >= len(args) {
				return options, fmt.Errorf("%s requires a path", arg)
			}
			index++
			options.OutputLast = args[index]
		case "--output-schema":
			if index+1 >= len(args) {
				return options, fmt.Errorf("--output-schema requires a path")
			}
			index++
			raw, err := os.ReadFile(args[index])
			if err != nil {
				return options, fmt.Errorf("read output schema: %w", err)
			}
			if !json.Valid(raw) {
				return options, fmt.Errorf("output schema is not valid JSON")
			}
			options.OutputSchema = append(json.RawMessage(nil), raw...)
		case "--color":
			if index+1 >= len(args) {
				return options, fmt.Errorf("--color requires a value")
			}
			index++
		case "-m", "--model":
			if index+1 >= len(args) {
				return options, fmt.Errorf("%s requires a model", arg)
			}
			index++
			options.AppServerArgs = append(options.AppServerArgs, "-c", `model="`+tomlEscapeString(args[index])+`"`)
		case "--oss":
			options.AppServerArgs = append(options.AppServerArgs, "-c", `model_provider="ollama"`)
		case "--local-provider":
			if index+1 >= len(args) {
				return options, fmt.Errorf("--local-provider requires a value")
			}
			index++
			options.AppServerArgs = append(options.AppServerArgs, "-c", `model_provider="`+tomlEscapeString(args[index])+`"`)
		case "--dangerously-bypass-hook-trust":
			options.AppServerArgs = append(options.AppServerArgs, "-c", "bypass_hook_trust=true")
		case "-c", "--config", "--enable", "--disable":
			if index+1 >= len(args) {
				return options, fmt.Errorf("%s requires a value", arg)
			}
			index++
			options.AppServerArgs = append(options.AppServerArgs, arg, args[index])
		case "--strict-config":
			options.AppServerArgs = append(options.AppServerArgs, arg)
		default:
			if strings.HasPrefix(arg, "-") && arg != "-" {
				return options, fmt.Errorf("codex exec option %q is not supported by the app-server facade", arg)
			}
			if options.Resume && options.ThreadID == "" && arg != "-" {
				options.ThreadID = arg
				continue
			}
			prompt = append(prompt, args[index])
		}
	}
	options.Prompt = strings.Join(prompt, " ")
	return options, nil
}

func runCodexExecFacade(
	ctx context.Context,
	root *rootOptions,
	store *config.Store,
	profile *config.Profile,
	instances []config.Instance,
	codexPath string,
	cwd string,
	useProxy bool,
	runOptions runTargetOptions,
	globalAppServerArgs []string,
	execArgs []string,
) error {
	if len(execArgs) > 0 && strings.TrimSpace(execArgs[0]) == "review" {
		var err error
		execArgs, err = codexReviewArgsToExecArgs(execArgs[1:])
		if err != nil {
			return err
		}
	}
	options, err := parseCodexExecFacadeArgs(execArgs, cwd)
	if err != nil {
		return err
	}
	options.AppServerArgs = append(globalAppServerArgs, options.AppServerArgs...)
	options.WorkingDir, err = normalizeWorkingDir(options.WorkingDir)
	if err != nil {
		return err
	}
	readStdin := strings.TrimSpace(options.Prompt) == "" || strings.TrimSpace(options.Prompt) == "-" || runOptions.Stdin != nil
	if readStdin {
		reader := runOptions.Stdin
		if reader == nil {
			reader = os.Stdin
		}
		raw, readErr := io.ReadAll(io.LimitReader(reader, 16<<20))
		if readErr != nil {
			return readErr
		}
		stdinText := strings.TrimSpace(string(raw))
		if strings.TrimSpace(options.Prompt) == "" || strings.TrimSpace(options.Prompt) == "-" {
			options.Prompt = stdinText
		} else if stdinText != "" {
			options.Prompt += "\n\n<stdin>\n" + stdinText + "\n</stdin>"
		}
	}
	if options.Prompt == "" {
		return fmt.Errorf("codex exec prompt is required")
	}

	installOptions := codexInstallOptions{}
	if useProxy {
		if profile == nil {
			return fmt.Errorf("proxy mode enabled but no profile configured")
		}
		installOptions.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, *profile, instances, runInstall)
		}
	}
	configPath := ""
	if root != nil {
		configPath = root.configPath
	}
	paths, err := resolveEffectiveLaunchPaths(configPath, "", options.WorkingDir)
	if err != nil {
		return err
	}
	allowAutomaticUpgrade := codexPathAllowsAutomaticUpgrade(codexPath)
	runtimeContract, err := resolveCodexBrokerRuntimeForLaunch(ctx, codexPath, runOptions.Log, installOptions, allowAutomaticUpgrade, paths.ExecIdentity, runOptions.ExtraEnv)
	if err != nil {
		return err
	}
	codexPath = runtimeContract.Command
	if err := prepareRuntimeMigration(store, paths, codexPath, runOptions.Log); err != nil {
		return err
	}
	extraEnv := mergeCLIEnvironment(runtimeContract.Environment, codexHomeEnv(paths.CodexDir))
	proxyURL := ""
	if useProxy {
		proxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *profile, instances, runOptions.Log)
		if err != nil {
			return err
		}
		extraEnv = env.WithProxy(extraEnv, proxyURL)
	}
	modelLaunch, cleanup, err := startModelProfileAdapterForCodex(ctx, store, runOptions.ModelProfileRef, runOptions.ModelProfileSnapshot, proxyURL, runOptions.Log)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	if modelLaunch.Enabled {
		args := appendCodexModelProfileArgs([]string{"codex"}, modelLaunch)
		options.AppServerArgs = append(options.AppServerArgs, args[1:]...)
		extraEnv = append(extraEnv, envCXPResponsesProxyKey+"="+modelLaunch.ProxyKey)
	}
	configureIdentity := func(command *exec.Cmd) error {
		updated, err := applyExecIdentity(command, command.Env, paths.ExecIdentity)
		if err != nil {
			return err
		}
		command.Env = updated
		return nil
	}
	runner := &codexrunner.AppServerRunner{
		Starter: codexrunner.PolicyAppServerStarter{
			ServerOptions: responsespolicy.ServerOptions{ProxyURL: proxyURL},
			ReadyHook:     runtimeMigrationReadyHook(store, paths, codexPath, runOptions.Log),
		},
		Command:       codexPath,
		AppServerArgs: append([]string{"--analytics-default-enabled"}, options.AppServerArgs...),
		ExtraEnv:      extraEnv,
		WorkingDir:    options.WorkingDir,
		Timeout:       0,
	}
	if runOptions.AgentAutoApprove {
		runner.ApprovalMode = codexrunner.ApprovalModeAutomatic
	}
	// AppServerRunner starts processes lazily and currently exposes no command
	// hook. Bind the effective identity through a tiny starter wrapper.
	runner.Starter = configureAppServerStarter{base: runner.Starter, configure: configureIdentity}
	defer runner.Close()

	stdout := runOptions.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	var writerMu = make(chan struct{}, 1)
	writerMu <- struct{}{}
	handler := codexrunner.EventHandler(nil)
	if options.JSONOutput {
		handler = func(event codexrunner.StreamEvent) {
			line := codexExecJSONEvent(event)
			if len(line) == 0 {
				return
			}
			<-writerMu
			_, _ = stdout.Write(append(line, '\n'))
			writerMu <- struct{}{}
		}
	}
	input := codexrunner.TurnInput{
		Prompt:         options.Prompt,
		ImagePaths:     options.ImagePaths,
		AdditionalDirs: options.AdditionalDirs,
		OutputSchema:   options.OutputSchema,
		WorkingDir:     options.WorkingDir,
		EventHandler:   handler,
		Ephemeral:      options.Ephemeral,
	}
	var result codexrunner.TurnResult
	if options.Resume {
		threadID := strings.TrimSpace(options.ThreadID)
		resumeWorkingDir := options.WorkingDir
		if options.ResumeAll {
			resumeWorkingDir = ""
		}
		if options.ResumeLast {
			threads, listErr := runner.ListThreads(ctx, codexrunner.ListThreadsOptions{WorkingDir: resumeWorkingDir, Limit: 1})
			if listErr != nil {
				return listErr
			}
			if len(threads) == 0 {
				return fmt.Errorf("no Codex thread is available to resume")
			}
			threadID = threads[0].ID
		}
		if threadID != "" && !options.ResumeLast {
			threads, listErr := runner.ListThreads(ctx, codexrunner.ListThreadsOptions{
				WorkingDir: resumeWorkingDir,
				Limit:      1000,
			})
			if listErr == nil {
				for _, thread := range threads {
					if thread.ID == threadID || thread.Name == threadID {
						threadID = thread.ID
						break
					}
				}
			}
		}
		if threadID == "" {
			return fmt.Errorf("codex exec resume requires a thread id or --last")
		}
		result, err = runner.ResumeThread(ctx, threadID, input)
	} else {
		result, err = runner.StartThread(ctx, input)
	}
	if err != nil {
		return err
	}
	if options.OutputLast != "" {
		if err := os.MkdirAll(filepath.Dir(options.OutputLast), 0o700); err != nil && filepath.Dir(options.OutputLast) != "." {
			return err
		}
		if err := os.WriteFile(options.OutputLast, []byte(result.FinalAgentMessage), 0o600); err != nil {
			return err
		}
	}
	if !options.JSONOutput && strings.TrimSpace(result.FinalAgentMessage) != "" {
		_, err = fmt.Fprintln(stdout, result.FinalAgentMessage)
	}
	return err
}

func codexReviewArgsToExecArgs(args []string) ([]string, error) {
	args = expandCodexOptionEquals(migration.RemoveLegacyCodexExecutionOverrides(args))
	var passthrough []string
	var prompt []string
	var target string
	var title string
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		switch arg {
		case "--uncommitted":
			if target != "" {
				return nil, fmt.Errorf("review target specified more than once")
			}
			target = "Review the staged, unstaged, and untracked changes in the current repository."
		case "--base", "--commit":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			if target != "" {
				return nil, fmt.Errorf("review target specified more than once")
			}
			index++
			if arg == "--base" {
				target = "Review the current changes against base branch " + args[index] + "."
			} else {
				target = "Review the changes introduced by commit " + args[index] + "."
			}
		case "--title":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("--title requires a value")
			}
			index++
			title = args[index]
		case "-c", "--config", "--enable", "--disable", "-m", "--model", "-o", "--output-last-message", "--output-schema":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			passthrough = append(passthrough, arg, args[index+1])
			index++
		case "--strict-config", "--skip-git-repo-check", "--ephemeral", "--json":
			passthrough = append(passthrough, arg)
		default:
			if strings.HasPrefix(arg, "-") && arg != "-" {
				return nil, fmt.Errorf("codex review option %q is not supported by the app-server facade", arg)
			}
			prompt = append(prompt, args[index])
		}
	}
	if len(prompt) > 0 {
		if target != "" {
			return nil, fmt.Errorf("custom review instructions cannot be combined with a review target")
		}
		target = strings.Join(prompt, " ")
	}
	if target == "" || target == "-" {
		if target == "-" {
			passthrough = append(passthrough, "-")
			return passthrough, nil
		}
		target = "Review the staged, unstaged, and untracked changes in the current repository."
	}
	if strings.TrimSpace(title) != "" {
		target += " Use this review title: " + title + "."
	}
	return append(passthrough, target), nil
}

type configureAppServerStarter struct {
	base      codexrunner.AppServerTransportStarter
	configure func(*exec.Cmd) error
}

func (s configureAppServerStarter) StartAppServer(ctx context.Context, request codexrunner.AppServerStartRequest) (codexrunner.AppServerLineTransport, error) {
	request.ConfigureCommand = s.configure
	return s.base.StartAppServer(ctx, request)
}

func codexExecJSONEvent(event codexrunner.StreamEvent) []byte {
	var value any
	switch event.Kind {
	case codexrunner.StreamEventThreadStarted:
		value = map[string]any{"type": "thread.started", "thread_id": event.ThreadID}
	case codexrunner.StreamEventAgentMessage:
		value = map[string]any{"type": "item.completed", "item": map[string]any{"id": event.ItemID, "type": "agent_message", "text": event.Text}}
	case codexrunner.StreamEventCommandStarted:
		value = map[string]any{"type": "item.started", "item": map[string]any{"id": event.ItemID, "type": "command_execution", "command": event.Command, "status": event.Status}}
	case codexrunner.StreamEventCommandCompleted:
		value = map[string]any{"type": "item.completed", "item": map[string]any{"id": event.ItemID, "type": "command_execution", "command": event.Command, "aggregated_output": event.AggregatedOutput, "exit_code": event.ExitCode, "status": event.Status}}
	case codexrunner.StreamEventTurnCompleted:
		value = map[string]any{"type": "turn.completed", "thread_id": event.ThreadID, "turn_id": event.TurnID, "usage": event.Usage}
	case codexrunner.StreamEventStreamRetry:
		value = map[string]any{"type": "turn.retrying", "thread_id": event.ThreadID, "turn_id": event.TurnID, "error": event.Failure}
	case codexrunner.StreamEventTurnFailed:
		value = map[string]any{"type": "turn.failed", "thread_id": event.ThreadID, "turn_id": event.TurnID, "error": event.Failure}
	default:
		return nil
	}
	raw, _ := json.Marshal(value)
	return raw
}
