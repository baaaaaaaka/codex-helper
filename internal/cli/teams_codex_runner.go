package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/env"
	"github.com/baaaaaaaka/codex-helper/internal/migration"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

var prepareTeamsAppServerModelProfileForRunner = prepareTeamsAppServerModelProfileWithContext

type teamsCodexExecutor struct {
	runner               codexrunner.Runner
	workDir              string
	timeout              time.Duration
	root                 *rootOptions
	runnerName           string
	codexPath            string
	codexArgs            []string
	modelProfileRef      string
	modelProfileSnapshot modelprofile.Snapshot
	log                  io.Writer
	runnerCacheMu        *sync.Mutex
	runnersByProfile     map[string]codexrunner.Runner
	staticImages         []string
	additionalDirs       []string
	outputSchema         json.RawMessage
	ephemeral            bool
}

func (e teamsCodexExecutor) Close() error {
	if e.runnerCacheMu != nil {
		e.runnerCacheMu.Lock()
		defer e.runnerCacheMu.Unlock()
	}
	seen := make(map[*codexrunner.AppServerRunner]bool)
	var errs []error
	closeRunner := func(runner codexrunner.Runner) {
		managed, ok := runner.(*codexrunner.AppServerRunner)
		if !ok || managed == nil || seen[managed] {
			return
		}
		seen[managed] = true
		if err := managed.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	closeRunner(e.runner)
	for _, runner := range e.runnersByProfile {
		closeRunner(runner)
	}
	return errors.Join(errs...)
}

func newManagedTeamsCodexExecutor(
	root *rootOptions,
	runnerName string,
	codexPath string,
	workDir string,
	codexArgs []string,
	modelProfile string,
	timeout time.Duration,
	log io.Writer,
) (teams.Executor, error) {
	return newManagedTeamsCodexExecutorWithContext(context.Background(), root, runnerName, codexPath, workDir, codexArgs, modelProfile, modelprofile.Snapshot{}, timeout, log)
}

func newManagedTeamsCodexExecutorWithSnapshot(
	root *rootOptions,
	runnerName string,
	codexPath string,
	workDir string,
	codexArgs []string,
	modelProfile string,
	snapshot modelprofile.Snapshot,
	timeout time.Duration,
	log io.Writer,
) (teams.Executor, error) {
	return newManagedTeamsCodexExecutorWithContext(context.Background(), root, runnerName, codexPath, workDir, codexArgs, modelProfile, snapshot, timeout, log)
}

func newManagedTeamsCodexExecutorWithContext(
	ctx context.Context,
	root *rootOptions,
	runnerName string,
	codexPath string,
	workDir string,
	codexArgs []string,
	modelProfile string,
	snapshot modelprofile.Snapshot,
	timeout time.Duration,
	log io.Writer,
) (teams.Executor, error) {
	rawCommand := strings.TrimSpace(codexPath)
	command := rawCommand
	if command == "" {
		command = "codex"
	}
	appServerExtraArgs, err := translateTeamsCodexArgsToAppServer(codexArgs)
	if err != nil {
		return nil, err
	}
	turnOptions, err := parseTeamsCodexTurnOptions(codexArgs)
	if err != nil {
		return nil, err
	}
	if turnOptions.WorkingDir != "" {
		workDir = turnOptions.WorkingDir
	}
	var runner codexrunner.Runner
	store, _, err := newRootStore(root, "")
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(runnerName)) {
	case "", "exec", "appserver", "app-server":
		configPath := ""
		if root != nil {
			configPath = root.configPath
		}
		paths, err := resolveEffectiveLaunchPaths(configPath, "", workDir)
		if err != nil {
			return nil, err
		}
		appServerModelArgs, appServerModelEnv, modelCleanup, err := prepareTeamsAppServerModelProfileForRunner(ctx, root, modelProfile, snapshot, log)
		if err != nil {
			return nil, err
		}
		extraEnv := append(teamsCodexChildEnv(), codexHomeEnv(paths.CodexDir)...)
		extraEnv = append(extraEnv, appServerModelEnv...)
		runner = &codexrunner.AppServerRunner{
			Starter:       teamsPolicyAppServerStarter{store: store, paths: paths, rawCommand: rawCommand, log: log},
			Command:       command,
			AppServerArgs: append(append([]string{"--analytics-default-enabled"}, appServerExtraArgs...), appServerModelArgs...),
			ExtraEnv:      extraEnv,
			WorkingDir:    strings.TrimSpace(workDir),
			Timeout:       timeout,
			CloseHook:     modelCleanup,
		}
	default:
		return nil, fmt.Errorf("unknown Teams codex runner %q", runnerName)
	}
	return teamsCodexExecutor{
		runner:               runner,
		workDir:              strings.TrimSpace(workDir),
		timeout:              timeout,
		root:                 root,
		runnerName:           runnerName,
		codexPath:            codexPath,
		codexArgs:            append([]string{}, codexArgs...),
		modelProfileRef:      strings.TrimSpace(modelProfile),
		modelProfileSnapshot: snapshot,
		log:                  log,
		runnerCacheMu:        &sync.Mutex{},
		runnersByProfile:     map[string]codexrunner.Runner{},
		staticImages:         turnOptions.ImagePaths,
		additionalDirs:       turnOptions.AdditionalDirs,
		outputSchema:         turnOptions.OutputSchema,
		ephemeral:            turnOptions.Ephemeral,
	}, nil
}

type teamsPolicyAppServerStarter struct {
	store      *config.Store
	paths      effectivePaths
	rawCommand string
	log        io.Writer
}

func (s teamsPolicyAppServerStarter) StartAppServer(ctx context.Context, request codexrunner.AppServerStartRequest) (codexrunner.AppServerLineTransport, error) {
	cfg, err := s.store.Load()
	if err != nil {
		return nil, err
	}
	proxyURL := ""
	installOptions := codexInstallOptions{}
	if upgradeUsesProxy(cfg) {
		profile, selectErr := selectProfile(cfg, "")
		if selectErr != nil {
			return nil, selectErr
		}
		proxyURL, err = codexAppEnsureProxyURLFn(ctx, s.store, profile, cfg.Instances, s.log)
		if err != nil {
			return nil, err
		}
		installOptions.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, s.store, profile, cfg.Instances, runInstall)
		}
		request.ExtraEnv = env.WithProxy(request.ExtraEnv, proxyURL)
	}
	command, err := ensureCodexBrokerRuntime(ctx, s.rawCommand, s.log, installOptions, codexPathAllowsAutomaticUpgrade(s.rawCommand))
	if err != nil {
		return nil, err
	}
	if err := prepareRuntimeMigration(s.store, s.paths, command, s.log); err != nil {
		return nil, err
	}
	request.Command = command
	request.ConfigureCommand = func(process *exec.Cmd) error {
		updated, applyErr := applyExecIdentity(process, process.Env, s.paths.ExecIdentity)
		if applyErr != nil {
			return applyErr
		}
		process.Env = updated
		return nil
	}
	return (codexrunner.PolicyAppServerStarter{
		ServerOptions: responsespolicy.ServerOptions{ProxyURL: proxyURL},
		ReadyHook:     runtimeMigrationReadyHook(s.store, s.paths, command, s.log),
	}).StartAppServer(ctx, request)
}

func (e teamsCodexExecutor) Run(ctx context.Context, session *teams.Session, prompt string) (teams.ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e teamsCodexExecutor) RunWithEventHandler(ctx context.Context, session *teams.Session, prompt string, handler codexrunner.EventHandler) (teams.ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, teams.ExecutionInput{Prompt: prompt}, handler)
}

func (e teamsCodexExecutor) RunInput(ctx context.Context, session *teams.Session, input teams.ExecutionInput) (teams.ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, input, nil)
}

func teamsCodexEffectiveWorkDir(session *teams.Session, fallback string) string {
	if session != nil {
		if cwd := strings.TrimSpace(session.Cwd); cwd != "" {
			return cwd
		}
	}
	return strings.TrimSpace(fallback)
}

func (e teamsCodexExecutor) RunInputWithEventHandler(ctx context.Context, session *teams.Session, input teams.ExecutionInput, handler codexrunner.EventHandler) (teams.ExecutionResult, error) {
	workDir := teamsCodexEffectiveWorkDir(session, e.workDir)
	runner, err := e.runnerForSessionProfile(ctx, session)
	if err != nil {
		return teams.ExecutionResult{}, err
	}
	turnInput := codexrunner.TurnInput{
		Prompt:         input.Prompt,
		ImagePaths:     append(append([]string{}, e.staticImages...), input.ImagePaths...),
		AdditionalDirs: append([]string{}, e.additionalDirs...),
		OutputSchema:   append(json.RawMessage(nil), e.outputSchema...),
		WorkingDir:     workDir,
		Timeout:        e.timeout,
		EventHandler:   handler,
		Ephemeral:      e.ephemeral,
	}
	if session != nil && teams.SessionAllowsAutoTitleUpdate(*session) {
		turnInput.BackfillThreadName = true
	}
	var result codexrunner.TurnResult
	if session != nil && strings.TrimSpace(session.CodexThreadID) != "" {
		result, err = runner.ResumeThread(ctx, session.CodexThreadID, turnInput)
	} else {
		result, err = runner.StartThread(ctx, turnInput)
	}
	if err != nil {
		if teamsCodexTurnCompletedDespiteCanceledError(result, err) {
			out := successfulTeamsExecutionResultFromCodexTurn(result)
			if session != nil {
				expectedThreadID := strings.TrimSpace(session.CodexThreadID)
				if expectedThreadID != "" && out.CodexThreadID != "" && out.CodexThreadID != expectedThreadID {
					return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, expectedThreadID)
				}
			}
			return out, nil
		}
		out := teamsExecutionResultFromCodexTurn(result)
		if teamsCodexTurnMayStillBeRunning(result) {
			return out, &teams.AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: err}
		}
		return out, err
	}
	out := successfulTeamsExecutionResultFromCodexTurn(result)
	if session != nil {
		expectedThreadID := strings.TrimSpace(session.CodexThreadID)
		if expectedThreadID != "" && out.CodexThreadID != "" && out.CodexThreadID != expectedThreadID {
			return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, expectedThreadID)
		}
	}
	return out, nil
}

func (e teamsCodexExecutor) runnerForSessionProfile(ctx context.Context, session *teams.Session) (codexrunner.Runner, error) {
	if session == nil || session.ModelProfile.IsZero() {
		return e.runner, nil
	}
	if modelProfileSnapshotKey(session.ModelProfile) == modelProfileSnapshotKey(e.modelProfileSnapshot) {
		return e.runner, nil
	}
	key := modelProfileSnapshotKey(session.ModelProfile)
	if e.runnerCacheMu == nil || e.runnersByProfile == nil {
		return e.runner, nil
	}
	e.runnerCacheMu.Lock()
	defer e.runnerCacheMu.Unlock()
	if runner, ok := e.runnersByProfile[key]; ok {
		return runner, nil
	}
	executor, err := newManagedTeamsCodexExecutorWithContext(ctx, e.root, e.runnerName, e.codexPath, e.workDir, e.codexArgs, "", session.ModelProfile, e.timeout, e.log)
	if err != nil {
		return nil, err
	}
	teamsExecutor, ok := executor.(teamsCodexExecutor)
	if !ok {
		return nil, fmt.Errorf("model profile executor type = %T, want teamsCodexExecutor", executor)
	}
	e.runnersByProfile[key] = teamsExecutor.runner
	return teamsExecutor.runner, nil
}

func modelProfileSnapshotKey(snapshot modelprofile.Snapshot) string {
	if snapshot.IsZero() {
		return ""
	}
	return strings.Join([]string{
		strings.TrimSpace(snapshot.Name),
		strings.TrimSpace(snapshot.Provider),
		strings.TrimSpace(snapshot.APIKeyRef),
		strings.TrimSpace(snapshot.Model),
		strings.TrimSpace(snapshot.SSHProxy),
		fmt.Sprint(snapshot.Revision),
		strings.TrimSpace(snapshot.KeyFingerprint),
		strings.TrimSpace(snapshot.BaseURLHash),
		strings.TrimSpace(snapshot.AdapterProfile),
		strings.TrimSpace(snapshot.DefaultModel),
		strings.TrimSpace(snapshot.ModelFingerprint),
		strings.TrimSpace(snapshot.CatalogFingerprint),
		strings.TrimSpace(snapshot.SSHProxyFingerprint),
	}, "\x00")
}

func teamsCodexTurnCompletedDespiteCanceledError(result codexrunner.TurnResult, err error) bool {
	return err != nil &&
		(errors.Is(err, context.Canceled) || codexrunner.IsKind(err, codexrunner.ErrorCanceled)) &&
		result.Status == codexrunner.TurnStatusCompleted &&
		result.Failure == nil
}

func teamsCodexTurnMayStillBeRunning(result codexrunner.TurnResult) bool {
	switch result.Status {
	case codexrunner.TurnStatusStarted, codexrunner.TurnStatusInProgress:
		return true
	case codexrunner.TurnStatusUnknown:
		return strings.TrimSpace(result.TurnID) != ""
	default:
		return false
	}
}

func teamsExecutionResultFromCodexTurn(result codexrunner.TurnResult) teams.ExecutionResult {
	return teams.ExecutionResult{
		Text:             strings.TrimSpace(result.FinalAgentMessage),
		CodexThreadID:    result.ThreadID,
		CodexThreadTitle: strings.TrimSpace(result.ThreadName),
		CodexTurnID:      result.TurnID,
	}
}

func successfulTeamsExecutionResultFromCodexTurn(result codexrunner.TurnResult) teams.ExecutionResult {
	text := strings.TrimSpace(result.FinalAgentMessage)
	if text == "" {
		text = "(Codex finished without a final message.)"
	}
	return teams.ExecutionResult{Text: text, CodexThreadID: result.ThreadID, CodexThreadTitle: strings.TrimSpace(result.ThreadName), CodexTurnID: result.TurnID}
}

func translateTeamsCodexArgsToAppServer(args []string) ([]string, error) {
	args = expandCodexOptionEquals(migration.RemoveLegacyCodexExecutionOverrides(args))
	out := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		switch arg {
		case "":
			continue
		case "--skip-git-repo-check":
			// app-server does not perform the exec subcommand's repository gate.
			continue
		case "--model", "-m":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a model", arg)
			}
			index++
			out = append(out, "-c", `model="`+tomlEscapeString(args[index])+`"`)
		case "--search":
			out = append(out, "-c", `web_search="live"`)
		case "--oss":
			out = append(out, "-c", `model_provider="ollama"`)
		case "--local-provider":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("--local-provider requires a value")
			}
			index++
			out = append(out, "-c", `model_provider="`+tomlEscapeString(args[index])+`"`)
		case "--dangerously-bypass-hook-trust":
			out = append(out, "-c", "bypass_hook_trust=true")
		case "-c", "--config", "--enable", "--disable":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			out = append(out, arg, args[index+1])
			index++
		case "--strict-config":
			out = append(out, arg)
		case "--sandbox", "-s", "--ask-for-approval", "-a":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			// The unified runtime owns these settings and appends its policy last.
			index++
		case "--ephemeral", "--json":
			// Ephemeral and turn inputs are carried over the app-server protocol.
		case "-C", "--cd", "--add-dir", "-i", "--image", "--output-schema", "-o", "--output-last-message", "--color":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			index++
		default:
			return nil, fmt.Errorf("Teams Codex argument %q cannot be translated to app-server", arg)
		}
	}
	return out, nil
}

type teamsCodexTurnOptions struct {
	WorkingDir     string
	ImagePaths     []string
	AdditionalDirs []string
	OutputSchema   json.RawMessage
	Ephemeral      bool
}

func parseTeamsCodexTurnOptions(args []string) (teamsCodexTurnOptions, error) {
	args = expandCodexOptionEquals(migration.RemoveLegacyCodexExecutionOverrides(args))
	var options teamsCodexTurnOptions
	for index := 0; index < len(args); index++ {
		switch strings.TrimSpace(args[index]) {
		case "--ephemeral":
			options.Ephemeral = true
		case "-C", "--cd":
			if index+1 >= len(args) {
				return options, fmt.Errorf("%s requires a directory", args[index])
			}
			index++
			options.WorkingDir = args[index]
		case "--add-dir":
			if index+1 >= len(args) {
				return options, fmt.Errorf("--add-dir requires a directory")
			}
			index++
			options.AdditionalDirs = append(options.AdditionalDirs, args[index])
		case "-i", "--image":
			if index+1 >= len(args) {
				return options, fmt.Errorf("%s requires a path", args[index])
			}
			index++
			options.ImagePaths = append(options.ImagePaths, args[index])
		case "--output-schema":
			if index+1 >= len(args) {
				return options, fmt.Errorf("--output-schema requires a path")
			}
			index++
			raw, err := os.ReadFile(args[index])
			if err != nil {
				return options, err
			}
			if !json.Valid(raw) {
				return options, fmt.Errorf("output schema is not valid JSON")
			}
			options.OutputSchema = append(json.RawMessage(nil), raw...)
		}
	}
	return options, nil
}

func teamsStoreConfigForStatus(root *rootOptions) (*config.Store, error) {
	store, _, err := newRootStore(root, "")
	if err != nil {
		return nil, err
	}
	return store, nil
}

var upgradeCodexInstalledForTeamsRun = upgradeCodexInstalledWithOptions

func runTeamsUpgradeCodexOnce(cmd interface {
	Context() context.Context
	ErrOrStderr() io.Writer
	OutOrStdout() io.Writer
}, root *rootOptions, codexPath string) error {
	if strings.TrimSpace(codexPath) != "" {
		return fmt.Errorf("--upgrade-codex cannot be used with --codex-path")
	}
	if err := ensureTeamsIdleBeforeCodexUpgrade(cmd.Context()); err != nil {
		return err
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	installOpts := codexInstallOptions{upgradeCodex: true}
	if upgradeUsesProxy(cfg) {
		profile, cfgWithProfile, err := ensureProfileRunFn(cmd.Context(), store, "", true, cmd.ErrOrStderr())
		if err != nil {
			return err
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, profile, cfgWithProfile.Instances, runInstall)
		}
	}
	path, err := upgradeCodexInstalledForTeamsRun(cmd.Context(), cmd.ErrOrStderr(), installOpts)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex upgraded before Teams listen: %s\n", path)
	return nil
}

func runTeamsCodexUpgradeFromBridge(ctx context.Context, root *rootOptions, out io.Writer, codexPath string) (teams.CodexUpgradeResult, error) {
	if strings.TrimSpace(codexPath) != "" {
		return teams.CodexUpgradeResult{}, fmt.Errorf("automatic Codex upgrade cannot be used with --codex-path")
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		return teams.CodexUpgradeResult{}, err
	}
	cfg, err := store.Load()
	if err != nil {
		return teams.CodexUpgradeResult{}, err
	}
	installOpts := codexInstallOptions{upgradeCodex: true}
	if upgradeUsesProxy(cfg) {
		profile, cfgWithProfile, err := ensureProfileRunFn(ctx, store, "", true, out)
		if err != nil {
			return teams.CodexUpgradeResult{}, err
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, profile, cfgWithProfile.Instances, runInstall)
		}
	}
	path, err := upgradeCodexInstalledForTeamsRun(ctx, out, installOpts)
	if err != nil {
		return teams.CodexUpgradeResult{}, err
	}
	return teams.CodexUpgradeResult{Path: path}, nil
}

func newTeamsModelProfileResolver(root *rootOptions) teams.ModelProfileResolver {
	return func(ctx context.Context, ref string) (modelprofile.Snapshot, error) {
		store, _, err := newRootStore(root, "")
		if err != nil {
			return modelprofile.Snapshot{}, err
		}
		cfg, err := store.Load()
		if err != nil {
			return modelprofile.Snapshot{}, err
		}
		resolved, err := modelprofile.Resolve(cfg, ref)
		if err != nil {
			return modelprofile.Snapshot{}, err
		}
		_ = ctx
		secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
		return resolved.RuntimeSnapshot(time.Now(), secrets, os.Getenv)
	}
}
