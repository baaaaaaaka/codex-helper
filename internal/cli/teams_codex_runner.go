package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/migration"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
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
	command := strings.TrimSpace(codexPath)
	if command == "" {
		command = "codex"
	}
	appServerExtraArgs, err := translateTeamsCodexArgsToAppServer(codexArgs)
	if err != nil {
		return nil, err
	}
	var runner codexrunner.Runner
	store, paths, err := newRootStore(root, "")
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(runnerName)) {
	case "", "exec", "appserver", "app-server":
		appServerModelArgs, appServerModelEnv, modelCleanup, err := prepareTeamsAppServerModelProfileForRunner(ctx, root, modelProfile, snapshot, log)
		if err != nil {
			return nil, err
		}
		runner = &codexrunner.AppServerRunner{
			Starter: codexrunner.PolicyAppServerStarter{
				ReadyHook: runtimeMigrationReadyHook(store, paths, command, log),
			},
			Command:       command,
			AppServerArgs: append(append([]string{"--analytics-default-enabled"}, appServerExtraArgs...), appServerModelArgs...),
			ExtraEnv:      append(teamsCodexChildEnv(), appServerModelEnv...),
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
	}, nil
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
		Prompt:       input.Prompt,
		ImagePaths:   append([]string{}, input.ImagePaths...),
		WorkingDir:   workDir,
		Timeout:      e.timeout,
		EventHandler: handler,
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
	args = migration.RemoveLegacyCodexExecutionOverrides(args)
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
		default:
			return nil, fmt.Errorf("Teams Codex argument %q cannot be translated to app-server", arg)
		}
	}
	return out, nil
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
