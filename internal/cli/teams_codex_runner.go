package cli

import (
	"bytes"
	"context"
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
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

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
	codexArgs = teamsCodexYoloSafeArgs(codexArgs)
	launcher := teamsCodexLauncher{root: root, log: log, modelProfileRef: strings.TrimSpace(modelProfile), modelProfileSnapshot: snapshot}
	execRunner := &codexrunner.ExecRunner{
		Launcher:   launcher,
		Command:    command,
		ExtraArgs:  append([]string{}, codexArgs...),
		WorkingDir: strings.TrimSpace(workDir),
		Timeout:    timeout,
	}
	var runner codexrunner.Runner = execRunner
	switch strings.ToLower(strings.TrimSpace(runnerName)) {
	case "", "exec":
	case "appserver", "app-server":
		appServerModelArgs, appServerModelEnv, err := prepareTeamsAppServerModelProfileWithContext(ctx, root, modelProfile, snapshot, log)
		if err != nil {
			return nil, err
		}
		runner = &codexrunner.AppServerRunner{
			Starter:       codexrunner.AppServerProcessStarter{},
			Fallback:      execRunner,
			Command:       command,
			AppServerArgs: append([]string{}, appServerModelArgs...),
			ExtraArgs:     append([]string{}, codexArgs...),
			ExtraEnv:      append(teamsCodexChildEnv(), appServerModelEnv...),
			WorkingDir:    strings.TrimSpace(workDir),
			Timeout:       timeout,
		}
	default:
		return nil, fmt.Errorf("unknown Teams codex runner %q (expected exec or appserver)", runnerName)
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

type teamsCodexLauncher struct {
	root                 *rootOptions
	log                  io.Writer
	modelProfileRef      string
	modelProfileSnapshot modelprofile.Snapshot
}

func (l teamsCodexLauncher) Launch(ctx context.Context, req codexrunner.LaunchRequest) (codexrunner.LaunchResult, error) {
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	command := strings.TrimSpace(req.Command)
	if command == "" {
		command = "codex"
	}
	cmdArgs := append([]string{command}, req.Args...)
	var stderr bytes.Buffer
	stdout := codexrunner.NewLaunchOutputRecorderWithOptions(req.EventHandler, codexrunner.LaunchOutputOptions{IncludeCommandOutput: false, RecoverParseErrors: true})

	log := l.log
	if log == nil {
		log = io.Discard
	}
	store, _, err := newRootStore(l.root, "")
	if err != nil {
		return codexrunner.LaunchResult{}, err
	}
	proxyRef := ""
	useProxy := false
	if l.modelProfileRef != "" || !l.modelProfileSnapshot.IsZero() {
		cfg, err := store.Load()
		if err != nil {
			return codexrunner.LaunchResult{}, err
		}
		var resolved modelprofile.Resolved
		if l.modelProfileSnapshot.IsZero() {
			resolved, err = modelprofile.Resolve(cfg, l.modelProfileRef)
		} else {
			resolved, err = modelprofile.ResolveSnapshot(cfg, l.modelProfileSnapshot)
		}
		if err != nil {
			return codexrunner.LaunchResult{}, err
		}
		if resolved.SSHProfile != nil {
			useProxy = true
			proxyRef = resolved.SSHProfile.Name
		}
	}
	if !useProxy {
		useProxy, _, err = ensureProxyPreferenceRunFn(ctx, store, "", log)
		if err != nil {
			return codexrunner.LaunchResult{}, err
		}
	}

	stdoutWriter := stdout.StdoutWriter()
	opts := runTargetOptions{
		Cwd:                  strings.TrimSpace(req.Dir),
		ExtraEnv:             teamsCodexChildEnv(),
		UseProxy:             useProxy,
		Log:                  log,
		Stdin:                strings.NewReader(req.Stdin),
		Stdout:               stdoutWriter,
		Stderr:               &stderr,
		YoloEnabled:          true,
		RequireYolo:          true,
		ModelProfileRef:      l.modelProfileRef,
		ModelProfileSnapshot: l.modelProfileSnapshot,
	}

	var runErr error
	if useProxy {
		profile, cfgWithProfile, err := ensureProfileRunFn(ctx, store, proxyRef, true, log)
		if err != nil {
			return stdout.LaunchResult(stderr.Bytes(), 0), err
		}
		runErr = runWithProfileOptions(ctx, store, profile, cfgWithProfile.Instances, cmdArgs, opts)
	} else {
		runErr = runTeamsCodexDirect(ctx, store, cmdArgs, log, stdoutWriter, &stderr, opts)
	}

	result := stdout.LaunchResult(stderr.Bytes(), 0)
	if runErr == nil {
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	return result, runErr
}

func runTeamsCodexDirect(
	ctx context.Context,
	store *config.Store,
	cmdArgs []string,
	log io.Writer,
	stdout io.Writer,
	stderr io.Writer,
	opts runTargetOptions,
) error {
	resolvedCmd, err := resolveRunCommandWithInstallOptions(ctx, cmdArgs, log, codexInstallOptions{})
	if err != nil {
		return err
	}
	if isCodexCommand(resolvedCmd[0]) {
		if err := applyDefaultCodexExecutionContext(&opts); err != nil {
			return err
		}
		var modelCleanup func()
		resolvedCmd, modelCleanup, err = prepareCodexModelProfileForRun(ctx, store, resolvedCmd, &opts, "")
		if err != nil {
			return err
		}
		if modelCleanup != nil {
			defer modelCleanup()
		}
		var cleanup func()
		resolvedCmd, cleanup = prepareYoloCodexCommandForRun(store, resolvedCmd, &opts)
		if cleanup != nil {
			defer cleanup()
		}
		if err := requireYoloLaunchArgs(resolvedCmd, opts); err != nil {
			return err
		}
	}
	opts.UseProxy = false
	opts.Stdout = stdout
	opts.Stderr = stderr
	return runTargetWithFallbackWithOptionsFn(ctx, resolvedCmd, "", nil, nil, opts)
}

func teamsCodexYoloSafeArgs(args []string) []string {
	return stripYoloArgs(args)
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
