package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

type teamsCodexExecutor struct {
	runner  codexrunner.Runner
	workDir string
	timeout time.Duration
}

func newManagedTeamsCodexExecutor(
	root *rootOptions,
	runnerName string,
	codexPath string,
	workDir string,
	codexArgs []string,
	timeout time.Duration,
	log io.Writer,
) (teams.Executor, error) {
	command := strings.TrimSpace(codexPath)
	if command == "" {
		command = "codex"
	}
	launcher := teamsCodexLauncher{root: root, log: log}
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
		runner = &codexrunner.AppServerRunner{
			Starter:    codexrunner.AppServerProcessStarter{},
			Fallback:   execRunner,
			Command:    command,
			ExtraArgs:  append([]string{}, codexArgs...),
			WorkingDir: strings.TrimSpace(workDir),
			Timeout:    timeout,
		}
	default:
		return nil, fmt.Errorf("unknown Teams codex runner %q (expected exec or appserver)", runnerName)
	}
	return teamsCodexExecutor{
		runner:  runner,
		workDir: strings.TrimSpace(workDir),
		timeout: timeout,
	}, nil
}

func (e teamsCodexExecutor) Run(ctx context.Context, session *teams.Session, prompt string) (teams.ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e teamsCodexExecutor) RunWithEventHandler(ctx context.Context, session *teams.Session, prompt string, handler codexrunner.EventHandler) (teams.ExecutionResult, error) {
	input := codexrunner.TurnInput{
		Prompt:       prompt,
		WorkingDir:   e.workDir,
		Timeout:      e.timeout,
		EventHandler: handler,
	}
	var result codexrunner.TurnResult
	var err error
	if session != nil && strings.TrimSpace(session.CodexThreadID) != "" {
		result, err = e.runner.ResumeThread(ctx, session.CodexThreadID, input)
	} else {
		result, err = e.runner.StartThread(ctx, input)
	}
	if err != nil {
		out := teams.ExecutionResult{CodexThreadID: result.ThreadID, CodexTurnID: result.TurnID}
		if result.ThreadID != "" || result.TurnID != "" || result.Status == codexrunner.TurnStatusStarted || result.Status == codexrunner.TurnStatusInProgress {
			return out, &teams.AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: err}
		}
		return out, err
	}
	text := strings.TrimSpace(result.FinalAgentMessage)
	if text == "" {
		text = "(Codex finished without a final message.)"
	}
	return teams.ExecutionResult{Text: text, CodexThreadID: result.ThreadID, CodexTurnID: result.TurnID}, nil
}

type teamsCodexLauncher struct {
	root *rootOptions
	log  io.Writer
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
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	log := l.log
	if log == nil {
		log = io.Discard
	}
	store, _, err := newRootStore(l.root, "")
	if err != nil {
		return codexrunner.LaunchResult{}, err
	}
	useProxy, _, err := ensureProxyPreferenceRunFn(ctx, store, "", log)
	if err != nil {
		return codexrunner.LaunchResult{}, err
	}

	stdoutWriter := codexrunner.NewEventStreamWriter(&stdout, req.EventHandler)
	opts := runTargetOptions{
		Cwd:      strings.TrimSpace(req.Dir),
		UseProxy: useProxy,
		Log:      log,
		Stdin:    strings.NewReader(req.Stdin),
		Stdout:   stdoutWriter,
		Stderr:   &stderr,
	}

	var runErr error
	if useProxy {
		profile, cfgWithProfile, err := ensureProfileRunFn(ctx, store, "", true, log)
		if err != nil {
			return codexrunner.LaunchResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, err
		}
		runErr = runWithProfileOptions(ctx, store, profile, cfgWithProfile.Instances, cmdArgs, opts)
	} else {
		runErr = runTeamsCodexDirect(ctx, cmdArgs, log, stdoutWriter, &stderr, opts)
	}

	result := codexrunner.LaunchResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
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
	}
	opts.UseProxy = false
	opts.Stdout = stdout
	opts.Stderr = stderr
	return runTargetWithFallbackWithOptionsFn(ctx, resolvedCmd, "", nil, nil, opts)
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
