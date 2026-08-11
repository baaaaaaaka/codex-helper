package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	runner                 codexrunner.Runner
	workDir                string
	timeout                time.Duration
	root                   *rootOptions
	runnerName             string
	codexPath              string
	codexArgs              []string
	modelProfileRef        string
	modelProfileSnapshot   modelprofile.Snapshot
	log                    io.Writer
	runnerCacheMu          *sync.Mutex
	runnersByProfile       map[string]codexrunner.Runner
	runnerKeyBySession     map[string]string
	staticImages           []string
	additionalDirs         []string
	outputSchema           json.RawMessage
	ephemeral              bool
	defaultReasoningEffort string
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

func (e teamsCodexExecutor) RestartCodexRunners() error {
	if e.runnerCacheMu != nil {
		e.runnerCacheMu.Lock()
		defer e.runnerCacheMu.Unlock()
	}
	seen := make(map[*codexrunner.AppServerRunner]bool)
	var errs []error
	restart := func(runner codexrunner.Runner) {
		managed, ok := runner.(*codexrunner.AppServerRunner)
		if !ok || managed == nil || seen[managed] {
			return
		}
		seen[managed] = true
		if err := managed.Restart(); err != nil {
			errs = append(errs, err)
		}
	}
	restart(e.runner)
	for _, runner := range e.runnersByProfile {
		restart(runner)
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
		modelPrepareCtx := withCodexLoginProbePath(ctx, codexPath)
		if contract, runtimeErr := resolveTeamsCodexRuntimeContract(ctx, root, codexPath, workDir, log); runtimeErr == nil {
			modelPrepareCtx = withCodexInvocation(modelPrepareCtx, codexInvocationForRuntime(contract))
		} else if log != nil {
			_, _ = fmt.Fprintf(log, "Teams official model runtime unavailable during profile preparation: %v\n", runtimeErr)
		}
		appServerModelArgs, appServerModelEnv, modelCleanup, err := prepareTeamsAppServerModelProfileForRunner(modelPrepareCtx, root, modelProfile, snapshot, log)
		if err != nil {
			return nil, err
		}
		extraEnv := append(teamsCodexChildEnv(), codexHomeEnv(paths.CodexDir)...)
		extraEnv = append(extraEnv, appServerModelEnv...)
		runner = &codexrunner.AppServerRunner{
			Starter:              teamsPolicyAppServerStarter{store: store, paths: paths, rawCommand: rawCommand, log: log},
			ApprovalMode:         codexrunner.ApprovalModeAutomatic,
			Command:              command,
			AppServerArgs:        append(append([]string{"--analytics-default-enabled"}, appServerExtraArgs...), appServerModelArgs...),
			ExtraEnv:             extraEnv,
			CodexHome:            paths.CodexDir,
			WorkingDir:           strings.TrimSpace(workDir),
			Timeout:              timeout,
			MetadataOnlyResume:   true,
			RequireCompleteFinal: true,
			CloseHook:            modelCleanup,
		}
	default:
		return nil, fmt.Errorf("unknown Teams codex runner %q", runnerName)
	}
	return teamsCodexExecutor{
		runner:                 runner,
		workDir:                strings.TrimSpace(workDir),
		timeout:                timeout,
		root:                   root,
		runnerName:             runnerName,
		codexPath:              codexPath,
		codexArgs:              append([]string{}, codexArgs...),
		modelProfileRef:        strings.TrimSpace(modelProfile),
		modelProfileSnapshot:   snapshot,
		log:                    log,
		runnerCacheMu:          &sync.Mutex{},
		runnersByProfile:       map[string]codexrunner.Runner{},
		runnerKeyBySession:     map[string]string{},
		staticImages:           turnOptions.ImagePaths,
		additionalDirs:         turnOptions.AdditionalDirs,
		outputSchema:           turnOptions.OutputSchema,
		ephemeral:              turnOptions.Ephemeral,
		defaultReasoningEffort: codexReasoningEffortFromArgs(codexArgs),
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
	installOptions := codexInstallOptions{requireManaged: strings.TrimSpace(s.rawCommand) == ""}
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
	pathResult, err := resolveTeamsCodexUserPath(ctx, cfg, s.paths, request.ExtraEnv, request.WorkingDir)
	if err != nil {
		return nil, err
	}
	request.ExtraEnv = setEnvValue(request.ExtraEnv, "PATH", pathResult.Path)
	runtimeContract, err := resolveCodexBrokerRuntimeForLaunch(
		ctx,
		s.rawCommand,
		s.log,
		installOptions,
		codexPathAllowsAutomaticUpgrade(s.rawCommand),
		s.paths.ExecIdentity,
		request.ExtraEnv,
	)
	if err != nil {
		return nil, err
	}
	runtimeContract = applyTeamsUserPathRuntime(runtimeContract, pathResult, s.log)
	request.ExtraEnv = runtimeContract.Environment
	if err := prepareRuntimeMigration(s.store, s.paths, runtimeContract.WrapperCommand, s.log); err != nil {
		return nil, err
	}
	request.Command = runtimeContract.Command
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
		ReadyHook:     runtimeMigrationReadyHook(s.store, s.paths, runtimeContract.WrapperCommand, s.log),
	}).StartAppServer(ctx, request)
}

func (e teamsCodexExecutor) Run(ctx context.Context, session *teams.Session, prompt string) (teams.ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e teamsCodexExecutor) ForkThread(ctx context.Context, session *teams.Session, cutoffCodexTurnID string) (teams.ForkResult, error) {
	if session == nil || strings.TrimSpace(session.CodexThreadID) == "" {
		return teams.ForkResult{}, fmt.Errorf("Codex parent thread is required for fork")
	}
	cutoffCodexTurnID = strings.TrimSpace(cutoffCodexTurnID)
	if cutoffCodexTurnID == "" {
		return teams.ForkResult{}, fmt.Errorf("last completed Codex turn is required for fork")
	}
	runner, err := e.runnerForSessionProfile(ctx, session)
	if err != nil {
		return teams.ForkResult{}, err
	}
	forger, ok := runner.(codexrunner.ThreadForker)
	if !ok {
		return teams.ForkResult{}, codexrunner.UnsupportedError("thread/fork")
	}
	child, err := forger.ForkThread(ctx, codexrunner.ThreadForkParams{
		ThreadID:              strings.TrimSpace(session.CodexThreadID),
		LastTurnID:            cutoffCodexTurnID,
		ExcludeTurns:          true,
		DeferGoalContinuation: true,
		Ephemeral:             false,
		WorkingDir:            teamsCodexEffectiveWorkDir(session, e.workDir),
	})
	if err != nil {
		return teams.ForkResult{}, err
	}
	return teams.ForkResult{CodexThreadID: child.ID, CodexThreadTitle: child.Name}, nil
}

func (e teamsCodexExecutor) ReconcileForkThread(ctx context.Context, session *teams.Session, cutoffCodexTurnID string, windowStart time.Time, windowEnd time.Time) (teams.ForkReconcileResult, error) {
	if session == nil || strings.TrimSpace(session.CodexThreadID) == "" {
		return teams.ForkReconcileResult{}, fmt.Errorf("Codex parent thread is required for fork reconciliation")
	}
	if windowStart.IsZero() || windowEnd.IsZero() || !windowEnd.After(windowStart) {
		return teams.ForkReconcileResult{}, fmt.Errorf("fork reconciliation window is invalid")
	}
	runner, err := e.runnerForSessionProfile(ctx, session)
	if err != nil {
		return teams.ForkReconcileResult{}, err
	}
	const reconciliationThreadLimit = 100
	threads, err := runner.ListThreads(ctx, codexrunner.ListThreadsOptions{
		WorkingDir: teamsCodexEffectiveWorkDir(session, e.workDir),
		Limit:      reconciliationThreadLimit,
	})
	if err != nil {
		return teams.ForkReconcileResult{}, err
	}
	if len(threads) >= reconciliationThreadLimit {
		return teams.ForkReconcileResult{}, fmt.Errorf("thread list reached reconciliation limit; refusing to infer a unique child")
	}
	parentThreadID := strings.TrimSpace(session.CodexThreadID)
	cutoffCodexTurnID = strings.TrimSpace(cutoffCodexTurnID)
	potentialMatches := 0
	verifiedMatches := make([]codexrunner.Thread, 0, len(threads))
	readUnresolved := false
	for _, thread := range threads {
		if strings.TrimSpace(thread.ID) == "" || strings.TrimSpace(thread.ID) == parentThreadID {
			continue
		}
		if strings.TrimSpace(thread.ForkedFromID) != parentThreadID {
			continue
		}
		if !thread.CreatedAt.IsZero() && (thread.CreatedAt.Before(windowStart) || thread.CreatedAt.After(windowEnd)) {
			continue
		}
		potentialMatches++
		read, readErr := runner.ReadThread(ctx, thread.ID)
		if readErr != nil {
			readUnresolved = true
			continue
		}
		if strings.TrimSpace(read.ID) == "" {
			read.ID = thread.ID
		}
		if !read.CreatedAt.IsZero() && (read.CreatedAt.Before(windowStart) || read.CreatedAt.After(windowEnd)) {
			continue
		}
		if read.ForkedFromID != "" && strings.TrimSpace(read.ForkedFromID) != parentThreadID {
			continue
		}
		// The current app-server Thread schema exposes the parent thread but not
		// the fork cutoff. Reading the child with turns included lets us prove
		// that the selected cutoff is its last completed historical turn instead
		// of trusting a non-protocol forkedFromTurnId field.
		if strings.TrimSpace(read.LatestTurnID) != cutoffCodexTurnID {
			continue
		}
		verifiedMatches = append(verifiedMatches, read)
	}
	result := teams.ForkReconcileResult{MatchCount: len(verifiedMatches)}
	if readUnresolved && potentialMatches > result.MatchCount {
		// An unreadable candidate is still a possible duplicate. Do not adopt a
		// different candidate merely because its read happened to succeed first.
		result.MatchCount = potentialMatches
	}
	if len(verifiedMatches) == 1 && !readUnresolved {
		result.Result = teams.ForkResult{
			CodexThreadID:    verifiedMatches[0].ID,
			CodexThreadTitle: firstNonEmptyCLI(verifiedMatches[0].Name),
		}
	}
	return result, nil
}

func (e teamsCodexExecutor) RunWithEventHandler(ctx context.Context, session *teams.Session, prompt string, handler codexrunner.EventHandler) (teams.ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, teams.ExecutionInput{Prompt: prompt}, handler)
}

func (e teamsCodexExecutor) RunInput(ctx context.Context, session *teams.Session, input teams.ExecutionInput) (teams.ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, input, nil)
}

func (e teamsCodexExecutor) DefaultReasoningEffort() string {
	return strings.TrimSpace(firstNonEmptyCLI(e.defaultReasoningEffort, codexReasoningEffortFromArgs(e.codexArgs)))
}

func (e teamsCodexExecutor) ReasoningEffortCatalog(ctx context.Context, session *teams.Session) (teams.ReasoningEffortCatalog, error) {
	runner, err := e.runnerForSessionProfile(ctx, session)
	if err != nil {
		return teams.ReasoningEffortCatalog{}, err
	}
	reader, ok := runner.(codexrunner.ModelCatalogReader)
	if !ok {
		return teams.ReasoningEffortCatalog{}, fmt.Errorf("Codex runner does not expose model/list")
	}
	models, err := reader.ListModels(ctx)
	if err != nil {
		return teams.ReasoningEffortCatalog{}, err
	}
	target := ""
	if session != nil {
		target = strings.TrimSpace(session.ModelProfile.Model)
		if target == "" {
			target = strings.TrimSpace(session.ModelProfile.DefaultModel)
		}
	}
	var selected *codexrunner.ModelInfo
	for i := range models {
		model := &models[i]
		if target != "" && (strings.EqualFold(model.Model, target) || strings.EqualFold(model.ID, target)) {
			selected = model
			break
		}
		if target == "" && model.IsDefault && selected == nil {
			selected = model
		}
	}
	if selected == nil && len(models) > 0 {
		if target != "" {
			return teams.ReasoningEffortCatalog{}, fmt.Errorf("Codex model/list did not include configured model %q", target)
		}
		selected = &models[0]
	}
	if selected == nil {
		return teams.ReasoningEffortCatalog{}, fmt.Errorf("Codex model/list returned no models")
	}
	catalog := teams.ReasoningEffortCatalog{
		Model:         firstNonEmptyCLI(selected.Model, selected.ID),
		DisplayName:   strings.TrimSpace(selected.DisplayName),
		DefaultEffort: strings.TrimSpace(selected.DefaultReasoningEffort),
	}
	for _, option := range selected.ReasoningEfforts {
		catalog.Options = append(catalog.Options, teams.ReasoningEffortOption{Effort: option.Effort, Description: option.Description})
	}
	return catalog, nil
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
		Prompt:          input.Prompt,
		ImagePaths:      append(append([]string{}, e.staticImages...), input.ImagePaths...),
		AdditionalDirs:  append([]string{}, e.additionalDirs...),
		OutputSchema:    append(json.RawMessage(nil), e.outputSchema...),
		WorkingDir:      workDir,
		Timeout:         e.timeout,
		EventHandler:    handler,
		Ephemeral:       e.ephemeral,
		BeforeFirstTurn: input.BeforeFirstTurn,
	}
	if session != nil {
		turnInput.Model = teamsSessionModel(session)
		turnInput.ReasoningEffort = strings.TrimSpace(session.ReasoningEffort)
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
			if strings.TrimSpace(out.Text) == "" {
				return out, teamsMissingFinalError(result, err)
			}
			if session != nil {
				expectedThreadID := strings.TrimSpace(session.CodexThreadID)
				if expectedThreadID != "" && out.CodexThreadID != "" && out.CodexThreadID != expectedThreadID {
					return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, expectedThreadID)
				}
			}
			return out, nil
		}
		out := teamsExecutionResultFromCodexTurn(result)
		if result.Status == codexrunner.TurnStatusCompleted && result.Failure == nil && !result.FinalAgentMessageComplete {
			return out, teamsMissingFinalError(result, err)
		}
		if teamsCodexTurnMayStillBeRunning(result) {
			return out, &teams.AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: err}
		}
		return out, err
	}
	out := successfulTeamsExecutionResultFromCodexTurn(result)
	if strings.TrimSpace(out.Text) == "" || (result.Status == codexrunner.TurnStatusCompleted && !result.FinalAgentMessageComplete) {
		return out, teamsMissingFinalError(result, nil)
	}
	if session != nil {
		expectedThreadID := strings.TrimSpace(session.CodexThreadID)
		if expectedThreadID != "" && out.CodexThreadID != "" && out.CodexThreadID != expectedThreadID {
			return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, expectedThreadID)
		}
	}
	return out, nil
}

func teamsSessionModel(session *teams.Session) string {
	if session == nil {
		return ""
	}
	if model := strings.TrimSpace(session.ModelProfile.Model); model != "" {
		return model
	}
	if model := strings.TrimSpace(session.ModelProfile.DefaultModel); model != "" {
		return model
	}
	if strings.EqualFold(strings.TrimSpace(session.ModelProfile.Provider), modelprofile.DefaultProvider) {
		name := strings.TrimSpace(session.ModelProfile.Name)
		if name != "" && !strings.EqualFold(name, config.DefaultModelProfileName) {
			return name
		}
	}
	return ""
}

func teamsMissingFinalError(result codexrunner.TurnResult, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("Codex turn completed without a final agent message")
	} else {
		cause = fmt.Errorf("Codex turn completed without a final agent message: %w", cause)
	}
	if strings.TrimSpace(result.TurnID) != "" {
		return &teams.AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: cause}
	}
	return cause
}

func (e teamsCodexExecutor) runnerForSessionProfile(ctx context.Context, session *teams.Session) (codexrunner.Runner, error) {
	if session == nil || session.ModelProfile.IsZero() {
		return e.runner, nil
	}
	if session.ModelGeneration == 0 && modelProfileSnapshotKey(session.ModelProfile) == modelProfileSnapshotKey(e.modelProfileSnapshot) {
		// The base runner is selected before a profile-specific cache entry is
		// needed. Remember it for this session so the first model-generation
		// switch can close the process before another runner resumes the same
		// Codex thread.
		if sessionID := strings.TrimSpace(session.ID); sessionID != "" && e.runnerCacheMu != nil && e.runnersByProfile != nil && e.runnerKeyBySession != nil {
			key := modelProfileRunnerSessionCacheKey(session)
			e.runnerCacheMu.Lock()
			e.runnersByProfile[key] = e.runner
			e.runnerKeyBySession[sessionID] = key
			e.runnerCacheMu.Unlock()
		}
		return e.runner, nil
	}
	key := modelProfileRunnerSessionCacheKey(session)
	if e.runnerCacheMu == nil || e.runnersByProfile == nil {
		return e.runner, nil
	}
	e.runnerCacheMu.Lock()
	defer e.runnerCacheMu.Unlock()
	if sessionID := strings.TrimSpace(session.ID); session.ModelGeneration > 0 && sessionID != "" && e.runnerKeyBySession != nil {
		if previousKey := e.runnerKeyBySession[sessionID]; previousKey != "" && previousKey != key {
			if previous := e.runnersByProfile[previousKey]; previous != nil {
				if managed, ok := previous.(*codexrunner.AppServerRunner); ok {
					if err := managed.Close(); err != nil && e.log != nil {
						_, _ = fmt.Fprintf(e.log, "Teams previous model-generation runner close reported an error for %s: %v\n", sessionID, err)
					}
				}
				delete(e.runnersByProfile, previousKey)
			}
		}
		e.runnerKeyBySession[sessionID] = key
	}
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

func modelProfileRunnerSessionCacheKey(session *teams.Session) string {
	if session == nil {
		return ""
	}
	key := modelProfileRunnerCacheKey(session.ModelProfile, session.ModelGeneration)
	if session.ModelGeneration <= 0 || strings.TrimSpace(session.ID) == "" {
		return key
	}
	return "session=" + strings.TrimSpace(session.ID) + "\x00" + key
}

func modelProfileRunnerCacheKey(snapshot modelprofile.Snapshot, generation int) string {
	key := modelProfileSnapshotKey(snapshot)
	if generation <= 0 {
		return key
	}
	return key + "\x00generation=" + fmt.Sprint(generation)
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
		result.FinalAgentMessageComplete &&
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
	return teams.ExecutionResult{Text: strings.TrimSpace(result.FinalAgentMessage), CodexThreadID: result.ThreadID, CodexThreadTitle: strings.TrimSpace(result.ThreadName), CodexTurnID: result.TurnID}
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

var resolveTeamsCodexUpgradeTargetForRun = resolveTeamsCodexUpgradeTarget

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
	store, paths, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	upgradeTarget, err := resolveTeamsCodexUpgradeTargetForRun(cmd.Context(), cfg, paths)
	if err != nil {
		return err
	}
	installOpts := managedCodexUpgradeInstallOptions(upgradeTarget)
	if upgradeUsesProxy(cfg) {
		profile, cfgWithProfile, err := ensureProfileRunFn(cmd.Context(), store, "", true, cmd.ErrOrStderr())
		if err != nil {
			return err
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, profile, cfgWithProfile.Instances, func(profileEnv []string) error {
				return runInstall(managedCodexUpgradeProxyEnvironment(upgradeTarget.environment, profileEnv))
			})
		}
	}
	path, err := upgradeOrInstallManagedCodex(cmd.Context(), cmd.ErrOrStderr(), upgradeTarget, installOpts)
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
	store, paths, err := newRootStore(root, "")
	if err != nil {
		return teams.CodexUpgradeResult{}, err
	}
	cfg, err := store.Load()
	if err != nil {
		return teams.CodexUpgradeResult{}, err
	}
	upgradeTarget, err := resolveTeamsCodexUpgradeTargetForRun(ctx, cfg, paths)
	if err != nil {
		return teams.CodexUpgradeResult{}, err
	}
	installOpts := managedCodexUpgradeInstallOptions(upgradeTarget)
	if upgradeUsesProxy(cfg) {
		profile, cfgWithProfile, err := ensureProfileRunFn(ctx, store, "", true, out)
		if err != nil {
			return teams.CodexUpgradeResult{}, err
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, profile, cfgWithProfile.Instances, func(profileEnv []string) error {
				return runInstall(managedCodexUpgradeProxyEnvironment(upgradeTarget.environment, profileEnv))
			})
		}
	}
	path, err := upgradeOrInstallManagedCodex(ctx, out, upgradeTarget, installOpts)
	if err != nil {
		return teams.CodexUpgradeResult{}, err
	}
	return teams.CodexUpgradeResult{Path: path}, nil
}

func resolveTeamsCodexUpgradeTarget(ctx context.Context, cfg config.Config, paths effectivePaths) (managedCodexUpgradeTarget, error) {
	workDir, err := os.Getwd()
	if err != nil {
		return managedCodexUpgradeTarget{}, fmt.Errorf("resolve Teams Codex upgrade working directory: %w", err)
	}
	pathResult, err := resolveTeamsCodexUserPath(ctx, cfg, paths, teamsCodexChildEnv(), workDir)
	if err != nil {
		return managedCodexUpgradeTarget{}, err
	}
	environment := codexRuntimeEnvironment(os.Environ(), []string{"PATH=" + pathResult.Path}, paths.ExecIdentity)
	return resolveManagedCodexUpgradeTarget(ctx, environment, paths.ExecIdentity), nil
}

func teamsCodexExecutableOnPath(pathValue string) (string, bool) {
	names := []string{"codex"}
	if runtime.GOOS == "windows" {
		names = []string{"codex.exe", "codex.cmd", "codex.ps1", "codex"}
	}
	for _, dir := range filepath.SplitList(pathValue) {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		for _, name := range names {
			candidate := filepath.Join(dir, name)
			if executableExists(candidate) {
				return normalizeExecutablePath(candidate), true
			}
		}
	}
	return "", false
}

func teamsCodexUpgraderForRun(root *rootOptions, out io.Writer, codexPath string, executors ...teams.Executor) teams.CodexUpgrader {
	if strings.TrimSpace(codexPath) != "" {
		return nil
	}
	return func(ctx context.Context) (teams.CodexUpgradeResult, error) {
		result, err := runTeamsCodexUpgradeFromBridge(ctx, root, out, "")
		if err != nil {
			return result, err
		}
		invalidateTeamsOfficialModelCache()
		for _, executor := range executors {
			if resetter, ok := executor.(interface{ RestartCodexRunners() error }); ok {
				if err := resetter.RestartCodexRunners(); err != nil {
					return teams.CodexUpgradeResult{}, fmt.Errorf("restart Teams Codex runners after upgrade: %w", err)
				}
			}
		}
		return result, nil
	}
}

func newTeamsModelProfileResolver(root *rootOptions, codexPaths ...string) teams.ModelProfileResolver {
	codexPath := "codex"
	if len(codexPaths) > 0 && strings.TrimSpace(codexPaths[0]) != "" {
		codexPath = strings.TrimSpace(codexPaths[0])
	}
	return newTeamsModelProfileResolverInternal(root, codexPath, nil)
}

func newTeamsModelProfileResolverWithRuntime(root *rootOptions, resolver teamsCodexRuntimeResolver) teams.ModelProfileResolver {
	return newTeamsModelProfileResolverInternal(root, "", resolver)
}

func newTeamsModelProfileResolverInternal(root *rootOptions, codexPath string, runtimeResolver teamsCodexRuntimeResolver) teams.ModelProfileResolver {
	return func(ctx context.Context, ref string) (modelprofile.Snapshot, error) {
		ref = strings.TrimSpace(ref)
		store, _, err := newRootStore(root, "")
		if err != nil {
			return modelprofile.Snapshot{}, err
		}
		cfg, err := store.Load()
		if err != nil {
			return modelprofile.Snapshot{}, err
		}
		if ref == "" {
			ref = cfg.EffectiveDefaultModelSelector()
		}
		forceProfile := strings.HasPrefix(strings.ToLower(ref), "profile:")
		forceOfficial := strings.HasPrefix(strings.ToLower(ref), "official:")
		if forceProfile || forceOfficial {
			_, ref, _ = strings.Cut(ref, ":")
			ref = strings.TrimSpace(ref)
		}
		catalogCtx := ctx
		catalogPath := codexPath
		var runtimeErr error
		catalogChecked := false
		if runtimeResolver != nil {
			contract, err := runtimeResolver(ctx)
			if err != nil {
				runtimeErr = err
			} else {
				catalogPath = contract.Runtime.Command
				catalogCtx = withCodexInvocation(ctx, codexInvocationForRuntime(contract))
			}
		}
		resolveOfficial := func() (modelprofile.Snapshot, bool) {
			if runtimeErr != nil {
				return modelprofile.Snapshot{}, false
			}
			models, catalogErr := listTeamsOfficialModelsFn(catalogCtx, catalogPath)
			if catalogErr == nil {
				catalogChecked = true
				for _, model := range models {
					matchesDefault := strings.EqualFold(strings.TrimSpace(ref), config.DefaultModelProfileName) && model.IsDefault
					if matchesDefault || strings.EqualFold(strings.TrimSpace(model.Slug), strings.TrimSpace(ref)) {
						effortsJSON, _ := json.Marshal(model.ReasoningEfforts)
						return modelprofile.Snapshot{
							Name:                          model.Slug,
							Provider:                      modelprofile.DefaultProvider,
							Model:                         model.Slug,
							DefaultModel:                  model.Slug,
							DefaultReasoningEffort:        model.DefaultReasoningLevel,
							SupportedReasoningEffortsJSON: string(effortsJSON),
							Revision:                      1,
							CapturedAt:                    time.Now().UTC(),
						}, true
					}
				}
			}
			return modelprofile.Snapshot{}, false
		}
		officialCandidate := !forceProfile && (forceOfficial || strings.HasPrefix(strings.ToLower(ref), "gpt-") || strings.EqualFold(ref, config.DefaultModelProfileName))
		if officialCandidate {
			if snapshot, ok := resolveOfficial(); ok {
				return snapshot, nil
			}
			if forceOfficial || catalogChecked {
				if runtimeErr != nil {
					return modelprofile.Snapshot{}, fmt.Errorf("official Codex runtime unavailable: %w", runtimeErr)
				}
				return modelprofile.Snapshot{}, fmt.Errorf("official Codex model %q is not available for the current account", ref)
			}
		}
		resolveRef := ref
		if forceProfile {
			resolveRef = "profile:" + ref
		}
		resolved, err := modelprofile.Resolve(cfg, resolveRef)
		if err != nil && ref != "" && !forceProfile && !catalogChecked {
			if snapshot, ok := resolveOfficial(); ok {
				return snapshot, nil
			}
			return modelprofile.Snapshot{}, err
		}
		if err != nil {
			return modelprofile.Snapshot{}, err
		}
		secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
		if !resolved.IsDefault() {
			profile, ok := cfg.FindModelProfile(resolved.Name)
			if !ok {
				return modelprofile.Snapshot{}, fmt.Errorf("model profile %q not found", resolved.Name)
			}
			// Git-sourced profiles are not trusted until their effective config and
			// current credential have passed the source binding probe. Legacy/local
			// model-profile setup predates that stamp and remains compatible.
			if strings.TrimSpace(profile.Source) != "" && !modelProfileVerificationCurrent(cfg, resolved.Name, profile, secrets) {
				apiKey, keyErr := modelprofile.ResolveAPIKey(resolved.Profile.APIKeyRef, secrets, os.Getenv)
				if keyErr != nil {
					return modelprofile.Snapshot{}, fmt.Errorf("model profile %q needs a credential before it can be verified: %w", resolved.Name, keyErr)
				}
				verifyErr := verifyAndStampTeamsModelProfile(ctx, &cfg, resolved.Name, apiKey)
				if saveErr := store.Save(cfg); saveErr != nil {
					return modelprofile.Snapshot{}, fmt.Errorf("save automatic verification for model profile %q: %w", resolved.Name, saveErr)
				}
				if verifyErr != nil {
					return modelprofile.Snapshot{}, fmt.Errorf("automatic authentication verification failed for model profile %q: %w", resolved.Name, verifyErr)
				}
				resolved, err = modelprofile.Resolve(cfg, resolved.Name)
				if err != nil {
					return modelprofile.Snapshot{}, err
				}
			}
		}
		return resolved.RuntimeSnapshot(time.Now(), secrets, os.Getenv)
	}
}
