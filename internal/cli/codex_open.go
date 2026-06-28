package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexbinary"
	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/env"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

const codexRemoteTUIFeatureConfig = "features.tui_app_server=true"

func normalizeWorkingDir(cwd string) (string, error) {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return "", fmt.Errorf("missing working directory")
	}
	if !filepath.IsAbs(cwd) {
		cwd, _ = filepath.Abs(cwd)
	}
	if stat, err := os.Stat(cwd); err != nil || !stat.IsDir() {
		if err != nil {
			return "", fmt.Errorf("working directory not found: %w", err)
		}
		return "", fmt.Errorf("working directory is not a directory: %s", cwd)
	}
	return cwd, nil
}

func runCodexSession(
	ctx context.Context,
	root *rootOptions,
	store *config.Store,
	profile *config.Profile,
	instances []config.Instance,
	session codexhistory.Session,
	project codexhistory.Project,
	codexPath string,
	codexDir string,
	useProxy bool,
	log io.Writer,
) error {
	cwd := codexhistory.SessionWorkingDir(session)
	if cwd == "" {
		cwd = project.Path
	}
	if strings.TrimSpace(session.SessionID) == "" {
		return fmt.Errorf("missing session id")
	}
	return runCodexTUIViaBroker(ctx, root, store, profile, instances, cwd, session.SessionID, codexPath, codexDir, useProxy, "", log)
}

func runCodexNewSession(
	ctx context.Context,
	root *rootOptions,
	store *config.Store,
	profile *config.Profile,
	instances []config.Instance,
	cwd string,
	codexPath string,
	codexDir string,
	useProxy bool,
	log io.Writer,
) error {
	return runCodexTUIViaBroker(ctx, root, store, profile, instances, cwd, "", codexPath, codexDir, useProxy, "", log)
}

func runCodexTUIViaBroker(
	ctx context.Context,
	root *rootOptions,
	store *config.Store,
	profile *config.Profile,
	instances []config.Instance,
	cwd string,
	sessionID string,
	codexPath string,
	codexDir string,
	useProxy bool,
	modelProfileRef string,
	log io.Writer,
) error {
	tail := []string{}
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		tail = []string{"resume", sessionID}
	}
	return runCodexTUIInvocationViaBroker(ctx, root, store, profile, instances, cwd, codexPath, codexDir, useProxy, modelProfileRef, nil, tail, nil, log)
}

func runCodexTUIInvocationViaBroker(
	ctx context.Context,
	root *rootOptions,
	store *config.Store,
	profile *config.Profile,
	instances []config.Instance,
	cwd string,
	codexPath string,
	codexDir string,
	useProxy bool,
	modelProfileRef string,
	tuiGlobalArgs []string,
	tuiTail []string,
	appServerExtraArgs []string,
	log io.Writer,
) error {
	cwd, err := normalizeWorkingDir(cwd)
	if err != nil {
		return err
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
	allowAutomaticUpgrade := codexPathAllowsAutomaticUpgrade(codexPath)
	codexPath, err = ensureCodexBrokerRuntime(ctx, codexPath, log, installOptions, allowAutomaticUpgrade)
	if err != nil {
		return err
	}
	identityPath := codexPath
	if nativePath, _, nativeErr := codexbinary.FindNativeBinary(codexPath); nativeErr == nil {
		identityPath = nativePath
	}
	originalHash, hashErr := hashFileSHA256(identityPath)

	configPath := ""
	if root != nil {
		configPath = root.configPath
	}
	paths, err := resolveEffectiveLaunchPaths(configPath, codexDir, cwd)
	if err != nil {
		return err
	}
	if err := prepareRuntimeMigration(store, paths, codexPath, log); err != nil {
		return err
	}
	extraEnv := codexHomeEnv(paths.CodexDir)
	proxyURL := ""
	if useProxy {
		proxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *profile, instances, log)
		if err != nil {
			return err
		}
		extraEnv = env.WithProxy(extraEnv, proxyURL)
	}

	modelLaunch, modelCleanup, err := startModelProfileAdapterForCodex(ctx, store, modelProfileRef, modelprofile.Snapshot{}, proxyURL, log)
	if err != nil {
		return err
	}
	if modelCleanup != nil {
		defer modelCleanup()
	}
	appServerArgs := append([]string{"app-server", "--analytics-default-enabled"}, appServerExtraArgs...)
	if modelLaunch.Enabled {
		modelArgs := appendCodexModelProfileArgs([]string{"codex"}, modelLaunch)
		appServerArgs = append(appServerArgs, modelArgs[1:]...)
		extraEnv = append(extraEnv, envCXPResponsesProxyKey+"="+modelLaunch.ProxyKey)
	}

	guardCleanup := func() {}
	if guarded, cleanup, guardErr := prepareCodexSelfUpdateGuardEnv(ctx, codexPath, append(os.Environ(), extraEnv...), paths.ExecIdentity); guardErr == nil {
		extraEnv = guarded
		guardCleanup = cleanup
	} else if log != nil {
		_, _ = fmt.Fprintf(log, "failed to arm codex self-update guard: %v\n", guardErr)
	}
	defer guardCleanup()

	configureIdentity := func(command *exec.Cmd) error {
		updated, err := applyExecIdentity(command, command.Env, paths.ExecIdentity)
		if err != nil {
			return err
		}
		command.Env = updated
		return nil
	}
	codexDirCreated := false
	if _, statErr := os.Stat(paths.CodexDir); os.IsNotExist(statErr) {
		if err := os.MkdirAll(paths.CodexDir, 0o700); err != nil {
			return fmt.Errorf("create Codex home for remote TUI: %w", err)
		}
		codexDirCreated = true
	} else if statErr != nil {
		return fmt.Errorf("inspect Codex home for remote TUI: %w", statErr)
	}
	if codexDirCreated {
		if err := ensurePathOwnedByIdentity(paths.CodexDir, paths.ExecIdentity); err != nil {
			return fmt.Errorf("prepare Codex home ownership for remote TUI: %w", err)
		}
	}
	remoteTUISQLiteHome, err := os.MkdirTemp(paths.CodexDir, ".cxp-remote-tui-sqlite-*")
	if err != nil {
		return fmt.Errorf("create isolated remote TUI sqlite home: %w", err)
	}
	defer os.RemoveAll(remoteTUISQLiteHome)
	if err := os.Chmod(remoteTUISQLiteHome, 0o700); err != nil {
		return fmt.Errorf("secure isolated remote TUI sqlite home: %w", err)
	}
	if err := ensurePathOwnedByIdentity(remoteTUISQLiteHome, paths.ExecIdentity); err != nil {
		return fmt.Errorf("prepare isolated remote TUI sqlite home ownership: %w", err)
	}
	starter := codexrunner.PolicyAppServerStarter{
		ServerOptions: responsespolicy.ServerOptions{ProxyURL: proxyURL},
		ReadyHook:     runtimeMigrationReadyHook(store, paths, codexPath, log),
	}
	broker, err := codexrunner.StartRemoteBroker(ctx, codexrunner.RemoteBrokerOptions{
		Starter: starter,
		Log:     log,
		StartRequest: codexrunner.AppServerStartRequest{
			Command:          codexPath,
			Args:             appServerArgs,
			WorkingDir:       cwd,
			ExtraEnv:         extraEnv,
			Timeout:          30 * time.Second,
			ConfigureCommand: configureIdentity,
		},
	})
	if err != nil {
		return err
	}
	extraEnv = setEnvValue(extraEnv, codexrunner.RemoteBrokerAuthTokenEnv, broker.AuthToken())
	extraEnv = setEnvValue(extraEnv, envCodexSQLiteHome, remoteTUISQLiteHome)

	args := append([]string{}, tuiGlobalArgs...)
	args = append(args,
		"-c", codexRemoteTUIFeatureConfig,
		"--remote", broker.URL(),
		"--remote-auth-token-env", codexrunner.RemoteBrokerAuthTokenEnv,
	)
	args = append(args, tuiTail...)
	runErr := runTargetSupervisedWithOptions(ctx, append([]string{codexPath}, args...), "", nil, broker.Done(), runTargetOptions{
		Cwd:          cwd,
		ExtraEnv:     extraEnv,
		PreserveTTY:  true,
		ExecIdentity: paths.ExecIdentity,
		Log:          log,
	})
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	closeErr := broker.Close(shutdownCtx)
	cancel()

	var identityErr error
	if hashErr == nil {
		if currentHash, err := hashFileSHA256(identityPath); err != nil {
			identityErr = fmt.Errorf("verify original Codex binary after launch: %w", err)
		} else if currentHash != originalHash {
			identityErr = fmt.Errorf("original Codex binary changed while the session was running")
		}
	}
	return errors.Join(runErr, closeErr, identityErr)
}
