package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func buildCodexResumeCommand(
	codexPath string,
	session codexhistory.Session,
	project codexhistory.Project,
	yolo bool,
) (string, []string, string, error) {
	if session.SessionID == "" {
		return "", nil, "", fmt.Errorf("missing session id")
	}

	cwd := codexhistory.SessionWorkingDir(session)
	if cwd == "" {
		return "", nil, "", fmt.Errorf("cannot determine session working directory")
	}
	cwd, err := normalizeWorkingDir(cwd)
	if err != nil {
		return "", nil, "", err
	}

	path := codexPath
	if path == "" {
		var err error
		path, err = exec.LookPath("codex")
		if err != nil {
			return "", nil, "", fmt.Errorf("codex CLI not found in PATH")
		}
	}

	args := []string{"resume", session.SessionID}
	if yolo {
		args = append([]string{"--yolo"}, args...)
	}
	return path, args, cwd, nil
}

func normalizeWorkingDir(cwd string) (string, error) {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return "", fmt.Errorf("missing working directory")
	}
	if !filepath.IsAbs(cwd) {
		cwd, _ = filepath.Abs(cwd)
	}
	if st, err := os.Stat(cwd); err != nil || !st.IsDir() {
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
	useYolo bool,
	log io.Writer,
) error {
	codexPathResolved, err := ensureCodexInstalled(ctx, codexPath, log, installProxyOptions{
		UseProxy:  useProxy,
		Profile:   profile,
		Instances: instances,
	})
	if err != nil {
		return err
	}
	codexPath = codexPathResolved

	if useYolo && !supportsYoloFlag(codexPath) {
		_ = persistYoloEnabled(store, false)
		useYolo = false
	}
	path, args, cwd, err := buildCodexResumeCommand(codexPath, session, project, useYolo)
	if err != nil {
		return err
	}

	cmdArgs := append([]string{path}, args...)

	extraEnv := []string{}
	if codexDir != "" {
		extraEnv = append(extraEnv, codexhistory.EnvCodexDir+"="+codexDir)
	}

	opts := runTargetOptions{
		Cwd:         cwd,
		ExtraEnv:    extraEnv,
		UseProxy:    useProxy,
		PreserveTTY: true,
		YoloEnabled: useYolo,
		OnYoloFallback: func() error {
			return persistYoloEnabled(store, false)
		},
	}
	if useProxy {
		if profile == nil {
			return fmt.Errorf("proxy mode enabled but no profile configured")
		}
		return runWithProfileOptions(ctx, store, *profile, instances, cmdArgs, opts)
	}
	return runTargetWithFallbackWithOptions(ctx, cmdArgs, "", nil, nil, opts)
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
	useYolo bool,
	log io.Writer,
) error {
	cwd, err := normalizeWorkingDir(cwd)
	if err != nil {
		return err
	}

	codexPathResolved, err := ensureCodexInstalled(ctx, codexPath, log, installProxyOptions{
		UseProxy:  useProxy,
		Profile:   profile,
		Instances: instances,
	})
	if err != nil {
		return err
	}
	codexPath = codexPathResolved
	if useYolo && !supportsYoloFlag(codexPath) {
		_ = persistYoloEnabled(store, false)
		useYolo = false
	}
	cmdArgs := []string{codexPath}
	if useYolo {
		cmdArgs = append(cmdArgs, "--yolo")
	}

	extraEnv := []string{}
	if codexDir != "" {
		extraEnv = append(extraEnv, codexhistory.EnvCodexDir+"="+codexDir)
	}

	opts := runTargetOptions{
		Cwd:         cwd,
		ExtraEnv:    extraEnv,
		UseProxy:    useProxy,
		PreserveTTY: true,
		YoloEnabled: useYolo,
		OnYoloFallback: func() error {
			return persistYoloEnabled(store, false)
		},
	}
	if useProxy {
		if profile == nil {
			return fmt.Errorf("proxy mode enabled but no profile configured")
		}
		return runWithProfileOptions(ctx, store, *profile, instances, cmdArgs, opts)
	}
	return runTargetWithFallbackWithOptions(ctx, cmdArgs, "", nil, nil, opts)
}
