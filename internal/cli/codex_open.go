package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/cloudgate"
	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/localproxy"
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
		cwd = project.Path
	}
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
		if yoloFlags := codexYoloArgs(path); len(yoloFlags) > 0 {
			args = append(yoloFlags, args...)
		}
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

	// Layer 3: Patch the native Codex binary to use permissive system requirements.
	extraEnv := []string{}
	if useYolo {
		configDir := filepath.Dir(store.Path())
		patchResult, patchEnv, patchErr := cloudgate.PrepareYoloBinary(codexPath, configDir)
		if patchErr == nil && patchResult != nil && patchResult.PatchedBinary != "" {
			codexPath = patchResult.PatchedBinary
			extraEnv = append(extraEnv, patchEnv...)
			defer patchResult.Cleanup()
		}
	}

	path, args, cwd, err := buildCodexResumeCommand(codexPath, session, project, useYolo)
	if err != nil {
		return err
	}

	cmdArgs := append([]string{path}, args...)

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

	if useYolo {
		configDir := filepath.Dir(store.Path())
		gateCfg, gateErr := cloudgate.Setup(configDir)
		if gateErr != nil {
			gateCfg = cloudgate.SetupFingerprintOnly()
		}
		defer gateCfg.Cleanup()
		opts.CloudGate = gateCfg
	}

	if useProxy {
		if profile == nil {
			return fmt.Errorf("proxy mode enabled but no profile configured")
		}
		return runWithProfileOptions(ctx, store, *profile, instances, cmdArgs, opts)
	}

	if useYolo && opts.CloudGate != nil {
		dialer := localproxy.DirectDialer(10 * time.Second)
		proxy := localproxy.NewHTTPProxy(dialer, localproxy.Options{CloudGate: opts.CloudGate})
		addr, startErr := proxy.Start("127.0.0.1:0")
		if startErr != nil {
			return runTargetWithFallbackWithOptions(ctx, cmdArgs, "", nil, nil, opts)
		}
		defer proxy.Close(ctx)
		proxyURL := "http://" + addr
		opts.UseProxy = true
		return runTargetWithFallbackWithOptions(ctx, cmdArgs, proxyURL, nil, nil, opts)
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

	// Layer 3: Patch the native Codex binary to use permissive system requirements.
	extraEnv := []string{}
	if useYolo {
		configDir := filepath.Dir(store.Path())
		patchResult, patchEnv, patchErr := cloudgate.PrepareYoloBinary(codexPath, configDir)
		if patchErr == nil && patchResult != nil && patchResult.PatchedBinary != "" {
			codexPath = patchResult.PatchedBinary
			extraEnv = append(extraEnv, patchEnv...)
			defer patchResult.Cleanup()
		}
	}

	cmdArgs := []string{codexPath}
	if useYolo {
		if yoloFlags := codexYoloArgs(codexPath); len(yoloFlags) > 0 {
			cmdArgs = append(cmdArgs, yoloFlags...)
		}
	}

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

	if useYolo {
		configDir := filepath.Dir(store.Path())
		gateCfg, gateErr := cloudgate.Setup(configDir)
		if gateErr != nil {
			gateCfg = cloudgate.SetupFingerprintOnly()
		}
		defer gateCfg.Cleanup()
		opts.CloudGate = gateCfg
	}

	if useProxy {
		if profile == nil {
			return fmt.Errorf("proxy mode enabled but no profile configured")
		}
		return runWithProfileOptions(ctx, store, *profile, instances, cmdArgs, opts)
	}

	if useYolo && opts.CloudGate != nil {
		dialer := localproxy.DirectDialer(10 * time.Second)
		proxy := localproxy.NewHTTPProxy(dialer, localproxy.Options{CloudGate: opts.CloudGate})
		addr, startErr := proxy.Start("127.0.0.1:0")
		if startErr != nil {
			return runTargetWithFallbackWithOptions(ctx, cmdArgs, "", nil, nil, opts)
		}
		defer proxy.Close(ctx)
		proxyURL := "http://" + addr
		opts.UseProxy = true
		return runTargetWithFallbackWithOptions(ctx, cmdArgs, proxyURL, nil, nil, opts)
	}

	return runTargetWithFallbackWithOptions(ctx, cmdArgs, "", nil, nil, opts)
}
