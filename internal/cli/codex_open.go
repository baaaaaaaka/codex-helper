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
)

func logYoloPatchStatus(
	log io.Writer,
	patchResult *cloudgate.PatchResult,
	skipped bool,
	patchErr error,
) {
	if log == nil {
		return
	}

	switch {
	case skipped:
		_, _ = fmt.Fprintln(log, "yolo patch skipped due to previous startup failure; launching original codex binary.")
	case patchErr != nil:
		_, _ = fmt.Fprintf(log, "yolo patch failed: %v; launching original codex binary.\n", patchErr)
	case patchResult != nil && patchResult.PatchedBinary != "":
		_, _ = fmt.Fprintf(log, "yolo patch active; launching patched codex binary: %s\n", patchResult.PatchedBinary)
	case patchResult != nil:
		_, _ = fmt.Fprintln(log, "yolo patch produced no modified binary; launching original codex binary.")
	}
}

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
	installOpts := codexInstallOptions{}
	if useProxy {
		if profile == nil {
			return fmt.Errorf("proxy mode enabled but no profile configured")
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, *profile, instances, runInstall)
		}
	}
	codexPathResolved, err := ensureCodexInstalledWithOptions(ctx, codexPath, log, installOpts)
	if err != nil {
		return err
	}
	codexPath = codexPathResolved

	// Layer 3: Patch the native Codex binary to use permissive system requirements.
	extraEnv := []string{}
	var pInfo *patchRunInfo
	effectiveCodexHome := ""
	if useYolo {
		configDir := filepath.Dir(store.Path())
		patchResult, patchEnv, info, skipped, patchErr := preparePatchedBinary(codexPath, configDir)
		logYoloPatchStatus(log, patchResult, skipped, patchErr)
		if !skipped && patchResult != nil && patchResult.PatchedBinary != "" {
			codexPath = patchResult.PatchedBinary
			extraEnv = append(extraEnv, patchEnv...)
			pInfo = info
			defer patchResult.Cleanup()
		}
	}

	path, args, cwd, err := buildCodexResumeCommand(codexPath, session, project, useYolo)
	if err != nil {
		return err
	}

	if useYolo {
		codexHome, homeErr := resolveCodexHome(codexDir, cwd)
		if homeErr != nil {
			return homeErr
		}
		effectiveCodexHome = codexHome
		authOverride, authErr := prepareYoloAuthOverride(codexHome)
		logYoloAuthStatus(log, authOverride, authErr)
		if authOverride != nil {
			defer authOverride.Cleanup()
		}
		// Always delete the cloud requirements cache when yolo is
		// requested, even if patching was skipped or failed. The cached
		// cloud requirements would otherwise override yolo flags.
		_ = cloudgate.RemoveCloudRequirementsCache(codexHome)
	}

	cmdArgs := append([]string{path}, args...)

	if effectiveCodexHome != "" {
		extraEnv = append(extraEnv, yoloCodexHomeEnv(effectiveCodexHome)...)
	} else if codexDir != "" {
		extraEnv = append(extraEnv, codexhistory.EnvCodexDir+"="+codexDir)
		extraEnv = append(extraEnv, envCodexHome+"="+codexDir)
	}

	opts := runTargetOptions{
		Cwd:         cwd,
		ExtraEnv:    extraEnv,
		UseProxy:    useProxy,
		PreserveTTY: true,
		YoloEnabled: useYolo,
		PatchInfo:   pInfo,
		OnYoloFallback: func() error {
			return persistYoloEnabled(store, false)
		},
	}

	if useProxy {
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

	installOpts := codexInstallOptions{}
	if useProxy {
		if profile == nil {
			return fmt.Errorf("proxy mode enabled but no profile configured")
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, *profile, instances, runInstall)
		}
	}
	codexPathResolved, err := ensureCodexInstalledWithOptions(ctx, codexPath, log, installOpts)
	if err != nil {
		return err
	}
	codexPath = codexPathResolved

	// Layer 3: Patch the native Codex binary to use permissive system requirements.
	extraEnv := []string{}
	var pInfo *patchRunInfo
	effectiveCodexHome := ""
	if useYolo {
		configDir := filepath.Dir(store.Path())
		patchResult, patchEnv, info, skipped, patchErr := preparePatchedBinary(codexPath, configDir)
		logYoloPatchStatus(log, patchResult, skipped, patchErr)
		if !skipped && patchResult != nil && patchResult.PatchedBinary != "" {
			codexPath = patchResult.PatchedBinary
			extraEnv = append(extraEnv, patchEnv...)
			pInfo = info
			defer patchResult.Cleanup()
		}
		codexHome, homeErr := resolveCodexHome(codexDir, cwd)
		if homeErr != nil {
			return homeErr
		}
		effectiveCodexHome = codexHome
		authOverride, authErr := prepareYoloAuthOverride(codexHome)
		logYoloAuthStatus(log, authOverride, authErr)
		if authOverride != nil {
			defer authOverride.Cleanup()
		}
		// Always delete the cloud requirements cache when yolo is
		// requested, even if patching was skipped or failed. The cached
		// cloud requirements would otherwise override yolo flags.
		_ = cloudgate.RemoveCloudRequirementsCache(codexHome)
	}

	cmdArgs := []string{codexPath}
	if useYolo {
		if yoloFlags := codexYoloArgs(codexPath); len(yoloFlags) > 0 {
			cmdArgs = append(cmdArgs, yoloFlags...)
		}
	}

	if effectiveCodexHome != "" {
		extraEnv = append(extraEnv, yoloCodexHomeEnv(effectiveCodexHome)...)
	} else if codexDir != "" {
		extraEnv = append(extraEnv, codexhistory.EnvCodexDir+"="+codexDir)
		extraEnv = append(extraEnv, envCodexHome+"="+codexDir)
	}

	opts := runTargetOptions{
		Cwd:         cwd,
		ExtraEnv:    extraEnv,
		UseProxy:    useProxy,
		PreserveTTY: true,
		YoloEnabled: useYolo,
		PatchInfo:   pInfo,
		OnYoloFallback: func() error {
			return persistYoloEnabled(store, false)
		},
	}

	if useProxy {
		return runWithProfileOptions(ctx, store, *profile, instances, cmdArgs, opts)
	}

	return runTargetWithFallbackWithOptions(ctx, cmdArgs, "", nil, nil, opts)
}

// preparePatchedBinary wraps cloudgate.PrepareYoloBinary with patch-history
// awareness. It skips patching only if a previous patch was recorded as failed
// (to avoid retrying a known-broken patch). Otherwise it always re-patches
// because the patched binary is ephemeral (removed by Cleanup after each session).
// Returns (result, env, patchRunInfo, skipped, err).
func preparePatchedBinary(codexPath string, configDir string) (*cloudgate.PatchResult, []string, *patchRunInfo, bool, error) {
	origHash, hashErr := hashFileSHA256(codexPath)

	// Only skip if a previous patch is known to have failed (crashed binary).
	phs, phsErr := config.NewPatchHistoryStore(configDir)
	if phsErr == nil && hashErr == nil {
		if failed, _ := phs.IsFailed(codexPath, origHash); failed {
			return nil, nil, nil, true, nil
		}
	}

	patchResult, patchEnv, patchErr := cloudgate.PrepareYoloBinary(codexPath, configDir)
	if patchErr != nil {
		return nil, nil, nil, false, patchErr
	}

	var info *patchRunInfo
	if hashErr == nil {
		info = &patchRunInfo{
			OrigBinaryPath: codexPath,
			OrigSHA256:     origHash,
			ConfigDir:      configDir,
		}
	}

	// Record successful patch in history.
	if phs != nil && hashErr == nil && patchResult != nil && patchResult.PatchedBinary != "" {
		patchedHash, _ := hashFileSHA256(patchResult.PatchedBinary)
		_ = phs.Upsert(config.PatchHistoryEntry{
			Path:          codexPath,
			OrigSHA256:    origHash,
			PatchedSHA256: patchedHash,
			ProxyVersion:  currentProxyVersion(),
			PatchedAt:     time.Now(),
		})
	}

	return patchResult, patchEnv, info, false, nil
}
