package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
)

const (
	envCodexProxyRealNPM         = "CODEX_PROXY_REAL_NPM"
	envCodexProxyOriginalPath    = "CODEX_PROXY_ORIGINAL_PATH"
	envCodexProxyUpdateOrigin    = "CODEX_PROXY_UPDATE_ORIGIN"
	envCodexProxyUpdateCodexPath = "CODEX_PROXY_UPDATE_CODEX_PATH"
	envCodexProxyUpdateNPMPrefix = "CODEX_PROXY_UPDATE_NPM_PREFIX"
	envCodexProxyWrapperExe      = "CODEX_PROXY_NPM_WRAPPER_EXE"
)

var (
	codexSelfUpdateLookPath     = exec.LookPath
	codexSelfUpdateExecutable   = os.Executable
	codexSelfUpdateDetectSource = detectCodexUpgradeSourceForPath
	codexSelfUpdateCleanupStale = cleanupStaleCodexRetiredPathsForSource
	codexSelfUpdateRunRealNpm   = runCodexSelfUpdateRealNpm
)

func newInternalNpmWrapperCmd() *cobra.Command {
	return &cobra.Command{
		Use:                "__internal-npm-wrapper",
		Short:              "Internal npm wrapper for Codex self-update",
		Hidden:             true,
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			os.Exit(runInternalNpmWrapper(cmd.Context(), args, cmd.ErrOrStderr()))
		},
	}
}

func prepareCodexSelfUpdateGuardEnv(
	ctx context.Context,
	codexPath string,
	envVars []string,
) ([]string, func(), error) {
	if !isCodexCommand(codexPath) {
		return envVars, func() {}, nil
	}

	source, err := codexSelfUpdateDetectSource(ctx, codexPath, envVars)
	if err != nil {
		return envVars, func() {}, err
	}
	if source.origin != codexInstallOriginManaged && source.origin != codexInstallOriginSystem {
		return envVars, func() {}, nil
	}

	npmPath, err := codexSelfUpdateLookPath("npm")
	if err != nil {
		return envVars, func() {}, nil
	}
	wrapperExe, err := codexSelfUpdateExecutable()
	if err != nil {
		return envVars, func() {}, err
	}

	wrapperDir, err := os.MkdirTemp("", "codex-proxy-npm-")
	if err != nil {
		return envVars, func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(wrapperDir) }
	if err := writeCodexSelfUpdateNpmWrapper(wrapperDir); err != nil {
		cleanup()
		return envVars, func() {}, err
	}

	origPath := envValue(envVars, "PATH")
	updated := envVars
	updated = setEnvValue(updated, envCodexProxyOriginalPath, origPath)
	updated = setEnvValue(updated, envCodexProxyRealNPM, npmPath)
	updated = setEnvValue(updated, envCodexProxyUpdateOrigin, string(source.origin))
	updated = setEnvValue(updated, envCodexProxyUpdateCodexPath, source.codexPath)
	updated = setEnvValue(updated, envCodexProxyUpdateNPMPrefix, source.npmPrefix)
	updated = setEnvValue(updated, envCodexProxyWrapperExe, wrapperExe)

	pathValue := wrapperDir
	if strings.TrimSpace(origPath) != "" {
		pathValue += string(os.PathListSeparator) + origPath
	}
	updated = setEnvValue(updated, "PATH", pathValue)
	return updated, cleanup, nil
}

func writeCodexSelfUpdateNpmWrapper(dir string) error {
	if strings.EqualFold(runtime.GOOS, "windows") {
		content := "@echo off\r\n" +
			"\"%" + envCodexProxyWrapperExe + "%\" __internal-npm-wrapper %*\r\n"
		return os.WriteFile(filepath.Join(dir, "npm.cmd"), []byte(content), 0o600)
	}

	content := "#!/bin/sh\n" +
		"exec \"$" + envCodexProxyWrapperExe + "\" __internal-npm-wrapper \"$@\"\n"
	path := filepath.Join(dir, "npm")
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		return err
	}
	return nil
}

func runInternalNpmWrapper(ctx context.Context, args []string, stderr io.Writer) int {
	realNpm := strings.TrimSpace(os.Getenv(envCodexProxyRealNPM))
	if realNpm == "" {
		_, _ = fmt.Fprintln(stderr, "codex self-update wrapper: missing real npm path")
		return 1
	}

	if isNpmGlobalCodexInstallArgs(args) {
		source := codexUpgradeSource{
			origin:    codexInstallOrigin(strings.TrimSpace(os.Getenv(envCodexProxyUpdateOrigin))),
			codexPath: strings.TrimSpace(os.Getenv(envCodexProxyUpdateCodexPath)),
			npmPrefix: strings.TrimSpace(os.Getenv(envCodexProxyUpdateNPMPrefix)),
		}
		if source.origin == codexInstallOriginManaged || source.origin == codexInstallOriginSystem {
			if err := codexSelfUpdateCleanupStale(stderr, source); err != nil {
				_, _ = fmt.Fprintf(stderr, "codex self-update preflight failed: %v\n", err)
				return 1
			}
		}
	}

	if err := codexSelfUpdateRunRealNpm(ctx, realNpm, args, sanitizeCodexSelfUpdateEnv(os.Environ())); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		_, _ = fmt.Fprintf(stderr, "codex self-update wrapper: %v\n", err)
		return 1
	}
	return 0
}

func isNpmGlobalCodexInstallArgs(args []string) bool {
	commandSeen := false
	global := false
	hasCodex := false
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue
		}
		if !commandSeen {
			switch arg {
			case "-g", "--global":
				global = true
				continue
			case "install", "i":
				commandSeen = true
				continue
			default:
				return false
			}
		}
		switch arg {
		case "-g", "--global":
			global = true
		case "@openai/codex":
			hasCodex = true
		default:
			if strings.HasPrefix(arg, "-") {
				continue
			}
		}
	}
	return commandSeen && global && hasCodex
}

func sanitizeCodexSelfUpdateEnv(env []string) []string {
	origPath := strings.TrimSpace(envValue(env, envCodexProxyOriginalPath))
	keysToDrop := []string{
		envCodexProxyRealNPM,
		envCodexProxyOriginalPath,
		envCodexProxyUpdateOrigin,
		envCodexProxyUpdateCodexPath,
		envCodexProxyUpdateNPMPrefix,
		envCodexProxyWrapperExe,
	}

	out := make([]string, 0, len(env))
	pathSet := false
	for _, kv := range env {
		key, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		for _, dropKey := range keysToDrop {
			if envKeyEqual(key, dropKey) {
				goto nextEntry
			}
		}
		if envKeyEqual(key, "PATH") {
			if !pathSet {
				out = append(out, "PATH="+origPath)
				pathSet = true
			}
			goto nextEntry
		}
		out = append(out, kv)
	nextEntry:
	}
	if !pathSet {
		out = append(out, "PATH="+origPath)
	}
	return out
}

func runCodexSelfUpdateRealNpm(ctx context.Context, npmPath string, args []string, env []string) error {
	cmd := exec.CommandContext(ctx, npmPath, args...)
	cmd.Env = env
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
