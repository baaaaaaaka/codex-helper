package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// resolvedCodexRuntime is the complete launch contract for an original Codex
// process. Environment is explicit so callers never depend on a process-global
// PATH mutation surviving a lazy app-server start or an execution-identity
// switch.
type resolvedCodexRuntime struct {
	Command     string
	Environment []string
}

func resolveCodexInstalledRuntimeForLaunch(
	ctx context.Context,
	codexPath string,
	out io.Writer,
	opts codexInstallOptions,
	identity *execIdentity,
	overlay []string,
) (resolvedCodexRuntime, error) {
	var command string
	var err error
	if identity != nil && identity.UID != 0 {
		command, err = ensureCodexAppAuthCodexInstalled(ctx, codexPath, out, opts, identity)
	} else {
		command, err = ensureCodexInstalledWithOptions(ctx, codexPath, out, opts)
	}
	if err != nil {
		return resolvedCodexRuntime{}, err
	}
	return resolvedCodexRuntime{
		Command:     command,
		Environment: codexRuntimeEnvironment(os.Environ(), overlay, identity),
	}, nil
}

func resolveCodexBrokerRuntimeForLaunch(
	ctx context.Context,
	codexPath string,
	out io.Writer,
	opts codexInstallOptions,
	allowAutomaticUpgrade bool,
	identity *execIdentity,
	overlay []string,
) (resolvedCodexRuntime, error) {
	var command string
	var err error
	if identity != nil && identity.UID != 0 {
		command, err = ensureCodexBrokerRuntimeForIdentity(ctx, codexPath, out, opts, allowAutomaticUpgrade, identity)
	} else {
		command, err = ensureCodexBrokerRuntime(ctx, codexPath, out, opts, allowAutomaticUpgrade)
	}
	if err != nil {
		return resolvedCodexRuntime{}, err
	}
	return resolvedCodexRuntime{
		Command:     command,
		Environment: codexRuntimeEnvironment(os.Environ(), overlay, identity),
	}, nil
}

func ensureCodexBrokerRuntimeForIdentity(
	ctx context.Context,
	codexPath string,
	out io.Writer,
	opts codexInstallOptions,
	allowAutomaticUpgrade bool,
	identity *execIdentity,
) (string, error) {
	lookup := codexPath
	if allowAutomaticUpgrade && (strings.EqualFold(strings.TrimSpace(lookup), "codex") || strings.EqualFold(strings.TrimSpace(lookup), "codex.exe")) {
		lookup = ""
	}
	resolved, err := ensureCodexAppAuthCodexInstalled(ctx, lookup, out, opts, identity)
	if err != nil {
		return "", err
	}
	probeEnv := codexRuntimeEnvironment(os.Environ(), nil, identity)
	if codexBrokerRuntimeCapableWithEnv(resolved, probeEnv, identity) {
		return resolved, nil
	}
	if !allowAutomaticUpgrade {
		return "", fmt.Errorf("codex at %s lacks the app-server workspace-root contract required by the standard approval runtime; install Codex 0.131.0 or newer", resolved)
	}
	upgradeOptions := opts
	upgradeOptions.upgradeCodex = true
	upgraded, err := ensureCodexAppAuthCodexInstalled(ctx, "", out, upgradeOptions, identity)
	if err != nil {
		return "", fmt.Errorf("installed Codex lacks remote app-server support and automatic upgrade failed: %w", err)
	}
	probeEnv = codexRuntimeEnvironment(os.Environ(), nil, identity)
	if !codexBrokerRuntimeCapableWithEnv(upgraded, probeEnv, identity) {
		return "", fmt.Errorf("upgraded codex at %s still lacks the app-server workspace-root contract required by the standard approval runtime", upgraded)
	}
	return upgraded, nil
}

// codexRuntimeEnvironment merges explicit values with unique-key semantics,
// applies the target identity, and prepends any managed Node installation that
// belongs to that identity. Overlay wins except that managed Node is always
// made discoverable in its resulting PATH.
func codexRuntimeEnvironment(base []string, overlay []string, identity *execIdentity) []string {
	resolved := mergeCLIEnvironment(base, overlay)
	resolved = applyExecIdentityEnv(resolved, identity)
	resolved = codexAppAuthRuntimeEnvForIdentity(resolved, identity)
	return mergeCLIEnvironment(nil, resolved)
}

// codexRuntimeEnvironmentOverlay carries the resolved executable search path
// into launchers that build their final environment later. Keeping the
// caller's other explicit values separate preserves their established
// precedence over proxy and execution-context values without replaying a
// complete, potentially stale process environment as an overlay.
func codexRuntimeEnvironmentOverlay(overlay []string, resolved []string) []string {
	pathValue, ok := explicitEnvironmentValue(resolved, "PATH")
	if !ok || pathValue == "" {
		return append([]string(nil), overlay...)
	}
	return setEnvValueWithoutDefault(overlay, "PATH", pathValue)
}

func explicitEnvironmentValue(environment []string, key string) (string, bool) {
	for i := len(environment) - 1; i >= 0; i-- {
		entryKey, value, ok := strings.Cut(environment[i], "=")
		if ok && envKeyEqual(entryKey, key) {
			return value, true
		}
	}
	return "", false
}

func mergeCLIEnvironment(base []string, overlay []string) []string {
	out := make([]string, 0, len(base)+len(overlay))
	appendValues := func(values []string) {
		for _, entry := range values {
			key, value, ok := strings.Cut(entry, "=")
			if !ok || strings.TrimSpace(key) == "" {
				out = append(out, entry)
				continue
			}
			out = setEnvValueWithoutDefault(out, key, value)
		}
	}
	appendValues(base)
	appendValues(overlay)
	return out
}

func setEnvValueWithoutDefault(base []string, key string, value string) []string {
	out := make([]string, 0, len(base)+1)
	replaced := false
	for _, entry := range base {
		entryKey, _, ok := strings.Cut(entry, "=")
		if ok && envKeyEqual(entryKey, key) {
			if !replaced {
				out = append(out, key+"="+value)
				replaced = true
			}
			continue
		}
		out = append(out, entry)
	}
	if !replaced {
		out = append(out, key+"="+value)
	}
	return out
}
