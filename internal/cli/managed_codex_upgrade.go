package cli

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/env"
)

var upgradeCodexInstalledForTargetRun = upgradeCodexInstalledWithOptions
var ensureCodexInstalledForTargetRun = ensureCodexInstalledWithOptions
var probeManagedCodexUpgradeCandidateForRun = probeManagedCodexUpgradeCandidate
var managedCodexProbeRetryDelay = 250 * time.Millisecond

const managedCodexProbeAttempts = 3

type managedCodexUpgradeTarget struct {
	path        string
	environment []string
	identity    *execIdentity
}

func resolveManagedCodexUpgradeTarget(ctx context.Context, environment []string, identity *execIdentity) managedCodexUpgradeTarget {
	target := managedCodexUpgradeTarget{
		environment: append([]string(nil), environment...),
		identity:    identity,
	}
	if path, ok := findManagedCodexUpgradeCandidate(ctx, target.environment, identity); ok {
		target.path = path
	}
	return target
}

func upgradeOrInstallManagedCodex(ctx context.Context, out io.Writer, target managedCodexUpgradeTarget, opts codexInstallOptions) (string, error) {
	if strings.TrimSpace(target.path) != "" {
		return upgradeCodexInstalledForTargetRun(ctx, out, opts)
	}
	opts.upgradeCodex = false
	opts.upgradeCodexPath = ""
	return ensureCodexInstalledForTargetRun(ctx, "", out, opts)
}

func findManagedCodexUpgradeCandidate(ctx context.Context, environment []string, identity *execIdentity) (string, bool) {
	var candidates []string
	seen := map[string]bool{}
	add := func(path string) {
		path = normalizeExecutablePath(path)
		key := path
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if path != "" && !seen[key] {
			seen[key] = true
			candidates = append(candidates, path)
		}
	}
	for _, prefix := range managedCodexPrefixCandidates(environment) {
		for _, candidate := range codexBinCandidatesForPrefixForOS(runtime.GOOS, prefix) {
			add(candidate)
		}
	}
	for _, candidate := range candidates {
		if !executableExists(candidate) || probeManagedCodexUpgradeCandidateForRun(ctx, candidate, environment, identity) != nil {
			continue
		}
		source, err := detectCodexUpgradeSourceForPath(ctx, candidate, environment)
		if err == nil && source.origin == codexInstallOriginManaged {
			return candidate, true
		}
	}
	return "", false
}

func probeManagedCodexUpgradeCandidate(ctx context.Context, path string, environment []string, identity *execIdentity) error {
	probeCtx, cancel := context.WithTimeout(ctx, codexProbeTimeout)
	defer cancel()
	if identity != nil && identity.UID != 0 {
		return probeCodexForAppAuthIdentity(probeCtx, path, identity)
	}
	var lastErr error
	for attempt := 0; attempt < managedCodexProbeAttempts; attempt++ {
		cmd := exec.CommandContext(probeCtx, path, "--version")
		cmd.Env = environment
		if out, err := cmd.CombinedOutput(); err == nil {
			return nil
		} else {
			lastErr = fmt.Errorf("codex at %s is not functional: %w: %s", path, err, strings.TrimSpace(string(out)))
		}
		if attempt+1 == managedCodexProbeAttempts || probeCtx.Err() != nil {
			break
		}
		timer := time.NewTimer(managedCodexProbeRetryDelay)
		select {
		case <-probeCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return lastErr
		case <-timer.C:
		}
	}
	return lastErr
}

func managedCodexUpgradeInstallOptions(target managedCodexUpgradeTarget) codexInstallOptions {
	return codexUpgradeTargetInstallOptions(target, true)
}

func codexUpgradeTargetInstallOptions(target managedCodexUpgradeTarget, requireManaged bool) codexInstallOptions {
	opts := codexInstallOptions{
		upgradeCodex:     true,
		upgradeCodexPath: target.path,
		installerEnv:     target.environment,
		requireManaged:   requireManaged,
		probeManagedCodex: func(ctx context.Context, path string, environment []string) error {
			return probeManagedCodexUpgradeCandidate(ctx, path, environment, target.identity)
		},
	}
	if target.identity != nil {
		opts.configureInstallerCommand = func(cmd *exec.Cmd) error {
			updated, err := applyExecIdentity(cmd, cmd.Env, target.identity)
			if err != nil {
				return err
			}
			cmd.Env = codexRuntimeEnvironment(updated, nil, target.identity)
			return nil
		}
	}
	return opts
}

func managedCodexUpgradeProxyEnvironment(targetEnvironment []string, profileEnvironment []string) []string {
	proxyURL := envValue(profileEnvironment, "HTTP_PROXY")
	if strings.TrimSpace(proxyURL) == "" {
		proxyURL = envValue(profileEnvironment, "http_proxy")
	}
	return env.WithProxy(targetEnvironment, proxyURL)
}
