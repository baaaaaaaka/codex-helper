package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// teamsCodexRuntimeContract is the single source of truth for every Codex
// consumer owned by a Teams service.  A command without its environment and
// execution identity is not a usable runtime contract: it can observe a
// different account, CODEX_HOME, PATH, or npm installation than the executor.
type teamsCodexRuntimeContract struct {
	Runtime     resolvedCodexRuntime
	Identity    *execIdentity
	Fingerprint string
}

type teamsCodexRuntimeResolver func(context.Context) (teamsCodexRuntimeContract, error)

var ensureManagedTeamsCodexForRuntime = ensureManagedTeamsCodexRuntime

func newTeamsCodexRuntimeResolver(root *rootOptions, rawCommand, workDir string, log io.Writer) teamsCodexRuntimeResolver {
	return func(ctx context.Context) (teamsCodexRuntimeContract, error) {
		return resolveTeamsCodexRuntimeContract(ctx, root, rawCommand, workDir, log)
	}
}

func resolveTeamsCodexRuntimeContract(ctx context.Context, root *rootOptions, rawCommand, workDir string, log io.Writer) (teamsCodexRuntimeContract, error) {
	store, paths, err := newRootStore(root, "")
	if err != nil {
		return teamsCodexRuntimeContract{}, err
	}
	paths, err = resolveEffectiveLaunchPaths(store.Path(), paths.CodexDir, workDir)
	if err != nil {
		return teamsCodexRuntimeContract{}, err
	}
	cfg, err := store.Load()
	if err != nil {
		return teamsCodexRuntimeContract{}, err
	}
	pathResult, err := resolveTeamsCodexUserPath(ctx, cfg, paths, teamsCodexChildEnv(), workDir)
	if err != nil {
		return teamsCodexRuntimeContract{}, err
	}
	environment := codexRuntimeEnvironment(os.Environ(), append(teamsCodexChildEnv(), "PATH="+pathResult.Path), paths.ExecIdentity)
	environment = mergeCLIEnvironment(environment, codexHomeEnv(paths.CodexDir))

	command := strings.TrimSpace(rawCommand)
	if command == "" {
		if candidate, ok := findManagedCodexUpgradeCandidate(ctx, environment, paths.ExecIdentity); ok {
			command = candidate
		} else {
			command, err = ensureManagedTeamsCodexForRuntime(ctx, store, cfg, environment, paths.ExecIdentity, log)
			if err != nil {
				return teamsCodexRuntimeContract{}, fmt.Errorf("prepare CXP-managed Codex for the Teams target account: %w", err)
			}
		}
	} else if !strings.ContainsAny(command, `/\\`) {
		resolved, lookupErr := lookPathInEnvironment(command, environment)
		if lookupErr != nil {
			return teamsCodexRuntimeContract{}, fmt.Errorf("resolve explicit Teams Codex command %q: %w", command, lookupErr)
		}
		command = resolved
	} else if !executableExists(command) {
		return teamsCodexRuntimeContract{}, fmt.Errorf("explicit Teams Codex executable does not exist: %s", command)
	}

	contract := resolvedCodexRuntime{Command: normalizeExecutablePath(command), WrapperCommand: normalizeExecutablePath(command), Environment: environment}
	contract = applyTeamsUserPathRuntime(contract, pathResult, log)
	result := teamsCodexRuntimeContract{Runtime: contract, Identity: paths.ExecIdentity}
	result.Fingerprint = teamsCodexRuntimeFingerprint(result)
	return result, nil
}

func ensureManagedTeamsCodexRuntime(ctx context.Context, store *config.Store, cfg config.Config, environment []string, identity *execIdentity, log io.Writer) (string, error) {
	target := managedCodexUpgradeTarget{environment: append([]string(nil), environment...), identity: identity}
	opts := managedCodexUpgradeInstallOptions(target)
	opts.upgradeCodex = false
	opts.upgradeCodexPath = ""
	if upgradeUsesProxy(cfg) {
		profile, cfgWithProfile, err := ensureProfileRunFn(ctx, store, "", true, log)
		if err != nil {
			return "", err
		}
		opts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, profile, cfgWithProfile.Instances, func(profileEnv []string) error {
				return runInstall(managedCodexUpgradeProxyEnvironment(environment, profileEnv))
			})
		}
	}
	return ensureCodexInstalledForTargetRun(ctx, "", log, opts)
}

func teamsCodexRuntimeFingerprint(contract teamsCodexRuntimeContract) string {
	parts := []string{
		normalizeExecutablePath(contract.Runtime.Command),
		normalizeExecutablePath(contract.Runtime.WrapperCommand),
		envValue(contract.Runtime.Environment, "CODEX_HOME"),
		envValue(contract.Runtime.Environment, "PATH"),
		runtime.GOOS,
		runtime.GOARCH,
	}
	if contract.Identity != nil {
		parts = append(parts, strconv.FormatUint(uint64(contract.Identity.UID), 10), strconv.FormatUint(uint64(contract.Identity.GID), 10), contract.Identity.Home)
	}
	if info, err := os.Stat(contract.Runtime.Command); err == nil {
		parts = append(parts, filepath.Clean(contract.Runtime.Command), strconv.FormatInt(info.Size(), 10), strconv.FormatInt(info.ModTime().UnixNano(), 10))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:16])
}
