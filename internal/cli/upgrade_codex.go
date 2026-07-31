package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func rootProfileArg(cmd *cobra.Command) (string, error) {
	all := cmd.Flags().Args()
	dash := cmd.Flags().ArgsLenAtDash()

	before := all
	after := []string{}
	if dash >= 0 {
		before = all[:dash]
		after = all[dash:]
	}

	if len(before) > 1 {
		return "", fmt.Errorf("unexpected args before -- (only profile is allowed)")
	}
	if len(after) > 0 {
		return "", fmt.Errorf("unexpected args after -- (use `codex-proxy run` to run commands)")
	}
	if len(before) == 1 {
		return before[0], nil
	}
	return "", nil
}

func runUpgradeCodexFromRoot(cmd *cobra.Command, root *rootOptions) error {
	profileRef, err := rootProfileArg(cmd)
	if err != nil {
		return err
	}
	explicitPath := ""
	configPath := ""
	if root != nil {
		explicitPath = strings.TrimSpace(root.upgradeCodexPath)
		configPath = root.configPath
	}
	if explicitPath != "" && !filepath.IsAbs(explicitPath) {
		return fmt.Errorf("--upgrade-codex-path must be an absolute executable path")
	}

	paths, err := resolveEffectiveLaunchPaths(configPath, "", "")
	if err != nil {
		return err
	}
	store, err := config.NewStore(paths.ConfigPath)
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}

	environment := codexRuntimeEnvironment(os.Environ(), nil, paths.ExecIdentity)
	target := managedCodexUpgradeTarget{environment: environment, identity: paths.ExecIdentity}
	managedTarget := explicitPath == ""
	if managedTarget {
		target = resolveManagedCodexUpgradeTarget(cmd.Context(), environment, paths.ExecIdentity)
	} else {
		target.path = normalizeExecutablePath(explicitPath)
		if !executableExists(target.path) {
			return fmt.Errorf("codex not found at %s", explicitPath)
		}
		if err := probeManagedCodexUpgradeCandidate(cmd.Context(), target.path, target.environment, target.identity); err != nil {
			return err
		}
	}

	installOpts := codexUpgradeTargetInstallOptions(target, managedTarget)
	if upgradeUsesProxy(cfg) {
		profile, err := selectProfile(cfg, profileRef)
		if err != nil {
			return err
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, profile, cfg.Instances, func(profileEnv []string) error {
				return runInstall(managedCodexUpgradeProxyEnvironment(target.environment, profileEnv))
			})
		}
	}

	if target.path != "" {
		kind := "CXP-managed"
		if !managedTarget {
			kind = "explicit external"
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex upgrade target (%s): %s\n", kind, target.path)
	} else if prefixes := managedCodexPrefixCandidates(target.environment); len(prefixes) > 0 {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex install target (CXP-managed): %s\n", prefixes[0])
	}

	var path string
	if managedTarget {
		path, err = upgradeOrInstallManagedCodex(cmd.Context(), cmd.ErrOrStderr(), target, installOpts)
	} else {
		path, err = upgradeCodexInstalledForTargetRun(cmd.Context(), cmd.ErrOrStderr(), installOpts)
	}
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex upgraded: %s\n", path)
	return nil
}

func upgradeUsesProxy(cfg config.Config) bool {
	if cfg.ProxyEnabled != nil {
		return *cfg.ProxyEnabled && len(cfg.Profiles) > 0
	}
	return len(cfg.Profiles) > 0
}
