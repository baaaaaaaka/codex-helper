package cli

import (
	"context"
	"fmt"

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

	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}

	installOpts := codexInstallOptions{upgradeCodex: true}
	if upgradeUsesProxy(cfg) {
		profile, err := selectProfile(cfg, profileRef)
		if err != nil {
			return err
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, profile, cfg.Instances, runInstall)
		}
	}

	path, err := upgradeCodexInstalledWithOptions(cmd.Context(), cmd.ErrOrStderr(), installOpts)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex upgraded: %s\n", path)
	return nil
}

func upgradeUsesProxy(cfg config.Config) bool {
	if cfg.ProxyEnabled != nil {
		return *cfg.ProxyEnabled
	}
	return len(cfg.Profiles) > 0
}
