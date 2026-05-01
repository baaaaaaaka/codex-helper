package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func newUpgradeCmd(_ *rootOptions) *cobra.Command {
	var repo string
	var versionOverride string
	var installPath string
	var teamsDrainTimeout time.Duration

	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade codex-proxy from GitHub releases",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			requested := update.ResolveVersion(versionOverride)
			if strings.EqualFold(requested, "latest") {
				status := checkForUpdate(ctx, update.CheckOptions{
					Repo:             repo,
					InstalledVersion: version,
					Timeout:          8 * time.Second,
				})
				if status.Supported && !status.UpdateAvailable {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Already up to date.")
					return nil
				}
			}

			finishTeams, err := prepareTeamsForHelperUpgrade(ctx, cmd.OutOrStdout(), teamsDrainTimeout)
			if err != nil {
				return err
			}
			res, err := performUpdate(ctx, update.UpdateOptions{
				Repo:        repo,
				Version:     versionOverride,
				InstallPath: installPath,
				Timeout:     120 * time.Second,
			})
			if finishTeams != nil {
				updateSucceeded := err == nil
				restartMode := teamsUpgradeRestartImmediate
				if updateSucceeded {
					if res.RestartRequired {
						restartMode = teamsUpgradeRestartDelayed
					}
				}
				if cleanupErr := finishTeams(context.Background(), teamsUpgradeFinishOptions{Success: updateSucceeded, ServiceRestart: restartMode}); cleanupErr != nil && err == nil {
					err = cleanupErr
				}
			}
			if err != nil {
				return err
			}

			if res.RestartRequired {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Update scheduled for v%s. Please restart `codex-proxy`.\n", res.Version)
				return nil
			}

			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Updated to v%s.\n", res.Version)
			return nil
		},
	}

	cmd.Flags().StringVar(&repo, "repo", "", "Override GitHub repo (owner/name)")
	cmd.Flags().StringVar(&versionOverride, "version", "", "Install a specific version (default: latest)")
	cmd.Flags().StringVar(&installPath, "install-path", "", "Override install path (file or directory)")
	cmd.Flags().DurationVar(&teamsDrainTimeout, "teams-drain-timeout", 2*time.Minute, "How long to wait for an active Teams bridge to drain before upgrading")

	return cmd
}
