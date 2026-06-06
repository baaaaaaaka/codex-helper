package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
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
	var includePrerelease bool

	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade codex-proxy from GitHub releases",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("upgrade the Teams helper", "helper reload now"); err != nil {
				return err
			}
			ctx := cmd.Context()

			requested := update.ResolveVersion(versionOverride)
			if strings.EqualFold(requested, "latest") {
				status := checkForUpdate(ctx, update.CheckOptions{
					Repo:              repo,
					InstalledVersion:  version,
					Timeout:           8 * time.Second,
					IncludePrerelease: includePrerelease,
				})
				if status.Supported && !status.UpdateAvailable {
					if err := rescueTeamsForNoopHelperUpgrade(ctx, cmd.InOrStdin(), cmd.OutOrStdout(), teamsDrainTimeout, installPath); err != nil {
						return err
					}
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Already up to date.")
					return nil
				}
			}

			var finishTeams teamsUpgradeFinalizer
			var res update.ApplyResult
			err := withTeamsHelperUpgradeInstallLock(ctx, installPath, func() error {
				var prepareErr error
				finishTeams, prepareErr = prepareTeamsForHelperUpgrade(ctx, cmd.InOrStdin(), cmd.OutOrStdout(), teamsDrainTimeout, nil)
				if prepareErr != nil {
					return prepareErr
				}
				res, prepareErr = performUpdate(ctx, update.UpdateOptions{
					Repo:              repo,
					Version:           versionOverride,
					InstallPath:       installPath,
					Timeout:           120 * time.Second,
					ValidateBinary:    true,
					IncludePrerelease: includePrerelease,
				})
				updateErr := prepareErr
				if finishTeams != nil {
					replacementPending := res.RestartRequired || strings.TrimSpace(res.PendingReplacePath) != ""
					updateSucceeded := updateErr == nil && !replacementPending
					restartMode := teamsUpgradeRestartImmediate
					if updateErr == nil && replacementPending {
						restartMode = teamsUpgradeRestartDelayed
					}
					if cleanupErr := finishTeams(context.Background(), teamsUpgradeFinishOptions{
						Success:            updateSucceeded,
						ServiceRestart:     restartMode,
						InstallPath:        res.InstallPath,
						PendingReplacePath: res.PendingReplacePath,
					}); cleanupErr != nil && updateErr == nil {
						updateErr = cleanupErr
					}
				}
				return updateErr
			})
			if err != nil {
				return err
			}

			if res.RestartRequired {
				installBundledSkillsFromHelper(ctx, firstNonEmptyString(res.PendingReplacePath, res.InstallPath), cmd.ErrOrStderr())
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Update replacement for v%s is pending. Restart `codex-proxy`, then verify `codex-proxy --version` before treating the update as installed.\n", res.Version)
				return nil
			}

			installBundledSkillsFromHelper(ctx, res.InstallPath, cmd.ErrOrStderr())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Updated to v%s.\n", res.Version)
			return nil
		},
	}

	cmd.Flags().StringVar(&repo, "repo", "", "Override GitHub repo (owner/name)")
	cmd.Flags().StringVar(&versionOverride, "version", "", "Install a specific version (default: latest)")
	cmd.Flags().BoolVar(&includePrerelease, "include-prerelease", false, "Allow latest to resolve to the newest GitHub prerelease")
	cmd.Flags().StringVar(&installPath, "install-path", "", "Override install path (file or directory)")
	cmd.Flags().DurationVar(&teamsDrainTimeout, "teams-drain-timeout", 2*time.Minute, "How long to wait for an active Teams bridge to drain before upgrading")

	return cmd
}

func installBundledSkillsFromHelper(ctx context.Context, helperPath string, out io.Writer) {
	helperPath = strings.TrimSpace(helperPath)
	if helperPath == "" {
		return
	}
	installCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	cmd := exec.CommandContext(installCtx, helperPath, "skills", "install-builtin", "--yes")
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			detail = ": " + detail
		}
		_, _ = fmt.Fprintf(out, "Warning: failed to install built-in cxp skill after upgrade; run `%s skills install-builtin --yes` to retry%s\n", helperPath, detail)
		return
	}
	if detail := strings.TrimSpace(stderr.String()); detail != "" {
		_, _ = fmt.Fprintln(out, detail)
	}
}
