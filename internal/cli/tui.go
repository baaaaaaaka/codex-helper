package cli

import (
	"time"

	"github.com/spf13/cobra"
)

func newTuiCmd(root *rootOptions) *cobra.Command {
	var codexDir string
	var codexPath string
	var profileRef string
	var refreshInterval time.Duration

	cmd := &cobra.Command{
		Use:   "tui",
		Short: "Browse Codex history in a terminal UI",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runHistoryTui(cmd, root, profileRef, codexDir, codexPath, refreshInterval)
		},
	}

	cmd.Flags().StringVar(&codexDir, "codex-dir", "", "Override Codex data dir (default: ~/.codex)")
	cmd.Flags().StringVar(&codexPath, "codex-path", "", "Override Codex CLI path (default: search PATH)")
	cmd.Flags().StringVar(&profileRef, "profile", "", "Proxy profile id or name")
	cmd.Flags().DurationVar(&refreshInterval, "refresh-interval", defaultRefreshInterval, "Auto-refresh interval (0 to disable)")
	return cmd
}
