package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
)

func newTeamsMigrateCmd() *cobra.Command {
	var opt migrate.Options
	var noRollouts bool
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Convert a stopped v0.1.20 or rc.59 Teams store into core.sqlite",
		Long: "Convert a legacy Teams store (v0.1.20 or v0.1.22-rc.59) into the new core store. " +
			"The helper must be stopped. The source store is never modified or deleted; a marker file is added after a real migration. " +
			"Rollouts under the Codex sessions directory are only stat'ed and their first line read. " +
			"This command never contacts Teams.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if opt.Source == "" {
				return fmt.Errorf("--source is required (the Teams scope directory holding state.json)")
			}
			if noRollouts {
				opt.CodexSessions = ""
			} else if opt.CodexSessions == "" {
				home := os.Getenv("CODEX_HOME")
				if home == "" {
					if h, err := os.UserHomeDir(); err == nil {
						home = filepath.Join(h, ".codex")
					}
				}
				if home != "" {
					opt.CodexSessions = filepath.Join(home, "sessions")
				}
			}
			rep, err := migrate.Run(opt)
			if rep != nil {
				rep.WriteText(cmd.OutOrStdout())
			}
			return err
		},
	}
	f := cmd.Flags()
	f.StringVar(&opt.Source, "source", "", "Legacy Teams scope directory (holding state.json)")
	f.StringVar(&opt.Out, "out", "", "Directory for core.sqlite (default: the source directory)")
	f.BoolVar(&opt.DryRun, "dry-run", false, "Write the result and report into --out without touching the source")
	f.BoolVar(&opt.Immutable, "immutable", false, "Read an existing consistent snapshot directly instead of taking a backup")
	f.StringVar(&opt.CodexSessions, "codex-sessions", "", "Codex rollout directory (default: $CODEX_HOME/sessions or ~/.codex/sessions)")
	f.BoolVar(&noRollouts, "no-rollouts", false, "Skip the rollout stat/first-line scan")
	return cmd
}
