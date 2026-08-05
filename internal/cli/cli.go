package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	version = "v0.1.22"
	commit  = ""
	date    = ""
)

type rootOptions struct {
	configPath       string
	upgradeCodex     bool
	upgradeCodexPath string
}

func Execute() int {
	if handled, err := runEarlyUserPathProbe(os.Args[1:]); handled {
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		return 0
	}
	if err := legacyUpdaterVersionPreflight(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: helper update compatibility check failed: %v\n", err)
		return 1
	}
	cmd := newRootCmd()
	if err := cmd.Execute(); err != nil {
		return 1
	}
	return 0
}

func newRootCmd() *cobra.Command {
	opts := &rootOptions{}

	cmd := &cobra.Command{
		Use:           "cxp [profile]",
		Short:         "Browse Codex history in a terminal UI",
		SilenceErrors: false,
		SilenceUsage:  true,
		Version:       buildVersion(),
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = args
			if strings.TrimSpace(opts.upgradeCodexPath) != "" && !opts.upgradeCodex {
				return fmt.Errorf("--upgrade-codex-path requires --upgrade-codex")
			}
			if opts.upgradeCodex {
				return runUpgradeCodexFromRoot(cmd, opts)
			}
			return runDefaultTui(cmd, opts)
		},
	}

	cmd.PersistentFlags().StringVar(&opts.configPath, "config", "", "Override config file path (default: OS user config dir)")
	cmd.Flags().BoolVar(&opts.upgradeCodex, "upgrade-codex", false, "Install or upgrade the CXP-managed Codex CLI")
	cmd.Flags().StringVar(&opts.upgradeCodexPath, "upgrade-codex-path", "", "Upgrade an explicit external Codex executable instead (requires --upgrade-codex)")

	cmd.AddCommand(
		newInternalNpmWrapperCmd(),
		newInternalUserPathProbeCmd(),
		newAppCmd(opts),
		newInitCmd(opts),
		newDelegateCmd(opts),
		newModelCmd(opts),
		newModelSourceCmd(opts),
		newModelProfileCmd(opts),
		newRunCmd(opts),
		newTuiCmd(opts),
		newBeaconCmd(opts),
		newTeamsCmd(opts),
		newProxyCmd(opts),
		newResponsesCmd(opts),
		newSkillsCmd(opts),
		newUpgradeCmd(opts),
		newHistoryCmd(opts),
		newSelftestCmd(opts),
	)

	return cmd
}

// RuntimeVersion is the build identity used by the immutable cxp runtime
// launcher before Cobra or any user configuration is initialized.
func RuntimeVersion() string {
	return buildVersion()
}

func buildVersion() string {
	v := version
	if commit != "" {
		v += " (" + commit + ")"
	}
	if date != "" {
		v += " " + date
	}
	return v
}
