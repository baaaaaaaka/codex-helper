package cli

import (
	"github.com/spf13/cobra"
)

var (
	version = "v0.0.9"
	commit  = ""
	date    = ""
)

type rootOptions struct {
	configPath string
}

func Execute() int {
	cmd := newRootCmd()
	if err := cmd.Execute(); err != nil {
		return 1
	}
	return 0
}

func newRootCmd() *cobra.Command {
	opts := &rootOptions{}

	cmd := &cobra.Command{
		Use:           "codex-proxy [profile]",
		Short:         "Browse Codex history in a terminal UI",
		SilenceErrors: false,
		SilenceUsage:  true,
		Version:       buildVersion(),
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = args
			return runDefaultTui(cmd, opts)
		},
	}

	cmd.PersistentFlags().StringVar(&opts.configPath, "config", "", "Override config file path (default: OS user config dir)")

	cmd.AddCommand(
		newInitCmd(opts),
		newRunCmd(opts),
		newTuiCmd(opts),
		newProxyCmd(opts),
		newUpgradeCmd(opts),
		newHistoryCmd(opts),
	)

	return cmd
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
