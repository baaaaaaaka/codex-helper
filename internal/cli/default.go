package cli

import "github.com/spf13/cobra"

func runDefaultTui(cmd *cobra.Command, root *rootOptions) error {
	profileRef, err := rootProfileArg(cmd)
	if err != nil {
		return err
	}

	return runHistoryTui(cmd, root, profileRef, "", "", defaultRefreshInterval)
}
