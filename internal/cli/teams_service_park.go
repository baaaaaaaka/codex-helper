package cli

import (
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/spf13/cobra"
)

func newTeamsServiceParkCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "park <session-id|codex-thread-id|teams-chat-id> <on|off|status>",
		Short: "Configure automatic idle parking for a work chat",
		Long:  "Configure automatic idle parking through the running helper. All work chats default to on. Off keeps an idle chat polling at the cold interval; it does not wake a parked chat or prevent manual park. The setting survives restarts.",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(2)(cmd, args); err != nil {
				return err
			}
			switch strings.ToLower(args[1]) {
			case "on", "off", "status":
				return nil
			}
			return fmt.Errorf("park requires on, off, or status")
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return submitTeamsCLIRequest(cmd.OutOrStdout(), teamsCLISubmission{req: engine.CLIRequest{
				Op: engine.CLISetParkPolicy, Selector: strings.TrimSpace(args[0]), Message: strings.ToLower(args[1]),
			}})
		},
	}
}
