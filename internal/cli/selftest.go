package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// newSelftestCmd returns a hidden readiness check: it verifies this binary can
// start and read its on-disk configuration, exercising the config version gate
// (see config.(*Store).loadUnlocked). It is side-effect-free — the config is
// only read, never written — and exits non-zero on failure, so it can gate a
// helper self-update before the new binary takes over from the old one.
func newSelftestCmd(opts *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:    "selftest",
		Short:  "Verify this binary can start and read its configuration",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := config.NewStore(opts.configPath)
			if err != nil {
				return fmt.Errorf("selftest: open config store: %w", err)
			}
			if _, err := store.Load(); err != nil {
				return fmt.Errorf("selftest: read config: %w", err)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "selftest: ok")
			return nil
		},
	}
}
