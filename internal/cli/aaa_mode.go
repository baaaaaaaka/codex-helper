package cli

import "github.com/baaaaaaaka/codex-helper/internal/config"

func resolveAAAEnabled(cfg config.Config) bool {
	return cfg.AgentAutoApproveEnabled != nil && *cfg.AgentAutoApproveEnabled
}

func persistAAAEnabled(store *config.Store, enabled bool) error {
	return store.Update(func(cfg *config.Config) error {
		cfg.AgentAutoApproveEnabled = &enabled
		return nil
	})
}
