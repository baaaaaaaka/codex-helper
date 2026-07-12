package modelprofile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// HasConfiguredThirdPartyModels reports whether CXP should activate its
// unified model gateway. Official-only configurations must stay on Codex's
// native provider path without any gateway or catalog overrides.
func HasConfiguredThirdPartyModels(cfg config.Config) bool {
	for _, profile := range cfg.ModelProfiles {
		if strings.TrimSpace(profile.Source) != "" && strings.TrimSpace(profile.VerificationFingerprint) == "" {
			continue
		}
		provider := strings.TrimSpace(profile.Provider)
		if provider != "" && !strings.EqualFold(provider, DefaultProvider) {
			return true
		}
	}
	return false
}

// ResolveConfiguredThirdPartyModels strictly resolves every configured
// third-party profile in stable name order. A broken configured route is an
// error rather than a silently missing model in Codex's picker.
func ResolveConfiguredThirdPartyModels(cfg config.Config) ([]Resolved, error) {
	names := make([]string, 0, len(cfg.ModelProfiles))
	for name, profile := range cfg.ModelProfiles {
		if strings.TrimSpace(profile.Source) != "" && strings.TrimSpace(profile.VerificationFingerprint) == "" {
			continue
		}
		provider := strings.TrimSpace(profile.Provider)
		if provider == "" || strings.EqualFold(provider, DefaultProvider) {
			continue
		}
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	resolved := make([]Resolved, 0, len(names))
	for _, name := range names {
		profile, err := Resolve(cfg, name)
		if err != nil {
			return nil, fmt.Errorf("resolve configured third-party model profile %q: %w", name, err)
		}
		if profile.IsDefault() || !profile.Provider.UsesAdapter {
			return nil, fmt.Errorf("configured third-party model profile %q does not use a CXP adapter", name)
		}
		resolved = append(resolved, profile)
	}
	return resolved, nil
}
