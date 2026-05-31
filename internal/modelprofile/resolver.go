package modelprofile

import (
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

type Snapshot struct {
	Name                string    `json:"name,omitempty"`
	Provider            string    `json:"provider,omitempty"`
	APIKeyRef           string    `json:"apiKeyRef,omitempty"`
	SSHProxy            string    `json:"sshProxy,omitempty"`
	Revision            int       `json:"revision,omitempty"`
	KeyFingerprint      string    `json:"keyFingerprint,omitempty"`
	BaseURLHash         string    `json:"baseUrlHash,omitempty"`
	AdapterProfile      string    `json:"adapterProfile,omitempty"`
	DefaultModel        string    `json:"defaultModel,omitempty"`
	CatalogFingerprint  string    `json:"catalogFingerprint,omitempty"`
	SSHProxyFingerprint string    `json:"sshProxyFingerprint,omitempty"`
	CapturedAt          time.Time `json:"capturedAt,omitempty"`
}

type Resolved struct {
	Name       string
	Profile    config.ModelProfile
	Provider   ProviderSpec
	SSHProfile *config.Profile
}

func (r Resolved) IsDefault() bool {
	return strings.EqualFold(r.Provider.ID, DefaultProvider)
}

func (r Resolved) Revision() int {
	if r.Profile.Revision <= 0 {
		return 1
	}
	return r.Profile.Revision
}

func (r Resolved) Snapshot(now time.Time) Snapshot {
	if now.IsZero() {
		now = time.Now()
	}
	name := strings.TrimSpace(r.Name)
	if name == "" {
		name = config.DefaultModelProfileName
	}
	provider := strings.TrimSpace(r.Profile.Provider)
	if provider == "" {
		provider = r.Provider.ID
	}
	if provider == "" {
		provider = DefaultProvider
	}
	return Snapshot{
		Name:                name,
		Provider:            provider,
		APIKeyRef:           strings.TrimSpace(r.Profile.APIKeyRef),
		SSHProxy:            strings.TrimSpace(r.Profile.SSHProxy),
		Revision:            r.Revision(),
		BaseURLHash:         BaseURLHash(r.Provider.BaseURL),
		AdapterProfile:      strings.TrimSpace(r.Provider.AdapterProfile),
		DefaultModel:        strings.TrimSpace(r.Provider.DefaultPublicModel()),
		CatalogFingerprint:  CatalogFingerprint(r.Provider),
		SSHProxyFingerprint: SSHProxyFingerprint(r.SSHProfile),
		CapturedAt:          now.UTC(),
	}
}

func (r Resolved) RuntimeSnapshot(now time.Time, secrets *SecretStore, env func(string) string) (Snapshot, error) {
	snapshot := r.Snapshot(now)
	if r.Provider.UsesAdapter {
		apiKey, err := ResolveAPIKey(r.Profile.APIKeyRef, secrets, env)
		if err != nil {
			return Snapshot{}, err
		}
		snapshot.KeyFingerprint = KeyFingerprint(apiKey, RuntimeKeySalt(r.Name, r.Revision()))
	}
	return snapshot, nil
}

func ValidateSnapshotRuntime(snapshot Snapshot, resolved Resolved, apiKey string) error {
	if snapshot.IsZero() {
		return nil
	}
	expected := resolved.Snapshot(snapshot.CapturedAt)
	if strings.TrimSpace(snapshot.BaseURLHash) != "" && snapshot.BaseURLHash != expected.BaseURLHash {
		return fmt.Errorf("model profile %q base URL changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.AdapterProfile) != "" && snapshot.AdapterProfile != expected.AdapterProfile {
		return fmt.Errorf("model profile %q adapter profile changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.DefaultModel) != "" && snapshot.DefaultModel != expected.DefaultModel {
		return fmt.Errorf("model profile %q default model changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.CatalogFingerprint) != "" && snapshot.CatalogFingerprint != expected.CatalogFingerprint {
		return fmt.Errorf("model profile %q model catalog changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.SSHProxyFingerprint) != "" && snapshot.SSHProxyFingerprint != expected.SSHProxyFingerprint {
		return fmt.Errorf("model profile %q ssh proxy changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.KeyFingerprint) != "" {
		got := KeyFingerprint(apiKey, RuntimeKeySalt(resolved.Name, resolved.Revision()))
		if got != snapshot.KeyFingerprint {
			return fmt.Errorf("model profile %q api key changed since the chat was pinned", snapshot.Name)
		}
	}
	return nil
}

func (s Snapshot) IsZero() bool {
	return strings.TrimSpace(s.Name) == "" &&
		strings.TrimSpace(s.Provider) == "" &&
		strings.TrimSpace(s.APIKeyRef) == "" &&
		strings.TrimSpace(s.SSHProxy) == "" &&
		s.Revision <= 0 &&
		strings.TrimSpace(s.KeyFingerprint) == "" &&
		strings.TrimSpace(s.BaseURLHash) == "" &&
		strings.TrimSpace(s.AdapterProfile) == "" &&
		strings.TrimSpace(s.DefaultModel) == "" &&
		strings.TrimSpace(s.CatalogFingerprint) == "" &&
		strings.TrimSpace(s.SSHProxyFingerprint) == "" &&
		s.CapturedAt.IsZero()
}

func (s Snapshot) IsDefault() bool {
	provider := strings.TrimSpace(s.Provider)
	name := strings.TrimSpace(s.Name)
	return strings.EqualFold(provider, DefaultProvider) ||
		(provider == "" && (name == "" || strings.EqualFold(name, config.DefaultModelProfileName)))
}

func Resolve(cfg config.Config, ref string) (Resolved, error) {
	name := strings.TrimSpace(ref)
	if name == "" {
		name = cfg.EffectiveDefaultModelProfile()
	}
	if name == "" {
		name = config.DefaultModelProfileName
	}

	var profile config.ModelProfile
	if strings.EqualFold(name, config.DefaultModelProfileName) {
		profile = config.ModelProfile{Provider: DefaultProvider, Revision: 1}
		name = config.DefaultModelProfileName
	} else {
		var ok bool
		name, profile, ok = findNamedModelProfile(cfg, name)
		if !ok {
			return Resolved{}, fmt.Errorf("model profile %q not found", strings.TrimSpace(ref))
		}
	}

	providerID := profile.Provider
	if strings.TrimSpace(providerID) == "" {
		providerID = DefaultProvider
	}
	spec, err := MustLookupProvider(providerID)
	if err != nil {
		return Resolved{}, err
	}
	if spec.UsesAdapter && strings.TrimSpace(profile.APIKeyRef) == "" {
		return Resolved{}, fmt.Errorf("model profile %q requires an api key for provider %q", name, spec.ID)
	}

	var sshProfile *config.Profile
	if strings.TrimSpace(profile.SSHProxy) != "" {
		p, ok := cfg.FindProfile(profile.SSHProxy)
		if !ok {
			return Resolved{}, fmt.Errorf("model profile %q references missing ssh proxy %q", name, profile.SSHProxy)
		}
		sshProfile = &p
	}

	return Resolved{
		Name:       name,
		Profile:    profile,
		Provider:   spec,
		SSHProfile: sshProfile,
	}, nil
}

func ResolveSnapshot(cfg config.Config, snapshot Snapshot) (Resolved, error) {
	if snapshot.IsZero() {
		return Resolve(cfg, "")
	}
	name := strings.TrimSpace(snapshot.Name)
	if name == "" {
		name = config.DefaultModelProfileName
	}
	providerID := strings.TrimSpace(snapshot.Provider)
	if providerID == "" {
		providerID = DefaultProvider
	}
	profile := config.ModelProfile{
		Provider:  providerID,
		APIKeyRef: strings.TrimSpace(snapshot.APIKeyRef),
		SSHProxy:  strings.TrimSpace(snapshot.SSHProxy),
		Revision:  snapshot.Revision,
	}
	if profile.Revision <= 0 {
		profile.Revision = 1
	}
	spec, err := MustLookupProvider(providerID)
	if err != nil {
		return Resolved{}, err
	}
	if spec.UsesAdapter && strings.TrimSpace(profile.APIKeyRef) == "" {
		return Resolved{}, fmt.Errorf("model profile %q requires an api key for provider %q", name, spec.ID)
	}
	var sshProfile *config.Profile
	if strings.TrimSpace(profile.SSHProxy) != "" {
		p, ok := cfg.FindProfile(profile.SSHProxy)
		if !ok {
			return Resolved{}, fmt.Errorf("model profile %q references missing ssh proxy %q", name, profile.SSHProxy)
		}
		sshProfile = &p
	}
	return Resolved{Name: name, Profile: profile, Provider: spec, SSHProfile: sshProfile}, nil
}

func findNamedModelProfile(cfg config.Config, ref string) (string, config.ModelProfile, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", config.ModelProfile{}, false
	}
	for name, profile := range cfg.ModelProfiles {
		if strings.EqualFold(strings.TrimSpace(name), ref) {
			return name, profile, true
		}
	}
	return "", config.ModelProfile{}, false
}
