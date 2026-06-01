package modelprofile

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestResolveDefaultModelProfile(t *testing.T) {
	got, err := Resolve(config.Config{Version: config.CurrentVersion}, "")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Name != config.DefaultModelProfileName || !got.IsDefault() || got.Provider.UsesAdapter {
		t.Fatalf("default resolved profile = %#v", got)
	}
}

func TestResolveThirdPartyModelProfileWithSSHProxy(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:        "ssh-1",
			Name:      "work",
			Host:      "host",
			Port:      22,
			User:      "user",
			CreatedAt: now,
		}},
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-work": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				SSHProxy:  "work",
				Revision:  3,
			},
		},
	}

	got, err := Resolve(cfg, "DeepSeek-Work")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Name != "deepseek-work" || got.Provider.ID != "deepseek" || got.SelectedPublicModel() != "deepseek/deepseek-v4-pro" || !got.Provider.UsesAdapter || got.Revision() != 3 {
		t.Fatalf("resolved profile = %#v", got)
	}
	if got.SSHProfile == nil || got.SSHProfile.ID != "ssh-1" {
		t.Fatalf("SSHProfile=%#v", got.SSHProfile)
	}
}

func TestResolveSnapshotPinsProfileFields(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:        "ssh-1",
			Name:      "jump",
			Host:      "host",
			Port:      22,
			User:      "user",
			CreatedAt: now,
		}},
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {
				Provider:  "mimo",
				APIKeyRef: "env:NEW_MIMO_KEY",
				SSHProxy:  "jump",
				Revision:  9,
			},
		},
	}
	snapshot := Snapshot{
		Name:      "mimo25",
		Provider:  "mimo",
		APIKeyRef: "env:OLD_MIMO_KEY",
		Model:     "mimo/mimo-v2.5-pro",
		SSHProxy:  "jump",
		Revision:  3,
	}
	got, err := ResolveSnapshot(cfg, snapshot)
	if err != nil {
		t.Fatalf("ResolveSnapshot: %v", err)
	}
	if got.Name != "mimo25" || got.Profile.APIKeyRef != "env:OLD_MIMO_KEY" || got.SelectedPublicModel() != "mimo/mimo-v2.5-pro" || got.Revision() != 3 || got.Provider.ID != "mimo" {
		t.Fatalf("resolved snapshot = %#v", got)
	}
	if got.SSHProfile == nil || got.SSHProfile.Name != "jump" {
		t.Fatalf("SSHProfile = %#v", got.SSHProfile)
	}
}

func TestRuntimeSnapshotCapturesAndValidatesRuntimeIdentity(t *testing.T) {
	cfg := config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:   "ssh-1",
			Name: "jump",
			Host: "host",
			Port: 22,
			User: "user",
		}},
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {
				Provider:  "mimo",
				Model:     "mimo/mimo-v2.5-pro",
				APIKeyRef: "env:MIMO_API_KEY",
				SSHProxy:  "jump",
				Revision:  2,
			},
		},
	}
	resolved, err := Resolve(cfg, "mimo25")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	snapshot, err := resolved.RuntimeSnapshot(time.Now(), nil, func(name string) string {
		if name != "MIMO_API_KEY" {
			t.Fatalf("env lookup name=%q", name)
		}
		return "sk-mimo-one"
	})
	if err != nil {
		t.Fatalf("RuntimeSnapshot: %v", err)
	}
	if snapshot.Model != "mimo/mimo-v2.5-pro" || snapshot.DefaultModel != "mimo/mimo-v2.5-pro" || snapshot.KeyFingerprint == "" || snapshot.BaseURLHash == "" || snapshot.ModelFingerprint == "" || snapshot.CatalogFingerprint == "" || snapshot.SSHProxyFingerprint == "" {
		t.Fatalf("runtime snapshot missing identity fields: %#v", snapshot)
	}
	if err := ValidateSnapshotRuntime(snapshot, resolved, "sk-mimo-one"); err != nil {
		t.Fatalf("ValidateSnapshotRuntime same key: %v", err)
	}
	if err := ValidateSnapshotRuntime(snapshot, resolved, "sk-mimo-two"); err == nil || !strings.Contains(err.Error(), "api key changed") {
		t.Fatalf("ValidateSnapshotRuntime changed key err=%v, want api key changed", err)
	}
	changedModel := snapshot
	changedModel.Model = "mimo/mimo-v2.5"
	if err := ValidateSnapshotRuntime(changedModel, resolved, "sk-mimo-one"); err == nil || !strings.Contains(err.Error(), "model changed") {
		t.Fatalf("ValidateSnapshotRuntime changed model err=%v, want model changed", err)
	}
	changedModelMapping := snapshot
	changedModelMapping.ModelFingerprint = "model:old"
	if err := ValidateSnapshotRuntime(changedModelMapping, resolved, "sk-mimo-one"); err == nil || !strings.Contains(err.Error(), "selected model mapping changed") {
		t.Fatalf("ValidateSnapshotRuntime changed model mapping err=%v, want selected model mapping changed", err)
	}
	additiveCatalogChange := snapshot
	additiveCatalogChange.ModelFingerprint = ""
	additiveCatalogChange.CatalogFingerprint = "catalog:old"
	if err := ValidateSnapshotRuntime(additiveCatalogChange, resolved, "sk-mimo-one"); err != nil {
		t.Fatalf("ValidateSnapshotRuntime old additive catalog fingerprint: %v", err)
	}
}

func TestResolveModelProfileErrors(t *testing.T) {
	cfg := config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"missing-key": {Provider: "deepseek"},
			"missing-ssh": {Provider: "deepseek", APIKeyRef: "env:DEEPSEEK_API_KEY", SSHProxy: "none"},
			"bad-model":   {Provider: "deepseek", Model: "nope", APIKeyRef: "env:DEEPSEEK_API_KEY"},
			"unknown":     {Provider: "unknown", APIKeyRef: "env:KEY"},
		},
	}
	for _, tc := range []struct {
		name string
		want string
	}{
		{"missing", "not found"},
		{"missing-key", "requires an api key"},
		{"missing-ssh", "missing ssh proxy"},
		{"bad-model", "available models"},
		{"unknown", "unknown model provider"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Resolve(cfg, tc.name)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Resolve error=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestSecretStoreAndAPIKeyRefs(t *testing.T) {
	store := NewSecretStore(filepath.Join(t.TempDir(), "secrets.json"))
	ref := SecretRefForProfile("deepseek-work")
	if err := store.Put(ref, "sk-test"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, err := ResolveAPIKey(ref, store, nil)
	if err != nil {
		t.Fatalf("ResolveAPIKey secret: %v", err)
	}
	if value != "sk-test" {
		t.Fatalf("secret value=%q", value)
	}
	value, err = ResolveAPIKey("env:MODEL_KEY", nil, func(name string) string {
		if name != "MODEL_KEY" {
			t.Fatalf("env lookup name=%q", name)
		}
		return "env-key"
	})
	if err != nil || value != "env-key" {
		t.Fatalf("ResolveAPIKey env value=%q err=%v", value, err)
	}
	if strings.Contains(MaskRef(ref), "deepseek-work") {
		t.Fatalf("MaskRef leaked secret key path: %q", MaskRef(ref))
	}
	if Fingerprint("sk-test") == "" || Fingerprint("sk-test") == Fingerprint("different") {
		t.Fatalf("Fingerprint not stable enough")
	}
}
