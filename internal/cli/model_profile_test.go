package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

func TestModelProfileSetupListDoctorAndDefault(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Now().UTC()
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:        "ssh-1",
			Name:      "work",
			Host:      "host",
			Port:      22,
			User:      "user",
			CreatedAt: now,
		}},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("DEEPSEEK_API_KEY", "sk-test-deepseek")

	out := runRootCommandForModelProfileTest(t,
		"--config", configPath,
		"model-profile", "setup", "deepseek-work",
		"--provider", "deepseek",
		"--model", "pro",
		"--api-key-env", "DEEPSEEK_API_KEY",
		"--ssh-proxy", "work",
		"--set-default",
	)
	for _, want := range []string{
		`Saved model profile "deepseek-work"`,
		"OK  model profile \"deepseek-work\"",
		"OK  provider deepseek",
		"OK  model deepseek/deepseek-v4-pro",
		"OK  api key env:DEEPSEEK_API_KEY fingerprint=",
		"OK  ssh proxy work",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("setup output missing %q:\n%s", want, out)
		}
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DefaultModelProfile != "deepseek-work" {
		t.Fatalf("DefaultModelProfile=%q", cfg.DefaultModelProfile)
	}
	mp := cfg.ModelProfiles["deepseek-work"]
	if mp.Provider != "deepseek" || mp.Model != "deepseek/deepseek-v4-pro" || mp.APIKeyRef != "env:DEEPSEEK_API_KEY" || mp.SSHProxy != "work" || mp.Revision != 1 {
		t.Fatalf("stored model profile=%#v", mp)
	}

	out = runRootCommandForModelProfileTest(t, "--config", configPath, "model-profile", "list")
	if !strings.Contains(out, "* deepseek-work: provider=deepseek model=deepseek/deepseek-v4-pro api_key=env:DEEPSEEK_API_KEY ssh_proxy=work revision=1") {
		t.Fatalf("list output did not mark default profile:\n%s", out)
	}

	out = runRootCommandForModelProfileTest(t, "--config", configPath, "model-profile", "set-default", "default")
	if !strings.Contains(out, "Default model profile: default") {
		t.Fatalf("set-default output:\n%s", out)
	}
}

func TestModelProfileSetupStoresSecretFromStdin(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader("sk-secret\n"))
	cmd.SetArgs([]string{
		"--config", configPath,
		"model-profile", "setup", "mimo25",
		"--provider", "mimo",
		"--model", "pro",
		"--api-key-stdin",
		"--no-doctor",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v\n%s", err, out.String())
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ref := cfg.ModelProfiles["mimo25"].APIKeyRef
	if ref != modelprofile.SecretRefForProfile("mimo25") {
		t.Fatalf("APIKeyRef=%q", ref)
	}
	if got := cfg.ModelProfiles["mimo25"].Model; got != "mimo/mimo-v2.5-pro" {
		t.Fatalf("Model=%q", got)
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	value, err := modelprofile.ResolveAPIKey(ref, secretStore, nil)
	if err != nil || value != "sk-secret" {
		t.Fatalf("ResolveAPIKey value=%q err=%v", value, err)
	}
}

func TestModelProfileSetupRejectsUnknownModel(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--config", configPath,
		"model-profile", "setup", "deepseek-work",
		"--provider", "deepseek",
		"--model", "ultra",
		"--api-key-env", "DEEPSEEK_API_KEY",
		"--no-doctor",
	})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "unknown model") || !strings.Contains(err.Error(), "deepseek/deepseek-v4-pro") {
		t.Fatalf("Execute err=%v out=%s", err, out.String())
	}
}

func TestTeamsModelProfileSetupGuideMentionsModelChoice(t *testing.T) {
	manager := newTeamsModelProfileManager(&rootOptions{configPath: filepath.Join(t.TempDir(), "config.json")})
	out, err := manager.ModelProfileSetupGuide(context.Background(), "deepseek")
	if err != nil {
		t.Fatalf("ModelProfileSetupGuide: %v", err)
	}
	for _, want := range []string{
		"`deepseek/deepseek-v4-flash`",
		"`deepseek/deepseek-v4-pro`",
		"--model <model>",
		"--teams-key-intake",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("setup guide missing %q:\n%s", want, out)
		}
	}
}

func TestTeamsModelProfileSetupGuideMentionsMiMoTierAliases(t *testing.T) {
	manager := newTeamsModelProfileManager(&rootOptions{configPath: filepath.Join(t.TempDir(), "config.json")})
	out, err := manager.ModelProfileSetupGuide(context.Background(), "mimo")
	if err != nil {
		t.Fatalf("ModelProfileSetupGuide: %v", err)
	}
	for _, want := range []string{
		"`mimo/mimo-v2.5`",
		"`mimo/mimo-v2.5-pro`",
		"aliases: base, standard",
		"aliases: pro",
		"--model <model>",
		"--teams-key-intake",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("setup guide missing %q:\n%s", want, out)
		}
	}
}

func TestTeamsModelProfileProvidersMentionsMiMoTierAliases(t *testing.T) {
	manager := newTeamsModelProfileManager(&rootOptions{configPath: filepath.Join(t.TempDir(), "config.json")})
	out, err := manager.ModelProfileProviders(context.Background())
	if err != nil {
		t.Fatalf("ModelProfileProviders: %v", err)
	}
	for _, want := range []string{
		"- mimo:",
		"`mimo/mimo-v2.5`",
		"`mimo/mimo-v2.5-pro`",
		"aliases: base, standard",
		"aliases: pro",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("providers output missing %q:\n%s", want, out)
		}
	}
}

func TestTeamsModelProfileManagerSaveModelProfileAPIKeyDefaultsModelForNewProfile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	manager := newTeamsModelProfileManager(&rootOptions{configPath: configPath})
	result, err := manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName: "mimo25",
		Provider:    "mimo",
		APIKey:      "sk-first-secret",
	})
	if err != nil {
		t.Fatalf("SaveModelProfileAPIKey: %v", err)
	}
	if result.Model != "mimo/mimo-v2.5" {
		t.Fatalf("default model = %q, want mimo base", result.Model)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.ModelProfiles["mimo25"].Model; got != "mimo/mimo-v2.5" {
		t.Fatalf("stored model = %q, want mimo base", got)
	}
}

func TestTeamsModelProfileManagerSaveModelProfileAPIKey(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Now().UTC()
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:        "ssh-1",
			Name:      "work",
			Host:      "host",
			Port:      22,
			User:      "user",
			CreatedAt: now,
		}},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	manager := newTeamsModelProfileManager(&rootOptions{configPath: configPath})
	result, err := manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName: "mimo25",
		Provider:    "mimo",
		Model:       "mimo/mimo-v2.5-pro",
		APIKey:      "sk-first-secret",
		SSHProxy:    "work",
		SetDefault:  true,
	})
	if err != nil {
		t.Fatalf("SaveModelProfileAPIKey first: %v", err)
	}
	if result.ProfileName != "mimo25" || result.Provider != "mimo" || result.Model != "mimo/mimo-v2.5-pro" || result.APIKeyRef != modelprofile.SecretRefForProfile("mimo25") || result.Revision != 1 || !result.SetDefault {
		t.Fatalf("first save result mismatch: %#v", result)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load after first save: %v", err)
	}
	if cfg.DefaultModelProfile != "mimo25" {
		t.Fatalf("DefaultModelProfile=%q", cfg.DefaultModelProfile)
	}
	profile := cfg.ModelProfiles["mimo25"]
	if profile.Provider != "mimo" || profile.Model != "mimo/mimo-v2.5-pro" || profile.APIKeyRef != modelprofile.SecretRefForProfile("mimo25") || profile.SSHProxy != "work" || profile.Revision != 1 {
		t.Fatalf("stored profile after first save = %#v", profile)
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	value, err := modelprofile.ResolveAPIKey(profile.APIKeyRef, secretStore, nil)
	if err != nil || value != "sk-first-secret" {
		t.Fatalf("first secret value_match=%v err=%v", value == "sk-first-secret", err)
	}

	result, err = manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName: "mimo25",
		Provider:    "mimo",
		APIKey:      "sk-first-secret",
		SSHProxy:    "work",
	})
	if err != nil {
		t.Fatalf("SaveModelProfileAPIKey same key: %v", err)
	}
	if result.Revision != 1 || result.Model != "mimo/mimo-v2.5-pro" {
		t.Fatalf("same key result = %#v, want revision 1 and preserved pro model", result)
	}

	result, err = manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName: "mimo25",
		Provider:    "mimo",
		APIKey:      "sk-second-secret",
		SSHProxy:    "work",
	})
	if err != nil {
		t.Fatalf("SaveModelProfileAPIKey rotated key: %v", err)
	}
	if result.Revision != 2 || result.Model != "mimo/mimo-v2.5-pro" {
		t.Fatalf("rotated key result = %#v, want revision 2 and preserved pro model", result)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatalf("Load after rotate: %v", err)
	}
	if cfg.ModelProfiles["mimo25"].Revision != 2 {
		t.Fatalf("stored revision after rotate=%d, want 2", cfg.ModelProfiles["mimo25"].Revision)
	}
	value, err = modelprofile.ResolveAPIKey(profile.APIKeyRef, secretStore, nil)
	if err != nil || value != "sk-second-secret" {
		t.Fatalf("rotated secret value_match=%v err=%v", value == "sk-second-secret", err)
	}

	result, err = manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName: "mimo25",
		Provider:    "mimo",
		Model:       "base",
		APIKey:      "sk-second-secret",
		SSHProxy:    "work",
	})
	if err != nil {
		t.Fatalf("SaveModelProfileAPIKey changed model: %v", err)
	}
	if result.Revision != 3 || result.Model != "mimo/mimo-v2.5" {
		t.Fatalf("changed model result = %#v, want revision 3 and mimo/mimo-v2.5", result)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatalf("Load after model change: %v", err)
	}
	if got := cfg.ModelProfiles["mimo25"]; got.Revision != 3 || got.Model != "mimo/mimo-v2.5" {
		t.Fatalf("stored profile after model change = %#v", got)
	}
}

func TestTeamsModelProfileManagerSimpleSetupReusesFamilyCredential(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	manager := newTeamsModelProfileManager(&rootOptions{configPath: configPath})
	result, err := manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName:     "mimo25",
		Provider:        "mimo",
		Model:           "mimo/mimo-v2.5",
		APIKey:          "sk-family",
		SetDefault:      true,
		CredentialScope: "mimo25",
	})
	if err != nil {
		t.Fatalf("SaveModelProfileAPIKey: %v", err)
	}
	familyRef := modelprofile.SecretRefForCredentialScope("mimo25")
	if result.APIKeyRef != familyRef {
		t.Fatalf("APIKeyRef=%q, want family ref %q", result.APIKeyRef, familyRef)
	}

	setup, err := manager.SetupModelProfile(context.Background(), teams.ModelProfileSetupRequest{
		Model:      "mimo-v2.5-pro",
		SetDefault: true,
	})
	if err != nil {
		t.Fatalf("SetupModelProfile: %v", err)
	}
	if setup.NeedsAPIKey || !setup.ReusedAPIKey || setup.ProfileName != "mimo25-pro" || setup.APIKeyRef != familyRef {
		t.Fatalf("setup result = %#v", setup)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DefaultModelProfile != "mimo25-pro" {
		t.Fatalf("DefaultModelProfile=%q", cfg.DefaultModelProfile)
	}
	if got := cfg.ModelProfiles["mimo25-pro"]; got.Model != "mimo/mimo-v2.5-pro" || got.APIKeyRef != familyRef {
		t.Fatalf("mimo25-pro profile=%#v", got)
	}
}

func TestModelSetupStoresFamilyCredentialAndSiblingReusesIt(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader("sk-mimo-family\n"))
	cmd.SetArgs([]string{
		"--config", configPath,
		"model", "setup", "mimo-v2.5",
		"--api-key-stdin",
		"--no-doctor",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("first setup: %v\n%s", err, out.String())
	}

	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load after first setup: %v", err)
	}
	familyRef := modelprofile.SecretRefForCredentialScope("mimo25")
	if cfg.DefaultModelProfile != "mimo25" {
		t.Fatalf("DefaultModelProfile after first setup = %q", cfg.DefaultModelProfile)
	}
	if got := cfg.ModelProfiles["mimo25"]; got.Provider != "mimo" || got.Model != "mimo/mimo-v2.5" || got.APIKeyRef != familyRef {
		t.Fatalf("mimo25 profile = %#v, want family credential %q", got, familyRef)
	}

	out = bytes.Buffer{}
	cmd = newRootCmd()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--config", configPath,
		"model", "setup", "mimo-v2.5-pro",
		"--no-doctor",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("second setup should reuse family key: %v\n%s", err, out.String())
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatalf("Load after second setup: %v", err)
	}
	if cfg.DefaultModelProfile != "mimo25-pro" {
		t.Fatalf("DefaultModelProfile after second setup = %q", cfg.DefaultModelProfile)
	}
	if got := cfg.ModelProfiles["mimo25-pro"]; got.Provider != "mimo" || got.Model != "mimo/mimo-v2.5-pro" || got.APIKeyRef != familyRef {
		t.Fatalf("mimo25-pro profile = %#v, want family credential %q", got, familyRef)
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	value, err := modelprofile.ResolveAPIKey(familyRef, secretStore, nil)
	if err != nil || value != "sk-mimo-family" {
		t.Fatalf("family credential value=%q err=%v", value, err)
	}
}

func TestModelUseCreatesSiblingProfileFromFamilyCredential(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	familyRef := modelprofile.SecretRefForCredentialScope("deepseek")
	if err := secretStore.Put(familyRef, "sk-deepseek-family"); err != nil {
		t.Fatalf("Put secret: %v", err)
	}
	out := runRootCommandForModelProfileTest(t, "--config", configPath, "model", "use", "deepseek-v4-pro")
	if !strings.Contains(out, "Default model: DeepSeek V4 Pro") {
		t.Fatalf("model use output:\n%s", out)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DefaultModelProfile != "deepseek-pro" {
		t.Fatalf("DefaultModelProfile=%q", cfg.DefaultModelProfile)
	}
	if got := cfg.ModelProfiles["deepseek-pro"]; got.Provider != "deepseek" || got.Model != "deepseek/deepseek-v4-pro" || got.APIKeyRef != familyRef {
		t.Fatalf("deepseek-pro profile=%#v, want family ref %q", got, familyRef)
	}
}

func TestModelListShowsDisplayNamesAndCustomDefaultModel(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Now().UTC()
	if err := store.Save(config.Config{
		Version:             config.CurrentVersion,
		DefaultModelProfile: "deepseek-work",
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-work": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  1,
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	out := runRootCommandForModelProfileTest(t, "--config", configPath, "model", "list")
	for _, want := range []string{
		"DeepSeek V4 Pro",
		"* 3. deepseek-v4-pro",
		"uses key env:DEEPSEEK_API_KEY",
		"MiMo 2.5 Pro",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("model list missing %q:\n%s", want, out)
		}
	}
}

func runRootCommandForModelProfileTest(t *testing.T, args ...string) string {
	t.Helper()
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute(%v): %v\n%s", args, err, out.String())
	}
	return out.String()
}
