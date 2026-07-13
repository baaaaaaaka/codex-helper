package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

func stubTeamsModelVerification(t *testing.T) {
	t.Helper()
	previous := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previous })
	verifyConfiguredModelAuthenticationFn = func(context.Context, modelprofile.Resolved, string) error { return nil }
}

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
	if !strings.Contains(out, "* deepseek-work: provider=deepseek model=deepseek/deepseek-v4-pro base_url=none api_key=env:DEEPSEEK_API_KEY ssh_proxy=work revision=1") {
		t.Fatalf("list output did not mark default profile:\n%s", out)
	}

	out = runRootCommandForModelProfileTest(t, "--config", configPath, "model-profile", "set-default", "default")
	if !strings.Contains(out, "Default model profile: default") {
		t.Fatalf("set-default output:\n%s", out)
	}
}

func TestModelProfileSetupConfiguresResponsesCompatibleProviderFromEnvironment(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("TEST_RESPONSES_BASE_URL", "https://responses.example.invalid/v1/")
	t.Setenv("TEST_RESPONSES_API_KEY", "sk-responses-test")

	out := runRootCommandForModelProfileTest(t,
		"--config", configPath,
		"model-profile", "setup", "custom-responses",
		"--provider", "responses-compatible",
		"--model", "example/reasoning-model",
		"--base-url-env", "TEST_RESPONSES_BASE_URL",
		"--api-key-env", "TEST_RESPONSES_API_KEY",
		"--no-doctor",
	)
	if !strings.Contains(out, `Saved model profile "custom-responses"`) || !strings.Contains(out, "base_url=configured") {
		t.Fatalf("setup output:\n%s", out)
	}

	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	profile := cfg.ModelProfiles["custom-responses"]
	if profile.Provider != "responses-compatible" || profile.Model != "example/reasoning-model" || profile.BaseURL != "https://responses.example.invalid/v1" || profile.APIKeyRef != "env:TEST_RESPONSES_API_KEY" {
		t.Fatalf("stored profile=%#v", profile)
	}
	out = runRootCommandForModelProfileTest(t, "--config", configPath, "model-profile", "doctor", "custom-responses")
	if !strings.Contains(out, "OK  web search fallback gpt-5.6-luna reasoning=high mode=live") {
		t.Fatalf("doctor output omitted web-search fallback:\n%s", out)
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
	if strings.Contains(out, "--set-default") {
		t.Fatalf("setup guide still couples availability to defaults:\n%s", out)
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
	if strings.Contains(out, "--set-default") {
		t.Fatalf("setup guide still couples availability to defaults:\n%s", out)
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

func TestTeamsModelListShowsOnlyCurrentlyAuthenticationVerifiedModels(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	if err := secrets.Put("secret:verified", "key-a"); err != nil {
		t.Fatal(err)
	}
	if err := secrets.Put("secret:stale", "key-b"); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"verified":   {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "secret:verified", Revision: 1},
		"unverified": {Provider: "deepseek", Model: "deepseek/deepseek-v4-flash", APIKeyRef: "env:MISSING_KEY", Revision: 1},
		"stale":      {Provider: "mimo", Model: "mimo/mimo-v2.5-pro", APIKeyRef: "secret:stale", Revision: 1, VerificationFingerprint: "stale"},
	}}
	resolved, err := modelprofile.Resolve(cfg, "verified")
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.ModelProfiles["verified"]
	p.VerificationFingerprint = modelVerificationFingerprint("", resolved, "key-a")
	p.VerifiedAt = time.Now()
	cfg.ModelProfiles["verified"] = p
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}

	previousProbe := codexLoginStatusProbeFn
	previousCatalog := listTeamsOfficialModelsFn
	t.Cleanup(func() {
		codexLoginStatusProbeFn = previousProbe
		listTeamsOfficialModelsFn = previousCatalog
	})
	codexLoginStatusProbeFn = func(_ context.Context, path string, _ []string, _ io.Writer) bool {
		if path != "/actual/codex" {
			t.Fatalf("probe path = %q", path)
		}
		return true
	}
	listTeamsOfficialModelsFn = func(_ context.Context, path string) ([]teamsOfficialModel, error) {
		if path != "/actual/codex" {
			t.Fatalf("catalog path = %q", path)
		}
		return []teamsOfficialModel{
			{Slug: "gpt-5.6-luna", DisplayName: "GPT-5.6-Luna", DefaultReasoningLevel: "medium"},
			{Slug: "gpt-5.6-sol", DisplayName: "GPT-5.6-Sol", IsDefault: true, DefaultReasoningLevel: "low"},
		}, nil
	}
	manager := newTeamsModelProfileManager(&rootOptions{configPath: configPath}, "/actual/codex")
	out, err := manager.ListModelProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Verified models",
		"global default: GPT-5.6-Sol (`gpt-5.6-sol`)",
		"Codex account default: GPT-5.6-Sol (`gpt-5.6-sol`)",
		"Official Codex models",
		"GPT-5.6-Sol (`gpt-5.6-sol`) [account default] effort=low",
		"GPT-5.6-Luna (`gpt-5.6-luna`) effort=medium",
		"Verified third-party profiles",
		"verified: MiMo 2.5 (MiMo, model `mimo/mimo-v2.5`, selector `profile:verified`) [verified]",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("list missing %q:\n%s", want, out)
		}
	}
	for _, hidden := range []string{"unverified", "stale:", "needs key", "DeepSeek V4 Pro", "Codex Auto Review"} {
		if strings.Contains(out, hidden) {
			t.Fatalf("list exposed %q:\n%s", hidden, out)
		}
	}
	if err := store.Update(func(current *config.Config) error {
		current.DefaultModelProfile = "verified"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	out, err = manager.ListModelProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "global default: MiMo 2.5") || !strings.Contains(out, "Codex account default: GPT-5.6-Sol (`gpt-5.6-sol`)") {
		t.Fatalf("third-party default is ambiguous:\n%s", out)
	}
}

func TestTeamsModelListDoesNotReadOrShowOfficialCatalogWhenLoggedOut(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	previousProbe := codexLoginStatusProbeFn
	previousCatalog := listTeamsOfficialModelsFn
	t.Cleanup(func() {
		codexLoginStatusProbeFn = previousProbe
		listTeamsOfficialModelsFn = previousCatalog
	})
	codexLoginStatusProbeFn = func(context.Context, string, []string, io.Writer) bool { return false }
	listTeamsOfficialModelsFn = func(context.Context, string) ([]teamsOfficialModel, error) {
		t.Fatal("logged-out model list must not load the official catalog")
		return nil, nil
	}
	out, err := newTeamsModelProfileManager(&rootOptions{configPath: configPath}, "/actual/codex").ListModelProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "Codex Official") || strings.Contains(out, "Official Codex models") {
		t.Fatalf("logged-out list exposed official models:\n%s", out)
	}
}

func TestTeamsModelProfileRuntimeWarningShowsActiveBackupJSON(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"work-glm": {Provider: "glm", Source: "models", Revision: 1},
		},
		ModelSources: map[string]config.ModelSource{
			"models": {
				URL:                     "https://example.invalid/models.git",
				Revision:                "4d19b77a12345678",
				BackupActive:            true,
				BackupAttemptedRevision: "8f3a1c2b87654321",
				BackupReason:            "validate cxp-models.json: unknown field badSetting",
				Profiles:                []string{"work-glm"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	warning, active, err := newTeamsModelProfileManager(&rootOptions{configPath: configPath}).ModelProfileRuntimeWarning(context.Background(), modelprofile.Snapshot{Name: "work-glm", Provider: "glm"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Backup JSON configuration is active", "work-glm", "4d19b77a1234", "8f3a1c2b8765", "badSetting", "every turn"} {
		if !active || !strings.Contains(warning, want) {
			t.Fatalf("warning active=%v missing %q:\n%s", active, want, warning)
		}
	}
}

func TestTeamsKeyIntakeVerificationFailureStaysHidden(t *testing.T) {
	previous := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previous })
	verifyConfiguredModelAuthenticationFn = func(context.Context, modelprofile.Resolved, string) error { return fmt.Errorf("unauthorized") }
	configPath := filepath.Join(t.TempDir(), "config.json")
	manager := newTeamsModelProfileManager(&rootOptions{configPath: configPath})
	_, err := manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName: "mimo25", Provider: "mimo", Model: "mimo/mimo-v2.5", APIKey: "secret-value",
	})
	if err == nil || !strings.Contains(err.Error(), "remains hidden") {
		t.Fatalf("Save error = %v", err)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	profile := cfg.ModelProfiles["mimo25"]
	if profile.VerificationFingerprint != "" || !strings.Contains(profile.VerificationError, "unauthorized") {
		t.Fatalf("failed profile = %#v", profile)
	}
}

func TestTeamsModelProfileManagerSaveModelProfileAPIKeyDefaultsModelForNewProfile(t *testing.T) {
	stubTeamsModelVerification(t)
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
	stubTeamsModelVerification(t)
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
		SetDefault:  false,
	})
	if err != nil {
		t.Fatalf("SaveModelProfileAPIKey first: %v", err)
	}
	if result.ProfileName != "mimo25" || result.Provider != "mimo" || result.Model != "mimo/mimo-v2.5-pro" || result.APIKeyRef != modelprofile.SecretRefForProfile("mimo25") || result.Revision != 1 || result.SetDefault {
		t.Fatalf("first save result mismatch: %#v", result)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load after first save: %v", err)
	}
	if cfg.DefaultModelProfile != "" || cfg.Defaults != nil {
		t.Fatalf("API-key save changed global defaults: legacy=%q typed=%#v", cfg.DefaultModelProfile, cfg.Defaults)
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
	stubTeamsModelVerification(t)
	configPath := filepath.Join(t.TempDir(), "config.json")
	manager := newTeamsModelProfileManager(&rootOptions{configPath: configPath})
	result, err := manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName:     "mimo25",
		Provider:        "mimo",
		Model:           "mimo/mimo-v2.5",
		APIKey:          "sk-family",
		SetDefault:      false,
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
		Model: "mimo-v2.5-pro",
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
	if cfg.DefaultModelProfile != "" || cfg.Defaults != nil {
		t.Fatalf("setup changed global defaults: legacy=%q typed=%#v", cfg.DefaultModelProfile, cfg.Defaults)
	}
	if got := cfg.ModelProfiles["mimo25-pro"]; got.Model != "mimo/mimo-v2.5-pro" || got.APIKeyRef != familyRef {
		t.Fatalf("mimo25-pro profile=%#v", got)
	}
}

func TestTeamsModelProfileManagerSetupCannotMutateGlobalDefault(t *testing.T) {
	manager := newTeamsModelProfileManager(&rootOptions{configPath: filepath.Join(t.TempDir(), "config.json")})
	if _, err := manager.SetupModelProfile(context.Background(), teams.ModelProfileSetupRequest{
		Model: "default", SetDefault: true,
	}); err == nil || !strings.Contains(err.Error(), "default model set") {
		t.Fatalf("SetupModelProfile SetDefault error = %v", err)
	}
	if _, err := manager.SaveModelProfileAPIKey(context.Background(), teams.ModelProfileAPIKeySaveRequest{
		ProfileName: "mimo25", Provider: "mimo", APIKey: "sk-test", SetDefault: true,
	}); err == nil || !strings.Contains(err.Error(), "default model set") {
		t.Fatalf("SaveModelProfileAPIKey SetDefault error = %v", err)
	}
}

func TestModelSetupStoresFamilyCredentialAndSiblingReusesIt(t *testing.T) {
	stubTeamsModelVerification(t)
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

func TestModelUseRejectsUnverifiedFamilyCredential(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	familyRef := modelprofile.SecretRefForCredentialScope("deepseek")
	if err := secretStore.Put(familyRef, "sk-deepseek-family"); err != nil {
		t.Fatalf("Put secret: %v", err)
	}
	err, out := runRootCommandForModelProfileTestError("--config", configPath, "model", "use", "deepseek-v4-pro")
	if err == nil || !strings.Contains(err.Error(), "not authentication-verified") {
		t.Fatalf("model use err=%v out=%s", err, out)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DefaultModelProfile != "" {
		t.Fatalf("DefaultModelProfile=%q", cfg.DefaultModelProfile)
	}
	if _, ok := cfg.ModelProfiles["deepseek-pro"]; ok {
		t.Fatal("model use synthesized an unverified profile")
	}
}

func TestModelListShowsNeedsSetupWhenFamilyCredentialExistsWithoutProfile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	familyRef := modelprofile.SecretRefForCredentialScope("deepseek")
	if err := secretStore.Put(familyRef, "sk-deepseek-family"); err != nil {
		t.Fatalf("Put secret: %v", err)
	}

	out := runRootCommandForModelProfileTest(t, "--config", configPath, "model", "list")
	for _, want := range []string{
		"deepseek-v4-flash",
		"DeepSeek V4 Flash",
		"deepseek-v4-pro",
		"needs setup (key secret:<saved>)",
		"mimo-v2.5",
		"needs key",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("model list missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "uses key secret:<saved>") {
		t.Fatalf("model list should not mark missing recommended profiles as using the key:\n%s", out)
	}
}

func TestModelDoctorExplainsMissingRecommendedProfileWithReusableCredential(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	if err := secretStore.Put(modelprofile.SecretRefForCredentialScope("deepseek"), "sk-deepseek-family"); err != nil {
		t.Fatalf("Put secret: %v", err)
	}

	err, out := runRootCommandForModelProfileTestError("--config", configPath, "model", "doctor", "deepseek-v4-flash")
	if err == nil {
		t.Fatalf("model doctor unexpectedly succeeded:\n%s", out)
	}
	body := err.Error() + "\n" + out
	for _, want := range []string{
		"model deepseek-v4-flash has a saved DeepSeek API key",
		"profile \"deepseek-flash\" is not configured",
		"run `model setup deepseek-v4-flash`",
		"retry `model doctor deepseek-v4-flash`",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("model doctor error missing %q:\n%s", want, body)
		}
	}
}

func TestTeamsModelProfileDoctorExplainsMissingRecommendedProfileWithReusableCredential(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	if err := secretStore.Put(modelprofile.SecretRefForCredentialScope("deepseek"), "sk-deepseek-family"); err != nil {
		t.Fatalf("Put secret: %v", err)
	}
	manager := newTeamsModelProfileManager(&rootOptions{configPath: configPath})

	out, err := manager.ModelProfileDoctor(context.Background(), "deepseek-v4-flash")
	if err == nil {
		t.Fatalf("ModelProfileDoctor unexpectedly succeeded:\n%s", out)
	}
	for _, want := range []string{
		"model deepseek-v4-flash has a saved DeepSeek API key",
		"profile \"deepseek-flash\" is not configured",
		"run `model setup deepseek-v4-flash`",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Teams doctor error missing %q:\n%s", want, err.Error())
		}
	}
}

func TestModelDoctorValidatesCustomProfileBackingModelChoice(t *testing.T) {
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
	t.Setenv("DEEPSEEK_API_KEY", "sk-deepseek-custom-profile")

	out := runRootCommandForModelProfileTest(t, "--config", configPath, "model", "doctor", "deepseek-v4-pro")
	for _, want := range []string{
		"OK  model profile \"deepseek-work\"",
		"OK  provider deepseek",
		"OK  model deepseek/deepseek-v4-pro",
		"OK  api key env:DEEPSEEK_API_KEY",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("model doctor output missing %q:\n%s", want, out)
		}
	}
}

func TestTeamsModelProfileDoctorValidatesCustomProfileBackingModelChoice(t *testing.T) {
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
	t.Setenv("DEEPSEEK_API_KEY", "sk-deepseek-custom-profile")
	manager := newTeamsModelProfileManager(&rootOptions{configPath: configPath})

	out, err := manager.ModelProfileDoctor(context.Background(), "deepseek-v4-pro")
	if err != nil {
		t.Fatalf("ModelProfileDoctor error: %v\n%s", err, out)
	}
	for _, want := range []string{
		"OK  model profile \"deepseek-work\"",
		"OK  provider deepseek",
		"OK  model deepseek/deepseek-v4-pro",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("Teams doctor output missing %q:\n%s", want, out)
		}
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
		"[global default] 3. deepseek-v4-pro",
		"ready (profile deepseek-work)",
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

func runRootCommandForModelProfileTestError(args ...string) (error, string) {
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return err, out.String()
}
