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
		"--api-key-env", "DEEPSEEK_API_KEY",
		"--ssh-proxy", "work",
		"--set-default",
	)
	for _, want := range []string{
		`Saved model profile "deepseek-work"`,
		"OK  model profile \"deepseek-work\"",
		"OK  provider deepseek",
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
	if mp.Provider != "deepseek" || mp.APIKeyRef != "env:DEEPSEEK_API_KEY" || mp.SSHProxy != "work" || mp.Revision != 1 {
		t.Fatalf("stored model profile=%#v", mp)
	}

	out = runRootCommandForModelProfileTest(t, "--config", configPath, "model-profile", "list")
	if !strings.Contains(out, "* deepseek-work: provider=deepseek api_key=env:DEEPSEEK_API_KEY ssh_proxy=work revision=1") {
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
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(configPath))
	value, err := modelprofile.ResolveAPIKey(ref, secretStore, nil)
	if err != nil || value != "sk-secret" {
		t.Fatalf("ResolveAPIKey value=%q err=%v", value, err)
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
		APIKey:      "sk-first-secret",
		SSHProxy:    "work",
		SetDefault:  true,
	})
	if err != nil {
		t.Fatalf("SaveModelProfileAPIKey first: %v", err)
	}
	if result.ProfileName != "mimo25" || result.Provider != "mimo" || result.APIKeyRef != modelprofile.SecretRefForProfile("mimo25") || result.Revision != 1 || !result.SetDefault {
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
	if profile.Provider != "mimo" || profile.APIKeyRef != modelprofile.SecretRefForProfile("mimo25") || profile.SSHProxy != "work" || profile.Revision != 1 {
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
	if result.Revision != 1 {
		t.Fatalf("same key revision=%d, want 1", result.Revision)
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
	if result.Revision != 2 {
		t.Fatalf("rotated key revision=%d, want 2", result.Revision)
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
