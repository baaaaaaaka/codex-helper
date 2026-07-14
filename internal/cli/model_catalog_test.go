package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelcatalog"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

func TestDueModelCatalogsHonorsAutoSyncAndStableOrder(t *testing.T) {
	now := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)
	cfg := config.Config{ModelCatalogs: map[string]config.ModelCatalog{
		"recent": {Type: config.ModelCatalogTypeGit, AutoSync: true, SyncedAt: now.Add(-time.Minute)},
		"due-b":  {Type: config.ModelCatalogTypeGit, AutoSync: true, SyncedAt: now.Add(-time.Hour)},
		"due-a":  {Type: config.ModelCatalogTypeGit, AutoSync: true, SyncedAt: now.Add(-31 * time.Minute)},
		"manual": {Type: config.ModelCatalogTypeManagedJSON, AutoSync: true},
	}}
	got := dueModelCatalogs(cfg, now, 30*time.Minute)
	if strings.Join(got, ",") != "due-a,due-b" {
		t.Fatalf("due catalogs=%v", got)
	}
	delayCfg := config.Config{ModelCatalogs: map[string]config.ModelCatalog{"recent": cfg.ModelCatalogs["recent"]}}
	if got := nextModelCatalogAutoSyncDelay(delayCfg, now, 30*time.Minute); got != 29*time.Minute {
		t.Fatalf("next delay=%s, want 29m", got)
	}
}

func TestModelCatalogAutoSyncFailureUsesNormalRetryInterval(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.Save(config.Config{ModelConfigVersion: config.CurrentModelConfigVersion, ModelCatalogs: map[string]config.ModelCatalog{
		"broken": {Type: config.ModelCatalogTypeGit, URL: filepath.Join(t.TempDir(), "missing.git"), File: "models.json", AutoSync: true, SyncedAt: now.Add(-time.Hour)},
	}}); err != nil {
		t.Fatal(err)
	}
	var warnings strings.Builder
	delay := runModelCatalogAutoSyncOnce(context.Background(), &rootOptions{configPath: configPath}, &warnings, now, 30*time.Minute)
	if delay != 30*time.Minute || !strings.Contains(warnings.String(), "Model catalog auto-sync warning") {
		t.Fatalf("delay=%s warnings=%q", delay, warnings.String())
	}
}

func TestCatalogInstallPreservesOfficialGlobalDefault(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		ModelConfigVersion: config.CurrentModelConfigVersion,
		Defaults:           &config.GlobalDefaults{Model: "official:gpt-5.6-sol"},
	}); err != nil {
		t.Fatal(err)
	}
	input := writeTestCatalog(t, t.TempDir())
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "nvidia", "--json", input)
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveDefaultModelSelector(); got != "official:gpt-5.6-sol" {
		t.Fatalf("official global default was changed during catalog install: %q", got)
	}
}

func testExternalCatalog() modelcatalog.Document {
	return modelcatalog.Document{
		CatalogVersion: modelcatalog.CurrentVersion,
		Providers: map[string]modelcatalog.Provider{
			"nvidia": {
				Interfaces: map[string]modelcatalog.Interface{
					"chat": {Adapter: "openai-chat", BaseURL: "https://integrate.api.nvidia.com/v1"},
				},
				Models: map[string]modelcatalog.Model{
					"deepseek-v4": {UpstreamModel: "deepseek-ai/deepseek-v4", DisplayName: "DeepSeek V4", DefaultInterface: "chat"},
					"mimo-v2.5":   {UpstreamModel: "xiaomi/mimo-v2.5", DisplayName: "MiMo V2.5", DefaultInterface: "chat"},
				},
			},
		},
	}
}

func writeTestCatalog(t *testing.T, dir string) string {
	t.Helper()
	raw, err := json.Marshal(testExternalCatalog())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "models.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestModelCatalogManagedImportProviderBatchSetupAndSwitch(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	input := writeTestCatalog(t, t.TempDir())
	t.Setenv("TEST_CATALOG_KEY", "synthetic-test-key")
	previous := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previous })
	var verified []string
	verifyConfiguredModelAuthenticationFn = func(_ context.Context, resolved modelprofile.Resolved, key string) error {
		if key != "synthetic-test-key" {
			t.Fatalf("unexpected verification key %q", key)
		}
		verified = append(verified, resolved.Name)
		return nil
	}
	out := runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "nvidia", "--json", input)
	if !strings.Contains(out, "2 model(s)") {
		t.Fatalf("catalog add output = %q", out)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.ModelProfiles["nvidia/deepseek-v4"]; !ok {
		t.Fatalf("deepseek route missing: %#v", cfg.ModelProfiles)
	}
	if _, ok := cfg.ModelProfiles["nvidia/mimo-v2.5"]; !ok {
		t.Fatalf("mimo route missing: %#v", cfg.ModelProfiles)
	}
	configRaw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(configRaw), "synthetic-test-key") {
		t.Fatal("raw provider key leaked into main config")
	}
	managedPath := filepath.Join(filepath.Dir(configPath), modelCatalogManagedDir, "nvidia.json")
	if info, statErr := os.Stat(managedPath); statErr != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("managed catalog permissions=%v err=%v", func() os.FileMode {
			if info == nil {
				return 0
			}
			return info.Mode().Perm()
		}(), statErr)
	}
	out = runRootCommandForModelProfileTest(t, "--config", configPath, "model", "provider", "setup", "nvidia", "--api-key-env", "TEST_CATALOG_KEY")
	if !strings.Contains(out, "2/2 model(s) verified") || len(verified) != 2 {
		t.Fatalf("provider setup output=%q verified=%v", out, verified)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ModelProviderBindings["nvidia"].Enabled {
		t.Fatal("provider was not enabled after batch verification")
	}
	beforeProof := cfg.ModelProfiles["nvidia/deepseek-v4"].VerificationFingerprint
	beforeRevision := cfg.ModelProfiles["nvidia/deepseek-v4"].Revision
	beforeCreatedAt := cfg.ModelProfiles["nvidia/deepseek-v4"].CreatedAt
	if beforeProof == "" {
		t.Fatal("provider setup did not persist verification proof")
	}
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "sync", "nvidia")
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	profile := cfg.ModelProfiles["nvidia/deepseek-v4"]
	if profile.VerificationFingerprint != beforeProof || profile.Revision != beforeRevision || !profile.CreatedAt.Equal(beforeCreatedAt) || !cfg.ModelProviderBindings["nvidia"].Enabled {
		t.Fatalf("unchanged catalog sync discarded a valid verification proof: before=%q after=%q binding=%#v profile=%#v", beforeProof, cfg.ModelProfiles["nvidia/deepseek-v4"].VerificationFingerprint, cfg.ModelProviderBindings["nvidia"], cfg.ModelProfiles["nvidia/deepseek-v4"])
	}
	if !cfg.ModelCatalogs["nvidia"].SyncedAt.After(beforeCreatedAt) {
		t.Fatalf("unchanged catalog sync did not refresh source metadata: %#v", cfg.ModelCatalogs["nvidia"])
	}
	out = runRootCommandForModelProfileTest(t, "--config", configPath, "model", "use", "nvidia/deepseek-v4")
	if !strings.Contains(out, "Default model: nvidia/deepseek-v4") {
		t.Fatalf("model use output = %q", out)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EffectiveDefaultModelSelector() != "profile:nvidia/deepseek-v4" {
		t.Fatalf("default selector=%q", cfg.EffectiveDefaultModelSelector())
	}
	list := runRootCommandForModelProfileTest(t, "--config", configPath, "model", "list")
	if !strings.Contains(list, "nvidia/deepseek-v4") || !strings.Contains(list, "nvidia/mimo-v2.5") || !strings.Contains(list, "ready") {
		t.Fatalf("model list output = %q", list)
	}
}

func TestModelCatalogChangedRevisionRebuildsMaterializedRoutes(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	input := writeTestCatalog(t, t.TempDir())
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "nvidia", "--json", input)
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	beforeRevision := cfg.ModelProfiles["nvidia/deepseek-v4"].Revision
	managedPath := filepath.Join(filepath.Dir(configPath), modelCatalogManagedDir, "nvidia.json")
	raw, err := os.ReadFile(managedPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc modelcatalog.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	model := doc.Providers["nvidia"].Models["deepseek-v4"]
	model.DisplayName = "DeepSeek V4 changed"
	doc.Providers["nvidia"].Models["deepseek-v4"] = model
	canonical, err := modelcatalog.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(managedPath, canonical, 0o600); err != nil {
		t.Fatal(err)
	}
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "sync", "nvidia")
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	profile := cfg.ModelProfiles["nvidia/deepseek-v4"]
	if profile.Revision != beforeRevision+1 || cfg.Models["nvidia/deepseek-v4"].DisplayName != "DeepSeek V4 changed" {
		t.Fatalf("changed catalog revision did not rebuild route: before=%d after=%d model=%#v", beforeRevision, profile.Revision, cfg.Models["nvidia/deepseek-v4"])
	}
}

func TestModelCatalogProviderCollisionLeavesExistingConfigUntouched(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	first := writeTestCatalog(t, t.TempDir())
	secondDoc := testExternalCatalog()
	secondDoc.Providers["nvidia"] = secondDoc.Providers["nvidia"]
	secondRaw, err := json.Marshal(secondDoc)
	if err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(t.TempDir(), "models.json")
	if err := os.WriteFile(second, secondRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "first", "--json", first)
	if err, _ := runRootCommandForModelProfileTestError("--config", configPath, "model", "catalog", "add", "second", "--json", second); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("expected provider collision, got %v", err)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.ModelCatalogs["second"]; ok {
		t.Fatal("failed catalog was persisted")
	}
}

func TestModelCatalogManagedReplaceFailurePreservesManagedDocument(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	input := writeTestCatalog(t, t.TempDir())
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "nvidia", "--json", input)
	managedPath := filepath.Join(filepath.Dir(configPath), modelCatalogManagedDir, "nvidia.json")
	previous, err := os.ReadFile(managedPath)
	if err != nil {
		t.Fatal(err)
	}

	// A replacement that introduces an unrelated provider must fail during the
	// preview phase. The old managed bytes and materialized routes must remain
	// untouched when that happens.
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(cfg *config.Config) error {
		if cfg.ModelProviders == nil {
			cfg.ModelProviders = map[string]config.ModelProvider{}
		}
		if cfg.ModelCredentials == nil {
			cfg.ModelCredentials = map[string]config.ModelCredential{}
		}
		cfg.ModelCredentials["manual-credential"] = config.ModelCredential{APIKeyRef: "env:MANUAL_KEY"}
		cfg.ModelProviders["manual-provider"] = config.ModelProvider{
			Protocol: "chat-completions", BaseURL: "https://manual.example.invalid/v1", Credential: "manual-credential",
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	replacement := modelcatalog.Document{
		CatalogVersion: modelcatalog.CurrentVersion,
		Providers: map[string]modelcatalog.Provider{
			"manual-provider": {
				Interfaces: map[string]modelcatalog.Interface{"chat": {Adapter: "openai-chat", BaseURL: "https://manual.example.invalid/v1"}},
				Models:     map[string]modelcatalog.Model{"replacement": {UpstreamModel: "manual-replacement", DefaultInterface: "chat"}},
			},
		},
	}
	replacementRaw, err := json.Marshal(replacement)
	if err != nil {
		t.Fatal(err)
	}
	replacementPath := filepath.Join(t.TempDir(), "replacement.json")
	if err := os.WriteFile(replacementPath, replacementRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err, _ := runRootCommandForModelProfileTestError("--config", configPath, "model", "catalog", "replace", "nvidia", "--json", replacementPath); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("expected replacement collision, got %v", err)
	}
	after, err := os.ReadFile(managedPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(previous) {
		t.Fatalf("managed catalog changed after failed replacement\nbefore=%s\nafter=%s", previous, after)
	}
	if _, err := store.Load(); err != nil {
		t.Fatalf("config became invalid after failed replacement: %v", err)
	}
}

func TestModelCatalogReplacementKeepsSharedRuntimeObjects(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	input := writeTestCatalog(t, t.TempDir())
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "nvidia", "--json", input)
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(cfg *config.Config) error {
		// A manually managed profile shares the old catalog provider/model and
		// credential. Replacing the catalog with a different provider must not
		// delete these still-referenced runtime objects.
		old := cfg.ModelProfiles["nvidia/deepseek-v4"]
		old.Source = ""
		cfg.ModelProfiles["manual-shared"] = old
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	replacement := modelcatalog.Document{
		CatalogVersion: modelcatalog.CurrentVersion,
		Providers: map[string]modelcatalog.Provider{
			"other": {
				Interfaces: map[string]modelcatalog.Interface{"chat": {Adapter: "openai-chat", BaseURL: "https://other.example.invalid/v1"}},
				Models:     map[string]modelcatalog.Model{"model": {UpstreamModel: "other-model", DefaultInterface: "chat"}},
			},
		},
	}
	replacementRaw, err := json.Marshal(replacement)
	if err != nil {
		t.Fatal(err)
	}
	replacementPath := filepath.Join(t.TempDir(), "replacement.json")
	if err := os.WriteFile(replacementPath, replacementRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "replace", "nvidia", "--json", replacementPath)
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.ModelProviders["nvidia"]; !ok {
		t.Fatalf("shared provider was removed: %#v", cfg.ModelProviders)
	}
	if _, ok := cfg.ModelCredentials["catalog/nvidia/nvidia"]; !ok {
		t.Fatalf("shared credential was removed: %#v", cfg.ModelCredentials)
	}
	if _, ok := cfg.ModelProfiles["manual-shared"]; !ok {
		t.Fatalf("shared manual profile was removed: %#v", cfg.ModelProfiles)
	}
}

func TestModelCatalogRemoveKeepsSharedRuntimeObjects(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	input := writeTestCatalog(t, t.TempDir())
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "nvidia", "--json", input)
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(cfg *config.Config) error {
		shared := cfg.ModelProfiles["nvidia/deepseek-v4"]
		shared.Source = ""
		cfg.ModelProfiles["manual-shared"] = shared
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "remove", "nvidia")
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.ModelCatalogs["nvidia"]; ok {
		t.Fatal("removed catalog remained configured")
	}
	if _, ok := cfg.ModelProfiles["manual-shared"]; !ok {
		t.Fatal("manual shared profile was removed")
	}
	if _, ok := cfg.Models["nvidia/deepseek-v4"]; !ok {
		t.Fatal("model referenced by manual profile was removed")
	}
	if _, ok := cfg.ModelProviders["nvidia"]; !ok {
		t.Fatal("provider referenced by manual profile was removed")
	}
	if _, ok := cfg.ModelCredentials["catalog/nvidia/nvidia"]; !ok {
		t.Fatal("credential referenced by manual profile was removed")
	}
}

func TestModelCatalogGitImportUsesNestedManifest(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is unavailable")
	}
	repo := filepath.Join(t.TempDir(), "catalog-repo")
	if err := os.MkdirAll(filepath.Join(repo, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(testExternalCatalog())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "nested", "models.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "catalog"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	configPath := filepath.Join(t.TempDir(), "config.json")
	out := runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "nvidia", "--git", repo, "--manifest", "nested/models.json")
	if !strings.Contains(out, "2 model(s)") {
		t.Fatalf("git catalog output = %q", out)
	}
}

func TestModelProviderSetupAttemptsEveryModelAndKeepsPartialStateHidden(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	input := writeTestCatalog(t, t.TempDir())
	t.Setenv("TEST_CATALOG_KEY", "synthetic-test-key")
	previous := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previous })
	var calls []string
	verifyConfiguredModelAuthenticationFn = func(_ context.Context, resolved modelprofile.Resolved, _ string) error {
		calls = append(calls, resolved.Name)
		if strings.HasSuffix(resolved.Name, "mimo-v2.5") {
			return fmt.Errorf("synthetic provider rejection")
		}
		return nil
	}
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "nvidia", "--json", input)
	err, output := runRootCommandForModelProfileTestError("--config", configPath, "model", "provider", "setup", "nvidia", "--api-key-env", "TEST_CATALOG_KEY")
	if err == nil || !strings.Contains(err.Error(), "unverified") || !strings.Contains(output, "mimo-v2.5") {
		t.Fatalf("partial setup err=%v output=%q", err, output)
	}
	if strings.Join(calls, ",") != "nvidia/deepseek-v4,nvidia/mimo-v2.5" {
		t.Fatalf("verification calls=%v", calls)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModelProviderBindings["nvidia"].Enabled {
		t.Fatal("provider became enabled after partial verification")
	}
	if strings.TrimSpace(cfg.ModelProfiles["nvidia/deepseek-v4"].VerificationFingerprint) == "" {
		t.Fatal("successful route did not retain its verification")
	}
	if strings.TrimSpace(cfg.ModelProfiles["nvidia/mimo-v2.5"].VerificationFingerprint) != "" {
		t.Fatal("failed route was exposed")
	}
	secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	if modelProfileVerificationCurrent(cfg, "nvidia/deepseek-v4", cfg.ModelProfiles["nvidia/deepseek-v4"], secrets) {
		t.Fatal("provider-wide activation gate exposed a partially verified route")
	}
}

func TestModelProviderSetupKeepsInterfaceCredentialsSeparate(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	doc := modelcatalog.Document{
		CatalogVersion: modelcatalog.CurrentVersion,
		Providers: map[string]modelcatalog.Provider{
			"deepseek": {
				DefaultInterface: "openai",
				Interfaces: map[string]modelcatalog.Interface{
					"openai":    {Adapter: "deepseek-openai", BaseURL: "https://api.deepseek.com"},
					"anthropic": {Adapter: "deepseek-anthropic", BaseURL: "https://api.deepseek.com/anthropic", Auth: modelcatalog.Auth{Type: "header", Header: "x-api-key"}},
				},
				Models: map[string]modelcatalog.Model{
					"openai-model":    {UpstreamModel: "deepseek-openai-model", DefaultInterface: "openai"},
					"anthropic-model": {UpstreamModel: "deepseek-anthropic-model", DefaultInterface: "anthropic"},
				},
			},
		},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "models.json")
	if err := os.WriteFile(input, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEEPSEEK_OPENAI_KEY", "openai-key")
	t.Setenv("DEEPSEEK_ANTHROPIC_KEY", "anthropic-key")
	previous := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previous })
	verifyConfiguredModelAuthenticationFn = func(_ context.Context, resolved modelprofile.Resolved, key string) error {
		want := "openai-key"
		if strings.HasSuffix(resolved.Name, "anthropic-model") {
			want = "anthropic-key"
		}
		if key != want {
			return fmt.Errorf("wrong key for %s: %s", resolved.Name, key)
		}
		return nil
	}
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "deepseek", "--json", input)
	if err, _ := runRootCommandForModelProfileTestError("--config", configPath, "model", "provider", "setup", "deepseek", "--api-key-env", "DEEPSEEK_OPENAI_KEY"); err == nil {
		t.Fatal("expected first interface setup to remain partial")
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ModelProfiles["deepseek/anthropic-model"].APIKeyRef; got == "env:DEEPSEEK_OPENAI_KEY" {
		t.Fatal("default interface setup overwrote the anthropic credential")
	}
	if err, _ := runRootCommandForModelProfileTestError("--config", configPath, "model", "provider", "setup", "deepseek", "--interface", "anthropic", "--api-key-env", "DEEPSEEK_ANTHROPIC_KEY"); err != nil {
		t.Fatalf("anthropic interface setup: %v", err)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ModelProviderBindings["deepseek"].Enabled {
		t.Fatalf("provider did not become ready after both interface keys: %#v", cfg.ModelProviderBindings["deepseek"])
	}
	if got := cfg.ModelCredentials["catalog/deepseek/deepseek/anthropic"].APIKeyRef; got != "env:DEEPSEEK_ANTHROPIC_KEY" {
		t.Fatalf("anthropic credential ref=%q", got)
	}
}

func TestModelCatalogNativeFeatureUsesItsInterfaceCredential(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	doc := modelcatalog.Document{
		CatalogVersion: modelcatalog.CurrentVersion,
		Providers: map[string]modelcatalog.Provider{
			"deepseek": {
				DefaultInterface: "openai",
				Interfaces: map[string]modelcatalog.Interface{
					"openai":    {Adapter: "deepseek-openai", BaseURL: "https://api.deepseek.com"},
					"anthropic": {Adapter: "deepseek-anthropic", BaseURL: "https://api.deepseek.com/anthropic", Auth: modelcatalog.Auth{Type: "header", Header: "x-api-key"}},
				},
				Models: map[string]modelcatalog.Model{
					"search-model": {
						UpstreamModel: "deepseek-v4", DefaultInterface: "openai",
						Features: map[string]modelcatalog.Feature{
							"webSearch": {Support: "native", Interface: "anthropic", NativeTool: &config.ModelNativeTool{InputTypes: []string{"web_search_preview"}, UpstreamType: "web_search"}},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "models.json")
	if err := os.WriteFile(input, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEEPSEEK_OPENAI_KEY", "openai-key")
	t.Setenv("DEEPSEEK_ANTHROPIC_KEY", "anthropic-key")
	previous := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previous })
	var verified []modelprofile.Resolved
	verifyConfiguredModelAuthenticationFn = func(_ context.Context, resolved modelprofile.Resolved, key string) error {
		if key != "openai-key" && key != "anthropic-key" {
			return fmt.Errorf("wrong key %q", key)
		}
		verified = append(verified, resolved)
		return nil
	}
	runRootCommandForModelProfileTest(t, "--config", configPath, "model", "catalog", "add", "deepseek", "--json", input)
	if err, _ := runRootCommandForModelProfileTestError("--config", configPath, "model", "provider", "setup", "deepseek", "--api-key-env", "DEEPSEEK_OPENAI_KEY"); err == nil {
		t.Fatal("default interface setup unexpectedly succeeded without the feature interface key")
	}
	if err, output := runRootCommandForModelProfileTestError("--config", configPath, "model", "provider", "setup", "deepseek", "--interface", "anthropic", "--api-key-env", "DEEPSEEK_ANTHROPIC_KEY"); err != nil {
		t.Fatalf("provider setup: %v output=%q", err, output)
	}
	foundAnthropic := false
	for _, resolved := range verified {
		if resolved.Provider.AdapterProfile == "deepseek-anthropic" && resolved.Profile.APIKeyRef == "env:DEEPSEEK_ANTHROPIC_KEY" && resolved.Provider.AuthHeader == "x-api-key" {
			foundAnthropic = true
		}
	}
	if !foundAnthropic {
		t.Fatalf("feature interface route was not verified with its adapter/credential: %#v", verified)
	}
}

func TestModelCatalogManagementCommandsAndTeamsManagerShareContract(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	input := writeTestCatalog(t, t.TempDir())
	rootArgs := func(args ...string) []string {
		return append([]string{"--config", configPath}, args...)
	}

	added := runRootCommandForModelProfileTest(t, rootArgs("model", "catalog", "add", "hub", "--json", input)...)
	if !strings.Contains(added, "2 model(s)") {
		t.Fatalf("catalog add output = %q", added)
	}
	list := runRootCommandForModelProfileTest(t, rootArgs("model", "catalog", "list")...)
	if !strings.Contains(list, "hub\ttype=managed-json\tproviders=1\tmodels=2") {
		t.Fatalf("catalog list output = %q", list)
	}
	providers := runRootCommandForModelProfileTest(t, rootArgs("model", "provider", "list")...)
	if !strings.Contains(providers, "nvidia\tcatalog=hub\tmodels=2\tinterface-keys=0\tstatus=needs setup") {
		t.Fatalf("provider list output = %q", providers)
	}
	doctor := runRootCommandForModelProfileTest(t, rootArgs("model", "provider", "doctor", "nvidia")...)
	if !strings.Contains(doctor, "nvidia/deepseek-v4\tneeds verification") || !strings.Contains(doctor, "nvidia/mimo-v2.5\tneeds verification") {
		t.Fatalf("provider doctor output = %q", doctor)
	}

	manager := teamsModelProfileManager{root: &rootOptions{configPath: configPath}}
	teamCatalogs, err := manager.ListModelCatalogs(context.Background())
	if err != nil || teamCatalogs != strings.TrimSpace(list) {
		t.Fatalf("Teams catalog list = %q err=%v, CLI=%q", teamCatalogs, err, list)
	}
	teamProviders, err := manager.ListModelProviders(context.Background())
	if err != nil || teamProviders != strings.TrimSpace(providers) {
		t.Fatalf("Teams provider list = %q err=%v, CLI=%q", teamProviders, err, providers)
	}
	teamSetup, err := manager.SetupModelProvider(context.Background(), "nvidia")
	if err != nil || !strings.Contains(teamSetup, "not activated yet") || strings.Contains(teamSetup, "synthetic") {
		t.Fatalf("Teams provider setup = %q err=%v", teamSetup, err)
	}
	if synced, err := manager.SyncModelCatalog(context.Background(), "hub"); err != nil || !strings.Contains(synced, "synchronized") {
		t.Fatalf("Teams catalog sync = %q err=%v", synced, err)
	}

	removed := runRootCommandForModelProfileTest(t, rootArgs("model", "catalog", "remove", "hub")...)
	if !strings.Contains(removed, "Removed model catalog") {
		t.Fatalf("catalog remove output = %q", removed)
	}
	if _, err := manager.ListModelCatalogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.ModelCatalogs) != 0 {
		t.Fatalf("catalog remove left durable catalogs: %#v", cfg.ModelCatalogs)
	}
}
