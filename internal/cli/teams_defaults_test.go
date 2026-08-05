package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

func testDefaultSnapshot(name string, provider string, model string, defaultEffort string, efforts ...string) modelprofile.Snapshot {
	raw, _ := json.Marshal(efforts)
	return modelprofile.Snapshot{
		Name: name, Provider: provider, Model: model, DefaultModel: model,
		DefaultReasoningEffort: defaultEffort, SupportedReasoningEffortsJSON: string(raw), Revision: 1,
	}
}

func TestTeamsDefaultManagerUsesRegistryAndAtomicallyReconcilesModelEffort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		Version:  config.CurrentVersion,
		Defaults: &config.GlobalDefaults{Model: "official:gpt-a", ReasoningEffort: "high"},
		ModelProfiles: map[string]config.ModelProfile{
			"work": {Provider: "deepseek", Model: "vendor/work", DefaultReasoningEffort: "medium", SupportedReasoningEfforts: []string{"medium"}, Revision: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	manager := teamsModelProfileManager{
		root: &rootOptions{configPath: path},
		defaultResolver: func(_ context.Context, ref string) (modelprofile.Snapshot, error) {
			switch strings.ToLower(strings.TrimSpace(ref)) {
			case "", "default", "official:gpt-a", "gpt-a":
				return testDefaultSnapshot("gpt-a", modelprofile.DefaultProvider, "gpt-a", "low", "low", "high"), nil
			case "profile:work", "work":
				return testDefaultSnapshot("work", "deepseek", "vendor/work", "medium", "medium"), nil
			default:
				return modelprofile.Snapshot{}, &unknownDefaultTestError{ref: ref}
			}
		},
	}

	status, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Action: teams.DefaultCommandStatus})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "Global defaults (Control chat only)") || !strings.Contains(status, "Scope: future Codex launches") || !strings.Contains(status, "official:gpt-a") || !strings.Contains(status, "Effective for future launches") || !strings.Contains(status, "high") {
		t.Fatalf("status = %q", status)
	}

	message, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "model", Action: teams.DefaultCommandSet, Value: "profile:work"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message, "reset to `medium`") || !strings.Contains(message, "Previous selector: `official:gpt-a`") || !strings.Contains(message, "this Control chat is unchanged") {
		t.Fatalf("model set message = %q", message)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || cfg.Defaults.Model != "profile:work" || cfg.Defaults.ReasoningEffort != "medium" || cfg.DefaultModelProfile != "work" {
		t.Fatalf("updated config = %#v", cfg)
	}
}

type unknownDefaultTestError struct{ ref string }

func (e *unknownDefaultTestError) Error() string { return "unknown model " + e.ref }

func TestTeamsDefaultManagerEffortValidationResetAndResolution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Defaults: &config.GlobalDefaults{Model: "official:gpt-a"}}); err != nil {
		t.Fatal(err)
	}
	snapshot := testDefaultSnapshot("gpt-a", modelprofile.DefaultProvider, "gpt-a", "medium", "low", "medium", "high")
	manager := teamsModelProfileManager{
		root:            &rootOptions{configPath: path},
		defaultResolver: func(context.Context, string) (modelprofile.Snapshot, error) { return snapshot, nil },
	}
	if _, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "effort", Action: teams.DefaultCommandSet, Value: "unsupported"}); err == nil {
		t.Fatal("unsupported effort set succeeded")
	}
	if _, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "effort", Action: teams.DefaultCommandSet, Value: "HIGH"}); err != nil {
		t.Fatal(err)
	}
	effort, source, err := manager.ResolveDefaultReasoningEffort(context.Background(), snapshot)
	if err != nil || effort != "high" || source != "global_default" {
		t.Fatalf("resolved effort/source = %q/%q err=%v", effort, source, err)
	}
	if _, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "effort", Action: teams.DefaultCommandReset}); err != nil {
		t.Fatal(err)
	}
	effort, source, err = manager.ResolveDefaultReasoningEffort(context.Background(), snapshot)
	if err != nil || effort != "medium" || source != "model_default" {
		t.Fatalf("reset resolved effort/source = %q/%q err=%v", effort, source, err)
	}
}

func TestTeamsDefaultManagerLegacyConfigKeepsRuntimeEffortFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	snapshot := testDefaultSnapshot("gpt-a", modelprofile.DefaultProvider, "gpt-a", "medium", "low", "medium", "high")
	manager := teamsModelProfileManager{
		root:            &rootOptions{configPath: path},
		defaultResolver: func(context.Context, string) (modelprofile.Snapshot, error) { return snapshot, nil },
	}
	effort, source, err := manager.ResolveDefaultReasoningEffort(context.Background(), snapshot)
	if err != nil || effort != "" || source != teams.ReasoningEffortSourceRuntimeDefault {
		t.Fatalf("legacy effort/source = %q/%q err=%v", effort, source, err)
	}
	status, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "effort", Action: teams.DefaultCommandStatus})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "runtime fallback") || !strings.Contains(status, "Source: Codex runtime fallback") {
		t.Fatalf("legacy effort status = %q", status)
	}
}

func TestTeamsDefaultManagerUnknownSettingUsesRegisteredNames(t *testing.T) {
	manager := teamsModelProfileManager{}
	_, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "future", Action: teams.DefaultCommandStatus})
	if err == nil || !strings.Contains(err.Error(), "`model`") || !strings.Contains(err.Error(), "`effort`") {
		t.Fatalf("unknown setting error = %v", err)
	}
}

func TestTeamsDefaultModelMutationDoesNotPartiallyPersistInvalidPair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion, Defaults: &config.GlobalDefaults{Model: "official:gpt-a", ReasoningEffort: "high"},
	}); err != nil {
		t.Fatal(err)
	}
	manager := teamsModelProfileManager{
		root: &rootOptions{configPath: path},
		defaultResolver: func(context.Context, string) (modelprofile.Snapshot, error) {
			return testDefaultSnapshot("broken", "vendor", "vendor/broken", "missing-default", "low"), nil
		},
	}
	if _, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{
		Setting: "model", Action: teams.DefaultCommandSet, Value: "profile:broken",
	}); err == nil || !strings.Contains(err.Error(), "valid default reasoning effort") {
		t.Fatalf("invalid pair error = %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || cfg.Defaults.Model != "official:gpt-a" || cfg.Defaults.ReasoningEffort != "high" {
		t.Fatalf("invalid pair partially persisted: %#v", cfg.Defaults)
	}
}

func TestTeamsDefaultModelResetDoesNotClearEffortWhenOfficialCatalogIsUnavailable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion, Defaults: &config.GlobalDefaults{Model: "official:gpt-a", ReasoningEffort: "high"},
	}); err != nil {
		t.Fatal(err)
	}
	manager := teamsModelProfileManager{
		root: &rootOptions{configPath: path},
		defaultResolver: func(context.Context, string) (modelprofile.Snapshot, error) {
			return modelprofile.Snapshot{Name: config.DefaultModelProfileName, Provider: modelprofile.DefaultProvider, Revision: 1}, nil
		},
	}
	if _, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{
		Setting: "model", Action: teams.DefaultCommandReset,
	}); err == nil || !strings.Contains(err.Error(), "official model catalog is unavailable") {
		t.Fatalf("catalog-unavailable reset error = %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || cfg.Defaults.Model != "official:gpt-a" || cfg.Defaults.ReasoningEffort != "high" {
		t.Fatalf("catalog-unavailable reset mutated defaults: %#v", cfg.Defaults)
	}
}

func TestTeamsDefaultModelMutationReconcilesLatestEffortInsideAtomicUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion, Defaults: &config.GlobalDefaults{Model: "official:gpt-a", ReasoningEffort: "high"},
		ModelProfiles: map[string]config.ModelProfile{
			"work": {Provider: "deepseek", Model: "vendor/work", DefaultReasoningEffort: "low", SupportedReasoningEfforts: []string{"low", "high"}, Revision: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	resume := make(chan struct{})
	manager := teamsModelProfileManager{
		root: &rootOptions{configPath: path},
		defaultResolver: func(_ context.Context, ref string) (modelprofile.Snapshot, error) {
			if strings.EqualFold(strings.TrimSpace(ref), "profile:work") {
				close(started)
				<-resume
				return testDefaultSnapshot("work", "deepseek", "vendor/work", "low", "low", "high"), nil
			}
			return testDefaultSnapshot("gpt-a", modelprofile.DefaultProvider, "gpt-a", "low", "low", "high"), nil
		},
	}
	done := make(chan error, 1)
	go func() {
		_, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "model", Action: teams.DefaultCommandSet, Value: "profile:work"})
		done <- err
	}()
	<-started
	if err := store.Update(func(cfg *config.Config) error {
		cfg.EnsureGlobalDefaults().ReasoningEffort = "low"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || cfg.Defaults.Model != "profile:work" || cfg.Defaults.ReasoningEffort != "low" {
		t.Fatalf("atomic defaults = %#v", cfg.Defaults)
	}
}

func TestTeamsDefaultEffortMutationRejectsConcurrentModelChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Defaults: &config.GlobalDefaults{Model: "official:gpt-a"}}); err != nil {
		t.Fatal(err)
	}
	manager := teamsModelProfileManager{
		root: &rootOptions{configPath: path},
		defaultResolver: func(context.Context, string) (modelprofile.Snapshot, error) {
			if err := store.Update(func(cfg *config.Config) error {
				cfg.EnsureGlobalDefaults().Model = "official:gpt-b"
				return nil
			}); err != nil {
				return modelprofile.Snapshot{}, err
			}
			return testDefaultSnapshot("gpt-a", modelprofile.DefaultProvider, "gpt-a", "low", "low", "high"), nil
		},
	}
	_, err = manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "effort", Action: teams.DefaultCommandSet, Value: "high"})
	if err == nil || !strings.Contains(err.Error(), "changed concurrently") {
		t.Fatalf("concurrent effort mutation error = %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || cfg.Defaults.Model != "official:gpt-b" || cfg.Defaults.ReasoningEffort != "" {
		t.Fatalf("concurrent defaults = %#v", cfg.Defaults)
	}
}

func TestTeamsDefaultEffortMutationRejectsConcurrentProfileCapabilityChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		Version:  config.CurrentVersion,
		Defaults: &config.GlobalDefaults{Model: "profile:work"},
		ModelProfiles: map[string]config.ModelProfile{
			"work": {Provider: "deepseek", Model: "vendor/work", DefaultReasoningEffort: "low", SupportedReasoningEfforts: []string{"low", "high"}, Revision: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	manager := teamsModelProfileManager{
		root: &rootOptions{configPath: path},
		defaultResolver: func(context.Context, string) (modelprofile.Snapshot, error) {
			validated := testDefaultSnapshot("work", "deepseek", "vendor/work", "low", "low", "high")
			if err := store.Update(func(cfg *config.Config) error {
				profile := cfg.ModelProfiles["work"]
				profile.DefaultReasoningEffort = "medium"
				profile.SupportedReasoningEfforts = []string{"medium"}
				profile.Revision++
				cfg.ModelProfiles["work"] = profile
				return nil
			}); err != nil {
				return modelprofile.Snapshot{}, err
			}
			return validated, nil
		},
	}
	_, err = manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "effort", Action: teams.DefaultCommandSet, Value: "high"})
	if err == nil || !strings.Contains(err.Error(), "profile \"profile:work\" changed concurrently") {
		t.Fatalf("concurrent profile mutation error = %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || cfg.Defaults.ReasoningEffort != "" {
		t.Fatalf("stale effort persisted: %#v", cfg.Defaults)
	}
}

func TestTeamsDefaultModelMutationRejectsConcurrentTargetProfileChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		Version:  config.CurrentVersion,
		Defaults: &config.GlobalDefaults{Model: "official:gpt-a", ReasoningEffort: "high"},
		ModelProfiles: map[string]config.ModelProfile{
			"work": {Provider: "deepseek", Model: "vendor/work", DefaultReasoningEffort: "low", SupportedReasoningEfforts: []string{"low", "high"}, Revision: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	manager := teamsModelProfileManager{
		root: &rootOptions{configPath: path},
		defaultResolver: func(context.Context, string) (modelprofile.Snapshot, error) {
			validated := testDefaultSnapshot("work", "deepseek", "vendor/work", "low", "low", "high")
			if err := store.Update(func(cfg *config.Config) error {
				profile := cfg.ModelProfiles["work"]
				profile.Model = "vendor/work-v2"
				profile.Revision++
				cfg.ModelProfiles["work"] = profile
				return nil
			}); err != nil {
				return modelprofile.Snapshot{}, err
			}
			return validated, nil
		},
	}
	_, err = manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "model", Action: teams.DefaultCommandSet, Value: "profile:work"})
	if err == nil || !strings.Contains(err.Error(), "profile \"profile:work\" changed concurrently") {
		t.Fatalf("concurrent target profile mutation error = %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || cfg.Defaults.Model != "official:gpt-a" || cfg.Defaults.ReasoningEffort != "high" {
		t.Fatalf("stale model selection persisted: %#v", cfg.Defaults)
	}
}

func TestTeamsDefaultModelSetDefaultKeepsDynamicOfficialDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	manager := teamsModelProfileManager{
		root: &rootOptions{configPath: path},
		defaultResolver: func(context.Context, string) (modelprofile.Snapshot, error) {
			return testDefaultSnapshot("gpt-current", modelprofile.DefaultProvider, "gpt-current", "medium", "low", "medium", "high"), nil
		},
	}
	if _, err := manager.HandleDefaultCommand(context.Background(), teams.DefaultCommand{Setting: "model", Action: teams.DefaultCommandSet, Value: "default"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || cfg.Defaults.Model != config.DefaultModelProfileName || cfg.DefaultModelProfile != "" {
		t.Fatalf("dynamic default was frozen: legacy=%q typed=%#v", cfg.DefaultModelProfile, cfg.Defaults)
	}
}
