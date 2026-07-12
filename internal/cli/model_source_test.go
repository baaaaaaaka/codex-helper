package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/spf13/cobra"
)

func TestDueModelSourcesUsesPerSourceSyncedAtAndStableOrder(t *testing.T) {
	now := time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC)
	cfg := config.Config{ModelSources: map[string]config.ModelSource{
		"recent":  {URL: "https://example.invalid/recent.git", SyncedAt: now.Add(-29 * time.Minute)},
		"missing": {URL: "https://example.invalid/missing.git"},
		"due-b":   {URL: "https://example.invalid/b.git", SyncedAt: now.Add(-31 * time.Minute)},
		"due-a":   {URL: "https://example.invalid/a.git", SyncedAt: now.Add(-30 * time.Minute)},
	}}
	got := dueModelSources(cfg, now, 30*time.Minute)
	want := []string{"due-a", "due-b", "missing"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("due sources = %v, want %v", got, want)
	}
	if delay := nextModelSourceAutoSyncDelay(config.Config{ModelSources: map[string]config.ModelSource{
		"recent": cfg.ModelSources["recent"],
	}}, now, 30*time.Minute); delay != time.Minute {
		t.Fatalf("next delay = %s, want 1m", delay)
	}
}

func TestModelSourceAutoSyncOnceRunsOnlyDueSourcesAndKeepsServiceAliveOnFailure(t *testing.T) {
	now := time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC)
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{ModelSources: map[string]config.ModelSource{
		"due":    {URL: "https://example.invalid/due.git", SyncedAt: now.Add(-time.Hour)},
		"recent": {URL: "https://example.invalid/recent.git", SyncedAt: now.Add(-time.Minute)},
	}}); err != nil {
		t.Fatal(err)
	}
	previous := runModelSourceSyncFn
	t.Cleanup(func() { runModelSourceSyncFn = previous })
	var calls []string
	runModelSourceSyncFn = func(_ *cobra.Command, _ *rootOptions, ref string, _ modelSourceSyncOptions) error {
		calls = append(calls, ref)
		return fmt.Errorf("offline")
	}
	var warnings bytes.Buffer
	delay := runModelSourceAutoSyncOnce(context.Background(), &rootOptions{configPath: configPath}, &warnings, now, 30*time.Minute)
	if strings.Join(calls, ",") != "due" {
		t.Fatalf("sync calls = %v, want only due", calls)
	}
	if !strings.Contains(warnings.String(), "Model source auto-sync warning: sync due: offline") {
		t.Fatalf("warnings = %q", warnings.String())
	}
	if delay != 30*time.Minute {
		t.Fatalf("retry delay = %s, want 30m", delay)
	}
}

func TestRecordModelSourceBackupFailureRequiresLastKnownGoodRevision(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{ModelSources: map[string]config.ModelSource{
		"new":    {URL: "https://example.invalid/new.git"},
		"active": {URL: "https://example.invalid/active.git", Revision: "good-revision"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := recordModelSourceBackupFailure(store, "new", "bad", fmt.Errorf("initial failure")); err != nil {
		t.Fatal(err)
	}
	if err := recordModelSourceBackupFailure(store, "active", "bad-revision", fmt.Errorf("invalid JSON")); err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModelSources["new"].BackupActive {
		t.Fatal("initial sync failure incorrectly claimed a backup JSON")
	}
	backup := cfg.ModelSources["active"]
	if !backup.BackupActive || backup.BackupAttemptedRevision != "bad-revision" || !strings.Contains(backup.BackupReason, "invalid JSON") || backup.BackupSince.IsZero() {
		t.Fatalf("backup state = %#v", backup)
	}
}

func TestModelSourceSyncClonesLocalRepoWithoutKey(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed in this minimal runtime compatibility environment")
	}
	repo := filepath.Join(t.TempDir(), "models")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]config.ModelCredential{"hub": {}},
		ModelProviders:     map[string]config.ModelProvider{"hub": {Protocol: "chat-completions", BaseURL: "https://example.invalid/v1", Credential: "hub"}},
		Models:             map[string]config.ModelDefinition{"m": {Provider: "hub", UpstreamModel: "vendor/m"}},
		ModelProfiles:      map[string]config.ModelProfile{"p": {Model: "m", Revision: 1}},
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, defaultModelSourceFile), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", defaultModelSourceFile}, {"-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "models"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	configPath := filepath.Join(t.TempDir(), "config.json")
	out := runRootCommandForModelProfileTest(t, "--config", configPath, "model-source", "sync", repo, "--name", "local")
	if !strings.Contains(out, "0 verified and available") || !strings.Contains(out, "model-source bind local p") {
		t.Fatalf("sync output = %q", out)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModelProfiles["p"].Source != "local" || modelprofile.HasConfiguredThirdPartyModels(cfg) {
		t.Fatalf("synced cfg = %#v", cfg)
	}
	if err := recordModelSourceBackupFailure(store, "local", "broken", fmt.Errorf("invalid JSON")); err != nil {
		t.Fatal(err)
	}
	_ = runRootCommandForModelProfileTest(t, "--config", configPath, "model-source", "sync", "local")
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if source := cfg.ModelSources["local"]; source.BackupActive || source.BackupReason != "" || !source.BackupSince.IsZero() {
		t.Fatalf("successful sync did not clear backup state: %#v", source)
	}
}

func TestSafeRepoFileCanonicalizesRepositoryRootAndRejectsEscapes(t *testing.T) {
	realParent := t.TempDir()
	repo := filepath.Join(realParent, "repo")
	if err := os.MkdirAll(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(repo, defaultModelSourceFile)
	if err := os.WriteFile(want, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("creating a Windows symlink requires additional privileges: %v", err)
		}
		t.Fatal(err)
	}
	got, err := safeRepoFile(filepath.Join(aliasParent, "repo"), defaultModelSourceFile)
	if err != nil {
		t.Fatalf("safe file through canonical root: %v", err)
	}
	if got != want {
		t.Fatalf("safe file = %q, want %q", got, want)
	}
	escape := filepath.Join(repo, "escape.json")
	outside := filepath.Join(realParent, "outside.json")
	if err := os.WriteFile(outside, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, escape); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("creating a Windows symlink requires additional privileges: %v", err)
		}
		t.Fatal(err)
	}
	if _, err := safeRepoFile(repo, "escape.json"); err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("escape error = %v", err)
	}
}

func TestSyncedModelStaysHiddenUntilRealVerificationSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()

	fragment := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]config.ModelCredential{"hub": {Pending: true}},
		ModelProviders:     map[string]config.ModelProvider{"hub": {Protocol: "chat-completions", BaseURL: server.URL + "/v1", Credential: "hub"}},
		Models:             map[string]config.ModelDefinition{"repo-model": {Provider: "hub", UpstreamModel: "vendor/model"}},
		ModelProfiles:      map[string]config.ModelProfile{"repo-profile": {Model: "repo-model", Revision: 1}},
	}
	cfg := config.Config{Version: config.CurrentVersion}
	source := config.ModelSource{URL: "https://example.invalid/models.git", Revision: strings.Repeat("a", 40), SyncedAt: time.Now()}
	if err := mergeModelSource(&cfg, "test-source", source, fragment); err != nil {
		t.Fatalf("mergeModelSource: %v", err)
	}
	if modelprofile.HasConfiguredThirdPartyModels(cfg) {
		t.Fatal("unverified synced model must stay hidden")
	}

	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_REPO_MODEL_KEY", "test-key")
	out := runRootCommandForModelProfileTest(t, "--config", configPath, "model-source", "bind", "test-source", "repo-profile", "--api-key-env", "TEST_REPO_MODEL_KEY")
	if !strings.Contains(out, "now available") {
		t.Fatalf("bind output = %q", out)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	profile := got.ModelProfiles["repo-profile"]
	if profile.VerificationFingerprint == "" || profile.VerifiedAt.IsZero() || profile.Source != "test-source" {
		t.Fatalf("verified profile = %#v", profile)
	}
	if !modelprofile.HasConfiguredThirdPartyModels(got) {
		t.Fatal("verified synced model should activate the gateway")
	}
}

func TestMergeModelSourcePreservesVerificationAcrossEquivalentRevisions(t *testing.T) {
	fragment := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]config.ModelCredential{"hub": {Pending: true}},
		ModelProviders:     map[string]config.ModelProvider{"hub": {Protocol: "chat-completions", BaseURL: "https://example.invalid/v1", Credential: "hub"}},
		Models:             map[string]config.ModelDefinition{"m": {Provider: "hub", UpstreamModel: "m"}},
		ModelProfiles:      map[string]config.ModelProfile{"p": {Model: "m", Revision: 1}},
	}
	cfg := config.Config{}
	first := config.ModelSource{URL: "https://example.invalid/repo.git", Revision: "one"}
	if err := mergeModelSource(&cfg, "source", first, fragment); err != nil {
		t.Fatal(err)
	}
	p := cfg.ModelProfiles["p"]
	p.VerificationFingerprint = "verified"
	cfg.ModelProfiles["p"] = p
	credential := cfg.ModelCredentials["hub"]
	credential.APIKeyRef = "env:HUB_KEY"
	credential.Pending = false
	cfg.ModelCredentials["hub"] = credential
	if err := mergeModelSource(&cfg, "source", first, fragment); err != nil {
		t.Fatal(err)
	}
	if cfg.ModelProfiles["p"].VerificationFingerprint != "verified" {
		t.Fatal("unchanged revision lost verification")
	}
	changed := first
	changed.Revision = "two"
	if err := mergeModelSource(&cfg, "source", changed, fragment); err != nil {
		t.Fatal(err)
	}
	if cfg.ModelProfiles["p"].VerificationFingerprint != "verified" {
		t.Fatal("equivalent config update lost verification")
	}
	changedFragment := fragment
	changedFragment.ModelProviders = map[string]config.ModelProvider{"hub": {Protocol: "chat-completions", BaseURL: "https://changed.example.invalid/v1", Credential: "hub"}}
	changed.Revision = "three"
	if err := mergeModelSource(&cfg, "source", changed, changedFragment); err != nil {
		t.Fatal(err)
	}
	if cfg.ModelProfiles["p"].VerificationFingerprint != "" {
		t.Fatal("effective provider change retained stale verification")
	}
}

func TestMergeModelSourceRepairsRemovedDefaultProfile(t *testing.T) {
	fragment := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]config.ModelCredential{"hub": {Pending: true}},
		ModelProviders:     map[string]config.ModelProvider{"hub": {Protocol: "chat-completions", BaseURL: "https://example.invalid/v1", Credential: "hub"}},
		Models:             map[string]config.ModelDefinition{"m": {Provider: "hub", UpstreamModel: "m"}},
		ModelProfiles:      map[string]config.ModelProfile{"p": {Model: "m", Revision: 1}},
	}
	cfg := config.Config{DefaultModelProfile: "p"}
	if err := mergeModelSource(&cfg, "source", config.ModelSource{URL: "https://example.invalid/repo.git", Revision: "one"}, fragment); err != nil {
		t.Fatal(err)
	}
	fragment.ModelProfiles = map[string]config.ModelProfile{}
	if err := mergeModelSource(&cfg, "source", config.ModelSource{URL: "https://example.invalid/repo.git", Revision: "two"}, fragment); err != nil {
		t.Fatal(err)
	}
	if cfg.DefaultModelProfile != config.DefaultModelProfileName {
		t.Fatalf("default after removal = %q", cfg.DefaultModelProfile)
	}
}

func TestReverifyUpdatedSourceProfilesUsesExistingCredentialWithoutPrompt(t *testing.T) {
	previous := verifyConfiguredModelAuthenticationFn
	t.Cleanup(func() { verifyConfiguredModelAuthenticationFn = previous })
	called := 0
	verifyConfiguredModelAuthenticationFn = func(_ context.Context, resolved modelprofile.Resolved, key string) error {
		called++
		if resolved.Name != "p" || key != "existing-key" {
			t.Fatalf("verification name=%q key=%q", resolved.Name, key)
		}
		return nil
	}
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Version:             config.CurrentVersion,
		DefaultModelProfile: "p",
		ModelConfigVersion:  1,
		ModelCredentials:    map[string]config.ModelCredential{"hub": {APIKeyRef: "env:EXISTING_KEY"}},
		ModelProviders:      map[string]config.ModelProvider{"hub": {Protocol: "chat-completions", BaseURL: "https://example.invalid/v1", Credential: "hub"}},
		Models:              map[string]config.ModelDefinition{"m": {Provider: "hub", UpstreamModel: "vendor/m"}},
		ModelProfiles:       map[string]config.ModelProfile{"p": {Model: "m", Source: "source", Revision: 1}},
		ModelSources:        map[string]config.ModelSource{"source": {URL: "https://example.invalid/repo.git", Revision: "two", Profiles: []string{"p"}, Credentials: []string{"hub"}, Providers: []string{"hub"}, Models: []string{"m"}}},
	}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXISTING_KEY", "existing-key")
	warnings, err := reverifyUpdatedSourceProfiles(context.Background(), store, "source", "two")
	if err != nil {
		t.Fatal(err)
	}
	if called != 1 || len(warnings) != 0 {
		t.Fatalf("called=%d warnings=%v", called, warnings)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.ModelProfiles["p"].VerificationFingerprint == "" || got.ModelProfiles["p"].VerifiedAt.IsZero() {
		t.Fatalf("profile was not automatically verified: %#v", got.ModelProfiles["p"])
	}
	if got.DefaultModelProfile != "p" {
		t.Fatalf("successful transparent reverification changed default to %q", got.DefaultModelProfile)
	}
	verifyConfiguredModelAuthenticationFn = func(context.Context, modelprofile.Resolved, string) error {
		return fmt.Errorf("temporary authentication failure")
	}
	if err := store.Update(func(current *config.Config) error {
		profile := current.ModelProfiles["p"]
		profile.VerificationFingerprint = ""
		current.ModelProfiles["p"] = profile
		current.DefaultModelProfile = "p"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	warnings, err = reverifyUpdatedSourceProfiles(context.Background(), store, "source", "two")
	if err != nil {
		t.Fatal(err)
	}
	got, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 1 || got.DefaultModelProfile != config.DefaultModelProfileName || got.ModelProfiles["p"].VerificationError == "" {
		t.Fatalf("failed reverification did not hide and safely fall back: warnings=%v default=%q profile=%#v", warnings, got.DefaultModelProfile, got.ModelProfiles["p"])
	}
}
