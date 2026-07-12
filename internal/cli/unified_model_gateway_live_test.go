package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

func TestLiveUnifiedModelGatewayWithRealCodexOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_UNIFIED_GATEWAY")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_UNIFIED_GATEWAY=1 to run real Codex unified gateway coverage")
	}
	codexPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_PATH"))
	configPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CONFIG_PATH"))
	profileName := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_MODEL_PROFILE"))
	thirdModel := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_THIRD_MODEL"))
	officialModel := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_OFFICIAL_MODEL"))
	codexHome := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_HOME"))
	if codexPath == "" || configPath == "" || profileName == "" || thirdModel == "" || officialModel == "" || codexHome == "" {
		t.Fatal("live unified gateway test requires Codex path, config path, profile, third model, and official model")
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	launch, cleanup, err := startModelProfileAdapterForCodex(withCodexLoginProbePath(ctx, codexPath), store, profileName, modelprofile.Snapshot{}, "", true, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if !launch.Unified {
		t.Fatalf("launch is not unified: %#v", launch)
	}
	extraEnv := append(codexHomeEnv(codexHome), launch.effectiveEnvKey()+"="+launch.ProxyKey)
	catalog, err := os.ReadFile(launch.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(catalog, []byte(`"slug": "`+officialModel+`"`)) || !bytes.Contains(catalog, []byte(`"slug": "`+thirdModel+`"`)) {
		t.Fatalf("merged catalog does not contain both target models: %s", catalog)
	}
	for _, test := range []struct {
		name   string
		model  string
		marker string
	}{
		{name: "official", model: officialModel, marker: "LIVE_UNIFIED_OFFICIAL_OK"},
		{name: "third-party", model: thirdModel, marker: "LIVE_UNIFIED_THIRD_OK"},
		{name: "official-return", model: officialModel, marker: "LIVE_UNIFIED_OFFICIAL_RETURN_OK"},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := appendCodexModelProfileArgs([]string{
				codexPath,
				"exec",
				"--ephemeral",
				"--ignore-user-config",
				"--json",
				"--sandbox", "read-only",
				"--skip-git-repo-check",
				"--model", test.model,
				"Reply with exactly " + test.marker + ".",
			}, launch)
			cmd := exec.CommandContext(ctx, args[0], args[1:]...)
			cmd.Env = mergeCLIEnvironment(os.Environ(), extraEnv)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Codex failed: %v\n%s", err, output)
			}
			if !bytes.Contains(output, []byte(test.marker)) || !bytes.Contains(output, []byte(`"type":"turn.completed"`)) {
				t.Fatalf("Codex did not complete through expected route:\n%s", output)
			}
			cachedInputTokens := 0
			for _, line := range bytes.Split(output, []byte("\n")) {
				var event struct {
					Type  string `json:"type"`
					Usage struct {
						CachedInputTokens int `json:"cached_input_tokens"`
					} `json:"usage"`
				}
				if json.Unmarshal(line, &event) == nil && event.Type == "turn.completed" {
					cachedInputTokens = event.Usage.CachedInputTokens
				}
			}
			t.Logf("cached_input_tokens=%d", cachedInputTokens)
		})
	}
}

func TestLiveTeamsOfficialCatalogWithResolvedRuntimeOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_UNIFIED_GATEWAY")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_UNIFIED_GATEWAY=1 to run real Teams official catalog coverage")
	}
	codexPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_PATH"))
	configPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CONFIG_PATH"))
	officialModel := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_OFFICIAL_MODEL"))
	codexHome := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_HOME"))
	if codexPath == "" || configPath == "" || officialModel == "" || codexHome == "" {
		t.Fatal("live Teams catalog test requires Codex path, config path, official model, and Codex home")
	}
	// Package tests intentionally isolate HOME. Bind managed discovery to the
	// supplied live launcher instead of accidentally installing or selecting a
	// second Codex under the test HOME.
	managedPrefix := filepath.Dir(filepath.Dir(codexPath))
	t.Setenv("CODEX_NPM_PREFIX", managedPrefix)
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(cfg *config.Config) error {
		cfg.TeamsCodexPath.Mode = "service"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	root := &rootOptions{configPath: configPath}
	resolver := newTeamsCodexRuntimeResolver(root, "", t.TempDir(), io.Discard)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	contract, err := resolver(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resolvedCommand, err := filepath.EvalSymlinks(contract.Runtime.Command)
	if err != nil {
		t.Fatal(err)
	}
	wantCommand, err := filepath.EvalSymlinks(codexPath)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedCommand != wantCommand || envValue(contract.Runtime.Environment, "CODEX_HOME") != codexHome {
		t.Fatalf("resolved Teams runtime = command %q CODEX_HOME %q, want %q and %q", resolvedCommand, envValue(contract.Runtime.Environment, "CODEX_HOME"), wantCommand, codexHome)
	}
	manager := newTeamsModelProfileManagerWithRuntime(root, resolver)
	listed, err := manager.ListModelProfiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(listed, "Official Codex models") || !strings.Contains(listed, "(`"+officialModel+"`)") {
		t.Fatalf("Teams model list did not expose the authenticated official catalog:\n%s", listed)
	}
	snapshot, err := newTeamsModelProfileResolverWithRuntime(root, resolver)(ctx, "official:"+officialModel)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Provider != modelprofile.DefaultProvider || snapshot.Model != officialModel {
		t.Fatalf("official resolver snapshot = %#v", snapshot)
	}
}
