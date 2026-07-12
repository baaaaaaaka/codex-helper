package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

const millionTokenContextWindowForLaunchTest = 1000000

func TestProbeCodexLoginStatusUsesOfficialStatusExitCode(t *testing.T) {
	previous := codexLoginStatusCommand
	t.Cleanup(func() { codexLoginStatusCommand = previous })
	codexLoginStatusCommand = func(ctx context.Context, command string, args ...string) *exec.Cmd {
		if len(args) != 2 || args[0] != "login" || args[1] != "status" {
			t.Fatalf("command = %q args = %#v", command, args)
		}
		return exec.CommandContext(ctx, os.Args[0], "-test.run=TestCodexLoginStatusHelperProcess", "--", command)
	}
	if !probeCodexLoginStatus(context.Background(), "logged-in", []string{"CXP_TEST_CODEX_LOGIN_MARKER=present"}, io.Discard) {
		t.Fatal("exit zero should report logged in")
	}
	if probeCodexLoginStatus(context.Background(), "logged-out", nil, io.Discard) {
		t.Fatal("non-zero exit should report logged out")
	}
}

func TestOfficialLoginControlsSnapshotCatalogCoexistence(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"mimo25": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:MIMO_API_KEY", Revision: 1},
	}}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MIMO_API_KEY", "test-key")
	resolved, err := modelprofile.Resolve(cfg, "mimo25")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := resolved.Snapshot(time.Now())

	loggedIn, cleanup, err := startModelProfileAdapterForCodex(context.Background(), store, "mimo25", snapshot, "", true, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	if !loggedIn.Unified || loggedIn.ProviderID != cxpUnifiedCodexModelProviderID {
		t.Fatalf("logged-in launch = %#v, want unified official + third-party", loggedIn)
	}

	thirdPartyOnly, cleanupOnly, err := startModelProfileAdapterForCodex(context.Background(), store, "mimo25", snapshot, "", false, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cleanupOnly != nil {
		defer cleanupOnly()
	}
	if thirdPartyOnly.Unified || thirdPartyOnly.ProviderID == cxpUnifiedCodexModelProviderID {
		t.Fatalf("logged-out launch = %#v, want third-party-only", thirdPartyOnly)
	}
}

func TestCodexLoginStatusHelperProcess(t *testing.T) {
	if len(os.Args) == 0 || !slices.Contains(os.Args, "--") {
		return
	}
	mode := os.Args[len(os.Args)-1]
	if mode == "logged-in" {
		if os.Getenv("CXP_TEST_CODEX_LOGIN_MARKER") != "present" {
			os.Exit(9)
		}
		os.Exit(0)
	}
	os.Exit(1)
}

func stubUnifiedModelCatalogPrewarm(t *testing.T) {
	t.Helper()
	previousCatalog := loadBundledCodexModelCatalogFn
	previousProbe := codexLoginStatusProbeFn
	t.Cleanup(func() { loadBundledCodexModelCatalogFn = previousCatalog; codexLoginStatusProbeFn = previousProbe })
	codexLoginStatusProbeFn = func(context.Context, string, []string, io.Writer) bool { return true }
	loadBundledCodexModelCatalogFn = func(context.Context, string) ([]byte, error) {
		return []byte(`{"models":[{"slug":"gpt-test","priority":1}]}`), nil
	}
}

func stubCodexLoginProbe(t *testing.T, loggedIn bool) {
	t.Helper()
	previous := codexLoginStatusProbeFn
	t.Cleanup(func() { codexLoginStatusProbeFn = previous })
	codexLoginStatusProbeFn = func(context.Context, string, []string, io.Writer) bool { return loggedIn }
}

func TestBuildInitialUnifiedCatalogUsesCompleteBundledOfficialCatalog(t *testing.T) {
	previous := loadBundledCodexModelCatalogFn
	t.Cleanup(func() { loadBundledCodexModelCatalogFn = previous })
	loadBundledCodexModelCatalogFn = func(context.Context, string) ([]byte, error) {
		return []byte(`{"models":[{"slug":"gpt-official","priority":4,"base_instructions":"official","future":{"kept":true}}]}`), nil
	}
	provider := modelprofile.ProviderSpec{ID: "third", DisplayName: "Third", DefaultModel: "cxp/third", UsesAdapter: true}
	raw, source, err := buildInitialUnifiedCatalog(context.Background(), "codex", []modelprofile.ProviderSpec{provider})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, "bundled official") {
		t.Fatalf("source = %q", source)
	}
	var catalog struct {
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Models) != 2 || catalog.Models[0]["slug"] != "gpt-official" || catalog.Models[1]["slug"] != "cxp/third" {
		t.Fatalf("models = %#v", catalog.Models)
	}
	if catalog.Models[0]["base_instructions"] != "official" {
		t.Fatalf("official metadata changed: %#v", catalog.Models[0])
	}
}

func TestBuildInitialUnifiedCatalogRequiresOfficialCatalog(t *testing.T) {
	previous := loadBundledCodexModelCatalogFn
	t.Cleanup(func() { loadBundledCodexModelCatalogFn = previous })
	loadBundledCodexModelCatalogFn = func(context.Context, string) ([]byte, error) {
		return nil, fmt.Errorf("unsupported Codex version")
	}
	provider := modelprofile.ProviderSpec{ID: "third", DisplayName: "Third", DefaultModel: "cxp/third", UsesAdapter: true}
	if _, _, err := buildInitialUnifiedCatalog(context.Background(), "codex", []modelprofile.ProviderSpec{provider}); err == nil {
		t.Fatal("missing official catalog unexpectedly succeeded")
	}
}

func TestOfficialDefaultBypassesUnifiedGatewayWhenCatalogUnavailable(t *testing.T) {
	previous := loadBundledCodexModelCatalogFn
	t.Cleanup(func() { loadBundledCodexModelCatalogFn = previous })
	loadBundledCodexModelCatalogFn = func(context.Context, string) ([]byte, error) {
		return nil, fmt.Errorf("unsupported Codex version")
	}
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"mimo25": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:MIMO_API_KEY", Revision: 1},
	}}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MIMO_API_KEY", "test-key")
	launch, cleanup, err := startModelProfileAdapterForCodex(context.Background(), store, "default", modelprofile.Snapshot{}, "", true, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cleanup != nil {
		cleanup()
	}
	if launch.Enabled {
		t.Fatalf("official default was redirected through fallback gateway: %#v", launch)
	}
}

func TestSelectedOfficialModelDoesNotSilentlyFallbackWhenGatewayUnavailable(t *testing.T) {
	previous := loadBundledCodexModelCatalogFn
	t.Cleanup(func() { loadBundledCodexModelCatalogFn = previous })
	loadBundledCodexModelCatalogFn = func(context.Context, string) ([]byte, error) {
		return nil, fmt.Errorf("unsupported Codex version")
	}
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"mimo25": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:MIMO_API_KEY", Revision: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MIMO_API_KEY", "test-key")
	snapshot := modelprofile.Snapshot{Name: "gpt-5.6-luna", Provider: modelprofile.DefaultProvider, Model: "gpt-5.6-luna", DefaultModel: "gpt-5.6-luna", Revision: 1}
	launch, cleanup, err := startModelProfileAdapterForCodex(context.Background(), store, "", snapshot, "", true, io.Discard)
	if cleanup != nil {
		cleanup()
	}
	if err != nil || !launch.Enabled || !launch.Native || launch.Model != "gpt-5.6-luna" {
		t.Fatalf("selected official native fallback launch=%#v err=%v", launch, err)
	}
	args := appendCodexModelProfileArgs([]string{"codex", "app-server"}, launch)
	if !slices.Contains(args, `model="gpt-5.6-luna"`) {
		t.Fatalf("native fallback omitted explicit model: %v", args)
	}
}

func TestOfficialDefaultFailsOpenWhenThirdPartyGatewayConfigurationConflicts(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Version:  config.CurrentVersion,
		Profiles: []config.Profile{{ID: "proxy-a"}, {ID: "proxy-b"}},
		ModelProfiles: map[string]config.ModelProfile{
			"a": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:KEY_A", SSHProxy: "proxy-a", Revision: 1},
			"b": {Provider: "deepseek", Model: "deepseek/deepseek-v4-pro", APIKeyRef: "env:KEY_B", SSHProxy: "proxy-b", Revision: 1},
		},
	}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KEY_A", "a")
	t.Setenv("KEY_B", "b")
	launch, cleanup, err := startModelProfileAdapterForCodex(context.Background(), store, "default", modelprofile.Snapshot{}, "", true, io.Discard)
	if err != nil {
		t.Fatalf("official default was blocked by third-party conflict: %v", err)
	}
	if cleanup != nil {
		cleanup()
	}
	if launch.Enabled {
		t.Fatalf("official default did not retain native provider: %#v", launch)
	}
}

func TestResolveRoutableConfiguredModelsIsolatesUnavailableCredential(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"good": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:GOOD_KEY", Revision: 1},
		"bad":  {Provider: "glm", APIKeyRef: "env:MISSING_KEY", Revision: 1},
	}}
	t.Setenv("GOOD_KEY", "usable")
	got, keys := resolveRoutableConfiguredModels(cfg, store, io.Discard)
	if len(got) != 1 || got[0].Name != "good" || keys["good"] != "usable" {
		t.Fatalf("resolved=%#v keys=%#v", got, keys)
	}
}

func TestPrepareCodexModelProfileForRunOfficialOnlyIsExactNoOp(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	previousProbe := codexLoginStatusProbeFn
	t.Cleanup(func() { codexLoginStatusProbeFn = previousProbe })
	codexLoginStatusProbeFn = func(context.Context, string, []string, io.Writer) bool {
		t.Fatal("official-only launch must not run a login probe")
		return false
	}
	original := []string{"codex", "exec", "-"}
	opts := runTargetOptions{ExtraEnv: []string{"UNCHANGED=value"}, Log: io.Discard}
	got, cleanup, err := prepareCodexModelProfileForRun(context.Background(), store, original, &opts, "")
	if err != nil {
		t.Fatal(err)
	}
	if cleanup != nil {
		t.Fatal("official-only launch returned a cleanup for a gateway that must not exist")
	}
	if !slices.Equal(got, original) {
		t.Fatalf("official-only args changed: got %#v want %#v", got, original)
	}
	if !slices.Equal(opts.ExtraEnv, []string{"UNCHANGED=value"}) {
		t.Fatalf("official-only environment changed: %#v", opts.ExtraEnv)
	}
}

func TestCodexDesktopUnifiedModelConfigUsesOfficialAuthAndLocalHeader(t *testing.T) {
	got := codexDesktopModelProfileConfigTOML(codexModelProfileLaunch{
		Enabled:      true,
		Unified:      true,
		BaseURL:      "http://127.0.0.1:12345/v1",
		ProviderName: "Unified official and third-party models",
		EnvKey:       envCXPUnifiedGatewayKey,
	}, "")
	for _, want := range []string{
		`model_provider = "cxp-unified"`,
		`[model_providers.cxp-unified]`,
		`requires_openai_auth = true`,
		`env_http_headers = { "X-CXP-Gateway-Key" = "CXP_UNIFIED_GATEWAY_KEY" }`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("desktop unified config missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "model =") || strings.Contains(got, "env_key =") {
		t.Fatalf("desktop unified official-default config contains a third-party-only override:\n%s", got)
	}
}

func TestCopyCodexOfficialAuthToProfileHome(t *testing.T) {
	source := t.TempDir()
	destination := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "auth.json"), []byte(`{"auth_mode":"chatgpt"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyCodexOfficialAuthToProfileHome(source, destination); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(destination, "auth.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"auth_mode":"chatgpt"}` {
		t.Fatalf("copied auth = %s", raw)
	}
	info, err := os.Stat(filepath.Join(destination, "auth.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("copied auth permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestCopyCodexOfficialAuthAllowsCredentialStoreOnlyLogin(t *testing.T) {
	if err := copyCodexOfficialAuthToProfileHome(t.TempDir(), t.TempDir()); err != nil {
		t.Fatalf("credential-store-only login: %v", err)
	}
}

func TestInheritedCodexDesktopModelProfileConfigPreservesUserSettings(t *testing.T) {
	source := t.TempDir()
	original := `model = "gpt-old"
model_provider = "openai"
cli_auth_credentials_store = "keyring"
sandbox_mode = "workspace-write"

[mcp_servers.docs]
command = "docs-server"

[model_providers.cxp-unified]
name = "stale"
base_url = "http://stale.invalid"
`
	if err := os.WriteFile(filepath.Join(source, "config.toml"), []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	generated := codexDesktopModelProfileConfigTOML(codexModelProfileLaunch{Unified: true, BaseURL: "http://127.0.0.1:1234/v1"}, "")
	got, err := inheritedCodexDesktopModelProfileConfig(source, generated)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`cli_auth_credentials_store = "keyring"`, `sandbox_mode = "workspace-write"`, `[mcp_servers.docs]`, `command = "docs-server"`, `base_url = "http://127.0.0.1:1234/v1"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("merged config missing %q:\n%s", want, got)
		}
	}
	for _, stale := range []string{`model = "gpt-old"`, `model_provider = "openai"`, `http://stale.invalid`} {
		if strings.Contains(got, stale) {
			t.Fatalf("merged config retained %q:\n%s", stale, got)
		}
	}
}

func waitForProxyPrepareContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		return fmt.Errorf("proxy prepare context was not canceled")
	}
}

func legacyModelFingerprintV1ForLaunchTest(t *testing.T, provider modelprofile.ProviderSpec, modelRef string) string {
	t.Helper()
	model, ok := provider.ResolveModel(modelRef)
	if !ok {
		t.Fatalf("ResolveModel(%q) failed", modelRef)
	}
	material := strings.Join([]string{
		strings.TrimSpace(provider.ID),
		strings.TrimSpace(model.PublicID()),
		strings.TrimSpace(model.UpstreamModel()),
		fmt.Sprint(model.ContextWindow),
		fmt.Sprint(model.MaxContextWindow),
		fmt.Sprint(model.SupportsTools),
		fmt.Sprint(model.SupportsVision),
		fmt.Sprint(model.SupportsReason),
		fmt.Sprint(model.SupportsSearch),
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return "model:" + hex.EncodeToString(sum[:])[:24]
}

func providerWithLegacy128KContextForLaunchTest(provider modelprofile.ProviderSpec) modelprofile.ProviderSpec {
	provider.Models = append([]modelprofile.ModelSpec(nil), provider.Models...)
	for i := range provider.Models {
		provider.Models[i].ContextWindow = 128000
		provider.Models[i].MaxContextWindow = 128000
	}
	return provider
}

func assertLaunchArgsCatalogHasMillionTokenModel(t *testing.T, args []string, model string) {
	t.Helper()
	catalogPath := ""
	const prefix = `model_catalog_json="`
	for _, arg := range args {
		if strings.HasPrefix(arg, prefix) && strings.HasSuffix(arg, `"`) {
			catalogPath = strings.TrimSuffix(strings.TrimPrefix(arg, prefix), `"`)
			break
		}
	}
	if catalogPath == "" {
		t.Fatalf("appserver args missing model_catalog_json path:\n%v", args)
	}
	raw, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatalf("read model catalog %q: %v", catalogPath, err)
	}
	var catalog struct {
		Models []struct {
			Slug             string `json:"slug"`
			ContextWindow    int    `json:"context_window"`
			MaxContextWindow int    `json:"max_context_window"`
		} `json:"models"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatalf("decode model catalog %q: %v\n%s", catalogPath, err, raw)
	}
	for _, entry := range catalog.Models {
		if entry.Slug != model {
			continue
		}
		if entry.ContextWindow != millionTokenContextWindowForLaunchTest || entry.MaxContextWindow != millionTokenContextWindowForLaunchTest {
			t.Fatalf("%s context window = %d/%d, want %d/%d", model, entry.ContextWindow, entry.MaxContextWindow, millionTokenContextWindowForLaunchTest, millionTokenContextWindowForLaunchTest)
		}
		return
	}
	t.Fatalf("model catalog missing %q:\n%s", model, raw)
}

func TestPrepareCodexModelProfileForRunStartsAdapterAndInjectsConfig(t *testing.T) {
	stubUnifiedModelCatalogPrewarm(t)
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-work": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  2,
			},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("DEEPSEEK_API_KEY", "sk-test")

	opts := runTargetOptions{
		ModelProfileRef: "deepseek-work",
		Log:             io.Discard,
	}
	gotArgs, cleanup, err := prepareCodexModelProfileForRun(context.Background(), store, []string{"codex", "exec", "-"}, &opts, "")
	if err != nil {
		t.Fatalf("prepareCodexModelProfileForRun: %v", err)
	}
	if cleanup == nil {
		t.Fatalf("expected adapter cleanup")
	}
	defer cleanup()
	joined := strings.Join(gotArgs, "\n")
	for _, want := range []string{
		`model_provider="cxp-unified"`,
		`model="deepseek/deepseek-v4-pro"`,
		`model_catalog_json="`,
		`model_providers.cxp-unified.wire_api="responses"`,
		`model_providers.cxp-unified.requires_openai_auth=true`,
		`model_providers.cxp-unified.env_http_headers=`,
		"exec\n-",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("codex args missing %q:\n%v", want, gotArgs)
		}
	}
	if strings.Contains(joined, `web_search="disabled"`) {
		t.Fatalf("provider with hosted web search unexpectedly disabled it: %v", gotArgs)
	}
	if !slices.ContainsFunc(opts.ExtraEnv, func(entry string) bool {
		return strings.HasPrefix(entry, envCXPUnifiedGatewayKey+"=") && len(entry) > len(envCXPUnifiedGatewayKey+"=")
	}) {
		t.Fatalf("missing proxy key env: %v", opts.ExtraEnv)
	}
}

func TestPrepareCodexResponsesCompatibleProfileUsesNativeResponsesAPI(t *testing.T) {
	stubUnifiedModelCatalogPrewarm(t)
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	const modelID = "example/reasoning-model"
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"custom": {
				Provider:  "responses-compatible",
				Model:     modelID,
				BaseURL:   "https://responses.example.invalid",
				APIKeyRef: "env:RESPONSES_API_KEY",
				Revision:  1,
			},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("RESPONSES_API_KEY", "sk-responses-test")
	opts := runTargetOptions{ModelProfileRef: "custom", Log: io.Discard}
	gotArgs, cleanup, err := prepareCodexModelProfileForRun(context.Background(), store, []string{"codex", "exec", "-"}, &opts, "")
	if err != nil {
		t.Fatalf("prepareCodexModelProfileForRun: %v", err)
	}
	defer cleanup()
	joined := strings.Join(gotArgs, "\n")
	for _, want := range []string{
		`model="` + modelID + `"`,
		`model_providers.cxp-unified.base_url="http://127.0.0.1:`,
		`model_providers.cxp-unified.wire_api="responses"`,
		`web_search="disabled"`,
		`features.multi_agent_v2.hide_spawn_agent_metadata=false`,
		`agents.gpt_search.description="`,
		`agents.gpt_search.config_file="`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("Codex args missing %q:\n%v", want, gotArgs)
		}
	}
	if !slices.ContainsFunc(opts.ExtraEnv, func(item string) bool {
		return strings.HasPrefix(item, envCXPUnifiedGatewayKey+"=") && item != envCXPUnifiedGatewayKey+"=sk-responses-test"
	}) {
		t.Fatalf("native Responses proxy key was not injected: %v", opts.ExtraEnv)
	}
	if strings.Contains(joined, "https://responses.example.invalid") {
		t.Fatalf("key-bearing Responses upstream was exposed directly to Codex: %v", gotArgs)
	}
	fallbackPath := ""
	for _, arg := range gotArgs {
		const prefix = `agents.gpt_search.config_file="`
		if strings.HasPrefix(arg, prefix) && strings.HasSuffix(arg, `"`) {
			fallbackPath = strings.TrimSuffix(strings.TrimPrefix(arg, prefix), `"`)
			break
		}
	}
	if fallbackPath == "" {
		t.Fatalf("Codex args omitted the generated web-search fallback path: %v", gotArgs)
	}
	fallbackRaw, err := os.ReadFile(fallbackPath)
	if err != nil {
		t.Fatalf("read generated web-search fallback config: %v", err)
	}
	for _, want := range []string{
		`model_provider = "openai"`,
		`model = "gpt-5.6-luna"`,
		`model_reasoning_effort = "high"`,
		`web_search = "live"`,
		`[features.multi_agent_v2]`,
		`enabled = false`,
		`context_size = "high"`,
	} {
		if !strings.Contains(string(fallbackRaw), want) {
			t.Fatalf("generated fallback config missing %q:\n%s", want, fallbackRaw)
		}
	}
	info, err := os.Stat(fallbackPath)
	if err != nil {
		t.Fatalf("stat generated web-search fallback config: %v", err)
	}
	if got := info.Mode().Perm(); runtime.GOOS != "windows" && got != 0o600 {
		t.Fatalf("fallback config permissions = %o, want 600", got)
	}
}

func TestCodexDesktopModelProfileConfigAddsSearchFallbackOnlyWhenNeeded(t *testing.T) {
	base := codexModelProfileLaunch{
		Enabled:      true,
		Model:        "example/model",
		BaseURL:      "http://127.0.0.1:12345/v1",
		ProviderName: "Example",
	}
	withoutFallback := codexDesktopModelProfileConfigTOML(base, "catalog.json", "gpt-search.toml")
	if strings.Contains(withoutFallback, "gpt_search") || strings.Contains(withoutFallback, "multi_agent_v2") {
		t.Fatalf("native-search profile unexpectedly received fallback config:\n%s", withoutFallback)
	}

	base.DisableHostedWebSearch = true
	withFallback := codexDesktopModelProfileConfigTOML(base, "catalog.json", "gpt-search.toml")
	for _, want := range []string{
		`web_search = "disabled"`,
		`[features.multi_agent_v2]`,
		`hide_spawn_agent_metadata = false`,
		`[agents.gpt_search]`,
		`config_file = "gpt-search.toml"`,
	} {
		if !strings.Contains(withFallback, want) {
			t.Fatalf("fallback desktop config missing %q:\n%s", want, withFallback)
		}
	}
}

func TestWriteCodexDesktopModelProfileConfigWritesPrivateSearchFallback(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	launch := codexModelProfileLaunch{
		Enabled:                true,
		Name:                   "private-responses",
		Model:                  "example/model",
		BaseURL:                "http://127.0.0.1:12345/v1",
		Revision:               2,
		ProviderName:           "Example",
		DisableHostedWebSearch: true,
		WebSearchFallbackTOML:  codexWebSearchFallbackRoleConfigTOML(),
	}
	codexHome, err := writeCodexDesktopModelProfileConfig(store, launch, codexDesktopPlatformMac)
	if err != nil {
		t.Fatalf("writeCodexDesktopModelProfileConfig: %v", err)
	}
	configRaw, err := os.ReadFile(filepath.Join(codexHome, "config.toml"))
	if err != nil {
		t.Fatalf("read generated desktop config: %v", err)
	}
	fallbackPath := filepath.Join(codexHome, codexWebSearchFallbackConfigName)
	fallbackRaw, err := os.ReadFile(fallbackPath)
	if err != nil {
		t.Fatalf("read generated desktop fallback config: %v", err)
	}
	if !strings.Contains(string(configRaw), `config_file = "`+tomlEscapeString(fallbackPath)+`"`) {
		t.Fatalf("desktop config omitted fallback path:\n%s", configRaw)
	}
	if !strings.Contains(string(fallbackRaw), `model = "gpt-5.6-luna"`) || strings.Contains(string(fallbackRaw), "example/model") || strings.Contains(string(fallbackRaw), "127.0.0.1") {
		t.Fatalf("desktop fallback config is missing Luna or leaked parent provider details:\n%s", fallbackRaw)
	}
	info, err := os.Stat(fallbackPath)
	if err != nil {
		t.Fatalf("stat generated desktop fallback config: %v", err)
	}
	if got := info.Mode().Perm(); runtime.GOOS != "windows" && got != 0o600 {
		t.Fatalf("desktop fallback permissions = %o, want 600", got)
	}
}

func TestAppendCodexModelProfileArgsInsertsConfigInExecScopeCI(t *testing.T) {
	launch := codexModelProfileLaunch{
		Enabled:      true,
		Model:        "mimo/mimo-v2.5",
		BaseURL:      "http://127.0.0.1:12345/v1",
		ProviderName: "MiMo",
	}
	tests := []struct {
		name      string
		args      []string
		wantIndex int
	}{
		{
			name:      "exec with subcommand config",
			args:      []string{"/tmp/codex", "exec", "--json", "-c", `model_reasoning_effort="high"`, "-"},
			wantIndex: 2,
		},
		{
			name:      "exec resume with subcommand config",
			args:      []string{"/tmp/codex", "exec", "resume", "--json", "-c", `model_reasoning_effort="high"`, "thread-1", "-"},
			wantIndex: 3,
		},
		{
			name:      "root command keeps root config",
			args:      []string{"/tmp/codex", "--help"},
			wantIndex: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := appendCodexModelProfileArgs(tt.args, launch)
			gotIndex := codexConfigPairIndex(got, `model_provider="`+cxpCodexModelProviderID+`"`)
			if gotIndex != tt.wantIndex {
				t.Fatalf("model provider config index = %d, want %d:\n%#v", gotIndex, tt.wantIndex, got)
			}
			if !slices.Contains(got, `model_providers.`+cxpCodexModelProviderID+`.requires_openai_auth=false`) {
				t.Fatalf("missing requires_openai_auth override:\n%#v", got)
			}
		})
	}
}

func codexConfigPairIndex(args []string, value string) int {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "-c" && args[i+1] == value {
			return i
		}
	}
	return -1
}

func TestStartModelProfileAdapterServesModels(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {
				Provider:  "mimo",
				Model:     "mimo/mimo-v2.5-pro",
				APIKeyRef: "env:MIMO_API_KEY",
				Revision:  1,
			},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("MIMO_API_KEY", "sk-test")
	launch, cleanup, err := startModelProfileAdapterForCodex(context.Background(), store, "mimo25", modelprofile.Snapshot{
		Name:       "mimo25",
		Provider:   "mimo",
		Model:      "mimo/mimo-v2.5-pro",
		APIKeyRef:  "env:MIMO_API_KEY",
		Revision:   1,
		CapturedAt: time.Now(),
	}, "", false, io.Discard)
	if err != nil {
		t.Fatalf("startModelProfileAdapterForCodex: %v", err)
	}
	defer cleanup()
	req, err := http.NewRequest(http.MethodGet, launch.BaseURL+"/models", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+launch.ProxyKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /models: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /models status=%d", resp.StatusCode)
	}
	if launch.Model != "mimo/mimo-v2.5-pro" {
		t.Fatalf("launch model = %q, want public model id", launch.Model)
	}
	if launch.CatalogPath == "" {
		t.Fatalf("launch catalog path is empty")
	}
	raw, err := os.ReadFile(launch.CatalogPath)
	if err != nil {
		t.Fatalf("read catalog: %v", err)
	}
	if !strings.Contains(string(raw), `"slug": "mimo/mimo-v2.5"`) || !strings.Contains(string(raw), `"slug": "mimo/mimo-v2.5-pro"`) {
		t.Fatalf("catalog missing MiMo public models:\n%s", raw)
	}
}

func TestCodexModelProfileFacadeEnablesExecutionTargetShellPolicy(t *testing.T) {
	facade := newCodexModelProfileFacade(nil, responsesadapter.NewMemoryStore())
	if facade.ShellPolicy == nil {
		t.Fatal("production model-profile facade omitted the execution-target shell policy")
	}
	prepared := facade.ShellPolicy.Prepare("call-gpu", responsespolicy.ShellCommandTool, `{"command":"nvidia-smi"}`)
	if !strings.Contains(prepared, responsespolicy.EscalationPermission) {
		t.Fatalf("prepared shell call = %s, want %q", prepared, responsespolicy.EscalationPermission)
	}
}

func TestPrepareTeamsAppServerModelProfileWithoutSSHUsesGlobalProxyPreferenceCI(t *testing.T) {
	lockCLITestHooks(t)
	stubCodexLoginProbe(t, true)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	enabled := true
	profile := config.Profile{ID: "p1", Name: "dev", Host: "host", Port: 22, User: "me"}
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &enabled,
		Profiles:     []config.Profile{profile},
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {Provider: "mimo", APIKeyRef: "env:MIMO_API_KEY", Revision: 1},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("MIMO_API_KEY", "sk-test")

	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	t.Cleanup(func() { codexAppEnsureProxyURLFn = prevEnsureProxyURL })
	codexAppEnsureProxyURLFn = func(_ context.Context, gotStore *config.Store, gotProfile config.Profile, _ []config.Instance, _ io.Writer) (string, error) {
		if gotStore.Path() != store.Path() {
			t.Fatalf("store path = %q, want %q", gotStore.Path(), store.Path())
		}
		if gotProfile.ID != profile.ID {
			t.Fatalf("upstream profile = %#v, want %#v", gotProfile, profile)
		}
		return "http://127.0.0.1:23456", nil
	}

	args, env, cleanup, err := prepareTeamsAppServerModelProfile(&rootOptions{configPath: store.Path()}, "mimo25", modelprofile.Snapshot{}, io.Discard)
	if err != nil {
		t.Fatalf("prepareTeamsAppServerModelProfile: %v", err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	if !slices.ContainsFunc(env, func(entry string) bool {
		return strings.HasPrefix(entry, envCXPUnifiedGatewayKey+"=")
	}) {
		t.Fatalf("missing proxy key env: %v", env)
	}
	joined := strings.Join(args, "\n")
	for _, want := range []string{
		`model_provider="` + cxpUnifiedCodexModelProviderID + `"`,
		`model="mimo/mimo-v2.5"`,
		`model_providers.` + cxpUnifiedCodexModelProviderID + `.wire_api="responses"`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("appserver args missing %q:\n%v", want, args)
		}
	}
}

func TestPrepareTeamsAppServerModelProfileAllowsLegacyDeepSeekContextFingerprintCI(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider string
		model    string
		keyRef   string
	}{
		{
			name:     "deepseek-flash",
			provider: "deepseek",
			model:    "deepseek/deepseek-v4-flash",
			keyRef:   "env:DEEPSEEK_API_KEY",
		},
		{
			name:     "deepseek-pro",
			provider: "deepseek",
			model:    "deepseek/deepseek-v4-pro",
			keyRef:   "env:DEEPSEEK_API_KEY",
		},
		{
			name:     "mimo25",
			provider: "mimo",
			model:    "mimo/mimo-v2.5",
			keyRef:   "env:MIMO_API_KEY",
		},
		{
			name:     "mimo25-pro",
			provider: "mimo",
			model:    "mimo/mimo-v2.5-pro",
			keyRef:   "env:MIMO_API_KEY",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
			if err != nil {
				t.Fatalf("NewStore: %v", err)
			}
			disabled := false
			if err := store.Save(config.Config{
				Version:      config.CurrentVersion,
				ProxyEnabled: &disabled,
				ModelProfiles: map[string]config.ModelProfile{
					tc.name: {
						Provider:  tc.provider,
						Model:     tc.model,
						APIKeyRef: tc.keyRef,
						Revision:  1,
					},
				},
			}); err != nil {
				t.Fatalf("Save: %v", err)
			}
			envName := strings.TrimPrefix(tc.keyRef, "env:")
			t.Setenv(envName, "sk-test")

			spec, ok := modelprofile.LookupProvider(tc.provider)
			if !ok {
				t.Fatalf("%s provider missing", tc.provider)
			}
			oldSpec := providerWithLegacy128KContextForLaunchTest(spec)
			snapshot := modelprofile.Snapshot{
				Name:               tc.name,
				Provider:           tc.provider,
				Model:              tc.model,
				APIKeyRef:          tc.keyRef,
				Revision:           1,
				BaseURLHash:        modelprofile.BaseURLHash(spec.BaseURL),
				AdapterProfile:     spec.AdapterProfile,
				DefaultModel:       tc.model,
				ModelFingerprint:   legacyModelFingerprintV1ForLaunchTest(t, oldSpec, tc.model),
				CatalogFingerprint: modelprofile.CatalogFingerprint(oldSpec),
				CapturedAt:         time.Now().UTC(),
			}

			args, _, cleanup, err := prepareTeamsAppServerModelProfile(&rootOptions{configPath: store.Path()}, "", snapshot, io.Discard)
			if err != nil {
				t.Fatalf("prepareTeamsAppServerModelProfile legacy context snapshot: %v", err)
			}
			if cleanup != nil {
				defer cleanup()
			}
			want := `model="` + tc.model + `"`
			if joined := strings.Join(args, "\n"); !strings.Contains(joined, want) {
				t.Fatalf("appserver args missing pinned model %q:\n%v", want, args)
			}
			assertLaunchArgsCatalogHasMillionTokenModel(t, args, tc.model)
		})
	}
}

func TestPrepareTeamsAppServerModelProfileProxyPrepareTimesOutCI(t *testing.T) {
	lockCLITestHooks(t)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	enabled := true
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &enabled,
		Profiles:     []config.Profile{{ID: "p1", Name: "dev", Host: "host", Port: 22, User: "me"}},
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-pro": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  1,
			},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("DEEPSEEK_API_KEY", "sk-test")

	oldTimeout := teamsAppServerModelProfilePrepareTimeout
	teamsAppServerModelProfilePrepareTimeout = 100 * time.Millisecond
	t.Cleanup(func() { teamsAppServerModelProfilePrepareTimeout = oldTimeout })

	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	t.Cleanup(func() { codexAppEnsureProxyURLFn = prevEnsureProxyURL })
	codexAppEnsureProxyURLFn = func(ctx context.Context, _ *config.Store, _ config.Profile, _ []config.Instance, _ io.Writer) (string, error) {
		return "", waitForProxyPrepareContext(ctx)
	}

	started := time.Now()
	_, _, _, err = prepareTeamsAppServerModelProfile(&rootOptions{configPath: store.Path()}, "deepseek-pro", modelprofile.Snapshot{}, io.Discard)
	if err == nil {
		t.Fatal("prepareTeamsAppServerModelProfile error = nil, want timeout")
	}
	if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("prepare error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Fatalf("prepare took %s, want bounded timeout", elapsed)
	}
}

func TestPrepareTeamsAppServerModelProfileUsesCallerCancellationCI(t *testing.T) {
	lockCLITestHooks(t)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	enabled := true
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &enabled,
		Profiles:     []config.Profile{{ID: "p1", Name: "dev", Host: "host", Port: 22, User: "me"}},
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-pro": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  1,
			},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("DEEPSEEK_API_KEY", "sk-test")

	oldTimeout := teamsAppServerModelProfilePrepareTimeout
	teamsAppServerModelProfilePrepareTimeout = time.Hour
	t.Cleanup(func() { teamsAppServerModelProfilePrepareTimeout = oldTimeout })

	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	t.Cleanup(func() { codexAppEnsureProxyURLFn = prevEnsureProxyURL })
	codexAppEnsureProxyURLFn = func(ctx context.Context, _ *config.Store, _ config.Profile, _ []config.Instance, _ io.Writer) (string, error) {
		return "", waitForProxyPrepareContext(ctx)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err = prepareTeamsAppServerModelProfileWithContext(ctx, &rootOptions{configPath: store.Path()}, "deepseek-pro", modelprofile.Snapshot{}, io.Discard)
	if err == nil {
		t.Fatal("prepareTeamsAppServerModelProfileWithContext error = nil, want cancellation")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("prepare error = %v, want canceled", err)
	}
}

func TestPrepareTeamsAppServerModelProfileClearsIncompleteProxyPreferenceCI(t *testing.T) {
	lockCLITestHooks(t)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	enabled := true
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &enabled,
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {Provider: "mimo", APIKeyRef: "env:MIMO_API_KEY", Revision: 1},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Setenv("MIMO_API_KEY", "sk-test")

	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	t.Cleanup(func() { codexAppEnsureProxyURLFn = prevEnsureProxyURL })
	codexAppEnsureProxyURLFn = func(context.Context, *config.Store, config.Profile, []config.Instance, io.Writer) (string, error) {
		t.Fatal("incomplete ProxyEnabled=true state should not attempt to select a missing upstream proxy")
		return "", nil
	}

	args, _, cleanup, err := prepareTeamsAppServerModelProfile(&rootOptions{configPath: store.Path()}, "mimo25", modelprofile.Snapshot{}, io.Discard)
	if err != nil {
		t.Fatalf("prepareTeamsAppServerModelProfile: %v", err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	if joined := strings.Join(args, "\n"); !strings.Contains(joined, `model="mimo/mimo-v2.5"`) {
		t.Fatalf("appserver args missing selected model:\n%v", args)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ProxyEnabled != nil {
		t.Fatalf("incomplete proxy preference should be cleared, got %v", cfg.ProxyEnabled)
	}
}

func TestEnsureLongLivedModelProfileAdapterReusesHealthyInstance(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg := config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {
				Provider:  "mimo",
				APIKeyRef: "env:MIMO_API_KEY",
				Revision:  4,
			},
		},
	}
	t.Setenv("MIMO_API_KEY", "sk-reusable")
	resolved, err := modelprofile.Resolve(cfg, "mimo25")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	configured, err := modelprofile.ResolveConfiguredThirdPartyModels(cfg)
	if err != nil {
		t.Fatal(err)
	}
	instanceProfileID := unifiedModelProfileAdapterInstanceProfileID(configured, map[string]string{"mimo25": "sk-reusable"}, modelProfileAdapterListenHostForApp(), "")
	const instanceID = "adapter-inst"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_codex_proxy/health" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"instanceId":"` + instanceID + `"}`))
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	_, portString, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split server URL host: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	snapshot := resolved.Snapshot(time.Now())
	cfg.Instances = []config.Instance{{
		ID:                   instanceID,
		ProfileID:            instanceProfileID,
		Kind:                 config.InstanceKindModelAdapter,
		HTTPPort:             port,
		DaemonPID:            os.Getpid(),
		LastSeenAt:           time.Now(),
		ModelUnified:         true,
		ModelProfileName:     snapshot.Name,
		ModelProvider:        snapshot.Provider,
		ModelAPIKeyRef:       snapshot.APIKeyRef,
		ModelSSHProxy:        snapshot.SSHProxy,
		ModelRevision:        snapshot.Revision,
		ModelProxyKey:        "reused-proxy-key",
		ModelProfileCaptured: snapshot.CapturedAt,
	}}
	if err := store.Save(cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	launch, err := ensureLongLivedModelProfileAdapterForApp(context.Background(), store, "mimo25", "", io.Discard)
	if err != nil {
		t.Fatalf("ensureLongLivedModelProfileAdapterForApp: %v", err)
	}
	if !launch.Enabled || !launch.Unified || launch.ProxyKey != "reused-proxy-key" {
		t.Fatalf("launch = %#v", launch)
	}
	if launch.Model != "mimo/mimo-v2.5" {
		t.Fatalf("launch model = %q, want public model id", launch.Model)
	}
	if launch.BaseURL != "http://127.0.0.1:"+portString+"/v1" {
		t.Fatalf("BaseURL = %q", launch.BaseURL)
	}
	if len(launch.CatalogJSON) != 0 || launch.CatalogPath != "" {
		t.Fatalf("App unified launch must use dynamic model/list before inference: %#v", launch)
	}
}

func TestModelProfileAdapterInstanceIdentitySeparatesSelectedModel(t *testing.T) {
	cfg := config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-flash": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-flash",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  7,
			},
			"deepseek-pro": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  7,
			},
		},
	}
	flash, err := modelprofile.Resolve(cfg, "deepseek-flash")
	if err != nil {
		t.Fatalf("resolve flash: %v", err)
	}
	pro, err := modelprofile.Resolve(cfg, "deepseek-pro")
	if err != nil {
		t.Fatalf("resolve pro: %v", err)
	}
	if flash.SelectedPublicModel() != "deepseek/deepseek-v4-flash" || pro.SelectedPublicModel() != "deepseek/deepseek-v4-pro" {
		t.Fatalf("selected models flash=%q pro=%q", flash.SelectedPublicModel(), pro.SelectedPublicModel())
	}
	flashID := modelProfileAdapterInstanceProfileID(flash, "sk-same-key", "127.0.0.1", "")
	proID := modelProfileAdapterInstanceProfileID(pro, "sk-same-key", "127.0.0.1", "")
	if flashID == proID {
		t.Fatalf("adapter instance profile ID did not include selected model: %s", flashID)
	}

	inst := config.Instance{
		HTTPPort:         12345,
		ModelProxyKey:    "proxy-key",
		ModelPublicModel: "deepseek/deepseek-v4-pro",
	}
	launch := modelProfileAdapterLaunchFromInstance(pro, inst)
	if launch.Model != "deepseek/deepseek-v4-pro" {
		t.Fatalf("launch model = %q", launch.Model)
	}
}

func TestResponsesCompatibleLongLivedAdapterPreservesDirectCapabilities(t *testing.T) {
	cfg := config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"custom": {
				Provider:  "responses-compatible",
				Model:     "example/reasoning-model",
				BaseURL:   "https://responses.example.invalid/v1",
				APIKeyRef: "env:RESPONSES_API_KEY",
				Revision:  4,
			},
		},
	}
	resolved, err := modelprofile.Resolve(cfg, "custom")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	inst := config.Instance{
		HTTPPort:         12345,
		ModelProxyKey:    "proxy-key",
		ModelProfileName: "custom",
		ModelProvider:    "responses-compatible",
		ModelPublicModel: "example/reasoning-model",
		ModelBaseURL:     "https://responses.example.invalid/v1",
		ModelAPIKeyRef:   "env:RESPONSES_API_KEY",
		ModelRevision:    4,
	}
	launch := modelProfileAdapterLaunchFromInstance(resolved, inst)
	if !launch.Direct || !launch.DisableHostedWebSearch || launch.Model != "example/reasoning-model" {
		t.Fatalf("launch=%#v", launch)
	}
	snapshot := modelProfileSnapshotFromInstance(inst)
	if snapshot.BaseURL != inst.ModelBaseURL || snapshot.Model != inst.ModelPublicModel {
		t.Fatalf("snapshot=%#v", snapshot)
	}
}

func TestModelProfileUpstreamProxyProfileUsesFallbackOnlyWhenModelProfileHasNoSSH(t *testing.T) {
	modelProxy := config.Profile{ID: "model-proxy", Name: "model"}
	globalProxy := config.Profile{ID: "global-proxy", Name: "global"}
	cfg := config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(true),
		Profiles:     []config.Profile{modelProxy, globalProxy},
		ModelProfiles: map[string]config.ModelProfile{
			"with-ssh": {Provider: "mimo", APIKeyRef: "env:MIMO_API_KEY", SSHProxy: "model", Revision: 1},
			"no-ssh":   {Provider: "mimo", APIKeyRef: "env:MIMO_API_KEY", Revision: 1},
		},
	}
	withSSH, err := modelprofile.Resolve(cfg, "with-ssh")
	if err != nil {
		t.Fatalf("resolve with ssh: %v", err)
	}
	got, err := modelProfileUpstreamProxyProfile(cfg, withSSH, "global")
	if err != nil {
		t.Fatalf("modelProfileUpstreamProxyProfile with ssh: %v", err)
	}
	if got == nil || got.ID != modelProxy.ID {
		t.Fatalf("explicit model ssh proxy should win, got %#v", got)
	}

	noSSH, err := modelprofile.Resolve(cfg, "no-ssh")
	if err != nil {
		t.Fatalf("resolve no ssh: %v", err)
	}
	got, err = modelProfileUpstreamProxyProfile(cfg, noSSH, "global")
	if err != nil {
		t.Fatalf("modelProfileUpstreamProxyProfile no ssh: %v", err)
	}
	if got == nil || got.ID != globalProxy.ID {
		t.Fatalf("fallback global proxy = %#v, want %#v", got, globalProxy)
	}

	disabled := false
	cfg.ProxyEnabled = &disabled
	got, err = modelProfileUpstreamProxyProfile(cfg, noSSH, "")
	if err != nil {
		t.Fatalf("modelProfileUpstreamProxyProfile disabled: %v", err)
	}
	if got != nil {
		t.Fatalf("disabled global proxy should not select an upstream profile: %#v", got)
	}
}

func TestModelProfileAdapterInstanceIdentitySeparatesUpstreamProxy(t *testing.T) {
	cfg := config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {Provider: "mimo", APIKeyRef: "env:MIMO_API_KEY", Revision: 1},
		},
	}
	resolved, err := modelprofile.Resolve(cfg, "mimo25")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	proxyA := config.Profile{ID: "a", Name: "a", Host: "a.example", Port: 22, User: "me"}
	proxyB := config.Profile{ID: "b", Name: "b", Host: "b.example", Port: 22, User: "me"}
	withoutProxy := modelProfileAdapterInstanceProfileID(resolved, "sk-same-key", "127.0.0.1", "")
	withProxyA := modelProfileAdapterInstanceProfileID(resolved, "sk-same-key", "127.0.0.1", modelprofile.SSHProxyFingerprint(&proxyA))
	withProxyB := modelProfileAdapterInstanceProfileID(resolved, "sk-same-key", "127.0.0.1", modelprofile.SSHProxyFingerprint(&proxyB))
	if withoutProxy == withProxyA || withProxyA == withProxyB || withoutProxy == withProxyB {
		t.Fatalf("adapter instance identity should include upstream proxy: none=%s a=%s b=%s", withoutProxy, withProxyA, withProxyB)
	}
}

func TestWithLoopbackNoProxyEnvPreservesExistingHosts(t *testing.T) {
	t.Setenv("NO_PROXY", "corp.example,localhost")
	got := withLoopbackNoProxyEnv([]string{"OTHER=value", "no_proxy=internal.example"})
	values := map[string]string{}
	for _, item := range got {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	if runtime.GOOS == "windows" {
		combined := ""
		for key, value := range values {
			if strings.EqualFold(key, "NO_PROXY") {
				combined = value
			}
		}
		for _, want := range []string{"corp.example", "internal.example", "127.0.0.1", "localhost", "::1"} {
			if !slices.Contains(strings.Split(combined, ","), want) {
				t.Fatalf("NO_PROXY = %q, missing %q", combined, want)
			}
		}
		return
	}
	for key, existing := range map[string]string{"NO_PROXY": "corp.example", "no_proxy": "internal.example"} {
		for _, want := range []string{existing, "127.0.0.1", "localhost", "::1"} {
			if !slices.Contains(strings.Split(values[key], ","), want) {
				t.Fatalf("%s = %q, missing %q", key, values[key], want)
			}
		}
	}
}

func TestConfigureOpenAIChatAdapterHTTPPreservesExplicitZeroAndPhaseTimeouts(t *testing.T) {
	zero := 0
	honor := false
	retryTransport := false
	var log strings.Builder
	adapter := responsesadapter.OpenAIChatAdapter{}
	err := configureOpenAIChatAdapterHTTP(&adapter, config.ModelHTTPPolicy{
		TimeoutSeconds:               600,
		ResponseHeaderTimeoutSeconds: 45,
		MaxRetries:                   &zero,
		HonorRetryAfter:              &honor,
		RetryTransportErrors:         &retryTransport,
		MaxConcurrentRequests:        1,
	}, config.ModelStreamPolicy{IdleTimeoutSeconds: 90}, "", &log)
	if err != nil {
		t.Fatal(err)
	}
	if !adapter.MaxRetriesSet || adapter.MaxRetries != 0 || adapter.HonorRetryAfter == nil || *adapter.HonorRetryAfter {
		t.Fatalf("retry policy = %#v", adapter)
	}
	if adapter.RetryTransportErrors == nil || *adapter.RetryTransportErrors || cap(adapter.RequestGate) != 1 {
		t.Fatalf("transport/concurrency policy = %#v", adapter)
	}
	if adapter.StreamIdleTimeout != 90*time.Second || adapter.HTTPClient.Timeout != 600*time.Second {
		t.Fatalf("adapter timeouts = stream:%s total:%s", adapter.StreamIdleTimeout, adapter.HTTPClient.Timeout)
	}
	transport := adapter.HTTPClient.Transport.(*http.Transport)
	if transport.ResponseHeaderTimeout != 45*time.Second {
		t.Fatalf("header timeout = %s", transport.ResponseHeaderTimeout)
	}
	adapter.Status("test status")
	if !strings.Contains(log.String(), "CXP upstream: test status") {
		t.Fatalf("status log = %q", log.String())
	}
}
