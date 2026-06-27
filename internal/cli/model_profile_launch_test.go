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
	"path/filepath"
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
		`model_provider="cxp-thirdparty"`,
		`model="deepseek/deepseek-v4-pro"`,
		`model_catalog_json="`,
		`model_providers.cxp-thirdparty.wire_api="responses"`,
		`model_providers.cxp-thirdparty.requires_openai_auth=false`,
		"exec\n-",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("codex args missing %q:\n%v", want, gotArgs)
		}
	}
	if !slices.ContainsFunc(opts.ExtraEnv, func(entry string) bool {
		return strings.HasPrefix(entry, envCXPResponsesProxyKey+"=") && len(entry) > len(envCXPResponsesProxyKey+"=")
	}) {
		t.Fatalf("missing proxy key env: %v", opts.ExtraEnv)
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
	launch, cleanup, err := startModelProfileAdapterForCodex(context.Background(), store, "mimo25", modelprofile.Snapshot{}, "", io.Discard)
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
		return strings.HasPrefix(entry, envCXPResponsesProxyKey+"=")
	}) {
		t.Fatalf("missing proxy key env: %v", env)
	}
	joined := strings.Join(args, "\n")
	for _, want := range []string{
		`model_provider="` + cxpCodexModelProviderID + `"`,
		`model="mimo/mimo-v2.5"`,
		`model_providers.` + cxpCodexModelProviderID + `.wire_api="responses"`,
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
	instanceProfileID := modelProfileAdapterInstanceProfileID(resolved, "sk-reusable", modelProfileAdapterListenHostForApp(), "")
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
	if !launch.Enabled || launch.ProxyKey != "reused-proxy-key" {
		t.Fatalf("launch = %#v", launch)
	}
	if launch.Model != "mimo/mimo-v2.5" {
		t.Fatalf("launch model = %q, want public model id", launch.Model)
	}
	if launch.BaseURL != "http://127.0.0.1:"+portString+"/v1" {
		t.Fatalf("BaseURL = %q", launch.BaseURL)
	}
	if !strings.Contains(string(launch.CatalogJSON), `"slug": "mimo/mimo-v2.5"`) ||
		!strings.Contains(string(launch.CatalogJSON), `"slug": "mimo/mimo-v2.5-pro"`) {
		t.Fatalf("launch catalog missing MiMo public models:\n%s", launch.CatalogJSON)
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
