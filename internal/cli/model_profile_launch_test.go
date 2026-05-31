package cli

import (
	"context"
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
)

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
		`model="deepseek/deepseek-v4-flash"`,
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
	if launch.Model != "mimo/mimo-v2.5" {
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
	instanceProfileID := modelProfileAdapterInstanceProfileID(resolved, "sk-reusable", modelProfileAdapterListenHostForApp())
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

	launch, err := ensureLongLivedModelProfileAdapterForApp(context.Background(), store, "mimo25", io.Discard)
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
