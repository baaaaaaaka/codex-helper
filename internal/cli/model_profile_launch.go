package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
)

const (
	cxpCodexModelProviderID = "cxp-thirdparty"
	envCXPResponsesProxyKey = "CXP_RESPONSES_PROXY_KEY"
)

const defaultTeamsAppServerModelProfilePrepareTimeout = 30 * time.Second

var teamsAppServerModelProfilePrepareTimeout = defaultTeamsAppServerModelProfilePrepareTimeout

func prepareCodexModelProfileForRun(
	ctx context.Context,
	store *config.Store,
	cmdArgs []string,
	opts *runTargetOptions,
	upstreamProxyURL string,
) ([]string, func(), error) {
	if opts == nil || len(cmdArgs) == 0 || !isCodexCommand(cmdArgs[0]) {
		return cmdArgs, nil, nil
	}
	ref := opts.ModelProfileRef
	if strings.TrimSpace(ref) == "" && store == nil {
		return cmdArgs, nil, nil
	}
	launch, cleanup, err := startModelProfileAdapterForCodex(ctx, store, ref, opts.ModelProfileSnapshot, upstreamProxyURL, opts.Log)
	if err != nil {
		return nil, nil, err
	}
	if !launch.Enabled {
		return cmdArgs, cleanup, nil
	}
	opts.ExtraEnv = append(opts.ExtraEnv, envCXPResponsesProxyKey+"="+launch.ProxyKey)
	return appendCodexModelProfileArgs(cmdArgs, launch), cleanup, nil
}

type codexModelProfileLaunch struct {
	Enabled      bool
	Name         string
	ProviderID   string
	Model        string
	BaseURL      string
	ProxyKey     string
	Revision     int
	ProviderName string
	CatalogPath  string
	CatalogJSON  []byte
}

func startModelProfileAdapterForCodex(
	ctx context.Context,
	store *config.Store,
	ref string,
	snapshot modelprofile.Snapshot,
	upstreamProxyURL string,
	log io.Writer,
) (codexModelProfileLaunch, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil {
		return codexModelProfileLaunch{}, nil, nil
	}
	if err := ctx.Err(); err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	cfg, err := store.Load()
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	var resolved modelprofile.Resolved
	if snapshot.IsZero() {
		resolved, err = modelprofile.Resolve(cfg, ref)
	} else {
		resolved, err = modelprofile.ResolveSnapshot(cfg, snapshot)
	}
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	if resolved.IsDefault() {
		return codexModelProfileLaunch{}, nil, nil
	}
	if err := ctx.Err(); err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	apiKey, err := modelprofile.ResolveAPIKey(
		resolved.Profile.APIKeyRef,
		modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path())),
		os.Getenv,
	)
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	if err := modelprofile.ValidateSnapshotRuntime(snapshot, resolved, apiKey); err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	catalogJSON, err := modelprofile.CodexModelCatalogJSON(resolved.Provider)
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	catalogPath, err := writeCodexModelProfileCatalog(store, resolved, catalogJSON)
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	proxyKey, err := ids.New()
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	storePath := defaultResponsesStorePath()
	responseStore, storeCleanup, err := responsesStoreFromOptions(responsesServeOptions{storePath: storePath})
	if err != nil {
		_ = ln.Close()
		return codexModelProfileLaunch{}, nil, err
	}
	adapter := responsesadapter.OpenAIChatAdapter{
		BaseURL: resolved.Provider.BaseURL,
		APIKey:  apiKey,
		Profile: responsesadapter.ProfileForProvider(resolved.Provider.AdapterProfile),
	}
	if strings.TrimSpace(upstreamProxyURL) != "" {
		proxyURL, err := url.Parse(upstreamProxyURL)
		if err != nil {
			_ = ln.Close()
			storeCleanup()
			return codexModelProfileLaunch{}, nil, fmt.Errorf("parse upstream proxy url: %w", err)
		}
		adapter.HTTPClient = responsesadapter.NewUpstreamHTTPClient(http.ProxyURL(proxyURL))
	}
	registry, err := responsesadapter.NewProviderRegistry(responsesadapter.ProviderRegistryOptions{
		DefaultProvider: resolved.Provider.ID,
		Providers: []responsesadapter.ProviderConfig{{
			ID:           resolved.Provider.ID,
			ProfileID:    resolved.Provider.AdapterProfile,
			BaseURL:      resolved.Provider.BaseURL,
			APIKey:       apiKey,
			DefaultModel: resolved.SelectedPublicModel(),
			Models:       responsesAdapterModelsForProvider(resolved.Provider),
			Adapter:      adapter,
		}},
		ProxyKeys: map[string]string{proxyKey: resolved.Provider.ID},
		KeySalt:   resolved.Name + ":" + strconv.Itoa(resolved.Revision()),
	})
	if err != nil {
		_ = ln.Close()
		storeCleanup()
		return codexModelProfileLaunch{}, nil, err
	}
	server := &http.Server{Handler: &responsesadapter.Facade{Router: registry, Store: responseStore}}
	done := make(chan error, 1)
	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			done <- err
			return
		}
		done <- nil
	}()
	cleanup := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		<-done
		storeCleanup()
	}
	if err := ctx.Err(); err != nil {
		cleanup()
		return codexModelProfileLaunch{}, nil, err
	}
	baseURL := "http://" + ln.Addr().String() + "/v1"
	if log != nil {
		_, _ = fmt.Fprintf(log, "using model profile %q through local Responses adapter at %s\n", resolved.Name, baseURL)
	}
	return codexModelProfileLaunch{
		Enabled:      true,
		Name:         resolved.Name,
		ProviderID:   resolved.Provider.ID,
		Model:        resolved.SelectedPublicModel(),
		BaseURL:      baseURL,
		ProxyKey:     proxyKey,
		Revision:     resolved.Revision(),
		ProviderName: resolved.Provider.DisplayName,
		CatalogPath:  catalogPath,
		CatalogJSON:  catalogJSON,
	}, cleanup, nil
}

func writeCodexModelProfileCatalog(store *config.Store, resolved modelprofile.Resolved, catalogJSON []byte) (string, error) {
	name := safeModelProfilePathPart(resolved.Name)
	if name == "" {
		name = "profile"
	}
	dirName := fmt.Sprintf("%s-rev%d", name, resolved.Revision())
	if resolved.Revision() <= 0 {
		dirName = name
	}
	dir := filepath.Join(filepath.Dir(store.Path()), "model-profiles", dirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "catalog.json")
	if err := os.WriteFile(path, catalogJSON, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func responsesAdapterModelsForProvider(provider modelprofile.ProviderSpec) []responsesadapter.ModelInfo {
	models := provider.ModelCatalog()
	out := make([]responsesadapter.ModelInfo, 0, len(models))
	for _, model := range models {
		publicID := model.PublicID()
		upstreamID := model.UpstreamModel()
		if publicID == "" || upstreamID == "" {
			continue
		}
		out = append(out, responsesadapter.ModelInfo{
			ID:         publicID,
			OwnedBy:    provider.ID,
			UpstreamID: upstreamID,
		})
	}
	return out
}

func modelProfileCatalogFingerprint(provider modelprofile.ProviderSpec) string {
	raw, err := modelprofile.CodexModelCatalogJSON(provider)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func modelProfileRequiresAdapter(root *rootOptions, ref string) (bool, error) {
	if strings.TrimSpace(ref) == "" {
		return false, nil
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		return false, err
	}
	cfg, err := store.Load()
	if err != nil {
		return false, err
	}
	resolved, err := modelprofile.Resolve(cfg, ref)
	if err != nil {
		return false, err
	}
	return resolved.Provider.UsesAdapter, nil
}

func prepareTeamsAppServerModelProfile(root *rootOptions, ref string, snapshot modelprofile.Snapshot, log io.Writer) ([]string, []string, error) {
	return prepareTeamsAppServerModelProfileWithContext(context.Background(), root, ref, snapshot, log)
}

func prepareTeamsAppServerModelProfileWithContext(ctx context.Context, root *rootOptions, ref string, snapshot modelprofile.Snapshot, log io.Writer) ([]string, []string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" && snapshot.IsZero() {
		return nil, nil, nil
	}
	ctx, cancel := withTeamsAppServerModelProfilePrepareTimeout(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		return nil, nil, err
	}
	cfg, err := store.Load()
	if err != nil {
		return nil, nil, err
	}
	var resolved modelprofile.Resolved
	if snapshot.IsZero() {
		resolved, err = modelprofile.Resolve(cfg, ref)
	} else {
		resolved, err = modelprofile.ResolveSnapshot(cfg, snapshot)
	}
	if err != nil {
		return nil, nil, err
	}
	if resolved.IsDefault() {
		return nil, nil, nil
	}
	if resolved.SSHProfile == nil {
		cfg, err = modelProfileConfigWithImplicitProxyPreference(store, cfg)
		if err != nil {
			return nil, nil, err
		}
	}
	upstreamProxyURL := ""
	upstreamProfile, err := modelProfileUpstreamProxyProfile(cfg, resolved, "")
	if err != nil {
		return nil, nil, err
	}
	if upstreamProfile != nil {
		upstreamProxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *upstreamProfile, cfg.Instances, log)
		if err != nil {
			return nil, nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	launch, _, err := startModelProfileAdapterForCodex(ctx, store, ref, snapshot, upstreamProxyURL, log)
	if err != nil {
		return nil, nil, err
	}
	if !launch.Enabled {
		return nil, nil, nil
	}
	args := appendCodexModelProfileArgs([]string{"codex"}, launch)
	if len(args) > 0 {
		args = args[1:]
	}
	env := []string{envCXPResponsesProxyKey + "=" + launch.ProxyKey}
	return args, env, nil
}

func withTeamsAppServerModelProfilePrepareTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	timeout := teamsAppServerModelProfilePrepareTimeout
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func modelProfileConfigWithImplicitProxyPreference(store *config.Store, cfg config.Config) (config.Config, error) {
	if cfg.ProxyEnabled != nil {
		if *cfg.ProxyEnabled && len(cfg.Profiles) == 0 && store != nil {
			if err := store.Update(func(updated *config.Config) error {
				updated.ProxyEnabled = nil
				return nil
			}); err != nil {
				return cfg, err
			}
			cfg.ProxyEnabled = nil
		}
		return cfg, nil
	}
	if len(cfg.Profiles) == 0 {
		return cfg, nil
	}
	enabled := true
	if err := persistProxyPreference(store, enabled); err != nil {
		return cfg, err
	}
	cfg.ProxyEnabled = &enabled
	return cfg, nil
}

func appendCodexModelProfileArgs(cmdArgs []string, launch codexModelProfileLaunch) []string {
	if !launch.Enabled || len(cmdArgs) == 0 {
		return cmdArgs
	}
	overrides := []string{
		`model_provider="` + cxpCodexModelProviderID + `"`,
		`model="` + tomlEscapeString(launch.Model) + `"`,
		`model_providers.` + cxpCodexModelProviderID + `.name="CXP ` + tomlEscapeString(launch.ProviderName) + `"`,
		`model_providers.` + cxpCodexModelProviderID + `.base_url="` + tomlEscapeString(launch.BaseURL) + `"`,
		`model_providers.` + cxpCodexModelProviderID + `.env_key="` + envCXPResponsesProxyKey + `"`,
		`model_providers.` + cxpCodexModelProviderID + `.wire_api="responses"`,
		`model_providers.` + cxpCodexModelProviderID + `.requires_openai_auth=false`,
		`model_providers.` + cxpCodexModelProviderID + `.supports_websockets=false`,
	}
	if strings.TrimSpace(launch.CatalogPath) != "" {
		overrides = append(overrides[:2], append([]string{`model_catalog_json="` + tomlEscapeString(launch.CatalogPath) + `"`}, overrides[2:]...)...)
	}
	insertAt := codexModelProfileConfigInsertIndex(cmdArgs)
	out := make([]string, 0, len(cmdArgs)+2*len(overrides))
	out = append(out, cmdArgs[:insertAt]...)
	for _, override := range overrides {
		out = append(out, "-c", override)
	}
	out = append(out, cmdArgs[insertAt:]...)
	return out
}

func codexModelProfileConfigInsertIndex(cmdArgs []string) int {
	if len(cmdArgs) <= 1 {
		return len(cmdArgs)
	}
	for i := 1; i < len(cmdArgs); i++ {
		if strings.TrimSpace(cmdArgs[i]) != "exec" {
			continue
		}
		if i+1 < len(cmdArgs) && strings.TrimSpace(cmdArgs[i+1]) == "resume" {
			return i + 2
		}
		return i + 1
	}
	return 1
}

func tomlEscapeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `"`, `\"`)
}
