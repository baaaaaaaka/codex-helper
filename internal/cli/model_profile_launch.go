package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

const (
	cxpCodexModelProviderID          = "cxp-thirdparty"
	envCXPResponsesProxyKey          = "CXP_RESPONSES_PROXY_KEY"
	codexWebSearchFallbackAgentName  = "gpt_search"
	codexWebSearchFallbackModel      = "gpt-5.6-luna"
	codexWebSearchFallbackConfigName = "gpt-search.toml"
)

const codexWebSearchFallbackAgentDescription = "Use this agent only for live web research when the current model cannot search the web. Give it the standalone search task without parent conversation context, wait for completion, and use its cited answer."

const codexWebSearchFallbackRootHint = "The current model profile does not support native hosted web search. For any task that requires current or external web information, you MUST delegate only that standalone research task to the gpt_search agent, use no parent turns (fork_turns=none), wait for it to finish, and use its answer with citations. Do not perform the research yourself with web, search, browser, MCP, shell, curl, local documentation, or model memory. This delegation is authorized only as the web-search fallback; do not spawn it for tasks that do not require web search."

const codexWebSearchFallbackInstructions = "You are a read-only web research subagent. Complete the standalone research task delegated by the parent. Use hosted live web search, open relevant pages, and return a clear answer with direct citations. Prefer primary authoritative sources. For current versions, tags, status, prices, dates, or availability, verify the current value from a direct authoritative page or machine-readable endpoint; do not rely on search-result snippets or stale cached page summaries. Answer only the delegated task. If current evidence is unavailable or conflicting, say so. Do not run shell commands or modify files."

const defaultTeamsAppServerModelProfilePrepareTimeout = 30 * time.Second

var teamsAppServerModelProfilePrepareTimeout = defaultTeamsAppServerModelProfilePrepareTimeout

var codexLoginStatusCommand = exec.CommandContext
var codexLoginStatusProbeFn = probeCodexLoginStatus
var loadBundledCodexModelCatalogFn = loadBundledCodexModelCatalog

type codexLoginProbePathContextKey struct{}

func withCodexLoginProbePath(ctx context.Context, path string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, codexLoginProbePathContextKey{}, strings.TrimSpace(path))
}

func codexLoginProbePath(ctx context.Context) string {
	if ctx != nil {
		if path, ok := ctx.Value(codexLoginProbePathContextKey{}).(string); ok && strings.TrimSpace(path) != "" {
			return strings.TrimSpace(path)
		}
	}
	return "codex"
}

func shouldProbeCodexLogin(store *config.Store) bool {
	if store == nil {
		return false
	}
	cfg, err := store.Load()
	return err == nil && modelprofile.HasConfiguredThirdPartyModels(cfg)
}

func probeCodexLoginStatus(ctx context.Context, codexPath string, commandEnv []string, log io.Writer) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(codexPath) == "" {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := codexLoginStatusCommand(probeCtx, codexPath, "login", "status")
	cmd.Env = mergeCLIEnvironment(os.Environ(), commandEnv)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	if err == nil {
		if log != nil {
			_, _ = fmt.Fprintln(log, "Codex official auth probe: logged in")
		}
		return true
	}
	if log != nil {
		status := "not logged in"
		if probeCtx.Err() != nil {
			status = "probe timed out"
		} else if _, ok := err.(*exec.ExitError); !ok {
			status = "probe unavailable"
		}
		_, _ = fmt.Fprintf(log, "Codex official auth probe: %s\n", status)
	}
	return false
}

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
	officialAuth := false
	if shouldProbeCodexLogin(store) {
		officialAuth = codexLoginStatusProbeFn(ctx, cmdArgs[0], opts.ExtraEnv, opts.Log)
	}
	launchCtx := withCodexLoginProbePath(ctx, cmdArgs[0])
	launch, cleanup, err := startModelProfileAdapterForCodex(launchCtx, store, ref, opts.ModelProfileSnapshot, upstreamProxyURL, officialAuth, opts.Log)
	if err != nil {
		return nil, nil, err
	}
	if !launch.Enabled {
		return cmdArgs, cleanup, nil
	}
	if !launch.Native {
		opts.ExtraEnv = withLoopbackNoProxyEnv(append(opts.ExtraEnv, launch.effectiveEnvKey()+"="+launch.ProxyKey))
	}
	return appendCodexModelProfileArgs(cmdArgs, launch), cleanup, nil
}

type codexModelProfileLaunch struct {
	Enabled                bool
	Unified                bool
	Name                   string
	ProviderID             string
	Model                  string
	BaseURL                string
	ProxyKey               string
	Revision               int
	ProviderName           string
	CatalogPath            string
	CatalogJSON            []byte
	WebSearchFallbackPath  string
	WebSearchFallbackTOML  []byte
	Direct                 bool
	Native                 bool
	DisableHostedWebSearch bool
	EnvKey                 string
}

func (l codexModelProfileLaunch) effectiveEnvKey() string {
	if strings.TrimSpace(l.EnvKey) != "" {
		return strings.TrimSpace(l.EnvKey)
	}
	return envCXPResponsesProxyKey
}

func startModelProfileAdapterForCodex(
	ctx context.Context,
	store *config.Store,
	ref string,
	snapshot modelprofile.Snapshot,
	upstreamProxyURL string,
	officialAuth bool,
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
	if officialAuth && modelprofile.HasConfiguredThirdPartyModels(cfg) {
		launch, cleanup, gatewayErr := startConfiguredUnifiedModelGateway(ctx, store, cfg, resolved, upstreamProxyURL, log)
		if gatewayErr != nil && resolved.IsDefault() {
			if model := strings.TrimSpace(resolved.SelectedPublicModel()); model != "" {
				return codexModelProfileLaunch{Enabled: true, Native: true, Model: model, Name: resolved.Name, ProviderID: modelprofile.DefaultProvider}, nil, nil
			}
			if log != nil {
				_, _ = fmt.Fprintf(log, "warning: unified model gateway unavailable; using native Codex provider: %v\n", gatewayErr)
			}
			return codexModelProfileLaunch{}, nil, nil
		}
		return launch, cleanup, gatewayErr
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
	webSearchFallbackPath := ""
	webSearchFallbackTOML := []byte(nil)
	if resolved.Provider.DisableHostedWebSearch {
		webSearchFallbackTOML = codexWebSearchFallbackRoleConfigTOML()
		webSearchFallbackPath, err = writeCodexWebSearchFallbackRoleConfig(catalogPath, webSearchFallbackTOML)
		if err != nil {
			return codexModelProfileLaunch{}, nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	if resolved.Provider.DirectResponses {
		proxyKey, err := ids.New()
		if err != nil {
			return codexModelProfileLaunch{}, nil, err
		}
		baseURL, proxyCleanup, err := startNativeResponsesCompatibilityProxy(resolved.Provider.BaseURL, apiKey, proxyKey, upstreamProxyURL, log)
		if err != nil {
			return codexModelProfileLaunch{}, nil, err
		}
		if log != nil {
			_, _ = fmt.Fprintf(log, "using model profile %q through the native Responses compatibility proxy at %s\n", resolved.Name, baseURL)
		}
		return codexModelProfileLaunch{
			Enabled:                true,
			Direct:                 true,
			DisableHostedWebSearch: resolved.Provider.DisableHostedWebSearch,
			Name:                   resolved.Name,
			ProviderID:             resolved.Provider.ID,
			Model:                  resolved.SelectedPublicModel(),
			BaseURL:                baseURL,
			ProxyKey:               proxyKey,
			Revision:               resolved.Revision(),
			ProviderName:           resolved.Provider.DisplayName,
			CatalogPath:            catalogPath,
			CatalogJSON:            catalogJSON,
			WebSearchFallbackPath:  webSearchFallbackPath,
			WebSearchFallbackTOML:  webSearchFallbackTOML,
			EnvKey:                 envCXPResponsesProxyKey,
		}, proxyCleanup, nil
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
	selectedModel := resolved.Model
	adapter := responsesadapter.OpenAIChatAdapter{
		BaseURL: resolved.Provider.BaseURL,
		APIKey:  apiKey,
		Profile: responsesadapter.ProfileForProvider(resolved.Provider.AdapterProfile).WithReasoningOverrides(
			resolved.Provider.DefaultReasoningEffort,
			resolved.Provider.ReasoningEffortMap,
		).WithModelPolicies(selectedModel.ReasoningPolicy, selectedModel.ToolPolicy, selectedModel.MessagePolicy, selectedModel.SamplingPolicy),
		RetryStatuses:      append([]int(nil), selectedModel.HTTPPolicy.RetryStatuses...),
		MaxOutputTokens:    selectedModel.MaxOutputTokens,
		Headers:            resolved.Provider.Headers,
		AuthType:           resolved.Provider.AuthType,
		AuthHeader:         resolved.Provider.AuthHeader,
		StreamMode:         selectedModel.StreamPolicy.UpstreamMode,
		ReasoningDeltaPath: selectedModel.StreamPolicy.ReasoningDeltaPath,
		CachedTokensPath:   selectedModel.StreamPolicy.CachedTokensPath,
		UsageField:         selectedModel.CachePolicy.UsageField,
	}
	if err := configureOpenAIChatAdapterHTTP(&adapter, selectedModel.HTTPPolicy, selectedModel.StreamPolicy, upstreamProxyURL, log); err != nil {
		_ = ln.Close()
		storeCleanup()
		return codexModelProfileLaunch{}, nil, err
	}
	registry, err := responsesadapter.NewProviderRegistry(responsesadapter.ProviderRegistryOptions{
		DefaultProvider: resolved.Provider.ID,
		Providers: []responsesadapter.ProviderConfig{{
			ID:             resolved.Provider.ID,
			ProfileID:      resolved.Provider.AdapterProfile,
			BaseURL:        resolved.Provider.BaseURL,
			APIKey:         apiKey,
			DefaultModel:   resolved.SelectedPublicModel(),
			Models:         responsesAdapterModelsForProvider(resolved.Provider),
			Adapter:        adapter,
			CustomToolMode: selectedModel.ToolPolicy.CustomToolMode,
		}},
		ProxyKeys: map[string]string{proxyKey: resolved.Provider.ID},
		KeySalt:   resolved.Name + ":" + strconv.Itoa(resolved.Revision()),
	})
	if err != nil {
		_ = ln.Close()
		storeCleanup()
		return codexModelProfileLaunch{}, nil, err
	}
	server := &http.Server{Handler: newCodexModelProfileFacade(registry, responseStore)}
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
		Enabled:               true,
		Name:                  resolved.Name,
		ProviderID:            resolved.Provider.ID,
		Model:                 resolved.SelectedPublicModel(),
		BaseURL:               baseURL,
		ProxyKey:              proxyKey,
		Revision:              resolved.Revision(),
		ProviderName:          resolved.Provider.DisplayName,
		CatalogPath:           catalogPath,
		CatalogJSON:           catalogJSON,
		WebSearchFallbackPath: webSearchFallbackPath,
		WebSearchFallbackTOML: webSearchFallbackTOML,
		EnvKey:                envCXPResponsesProxyKey,
	}, cleanup, nil
}

func configureOpenAIChatAdapterHTTP(adapter *responsesadapter.OpenAIChatAdapter, policy config.ModelHTTPPolicy, stream config.ModelStreamPolicy, upstreamProxyURL string, log io.Writer) error {
	if adapter == nil {
		return fmt.Errorf("chat adapter is nil")
	}
	if policy.MaxRetries != nil {
		adapter.MaxRetries = *policy.MaxRetries
		adapter.MaxRetriesSet = true
	}
	adapter.HonorRetryAfter = policy.HonorRetryAfter
	adapter.RetryTransportErrors = policy.RetryTransportErrors
	adapter.ResponseHeaderTimeout = time.Duration(policy.ResponseHeaderTimeoutSeconds) * time.Second
	adapter.StreamIdleTimeout = time.Duration(stream.IdleTimeoutSeconds) * time.Second
	if policy.MaxConcurrentRequests > 0 {
		adapter.RequestGate = make(chan struct{}, policy.MaxConcurrentRequests)
	}
	if log != nil {
		adapter.Status = func(status string) {
			_, _ = fmt.Fprintf(log, "CXP upstream: %s\n", status)
		}
	}
	proxy := http.ProxyFromEnvironment
	if strings.TrimSpace(upstreamProxyURL) != "" {
		proxyURL, err := url.Parse(upstreamProxyURL)
		if err != nil {
			return fmt.Errorf("parse upstream proxy url: %w", err)
		}
		proxy = http.ProxyURL(proxyURL)
	}
	adapter.HTTPClient = responsesadapter.NewUpstreamHTTPClientWithResponseHeaderTimeout(proxy, adapter.ResponseHeaderTimeout)
	if policy.TimeoutSeconds > 0 {
		adapter.HTTPClient.Timeout = time.Duration(policy.TimeoutSeconds) * time.Second
	}
	return nil
}

func startConfiguredUnifiedModelGateway(
	ctx context.Context,
	store *config.Store,
	cfg config.Config,
	selected modelprofile.Resolved,
	upstreamProxyURL string,
	log io.Writer,
) (codexModelProfileLaunch, func(), error) {
	configured, apiKeys := resolveRoutableConfiguredModels(cfg, store, log)
	if len(configured) == 0 {
		if !selected.IsDefault() {
			return codexModelProfileLaunch{}, nil, fmt.Errorf("selected third-party model profile %q is not currently verified and routable", selected.Name)
		}
		return codexModelProfileLaunch{}, nil, nil
	}
	if !selected.IsDefault() {
		selectedAvailable := false
		for _, candidate := range configured {
			if strings.EqualFold(candidate.Name, selected.Name) {
				selectedAvailable = true
				break
			}
		}
		if !selectedAvailable {
			return codexModelProfileLaunch{}, nil, fmt.Errorf("selected third-party model profile %q is not currently verified and routable", selected.Name)
		}
	}
	var err error
	unifiedProxyProfile, err := unifiedModelUpstreamProxyProfile(cfg, configured, "")
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	if strings.TrimSpace(upstreamProxyURL) == "" && unifiedProxyProfile != nil {
		upstreamProxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *unifiedProxyProfile, cfg.Instances, log)
		if err != nil {
			return codexModelProfileLaunch{}, nil, err
		}
	}
	localKey, err := ids.New()
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	catalogPath := unifiedModelCatalogPath(store, configured)
	catalogProviders := configuredCatalogProviders(configured)
	catalogJSON, catalogSource, catalogErr := readUnifiedCatalogSnapshot(catalogPath)
	if catalogErr == nil && selected.IsDefault() && !unifiedCatalogHasOfficialModel(catalogJSON, catalogProviders) {
		catalogErr = fmt.Errorf("cached unified catalog contains only third-party models")
	}
	if catalogErr != nil {
		catalogJSON, catalogSource, catalogErr = buildInitialUnifiedCatalog(ctx, codexLoginProbePath(ctx), catalogProviders)
		if catalogErr != nil {
			if selected.IsDefault() {
				if model := strings.TrimSpace(selected.SelectedPublicModel()); model != "" {
					return codexModelProfileLaunch{Enabled: true, Native: true, Model: model, Name: selected.Name, ProviderID: modelprofile.DefaultProvider}, nil, nil
				}
				// Never let a third-party-only fallback replace an explicitly
				// selected official default. Bypass the gateway and retain Codex's
				// native official path until its catalog is available again.
				if log != nil {
					_, _ = fmt.Fprintf(log, "warning: unified official catalog unavailable; using native Codex provider: %v\n", catalogErr)
				}
				return codexModelProfileLaunch{}, nil, nil
			}
			catalogJSON, catalogErr = modelprofile.ThirdPartyCodexModelCatalogJSON(catalogProviders)
			catalogSource = "verified third-party configuration"
			if catalogErr != nil {
				return codexModelProfileLaunch{}, nil, catalogErr
			}
		}
		if err := writeAtomicPrivateFile(catalogPath, catalogJSON); err != nil {
			return codexModelProfileLaunch{}, nil, fmt.Errorf("write initial unified model catalog: %w", err)
		}
	}
	if log != nil {
		_, _ = fmt.Fprintf(log, "unified model catalog ready from %s: %s\n", catalogSource, catalogPath)
	}
	baseURL, gatewayCleanup, err := startUnifiedModelGateway(unifiedModelGatewayOptions{
		LocalKey:      localKey,
		CatalogPath:   catalogPath,
		Providers:     configured,
		APIKeys:       apiKeys,
		UpstreamProxy: upstreamProxyURL,
		Log:           log,
	})
	if err != nil {
		return codexModelProfileLaunch{}, nil, err
	}
	cleanup := func() {
		gatewayCleanup()
		_ = os.Remove(filepath.Join(filepath.Dir(catalogPath), codexWebSearchFallbackConfigName))
	}
	launch := codexModelProfileLaunch{
		Enabled:      true,
		Unified:      true,
		Name:         "unified",
		ProviderID:   cxpUnifiedCodexModelProviderID,
		BaseURL:      baseURL,
		ProxyKey:     localKey,
		Revision:     1,
		ProviderName: "Unified official and third-party models",
		CatalogPath:  catalogPath,
		EnvKey:       envCXPUnifiedGatewayKey,
	}
	if !selected.IsDefault() {
		launch.Name = selected.Name
		launch.Model = selected.SelectedPublicModel()
		launch.DisableHostedWebSearch = selected.Provider.DisableHostedWebSearch
		if launch.DisableHostedWebSearch {
			launch.WebSearchFallbackTOML = codexWebSearchFallbackRoleConfigTOML()
			launch.WebSearchFallbackPath, err = writeCodexWebSearchFallbackRoleConfig(catalogPath, launch.WebSearchFallbackTOML)
			if err != nil {
				cleanup()
				return codexModelProfileLaunch{}, nil, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		cleanup()
		return codexModelProfileLaunch{}, nil, err
	}
	return launch, cleanup, nil
}

func unifiedCatalogHasOfficialModel(raw []byte, providers []modelprofile.ProviderSpec) bool {
	thirdParty := map[string]bool{}
	for _, provider := range providers {
		for _, model := range provider.Models {
			if slug := strings.ToLower(strings.TrimSpace(model.PublicID())); slug != "" {
				thirdParty[slug] = true
			}
		}
	}
	var catalog struct {
		Models []struct {
			Slug string `json:"slug"`
		} `json:"models"`
	}
	if json.Unmarshal(raw, &catalog) != nil {
		return false
	}
	for _, model := range catalog.Models {
		slug := strings.ToLower(strings.TrimSpace(model.Slug))
		if slug != "" && !thirdParty[slug] {
			return true
		}
	}
	return false
}

func resolveRoutableConfiguredModels(cfg config.Config, store *config.Store, log io.Writer) ([]modelprofile.Resolved, map[string]string) {
	names := make([]string, 0, len(cfg.ModelProfiles))
	for name, profile := range cfg.ModelProfiles {
		provider := strings.TrimSpace(profile.Provider)
		if provider == "" || strings.EqualFold(provider, modelprofile.DefaultProvider) {
			continue
		}
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return strings.ToLower(names[i]) < strings.ToLower(names[j]) })
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	resolved := make([]modelprofile.Resolved, 0, len(names))
	apiKeys := make(map[string]string, len(names))
	for _, name := range names {
		profile := cfg.ModelProfiles[name]
		if strings.TrimSpace(profile.Source) != "" && !modelProfileVerificationCurrent(cfg, name, profile, secretStore) {
			if log != nil {
				_, _ = fmt.Fprintf(log, "warning: third-party model profile %q is hidden because its verification is missing or stale\n", name)
			}
			continue
		}
		candidate, err := modelprofile.Resolve(cfg, name)
		if err != nil || candidate.IsDefault() || !candidate.Provider.UsesAdapter {
			if log != nil {
				_, _ = fmt.Fprintf(log, "warning: third-party model profile %q is hidden because its configuration is invalid\n", name)
			}
			continue
		}
		apiKey, err := modelprofile.ResolveAPIKey(candidate.Profile.APIKeyRef, secretStore, os.Getenv)
		if err != nil {
			if log != nil {
				_, _ = fmt.Fprintf(log, "warning: third-party model profile %q is hidden because its credential is unavailable\n", name)
			}
			continue
		}
		resolved = append(resolved, candidate)
		apiKeys[candidate.Name] = apiKey
	}
	return resolved, apiKeys
}

func readUnifiedCatalogSnapshot(path string) ([]byte, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	var catalog struct {
		Models []json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return nil, "", err
	}
	if len(catalog.Models) == 0 {
		return nil, "", fmt.Errorf("cached unified model catalog contains no models")
	}
	return raw, "last-known-good cache", nil
}

func unifiedModelCatalogPath(store *config.Store, profiles []modelprofile.Resolved, _ ...string) string {
	parts := make([]string, 0, len(profiles)+1)
	parts = append(parts, "catalog-schema:v2")
	for _, profile := range profiles {
		parts = append(parts, profile.Name+":"+strconv.Itoa(profile.Revision())+":"+modelProfileCatalogFingerprint(profile.Provider))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	dir := filepath.Join(filepath.Dir(store.Path()), "model-profiles", "unified-"+hex.EncodeToString(sum[:8]))
	return filepath.Join(dir, "catalog.json")
}

func configuredCatalogProviders(configured []modelprofile.Resolved) []modelprofile.ProviderSpec {
	providers := make([]modelprofile.ProviderSpec, 0, len(configured))
	for _, resolved := range configured {
		provider := resolved.Provider
		provider.Models = []modelprofile.ModelSpec{resolved.Model}
		provider.DefaultModel = resolved.SelectedPublicModel()
		providers = append(providers, provider)
	}
	return providers
}

func buildInitialUnifiedCatalog(ctx context.Context, codexPath string, providers []modelprofile.ProviderSpec) ([]byte, string, error) {
	official, err := loadBundledCodexModelCatalogFn(ctx, codexPath)
	if err != nil {
		return nil, "", err
	}
	merged, err := modelprofile.MergeCodexModelCatalogJSON(official, providers)
	if err != nil {
		return nil, "", err
	}
	return merged, "Codex bundled official catalog plus verified third-party configuration", nil
}

func loadBundledCodexModelCatalog(ctx context.Context, codexPath string) ([]byte, error) {
	if strings.TrimSpace(codexPath) == "" {
		return nil, fmt.Errorf("Codex executable is empty")
	}
	commandCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := unifiedCatalogCommand(commandCtx, codexPath, "debug", "models", "--bundled")
	raw, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("load bundled Codex model catalog: %w", err)
	}
	var catalog struct {
		Models []json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return nil, fmt.Errorf("decode bundled Codex model catalog: %w", err)
	}
	if len(catalog.Models) == 0 {
		return nil, fmt.Errorf("bundled Codex model catalog contains no models")
	}
	return raw, nil
}

func unifiedCatalogCommand(ctx context.Context, command string, args ...string) *exec.Cmd {
	if runtime.GOOS != "windows" {
		return exec.CommandContext(ctx, command, args...)
	}
	switch strings.ToLower(filepath.Ext(command)) {
	case ".cmd", ".bat":
		return exec.CommandContext(ctx, "cmd.exe", append([]string{"/d", "/s", "/c", command}, args...)...)
	case ".ps1":
		return exec.CommandContext(ctx, "pwsh", append([]string{"-NoProfile", "-File", command}, args...)...)
	default:
		return exec.CommandContext(ctx, command, args...)
	}
}

func newCodexModelProfileFacade(router responsesadapter.ProviderRouter, store responsesadapter.ResponseStore) *responsesadapter.Facade {
	return &responsesadapter.Facade{
		Router:      router,
		Store:       store,
		ShellPolicy: responsespolicy.NewShellEscalationPolicy(0),
	}
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

func writeCodexWebSearchFallbackRoleConfig(catalogPath string, configTOML []byte) (string, error) {
	if strings.TrimSpace(catalogPath) == "" || len(configTOML) == 0 {
		return "", nil
	}
	path := filepath.Join(filepath.Dir(catalogPath), codexWebSearchFallbackConfigName)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, configTOML, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func codexWebSearchFallbackRoleConfigTOML() []byte {
	lines := []string{
		`model_provider = "openai"`,
		`model = "` + codexWebSearchFallbackModel + `"`,
		`model_reasoning_effort = "high"`,
		`web_search = "live"`,
		`sandbox_mode = "read-only"`,
		`approval_policy = "never"`,
		`developer_instructions = "` + tomlEscapeString(codexWebSearchFallbackInstructions) + `"`,
		"",
		`[features.multi_agent_v2]`,
		`enabled = false`,
		"",
		`[tools.web_search]`,
		`context_size = "high"`,
		"",
	}
	return []byte(strings.Join(lines, "\n"))
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

func prepareTeamsAppServerModelProfile(root *rootOptions, ref string, snapshot modelprofile.Snapshot, log io.Writer) ([]string, []string, func(), error) {
	return prepareTeamsAppServerModelProfileWithContext(context.Background(), root, ref, snapshot, log)
}

func prepareTeamsAppServerModelProfileWithContext(ctx context.Context, root *rootOptions, ref string, snapshot modelprofile.Snapshot, log io.Writer) ([]string, []string, func(), error) {
	ref = strings.TrimSpace(ref)
	ctx, cancel := withTeamsAppServerModelProfilePrepareTimeout(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		return nil, nil, nil, err
	}
	cfg, err := store.Load()
	if err != nil {
		return nil, nil, nil, err
	}
	if ref == "" && snapshot.IsZero() && !modelprofile.HasConfiguredThirdPartyModels(cfg) {
		return nil, nil, nil, nil
	}
	var resolved modelprofile.Resolved
	if snapshot.IsZero() {
		resolved, err = modelprofile.Resolve(cfg, ref)
	} else {
		resolved, err = modelprofile.ResolveSnapshot(cfg, snapshot)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if resolved.IsDefault() && !modelprofile.HasConfiguredThirdPartyModels(cfg) {
		return nil, nil, nil, nil
	}
	if resolved.SSHProfile == nil {
		cfg, err = modelProfileConfigWithImplicitProxyPreference(store, cfg)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	upstreamProxyURL := ""
	upstreamProfile, err := modelProfileUpstreamProxyProfile(cfg, resolved, "")
	if err != nil {
		return nil, nil, nil, err
	}
	if upstreamProfile != nil {
		upstreamProxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *upstreamProfile, cfg.Instances, log)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	officialAuth := false
	if shouldProbeCodexLogin(store) {
		officialAuth = codexLoginStatusProbeFn(ctx, codexLoginProbePath(ctx), nil, log)
	}
	launch, cleanup, err := startModelProfileAdapterForCodex(ctx, store, ref, snapshot, upstreamProxyURL, officialAuth, log)
	if err != nil {
		return nil, nil, nil, err
	}
	if !launch.Enabled {
		return nil, nil, cleanup, nil
	}
	args := appendCodexModelProfileArgs([]string{"codex"}, launch)
	if len(args) > 0 {
		args = args[1:]
	}
	env := []string(nil)
	if !launch.Native {
		env = withLoopbackNoProxyEnv([]string{launch.effectiveEnvKey() + "=" + launch.ProxyKey})
	}
	return args, env, cleanup, nil
}

func withLoopbackNoProxyEnv(values []string) []string {
	out := append([]string(nil), values...)
	for _, key := range []string{"NO_PROXY", "no_proxy"} {
		current := ""
		for _, item := range append(os.Environ(), values...) {
			name, value, ok := strings.Cut(item, "=")
			if ok && name == key {
				current = value
			}
		}
		seen := map[string]bool{}
		parts := make([]string, 0, 6)
		for _, value := range strings.Split(current, ",") {
			value = strings.TrimSpace(value)
			if value != "" && !seen[strings.ToLower(value)] {
				seen[strings.ToLower(value)] = true
				parts = append(parts, value)
			}
		}
		for _, value := range []string{"127.0.0.1", "localhost", "::1"} {
			if !seen[strings.ToLower(value)] {
				seen[strings.ToLower(value)] = true
				parts = append(parts, value)
			}
		}
		out = setEnvValue(out, key, strings.Join(parts, ","))
	}
	return out
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
	if launch.Native {
		if strings.TrimSpace(launch.Model) == "" {
			return cmdArgs
		}
		return append(cmdArgs[:1], append([]string{"-c", `model="` + tomlEscapeString(launch.Model) + `"`}, cmdArgs[1:]...)...)
	}
	providerID := cxpCodexModelProviderID
	overrides := make([]string, 0, 12)
	if launch.Unified {
		providerID = cxpUnifiedCodexModelProviderID
		overrides = append(overrides,
			`model_provider="`+providerID+`"`,
			`model_providers.`+providerID+`.name="CXP Unified models"`,
			`model_providers.`+providerID+`.base_url="`+tomlEscapeString(launch.BaseURL)+`"`,
			`model_providers.`+providerID+`.wire_api="responses"`,
			`model_providers.`+providerID+`.requires_openai_auth=true`,
			`model_providers.`+providerID+`.supports_websockets=false`,
			`model_providers.`+providerID+`.env_http_headers={ "`+cxpUnifiedGatewayHeader+`" = "`+launch.effectiveEnvKey()+`" }`,
		)
	} else {
		overrides = append(overrides,
			`model_provider="`+providerID+`"`,
			`model_providers.`+providerID+`.name="CXP `+tomlEscapeString(launch.ProviderName)+`"`,
			`model_providers.`+providerID+`.base_url="`+tomlEscapeString(launch.BaseURL)+`"`,
			`model_providers.`+providerID+`.env_key="`+launch.effectiveEnvKey()+`"`,
			`model_providers.`+providerID+`.wire_api="responses"`,
			`model_providers.`+providerID+`.requires_openai_auth=false`,
			`model_providers.`+providerID+`.supports_websockets=false`,
		)
	}
	if strings.TrimSpace(launch.Model) != "" {
		overrides = append(overrides[:1], append([]string{`model="` + tomlEscapeString(launch.Model) + `"`}, overrides[1:]...)...)
	}
	if launch.DisableHostedWebSearch {
		// Some Responses-compatible providers do not expose OpenAI's hosted
		// web_search tool. Local MCP and skill search tools remain available.
		overrides = append(overrides, `web_search="disabled"`)
		if strings.TrimSpace(launch.WebSearchFallbackPath) != "" {
			overrides = append(overrides,
				`features.multi_agent_v2.hide_spawn_agent_metadata=false`,
				`features.multi_agent_v2.root_agent_usage_hint_text="`+tomlEscapeString(codexWebSearchFallbackRootHint)+`"`,
				`agents.`+codexWebSearchFallbackAgentName+`.description="`+tomlEscapeString(codexWebSearchFallbackAgentDescription)+`"`,
				`agents.`+codexWebSearchFallbackAgentName+`.config_file="`+tomlEscapeString(launch.WebSearchFallbackPath)+`"`,
			)
		}
	}
	catalogReady := strings.TrimSpace(launch.CatalogPath) != ""
	if catalogReady && launch.Unified {
		info, err := os.Stat(launch.CatalogPath)
		catalogReady = err == nil && info.Size() > 0
	}
	if catalogReady {
		insertAt := 1
		if strings.TrimSpace(launch.Model) != "" {
			insertAt = 2
		}
		overrides = append(overrides[:insertAt], append([]string{`model_catalog_json="` + tomlEscapeString(launch.CatalogPath) + `"`}, overrides[insertAt:]...)...)
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
