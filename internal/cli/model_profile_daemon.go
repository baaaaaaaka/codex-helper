package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
)

const modelProfileAdapterInstancePrefix = "model-adapter:"

const envCXPModelProfileAdapterListenHost = "CXP_MODEL_PROFILE_ADAPTER_LISTEN_HOST"

var (
	codexAppEnsureModelProfileLaunchFn = ensureLongLivedModelProfileAdapterForApp
	modelProfileAdapterReadyTimeout    = 15 * time.Second
	modelProfileAdapterPollInterval    = 200 * time.Millisecond
)

func ensureLongLivedModelProfileAdapterForApp(ctx context.Context, store *config.Store, ref string, proxyRef string, log io.Writer) (codexModelProfileLaunch, error) {
	if store == nil {
		return codexModelProfileLaunch{}, nil
	}
	cfg, err := store.Load()
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	resolved, err := modelprofile.Resolve(cfg, ref)
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	usingGlobalDefault := strings.TrimSpace(ref) == ""
	globalEffort := ""
	if usingGlobalDefault {
		globalEffort = cfg.ExplicitDefaultReasoningEffort()
	}
	applyGlobalDefaults := func(launch codexModelProfileLaunch) codexModelProfileLaunch {
		launch.ReasoningEffort = globalEffort
		return launch
	}
	nativeLaunch := func() codexModelProfileLaunch {
		model := strings.TrimSpace(resolved.Profile.Model)
		if model == "" && globalEffort == "" {
			return codexModelProfileLaunch{}
		}
		return applyGlobalDefaults(codexModelProfileLaunch{
			Enabled: true, Native: true, Name: resolved.Name, ProviderID: modelprofile.DefaultProvider, Model: model,
		})
	}
	if modelprofile.HasConfiguredThirdPartyModels(cfg) {
		launch, gatewayErr := ensureLongLivedUnifiedModelGatewayForApp(ctx, store, cfg, resolved, proxyRef, log)
		if gatewayErr != nil && resolved.IsDefault() {
			if log != nil {
				_, _ = fmt.Fprintf(log, "warning: unified App model gateway unavailable; using native Codex provider: %v\n", gatewayErr)
			}
			return nativeLaunch(), nil
		}
		if gatewayErr == nil && resolved.IsDefault() && !launch.Enabled {
			return nativeLaunch(), nil
		}
		return applyGlobalDefaults(launch), gatewayErr
	}
	if resolved.IsDefault() {
		return nativeLaunch(), nil
	}
	apiKey, err := modelprofile.ResolveAPIKey(
		resolved.Profile.APIKeyRef,
		modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path())),
		os.Getenv,
	)
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	upstreamProfile, err := modelProfileUpstreamProxyProfile(cfg, resolved, proxyRef)
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	listenHost := modelProfileAdapterListenHostForApp()
	instanceProfileID := modelProfileAdapterInstanceProfileID(resolved, apiKey, listenHost, modelprofile.SSHProxyFingerprint(upstreamProfile))
	if inst := reusableModelProfileAdapterInstance(cfg.Instances, instanceProfileID); inst != nil {
		return applyGlobalDefaults(modelProfileAdapterLaunchFromInstance(resolved, *inst)), nil
	}
	if freshCfg, err := store.Load(); err == nil {
		if inst := reusableModelProfileAdapterInstance(freshCfg.Instances, instanceProfileID); inst != nil {
			return applyGlobalDefaults(modelProfileAdapterLaunchFromInstance(resolved, *inst)), nil
		}
	}
	if log != nil {
		_, _ = fmt.Fprintf(log, "starting a long-lived model adapter for profile %q...\n", resolved.Name)
	}
	instanceID, err := startModelProfileAdapterDaemon(ctx, store, resolved, instanceProfileID, listenHost, upstreamProfile, false)
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	inst, err := waitForModelProfileAdapterInstance(ctx, store, instanceProfileID, instanceID, manager.HealthClient{Timeout: 1 * time.Second})
	if err != nil {
		cleanupModelProfileAdapterStartup(store, instanceID)
		return codexModelProfileLaunch{}, err
	}
	return applyGlobalDefaults(modelProfileAdapterLaunchFromInstance(resolved, inst)), nil
}

func ensureLongLivedUnifiedModelGatewayForApp(
	ctx context.Context,
	store *config.Store,
	cfg config.Config,
	selected modelprofile.Resolved,
	proxyRef string,
	log io.Writer,
) (codexModelProfileLaunch, error) {
	configured, apiKeys := resolveRoutableConfiguredModels(cfg, store, log)
	if len(configured) == 0 {
		if selected.IsDefault() {
			return codexModelProfileLaunch{}, nil
		}
		return codexModelProfileLaunch{}, fmt.Errorf("selected third-party model profile %q is not currently verified and routable", selected.Name)
	}
	upstreamProfile, err := unifiedModelUpstreamProxyProfile(cfg, configured, proxyRef)
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	listenHost := modelProfileAdapterListenHostForApp()
	instanceProfileID := unifiedModelProfileAdapterInstanceProfileID(configured, apiKeys, listenHost, modelprofile.SSHProxyFingerprint(upstreamProfile))
	if inst := reusableModelProfileAdapterInstance(cfg.Instances, instanceProfileID); inst != nil {
		return modelProfileAdapterLaunchFromInstance(selected, *inst), nil
	}
	if freshCfg, err := store.Load(); err == nil {
		if inst := reusableModelProfileAdapterInstance(freshCfg.Instances, instanceProfileID); inst != nil {
			return modelProfileAdapterLaunchFromInstance(selected, *inst), nil
		}
	}
	if log != nil {
		_, _ = fmt.Fprintf(log, "starting a long-lived unified model gateway for %d third-party profile(s)...\n", len(configured))
	}
	instanceID, err := startModelProfileAdapterDaemon(ctx, store, selected, instanceProfileID, listenHost, upstreamProfile, true)
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	inst, err := waitForModelProfileAdapterInstance(ctx, store, instanceProfileID, instanceID, manager.HealthClient{Timeout: time.Second})
	if err != nil {
		cleanupModelProfileAdapterStartup(store, instanceID)
		return codexModelProfileLaunch{}, err
	}
	return modelProfileAdapterLaunchFromInstance(selected, inst), nil
}

func reusableModelProfileAdapterInstance(instances []config.Instance, instanceProfileID string) *config.Instance {
	return findReusableModelProfileAdapterInstance(instances, instanceProfileID, manager.HealthClient{Timeout: 500 * time.Millisecond})
}

func findReusableModelProfileAdapterInstance(instances []config.Instance, instanceProfileID string, hc manager.HealthClient) *config.Instance {
	var best *config.Instance
	for i := range instances {
		inst := &instances[i]
		if inst.ProfileID != instanceProfileID || inst.Kind != config.InstanceKindModelAdapter {
			continue
		}
		if strings.TrimSpace(inst.ModelProxyKey) == "" {
			continue
		}
		if inst.DaemonPID <= 0 || !proxyProcessAlive(inst.DaemonPID) {
			continue
		}
		if err := hc.CheckHTTPProxy(inst.HTTPPort, inst.ID); err != nil {
			continue
		}
		if best == nil || inst.LastSeenAt.After(best.LastSeenAt) || best.LastSeenAt.IsZero() {
			copy := *inst
			best = &copy
		}
	}
	return best
}

func modelProfileAdapterLaunchFromInstance(resolved modelprofile.Resolved, inst config.Instance) codexModelProfileLaunch {
	if inst.ModelUnified {
		launch := codexModelProfileLaunch{
			Enabled:      true,
			Unified:      true,
			Name:         "unified",
			ProviderID:   cxpUnifiedCodexModelProviderID,
			BaseURL:      fmt.Sprintf("http://127.0.0.1:%d/v1", inst.HTTPPort),
			ProxyKey:     inst.ModelProxyKey,
			Revision:     1,
			ProviderName: "Unified official and third-party models",
			EnvKey:       envCXPUnifiedGatewayKey,
		}
		if model := strings.TrimSpace(resolved.SelectedPublicModel()); model != "" {
			launch.Model = model
		}
		if !resolved.IsDefault() {
			launch.Name = resolved.Name
			launch.DisableHostedWebSearch = resolved.Provider.DisableHostedWebSearch
			if launch.DisableHostedWebSearch {
				launch.WebSearchFallbackTOML = codexWebSearchFallbackRoleConfigTOML(resolved.Provider.SearchFallback)
			}
		}
		return launch
	}
	catalogJSON, _ := modelprofile.CodexModelCatalogJSON(resolved.Provider)
	webSearchFallbackTOML := []byte(nil)
	if resolved.Provider.DisableHostedWebSearch {
		webSearchFallbackTOML = codexWebSearchFallbackRoleConfigTOML(resolved.Provider.SearchFallback)
	}
	return codexModelProfileLaunch{
		Enabled:                true,
		Name:                   resolved.Name,
		ProviderID:             resolved.Provider.ID,
		Model:                  resolved.SelectedPublicModel(),
		BaseURL:                fmt.Sprintf("http://127.0.0.1:%d/v1", inst.HTTPPort),
		ProxyKey:               inst.ModelProxyKey,
		Revision:               resolved.Revision(),
		ProviderName:           resolved.Provider.DisplayName,
		CatalogJSON:            catalogJSON,
		WebSearchFallbackTOML:  webSearchFallbackTOML,
		Direct:                 resolved.Provider.DirectResponses,
		DisableHostedWebSearch: resolved.Provider.DisableHostedWebSearch,
	}
}

func startModelProfileAdapterDaemon(_ context.Context, store *config.Store, resolved modelprofile.Resolved, instanceProfileID string, listenHost string, upstreamProfile *config.Profile, unified bool) (string, error) {
	instanceID, err := ids.New()
	if err != nil {
		return "", err
	}
	proxyKey, err := ids.New()
	if err != nil {
		return "", err
	}
	now := proxyNow()
	snapshot := resolved.Snapshot(now)
	inst := config.Instance{
		ID:                   instanceID,
		ProfileID:            instanceProfileID,
		Kind:                 config.InstanceKindModelAdapter,
		HTTPPort:             0,
		SocksPort:            0,
		DaemonPID:            0,
		StartedAt:            now,
		LastSeenAt:           now,
		ModelProfileName:     snapshot.Name,
		ModelUnified:         unified,
		ModelProvider:        snapshot.Provider,
		ModelPublicModel:     snapshot.Model,
		ModelBaseURL:         snapshot.BaseURL,
		ModelAPIKeyRef:       snapshot.APIKeyRef,
		ModelSSHProxy:        snapshot.SSHProxy,
		ModelRevision:        snapshot.Revision,
		ModelProxyKey:        proxyKey,
		ModelProfileCaptured: snapshot.CapturedAt,
	}
	if upstreamProfile != nil {
		inst.ModelUpstreamProxyID = upstreamProfile.ID
	}
	if err := proxyRecordInstance(store, inst); err != nil {
		return "", err
	}
	started := false
	defer func() {
		if !started {
			cleanupModelProfileAdapterStartup(store, instanceID)
		}
	}()

	exe, err := proxyExecutable()
	if err != nil {
		return "", err
	}
	resolvedExe, err := helperpath.StableRunnablePathFromSources(exe, restartArgv0(), helperpath.Options{})
	if err != nil {
		return "", err
	}
	exe = resolvedExe.Path

	args := []string{"--config", store.Path(), "responses", "serve", "--model-profile-instance-id", instanceID}
	c := proxyCommand(exe, args...)
	c.Stdin = nil
	if strings.TrimSpace(listenHost) != "" {
		c.Env = append(os.Environ(), envCXPModelProfileAdapterListenHost+"="+strings.TrimSpace(listenHost))
	}

	logPath := filepath.Join(filepath.Dir(store.Path()), "instances", instanceID+".log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return "", err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	defer logFile.Close()
	c.Stdout = logFile
	c.Stderr = logFile
	configureTeamsServiceDetachedCommand(c)

	if err := c.Start(); err != nil {
		return "", err
	}
	started = true
	pid := c.Process.Pid
	_ = c.Process.Release()
	_ = store.Update(func(cfg *config.Config) error {
		for i := range cfg.Instances {
			if cfg.Instances[i].ID == instanceID {
				cfg.Instances[i].DaemonPID = pid
				cfg.Instances[i].LastSeenAt = proxyNow()
				return nil
			}
		}
		return nil
	})
	return instanceID, nil
}

func waitForModelProfileAdapterInstance(ctx context.Context, store *config.Store, instanceProfileID string, instanceID string, hc manager.HealthClient) (config.Instance, error) {
	deadline := time.NewTimer(modelProfileAdapterReadyTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(modelProfileAdapterPollInterval)
	defer ticker.Stop()

	for {
		cfg, err := store.Load()
		if err == nil {
			if inst := findReusableModelProfileAdapterInstance(cfg.Instances, instanceProfileID, hc); inst != nil {
				return *inst, nil
			}
		}
		select {
		case <-ctx.Done():
			return config.Instance{}, ctx.Err()
		case <-deadline.C:
			return config.Instance{}, fmt.Errorf("model adapter instance %s did not become ready within %s", instanceID, modelProfileAdapterReadyTimeout)
		case <-ticker.C:
		}
	}
}

func cleanupModelProfileAdapterStartup(store *config.Store, instanceID string) {
	if cfg, err := store.Load(); err == nil {
		for _, inst := range cfg.Instances {
			if inst.ID == instanceID {
				_ = stopProxyInstances([]config.Instance{inst})
				break
			}
		}
	}
	_ = proxyRemoveInstance(store, instanceID)
}

func runModelProfileAdapterDaemon(parentCtx context.Context, store *config.Store, instanceID string) error {
	ctx, stop := signal.NotifyContext(parentCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := store.Load()
	if err != nil {
		return err
	}
	inst, ok := findInstanceByID(cfg.Instances, instanceID)
	if !ok {
		return fmt.Errorf("instance %q not found in config", instanceID)
	}
	if inst.Kind != config.InstanceKindModelAdapter {
		return fmt.Errorf("instance %q is %q, not %q", instanceID, inst.Kind, config.InstanceKindModelAdapter)
	}
	listenHost := strings.TrimSpace(os.Getenv(envCXPModelProfileAdapterListenHost))
	if listenHost == "" {
		listenHost = "127.0.0.1"
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	var resolved modelprofile.Resolved
	var apiKey string
	var configured []modelprofile.Resolved
	apiKeys := map[string]string{}
	var upstreamProfile *config.Profile
	if inst.ModelUnified {
		configured, apiKeys = resolveRoutableConfiguredModels(cfg, store, os.Stderr)
		if len(configured) == 0 {
			return fmt.Errorf("unified model gateway has no currently verified and routable third-party profiles")
		}
		upstreamProfile, err = unifiedModelUpstreamProxyProfile(cfg, configured, inst.ModelUpstreamProxyID)
		if err != nil {
			return err
		}
		expected := unifiedModelProfileAdapterInstanceProfileID(configured, apiKeys, listenHost, modelprofile.SSHProxyFingerprint(upstreamProfile))
		if inst.ProfileID != expected {
			return fmt.Errorf("unified model gateway instance profile changed since it was recorded")
		}
	} else {
		snapshot := modelProfileSnapshotFromInstance(inst)
		resolved, err = modelprofile.ResolveSnapshot(cfg, snapshot)
		if err != nil {
			return err
		}
		apiKey, err = modelprofile.ResolveAPIKey(resolved.Profile.APIKeyRef, secretStore, os.Getenv)
		if err != nil {
			return err
		}
		upstreamProfile, err = modelProfileUpstreamProxyProfile(cfg, resolved, inst.ModelUpstreamProxyID)
		if err != nil {
			return err
		}
		if expectedProfileID := modelProfileAdapterInstanceProfileID(resolved, apiKey, listenHost, modelprofile.SSHProxyFingerprint(upstreamProfile)); inst.ProfileID != expectedProfileID {
			return fmt.Errorf("model adapter instance profile changed since it was recorded")
		}
	}
	upstreamProxyURL := ""
	if upstreamProfile != nil {
		upstreamProxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *upstreamProfile, cfg.Instances, os.Stderr)
		if err != nil {
			return err
		}
	}
	listenAddr := net.JoinHostPort(listenHost, "0")
	if inst.HTTPPort > 0 {
		listenAddr = net.JoinHostPort(listenHost, strconv.Itoa(inst.HTTPPort))
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	var handler http.Handler
	var cleanup func()
	if inst.ModelUnified {
		handler, cleanup, err = newUnifiedModelGateway(unifiedModelGatewayOptions{
			LocalKey:      inst.ModelProxyKey,
			CatalogPath:   unifiedModelCatalogPath(store, configured),
			Providers:     configured,
			APIKeys:       apiKeys,
			UpstreamProxy: upstreamProxyURL,
			Log:           os.Stderr,
			InstanceID:    instanceID,
		})
	} else if resolved.Provider.DirectResponses {
		handler, cleanup, err = newNativeResponsesCompatibilityProxy(resolved.Provider.BaseURL, apiKey, inst.ModelProxyKey, upstreamProxyURL, os.Stderr)
		if err == nil {
			policy := responsesAdapterSourcePolicy(resolved.Model)
			if policyErr := responsesadapter.ValidateDirectSourcePolicy(policy); policyErr != nil {
				cleanup()
				handler = nil
				cleanup = nil
				err = policyErr
			} else {
				if direct, ok := handler.(*nativeResponsesCompatibilityProxy); ok {
					direct.sourcePolicy = policy
					direct.responsesPolicy = responsesAdapterResponsesPolicy(resolved.Model)
					direct.unsupportedToolPolicy = resolved.Model.UnsupportedToolPolicy
					direct.nativeTools = append([]responsesadapter.NativeToolSpec(nil), responsesAdapterNativeTools(resolved.Model)...)
				}
			}
		}
	} else {
		handler, cleanup, err = modelProfileAdapterFacade(cfg, store, resolved, apiKey, inst.ModelProxyKey, upstreamProxyURL, instanceID)
	}
	if err != nil {
		return err
	}
	defer cleanup()

	server := &http.Server{Handler: handler}
	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	now := proxyNow()
	inst.DaemonPID = os.Getpid()
	inst.Kind = config.InstanceKindModelAdapter
	inst.HTTPPort = ln.Addr().(*net.TCPAddr).Port
	if inst.StartedAt.IsZero() {
		inst.StartedAt = now
	}
	inst.LastSeenAt = now
	_ = proxyRecordInstance(store, inst)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-errCh:
			_ = proxyRemoveInstance(store, instanceID)
			return err
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := server.Shutdown(shutdownCtx)
			cancel()
			<-errCh
			_ = proxyRemoveInstance(store, instanceID)
			return err
		case <-ticker.C:
			_ = proxyHeartbeat(store, instanceID, proxyNow())
		}
	}
}

func modelProfileAdapterFacade(
	cfg config.Config,
	store *config.Store,
	resolved modelprofile.Resolved,
	apiKey string,
	proxyKey string,
	upstreamProxyURL string,
	instanceID string,
) (*responsesadapter.Facade, func(), error) {
	storePath := defaultResponsesStorePath()
	responseStore, storeCleanup, err := responsesStoreFromOptions(responsesServeOptions{storePath: storePath})
	if err != nil {
		return nil, nil, err
	}
	adapter, err := newResolvedProviderAdapter(resolved, apiKey, upstreamProxyURL, nil)
	if err != nil {
		storeCleanup()
		return nil, nil, err
	}
	interfaceAPIKeys := resolveConfiguredInterfaceAPIKeys(cfg, []modelprofile.Resolved{resolved}, map[string]string{resolved.Name: apiKey}, store)[resolved.Name]
	routeConfigs, err := resolvedProviderRouteConfigs(resolved, apiKey, interfaceAPIKeys, adapter, upstreamProxyURL, nil)
	if err != nil {
		storeCleanup()
		return nil, nil, err
	}
	registry, err := responsesadapter.NewProviderRegistry(responsesadapter.ProviderRegistryOptions{
		DefaultProvider: resolved.Provider.ID,
		Providers: []responsesadapter.ProviderConfig{{
			ID:                    resolved.Provider.ID,
			ProfileID:             resolved.Provider.AdapterProfile,
			BaseURL:               resolved.Provider.BaseURL,
			APIKey:                apiKey,
			DefaultModel:          resolved.SelectedPublicModel(),
			Models:                responsesAdapterModelsForProvider(resolved.Provider),
			Adapter:               adapter,
			CustomToolMode:        resolved.Model.ToolPolicy.CustomToolMode,
			UnsupportedToolPolicy: resolved.Model.UnsupportedToolPolicy,
			ConversionProfile:     resolved.Provider.ConversionProfile,
			StrictConversion:      resolved.Provider.StrictConversion,
			Operation:             resolved.Provider.Operation,
			NativeTools:           responsesAdapterNativeTools(resolved.Model),
			SourcePolicy:          responsesAdapterSourcePolicy(resolved.Model),
			ResponsesPolicy:       responsesAdapterResponsesPolicy(resolved.Model),
			Routes:                routeConfigs,
		}},
		ProxyKeys: map[string]string{proxyKey: resolved.Provider.ID},
		KeySalt:   resolved.Name + ":" + strconv.Itoa(resolved.Revision()),
	})
	if err != nil {
		storeCleanup()
		return nil, nil, err
	}
	facade := newCodexModelProfileFacade(registry, responseStore)
	facade.InstanceID = instanceID
	return facade, storeCleanup, nil
}

func modelProfileAdapterListenHostForApp() string {
	if codexAppGOOS() == "linux" && codexAppIsWSL() {
		return "0.0.0.0"
	}
	return "127.0.0.1"
}

func modelProfileAdapterInstanceProfileID(resolved modelprofile.Resolved, apiKey string, listenHost string, upstreamProxyFingerprint string) string {
	parts := []string{
		resolved.Name,
		resolved.Provider.ID,
		resolved.Provider.BaseURL,
		resolved.SelectedPublicModel(),
		resolved.Provider.AdapterProfile,
		modelProfileCatalogFingerprint(resolved.Provider),
		resolved.Profile.APIKeyRef,
		resolved.Profile.SSHProxy,
		strconv.Itoa(resolved.Revision()),
		responsesadapter.KeyFingerprint(apiKey, "cxp-model-profile-adapter-instance-v1"),
		strings.TrimSpace(listenHost),
	}
	if upstreamProxyFingerprint = strings.TrimSpace(upstreamProxyFingerprint); upstreamProxyFingerprint != "" {
		parts = append(parts, "upstream-proxy:"+upstreamProxyFingerprint)
	}
	material := strings.Join(parts, "\n")
	sum := sha256.Sum256([]byte(material))
	return modelProfileAdapterInstancePrefix + hex.EncodeToString(sum[:])[:32]
}

func unifiedModelProfileAdapterInstanceProfileID(profiles []modelprofile.Resolved, apiKeys map[string]string, listenHost string, upstreamProxyFingerprint string) string {
	parts := []string{"unified-v1", strings.TrimSpace(listenHost)}
	for _, profile := range profiles {
		parts = append(parts,
			profile.Name,
			profile.Provider.ID,
			profile.Provider.BaseURL,
			profile.SelectedPublicModel(),
			modelProfileCatalogFingerprint(profile.Provider),
			strconv.Itoa(profile.Revision()),
			responsesadapter.KeyFingerprint(apiKeys[profile.Name], "cxp-unified-model-gateway-instance-v1"),
		)
	}
	if upstreamProxyFingerprint = strings.TrimSpace(upstreamProxyFingerprint); upstreamProxyFingerprint != "" {
		parts = append(parts, "upstream-proxy:"+upstreamProxyFingerprint)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return modelProfileAdapterInstancePrefix + hex.EncodeToString(sum[:])[:32]
}

func unifiedModelUpstreamProxyProfile(cfg config.Config, profiles []modelprofile.Resolved, fallbackProxyRef string) (*config.Profile, error) {
	var selected *config.Profile
	for _, resolved := range profiles {
		profile, err := modelProfileUpstreamProxyProfile(cfg, resolved, fallbackProxyRef)
		if err != nil {
			return nil, err
		}
		if profile == nil {
			continue
		}
		if selected != nil && selected.ID != profile.ID {
			return nil, fmt.Errorf("configured third-party model profiles require different SSH proxies (%q and %q); a unified gateway currently requires one upstream proxy", selected.ID, profile.ID)
		}
		copy := *profile
		selected = &copy
	}
	return selected, nil
}

func modelProfileUpstreamProxyProfile(cfg config.Config, resolved modelprofile.Resolved, fallbackProxyRef string) (*config.Profile, error) {
	if resolved.SSHProfile != nil {
		profile := *resolved.SSHProfile
		return &profile, nil
	}
	if fallbackProxyRef = strings.TrimSpace(fallbackProxyRef); fallbackProxyRef != "" {
		profile, err := selectProfile(cfg, fallbackProxyRef)
		if err != nil {
			return nil, err
		}
		return &profile, nil
	}
	if cfg.ProxyEnabled == nil || !*cfg.ProxyEnabled {
		return nil, nil
	}
	profile, err := selectProfile(cfg, "")
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

func modelProfileSnapshotFromInstance(inst config.Instance) modelprofile.Snapshot {
	return modelprofile.Snapshot{
		Name:       inst.ModelProfileName,
		Provider:   inst.ModelProvider,
		Model:      inst.ModelPublicModel,
		BaseURL:    inst.ModelBaseURL,
		APIKeyRef:  inst.ModelAPIKeyRef,
		SSHProxy:   inst.ModelSSHProxy,
		Revision:   inst.ModelRevision,
		CapturedAt: inst.ModelProfileCaptured,
	}
}

func findInstanceByID(instances []config.Instance, id string) (config.Instance, bool) {
	for _, inst := range instances {
		if inst.ID == id {
			return inst, true
		}
	}
	return config.Instance{}, false
}
