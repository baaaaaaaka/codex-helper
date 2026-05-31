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

func ensureLongLivedModelProfileAdapterForApp(ctx context.Context, store *config.Store, ref string, log io.Writer) (codexModelProfileLaunch, error) {
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
	if resolved.IsDefault() {
		return codexModelProfileLaunch{}, nil
	}
	apiKey, err := modelprofile.ResolveAPIKey(
		resolved.Profile.APIKeyRef,
		modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path())),
		os.Getenv,
	)
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	listenHost := modelProfileAdapterListenHostForApp()
	instanceProfileID := modelProfileAdapterInstanceProfileID(resolved, apiKey, listenHost)
	if inst := reusableModelProfileAdapterInstance(cfg.Instances, instanceProfileID); inst != nil {
		return modelProfileAdapterLaunchFromInstance(resolved, *inst), nil
	}
	if freshCfg, err := store.Load(); err == nil {
		if inst := reusableModelProfileAdapterInstance(freshCfg.Instances, instanceProfileID); inst != nil {
			return modelProfileAdapterLaunchFromInstance(resolved, *inst), nil
		}
	}
	if log != nil {
		_, _ = fmt.Fprintf(log, "starting a long-lived model adapter for profile %q...\n", resolved.Name)
	}
	instanceID, err := startModelProfileAdapterDaemon(ctx, store, resolved, instanceProfileID, listenHost)
	if err != nil {
		return codexModelProfileLaunch{}, err
	}
	inst, err := waitForModelProfileAdapterInstance(ctx, store, instanceProfileID, instanceID, manager.HealthClient{Timeout: 1 * time.Second})
	if err != nil {
		cleanupModelProfileAdapterStartup(store, instanceID)
		return codexModelProfileLaunch{}, err
	}
	return modelProfileAdapterLaunchFromInstance(resolved, inst), nil
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
	catalogJSON, _ := modelprofile.CodexModelCatalogJSON(resolved.Provider)
	return codexModelProfileLaunch{
		Enabled:      true,
		Name:         resolved.Name,
		ProviderID:   resolved.Provider.ID,
		Model:        resolved.Provider.DefaultPublicModel(),
		BaseURL:      fmt.Sprintf("http://127.0.0.1:%d/v1", inst.HTTPPort),
		ProxyKey:     inst.ModelProxyKey,
		Revision:     resolved.Revision(),
		ProviderName: resolved.Provider.DisplayName,
		CatalogJSON:  catalogJSON,
	}
}

func startModelProfileAdapterDaemon(_ context.Context, store *config.Store, resolved modelprofile.Resolved, instanceProfileID string, listenHost string) (string, error) {
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
		ModelProvider:        snapshot.Provider,
		ModelAPIKeyRef:       snapshot.APIKeyRef,
		ModelSSHProxy:        snapshot.SSHProxy,
		ModelRevision:        snapshot.Revision,
		ModelProxyKey:        proxyKey,
		ModelProfileCaptured: snapshot.CapturedAt,
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
	snapshot := modelProfileSnapshotFromInstance(inst)
	resolved, err := modelprofile.ResolveSnapshot(cfg, snapshot)
	if err != nil {
		return err
	}
	apiKey, err := modelprofile.ResolveAPIKey(
		resolved.Profile.APIKeyRef,
		modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path())),
		os.Getenv,
	)
	if err != nil {
		return err
	}
	upstreamProxyURL := ""
	if resolved.SSHProfile != nil {
		upstreamProxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *resolved.SSHProfile, cfg.Instances, os.Stderr)
		if err != nil {
			return err
		}
	}
	listenHost := strings.TrimSpace(os.Getenv(envCXPModelProfileAdapterListenHost))
	if listenHost == "" {
		listenHost = "127.0.0.1"
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

	facade, cleanup, err := modelProfileAdapterFacade(resolved, apiKey, inst.ModelProxyKey, upstreamProxyURL, instanceID)
	if err != nil {
		return err
	}
	defer cleanup()

	server := &http.Server{Handler: facade}
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
	adapter := responsesadapter.OpenAIChatAdapter{
		BaseURL: resolved.Provider.BaseURL,
		APIKey:  apiKey,
		Profile: responsesadapter.ProfileForProvider(resolved.Provider.AdapterProfile),
	}
	if strings.TrimSpace(upstreamProxyURL) != "" {
		proxyURL, err := url.Parse(upstreamProxyURL)
		if err != nil {
			storeCleanup()
			return nil, nil, fmt.Errorf("parse upstream proxy url: %w", err)
		}
		adapter.HTTPClient = &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	}
	registry, err := responsesadapter.NewProviderRegistry(responsesadapter.ProviderRegistryOptions{
		DefaultProvider: resolved.Provider.ID,
		Providers: []responsesadapter.ProviderConfig{{
			ID:           resolved.Provider.ID,
			ProfileID:    resolved.Provider.AdapterProfile,
			BaseURL:      resolved.Provider.BaseURL,
			APIKey:       apiKey,
			DefaultModel: resolved.Provider.DefaultPublicModel(),
			Models:       responsesAdapterModelsForProvider(resolved.Provider),
			Adapter:      adapter,
		}},
		ProxyKeys: map[string]string{proxyKey: resolved.Provider.ID},
		KeySalt:   resolved.Name + ":" + strconv.Itoa(resolved.Revision()),
	})
	if err != nil {
		storeCleanup()
		return nil, nil, err
	}
	return &responsesadapter.Facade{Router: registry, Store: responseStore, InstanceID: instanceID}, storeCleanup, nil
}

func modelProfileAdapterListenHostForApp() string {
	if codexAppGOOS() == "linux" && codexAppIsWSL() {
		return "0.0.0.0"
	}
	return "127.0.0.1"
}

func modelProfileAdapterInstanceProfileID(resolved modelprofile.Resolved, apiKey string, listenHost string) string {
	material := strings.Join([]string{
		resolved.Name,
		resolved.Provider.ID,
		resolved.Provider.BaseURL,
		resolved.Provider.DefaultPublicModel(),
		resolved.Provider.AdapterProfile,
		modelProfileCatalogFingerprint(resolved.Provider),
		resolved.Profile.APIKeyRef,
		resolved.Profile.SSHProxy,
		strconv.Itoa(resolved.Revision()),
		responsesadapter.KeyFingerprint(apiKey, "cxp-model-profile-adapter-instance-v1"),
		strings.TrimSpace(listenHost),
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return modelProfileAdapterInstancePrefix + hex.EncodeToString(sum[:])[:32]
}

func modelProfileSnapshotFromInstance(inst config.Instance) modelprofile.Snapshot {
	return modelprofile.Snapshot{
		Name:       inst.ModelProfileName,
		Provider:   inst.ModelProvider,
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
