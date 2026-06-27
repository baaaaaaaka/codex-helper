package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
)

type responsesServeOptions struct {
	listen                 string
	baseURL                string
	apiKey                 string
	apiKeyEnv              string
	model                  string
	provider               string
	storePath              string
	providersJSON          string
	proxyKeys              string
	scopeSalt              string
	modelProfileInstanceID string
}

func newResponsesCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "responses",
		Short: "Run a local Responses-compatible adapter",
	}
	cmd.AddCommand(newResponsesServeCmd(root))
	return cmd
}

func newResponsesServeCmd(root *rootOptions) *cobra.Command {
	_ = root
	opts := responsesServeOptions{
		listen:    "127.0.0.1:8787",
		apiKeyEnv: "OPENAI_API_KEY",
		provider:  "openai-chat",
		storePath: defaultResponsesStorePath(),
	}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve /v1/responses backed by an OpenAI-compatible chat upstream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(opts.modelProfileInstanceID) != "" {
				store, _, err := newRootStore(root, "")
				if err != nil {
					return err
				}
				return runModelProfileAdapterDaemon(cmd.Context(), store, opts.modelProfileInstanceID)
			}
			return runResponsesServe(cmd.Context(), cmd.OutOrStdout(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.listen, "listen", opts.listen, "Listen address for the local Responses adapter")
	cmd.Flags().StringVar(&opts.baseURL, "base-url", "", "OpenAI-compatible upstream base URL, for example https://api.deepseek.com/v1")
	cmd.Flags().StringVar(&opts.apiKey, "api-key", "", "Upstream API key; prefer --api-key-env for normal use")
	cmd.Flags().StringVar(&opts.apiKeyEnv, "api-key-env", opts.apiKeyEnv, "Environment variable containing the upstream API key")
	cmd.Flags().StringVar(&opts.model, "model", "", "Default model to advertise when Codex omits model")
	cmd.Flags().StringVar(&opts.provider, "provider", opts.provider, "Provider id used for response-store scoping")
	cmd.Flags().StringVar(&opts.storePath, "store-path", opts.storePath, "SQLite response store path; set empty to use in-memory store")
	cmd.Flags().StringVar(&opts.providersJSON, "providers-json", "", "JSON file declaring multiple upstream providers")
	cmd.Flags().StringVar(&opts.proxyKeys, "proxy-keys", "", "Comma-separated inbound key locks, e.g. key1:deepseek,key2:mimo,key3:*")
	cmd.Flags().StringVar(&opts.scopeSalt, "scope-salt", "", "Salt used to fingerprint upstream API keys for response-store scoping")
	cmd.Flags().StringVar(&opts.modelProfileInstanceID, "model-profile-instance-id", "", "Internal model profile adapter instance id")
	_ = cmd.Flags().MarkHidden("model-profile-instance-id")
	return cmd
}

func runResponsesServe(ctx context.Context, out io.Writer, opts responsesServeOptions) error {
	if strings.TrimSpace(opts.modelProfileInstanceID) != "" {
		return fmt.Errorf("--model-profile-instance-id must be run through the root command")
	}
	apiKey := strings.TrimSpace(opts.apiKey)
	if apiKey == "" && strings.TrimSpace(opts.apiKeyEnv) != "" {
		apiKey = os.Getenv(strings.TrimSpace(opts.apiKeyEnv))
	}
	ln, err := net.Listen("tcp", opts.listen)
	if err != nil {
		return err
	}
	defer ln.Close()

	store, cleanup, err := responsesStoreFromOptions(opts)
	if err != nil {
		return err
	}
	defer cleanup()

	facade, err := responsesFacadeFromOptions(opts, apiKey, store)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: facade}
	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	_, _ = fmt.Fprintf(out, "Responses adapter listening on http://%s/v1\n", ln.Addr().String())
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}

func responsesStoreFromOptions(opts responsesServeOptions) (responsesadapter.ResponseStore, func(), error) {
	if strings.TrimSpace(opts.storePath) == "" {
		return responsesadapter.NewMemoryStore(), func() {}, nil
	}
	store, err := responsesadapter.NewSQLiteStore(opts.storePath, responsesadapter.SQLiteStoreOptions{
		TTL:        7 * 24 * time.Hour,
		MaxRecords: 10000,
	})
	if err != nil {
		return nil, nil, err
	}
	return store, func() { _ = store.Close() }, nil
}

func responsesFacadeFromOptions(opts responsesServeOptions, apiKey string, store responsesadapter.ResponseStore) (*responsesadapter.Facade, error) {
	if strings.TrimSpace(opts.providersJSON) != "" {
		registry, err := responsesRegistryFromFile(opts.providersJSON, opts.proxyKeys, opts.scopeSalt)
		if err != nil {
			return nil, err
		}
		return &responsesadapter.Facade{
			Router: registry,
			Store:  store,
		}, nil
	}
	if strings.TrimSpace(opts.baseURL) == "" {
		return nil, fmt.Errorf("--base-url is required")
	}
	adapter := responsesadapter.OpenAIChatAdapter{
		BaseURL: opts.baseURL,
		APIKey:  apiKey,
		Profile: responsesadapter.ProfileForProvider(opts.provider),
	}
	models := []responsesadapter.ModelInfo(nil)
	if strings.TrimSpace(opts.model) != "" {
		models = []responsesadapter.ModelInfo{{
			ID:      strings.TrimSpace(opts.model),
			OwnedBy: opts.provider,
		}}
	}
	return &responsesadapter.Facade{
		Adapter:        adapter,
		Store:          store,
		ProviderID:     opts.provider,
		DefaultModel:   opts.model,
		Models:         models,
		KeyFingerprint: responsesadapter.KeyFingerprint(apiKey, opts.scopeSalt),
		BaseURLHash:    responsesadapter.BaseURLHash(opts.baseURL),
		ProfileVersion: strings.TrimSpace(opts.provider) + ":v1",
	}, nil
}

type responsesProvidersFile struct {
	DefaultProvider string                  `json:"default_provider"`
	Providers       []responsesProviderFile `json:"providers"`
	ProxyKeys       map[string]string       `json:"proxy_keys"`
}

type responsesProviderFile struct {
	ID           string   `json:"id"`
	Profile      string   `json:"profile"`
	BaseURL      string   `json:"base_url"`
	APIKeyEnv    string   `json:"api_key_env"`
	DefaultModel string   `json:"default_model"`
	Models       []string `json:"models"`
}

func responsesRegistryFromFile(path string, proxyKeysCSV string, scopeSalt string) (*responsesadapter.ProviderRegistry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var file responsesProvidersFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("parse providers json: %w", err)
	}
	proxyKeys := map[string]string{}
	for key, provider := range file.ProxyKeys {
		proxyKeys[key] = provider
	}
	parsedProxyKeys, err := parseResponsesProxyKeys(proxyKeysCSV)
	if err != nil {
		return nil, err
	}
	for key, provider := range parsedProxyKeys {
		proxyKeys[key] = provider
	}
	providers := make([]responsesadapter.ProviderConfig, 0, len(file.Providers))
	for _, provider := range file.Providers {
		var models []responsesadapter.ModelInfo
		for _, model := range provider.Models {
			if strings.TrimSpace(model) != "" {
				models = append(models, responsesadapter.ModelInfo{ID: strings.TrimSpace(model), OwnedBy: provider.ID})
			}
		}
		apiKey := ""
		if strings.TrimSpace(provider.APIKeyEnv) != "" {
			apiKey = os.Getenv(strings.TrimSpace(provider.APIKeyEnv))
		}
		providers = append(providers, responsesadapter.ProviderConfig{
			ID:           provider.ID,
			ProfileID:    provider.Profile,
			BaseURL:      provider.BaseURL,
			APIKey:       apiKey,
			DefaultModel: provider.DefaultModel,
			Models:       models,
		})
	}
	return responsesadapter.NewProviderRegistry(responsesadapter.ProviderRegistryOptions{
		DefaultProvider: file.DefaultProvider,
		Providers:       providers,
		ProxyKeys:       proxyKeys,
		KeySalt:         scopeSalt,
	})
}

func parseResponsesProxyKeys(raw string) (map[string]string, error) {
	out := map[string]string{}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, provider, ok := strings.Cut(entry, ":")
		if !ok {
			return nil, fmt.Errorf("proxy key entry %q must be key:provider", entry)
		}
		key = strings.TrimSpace(key)
		provider = strings.TrimSpace(provider)
		if key == "" || provider == "" {
			return nil, fmt.Errorf("proxy key entry %q must include both key and provider", entry)
		}
		out[key] = provider
	}
	return out, nil
}

func defaultResponsesStorePath() string {
	path, err := appdirs.StatePath("responses", "adapter.sqlite")
	if err != nil || strings.TrimSpace(path) == "" {
		return ""
	}
	legacyPath, legacyErr := appdirs.LegacyCachePath("responses-adapter.sqlite")
	if legacyErr != nil {
		return path
	}
	resolvedPath, err := appdirs.ResolveMigratedRelatedFiles(path, legacyPath, "-wal", "-shm")
	if err != nil {
		return path
	}
	return resolvedPath
}
