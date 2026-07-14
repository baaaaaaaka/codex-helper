package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

const (
	cxpUnifiedCodexModelProviderID = "cxp-unified"
	envCXPUnifiedGatewayKey        = "CXP_UNIFIED_GATEWAY_KEY"
	cxpUnifiedGatewayHeader        = "X-CXP-Gateway-Key"
	defaultOfficialCodexBaseURL    = "https://chatgpt.com/backend-api/codex"
)

type unifiedThirdPartyRoute struct {
	handler      http.Handler
	proxyKey     string
	upstream     string
	rewriteModel bool
	signature    string
}

type unifiedModelGateway struct {
	officialBase *url.URL
	client       *http.Client
	localKey     string
	instanceID   string
	catalogPath  string
	providers    []modelprofile.ProviderSpec
	third        map[string]unifiedThirdPartyRoute
	log          io.Writer

	mu             sync.RWMutex
	officialModels map[string]bool
	mergedETag     string
}

type unifiedModelGatewayOptions struct {
	OfficialBaseURL  string
	LocalKey         string
	CatalogPath      string
	Providers        []modelprofile.Resolved
	APIKeys          map[string]string
	InterfaceAPIKeys map[string]map[string]string
	UpstreamProxy    string
	Log              io.Writer
	InstanceID       string
}

func startUnifiedModelGateway(opts unifiedModelGatewayOptions) (string, func(), error) {
	handler, handlerCleanup, err := newUnifiedModelGateway(opts)
	if err != nil {
		return "", nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		handlerCleanup()
		return "", nil, err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(ln)
	}()
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-done
		handlerCleanup()
	}
	return "http://" + ln.Addr().String() + "/v1", cleanup, nil
}

func newUnifiedModelGateway(opts unifiedModelGatewayOptions) (*unifiedModelGateway, func(), error) {
	officialBase := strings.TrimRight(strings.TrimSpace(opts.OfficialBaseURL), "/")
	if officialBase == "" {
		officialBase = defaultOfficialCodexBaseURL
	}
	officialURL, err := url.Parse(officialBase)
	if err != nil || officialURL.Scheme == "" || officialURL.Host == "" {
		return nil, nil, fmt.Errorf("parse official Codex base URL %q", officialBase)
	}
	if strings.TrimSpace(opts.LocalKey) == "" {
		return nil, nil, fmt.Errorf("unified model gateway requires a local key")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.TrimSpace(opts.UpstreamProxy) != "" {
		proxyURL, err := url.Parse(opts.UpstreamProxy)
		if err != nil {
			return nil, nil, fmt.Errorf("parse unified gateway upstream proxy: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	responseStore, responseStoreCleanup, err := responsesStoreFromOptions(responsesServeOptions{storePath: defaultResponsesStorePath()})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, err
	}
	cleanups := []func(){responseStoreCleanup}
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
		transport.CloseIdleConnections()
	}

	gateway := &unifiedModelGateway{
		officialBase:   officialURL,
		client:         &http.Client{Transport: transport},
		localKey:       strings.TrimSpace(opts.LocalKey),
		instanceID:     strings.TrimSpace(opts.InstanceID),
		catalogPath:    strings.TrimSpace(opts.CatalogPath),
		third:          map[string]unifiedThirdPartyRoute{},
		log:            opts.Log,
		officialModels: map[string]bool{},
	}
	for _, resolved := range opts.Providers {
		apiKey := strings.TrimSpace(opts.APIKeys[resolved.Name])
		if apiKey == "" {
			cleanup()
			return nil, nil, fmt.Errorf("missing API key for configured model profile %q", resolved.Name)
		}
		providerKey, err := ids.New()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		var handler http.Handler
		if resolved.Provider.DirectResponses {
			var directCleanup func()
			var directHandler *nativeResponsesCompatibilityProxy
			directHandler, directCleanup, err = newNativeResponsesCompatibilityProxy(resolved.Provider.BaseURL, apiKey, providerKey, opts.UpstreamProxy, opts.Log)
			if err == nil {
				directHandler.responsesPolicy = responsesadapter.ResponsesPolicy{PreviousResponseID: resolved.Model.ResponsesPolicy.PreviousResponseID, Background: resolved.Model.ResponsesPolicy.Background, ContextManagement: resolved.Model.ResponsesPolicy.ContextManagement}
				directHandler.sourcePolicy = responsesAdapterSourcePolicy(resolved.Model)
				directHandler.unsupportedToolPolicy = resolved.Model.UnsupportedToolPolicy
				directHandler.nativeTools = append([]responsesadapter.NativeToolSpec(nil), responsesAdapterNativeTools(resolved.Model)...)
				if sourceErr := responsesadapter.ValidateDirectSourcePolicy(directHandler.sourcePolicy); sourceErr != nil {
					directCleanup()
					err = sourceErr
				} else {
					handler = directHandler
				}
			}
			if err == nil {
				cleanups = append(cleanups, directCleanup)
			}
		} else {
			selectedModel := resolved.Model
			adapter, configureErr := newResolvedProviderAdapter(resolved, apiKey, opts.UpstreamProxy, opts.Log)
			if configureErr != nil {
				cleanup()
				return nil, nil, configureErr
			}
			routeConfigs, routeErr := resolvedProviderRouteConfigs(resolved, apiKey, opts.InterfaceAPIKeys[resolved.Name], adapter, opts.UpstreamProxy, opts.Log)
			if routeErr != nil {
				cleanup()
				return nil, nil, routeErr
			}
			registry, registryErr := responsesadapter.NewProviderRegistry(responsesadapter.ProviderRegistryOptions{
				DefaultProvider: resolved.Provider.ID,
				Providers: []responsesadapter.ProviderConfig{{
					ID:                    resolved.Provider.ID,
					ProfileID:             resolved.Provider.AdapterProfile,
					BaseURL:               resolved.Provider.BaseURL,
					APIKey:                apiKey,
					DefaultModel:          resolved.SelectedPublicModel(),
					Models:                responsesAdapterModelsForProvider(resolved.Provider),
					Adapter:               adapter,
					CustomToolMode:        selectedModel.ToolPolicy.CustomToolMode,
					UnsupportedToolPolicy: selectedModel.UnsupportedToolPolicy,
					ConversionProfile:     resolved.Provider.ConversionProfile,
					StrictConversion:      resolved.Provider.StrictConversion,
					Operation:             resolved.Provider.Operation,
					NativeTools:           responsesAdapterNativeTools(resolved.Model),
					SourcePolicy:          responsesAdapterSourcePolicy(resolved.Model),
					ResponsesPolicy:       responsesAdapterResponsesPolicy(resolved.Model),
					Routes:                routeConfigs,
				}},
				ProxyKeys: map[string]string{providerKey: resolved.Provider.ID},
				KeySalt:   resolved.Name + ":unified",
			})
			if registryErr != nil {
				err = registryErr
			} else {
				handler = &responsesadapter.Facade{
					Router:      registry,
					Store:       responseStore,
					ShellPolicy: responsespolicy.NewShellEscalationPolicy(0),
				}
			}
		}
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		catalogProvider := resolved.Provider
		catalogProvider.Models = nil
		// A synced credential is verified per configured profile/model. Do not
		// expose every model declared by the provider merely because one model
		// using the same endpoint and key passed verification.
		for _, model := range []modelprofile.ModelSpec{resolved.Model} {
			publicID := strings.TrimSpace(model.PublicID())
			if publicID == "" {
				continue
			}
			key := strings.ToLower(publicID)
			reasoningMap, _ := json.Marshal(resolved.Provider.ReasoningEffortMap)
			signature := strings.Join([]string{
				resolved.Provider.BaseURL,
				resolved.Provider.AdapterProfile,
				model.UpstreamModel(),
				responsesadapter.KeyFingerprint(apiKey, "cxp-unified-route-v1"),
				resolved.Provider.DefaultReasoningEffort,
				string(reasoningMap),
			}, "\n")
			if existing, exists := gateway.third[key]; exists {
				if existing.signature == signature {
					continue
				}
				cleanup()
				return nil, nil, fmt.Errorf("configured third-party model slug %q has conflicting routes", publicID)
			}
			gateway.third[key] = unifiedThirdPartyRoute{
				handler:      handler,
				proxyKey:     providerKey,
				upstream:     model.UpstreamModel(),
				rewriteModel: resolved.Provider.DirectResponses,
				signature:    signature,
			}
			catalogProvider.Models = append(catalogProvider.Models, model)
		}
		if len(catalogProvider.Models) > 0 {
			catalogProvider.DefaultModel = catalogProvider.Models[0].PublicID()
			gateway.providers = append(gateway.providers, catalogProvider)
		}
	}
	_, statErr := os.Stat(gateway.catalogPath)
	if os.IsNotExist(statErr) && len(gateway.providers) > 0 {
		fallback, fallbackErr := modelprofile.ThirdPartyCodexModelCatalogJSON(gateway.providers)
		if fallbackErr != nil {
			cleanup()
			return nil, nil, fallbackErr
		}
		if writeErr := writeAtomicPrivateFile(gateway.catalogPath, fallback); writeErr != nil {
			cleanup()
			return nil, nil, writeErr
		}
		statErr = nil
	}
	if statErr == nil {
		if err := gateway.loadCatalogSnapshot(); err != nil {
			cleanup()
			return nil, nil, err
		}
	} else if !os.IsNotExist(statErr) {
		cleanup()
		return nil, nil, statErr
	}
	return gateway, cleanup, nil
}

func (g *unifiedModelGateway) loadCatalogSnapshot() error {
	raw, err := os.ReadFile(g.catalogPath)
	if err != nil {
		return fmt.Errorf("read unified model catalog snapshot: %w", err)
	}
	var decoded struct {
		Models []struct {
			Slug string `json:"slug"`
		} `json:"models"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return fmt.Errorf("decode unified model catalog snapshot: %w", err)
	}
	if len(decoded.Models) == 0 {
		return fmt.Errorf("unified model catalog snapshot contains no models")
	}
	official := make(map[string]bool, len(decoded.Models))
	for _, model := range decoded.Models {
		key := strings.ToLower(strings.TrimSpace(model.Slug))
		if key != "" {
			if _, thirdParty := g.third[key]; !thirdParty {
				official[key] = true
			}
		}
	}
	sum := sha256.Sum256(raw)
	g.officialModels = official
	g.mergedETag = `"cxp-unified-` + hex.EncodeToString(sum[:8]) + `"`
	return nil
}

func (g *unifiedModelGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/_codex_proxy/health" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"ok":true,"instanceId":%q}`, g.instanceID)
		return
	}
	if !constantSecretMatch(r.Header.Get(cxpUnifiedGatewayHeader), g.localKey) {
		http.Error(w, `{"error":{"message":"invalid unified gateway key"}}`, http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/_cxp/ready":
		if g.currentETag() == "" {
			http.Error(w, `{"ready":false}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ready":true}`)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/models"):
		g.serveModels(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/responses"):
		g.serveResponses(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (g *unifiedModelGateway) serveModels(w http.ResponseWriter, r *http.Request) {
	resp, err := g.doOfficial(r, nil)
	if err != nil {
		http.Error(w, `{"error":{"message":"official model catalog unavailable"}}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, nativeResponsesProxyBodyLimit+1))
	if err != nil || len(raw) > nativeResponsesProxyBodyLimit {
		http.Error(w, `{"error":{"message":"official model catalog is too large"}}`, http.StatusBadGateway)
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		copyUnifiedResponse(w, resp, raw, "")
		return
	}
	merged, err := modelprofile.MergeCodexModelCatalogJSON(raw, g.providers)
	if err != nil {
		http.Error(w, `{"error":{"message":"merge model catalog failed"}}`, http.StatusBadGateway)
		return
	}
	var decoded struct {
		Models []struct {
			Slug string `json:"slug"`
		} `json:"models"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		http.Error(w, `{"error":{"message":"decode official model catalog failed"}}`, http.StatusBadGateway)
		return
	}
	officialModels := make(map[string]bool, len(decoded.Models))
	for _, model := range decoded.Models {
		officialModels[strings.ToLower(strings.TrimSpace(model.Slug))] = true
	}
	sum := sha256.Sum256(merged)
	etag := `"cxp-unified-` + hex.EncodeToString(sum[:8]) + `"`
	g.mu.Lock()
	g.officialModels = officialModels
	g.mergedETag = etag
	g.mu.Unlock()
	if g.log != nil {
		_, _ = fmt.Fprintf(g.log, "unified model catalog ready: official=%d third_party=%d\n", len(officialModels), len(g.third))
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(merged)))
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(merged)
}

func (g *unifiedModelGateway) serveResponses(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, nativeResponsesProxyBodyLimit+1))
	if err != nil || len(body) > nativeResponsesProxyBodyLimit {
		http.Error(w, `{"error":{"message":"request body is too large"}}`, http.StatusRequestEntityTooLarge)
		return
	}
	var request struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &request); err != nil || strings.TrimSpace(request.Model) == "" {
		http.Error(w, `{"error":{"message":"request model is required"}}`, http.StatusBadRequest)
		return
	}
	modelKey := strings.ToLower(strings.TrimSpace(request.Model))
	if route, ok := g.third[modelKey]; ok {
		body, strippedReasoning, err := sanitizeThirdPartyResponsesHistory(body)
		if err != nil {
			http.Error(w, `{"error":{"message":"invalid third-party Responses history"}}`, http.StatusBadRequest)
			return
		}
		if strippedReasoning > 0 && g.log != nil {
			_, _ = fmt.Fprintf(g.log, "unified third-party route removed encrypted content from %d reasoning item(s) while preserving portable summaries and conversation history\n", strippedReasoning)
		}
		if route.rewriteModel && !strings.EqualFold(request.Model, route.upstream) {
			var value map[string]any
			if err := json.Unmarshal(body, &value); err != nil {
				http.Error(w, `{"error":{"message":"invalid Responses request"}}`, http.StatusBadRequest)
				return
			}
			value["model"] = route.upstream
			body, err = json.Marshal(value)
			if err != nil {
				http.Error(w, `{"error":{"message":"rewrite Responses model failed"}}`, http.StatusInternalServerError)
				return
			}
		}
		cloned := r.Clone(r.Context())
		cloned.Body = io.NopCloser(bytes.NewReader(body))
		cloned.ContentLength = int64(len(body))
		cloned.Header = r.Header.Clone()
		cloned.Header.Del(cxpUnifiedGatewayHeader)
		stripOfficialCredentialHeaders(cloned.Header)
		cloned.Header.Set("Authorization", "Bearer "+route.proxyKey)
		guard := &unifiedTerminalResponseWriter{ResponseWriter: w, etag: g.currentETag()}
		route.handler.ServeHTTP(guard, cloned)
		guard.ensureTerminal()
		return
	}
	g.mu.RLock()
	official := g.officialModels[modelKey]
	g.mu.RUnlock()
	if !official {
		http.Error(w, `{"error":{"message":"unknown unified gateway model"}}`, http.StatusBadRequest)
		return
	}
	body, droppedReasoning, err := sanitizeOfficialResponsesHistory(body)
	if err != nil {
		http.Error(w, `{"error":{"message":"invalid official Responses history"}}`, http.StatusBadRequest)
		return
	}
	if droppedReasoning > 0 && g.log != nil {
		_, _ = fmt.Fprintf(g.log, "unified official route removed %d third-party reasoning item(s) while preserving portable conversation history\n", droppedReasoning)
	}
	resp, err := g.doOfficial(r, body)
	if err != nil {
		http.Error(w, `{"error":{"message":"official Responses upstream unavailable"}}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	copyUnifiedStreamingResponse(w, resp, g.currentETag())
}

func sanitizeThirdPartyResponsesHistory(body []byte) ([]byte, int, error) {
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, 0, err
	}
	rawInput, ok := request["input"]
	if !ok || len(bytes.TrimSpace(rawInput)) == 0 {
		return body, 0, nil
	}
	var input []json.RawMessage
	if err := json.Unmarshal(rawInput, &input); err != nil {
		// String and object input forms cannot contain replayed reasoning items.
		return body, 0, nil
	}
	stripped := 0
	for index, raw := range input {
		var item map[string]json.RawMessage
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, 0, err
		}
		if !strings.EqualFold(strings.TrimSpace(rawStringCLI(item["type"])), "reasoning") {
			continue
		}
		encrypted, exists := item["encrypted_content"]
		if !exists || len(bytes.TrimSpace(encrypted)) == 0 || bytes.Equal(bytes.TrimSpace(encrypted), []byte("null")) || bytes.Equal(bytes.TrimSpace(encrypted), []byte(`""`)) {
			continue
		}
		delete(item, "encrypted_content")
		rewritten, err := json.Marshal(item)
		if err != nil {
			return nil, 0, err
		}
		input[index] = rewritten
		stripped++
	}
	if stripped == 0 {
		return body, 0, nil
	}
	request["input"], _ = json.Marshal(input)
	sanitized, err := json.Marshal(request)
	if err != nil {
		return nil, 0, err
	}
	return sanitized, stripped, nil
}

func rawStringCLI(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func sanitizeOfficialResponsesHistory(body []byte) ([]byte, int, error) {
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, 0, err
	}
	rawInput, ok := request["input"]
	if !ok || len(bytes.TrimSpace(rawInput)) == 0 {
		return body, 0, nil
	}
	var input []json.RawMessage
	if err := json.Unmarshal(rawInput, &input); err != nil {
		// String and object input forms cannot contain replayed reasoning items.
		return body, 0, nil
	}
	filtered := make([]json.RawMessage, 0, len(input))
	dropped := 0
	for _, item := range input {
		thirdParty, err := isThirdPartyReasoningItem(item)
		if err != nil {
			return nil, 0, err
		}
		if thirdParty {
			dropped++
			continue
		}
		filtered = append(filtered, item)
	}
	if dropped == 0 {
		return body, 0, nil
	}
	request["input"], _ = json.Marshal(filtered)
	sanitized, err := json.Marshal(request)
	if err != nil {
		return nil, 0, err
	}
	return sanitized, dropped, nil
}

func isThirdPartyReasoningItem(raw json.RawMessage) (bool, error) {
	var item struct {
		Type             string          `json:"type"`
		ID               string          `json:"id"`
		EncryptedContent string          `json:"encrypted_content"`
		Content          json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		return false, err
	}
	if !strings.EqualFold(strings.TrimSpace(item.Type), "reasoning") {
		return false, nil
	}
	// responsesadapter response IDs are resp_*, so its reasoning item IDs are
	// rs_resp_*. Keep this provenance check for old rollouts even after the
	// adapter stops misusing encrypted_content for plaintext reasoning.
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(item.ID)), "rs_resp_") {
		return true, nil
	}
	if strings.TrimSpace(item.EncryptedContent) != "" {
		// Preserve official opaque reasoning even if a future API revision also
		// attaches displayable content. Only the CXP adapter-owned rs_resp_ IDs
		// above are known to have mislabeled plaintext as encrypted historically.
		return false, nil
	}
	content := bytes.TrimSpace(item.Content)
	if len(content) == 0 || bytes.Equal(content, []byte("null")) || bytes.Equal(content, []byte("[]")) {
		return false, nil
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(content, &parts); err != nil {
		return false, err
	}
	// Reasoning without an official opaque payload is adapter/vendor output and
	// must not be forwarded to the official endpoint.
	return len(parts) > 0, nil
}

func stripOfficialCredentialHeaders(header http.Header) {
	for name := range header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "chatgpt-") || strings.HasPrefix(lower, "openai-") || lower == "cookie" {
			header.Del(name)
		}
	}
	header.Del("Authorization")
}

func (g *unifiedModelGateway) doOfficial(r *http.Request, body []byte) (*http.Response, error) {
	target := *g.officialBase
	requestPath := "/" + strings.TrimLeft(r.URL.Path, "/")
	if requestPath == "/v1" || strings.HasPrefix(requestPath, "/v1/") {
		requestPath = strings.TrimPrefix(requestPath, "/v1")
		if requestPath == "" {
			requestPath = "/"
		}
	}
	target.Path = strings.TrimRight(g.officialBase.Path, "/") + requestPath
	target.RawQuery = r.URL.RawQuery
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), reader)
	if err != nil {
		return nil, err
	}
	copyUnifiedRequestHeaders(req.Header, r.Header)
	return g.client.Do(req)
}

func (g *unifiedModelGateway) currentETag() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.mergedETag
}

func copyUnifiedRequestHeaders(dst http.Header, src http.Header) {
	for name, values := range src {
		switch strings.ToLower(name) {
		case "connection", "content-length", "host", "proxy-connection", "transfer-encoding", "upgrade", "x-cxp-gateway-key":
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func copyUnifiedResponse(w http.ResponseWriter, resp *http.Response, body []byte, etag string) {
	copyUnifiedResponseHeaders(w.Header(), resp.Header, etag)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

func copyUnifiedStreamingResponse(w http.ResponseWriter, resp *http.Response, etag string) {
	copyUnifiedResponseHeaders(w.Header(), resp.Header, etag)
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)
	buffer := make([]byte, 64<<10)
	terminal := false
	tail := []byte(nil)
	for {
		n, err := resp.Body.Read(buffer)
		if n > 0 {
			terminal, tail = unifiedStreamTerminalState(terminal, tail, buffer[:n])
			_, _ = w.Write(buffer[:n])
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") && !terminal {
				_, _ = io.WriteString(w, unifiedIncompleteStreamFailureEvent())
				if flusher != nil {
					flusher.Flush()
				}
			}
			return
		}
	}
}

func copyUnifiedResponseHeaders(dst http.Header, src http.Header, etag string) {
	for name, values := range src {
		switch strings.ToLower(name) {
		case "connection", "content-length", "transfer-encoding", "x-models-etag":
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
	if etag != "" {
		dst.Set("X-Models-Etag", etag)
	}
}

type unifiedTerminalResponseWriter struct {
	http.ResponseWriter
	etag        string
	status      int
	terminal    bool
	tail        []byte
	wroteHeader bool
}

func (w *unifiedTerminalResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.wroteHeader = true
	if w.etag != "" {
		w.Header().Set("X-Models-Etag", w.etag)
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *unifiedTerminalResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	w.terminal, w.tail = unifiedStreamTerminalState(w.terminal, w.tail, body)
	return w.ResponseWriter.Write(body)
}

func (w *unifiedTerminalResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *unifiedTerminalResponseWriter) ensureTerminal() {
	if w.status < 200 || w.status >= 300 || w.terminal || !strings.Contains(strings.ToLower(w.Header().Get("Content-Type")), "text/event-stream") {
		return
	}
	_, _ = io.WriteString(w.ResponseWriter, unifiedIncompleteStreamFailureEvent())
	w.Flush()
}

func unifiedStreamTerminalState(terminal bool, tail []byte, body []byte) (bool, []byte) {
	combined := append(append([]byte{}, tail...), body...)
	if bytes.Contains(combined, []byte("response.completed")) || bytes.Contains(combined, []byte("response.failed")) || bytes.Contains(combined, []byte("response.incomplete")) {
		terminal = true
	}
	const tailLimit = 64
	if len(combined) > tailLimit {
		combined = combined[len(combined)-tailLimit:]
	}
	return terminal, combined
}

func unifiedIncompleteStreamFailureEvent() string {
	return "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"cxp-upstream-disconnected\",\"status\":\"failed\",\"error\":{\"code\":\"upstream_stream_incomplete\",\"message\":\"Upstream stream ended without a terminal event.\"}}}\n\n"
}

func constantSecretMatch(presented string, expected string) bool {
	presented = strings.TrimSpace(presented)
	expected = strings.TrimSpace(expected)
	if presented == "" || expected == "" || len(presented) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(expected)) == 1
}

func writeAtomicPrivateFile(path string, body []byte) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("merged model catalog path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".catalog-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(body); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
