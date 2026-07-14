package cli

import (
	"bufio"
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
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
)

const nativeResponsesProxyBodyLimit = 32 << 20

type nativeResponsesToolName struct {
	Namespace string
	Name      string
	Custom    bool
}

type nativeResponsesCompatibilityProxy struct {
	upstream        *url.URL
	apiKey          string
	proxyKey        string
	client          *http.Client
	log             io.Writer
	logMu           sync.Mutex
	mu              sync.RWMutex
	toWire          map[nativeResponsesToolName]string
	fromWire        map[string]nativeResponsesToolName
	responsesPolicy responsesadapter.ResponsesPolicy
	// sourcePolicy is intentionally restricted for native Responses routes.
	// The proxy can validate request fields, but it cannot safely validate
	// provider-generated citation events without translating the response
	// stream. A catalog that requires structured sources must therefore use a
	// translated/chat route instead of silently bypassing that policy here.
	sourcePolicy responsesadapter.SourcePolicy
	// unsupportedToolPolicy and nativeTools mirror the catalog route metadata.
	// A native Responses pass-through cannot translate a provider-owned tool,
	// but it can still reject a declared unsupported tool instead of forwarding
	// it and pretending the provider supports it.
	unsupportedToolPolicy string
	nativeTools           []responsesadapter.NativeToolSpec
}

func startNativeResponsesCompatibilityProxy(upstream string, apiKey string, proxyKey string, upstreamProxyURL string, log io.Writer) (string, func(), error) {
	return startNativeResponsesCompatibilityProxyWithPolicy(upstream, apiKey, proxyKey, upstreamProxyURL, log, responsesadapter.ResponsesPolicy{})
}

func startNativeResponsesCompatibilityProxyWithPolicy(upstream string, apiKey string, proxyKey string, upstreamProxyURL string, log io.Writer, policy responsesadapter.ResponsesPolicy) (string, func(), error) {
	return startNativeResponsesCompatibilityProxyWithPolicies(upstream, apiKey, proxyKey, upstreamProxyURL, log, policy, responsesadapter.SourcePolicy{})
}

func startNativeResponsesCompatibilityProxyWithPolicies(upstream string, apiKey string, proxyKey string, upstreamProxyURL string, log io.Writer, policy responsesadapter.ResponsesPolicy, sourcePolicy responsesadapter.SourcePolicy) (string, func(), error) {
	return startNativeResponsesCompatibilityProxyWithRoutePolicies(upstream, apiKey, proxyKey, upstreamProxyURL, log, policy, sourcePolicy, "", nil)
}

func startNativeResponsesCompatibilityProxyWithRoutePolicies(upstream string, apiKey string, proxyKey string, upstreamProxyURL string, log io.Writer, policy responsesadapter.ResponsesPolicy, sourcePolicy responsesadapter.SourcePolicy, unsupportedToolPolicy string, nativeTools []responsesadapter.NativeToolSpec) (string, func(), error) {
	if err := responsesadapter.ValidateDirectSourcePolicy(sourcePolicy); err != nil {
		return "", nil, err
	}
	handler, transportCleanup, err := newNativeResponsesCompatibilityProxy(upstream, apiKey, proxyKey, upstreamProxyURL, log)
	if err != nil {
		return "", nil, err
	}
	handler.responsesPolicy = policy
	handler.sourcePolicy = sourcePolicy
	handler.unsupportedToolPolicy = strings.TrimSpace(unsupportedToolPolicy)
	handler.nativeTools = append([]responsesadapter.NativeToolSpec(nil), nativeTools...)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		transportCleanup()
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
		transportCleanup()
	}
	return "http://" + ln.Addr().String() + "/v1", cleanup, nil
}

func newNativeResponsesCompatibilityProxy(upstream string, apiKey string, proxyKey string, upstreamProxyURL string, log io.Writer) (*nativeResponsesCompatibilityProxy, func(), error) {
	target, err := url.Parse(strings.TrimRight(strings.TrimSpace(upstream), "/"))
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, nil, fmt.Errorf("parse native Responses upstream %q", upstream)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.TrimSpace(upstreamProxyURL) != "" {
		proxyURL, err := url.Parse(upstreamProxyURL)
		if err != nil {
			return nil, nil, fmt.Errorf("parse native Responses upstream proxy: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	handler := &nativeResponsesCompatibilityProxy{
		upstream: target,
		apiKey:   strings.TrimSpace(apiKey),
		proxyKey: strings.TrimSpace(proxyKey),
		client:   &http.Client{Transport: transport},
		log:      log,
		toWire:   map[nativeResponsesToolName]string{},
		fromWire: map[string]nativeResponsesToolName{},
	}
	cleanup := func() {
		transport.CloseIdleConnections()
	}
	return handler, cleanup, nil
}

func (p *nativeResponsesCompatibilityProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !constantBearerMatch(r.Header.Get("Authorization"), p.proxyKey) {
		http.Error(w, `{"error":{"message":"invalid proxy authorization key"}}`, http.StatusUnauthorized)
		return
	}
	if err := responsesadapter.ValidateDirectSourcePolicy(p.sourcePolicy); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, err.Error()), http.StatusInternalServerError)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, nativeResponsesProxyBodyLimit+1))
	if err != nil || len(body) > nativeResponsesProxyBodyLimit {
		http.Error(w, `{"error":{"message":"request body is too large"}}`, http.StatusRequestEntityTooLarge)
		return
	}
	if len(bytes.TrimSpace(body)) > 0 && strings.Contains(r.URL.Path, "/responses") {
		var request responsesadapter.ResponsesRequest
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, `{"error":{"message":"invalid Responses request"}}`, http.StatusBadRequest)
			return
		}
		if p.responsesPolicy != (responsesadapter.ResponsesPolicy{}) {
			if err := responsesadapter.ValidateResponsesRequestPolicy(request, p.responsesPolicy); err != nil {
				http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, err.Error()), http.StatusBadRequest)
				return
			}
		}
		if err := p.validateRequestTools(request.Tools); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, err.Error()), http.StatusBadRequest)
			return
		}
		body, err = p.rewriteRequest(body)
		if err != nil {
			http.Error(w, `{"error":{"message":"invalid Responses request"}}`, http.StatusBadRequest)
			return
		}
	}
	target := *p.upstream
	target.Path = nativeResponsesTargetPath(p.upstream.Path, r.URL.Path)
	target.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		http.Error(w, `{"error":{"message":"build upstream request failed"}}`, http.StatusBadGateway)
		return
	}
	copyNativeResponsesHeaders(req.Header, r.Header)
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	resp, err := p.client.Do(req)
	if err != nil {
		http.Error(w, `{"error":{"message":"native Responses upstream unavailable"}}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	copyNativeResponsesHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		p.copySSE(w, resp.Body)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, nativeResponsesProxyBodyLimit+1))
	if err != nil || len(raw) > nativeResponsesProxyBodyLimit {
		return
	}
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "json") {
		if rewritten, rewriteErr := p.rewriteResponse(raw); rewriteErr == nil {
			raw = rewritten
		}
		p.logUsage(raw)
	}
	_, _ = w.Write(raw)
}

func (p *nativeResponsesCompatibilityProxy) validateRequestTools(raw json.RawMessage) error {
	if !strings.EqualFold(strings.TrimSpace(p.unsupportedToolPolicy), "error") || len(raw) == 0 {
		return nil
	}
	_, _, _, err := responsesadapter.NormalizeRequestTools(raw, "", "error", p.nativeTools)
	return err
}

func nativeResponsesTargetPath(basePath string, requestPath string) string {
	basePath = strings.TrimRight(basePath, "/")
	requestPath = "/" + strings.TrimLeft(requestPath, "/")
	if strings.HasSuffix(basePath, "/v1") && (requestPath == "/v1" || strings.HasPrefix(requestPath, "/v1/")) {
		requestPath = strings.TrimPrefix(requestPath, "/v1")
		if requestPath == "" {
			requestPath = "/"
		}
	}
	return basePath + requestPath
}

func constantBearerMatch(header string, expected string) bool {
	presented := strings.TrimSpace(header)
	if len(presented) >= 7 && strings.EqualFold(presented[:7], "Bearer ") {
		presented = strings.TrimSpace(presented[7:])
	}
	if presented == "" || expected == "" || len(presented) != len(expected) {
		return false
	}
	var diff byte
	for i := range presented {
		diff |= presented[i] ^ expected[i]
	}
	return diff == 0
}

func copyNativeResponsesHeaders(dst http.Header, src http.Header) {
	for name, values := range src {
		switch strings.ToLower(name) {
		case "authorization", "connection", "content-length", "host", "proxy-connection", "transfer-encoding", "upgrade":
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func (p *nativeResponsesCompatibilityProxy) rewriteRequest(raw []byte) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	p.rewriteRequestValue(value)
	return json.Marshal(value)
}

func (p *nativeResponsesCompatibilityProxy) rewriteRequestValue(value any) {
	switch item := value.(type) {
	case []any:
		for _, child := range item {
			p.rewriteRequestValue(child)
		}
	case map[string]any:
		if item["type"] == "output_text" {
			item["type"] = "input_text"
		}
		if item["type"] == "agent_message" {
			// Codex uses an internal agent_message replay item to deliver a
			// completed subagent result to its parent. Responses-compatible
			// providers generally accept only standard input messages, so keep
			// the content while translating the envelope.
			item["type"] = "message"
			item["role"] = "developer"
			delete(item, "author")
			delete(item, "recipient")
		}
		if input, ok := item["input"].([]any); ok {
			filtered := make([]any, 0, len(input))
			for _, child := range input {
				if childMap, ok := child.(map[string]any); ok && childMap["type"] == "reasoning" {
					continue
				}
				filtered = append(filtered, child)
			}
			item["input"] = filtered
		}
		if tools, ok := item["tools"].([]any); ok {
			item["tools"] = p.flattenTools(tools)
		}
		if item["type"] == "custom_tool_call" {
			name, _ := item["name"].(string)
			input, _ := item["input"].(string)
			if name != "" {
				item["type"] = "function_call"
				item["name"] = p.wireCustomToolName(name)
				arguments, _ := json.Marshal(map[string]string{"input": input})
				item["arguments"] = string(arguments)
				delete(item, "input")
			}
		} else if item["type"] == "custom_tool_call_output" || item["type"] == "mcp_tool_call_output" {
			item["type"] = "function_call_output"
		} else if item["type"] == "function_call" {
			namespace, _ := item["namespace"].(string)
			name, _ := item["name"].(string)
			if namespace != "" && name != "" {
				item["name"] = p.wireToolName(namespace, name)
				delete(item, "namespace")
			}
		}
		if item["type"] == "function_call_output" {
			if _, ok := item["output"].(string); !ok {
				item["output"] = nativeResponsesOutputText(item["output"])
			}
		}
		for _, child := range item {
			p.rewriteRequestValue(child)
		}
	}
}

func nativeResponsesOutputText(value any) string {
	switch item := value.(type) {
	case string:
		return item
	case []any:
		parts := make([]string, 0, len(item))
		for _, child := range item {
			if text := nativeResponsesOutputText(child); strings.TrimSpace(text) != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		for _, key := range []string{"text", "output_text", "content"} {
			if text := nativeResponsesOutputText(item[key]); strings.TrimSpace(text) != "" {
				return text
			}
		}
	}
	return ""
}

func (p *nativeResponsesCompatibilityProxy) flattenTools(tools []any) []any {
	out := make([]any, 0, len(tools))
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if ok && tool["type"] == "custom" {
			name, _ := tool["name"].(string)
			if name == "" {
				continue
			}
			out = append(out, map[string]any{
				"type":        "function",
				"name":        p.wireCustomToolName(name),
				"description": tool["description"],
				"parameters": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{"input": map[string]any{"type": "string"}},
					"required":             []string{"input"},
					"additionalProperties": false,
				},
			})
			continue
		}
		if !ok || tool["type"] != "namespace" {
			out = append(out, raw)
			continue
		}
		namespace, _ := tool["name"].(string)
		children, _ := tool["tools"].([]any)
		for _, childRaw := range children {
			child, ok := childRaw.(map[string]any)
			if !ok || child["type"] != "function" {
				continue
			}
			copy := make(map[string]any, len(child))
			for key, value := range child {
				copy[key] = value
			}
			if name, _ := copy["name"].(string); namespace != "" && name != "" {
				copy["name"] = p.wireToolName(namespace, name)
			}
			out = append(out, copy)
		}
	}
	return out
}

func (p *nativeResponsesCompatibilityProxy) wireToolName(namespace string, name string) string {
	return p.wireToolNameFor(nativeResponsesToolName{Namespace: namespace, Name: name})
}

func (p *nativeResponsesCompatibilityProxy) wireCustomToolName(name string) string {
	return p.wireToolNameFor(nativeResponsesToolName{Name: name, Custom: true})
}

func (p *nativeResponsesCompatibilityProxy) wireToolNameFor(key nativeResponsesToolName) string {
	p.mu.RLock()
	wire := p.toWire[key]
	p.mu.RUnlock()
	if wire != "" {
		return wire
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%t\x00%s\x00%s", key.Custom, key.Namespace, key.Name)))
	wire = "cxpns_" + hex.EncodeToString(sum[:8])
	p.mu.Lock()
	p.toWire[key] = wire
	p.fromWire[wire] = key
	p.mu.Unlock()
	return wire
}

func (p *nativeResponsesCompatibilityProxy) rewriteResponse(raw []byte) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	p.rewriteResponseValue(value)
	return json.Marshal(value)
}

func (p *nativeResponsesCompatibilityProxy) rewriteResponseValue(value any) {
	switch item := value.(type) {
	case []any:
		for _, child := range item {
			p.rewriteResponseValue(child)
		}
	case map[string]any:
		if item["type"] == "function_call" {
			if wire, _ := item["name"].(string); wire != "" {
				p.mu.RLock()
				tool, ok := p.fromWire[wire]
				p.mu.RUnlock()
				if ok {
					if tool.Custom {
						item["type"] = "custom_tool_call"
						item["name"] = tool.Name
						arguments, _ := item["arguments"].(string)
						var decoded struct {
							Input string `json:"input"`
						}
						_ = json.Unmarshal([]byte(arguments), &decoded)
						item["input"] = decoded.Input
						delete(item, "arguments")
					} else {
						item["name"] = tool.Name
						item["namespace"] = tool.Namespace
					}
				}
			}
		}
		for _, child := range item {
			p.rewriteResponseValue(child)
		}
	}
}

func (p *nativeResponsesCompatibilityProxy) copySSE(w http.ResponseWriter, body io.Reader) {
	reader := bufio.NewReaderSize(body, 64<<10)
	flusher, _ := w.(http.Flusher)
	for {
		line, err := reader.ReadString('\n')
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			if payload != "" && payload != "[DONE]" {
				if rewritten, rewriteErr := p.rewriteResponse([]byte(payload)); rewriteErr == nil {
					p.logUsage(rewritten)
					line = "data: " + string(rewritten) + "\n"
				}
			}
		}
		_, _ = io.WriteString(w, line)
		if flusher != nil {
			flusher.Flush()
		}
		if err != nil {
			return
		}
	}
}

func (p *nativeResponsesCompatibilityProxy) logUsage(raw []byte) {
	if p.log == nil {
		return
	}
	var event struct {
		Type     string `json:"type"`
		Response struct {
			Model string `json:"model"`
			Usage struct {
				InputTokens int `json:"input_tokens"`
				Details     struct {
					CachedTokens int `json:"cached_tokens"`
				} `json:"input_tokens_details"`
			} `json:"usage"`
		} `json:"response"`
		Model string `json:"model"`
		Usage struct {
			InputTokens int `json:"input_tokens"`
			Details     struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"input_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(raw, &event) != nil {
		return
	}
	input := event.Response.Usage.InputTokens
	cached := event.Response.Usage.Details.CachedTokens
	model := event.Response.Model
	if input == 0 {
		input = event.Usage.InputTokens
		cached = event.Usage.Details.CachedTokens
		model = event.Model
	}
	if input <= 0 {
		return
	}
	hitRate := 100 * float64(cached) / float64(input)
	p.logMu.Lock()
	_, _ = fmt.Fprintf(p.log, "responses upstream cache usage: model=%s input=%d cached=%d hit_rate=%.1f%%\n", model, input, cached, hitRate)
	p.logMu.Unlock()
}
