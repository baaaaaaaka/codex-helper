package responsesadapter

// This file contains the deliberately small, typed Anthropic Messages wire
// converter used by catalog profiles such as deepseek-anthropic-v1.  The
// catalog selects this converter by name; it cannot supply arbitrary JSON
// templates.  That keeps request history, tools, auth and streaming semantics
// under the adapter's control.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type AnthropicAdapter struct {
	BaseURL               string
	APIKey                string
	Headers               map[string]string
	Endpoints             map[string]string
	AuthType              string
	AuthHeader            string
	HTTPClient            *http.Client
	MaxRetries            int
	MaxRetriesSet         bool
	RetryStatuses         []int
	HonorRetryAfter       *bool
	RetryTransportErrors  *bool
	ResponseHeaderTimeout time.Duration
	StreamIdleTimeout     time.Duration
	MaxOutputTokens       int
	StreamMode            string
	Strict                bool
	Status                func(string)
	RequestGate           chan struct{}
}

type anthropicRequest struct {
	Model       string             `json:"model"`
	System      string             `json:"system,omitempty"`
	Messages    []anthropicMessage `json:"messages"`
	Tools       []any              `json:"tools,omitempty"`
	ToolChoice  any                `json:"tool_choice,omitempty"`
	MaxTokens   int                `json:"max_tokens"`
	Temperature *float64           `json:"temperature,omitempty"`
	TopP        *float64           `json:"top_p,omitempty"`
	Thinking    any                `json:"thinking,omitempty"`
	Stream      bool               `json:"stream"`
}

type anthropicMessage struct {
	Role    string             `json:"role"`
	Content []anthropicContent `json:"content"`
}

type anthropicContent struct {
	Type      string                `json:"type"`
	Text      string                `json:"text,omitempty"`
	Thinking  string                `json:"thinking,omitempty"`
	ID        string                `json:"id,omitempty"`
	Name      string                `json:"name,omitempty"`
	Input     json.RawMessage       `json:"input,omitempty"`
	ToolUseID string                `json:"tool_use_id,omitempty"`
	Content   any                   `json:"content,omitempty"`
	Source    *anthropicImageSource `json:"source,omitempty"`
}

type anthropicImageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

type anthropicTool struct {
	Type        string          `json:"type,omitempty"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

func (a AnthropicAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	if err := validateProviderResponseFields(req, "Anthropic"); err != nil {
		return nil, err
	}
	if len(req.ResponseFormat) > 0 {
		// The Messages converter does not have a typed structured-output
		// mapping. Reject it instead of silently dropping response_format and
		// returning an unstructured answer.
		return nil, fmt.Errorf("Anthropic adapter does not support response_format")
	}
	if strings.EqualFold(strings.TrimSpace(req.Operation), "fim") || strings.EqualFold(strings.TrimSpace(req.Operation), "prefix") {
		return nil, fmt.Errorf("anthropic converter does not support operation %q", req.Operation)
	}
	body, err := a.marshalRequest(req, a.streamEnabled(req))
	if err != nil {
		return nil, err
	}
	endpoint, err := anthropicEndpoint(a.BaseURL, a.Endpoints, "messages", "/messages")
	if err != nil {
		return nil, err
	}
	client := a.HTTPClient
	if client == nil {
		client = NewUpstreamHTTPClientWithResponseHeaderTimeout(http.ProxyFromEnvironment, a.ResponseHeaderTimeout)
	}
	if a.RequestGate != nil {
		if a.Status != nil {
			a.Status("waiting for Anthropic upstream concurrency slot")
		}
		select {
		case a.RequestGate <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	releaseGate := func() {
		if a.RequestGate != nil {
			<-a.RequestGate
		}
	}
	resp, err := a.doRequest(ctx, client, endpoint, body, req.Model)
	if err != nil {
		releaseGate()
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		releaseGate()
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, upstreamHTTPError(resp.StatusCode, resp.Status, strings.TrimSpace(string(raw)), req.Model)
	}
	out := make(chan ProviderEvent)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		defer releaseGate()
		if !a.streamEnabled(req) {
			a.parseBuffered(ctx, resp.Body, out, req.SourcePolicy)
			return
		}
		a.parseSSE(ctx, resp.Body, out, req.SourcePolicy)
	}()
	return out, nil
}

func (a AnthropicAdapter) streamEnabled(req ProviderRequest) bool {
	// Anthropic's streaming endpoint is required for normal Responses
	// streaming, but callers may explicitly request a buffered upstream.
	return !strings.EqualFold(strings.TrimSpace(a.StreamMode), "nonstream-buffered")
}

func (a AnthropicAdapter) marshalRequest(req ProviderRequest, stream bool) ([]byte, error) {
	messages, err := anthropicMessages(req.Messages)
	if err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		messages = []anthropicMessage{{Role: "user", Content: []anthropicContent{{Type: "text", Text: req.InputText}}}}
	}
	maxTokens := 4096
	if req.MaxOutputTokens != nil && *req.MaxOutputTokens > 0 {
		maxTokens = *req.MaxOutputTokens
	}
	if a.MaxOutputTokens > 0 && maxTokens > a.MaxOutputTokens {
		maxTokens = a.MaxOutputTokens
	}
	body := anthropicRequest{
		Model: req.Model, System: req.Instructions, Messages: messages,
		Tools: anthropicRequestTools(req.Tools, req.NativeTools), ToolChoice: anthropicToolChoice(req.ToolChoice),
		MaxTokens: maxTokens, Temperature: req.Temperature, TopP: req.TopP, Stream: stream,
	}
	if effort := strings.ToLower(strings.TrimSpace(req.ReasoningEffort)); effort != "" && effort != "none" && effort != "minimal" {
		budget := 4096
		switch effort {
		case "low":
			budget = 2048
		case "high":
			budget = 8192
		case "xhigh":
			budget = 16384
		}
		body.Thinking = map[string]any{"type": "enabled", "budget_tokens": budget}
		// Anthropic rejects sampling controls together with thinking on some
		// deployments. The catalog can still explicitly request sampling by
		// setting a provider-specific profile, so only omit it when thinking is
		// selected by this converter.
		body.Temperature = nil
		body.TopP = nil
	}
	return json.Marshal(body)
}

func anthropicMessages(messages []ProviderMessage) ([]anthropicMessage, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	out := make([]anthropicMessage, 0, len(messages))
	for _, message := range messages {
		originalRole := strings.ToLower(strings.TrimSpace(message.Role))
		role := originalRole
		switch role {
		case "system", "developer":
			continue
		case "assistant":
			role = "assistant"
		case "tool":
			role = "user"
		case "user":
		default:
			return nil, fmt.Errorf("anthropic converter cannot translate message role %q", message.Role)
		}
		content := make([]anthropicContent, 0, 1+len(message.ToolCalls)+len(message.ContentParts))
		if message.Content != "" {
			content = append(content, anthropicContent{Type: "text", Text: message.Content})
		}
		for _, part := range message.ContentParts {
			switch part.Type {
			case "text":
				content = append(content, anthropicContent{Type: "text", Text: part.Text})
			case "image_url":
				content = append(content, anthropicContent{Type: "image", Source: &anthropicImageSource{Type: "url", URL: part.ImageURL}})
			case "audio", "video":
				return nil, fmt.Errorf("anthropic converter does not support %s input", part.Type)
			}
		}
		for _, call := range message.ToolCalls {
			arguments := json.RawMessage(call.Arguments)
			if len(arguments) == 0 {
				arguments = json.RawMessage(`{}`)
			}
			if !json.Valid(arguments) {
				return nil, fmt.Errorf("anthropic tool call %q has invalid JSON arguments", call.Name)
			}
			content = append(content, anthropicContent{Type: "tool_use", ID: call.ID, Name: call.Name, Input: arguments})
		}
		if originalRole == "tool" {
			content = []anthropicContent{{Type: "tool_result", ToolUseID: message.ToolCallID, Content: message.Content}}
		}
		if len(content) == 0 {
			content = []anthropicContent{{Type: "text", Text: ""}}
		}
		out = append(out, anthropicMessage{Role: role, Content: content})
	}
	return out, nil
}

func anthropicTools(tools []ChatTool) []anthropicTool {
	out := make([]anthropicTool, 0, len(tools))
	for _, tool := range tools {
		if strings.TrimSpace(tool.Function.Name) == "" {
			continue
		}
		schema := append(json.RawMessage(nil), tool.Function.Parameters...)
		if len(schema) == 0 {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		out = append(out, anthropicTool{Name: tool.Function.Name, Description: tool.Function.Description, InputSchema: schema})
	}
	return out
}

func anthropicRequestTools(functions []ChatTool, native []ProviderNativeTool) []any {
	if len(functions) == 0 && len(native) == 0 {
		return nil
	}
	out := make([]any, 0, len(functions)+len(native))
	for _, tool := range anthropicTools(functions) {
		out = append(out, tool)
	}
	for _, tool := range native {
		payload := map[string]any{"type": tool.UpstreamType}
		if tool.Name != "" {
			payload["name"] = tool.Name
		}
		for key, value := range tool.Fields {
			if key != "type" && key != "name" {
				payload[key] = value
			}
		}
		out = append(out, payload)
	}
	return out
}

func anthropicToolChoice(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	if name, ok := value.(string); ok {
		switch strings.ToLower(name) {
		case "auto":
			return map[string]string{"type": "auto"}
		case "required", "any":
			return map[string]string{"type": "any"}
		case "none":
			return map[string]string{"type": "none"}
		}
	}
	if object, ok := value.(map[string]any); ok {
		if function, ok := object["function"].(map[string]any); ok {
			if name, ok := function["name"].(string); ok && name != "" {
				return map[string]any{"type": "tool", "name": name}
			}
		}
		if typ, ok := object["type"].(string); ok {
			switch strings.ToLower(typ) {
			case "auto":
				return map[string]string{"type": "auto"}
			case "required":
				return map[string]string{"type": "any"}
			}
		}
	}
	return nil
}

func (a AnthropicAdapter) doRequest(ctx context.Context, client *http.Client, endpoint string, payload []byte, model string) (*http.Response, error) {
	maxRetries := a.MaxRetries
	if !a.MaxRetriesSet && maxRetries == 0 {
		maxRetries = 2
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		for key, value := range a.Headers {
			if strings.TrimSpace(key) != "" {
				req.Header.Set(key, value)
			}
		}
		if req.Header.Get("anthropic-version") == "" {
			req.Header.Set("anthropic-version", "2023-06-01")
		}
		if strings.TrimSpace(a.APIKey) != "" {
			if strings.EqualFold(strings.TrimSpace(a.AuthType), "header") {
				header := strings.TrimSpace(a.AuthHeader)
				if header == "" {
					header = "x-api-key"
				}
				req.Header.Set(header, a.APIKey)
			} else {
				req.Header.Set("Authorization", "Bearer "+a.APIKey)
			}
		}
		if a.Status != nil {
			a.Status("waiting for Anthropic response headers (attempt " + strconv.Itoa(attempt+1) + ")")
		}
		resp, err := client.Do(req)
		if err == nil && !a.shouldRetry(resp.StatusCode) {
			return resp, nil
		}
		if err != nil && (a.RetryTransportErrors != nil && !*a.RetryTransportErrors || attempt >= maxRetries) {
			return nil, err
		}
		if attempt >= maxRetries {
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(a.retryDelay(attempt, resp)):
		}
	}
}

func (a AnthropicAdapter) retryDelay(attempt int, resp *http.Response) time.Duration {
	if resp != nil && (a.HonorRetryAfter == nil || *a.HonorRetryAfter) {
		if retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After")); retryAfter != "" {
			if seconds, err := strconv.ParseFloat(retryAfter, 64); err == nil && seconds >= 0 {
				return time.Duration(seconds * float64(time.Second))
			}
		}
	}
	if attempt > 6 {
		attempt = 6
	}
	return (100 * time.Millisecond) << attempt
}

func (a AnthropicAdapter) shouldRetry(status int) bool {
	if len(a.RetryStatuses) == 0 {
		return status == 408 || status == 409 || status == 429 || status >= 500
	}
	for _, candidate := range a.RetryStatuses {
		if candidate == status {
			return true
		}
	}
	return false
}

type anthropicResponse struct {
	StopReason string             `json:"stop_reason"`
	Content    []anthropicContent `json:"content"`
	Usage      anthropicUsage     `json:"usage"`
}

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	CacheRead    int `json:"cache_read_input_tokens"`
	CacheCreate  int `json:"cache_creation_input_tokens"`
}

func (u anthropicUsage) usage() *Usage {
	return &Usage{InputTokens: u.InputTokens, OutputTokens: u.OutputTokens, TotalTokens: u.InputTokens + u.OutputTokens, CachedTokens: u.CacheRead}
}

func (a AnthropicAdapter) parseBuffered(ctx context.Context, body io.Reader, out chan<- ProviderEvent, policy SourcePolicy) {
	var response anthropicResponse
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("invalid Anthropic response: %w", err)})
		return
	}
	a.emitBlocks(ctx, out, response.Content, policy)
	if response.Usage.InputTokens != 0 || response.Usage.OutputTokens != 0 {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventUsage, Usage: response.Usage.usage()})
	}
	sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventDone, FinishReason: response.StopReason})
}

func (a AnthropicAdapter) emitBlocks(ctx context.Context, out chan<- ProviderEvent, blocks []anthropicContent, policy SourcePolicy) {
	for index, block := range blocks {
		switch strings.ToLower(block.Type) {
		case "text":
			if block.Text != "" {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventTextDelta, Delta: block.Text})
			}
		case "thinking":
			if text := firstNonEmpty(block.Text, block.Thinking); text != "" {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: text})
			}
		case "tool_use":
			sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: index, ID: block.ID, Name: block.Name, ArgumentsDelta: string(block.Input)}})
		case "web_search_tool_result", "server_tool_use":
			if strings.EqualFold(strings.TrimSpace(policy.Mode), "unsupported") && hasSourceURLs(block.Content) {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned source citations but catalog marks sources unsupported")})
				return
			}
			if policy.RequireURL && hasSourceMetadata(block.Content) && !hasSourceURLs(block.Content) {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned source metadata without a URL")})
				return
			}
			for _, source := range anthropicSources(block.Content, policy) {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventSource, Source: &source})
			}
			if strings.EqualFold(strings.TrimSpace(policy.Mode), "text") {
				if text := anthropicNestedText(block.Content); text != "" {
					sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventTextDelta, Delta: text})
				}
			}
		}
	}
}

func anthropicSources(value any, policy SourcePolicy) []SourceCitation {
	if strings.EqualFold(strings.TrimSpace(policy.Mode), "unsupported") || strings.EqualFold(strings.TrimSpace(policy.Mode), "text") {
		return nil
	}
	var out []SourceCitation
	seen := map[string]bool{}
	var walk func(any)
	walk = func(current any) {
		switch typed := current.(type) {
		case []any:
			for _, item := range typed {
				walk(item)
			}
		case map[string]any:
			urlValue, _ := typed["url"].(string)
			urlValue = strings.TrimSpace(urlValue)
			if strings.HasPrefix(urlValue, "http://") || strings.HasPrefix(urlValue, "https://") {
				if !seen[urlValue] {
					title, _ := typed["title"].(string)
					out = append(out, SourceCitation{Type: "url_citation", URL: urlValue, Title: strings.TrimSpace(title)})
					seen[urlValue] = true
				}
			}
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)
	return out
}

func anthropicNestedText(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	if list, ok := value.([]any); ok {
		var parts []string
		for _, item := range list {
			if text := anthropicNestedText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	}
	if object, ok := value.(map[string]any); ok {
		for _, key := range []string{"text", "content", "title", "description", "url"} {
			if text := anthropicNestedText(object[key]); text != "" {
				return text
			}
		}
	}
	return ""
}

func (a AnthropicAdapter) parseSSE(ctx context.Context, body io.ReadCloser, out chan<- ProviderEvent, policy SourcePolicy) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	eventName, data := "", ""
	finishReason := ""
	usage := Usage{}
	hasUsage := false
	serverToolIndices := map[int]bool{}
	messageStopped := false
	idleExpired, touchIdleWatch, stopIdleWatch := watchUpstreamStreamIdle(ctx, body, a.StreamIdleTimeout)
	defer stopIdleWatch()
	flush := func() {
		if strings.TrimSpace(data) == "" {
			eventName, data = "", ""
			return
		}
		a.handleSSEEvent(ctx, out, eventName, data, &finishReason, &usage, &hasUsage, serverToolIndices, &messageStopped, policy)
		eventName, data = "", ""
	}
	for scanner.Scan() {
		touchIdleWatch()
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data != "" {
				data += "\n"
			}
			data += strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
	flush()
	select {
	case <-idleExpired:
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("upstream Anthropic stream idle timeout after %s", effectiveUpstreamStreamIdleTimeout(a.StreamIdleTimeout))})
		return
	default:
	}
	if hasUsage {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventUsage, Usage: &usage})
	}
	if messageStopped || finishReason != "" {
		if finishReason == "" {
			finishReason = "stop"
		}
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventDone, FinishReason: finishReason})
	}
	if err := scanner.Err(); err != nil {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("Anthropic stream read failed: %w", err)})
	}
}

func (a AnthropicAdapter) handleSSEEvent(ctx context.Context, out chan<- ProviderEvent, name, raw string, finishReason *string, usage *Usage, hasUsage *bool, serverToolIndices map[int]bool, messageStopped *bool, policy SourcePolicy) {
	var event map[string]any
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("invalid Anthropic SSE event: %w", err)})
		return
	}
	switch name {
	case "content_block_delta":
		index := anthropicEventIndex(event)
		if serverToolIndices[index] {
			return
		}
		delta, _ := event["delta"].(map[string]any)
		typ, _ := delta["type"].(string)
		switch typ {
		case "text_delta":
			if text, _ := delta["text"].(string); text != "" {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventTextDelta, Delta: text})
			}
		case "thinking_delta", "signature_delta":
			if text, _ := delta["thinking"].(string); text != "" {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: text})
			}
		case "input_json_delta":
			if text, _ := delta["partial_json"].(string); text != "" {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: index, ArgumentsDelta: text}})
			}
		}
	case "content_block_start":
		block, _ := event["content_block"].(map[string]any)
		typ, _ := block["type"].(string)
		index := anthropicEventIndex(event)
		if typ == "server_tool_use" {
			// This is an Anthropic-owned server-side tool (for example
			// web_search), not a client function call. Never expose it as a
			// Responses function call that the caller could accidentally execute.
			serverToolIndices[index] = true
			return
		}
		if typ == "web_search_tool_result" {
			if strings.EqualFold(strings.TrimSpace(policy.Mode), "unsupported") && hasSourceURLs(block["content"]) {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned source citations but catalog marks sources unsupported")})
				return
			}
			if policy.RequireURL && hasSourceMetadata(block["content"]) && !hasSourceURLs(block["content"]) {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned source metadata without a URL")})
				return
			}
			for _, source := range anthropicSources(block["content"], policy) {
				sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventSource, Source: &source})
			}
			if strings.EqualFold(strings.TrimSpace(policy.Mode), "text") {
				if text := anthropicNestedText(block["content"]); text != "" {
					sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventTextDelta, Delta: text})
				}
			}
			return
		}
		if typ == "tool_use" {
			id, _ := block["id"].(string)
			name, _ := block["name"].(string)
			input, _ := block["input"].(map[string]any)
			args := ""
			if input != nil {
				encoded, _ := json.Marshal(input)
				args = string(encoded)
			}
			sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: index, ID: id, Name: name, ArgumentsDelta: args}})
		}
	case "message_start":
		message, _ := event["message"].(map[string]any)
		if usageRaw, ok := message["usage"].(map[string]any); ok {
			mergeUsage(usage, anthropicUsageFromMap(usageRaw).usage())
			*hasUsage = true
		}
	case "message_delta":
		delta, _ := event["delta"].(map[string]any)
		stop, _ := delta["stop_reason"].(string)
		if stop != "" && finishReason != nil {
			*finishReason = stop
		}
		if usageRaw, ok := event["usage"].(map[string]any); ok {
			mergeUsage(usage, anthropicUsageFromMap(usageRaw).usage())
			*hasUsage = true
		}
	case "message_stop":
		// parseSSE emits the single terminal event after it has observed the
		// optional message_delta stop reason.
		if messageStopped != nil {
			*messageStopped = true
		}
	}
}

func anthropicEventIndex(event map[string]any) int {
	if value, ok := event["index"].(float64); ok {
		return int(value)
	}
	return 0
}

func anthropicUsageFromMap(value map[string]any) anthropicUsage {
	read := func(key string) int {
		if n, ok := value[key].(float64); ok {
			return int(n)
		}
		return 0
	}
	return anthropicUsage{InputTokens: read("input_tokens"), OutputTokens: read("output_tokens"), CacheRead: read("cache_read_input_tokens"), CacheCreate: read("cache_creation_input_tokens")}
}

func mergeUsage(dst *Usage, src *Usage) {
	if dst == nil || src == nil {
		return
	}
	if src.InputTokens > 0 {
		dst.InputTokens = src.InputTokens
	}
	if src.OutputTokens > 0 {
		dst.OutputTokens = src.OutputTokens
	}
	if src.TotalTokens > 0 {
		dst.TotalTokens = src.TotalTokens
	}
	if src.CachedTokens > 0 {
		dst.CachedTokens = src.CachedTokens
	}
	if dst.TotalTokens == 0 {
		dst.TotalTokens = dst.InputTokens + dst.OutputTokens
	}
}

func sendProviderEvent(ctx context.Context, out chan<- ProviderEvent, event ProviderEvent) bool {
	select {
	case out <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func anthropicEndpoint(base string, endpoints map[string]string, key, fallback string) (string, error) {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	path := strings.TrimSpace(endpoints[key])
	if path == "" {
		path = fallback
	}
	if base == "" {
		return "", fmt.Errorf("Anthropic base URL is empty")
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid Anthropic base URL %q", base)
	}
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path, nil
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(path, "/")
	return strings.TrimRight(u.String(), "/"), nil
}
