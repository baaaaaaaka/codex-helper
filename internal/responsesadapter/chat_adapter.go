package responsesadapter

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

type OpenAIChatAdapter struct {
	BaseURL                 string
	APIKey                  string
	Profile                 ProviderProfile
	HTTPClient              *http.Client
	MaxRetries              int
	MaxRetriesSet           bool
	RetryBase               time.Duration
	RetryStatuses           []int
	HonorRetryAfter         *bool
	RetryTransportErrors    *bool
	ResponseHeaderTimeout   time.Duration
	StreamIdleTimeout       time.Duration
	FirstEventTimeout       time.Duration
	SemanticProgressTimeout time.Duration
	MaxDuration             time.Duration
	Status                  func(string)
	MaxOutputTokens         int
	Headers                 map[string]string
	AuthType                string
	AuthHeader              string
	StreamMode              string
	HeartbeatMode           string
	ReasoningDeltaPath      string
	ReasoningTokensPath     string
	CachedTokensPath        string
	UsageField              string
	RequestGate             chan struct{}
}

type chatCompletionRequest struct {
	Model               string               `json:"model"`
	Messages            []chatMessage        `json:"messages"`
	Tools               []ChatTool           `json:"tools,omitempty"`
	NativeTools         []ProviderNativeTool `json:"-"`
	ToolChoice          json.RawMessage      `json:"tool_choice,omitempty"`
	ParallelToolCalls   *bool                `json:"parallel_tool_calls,omitempty"`
	MaxCompletionTokens *int                 `json:"max_completion_tokens,omitempty"`
	ReasoningEffort     string               `json:"reasoning_effort,omitempty"`
	Thinking            *thinkingConfig      `json:"thinking,omitempty"`
	Temperature         *float64             `json:"temperature,omitempty"`
	TopP                *float64             `json:"top_p,omitempty"`
	ResponseFormat      json.RawMessage      `json:"response_format,omitempty"`
	Stream              bool                 `json:"stream"`
	StreamOptions       *chatStreamOptions   `json:"stream_options,omitempty"`
}

func (r chatCompletionRequest) MarshalJSON() ([]byte, error) {
	type alias chatCompletionRequest
	base, err := json.Marshal(alias(r))
	if err != nil {
		return nil, err
	}
	if len(r.NativeTools) == 0 {
		return base, nil
	}
	var object map[string]any
	if err := json.Unmarshal(base, &object); err != nil {
		return nil, err
	}
	tools := make([]any, 0, len(r.Tools)+len(r.NativeTools))
	for _, tool := range r.Tools {
		tools = append(tools, tool)
	}
	for _, tool := range r.NativeTools {
		payload := map[string]any{"type": tool.UpstreamType}
		if tool.Name != "" {
			payload["name"] = tool.Name
		}
		for key, value := range tool.Fields {
			if key != "type" && key != "name" {
				payload[key] = value
			}
		}
		tools = append(tools, payload)
	}
	object["tools"] = tools
	return json.Marshal(object)
}

type thinkingConfig struct {
	Type string `json:"type"`
}

type chatStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatMessage struct {
	Role             string                `json:"role"`
	Content          string                `json:"content"`
	ContentParts     []chatContentPart     `json:"-"`
	ReasoningContent string                `json:"reasoning_content,omitempty"`
	ToolCalls        []chatMessageToolCall `json:"tool_calls,omitempty"`
	ToolCallID       string                `json:"tool_call_id,omitempty"`
	OmitContent      bool                  `json:"-"`
}

type chatContentPart struct {
	Type     string        `json:"type"`
	Text     string        `json:"text,omitempty"`
	ImageURL *chatImageURL `json:"image_url,omitempty"`
	Audio    *chatAudio    `json:"input_audio,omitempty"`
	VideoURL *chatVideoURL `json:"video_url,omitempty"`
}

type chatImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

type chatAudio struct {
	URL    string `json:"url,omitempty"`
	Data   string `json:"data,omitempty"`
	Format string `json:"format,omitempty"`
}

type chatVideoURL struct {
	URL string `json:"url"`
}

type chatMessageToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func chatRequestTools(functions []ChatTool, native []ProviderNativeTool) []any {
	if len(functions) == 0 && len(native) == 0 {
		return nil
	}
	out := make([]any, 0, len(functions)+len(native))
	for _, tool := range functions {
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

func (a OpenAIChatAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	if err := validateProviderResponseFields(req, "OpenAI Chat"); err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(req.Operation)) {
	case "", "chat", "responses":
	default:
		return nil, fmt.Errorf("OpenAI Chat converter does not support operation %q", req.Operation)
	}
	profile := a.Profile.withDefaults()
	buffered := strings.EqualFold(strings.TrimSpace(a.StreamMode), "nonstream-buffered")
	body := chatCompletionRequest{
		Model:               req.Model,
		Messages:            chatMessagesFromProviderRequestWithProfile(req, profile),
		Tools:               req.Tools,
		NativeTools:         req.NativeTools,
		ToolChoice:          chatToolChoice(req.ToolChoice, profile),
		ParallelToolCalls:   req.ParallelToolCalls,
		MaxCompletionTokens: req.MaxOutputTokens,
		ReasoningEffort:     profile.reasoningEffort(req.ReasoningEffort),
		Temperature:         req.Temperature,
		TopP:                req.TopP,
		ResponseFormat:      append(json.RawMessage(nil), req.ResponseFormat...),
		Stream:              !buffered,
	}
	if a.MaxOutputTokens > 0 && (body.MaxCompletionTokens == nil || *body.MaxCompletionTokens > a.MaxOutputTokens) {
		value := a.MaxOutputTokens
		body.MaxCompletionTokens = &value
	}
	if profile.ForceParallelToolCalls != nil {
		body.ParallelToolCalls = profile.ForceParallelToolCalls
	}
	if thinkingType := profile.thinkingType(req.Model, req.ReasoningEffort); thinkingType != "" {
		body.Thinking = &thinkingConfig{Type: thinkingType}
	}
	if profile.shouldStripSampling(req.Model) || profile.TemperaturePolicy == "strip" || (profile.TemperaturePolicy == "strip-when-reasoning" && body.Thinking != nil && body.Thinking.Type == "enabled") {
		body.Temperature = nil
	}
	if profile.shouldStripSampling(req.Model) || profile.TopPPolicy == "strip" || (profile.TopPPolicy == "strip-when-reasoning" && body.Thinking != nil && body.Thinking.Type == "enabled") {
		body.TopP = nil
	}
	if profile.IncludeUsageStreamOptions && !buffered {
		body.StreamOptions = &chatStreamOptions{IncludeUsage: true}
	}
	payload, err := marshalChatCompletionRequest(body, profile, req.Model, req.ReasoningEffort)
	if err != nil {
		return nil, err
	}
	endpoint, err := chatCompletionsURL(a.BaseURL)
	if err != nil {
		return nil, err
	}
	if a.RequestGate != nil {
		if a.Status != nil {
			a.Status("waiting for upstream concurrency slot")
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
	client := a.HTTPClient
	if client == nil {
		client = NewUpstreamHTTPClientWithResponseHeaderTimeout(http.ProxyFromEnvironment, a.ResponseHeaderTimeout)
	}
	resp, err := a.doChatCompletionsRequest(ctx, client, endpoint, payload)
	if err != nil {
		releaseGate()
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		releaseGate()
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, upstreamHTTPError(resp.StatusCode, resp.Status, strings.TrimSpace(string(raw)), req.Model)
	}
	ch := make(chan ProviderEvent)
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		defer releaseGate()
		if buffered {
			parseBufferedChatCompletionWithPolicy(ctx, resp.Body, ch, profile.ValidateToolArguments, a.reasoningPath(profile), a.cachedUsagePath(), a.Status, req.SourcePolicy, profile.PlainTextToolCall)
			return
		}
		parseChatCompletionSSEWithProgressAndSources(ctx, resp.Body, ch, profile.ValidateToolArguments, a.StreamIdleTimeout, a.FirstEventTimeout, a.SemanticProgressTimeout, a.MaxDuration, a.heartbeatMode(), a.reasoningPath(profile), a.cachedUsagePath(), a.ReasoningTokensPath, a.Status, req.SourcePolicy, profile.PlainTextToolCall)
	}()
	return ch, nil
}

type bufferedChatCompletion struct {
	Choices []struct {
		FinishReason string              `json:"finish_reason"`
		Message      chatBufferedMessage `json:"message"`
	} `json:"choices"`
	Usage *chatUsage `json:"usage"`
}

type chatBufferedMessage struct {
	Content          string                `json:"content"`
	ReasoningContent string                `json:"reasoning_content"`
	Reasoning        string                `json:"reasoning"`
	ToolCalls        []chatMessageToolCall `json:"tool_calls"`
	Raw              map[string]any        `json:"-"`
}

func (m *chatBufferedMessage) UnmarshalJSON(data []byte) error {
	type alias chatBufferedMessage
	var decoded alias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*m = chatBufferedMessage(decoded)
	return json.Unmarshal(data, &m.Raw)
}

func parseBufferedChatCompletion(ctx context.Context, body io.Reader, out chan<- ProviderEvent, validateArguments bool, reasoningPath, cachedTokensPath string, status func(string)) {
	parseBufferedChatCompletionWithPolicy(ctx, body, out, validateArguments, reasoningPath, cachedTokensPath, status, SourcePolicy{}, "")
}

func parseBufferedChatCompletionWithPolicy(ctx context.Context, body io.Reader, out chan<- ProviderEvent, validateArguments bool, reasoningPath, cachedTokensPath string, status func(string), sourcePolicy SourcePolicy, plainTextToolCall ...string) {
	var completion bufferedChatCompletion
	if err := json.NewDecoder(body).Decode(&completion); err != nil {
		out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("invalid buffered chat completion: %w", err)}
		return
	}
	if len(completion.Choices) == 0 {
		out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("buffered chat completion has no choices")}
		return
	}
	choice := completion.Choices[0]
	select {
	case <-ctx.Done():
		return
	default:
	}
	reasoning := firstNonEmptyJSONPath(choice.Message.Raw, reasoningPath, choice.Message.ReasoningContent, choice.Message.Reasoning)
	if reasoning != "" {
		out <- ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: reasoning}
	}
	if choice.Message.Content != "" {
		if plainTextToolCallRejected(plainTextToolCall, choice.Message.Content) {
			out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned plain-text tool call while policy rejects plain-text tool calls")}
			return
		}
		parser := newInlineThinkParser()
		emitSplitText(out, parser.feed(choice.Message.Content))
		emitSplitText(out, parser.flush())
	}
	if strings.EqualFold(strings.TrimSpace(sourcePolicy.Mode), "unsupported") && hasSourceURLs(choice.Message.Raw) {
		out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned source citations but catalog marks sources unsupported")}
		return
	}
	if sourcePolicy.RequireURL && hasSourceMetadata(choice.Message.Raw) && !hasSourceURLs(choice.Message.Raw) {
		out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned source metadata without a URL")}
		return
	}
	if strings.EqualFold(strings.TrimSpace(sourcePolicy.Mode), "text") {
		if text := sourceTextFromValue(choice.Message.Raw); text != "" {
			out <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: text}
		}
	}
	for _, source := range sourceCitationsFromValue(choice.Message.Raw, sourcePolicy) {
		out <- ProviderEvent{Kind: ProviderEventSource, Source: &source}
	}
	for index, call := range choice.Message.ToolCalls {
		if validateArguments && !json.Valid([]byte(strings.TrimSpace(call.Function.Arguments))) {
			out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("tool call %d returned invalid JSON arguments", index)}
			return
		}
		out <- ProviderEvent{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{
			Index: index, ID: call.ID, Name: call.Function.Name, ArgumentsDelta: call.Function.Arguments,
		}}
	}
	if completion.Usage != nil {
		out <- ProviderEvent{Kind: ProviderEventUsage, Usage: completion.Usage.toUsage(cachedTokensPath, status)}
	}
	out <- ProviderEvent{Kind: ProviderEventDone, FinishReason: choice.FinishReason}
}

func (a OpenAIChatAdapter) reasoningPath(profile ProviderProfile) string {
	if value := strings.TrimSpace(a.ReasoningDeltaPath); value != "" {
		return value
	}
	return strings.TrimSpace(profile.ReasoningResponseField)
}

func (a OpenAIChatAdapter) cachedUsagePath() string {
	if value := strings.TrimSpace(a.CachedTokensPath); value != "" {
		return value
	}
	return strings.TrimSpace(a.UsageField)
}

func (a OpenAIChatAdapter) heartbeatMode() string {
	switch value := strings.ToLower(strings.TrimSpace(a.HeartbeatMode)); value {
	case "ignore", "transport-only", "semantic":
		return value
	default:
		return "transport-only"
	}
}

// marshalChatCompletionRequest applies model-specific request fragments only
// after the stable Chat Completions shape has been built. Core routing and
// conversation fields cannot be overridden by repository-supplied config.
func marshalChatCompletionRequest(body chatCompletionRequest, profile ProviderProfile, model, effort string) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	var merged map[string]any
	if err := json.Unmarshal(raw, &merged); err != nil {
		return nil, err
	}
	var fragment map[string]any
	switch profile.thinkingType(model, effort) {
	case "enabled":
		fragment = profile.ThinkingEnabledRequest
	case "disabled":
		fragment = profile.ThinkingDisabledRequest
	}
	for key, value := range fragment {
		switch key {
		case "model", "messages", "tools", "tool_choice", "parallel_tool_calls", "stream", "stream_options":
			continue
		default:
			merged[key] = value
		}
	}
	return json.Marshal(merged)
}

func (a OpenAIChatAdapter) doChatCompletionsRequest(ctx context.Context, client *http.Client, endpoint string, payload []byte) (*http.Response, error) {
	maxRetries := a.MaxRetries
	if !a.MaxRetriesSet && maxRetries == 0 {
		maxRetries = 2
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := 0; ; attempt++ {
		if a.Status != nil {
			a.Status(fmt.Sprintf("waiting for upstream response headers (attempt %d/%d)", attempt+1, maxRetries+1))
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		for key, value := range a.Headers {
			if strings.TrimSpace(key) != "" {
				httpReq.Header.Set(key, value)
			}
		}
		if strings.TrimSpace(a.APIKey) != "" {
			if strings.EqualFold(strings.TrimSpace(a.AuthType), "header") {
				header := strings.TrimSpace(a.AuthHeader)
				if header == "" {
					header = "X-API-Key"
				}
				httpReq.Header.Set(header, strings.TrimSpace(a.APIKey))
			} else {
				httpReq.Header.Set("Authorization", "Bearer "+strings.TrimSpace(a.APIKey))
			}
		}
		resp, err := client.Do(httpReq)
		if err != nil && a.RetryTransportErrors != nil && !*a.RetryTransportErrors {
			return nil, err
		}
		if err == nil && !a.shouldRetryStatus(resp.StatusCode) {
			if a.Status != nil {
				a.Status(fmt.Sprintf("upstream response started with HTTP %d", resp.StatusCode))
			}
			return resp, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if resp != nil && resp.Body != nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
				_ = resp.Body.Close()
			}
			return nil, ctxErr
		}
		if attempt >= maxRetries {
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
		delay := a.retryDelay(attempt, resp)
		if a.Status != nil {
			reason := "transport error"
			if resp != nil {
				reason = fmt.Sprintf("HTTP %d", resp.StatusCode)
			}
			a.Status(fmt.Sprintf("retrying upstream after %s (next attempt %d/%d)", reason, attempt+2, maxRetries+1))
		}
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (a OpenAIChatAdapter) shouldRetryStatus(status int) bool {
	if len(a.RetryStatuses) == 0 {
		return shouldRetryStatus(status) || status == 529
	}
	for _, candidate := range a.RetryStatuses {
		if status == candidate {
			return true
		}
	}
	return false
}

func shouldRetryStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout || status >= 500
}

func (a OpenAIChatAdapter) retryDelay(attempt int, resp *http.Response) time.Duration {
	if resp != nil && (a.HonorRetryAfter == nil || *a.HonorRetryAfter) {
		if retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After")); retryAfter != "" {
			if seconds, err := strconv.ParseFloat(retryAfter, 64); err == nil && seconds >= 0 {
				return time.Duration(seconds * float64(time.Second))
			}
		}
	}
	base := a.RetryBase
	if base <= 0 {
		base = 25 * time.Millisecond
	}
	if attempt > 6 {
		attempt = 6
	}
	return base << attempt
}

func chatToolChoice(raw json.RawMessage, profile ProviderProfile) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	if !profile.DropNonAutoToolChoice {
		return raw
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte(`"auto"`)) {
		return raw
	}
	return nil
}

func chatMessagesFromProviderRequest(req ProviderRequest) []chatMessage {
	return chatMessagesFromProviderRequestWithProfile(req, ProfileForProvider("generic"))
}

func chatMessagesFromProviderRequestWithProfile(req ProviderRequest, profile ProviderProfile) []chatMessage {
	if len(req.Messages) > 0 {
		messages := make([]chatMessage, 0, len(req.Messages))
		for _, message := range req.Messages {
			messages = append(messages, chatMessageFromProviderMessage(message, req.Model, profile))
		}
		return normalizeChatMessages(prependInstructionMessage(req.Instructions, messages), profile)
	}
	messages := make([]chatMessage, 0, len(req.History)*2+1)
	for _, record := range req.History {
		if len(record.InputMessages) > 0 {
			for _, message := range record.InputMessages {
				messages = append(messages, chatMessageFromProviderMessage(message, req.Model, profile))
			}
		} else if strings.TrimSpace(record.InputText) != "" {
			messages = append(messages, chatMessage{Role: "user", Content: record.InputText})
		}
		if strings.TrimSpace(record.OutputText) != "" || len(record.ToolCalls) > 0 {
			messages = append(messages, chatMessageFromProviderMessage(ProviderMessage{
				Role:             "assistant",
				Content:          record.OutputText,
				ReasoningContent: record.ReasoningText,
				ToolCalls:        record.ToolCalls,
			}, req.Model, profile))
		}
	}
	if strings.TrimSpace(req.InputText) != "" {
		messages = append(messages, chatMessage{Role: "user", Content: req.InputText})
	}
	return normalizeChatMessages(prependInstructionMessage(req.Instructions, messages), profile)
}

func prependInstructionMessage(instructions string, messages []chatMessage) []chatMessage {
	instructions = strings.TrimSpace(instructions)
	if instructions == "" {
		return messages
	}
	out := make([]chatMessage, 0, len(messages)+1)
	out = append(out, chatMessage{Role: "system", Content: instructions})
	out = append(out, messages...)
	return out
}

func chatMessageFromProviderMessage(message ProviderMessage, model string, profile ProviderProfile) chatMessage {
	reasoningContent := message.ReasoningContent
	if !profile.shouldSendReasoningContent(model) {
		reasoningContent = ""
	}
	chat := chatMessage{
		Role:             chatRole(message.Role),
		Content:          message.Content,
		ReasoningContent: reasoningContent,
		ToolCallID:       message.ToolCallID,
	}
	if profile.shouldForwardImages(model, message.ContentParts, chat.Role, message.Content) {
		chat.ContentParts = chatContentParts(message.ContentParts)
	}
	for _, call := range message.ToolCalls {
		chat.ToolCalls = append(chat.ToolCalls, chatMessageToolCall{
			ID:   call.ID,
			Type: "function",
			Function: chatToolFunction{
				Name:      call.Name,
				Arguments: call.Arguments,
			},
		})
	}
	if profile.OmitEmptyAssistantContentWithToolCalls && chat.Role == "assistant" && chat.Content == "" && len(chat.ToolCalls) > 0 {
		chat.OmitContent = true
	}
	return chat
}

func normalizeChatMessages(messages []chatMessage, profile ProviderProfile) []chatMessage {
	if !profile.MergeSystemMessages {
		return messages
	}
	var systemParts []string
	nonSystem := make([]chatMessage, 0, len(messages))
	for _, message := range messages {
		if message.Role == "system" {
			if strings.TrimSpace(message.Content) != "" {
				systemParts = append(systemParts, message.Content)
			}
			continue
		}
		nonSystem = append(nonSystem, message)
	}
	if len(systemParts) == 0 {
		return nonSystem
	}
	out := make([]chatMessage, 0, len(nonSystem)+1)
	out = append(out, chatMessage{Role: "system", Content: strings.Join(systemParts, "\n\n")})
	out = append(out, nonSystem...)
	return out
}

func (m chatMessage) MarshalJSON() ([]byte, error) {
	type wireMessage struct {
		Role             string                `json:"role"`
		Content          any                   `json:"content,omitempty"`
		ReasoningContent string                `json:"reasoning_content,omitempty"`
		ToolCalls        []chatMessageToolCall `json:"tool_calls,omitempty"`
		ToolCallID       string                `json:"tool_call_id,omitempty"`
	}
	var content any
	if !m.OmitContent {
		if len(m.ContentParts) > 0 {
			content = m.ContentParts
		} else {
			content = m.Content
		}
	}
	return json.Marshal(wireMessage{
		Role:             m.Role,
		Content:          content,
		ReasoningContent: m.ReasoningContent,
		ToolCalls:        m.ToolCalls,
		ToolCallID:       m.ToolCallID,
	})
}

func chatContentParts(parts []ProviderContentPart) []chatContentPart {
	out := make([]chatContentPart, 0, len(parts)+1)
	hasText := false
	for _, part := range parts {
		switch part.Type {
		case "text":
			if strings.TrimSpace(part.Text) == "" {
				continue
			}
			hasText = true
			out = append(out, chatContentPart{Type: "text", Text: part.Text})
		case "image_url":
			if strings.TrimSpace(part.ImageURL) == "" {
				continue
			}
			image := &chatImageURL{URL: part.ImageURL}
			if strings.TrimSpace(part.Detail) != "" {
				image.Detail = strings.TrimSpace(part.Detail)
			}
			out = append(out, chatContentPart{Type: "image_url", ImageURL: image})
		case "audio":
			if strings.TrimSpace(part.AudioURL) == "" && strings.TrimSpace(part.AudioData) == "" {
				continue
			}
			out = append(out, chatContentPart{Type: "input_audio", Audio: &chatAudio{URL: part.AudioURL, Data: part.AudioData, Format: part.AudioFormat}})
		case "video":
			if strings.TrimSpace(part.VideoURL) == "" {
				continue
			}
			out = append(out, chatContentPart{Type: "video_url", VideoURL: &chatVideoURL{URL: part.VideoURL}})
		}
	}
	if !hasText && hasChatMediaPart(out) {
		out = append([]chatContentPart{{Type: "text", Text: "Please analyze the attached media."}}, out...)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func hasChatImagePart(parts []chatContentPart) bool {
	for _, part := range parts {
		if part.Type == "image_url" && part.ImageURL != nil && strings.TrimSpace(part.ImageURL.URL) != "" {
			return true
		}
	}
	return false
}

func hasChatMediaPart(parts []chatContentPart) bool {
	for _, part := range parts {
		if part.Type == "image_url" || part.Type == "input_audio" || part.Type == "video_url" {
			return true
		}
	}
	return false
}

func chatRole(role string) string {
	role = firstNonEmpty(role, "user")
	if role == "developer" {
		return "system"
	}
	return role
}

func chatCompletionsURL(base string) (string, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		return "", fmt.Errorf("base URL is required")
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/chat/completions"
	return u.String(), nil
}

func parseChatCompletionSSE(ctx context.Context, body io.ReadCloser, out chan<- ProviderEvent, validateArguments bool) {
	parseChatCompletionSSEWithPolicy(ctx, body, out, validateArguments, 0, "", "", nil)
}

func parseChatCompletionSSEWithIdleTimeout(ctx context.Context, body io.ReadCloser, out chan<- ProviderEvent, validateArguments bool, idleTimeout time.Duration) {
	parseChatCompletionSSEWithPolicy(ctx, body, out, validateArguments, idleTimeout, "", "", nil)
}

func parseChatCompletionSSEWithPolicy(ctx context.Context, body io.ReadCloser, out chan<- ProviderEvent, validateArguments bool, idleTimeout time.Duration, reasoningPath, cachedTokensPath string, status func(string)) {
	parseChatCompletionSSEWithPolicyAndSources(ctx, body, out, validateArguments, idleTimeout, reasoningPath, cachedTokensPath, status, SourcePolicy{})
}

func parseChatCompletionSSEWithPolicyAndSources(ctx context.Context, body io.ReadCloser, out chan<- ProviderEvent, validateArguments bool, idleTimeout time.Duration, reasoningPath, cachedTokensPath string, status func(string), sourcePolicy SourcePolicy) {
	parseChatCompletionSSEWithProgressAndSources(ctx, body, out, validateArguments, idleTimeout, 0, 0, 0, "transport-only", reasoningPath, cachedTokensPath, "", status, sourcePolicy, "")
}

func parseChatCompletionSSEWithProgressAndSources(ctx context.Context, body io.ReadCloser, out chan<- ProviderEvent, validateArguments bool, idleTimeout, firstEventTimeout, semanticProgressTimeout, maxDuration time.Duration, heartbeatMode, reasoningPath, cachedTokensPath, reasoningTokensPath string, status func(string), sourcePolicy SourcePolicy, plainTextToolCall ...string) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	sawDone := false
	finishReason := ""
	inlineThink := newInlineThinkParser()
	toolArguments := map[int]*strings.Builder{}
	progress := watchUpstreamProgress(ctx, body, idleTimeout, firstEventTimeout, semanticProgressTimeout, maxDuration)
	defer progress.stop()
	for scanner.Scan() {
		progress.touchTransport()
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			if strings.HasPrefix(line, ":") && strings.EqualFold(strings.TrimSpace(heartbeatMode), "semantic") {
				progress.touchSemantic()
			}
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			if validateArguments {
				for index, arguments := range toolArguments {
					if !json.Valid([]byte(strings.TrimSpace(arguments.String()))) {
						out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("tool call %d returned invalid JSON arguments", index)}
						return
					}
				}
			}
			sawDone = true
			emitSplitText(out, inlineThink.flush())
			out <- ProviderEvent{Kind: ProviderEventDone, FinishReason: finishReason}
			return
		}
		var chunk chatCompletionChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("invalid chat completion SSE chunk: %w", err)}
			return
		}
		for _, choice := range chunk.Choices {
			if strings.TrimSpace(choice.FinishReason) != "" {
				finishReason = strings.TrimSpace(choice.FinishReason)
			}
			reasoning := firstNonEmptyJSONPath(choice.Delta.Raw, reasoningPath, choice.Delta.ReasoningContent, choice.Delta.Reasoning)
			if reasoning != "" {
				progress.touchSemantic()
				out <- ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: reasoning}
			}
			if choice.Delta.Content != "" {
				if plainTextToolCallRejected(plainTextToolCall, choice.Delta.Content) {
					out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned plain-text tool call while policy rejects plain-text tool calls")}
					return
				}
				progress.touchSemantic()
				emitSplitText(out, inlineThink.feed(choice.Delta.Content))
			}
			if strings.EqualFold(strings.TrimSpace(sourcePolicy.Mode), "unsupported") && hasSourceURLs(choice.Delta.Raw) {
				out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned source citations but catalog marks sources unsupported")}
				return
			}
			if sourcePolicy.RequireURL && hasSourceMetadata(choice.Delta.Raw) && !hasSourceURLs(choice.Delta.Raw) {
				out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("provider returned source metadata without a URL")}
				return
			}
			if strings.EqualFold(strings.TrimSpace(sourcePolicy.Mode), "text") {
				if text := sourceTextFromValue(choice.Delta.Raw); text != "" {
					out <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: text}
				}
			}
			for _, source := range sourceCitationsFromValue(choice.Delta.Raw, sourcePolicy) {
				progress.touchSemantic()
				out <- ProviderEvent{Kind: ProviderEventSource, Source: &source}
			}
			for _, toolCall := range choice.Delta.ToolCalls {
				progress.touchSemantic()
				arguments := toolArguments[toolCall.Index]
				if arguments == nil {
					arguments = &strings.Builder{}
					toolArguments[toolCall.Index] = arguments
				}
				arguments.WriteString(toolCall.Function.Arguments)
				out <- ProviderEvent{
					Kind: ProviderEventToolCallDelta,
					ToolCall: &ProviderToolCallDelta{
						Index:          toolCall.Index,
						ID:             toolCall.ID,
						Name:           toolCall.Function.Name,
						Namespace:      firstNonEmpty(toolCall.Namespace, toolCall.Function.Namespace),
						ArgumentsDelta: toolCall.Function.Arguments,
					},
				}
			}
		}
		if chunk.Usage != nil {
			progress.touchSemantic()
			out <- ProviderEvent{Kind: ProviderEventUsage, Usage: chunk.Usage.toUsage(cachedTokensPath, status, reasoningTokensPath)}
		}
	}
	select {
	case timeout := <-progress.expired:
		out <- ProviderEvent{Kind: ProviderEventError, Err: timeout}
		return
	default:
	}
	if ctx.Err() != nil {
		return
	}
	if err := scanner.Err(); err != nil {
		out <- ProviderEvent{Kind: ProviderEventError, Err: err}
		return
	}
	if !sawDone {
		out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("upstream chat stream ended before [DONE]")}
	}
}

func plainTextToolCallRejected(policy []string, text string) bool {
	if len(policy) == 0 || !strings.EqualFold(strings.TrimSpace(policy[0]), "reject") {
		return false
	}
	return strings.Contains(strings.ToLower(text), "<tool_call>")
}

func watchUpstreamStreamIdle(ctx context.Context, body io.Closer, configured time.Duration) (<-chan struct{}, func(), func()) {
	timeout := effectiveUpstreamStreamIdleTimeout(configured)
	expired := make(chan struct{}, 1)
	if timeout <= 0 {
		return expired, func() {}, func() {}
	}
	activity := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = body.Close()
				return
			case <-done:
				return
			case <-activity:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(timeout)
			case <-timer.C:
				select {
				case expired <- struct{}{}:
				default:
				}
				_ = body.Close()
				return
			}
		}
	}()
	touch := func() {
		select {
		case activity <- struct{}{}:
		default:
		}
	}
	stop := func() {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	return expired, touch, stop
}

func effectiveUpstreamStreamIdleTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return upstreamHTTPStreamIdleTimeout
}

type chatCompletionChunk struct {
	Choices []struct {
		FinishReason string              `json:"finish_reason"`
		Delta        chatCompletionDelta `json:"delta"`
	} `json:"choices"`
	Usage *chatUsage `json:"usage"`
}

type chatCompletionDelta struct {
	Content          string              `json:"content"`
	ReasoningContent string              `json:"reasoning_content"`
	Reasoning        string              `json:"reasoning"`
	ToolCalls        []chatToolCallDelta `json:"tool_calls"`
	Raw              map[string]any      `json:"-"`
}

type chatToolCallDelta struct {
	Index     int    `json:"index"`
	ID        string `json:"id"`
	Type      string `json:"type"`
	Namespace string `json:"namespace,omitempty"`
	Function  struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
		Namespace string `json:"namespace,omitempty"`
	} `json:"function"`
}

type chatUsage struct {
	PromptTokens        int `json:"prompt_tokens"`
	CompletionTokens    int `json:"completion_tokens"`
	TotalTokens         int `json:"total_tokens"`
	PromptTokensDetails struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionTokensDetails struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
	Raw map[string]any `json:"-"`
}

func (u *chatUsage) UnmarshalJSON(data []byte) error {
	type alias chatUsage
	var decoded alias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*u = chatUsage(decoded)
	return json.Unmarshal(data, &u.Raw)
}

func (d *chatCompletionDelta) UnmarshalJSON(data []byte) error {
	type alias chatCompletionDelta
	var decoded alias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*d = chatCompletionDelta(decoded)
	return json.Unmarshal(data, &d.Raw)
}

func (u chatUsage) toUsage(cachedTokensPath string, status func(string), reasoningTokensPath ...string) *Usage {
	result := &Usage{
		InputTokens:     u.PromptTokens,
		OutputTokens:    u.CompletionTokens,
		TotalTokens:     u.TotalTokens,
		CachedTokens:    u.PromptTokensDetails.CachedTokens,
		ReasoningTokens: u.CompletionTokensDetails.ReasoningTokens,
	}
	if value, ok := intAtJSONPath(u.Raw, cachedTokensPath, "usage"); ok {
		result.CachedTokens = value
	}
	if len(reasoningTokensPath) > 0 {
		if value, ok := intAtJSONPath(u.Raw, reasoningTokensPath[0], "usage"); ok {
			result.ReasoningTokens = value
		}
	}
	if result.CachedTokens < 0 {
		warnUsage(status, "upstream reported negative cached token count; clamped to zero")
		result.CachedTokens = 0
	}
	if result.InputTokens > 0 && result.CachedTokens > result.InputTokens {
		warnUsage(status, fmt.Sprintf("upstream reported cached tokens %d greater than input tokens %d; clamped", result.CachedTokens, result.InputTokens))
		result.CachedTokens = result.InputTokens
	}
	if result.ReasoningTokens < 0 {
		warnUsage(status, "upstream reported negative reasoning token count; clamped to zero")
		result.ReasoningTokens = 0
	}
	if result.OutputTokens > 0 && result.ReasoningTokens > result.OutputTokens {
		warnUsage(status, fmt.Sprintf("upstream reported reasoning tokens %d greater than output tokens %d; clamped", result.ReasoningTokens, result.OutputTokens))
		result.ReasoningTokens = result.OutputTokens
	}
	return result
}

type ProviderTimeoutKind string

const (
	ProviderTimeoutTransportIdle    ProviderTimeoutKind = "transport_idle_timeout"
	ProviderTimeoutFirstEvent       ProviderTimeoutKind = "first_event_timeout"
	ProviderTimeoutSemanticProgress ProviderTimeoutKind = "semantic_progress_timeout"
	ProviderTimeoutDeadline         ProviderTimeoutKind = "deadline_exceeded"
)

type ProviderTimeoutError struct {
	Kind     ProviderTimeoutKind
	Duration time.Duration
}

func (e ProviderTimeoutError) Error() string {
	switch e.Kind {
	case ProviderTimeoutTransportIdle:
		return fmt.Sprintf("upstream chat stream idle timeout after %s", e.Duration)
	case ProviderTimeoutFirstEvent:
		return fmt.Sprintf("upstream chat stream first semantic event timeout after %s", e.Duration)
	case ProviderTimeoutSemanticProgress:
		return fmt.Sprintf("upstream chat stream semantic progress timeout after %s", e.Duration)
	case ProviderTimeoutDeadline:
		return fmt.Sprintf("upstream chat stream hard deadline exceeded after %s", e.Duration)
	default:
		return fmt.Sprintf("upstream chat stream timeout after %s", e.Duration)
	}
}

type upstreamProgressWatch struct {
	expired        <-chan ProviderTimeoutError
	touchTransport func()
	touchSemantic  func()
	stop           func()
}

func watchUpstreamProgress(ctx context.Context, body io.Closer, idleTimeout, firstEventTimeout, semanticTimeout, maxDuration time.Duration) upstreamProgressWatch {
	idleTimeout = effectiveUpstreamStreamIdleTimeout(idleTimeout)
	if firstEventTimeout <= 0 && semanticTimeout <= 0 && maxDuration <= 0 && idleTimeout <= 0 {
		return upstreamProgressWatch{expired: make(chan ProviderTimeoutError), touchTransport: func() {}, touchSemantic: func() {}, stop: func() {}}
	}
	expired := make(chan ProviderTimeoutError, 1)
	transport := make(chan struct{}, 1)
	semantic := make(chan struct{}, 1)
	stop := make(chan struct{})
	go func() {
		start := time.Now()
		lastTransport := start
		lastSemantic := start
		seenSemantic := false
		interval := 25 * time.Millisecond
		if maxDuration > 0 && maxDuration < interval {
			interval = maxDuration / 4
		}
		if interval <= 0 {
			interval = time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		timeout := func(kind ProviderTimeoutKind, duration time.Duration) {
			select {
			case expired <- ProviderTimeoutError{Kind: kind, Duration: duration}:
			default:
			}
			_ = body.Close()
		}
		for {
			select {
			case <-ctx.Done():
				_ = body.Close()
				return
			case <-stop:
				return
			case <-transport:
				lastTransport = time.Now()
			case <-semantic:
				lastSemantic = time.Now()
				seenSemantic = true
			case now := <-ticker.C:
				switch {
				case maxDuration > 0 && now.Sub(start) >= maxDuration:
					timeout(ProviderTimeoutDeadline, maxDuration)
					return
				case firstEventTimeout > 0 && !seenSemantic && now.Sub(start) >= firstEventTimeout:
					timeout(ProviderTimeoutFirstEvent, firstEventTimeout)
					return
				case semanticTimeout > 0 && seenSemantic && now.Sub(lastSemantic) >= semanticTimeout:
					timeout(ProviderTimeoutSemanticProgress, semanticTimeout)
					return
				case idleTimeout > 0 && now.Sub(lastTransport) >= idleTimeout:
					timeout(ProviderTimeoutTransportIdle, idleTimeout)
					return
				}
			}
		}
	}()
	touchTransport := func() {
		select {
		case transport <- struct{}{}:
		default:
		}
	}
	touchSemantic := func() {
		touchTransport()
		select {
		case semantic <- struct{}{}:
		default:
		}
	}
	stopFn := func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}
	return upstreamProgressWatch{expired: expired, touchTransport: touchTransport, touchSemantic: touchSemantic, stop: stopFn}
}

func warnUsage(status func(string), message string) {
	if status != nil {
		status("usage warning: " + message)
	}
}

func firstNonEmptyJSONPath(raw map[string]any, path string, fallbacks ...string) string {
	if value, ok := valueAtJSONPath(raw, path, "choices.0.delta", "choices.0.message", "delta", "message"); ok {
		if text, ok := value.(string); ok && text != "" {
			return text
		}
	}
	for _, value := range fallbacks {
		if value != "" {
			return value
		}
	}
	return ""
}

func intAtJSONPath(raw map[string]any, path string, prefixes ...string) (int, bool) {
	value, ok := valueAtJSONPath(raw, path, prefixes...)
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return int(typed), typed == float64(int(typed))
	case json.Number:
		integer, err := typed.Int64()
		return int(integer), err == nil
	case int:
		return typed, true
	case int64:
		return int(typed), true
	default:
		return 0, false
	}
}

func valueAtJSONPath(raw map[string]any, path string, prefixes ...string) (any, bool) {
	path = strings.TrimSpace(path)
	if path == "" || len(raw) == 0 {
		return nil, false
	}
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	path = strings.ReplaceAll(path, "[", ".")
	path = strings.ReplaceAll(path, "]", "")
	path = strings.ReplaceAll(path, "/", ".")
	for _, prefix := range prefixes {
		prefix = strings.Trim(strings.TrimSpace(prefix), ".")
		if strings.EqualFold(path, prefix) {
			path = ""
			break
		}
		if len(path) > len(prefix) && strings.EqualFold(path[:len(prefix)], prefix) && path[len(prefix)] == '.' {
			path = path[len(prefix)+1:]
			break
		}
	}
	if path == "" {
		return raw, true
	}
	var current any = raw
	for _, segment := range strings.Split(path, ".") {
		if segment == "" {
			continue
		}
		switch typed := current.(type) {
		case map[string]any:
			var found bool
			for key, value := range typed {
				if strings.EqualFold(key, segment) {
					current, found = value, true
					break
				}
			}
			if !found {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, false
			}
			current = typed[index]
		default:
			return nil, false
		}
	}
	return current, true
}

type splitText struct {
	reasoning string
	text      string
}

type inlineThinkParser struct {
	buffer  string
	inThink bool
}

func newInlineThinkParser() *inlineThinkParser {
	return &inlineThinkParser{}
}

func (p *inlineThinkParser) feed(delta string) splitText {
	p.buffer += delta
	return p.drain(false)
}

func (p *inlineThinkParser) flush() splitText {
	return p.drain(true)
}

func (p *inlineThinkParser) drain(flush bool) splitText {
	var out splitText
	for p.buffer != "" {
		if p.inThink {
			idx := strings.Index(p.buffer, "</think>")
			if idx < 0 {
				consume := len(p.buffer)
				if !flush {
					consume -= longestSuffixPrefix(p.buffer, "</think>")
				}
				if consume <= 0 {
					return out
				}
				out.reasoning += p.buffer[:consume]
				p.buffer = p.buffer[consume:]
				continue
			}
			out.reasoning += p.buffer[:idx]
			p.buffer = p.buffer[idx+len("</think>"):]
			p.inThink = false
			continue
		}
		idx := strings.Index(p.buffer, "<think>")
		if idx < 0 {
			consume := len(p.buffer)
			if !flush {
				consume -= longestSuffixPrefix(p.buffer, "<think>")
			}
			if consume <= 0 {
				return out
			}
			out.text += p.buffer[:consume]
			p.buffer = p.buffer[consume:]
			continue
		}
		out.text += p.buffer[:idx]
		p.buffer = p.buffer[idx+len("<think>"):]
		p.inThink = true
	}
	return out
}

func emitSplitText(out chan<- ProviderEvent, split splitText) {
	if split.reasoning != "" {
		out <- ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: split.reasoning}
	}
	if split.text != "" {
		out <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: split.text}
	}
}

func sourceCitationsFromValue(value any, policy SourcePolicy) []SourceCitation {
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

// sourceTextFromValue extracts provider search-result text only when the raw
// payload contains an explicit source/search marker. This prevents text-mode
// conversion from duplicating the assistant's ordinary message content.
func sourceTextFromValue(value any) string {
	var parts []string
	var walk func(any, bool)
	walk = func(current any, inSource bool) {
		switch typed := current.(type) {
		case []any:
			for _, item := range typed {
				walk(item, inSource)
			}
		case map[string]any:
			marked := inSource
			for key, child := range typed {
				lower := strings.ToLower(strings.TrimSpace(key))
				if lower == "type" {
					if typeName, ok := child.(string); ok && strings.Contains(strings.ToLower(typeName), "search") {
						marked = true
					}
				}
				switch lower {
				case "annotations", "annotation", "sources", "source", "citations", "citation", "web_search_results", "web_search_tool_result":
					marked = true
				}
			}
			for key, child := range typed {
				lower := strings.ToLower(strings.TrimSpace(key))
				if marked {
					switch lower {
					case "text", "snippet", "content", "title", "description":
						if text, ok := child.(string); ok && strings.TrimSpace(text) != "" {
							parts = append(parts, text)
							continue
						}
					}
				}
				walk(child, marked)
			}
		}
	}
	walk(value, false)
	return strings.Join(parts, "\n")
}

func hasSourceURLs(value any) bool {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if hasSourceURLs(item) {
				return true
			}
		}
	case map[string]any:
		if urlValue, _ := typed["url"].(string); strings.HasPrefix(strings.TrimSpace(urlValue), "http://") || strings.HasPrefix(strings.TrimSpace(urlValue), "https://") {
			return true
		}
		for _, child := range typed {
			if hasSourceURLs(child) {
				return true
			}
		}
	}
	return false
}

func hasSourceMetadata(value any) bool {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if hasSourceMetadata(item) {
				return true
			}
		}
	case map[string]any:
		for key, child := range typed {
			if strings.EqualFold(strings.TrimSpace(key), "type") {
				if typeName, ok := child.(string); ok && strings.Contains(strings.ToLower(typeName), "search") {
					return true
				}
			}
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "annotations", "annotation", "sources", "source", "citations", "citation", "web_search_results", "web_search_tool_result":
				return true
			}
			if hasSourceMetadata(child) {
				return true
			}
		}
	}
	return false
}

func longestSuffixPrefix(s string, prefixOf string) int {
	max := len(s)
	if len(prefixOf)-1 < max {
		max = len(prefixOf) - 1
	}
	for n := max; n > 0; n-- {
		if strings.HasSuffix(s, prefixOf[:n]) {
			return n
		}
	}
	return 0
}
