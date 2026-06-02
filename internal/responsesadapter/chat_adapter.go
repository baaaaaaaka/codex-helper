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
	BaseURL    string
	APIKey     string
	Profile    ProviderProfile
	HTTPClient *http.Client
	MaxRetries int
	RetryBase  time.Duration
}

type chatCompletionRequest struct {
	Model               string             `json:"model"`
	Messages            []chatMessage      `json:"messages"`
	Tools               []ChatTool         `json:"tools,omitempty"`
	ToolChoice          json.RawMessage    `json:"tool_choice,omitempty"`
	ParallelToolCalls   *bool              `json:"parallel_tool_calls,omitempty"`
	MaxCompletionTokens *int               `json:"max_completion_tokens,omitempty"`
	ReasoningEffort     string             `json:"reasoning_effort,omitempty"`
	Thinking            *thinkingConfig    `json:"thinking,omitempty"`
	Temperature         *float64           `json:"temperature,omitempty"`
	TopP                *float64           `json:"top_p,omitempty"`
	Stream              bool               `json:"stream"`
	StreamOptions       *chatStreamOptions `json:"stream_options,omitempty"`
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
}

type chatImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
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

func (a OpenAIChatAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	profile := a.Profile.withDefaults()
	body := chatCompletionRequest{
		Model:               req.Model,
		Messages:            chatMessagesFromProviderRequestWithProfile(req, profile),
		Tools:               req.Tools,
		ToolChoice:          chatToolChoice(req.ToolChoice, profile),
		ParallelToolCalls:   req.ParallelToolCalls,
		MaxCompletionTokens: req.MaxOutputTokens,
		ReasoningEffort:     profile.reasoningEffort(req.ReasoningEffort),
		Temperature:         req.Temperature,
		TopP:                req.TopP,
		Stream:              true,
	}
	if profile.ForceParallelToolCalls != nil {
		body.ParallelToolCalls = profile.ForceParallelToolCalls
	}
	if profile.shouldEnableThinking(req.Model) {
		body.Thinking = &thinkingConfig{Type: "enabled"}
	}
	if profile.shouldStripSampling(req.Model) {
		body.Temperature = nil
		body.TopP = nil
	}
	if profile.IncludeUsageStreamOptions {
		body.StreamOptions = &chatStreamOptions{IncludeUsage: true}
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	endpoint, err := chatCompletionsURL(a.BaseURL)
	if err != nil {
		return nil, err
	}
	client := a.HTTPClient
	if client == nil {
		client = defaultUpstreamHTTPClient()
	}
	resp, err := a.doChatCompletionsRequest(ctx, client, endpoint, payload)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, upstreamHTTPError(resp.StatusCode, resp.Status, strings.TrimSpace(string(raw)), req.Model)
	}
	ch := make(chan ProviderEvent)
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		parseChatCompletionSSE(ctx, resp.Body, ch)
	}()
	return ch, nil
}

func (a OpenAIChatAdapter) doChatCompletionsRequest(ctx context.Context, client *http.Client, endpoint string, payload []byte) (*http.Response, error) {
	maxRetries := a.MaxRetries
	if maxRetries == 0 {
		maxRetries = 2
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := 0; ; attempt++ {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if strings.TrimSpace(a.APIKey) != "" {
			httpReq.Header.Set("Authorization", "Bearer "+strings.TrimSpace(a.APIKey))
		}
		resp, err := client.Do(httpReq)
		if err == nil && !shouldRetryStatus(resp.StatusCode) {
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

func shouldRetryStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout || status >= 500
}

func (a OpenAIChatAdapter) retryDelay(attempt int, resp *http.Response) time.Duration {
	if resp != nil {
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
		}
	}
	if !hasText && hasChatImagePart(out) {
		out = append([]chatContentPart{{Type: "text", Text: "Please analyze the attached image."}}, out...)
	}
	if !hasChatImagePart(out) {
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

func parseChatCompletionSSE(ctx context.Context, body io.ReadCloser, out chan<- ProviderEvent) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	sawDone := false
	inlineThink := newInlineThinkParser()
	idleExpired, touchIdleWatch, stopIdleWatch := watchUpstreamStreamIdle(ctx, body)
	defer stopIdleWatch()
	for scanner.Scan() {
		touchIdleWatch()
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			sawDone = true
			emitSplitText(out, inlineThink.flush())
			out <- ProviderEvent{Kind: ProviderEventDone}
			return
		}
		var chunk chatCompletionChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("invalid chat completion SSE chunk: %w", err)}
			return
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.ReasoningContent != "" {
				out <- ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: choice.Delta.ReasoningContent}
			}
			if choice.Delta.Reasoning != "" {
				out <- ProviderEvent{Kind: ProviderEventReasoningDelta, Delta: choice.Delta.Reasoning}
			}
			if choice.Delta.Content != "" {
				emitSplitText(out, inlineThink.feed(choice.Delta.Content))
			}
			for _, toolCall := range choice.Delta.ToolCalls {
				out <- ProviderEvent{
					Kind: ProviderEventToolCallDelta,
					ToolCall: &ProviderToolCallDelta{
						Index:          toolCall.Index,
						ID:             toolCall.ID,
						Name:           toolCall.Function.Name,
						ArgumentsDelta: toolCall.Function.Arguments,
					},
				}
			}
		}
		if chunk.Usage != nil {
			out <- ProviderEvent{Kind: ProviderEventUsage, Usage: chunk.Usage.toUsage()}
		}
	}
	select {
	case <-idleExpired:
		out <- ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("upstream chat stream idle timeout after %s", upstreamHTTPStreamIdleTimeout)}
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

func watchUpstreamStreamIdle(ctx context.Context, body io.Closer) (<-chan struct{}, func(), func()) {
	timeout := upstreamHTTPStreamIdleTimeout
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

type chatCompletionChunk struct {
	Choices []struct {
		Delta struct {
			Content          string              `json:"content"`
			ReasoningContent string              `json:"reasoning_content"`
			Reasoning        string              `json:"reasoning"`
			ToolCalls        []chatToolCallDelta `json:"tool_calls"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *chatUsage `json:"usage"`
}

type chatToolCallDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
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
}

func (u chatUsage) toUsage() *Usage {
	return &Usage{
		InputTokens:     u.PromptTokens,
		OutputTokens:    u.CompletionTokens,
		TotalTokens:     u.TotalTokens,
		CachedTokens:    u.PromptTokensDetails.CachedTokens,
		ReasoningTokens: u.CompletionTokensDetails.ReasoningTokens,
	}
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
