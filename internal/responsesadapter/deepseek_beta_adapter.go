package responsesadapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DeepSeekBetaAdapter handles the typed Beta/FIM conversion profile. Normal
// chat requests deliberately reuse the tested OpenAI Chat adapter; prefix/FIM
// requests use the completion wire shape and never send chat history.
type DeepSeekBetaAdapter struct {
	Chat                  OpenAIChatAdapter
	BaseURL               string
	APIKey                string
	Headers               map[string]string
	Endpoints             map[string]string
	AuthType              string
	AuthHeader            string
	HTTPClient            *http.Client
	StreamMode            string
	MaxOutputTokens       int
	ResponseHeaderTimeout time.Duration
	StreamIdleTimeout     time.Duration
	Status                func(string)
	RequestGate           chan struct{}
}

type deepSeekFIMRequest struct {
	Model       string   `json:"model"`
	Prompt      string   `json:"prompt"`
	Suffix      string   `json:"suffix,omitempty"`
	MaxTokens   *int     `json:"max_tokens,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
	TopP        *float64 `json:"top_p,omitempty"`
	Stream      bool     `json:"stream"`
}

func (a DeepSeekBetaAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	if err := validateProviderResponseFields(req, "DeepSeek Beta"); err != nil {
		return nil, err
	}
	operation := strings.ToLower(strings.TrimSpace(req.Operation))
	if operation == "" || operation == "chat" || operation == "responses" {
		return a.Chat.Stream(ctx, req)
	}
	if operation != "prefix" && operation != "fim" {
		return nil, fmt.Errorf("deepseek beta converter does not support operation %q", req.Operation)
	}
	if len(req.ResponseFormat) > 0 {
		return nil, fmt.Errorf("DeepSeek Beta %s operation does not support response_format", operation)
	}
	return a.streamFIM(ctx, req)
}

func (a DeepSeekBetaAdapter) streamFIM(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	prompt := req.Prefix
	if prompt == "" {
		prompt = req.InputText
	}
	if strings.TrimSpace(prompt) == "" {
		return nil, fmt.Errorf("deepseek beta %s operation requires prefix or input", req.Operation)
	}
	body, err := json.Marshal(deepSeekFIMRequest{Model: req.Model, Prompt: prompt, Suffix: req.Suffix, MaxTokens: req.MaxOutputTokens, Temperature: req.Temperature, TopP: req.TopP, Stream: !strings.EqualFold(strings.TrimSpace(a.StreamMode), "nonstream-buffered")})
	if err != nil {
		return nil, err
	}
	endpoint, err := anthropicEndpoint(a.BaseURL, a.Endpoints, "fim", "/completions")
	if err != nil {
		return nil, err
	}
	client := a.HTTPClient
	if client == nil {
		client = NewUpstreamHTTPClientWithResponseHeaderTimeout(http.ProxyFromEnvironment, a.ResponseHeaderTimeout)
	}
	if a.RequestGate != nil {
		if a.Status != nil {
			a.Status("waiting for DeepSeek Beta upstream concurrency slot")
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
	resp, err := a.doFIMRequest(ctx, client, endpoint, body, req.Model)
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
		if strings.EqualFold(strings.TrimSpace(a.StreamMode), "nonstream-buffered") {
			parseDeepSeekFIMBuffered(ctx, resp.Body, out)
			return
		}
		parseDeepSeekFIMSSEWithPolicy(ctx, resp.Body, out, a.StreamIdleTimeout)
	}()
	return out, nil
}

func (a DeepSeekBetaAdapter) doFIMRequest(ctx context.Context, client *http.Client, endpoint string, payload []byte, model string) (*http.Response, error) {
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
	if strings.TrimSpace(a.APIKey) != "" {
		if strings.EqualFold(strings.TrimSpace(a.AuthType), "header") {
			header := a.AuthHeader
			if strings.TrimSpace(header) == "" {
				header = "api-key"
			}
			req.Header.Set(header, a.APIKey)
		} else {
			req.Header.Set("Authorization", "Bearer "+a.APIKey)
		}
	}
	if a.Status != nil {
		a.Status("waiting for DeepSeek Beta response headers")
	}
	return client.Do(req)
}

type deepSeekFIMResponse struct {
	Choices []struct {
		Text         string `json:"text"`
		FinishReason string `json:"finish_reason"`
		Delta        struct {
			Text    string `json:"text"`
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *chatUsage `json:"usage"`
}

func parseDeepSeekFIMBuffered(ctx context.Context, body io.Reader, out chan<- ProviderEvent) {
	var response deepSeekFIMResponse
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("invalid DeepSeek Beta response: %w", err)})
		return
	}
	if len(response.Choices) == 0 {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("DeepSeek Beta response has no choices")})
		return
	}
	choice := response.Choices[0]
	text := choice.Text
	if text == "" {
		text = firstNonEmpty(choice.Delta.Text, choice.Delta.Content)
	}
	if text != "" {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventTextDelta, Delta: text})
	}
	if response.Usage != nil {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventUsage, Usage: response.Usage.toUsage("", nil)})
	}
	sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventDone, FinishReason: choice.FinishReason})
}

func parseDeepSeekFIMSSE(ctx context.Context, body io.Reader, out chan<- ProviderEvent) {
	parseDeepSeekFIMSSEWithPolicy(ctx, io.NopCloser(body), out, 0)
}

func parseDeepSeekFIMSSEWithPolicy(ctx context.Context, body io.ReadCloser, out chan<- ProviderEvent, idleTimeout time.Duration) {
	defer body.Close()
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	finishReason := ""
	sawDone := false
	idleExpired, touchIdleWatch, stopIdleWatch := watchUpstreamStreamIdle(ctx, body, idleTimeout)
	defer stopIdleWatch()
	for scanner.Scan() {
		touchIdleWatch()
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" {
			continue
		}
		if data == "[DONE]" {
			sawDone = true
			break
		}
		var response deepSeekFIMResponse
		if err := json.Unmarshal([]byte(data), &response); err != nil {
			sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("invalid DeepSeek Beta SSE event: %w", err)})
			return
		}
		if len(response.Choices) == 0 {
			continue
		}
		choice := response.Choices[0]
		text := firstNonEmpty(choice.Delta.Text, choice.Delta.Content, choice.Text)
		if text != "" {
			sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventTextDelta, Delta: text})
		}
		if response.Usage != nil {
			sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventUsage, Usage: response.Usage.toUsage("", nil)})
		}
		if choice.FinishReason != "" {
			finishReason = choice.FinishReason
		}
	}
	if err := scanner.Err(); err != nil {
		select {
		case <-idleExpired:
			sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("upstream DeepSeek Beta stream idle timeout after %s", effectiveUpstreamStreamIdleTimeout(idleTimeout))})
			return
		default:
		}
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("DeepSeek Beta stream read failed: %w", err)})
		return
	}
	select {
	case <-idleExpired:
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("upstream DeepSeek Beta stream idle timeout after %s", effectiveUpstreamStreamIdleTimeout(idleTimeout))})
		return
	default:
	}
	if !sawDone {
		sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventError, Err: fmt.Errorf("DeepSeek Beta stream ended before [DONE]")})
		return
	}
	sendProviderEvent(ctx, out, ProviderEvent{Kind: ProviderEventDone, FinishReason: finishReason})
}
