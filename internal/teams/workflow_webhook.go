package teams

import (
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

const (
	MaxWorkflowWebhookPayloadBytes = 28 * 1024
	workflowWebhookRetries         = 3
)

type WorkflowWebhookMessage struct {
	Title          string
	Text           string
	ChatTitle      string
	RequestSummary string
	Hint           string
	Footer         string
	Actions        []OpenURLCardAction
}

type WorkflowWebhookResult struct {
	StatusCode int
	Body       string
}

func PostWorkflowWebhook(ctx context.Context, client *http.Client, webhookURL string, msg WorkflowWebhookMessage) (WorkflowWebhookResult, error) {
	webhookURL = strings.TrimSpace(webhookURL)
	if webhookURL == "" {
		return WorkflowWebhookResult{}, fmt.Errorf("webhook URL is required")
	}
	if !safeWorkflowWebhookURL(webhookURL) {
		return WorkflowWebhookResult{}, fmt.Errorf("webhook URL must be an absolute https URL")
	}
	payload, err := workflowWebhookPayload(msg)
	if err != nil {
		return WorkflowWebhookResult{}, err
	}
	if client == nil {
		client = http.DefaultClient
	}
	var last WorkflowWebhookResult
	for attempt := 0; attempt < workflowWebhookRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payload))
		if err != nil {
			return WorkflowWebhookResult{}, fmt.Errorf("create Teams workflow webhook request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return WorkflowWebhookResult{}, fmt.Errorf("Teams workflow webhook request failed")
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		last = WorkflowWebhookResult{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(body))}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return last, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == workflowWebhookRetries-1 {
			return last, fmt.Errorf("Teams workflow webhook failed: HTTP %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		if err := sleepContext(ctx, workflowWebhookRetryDelay(resp, attempt)); err != nil {
			return last, err
		}
	}
	return last, fmt.Errorf("Teams workflow webhook failed after retries")
}

func safeWorkflowWebhookURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	if parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return false
	}
	return parsed.IsAbs()
}

func workflowWebhookPayload(msg WorkflowWebhookMessage) ([]byte, error) {
	title := strings.TrimSpace(msg.Title)
	if title == "" {
		title = "Codex helper test"
	}
	text := strings.TrimSpace(msg.Text)
	if text == "" {
		text = "Codex helper workflow webhook test."
	}
	payload := map[string]any{
		"type": "message",
		"attachments": []map[string]any{
			{
				"contentType": "application/vnd.microsoft.card.adaptive",
				"contentUrl":  nil,
				"content": map[string]any{
					"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
					"type":    "AdaptiveCard",
					"version": "1.2",
				},
			},
		},
	}
	content := payload["attachments"].([]map[string]any)[0]["content"].(map[string]any)
	body := []map[string]any{{
		"type":   "TextBlock",
		"text":   title,
		"weight": "Bolder",
		"size":   "Medium",
		"wrap":   true,
	}}
	if chatTitle := strings.TrimSpace(msg.ChatTitle); chatTitle != "" {
		body = append(body,
			map[string]any{"type": "TextBlock", "text": "Chat", "isSubtle": true, "spacing": "Medium", "wrap": true},
			map[string]any{"type": "TextBlock", "text": chatTitle, "weight": "Bolder", "spacing": "None", "wrap": true},
		)
	}
	if requestSummary := strings.TrimSpace(msg.RequestSummary); requestSummary != "" {
		body = append(body,
			map[string]any{"type": "TextBlock", "text": "Request", "isSubtle": true, "spacing": "Medium", "wrap": true},
			map[string]any{"type": "TextBlock", "text": requestSummary, "spacing": "None", "wrap": true},
		)
	}
	if hint := strings.TrimSpace(msg.Hint); hint != "" {
		text = hint
	}
	if text != "" {
		body = append(body, map[string]any{"type": "TextBlock", "text": text, "wrap": true, "spacing": "Medium"})
	}
	if footer := strings.TrimSpace(msg.Footer); footer != "" {
		body = append(body, map[string]any{"type": "TextBlock", "text": footer, "isSubtle": true, "spacing": "Small", "wrap": true})
	}
	content["body"] = body
	if len(msg.Actions) > 0 {
		actions := make([]map[string]any, 0, len(msg.Actions))
		for _, action := range msg.Actions {
			title := strings.TrimSpace(action.Title)
			actionURL := strings.TrimSpace(action.URL)
			if title == "" || actionURL == "" {
				return nil, fmt.Errorf("Teams workflow card action title and URL are required")
			}
			if !safeTeamsOpenURL(actionURL) {
				return nil, fmt.Errorf("refusing unsafe Teams workflow card URL")
			}
			actions = append(actions, map[string]any{
				"type":  "Action.OpenUrl",
				"title": title,
				"url":   actionURL,
			})
		}
		content["actions"] = actions
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxWorkflowWebhookPayloadBytes {
		return nil, fmt.Errorf("Teams workflow webhook payload is %d bytes; maximum is %d bytes", len(raw), MaxWorkflowWebhookPayloadBytes)
	}
	return raw, nil
}

func workflowWebhookRetryDelay(resp *http.Response, attempt int) time.Duration {
	if value := strings.TrimSpace(resp.Header.Get("Retry-After")); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	delay := time.Duration(1<<attempt) * 500 * time.Millisecond
	if delay > 5*time.Second {
		return 5 * time.Second
	}
	return delay
}
