package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// WorkflowNotifier posts owner notifications as an Adaptive Card to a
// Teams Workflow ("Post to a chat when a webhook request is received")
// webhook. The URL is pre-signed: no Authorization header is sent. Each
// card is posted at most once; a failure is returned and never retried
// here, because a failed post may still have been delivered.
type WorkflowNotifier struct {
	url  string
	http *http.Client
}

// NewWorkflowNotifier returns a notifier for the webhook url ("" disables it).
func NewWorkflowNotifier(url string, client *http.Client) *WorkflowNotifier {
	if client == nil {
		client = &http.Client{}
	}
	return &WorkflowNotifier{url: strings.TrimSpace(url), http: client}
}

// Available reports whether a webhook is configured.
func (w *WorkflowNotifier) Available() bool { return w != nil && w.url != "" }

// maxCardBytes is the Workflow request size limit (28 KB) with headroom.
const maxCardBytes = 24 << 10

// NotifyFinal posts one "Codex finished" card.
func (w *WorkflowNotifier) NotifyFinal(ctx context.Context, conv, title, text, url string) error {
	if !w.Available() {
		return fmt.Errorf("teams: no workflow webhook configured")
	}
	heading := "Codex finished"
	if title != "" {
		heading += ": " + title
	} else if conv != "" {
		heading += ": " + conv
	}
	body := []any{
		map[string]any{"type": "TextBlock", "text": heading, "weight": "Bolder", "wrap": true},
	}
	if t := strings.TrimSpace(text); t != "" {
		if len(t) > 2000 {
			t = t[:2000] + "…"
		}
		body = append(body, map[string]any{"type": "TextBlock", "text": t, "wrap": true})
	}
	card := map[string]any{
		"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
		"type":    "AdaptiveCard", "version": "1.4", "body": body,
	}
	if url != "" {
		card["actions"] = []any{map[string]any{"type": "Action.OpenUrl", "title": "Open chat", "url": url}}
	}
	msg := map[string]any{
		"type": "message",
		"attachments": []any{map[string]any{
			"contentType": "application/vnd.microsoft.card.adaptive",
			"contentUrl":  nil,
			"content":     card,
		}},
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(raw) > maxCardBytes {
		return fmt.Errorf("teams: workflow card is %d bytes", len(raw))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.http.Do(req)
	if err != nil {
		return fmt.Errorf("teams: workflow webhook: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("teams: workflow webhook answered %s", resp.Status)
	}
	return nil
}
