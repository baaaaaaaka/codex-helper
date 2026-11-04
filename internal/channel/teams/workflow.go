package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
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
	return w.post(ctx, heading, text, url)
}

// NotifyEvent posts a card with heading, text and an "Open chat" button
// (v0.1.20's local_session_started card).
func (w *WorkflowNotifier) NotifyEvent(ctx context.Context, heading, text, url string) error {
	if !w.Available() {
		return fmt.Errorf("teams: no workflow webhook configured")
	}
	return w.post(ctx, heading, text, url)
}

func (w *WorkflowNotifier) post(ctx context.Context, heading, text, url string) error {
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

// ReadWebhookURLFile reads the Workflow webhook URL from the private file
// the owner configured. The URL is a secret (pre-signed), so the file must
// be an absolute, non-symlinked path that only the user can read (0600 or
// stricter outside Windows) under parents nobody else can write, and hold
// one absolute https URL.
func ReadWebhookURLFile(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("workflow webhook URL file is not configured")
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("workflow webhook URL file must be an absolute path")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("workflow webhook URL file must not be a symlink")
	}
	if info.IsDir() {
		return "", fmt.Errorf("workflow webhook URL file must be a file")
	}
	if runtime.GOOS != "windows" {
		if info.Mode().Perm()&0o077 != 0 {
			return "", fmt.Errorf("workflow webhook URL file permissions must be 0600 or stricter")
		}
		if err := webhookParentsPrivate(path); err != nil {
			return "", err
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	u := strings.TrimSpace(string(raw))
	if !SafeWebhookURL(u) {
		return "", fmt.Errorf("workflow webhook URL must be an absolute https URL")
	}
	return u, nil
}

// SafeWebhookURL reports whether raw is an absolute https URL without
// user info or fragment.
func SafeWebhookURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == "" && u.IsAbs()
}

// webhookParentsPrivate checks the parents up to the first private one:
// none is a symlink or writable by group or others.
func webhookParentsPrivate(path string) error {
	dir := filepath.Dir(path)
	for {
		info, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("workflow webhook URL file parent directories must not be symlinks")
		}
		if info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("workflow webhook URL file parent directories must not be group/world-writable")
		}
		if info.Mode().Perm()&0o077 == 0 {
			return nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}
