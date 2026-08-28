package teams

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The message-poll path must report a bounded response as a size failure. A
// truncated JSON document is indistinguishable from a corrupt Graph response
// to the caller and can incorrectly poison per-chat recovery state.
func TestGraphOversizedJSONResponseIsNotMisclassifiedAsCorruptJSON(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"u1","padding":"` + strings.Repeat("x", 8<<20) + `"}`))
	}))
	t.Cleanup(server.Close)

	graph := newTestGraphClient(auth, server, nil)
	_, err := graph.ListMessagesWindow(context.Background(), "chat-oversized", 50, time.Time{})
	if err == nil {
		t.Fatal("oversized JSON response unexpectedly succeeded")
	}
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		t.Fatalf("oversized Graph response was misclassified as JSON corruption: %v", err)
	}
	var sizeErr *GraphResponseTooLargeError
	if !errors.As(err, &sizeErr) {
		t.Fatalf("oversized Graph response error = %T %v, want typed response-size error", err, err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "exceed") {
		t.Fatalf("oversized Graph response error = %v, want a response-size error", err)
	}
}

func TestGraphOversizedErrorPreservesHTTPStatusAndRetryAfter(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":"TooLarge","message":"` + strings.Repeat("x", 8<<20) + `"}}`))
	}))
	t.Cleanup(server.Close)

	graph := newTestGraphClient(auth, server, nil)
	_, err := graph.ListMessagesWindowWithoutRateLimitRetry(context.Background(), "chat-oversized-status", 50, time.Time{})
	if err == nil {
		t.Fatal("oversized Graph error response unexpectedly succeeded")
	}
	var statusErr *GraphStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("oversized Graph error response = %T %v, want GraphStatusError", err, err)
	}
	if statusErr.StatusCode != http.StatusTooManyRequests || statusErr.RetryAfter != 17*time.Second {
		t.Fatalf("Graph status error = %#v, want HTTP 429 with Retry-After 17s", statusErr)
	}
	var sizeErr *GraphResponseTooLargeError
	if errors.As(err, &sizeErr) {
		t.Fatalf("oversized non-2xx response was reported as size error instead of status: %v", err)
	}
}

func TestGraphMessagePageCompactsOversizedRecordAndKeepsLaterMessages(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	large := map[string]any{
		"id":                   "large-image",
		"chatId":               "chat-large",
		"createdDateTime":      "2026-08-28T01:00:00Z",
		"lastModifiedDateTime": "2026-08-28T01:00:00Z",
		"messageType":          "message",
		"body": map[string]string{
			"contentType": "html",
			"content":     strings.Repeat("x", maxGraphMessageRecordBytes),
		},
	}
	largeRaw, err := json.Marshal(large)
	if err != nil {
		t.Fatalf("marshal oversized message: %v", err)
	}
	later := bridgePollMessage("later-message", "2026-08-28T01:01:00Z", "later user message")
	laterRaw, err := json.Marshal(later)
	if err != nil {
		t.Fatalf("marshal later message: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"value":           []json.RawMessage{largeRaw, laterRaw},
		"@odata.nextLink": "/chats/chat-large/messages?$skiptoken=after-large",
	})
	if err != nil {
		t.Fatalf("marshal oversized page: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)
	graph := newTestGraphClient(auth, server, nil)
	window, err := graph.ListMessagesWindowWithoutRateLimitRetry(context.Background(), "chat-large", 50, time.Time{})
	if err != nil {
		t.Fatalf("large message page: %v", err)
	}
	if len(window.Messages) != 2 || !window.Truncated || !strings.Contains(window.NextPath, "skiptoken=after-large") {
		t.Fatalf("large message page = %#v, want two messages and continuation", window)
	}
	if window.Messages[0].ID != "large-image" || !window.Messages[0].oversizedForPoll || window.Messages[0].Body.Content != "" {
		t.Fatalf("oversized message was not compacted safely: %#v", window.Messages[0])
	}
	if window.Messages[1].ID != later.ID || window.Messages[1].Body.Content == "" {
		t.Fatalf("later message was lost while compacting oversized record: %#v", window.Messages[1])
	}
}

func TestGraphMessagePageRejectsOversizedRecordWithoutStableID(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	large := map[string]any{
		"createdDateTime":      "2026-08-28T01:00:00Z",
		"lastModifiedDateTime": "2026-08-28T01:00:00Z",
		"body": map[string]string{
			"contentType": "html",
			"content":     strings.Repeat("x", maxGraphMessageRecordBytes),
		},
	}
	raw, err := json.Marshal(map[string]any{"value": []any{large}})
	if err != nil {
		t.Fatalf("marshal missing-id page: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	}))
	t.Cleanup(server.Close)
	graph := newTestGraphClient(auth, server, nil)
	// The ordinary public path intentionally rejects this response at its
	// smaller cap. Exercise the poller's bounded recovery decoder so the test
	// reaches the per-record identity guard as well.
	_, err = graph.ListMessagesWindowWithoutRateLimitRetry(context.Background(), "chat-missing-id", 50, time.Time{})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "safe identity") {
		t.Fatalf("oversized record without ID error = %v, want explicit identity failure", err)
	}
}
