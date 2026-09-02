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

func TestGraphMessagePageRejectsMissingOrNullValue(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "missing", raw: `{}`},
		{name: "null", raw: `{"value":null}`},
		{name: "top-level-null", raw: `null`},
		{name: "wrong-type", raw: `{"value":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var page graphMessagePage
			err := json.Unmarshal([]byte(tc.raw), &page)
			if !errors.Is(err, errGraphMessagePageInvalid) {
				t.Fatalf("page %s error = %v, want invalid-page classification", tc.name, err)
			}
		})
	}
}

func TestGraphMessagePageRejectsMalformedJSONAsInvalidPage(t *testing.T) {
	for _, raw := range []string{"{", `{"value":[}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(raw))
		}))
		graph := newTestGraphClient(&fakeGraphAuth{token: "access"}, server, nil)
		_, err := graph.ListMessagesWindowWithoutRateLimitRetry(context.Background(), "chat-malformed-page", 50, time.Time{})
		server.Close()
		if !errors.Is(err, errGraphMessagePageInvalid) {
			t.Fatalf("malformed page %q error = %v, want invalid-page classification", raw, err)
		}
	}
}

func TestGraphMessagePageRejectsEmptySuccessBody(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	graph := newTestGraphClient(auth, server, nil)
	_, err := graph.ListMessagesWindowWithoutRateLimitRetry(context.Background(), "chat-empty-page", 50, time.Time{})
	if !errors.Is(err, errGraphMessagePageInvalid) {
		t.Fatalf("empty success page error = %v, want invalid-page classification", err)
	}
}

func TestGraphMessagePageQuarantinesOnlySemanticallyMalformedRecord(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	first := bridgePollMessage("valid-before-malformed", "2026-08-28T01:00:00Z", "first")
	last := bridgePollMessage("valid-after-malformed", "2026-08-28T01:01:00Z", "last")
	firstRaw, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first message: %v", err)
	}
	lastRaw, err := json.Marshal(last)
	if err != nil {
		t.Fatalf("marshal last message: %v", err)
	}
	// The page is valid JSON and the malformed record has a stable provider ID,
	// but one optional object has an unexpected scalar type. The direct decoder
	// must fall back to per-record recovery rather than rejecting both valid
	// messages with the page.
	payload, err := json.Marshal(map[string]any{
		"value": []json.RawMessage{
			firstRaw,
			json.RawMessage(`{"id":"malformed-record","chatId":"chat-malformed-record","body":"not-an-object"}`),
			lastRaw,
		},
		"@odata.nextLink": "/chats/chat-malformed-record/messages?$skiptoken=after-malformed",
	})
	if err != nil {
		t.Fatalf("marshal malformed-record page: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	graph := newTestGraphClient(auth, server, nil)
	window, err := graph.ListMessagesWindowWithoutRateLimitRetry(context.Background(), "chat-malformed-record", 50, time.Time{})
	if err != nil {
		t.Fatalf("malformed-record page: %v", err)
	}
	if len(window.Messages) != 3 || !window.Truncated || !strings.Contains(window.NextPath, "after-malformed") {
		t.Fatalf("malformed-record page = %#v, want all three records and continuation", window)
	}
	if window.Messages[0].ID != first.ID || !strings.Contains(window.Messages[0].Body.Content, "first") {
		t.Fatalf("valid record before malformed record = %#v", window.Messages[0])
	}
	if window.Messages[1].ID != "malformed-record" || !window.Messages[1].invalidForPoll || window.Messages[1].quarantinedForPoll || window.Messages[1].Body.Content != "" {
		t.Fatalf("malformed record = %#v, want retryable invalid marker", window.Messages[1])
	}
	if window.Messages[2].ID != last.ID || !strings.Contains(window.Messages[2].Body.Content, "last") {
		t.Fatalf("valid record after malformed record = %#v", window.Messages[2])
	}
}

func TestGraphMessagePageCompactsOversizedRecordWithMalformedOptionalField(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	large := []byte(`{"id":"large-malformed","chatId":"chat-large-malformed","createdDateTime":"2026-08-28T01:00:00Z","lastModifiedDateTime":123,"messageType":"message","body":{"contentType":"html","content":"` + strings.Repeat("x", maxGraphMessageRecordBytes) + `"}}`)
	later := bridgePollMessage("later-after-large-malformed", "2026-08-28T01:01:00Z", "later user message")
	laterRaw, err := json.Marshal(later)
	if err != nil {
		t.Fatalf("marshal later message: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"value":           []json.RawMessage{large, laterRaw},
		"@odata.nextLink": "/chats/chat-large-malformed/messages?$skiptoken=after-large-malformed",
	})
	if err != nil {
		t.Fatalf("marshal malformed oversized page: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	graph := newTestGraphClient(auth, server, nil)
	window, err := graph.ListMessagesWindowWithoutRateLimitRetry(context.Background(), "chat-large-malformed", 50, time.Time{})
	if err != nil {
		t.Fatalf("malformed oversized page: %v", err)
	}
	if len(window.Messages) != 2 || !window.Truncated || !strings.Contains(window.NextPath, "after-large-malformed") {
		t.Fatalf("malformed oversized page = %#v, want two records and continuation", window)
	}
	if window.Messages[0].ID != "large-malformed" || !window.Messages[0].oversizedForPoll || window.Messages[0].invalidForPoll {
		t.Fatalf("malformed oversized record = %#v, want oversized retry marker", window.Messages[0])
	}
	if window.Messages[1].ID != later.ID || window.Messages[1].Body.Content == "" {
		t.Fatalf("later message was lost after malformed oversized record: %#v", window.Messages[1])
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
