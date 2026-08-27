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
