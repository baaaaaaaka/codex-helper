package teams

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func testMathPNG() []byte {
	data, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")
	if err != nil {
		panic(err)
	}
	return data
}

func TestBridgePermanentlyRejectedHostedContentFallsBackToCodeOnly(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		_, hasHosted := payload["hostedContents"]
		if requests == 1 {
			if !hasHosted {
				t.Fatal("first request did not contain hosted content")
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"BadRequest","message":"hosted content rejected"}}`))
			return
		}
		if hasHosted {
			t.Fatal("code-only retry still contained hosted content")
		}
		body, _ := payload["body"].(map[string]any)
		content, _ := body["content"].(string)
		if !strings.Contains(content, `<pre><code>x_i</code></pre>`) || strings.Contains(content, `<img`) {
			t.Fatalf("fallback content = %q", content)
		}
		_, _ = w.Write([]byte(`{"id":"message-code-only"}`))
	}))
	defer server.Close()

	store, err := teamstore.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	outbox, _, err := store.QueueOutbox(context.Background(), freezeTestOutboxMath(teamstore.OutboxMessage{
		ID:               "outbox:math-fallback",
		TeamsChatID:      "chat-1",
		Kind:             "final",
		Body:             `<m>x_i</m>`,
		NotificationKind: "turn_completed",
		PartIndex:        1,
		PartCount:        1,
	}))
	if err != nil {
		t.Fatalf("queue outbox: %v", err)
	}
	graph := newTestGraphClient(auth, server, nil)
	bridge := &Bridge{graph: graph, readGraph: graph, store: store, mathRenderer: fakeTeamsMathRenderer{}, markAnswerChatsUnread: false}
	if err := bridge.sendQueuedOutbox(context.Background(), outbox); err != nil {
		t.Fatalf("sendQueuedOutbox: %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
	state, err := store.OutboxStateSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	stored := state.OutboxMessages[outbox.ID]
	if !stored.MathMediaFallback || stored.Status != teamstore.OutboxStatusSent || stored.TeamsMessageID != "message-code-only" {
		t.Fatalf("stored fallback outbox = %#v", stored)
	}
}

func TestBridgeQuoteHostedContentFallsBackToCodeOnlyWithoutLosingQuote(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/chats/chat-1/messages/replyWithQuote" {
			t.Fatalf("unexpected request path %q", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		reply, _ := payload["replyMessage"].(map[string]any)
		_, hasHosted := reply["hostedContents"]
		if requests == 1 {
			if !hasHosted {
				t.Fatal("first quote request did not contain hosted content")
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnsupportedMediaType)
			_, _ = w.Write([]byte(`{"error":{"code":"UnsupportedMediaType","message":"hosted content rejected"}}`))
			return
		}
		if hasHosted {
			t.Fatal("code-only quote retry still contained hosted content")
		}
		ids, _ := payload["messageIds"].([]any)
		if len(ids) != 1 || ids[0] != "quoted-1" {
			t.Fatalf("quote target was not preserved: %#v", payload["messageIds"])
		}
		_, _ = w.Write([]byte(`{"id":"message-code-only-quote"}`))
	}))
	defer server.Close()

	store, err := teamstore.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	outbox, _, err := store.QueueOutbox(context.Background(), freezeTestOutboxMath(teamstore.OutboxMessage{
		ID: "outbox:math-quote-fallback", TeamsChatID: "chat-1", Kind: "final", Body: `<m>x_i</m>`,
		QuoteReplyToMessageID: "quoted-1", PartIndex: 1, PartCount: 1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	graph := newTestGraphClient(auth, server, nil)
	bridge := &Bridge{graph: graph, readGraph: graph, store: store, mathRenderer: fakeTeamsMathRenderer{}, markAnswerChatsUnread: false}
	if err := bridge.sendQueuedOutbox(context.Background(), outbox); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want 2", requests)
	}
}

func TestBridgeSendsMathCodeAndInlineHostedPNGAtomically(t *testing.T) {
	t.Parallel()
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
			Hosted      []json.RawMessage `json:"hostedContents"`
			Attachments []json.RawMessage `json:"attachments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		code := `<pre><code>x_i^2+y_i^2=r^2</code></pre>`
		image := `<img src="../hostedContents/math-`
		if !strings.Contains(payload.Body.Content, code) || !strings.Contains(payload.Body.Content, image) || strings.Index(payload.Body.Content, code) > strings.Index(payload.Body.Content, image) {
			t.Fatalf("body did not keep source before image: %s", payload.Body.Content)
		}
		if len(payload.Hosted) != 1 || len(payload.Attachments) != 0 {
			t.Fatalf("hosted=%d attachments=%d", len(payload.Hosted), len(payload.Attachments))
		}
		_, _ = w.Write([]byte(`{"id":"message-math"}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	bridge := &Bridge{graph: graph, mathRenderer: fakeTeamsMathRenderer{}}
	msg, err := bridge.sendOutboxHTMLWithoutRateLimitRetry(context.Background(), freezeTestOutboxMath(teamstore.OutboxMessage{
		ID:               "outbox:turn-math:final",
		TeamsChatID:      "chat-1",
		Kind:             "final",
		Body:             `Pythagoras: <m>x_i^2+y_i^2=r^2</m>`,
		NotificationKind: "turn_completed",
		PartIndex:        1,
		PartCount:        1,
	}))
	if err != nil {
		t.Fatalf("sendOutboxHTMLWithoutRateLimitRetry: %v", err)
	}
	if msg.ID != "message-math" {
		t.Fatalf("message id = %q", msg.ID)
	}
}

func TestGraphSendHTMLWithHostedContentsPayload(t *testing.T) {
	t.Parallel()
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		var payload struct {
			Body struct {
				ContentType string `json:"contentType"`
				Content     string `json:"content"`
			} `json:"body"`
			Hosted []struct {
				TemporaryID  string `json:"@microsoft.graph.temporaryId"`
				ContentType  string `json:"contentType"`
				ContentBytes string `json:"contentBytes"`
			} `json:"hostedContents"`
			Mentions []json.RawMessage `json:"mentions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.Body.ContentType != "html" || payload.Body.Content != `<pre><code>x</code></pre><img src="../hostedContents/math-1/$value">` {
			t.Fatalf("body = %#v", payload.Body)
		}
		if len(payload.Hosted) != 1 || payload.Hosted[0].TemporaryID != "math-1" || payload.Hosted[0].ContentType != "image/png" {
			t.Fatalf("hosted contents = %#v", payload.Hosted)
		}
		decoded, err := base64.StdEncoding.DecodeString(payload.Hosted[0].ContentBytes)
		if err != nil || string(decoded) != string(testMathPNG()) {
			t.Fatalf("hosted bytes mismatch: %v", err)
		}
		if len(payload.Mentions) != 1 {
			t.Fatalf("mentions = %d", len(payload.Mentions))
		}
		_, _ = w.Write([]byte(`{"id":"message-1"}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	msg, err := graph.SendHTMLWithHostedContentsWithoutRateLimitRetry(context.Background(), "chat-1", `<pre><code>x</code></pre><img src="../hostedContents/math-1/$value">`, []ChatMention{{
		ID: 0, Text: "Owner", User: User{ID: "user-1", DisplayName: "Owner"},
	}}, []OutboundHostedContent{{TemporaryID: "math-1", ContentType: "image/png", Bytes: testMathPNG()}})
	if err != nil {
		t.Fatalf("SendHTMLWithHostedContentsWithoutRateLimitRetry: %v", err)
	}
	if msg.ID != "message-1" {
		t.Fatalf("message id = %q", msg.ID)
	}
}

func TestGraphSendQuoteReplyWithHostedContentsPayload(t *testing.T) {
	t.Parallel()
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages/replyWithQuote" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		reply, ok := payload["replyMessage"].(map[string]any)
		if !ok {
			t.Fatalf("missing replyMessage: %#v", payload)
		}
		if hosted, ok := reply["hostedContents"].([]any); !ok || len(hosted) != 1 {
			t.Fatalf("reply hostedContents = %#v", reply["hostedContents"])
		}
		_, _ = w.Write([]byte(`{"id":"message-2"}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	_, err := graph.SendHTMLReplyWithQuoteAndHostedContentsWithoutRateLimitRetry(context.Background(), "chat-1", "quoted-1", `<img src="../hostedContents/math-1/$value">`, nil, []OutboundHostedContent{{
		TemporaryID: "math-1", ContentType: "image/png", Bytes: testMathPNG(),
	}})
	if err != nil {
		t.Fatalf("SendHTMLReplyWithQuoteAndHostedContentsWithoutRateLimitRetry: %v", err)
	}
}

func TestGraphHostedContentPayloadRejectsUnsafeInput(t *testing.T) {
	t.Parallel()
	for _, item := range []OutboundHostedContent{
		{TemporaryID: "../bad", ContentType: "image/png", Bytes: testMathPNG()},
		{TemporaryID: "math-1", ContentType: "text/html", Bytes: testMathPNG()},
		{TemporaryID: "math-1", ContentType: "image/png", Bytes: []byte("not png")},
	} {
		if _, err := graphHostedContentPayloads([]OutboundHostedContent{item}); err == nil {
			t.Fatalf("accepted unsafe hosted content %#v", item)
		}
	}
}
