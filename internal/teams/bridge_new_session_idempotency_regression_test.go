package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// A create-or-get POST can succeed at Graph and lose its response on the
// network.  Replaying the durable /new inbound must address the same remote
// operation rather than creating a second Work chat.
func TestBridgeDeferredControlNewUnknownCreateResultUsesStableCreateOrGetKey(t *testing.T) {
	ctx := context.Background()
	workDir := t.TempDir()
	var externalIDs []string
	resources := make(map[string]string)
	var createdResources int
	var sent int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.String() == "/me?$select=id,displayName,userPrincipalName":
			_, _ = fmt.Fprint(w, `{"id":"new-user","displayName":"New User","userPrincipalName":"new@example.test"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings/createOrGet":
			var body struct {
				ExternalID string `json:"externalId"`
				Subject    string `json:"subject"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create-or-get body: %v", err)
			}
			if strings.TrimSpace(body.ExternalID) == "" {
				t.Fatal("create-or-get request did not contain an external id")
			}
			externalIDs = append(externalIDs, body.ExternalID)
			chatID, ok := resources[body.ExternalID]
			if !ok {
				createdResources++
				chatID = fmt.Sprintf("work-chat-%d", createdResources)
				resources[body.ExternalID] = chatID
			}
			if len(externalIDs) == 1 {
				// The remote operation has happened, but the client sees only an
				// unknown transport result.  This is the failure mode that a plain
				// POST cannot safely replay.
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					t.Fatal("test server does not support connection hijacking")
				}
				conn, _, err := hijacker.Hijack()
				if err != nil {
					t.Fatalf("hijack create-or-get response: %v", err)
				}
				_ = conn.Close()
				return
			}
			writeTestOnlineMeeting(w, chatID, body.Subject)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			sent++
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, sent)
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := newBridgeTestStore(t)
	messageID := "teams-new-unknown-result"
	if _, _, err := store.PersistInbound(ctx, teamstore.InboundEvent{
		SessionID:      controlFallbackSessionID,
		TeamsChatID:    "control-chat",
		TeamsMessageID: messageID,
		Text:           "new " + workDir,
		TeamsBodyType:  "text",
		Source:         "teams_control_new",
		Status:         teamstore.InboundStatusDeferred,
	}); err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.reg.Sessions = nil

	if err := bridge.processDeferredInbound(ctx); err == nil {
		t.Fatal("first replay unexpectedly succeeded after unknown create-or-get result")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after unknown result: %v", err)
	}
	deferred, err := store.DeferredInbound(ctx)
	if err != nil || len(deferred) != 1 {
		t.Fatalf("DeferredInbound after unknown result = %#v, err=%v", deferred, err)
	}
	inboundID := deferred[0].ID
	if got := state.InboundEvents[inboundID].Status; got != teamstore.InboundStatusDeferred {
		t.Fatalf("inbound status after unknown result = %s, want deferred", got)
	}
	if !state.InboundEvents[inboundID].NextAttemptAt.After(time.Now()) || state.InboundEvents[inboundID].FailureCount != 1 {
		t.Fatalf("deferred retry metadata after unknown result = %#v, want one future-gated failure", state.InboundEvents[inboundID])
	}
	if _, _, err := store.UpdateInboundEvent(ctx, inboundID, func(current teamstore.InboundEvent, found bool, now time.Time) (teamstore.InboundEvent, bool, error) {
		if !found {
			t.Fatalf("unknown-result inbound disappeared before forced wake")
		}
		current.NextAttemptAt = time.Time{}
		return current, true, nil
	}); err != nil {
		t.Fatalf("force unknown-result retry due: %v", err)
	}

	if err := bridge.processDeferredInbound(ctx); err != nil {
		t.Fatalf("replay after unknown result: %v", err)
	}
	if len(externalIDs) != 2 || externalIDs[0] != externalIDs[1] {
		t.Fatalf("create-or-get external ids = %#v, want the same durable key on replay", externalIDs)
	}
	if createdResources != 1 {
		t.Fatalf("remote Work chat resources = %d, want exactly one", createdResources)
	}
	if len(bridge.reg.Sessions) != 1 || bridge.reg.Sessions[0].ChatID != "work-chat-1" {
		t.Fatalf("local session projection = %#v, want one session bound to the recovered chat", bridge.reg.Sessions)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after successful replay: %v", err)
	}
	if got := state.InboundEvents[inboundID].Status; got != teamstore.InboundStatusIgnored {
		t.Fatalf("inbound status after replay = %s, want ignored", got)
	}
}

// A retryable failure in the first deferred row must be reported, but it must
// not prevent a later durable control command from making progress in the same
// recovery pass.
func TestBridgeDeferredInboundGraphFailureDoesNotBlockLaterRow(t *testing.T) {
	ctx := context.Background()
	var firstExternalID string
	var createAttempts int
	var sent int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.String() == "/me?$select=id,displayName,userPrincipalName":
			_, _ = fmt.Fprint(w, `{"id":"new-user","displayName":"New User","userPrincipalName":"new@example.test"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings/createOrGet":
			var body struct {
				ExternalID string `json:"externalId"`
				Subject    string `json:"subject"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create-or-get body: %v", err)
			}
			createAttempts++
			if firstExternalID == "" {
				firstExternalID = body.ExternalID
			}
			if body.ExternalID == firstExternalID {
				w.Header().Set("Retry-After", "60")
				http.Error(w, `{"error":{"code":"TooManyRequests","message":"synthetic account throttle"}}`, http.StatusTooManyRequests)
				return
			}
			writeTestOnlineMeeting(w, "healthy-work-chat", body.Subject)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			sent++
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, sent)
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := newBridgeTestStore(t)
	firstMessageID := "teams-deferred-poison"
	secondMessageID := "teams-deferred-healthy"
	if _, _, err := store.PersistInbound(ctx, teamstore.InboundEvent{
		SessionID:      controlFallbackSessionID,
		TeamsChatID:    "control-chat",
		TeamsMessageID: firstMessageID,
		Text:           "new " + t.TempDir(),
		TeamsBodyType:  "text",
		Source:         "teams_control_new",
		Status:         teamstore.InboundStatusDeferred,
		CreatedAt:      time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("persist first deferred inbound: %v", err)
	}
	if _, _, err := store.PersistInbound(ctx, teamstore.InboundEvent{
		SessionID:      controlFallbackSessionID,
		TeamsChatID:    "control-chat",
		TeamsMessageID: secondMessageID,
		Text:           "new " + t.TempDir(),
		TeamsBodyType:  "text",
		Source:         "teams_control_new",
		Status:         teamstore.InboundStatusDeferred,
		CreatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("persist second deferred inbound: %v", err)
	}
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 1,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.reg.Sessions = nil

	if err := bridge.processDeferredInbound(ctx); err == nil {
		t.Fatal("deferred recovery unexpectedly hid the first row's Graph 429")
	}
	if createAttempts < 2 {
		t.Fatalf("create-or-get attempts = %d, want the throttled first row and a later healthy row", createAttempts)
	}
	deferred, err := store.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound after mixed recovery: %v", err)
	}
	if len(deferred) != 1 || deferred[0].TeamsMessageID != firstMessageID {
		t.Fatalf("deferred rows after mixed recovery = %#v, want only the throttled first row", deferred)
	}
	first, ok, err := store.InboundEventByID(ctx, deferred[0].ID)
	if err != nil || !ok {
		t.Fatalf("throttled inbound after mixed recovery: %#v ok=%v err=%v", first, ok, err)
	}
	if !first.NextAttemptAt.After(time.Now()) || first.FailureCount != 1 {
		t.Fatalf("throttled inbound retry metadata = %#v, want a future durable gate", first)
	}
	attemptsAfterFirstPass := createAttempts
	if err := bridge.processDeferredInbound(ctx); err != nil {
		t.Fatalf("future-gated deferred recovery returned error: %v", err)
	}
	if createAttempts != attemptsAfterFirstPass {
		t.Fatalf("future-gated deferred row was retried immediately: attempts=%d want=%d", createAttempts, attemptsAfterFirstPass)
	}
	if len(bridge.reg.Sessions) != 1 || bridge.reg.Sessions[0].ChatID != "healthy-work-chat" {
		t.Fatalf("later deferred row did not create its Work chat: %#v", bridge.reg.Sessions)
	}
	if sent == 0 {
		t.Fatal("later deferred row made no durable/outbound progress")
	}
}

func TestBridgeDeferredAttachmentNotFoundUsesExplicitPermanentDisposition(t *testing.T) {
	ctx := context.Background()
	readServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/chat-1/messages/missing-attachment" {
			t.Fatalf("unexpected attachment lookup: %s %s", r.Method, r.URL.String())
		}
		http.Error(w, `{"error":{"code":"NotFound","message":"message was deleted"}}`, http.StatusNotFound)
	}))
	t.Cleanup(readServer.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     readServer.Client(),
		baseURL:    readServer.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	writeGraph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}
	const inboundID = "inbound:missing-attachment"
	if _, _, err := store.PersistInbound(ctx, teamstore.InboundEvent{
		ID:             inboundID,
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "missing-attachment",
		Source:         "teams_session_import_deferred_attachment",
		Status:         teamstore.InboundStatusDeferred,
		TeamsAttachments: []teamstore.InboundAttachmentContext{{
			ID: "attachment-1", ContentType: "reference", Name: "deleted.txt",
		}},
	}); err != nil {
		t.Fatalf("PersistInbound: %v", err)
	}

	if err := bridge.processDeferredInbound(ctx); err != nil {
		t.Fatalf("processDeferredInbound: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after missing attachment: %v", err)
	}
	if got := state.InboundEvents[inboundID].Status; got != teamstore.InboundStatusIgnored {
		t.Fatalf("missing attachment status = %s, want ignored by explicit not-found policy", got)
	}
	if len(state.Turns) != 0 {
		t.Fatalf("missing attachment created turns: %#v", state.Turns)
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "no longer available") {
		t.Fatalf("missing attachment diagnostic messages = %#v", *sent)
	}
}

func TestBridgeDeferredAttachmentContentNotFoundUsesExplicitPermanentDisposition(t *testing.T) {
	ctx := context.Background()
	readServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages/missing-content":
			msg := ChatMessage{ID: "missing-content", ChatID: "chat-1", MessageType: "message"}
			msg.Body.ContentType = "html"
			msg.Body.Content = `<p>image</p><img src="../hostedContents/deleted-content/$value">`
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(msg); err != nil {
				t.Fatalf("encode message: %v", err)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages/missing-content/hostedContents/deleted-content/$value":
			http.Error(w, `{"error":{"code":"NotFound","message":"attachment content was deleted"}}`, http.StatusNotFound)
		default:
			t.Fatalf("unexpected attachment content request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(readServer.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     readServer.Client(),
		baseURL:    readServer.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	writeGraph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}
	const inboundID = "inbound:missing-attachment-content"
	if _, _, err := store.PersistInbound(ctx, teamstore.InboundEvent{
		ID:             inboundID,
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "missing-content",
		Source:         "teams_session_import_deferred_attachment",
		Status:         teamstore.InboundStatusDeferred,
	}); err != nil {
		t.Fatalf("PersistInbound: %v", err)
	}

	if err := bridge.processDeferredInbound(ctx); err != nil {
		t.Fatalf("processDeferredInbound: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after missing attachment content: %v", err)
	}
	if got := state.InboundEvents[inboundID].Status; got != teamstore.InboundStatusIgnored {
		t.Fatalf("missing attachment content status = %s, want ignored", got)
	}
	if got := state.InboundEvents[inboundID].FailureCount; got != 0 {
		t.Fatalf("missing attachment content failure count = %d, want terminal zero", got)
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "attachment content") {
		t.Fatalf("missing attachment content diagnostic messages = %#v", *sent)
	}
}
