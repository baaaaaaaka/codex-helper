package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// TestBridgePollRetainsOldContinuationWhenHeadWindowTimesOut combines the
// long-outage shape with a full head window and a stalled old continuation.
// The operational contract is one frontier per quantum: the stalled P is
// retried first and the head is not read until P has completed.
func TestBridgePollRetainsOldContinuationWhenHeadWindowTimesOut(t *testing.T) {
	previousTimeout := inboundPollGraphTimeout
	inboundPollGraphTimeout = 100 * time.Millisecond
	t.Cleanup(func() { inboundPollGraphTimeout = previousTimeout })

	var mu sync.Mutex
	oldContinuationRequests := 0
	headMessageJSON := mustJSONChatMessage(t, bridgePollMessage("head-1", "2026-08-26T01:20:00Z", "fresh head"))
	oldMessageJSON := mustJSONChatMessage(t, bridgePollMessage("old-1", "2026-08-26T01:01:00Z", "old backlog"))
	headContinuationJSON := mustJSONChatMessage(t, bridgePollMessage("head-2", "2026-08-26T01:21:00Z", "head continuation"))
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, fmt.Sprintf("unexpected Graph request: %s %s", r.Method, r.URL.String()), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$skiptoken") {
		case "":
			_, _ = fmt.Fprintf(w, `{"value":[%s],"@odata.nextLink":%q}`, headMessageJSON, server.URL+"/chats/chat-1/messages?$skiptoken=head-next")
		case "old":
			mu.Lock()
			oldContinuationRequests++
			attempt := oldContinuationRequests
			mu.Unlock()
			if attempt == 1 {
				<-r.Context().Done()
				return
			}
			_, _ = fmt.Fprintf(w, `{"value":[%s]}`, oldMessageJSON)
		case "head-next":
			_, _ = fmt.Fprintf(w, `{"value":[%s]}`, headContinuationJSON)
		default:
			http.Error(w, fmt.Sprintf("unexpected continuation %q", r.URL.Query().Get("$skiptoken")), http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	cursor := time.Date(2026, 8, 26, 1, 10, 0, 0, time.UTC)
	if _, err := store.RecordChatPollSuccessWithContinuation(context.Background(), "chat-1", cursor, true, true, 50, "/chats/chat-1/messages?$skiptoken=old"); err != nil {
		t.Fatalf("seed old continuation: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, handle); err == nil {
		t.Fatal("head plus stalled continuation poll succeeded, want bounded timeout")
	}
	poll, ok, err := store.ChatPoll(context.Background(), "chat-1")
	if err != nil || !ok {
		t.Fatalf("poll after stalled continuation: ok=%v err=%v state=%#v", ok, err, poll)
	}
	if poll.ContinuationPath != "/chats/chat-1/messages?$skiptoken=old" || poll.DeferredContinuationPath != "" {
		t.Fatalf("frontiers after timeout = %#v, want only old continuation", poll)
	}
	if !poll.LastModifiedCursor.Equal(cursor) {
		t.Fatalf("cursor advanced across unresolved old continuation: got %s want %s", poll.LastModifiedCursor, cursor)
	}
	if got := strings.Join(handled, ","); got != "" {
		t.Fatalf("handled after timeout = %q, want no message", got)
	}

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, handle); err != nil {
		t.Fatalf("recover old continuation: %v", err)
	}
	poll, ok, err = store.ChatPoll(context.Background(), "chat-1")
	if err != nil || !ok {
		t.Fatalf("poll after old continuation recovery: ok=%v err=%v state=%#v", ok, err, poll)
	}
	if poll.ContinuationPath != "" || poll.DeferredContinuationPath != "" {
		t.Fatalf("frontiers after old continuation recovery = %#v, want drained old frontier", poll)
	}

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, handle); err != nil {
		t.Fatalf("read fresh head: %v", err)
	}
	if got := strings.Join(handled, ","); got != "old backlog,fresh head" {
		t.Fatalf("handled messages = %q, want old backlog then fresh head", got)
	}
	poll, ok, err = store.ChatPoll(context.Background(), "chat-1")
	if err != nil || !ok || poll.ContinuationPath != "/chats/chat-1/messages?$skiptoken=head-next" || poll.DeferredContinuationPath != "" {
		t.Fatalf("final poll state = %#v ok=%v err=%v, want fresh head continuation retained", poll, ok, err)
	}

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, handle); err != nil {
		t.Fatalf("replay fresh head page: %v", err)
	}
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, handle); err != nil {
		t.Fatalf("drain fresh head continuation: %v", err)
	}
	if got := strings.Join(handled, ","); got != "old backlog,fresh head,head continuation" {
		t.Fatalf("final handled messages = %q, want all records", got)
	}
}

// TestBridgePollRetainsBothFrontiersWhenOldContinuationRateLimitsAfterFullHeadWindow
// is the rate-limit version of the dual-frontier outage shape. A 429 on P is a
// chat-scoped retry schedule, not a semantic block, and no head frontier is
// created until P has completed.
func TestBridgePollRetainsBothFrontiersWhenOldContinuationRateLimitsAfterFullHeadWindow(t *testing.T) {
	ctx := context.Background()
	chatID := "chat-full-head-429"
	oldPath := "/chats/" + chatID + "/messages?$skiptoken=old"
	headPath := "/chats/" + chatID + "/messages?$skiptoken=head-next"
	headMessageJSON := mustJSONChatMessage(t, bridgePollMessage("head-429-1", "2026-08-26T01:20:00Z", "fresh head"))
	oldMessageJSON := mustJSONChatMessage(t, bridgePollMessage("old-429-1", "2026-08-26T01:01:00Z", "old backlog"))
	headContinuationJSON := mustJSONChatMessage(t, bridgePollMessage("head-429-2", "2026-08-26T01:21:00Z", "head continuation"))
	var mu sync.Mutex
	oldContinuationRequests := 0
	allowOldContinuation := false
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/"+chatID+"/messages" {
			http.Error(w, fmt.Sprintf("unexpected Graph request: %s %s", r.Method, r.URL.String()), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$skiptoken") {
		case "":
			_, _ = fmt.Fprintf(w, `{"value":[%s],"@odata.nextLink":%q}`, headMessageJSON, server.URL+headPath)
		case "old":
			mu.Lock()
			oldContinuationRequests++
			allowed := allowOldContinuation
			mu.Unlock()
			if !allowed {
				w.Header().Set("Retry-After", "60")
				http.Error(w, `{"error":{"code":"TooManyRequests","message":"old continuation rate limited"}}`, http.StatusTooManyRequests)
				return
			}
			_, _ = fmt.Fprintf(w, `{"value":[%s]}`, oldMessageJSON)
		case "head-next":
			_, _ = fmt.Fprintf(w, `{"value":[%s]}`, headContinuationJSON)
		default:
			http.Error(w, fmt.Sprintf("unexpected continuation %q", r.URL.Query().Get("$skiptoken")), http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	cursor := time.Date(2026, 8, 26, 1, 10, 0, 0, time.UTC)
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, chatID, cursor, true, true, 1, oldPath); err != nil {
		t.Fatalf("seed old continuation: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}

	if _, err := bridge.pollChat(ctx, chatID, 1, handle); err == nil || !isGraphRateLimitError(err) {
		t.Fatalf("full-head poll with rate-limited old continuation error = %v, want Graph 429", err)
	}
	poll, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok {
		t.Fatalf("poll after rate-limited continuation: ok=%v err=%v state=%#v", ok, err, poll)
	}
	if poll.PollState == inboundPollStateBlocked || !poll.BlockedUntil.IsZero() || poll.FailureCount == 0 || !strings.Contains(strings.ToLower(poll.LastError), "rate limited") {
		t.Fatalf("rate-limited continuation became a semantic block: %#v", poll)
	}
	if poll.ContinuationPath != oldPath || poll.DeferredContinuationPath != "" || !poll.LastModifiedCursor.Equal(cursor) {
		t.Fatalf("frontiers after rate limit = %#v, want only old continuation and unchanged cursor", poll)
	}
	if got := strings.Join(handled, ","); got != "" {
		t.Fatalf("handled after rate limit = %q, want no head message", got)
	}

	mu.Lock()
	allowOldContinuation = true
	mu.Unlock()
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate rate-limited dual-frontier state: %v", err)
	}
	storePath := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close rate-limited dual-frontier store: %v", err)
	}
	recoveredStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen rate-limited dual-frontier store: %v", err)
	}
	t.Cleanup(func() {
		if err := recoveredStore.Close(); err != nil {
			t.Errorf("close recovered rate-limited dual-frontier store: %v", err)
		}
	})
	restarted := newBridgeTestBridge(graph, recoveredStore, &recordingExecutor{})
	if _, err := recoveredStore.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:            chatID,
		PollState:         inboundPollStateWarm,
		NextPollAt:        time.Now(),
		ClearBlockedUntil: true,
		ResetFailures:     true,
	}); err != nil {
		t.Fatalf("clear recovered poll rate-limit state: %v", err)
	}
	if _, err := restarted.pollChat(ctx, chatID, 1, handle); err != nil {
		t.Fatalf("recover old continuation after restart: %v", err)
	}
	if _, err := restarted.pollChat(ctx, chatID, 1, handle); err != nil {
		t.Fatalf("read fresh head after restart: %v", err)
	}
	if _, err := restarted.pollChat(ctx, chatID, 1, handle); err != nil {
		t.Fatalf("drain fresh head continuation after restart: %v", err)
	}
	if got := strings.Join(handled, ","); got != "old-429-1,head-429-1,head-429-2" {
		t.Fatalf("handled messages = %q, want old then fresh frontier", got)
	}
	poll, ok, err = recoveredStore.ChatPoll(ctx, chatID)
	if err != nil || !ok || poll.ContinuationPath != "" || poll.DeferredContinuationPath != "" {
		t.Fatalf("final rate-limit poll = %#v ok=%v err=%v, want frontier drained", poll, ok, err)
	}
	mu.Lock()
	requests := oldContinuationRequests
	mu.Unlock()
	if requests != 2 {
		t.Fatalf("old continuation requests = %d, want one failed and one recovered request", requests)
	}
}

// TestBridgePollTeamsTurnThenTUITranscriptSyncKeepsBothSurfacesExactlyOnce
// drives a real Graph poll into handleSessionMessage, then appends a separate
// TUI result to the linked rollout transcript before the next history sync.
// The two surfaces must not replay each other or turn the normal interleaving
// into a transcript quarantine.
func TestBridgePollTeamsTurnThenTUITranscriptSyncKeepsBothSurfacesExactlyOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-cross-poll-tui"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-cross-poll-tui"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, threadID, path)
	defer restoreDiscover()
	var mu sync.Mutex
	var sent []string
	var pollServed bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages":
			mu.Lock()
			first := !pollServed
			pollServed = true
			mu.Unlock()
			if first {
				message := bridgePollMessage("teams-cross-poll", "2026-08-26T02:00:00Z", "Teams request before TUI append")
				if err := json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{message}}); err != nil {
					http.Error(w, fmt.Sprintf("encode Teams poll response: %v", err), http.StatusInternalServerError)
				}
				return
			}
			_, _ = fmt.Fprint(w, `{"value":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/chats/chat-1/messages":
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, fmt.Sprintf("decode Teams send: %v", err), http.StatusBadRequest)
				return
			}
			mu.Lock()
			sent = append(sent, PlainTextFromTeamsHTML(body.Body.Content))
			messageID := len(sent)
			mu.Unlock()
			_, _ = fmt.Fprintf(w, `{"id":"cross-poll-sent-%d","messageType":"message"}`, messageID)
		default:
			http.Error(w, fmt.Sprintf("unexpected Graph request: %s %s", r.Method, r.URL.String()), http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "Teams executor answer",
		CodexThreadID: threadID,
		CodexTurnID:   "teams-turn",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := seedLinkedTranscriptForTest(t, bridge, path, threadID)
	if _, err := store.RecordChatPollSuccess(context.Background(), session.ChatID, time.Date(2026, 8, 26, 1, 59, 0, 0, time.UTC), true, false, 0); err != nil {
		t.Fatalf("seed cross-poll chat cursor: %v", err)
	}

	if _, err := bridge.pollChat(context.Background(), session.ChatID, 50, func(ctx context.Context, message ChatMessage, text string) error {
		return bridge.handleSessionMessage(ctx, session.ChatID, message, text)
	}); err != nil {
		t.Fatalf("poll Teams turn before TUI append: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	mu.Lock()
	teamsSurface := append([]string(nil), sent...)
	mu.Unlock()
	if countStringsContaining(teamsSurface, "Teams executor answer") != 1 || countStringsContaining(teamsSurface, "Codex is working") != 1 {
		t.Fatalf("Teams poll surface = %#v, want one ack and one final", teamsSurface)
	}

	appendLine(t, path, `{"id":"tui-after-teams","thread_id":"thread-cross-poll-tui","role":"assistant","text":"TUI independent answer"}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync TUI transcript after Teams poll: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	mu.Lock()
	combinedSurface := append([]string(nil), sent...)
	mu.Unlock()
	if countStringsContaining(combinedSurface, "TUI independent answer") != 1 || countStringsContaining(combinedSurface, "Teams executor answer") != 1 {
		t.Fatalf("combined Teams/TUI surface = %#v, want both answers once", combinedSurface)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load cross-poll state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.TranscriptQuarantine != nil || checkpoint.PendingHistoryRange != nil || checkpoint.UnresolvedExecution != nil || checkpoint.LastOffset != checkpoint.SourceSize {
		t.Fatalf("cross-poll checkpoint = %#v, want clean EOF without history gate", checkpoint)
	}
	assertSingleSentTranscriptFinal(t, state, session.ID, path, "TUI independent answer", "")

	if _, err := bridge.pollChat(context.Background(), session.ChatID, 50, func(ctx context.Context, message ChatMessage, text string) error {
		return bridge.handleSessionMessage(ctx, session.ChatID, message, text)
	}); err != nil {
		t.Fatalf("repeat cross-poll read: %v", err)
	}
	if len(executor.prompts) != 1 {
		t.Fatalf("Teams executor prompts = %#v, want one after repeat poll", executor.prompts)
	}
}

// TestBridgeLinkedTranscriptAppendsDuringDrainAndResumesAcrossRestart models
// a TUI-produced transcript record arriving after the Teams listener starts a
// drain. Transcript catch-up is allowed to run while the service drains, but
// ownership transfer must preserve the durable cursor and never publish the
// record twice. If the record is still pending, the next owner must finish it
// after the drain clears.
func TestBridgeLinkedTranscriptAppendsDuringDrainAndResumesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-tui-during-drain"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-tui-during-drain"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, threadID, path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, threadID)
	*sent = nil
	if _, err := store.SetDraining(context.Background(), "restart"); err != nil {
		t.Fatalf("set drain: %v", err)
	}
	appendLine(t, path, `{"id":"tui-final","thread_id":"thread-tui-during-drain","role":"assistant","text":"TUI answer during drain"}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("old-owner sync during drain: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	oldOwnerSent := countSentPlainContaining(*sent, "TUI answer during drain")
	if oldOwnerSent > 1 {
		t.Fatalf("old owner sent transcript more than once during drain: %d %#v", oldOwnerSent, *sent)
	}
	drainingBeforeRestart, err := store.ReadControl(context.Background())
	if err != nil {
		t.Fatalf("read drain state before restart: %v", err)
	}
	if !drainingBeforeRestart.Draining {
		t.Fatalf("old owner lost drain state before restart: %#v", drainingBeforeRestart)
	}

	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("migrate draining state: %v", err)
	}
	storePath := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close draining owner store: %v", err)
	}
	restartedStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen after drain: %v", err)
	}
	t.Cleanup(func() {
		if err := restartedStore.Close(); err != nil {
			t.Errorf("close restarted store: %v", err)
		}
	})
	drainingAfterRestart, err := restartedStore.ReadControl(context.Background())
	if err != nil {
		t.Fatalf("read drain state after restart: %v", err)
	}
	if !drainingAfterRestart.Draining {
		t.Fatalf("restarted owner lost drain state: %#v", drainingAfterRestart)
	}
	restartedGraph, restartedSent := newBridgeTestGraph(t)
	restarted := newBridgeTestBridge(restartedGraph, restartedStore, &recordingExecutor{})
	resumedSession := restarted.reg.SessionByID(session.ID)
	if resumedSession == nil {
		t.Fatalf("restarted registry missing session %s", session.ID)
	}
	resumedSession.CodexThreadID = threadID
	if err := restarted.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync while drain remains active after restart: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, restarted)
	whileDrainingSent := countSentPlainContaining(*restartedSent, "TUI answer during drain")
	if got := oldOwnerSent + whileDrainingSent; got > 1 {
		state, _ := restartedStore.Load(context.Background())
		t.Fatalf("transcript deliveries before drain clear = %d, want at most one: old_sent=%d restarted_sent=%#v checkpoint=%#v outbox=%#v deliveries=%#v", got, oldOwnerSent, *restartedSent, state.ImportCheckpoints[transcriptCheckpointID(resumedSession.ID)], state.OutboxMessages, state.TranscriptDeliveries)
	}
	if control, err := restartedStore.ReadControl(context.Background()); err != nil {
		t.Fatalf("read drain state before clear: %v", err)
	} else if !control.Draining {
		t.Fatalf("restart unexpectedly cleared drain state: %#v", control)
	}
	if _, err := restartedStore.ClearDrain(context.Background()); err != nil {
		t.Fatalf("clear recovered drain: %v", err)
	}
	if err := restarted.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync after drain restart: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, restarted)
	afterClearSent := countSentPlainContaining(*restartedSent, "TUI answer during drain") - whileDrainingSent
	if afterClearSent < 0 {
		t.Fatalf("transcript send count decreased after drain clear: while_draining=%d restarted_sent=%#v", whileDrainingSent, *restartedSent)
	}
	if got := oldOwnerSent + whileDrainingSent + afterClearSent; got != 1 {
		state, _ := restartedStore.Load(context.Background())
		t.Fatalf("post-drain transcript deliveries = %d, want exactly one: old_sent=%d while_draining=%d after_clear=%d restarted_sent=%#v checkpoint=%#v outbox=%#v deliveries=%#v", got, oldOwnerSent, whileDrainingSent, afterClearSent, *restartedSent, state.ImportCheckpoints[transcriptCheckpointID(resumedSession.ID)], state.OutboxMessages, state.TranscriptDeliveries)
	}
	state, err := restartedStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load post-drain state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(resumedSession.ID)]
	if checkpoint.LastOffset != checkpoint.SourceSize || checkpoint.TranscriptQuarantine != nil || checkpoint.PendingHistoryRange != nil {
		t.Fatalf("post-drain checkpoint = %#v, want clean EOF", checkpoint)
	}
	assertSingleSentTranscriptFinal(t, state, resumedSession.ID, path, "TUI answer during drain", "")

	beforeRepeat := len(*restartedSent)
	if err := restarted.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("repeat post-drain sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, restarted)
	if len(*restartedSent) != beforeRepeat {
		t.Fatalf("post-drain sync replayed transcript delivery: %#v", *restartedSent)
	}
}

// TestBridgeLinkedTranscriptConcurrentSQLiteSyncPublishesExactlyOnce models the
// narrow handoff window where an old owner is already sending a transcript
// outbox row while a restarted owner begins its own sync against the same
// SQLite state. This is deliberately a shared-store/outbox race, not a test of
// control-lease takeover or process fencing; those are covered by the runtime
// ownership tests. Per-Bridge mutexes cannot coordinate this case, so durable
// outbox claiming and transcript delivery CAS must avoid a duplicate Graph POST
// or a regressed checkpoint.
func TestBridgeLinkedTranscriptConcurrentSQLiteSyncPublishesExactlyOnce(t *testing.T) {
	// This test deliberately contends on a shared SQLite store while the race
	// detector is active. The 3s bound was enough on an idle workstation but
	// allowed scheduler/SQLite startup latency to turn a completed handoff into
	// a false delivery failure in a busy CI worker. Keep the same blocked-send
	// ordering and exact-once assertions, but give the fixture a finite margin.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-concurrent-owner-sync"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-concurrent-owner-sync"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, threadID, path)
	defer restoreDiscover()

	seedStore, err := teamstore.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("open seed store: %v", err)
	}
	seedClosed := false
	t.Cleanup(func() {
		if !seedClosed {
			_ = seedStore.Close()
		}
	})
	seedGraph, _ := newBridgeTestGraph(t)
	seedBridge := newBridgeTestBridge(seedGraph, seedStore, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, seedBridge, path, threadID)
	if _, err := seedStore.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate shared owner store: %v", err)
	}
	if _, err := seedStore.SetDraining(ctx, "concurrent-owner-handoff"); err != nil {
		t.Fatalf("set shared owner drain: %v", err)
	}
	storePath := seedStore.Path()
	if err := seedStore.Close(); err != nil {
		t.Fatalf("close seed owner store: %v", err)
	}
	seedClosed = true
	oldStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open old owner store: %v", err)
	}
	t.Cleanup(func() { _ = oldStore.Close() })
	newStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open restarted owner store: %v", err)
	}
	t.Cleanup(func() { _ = newStore.Close() })

	appendLine(t, path, `{"id":"handoff-final","thread_id":"thread-concurrent-owner-sync","role":"assistant","text":"answer during concurrent owner handoff"}`)

	var mu sync.Mutex
	postCount := 0
	var sent []string
	postStarted := make(chan struct{})
	releasePost := make(chan struct{})
	var postOnce sync.Once
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			return nil, fmt.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			return nil, fmt.Errorf("decode concurrent owner send: %w", err)
		}
		mu.Lock()
		postCount++
		attempt := postCount
		sent = append(sent, PlainTextFromTeamsHTML(body.Body.Content))
		mu.Unlock()
		if attempt == 1 {
			postOnce.Do(func() { close(postStarted) })
			select {
			case <-releasePost:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		w := httptest.NewRecorder()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"handoff-sent-%d","messageType":"message"}`, attempt)
		return w.Result(), nil
	})
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     &http.Client{Transport: transport},
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	oldOwner := newBridgeTestBridge(graph, oldStore, &recordingExecutor{})
	newOwner := newBridgeTestBridge(graph, newStore, &recordingExecutor{})
	oldOwner.registryPath = filepath.Join(t.TempDir(), "old-registry.json")
	newOwner.registryPath = filepath.Join(t.TempDir(), "new-registry.json")
	if err := oldOwner.restoreRegistryFromStore(ctx); err != nil {
		t.Fatalf("restore old owner registry: %v", err)
	}
	if err := newOwner.restoreRegistryFromStore(ctx); err != nil {
		t.Fatalf("restore new owner registry: %v", err)
	}

	oldErrCh := make(chan error, 1)
	go func() { oldErrCh <- oldOwner.syncLinkedTranscripts(ctx) }()
	select {
	case <-postStarted:
	case <-ctx.Done():
		close(releasePost)
		t.Fatalf("old owner did not reach Graph send: %v", ctx.Err())
	}

	newErrCh := make(chan error, 1)
	newStarted := make(chan struct{})
	go func() {
		close(newStarted)
		newErrCh <- newOwner.syncLinkedTranscripts(ctx)
	}()
	<-newStarted
	var newErr error
	newFinished := false
	select {
	case newErr = <-newErrCh:
		newFinished = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePost)
	if err := <-oldErrCh; err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("concurrent old owner sync: %v", err)
	}
	if !newFinished {
		select {
		case newErr = <-newErrCh:
		case <-ctx.Done():
			t.Fatalf("restarted owner sync did not finish: %v", ctx.Err())
		}
	}
	if newErr != nil && !isOutboxDeliveryDeferred(newErr) {
		t.Fatalf("concurrent restarted owner sync: %v", newErr)
	}
	flushBridgeQueuedNotificationsForTest(t, oldOwner)
	flushBridgeQueuedNotificationsForTest(t, newOwner)
	if _, err := newStore.ClearDrain(ctx); err != nil {
		t.Fatalf("clear shared owner drain: %v", err)
	}

	mu.Lock()
	gotPosts := postCount
	gotSent := append([]string(nil), sent...)
	mu.Unlock()
	if gotPosts != 1 || countStringsContaining(gotSent, "answer during concurrent owner handoff") != 1 {
		state, _ := newStore.Load(ctx)
		t.Fatalf("concurrent owner transcript sends = posts:%d sent:%#v checkpoint=%#v outbox=%#v deliveries=%#v, want one Graph POST", gotPosts, gotSent, state.ImportCheckpoints[transcriptCheckpointID(session.ID)], state.OutboxMessages, state.TranscriptDeliveries)
	}
	state, err := newStore.Load(ctx)
	if err != nil {
		t.Fatalf("load concurrent owner state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.LastOffset != checkpoint.SourceSize || checkpoint.TranscriptQuarantine != nil || checkpoint.UnresolvedExecution != nil {
		t.Fatalf("concurrent owner checkpoint = %#v, want clean EOF without quarantine", checkpoint)
	}
	assertSingleSentTranscriptFinal(t, state, session.ID, path, "answer during concurrent owner handoff", "")

	if err := newOwner.syncLinkedTranscripts(ctx); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("repeat concurrent owner sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, newOwner)
	mu.Lock()
	deferredPosts := postCount
	mu.Unlock()
	if deferredPosts != 1 {
		t.Fatalf("repeat concurrent owner sync added a Graph POST: %d -> %d", gotPosts, deferredPosts)
	}
}

// TestBridgeLinkedTranscriptGraph429RetainsCheckpointUntilOutboxDelivery
// checks the write-side half of a rate-limited outage. A transcript POST that
// receives 429 must leave the source cursor behind the record while retaining
// the queued, source-proofed outbox row; clearing the chat block then sends it
// once and advances the checkpoint.
func TestBridgeLinkedTranscriptGraph429RetainsCheckpointUntilOutboxDelivery(t *testing.T) {
	var mu sync.Mutex
	blocked := false
	requests := 0
	var sent []string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		mu.Lock()
		requests++
		isBlocked := blocked
		mu.Unlock()
		if isBlocked {
			w := httptest.NewRecorder()
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"transcript rate limit"}}`, http.StatusTooManyRequests)
			return w.Result(), nil
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode transcript Graph request: %v", err)
		}
		mu.Lock()
		sent = append(sent, PlainTextFromTeamsHTML(body.Body.Content))
		messageID := len(sent)
		mu.Unlock()
		w := httptest.NewRecorder()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"transcript-sent-%d","messageType":"message"}`, messageID)
		return w.Result(), nil
	})}
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     client,
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep: func(context.Context, time.Duration) error {
			t.Fatal("transcript 429 must return before hidden Retry-After sleep")
			return nil
		},
		jitter: func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-transcript-429"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-transcript-429"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, threadID, path)
	defer restoreDiscover()
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, threadID)
	appendLine(t, path, `{"id":"rate-limited-final","thread_id":"thread-transcript-429","role":"assistant","text":"final after Graph 429"}`)
	checkpointBefore, found, err := store.ImportCheckpoint(context.Background(), transcriptCheckpointID(session.ID))
	if err != nil || !found {
		t.Fatalf("load checkpoint before 429: found=%v err=%v", found, err)
	}
	mu.Lock()
	blocked = true
	mu.Unlock()
	if err := bridge.syncLinkedTranscripts(context.Background()); err == nil || !isGraphRateLimitError(err) {
		state, _ := store.Load(context.Background())
		t.Fatalf("sync under transcript Graph 429 error = %v, want rate-limit error; checkpoint=%#v outbox=%#v deliveries=%#v requests=%d", err, state.ImportCheckpoints[transcriptCheckpointID(session.ID)], state.OutboxMessages, state.TranscriptDeliveries, requests)
	}
	checkpointAfter429, found, err := store.ImportCheckpoint(context.Background(), transcriptCheckpointID(session.ID))
	if err != nil || !found {
		t.Fatalf("load checkpoint after 429: found=%v err=%v", found, err)
	}
	if checkpointAfter429.LastOffset != checkpointBefore.LastOffset || checkpointAfter429.LastRecordID != checkpointBefore.LastRecordID {
		t.Fatalf("checkpoint advanced before transcript delivery: before=%#v after=%#v", checkpointBefore, checkpointAfter429)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after transcript 429: %v", err)
	}
	queued := 0
	for _, outbox := range state.OutboxMessages {
		if outbox.SessionID == session.ID && strings.Contains(outbox.Body, "final after Graph 429") {
			if outbox.Status != teamstore.OutboxStatusQueued {
				t.Fatalf("transcript outbox after 429 = %#v, want queued", outbox)
			}
			queued++
		}
	}
	if queued != 1 {
		t.Fatalf("queued transcript outboxes after 429 = %d, want one; state=%#v", queued, state.OutboxMessages)
	}
	limit, ok := state.ChatRateLimits[session.ChatID]
	if !ok || !limit.BlockedUntil.After(time.Now()) || !strings.Contains(limit.Reason, "429") {
		t.Fatalf("transcript Graph 429 did not persist a chat block: ok=%v limit=%#v", ok, limit)
	}
	var queuedDelivery teamstore.TranscriptDeliveryRecord
	for _, delivery := range state.TranscriptDeliveries {
		if delivery.SessionID == session.ID && delivery.SourceRecordID == "rate-limited-final" {
			queuedDelivery = delivery
			break
		}
	}
	if queuedDelivery.ID == "" || queuedDelivery.Status != teamstore.TranscriptDeliveryStatusQueued || queuedDelivery.SourcePath != path || queuedDelivery.SourceLine != 3 {
		t.Fatalf("queued transcript delivery after 429 = %#v, want source-bound queued row", queuedDelivery)
	}
	proofChecked := false
	for _, outbox := range state.OutboxMessages {
		if outbox.ID != queuedDelivery.OutboxID {
			continue
		}
		proofChecked = true
		if outbox.TranscriptSourcePath != path || !outbox.TranscriptSourceOffsetKnown || outbox.TranscriptSourceProofFingerprint == "" || outbox.TranscriptSourceProofOffset < checkpointBefore.LastOffset {
			t.Fatalf("queued transcript outbox source proof after 429 = %#v, want proofed record after checkpoint", outbox)
		}
	}
	if !proofChecked {
		t.Fatalf("queued transcript delivery %q has no matching outbox row", queuedDelivery.OutboxID)
	}
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("migrate transcript 429 state before restart: %v", err)
	}
	storePath := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close transcript 429 owner store: %v", err)
	}
	recoveredStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen transcript 429 state: %v", err)
	}
	t.Cleanup(func() {
		if err := recoveredStore.Close(); err != nil {
			t.Errorf("close recovered transcript 429 store: %v", err)
		}
	})
	recoveredBridge := newBridgeTestBridge(graph, recoveredStore, &recordingExecutor{})
	recoveredBridge.registryPath = filepath.Join(t.TempDir(), "recovered-registry.json")
	if err := recoveredBridge.restoreRegistryFromStore(context.Background()); err != nil {
		t.Fatalf("restore transcript 429 registry: %v", err)
	}
	recoveredSession := recoveredBridge.reg.SessionByID(session.ID)
	if recoveredSession == nil || recoveredSession.CodexThreadID != threadID {
		t.Fatalf("recovered transcript 429 session = %#v, want linked thread %q", recoveredSession, threadID)
	}

	mu.Lock()
	blocked = false
	mu.Unlock()
	if err := recoveredStore.ClearChatRateLimit(context.Background(), recoveredSession.ChatID); err != nil {
		t.Fatalf("clear transcript chat rate limit: %v", err)
	}
	if err := recoveredBridge.flushPendingOutboxForChat(context.Background(), recoveredSession.ChatID); err != nil {
		t.Fatalf("flush queued transcript after Graph 429 recovery: %v", err)
	}
	mu.Lock()
	sentAfterRecovery := append([]string(nil), sent...)
	mu.Unlock()
	if got := countStringsContaining(sentAfterRecovery, "final after Graph 429"); got != 1 {
		recoveredState, loadErr := recoveredStore.Load(context.Background())
		t.Fatalf("recovered transcript sends = %d, want one: sent=%#v checkpoint=%#v outbox=%#v deliveries=%#v load_err=%v", got, sentAfterRecovery, recoveredState.ImportCheckpoints[transcriptCheckpointID(session.ID)], recoveredState.OutboxMessages, recoveredState.TranscriptDeliveries, loadErr)
	}
	if err := recoveredBridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync after recovered transcript POST: %v", err)
	}
	checkpointAfterRecovery, found, err := recoveredStore.ImportCheckpoint(context.Background(), transcriptCheckpointID(recoveredSession.ID))
	if err != nil || !found || checkpointAfterRecovery.LastOffset != checkpointAfterRecovery.SourceSize {
		t.Fatalf("checkpoint after transcript 429 recovery = %#v found=%v err=%v, want EOF", checkpointAfterRecovery, found, err)
	}
	state, err = recoveredStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after transcript 429 recovery: %v", err)
	}
	var recoveredDelivery teamstore.TranscriptDeliveryRecord
	for _, delivery := range state.TranscriptDeliveries {
		if delivery.SessionID == session.ID && delivery.SourceRecordID == "rate-limited-final" {
			if recoveredDelivery.ID != "" {
				t.Fatalf("multiple transcript deliveries after Graph 429 recovery: first=%#v second=%#v", recoveredDelivery, delivery)
			}
			recoveredDelivery = delivery
		}
	}
	if recoveredDelivery.ID == "" || recoveredDelivery.Status != teamstore.TranscriptDeliveryStatusSent || recoveredDelivery.SourcePath != path || recoveredDelivery.SourceLine != 3 || recoveredDelivery.OutboxID == "" {
		t.Fatalf("recovered transcript delivery = %#v, want one sent source-bound delivery", recoveredDelivery)
	}
	recoveredOutbox, ok := state.OutboxMessages[recoveredDelivery.OutboxID]
	if !ok || recoveredOutbox.Status != teamstore.OutboxStatusSent || recoveredOutbox.TranscriptSourcePath != path || !recoveredOutbox.TranscriptSourceOffsetKnown || recoveredOutbox.TranscriptSourceProofFingerprint == "" || !recoveredOutbox.TranscriptSourceReadProofRangeKnown || recoveredOutbox.TranscriptSourceReadProofFingerprint == "" || recoveredOutbox.TranscriptSourceReadProofStartOffset > checkpointBefore.LastOffset || recoveredOutbox.TranscriptSourceReadProofEndOffset < recoveredOutbox.TranscriptSourceProofOffset {
		t.Fatalf("recovered transcript outbox provenance = %#v found=%v, want sent offset/read-range proof", recoveredOutbox, ok)
	}
	mu.Lock()
	gotRequests := requests
	mu.Unlock()
	if gotRequests != 2 {
		t.Fatalf("transcript Graph POST requests = %d, want one failed and one recovered send", gotRequests)
	}
}

func mustJSONChatMessage(t *testing.T, message ChatMessage) string {
	t.Helper()
	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal test chat message: %v", err)
	}
	return string(raw)
}

func countStringsContaining(values []string, needle string) int {
	count := 0
	for _, value := range values {
		if strings.Contains(value, needle) {
			count++
		}
	}
	return count
}
