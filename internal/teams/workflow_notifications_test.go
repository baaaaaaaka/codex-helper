package teams

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestWorkflowNotificationsDisabledByDefault(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	bridge.queueWorkflowNotificationForSentOutbox(context.Background(), workflowNotificationTestOutbox("outbox-final", "final", "turn_completed"))

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if len(state.Notifications) != 0 {
		t.Fatalf("notifications = %#v, want none", state.Notifications)
	}
}

func TestWorkflowNotificationSendsAfterDurableOutboxSent(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Fix the installer regression and keep the answer concise")

	var seen map[string]any
	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	outbox := workflowNotificationTestOutbox("outbox-final", "final", "turn_completed")
	bridge.queueWorkflowNotificationForSentOutbox(ctx, outbox)
	bridge.queueWorkflowNotificationForSentOutbox(ctx, outbox)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls = %d, want 1", got)
	}
	raw, _ := json.Marshal(seen)
	for _, want := range []string{"✅ Codex finished", "💬", "Fix the installer regression", "Open answer", "teams.microsoft.com"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("workflow payload missing %q: %s", want, raw)
		}
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	rec := state.Notifications["workflow:"+shortStableID(outbox.ID)]
	if rec.Status != teamstore.NotificationStatusSent || rec.Attempts != 1 || rec.SentAt.IsZero() {
		t.Fatalf("notification record = %#v, want sent with one attempt", rec)
	}
}

func TestWorkflowNotificationFallsBackToSidecarConfigWhenOldHelperErasesState(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Finish after reload")

	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Workflow = teamstore.WorkflowNotificationConfig{}
		return nil
	}); err != nil {
		t.Fatalf("simulate old helper erasing workflow state: %v", err)
	}

	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-sidecar", "final", "turn_completed"))
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls = %d, want 1 from sidecar config", got)
	}
}

func TestWorkflowNotificationDoesNotDuplicateAfterAmbiguousFailure(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Avoid duplicate cards after an ambiguous webhook result")

	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "temporary", http.StatusInternalServerError)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	outbox := workflowNotificationTestOutbox("outbox-ambiguous", "final", "turn_completed")
	bridge.queueWorkflowNotificationForSentOutbox(ctx, outbox)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls after failure = %d, want 1", got)
	}
	if err := bridge.flushPendingWorkflowNotifications(ctx); err != nil {
		t.Fatalf("flush should skip delivery-uncertain records, got %v", err)
	}
	bridge.queueWorkflowNotificationForSentOutbox(ctx, outbox)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls after requeue = %d, want 1", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	rec := state.Notifications["workflow:"+shortStableID(outbox.ID)]
	if rec.Status != teamstore.NotificationStatusUnknown || rec.Attempts != 1 || !rec.DeliveryUncertain || rec.LastErrorRetryable {
		t.Fatalf("notification record after ambiguous failure = %#v, want unknown delivery with one attempt", rec)
	}
}

func TestWorkflowNotificationRetriesOnlyRetryableFailuresWithBackoff(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Retry only explicit retryable webhook failures")

	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	outbox := workflowNotificationTestOutbox("outbox-retryable", "final", "turn_completed")
	if err := bridge.queueWorkflowNotification(ctx, WorkflowNotificationEvent{
		ID:             "workflow:" + shortStableID(outbox.ID),
		SessionID:      outbox.SessionID,
		TurnID:         outbox.TurnID,
		OutboxID:       outbox.ID,
		Kind:           "turn_completed",
		Title:          "✅ Codex finished",
		ChatTitle:      "💬 test-host - Fix installer",
		RequestSummary: "Retry only after explicit backoff",
		ButtonTitle:    "Open answer",
		ButtonURL:      TeamsChatURL("chat-1", "tenant-1"),
	}); err != nil {
		t.Fatalf("queueWorkflowNotification: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		rec := state.Notifications["workflow:"+shortStableID(outbox.ID)]
		rec.Status = teamstore.NotificationStatusFailed
		rec.Attempts = 1
		rec.LastError = "Teams workflow webhook failed: HTTP 429 Too Many Requests"
		rec.LastErrorRetryable = true
		rec.LastAttemptAt = time.Now()
		state.Notifications[rec.ID] = rec
		return nil
	}); err != nil {
		t.Fatalf("seed retryable failure: %v", err)
	}
	if err := bridge.flushPendingWorkflowNotifications(ctx); err != nil {
		t.Fatalf("immediate flush should skip retry backoff, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("webhook calls during backoff = %d, want 0", got)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		rec := state.Notifications["workflow:"+shortStableID(outbox.ID)]
		rec.LastAttemptAt = time.Now().Add(-10 * time.Minute)
		state.Notifications[rec.ID] = rec
		return nil
	}); err != nil {
		t.Fatalf("age notification retry: %v", err)
	}
	if err := bridge.flushPendingWorkflowNotifications(ctx); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls after retry = %d, want 1", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	rec := state.Notifications["workflow:"+shortStableID(outbox.ID)]
	if rec.Status != teamstore.NotificationStatusSent || rec.Attempts != 2 || rec.LastError != "" || rec.LastErrorRetryable || rec.DeliveryUncertain {
		t.Fatalf("notification record after retry = %#v, want sent with two attempts", rec)
	}
}

func TestWorkflowNotificationSuppressesAfterControlChatRebind(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Control changed")

	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}
	bridge.reg.ControlChatID = "new-control-chat"
	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-rebound", "final", "turn_completed"))

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("webhook calls = %d, want 0 after control chat rebind", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	rec := state.Notifications["workflow:"+shortStableID("outbox-rebound")]
	if rec.Status != teamstore.NotificationStatusQueued {
		t.Fatalf("notification status = %q, want queued while control binding mismatches", rec.Status)
	}
}

func TestWorkflowNotificationSkipsHistoryAndNonFinalParts(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Only final completion should notify")

	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-import", "import-history", ""))
	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-part-1", "final-001", ""))
	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-part-2", "final-002", "turn_completed"))

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls = %d, want 1", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if len(state.Notifications) != 1 {
		t.Fatalf("notifications = %#v, want only final completion notification", state.Notifications)
	}
}

func TestWorkflowNotificationSupportsRecreatedChatMovedNotice(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Continue after chat migration")

	var seen map[string]any
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-chat-moved", "chat-moved", "chat_recreated"))
	raw, _ := json.Marshal(seen)
	for _, want := range []string{"🔁 Codex chat moved", "Open new chat", "Continue after chat migration"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("workflow payload missing %q: %s", want, raw)
		}
	}
}

func TestQueueOutboxSuppressesOwnerMentionWhenWorkflowNotificationEnabled(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Suppress graph mention when webhook card will notify")
	urlFile := writeWorkflowWebhookURLFile(t, "https://workflow.example.test/hook")
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	queued, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:mention-suppressed",
		SessionID:        "s001",
		TurnID:           "turn-1",
		TeamsChatID:      "chat-1",
		Kind:             "final",
		Body:             "done",
		MentionOwner:     true,
		NotificationKind: "turn_completed",
	})
	if err != nil {
		t.Fatalf("queueOutbox: %v", err)
	}
	if queued.MentionOwner {
		t.Fatalf("MentionOwner = true, want false when workflow card is enabled: %#v", queued)
	}
	if queued.NotificationKind != "turn_completed" {
		t.Fatalf("NotificationKind = %q, want retained for workflow planning", queued.NotificationKind)
	}
	moved, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:chat-moved",
		SessionID:        "s001",
		TeamsChatID:      "old-chat",
		Kind:             "chat-moved",
		Body:             "chat moved",
		MentionOwner:     true,
		NotificationKind: "chat_recreated",
	})
	if err != nil {
		t.Fatalf("queue moved outbox: %v", err)
	}
	if moved.MentionOwner {
		t.Fatalf("chat moved MentionOwner = true, want false when workflow card is enabled: %#v", moved)
	}
}

func TestQueueOutboxKeepsOwnerMentionWhenWorkflowNotificationDisabled(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Keep graph mention without webhook card")

	queued, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:mention-kept",
		SessionID:        "s001",
		TurnID:           "turn-1",
		TeamsChatID:      "chat-1",
		Kind:             "final",
		Body:             "done",
		MentionOwner:     true,
		NotificationKind: "turn_completed",
	})
	if err != nil {
		t.Fatalf("queueOutbox: %v", err)
	}
	if !queued.MentionOwner {
		t.Fatalf("MentionOwner = false, want true when workflow card is disabled: %#v", queued)
	}
}

func TestWorkflowWebhookURLFileSafety(t *testing.T) {
	path := writeWorkflowWebhookURLFile(t, "https://workflow.example.test/hook")
	if err := ValidateWorkflowWebhookURLFile(path); err != nil {
		t.Fatalf("ValidateWorkflowWebhookURLFile private file: %v", err)
	}

	link := path + ".link"
	if err := os.Symlink(path, link); err == nil {
		if err := ValidateWorkflowWebhookURLFile(link); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("expected symlink rejection, got %v", err)
		}
	} else if runtime.GOOS != "windows" {
		t.Fatalf("create symlink: %v", err)
	}

	if runtime.GOOS != "windows" {
		openPath := path + ".open"
		if err := os.WriteFile(openPath, []byte("https://workflow.example.test/hook"), 0o644); err != nil {
			t.Fatalf("write open URL file: %v", err)
		}
		if err := ValidateWorkflowWebhookURLFile(openPath); err == nil || !strings.Contains(err.Error(), "permissions") {
			t.Fatalf("expected permissions rejection, got %v", err)
		}

		openDir := filepath.Join(t.TempDir(), "open-dir")
		if err := os.Mkdir(openDir, 0o700); err != nil {
			t.Fatalf("make open parent dir: %v", err)
		}
		if err := os.Chmod(openDir, 0o777); err != nil {
			t.Fatalf("chmod open parent dir: %v", err)
		}
		parentOpenPath := filepath.Join(openDir, "webhook-url")
		if err := os.WriteFile(parentOpenPath, []byte("https://workflow.example.test/hook"), 0o600); err != nil {
			t.Fatalf("write parent-open URL file: %v", err)
		}
		if err := ValidateWorkflowWebhookURLFile(parentOpenPath); err == nil || !strings.Contains(err.Error(), "parent directories") {
			t.Fatalf("expected parent directory rejection, got %v", err)
		}
	}

	badPath := path + ".bad"
	if err := os.WriteFile(badPath, []byte("http://workflow.example.test/hook"), 0o600); err != nil {
		t.Fatalf("write bad URL file: %v", err)
	}
	if err := ValidateWorkflowWebhookURLFile(badPath); err == nil || !strings.Contains(err.Error(), "https URL") {
		t.Fatalf("expected URL validation error, got %v", err)
	}
}

func seedWorkflowNotificationState(t *testing.T, store *teamstore.Store, request string) {
	t.Helper()
	now := time.Now()
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions["s001"] = teamstore.SessionContext{
			ID:           "s001",
			Status:       teamstore.SessionStatusActive,
			TeamsChatID:  "chat-1",
			TeamsChatURL: TeamsChatURL("chat-1", "tenant-1"),
			TeamsTopic:   "💬 test-host - Fix installer",
			UserTitle:    "Fix installer",
			Cwd:          "/home/user/project",
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		state.Turns["turn-1"] = teamstore.Turn{
			ID:             "turn-1",
			SessionID:      "s001",
			InboundEventID: "inbound-1",
			Status:         teamstore.TurnStatusCompleted,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		state.InboundEvents["inbound-1"] = teamstore.InboundEvent{
			ID:             "inbound-1",
			SessionID:      "s001",
			TeamsChatID:    "chat-1",
			TeamsMessageID: "teams-message-1",
			Text:           request,
			Status:         teamstore.InboundStatusPersisted,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed workflow notification state: %v", err)
	}
}

func newWorkflowNotificationTestBridge(t *testing.T, graph *GraphClient, store *teamstore.Store) *Bridge {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.scope.ID = "scope:" + shortStableID(store.Path())
	bridge.reg.ControlChatURL = TeamsChatURL("control-chat", "tenant-1")
	if len(bridge.reg.Sessions) > 0 {
		bridge.reg.Sessions[0].ChatURL = TeamsChatURL("chat-1", "tenant-1")
	}
	return bridge
}

func workflowNotificationTestOutbox(id string, kind string, notificationKind string) teamstore.OutboxMessage {
	return teamstore.OutboxMessage{
		ID:               id,
		SessionID:        "s001",
		TurnID:           "turn-1",
		TeamsChatID:      "chat-1",
		Kind:             kind,
		NotificationKind: notificationKind,
		Body:             "done",
		Status:           teamstore.OutboxStatusSent,
	}
}

func writeWorkflowWebhookURLFile(t *testing.T, raw string) string {
	t.Helper()
	path := t.TempDir() + "/webhook-url"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write webhook URL file: %v", err)
	}
	return path
}
