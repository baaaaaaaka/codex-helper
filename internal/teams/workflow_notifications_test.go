package teams

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestWorkflowNotificationsDisabledByDefault(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	bindBridgeTestControlChat(t, store, "control-chat")
	bridge.queueWorkflowNotificationForSentOutbox(context.Background(), workflowNotificationTestOutbox("outbox-final", "final", "turn_completed"))

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if len(state.Notifications) != 0 {
		t.Fatalf("notifications = %#v, want none", state.Notifications)
	}
	if got := countSentPlainContainingForChat(*sent, "control-chat", "✅ Codex finished"); got != 1 {
		t.Fatalf("control fallback mentions = %d, want 1; sent=%#v", got, *sent)
	}
	if (*sent)[0].Mentions == 0 {
		t.Fatalf("control fallback did not mention owner: %#v", *sent)
	}
	if !strings.Contains((*sent)[0].Content, `<a href="https://teams.microsoft.com/l/chat/`) || !strings.Contains((*sent)[0].Content, `>Open answer</a>`) {
		t.Fatalf("control fallback should render a clickable open link:\n%s", (*sent)[0].Content)
	}
	if plain := PlainTextFromTeamsHTML((*sent)[0].Content); strings.Contains(plain, "teams.microsoft.com") {
		t.Fatalf("control fallback plain text leaked raw URL:\n%s", plain)
	}
}

func TestWorkflowFallbackFormatsLocalSessionMentionAsFocusedClickableMessage(t *testing.T) {
	url := "https://teams.microsoft.com/l/chat/19%3Ameeting_ZWYyZjFlNmUtZTkwYi00YzRkLTliODQtODA1YjE3NzU1YmY2%40thread.v2/0?tenantId=tenant-1"
	body := workflowNotificationFallbackBody(WorkflowNotificationEvent{
		Title:          "💬 New local Codex chat detected",
		ChatTitle:      "💬 ipp1-1551 - 💬 ipp1-1551 - 用 skill 里面的技能刷一下 vbios 试试",
		RequestSummary: "💬 ipp1-1551 - 用 skill 里面的技能刷一下 vbios 试试",
		Hint:           "Open this chat to watch the answer and continue from the full local context.",
		ButtonTitle:    "Open chat",
		ButtonURL:      url,
	}, "")

	if strings.Contains(body, "💬 ipp1-1551 - 💬 ipp1-1551") {
		t.Fatalf("fallback body kept duplicated chat title:\n%s", body)
	}
	if got := strings.Count(body, "用 skill 里面的技能刷一下 vbios 试试"); got != 1 {
		t.Fatalf("fallback body repeated request %d times:\n%s", got, body)
	}
	rendered, mentions := renderOutboxMentionHTML(teamstore.OutboxMessage{
		Kind: "workflow-fallback",
		Body: body,
	}, User{ID: "owner-1", DisplayName: "Jason Wei"})
	for _, want := range []string{
		`<strong>🔧 Helper:</strong> <at id="0">Jason Wei</at>`,
		`<strong>💬 New local Codex chat detected</strong>`,
		`<strong>Chat:</strong> 💬 ipp1-1551 - 用 skill 里面的技能刷一下 vbios 试试`,
		`<a href="` + url + `">Open chat</a>`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered fallback missing %q in:\n%s", want, rendered)
		}
	}
	if len(mentions) != 1 || mentions[0].Text != "Jason Wei" {
		t.Fatalf("mentions = %#v, want Jason Wei", mentions)
	}
	plain := PlainTextFromTeamsHTML(rendered)
	if strings.Contains(plain, "teams.microsoft.com") {
		t.Fatalf("rendered fallback plain text leaked raw URL:\n%s", plain)
	}
	requirePlainTextInOrder(t, plain,
		"🔧 Helper: Jason Wei",
		"💬 New local Codex chat detected",
		"Chat: 💬 ipp1-1551 - 用 skill 里面的技能刷一下 vbios 试试",
		"Open: Open chat",
	)
}

func TestWorkflowFallbackEscapesTextAndDoesNotLinkUnsafeURL(t *testing.T) {
	body := workflowNotificationFallbackBody(WorkflowNotificationEvent{
		Title:          `<img src=x onerror=alert(1)>`,
		ChatTitle:      `chat <b>bad</b>`,
		RequestSummary: `fix <script>alert(1)</script>`,
		Hint:           `open <a href="javascript:alert(1)">bad</a>`,
		ButtonTitle:    `Open <bad>`,
		ButtonURL:      `javascript:alert(1)`,
	}, `reason <iframe src=x>`)

	rendered, _ := renderOutboxMentionHTML(teamstore.OutboxMessage{
		Kind: "workflow-fallback",
		Body: body,
	}, User{ID: "owner-1", DisplayName: "Jason Wei"})
	for _, forbidden := range []string{`<img`, `<script`, `<iframe`, `<a href="javascript:`, `href="javascript:`} {
		if strings.Contains(rendered, forbidden) {
			t.Fatalf("rendered fallback contains unsafe raw HTML/URL %q:\n%s", forbidden, rendered)
		}
	}
	for _, want := range []string{
		`&lt;img src=x onerror=alert(1)&gt;`,
		`fix &lt;script&gt;alert(1)&lt;/script&gt;`,
		`Open &lt;bad&gt;`,
		`javascript:alert(1)`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered fallback missing escaped text %q:\n%s", want, rendered)
		}
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

func TestWorkflowNotificationFlushInvalidPendingDoesNotLoadHotSQLiteTables(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Keep workflow notification flush off hot tables")

	now := time.Now()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		for i := 0; i < 256; i++ {
			id := fmt.Sprintf("inbound-hot-%03d", i)
			state.InboundEvents[id] = teamstore.InboundEvent{
				ID:        id,
				SessionID: "s001",
				Text:      strings.Repeat("hot payload ", 512),
				Status:    teamstore.InboundStatusPersisted,
				CreatedAt: now,
				UpdatedAt: now,
			}
		}
		state.Notifications["workflow:invalid-empty"] = teamstore.NotificationRecord{
			ID:        "workflow:invalid-empty",
			Status:    "",
			CreatedAt: now,
			UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed invalid pending notification: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	urlFile := writeWorkflowWebhookURLFile(t, "https://workflow.example.test/hook")
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	dbPath := filepath.Join(filepath.Dir(store.Path()), "store.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `UPDATE inbound_events SET json = ? WHERE id = ?`, []byte(`{"broken"`), "inbound-hot-000"); err != nil {
		t.Fatalf("corrupt hot inbound row: %v", err)
	}

	if err := bridge.flushPendingWorkflowNotificationsWithLimit(ctx, 1); err != nil {
		t.Fatalf("flushPendingWorkflowNotificationsWithLimit should not load corrupt hot inbound row: %v", err)
	}
}

func TestWorkflowNotificationFlushEnabledNoPendingSkipsWebhookSecretRead(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	bridge.reg.ControlChatID = "control-chat"
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{TeamsChatID: "control-chat"}
		state.Workflow = teamstore.WorkflowNotificationConfig{
			Enabled:               true,
			ControlChatID:         "control-chat",
			ControlWebhookURLFile: filepath.Join(t.TempDir(), "missing-webhook-url"),
			UpdatedAt:             time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed workflow config: %v", err)
	}
	if err := bridge.flushPendingWorkflowNotificationsWithLimit(ctx, 1); err != nil {
		t.Fatalf("flush with no pending notifications should not read missing webhook secret: %v", err)
	}
}

func TestWorkflowNotificationFlushNoPendingSkipsSidecarConfigRead(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	path, err := WorkflowNotificationConfigFilePathForScope(bridge.scope.ID)
	if err != nil {
		t.Fatalf("WorkflowNotificationConfigFilePathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir workflow sidecar dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"enabled":`), 0o600); err != nil {
		t.Fatalf("write invalid workflow sidecar config: %v", err)
	}
	if err := bridge.flushPendingWorkflowNotificationsWithLimit(ctx, 1); err != nil {
		t.Fatalf("flush with no pending notifications should not read invalid sidecar config: %v", err)
	}
}

func TestWorkflowNotificationSendsManualHelperUpgradeCard(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	bridge.reg.ControlChatID = "control-chat"
	bridge.reg.ControlChatURL = TeamsChatURL("control-chat", "tenant-1")

	var seen map[string]any
	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
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

	bridge.queueWorkflowNotificationForSentOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:control:helper-upgrade-complete:manual",
		TeamsChatID:      "control-chat",
		Kind:             "control-upgrade-complete",
		NotificationKind: "helper_upgrade_completed",
		Body:             helperLifecycleCompletedNoticeBody(helperRestartNoticeActionUpgrade, "v1.2.4"),
		MentionOwner:     true,
		Status:           teamstore.OutboxStatusSent,
	})

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls = %d, want 1", got)
	}
	raw, _ := json.Marshal(seen)
	for _, want := range []string{"✅ Helper update completed", "running the updated helper", "Open Control", "teams.microsoft.com"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("workflow payload missing %q: %s", want, raw)
		}
	}
}

func TestWorkflowNotificationSendsHelperActivationAttentionCard(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	bridge.reg.ControlChatID = "control-chat"
	bridge.reg.ControlChatURL = TeamsChatURL("control-chat", "tenant-1")

	var seen map[string]any
	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
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

	bridge.queueWorkflowNotificationForSentOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:control:helper-upgrade-activation-success:mismatch",
		TeamsChatID:      "control-chat",
		Kind:             "mismatched-helper-upgrade-activation",
		NotificationKind: helperUpgradeActivationActionRequiredNotificationKind,
		Body:             "helper activation mismatch",
		MentionOwner:     true,
		Status:           teamstore.OutboxStatusSent,
	})

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls = %d, want 1", got)
	}
	raw, _ := json.Marshal(seen)
	for _, want := range []string{"⚠️ Helper update needs attention", "reports activation completed", "Open Control", "teams.microsoft.com"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("workflow payload missing %q: %s", want, raw)
		}
	}
}

func TestWorkflowNotificationSkipsPlainAutoHelperUpgradeNotice(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)

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

	bridge.queueWorkflowNotificationForSentOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:control:helper-upgrade-complete:auto",
		TeamsChatID: "control-chat",
		Kind:        "control-upgrade-complete",
		Body:        helperLifecycleCompletedNoticeBody(helperRestartNoticeActionUpgrade, "v1.2.4"),
		Status:      teamstore.OutboxStatusSent,
	})

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("webhook calls = %d, want 0 for plain auto helper update notice", got)
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
	graph, sent := newBridgeTestGraph(t)
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
	if len(*sent) != 0 {
		t.Fatalf("ambiguous webhook failure should not also send fallback mention: %#v", *sent)
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

func TestSendQueuedOutboxFallsBackToControlMentionAfterDefiniteWebhookFailure(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Fallback after definite webhook failure")
	var workflowCalls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&workflowCalls, 1)
		http.Error(w, "bad workflow payload", http.StatusBadRequest)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	queued, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:definite-webhook-failure",
		SessionID:        "s001",
		TurnID:           "turn-1",
		TeamsChatID:      "chat-1",
		Kind:             "final",
		Body:             "done",
		MentionOwner:     true,
		NotificationKind: "turn_completed",
	})
	if err != nil {
		t.Fatalf("queue outbox: %v", err)
	}
	if queued.MentionOwner {
		t.Fatalf("queued MentionOwner = true, want false while workflow card is configured: %#v", queued)
	}
	if err := bridge.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{}); err != nil {
		t.Fatalf("sendQueuedOutboxWithOptions: %v", err)
	}
	if got := atomic.LoadInt32(&workflowCalls); got != 1 {
		t.Fatalf("workflow webhook calls = %d, want 1", got)
	}
	var sawWorkChat bool
	var sawFallback bool
	for _, msg := range *sent {
		switch msg.ChatID {
		case "chat-1":
			sawWorkChat = true
			if msg.Mentions != 0 {
				t.Fatalf("work chat mentions = %d, want 0: %#v", msg.Mentions, msg)
			}
		case "control-chat":
			sawFallback = true
			if msg.Mentions == 0 {
				t.Fatalf("control fallback mentions = 0, want owner mention: %#v", msg)
			}
			plain := PlainTextFromTeamsHTML(msg.Content)
			for _, want := range []string{"✅ Codex finished", "Workflow card send failed", "Open answer"} {
				if !strings.Contains(plain, want) {
					t.Fatalf("control fallback missing %q:\n%s", want, plain)
				}
			}
		}
	}
	if !sawWorkChat || !sawFallback {
		t.Fatalf("sent messages missing work or fallback message: %#v", *sent)
	}
}

func TestSendQueuedOutboxFallsBackToControlMentionAfterDefiniteWebhookFailureSQLite(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Fallback after definite webhook failure")
	var workflowCalls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&workflowCalls, 1)
		http.Error(w, "bad workflow payload", http.StatusBadRequest)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}

	queued, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:definite-webhook-failure-sqlite",
		SessionID:        "s001",
		TurnID:           "turn-1",
		TeamsChatID:      "chat-1",
		Kind:             "final",
		Body:             "done",
		MentionOwner:     true,
		NotificationKind: "turn_completed",
	})
	if err != nil {
		t.Fatalf("queue outbox: %v", err)
	}
	if queued.MentionOwner {
		t.Fatalf("queued MentionOwner = true, want false while workflow card is configured: %#v", queued)
	}
	if err := bridge.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{}); err != nil {
		t.Fatalf("sendQueuedOutboxWithOptions: %v", err)
	}
	if got := atomic.LoadInt32(&workflowCalls); got != 1 {
		t.Fatalf("workflow webhook calls = %d, want 1", got)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"done", "Workflow card send failed", "Open answer"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("sqlite fallback missing %q:\n%s\nsent=%#v", want, joined, *sent)
		}
	}
	if got := countSentPlainContainingForChat(*sent, "control-chat", "Workflow card send failed"); got != 1 {
		t.Fatalf("control fallback messages = %d, want 1; sent=%#v", got, *sent)
	}
}

func TestWorkflowNotificationConcurrentFlushClaimsOnce(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Avoid duplicate cards from concurrent flush")

	var calls int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}
	outbox := workflowNotificationTestOutbox("outbox-concurrent", "final", "turn_completed")
	if err := bridge.queueWorkflowNotification(ctx, WorkflowNotificationEvent{
		ID:             "workflow:" + shortStableID(outbox.ID),
		SessionID:      outbox.SessionID,
		TurnID:         outbox.TurnID,
		OutboxID:       outbox.ID,
		Kind:           "turn_completed",
		Title:          "✅ Codex finished",
		ChatTitle:      "💬 test-host - Fix installer",
		RequestSummary: "Avoid duplicate cards from concurrent flush",
		ButtonTitle:    "Open answer",
		ButtonURL:      TeamsChatURL("chat-1", "tenant-1"),
	}); err != nil {
		t.Fatalf("queueWorkflowNotification: %v", err)
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- bridge.flushPendingWorkflowNotifications(ctx)
	}()
	select {
	case <-entered:
	case <-time.After(bridgeAsyncTestTimeout):
		close(release)
		t.Fatal("first webhook request did not start")
	}
	if err := bridge.flushPendingWorkflowNotifications(ctx); err != nil {
		close(release)
		t.Fatalf("second concurrent flush error: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		close(release)
		t.Fatalf("webhook calls while first send in flight = %d, want 1", got)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first flush error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	rec := state.Notifications["workflow:"+shortStableID(outbox.ID)]
	if rec.Status != teamstore.NotificationStatusSent || rec.Attempts != 1 {
		t.Fatalf("notification record = %#v, want sent with one attempt", rec)
	}
}

func TestWorkflowNotificationFlushLimitSendsOneQueuedCard(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Budget webhook sends")

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
	for i := 1; i <= 3; i++ {
		if err := bridge.queueWorkflowNotification(ctx, WorkflowNotificationEvent{
			ID:          fmt.Sprintf("workflow:budget:%d", i),
			Kind:        "turn_completed",
			Title:       "✅ Codex finished",
			ChatTitle:   "💬 test-host - Budget",
			ButtonTitle: "Open answer",
			ButtonURL:   TeamsChatURL("chat-1", "tenant-1"),
		}); err != nil {
			t.Fatalf("queueWorkflowNotification %d: %v", i, err)
		}
	}

	if err := bridge.flushPendingWorkflowNotificationsWithLimit(ctx, 1); err != nil {
		t.Fatalf("flushPendingWorkflowNotificationsWithLimit: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls = %d, want 1", got)
	}
}

func TestWorkflowNotificationStaleSendingBecomesUnknown(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Do not duplicate ambiguous stale sends")

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
	outbox := workflowNotificationTestOutbox("outbox-stale-sending", "final", "turn_completed")
	if err := bridge.queueWorkflowNotification(ctx, WorkflowNotificationEvent{
		ID:             "workflow:" + shortStableID(outbox.ID),
		SessionID:      outbox.SessionID,
		TurnID:         outbox.TurnID,
		OutboxID:       outbox.ID,
		Kind:           "turn_completed",
		Title:          "✅ Codex finished",
		ChatTitle:      "💬 test-host - Fix installer",
		RequestSummary: "Do not duplicate ambiguous stale sends",
		ButtonTitle:    "Open answer",
		ButtonURL:      TeamsChatURL("chat-1", "tenant-1"),
	}); err != nil {
		t.Fatalf("queueWorkflowNotification: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		rec := state.Notifications["workflow:"+shortStableID(outbox.ID)]
		rec.Status = teamstore.NotificationStatusSending
		rec.Attempts = 1
		rec.LastAttemptAt = time.Now().Add(-workflowNotificationSendLease - time.Minute)
		state.Notifications[rec.ID] = rec
		return nil
	}); err != nil {
		t.Fatalf("seed stale sending notification: %v", err)
	}

	if err := bridge.flushPendingWorkflowNotifications(ctx); err != nil {
		t.Fatalf("flush stale sending notification: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("webhook calls for stale ambiguous notification = %d, want 0", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	rec := state.Notifications["workflow:"+shortStableID(outbox.ID)]
	if rec.Status != teamstore.NotificationStatusUnknown || !rec.DeliveryUncertain || rec.LastError == "" {
		t.Fatalf("stale sending notification = %#v, want unknown delivery", rec)
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

func TestWorkflowNotificationFallsBackAfterControlChatRebind(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
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
	bindBridgeTestControlChat(t, store, "new-control-chat")
	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-rebound", "final", "turn_completed"))

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("webhook calls = %d, want 0 after control chat rebind", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if rec := state.Notifications["workflow:"+shortStableID("outbox-rebound")]; rec.ID != "" {
		t.Fatalf("control rebind should not queue a stale workflow card: %#v", rec)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"✅ Codex finished", "Workflow card is unavailable because the control chat changed", "Open answer"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("control-rebind fallback missing %q:\n%s", want, joined)
		}
	}
	if len(*sent) != 1 || (*sent)[0].ChatID != "new-control-chat" || (*sent)[0].Mentions == 0 {
		t.Fatalf("control-rebind fallback sent=%#v, want one owner mention in new control chat", *sent)
	}
}

func TestWorkflowNotificationDoesNotUseWebhookWhenCurrentControlUnknown(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Control binding disappeared")

	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Workflow = teamstore.WorkflowNotificationConfig{
			Enabled:               true,
			ControlWebhookURLFile: urlFile,
			ControlChatID:         "control-chat",
		}
		state.ControlChat = teamstore.ControlChatBinding{}
		return nil
	}); err != nil {
		t.Fatalf("seed workflow config: %v", err)
	}
	bridge.reg.ControlChatID = ""
	bridge.reg.ControlChatURL = ""

	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-unknown-control", "final", "turn_completed"))

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("webhook calls = %d, want 0 when configured control chat cannot be matched", got)
	}
	if len(*sent) != 0 {
		t.Fatalf("fallback messages = %#v, want none without a durable control fallback", *sent)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if rec := state.Notifications["workflow:"+shortStableID("outbox-unknown-control")]; rec.ID != "" {
		t.Fatalf("unknown control should not queue a stale workflow card: %#v", rec)
	}
}

func TestWorkflowNotificationSkipsHistoricalImportsAndNotifiesDetectedSyncFinals(t *testing.T) {
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
	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-sync-status", "sync-status-001", ""))
	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-part-1", "final-001", ""))
	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-sync-final", "sync-assistant-001", "turn_completed"))
	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-part-2", "final-002", "turn_completed"))

	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("webhook calls = %d, want 2", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if len(state.Notifications) != 2 {
		t.Fatalf("notifications = %#v, want live final and detected sync final notifications", state.Notifications)
	}
	if _, ok := state.Notifications["workflow:"+shortStableID("outbox-sync-final")]; !ok {
		t.Fatalf("sync assistant final notification missing: %#v", state.Notifications)
	}
	if _, ok := state.Notifications["workflow:"+shortStableID("outbox-part-2")]; !ok {
		t.Fatalf("live final notification missing: %#v", state.Notifications)
	}
}

func TestWorkflowNotificationSendsDetectedCodexAnswerOnce(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Detected answer should notify once")

	var seen map[string]any
	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
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
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test session missing")
	}

	bridge.queueWorkflowNotificationForDetectedCodexAnswer(ctx, session, "codex-final-key")
	bridge.queueWorkflowNotificationForDetectedCodexAnswer(ctx, session, "codex-final-key")

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("webhook calls before flush = %d, want deferred delivery", got)
	}
	if err := bridge.flushPendingWorkflowNotificationsWithLimit(ctx, mainLoopWorkflowFlushMaxNotifications); err != nil {
		t.Fatalf("flushPendingWorkflowNotificationsWithLimit: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls = %d, want 1", got)
	}
	raw, _ := json.Marshal(seen)
	for _, want := range []string{"✅ Codex finished", "Open answer", "teams.microsoft.com"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("workflow payload missing %q: %s", want, raw)
		}
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	key := "workflow:detected-codex-answer:" + shortStableID(session.ID)
	if rec := state.Notifications[key]; rec.Status != teamstore.NotificationStatusSent || rec.Attempts != 1 {
		t.Fatalf("detected answer notification = %#v, want sent once", rec)
	}
}

func TestWorkflowNotificationSendsInterruptedAfterRestartNeedsAttention(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Resume after helper restart")

	var seen map[string]any
	var calls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
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

	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-interrupted-after-restart", "interrupted-after-restart", "needs_attention"))
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("webhook calls = %d, want 1", got)
	}
	raw, _ := json.Marshal(seen)
	for _, want := range []string{"⚠️ Codex needs attention", "Open chat", "Resume after helper restart"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("workflow payload missing %q: %s", want, raw)
		}
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
	interrupted, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:interrupted-after-restart",
		SessionID:        "s001",
		TurnID:           "turn-interrupted",
		TeamsChatID:      "chat-1",
		Kind:             "interrupted-after-restart",
		Body:             "turn was interrupted after helper restart",
		MentionOwner:     true,
		NotificationKind: "needs_attention",
	})
	if err != nil {
		t.Fatalf("queue interrupted outbox: %v", err)
	}
	if interrupted.MentionOwner {
		t.Fatalf("interrupted MentionOwner = true, want false when workflow card is enabled: %#v", interrupted)
	}
	chunkedError, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:error-001",
		SessionID:        "s001",
		TurnID:           "turn-error",
		TeamsChatID:      "chat-1",
		Kind:             "error-001",
		Body:             "chunked error",
		MentionOwner:     true,
		NotificationKind: "needs_attention",
	})
	if err != nil {
		t.Fatalf("queue chunked error outbox: %v", err)
	}
	if chunkedError.MentionOwner {
		t.Fatalf("chunked error MentionOwner = true, want false when workflow card is enabled: %#v", chunkedError)
	}
	helperAttention, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:helper-needs-attention",
		SessionID:        "s001",
		TurnID:           "turn-helper-attention",
		TeamsChatID:      "chat-1",
		Kind:             "helper",
		Body:             "helper needs attention",
		MentionOwner:     true,
		NotificationKind: "needs_attention",
	})
	if err != nil {
		t.Fatalf("queue helper needs-attention outbox: %v", err)
	}
	if helperAttention.MentionOwner {
		t.Fatalf("helper needs-attention MentionOwner = true, want false when workflow card is enabled: %#v", helperAttention)
	}
}

func TestTranscriptDeliverySuppressesOwnerMentionWhenWorkflowNotificationEnabled(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Suppress transcript sync mention when webhook card will notify")
	urlFile := writeWorkflowWebhookURLFile(t, "https://workflow.example.test/hook")
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	session := bridge.reg.Sessions[0]
	record := TranscriptRecord{
		ItemID:       "assistant-final-1",
		ThreadID:     "thread-1",
		Kind:         TranscriptKindAssistant,
		Text:         "synced final answer",
		SourceLine:   42,
		SourceOffset: 4096,
	}
	queued, err := bridge.queueTranscriptDeliveryChunksWithOptions(ctx, session, codexhistory.Session{
		SessionID: "thread-1",
		FilePath:  filepath.Join(t.TempDir(), "session.jsonl"),
	}, record, record.SourceLine, record.SourceOffset, "sync-assistant-b7a357a4d7f59c5d", record.Text, outboxQueueOptions{
		MentionOwner:     true,
		NotificationKind: "turn_completed",
	}, "sync:"+session.ID)
	if err != nil {
		t.Fatalf("queueTranscriptDeliveryChunksWithOptions: %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("queued transcript outbox len = %d, want 1: %#v", len(queued), queued)
	}
	outbox := queued[0]
	if !strings.HasPrefix(outbox.ID, "outbox:transcript-delivery:"+session.ID+":") {
		t.Fatalf("outbox id = %q, want transcript delivery outbox", outbox.ID)
	}
	if outbox.MentionOwner {
		t.Fatalf("transcript delivery MentionOwner = true, want false when workflow card is enabled: %#v", outbox)
	}
	if outbox.NotificationKind != "turn_completed" {
		t.Fatalf("NotificationKind = %q, want retained for workflow planning", outbox.NotificationKind)
	}
}

func TestSendQueuedOutboxSuppressesPersistedOwnerMentionWhenWorkflowNotificationEnabled(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Suppress persisted mention when webhook card will notify")
	var workflowCalls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&workflowCalls, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:transcript-delivery:s001:legacy",
		SessionID:        "s001",
		TurnID:           "sync:s001",
		TeamsChatID:      "chat-1",
		Kind:             "sync-assistant-b7a357a4d7f59c5d",
		Body:             "legacy queued answer",
		MentionOwner:     true,
		NotificationKind: "turn_completed",
	})
	if err != nil {
		t.Fatalf("seed legacy outbox: %v", err)
	}
	if err := bridge.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{}); err != nil {
		t.Fatalf("sendQueuedOutboxWithOptions: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent Teams messages = %d, want 1: %#v", len(*sent), *sent)
	}
	if (*sent)[0].Mentions != 0 {
		t.Fatalf("sent Teams mentions = %d, want 0: %#v", (*sent)[0].Mentions, *sent)
	}
	if got := atomic.LoadInt32(&workflowCalls); got != 1 {
		t.Fatalf("workflow webhook calls = %d, want 1", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	stored := state.OutboxMessages[queued.ID]
	if stored.MentionOwner {
		t.Fatalf("stored MentionOwner = true, want false after send-time suppression: %#v", stored)
	}
}

func TestHelperNeedsAttentionQueuesWorkflowCardWithoutOwnerMention(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Notify helper needs-attention via workflow card")
	var workflowCalls int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&workflowCalls, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	bridge.httpClient = server.Client()
	urlFile := writeWorkflowWebhookURLFile(t, server.URL)
	if _, err := bridge.ConfigureWorkflowNotifications(ctx, urlFile, true); err != nil {
		t.Fatalf("ConfigureWorkflowNotifications: %v", err)
	}

	queued, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:helper-attention",
		SessionID:        "s001",
		TurnID:           "turn-helper-attention",
		TeamsChatID:      "chat-1",
		Kind:             "helper",
		Body:             "helper needs attention",
		MentionOwner:     true,
		NotificationKind: "needs_attention",
	})
	if err != nil {
		t.Fatalf("queue helper needs-attention outbox: %v", err)
	}
	if queued.MentionOwner {
		t.Fatalf("queued helper needs-attention MentionOwner = true, want false when workflow card is enabled: %#v", queued)
	}
	if err := bridge.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{}); err != nil {
		t.Fatalf("sendQueuedOutboxWithOptions: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent Teams messages = %d, want 1: %#v", len(*sent), *sent)
	}
	if (*sent)[0].Mentions != 0 {
		t.Fatalf("sent Teams mentions = %d, want 0: %#v", (*sent)[0].Mentions, *sent)
	}
	if got := atomic.LoadInt32(&workflowCalls); got != 1 {
		t.Fatalf("workflow webhook calls = %d, want 1", got)
	}
}

func TestQueueOutboxRedirectsOwnerMentionToControlFallbackWhenWorkflowNotificationDisabled(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Use control chat fallback without webhook card")

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
	if queued.MentionOwner {
		t.Fatalf("MentionOwner = true, want false because control chat fallback will mention owner: %#v", queued)
	}
}

func TestSendQueuedOutboxRedirectsOwnerMentionToControlFallbackWhenWorkflowNotificationDisabled(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Use control chat fallback without webhook card")

	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:legacy-work-mention",
		SessionID:        "s001",
		TurnID:           "turn-1",
		TeamsChatID:      "chat-1",
		Kind:             "final",
		Body:             "done",
		MentionOwner:     true,
		NotificationKind: "turn_completed",
	})
	if err != nil {
		t.Fatalf("seed legacy outbox: %v", err)
	}
	if err := bridge.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{}); err != nil {
		t.Fatalf("sendQueuedOutboxWithOptions: %v", err)
	}

	var sawWorkChat bool
	var sawControlFallback bool
	for _, msg := range *sent {
		switch msg.ChatID {
		case "chat-1":
			sawWorkChat = true
			if msg.Mentions != 0 {
				t.Fatalf("work chat mentions = %d, want 0: %#v", msg.Mentions, msg)
			}
		case "control-chat":
			sawControlFallback = true
			if msg.Mentions == 0 {
				t.Fatalf("control fallback mentions = 0, want owner mention: %#v", msg)
			}
			plain := PlainTextFromTeamsHTML(msg.Content)
			for _, want := range []string{"✅ Codex finished", "Open answer"} {
				if !strings.Contains(plain, want) {
					t.Fatalf("control fallback missing %q:\n%s", want, plain)
				}
			}
		}
	}
	if !sawWorkChat || !sawControlFallback {
		t.Fatalf("sent messages missing work or fallback message: %#v", *sent)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if stored := state.OutboxMessages[queued.ID]; stored.MentionOwner {
		t.Fatalf("stored MentionOwner = true, want false after fallback suppression: %#v", stored)
	}
}

func TestQueueOutboxKeepsOwnerMentionWhenNoCardOrControlFallbackExists(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	bridge.reg.ControlChatID = ""
	bridge.reg.ControlChatURL = ""
	seedWorkflowNotificationState(t, store, "Keep work chat mention without card or control fallback")
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{}
		return nil
	}); err != nil {
		t.Fatalf("clear control chat: %v", err)
	}

	queued, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:mention-kept-without-control",
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
		t.Fatalf("MentionOwner = false, want true when no workflow card or control fallback exists: %#v", queued)
	}
}

func TestWorkflowNotificationFallsBackToControlMentionWhenWebhookConfigInvalid(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)
	seedWorkflowNotificationState(t, store, "Invalid webhook should still notify")
	badPath := filepath.Join(t.TempDir(), "webhook-url")
	if err := os.WriteFile(badPath, []byte("http://workflow.example.test/hook"), 0o600); err != nil {
		t.Fatalf("write invalid webhook file: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Workflow = teamstore.WorkflowNotificationConfig{
			Enabled:               true,
			ControlWebhookURLFile: badPath,
			ControlChatID:         "control-chat",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed invalid workflow config: %v", err)
	}

	bridge.queueWorkflowNotificationForSentOutbox(ctx, workflowNotificationTestOutbox("outbox-invalid-webhook", "final", "turn_completed"))

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"✅ Codex finished", "Workflow card is unavailable", "absolute https URL", "Open answer"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("fallback message missing %q:\n%s", want, joined)
		}
	}
	if len(*sent) != 1 || (*sent)[0].ChatID != "control-chat" || (*sent)[0].Mentions == 0 {
		t.Fatalf("invalid webhook fallback sent=%#v, want one owner mention in control chat", *sent)
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

func TestConfigureWorkflowNotificationsFromWebhookURLWritesPrivateSecret(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newWorkflowNotificationTestBridge(t, graph, store)

	cfg, err := bridge.ConfigureWorkflowNotificationsFromWebhookURL(ctx, "https://workflow.example.test/secret-token")
	if err != nil {
		t.Fatalf("ConfigureWorkflowNotificationsFromWebhookURL error: %v", err)
	}
	if !cfg.Enabled || cfg.ControlWebhookURLFile == "" || cfg.ControlChatID != "control-chat" {
		t.Fatalf("workflow config = %#v, want enabled and bound to control chat", cfg)
	}
	if filepath.Base(cfg.ControlWebhookURLFile) != workflowWebhookURLFileName {
		t.Fatalf("webhook URL file = %q, want basename %q", cfg.ControlWebhookURLFile, workflowWebhookURLFileName)
	}
	info, err := os.Lstat(cfg.ControlWebhookURLFile)
	if err != nil {
		t.Fatalf("stat webhook URL file: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("webhook URL file must not be a symlink")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("webhook URL file permissions = %v, want private", info.Mode().Perm())
	}
	got, err := readWorkflowWebhookURLFile(cfg.ControlWebhookURLFile)
	if err != nil {
		t.Fatalf("read stored webhook URL file: %v", err)
	}
	if got != "https://workflow.example.test/secret-token" {
		t.Fatalf("stored webhook URL = %q", got)
	}
}

func seedWorkflowNotificationState(t *testing.T, store *teamstore.Store, request string) {
	t.Helper()
	now := time.Now()
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{
			TeamsChatID:  "control-chat",
			TeamsChatURL: TeamsChatURL("control-chat", "tenant-1"),
		}
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
