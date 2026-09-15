package teams

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgeQueuedRecoveryRejectsUntrustedDirectMessageResponse(t *testing.T) {
	cases := []struct {
		name     string
		response string
	}{
		{
			name:     "wrong message id",
			response: `{"id":"different-message","chatId":"chat-1","messageType":"message","body":{"contentType":"html","content":"<p>wrong message</p>"}}`,
		},
		{
			name:     "wrong chat id",
			response: `{"id":"recovery-message","chatId":"other-chat","messageType":"message","body":{"contentType":"html","content":"<p>wrong chat</p>"}}`,
		},
		{
			name:     "malformed json",
			response: `{`,
		},
		{
			name:     "empty response",
			response: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, migrate := range []bool{false, true} {
				t.Run(map[bool]string{false: "json", true: "sqlite"}[migrate], func(t *testing.T) {
					ctx := context.Background()
					store := newBridgeTestStore(t)
					var gets atomic.Int32
					var posts atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						switch {
						case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages/recovery-message":
							gets.Add(1)
							w.Header().Set("Content-Type", "application/json")
							_, _ = fmt.Fprint(w, tc.response)
						case r.Method == http.MethodPost && r.URL.Path == "/chats/chat-1/messages":
							posts.Add(1)
							w.Header().Set("Content-Type", "application/json")
							_, _ = fmt.Fprint(w, `{"id":"recovery-diagnostic","messageType":"message"}`)
						default:
							t.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
							http.Error(w, "unexpected request", http.StatusBadRequest)
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
					executor := &recordingExecutor{}
					bridge := newBridgeTestBridge(graph, store, executor)
					session := bridge.reg.SessionByChatID("chat-1")
					if err := bridge.ensureDurableSession(ctx, session); err != nil {
						t.Fatalf("ensureDurableSession: %v", err)
					}
					createdAt := time.Now().UTC().Add(-time.Minute)
					inbound, created, err := store.PersistInbound(ctx, teamstore.InboundEvent{
						ID:             "recovery-inbound",
						SessionID:      session.ID,
						TeamsChatID:    session.ChatID,
						TeamsMessageID: "recovery-message",
						TeamsBodyType:  "html",
						Status:         teamstore.InboundStatusPersisted,
						Source:         "teams",
						CreatedAt:      createdAt,
						UpdatedAt:      createdAt,
					})
					if err != nil || !created {
						t.Fatalf("PersistInbound created=%v err=%v", created, err)
					}
					turn, created, err := store.QueueTurn(ctx, teamstore.Turn{
						ID:             "recovery-turn",
						SessionID:      session.ID,
						InboundEventID: inbound.ID,
						Status:         teamstore.TurnStatusQueued,
						CreatedAt:      createdAt,
						UpdatedAt:      createdAt,
					})
					if err != nil || !created {
						t.Fatalf("QueueTurn created=%v err=%v", created, err)
					}
					if migrate {
						if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
							t.Fatalf("MigrateLargeStateToSQLite: %v", err)
						}
					}

					if err := bridge.recoverUnfinishedTurns(ctx); err != nil {
						t.Fatalf("first recoverUnfinishedTurns: %v", err)
					}
					if len(executor.prompts) != 0 {
						t.Fatalf("untrusted response was dispatched: %#v", executor.prompts)
					}
					state, err := store.Load(ctx)
					if err != nil {
						t.Fatalf("Load after first recovery: %v", err)
					}
					got := state.Turns[turn.ID]
					if got.Status != teamstore.TurnStatusInterrupted {
						t.Fatalf("turn status = %q, want interrupted; turn=%#v", got.Status, got)
					}
					if !strings.Contains(got.RecoveryReason, "could not be recovered safely") {
						t.Fatalf("turn recovery reason = %q, want explicit input disposition", got.RecoveryReason)
					}
					if posts.Load() != 1 {
						t.Fatalf("diagnostic Graph posts = %d, want one stable notice", posts.Load())
					}

					if err := bridge.recoverUnfinishedTurns(ctx); err != nil {
						t.Fatalf("second recoverUnfinishedTurns: %v", err)
					}
					if gets.Load() != 1 {
						t.Fatalf("direct message GETs = %d, want no repeat after durable disposition", gets.Load())
					}
					if posts.Load() != 1 {
						t.Fatalf("diagnostic Graph posts after second recovery = %d, want one", posts.Load())
					}
				})
			}
		})
	}
}

func TestPendingLiveFinalDedupeRequiresExactSourceWitness(t *testing.T) {
	now := time.Now().UTC()
	base := teamstore.OutboxMessage{
		ID:                       "outbox:live-final-source-bound",
		SessionID:                "session-source-bound",
		TurnID:                   "turn-source-bound",
		TeamsChatID:              "chat-source-bound",
		Kind:                     "final",
		NotificationKind:         "turn_completed",
		Body:                     "same answer",
		SourceTextHash:           normalizedTextHash("same answer"),
		TranscriptSourceRecordID: "record-a",
		Status:                   teamstore.OutboxStatusQueued,
		CreatedAt:                now,
	}
	state := teamstore.State{OutboxMessages: map[string]teamstore.OutboxMessage{base.ID: base}}
	known := newKnownTranscriptOutboxDedupeState(state, base.SessionID, now.Add(-time.Minute))
	if !known.shouldDeferPendingFinal(TranscriptRecord{Kind: TranscriptKindAssistant, ItemID: "record-a"}, "same answer") {
		t.Fatal("exact pending live final was not deferred")
	}
	for _, tc := range []struct {
		name   string
		record TranscriptRecord
		body   string
		want   bool
	}{
		{name: "different source record", record: TranscriptRecord{Kind: TranscriptKindAssistant, ItemID: "record-b"}, body: "same answer"},
		{name: "different body", record: TranscriptRecord{Kind: TranscriptKindAssistant, ItemID: "record-a"}, body: "different answer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := known.shouldDeferPendingFinal(tc.record, tc.body); got != tc.want {
				t.Fatalf("shouldDeferPendingFinal=%v, want %v", got, tc.want)
			}
		})
	}
	if known.shouldSkip(TranscriptRecord{Kind: TranscriptKindAssistant, ItemID: "record-a"}, "same answer") {
		t.Fatal("pending final was treated as delivered text")
	}

	for _, tc := range []struct {
		name   string
		mutate func(*teamstore.OutboxMessage)
	}{
		{name: "missing source record", mutate: func(message *teamstore.OutboxMessage) { message.TranscriptSourceRecordID = "" }},
		{name: "wrong notification kind", mutate: func(message *teamstore.OutboxMessage) { message.NotificationKind = "owner_notification" }},
		{name: "accepted without Teams id", mutate: func(message *teamstore.OutboxMessage) { message.Status = teamstore.OutboxStatusAccepted }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := base
			tc.mutate(&message)
			candidate := teamstore.State{OutboxMessages: map[string]teamstore.OutboxMessage{message.ID: message}}
			candidateKnown := newKnownTranscriptOutboxDedupeState(candidate, message.SessionID, now.Add(-time.Minute))
			if candidateKnown.shouldDeferPendingFinal(TranscriptRecord{Kind: TranscriptKindAssistant, ItemID: "record-a"}, "same answer") {
				t.Fatal("unsafe pending-final shape suppressed the source")
			}
		})
	}
}

func TestLiveTranscriptBackfillDeliveryTerminalRequiresCompleteDurableWitness(t *testing.T) {
	now := time.Now().UTC()
	record := TranscriptRecord{ItemID: "record-live-status", Kind: TranscriptKindStatus}
	body := "a status update"
	const (
		sessionID = "session-live-status"
		turnID    = "turn-live-status"
		chatID    = "chat-live-status"
		kind      = "codex-status-record-live"
	)
	textHash := normalizedTextHash(body)
	makeState := func(statuses map[int]teamstore.OutboxStatus, deliveryStatuses map[int]teamstore.TranscriptDeliveryStatus, withIDs bool) teamstore.State {
		state := teamstore.State{
			OutboxMessages:       make(map[string]teamstore.OutboxMessage),
			TranscriptDeliveries: make(map[string]teamstore.TranscriptDeliveryRecord),
		}
		for part := 1; part <= 2; part++ {
			outboxID := fmt.Sprintf("outbox:transcript-delivery:%s:part:%d", record.ItemID, part)
			messageID := ""
			if withIDs {
				messageID = fmt.Sprintf("teams-status-%d", part)
			}
			state.OutboxMessages[outboxID] = teamstore.OutboxMessage{
				ID: outboxID, SessionID: sessionID, TurnID: turnID, TeamsChatID: chatID,
				Kind: fmt.Sprintf("%s-%03d", kind, part), Body: body,
				SourceTextHash: textHash, TranscriptSourceRecordID: record.ItemID,
				PartIndex: part, PartCount: 2, Status: statuses[part], TeamsMessageID: messageID,
				CreatedAt: now, UpdatedAt: now,
			}
			deliveryID := fmt.Sprintf("transcript-delivery:%s:part:%d", record.ItemID, part)
			state.TranscriptDeliveries[deliveryID] = teamstore.TranscriptDeliveryRecord{
				ID: deliveryID, SessionID: sessionID, OutboxID: outboxID,
				SourceRecordID: record.ItemID, TextHash: textHash,
				Kind: kind, PartIndex: part, PartCount: 2, Status: deliveryStatuses[part],
				TeamsMessageID: messageID, CreatedAt: now, UpdatedAt: now,
			}
		}
		return state
	}
	terminalOutbox := map[int]teamstore.OutboxStatus{1: teamstore.OutboxStatusSent, 2: teamstore.OutboxStatusSent}
	terminalDelivery := map[int]teamstore.TranscriptDeliveryStatus{1: teamstore.TranscriptDeliveryStatusSent, 2: teamstore.TranscriptDeliveryStatusSent}
	if !liveTranscriptBackfillDeliveryTerminal(makeState(terminalOutbox, terminalDelivery, true), sessionID, turnID, chatID, record, body, kind) {
		t.Fatal("complete terminal multipart witness was rejected")
	}

	cases := []struct {
		name  string
		state teamstore.State
	}{
		{
			name: "fresh sending outbox",
			state: func() teamstore.State {
				statuses := map[int]teamstore.OutboxStatus{1: teamstore.OutboxStatusSending, 2: teamstore.OutboxStatusSent}
				return makeState(statuses, terminalDelivery, true)
			}(),
		},
		{
			name: "accepted without provider identity",
			state: func() teamstore.State {
				statuses := map[int]teamstore.OutboxStatus{1: teamstore.OutboxStatusAccepted, 2: teamstore.OutboxStatusSent}
				return makeState(statuses, terminalDelivery, false)
			}(),
		},
		{
			name: "missing second part",
			state: func() teamstore.State {
				state := makeState(terminalOutbox, terminalDelivery, true)
				delete(state.OutboxMessages, "outbox:transcript-delivery:record-live-status:part:2")
				delete(state.TranscriptDeliveries, "transcript-delivery:record-live-status:part:2")
				return state
			}(),
		},
		{
			name: "second delivery still queued",
			state: func() teamstore.State {
				deliveryStatuses := map[int]teamstore.TranscriptDeliveryStatus{1: teamstore.TranscriptDeliveryStatusSent, 2: teamstore.TranscriptDeliveryStatusQueued}
				return makeState(terminalOutbox, deliveryStatuses, true)
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if liveTranscriptBackfillDeliveryTerminal(tc.state, sessionID, turnID, chatID, record, body, kind) {
				t.Fatal("incomplete durable witness was accepted")
			}
		})
	}

	duplicate := makeState(terminalOutbox, terminalDelivery, true)
	duplicate.OutboxMessages["outbox:transcript-delivery:record-live-status:part:1-duplicate"] = duplicate.OutboxMessages["outbox:transcript-delivery:record-live-status:part:1"]
	duplicate.OutboxMessages["outbox:transcript-delivery:record-live-status:part:1-duplicate"] = func(message teamstore.OutboxMessage) teamstore.OutboxMessage {
		message.ID = "outbox:transcript-delivery:record-live-status:part:1-duplicate"
		return message
	}(duplicate.OutboxMessages["outbox:transcript-delivery:record-live-status:part:1-duplicate"])
	if liveTranscriptBackfillDeliveryTerminal(duplicate, sessionID, turnID, chatID, record, body, kind) {
		t.Fatal("duplicate durable part identities were accepted")
	}
}

func TestBridgeQueuedRecoveryRetiresMissingSessionWithoutOrphanLoop(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			var posts atomic.Int32
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-missing-session/messages" {
					t.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				posts.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"id":"missing-session-diagnostic","messageType":"message"}`)
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
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			now := time.Now().UTC().Add(-time.Minute)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.InboundEvents["missing-session-inbound"] = teamstore.InboundEvent{
					ID: "missing-session-inbound", SessionID: "missing-session", TeamsChatID: "chat-missing-session",
					TeamsMessageID: "missing-session-message", Text: "durable prompt", Source: "teams",
					Status: teamstore.InboundStatusPersisted, CreatedAt: now, UpdatedAt: now,
				}
				state.Turns["missing-session-turn"] = teamstore.Turn{
					ID: "missing-session-turn", SessionID: "missing-session", InboundEventID: "missing-session-inbound",
					Status: teamstore.TurnStatusQueued, CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed missing-session turn: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			if err := bridge.recoverUnfinishedTurns(ctx); err != nil {
				t.Fatalf("first recovery: %v", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after first recovery: %v", err)
			}
			turn := state.Turns["missing-session-turn"]
			if turn.Status != teamstore.TurnStatusInterrupted {
				t.Fatalf("missing-session turn = %#v, want interrupted disposition", turn)
			}
			if !strings.Contains(turn.RecoveryReason, "durable session") {
				t.Fatalf("missing-session recovery reason = %q, want durable-session diagnostic", turn.RecoveryReason)
			}
			notice, ok := state.OutboxMessages["outbox:missing-session-turn:recovery-input"]
			if !ok || notice.TeamsChatID != "chat-missing-session" || notice.SessionID != "" {
				t.Fatalf("missing-session notice = %#v found=%v, want chat-only durable diagnostic", notice, ok)
			}
			if posts.Load() != 1 {
				t.Fatalf("missing-session diagnostic posts = %d, want one", posts.Load())
			}
			if err := bridge.recoverUnfinishedTurns(ctx); err != nil {
				t.Fatalf("second recovery: %v", err)
			}
			if posts.Load() != 1 {
				t.Fatalf("missing-session diagnostic posts after second recovery = %d, want one", posts.Load())
			}
		})
	}
}

func TestPollOversizedRefetchRechecksAccountReadGate(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	var refetches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refetches.Add(1)
		http.Error(w, "refetch must be suppressed by the account gate", http.StatusInternalServerError)
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
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	if _, err := store.SetChatRateLimit(ctx, graphReadAccountRateLimitKey, time.Now().Add(time.Hour), "account read throttle"); err != nil {
		t.Fatalf("set account read gate: %v", err)
	}
	msg := bridgeTestMessage("oversized-gated-refetch")
	msg.oversizedForPoll = true
	result, err := bridge.handlePollMessageWindow(ctx, "chat-1", inboundPollRoleWork, teamstore.ChatPollState{
		ChatID: "chat-1", Seeded: true, PollState: "warm",
	}, true, MessageWindow{Messages: []ChatMessage{msg}}, 20, 1, func(context.Context, ChatMessage, string) error {
		t.Fatal("gated oversized message reached the handler")
		return nil
	})
	var gateErr *graphReadGateActiveError
	if !errors.As(err, &gateErr) {
		t.Fatalf("gated oversized refetch error = %v, want graph read gate", err)
	}
	if result.PendingRecordRefetchFailedID != msg.ID {
		t.Fatalf("pending refetch ID = %q, want %q", result.PendingRecordRefetchFailedID, msg.ID)
	}
	if got := refetches.Load(); got != 0 {
		t.Fatalf("oversized refetches = %d, want zero while account read gate is active", got)
	}
}

func TestAmbiguousOutboxRecoveryRechecksReadGateBetweenPages(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			now := time.Now().UTC().Add(-time.Minute)
			const chatID = "chat-ambiguous-gated-pages"
			const nextPath = "/chats/" + chatID + "/messages?$skiptoken=opaque-next"
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.OutboxMessages["outbox:ambiguous-gated-pages"] = teamstore.OutboxMessage{
					ID: "outbox:ambiguous-gated-pages", TeamsChatID: chatID,
					Status: teamstore.OutboxStatusSending, SendAttemptToken: "attempt-gated-pages",
					CreatedAt: now, LastSendAttempt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed ambiguous outbox: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			var headReads atomic.Int32
			var continuationReads atomic.Int32
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected recovery method: %s", r.Method)
					http.Error(w, "unexpected method", http.StatusBadRequest)
					return
				}
				if r.URL.RequestURI() == nextPath {
					continuationReads.Add(1)
					http.Error(w, "continuation should be gated", http.StatusInternalServerError)
					return
				}
				if r.URL.Path != "/chats/"+chatID+"/messages" {
					t.Errorf("unexpected recovery path: %s", r.URL.RequestURI())
					http.Error(w, "unexpected path", http.StatusBadRequest)
					return
				}
				headReads.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"value":[{"id":"unrelated-message","createdDateTime":"2026-09-13T08:00:00Z","messageType":"message","body":{"contentType":"html","content":"<p>unrelated</p>"}}],"@odata.nextLink":%q}`, server.URL+nextPath)
				if _, err := store.SetChatRateLimit(ctx, graphReadAccountRateLimitKey, time.Now().Add(time.Hour), "account read throttle between recovery pages"); err != nil {
					t.Errorf("set account gate from first page: %v", err)
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
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			outbox, err := store.OutboxMessageByID(ctx, "outbox:ambiguous-gated-pages")
			if err != nil {
				t.Fatalf("load ambiguous outbox: err=%v", err)
			}
			pageBudget := 3
			recovered, err := bridge.recoverAcceptedOutboxFromGraph(ctx, outbox, outboxSendOptions{RecoveryPageBudget: &pageBudget})
			if !recovered {
				t.Fatalf("ambiguous recovery recovered=%v err=%v, want bounded deferred recovery", recovered, err)
			}
			var deferred outboxDeliveryDeferredError
			if !errors.As(err, &deferred) {
				t.Fatalf("ambiguous recovery error=%v, want deferred gate", err)
			}
			if headReads.Load() != 1 || continuationReads.Load() != 0 {
				t.Fatalf("recovery reads head=%d continuation=%d, want 1/0", headReads.Load(), continuationReads.Load())
			}
			stored, err := store.OutboxMessageByID(ctx, outbox.ID)
			if err != nil {
				t.Fatalf("reload ambiguous outbox: err=%v", err)
			}
			if stored.GraphRecoveryNextPath == "" {
				t.Fatalf("ambiguous recovery lost durable next path: %#v", stored)
			}
		})
	}
}
