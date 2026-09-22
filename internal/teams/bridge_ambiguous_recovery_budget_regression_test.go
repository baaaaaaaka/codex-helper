package teams

import (
	"context"
	"encoding/json"
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

// The recovery page budget is a sweep-level bound. Resetting it for every
// ambiguous candidate would let one bounded page multiply the intended Graph
// evidence work by the number of rows, delaying known queued delivery. The
// durable continuation/ambiguous state must remain intact when the shared
// budget is exhausted, and no recovery path may fall through to POST.
func TestBridgeAmbiguousRecoverySharesPageBudgetAcrossCandidates(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	chatID := "chat:shared-recovery-budget"
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		for i := 0; i < 2; i++ {
			id := fmt.Sprintf("outbox:shared-recovery-budget:%d", i)
			state.OutboxMessages[id] = teamstore.OutboxMessage{
				ID: id, TeamsChatID: chatID, Kind: "final",
				Body:            fmt.Sprintf("ambiguous candidate %d", i),
				Status:          teamstore.OutboxStatusSending,
				Sequence:        int64(i + 1),
				CreatedAt:       now.Add(time.Duration(i) * time.Nanosecond),
				UpdatedAt:       now,
				LastSendAttempt: now.Add(-3 * time.Minute),
				LastSendError:   "ambiguous Graph send; response was lost",
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed ambiguous recovery candidates: %v", err)
	}

	var gets atomic.Int32
	var posts atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			gets.Add(1)
			page := r.URL.Query().Get("$skiptoken")
			if page == "" {
				page = "1"
			}
			w.Header().Set("Content-Type", "application/json")
			response := map[string]any{
				"value": []map[string]any{{
					"id":                   "unrelated-recovery-message-" + page,
					"messageType":          "message",
					"createdDateTime":      now.Add(-time.Hour).Format(time.RFC3339Nano),
					"lastModifiedDateTime": now.Add(-time.Hour).Format(time.RFC3339Nano),
					"body":                 map[string]string{"contentType": "html", "content": "<p>unrelated</p>"},
				}},
				"@odata.nextLink": fmt.Sprintf("%s/chats/%s/messages?$skiptoken=%d", server.URL, chatID, gets.Load()+1),
			}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Errorf("encode recovery page: %v", err)
			}
		case http.MethodPost:
			posts.Add(1)
			http.Error(w, "recovery must never POST", http.StatusInternalServerError)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	err := bridge.recoverAmbiguousOutboxMainLoop(ctx)
	var deferred outboxDeliveryDeferredError
	if err != nil && !errors.As(err, &deferred) {
		t.Fatalf("ambiguous recovery sweep error = %v, want only bounded deferral", err)
	}
	if got := gets.Load(); got != int32(outboxRecoveryMaxPagesPerFlush) {
		t.Fatalf("recovery Graph GETs = %d, want shared budget %d", got, outboxRecoveryMaxPagesPerFlush)
	}
	if got := posts.Load(); got != 0 {
		t.Fatalf("recovery Graph POSTs = %d, want zero", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load ambiguous recovery state: %v", err)
	}
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("outbox:shared-recovery-budget:%d", i)
		row := state.OutboxMessages[id]
		if row.Status != teamstore.OutboxStatusSending || strings.TrimSpace(row.TeamsMessageID) != "" || !teamstore.OutboxSendIsAmbiguous(row) {
			t.Fatalf("candidate %s changed unsafely after shared budget: %#v", id, row)
		}
	}
}

func TestBridgeAmbiguousRecoverySkipsSweepWhenAccountReadGateActive(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			now := time.Now().UTC()
			for i := 0; i < 2; i++ {
				id := fmt.Sprintf("outbox:account-read-gate:%d", i)
				queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
					ID: id, TeamsChatID: fmt.Sprintf("chat:account-read-gate:%d", i),
					Kind: "final", Body: fmt.Sprintf("ambiguous account gate candidate %d", i),
				})
				if err != nil {
					t.Fatalf("queue candidate %d: %v", i, err)
				}
				claimed, err := store.MarkOutboxSendAttempt(ctx, queued.ID)
				if err != nil {
					t.Fatalf("claim candidate %d: %v", i, err)
				}
				if err := store.Update(ctx, func(state *teamstore.State) error {
					current := state.OutboxMessages[claimed.ID]
					current.LastSendAttempt = now.Add(-3 * time.Minute)
					current.LastSendError = "ambiguous Graph send; response was lost"
					state.OutboxMessages[claimed.ID] = current
					return nil
				}); err != nil {
					t.Fatalf("age candidate %d: %v", i, err)
				}
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			blockedUntil := now.Add(time.Hour)
			if _, err := store.SetChatRateLimit(ctx, graphReadAccountRateLimitKey, blockedUntil, "account read throttle"); err != nil {
				t.Fatalf("set account read gate: %v", err)
			}
			beforeControl, err := store.ReadControl(ctx)
			if err != nil {
				t.Fatalf("read control before recovery: %v", err)
			}
			beforeRows := make(map[string]teamstore.OutboxMessage, 2)
			for i := 0; i < 2; i++ {
				id := fmt.Sprintf("outbox:account-read-gate:%d", i)
				row, err := store.OutboxMessageByID(ctx, id)
				if err != nil {
					t.Fatalf("read candidate %d before recovery: %v", i, err)
				}
				beforeRows[id] = row
			}

			var graphGets atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					graphGets.Add(1)
				}
				http.Error(w, "unexpected Graph request", http.StatusInternalServerError)
			}))
			t.Cleanup(server.Close)
			bridge := newBridgeTestBridge(&GraphClient{
				auth:       &fakeGraphAuth{token: "access"},
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 0,
				sleep:      sleepContext,
				jitter:     func(d time.Duration) time.Duration { return d },
			}, store, &recordingExecutor{})

			err = bridge.recoverAmbiguousOutboxMainLoop(ctx)
			var deferred outboxDeliveryDeferredError
			if err == nil || !errors.As(err, &deferred) {
				t.Fatalf("account-gated recovery error = %v, want durable deferral", err)
			}
			var gateErr *graphReadGateActiveError
			if !errors.As(err, &gateErr) || gateErr.ChatID != graphReadAccountRateLimitKey {
				t.Fatalf("account-gated recovery error = %v, want account read gate cause", err)
			}
			if got := graphGets.Load(); got != 0 {
				t.Fatalf("account-gated recovery Graph GETs = %d, want zero", got)
			}
			afterControl, err := store.ReadControl(ctx)
			if err != nil {
				t.Fatalf("read control after recovery: %v", err)
			}
			if afterControl.AmbiguousOutboxRecoveryCursor != beforeControl.AmbiguousOutboxRecoveryCursor {
				t.Fatalf("account-gated recovery advanced cursor from %q to %q", beforeControl.AmbiguousOutboxRecoveryCursor, afterControl.AmbiguousOutboxRecoveryCursor)
			}
			for id, before := range beforeRows {
				after, err := store.OutboxMessageByID(ctx, id)
				if err != nil {
					t.Fatalf("read candidate %s after recovery: %v", id, err)
				}
				if after.Status != teamstore.OutboxStatusSending || !teamstore.OutboxSendIsAmbiguous(after) {
					t.Fatalf("account-gated candidate %s changed unsafely: %#v", id, after)
				}
				if after.UpdatedAt != before.UpdatedAt || after.SendAttemptToken != before.SendAttemptToken {
					t.Fatalf("account-gated candidate %s was durably rewritten: before=%#v after=%#v", id, before, after)
				}
			}
		})
	}
}
