package teams

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgePermanentGraph4xxUnblocksLaterOutboxWithoutSilentlyCompletingTranscript(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqliteMode], func(t *testing.T) {
			ctx := context.Background()
			var mu sync.Mutex
			var posts []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
					http.Error(w, "unexpected fake Graph request", http.StatusNotFound)
					return
				}
				var payload struct {
					Body struct {
						Content string `json:"content"`
					} `json:"body"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				mu.Lock()
				posts = append(posts, payload.Body.Content)
				mu.Unlock()
				if strings.Contains(payload.Body.Content, "TRANSCRIPT_PERMANENT_4XX") {
					http.Error(w, `{"error":{"code":"BadRequest","message":"invalid transcript payload"}}`, http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "later-transcript-safe", "messageType": "message"})
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
			first, created, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID: "outbox:bridge-transcript-permanent", SessionID: "s001",
				TeamsChatID: "chat-1", Kind: "status-progress", Body: "TRANSCRIPT_PERMANENT_4XX",
			})
			if err != nil || !created {
				t.Fatalf("queue first outbox: out=%#v created=%v err=%v", first, created, err)
			}
			later, created, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID: "outbox:bridge-transcript-later", SessionID: "s001",
				TeamsChatID: "chat-1", Kind: "status-progress", Body: "TRANSCRIPT_LATER_SAFE",
			})
			if err != nil || !created {
				t.Fatalf("queue later outbox: out=%#v created=%v err=%v", later, created, err)
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.Sessions["s001"] = teamstore.SessionContext{ID: "s001", Status: teamstore.SessionStatusActive, TeamsChatID: "chat-1"}
				state.TranscriptDeliveries["delivery:bridge-transcript-permanent"] = teamstore.TranscriptDeliveryRecord{
					ID: "delivery:bridge-transcript-permanent", SessionID: "s001", OutboxID: first.ID,
					SourcePath: "/codex/session.jsonl", SourceLine: 7, SourceRecordID: "record-7",
					Kind: "status-progress", TextHash: "transcript-permanent-hash",
					Status: teamstore.TranscriptDeliveryStatusQueued,
				}
				return nil
			}); err != nil {
				t.Fatalf("link transcript delivery: %v", err)
			}
			if sqliteMode {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate to SQLite: %v", err)
				}
			}

			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			err = bridge.flushPendingOutboxForChat(ctx, "chat-1")
			if err == nil {
				t.Fatal("flush after permanent Graph 4xx returned nil; want diagnostic error")
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after permanent Graph 4xx: %v", err)
			}
			failed := state.OutboxMessages[first.ID]
			if failed.Status != teamstore.OutboxStatusSkipped {
				t.Fatalf("rejected outbox status=%q, want skipped: %#v", failed.Status, failed)
			}
			if got := state.TranscriptDeliveries["delivery:bridge-transcript-permanent"].Status; got != teamstore.TranscriptDeliveryStatusNeedsAttention {
				t.Fatalf("linked transcript status=%q, want needs_attention", got)
			}
			if got := state.OutboxMessages[later.ID].Status; got != teamstore.OutboxStatusSent {
				t.Fatalf("later same-chat outbox status=%q, want sent", got)
			}
			foundFailedHelper := false
			for _, delivery := range state.HelperDeliveries {
				if delivery.OutboxID == first.ID && delivery.Status == teamstore.HelperDeliveryStatusFailed {
					foundFailedHelper = true
				}
			}
			if !foundFailedHelper {
				t.Fatalf("linked helper delivery was not failed: %#v", state.HelperDeliveries)
			}
			mu.Lock()
			gotPosts := append([]string(nil), posts...)
			mu.Unlock()
			if len(gotPosts) != 2 || !strings.Contains(gotPosts[0], "TRANSCRIPT_PERMANENT_4XX") || !strings.Contains(gotPosts[1], "TRANSCRIPT_LATER_SAFE") {
				t.Fatalf("Graph posts=%#v, want one rejected first and one later success", gotPosts)
			}
		})
	}
}

func TestAutomaticTranscriptSourceDispositionLeavesSettledFailureRepairable(t *testing.T) {
	now := time.Date(2026, 9, 14, 7, 0, 0, 0, time.UTC)
	body := "visible transcript answer"
	baseState := teamstore.State{
		OutboxMessages:       map[string]teamstore.OutboxMessage{},
		TranscriptDeliveries: map[string]teamstore.TranscriptDeliveryRecord{},
	}
	baseState.OutboxMessages["outbox:failed"] = teamstore.OutboxMessage{
		ID: "outbox:failed", SessionID: "s001", TurnID: "import-bg:s001", TeamsChatID: "chat-1",
		Kind: "import-assistant-001", Body: body, PartIndex: 1, PartCount: 1,
		Status: teamstore.OutboxStatusSkipped, LastSendError: "permanent Graph rejection: 422", CreatedAt: now, UpdatedAt: now,
	}
	baseState.TranscriptDeliveries["delivery:failed"] = teamstore.TranscriptDeliveryRecord{
		ID: "delivery:failed", SessionID: "s001", OutboxID: "outbox:failed", SourcePath: "/codex/session.jsonl",
		SourceRecordID: "a1", TextHash: normalizedTextHash(body), PartIndex: 1, PartCount: 1,
		RenderedHash: bodyHash(body), Status: teamstore.TranscriptDeliveryStatusNeedsAttention, CreatedAt: now, UpdatedAt: now,
	}

	covered, pending := automaticTranscriptSourceDisposition(baseState, "s001", "/codex/session.jsonl", "a1", body)
	if covered || pending {
		t.Fatalf("settled permanent failure disposition=(covered=%v,pending=%v), want repairable non-pending", covered, pending)
	}

	state := baseState
	msg := state.OutboxMessages["outbox:failed"]
	msg.Status = teamstore.OutboxStatusSending
	state.OutboxMessages[msg.ID] = msg
	covered, pending = automaticTranscriptSourceDisposition(state, "s001", "/codex/session.jsonl", "a1", body)
	if covered || !pending {
		t.Fatalf("non-skipped NeedsAttention disposition=(covered=%v,pending=%v), want fenced pending", covered, pending)
	}

	state = baseState
	msg = state.OutboxMessages["outbox:failed"]
	msg.Status = teamstore.OutboxStatusSent
	msg.TeamsMessageID = "teams-a1"
	state.OutboxMessages[msg.ID] = msg
	delivery := state.TranscriptDeliveries["delivery:failed"]
	delivery.Status = teamstore.TranscriptDeliveryStatusSent
	delivery.TeamsMessageID = "teams-a1"
	state.TranscriptDeliveries[delivery.ID] = delivery
	covered, pending = automaticTranscriptSourceDisposition(state, "s001", "/codex/session.jsonl", "a1", body)
	if !covered || pending {
		t.Fatalf("terminal delivery disposition=(covered=%v,pending=%v), want covered", covered, pending)
	}
}

func TestAutomaticTranscriptNeedsAttentionDoesNotBecomeCompleteOrPermanentInFlight(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqliteMode], func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			checkpointID := transcriptCheckpointID("s001")
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.Sessions["s001"] = teamstore.SessionContext{ID: "s001", Status: teamstore.SessionStatusActive, TeamsChatID: "chat-1"}
				state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
					ID: checkpointID, SessionID: "s001", SourcePath: "/codex/session.jsonl",
					LastRecordID: "safe-record", LastSourceLine: 7, LastOffset: 700,
					LastOffsetKnown: true, Status: "complete",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed checkpoint: %v", err)
			}
			if sqliteMode {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate to SQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			if err := bridge.markTranscriptImportNeedsAttention(ctx, Session{ID: "s001", ChatID: "chat-1"}, "/codex/session.jsonl", checkpointID, "import:s001", "import"); err != nil {
				t.Fatalf("mark automatic delivery attention: %v", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load attention checkpoint: %v", err)
			}
			checkpoint := state.ImportCheckpoints[checkpointID]
			if checkpoint.Status != "importing" || !checkpoint.DeliveryNeedsAttention {
				t.Fatalf("attention checkpoint=%#v, want importing with durable repair fence", checkpoint)
			}
			if checkpoint.LastRecordID != "safe-record" || checkpoint.LastOffset != 700 {
				t.Fatalf("attention moved source cursor: %#v", checkpoint)
			}
			if active, err := bridge.sessionTranscriptImportInProgress(ctx, "s001"); err != nil {
				t.Fatalf("check attention import activity: %v", err)
			} else if active {
				t.Fatal("repair-fenced automatic import still looks permanently in flight")
			}
			if err := bridge.markTranscriptImportPausedAt(ctx, Session{ID: "s001", ChatID: "chat-1"}, "/codex/session.jsonl", "unsafe-record", 8, 800, checkpointID, "import:s001", "import"); err != nil {
				t.Fatalf("stale pause over attention fence: %v", err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("reload after stale pause: %v", err)
			}
			checkpoint = state.ImportCheckpoints[checkpointID]
			if checkpoint.Status != "importing" || !checkpoint.DeliveryNeedsAttention || checkpoint.LastRecordID != "safe-record" || checkpoint.LastOffset != 700 {
				t.Fatalf("stale pause changed attention fence/cursor: %#v", checkpoint)
			}
			if err := bridge.markTranscriptImportStartedForRun(ctx, Session{ID: "s001", ChatID: "chat-1"}, "/codex/session.jsonl", checkpointID, "publish-history:s001", "sync"); err != nil {
				t.Fatalf("start explicit repair: %v", err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("reload after explicit repair start: %v", err)
			}
			checkpoint = state.ImportCheckpoints[checkpointID]
			if checkpoint.DeliveryNeedsAttention || checkpoint.Status != "importing" || checkpoint.ImportTurnID != "publish-history:s001" {
				t.Fatalf("explicit repair did not clear import fence: %#v", checkpoint)
			}
		})
	}
}
