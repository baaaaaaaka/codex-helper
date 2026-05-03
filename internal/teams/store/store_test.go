package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoadMissingReturnsEmptyState(t *testing.T) {
	store := newTestStore(t)

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", state.SchemaVersion, SchemaVersion)
	}
	if len(state.Sessions) != 0 || len(state.Turns) != 0 || len(state.InboundEvents) != 0 || len(state.OutboxMessages) != 0 {
		t.Fatalf("missing store should be empty: %#v", state)
	}
	if _, err := os.Stat(store.Path()); !os.IsNotExist(err) {
		t.Fatalf("Load should not create state file, stat err = %v", err)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.SetPaused(ctx, true, "operator maintenance"); err != nil {
		t.Fatalf("SetPaused error: %v", err)
	}
	if _, err := store.SetDraining(ctx, "upgrade"); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}
	inbound, created, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	if !created {
		t.Fatal("PersistInbound created = false")
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if !created {
		t.Fatal("QueueTurn created = false")
	}
	if _, err := store.MarkTurnCompleted(ctx, turn.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnCompleted error: %v", err)
	}
	if _, created, err := store.QueueOutbox(ctx, OutboxMessage{
		SessionID:   "s1",
		TurnID:      turn.ID,
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done",
	}); err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	} else if !created {
		t.Fatal("QueueOutbox created = false")
	}

	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	state, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load reopened error: %v", err)
	}
	if got := state.Sessions["s1"].CodexThreadID; got != "thread-1" {
		t.Fatalf("CodexThreadID = %q, want thread-1", got)
	}
	if got := state.Sessions["s1"].LatestCodexTurnID; got != "codex-turn-1" {
		t.Fatalf("LatestCodexTurnID = %q, want codex-turn-1", got)
	}
	if got := state.Turns[turn.ID].Status; got != TurnStatusCompleted {
		t.Fatalf("turn status = %q, want %q", got, TurnStatusCompleted)
	}
	if got := len(state.OutboxMessages); got != 1 {
		t.Fatalf("outbox count = %d, want 1", got)
	}
	if !state.ServiceControl.Paused || !state.ServiceControl.Draining {
		t.Fatalf("service control flags did not roundtrip: %#v", state.ServiceControl)
	}
	if got := state.ServiceControl.Reason; got != "upgrade" {
		t.Fatalf("service control reason = %q, want upgrade", got)
	}
	if state.ServiceControl.UpdatedAt.IsZero() {
		t.Fatal("service control UpdatedAt is zero after roundtrip")
	}
}

func TestLoadMigratesV1StateToV2SemanticBackbone(t *testing.T) {
	store := newTestStore(t)
	data := []byte(`{
		"schema_version": 1,
		"service_owner": {
			"pid": 4242,
			"hostname": "legacy-host",
			"executable_path": "/usr/local/bin/codex-helper",
			"helper_version": "v0.0.legacy",
			"active_session_id": "s1",
			"active_turn_id": "turn:legacy"
		},
		"sessions": {
			"s1": {
				"id": "s1",
				"status": "active",
				"teams_chat_id": "chat-1",
				"teams_chat_url": "https://teams.example/chat-1",
				"teams_topic": "topic",
				"runner_kind": "exec",
				"codex_version": "1.2.3",
				"cwd": "/workspace/project",
				"codex_home": "/home/user/.codex",
				"profile": "default",
				"model": "gpt-test",
				"sandbox": "workspace-write",
				"proxy_mode": "on",
				"yolo_mode": "off",
				"codex_thread_id": "thread-0"
			}
		},
		"turns": {
			"turn:legacy": {
				"id": "turn:legacy",
				"session_id": "s1",
				"status": "completed",
				"codex_thread_id": "thread-0"
			}
		},
		"inbound_events": {
			"inbound:chat-1:message-1": {
				"id": "inbound:chat-1:message-1",
				"session_id": "s1",
				"teams_chat_id": "chat-1",
				"teams_message_id": "message-1",
				"source": "teams",
				"status": "queued",
				"turn_id": "turn:legacy"
			}
		},
		"outbox_messages": {
			"outbox:legacy": {
				"id": "outbox:legacy",
				"session_id": "s1",
				"teams_chat_id": "chat-1",
				"kind": "final",
				"body": "legacy body",
				"status": "queued"
			},
			"outbox:accepted": {
				"id": "outbox:accepted",
				"session_id": "s1",
				"teams_chat_id": "chat-1",
				"kind": "final",
				"body": "accepted body",
				"status": "accepted",
				"teams_message_id": "teams-accepted"
			}
		},
		"chat_polls": {
			"chat-1": {
				"chat_id": "chat-1",
				"seeded": true
			}
		}
	}`)
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatalf("write old state: %v", err)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load migrated state error: %v", err)
	}
	if state.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", state.SchemaVersion, SchemaVersion)
	}
	if state.Sessions["s1"].TeamsChatID != "chat-1" {
		t.Fatalf("session missing after migration: %#v", state.Sessions["s1"])
	}
	msg := state.OutboxMessages["outbox:legacy"]
	if msg.Sequence <= 0 || msg.PartIndex != 1 || msg.PartCount != 1 || msg.RenderedHash == "" {
		t.Fatalf("legacy outbox metadata not initialized: %#v", msg)
	}
	accepted := state.OutboxMessages["outbox:accepted"]
	if accepted.Sequence <= 0 || accepted.Sequence == msg.Sequence || accepted.PartIndex != 1 || accepted.PartCount != 1 || accepted.RenderedHash == "" || accepted.TeamsMessageID != "teams-accepted" {
		t.Fatalf("accepted legacy outbox metadata not initialized: %#v", accepted)
	}
	if state.ChatSequences["chat-1"].Next != 3 {
		t.Fatalf("chat sequence next = %d, want 3", state.ChatSequences["chat-1"].Next)
	}
	if state.Workspaces == nil || state.DashboardViews == nil || state.DashboardNumbers == nil || state.TranscriptLedger == nil || state.ImportCheckpoints == nil || state.ChatRateLimits == nil || state.ArtifactRecords == nil || state.Notifications == nil {
		t.Fatalf("semantic v2 maps were not initialized: %#v", state)
	}
	if state.ServiceOwner == nil || state.ServiceOwner.ActiveTurnID != "turn:legacy" {
		t.Fatalf("legacy owner not preserved: %#v", state.ServiceOwner)
	}
}

func TestLoadMigratesLocalPOCV1StateShape(t *testing.T) {
	store := newTestStore(t)
	data := []byte(`{
		"schema_version": 1,
		"created_at": "2026-04-30T13:48:19+08:00",
		"updated_at": "2026-04-30T14:06:44+08:00",
		"service_control": {
			"updated_at": "2026-04-30T14:06:44+08:00"
		},
		"sessions": {
			"s001": {
				"id": "s001",
				"status": "active",
				"teams_chat_id": "19:work-chat@thread.v2",
				"teams_chat_url": "https://teams.example/l/chat/19%3Awork-chat%40thread.v2/0",
				"teams_topic": "codex poc test",
				"codex_thread_id": "thread-poc",
				"latest_turn_id": "turn:inbound:19:work-chat@thread.v2:2",
				"runner_kind": "executor",
				"created_at": "2026-04-30T10:56:00+08:00",
				"updated_at": "2026-04-30T13:49:12+08:00"
			}
		},
		"turns": {
			"turn:inbound:19:work-chat@thread.v2:1": {
				"id": "turn:inbound:19:work-chat@thread.v2:1",
				"session_id": "s001",
				"inbound_event_id": "inbound:19:work-chat@thread.v2:1",
				"status": "completed",
				"codex_thread_id": "thread-poc",
				"queued_at": "2026-04-30T13:48:19+08:00",
				"started_at": "2026-04-30T13:48:19+08:00",
				"completed_at": "2026-04-30T13:48:19+08:00"
			},
			"turn:inbound:19:work-chat@thread.v2:2": {
				"id": "turn:inbound:19:work-chat@thread.v2:2",
				"session_id": "s001",
				"inbound_event_id": "inbound:19:work-chat@thread.v2:2",
				"status": "completed",
				"codex_thread_id": "thread-poc",
				"queued_at": "2026-04-30T13:48:47+08:00",
				"started_at": "2026-04-30T13:48:47+08:00",
				"completed_at": "2026-04-30T13:48:47+08:00"
			}
		},
		"inbound_events": {
			"inbound:19:work-chat@thread.v2:1": {
				"id": "inbound:19:work-chat@thread.v2:1",
				"session_id": "s001",
				"teams_chat_id": "19:work-chat@thread.v2",
				"teams_message_id": "1",
				"source": "teams",
				"status": "queued",
				"turn_id": "turn:inbound:19:work-chat@thread.v2:1"
			},
			"inbound:19:work-chat@thread.v2:2": {
				"id": "inbound:19:work-chat@thread.v2:2",
				"session_id": "s001",
				"teams_chat_id": "19:work-chat@thread.v2",
				"teams_message_id": "2",
				"source": "teams",
				"status": "queued",
				"turn_id": "turn:inbound:19:work-chat@thread.v2:2"
			}
		}
	}`)
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatalf("write local poc state: %v", err)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load local poc state error: %v", err)
	}
	if state.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", state.SchemaVersion, SchemaVersion)
	}
	session := state.Sessions["s001"]
	if session.TeamsChatID != "19:work-chat@thread.v2" || session.LatestTurnID != "turn:inbound:19:work-chat@thread.v2:2" {
		t.Fatalf("local poc session not preserved: %#v", session)
	}
	if got := state.Turns["turn:inbound:19:work-chat@thread.v2:2"].Status; got != TurnStatusCompleted {
		t.Fatalf("latest turn status = %q, want completed", got)
	}
	if state.Workspaces == nil || state.DashboardViews == nil || state.DashboardNumbers == nil || state.ChatSequences == nil || state.ChatRateLimits == nil || state.Notifications == nil {
		t.Fatalf("semantic maps were not initialized for local poc shape: %#v", state)
	}
	if state.ServiceControl.UpdatedAt.IsZero() {
		t.Fatalf("service control timestamp not preserved: %#v", state.ServiceControl)
	}
}

func TestLoadUnsupportedFutureSchemaFailsClosed(t *testing.T) {
	store := newTestStore(t)
	data := []byte(`{"schema_version":999,"sessions":{"s1":{"id":"s1","teams_chat_id":"chat-1"}}}`)
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatalf("write future state: %v", err)
	}

	if _, err := store.Load(context.Background()); !errors.Is(err, ErrUnsupportedSchemaVersion) || !strings.Contains(err.Error(), "999") {
		t.Fatalf("Load future schema error = %v, want unsupported schema version 999", err)
	}
	if _, err := store.SetPaused(context.Background(), true, "should not write"); !errors.Is(err, ErrUnsupportedSchemaVersion) {
		t.Fatalf("SetPaused future schema error = %v, want unsupported schema", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read future state after failed update: %v", err)
	}
	if string(after) != string(data) {
		t.Fatalf("future state was modified after failed update:\n%s", string(after))
	}
}

func TestAcceptedOutboxIsPromotedWithoutResend(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:accepted",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "already accepted",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	if _, err := store.MarkOutboxAccepted(ctx, msg.ID, "teams-message-1"); err != nil {
		t.Fatalf("MarkOutboxAccepted error: %v", err)
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 1 || pending[0].Status != OutboxStatusAccepted || pending[0].TeamsMessageID != "teams-message-1" {
		t.Fatalf("pending accepted outbox = %#v, want one accepted message", pending)
	}
	if _, err := store.MarkOutboxSent(ctx, msg.ID, "teams-message-1"); err != nil {
		t.Fatalf("MarkOutboxSent error: %v", err)
	}
	pending, err = store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox after sent error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after sent = %#v, want none", pending)
	}
}

func TestMarkTurnCompletedWithEmptyCodexTurnIDPreservesLatestKnownCodexTurnID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}

	firstInbound, _, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound first error: %v", err)
	}
	firstTurn, _, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: firstInbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn first error: %v", err)
	}
	if _, err := store.MarkTurnCompleted(ctx, firstTurn.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnCompleted first error: %v", err)
	}

	secondInbound := testInbound()
	secondInbound.ID = "inbound-2"
	secondInbound.TeamsMessageID = "message-2"
	secondInbound, _, err = store.PersistInbound(ctx, secondInbound)
	if err != nil {
		t.Fatalf("PersistInbound second error: %v", err)
	}
	secondTurn, _, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: secondInbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn second error: %v", err)
	}
	completed, err := store.MarkTurnCompleted(ctx, secondTurn.ID, "thread-1", "")
	if err != nil {
		t.Fatalf("MarkTurnCompleted second error: %v", err)
	}
	if completed.CodexTurnID != "" {
		t.Fatalf("second turn CodexTurnID = %q, want empty", completed.CodexTurnID)
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	session := state.Sessions["s1"]
	if session.LatestTurnID != secondTurn.ID {
		t.Fatalf("LatestTurnID = %q, want %q", session.LatestTurnID, secondTurn.ID)
	}
	if session.CodexThreadID != "thread-1" {
		t.Fatalf("CodexThreadID = %q, want thread-1", session.CodexThreadID)
	}
	if session.LatestCodexTurnID != "codex-turn-1" {
		t.Fatalf("LatestCodexTurnID = %q, want latest known codex-turn-1", session.LatestCodexTurnID)
	}
}

func TestQueueOutboxAssignsPerChatSequenceAndMetadata(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	first, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:first",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final-001",
		Body:        "first",
		PartIndex:   1,
		PartCount:   2,
	})
	if err != nil {
		t.Fatalf("QueueOutbox first error: %v", err)
	}
	if !created {
		t.Fatal("first QueueOutbox created = false")
	}
	second, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:second",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final-002",
		Body:        "second",
		PartIndex:   2,
		PartCount:   2,
	})
	if err != nil {
		t.Fatalf("QueueOutbox second error: %v", err)
	}
	other, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:other",
		TeamsChatID: "chat-2",
		Kind:        "control",
		Body:        "other",
	})
	if err != nil {
		t.Fatalf("QueueOutbox other chat error: %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || other.Sequence != 1 {
		t.Fatalf("unexpected sequences: first=%d second=%d other=%d", first.Sequence, second.Sequence, other.Sequence)
	}
	if first.RenderedHash == "" || first.PartIndex != 1 || first.PartCount != 2 {
		t.Fatalf("first outbox metadata mismatch: %#v", first)
	}
	if other.PartIndex != 1 || other.PartCount != 1 {
		t.Fatalf("default part metadata mismatch: %#v", other)
	}
}

func TestStoreConcurrentSessionAndOutboxWritersPreserveState(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}

	const workers = 6
	const perWorker = 12
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerStore, err := Open(store.Path())
			if err != nil {
				errCh <- fmt.Errorf("open worker store %d: %w", worker, err)
				return
			}
			for i := 0; i < perWorker; i++ {
				idx := worker*perWorker + i
				inbound, created, err := workerStore.PersistInbound(ctx, InboundEvent{
					ID:             fmt.Sprintf("inbound:stress:%03d", idx),
					SessionID:      "s1",
					TeamsChatID:    "chat-1",
					TeamsMessageID: fmt.Sprintf("teams-message-%03d", idx),
					Source:         "teams",
				})
				if err != nil {
					errCh <- fmt.Errorf("persist inbound %d: %w", idx, err)
					return
				}
				if !created {
					errCh <- fmt.Errorf("inbound %d unexpectedly deduplicated", idx)
					return
				}
				turn, created, err := workerStore.QueueTurn(ctx, Turn{
					SessionID:      "s1",
					InboundEventID: inbound.ID,
				})
				if err != nil {
					errCh <- fmt.Errorf("queue turn %d: %w", idx, err)
					return
				}
				if !created {
					errCh <- fmt.Errorf("turn %d unexpectedly deduplicated", idx)
					return
				}
				if _, err := workerStore.MarkTurnRunning(ctx, turn.ID, "thread-stress", fmt.Sprintf("codex-turn-%03d", idx)); err != nil {
					errCh <- fmt.Errorf("mark turn running %d: %w", idx, err)
					return
				}
				if idx%5 == 0 {
					if _, err := workerStore.MarkTurnFailed(ctx, turn.ID, "synthetic failure"); err != nil {
						errCh <- fmt.Errorf("mark turn failed %d: %w", idx, err)
						return
					}
				} else if _, err := workerStore.MarkTurnCompleted(ctx, turn.ID, "thread-stress", fmt.Sprintf("codex-turn-%03d", idx)); err != nil {
					errCh <- fmt.Errorf("mark turn completed %d: %w", idx, err)
					return
				}

				chatID := fmt.Sprintf("chat-%d", 1+(idx%3))
				outbox, created, err := workerStore.QueueOutbox(ctx, OutboxMessage{
					ID:          fmt.Sprintf("outbox:stress:%03d", idx),
					SessionID:   "s1",
					TurnID:      turn.ID,
					TeamsChatID: chatID,
					Kind:        "stress",
					Body:        fmt.Sprintf("stress body %03d", idx),
				})
				if err != nil {
					errCh <- fmt.Errorf("queue outbox %d: %w", idx, err)
					return
				}
				if !created {
					errCh <- fmt.Errorf("outbox %d unexpectedly deduplicated", idx)
					return
				}
				switch idx % 4 {
				case 0:
					if _, err := workerStore.MarkOutboxSendAttempt(ctx, outbox.ID); err != nil {
						errCh <- fmt.Errorf("mark outbox send attempt %d: %w", idx, err)
						return
					}
					if _, err := workerStore.MarkOutboxAccepted(ctx, outbox.ID, fmt.Sprintf("teams-sent-%03d", idx)); err != nil {
						errCh <- fmt.Errorf("mark outbox accepted %d: %w", idx, err)
						return
					}
					if _, err := workerStore.MarkOutboxSent(ctx, outbox.ID, fmt.Sprintf("teams-sent-%03d", idx)); err != nil {
						errCh <- fmt.Errorf("mark outbox sent %d: %w", idx, err)
						return
					}
				case 1:
					if _, err := workerStore.MarkOutboxSendAttempt(ctx, outbox.ID); err != nil {
						errCh <- fmt.Errorf("mark outbox failed attempt %d: %w", idx, err)
						return
					}
					if _, err := workerStore.MarkOutboxSendError(ctx, outbox.ID, "synthetic 429"); err != nil {
						errCh <- fmt.Errorf("mark outbox send error %d: %w", idx, err)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	want := workers * perWorker
	if got := len(state.InboundEvents); got != want {
		t.Fatalf("inbound count = %d, want %d", got, want)
	}
	if got := len(state.Turns); got != want {
		t.Fatalf("turn count = %d, want %d", got, want)
	}
	if got := len(state.OutboxMessages); got != want {
		t.Fatalf("outbox count = %d, want %d", got, want)
	}
	sequencesByChat := map[string]map[int64]string{}
	countByChat := map[string]int{}
	for _, msg := range state.OutboxMessages {
		if msg.Sequence <= 0 {
			t.Fatalf("outbox %s has non-positive sequence: %#v", msg.ID, msg)
		}
		if sequencesByChat[msg.TeamsChatID] == nil {
			sequencesByChat[msg.TeamsChatID] = map[int64]string{}
		}
		if previous := sequencesByChat[msg.TeamsChatID][msg.Sequence]; previous != "" {
			t.Fatalf("duplicate sequence %d for chat %s: %s and %s", msg.Sequence, msg.TeamsChatID, previous, msg.ID)
		}
		sequencesByChat[msg.TeamsChatID][msg.Sequence] = msg.ID
		countByChat[msg.TeamsChatID]++
	}
	for chatID, count := range countByChat {
		for seq := int64(1); seq <= int64(count); seq++ {
			if sequencesByChat[chatID][seq] == "" {
				t.Fatalf("chat %s missing sequence %d in %#v", chatID, seq, sequencesByChat[chatID])
			}
		}
		if next := state.ChatSequences[chatID].Next; next != int64(count+1) {
			t.Fatalf("chat %s next sequence = %d, want %d", chatID, next, count+1)
		}
	}
}

func TestStoreConcurrentInboundTurnDedupAcrossHandles(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}

	const workers = 12
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerStore, err := Open(store.Path())
			if err != nil {
				errCh <- fmt.Errorf("open worker store %d: %w", worker, err)
				return
			}
			inbound, _, err := workerStore.PersistInbound(ctx, InboundEvent{
				SessionID:      "s1",
				TeamsChatID:    "chat-1",
				TeamsMessageID: "duplicate-message",
				Source:         "teams",
			})
			if err != nil {
				errCh <- fmt.Errorf("persist duplicate inbound %d: %w", worker, err)
				return
			}
			if _, _, err := workerStore.QueueTurn(ctx, Turn{
				SessionID:      "s1",
				InboundEventID: inbound.ID,
			}); err != nil {
				errCh <- fmt.Errorf("queue duplicate turn %d: %w", worker, err)
				return
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 1 {
		t.Fatalf("inbound count = %d, want one deduped event: %#v", got, state.InboundEvents)
	}
	if got := len(state.Turns); got != 1 {
		t.Fatalf("turn count = %d, want one deduped turn: %#v", got, state.Turns)
	}
	for _, inbound := range state.InboundEvents {
		if inbound.TurnID == "" || inbound.Status != InboundStatusQueued {
			t.Fatalf("deduped inbound not linked to queued turn: %#v", inbound)
		}
	}
}

func TestPendingOutboxSkipsRateLimitedChatWithoutBlockingOthers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:blocked", SessionID: "s1", TeamsChatID: "chat-1", Kind: "final", Body: "blocked"}); err != nil {
		t.Fatalf("QueueOutbox blocked error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:open", TeamsChatID: "chat-2", Kind: "control", Body: "open"}); err != nil {
		t.Fatalf("QueueOutbox open error: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-1", time.Now().Add(time.Minute), "429 Retry-After"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 1 || pending[0].TeamsChatID != "chat-2" {
		t.Fatalf("pending outbox = %#v, want only unblocked chat-2", pending)
	}
	if err := store.ClearChatRateLimit(ctx, "chat-1"); err != nil {
		t.Fatalf("ClearChatRateLimit error: %v", err)
	}
	pending, err = store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox after clear error: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending after clear = %#v, want both messages", pending)
	}
}

func TestPendingOutboxAtRespectsRateLimitExpiry(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:blocked", TeamsChatID: "chat-1", Kind: "helper", Body: "blocked"}); err != nil {
		t.Fatalf("QueueOutbox blocked error: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-1", now.Add(time.Minute), "429"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	pending, err := store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("PendingOutboxAt before expiry error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending before expiry = %#v, want none", pending)
	}
	pending, err = store.PendingOutboxAt(ctx, now.Add(time.Minute+time.Nanosecond))
	if err != nil {
		t.Fatalf("PendingOutboxAt after expiry error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "outbox:blocked" {
		t.Fatalf("pending after expiry = %#v, want blocked outbox", pending)
	}
}

func TestServiceControlPauseResumeIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	initial, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if initial.Paused || initial.Draining || initial.Reason != "" || !initial.UpdatedAt.IsZero() {
		t.Fatalf("initial control = %#v, want zero value", initial)
	}

	paused, err := store.SetPaused(ctx, true, " maintenance ")
	if err != nil {
		t.Fatalf("SetPaused true error: %v", err)
	}
	if !paused.Paused || paused.Draining || paused.Reason != "maintenance" || paused.UpdatedAt.IsZero() {
		t.Fatalf("paused control mismatch: %#v", paused)
	}
	again, err := store.SetPaused(ctx, true, "maintenance")
	if err != nil {
		t.Fatalf("idempotent SetPaused true error: %v", err)
	}
	if !sameControl(again, paused) {
		t.Fatalf("idempotent pause changed control: got %#v want %#v", again, paused)
	}

	resumed, err := store.SetPaused(ctx, false, "")
	if err != nil {
		t.Fatalf("SetPaused false error: %v", err)
	}
	if resumed.Paused || resumed.Draining || resumed.Reason != "" || resumed.UpdatedAt.IsZero() {
		t.Fatalf("resumed control mismatch: %#v", resumed)
	}
	resumedAgain, err := store.SetPaused(ctx, false, "")
	if err != nil {
		t.Fatalf("idempotent SetPaused false error: %v", err)
	}
	if !sameControl(resumedAgain, resumed) {
		t.Fatalf("idempotent resume changed control: got %#v want %#v", resumedAgain, resumed)
	}
}

func TestServiceControlDrainSetAndClear(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	draining, err := store.SetDraining(ctx, " upgrade ")
	if err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	if draining.Paused || !draining.Draining || draining.Reason != "upgrade" || draining.UpdatedAt.IsZero() {
		t.Fatalf("draining control mismatch: %#v", draining)
	}
	drainingAgain, err := store.SetDraining(ctx, "upgrade")
	if err != nil {
		t.Fatalf("idempotent SetDraining error: %v", err)
	}
	if !sameControl(drainingAgain, draining) {
		t.Fatalf("idempotent drain changed control: got %#v want %#v", drainingAgain, draining)
	}

	cleared, err := store.ClearDrain(ctx)
	if err != nil {
		t.Fatalf("ClearDrain error: %v", err)
	}
	if cleared.Paused || cleared.Draining || cleared.Reason != "" || cleared.UpdatedAt.IsZero() {
		t.Fatalf("cleared drain control mismatch: %#v", cleared)
	}
	clearedAgain, err := store.ClearDrain(ctx)
	if err != nil {
		t.Fatalf("idempotent ClearDrain error: %v", err)
	}
	if !sameControl(clearedAgain, cleared) {
		t.Fatalf("idempotent drain clear changed control: got %#v want %#v", clearedAgain, cleared)
	}
}

func TestUpgradeLifecycleRestoresControl(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	req, err := store.BeginUpgrade(ctx, HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if req.ID == "" || req.Phase != UpgradePhaseDraining || req.Reason != HelperUpgradeReason || req.DeadlineAt.IsZero() {
		t.Fatalf("upgrade request mismatch: %#v", req)
	}
	control, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if !control.Draining || control.Reason != HelperUpgradeReason {
		t.Fatalf("control after BeginUpgrade = %#v, want helper drain", control)
	}

	ready, err := store.MarkUpgradeReady(ctx, req.ID)
	if err != nil {
		t.Fatalf("MarkUpgradeReady error: %v", err)
	}
	if ready.Phase != UpgradePhaseReady || ready.ReadyAt.IsZero() {
		t.Fatalf("ready request mismatch: %#v", ready)
	}
	completed, err := store.CompleteUpgrade(ctx, req.ID)
	if err != nil {
		t.Fatalf("CompleteUpgrade error: %v", err)
	}
	if completed.Phase != UpgradePhaseCompleted || completed.CompletedAt.IsZero() {
		t.Fatalf("completed request mismatch: %#v", completed)
	}
	control, err = store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl after complete error: %v", err)
	}
	if control.Paused || control.Draining || control.Reason != "" {
		t.Fatalf("control after complete = %#v, want restored running", control)
	}
}

func TestUpgradeAbortPreservesPreviousDrain(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.SetDraining(ctx, "maintenance"); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	req, err := store.BeginUpgrade(ctx, HelperUpgradeReason, 0)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if !req.PreviousControl.Draining || req.PreviousControl.Reason != "maintenance" {
		t.Fatalf("previous control mismatch: %#v", req.PreviousControl)
	}
	if _, err := store.AbortUpgrade(ctx, req.ID, "download failed"); err != nil {
		t.Fatalf("AbortUpgrade error: %v", err)
	}
	control, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl after abort error: %v", err)
	}
	if !control.Draining || control.Reason != "maintenance" {
		t.Fatalf("control after abort = %#v, want previous drain", control)
	}
	read, ok, err := store.ReadUpgrade(ctx)
	if err != nil {
		t.Fatalf("ReadUpgrade error: %v", err)
	}
	if !ok || read.Phase != UpgradePhaseAborted || read.AbortReason != "download failed" {
		t.Fatalf("aborted upgrade mismatch: %#v ok=%v", read, ok)
	}
}

func TestDeferredInboundIsDurableAndListed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	event, created, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-deferred",
		Source:         "teams",
		Status:         InboundStatusDeferred,
	})
	if err != nil {
		t.Fatalf("PersistInbound deferred error: %v", err)
	}
	if !created || event.Status != InboundStatusDeferred {
		t.Fatalf("deferred event mismatch: %#v created=%v", event, created)
	}
	deferred, err := store.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].ID != event.ID {
		t.Fatalf("deferred inbound = %#v, want %s", deferred, event.ID)
	}
	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	deferred, err = reopened.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound reopened error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].Status != InboundStatusDeferred {
		t.Fatalf("reopened deferred inbound = %#v", deferred)
	}
}

func TestHasUpgradeBlockingWorkAllowsDeferredAndRateLimitedOutbox(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "deferred-only",
		Status:         InboundStatusDeferred,
	}); err != nil {
		t.Fatalf("PersistInbound deferred error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load deferred-only error: %v", err)
	}
	if HasUpgradeBlockingWork(state, now) {
		t.Fatal("deferred inbound should not block upgrade")
	}

	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:blocked",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "blocked",
	}); err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load queued outbox error: %v", err)
	}
	if !HasUpgradeBlockingWork(state, now) {
		t.Fatal("queued outbox without rate limit should block upgrade")
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-1", now.Add(time.Minute), "429"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load rate-limited outbox error: %v", err)
	}
	if HasUpgradeBlockingWork(state, now) {
		t.Fatal("rate-limited durable outbox should not block upgrade")
	}

	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:                 "outbox:nonblocking-ack",
		TeamsChatID:        "chat-2",
		Kind:               "ack",
		Body:               "upgrade notice",
		UpgradeNonBlocking: true,
	}); err != nil {
		t.Fatalf("QueueOutbox nonblocking ack error: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load nonblocking ack error: %v", err)
	}
	if HasUpgradeBlockingWork(state, now) {
		t.Fatal("upgrade non-blocking ACK outbox should not block upgrade")
	}
}

func TestOutboxBlocksUpgradeStatusMatrix(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	base := State{
		ChatRateLimits: map[string]ChatRateLimitState{},
	}
	freshAttempt := now.Add(-outboxSendLease + time.Second)
	staleAttempt := now.Add(-outboxSendLease - time.Second)
	tests := []struct {
		name  string
		msg   OutboxMessage
		limit ChatRateLimitState
		want  bool
	}{
		{
			name: "queued blocks",
			msg:  OutboxMessage{ID: "queued", TeamsChatID: "chat-1", Status: OutboxStatusQueued},
			want: true,
		},
		{
			name:  "queued rate limited does not block",
			msg:   OutboxMessage{ID: "queued-rate-limited", TeamsChatID: "chat-1", Status: OutboxStatusQueued},
			limit: ChatRateLimitState{ChatID: "chat-1", BlockedUntil: now.Add(time.Minute)},
			want:  false,
		},
		{
			name: "fresh sending blocks",
			msg:  OutboxMessage{ID: "fresh-sending", TeamsChatID: "chat-1", Status: OutboxStatusSending, LastSendAttempt: freshAttempt},
			want: true,
		},
		{
			name: "sending with missing attempt blocks",
			msg:  OutboxMessage{ID: "missing-attempt", TeamsChatID: "chat-1", Status: OutboxStatusSending},
			want: true,
		},
		{
			name: "stale sending does not block",
			msg:  OutboxMessage{ID: "stale-sending", TeamsChatID: "chat-1", Status: OutboxStatusSending, LastSendAttempt: staleAttempt},
			want: false,
		},
		{
			name: "accepted does not block",
			msg:  OutboxMessage{ID: "accepted", TeamsChatID: "chat-1", Status: OutboxStatusAccepted, TeamsMessageID: "teams-1"},
			want: false,
		},
		{
			name: "sent does not block",
			msg:  OutboxMessage{ID: "sent", TeamsChatID: "chat-1", Status: OutboxStatusSent, TeamsMessageID: "teams-1"},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := base
			state.ChatRateLimits = map[string]ChatRateLimitState{}
			if !tc.limit.BlockedUntil.IsZero() {
				state.ChatRateLimits[tc.limit.ChatID] = tc.limit
			}
			if got := OutboxBlocksUpgrade(state, tc.msg, now); got != tc.want {
				t.Fatalf("OutboxBlocksUpgrade = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasDeliveredOutboxMessageStatusMatrix(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, msg := range []OutboxMessage{
		{ID: "queued", TeamsChatID: "chat-1", TeamsMessageID: "teams-queued", Status: OutboxStatusQueued},
		{ID: "sending", TeamsChatID: "chat-1", TeamsMessageID: "teams-sending", Status: OutboxStatusSending},
		{ID: "accepted", TeamsChatID: "chat-1", TeamsMessageID: "teams-accepted", Status: OutboxStatusAccepted},
		{ID: "sent", TeamsChatID: "chat-1", TeamsMessageID: "teams-sent", Status: OutboxStatusSent},
		{ID: "other-chat", TeamsChatID: "chat-2", TeamsMessageID: "teams-other", Status: OutboxStatusSent},
	} {
		if _, _, err := store.QueueOutbox(ctx, msg); err != nil {
			t.Fatalf("QueueOutbox %s error: %v", msg.ID, err)
		}
	}
	tests := []struct {
		name      string
		chatID    string
		messageID string
		want      bool
	}{
		{name: "queued", chatID: "chat-1", messageID: "teams-queued", want: false},
		{name: "sending", chatID: "chat-1", messageID: "teams-sending", want: false},
		{name: "accepted", chatID: "chat-1", messageID: "teams-accepted", want: true},
		{name: "sent", chatID: "chat-1", messageID: "teams-sent", want: true},
		{name: "other chat", chatID: "chat-1", messageID: "teams-other", want: false},
		{name: "missing", chatID: "chat-1", messageID: "missing", want: false},
		{name: "empty", chatID: "", messageID: "teams-sent", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.HasDeliveredOutboxMessage(ctx, tc.chatID, tc.messageID)
			if err != nil {
				t.Fatalf("HasDeliveredOutboxMessage error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("HasDeliveredOutboxMessage = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasUpgradeBlockingWorkTurnStatusMatrix(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		status TurnStatus
		want   bool
	}{
		{name: "queued", status: TurnStatusQueued, want: true},
		{name: "running", status: TurnStatusRunning, want: true},
		{name: "completed", status: TurnStatusCompleted, want: false},
		{name: "failed", status: TurnStatusFailed, want: false},
		{name: "interrupted", status: TurnStatusInterrupted, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := State{
				Turns: map[string]Turn{
					"turn-1": {ID: "turn-1", SessionID: "s1", Status: tc.status},
				},
				OutboxMessages: map[string]OutboxMessage{},
				ChatRateLimits: map[string]ChatRateLimitState{},
			}
			if got := HasUpgradeBlockingWork(state, now); got != tc.want {
				t.Fatalf("HasUpgradeBlockingWork = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPermissionsUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix file mode assertion")
	}
	store := newTestStore(t)
	if _, _, err := store.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}

	dirInfo, err := os.Stat(filepath.Dir(store.Path()))
	if err != nil {
		t.Fatalf("stat state dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("dir mode = %o, want 700", got)
	}
	fileInfo, err := os.Stat(store.Path())
	if err != nil {
		t.Fatalf("stat state file: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("file mode = %o, want 600", got)
	}
}

func TestDuplicateInboundIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}

	first, created, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("first PersistInbound error: %v", err)
	}
	if !created {
		t.Fatal("first inbound created = false")
	}
	second, created, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Source:         "teams",
	})
	if err != nil {
		t.Fatalf("second PersistInbound error: %v", err)
	}
	if created {
		t.Fatal("duplicate inbound created = true")
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate inbound ID = %q, want %q", second.ID, first.ID)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 1 {
		t.Fatalf("inbound count = %d, want 1", got)
	}
}

func TestRecoverInterruptsAmbiguousTurns(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	running, _, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn running error: %v", err)
	}
	if _, err := store.MarkTurnRunning(ctx, running.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnRunning error: %v", err)
	}
	queued, _, err := store.QueueTurn(ctx, Turn{ID: "turn:manual", SessionID: "s1"})
	if err != nil {
		t.Fatalf("QueueTurn queued error: %v", err)
	}

	report, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover error: %v", err)
	}
	if got := len(report.InterruptedTurnIDs); got != 2 {
		t.Fatalf("interrupted turn count = %d, want 2 (%v)", got, report.InterruptedTurnIDs)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	for _, id := range []string{running.ID, queued.ID} {
		turn := state.Turns[id]
		if turn.Status != TurnStatusInterrupted {
			t.Fatalf("turn %s status = %q, want %q", id, turn.Status, TurnStatusInterrupted)
		}
		if turn.RecoveryReason == "" {
			t.Fatalf("turn %s recovery reason is empty", id)
		}
	}
	if got := state.InboundEvents[inbound.ID].Status; got != InboundStatusIgnored {
		t.Fatalf("interrupted turn inbound status = %q, want %q", got, InboundStatusIgnored)
	}
}

func TestClaimNextQueuedTurnSerializesPerSession(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	first, _, err := store.QueueTurn(ctx, Turn{ID: "turn:first", SessionID: "s1", QueuedAt: time.Date(2026, 5, 3, 1, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("QueueTurn first error: %v", err)
	}
	if _, _, err := store.QueueTurn(ctx, Turn{ID: "turn:second", SessionID: "s1", QueuedAt: time.Date(2026, 5, 3, 1, 1, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("QueueTurn second error: %v", err)
	}
	claimed, ok, err := store.ClaimNextQueuedTurn(ctx, "s1")
	if err != nil {
		t.Fatalf("ClaimNextQueuedTurn error: %v", err)
	}
	if !ok || claimed.ID != first.ID || claimed.Status != TurnStatusRunning || claimed.StartedAt.IsZero() {
		t.Fatalf("claimed first queued turn mismatch: ok=%v claimed=%#v", ok, claimed)
	}
	if again, ok, err := store.ClaimNextQueuedTurn(ctx, "s1"); err != nil || ok {
		t.Fatalf("ClaimNextQueuedTurn while running = ok %v turn %#v err %v, want no claim", ok, again, err)
	}
	if _, err := store.MarkTurnCompleted(ctx, claimed.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnCompleted error: %v", err)
	}
	second, ok, err := store.ClaimNextQueuedTurn(ctx, "s1")
	if err != nil {
		t.Fatalf("ClaimNextQueuedTurn second error: %v", err)
	}
	if !ok || second.ID != "turn:second" || second.Status != TurnStatusRunning {
		t.Fatalf("claimed second queued turn mismatch: ok=%v second=%#v", ok, second)
	}
}

func TestInterruptedTurnCannotBeCompletedOrFailed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, Turn{ID: "turn:manual", SessionID: "s1"})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "forced recovery"); err != nil {
		t.Fatalf("MarkTurnInterrupted error: %v", err)
	}
	if _, err := store.MarkTurnCompleted(ctx, turn.ID, "thread-1", "codex-turn-1"); err == nil || !strings.Contains(err.Error(), "cannot be completed") {
		t.Fatalf("MarkTurnCompleted error = %v, want interrupted guard", err)
	}
	if _, err := store.MarkTurnFailed(ctx, turn.ID, "failed after recovery"); err == nil || !strings.Contains(err.Error(), "cannot be failed") {
		t.Fatalf("MarkTurnFailed error = %v, want interrupted guard", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
}

func TestOutboxResendDoesNotCreateNewTurn(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if !created {
		t.Fatal("initial QueueTurn created = false")
	}
	outbox := OutboxMessage{
		SessionID:   "s1",
		TurnID:      turn.ID,
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done",
	}
	if _, created, err := store.QueueOutbox(ctx, outbox); err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	} else if !created {
		t.Fatal("initial QueueOutbox created = false")
	}

	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	duplicateInbound, created, err := reopened.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("duplicate PersistInbound error: %v", err)
	}
	if created {
		t.Fatal("duplicate inbound created = true")
	}
	duplicateTurn, created, err := reopened.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: duplicateInbound.ID})
	if err != nil {
		t.Fatalf("duplicate QueueTurn error: %v", err)
	}
	if created {
		t.Fatal("duplicate QueueTurn created = true")
	}
	if duplicateTurn.ID != turn.ID {
		t.Fatalf("duplicate turn ID = %q, want %q", duplicateTurn.ID, turn.ID)
	}
	if _, created, err := reopened.QueueOutbox(ctx, outbox); err != nil {
		t.Fatalf("duplicate QueueOutbox error: %v", err)
	} else if created {
		t.Fatal("duplicate QueueOutbox created = true")
	}
	state, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.Turns); got != 1 {
		t.Fatalf("turn count = %d, want 1", got)
	}
	if got := len(state.OutboxMessages); got != 1 {
		t.Fatalf("outbox count = %d, want 1", got)
	}
	pending, err := reopened.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if got := len(pending); got != 1 {
		t.Fatalf("pending outbox count = %d, want 1", got)
	}
}

func TestMarkOutboxSentIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:anchor",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "anchor",
		Body:        "ready",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	first, err := store.MarkOutboxSent(ctx, msg.ID, "teams-message-1")
	if err != nil {
		t.Fatalf("first MarkOutboxSent error: %v", err)
	}
	second, err := store.MarkOutboxSent(ctx, msg.ID, "teams-message-1")
	if err != nil {
		t.Fatalf("second MarkOutboxSent error: %v", err)
	}
	if first.ID != second.ID || second.Status != OutboxStatusSent || second.TeamsMessageID != "teams-message-1" {
		t.Fatalf("unexpected sent outbox after duplicate update: %#v", second)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.OutboxMessages); got != 1 {
		t.Fatalf("outbox count = %d, want 1", got)
	}
}

func TestOutboxSendAttemptClaimsQueuedMessage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:claim",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	claimed, err := store.MarkOutboxSendAttempt(ctx, msg.ID)
	if err != nil {
		t.Fatalf("MarkOutboxSendAttempt error: %v", err)
	}
	if claimed.Status != OutboxStatusSending || claimed.LastSendAttempt.IsZero() {
		t.Fatalf("unexpected claimed outbox: %#v", claimed)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); !errors.Is(err, ErrOutboxSendNotClaimed) {
		t.Fatalf("second MarkOutboxSendAttempt error = %v, want ErrOutboxSendNotClaimed", err)
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("fresh sending outbox should not be pending: %#v", pending)
	}
}

func TestPendingOutboxIncludesStaleSendingMessage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:stale-send",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); err != nil {
		t.Fatalf("MarkOutboxSendAttempt error: %v", err)
	}
	if err := store.UpdateSession(ctx, "s1", func(state *State) error {
		stale := state.OutboxMessages[msg.ID]
		stale.LastSendAttempt = time.Now().Add(-outboxSendLease - time.Second)
		state.OutboxMessages[msg.ID] = stale
		return nil
	}); err != nil {
		t.Fatalf("make outbox send attempt stale: %v", err)
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != msg.ID {
		t.Fatalf("pending outbox = %#v, want stale sending message", pending)
	}
}

func TestSavePrunesOldSentOutboxMessages(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	err := store.Update(ctx, func(state *State) error {
		for i := 0; i < maxRetainedSentOutboxMessages+20; i++ {
			id := fmt.Sprintf("outbox:sent:%03d", i)
			state.OutboxMessages[id] = OutboxMessage{
				ID:          id,
				TeamsChatID: "chat-1",
				Status:      OutboxStatusSent,
				Kind:        "helper",
				Body:        "sent",
				SentAt:      now.Add(time.Duration(i) * time.Second),
				CreatedAt:   now.Add(time.Duration(i) * time.Second),
			}
		}
		state.OutboxMessages["outbox:queued"] = OutboxMessage{
			ID:          "outbox:queued",
			TeamsChatID: "chat-1",
			Status:      OutboxStatusQueued,
			Kind:        "helper",
			Body:        "queued",
			CreatedAt:   now,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.OutboxMessages); got != maxRetainedSentOutboxMessages {
		t.Fatalf("outbox len = %d, want %d", got, maxRetainedSentOutboxMessages)
	}
	if _, ok := state.OutboxMessages["outbox:queued"]; !ok {
		t.Fatal("queued outbox was pruned")
	}
	if _, ok := state.OutboxMessages["outbox:sent:000"]; ok {
		t.Fatal("oldest sent outbox was not pruned")
	}
	if _, ok := state.OutboxMessages[fmt.Sprintf("outbox:sent:%03d", maxRetainedSentOutboxMessages+19)]; !ok {
		t.Fatal("newest sent outbox was pruned")
	}
}

func TestSavePrunesOldTranscriptLedgerRecords(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	err := store.Update(ctx, func(state *State) error {
		for i := 0; i < maxRetainedTranscriptLedgerRecords+20; i++ {
			id := fmt.Sprintf("ledger:s1:%04d", i)
			state.TranscriptLedger[id] = TranscriptLedgerRecord{
				ID:             id,
				SessionID:      "s1",
				SourceRecordID: fmt.Sprintf("record-%04d", i),
				UpdatedAt:      now.Add(time.Duration(i) * time.Second),
				CreatedAt:      now.Add(time.Duration(i) * time.Second),
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.TranscriptLedger); got != maxRetainedTranscriptLedgerRecords {
		t.Fatalf("ledger len = %d, want %d", got, maxRetainedTranscriptLedgerRecords)
	}
	if _, ok := state.TranscriptLedger["ledger:s1:0000"]; ok {
		t.Fatal("oldest transcript ledger record was not pruned")
	}
	if _, ok := state.TranscriptLedger[fmt.Sprintf("ledger:s1:%04d", maxRetainedTranscriptLedgerRecords+19)]; !ok {
		t.Fatal("newest transcript ledger record was pruned")
	}
}

func TestTeamsBackgroundKeepaliveClockSkewOutboxAndRateLimitCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:future-send", TeamsChatID: "chat-1", Kind: "helper", Body: "sending"}); err != nil {
		t.Fatalf("QueueOutbox future-send error: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, "outbox:future-send"); err != nil {
		t.Fatalf("MarkOutboxSendAttempt error: %v", err)
	}
	if err := store.UpdateSession(ctx, "clock skew", func(state *State) error {
		msg := state.OutboxMessages["outbox:future-send"]
		msg.LastSendAttempt = now.Add(30 * time.Second)
		state.OutboxMessages[msg.ID] = msg
		return nil
	}); err != nil {
		t.Fatalf("set future send attempt: %v", err)
	}
	pending, err := store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("PendingOutboxAt before future attempt error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("future send attempt should remain leased after backward clock skew: %#v", pending)
	}
	pending, err = store.PendingOutboxAt(ctx, now.Add(30*time.Second+outboxSendLease+time.Nanosecond))
	if err != nil {
		t.Fatalf("PendingOutboxAt after future lease expiry error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "outbox:future-send" {
		t.Fatalf("future send attempt should become pending after lease expiry: %#v", pending)
	}

	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:rate-limited", TeamsChatID: "chat-2", Kind: "helper", Body: "blocked"}); err != nil {
		t.Fatalf("QueueOutbox rate-limited error: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-2", now.Add(10*time.Minute), "429 after sleep"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	pending, err = store.PendingOutboxAt(ctx, now.Add(9*time.Minute))
	if err != nil {
		t.Fatalf("PendingOutboxAt before rate limit expiry error: %v", err)
	}
	for _, msg := range pending {
		if msg.ID == "outbox:rate-limited" {
			t.Fatalf("rate-limited outbox should not be pending before Retry-After expires: %#v", pending)
		}
	}
	pending, err = store.PendingOutboxAt(ctx, now.Add(10*time.Minute+time.Nanosecond))
	if err != nil {
		t.Fatalf("PendingOutboxAt after rate limit expiry error: %v", err)
	}
	found := false
	for _, msg := range pending {
		if msg.ID == "outbox:rate-limited" {
			found = true
		}
	}
	if !found {
		t.Fatalf("rate-limited outbox should return after Retry-After expires: %#v", pending)
	}
}

func TestChatPollStateTracksCursorAndErrors(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	cursor := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	poll, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, true, 50)
	if err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	if !poll.Seeded || !poll.LastModifiedCursor.Equal(cursor) || poll.LastSuccessfulPollAt.IsZero() {
		t.Fatalf("unexpected poll success state: %#v", poll)
	}
	if poll.LastWindowFullAt.IsZero() || !strings.Contains(poll.LastWindowFullMessage, "full message window") {
		t.Fatalf("expected window diagnostic, got %#v", poll)
	}
	poll, err = store.RecordChatPollSuccessWithContinuation(ctx, "chat-1", cursor, true, true, 50, "/chats/chat-1/messages?$skiptoken=next")
	if err != nil {
		t.Fatalf("RecordChatPollSuccessWithContinuation error: %v", err)
	}
	if poll.ContinuationPath != "/chats/chat-1/messages?$skiptoken=next" {
		t.Fatalf("continuation path = %q", poll.ContinuationPath)
	}

	if err := store.RecordChatPollError(ctx, "chat-1", strings.Repeat("x", 300)); err != nil {
		t.Fatalf("RecordChatPollError error: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil {
		t.Fatalf("ChatPoll error: %v", err)
	}
	if !ok {
		t.Fatal("expected chat poll state")
	}
	if len(poll.LastError) != 240 || poll.LastErrorAt.IsZero() {
		t.Fatalf("unexpected poll error state: %#v", poll)
	}
	if poll.FailureCount != 1 {
		t.Fatalf("failure count = %d, want 1", poll.FailureCount)
	}

	later := cursor.Add(time.Minute)
	poll, err = store.RecordChatPollSuccess(ctx, "chat-1", later, false, false, 1)
	if err != nil {
		t.Fatalf("second RecordChatPollSuccess error: %v", err)
	}
	if !poll.Seeded || !poll.LastModifiedCursor.Equal(later) || poll.LastError != "" || poll.LastWindowFullMessage != "" || poll.ContinuationPath != "" || poll.FailureCount != 0 {
		t.Fatalf("unexpected recovered poll state: %#v", poll)
	}
}

func TestChatPollScheduleStatePersistsParkAndBlock(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Add(time.Hour)

	poll, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      "hot",
		NextPollAt:     now.Add(time.Second),
		LastActivityAt: now,
		ResetFailures:  true,
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedule hot error: %v", err)
	}
	if poll.PollState != "hot" || !poll.NextPollAt.Equal(now.Add(time.Second)) || !poll.LastActivityAt.Equal(now) {
		t.Fatalf("hot poll schedule mismatch: %#v", poll)
	}

	blockedUntil := now.Add(time.Minute)
	if err := store.RecordChatPollErrorWithBlock(ctx, "chat-1", "429", blockedUntil); err != nil {
		t.Fatalf("RecordChatPollErrorWithBlock error: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll after block ok=%v err=%v", ok, err)
	}
	if poll.PollState != "blocked" || poll.PreviousPollState != "hot" || !poll.BlockedUntil.Equal(blockedUntil) || !poll.NextPollAt.Equal(blockedUntil) {
		t.Fatalf("blocked poll schedule mismatch: %#v", poll)
	}

	poll, err = store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:            "chat-1",
		PollState:         "parked",
		PreviousPollState: "",
		ClearBlockedUntil: true,
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedule parked error: %v", err)
	}
	if poll.PollState != "parked" || poll.ParkedAt.IsZero() || !poll.BlockedUntil.IsZero() {
		t.Fatalf("parked poll schedule mismatch: %#v", poll)
	}
	poll, err = store.MarkChatPollParkNoticeSent(ctx, "chat-1", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("MarkChatPollParkNoticeSent error: %v", err)
	}
	if poll.ParkNoticeSentAt.IsZero() {
		t.Fatalf("park notice timestamp missing: %#v", poll)
	}

	poll, err = store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:            "chat-1",
		PollState:         "hot",
		NextPollAt:        now,
		LastActivityAt:    now.Add(3 * time.Minute),
		ClearBlockedUntil: true,
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedule resume error: %v", err)
	}
	if poll.PollState != "hot" || !poll.ParkedAt.IsZero() || !poll.ParkNoticeSentAt.IsZero() {
		t.Fatalf("resume should clear park markers: %#v", poll)
	}
}

func TestRecordOwnerHeartbeatWritesAndReadsOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	owner := testOwner("session-1", "turn-1", now)

	recorded, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now)
	if err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	if recorded.LastHeartbeat != now {
		t.Fatalf("LastHeartbeat = %s, want %s", recorded.LastHeartbeat, now)
	}
	read, ok, err := store.ReadOwner(ctx)
	if err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	}
	if !ok {
		t.Fatal("ReadOwner ok = false")
	}
	if read.PID != owner.PID || read.Hostname != owner.Hostname || read.ExecutablePath != owner.ExecutablePath {
		t.Fatalf("owner identity mismatch: %#v", read)
	}
	if read.HelperVersion != "v-test" || read.ActiveSessionID != "session-1" || read.ActiveTurnID != "turn-1" {
		t.Fatalf("owner payload mismatch: %#v", read)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.ServiceOwner == nil || state.LockOwner == nil {
		t.Fatalf("state owner metadata missing: service=%#v lock=%#v", state.ServiceOwner, state.LockOwner)
	}
	if *state.ServiceOwner != *state.LockOwner {
		t.Fatalf("service owner and lock owner diverged: service=%#v lock=%#v", *state.ServiceOwner, *state.LockOwner)
	}
}

func TestDeferredInboundSortedByChatAndMessageTime(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	events := []InboundEvent{
		{SessionID: "s2", TeamsChatID: "chat-b", TeamsMessageID: "b2", Text: "b2", Status: InboundStatusDeferred, CreatedAt: base.Add(2 * time.Second)},
		{SessionID: "s1", TeamsChatID: "chat-a", TeamsMessageID: "a2", Text: "a2", Status: InboundStatusDeferred, CreatedAt: base.Add(2 * time.Second)},
		{SessionID: "s1", TeamsChatID: "chat-a", TeamsMessageID: "a1", Text: "a1", Status: InboundStatusDeferred, CreatedAt: base.Add(time.Second)},
	}
	for _, event := range events {
		if _, _, err := store.PersistInbound(ctx, event); err != nil {
			t.Fatalf("PersistInbound error: %v", err)
		}
	}

	deferred, err := store.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound error: %v", err)
	}
	var got []string
	for _, event := range deferred {
		got = append(got, event.TeamsMessageID)
	}
	want := []string{"a1", "a2", "b2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deferred order = %#v, want %#v", got, want)
	}
}

func TestClaimControlLeasePrimaryBeatsEphemeralAndEphemeralStaysStandby(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}

	ephemeral := MachineRecord{ID: "machine-temp", ScopeID: scope.ID, Kind: MachineKindEphemeral, Label: "temp"}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ephemeral ClaimControlLease error: %v", err)
	}
	if first.Mode != LeaseModeActive || first.Lease.HolderMachineID != ephemeral.ID || first.Lease.Generation != 1 {
		t.Fatalf("ephemeral decision = %#v, want active generation 1", first)
	}

	primary := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary, Label: "primary"}
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: primary, Duration: time.Minute, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("primary ClaimControlLease error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.HolderMachineID != primary.ID || second.Lease.Generation != 2 {
		t.Fatalf("primary decision = %#v, want preempted active generation 2", second)
	}
	stateAfterPreempt, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after primary preempt error: %v", err)
	}
	if got := stateAfterPreempt.Machines[ephemeral.ID].Status; got != MachineStatusStandby {
		t.Fatalf("preempted ephemeral status = %q, want standby", got)
	}

	third, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatalf("second ephemeral ClaimControlLease error: %v", err)
	}
	if third.Mode != LeaseModeStandby || third.Lease.HolderMachineID != primary.ID {
		t.Fatalf("second ephemeral decision = %#v, want standby behind primary", third)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Machines[ephemeral.ID].Status; got != MachineStatusStandby {
		t.Fatalf("ephemeral status = %q, want standby", got)
	}
	if got := state.Machines[primary.ID].Status; got != MachineStatusActive {
		t.Fatalf("primary status = %q, want active", got)
	}
}

func TestClaimControlLeaseDoesNotPreemptActiveTurn(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	ephemeral := MachineRecord{ID: "machine-temp", ScopeID: scope.ID, Kind: MachineKindEphemeral}
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ephemeral ClaimControlLease error: %v", err)
	}
	owner := OwnerMetadata{PID: 123, Hostname: "temp", ExecutablePath: "/bin/helper", MachineID: ephemeral.ID, LeaseGeneration: decision.Lease.Generation, ActiveTurnID: "turn-1", StartedAt: now}
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	primary := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	primaryDecision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: primary, Duration: time.Minute, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("primary ClaimControlLease error: %v", err)
	}
	if primaryDecision.Mode != LeaseModeStandby || primaryDecision.Lease.HolderMachineID != ephemeral.ID {
		t.Fatalf("primary decision during active turn = %#v, want standby behind active ephemeral turn", primaryDecision)
	}
}

func TestControlLeaseForwardJumpAllowsTakeoverAfterSleep(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	ephemeral := MachineRecord{ID: "machine-temp", ScopeID: scope.ID, Kind: MachineKindEphemeral}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ephemeral ClaimControlLease error: %v", err)
	}
	owner := OwnerMetadata{PID: 123, Hostname: "temp", ExecutablePath: "/bin/helper", MachineID: ephemeral.ID, LeaseGeneration: first.Lease.Generation, ActiveTurnID: "turn-1", StartedAt: now}
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	primary := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	wake := now.Add(2 * time.Minute)
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: primary, Duration: time.Minute, Now: wake})
	if err != nil {
		t.Fatalf("primary ClaimControlLease after sleep error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.HolderMachineID != primary.ID {
		t.Fatalf("primary decision after sleep = %#v, want takeover", second)
	}
	if _, err := store.ValidateControlLease(ctx, ephemeral.ID, first.Lease.Generation, wake); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("old sleepy holder ValidateControlLease error = %v, want ErrControlLeaseNotHeld", err)
	}
}

func TestClaimControlLeaseConcurrentManyMachinesSingleActive(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	errs := make(chan error, 80)
	for i := 0; i < 40; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			kind := MachineKindEphemeral
			if i%5 == 0 {
				kind = MachineKindPrimary
			}
			machine := MachineRecord{
				ID:      fmt.Sprintf("machine-%02d", i),
				ScopeID: scope.ID,
				Kind:    kind,
				Label:   fmt.Sprintf("machine %02d", i),
			}
			for j := 0; j < 5; j++ {
				now := start.Add(time.Duration(i*5+j) * time.Millisecond)
				if _, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Minute, Now: now}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("ClaimControlLease concurrent error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.ControlLease.HolderMachineID == "" || state.ControlLease.Generation <= 0 {
		t.Fatalf("missing active control lease after concurrent claims: %#v", state.ControlLease)
	}
	active := 0
	for _, machine := range state.Machines {
		if machine.Status == MachineStatusActive {
			active++
			if machine.ID != state.ControlLease.HolderMachineID {
				t.Fatalf("machine %s is active but lease holder is %s", machine.ID, state.ControlLease.HolderMachineID)
			}
		}
	}
	if active != 1 {
		t.Fatalf("active machine count = %d, want 1; machines=%#v lease=%#v", active, state.Machines, state.ControlLease)
	}
}

func TestTeamsBackgroundKeepaliveOldGenerationCannotReleaseNewHolderCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	firstMachine := MachineRecord{ID: "machine-first", ScopeID: scope.ID, Kind: MachineKindPrimary}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: firstMachine, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("first ClaimControlLease error: %v", err)
	}
	secondMachine := MachineRecord{ID: "machine-second", ScopeID: scope.ID, Kind: MachineKindPrimary}
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: secondMachine, Duration: time.Minute, Now: now.Add(2 * time.Minute)})
	if err != nil {
		t.Fatalf("second ClaimControlLease after lease expiry error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.HolderMachineID != secondMachine.ID || second.Lease.Generation <= first.Lease.Generation {
		t.Fatalf("second decision = %#v, want newer active generation", second)
	}
	released, err := store.ReleaseControlLeaseIfHolder(ctx, firstMachine.ID, first.Lease.Generation)
	if err != nil {
		t.Fatalf("old generation ReleaseControlLeaseIfHolder error: %v", err)
	}
	if released {
		t.Fatal("old generation release cleared the new holder")
	}
	if _, err := store.ValidateControlLease(ctx, secondMachine.ID, second.Lease.Generation, now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatalf("new holder lease was not preserved: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.ControlLease.HolderMachineID != secondMachine.ID || state.Machines[firstMachine.ID].Status != MachineStatusStandby {
		t.Fatalf("unexpected state after stale release: lease=%#v machines=%#v", state.ControlLease, state.Machines)
	}
}

func TestTeamsBackgroundKeepaliveSameHolderRefreshPreservesGenerationCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	machine := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("first ClaimControlLease error: %v", err)
	}
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Minute, Now: now.Add(30 * time.Second)})
	if err != nil {
		t.Fatalf("refresh ClaimControlLease error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.Generation != first.Lease.Generation {
		t.Fatalf("same holder refresh = %#v, want active generation %d", second, first.Lease.Generation)
	}
	if !second.Lease.LeaseUntil.After(first.Lease.LeaseUntil) {
		t.Fatalf("same holder refresh did not extend lease: before=%s after=%s", first.Lease.LeaseUntil, second.Lease.LeaseUntil)
	}
}

func TestTeamsBackgroundKeepaliveScopeIsolationRejectsSharedStateReuseCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	scopeA := ScopeIdentity{ID: "scope-a", AccountID: "user-a", OSUser: "alice", Profile: "default"}
	scopeB := ScopeIdentity{ID: "scope-b", AccountID: "user-b", OSUser: "bob", Profile: "default"}
	if _, err := store.RecordScope(ctx, scopeA); err != nil {
		t.Fatalf("RecordScope A error: %v", err)
	}
	if _, err := store.RecordScope(ctx, scopeB); err == nil || !strings.Contains(err.Error(), `belongs to scope "scope-a"`) {
		t.Fatalf("RecordScope B error = %v, want scope isolation guard", err)
	}
	_, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:   scopeB,
		Machine: MachineRecord{ID: "machine-b", ScopeID: scopeB.ID, Kind: MachineKindPrimary},
		Now:     time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
	})
	if err == nil || !strings.Contains(err.Error(), `belongs to scope "scope-a"`) {
		t.Fatalf("ClaimControlLease for scope B error = %v, want scope isolation guard", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.Scope.ID != scopeA.ID || len(state.Machines) != 0 {
		t.Fatalf("scope isolation failure mutated state: %#v", state)
	}
}

func TestIsStaleHandlesFutureHeartbeatAndThresholdBoundary(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	staleAfter := time.Minute
	owner := OwnerMetadata{LastHeartbeat: now.Add(30 * time.Second)}
	if IsStale(owner, staleAfter, now) {
		t.Fatal("future heartbeat should not be stale")
	}
	owner.LastHeartbeat = now.Add(-staleAfter)
	if IsStale(owner, staleAfter, now) {
		t.Fatal("heartbeat exactly at stale threshold should not be stale")
	}
	owner.LastHeartbeat = now.Add(-staleAfter - time.Nanosecond)
	if !IsStale(owner, staleAfter, now) {
		t.Fatal("heartbeat older than stale threshold should be stale")
	}
}

func TestValidateAndReleaseControlLease(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	machine := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ClaimControlLease error: %v", err)
	}
	if _, err := store.ValidateControlLease(ctx, machine.ID, decision.Lease.Generation, now.Add(30*time.Second)); err != nil {
		t.Fatalf("ValidateControlLease held error: %v", err)
	}
	if _, err := store.ValidateControlLease(ctx, "other-machine", decision.Lease.Generation, now.Add(30*time.Second)); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("ValidateControlLease wrong holder error = %v, want ErrControlLeaseNotHeld", err)
	}
	released, err := store.ReleaseControlLeaseIfHolder(ctx, machine.ID, decision.Lease.Generation)
	if err != nil {
		t.Fatalf("ReleaseControlLeaseIfHolder error: %v", err)
	}
	if !released {
		t.Fatal("ReleaseControlLeaseIfHolder released = false, want true")
	}
	if _, err := store.ValidateControlLease(ctx, machine.ID, decision.Lease.Generation, now.Add(30*time.Second)); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("ValidateControlLease after release error = %v, want ErrControlLeaseNotHeld", err)
	}
}

func TestRecordOwnerHeartbeatUpdatesExistingOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	startedAt := testOwnerStart()
	firstHeartbeat := startedAt.Add(5 * time.Second)
	secondHeartbeat := startedAt.Add(20 * time.Second)
	owner := testOwner("session-1", "turn-1", startedAt)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, firstHeartbeat); err != nil {
		t.Fatalf("first RecordOwnerHeartbeat error: %v", err)
	}

	owner.ActiveSessionID = "session-2"
	owner.ActiveTurnID = "turn-2"
	owner.HelperVersion = "v-test.2"
	updated, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, secondHeartbeat)
	if err != nil {
		t.Fatalf("second RecordOwnerHeartbeat error: %v", err)
	}
	if updated.StartedAt != startedAt {
		t.Fatalf("StartedAt = %s, want %s", updated.StartedAt, startedAt)
	}
	if updated.LastHeartbeat != secondHeartbeat {
		t.Fatalf("LastHeartbeat = %s, want %s", updated.LastHeartbeat, secondHeartbeat)
	}
	if updated.ActiveSessionID != "session-2" || updated.ActiveTurnID != "turn-2" || updated.HelperVersion != "v-test.2" {
		t.Fatalf("updated owner payload mismatch: %#v", updated)
	}
}

func TestRecordOwnerHeartbeatRefusesLiveOwnerWithDiagnostic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	existing := testOwner("session-live", "turn-live", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, existing, time.Minute, now); err != nil {
		t.Fatalf("existing RecordOwnerHeartbeat error: %v", err)
	}

	contender := testOwner("session-new", "turn-new", now.Add(10*time.Second))
	contender.PID = existing.PID + 1
	_, err := store.RecordOwnerHeartbeat(ctx, contender, time.Minute, now.Add(15*time.Second))
	if !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("RecordOwnerHeartbeat error = %v, want ErrOwnerLive", err)
	}
	msg := err.Error()
	for _, want := range []string{
		"pid=4242",
		`host="host-a"`,
		`executable="/usr/local/bin/codex-helper"`,
		`helper_version="v-test"`,
		`active_session_id="session-live"`,
		`active_turn_id="turn-live"`,
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("diagnostic %q missing from error: %s", want, msg)
		}
	}
	read, ok, err := store.ReadOwner(ctx)
	if err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	}
	if !ok {
		t.Fatal("ReadOwner ok = false")
	}
	if read.PID != existing.PID || read.ActiveSessionID != "session-live" {
		t.Fatalf("live owner was unexpectedly replaced: %#v", read)
	}
}

func TestRecoverStaleOwnerReplacesStaleOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	startedAt := testOwnerStart()
	staleHeartbeat := startedAt.Add(10 * time.Second)
	staleOwner := testOwner("session-old", "turn-old", startedAt)
	if _, err := store.RecordOwnerHeartbeat(ctx, staleOwner, time.Minute, staleHeartbeat); err != nil {
		t.Fatalf("stale RecordOwnerHeartbeat error: %v", err)
	}

	now := staleHeartbeat.Add(2 * time.Minute)
	if !IsStale(staleOwner.withLastHeartbeat(staleHeartbeat), time.Minute, now) {
		t.Fatal("owner should be stale")
	}
	next := testOwner("session-new", "turn-new", now)
	next.PID = 5252
	recoveredOwner, recovered, err := store.RecoverStaleOwner(ctx, next, time.Minute, now)
	if err != nil {
		t.Fatalf("RecoverStaleOwner error: %v", err)
	}
	if !recovered {
		t.Fatal("RecoverStaleOwner recovered = false")
	}
	if recoveredOwner.PID != next.PID || recoveredOwner.ActiveSessionID != "session-new" || recoveredOwner.ActiveTurnID != "turn-new" {
		t.Fatalf("recovered owner mismatch: %#v", recoveredOwner)
	}
}

func TestRecoverStaleOwnerRefusesLiveOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	liveOwner := testOwner("session-live", "turn-live", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, liveOwner, time.Minute, now); err != nil {
		t.Fatalf("live RecordOwnerHeartbeat error: %v", err)
	}

	next := testOwner("session-new", "turn-new", now.Add(20*time.Second))
	next.PID = 5252
	_, recovered, err := store.RecoverStaleOwner(ctx, next, time.Minute, now.Add(30*time.Second))
	if !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("RecoverStaleOwner error = %v, want ErrOwnerLive", err)
	}
	if recovered {
		t.Fatal("RecoverStaleOwner recovered = true for live owner")
	}
}

func TestClearOwnerIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.ClearOwner(ctx); err != nil {
		t.Fatalf("first empty ClearOwner error: %v", err)
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after empty clear error: %v", err)
	} else if ok {
		t.Fatal("ReadOwner ok = true after empty clear")
	}
	now := testOwnerStart()
	if _, err := store.RecordOwnerHeartbeat(ctx, testOwner("session-1", "turn-1", now), time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	if err := store.ClearOwner(ctx); err != nil {
		t.Fatalf("ClearOwner error: %v", err)
	}
	if err := store.ClearOwner(ctx); err != nil {
		t.Fatalf("second ClearOwner error: %v", err)
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after clear error: %v", err)
	} else if ok {
		t.Fatal("ReadOwner ok = true after clear")
	}
}

func TestClearOwnerIfSameDoesNotClearDifferentOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	owner := testOwner("session-1", "turn-1", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	other := owner
	other.PID++
	cleared, err := store.ClearOwnerIfSame(ctx, other)
	if err != nil {
		t.Fatalf("ClearOwnerIfSame different owner error: %v", err)
	}
	if cleared {
		t.Fatal("ClearOwnerIfSame cleared different owner")
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after different clear error: %v", err)
	} else if !ok {
		t.Fatal("owner missing after different clear")
	}
	cleared, err = store.ClearOwnerIfSame(ctx, owner)
	if err != nil {
		t.Fatalf("ClearOwnerIfSame same owner error: %v", err)
	}
	if !cleared {
		t.Fatal("ClearOwnerIfSame did not clear same owner")
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after same clear error: %v", err)
	} else if ok {
		t.Fatal("owner still present after same clear")
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "teams-state", "state.json"))
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	return store
}

func testSession() SessionContext {
	return SessionContext{
		ID:            "s1",
		TeamsChatID:   "chat-1",
		TeamsChatURL:  "https://teams.example/chat-1",
		TeamsTopic:    "topic",
		RunnerKind:    "exec",
		CodexVersion:  "1.2.3",
		Cwd:           "/workspace/project",
		CodexHome:     "/home/user/.codex",
		Profile:       "default",
		Model:         "gpt-test",
		Sandbox:       "workspace-write",
		ProxyMode:     "on",
		YoloMode:      "off",
		CodexThreadID: "thread-0",
	}
}

func testInbound() InboundEvent {
	return InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Source:         "teams",
	}
}

func testOwnerStart() time.Time {
	return time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
}

func testOwner(activeSessionID string, activeTurnID string, startedAt time.Time) OwnerMetadata {
	return OwnerMetadata{
		PID:             4242,
		Hostname:        "host-a",
		ExecutablePath:  "/usr/local/bin/codex-helper",
		HelperVersion:   "v-test",
		StartedAt:       startedAt,
		ActiveSessionID: activeSessionID,
		ActiveTurnID:    activeTurnID,
	}
}

func sameControl(a ServiceControl, b ServiceControl) bool {
	return a.Paused == b.Paused &&
		a.Draining == b.Draining &&
		a.Reason == b.Reason &&
		a.UpdatedAt.Equal(b.UpdatedAt)
}

func (owner OwnerMetadata) withLastHeartbeat(lastHeartbeat time.Time) OwnerMetadata {
	owner.LastHeartbeat = lastHeartbeat
	return owner
}
