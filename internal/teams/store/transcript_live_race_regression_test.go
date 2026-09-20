package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func transcriptLiveRaceQueueRequest() TranscriptDeliveryQueueRequest {
	return TranscriptDeliveryQueueRequest{
		Message: OutboxMessage{
			ID:                       "outbox:transcript-delivery:live-race:final",
			SessionID:                "live-race-session",
			TurnID:                   "sync:live-race-session",
			TeamsChatID:              "live-race-chat",
			Kind:                     "sync-assistant-final-record",
			Body:                     "live race answer",
			SourceTextHash:           "live-race-hash",
			TranscriptCheckpointID:   "transcript:live-race-session",
			TranscriptSourceRecordID: "live-race-final-record",
			TranscriptSourcePath:     "/codex/live-race.jsonl",
		},
		Delivery: TranscriptDeliveryRecord{
			ID:             "delivery:live-race:final",
			SessionID:      "live-race-session",
			SourcePath:     "/codex/live-race.jsonl",
			SourceRecordID: "live-race-final-record",
			Kind:           "sync-assistant-final-record",
			TextHash:       "live-race-hash",
		},
		Checkpoint: ImportCheckpoint{
			ID:                "transcript:live-race-session",
			SessionID:         "live-race-session",
			SourcePath:        "/codex/live-race.jsonl",
			SourceFingerprint: "live-race-proof",
			LastRecordID:      "live-race-before",
			LastOffset:        128,
			LastOffsetKnown:   true,
			Status:            importCheckpointStatusComplete,
		},
	}
}

func TestAutomaticTranscriptDeliveryDefersBehindLiveTurnAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		name := "json"
		if sqliteMode {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["live-race-session"] = SessionContext{
					ID: "live-race-session", Status: SessionStatusActive,
					TeamsChatID: "live-race-chat", CodexThreadID: "live-race-thread",
				}
				state.Turns["live-race-turn"] = Turn{
					ID: "live-race-turn", SessionID: "live-race-session", Status: TurnStatusRunning,
					CodexThreadID: "live-race-thread", CodexTurnID: "live-race-codex-turn", StartedAt: now,
				}
				checkpoint := transcriptLiveRaceQueueRequest().Checkpoint
				state.ImportCheckpoints[checkpoint.ID] = checkpoint
				return nil
			}); err != nil {
				t.Fatalf("seed live race state: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}

			_, _, _, err := store.QueueTranscriptDeliveryOutbox(ctx, transcriptLiveRaceQueueRequest())
			if !errors.Is(err, ErrTranscriptDeliveryDeferredByLiveTurn) {
				t.Fatalf("QueueTranscriptDeliveryOutbox error = %v, want ErrTranscriptDeliveryDeferredByLiveTurn", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after deferred queue: %v", err)
			}
			if _, found := state.OutboxMessages["outbox:transcript-delivery:live-race:final"]; found {
				t.Fatal("deferred live-race queue created an outbox row")
			}
			if _, found := state.TranscriptDeliveries["delivery:live-race:final"]; found {
				t.Fatal("deferred live-race queue created a delivery row")
			}
		})
	}
}

func TestAutomaticTranscriptDeliveryDoesNotReplaySourceAfterCheckpointAdvanceAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		name := "json"
		if sqliteMode {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["live-race-session"] = SessionContext{
					ID: "live-race-session", Status: SessionStatusActive, TeamsChatID: "live-race-chat",
				}
				checkpoint := transcriptLiveRaceQueueRequest().Checkpoint
				checkpoint.LastRecordID = "live-race-final-record"
				state.ImportCheckpoints[checkpoint.ID] = checkpoint
				return nil
			}); err != nil {
				t.Fatalf("seed advanced checkpoint: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}

			_, _, _, err := store.QueueTranscriptDeliveryOutbox(ctx, transcriptLiveRaceQueueRequest())
			if !errors.Is(err, ErrTranscriptDeliverySupersededByCheckpoint) {
				t.Fatalf("QueueTranscriptDeliveryOutbox error = %v, want ErrTranscriptDeliverySupersededByCheckpoint", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after superseded queue: %v", err)
			}
			if _, found := state.OutboxMessages["outbox:transcript-delivery:live-race:final"]; found {
				t.Fatal("superseded source queue created an outbox row")
			}
			if _, found := state.TranscriptDeliveries["delivery:live-race:final"]; found {
				t.Fatal("superseded source queue created a delivery row")
			}
		})
	}
}

func TestAutomaticTranscriptDeliveryLinksSentLiveFinalWithoutSecondOutboxJSON(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	request := transcriptLiveRaceQueueRequest()
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["live-race-session"] = SessionContext{
			ID: "live-race-session", Status: SessionStatusActive, TeamsChatID: "live-race-chat",
		}
		state.Turns["live-race-turn"] = Turn{
			ID: "live-race-turn", SessionID: "live-race-session", Status: TurnStatusCompleted,
		}
		state.ImportCheckpoints[request.Checkpoint.ID] = request.Checkpoint
		state.OutboxMessages["outbox:live-race:final"] = OutboxMessage{
			ID:                       "outbox:live-race:final",
			SessionID:                "live-race-session",
			TurnID:                   "live-race-turn",
			TeamsChatID:              "live-race-chat",
			Kind:                     "final",
			NotificationKind:         "turn_completed",
			Body:                     request.Message.Body,
			Status:                   OutboxStatusSent,
			TeamsMessageID:           "teams-live-race-final",
			TranscriptSourceRecordID: request.Message.TranscriptSourceRecordID,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed sent live final: %v", err)
	}

	out, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, request)
	if err != nil || created || !alreadyDelivered || out.ID != "outbox:live-race:final" {
		t.Fatalf("QueueTranscriptDeliveryOutbox out=%#v created=%v alreadyDelivered=%v err=%v, want existing sent live final", out, created, alreadyDelivered, err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load linked live final state: %v", err)
	}
	delivery := state.TranscriptDeliveries[request.Delivery.ID]
	if delivery.OutboxID != out.ID || delivery.Status != TranscriptDeliveryStatusSent || delivery.TeamsMessageID != "teams-live-race-final" {
		t.Fatalf("linked transcript delivery = %#v, want sent live final identity", delivery)
	}
	if _, found := state.OutboxMessages[request.Message.ID]; found {
		t.Fatal("linking the live final created a second transcript outbox row")
	}
}
