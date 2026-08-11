package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestTranscriptPublicationWritesRespectParentForkFence(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqliteMode], func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			seedParentForkPerfState(t, store, parentForkPerfScale{name: "fence", sessions: 1}, true, ForkPhaseParentFenced)
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}

			before, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load before fenced publication writes: %v", err)
			}
			checkpoint := ImportCheckpoint{
				ID:             "transcript:parent",
				SessionID:      "parent",
				SourcePath:     "parent.jsonl",
				LastRecordID:   "after-fork-record",
				LastSourceLine: 10,
			}
			ledger := TranscriptLedgerRecord{
				ID:             "ledger:parent:after-fork-record",
				SessionID:      "parent",
				SourceRecordID: "after-fork-record",
			}
			delivery := TranscriptDeliveryRecord{
				ID:             "transcript-delivery:parent:after-fork-record",
				SessionID:      "parent",
				SourceRecordID: "after-fork-record",
				Status:         TranscriptDeliveryStatusSkipped,
			}
			request := TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID:          "outbox:transcript-delivery:parent:after-fork-record",
					SessionID:   "parent",
					TurnID:      "sync:parent",
					TeamsChatID: "parent-chat",
					Kind:        "sync-assistant-001",
					Body:        "must not queue after fork fence",
				},
				Delivery:             delivery,
				Checkpoint:           checkpoint,
				ParentFenceSessionID: "parent",
			}

			if _, _, _, err := store.QueueTranscriptDeliveryOutbox(ctx, request); !errors.Is(err, ErrForkParentFenced) {
				t.Fatalf("QueueTranscriptDeliveryOutbox error = %v, want ErrForkParentFenced", err)
			}
			if _, _, err := store.RecordTranscriptDeliveryIfParentUnfenced(ctx, "parent", delivery, checkpoint); !errors.Is(err, ErrForkParentFenced) {
				t.Fatalf("RecordTranscriptDeliveryIfParentUnfenced error = %v, want ErrForkParentFenced", err)
			}
			if err := store.RecordTranscriptCheckpointIfParentUnfenced(ctx, "parent", checkpoint, ledger); !errors.Is(err, ErrForkParentFenced) {
				t.Fatalf("RecordTranscriptCheckpointIfParentUnfenced error = %v, want ErrForkParentFenced", err)
			}
			if _, _, err := store.UpdateImportCheckpointIfParentUnfenced(ctx, "parent", checkpoint.ID, func(current ImportCheckpoint, _ bool, _ time.Time) (ImportCheckpoint, bool, error) {
				current.LastRecordID = "must-not-advance"
				return current, true, nil
			}); !errors.Is(err, ErrForkParentFenced) {
				t.Fatalf("UpdateImportCheckpointIfParentUnfenced error = %v, want ErrForkParentFenced", err)
			}

			after, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after fenced publication writes: %v", err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("fenced publication writes changed durable state:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

func TestTranscriptDeliveryOutboxCannotBeClaimedAfterParentForkFence(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqliteMode], func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			seedParentForkPerfState(t, store, parentForkPerfScale{name: "outbox-fence", sessions: 1}, false, "")
			delivery := TranscriptDeliveryRecord{
				ID:             "transcript-delivery:parent:queued-before-fork",
				SessionID:      "parent",
				SourceRecordID: "queued-before-fork",
				Status:         TranscriptDeliveryStatusQueued,
			}
			if _, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID:          "outbox:transcript-delivery:parent:queued-before-fork",
					SessionID:   "parent",
					TurnID:      "sync:parent",
					TeamsChatID: "parent-chat",
					Kind:        "sync-assistant-001",
					Body:        "queued before fork",
				},
				Delivery: delivery,
			}); err != nil || !created || alreadyDelivered {
				t.Fatalf("queue before fork created=%v alreadyDelivered=%v err=%v", created, alreadyDelivered, err)
			}
			if err := store.Update(ctx, func(state *State) error {
				state.ForkOperations["outbox-fence-operation"] = ForkOperation{
					ID:               "outbox-fence-operation",
					ParentSessionID:  "parent",
					ParentChatID:     "parent-chat",
					ChildSessionID:   "child",
					Phase:            ForkPhaseParentFenced,
					HistoryNamespace: "fork-history:outbox-fence-operation",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed parent fence after queue: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			before, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load before fenced outbox claim: %v", err)
			}
			if _, err := store.MarkOutboxSendAttempt(ctx, "outbox:transcript-delivery:parent:queued-before-fork"); !errors.Is(err, ErrOutboxSendNotClaimed) {
				t.Fatalf("MarkOutboxSendAttempt error = %v, want ErrOutboxSendNotClaimed", err)
			}
			after, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after fenced outbox claim: %v", err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("fenced outbox claim changed durable state:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}
