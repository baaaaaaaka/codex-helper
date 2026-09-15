package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPermanentTranscriptGraphFailureNeedsAttentionAcrossBackendsAndRestart(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		name := "json"
		if sqliteMode {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			const (
				sessionID  = "transcript-failure-session"
				outboxID   = "outbox:transcript-permanent-failure"
				deliveryID = "delivery:transcript-permanent-failure"
				helperID   = "helper:transcript-permanent-failure"
				artifactID = "artifact:transcript-permanent-failure"
			)
			now := time.Date(2026, 9, 14, 2, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: "transcript-failure-chat",
					CodexThreadID: "transcript-failure-thread", CreatedAt: now, UpdatedAt: now,
				}
				state.OutboxMessages[outboxID] = OutboxMessage{
					ID: outboxID, SessionID: sessionID, TeamsChatID: "transcript-failure-chat",
					Kind: "helper",
					Body: "transcript that Graph rejected", Status: OutboxStatusQueued,
					ArtifactIDs: []string{artifactID}, CreatedAt: now, UpdatedAt: now,
				}
				state.TranscriptDeliveries[deliveryID] = TranscriptDeliveryRecord{
					ID: deliveryID, SessionID: sessionID, OutboxID: outboxID,
					SourcePath: "/codex/session.jsonl", SourceLine: 42,
					SourceRecordID: "record-42", Kind: "import-assistant-001",
					TextHash: "hash-42", Status: TranscriptDeliveryStatusQueued,
					CreatedAt: now, UpdatedAt: now,
				}
				state.HelperDeliveries[helperID] = HelperDeliveryRecord{
					ID: helperID, SessionID: sessionID, TeamsChatID: "transcript-failure-chat",
					OutboxID: outboxID, Kind: "import-assistant-001",
					Status: HelperDeliveryStatusQueued, CreatedAt: now, UpdatedAt: now,
				}
				state.ArtifactRecords[artifactID] = ArtifactRecord{
					ID: artifactID, SessionID: sessionID, OutboxID: outboxID,
					Path: "/codex/artifact.txt", Status: "queued", CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed permanent transcript fixture: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			seeded, err := store.OutboxMessageByID(ctx, outboxID)
			if err != nil || seeded.Status != OutboxStatusQueued {
				t.Fatalf("seeded outbox=%#v err=%v", seeded, err)
			}

			claimed, err := store.MarkOutboxSendAttempt(ctx, outboxID)
			if err != nil || claimed.SendAttemptToken == "" {
				t.Fatalf("MarkOutboxSendAttempt: out=%#v err=%v", claimed, err)
			}
			beforeFailure, err := store.Load(ctx)
			if err != nil || beforeFailure.TranscriptDeliveries[deliveryID].OutboxID != outboxID {
				t.Fatalf("linked delivery before permanent failure=%#v present=%v err=%v", beforeFailure.TranscriptDeliveries[deliveryID], beforeFailure.TranscriptDeliveries[deliveryID].ID != "", err)
			}
			if _, err := store.MarkOutboxPermanentSendFailureForAttempt(ctx, outboxID, "stale-attempt", "permanent Graph rejection: HTTP 400"); !errors.Is(err, ErrOutboxSendNotClaimed) {
				t.Fatalf("stale permanent failure err=%v, want ErrOutboxSendNotClaimed", err)
			}
			failed, err := store.MarkOutboxPermanentSendFailureForAttempt(ctx, outboxID, claimed.SendAttemptToken, "permanent Graph rejection: HTTP 400")
			if err != nil || failed.Status != OutboxStatusSkipped {
				t.Fatalf("MarkOutboxPermanentSendFailureForAttempt: out=%#v err=%v", failed, err)
			}

			assertPermanentTranscriptFailureState(t, store, outboxID, deliveryID, helperID, artifactID)

			// The state must retain the disposition after a process restart. Closing
			// and reopening also exercises the SQLite projection rather than only
			// the in-memory reducer.
			path := store.Path()
			if err := store.Close(); err != nil {
				t.Fatalf("close before restart: %v", err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatalf("reopen store: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			assertPermanentTranscriptFailureState(t, reopened, outboxID, deliveryID, helperID, artifactID)

			// An ordinary background/import retry is suppressed: no new POST can be
			// caused by merely seeing the same source record again.
			background, created, alreadyDelivered, err := reopened.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID: outboxID, SessionID: sessionID, TeamsChatID: "transcript-failure-chat",
					Kind: "helper",
					Body: "transcript that Graph rejected",
				},
				Delivery: TranscriptDeliveryRecord{ID: deliveryID, SessionID: sessionID, Status: TranscriptDeliveryStatusQueued},
			})
			if !errors.Is(err, ErrTranscriptDeliveryNeedsAttention) {
				t.Fatalf("background retry after permanent failure error = %v, want ErrTranscriptDeliveryNeedsAttention", err)
			}
			if background.ID != "" || created || alreadyDelivered {
				t.Fatalf("background retry out=%#v created=%v alreadyDelivered=%v, want no mutation", background, created, alreadyDelivered)
			}

			// Explicit history repair is the only path allowed to requeue the
			// quarantined source. It reuses the stable outbox/delivery identity,
			// preventing a second automatic identity from being created.
			repaired, created, alreadyDelivered, err := reopened.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID: outboxID, SessionID: sessionID, TeamsChatID: "transcript-failure-chat",
					TurnID: "publish-history:repair-transcript-failure", Kind: "import-assistant-001",
					Body: "transcript that Graph rejected",
				},
				Delivery: TranscriptDeliveryRecord{
					ID: deliveryID, SessionID: sessionID, SourcePath: "/codex/session.jsonl",
					SourceLine: 42, SourceRecordID: "record-42", Kind: "import-assistant-001",
					TextHash: "hash-42", Status: TranscriptDeliveryStatusQueued,
				},
			})
			if err != nil || created || alreadyDelivered || repaired.Status != OutboxStatusQueued {
				t.Fatalf("explicit history repair out=%#v created=%v alreadyDelivered=%v err=%v, want reused queued row", repaired, created, alreadyDelivered, err)
			}
			state, err := reopened.Load(ctx)
			if err != nil {
				t.Fatalf("load after explicit repair: %v", err)
			}
			if got := state.TranscriptDeliveries[deliveryID].Status; got != TranscriptDeliveryStatusQueued {
				t.Fatalf("delivery status after explicit repair=%q, want queued", got)
			}
			if got := state.HelperDeliveries[helperID].Status; got != HelperDeliveryStatusQueued {
				t.Fatalf("helper status after explicit repair=%q, want queued", got)
			}
		})
	}
}

func TestPermanentOrdinaryGraphFailureRetainsExistingSkipSemanticsAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		name := "json"
		if sqliteMode {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			const outboxID = "outbox:ordinary-permanent-failure"
			if err := store.Update(ctx, func(state *State) error {
				state.OutboxMessages[outboxID] = OutboxMessage{
					ID: outboxID, TeamsChatID: "ordinary-failure-chat", Kind: "helper",
					Body: "ordinary message", Status: OutboxStatusQueued,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed ordinary outbox: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			claimed, err := store.MarkOutboxSendAttempt(ctx, outboxID)
			if err != nil {
				t.Fatalf("MarkOutboxSendAttempt: %v", err)
			}
			if _, err := store.MarkOutboxPermanentSendFailureForAttempt(ctx, outboxID, claimed.SendAttemptToken, "permanent Graph rejection: HTTP 403"); err != nil {
				t.Fatalf("MarkOutboxPermanentSendFailureForAttempt: %v", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load ordinary failure: %v", err)
			}
			if got := state.OutboxMessages[outboxID].Status; got != OutboxStatusSkipped {
				t.Fatalf("ordinary outbox status=%q, want skipped", got)
			}
			for _, delivery := range state.HelperDeliveries {
				if delivery.OutboxID == outboxID && delivery.Status != HelperDeliveryStatusSkipped {
					t.Fatalf("ordinary helper status=%q, want skipped", delivery.Status)
				}
			}
		})
	}
}

func TestExplicitHistoryRepairsPermanentAutomaticTranscriptAcrossNamespaces(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		name := "json"
		if sqliteMode {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			const (
				sessionID       = "namespace-repair-session"
				automaticOutbox = "outbox:namespace-repair-automatic"
				automaticID     = "delivery:namespace-repair-automatic"
				explicitID      = "delivery:namespace-repair-explicit"
			)
			now := time.Date(2026, 9, 14, 3, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: "namespace-repair-chat",
					CodexThreadID: "namespace-repair-thread", CreatedAt: now, UpdatedAt: now,
				}
				state.OutboxMessages[automaticOutbox] = OutboxMessage{
					ID: automaticOutbox, SessionID: sessionID, TeamsChatID: "namespace-repair-chat",
					TurnID: "import-bg:namespace-repair", Kind: "import-assistant-001",
					Body: "same source record", Status: OutboxStatusSkipped,
					LastSendError: "permanent Graph rejection: Graph POST failed: HTTP 400",
					CreatedAt:     now, UpdatedAt: now,
				}
				state.TranscriptDeliveries[automaticID] = TranscriptDeliveryRecord{
					ID: automaticID, SessionID: sessionID, OutboxID: automaticOutbox,
					SourcePath: "/codex/session.jsonl", SourceLine: 17,
					SourceRecordID: "record-17", TextHash: "same-hash",
					Status: TranscriptDeliveryStatusNeedsAttention, CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed namespace repair fixture: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}

			repaired, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID: automaticOutbox + ":explicit", SessionID: sessionID, TeamsChatID: "namespace-repair-chat",
					TurnID: "publish-full:namespace-repair", Kind: "import-assistant-001", Body: "same source record",
				},
				Delivery: TranscriptDeliveryRecord{
					ID: explicitID, SessionID: sessionID, SourcePath: "/codex/session.jsonl", SourceLine: 17,
					SourceRecordID: "record-17", TextHash: "same-hash", Status: TranscriptDeliveryStatusQueued,
				},
			})
			if err != nil || created || alreadyDelivered || repaired.ID != automaticOutbox || repaired.Status != OutboxStatusQueued {
				t.Fatalf("explicit namespace repair outbox=%#v created=%v alreadyDelivered=%v err=%v, want reused queued automatic row", repaired, created, alreadyDelivered, err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load repaired namespace fixture: %v", err)
			}
			if got := state.OutboxMessages[automaticOutbox].Status; got != OutboxStatusQueued {
				t.Fatalf("automatic outbox status after repair=%q, want queued", got)
			}
			if got := state.TranscriptDeliveries[explicitID].OutboxID; got != automaticOutbox {
				t.Fatalf("explicit delivery outbox=%q, want %q", got, automaticOutbox)
			}
			if got := state.TranscriptDeliveries[explicitID].Status; got != TranscriptDeliveryStatusQueued {
				t.Fatalf("explicit delivery status=%q, want queued", got)
			}
		})
	}
}

func TestNeedsAttentionReplayDoesNotAdvanceTranscriptCheckpointAcrossBackendsAndRestart(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		name := "json"
		if sqliteMode {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			const (
				sessionID  = "needs-attention-cursor-session"
				deliveryID = "delivery:needs-attention-cursor"
			)
			now := time.Date(2026, 9, 14, 4, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: "needs-attention-cursor-chat",
					CreatedAt: now, UpdatedAt: now,
				}
				state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID,
					SourcePath: "/codex/needs-attention.jsonl", SourceGeneration: "generation-1",
					SourceFingerprint: "fingerprint-1", LastRecordID: "record-before",
					LastSourceLine: 10, LastOffset: 100, LastOffsetKnown: true,
					SourceSize: 100, Status: importCheckpointStatusComplete, UpdatedAt: now,
				}
				state.TranscriptDeliveries[deliveryID] = TranscriptDeliveryRecord{
					ID: deliveryID, SessionID: sessionID, OutboxID: "outbox:needs-attention-cursor",
					SourcePath: "/codex/needs-attention.jsonl", SourceLine: 11,
					SourceOffset: 110, SourceRecordID: "record-blocked", TextHash: "blocked-hash",
					Status: TranscriptDeliveryStatusNeedsAttention, CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed NeedsAttention cursor fixture: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}

			later := ImportCheckpoint{
				ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID,
				SourcePath: "/codex/needs-attention.jsonl", SourceGeneration: "generation-1",
				SourceFingerprint: "fingerprint-1", LastRecordID: "record-after",
				LastSourceLine: 12, LastOffset: 120, LastOffsetKnown: true,
				SourceSize: 120, Status: importCheckpointStatusComplete,
			}
			if _, created, err := store.RecordTranscriptDelivery(ctx, TranscriptDeliveryRecord{
				ID: deliveryID, SessionID: sessionID, SourcePath: "/codex/needs-attention.jsonl",
				SourceLine: 12, SourceOffset: 120, SourceRecordID: "record-after",
				Status: TranscriptDeliveryStatusNeedsAttention,
			}, later); err != nil || created {
				t.Fatalf("replay NeedsAttention delivery created=%v err=%v, want existing without cursor advance", created, err)
			}
			assertNeedsAttentionCursorUnchanged(t, store, sessionID)

			path := store.Path()
			if err := store.Close(); err != nil {
				t.Fatalf("close before NeedsAttention restart: %v", err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatalf("reopen NeedsAttention store: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			assertNeedsAttentionCursorUnchanged(t, reopened, sessionID)
		})
	}
}

func TestQueuedTranscriptDeliveryDoesNotAdvanceCheckpointAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		name := "json"
		if sqliteMode {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "queued-transcript-cursor-session"
			now := time.Date(2026, 9, 14, 5, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: "queued-transcript-chat",
					CreatedAt: now, UpdatedAt: now,
				}
				state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID,
					SourcePath: "/codex/queued-transcript.jsonl", LastRecordID: "before",
					LastSourceLine: 10, LastOffset: 100, LastOffsetKnown: true,
					SourceSize: 100, Status: "complete", UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed queued transcript cursor: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			out, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID: "outbox:queued-transcript-cursor", SessionID: sessionID,
					TurnID: "sync:" + sessionID, TeamsChatID: "queued-transcript-chat",
					Kind: "sync-assistant-001", Body: "queued answer",
				},
				Delivery: TranscriptDeliveryRecord{
					ID: "delivery:queued-transcript-cursor", SessionID: sessionID,
					SourcePath: "/codex/queued-transcript.jsonl", SourceLine: 11,
					SourceOffset: 110, SourceRecordID: "after", TextHash: "queued-answer-hash",
					Status: TranscriptDeliveryStatusQueued,
				},
				Checkpoint: ImportCheckpoint{
					ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID,
					SourcePath: "/codex/queued-transcript.jsonl", LastRecordID: "after",
					LastSourceLine: 11, LastOffset: 110, LastOffsetKnown: true,
					SourceSize: 110, Status: "complete",
				},
			})
			if err != nil || !created || alreadyDelivered || out.Status != OutboxStatusQueued {
				t.Fatalf("queue transcript out=%#v created=%v alreadyDelivered=%v err=%v", out, created, alreadyDelivered, err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load queued transcript cursor: %v", err)
			}
			checkpoint := state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)]
			if checkpoint.LastRecordID != "before" || checkpoint.LastSourceLine != 10 || checkpoint.LastOffset != 100 {
				t.Fatalf("queued delivery advanced checkpoint=%#v, want durable cursor before queued row", checkpoint)
			}
		})
	}
}

func TestExplicitHistoryCannotLinkPendingOrWrongMultipartAutomaticDelivery(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		name := "json"
		if sqliteMode {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "multipart-explicit-fence-session"
			now := time.Date(2026, 9, 14, 6, 0, 0, 0, time.UTC)
			body := "multipart source body"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: "multipart-explicit-chat", CreatedAt: now, UpdatedAt: now}
				state.OutboxMessages["outbox:automatic-part-1"] = OutboxMessage{
					ID: "outbox:automatic-part-1", SessionID: sessionID, TurnID: "import-bg:" + sessionID,
					TeamsChatID: "multipart-explicit-chat", Kind: "import-assistant-001-001", Body: body,
					PartIndex: 1, PartCount: 2, Status: OutboxStatusQueued, CreatedAt: now, UpdatedAt: now,
				}
				state.TranscriptDeliveries["delivery:automatic-part-1"] = TranscriptDeliveryRecord{
					ID: "delivery:automatic-part-1", SessionID: sessionID, OutboxID: "outbox:automatic-part-1",
					SourcePath: "/codex/multipart.jsonl", SourceLine: 20, SourceRecordID: "record-20",
					TextHash: normalizedStoreTestHash(body), PartIndex: 1, PartCount: 2,
					RenderedHash: bodyHash(body), Status: TranscriptDeliveryStatusQueued, CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed pending multipart delivery: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			_, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID: "outbox:explicit-pending", SessionID: sessionID, TurnID: "publish-history:" + sessionID,
					TeamsChatID: "multipart-explicit-chat", Kind: "publish-history-record-20", Body: body,
					PartIndex: 1, PartCount: 2, RenderedHash: bodyHash(body),
				},
				Delivery: TranscriptDeliveryRecord{
					ID: "delivery:explicit-pending", SessionID: sessionID, SourcePath: "/codex/multipart.jsonl",
					SourceLine: 20, SourceRecordID: "record-20", TextHash: normalizedStoreTestHash(body),
					PartIndex: 1, PartCount: 2, RenderedHash: bodyHash(body), Status: TranscriptDeliveryStatusQueued,
				},
			})
			if !errors.Is(err, ErrTranscriptDeliveryPending) || created || alreadyDelivered {
				t.Fatalf("explicit pending multipart queue created=%v alreadyDelivered=%v err=%v, want pending fence", created, alreadyDelivered, err)
			}

			if err := store.Update(ctx, func(state *State) error {
				automatic := state.OutboxMessages["outbox:automatic-part-1"]
				automatic.Status = OutboxStatusSent
				automatic.TeamsMessageID = "teams-part-1"
				state.OutboxMessages[automatic.ID] = automatic
				delivery := state.TranscriptDeliveries["delivery:automatic-part-1"]
				delivery.Status = TranscriptDeliveryStatusSent
				delivery.TeamsMessageID = "teams-part-1"
				state.TranscriptDeliveries[delivery.ID] = delivery
				return nil
			}); err != nil {
				t.Fatalf("settle first multipart part: %v", err)
			}
			mismatch, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID: "outbox:explicit-wrong-part", SessionID: sessionID, TurnID: "publish-history:" + sessionID,
					TeamsChatID: "multipart-explicit-chat", Kind: "publish-history-record-20-002", Body: "different second part",
					PartIndex: 2, PartCount: 2, RenderedHash: bodyHash("different second part"),
				},
				Delivery: TranscriptDeliveryRecord{
					ID: "delivery:explicit-wrong-part", SessionID: sessionID, SourcePath: "/codex/multipart.jsonl",
					SourceLine: 20, SourceRecordID: "record-20", TextHash: normalizedStoreTestHash(body),
					PartIndex: 2, PartCount: 2, RenderedHash: bodyHash("different second part"), Status: TranscriptDeliveryStatusQueued,
				},
			})
			if err != nil || !created || alreadyDelivered || mismatch.ID != "outbox:explicit-wrong-part" {
				t.Fatalf("explicit wrong multipart queue out=%#v created=%v alreadyDelivered=%v err=%v, want independent row", mismatch, created, alreadyDelivered, err)
			}
		})
	}
}

func normalizedStoreTestHash(text string) string {
	text = strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
	if text == "" {
		return ""
	}
	return bodyHash(text)
}

func assertNeedsAttentionCursorUnchanged(t *testing.T, store *Store, sessionID string) {
	t.Helper()
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load NeedsAttention cursor fixture: %v", err)
	}
	checkpoint, ok := state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)]
	if !ok || checkpoint.LastRecordID != "record-before" || checkpoint.LastSourceLine != 10 || checkpoint.LastOffset != 100 {
		t.Fatalf("NeedsAttention replay advanced checkpoint=%#v present=%v, want record-before/10/100", checkpoint, ok)
	}
	if delivery := state.TranscriptDeliveries["delivery:needs-attention-cursor"]; delivery.Status != TranscriptDeliveryStatusNeedsAttention {
		t.Fatalf("NeedsAttention replay changed delivery=%#v", delivery)
	}
}

func assertPermanentTranscriptFailureState(t *testing.T, store *Store, outboxID, deliveryID, helperID, artifactID string) {
	t.Helper()
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load permanent transcript failure: %v", err)
	}
	if got := state.OutboxMessages[outboxID].Status; got != OutboxStatusSkipped {
		t.Fatalf("outbox status=%q, want skipped", got)
	}
	delivery, ok := state.TranscriptDeliveries[deliveryID]
	if !ok || delivery.Status != TranscriptDeliveryStatusNeedsAttention || delivery.TeamsMessageID != "" {
		t.Fatalf("transcript delivery=%#v present=%v, want needs_attention without Teams id", delivery, ok)
	}
	if helper, ok := state.HelperDeliveries[helperID]; !ok || helper.Status != HelperDeliveryStatusFailed {
		t.Fatalf("helper delivery=%#v present=%v, want failed", helper, ok)
	}
	artifact, ok := state.ArtifactRecords[artifactID]
	if !ok || artifact.Status != "needs_attention" {
		t.Fatalf("artifact=%#v present=%v, want needs_attention", artifact, ok)
	}
	if got := state.OutboxMessages[outboxID].LastSendError; got == "" {
		t.Fatal("outbox lost permanent rejection diagnostic")
	}
	if !strings.Contains(strings.ToLower(state.ArtifactRecords[artifactID].Error), "permanent graph rejection") {
		t.Fatalf("artifact error=%q, want permanent rejection diagnostic", artifact.Error)
	}
}
