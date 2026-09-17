package store

import (
	"context"
	"testing"
	"time"
)

func TestTerminalIgnoredInboundClearsRetryMetadataAcrossLifecyclePaths(t *testing.T) {
	ctx := context.Background()
	paths := []struct {
		name string
		run  func(*testing.T, *Store, SessionContext, InboundEvent)
	}{
		{
			name: "interrupted turn",
			run: func(t *testing.T, store *Store, session SessionContext, inbound InboundEvent) {
				turn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
				if err != nil || !created {
					t.Fatalf("QueueTurn created=%v err=%v", created, err)
				}
				setInboundRetryMetadata(t, ctx, store, inbound.ID, InboundStatusQueued)
				if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "test interruption"); err != nil {
					t.Fatalf("MarkTurnInterrupted: %v", err)
				}
				assertIgnoredRetryMetadataCleared(t, ctx, store, inbound.ID)
			},
		},
		{
			name: "session quarantine",
			run: func(t *testing.T, store *Store, session SessionContext, inbound InboundEvent) {
				setInboundRetryMetadata(t, ctx, store, inbound.ID, InboundStatusPersisted)
				report, err := store.QuarantineSession(ctx, SessionQuarantineRequest{
					SessionID: session.ID,
					Reason:    "test quarantine",
					Source:    "test",
				})
				if err != nil || !report.Changed {
					t.Fatalf("QuarantineSession changed=%v err=%v", report.Changed, err)
				}
				assertIgnoredRetryMetadataCleared(t, ctx, store, inbound.ID)
			},
		},
		{
			name: "helper provenance suppression",
			run: func(t *testing.T, store *Store, session SessionContext, inbound InboundEvent) {
				turn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
				if err != nil || !created {
					t.Fatalf("QueueTurn created=%v err=%v", created, err)
				}
				setInboundRetryMetadata(t, ctx, store, inbound.ID, InboundStatusQueued)
				if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
					TeamsChatID:    session.TeamsChatID,
					TeamsMessageID: inbound.TeamsMessageID,
					Origin:         MessageOriginHelperOutbox,
					OutboxID:       "outbox:ignored-cleanup",
					TurnID:         turn.ID,
				}); err != nil {
					t.Fatalf("RecordMessageProvenance: %v", err)
				}
				assertIgnoredRetryMetadataCleared(t, ctx, store, inbound.ID)
			},
		},
	}

	for _, path := range paths {
		path := path
		t.Run(path.name, func(t *testing.T) {
			for _, useSQLite := range []bool{false, true} {
				useSQLite := useSQLite
				backend := "json"
				if useSQLite {
					backend = "sqlite"
				}
				t.Run(backend, func(t *testing.T) {
					store := newTestStore(t)
					if useSQLite {
						seedLegacyStateFileForSQLiteMigrationTest(t, store)
						migrateStoreToSQLiteForTest(t, store)
					}
					session := testSession()
					session.ID = "ignored-cleanup-session-" + path.name + "-" + backend
					session.TeamsChatID = "ignored-cleanup-chat-" + path.name + "-" + backend
					if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
						t.Fatalf("CreateSession created=%v err=%v", created, err)
					}
					inbound := InboundEvent{
						ID:             "inbound:ignored-cleanup:" + path.name + ":" + backend,
						SessionID:      session.ID,
						TeamsChatID:    session.TeamsChatID,
						TeamsMessageID: "message:ignored-cleanup:" + path.name + ":" + backend,
						Status:         InboundStatusPersisted,
					}
					if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
						t.Fatalf("PersistInbound created=%v err=%v", created, err)
					}
					path.run(t, store, session, inbound)
				})
			}
		})
	}
}

func TestRequeueTurnDoesNotReviveIgnoredInboundAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if useSQLite {
				seedLegacyStateFileForSQLiteMigrationTest(t, store)
				migrateStoreToSQLiteForTest(t, store)
			}
			session := testSession()
			session.ID = "requeue-ignored-session-" + backend
			session.TeamsChatID = "requeue-ignored-chat-" + backend
			if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			inbound := InboundEvent{
				ID:             "inbound:requeue-ignored:" + backend,
				SessionID:      session.ID,
				TeamsChatID:    session.TeamsChatID,
				TeamsMessageID: "message:requeue-ignored:" + backend,
				Status:         InboundStatusPersisted,
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("PersistInbound created=%v err=%v", created, err)
			}
			turn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
			if err != nil || !created {
				t.Fatalf("QueueTurn created=%v err=%v", created, err)
			}
			if _, err := store.MarkTurnRunning(ctx, turn.ID, "thread:requeue-ignored", "codex:requeue-ignored"); err != nil {
				t.Fatalf("MarkTurnRunning: %v", err)
			}
			if _, _, err := store.UpdateInboundEvent(ctx, inbound.ID, func(current InboundEvent, found bool, now time.Time) (InboundEvent, bool, error) {
				if !found {
					t.Fatalf("inbound missing while preparing ignored state")
				}
				current.Status = InboundStatusIgnored
				current.NextAttemptAt = now.Add(time.Hour)
				current.FailureCount = 4
				current.LastError = "terminal test evidence"
				return current, true, nil
			}); err != nil {
				t.Fatalf("mark inbound ignored: %v", err)
			}

			requeued, err := store.RequeueTurn(ctx, turn.ID)
			if err != nil || requeued.Status != TurnStatusInterrupted {
				t.Fatalf("RequeueTurn = %#v err=%v, want interrupted terminal turn", requeued, err)
			}
			fixedTurn, ok, err := store.TurnByID(ctx, turn.ID)
			if err != nil || !ok || fixedTurn.Status != TurnStatusInterrupted {
				t.Fatalf("durable requeued turn = %#v ok=%v err=%v", fixedTurn, ok, err)
			}
			fixedInbound, ok, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil || !ok {
				t.Fatalf("durable ignored inbound = %#v ok=%v err=%v", fixedInbound, ok, err)
			}
			if fixedInbound.Status != InboundStatusIgnored || !fixedInbound.NextAttemptAt.IsZero() || fixedInbound.FailureCount != 0 || fixedInbound.LastError != "" {
				t.Fatalf("RequeueTurn revived or retained retry metadata: %#v", fixedInbound)
			}
		})
	}
}

func TestInterruptedTurnDoesNotIgnoreForeignInboundAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if useSQLite {
				seedLegacyStateFileForSQLiteMigrationTest(t, store)
				migrateStoreToSQLiteForTest(t, store)
			}
			sessionA := testSession()
			sessionA.ID = "interrupt-foreign-session-a-" + backend
			sessionA.TeamsChatID = "interrupt-foreign-chat-a-" + backend
			sessionB := testSession()
			sessionB.ID = "interrupt-foreign-session-b-" + backend
			sessionB.TeamsChatID = "interrupt-foreign-chat-b-" + backend
			for _, session := range []SessionContext{sessionA, sessionB} {
				if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
					t.Fatalf("CreateSession %q created=%v err=%v", session.ID, created, err)
				}
			}
			inbound := InboundEvent{
				ID:             "inbound:interrupt-foreign:" + backend,
				SessionID:      sessionB.ID,
				TeamsChatID:    sessionB.TeamsChatID,
				TeamsMessageID: "message:interrupt-foreign:" + backend,
				Status:         InboundStatusQueued,
				TurnID:         "turn:interrupt-foreign:" + backend,
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("PersistInbound created=%v err=%v", created, err)
			}
			turn := Turn{
				ID:             inbound.TurnID,
				SessionID:      sessionA.ID,
				InboundEventID: inbound.ID,
				Status:         TurnStatusRunning,
				StartedAt:      time.Now(),
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Turns[turn.ID] = turn
				return nil
			}); err != nil {
				t.Fatalf("seed foreign turn: %v", err)
			}
			if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "foreign inbound test"); err != nil {
				t.Fatalf("MarkTurnInterrupted: %v", err)
			}
			fixedInbound, ok, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil || !ok {
				t.Fatalf("foreign inbound after interruption = %#v ok=%v err=%v", fixedInbound, ok, err)
			}
			if fixedInbound.Status != InboundStatusQueued || fixedInbound.TurnID != turn.ID {
				t.Fatalf("interruption mutated foreign inbound: %#v", fixedInbound)
			}
		})
	}
}

func setInboundRetryMetadata(t *testing.T, ctx context.Context, store *Store, inboundID string, status InboundStatus) {
	t.Helper()
	if _, _, err := store.UpdateInboundEvent(ctx, inboundID, func(current InboundEvent, found bool, now time.Time) (InboundEvent, bool, error) {
		if !found {
			t.Fatalf("inbound %q not found", inboundID)
		}
		current.Status = status
		current.NextAttemptAt = now.Add(time.Hour)
		current.FailureCount = 5
		current.LastError = "stale retry metadata"
		return current, true, nil
	}); err != nil {
		t.Fatalf("set retry metadata: %v", err)
	}
}

func assertIgnoredRetryMetadataCleared(t *testing.T, ctx context.Context, store *Store, inboundID string) {
	t.Helper()
	inbound, ok, err := store.InboundEventByID(ctx, inboundID)
	if err != nil || !ok {
		t.Fatalf("InboundEventByID ok=%v err=%v", ok, err)
	}
	if inbound.Status != InboundStatusIgnored || !inbound.NextAttemptAt.IsZero() || inbound.FailureCount != 0 || inbound.LastError != "" {
		t.Fatalf("terminal inbound = %#v, want ignored with retry metadata cleared", inbound)
	}
}
