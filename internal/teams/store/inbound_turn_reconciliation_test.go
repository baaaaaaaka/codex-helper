package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestQueueTurnReconcilesExistingDeferredInboundAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			if useSQLite {
				seedLegacyStateFileForSQLiteMigrationTest(t, store)
				migrateStoreToSQLiteForTest(t, store)
			}
			session := testSession()
			session.ID = "queue-reconcile-session-" + name
			session.TeamsChatID = "queue-reconcile-chat-" + name
			if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			inbound := InboundEvent{
				ID:             "inbound:queue-reconcile:" + name,
				SessionID:      session.ID,
				TeamsChatID:    session.TeamsChatID,
				TeamsMessageID: "message-queue-reconcile-" + name,
				Text:           "reconcile me",
				Status:         InboundStatusDeferred,
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("PersistInbound created=%v err=%v", created, err)
			}
			queued, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
			if err != nil || !created {
				t.Fatalf("initial QueueTurn created=%v err=%v", created, err)
			}
			// Model the crash window: the Turn is durable, but an older writer
			// left the inbound row deferred with retry evidence.
			if _, _, err := store.UpdateInboundEvent(ctx, inbound.ID, func(current InboundEvent, found bool, now time.Time) (InboundEvent, bool, error) {
				if !found {
					t.Fatalf("inbound disappeared before crash-window setup")
				}
				current.Status = InboundStatusDeferred
				current.NextAttemptAt = now.Add(time.Hour)
				current.FailureCount = 7
				current.LastError = "synthetic retry evidence"
				return current, true, nil
			}); err != nil {
				t.Fatalf("prepare deferred crash window: %v", err)
			}

			replayed, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
			if err != nil || created || replayed.ID != queued.ID {
				t.Fatalf("idempotent QueueTurn = %#v created=%v err=%v, want existing turn", replayed, created, err)
			}
			fixed, ok, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil || !ok {
				t.Fatalf("reconciled inbound = %#v ok=%v err=%v", fixed, ok, err)
			}
			if fixed.Status != InboundStatusQueued || fixed.TurnID != queued.ID || !fixed.NextAttemptAt.IsZero() || fixed.FailureCount != 0 || fixed.LastError != "" {
				t.Fatalf("reconciled inbound = %#v, want queued with retry metadata cleared", fixed)
			}
			candidates, err := store.InboundRecoveryCandidates(ctx)
			if err != nil {
				t.Fatalf("InboundRecoveryCandidates: %v", err)
			}
			for _, candidate := range candidates {
				if candidate.ID == inbound.ID {
					t.Fatalf("reconciled inbound remained a recovery candidate: %#v", candidate)
				}
			}
		})
	}
}

func TestQueueTurnAlreadyReconciledDuplicateIsReadOnlyAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			if useSQLite {
				seedLegacyStateFileForSQLiteMigrationTest(t, store)
				migrateStoreToSQLiteForTest(t, store)
			}
			session := testSession()
			session.ID = "queue-noop-session-" + name
			session.TeamsChatID = "queue-noop-chat-" + name
			if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			inbound := InboundEvent{
				ID:             "inbound:queue-noop:" + name,
				SessionID:      session.ID,
				TeamsChatID:    session.TeamsChatID,
				TeamsMessageID: "message-queue-noop-" + name,
				Status:         InboundStatusPersisted,
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("PersistInbound created=%v err=%v", created, err)
			}
			first, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
			if err != nil || !created {
				t.Fatalf("initial QueueTurn = %#v created=%v err=%v", first, created, err)
			}
			second, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
			if err != nil || created || second.ID != first.ID {
				t.Fatalf("duplicate QueueTurn = %#v created=%v err=%v, want read-only existing turn", second, created, err)
			}
			fixed, ok, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil || !ok {
				t.Fatalf("inbound after duplicate = %#v ok=%v err=%v", fixed, ok, err)
			}
			if fixed.Status != InboundStatusQueued || fixed.TurnID != first.ID || !fixed.NextAttemptAt.IsZero() || fixed.FailureCount != 0 || fixed.LastError != "" {
				t.Fatalf("duplicate changed reconciled inbound = %#v", fixed)
			}
		})
	}
}

func TestQueueTurnFindsLegacyTurnByInboundEventIDWithoutCreatingDuplicate(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			if useSQLite {
				seedLegacyStateFileForSQLiteMigrationTest(t, store)
				migrateStoreToSQLiteForTest(t, store)
			}
			session := testSession()
			session.ID = "legacy-turn-session-" + name
			session.TeamsChatID = "legacy-turn-chat-" + name
			if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			inbound := InboundEvent{
				ID:             "inbound:legacy-turn:" + name,
				SessionID:      session.ID,
				TeamsChatID:    session.TeamsChatID,
				TeamsMessageID: "message-legacy-turn-" + name,
				Text:           "legacy turn",
				Status:         InboundStatusDeferred,
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("PersistInbound created=%v err=%v", created, err)
			}
			const legacyTurnID = "turn:legacy-nondeterministic"
			if err := store.Update(ctx, func(state *State) error {
				state.Turns[legacyTurnID] = Turn{
					ID:             legacyTurnID,
					SessionID:      session.ID,
					InboundEventID: inbound.ID,
					Status:         TurnStatusQueued,
					CreatedAt:      time.Now().UTC(),
					QueuedAt:       time.Now().UTC(),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed legacy turn: %v", err)
			}

			queued, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
			if err != nil || created || queued.ID != legacyTurnID {
				t.Fatalf("legacy QueueTurn = %#v created=%v err=%v, want existing turn", queued, created, err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after legacy reconciliation: %v", err)
			}
			if len(state.Turns) != 1 {
				t.Fatalf("turn count = %d, want one legacy turn", len(state.Turns))
			}
			fixed := state.InboundEvents[inbound.ID]
			if fixed.Status != InboundStatusQueued || fixed.TurnID != legacyTurnID {
				t.Fatalf("legacy inbound after reconciliation = %#v", fixed)
			}
		})
	}
}

func TestQueueTurnFailsClosedWhenInboundHasConflictingLegacyTurns(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	session := testSession()
	session.ID = "conflicting-turn-session"
	session.TeamsChatID = "conflicting-turn-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	inbound := InboundEvent{ID: "inbound:conflict", SessionID: session.ID, TeamsChatID: session.TeamsChatID, TeamsMessageID: "message-conflict", Status: InboundStatusDeferred}
	if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
		t.Fatalf("PersistInbound created=%v err=%v", created, err)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Turns["turn:conflict-a"] = Turn{ID: "turn:conflict-a", SessionID: session.ID, InboundEventID: inbound.ID, Status: TurnStatusQueued}
		state.Turns["turn:conflict-b"] = Turn{ID: "turn:conflict-b", SessionID: session.ID, InboundEventID: inbound.ID, Status: TurnStatusQueued}
		return nil
	}); err != nil {
		t.Fatalf("seed conflicting turns: %v", err)
	}
	if _, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID}); !errors.Is(err, ErrInboundTurnConflict) || created {
		t.Fatalf("conflicting QueueTurn created=%v err=%v, want ErrInboundTurnConflict", created, err)
	}
}

func TestQueueTurnRejectsForeignInboundAndTurnAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			if useSQLite {
				seedLegacyStateFileForSQLiteMigrationTest(t, store)
				migrateStoreToSQLiteForTest(t, store)
			}
			requested := testSession()
			requested.ID = "queue-foreign-requested-" + name
			requested.TeamsChatID = "queue-foreign-requested-chat-" + name
			foreign := testSession()
			foreign.ID = "queue-foreign-owner-" + name
			foreign.TeamsChatID = "queue-foreign-owner-chat-" + name
			for _, session := range []SessionContext{requested, foreign} {
				if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
					t.Fatalf("CreateSession %q created=%v err=%v", session.ID, created, err)
				}
			}
			inbound := InboundEvent{
				ID:             "inbound:queue-foreign:" + name,
				SessionID:      foreign.ID,
				TeamsChatID:    foreign.TeamsChatID,
				TeamsMessageID: "message-queue-foreign-" + name,
				Status:         InboundStatusPersisted,
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("PersistInbound created=%v err=%v", created, err)
			}
			if _, created, err := store.QueueTurn(ctx, Turn{SessionID: requested.ID, InboundEventID: inbound.ID}); !errors.Is(err, ErrSessionStateProvenanceMismatch) || created {
				t.Fatalf("foreign inbound QueueTurn created=%v err=%v, want fenced mismatch", created, err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after foreign inbound rejection: %v", err)
			}
			if len(state.Turns) != 0 || state.InboundEvents[inbound.ID].TurnID != "" || state.InboundEvents[inbound.ID].Status != InboundStatusPersisted {
				t.Fatalf("foreign inbound was mutated: turns=%#v inbound=%#v", state.Turns, state.InboundEvents[inbound.ID])
			}

			const foreignTurnID = "turn:queue-foreign-legacy"
			if err := store.Update(ctx, func(state *State) error {
				state.Turns[foreignTurnID] = Turn{ID: foreignTurnID, SessionID: foreign.ID, InboundEventID: inbound.ID, Status: TurnStatusQueued}
				return nil
			}); err != nil {
				t.Fatalf("seed foreign legacy turn: %v", err)
			}
			if _, created, err := store.QueueTurn(ctx, Turn{SessionID: requested.ID, InboundEventID: inbound.ID}); !errors.Is(err, ErrSessionStateProvenanceMismatch) || created {
				t.Fatalf("foreign legacy Turn QueueTurn created=%v err=%v, want fenced mismatch", created, err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after foreign legacy rejection: %v", err)
			}
			if len(state.Turns) != 1 || state.Turns[foreignTurnID].SessionID != foreign.ID || state.InboundEvents[inbound.ID].TurnID != "" {
				t.Fatalf("foreign legacy state was mutated: turns=%#v inbound=%#v", state.Turns, state.InboundEvents[inbound.ID])
			}
		})
	}
}

func TestQueueTurnRejectsIgnoredInboundWithoutRevivingAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			if useSQLite {
				seedLegacyStateFileForSQLiteMigrationTest(t, store)
				migrateStoreToSQLiteForTest(t, store)
			}
			session := testSession()
			session.ID = "queue-ignored-session-" + name
			session.TeamsChatID = "queue-ignored-chat-" + name
			if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			inbound := InboundEvent{
				ID:             "inbound:queue-ignored:" + name,
				SessionID:      session.ID,
				TeamsChatID:    session.TeamsChatID,
				TeamsMessageID: "message-queue-ignored-" + name,
				Status:         InboundStatusIgnored,
				NextAttemptAt:  time.Now().Add(time.Hour),
				FailureCount:   9,
				LastError:      "must remain terminal",
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("PersistInbound created=%v err=%v", created, err)
			}
			if _, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID}); !errors.Is(err, ErrInboundIgnored) || created {
				t.Fatalf("ignored QueueTurn created=%v err=%v, want terminal rejection", created, err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after ignored rejection: %v", err)
			}
			fixed := state.InboundEvents[inbound.ID]
			if len(state.Turns) != 0 || fixed.Status != InboundStatusIgnored || fixed.TurnID != "" || fixed.FailureCount != 9 || fixed.LastError != "must remain terminal" || fixed.NextAttemptAt.IsZero() {
				t.Fatalf("ignored inbound changed: turns=%#v inbound=%#v", state.Turns, fixed)
			}
		})
	}
}

func TestQueueTurnRejectsMissingInboundWithoutCreatingOrphanAcrossBackends(t *testing.T) {
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
			session.ID = "queue-missing-inbound-session-" + backend
			session.TeamsChatID = "queue-missing-inbound-chat-" + backend
			if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			const missingInboundID = "inbound:queue-missing-inbound"
			turn, created, err := store.QueueTurn(ctx, Turn{
				ID:             "turn:queue-missing-inbound-" + backend,
				SessionID:      session.ID,
				InboundEventID: missingInboundID,
			})
			if !errors.Is(err, ErrInboundNotFound) || created || turn.ID != "" {
				t.Fatalf("missing inbound QueueTurn = %#v created=%v err=%v, want no orphan", turn, created, err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after missing inbound rejection: %v", err)
			}
			if len(state.Turns) != 0 {
				t.Fatalf("missing inbound created orphan turns: %#v", state.Turns)
			}
		})
	}
}
