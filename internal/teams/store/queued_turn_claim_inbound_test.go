package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestClaimNextQueuedTurnWithInboundPreservesSingleTurnFencesAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			now := time.Now().UTC()
			const sessionID = "claim-with-inbound-session"
			const inboundID = "claim-with-inbound-event"
			const turnID = "claim-with-inbound-turn"
			inbound := InboundEvent{
				ID:             inboundID,
				SessionID:      sessionID,
				TeamsChatID:    "claim-with-inbound-chat",
				TeamsMessageID: "claim-with-inbound-message",
				TeamsBodyType:  "text",
				Text:           "durable claim input",
				Status:         InboundStatusQueued,
				TurnID:         turnID,
				CreatedAt:      now,
				UpdatedAt:      now,
			}
			if err := st.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: inbound.TeamsChatID}
				state.InboundEvents[inboundID] = inbound
				state.Turns[turnID] = Turn{
					ID:             turnID,
					SessionID:      sessionID,
					InboundEventID: inboundID,
					Status:         TurnStatusQueued,
					QueuedAt:       now,
					CreatedAt:      now,
					UpdatedAt:      now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}

			claimed, gotInbound, claimedOK, inboundFound, inboundRead, err := st.ClaimNextQueuedTurnWithInbound(ctx, sessionID)
			if err != nil {
				t.Fatalf("claim with inbound: %v", err)
			}
			if !claimedOK || !inboundFound || !inboundRead {
				t.Fatalf("claim flags = claimed:%v inbound:%v read:%v, want true/true/true", claimedOK, inboundFound, inboundRead)
			}
			if claimed.ID != turnID || claimed.Status != TurnStatusRunning {
				t.Fatalf("claimed turn = %#v, want running %q", claimed, turnID)
			}
			if gotInbound.ID != inbound.ID || gotInbound.SessionID != inbound.SessionID || gotInbound.TurnID != inbound.TurnID || gotInbound.Text != inbound.Text {
				t.Fatalf("claimed inbound = %#v, want %#v", gotInbound, inbound)
			}

			// The optimization must not change the existing per-session FIFO/running
			// fence: a second single-turn claim cannot skip the running row.
			if _, _, claimedAgain, inboundAgain, inboundReadAgain, err := st.ClaimNextQueuedTurnWithInbound(ctx, sessionID); err != nil {
				t.Fatalf("second claim with inbound: %v", err)
			} else if claimedAgain || inboundAgain || inboundReadAgain {
				t.Fatalf("second claim flags = claimed:%v inbound:%v read:%v, want false/false/false", claimedAgain, inboundAgain, inboundReadAgain)
			}
		})
	}
}

func TestClaimNextQueuedTurnWithInboundReportsMissingSnapshotAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			now := time.Now().UTC()
			const sessionID = "claim-missing-inbound-session"
			const inboundID = "claim-missing-inbound-event"
			const turnID = "claim-missing-inbound-turn"
			if err := st.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, InboundEventID: inboundID, Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			claimed, inbound, claimedOK, inboundFound, inboundRead, err := st.ClaimNextQueuedTurnWithInbound(ctx, sessionID)
			if err != nil {
				t.Fatalf("claim missing inbound: %v", err)
			}
			if !claimedOK || inbound.ID != "" || inboundFound || !inboundRead || claimed.Status != TurnStatusRunning {
				t.Fatalf("missing inbound result = turn:%#v inbound:%#v claimed:%v found:%v read:%v", claimed, inbound, claimedOK, inboundFound, inboundRead)
			}
		})
	}
}

func TestClaimNextQueuedTurnWithInboundCommitsClaimBeforeMalformedInboundError(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	now := time.Now().UTC()
	const sessionID = "claim-malformed-inbound-session"
	const inboundID = "claim-malformed-inbound-event"
	const turnID = "claim-malformed-inbound-turn"
	if err := st.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
		state.InboundEvents[inboundID] = InboundEvent{
			ID:        inboundID,
			SessionID: sessionID,
			Status:    InboundStatusQueued,
			TurnID:    turnID,
			CreatedAt: now,
			UpdatedAt: now,
		}
		state.Turns[turnID] = Turn{
			ID:             turnID,
			SessionID:      sessionID,
			InboundEventID: inboundID,
			Status:         TurnStatusQueued,
			QueuedAt:       now,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE inbound_events SET json = ? WHERE id = ?`, []byte(`{"broken"`), inboundID)
		return err
	})

	claimed, inbound, claimedOK, inboundFound, inboundRead, err := st.ClaimNextQueuedTurnWithInbound(ctx, sessionID)
	if err == nil {
		t.Fatal("claim with malformed inbound unexpectedly succeeded")
	}
	if !claimedOK || inbound.ID != "" || inboundFound || inboundRead || claimed.Status != TurnStatusRunning {
		t.Fatalf("malformed inbound result = turn:%#v inbound:%#v claimed:%v found:%v read:%v err:%v; want committed running claim with unread inbound", claimed, inbound, claimedOK, inboundFound, inboundRead, err)
	}

	// The durable running fence must survive the decode error. A retry must not
	// claim the same turn again and loop forever on the same malformed row.
	if _, _, claimedAgain, inboundAgain, inboundReadAgain, err := st.ClaimNextQueuedTurnWithInbound(ctx, sessionID); err != nil {
		t.Fatalf("reclaim after malformed inbound: %v", err)
	} else if claimedAgain || inboundAgain || inboundReadAgain {
		t.Fatalf("reclaim after malformed inbound = claimed:%v found:%v read:%v, want false/false/false", claimedAgain, inboundAgain, inboundReadAgain)
	}
}
