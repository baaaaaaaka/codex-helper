package teams

import (
	"context"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestRecoverQueuedTurnStopsBeforeGraphOnForeignInboundProvenance(t *testing.T) {
	ctx := context.WithValue(context.Background(), workPollQueueOnlyContextKey{}, true)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	session := &Session{ID: "recovery-provenance-session", ChatID: "recovery-provenance-chat"}
	turn := teamstore.Turn{
		ID:             "turn:recovery-provenance",
		SessionID:      session.ID,
		InboundEventID: "inbound:recovery-provenance",
		Status:         teamstore.TurnStatusQueued,
		CreatedAt:      time.Now(),
	}
	inbound := teamstore.InboundEvent{
		ID:             turn.InboundEventID,
		SessionID:      "foreign-session",
		TeamsChatID:    "foreign-chat",
		TeamsMessageID: "message:recovery-provenance",
		Status:         teamstore.InboundStatusQueued,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Sessions[session.ID] = teamstore.SessionContext{ID: session.ID, TeamsChatID: session.ChatID, Status: teamstore.SessionStatusActive}
		state.Turns[turn.ID] = turn
		state.InboundEvents[inbound.ID] = inbound
		return nil
	}); err != nil {
		t.Fatalf("seed provenance fixture: %v", err)
	}
	state := teamstore.State{
		Turns:         map[string]teamstore.Turn{turn.ID: turn},
		InboundEvents: map[string]teamstore.InboundEvent{inbound.ID: inbound},
	}
	if err := bridge.recoverQueuedTurn(ctx, session, turn, state); err != nil {
		t.Fatalf("recoverQueuedTurn: %v", err)
	}
	current, ok, err := store.TurnByID(ctx, turn.ID)
	if err != nil || !ok {
		t.Fatalf("recovered turn = %#v ok=%v err=%v", current, ok, err)
	}
	if current.Status != teamstore.TurnStatusInterrupted {
		t.Fatalf("foreign-provenance turn status = %q, want interrupted", current.Status)
	}
	foreign, ok, err := store.InboundEventByID(ctx, inbound.ID)
	if err != nil || !ok {
		t.Fatalf("foreign inbound = %#v ok=%v err=%v", foreign, ok, err)
	}
	if foreign.Status != teamstore.InboundStatusQueued || foreign.SessionID != "foreign-session" {
		t.Fatalf("foreign inbound was mutated: %#v", foreign)
	}
	stateAfter, err := store.OutboxStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("OutboxStateSnapshot: %v", err)
	}
	foundNotice := false
	for _, msg := range stateAfter.OutboxMessages {
		if msg.TurnID == turn.ID && strings.TrimSpace(msg.Kind) == "recovery-provenance" && msg.Status == teamstore.OutboxStatusQueued {
			foundNotice = true
		}
	}
	if !foundNotice {
		t.Fatalf("missing queued provenance notice: %#v", stateAfter.OutboxMessages)
	}
}
