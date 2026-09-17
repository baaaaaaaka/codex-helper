package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestJSONOutboxFIFOSnapshotRejectsPredecessorMutationBeforeClaim(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	predecessor := OutboxMessage{
		ID: "outbox:json-proof-predecessor", TeamsChatID: "chat:json-proof",
		Kind: "helper", Body: "old", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	target := OutboxMessage{
		ID: "outbox:json-proof-target", TeamsChatID: predecessor.TeamsChatID,
		Kind: "final", Body: "target", Status: OutboxStatusQueued,
		Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[predecessor.ID] = predecessor
		state.OutboxMessages[target.ID] = target
		return nil
	}); err != nil {
		t.Fatalf("seed JSON FIFO proof fixture: %v", err)
	}
	loaded, err := store.OutboxMessageByID(ctx, target.ID)
	if err != nil {
		t.Fatalf("load target: %v", err)
	}
	_, found, proof, err := store.EarlierUnsentOutboxWithProof(ctx, loaded)
	if err != nil {
		t.Fatalf("FIFO lookup: %v", err)
	}
	if !found || proof == nil || !proof.jsonBackend {
		t.Fatalf("FIFO lookup found=%v proof=%#v, want JSON proof with predecessor", found, proof)
	}

	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open peer JSON store: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	previousHook := outboxFIFOSnapshotClaimTestHook
	outboxFIFOSnapshotClaimTestHook = func() {
		err := peer.Update(ctx, func(state *State) error {
			changed := state.OutboxMessages[predecessor.ID]
			changed.Body = "mutated after lookup"
			state.OutboxMessages[predecessor.ID] = changed
			return nil
		})
		if err != nil {
			t.Errorf("mutate predecessor in claim barrier: %v", err)
		}
	}
	t.Cleanup(func() { outboxFIFOSnapshotClaimTestHook = previousHook })

	if _, err := store.MarkOutboxSendAttemptWithFIFOSnapshotProof(ctx, loaded.ID, proof); !errors.Is(err, ErrOutboxPredecessorIndeterminate) {
		t.Fatalf("claim after predecessor mutation error = %v, want ErrOutboxPredecessorIndeterminate", err)
	}
	current, err := store.OutboxMessageByID(ctx, loaded.ID)
	if err != nil {
		t.Fatalf("reload target after rejected claim: %v", err)
	}
	if current.Status != OutboxStatusQueued || current.SendAttemptToken != "" {
		t.Fatalf("target after rejected claim = %#v, want unchanged queued row", current)
	}
}
