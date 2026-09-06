package store

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// The schedule batch is used after polling a bounded set of chats.  It must
// keep the owner fence and the per-row revision CAS even when one stale row
// makes the whole batch abort.  Run the same assertions against both storage
// backends because the JSON and SQLite implementations have different
// transaction machinery.
func TestUpdateChatPollSchedulesForOwnerFencesAndAppliesAtomicallyAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "machine-a",
					Generation:      7,
					Status:          ControlLeaseStatusActive,
					LeaseUntil:      now.Add(time.Hour),
					LastHeartbeat:   now,
				}
				state.ChatPolls["chat-a"] = ChatPollState{
					ChatID: "chat-a", PollState: chatPollStateWarm, PollRevision: 11,
					NextPollAt: now.Add(time.Hour), UpdatedAt: now,
				}
				state.ChatPolls["chat-b"] = ChatPollState{
					ChatID: "chat-b", PollState: chatPollStateCool, PollRevision: 19,
					NextPollAt: now.Add(time.Hour), UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed owner and polls: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}

			updates := []ChatPollScheduleUpdate{
				{
					ChatID:                  "chat-a",
					PollState:               chatPollStateHot,
					NextPollAt:              now,
					ExpectedPollRevision:    11,
					HasExpectedPollRevision: true,
				},
				{
					ChatID:                  "chat-b",
					PollState:               chatPollStateWarm,
					NextPollAt:              now,
					ExpectedPollRevision:    19,
					HasExpectedPollRevision: true,
				},
			}
			applied, err := store.UpdateChatPollSchedulesForOwner(ctx, updates, "machine-a", 7)
			if err != nil {
				t.Fatalf("owner schedule batch: %v", err)
			}
			if applied["chat-a"].PollState != chatPollStateHot || applied["chat-b"].PollState != chatPollStateWarm {
				t.Fatalf("owner schedule batch result = %#v", applied)
			}

			beforeA, ok, err := store.ChatPoll(ctx, "chat-a")
			if err != nil || !ok {
				t.Fatalf("load chat-a after first batch: ok=%v err=%v", ok, err)
			}
			beforeB, ok, err := store.ChatPoll(ctx, "chat-b")
			if err != nil || !ok {
				t.Fatalf("load chat-b after first batch: ok=%v err=%v", ok, err)
			}

			// chat-a is current, but chat-b is deliberately stale.  The valid
			// first update must not commit when the second update fails its CAS.
			_, err = store.UpdateChatPollSchedulesForOwner(ctx, []ChatPollScheduleUpdate{
				{
					ChatID:                  "chat-a",
					PollState:               chatPollStateCool,
					ExpectedPollRevision:    beforeA.PollRevision,
					HasExpectedPollRevision: true,
				},
				{
					ChatID:                  "chat-b",
					PollState:               chatPollStateCold,
					ExpectedPollRevision:    beforeB.PollRevision - 1,
					HasExpectedPollRevision: true,
				},
			}, "machine-a", 7)
			if !errors.Is(err, ErrChatPollRevisionChanged) {
				t.Fatalf("stale owner schedule batch error = %v, want ErrChatPollRevisionChanged", err)
			}
			afterA, ok, err := store.ChatPoll(ctx, "chat-a")
			if err != nil || !ok {
				t.Fatalf("load chat-a after stale batch: ok=%v err=%v", ok, err)
			}
			afterB, ok, err := store.ChatPoll(ctx, "chat-b")
			if err != nil || !ok {
				t.Fatalf("load chat-b after stale batch: ok=%v err=%v", ok, err)
			}
			if !reflect.DeepEqual(afterA, beforeA) || !reflect.DeepEqual(afterB, beforeB) {
				t.Fatalf("stale batch partially committed: chat-a=%#v (before %#v), chat-b=%#v (before %#v)", afterA, beforeA, afterB, beforeB)
			}

			// A takeover must reject the old owner before any row is touched.
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "machine-b",
					Generation:      8,
					Status:          ControlLeaseStatusActive,
					LeaseUntil:      now.Add(time.Hour),
					LastHeartbeat:   now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed takeover: %v", err)
			}
			_, err = store.UpdateChatPollSchedulesForOwner(ctx, []ChatPollScheduleUpdate{
				{ChatID: "chat-a", PollState: chatPollStateCold, ExpectedPollRevision: beforeA.PollRevision, HasExpectedPollRevision: true},
				{ChatID: "chat-b", PollState: chatPollStateCold, ExpectedPollRevision: beforeB.PollRevision, HasExpectedPollRevision: true},
			}, "machine-a", 7)
			if !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale owner takeover error = %v, want ErrControlLeaseNotHeld", err)
			}
			finalA, ok, err := store.ChatPoll(ctx, "chat-a")
			if err != nil || !ok {
				t.Fatalf("load chat-a after takeover rejection: ok=%v err=%v", ok, err)
			}
			finalB, ok, err := store.ChatPoll(ctx, "chat-b")
			if err != nil || !ok {
				t.Fatalf("load chat-b after takeover rejection: ok=%v err=%v", ok, err)
			}
			if !reflect.DeepEqual(finalA, beforeA) || !reflect.DeepEqual(finalB, beforeB) {
				t.Fatalf("takeover rejection changed polls: chat-a=%#v chat-b=%#v", finalA, finalB)
			}
		})
	}
}

func TestUpdateChatPollSchedulesForOwnerPreservesDuplicateOrderAndNoopAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "machine-a",
					Generation:      9,
					Status:          ControlLeaseStatusActive,
					LeaseUntil:      now.Add(time.Hour),
					LastHeartbeat:   now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed owner: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}

			applied, err := store.UpdateChatPollSchedulesForOwner(ctx, []ChatPollScheduleUpdate{
				{
					ChatID:                  "chat-order",
					PollState:               chatPollStateWarm,
					ExpectedPollRevision:    0,
					HasExpectedPollRevision: true,
				},
				{
					ChatID:                  "chat-order",
					PollState:               chatPollStateBlocked,
					BlockedUntil:            now.Add(time.Minute),
					ExpectedPollRevision:    1,
					HasExpectedPollRevision: true,
				},
			}, "machine-a", 9)
			if err != nil {
				t.Fatalf("duplicate owner schedule batch: %v", err)
			}
			poll := applied["chat-order"]
			if poll.PollState != chatPollStateBlocked || poll.PreviousPollState != chatPollStateWarm || poll.PollRevision != 2 {
				t.Fatalf("duplicate owner schedule result = %#v, want blocked after warm", poll)
			}

			before, ok, err := store.ChatPoll(ctx, "chat-order")
			if err != nil || !ok {
				t.Fatalf("load duplicate owner schedule: ok=%v err=%v", ok, err)
			}
			noOp, err := store.UpdateChatPollSchedulesForOwner(ctx, []ChatPollScheduleUpdate{{
				ChatID:                  "chat-order",
				PollState:               chatPollStateBlocked,
				BlockedUntil:            before.BlockedUntil,
				ExpectedPollRevision:    before.PollRevision,
				HasExpectedPollRevision: true,
			}}, "machine-a", 9)
			if err != nil {
				t.Fatalf("noop owner schedule batch: %v", err)
			}
			if !reflect.DeepEqual(noOp["chat-order"], before) {
				t.Fatalf("noop owner schedule changed poll: before=%#v after=%#v", before, noOp["chat-order"])
			}
		})
	}
}

func TestUpdateChatPollSchedulesForOwnerConcurrentCASAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "machine-concurrent",
					Generation:      13,
					Status:          ControlLeaseStatusActive,
					LeaseUntil:      now.Add(time.Hour),
					LastHeartbeat:   now,
				}
				state.ChatPolls["chat-concurrent-a"] = ChatPollState{
					ChatID: "chat-concurrent-a", PollState: chatPollStateWarm, PollRevision: 4,
					NextPollAt: now.Add(time.Hour), UpdatedAt: now,
				}
				state.ChatPolls["chat-concurrent-b"] = ChatPollState{
					ChatID: "chat-concurrent-b", PollState: chatPollStateCool, PollRevision: 9,
					NextPollAt: now.Add(time.Hour), UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed concurrent owner batch: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}

			batches := [][]ChatPollScheduleUpdate{
				{
					{ChatID: "chat-concurrent-a", PollState: chatPollStateHot, NextPollAt: now, ExpectedPollRevision: 4, HasExpectedPollRevision: true},
					{ChatID: "chat-concurrent-b", PollState: chatPollStateWarm, NextPollAt: now, ExpectedPollRevision: 9, HasExpectedPollRevision: true},
				},
				{
					{ChatID: "chat-concurrent-a", PollState: chatPollStateBlocked, BlockedUntil: now.Add(time.Minute), NextPollAt: now.Add(time.Minute), ExpectedPollRevision: 4, HasExpectedPollRevision: true},
					{ChatID: "chat-concurrent-b", PollState: chatPollStateCold, NextPollAt: now.Add(time.Minute), ExpectedPollRevision: 9, HasExpectedPollRevision: true},
				},
			}
			start := make(chan struct{})
			type result struct {
				index int
				err   error
			}
			results := make(chan result, len(batches))
			var wg sync.WaitGroup
			for index, updates := range batches {
				index, updates := index, updates
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, err := store.UpdateChatPollSchedulesForOwner(ctx, updates, "machine-concurrent", 13)
					results <- result{index: index, err: err}
				}()
			}
			close(start)
			wg.Wait()
			close(results)

			winner := -1
			stale := 0
			for result := range results {
				switch {
				case result.err == nil:
					if winner != -1 {
						t.Fatalf("both concurrent owner batches committed: winner=%d and=%d", winner, result.index)
					}
					winner = result.index
				case errors.Is(result.err, ErrChatPollRevisionChanged):
					stale++
				default:
					t.Fatalf("concurrent owner batch %d error = %v, want success or revision CAS rejection", result.index, result.err)
				}
			}
			if winner < 0 || stale != 1 {
				t.Fatalf("concurrent owner batch outcomes winner=%d stale=%d, want one each", winner, stale)
			}

			for _, chatID := range []string{"chat-concurrent-a", "chat-concurrent-b"} {
				poll, ok, err := store.ChatPoll(ctx, chatID)
				if err != nil || !ok {
					t.Fatalf("load concurrent result %s: ok=%v err=%v", chatID, ok, err)
				}
				var want ChatPollScheduleUpdate
				for _, update := range batches[winner] {
					if update.ChatID == chatID {
						want = update
						break
					}
				}
				if poll.PollState != want.PollState || poll.PollRevision != map[string]uint64{"chat-concurrent-a": 5, "chat-concurrent-b": 10}[chatID] {
					t.Fatalf("concurrent result %s = %#v, want winner state %q and one revision increment", chatID, poll, want.PollState)
				}
				if !poll.NextPollAt.Equal(want.NextPollAt) {
					t.Fatalf("concurrent result %s next poll = %s, want %s", chatID, poll.NextPollAt, want.NextPollAt)
				}
			}
		})
	}
}
