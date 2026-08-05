package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestBindCodexThreadForRunningTurnIsAtomicAndIdempotentAcrossBackends(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqlite], func(t *testing.T) {
			store := newTestStore(t)
			seedRunningThreadBindingState(t, store, false)
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			beforePointer, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read store pointer before bind: %v", err)
			}
			request := CodexThreadStartBindingRequest{
				SessionID:       "session-binding",
				TurnID:          "turn-binding",
				ThreadID:        "thread-binding",
				ModelGeneration: 4,
			}
			result, err := store.BindCodexThreadForRunningTurn(context.Background(), request)
			if err != nil {
				t.Fatalf("first bind: %v", err)
			}
			if !result.Changed || result.Session.CodexThreadID != request.ThreadID || result.Turn.CodexThreadID != request.ThreadID {
				t.Fatalf("first result = %#v", result)
			}
			afterFirst, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read store pointer after bind: %v", err)
			}
			if sqlite && !bytes.Equal(beforePointer, afterFirst) {
				t.Fatal("SQLite binding rewrote the pointer/state_json file")
			}
			second, err := store.BindCodexThreadForRunningTurn(context.Background(), request)
			if err != nil {
				t.Fatalf("idempotent bind: %v", err)
			}
			if second.Changed {
				t.Fatalf("idempotent bind reported a write: %#v", second)
			}
			if _, err := store.BindCodexThreadForRunningTurn(context.Background(), CodexThreadStartBindingRequest{
				SessionID:       request.SessionID,
				TurnID:          request.TurnID,
				ThreadID:        "different-thread",
				ModelGeneration: request.ModelGeneration,
			}); err == nil {
				t.Fatal("conflicting thread bind succeeded")
			} else {
				var conflict CodexThreadBindingConflictError
				if !errors.As(err, &conflict) {
					t.Fatalf("conflict error = %v, want CodexThreadBindingConflictError", err)
				}
			}
		})
	}
}

func TestBindCodexThreadForRunningTurnRejectsLifecycleAndGenerationChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*State)
	}{
		{name: "missing turn", mutate: func(state *State) { delete(state.Turns, "turn-binding") }},
		{name: "wrong session", mutate: func(state *State) {
			turn := state.Turns["turn-binding"]
			turn.SessionID = "other"
			state.Turns[turn.ID] = turn
		}},
		{name: "queued", mutate: func(state *State) {
			turn := state.Turns["turn-binding"]
			turn.Status = TurnStatusQueued
			state.Turns[turn.ID] = turn
		}},
		{name: "completed", mutate: func(state *State) {
			turn := state.Turns["turn-binding"]
			turn.Status = TurnStatusCompleted
			state.Turns[turn.ID] = turn
		}},
		{name: "generation", mutate: func(state *State) {
			turn := state.Turns["turn-binding"]
			turn.ModelGeneration = 5
			state.Turns[turn.ID] = turn
		}},
		{name: "closed session", mutate: func(state *State) {
			session := state.Sessions["session-binding"]
			session.Status = SessionStatusClosed
			state.Sessions[session.ID] = session
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			seedRunningThreadBindingState(t, store, false)
			if err := store.Update(context.Background(), func(state *State) error {
				tc.mutate(state)
				return nil
			}); err != nil {
				t.Fatalf("mutate fixture: %v", err)
			}
			_, err := store.BindCodexThreadForRunningTurn(context.Background(), CodexThreadStartBindingRequest{
				SessionID:       "session-binding",
				TurnID:          "turn-binding",
				ThreadID:        "thread-rejected",
				ModelGeneration: 4,
			})
			var fence *CodexThreadStartBindingFenceError
			if !errors.As(err, &fence) {
				t.Fatalf("error = %v, want fenced rejection", err)
			}
			state, err := store.Load(context.Background())
			if err != nil {
				t.Fatalf("load after rejection: %v", err)
			}
			if state.Sessions["session-binding"].CodexThreadID != "" || state.Turns["turn-binding"].CodexThreadID != "" {
				t.Fatal("rejected bind changed durable thread ids")
			}
		})
	}
}

func TestBindCodexThreadForRunningTurnFencesOwnerTakeover(t *testing.T) {
	started := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	owner := OwnerMetadata{PID: os.Getpid(), Hostname: "binding-host", ExecutablePath: "/opt/codex-helper", StartedAt: started, MachineID: "machine-binding", LeaseGeneration: 9, ActiveSessionID: "session-binding", ActiveTurnID: "turn-binding"}
	for _, sqlite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqlite], func(t *testing.T) {
			store := newTestStore(t)
			seedRunningThreadBindingState(t, store, false)
			if err := store.Update(context.Background(), func(state *State) error {
				state.ControlLease = ControlLease{HolderMachineID: owner.MachineID, Generation: owner.LeaseGeneration, LeaseUntil: time.Now().Add(time.Minute)}
				state.ServiceOwner = &owner
				turn := state.Turns["turn-binding"]
				turn.MachineID = owner.MachineID
				turn.LeaseGeneration = owner.LeaseGeneration
				state.Turns[turn.ID] = turn
				return nil
			}); err != nil {
				t.Fatalf("seed owner fence: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			request := CodexThreadStartBindingRequest{SessionID: "session-binding", TurnID: "turn-binding", ThreadID: "thread-owner", ModelGeneration: 4, MachineID: owner.MachineID, LeaseGeneration: owner.LeaseGeneration, Owner: owner}
			if _, err := store.BindCodexThreadForRunningTurn(context.Background(), request); err != nil {
				t.Fatalf("current owner bind: %v", err)
			}
			if err := store.Update(context.Background(), func(state *State) error {
				taken := owner
				taken.PID++
				state.ServiceOwner = &taken
				return nil
			}); err != nil {
				t.Fatalf("takeover fixture: %v", err)
			}
			_, err := store.BindCodexThreadForRunningTurn(context.Background(), CodexThreadStartBindingRequest{SessionID: "session-binding", TurnID: "turn-binding", ThreadID: "thread-owner", ModelGeneration: 4, MachineID: owner.MachineID, LeaseGeneration: owner.LeaseGeneration, Owner: owner})
			if !errors.Is(err, ErrCodexThreadStartBindingOwnerFence) {
				t.Fatalf("stale owner error = %v, want owner fence", err)
			}
		})
	}
}

func TestBindCodexThreadForRunningTurnConcurrentCallsAreIdempotent(t *testing.T) {
	store := newTestStore(t)
	seedRunningThreadBindingState(t, store, false)
	request := CodexThreadStartBindingRequest{SessionID: "session-binding", TurnID: "turn-binding", ThreadID: "thread-concurrent", ModelGeneration: 4}
	const calls = 8
	results := make(chan CodexThreadStartBindingResult, calls)
	errs := make(chan error, calls)
	var wg sync.WaitGroup
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.BindCodexThreadForRunningTurn(context.Background(), request)
			results <- result
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	changed := 0
	for result := range results {
		if result.Changed {
			changed++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent bind error: %v", err)
		}
	}
	if changed != 1 {
		t.Fatalf("concurrent Changed count = %d, want exactly one durable write", changed)
	}
}

func seedRunningThreadBindingState(t *testing.T, store *Store, _ bool) {
	t.Helper()
	if err := store.Update(context.Background(), func(state *State) error {
		state.Sessions["session-binding"] = SessionContext{ID: "session-binding", Status: SessionStatusActive, ModelGeneration: 4, TeamsChatID: "chat-binding"}
		state.Turns["turn-binding"] = Turn{ID: "turn-binding", SessionID: "session-binding", Status: TurnStatusRunning, ModelGeneration: 4, CreatedAt: time.Now()}
		return nil
	}); err != nil {
		t.Fatalf("seed running bind state: %v", err)
	}
}
