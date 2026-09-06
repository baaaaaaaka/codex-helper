package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestTeamsOperationalBacklogAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
			if err := st.Update(ctx, func(state *State) error {
				state.Turns["turn-active"] = Turn{ID: "turn-active", SessionID: "session-active", Status: TurnStatusRunning, CreatedAt: now}
				state.InboundEvents["inbound-pending"] = InboundEvent{ID: "inbound-pending", Status: InboundStatusPersisted, CreatedAt: now}
				state.InboundEvents["inbound-deferred"] = InboundEvent{ID: "inbound-deferred", Status: InboundStatusDeferred, CreatedAt: now}
				state.OutboxMessages["unrelated-outbox"] = OutboxMessage{ID: "unrelated-outbox", Status: OutboxStatusSent, CreatedAt: now}
				state.ChatPolls["chat-frontier"] = ChatPollState{
					ChatID:           "chat-frontier",
					ContinuationPath: "/chats/chat-frontier/messages?$skiptoken=durable",
					PendingPage:      &ChatPollPendingPage{},
					Gap:              &ChatPollGap{Kind: "test-gap"},
					UpdatedAt:        now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed operational backlog: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
				withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
					for i := 0; i < 1024; i++ {
						if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO outbox_messages (id, status, json) VALUES (?, ?, ?)`, fmt.Sprintf("cold-unrelated-outbox-%04d", i), string(OutboxStatusSent), `{"id":"cold"}`); err != nil {
							return err
						}
					}
					_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = '{broken-unrelated-outbox' WHERE id = ?`, "unrelated-outbox")
					return err
				})
			}
			clean := newTestStore(t)
			if err := clean.Update(ctx, func(*State) error { return nil }); err != nil {
				t.Fatalf("materialize clean store: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, clean)
			}

			var fullLoads int
			beforePollState, err := st.PollStateSnapshot(ctx)
			if err != nil {
				t.Fatalf("poll snapshot before backlog probe: %v", err)
			}
			previousHook := sqliteStateLoadTestHook
			sqliteStateLoadTestHook = func() { fullLoads++ }
			t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })
			got, err := st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("TeamsOperationalBacklog: %v", err)
			}
			if !got.ActiveTurns || !got.PendingInbound || !got.OperationalPollFrontier || !got.Active() {
				t.Fatalf("backlog = %#v, want all durable work lanes active", got)
			}
			if backend == "sqlite" && fullLoads != 0 {
				t.Fatalf("bounded SQLite backlog probe invoked full loader %d time(s)", fullLoads)
			}
			afterPollState, err := st.PollStateSnapshot(ctx)
			if err != nil {
				t.Fatalf("poll snapshot after backlog probe: %v", err)
			}
			if !reflect.DeepEqual(beforePollState.ChatPolls, afterPollState.ChatPolls) {
				t.Fatalf("backlog probe changed durable poll frontier: before=%#v after=%#v", beforePollState.ChatPolls, afterPollState.ChatPolls)
			}

			got, err = clean.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("TeamsOperationalBacklog on clean store: %v", err)
			}
			if got.Active() {
				t.Fatalf("clean-store backlog = %#v, want inactive", got)
			}
		})
	}
}

func TestOptionalMaintenanceDeferralIsDurableAndOwnerFenced(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	leaseUntil := time.Now().Add(time.Hour)
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "machine-a",
					Generation:      7,
					Status:          ControlLeaseStatusActive,
					LeaseUntil:      leaseUntil,
					LastHeartbeat:   now,
				}
				state.ServiceControl = ServiceControl{
					Paused:           true,
					Draining:         true,
					Reason:           "test-control",
					DrainOperationID: "drain-test",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed control lease: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}

			until := now.Add(2 * time.Second)
			control, err := st.SetOptionalMaintenanceDeferredForOwner(ctx, until, "teams backlog: active turn", "machine-a", 7)
			if err != nil {
				t.Fatalf("set optional maintenance deferral: %v", err)
			}
			if !control.OptionalMaintenanceDeferredUntil.Equal(until) || control.OptionalMaintenanceDeferredReason == "" {
				t.Fatalf("set control = %#v, want durable deadline/reason", control)
			}
			repeated, err := st.SetOptionalMaintenanceDeferredForOwner(ctx, until, "teams backlog: active turn", "machine-a", 7)
			if err != nil {
				t.Fatalf("repeat unchanged optional maintenance deferral: %v", err)
			}
			if !reflect.DeepEqual(repeated, control) {
				t.Fatalf("unchanged deferral changed control: first=%#v repeated=%#v", control, repeated)
			}
			if !control.Paused || !control.Draining || control.Reason != "test-control" || control.DrainOperationID != "drain-test" {
				t.Fatalf("optional deferral changed unrelated control fields: %#v", control)
			}
			if _, err := st.SetOptionalMaintenanceDeferredForOwner(ctx, until, "teams backlog: active turn", "machine-b", 8); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale set error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := st.ClearOptionalMaintenanceDeferredForOwner(ctx, "machine-b", 8); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale clear error = %v, want ErrControlLeaseNotHeld", err)
			}

			if err := st.Close(); err != nil {
				t.Fatalf("close store before reopen: %v", err)
			}
			reopened, err := Open(st.Path())
			if err != nil {
				t.Fatalf("reopen store: %v", err)
			}
			defer reopened.Close()
			control, err = reopened.ReadControl(ctx)
			if err != nil {
				t.Fatalf("read durable deferral after reopen: %v", err)
			}
			if !control.OptionalMaintenanceDeferredUntil.Equal(until) || control.OptionalMaintenanceDeferredReason != "teams backlog: active turn" {
				t.Fatalf("reopened control = %#v, want preserved deferral", control)
			}
			control, err = reopened.ClearOptionalMaintenanceDeferredForOwner(ctx, "machine-a", 7)
			if err != nil {
				t.Fatalf("clear optional maintenance deferral: %v", err)
			}
			if !control.OptionalMaintenanceDeferredUntil.IsZero() || control.OptionalMaintenanceDeferredReason != "" {
				t.Fatalf("cleared control = %#v, want no deferral", control)
			}
			if !control.Paused || !control.Draining || control.Reason != "test-control" || control.DrainOperationID != "drain-test" {
				t.Fatalf("clearing optional deferral changed unrelated control fields: %#v", control)
			}
		})
	}
}

func TestTeamsOperationalBacklogIgnoresQueuedInboundForTerminalTurn(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.Turns["turn-terminal"] = Turn{ID: "turn-terminal", Status: TurnStatusCompleted, CompletedAt: now}
				state.InboundEvents["inbound-terminal"] = InboundEvent{ID: "inbound-terminal", Status: InboundStatusQueued, TurnID: "turn-terminal", CreatedAt: now}
				state.InboundEvents["inbound-terminal-persisted"] = InboundEvent{ID: "inbound-terminal-persisted", Status: InboundStatusPersisted, TurnID: "turn-terminal", CreatedAt: now}
				state.InboundEvents["inbound-terminal-deferred"] = InboundEvent{ID: "inbound-terminal-deferred", Status: InboundStatusDeferred, TurnID: "turn-terminal", CreatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed terminal inbound ledger: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			backlog, err := st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("terminal queued inbound backlog probe: %v", err)
			}
			if backlog.Active() {
				t.Fatalf("terminal queued inbound = %#v, want inactive", backlog)
			}

			if err := st.Update(ctx, func(state *State) error {
				state.InboundEvents["inbound-orphan"] = InboundEvent{ID: "inbound-orphan", Status: InboundStatusQueued, CreatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed orphan queued inbound: %v", err)
			}
			backlog, err = st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("orphan queued inbound backlog probe: %v", err)
			}
			if !backlog.PendingInbound || !backlog.Active() {
				t.Fatalf("orphan queued inbound = %#v, want pending", backlog)
			}
		})
	}
}
