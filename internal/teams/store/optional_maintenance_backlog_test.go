package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
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
				for i := 0; i < 1024; i++ {
					id := fmt.Sprintf("cold-unrelated-outbox-%04d", i)
					state.OutboxMessages[id] = OutboxMessage{ID: id, Status: OutboxStatusSent, CreatedAt: now}
				}
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
			previousJSONFullLoadHook := loadUnlockedTestHook
			loadUnlockedTestHook = func() { fullLoads++ }
			t.Cleanup(func() { loadUnlockedTestHook = previousJSONFullLoadHook })
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
			if backend == "json" && fullLoads != 0 {
				t.Fatalf("bounded JSON backlog probe invoked full loader %d time(s)", fullLoads)
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

func TestMissingSessionActiveTurnDoesNotKeepRecoveryDueAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.Turns["turn:missing-session"] = Turn{
					ID: "turn:missing-session", Status: TurnStatusQueued,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed malformed active turn: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			hasUnfinished, err := st.HasUnfinishedTurns(ctx)
			if err != nil {
				t.Fatalf("HasUnfinishedTurns: %v", err)
			}
			if hasUnfinished {
				t.Fatal("active turn without a session kept startup recovery due")
			}
			hasQueued, err := st.HasQueuedTurns(ctx)
			if err != nil {
				t.Fatalf("HasQueuedTurns: %v", err)
			}
			if hasQueued {
				t.Fatal("queued turn without a session kept queued-turn processing due")
			}
			loaded, err := st.Load(ctx)
			if err != nil {
				t.Fatalf("Load malformed active turn: %v", err)
			}
			if _, ok := loaded.Turns["turn:missing-session"]; !ok {
				t.Fatal("malformed active turn was dropped from the durable diagnostic projection")
			}
			backlog, err := st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("TeamsOperationalBacklog: %v", err)
			}
			if backlog.ActiveTurns {
				t.Fatalf("malformed active turn kept operational backlog due: %#v", backlog)
			}
		})
	}
}

func TestHistoryWatchStateUsesNarrowProjectionAcrossBackends(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			const checkpointID = "history:narrow"
			if err := st.Update(ctx, func(state *State) error {
				state.HistoryWatch[checkpointID] = HistoryWatchCheckpoint{
					ID: checkpointID, Path: "/tmp/narrow.jsonl", Size: 12,
					Offset: 12, UpdatedAt: now,
				}
				state.HistoryWatchReady = now
				return nil
			}); err != nil {
				t.Fatalf("seed history-watch state: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			fullLoads := 0
			previousSQLiteHook := sqliteStateLoadTestHook
			previousJSONHook := loadUnlockedTestHook
			sqliteStateLoadTestHook = func() { fullLoads++ }
			loadUnlockedTestHook = func() { fullLoads++ }
			t.Cleanup(func() {
				sqliteStateLoadTestHook = previousSQLiteHook
				loadUnlockedTestHook = previousJSONHook
			})

			state, err := st.HistoryWatchState(ctx)
			if err != nil {
				t.Fatalf("HistoryWatchState: %v", err)
			}
			checkpoint, ok := state.HistoryWatch[checkpointID]
			if !ok || checkpoint.Path != "/tmp/narrow.jsonl" || !state.HistoryWatchReady.Equal(now) {
				t.Fatalf("narrow history state = %#v ready=%s", state.HistoryWatch, state.HistoryWatchReady)
			}
			if fullLoads != 0 {
				t.Fatalf("HistoryWatchState invoked a full loader %d time(s)", fullLoads)
			}
		})
	}
}

func TestPendingOutboxDistinctChatKeysetOrderMatchesAcrossBackends(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			created := map[string]time.Time{
				"chat-a": now.Add(4 * time.Hour),
				"chat-b": now.Add(3 * time.Hour),
				"chat-c": now.Add(2 * time.Hour),
				"chat-d": now.Add(1 * time.Hour),
				"chat-e": now,
			}
			if err := st.Update(ctx, func(state *State) error {
				for chatID, at := range created {
					id := "outbox:keyset:" + chatID
					state.OutboxMessages[id] = OutboxMessage{
						ID: id, TeamsChatID: chatID, Kind: "helper", Body: chatID,
						Status: OutboxStatusQueued, Sequence: 1, CreatedAt: at, UpdatedAt: at,
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed keyset rows: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			got, err := st.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{
				Now: now, AfterChatID: "chat-b",
			}, 2)
			if err != nil {
				t.Fatalf("PendingOutboxChatIDsAt: %v", err)
			}
			want := []string{"chat-c", "chat-d"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("keyset chat order = %v, want %v", got, want)
			}
		})
	}
}

func TestPendingOutboxDistinctChatKeysetInitialOrderMatchesContinuation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			// Deliberately make age order disagree with lexical chat order. The
			// initial page and its AfterChatID continuation must still cover every
			// eligible chat exactly once.
			created := map[string]time.Time{
				"chat-z": now.Add(-4 * time.Hour),
				"chat-m": now.Add(-3 * time.Hour),
				"chat-a": now.Add(-2 * time.Hour),
				"chat-b": now.Add(-time.Hour),
			}
			if err := st.Update(ctx, func(state *State) error {
				for chatID, at := range created {
					id := "outbox:initial-keyset:" + chatID
					state.OutboxMessages[id] = OutboxMessage{
						ID: id, TeamsChatID: chatID, Kind: "helper", Body: chatID,
						Status: OutboxStatusQueued, Sequence: 1, CreatedAt: at, UpdatedAt: at,
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed initial keyset rows: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}

			first, err := st.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 2)
			if err != nil {
				t.Fatalf("initial PendingOutboxChatIDsAt: %v", err)
			}
			if want := []string{"chat-a", "chat-b"}; !reflect.DeepEqual(first, want) {
				t.Fatalf("initial chat keyset page = %v, want %v", first, want)
			}
			second, err := st.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{
				Now: now, AfterChatID: first[len(first)-1],
			}, 2)
			if err != nil {
				t.Fatalf("continuation PendingOutboxChatIDsAt: %v", err)
			}
			if want := []string{"chat-m", "chat-z"}; !reflect.DeepEqual(second, want) {
				t.Fatalf("continuation chat keyset page = %v, want %v", second, want)
			}
		})
	}
}

func TestSQLitePendingOutboxUsesCanonicalJSONScheduleWhenScalarIsStale(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	st := newTestStore(t)
	if err := st.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:stale-schedule"] = OutboxMessage{
			ID: "outbox:stale-schedule", TeamsChatID: "chat-stale-schedule", Kind: "helper", Body: "due in JSON",
			Status: OutboxStatusQueued, CreatedAt: now, UpdatedAt: now,
			NextAttemptAt: now.Add(time.Hour),
		}
		state.OutboxMessages["outbox:stale-schedule-tail"] = OutboxMessage{
			ID: "outbox:stale-schedule-tail", TeamsChatID: "chat-stale-schedule-tail", Kind: "helper", Body: "healthy tail",
			Status: OutboxStatusQueued, CreatedAt: now.Add(time.Second), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale schedule rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	msg, err := st.OutboxMessageByID(ctx, "outbox:stale-schedule")
	if err != nil {
		t.Fatalf("load stale schedule row: %v", err)
	}
	msg.NextAttemptAt = now.Add(-time.Minute)
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal canonical due row: %v", err)
	}
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		// Leave deliver_after at the old future scalar while making the canonical
		// JSON schedule due. Admission must follow the JSON, not hide this row.
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, raw, msg.ID)
		return err
	})
	page, err := st.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 1})
	if err != nil {
		t.Fatalf("PendingOutboxPageAt with stale scalar schedule: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != msg.ID {
		t.Fatalf("pending page = %#v, want canonical due row %q", page.Messages, msg.ID)
	}
	chatIDs, err := st.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 1)
	if err != nil {
		t.Fatalf("PendingOutboxChatIDsAt with stale scalar schedule: %v", err)
	}
	if !reflect.DeepEqual(chatIDs, []string{msg.TeamsChatID}) {
		t.Fatalf("pending chat IDs = %v, want %q", chatIDs, msg.TeamsChatID)
	}
}

func TestSQLitePendingOutboxUsesLegacyScalarScheduleWhenJSONFieldIsAbsent(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	st := newTestStore(t)
	legacyID := "outbox:legacy-future-schedule"
	healthyID := "outbox:legacy-healthy-tail"
	if err := st.Update(ctx, func(state *State) error {
		state.OutboxMessages[legacyID] = OutboxMessage{
			ID: legacyID, TeamsChatID: "chat:legacy-future", Kind: "helper", Body: "legacy future",
			Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now,
			NextAttemptAt: now.Add(time.Hour),
		}
		state.OutboxMessages[healthyID] = OutboxMessage{
			ID: healthyID, TeamsChatID: "chat:legacy-healthy", Kind: "helper", Body: "healthy tail",
			Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now.Add(time.Second), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy schedule rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	msg, err := st.OutboxMessageByID(ctx, legacyID)
	if err != nil {
		t.Fatalf("load legacy schedule row: %v", err)
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal legacy schedule row: %v", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode legacy schedule row: %v", err)
	}
	delete(payload, "next_attempt_at")
	raw, err = json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal legacy JSON projection: %v", err)
	}
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		// The scalar remains future while the canonical JSON field is absent,
		// which is the shape produced by an older SQLite writer.
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, raw, legacyID)
		return err
	})

	page, err := st.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 1})
	if err != nil {
		t.Fatalf("PendingOutboxPageAt with legacy scalar schedule: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != healthyID {
		t.Fatalf("pending page = %#v, want healthy tail %q", page.Messages, healthyID)
	}
	if page.More {
		t.Fatalf("pending page unexpectedly reports more rows after filtering legacy future row: %#v", page)
	}
	chatIDs, err := st.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 2)
	if err != nil {
		t.Fatalf("PendingOutboxChatIDsAt with legacy scalar schedule: %v", err)
	}
	if want := []string{"chat:legacy-healthy"}; !reflect.DeepEqual(chatIDs, want) {
		t.Fatalf("pending chat IDs = %v, want %v", chatIDs, want)
	}
}

func TestSQLiteSentOutboxSideEffectsUseCanonicalSchedule(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	st := newTestStore(t)
	ids := []string{"outbox:side-effect:due", "outbox:side-effect:zero"}
	if err := st.Update(ctx, func(state *State) error {
		for i, id := range ids {
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: "chat:side-effects", Kind: "helper", Body: id,
				Status: OutboxStatusSent, PostSendEffectsPending: true,
				CreatedAt: now.Add(time.Duration(i) * time.Second), UpdatedAt: now,
				NextAttemptAt: now.Add(time.Hour),
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed sent side-effect rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)

	for i, id := range ids {
		msg, err := st.OutboxMessageByID(ctx, id)
		if err != nil {
			t.Fatalf("load sent side-effect row %s: %v", id, err)
		}
		if i == 0 {
			msg.NextAttemptAt = now.Add(-time.Minute)
		} else {
			// An explicitly persisted zero schedule is due and must not be
			// replaced by the stale future deliver_after scalar during hydration.
			msg.NextAttemptAt = time.Time{}
		}
		raw, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal sent side-effect row %s: %v", id, err)
		}
		withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, raw, id)
			return err
		})
	}

	got, err := st.PendingSentOutboxSideEffects(ctx, 10)
	if err != nil {
		t.Fatalf("PendingSentOutboxSideEffects: %v", err)
	}
	if want := []string{ids[0], ids[1]}; !reflect.DeepEqual(backlogOutboxIDs(got), want) {
		t.Fatalf("sent side-effect IDs = %v, want %v", backlogOutboxIDs(got), want)
	}
	if !got[1].NextAttemptAt.IsZero() {
		t.Fatalf("explicit zero side-effect schedule was hydrated from stale scalar: %s", got[1].NextAttemptAt)
	}
}

func TestSQLiteOutboxAdmissionQuarantinesMalformedOptionalText(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	st := newTestStore(t)
	badID := "outbox:malformed-last-send-error"
	goodID := "outbox:healthy-after-malformed"
	if err := st.Update(ctx, func(state *State) error {
		state.OutboxMessages[badID] = OutboxMessage{
			ID: badID, TeamsChatID: "chat:malformed", Kind: "helper", Body: "bad",
			Status: OutboxStatusQueued, LastSendError: "legacy text", CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		state.OutboxMessages[goodID] = OutboxMessage{
			ID: goodID, TeamsChatID: "chat:healthy", Kind: "helper", Body: "good",
			Status: OutboxStatusQueued, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed malformed outbox rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	msg, err := st.OutboxMessageByID(ctx, badID)
	if err != nil {
		t.Fatalf("load malformed outbox row: %v", err)
	}
	var payload map[string]json.RawMessage
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal malformed outbox row: %v", err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode malformed outbox row: %v", err)
	}
	payload["last_send_error"] = json.RawMessage(`42`)
	raw, err = json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal malformed optional text: %v", err)
	}
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, raw, badID)
		return err
	})

	page, err := st.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 1})
	if err != nil {
		t.Fatalf("PendingOutboxPageAt with malformed optional text: %v", err)
	}
	if got := backlogOutboxIDs(page.Messages); !reflect.DeepEqual(got, []string{goodID}) || page.More {
		t.Fatalf("malformed optional text affected healthy page: ids=%v more=%v", got, page.More)
	}
}

func TestSQLiteOutboxProjectionRetainsUnboundChatRows(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	st := newTestStore(t)
	const id = "outbox:unbound-helper"
	if err := st.Update(ctx, func(state *State) error {
		state.OutboxMessages[id] = OutboxMessage{
			ID: id, Kind: "helper", Body: "internal helper result",
			Status: OutboxStatusSent, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed unbound outbox row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	loaded, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("load SQLite state with unbound outbox row: %v", err)
	}
	if got, ok := loaded.OutboxMessages[id]; !ok || got.TeamsChatID != "" || got.Status != OutboxStatusSent {
		t.Fatalf("unbound outbox row = %#v present=%v, want retained sent row", got, ok)
	}
}

func TestSQLiteOutboxTurnQueriesUseCanonicalJSONForLegacyRows(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	st := newTestStore(t)
	const chatID = "chat:legacy-turn"
	const turnID = "turn:legacy-json-only"
	const earlierID = "outbox:legacy-turn-earlier"
	const laterID = "outbox:legacy-turn-later"
	if err := st.Update(ctx, func(state *State) error {
		state.OutboxMessages[earlierID] = OutboxMessage{
			ID: earlierID, TeamsChatID: chatID, TurnID: turnID, Kind: "helper", Body: "earlier",
			Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		state.OutboxMessages[laterID] = OutboxMessage{
			ID: laterID, TeamsChatID: chatID, TurnID: turnID, Kind: "helper", Body: "later",
			Status: OutboxStatusQueued, Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy turn rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		// Older rows can carry turn_id only in JSON while the compatibility
		// column is NULL. The canonical JSON field must remain queryable.
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET turn_id = NULL WHERE id IN (?, ?)`, earlierID, laterID)
		return err
	})
	page, err := st.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, TurnID: turnID, Limit: 10})
	if err != nil {
		t.Fatalf("legacy JSON-only turn pending page: %v", err)
	}
	if got := backlogOutboxIDs(page.Messages); !reflect.DeepEqual(got, []string{earlierID, laterID}) {
		t.Fatalf("legacy JSON-only turn page = %v, want both rows", got)
	}
	later, err := st.OutboxMessageByID(ctx, laterID)
	if err != nil {
		t.Fatalf("load later legacy turn row: %v", err)
	}
	earlier, found, err := st.EarlierUnsentOutbox(ctx, later)
	if err != nil || !found || earlier.ID != earlierID {
		t.Fatalf("legacy JSON-only turn predecessor = %#v found=%v err=%v, want %s", earlier, found, err, earlierID)
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

func TestOptionalMaintenanceFairDueIsDurableAndOwnerFenced(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "fair-owner", Generation: 19,
					Status: ControlLeaseStatusActive, LeaseUntil: time.Now().Add(time.Hour), LastHeartbeat: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed fairness owner: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			due := now.Add(30 * time.Second)
			control, err := st.SetOptionalMaintenanceFairDueForOwner(ctx, due, "fair-owner", 19)
			if err != nil {
				t.Fatalf("set durable fairness due: %v", err)
			}
			if !control.OptionalMaintenanceFairDueAt.Equal(due) {
				t.Fatalf("fairness due = %v, want %v", control.OptionalMaintenanceFairDueAt, due)
			}
			if _, err := st.SetOptionalMaintenanceFairDueForOwner(ctx, due, "stale-owner", 20); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale fairness set error = %v, want ErrControlLeaseNotHeld", err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("close fairness store: %v", err)
			}
			reopened, err := Open(st.Path())
			if err != nil {
				t.Fatalf("reopen fairness store: %v", err)
			}
			defer reopened.Close()
			control, err = reopened.ReadControl(ctx)
			if err != nil {
				t.Fatalf("read fairness due after reopen: %v", err)
			}
			if !control.OptionalMaintenanceFairDueAt.Equal(due) {
				t.Fatalf("reopened fairness due = %v, want %v", control.OptionalMaintenanceFairDueAt, due)
			}
			control, err = reopened.ClearOptionalMaintenanceFairDueForOwner(ctx, "fair-owner", 19)
			if err != nil {
				t.Fatalf("clear durable fairness due: %v", err)
			}
			if !control.OptionalMaintenanceFairDueAt.IsZero() {
				t.Fatalf("cleared fairness due = %v, want zero", control.OptionalMaintenanceFairDueAt)
			}
		})
	}
}

func TestOutboxFairCursorIsDurableAndOwnerFenced(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "machine-fair-a", Generation: 7,
					Status: ControlLeaseStatusActive, LeaseUntil: time.Now().Add(time.Hour),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed fairness owner: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			control, err := st.SetOutboxFairCursorForOwner(ctx, "chat-tail", "machine-fair-a", 7)
			if err != nil || control.OutboxFairCursor != "chat-tail" {
				t.Fatalf("set owner-fenced fairness cursor = %#v err=%v", control, err)
			}
			ambiguousCursor := "2026-09-06T12:00:00.123456Z\x00outbox-ambiguous"
			control, err = st.SetAmbiguousOutboxRecoveryCursorForOwner(ctx, ambiguousCursor, "machine-fair-a", 7)
			if err != nil || control.AmbiguousOutboxRecoveryCursor != ambiguousCursor {
				t.Fatalf("set owner-fenced ambiguous recovery cursor = %#v err=%v", control, err)
			}
			if _, err := st.SetOutboxFairCursorForOwner(ctx, "chat-stale", "machine-fair-b", 8); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale fairness cursor error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := st.SetAmbiguousOutboxRecoveryCursorForOwner(ctx, "stale", "machine-fair-b", 8); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale ambiguous recovery cursor error = %v, want ErrControlLeaseNotHeld", err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("close fairness store: %v", err)
			}
			reopened, err := Open(st.Path())
			if err != nil {
				t.Fatalf("reopen fairness store: %v", err)
			}
			defer reopened.Close()
			control, err = reopened.ReadControl(ctx)
			if err != nil || control.OutboxFairCursor != "chat-tail" || control.AmbiguousOutboxRecoveryCursor != ambiguousCursor {
				t.Fatalf("reopened fairness cursors = %#v err=%v, want chat-tail and %q", control, err, ambiguousCursor)
			}
		})
	}
}

func TestAmbiguousOutboxRecoveryCursorUsesExpectedCAS(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "machine-cursor", Generation: 11,
					Status:     ControlLeaseStatusActive,
					LeaseUntil: time.Now().Add(time.Hour),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed cursor owner: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			if _, err := st.SetAmbiguousOutboxRecoveryCursorExpected(ctx, "cursor-1", ""); err != nil {
				t.Fatalf("set initial cursor: %v", err)
			}
			if _, err := st.SetAmbiguousOutboxRecoveryCursorForOwnerExpected(ctx, "cursor-2", "cursor-1", "machine-cursor", 11); err != nil {
				t.Fatalf("advance owner cursor: %v", err)
			}
			if _, err := st.SetAmbiguousOutboxRecoveryCursorForOwnerExpected(ctx, "cursor-stale", "cursor-1", "machine-cursor", 11); !errors.Is(err, ErrAmbiguousOutboxRecoveryCursorChanged) {
				t.Fatalf("stale cursor update error = %v, want ErrAmbiguousOutboxRecoveryCursorChanged", err)
			}
			control, err := st.ReadControl(ctx)
			if err != nil {
				t.Fatalf("read cursor after stale update: %v", err)
			}
			if control.AmbiguousOutboxRecoveryCursor != "cursor-2" {
				t.Fatalf("stale update changed cursor to %q, want cursor-2", control.AmbiguousOutboxRecoveryCursor)
			}
		})
	}
}

func TestBacklogFairCursorUsesExpectedCAS(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "backlog-cursor-owner", Generation: 23,
					Status: ControlLeaseStatusActive, LeaseUntil: time.Now().Add(time.Hour),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed backlog cursor owner: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			if _, err := st.SetBacklogFairCursorForOwnerExpected(ctx, BacklogFairLaneLinked, "cursor-1", "", "backlog-cursor-owner", 23); err != nil {
				t.Fatalf("set initial backlog cursor: %v", err)
			}
			if _, err := st.SetBacklogFairCursorForOwnerExpected(ctx, BacklogFairLaneLinked, "cursor-2", "cursor-1", "backlog-cursor-owner", 23); err != nil {
				t.Fatalf("advance backlog cursor: %v", err)
			}
			if _, err := st.SetBacklogFairCursorForOwnerExpected(ctx, BacklogFairLaneLinked, "cursor-stale", "cursor-1", "backlog-cursor-owner", 23); !errors.Is(err, ErrBacklogFairCursorChanged) {
				t.Fatalf("stale backlog cursor error = %v, want ErrBacklogFairCursorChanged", err)
			}
			control, err := st.ReadControl(ctx)
			if err != nil {
				t.Fatalf("read backlog cursor after stale update: %v", err)
			}
			if control.BacklogLinkedFairCursor != "cursor-2" {
				t.Fatalf("stale backlog update changed cursor to %q, want cursor-2", control.BacklogLinkedFairCursor)
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

func TestTeamsOperationalBacklogTreatsDormantGapAsNonBlocking(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.ChatPolls["chat-dormant-gap"] = ChatPollState{
					ChatID: "chat-dormant-gap",
					Gap: &ChatPollGap{
						Kind:             "unverified-continuation",
						HeadProbePending: true,
						LastProgressAt:   now,
					},
				}
				state.ChatPolls["chat-actionable-gap"] = ChatPollState{
					ChatID: "chat-actionable-gap",
					Gap: &ChatPollGap{
						Kind:         "unverified-continuation",
						RecoveryPath: "/chats/chat-actionable-gap/messages?$skiptoken=next",
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed gap states: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			backlog, err := st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("dormant gap backlog probe: %v", err)
			}
			if !backlog.OperationalPollFrontier {
				t.Fatalf("actionable gap was hidden: %#v", backlog)
			}

			if err := st.Update(ctx, func(state *State) error {
				delete(state.ChatPolls, "chat-actionable-gap")
				return nil
			}); err != nil {
				t.Fatalf("remove actionable gap: %v", err)
			}
			backlog, err = st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("dormant-only backlog probe: %v", err)
			}
			if backlog.OperationalPollFrontier {
				t.Fatalf("dormant gap kept optional maintenance blocked: %#v", backlog)
			}
		})
	}
}

func TestSQLiteBackfillRepairsStaleDormantGapHint(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	if err := st.Update(ctx, func(state *State) error {
		state.ChatPolls["chat-stale-dormant-hint"] = ChatPollState{
			ChatID: "chat-stale-dormant-hint",
			Gap: &ChatPollGap{
				Kind:             "unverified-continuation",
				HeadProbePending: true,
				LastProgressAt:   time.Now().UTC(),
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale dormant hint: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET frontier_active = 1 WHERE chat_id = ?`, "chat-stale-dormant-hint")
		return err
	})
	if err := st.Close(); err != nil {
		t.Fatalf("close stale hint store: %v", err)
	}
	reopened, err := Open(st.Path())
	if err != nil {
		t.Fatalf("reopen stale hint store: %v", err)
	}
	defer reopened.Close()
	backlog, err := reopened.TeamsOperationalBacklog(ctx)
	if err != nil {
		t.Fatalf("stale dormant hint backlog probe: %v", err)
	}
	if backlog.OperationalPollFrontier {
		t.Fatalf("stale frontier_active hint kept dormant gap operational: %#v", backlog)
	}
}

func TestTeamsOperationalBacklogDoesNotHoldOnFutureGraph429Retry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.ChatPolls["chat-rate-limited"] = ChatPollState{
					ChatID:           "chat-rate-limited",
					ContinuationPath: "/chats/chat-rate-limited/messages?$skiptoken=429",
					LastError:        "Graph HTTP 429 Too Many Requests",
					LastErrorAt:      now,
					NextPollAt:       now.Add(time.Minute),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed future 429 frontier: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			backlog, err := st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("future 429 backlog probe: %v", err)
			}
			if backlog.OperationalPollFrontier {
				t.Fatalf("future 429 retry held optional maintenance: %#v", backlog)
			}
			if _, err := st.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
				ChatID: "chat-rate-limited", NextPollAt: now.Add(-time.Second), LastActivityAt: now,
			}); err != nil {
				t.Fatalf("make 429 retry due: %v", err)
			}
			backlog, err = st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("due 429 backlog probe: %v", err)
			}
			if !backlog.OperationalPollFrontier {
				t.Fatalf("due 429 frontier was hidden from scheduler gate: %#v", backlog)
			}
		})
	}
}

func TestTeamsOperationalBacklogKeepsFuture429RecoveryRequired(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.ChatPolls["chat-recovery-required-429"] = ChatPollState{
					ChatID:           "chat-recovery-required-429",
					RecoveryRequired: true,
					ContinuationPath: "/chats/chat-recovery-required-429/messages?$skiptoken=429",
					LastError:        "Graph HTTP 429 Too Many Requests",
					LastErrorAt:      now,
					NextPollAt:       now.Add(time.Minute),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed recovery-required future 429: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, st)
			}
			backlog, err := st.TeamsOperationalBacklog(ctx)
			if err != nil {
				t.Fatalf("future 429 recovery-required backlog probe: %v", err)
			}
			if !backlog.OperationalPollFrontier {
				t.Fatalf("future 429 recovery-required state was hidden: %#v", backlog)
			}
		})
	}
}

func TestSQLiteOperationalBacklogTreatsSemanticallyInvalidPollAsRecovery(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	if err := st.Update(ctx, func(state *State) error {
		state.ChatPolls["chat-invalid-poll"] = ChatPollState{ChatID: "chat-invalid-poll"}
		return nil
	}); err != nil {
		t.Fatalf("seed invalid poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		// This is valid JSON but cannot be decoded as ChatPollState because an
		// object-valued attempt was replaced by an array. SQLite must classify it
		// as recovery work instead of trusting a shallow frontier hint.
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ?, frontier_active = 0 WHERE chat_id = ?`,
			[]byte(`{"chat_id":"chat-invalid-poll","attempt":[]}`), "chat-invalid-poll")
		return err
	})
	backlog, err := st.TeamsOperationalBacklog(ctx)
	if err != nil {
		t.Fatalf("semantically invalid poll backlog probe: %v", err)
	}
	if !backlog.OperationalPollFrontier {
		t.Fatalf("semantically invalid poll was hidden from recovery: %#v", backlog)
	}
}

func TestSQLiteChatPollFrontierHintRepairIsVersioned(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	if err := st.Update(ctx, func(state *State) error {
		state.ChatPolls["chat-hint-version"] = ChatPollState{ChatID: "chat-hint-version"}
		return nil
	}); err != nil {
		t.Fatalf("seed hint-version poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		var version string
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteChatPollFrontierHintVersionKey).Scan(&version); err != nil {
			return err
		}
		if version != sqliteChatPollFrontierHintVersion {
			return fmt.Errorf("frontier hint version = %q, want %q", version, sqliteChatPollFrontierHintVersion)
		}
		return nil
	})
	if err := st.Update(ctx, func(state *State) error {
		return nil
	}); err != nil {
		t.Fatalf("generic SQLite state update after frontier migration: %v", err)
	}
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		var version string
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteChatPollFrontierHintVersionKey).Scan(&version); err != nil {
			return err
		}
		if version != sqliteChatPollFrontierHintVersion {
			return fmt.Errorf("generic state update lost frontier hint version = %q, want %q", version, sqliteChatPollFrontierHintVersion)
		}
		return nil
	})
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		// The frontier marker must not disable the independent NULL-only repair
		// for older scheduling projections. This models a store that already ran
		// the frontier migration but was interrupted before another derived column
		// was populated.
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET last_activity_at = NULL WHERE chat_id = ?`, "chat-hint-version")
		return err
	})
	if err := st.Close(); err != nil {
		t.Fatalf("close hint-version store: %v", err)
	}
	reopened, err := Open(st.Path())
	if err != nil {
		t.Fatalf("reopen hint-version store: %v", err)
	}
	defer reopened.Close()
	withSQLiteTxForTest(t, reopened, func(tx *sql.Tx) error {
		var version string
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteChatPollFrontierHintVersionKey).Scan(&version); err != nil {
			return err
		}
		if version != sqliteChatPollFrontierHintVersion {
			return fmt.Errorf("reopened frontier hint version = %q, want %q", version, sqliteChatPollFrontierHintVersion)
		}
		var missing int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM chat_polls WHERE chat_id = ? AND last_activity_at IS NULL`, "chat-hint-version").Scan(&missing); err != nil {
			return err
		}
		if missing != 0 {
			return fmt.Errorf("reopened derived scheduling columns still missing: %d", missing)
		}
		return nil
	})
}

func TestSQLiteChatPollFrontierHintMigrationRecreatesOldGapTrigger(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	const chatID = "chat-old-frontier-trigger"
	if err := st.Update(ctx, func(state *State) error {
		state.ChatPolls[chatID] = ChatPollState{ChatID: chatID}
		return nil
	}); err != nil {
		t.Fatalf("seed old-trigger poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)

	// Recreate the trigger body from the pre-dormant-gap schema.  The old
	// trigger classified every gap as operational, so merely changing the
	// marker/backfill without replacing the trigger would make the next JSON
	// update turn a dormant gap back into frontier_active=1.
	legacyHint := `(CASE WHEN json_valid(NEW.json) THEN COALESCE((
    json_type(NEW.json, '$.pending_page') IN ('object', 'array')
    OR (json_type(NEW.json, '$.continuation_path') = 'text' AND COALESCE(json_extract(NEW.json, '$.continuation_path'), '') <> '')
    OR (json_type(NEW.json, '$.deferred_continuation_path') = 'text' AND COALESCE(json_extract(NEW.json, '$.deferred_continuation_path'), '') <> '')
    OR json_type(NEW.json, '$.gap') IN ('object', 'array')
  ), 0) ELSE 0 END)`
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		for _, stmt := range []string{
			`DROP TRIGGER IF EXISTS chat_polls_frontier_hint_repair_insert`,
			`DROP TRIGGER IF EXISTS chat_polls_frontier_hint_repair_update`,
			`CREATE TRIGGER chat_polls_frontier_hint_repair_insert
AFTER INSERT ON chat_polls
WHEN json_valid(NEW.json)
 AND COALESCE(NEW.frontier_active, 0) != ` + legacyHint + `
BEGIN
  UPDATE chat_polls SET frontier_active = ` + legacyHint + ` WHERE chat_id = NEW.chat_id;
END`,
			`CREATE TRIGGER chat_polls_frontier_hint_repair_update
AFTER UPDATE OF json, frontier_active ON chat_polls
WHEN json_valid(NEW.json)
 AND COALESCE(NEW.frontier_active, 0) != ` + legacyHint + `
BEGIN
  UPDATE chat_polls SET frontier_active = ` + legacyHint + ` WHERE chat_id = NEW.chat_id;
END`,
			`UPDATE state_meta SET value = '1' WHERE key = ?`,
		} {
			if strings.HasPrefix(stmt, `UPDATE state_meta`) {
				if _, err := tx.ExecContext(ctx, stmt, sqliteChatPollFrontierHintVersionKey); err != nil {
					return err
				}
				continue
			}
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		dormant := ChatPollState{
			ChatID:    chatID,
			Seeded:    true,
			PollState: "warm",
			Gap: &ChatPollGap{
				Kind:             "unverified-continuation",
				SafeCursor:       time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
				RecoveryCursor:   time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC),
				HeadProbePending: true,
			},
		}
		raw, err := json.Marshal(dormant)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE chat_polls SET json = ?, frontier_active = 1 WHERE chat_id = ?`, raw, chatID)
		return err
	})

	if err := st.Close(); err != nil {
		t.Fatalf("close old-trigger store: %v", err)
	}
	reopened, err := Open(st.Path())
	if err != nil {
		t.Fatalf("reopen old-trigger store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Load(ctx); err != nil {
		t.Fatalf("load old-trigger store after migration: %v", err)
	}
	withSQLiteTxForTest(t, reopened, func(tx *sql.Tx) error {
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT frontier_active FROM chat_polls WHERE chat_id = ?`, chatID).Scan(&active); err != nil {
			return err
		}
		if active != 0 {
			return fmt.Errorf("dormant gap frontier_active = %d after trigger migration, want 0", active)
		}
		var triggerSQL string
		if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?`, "chat_polls_frontier_hint_repair_update").Scan(&triggerSQL); err != nil {
			return err
		}
		if !strings.Contains(triggerSQL, "head_probe_pending") || !strings.Contains(triggerSQL, "recovery_path") {
			return fmt.Errorf("frontier trigger was not upgraded to dormant-gap predicate: %s", triggerSQL)
		}
		return nil
	})
}

func TestSQLiteChatPollFrontierHintSaveDoesNotRecreateTriggers(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	if err := st.Update(ctx, func(state *State) error {
		state.ChatPolls["chat-trigger-stable"] = ChatPollState{ChatID: "chat-trigger-stable", Seeded: true}
		return nil
	}); err != nil {
		t.Fatalf("seed trigger-stable poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, st)
	var before int
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&before)
	})
	if err := st.Update(ctx, func(state *State) error {
		state.HistoryWatchReady = time.Now().UTC()
		return nil
	}); err != nil {
		t.Fatalf("ordinary SQLite state save: %v", err)
	}
	var after int
	withSQLiteTxForTest(t, st, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&after)
	})
	if after != before {
		t.Fatalf("SQLite schema_version changed during ordinary save: before=%d after=%d; trigger DDL was repeated", before, after)
	}
}
