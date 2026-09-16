package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLegacyJSONChatPollScheduleRawHashFenceRejectsReplacement(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 14, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const chatID = "chat-json-raw-fence"
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed JSON poll: %v", err)
	}
	rawState, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read JSON state: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rawState, &root); err != nil {
		t.Fatalf("decode JSON root: %v", err)
	}
	var polls map[string]json.RawMessage
	if err := json.Unmarshal(root["chat_polls"], &polls); err != nil {
		t.Fatalf("decode JSON polls: %v", err)
	}
	witnessHash := sha256Bytes(polls[chatID])

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load JSON state: %v", err)
	}
	poll := state.ChatPolls[chatID]
	poll.LastError = "replacement with the same logical revision"
	state.ChatPolls[chatID] = poll
	replacement, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal replacement state: %v", err)
	}
	if err := os.WriteFile(store.Path(), append(replacement, '\n'), 0o600); err != nil {
		t.Fatalf("replace JSON state: %v", err)
	}

	_, err = store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID: chatID, PollState: chatPollStateBlocked,
		ExpectedPollRevision: poll.PollRevision, HasExpectedPollRevision: true,
		ExpectedPollJSONHash: witnessHash, HasExpectedPollJSONHash: true,
	})
	if !errors.Is(err, ErrChatPollRevisionChanged) {
		t.Fatalf("stale JSON raw-fence update err=%v, want ErrChatPollRevisionChanged", err)
	}
	current, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("reload JSON state: %v", err)
	}
	if current.ChatPolls[chatID].PollState != poll.PollState || current.ChatPolls[chatID].LastError != poll.LastError {
		t.Fatalf("stale raw-fence update changed replacement poll: %#v", current.ChatPolls[chatID])
	}
}

func TestSQLiteChatPollAdmissionRejectsMalformedNestedRecoveryFields(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	const chatID = "chat-nested-admission-validator"
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateHot,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed nested admission validator poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	cases := []struct {
		name string
		raw  string
		want int
	}{
		{
			name: "valid-nested-envelope",
			raw:  fmt.Sprintf(`{"chat_id":%q,"seeded":true,"state":"hot","attempt":{"id":"attempt-1","lease_generation":3,"expected_poll_role":"work"},"gap":{"quarantined_page":{"chat_id":%q,"records":[],"record_ids":[],"record_hashes":[],"dispositions":[],"refetch_failures":[]}}}`, chatID, chatID),
			want: 1,
		},
		{
			name: "attempt-lease-generation-type",
			raw:  fmt.Sprintf(`{"chat_id":%q,"attempt":{"id":"attempt-1","lease_generation":"bad"}}`, chatID),
			want: 0,
		},
		{
			name: "attempt-expected-role-type",
			raw:  fmt.Sprintf(`{"chat_id":%q,"attempt":{"id":"attempt-1","expected_poll_role":[]}}`, chatID),
			want: 0,
		},
		{
			name: "quarantined-page-record-ids-type",
			raw:  fmt.Sprintf(`{"chat_id":%q,"gap":{"quarantined_page":{"chat_id":%q,"record_ids":{}}}}`, chatID, chatID),
			want: 0,
		},
		{
			name: "quarantined-page-records-type",
			raw:  fmt.Sprintf(`{"chat_id":%q,"gap":{"quarantined_page":{"chat_id":%q,"records":{}}}}`, chatID, chatID),
			want: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(tc.raw)
			sqliteWriteRawChatPollJSONForTest(t, store, chatID, raw)
			var got int
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				query := `SELECT CASE WHEN ` + sqliteChatPollAdmissionValidJSONSQL("json", "chat_id") + ` THEN 1 ELSE 0 END FROM chat_polls WHERE chat_id = ?`
				return tx.QueryRowContext(ctx, query, chatID).Scan(&got)
			})
			if got != tc.want {
				t.Fatalf("nested admission validator = %d, want %d for raw %s", got, tc.want, raw)
			}
		})
	}
}

func TestSQLiteHotPollPendingPageGraphReplaySQLMatchesReceiptMetadata(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tests := []struct {
		name string
		page *ChatPollPendingPage
		want int64
	}{
		{
			name: "ordinary local receipt",
			page: &ChatPollPendingPage{
				ChatID: "pending-local", ReceiptID: "receipt-local",
				RequestPath: "/chats/pending-local/messages", Frontier: "head",
			},
			want: 0,
		},
		{
			name: "invalid record without refetch failure",
			page: &ChatPollPendingPage{
				ChatID: "pending-invalid", ReceiptID: "receipt-invalid",
				RequestPath: "/chats/pending-invalid/messages", Frontier: "head",
				Records: []json.RawMessage{json.RawMessage(`{}`)}, RecordIDs: []string{"m1"},
				RecordHashes: []string{"h1"}, Dispositions: []string{"invalid_record"},
				RefetchFailures: []int{0},
			},
			want: 1,
		},
		{
			name: "oversized record without refetch failure",
			page: &ChatPollPendingPage{
				ChatID: "pending-oversized", ReceiptID: "receipt-oversized",
				RequestPath: "/chats/pending-oversized/messages", Frontier: "head",
				Records: []json.RawMessage{json.RawMessage(`{}`)}, RecordIDs: []string{"m1"},
				RecordHashes: []string{"h1"}, Dispositions: []string{"oversized_record"},
				RefetchFailures: []int{0},
			},
			want: 1,
		},
		{
			name: "trimmed invalid disposition",
			page: &ChatPollPendingPage{
				ChatID: "pending-invalid-trimmed", ReceiptID: "receipt-invalid-trimmed",
				RequestPath: "/chats/pending-invalid-trimmed/messages", Frontier: "head",
				Records: []json.RawMessage{json.RawMessage(`{}`)}, RecordIDs: []string{"m1"},
				RecordHashes: []string{"h1"}, Dispositions: []string{" invalid_record "},
				RefetchFailures: []int{0},
			},
			want: 1,
		},
		{
			name: "quarantined record is local",
			page: &ChatPollPendingPage{
				ChatID: "pending-quarantined", ReceiptID: "receipt-quarantined",
				RequestPath: "/chats/pending-quarantined/messages", Frontier: "head",
				Records: []json.RawMessage{json.RawMessage(`{}`)}, RecordIDs: []string{"m1"},
				RecordHashes: []string{"h1"}, Dispositions: []string{"invalid_record_quarantined"},
				RefetchFailures: []int{2},
			},
			want: 0,
		},
		{
			name: "trimmed quarantined record is local",
			page: &ChatPollPendingPage{
				ChatID: "pending-quarantined-trimmed", ReceiptID: "receipt-quarantined-trimmed",
				RequestPath: "/chats/pending-quarantined-trimmed/messages", Frontier: "head",
				Records: []json.RawMessage{json.RawMessage(`{}`)}, RecordIDs: []string{"m1"},
				RecordHashes: []string{"h1"}, Dispositions: []string{" invalid_record_quarantined "},
				RefetchFailures: []int{2},
			},
			want: 0,
		},
		{
			name: "legacy positive refetch failure",
			page: &ChatPollPendingPage{
				ChatID: "pending-legacy", ReceiptID: "receipt-legacy",
				RequestPath: "/chats/pending-legacy/messages", Frontier: "head",
				Records: []json.RawMessage{json.RawMessage(`{}`)}, RecordIDs: []string{"m1"},
				RecordHashes: []string{"h1"}, RefetchFailures: []int{1},
			},
			want: 1,
		},
		{
			name: "unknown disposition fails closed",
			page: &ChatPollPendingPage{
				ChatID: "pending-unknown", ReceiptID: "receipt-unknown",
				RequestPath: "/chats/pending-unknown/messages", Frontier: "head",
				Records: []json.RawMessage{json.RawMessage(`{}`)}, RecordIDs: []string{"m1"},
				RecordHashes: []string{"h1"}, Dispositions: []string{"future-disposition"},
				RefetchFailures: []int{0},
			},
			want: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			chatID := tc.page.ChatID
			if err := store.Update(ctx, func(state *State) error {
				state.ChatPolls[chatID] = ChatPollState{
					ChatID: chatID, Seeded: true, PollState: "warm",
					NextPollAt: now.Add(-time.Minute), UpdatedAt: now, PendingPage: tc.page,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed pending page: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			var got int64
			if err := store.withStateLock(ctx, func() error {
				pointer, ok, err := store.currentSQLitePointerUnlocked()
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("sqlite pointer missing")
				}
				db, err := store.sqliteDBUnlocked(pointer)
				if err != nil {
					return err
				}
				return db.QueryRowContext(ctx, `SELECT `+sqliteChatPollPendingPageGraphReplaySQL("chat_polls.json")+` FROM chat_polls WHERE chat_id = ?`, chatID).Scan(&got)
			}); err != nil {
				t.Fatalf("evaluate pending-page scalar hint: %v", err)
			}
			if got != tc.want {
				t.Fatalf("pending-page scalar hint=%d, want %d", got, tc.want)
			}
		})
	}
}

func TestSQLiteHotPollPendingPageScalarActiveBitSkipsJSONAndFailsClosed(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	const chatID = "chat-pending-scalar-active"
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now, UpdatedAt: now,
			PendingPage: &ChatPollPendingPage{
				ChatID: chatID, ReceiptID: "receipt-pending-scalar-active",
				RequestPath: "/chats/" + chatID + "/messages", Frontier: "head",
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed pending scalar-active fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// Simulate a stale/inactive scalar bit with an otherwise valid canonical
		// receipt. The conservative gate must not admit it as local-only, and the
		// scalar replay hint must not parse the JSON column on this path.
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET pending_page_active = 0 WHERE chat_id = ?`, chatID)
		return err
	})
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = errors.New("sqlite pointer missing")
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		var replay, localOnly int64
		if err := db.QueryRowContext(ctx, `SELECT `+
			sqliteChatPollPendingPageGraphReplayWithScalarActiveSQL("p.json", "p.pending_page_active")+
			` FROM chat_polls p WHERE p.chat_id = ?`, chatID).Scan(&replay); err != nil {
			return fmt.Errorf("scalar replay hint: %w", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT `+
			sqliteChatPollLocalOnlyPendingPageWithScalarActiveSQL("p.json", "p.pending_page_active")+
			` FROM chat_polls p WHERE p.chat_id = ?`, chatID).Scan(&localOnly); err != nil {
			return fmt.Errorf("local-only hint: %w", err)
		}
		if replay != 0 || localOnly != 0 {
			return fmt.Errorf("inactive pending scalar hints replay=%d local_only=%d, want both zero", replay, localOnly)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// An invalid canonical payload must still be handled without turning the
	// inactive scalar path into a JSON error or an unsafe local admission.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ?, pending_page_active = 0 WHERE chat_id = ?`, []byte(`{"pending_page":`), chatID)
		return err
	})
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = errors.New("sqlite pointer missing")
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		var localOnly int64
		if err := db.QueryRowContext(ctx, `SELECT `+
			sqliteChatPollLocalOnlyPendingPageWithScalarActiveSQL("p.json", "p.pending_page_active")+
			` FROM chat_polls p WHERE p.chat_id = ?`, chatID).Scan(&localOnly); err != nil {
			return fmt.Errorf("invalid-json local-only hint: %w", err)
		}
		if localOnly != 0 {
			return fmt.Errorf("invalid-json inactive row became local-only: %d", localOnly)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHotPollCorruptWorkProbeIgnoresStaleScalarGates(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		sessionID     string
		canonicalChat string
		raw           []byte
		scalarChatID  string
		wantChatID    string
		wantPollFence bool
	}{
		{
			name:          "typed corruption with blank scalar identity",
			sessionID:     "session-stale-scalar",
			canonicalChat: "chat-stale-scalar",
			raw:           []byte(`{"id":"session-stale-scalar","teams_chat_id":"chat-stale-scalar","status":"active","model_generation":"not-an-integer"}`),
			scalarChatID:  "",
			wantChatID:    "chat-stale-scalar",
			wantPollFence: true,
		},
		{
			name:          "malformed JSON with closed scalar status",
			sessionID:     "session-malformed-scalar",
			canonicalChat: "chat-malformed-scalar",
			raw:           []byte(`{"id":"session-malformed-scalar","teams_chat_id":"chat-malformed-scalar"`),
			scalarChatID:  "chat-malformed-scalar",
			wantChatID:    "chat-malformed-scalar",
			wantPollFence: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			id, chatID := tc.sessionID, tc.canonicalChat
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[id] = SessionContext{
					ID: id, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
				}
				state.ChatPolls[chatID] = ChatPollState{
					ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
					NextPollAt: now.Add(time.Hour), BlockedUntil: now.Add(2 * time.Hour), UpdatedAt: now,
					PendingPage: &ChatPollPendingPage{
						ChatID: chatID, ReceiptID: "receipt-" + chatID,
						RequestPath: "/chats/" + chatID + "/messages", Frontier: "head",
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed stale-scalar fixture: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, tc.raw, id); err != nil {
					return err
				}
				// These values deliberately make the old scalar-only recovery
				// predicate skip the row: closed status, a blank/stale chat ID,
				// and a future poll with pending_page_active=0.
				if _, err := tx.ExecContext(ctx, `UPDATE sessions SET teams_chat_id = ?, status = ?, projection_trusted = 0 WHERE id = ?`, tc.scalarChatID, string(SessionStatusClosed), id); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET pending_page_active = 0, next_poll_at = ?, blocked_until = ? WHERE chat_id = ?`, now.Add(time.Hour).UnixNano(), now.Add(2*time.Hour).UnixNano(), chatID)
				return err
			})

			admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
			if err != nil {
				t.Fatalf("stale-scalar admission: %v", err)
			}
			if admission.Disposition != HotPollWorkAdmissionDurableCorrupt {
				t.Fatalf("disposition = %q, want durable corruption", admission.Disposition)
			}
			if len(admission.CorruptSessions) != 1 {
				t.Fatalf("corrupt evidence = %#v, want one row", admission.CorruptSessions)
			}
			evidence := admission.CorruptSessions[0]
			if evidence.TeamsChatID != tc.wantChatID || evidence.HasPoll != tc.wantPollFence {
				t.Fatalf("evidence = %#v, want chat=%q poll=%t", evidence, tc.wantChatID, tc.wantPollFence)
			}
		})
	}
}

func TestSQLiteHotPollCorruptProbeFailsClosedOnNullScalarAndInvalidTime(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 12, 30, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		sessionID = "session-null-scalar-invalid-time"
		chatID    = "chat-null-scalar-invalid-time"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{
			ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
		}
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed null-scalar fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		raw := []byte(`{"id":"session-null-scalar-invalid-time","teams_chat_id":"chat-null-scalar-invalid-time","status":"active","created_at":"not-a-time"}`)
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ?, teams_chat_id = NULL, status = ?, projection_trusted = 1 WHERE id = ?`, raw, string(SessionStatusActive), sessionID); err != nil {
			return err
		}
		return nil
	})

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("null-scalar/invalid-time admission: %v", err)
	}
	if admission.Disposition != HotPollWorkAdmissionDurableCorrupt || len(admission.CorruptSessions) != 1 {
		t.Fatalf("admission = %#v, want one durable-corrupt witness", admission)
	}
	evidence := admission.CorruptSessions[0]
	if evidence.SessionID != sessionID || evidence.TeamsChatID != chatID || !evidence.HasPoll || !evidence.HasPollJSON || !evidence.PollJSONObserved {
		t.Fatalf("evidence = %#v, want canonical chat and observed poll", evidence)
	}
}

func TestSQLiteHotPollCorruptProbeRejectsNonRFC3339Times(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 12, 35, 0, 0, time.UTC)
	cases := []struct {
		name string
		time string
	}{
		{name: "date only", time: "2026-09-14"},
		{name: "timezone missing", time: "2026-09-14T12:00:00"},
		{name: "leading whitespace", time: " 2026-09-14T12:00:00Z"},
		{name: "trailing whitespace", time: "2026-09-14T12:00:00Z "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			const (
				sessionID = "session-non-rfc3339-time"
				chatID    = "chat-non-rfc3339-time"
			)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now}
				state.ChatPolls[chatID] = ChatPollState{ChatID: chatID, Seeded: true, PollState: chatPollStateWarm, NextPollAt: now.Add(-time.Minute), UpdatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed non-RFC3339 fixture: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				raw := []byte(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":"active","created_at":%q}`, sessionID, chatID, tc.time))
				_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ?, projection_trusted = 0 WHERE id = ?`, raw, sessionID)
				return err
			})

			admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
			if err != nil {
				t.Fatalf("non-RFC3339 admission: %v", err)
			}
			if admission.Disposition != HotPollWorkAdmissionDurableCorrupt || len(admission.CorruptSessions) != 1 {
				t.Fatalf("admission = %#v, want one durable-corrupt witness", admission)
			}
		})
	}
}

func TestSQLiteHotPollCorruptProbeAdvancesPastControlAndFencedPrefix(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 12, 40, 0, 0, time.UTC)
	store := newTestStore(t)
	const controlChatID = "chat-probe-control"
	rows := []struct {
		sessionID string
		chatID    string
	}{
		{sessionID: "session-probe-000-control", chatID: controlChatID},
		{sessionID: "session-probe-001-fenced", chatID: "chat-probe-fenced"},
		{sessionID: "session-probe-002-target", chatID: "chat-probe-target"},
	}
	if err := store.Update(ctx, func(state *State) error {
		for _, row := range rows {
			state.Sessions[row.sessionID] = SessionContext{
				ID: row.sessionID, Status: SessionStatusActive, TeamsChatID: row.chatID, UpdatedAt: now,
			}
			state.ChatPolls[row.chatID] = ChatPollState{
				ChatID: row.chatID, Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed probe-prefix fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	rawBySession := make(map[string][]byte, len(rows))
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, row := range rows {
			raw := []byte(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":"active","model_generation":"bad"}`, row.sessionID, row.chatID))
			rawBySession[row.sessionID] = raw
			if _, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ?, projection_trusted = 0 WHERE id = ?`, raw, row.sessionID); err != nil {
				return err
			}
		}
		return nil
	})
	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID: rows[1].chatID, SetRecoveryRequired: true,
		RecoveryReason: "already fenced", RecoverySourceHash: sha256Bytes(rawBySession[rows[1].sessionID]),
	}); err != nil {
		t.Fatalf("fence prefix schedule: %v", err)
	}

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, controlChatID, time.Time{}, now)
	if err != nil {
		t.Fatalf("probe-prefix admission: %v", err)
	}
	if admission.Disposition != HotPollWorkAdmissionDurableCorrupt || len(admission.CorruptSessions) != 1 {
		t.Fatalf("admission = %#v, want only target witness", admission)
	}
	if got := admission.CorruptSessions[0]; got.SessionID != rows[2].sessionID || got.TeamsChatID != rows[2].chatID {
		t.Fatalf("target evidence = %#v, want %s/%s", got, rows[2].sessionID, rows[2].chatID)
	}
}

func TestSQLiteHotPollRecoveryHashFenceRejectsMissingAndReplacedPoll(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 12, 50, 0, 0, time.UTC)
	t.Run("missing row", func(t *testing.T) {
		store := newTestStore(t)
		const sessionID = "session-missing-poll-witness"
		const chatID = "chat-missing-poll-witness"
		if err := store.Update(ctx, func(state *State) error {
			state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now}
			return nil
		}); err != nil {
			t.Fatalf("seed missing-poll session: %v", err)
		}
		migrateStoreToSQLiteForTest(t, store)
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ?, projection_trusted = 0 WHERE id = ?`, []byte(`{"id":"session-missing-poll-witness","teams_chat_id":"chat-missing-poll-witness","status":"active","model_generation":"bad"}`), sessionID)
			return err
		})
		admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
		if err != nil || len(admission.CorruptSessions) != 1 {
			t.Fatalf("missing-poll admission=%#v err=%v", admission, err)
		}
		witness := admission.CorruptSessions[0]
		if witness.HasPollJSON || !witness.PollJSONObserved || witness.PollJSONHash != "" {
			t.Fatalf("missing-poll witness=%#v, want observed absence", witness)
		}
		if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{ChatID: chatID, PollState: chatPollStateWarm}); err != nil {
			t.Fatalf("insert concurrent poll: %v", err)
		}
		_, err = store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
			ChatID: chatID, SetRecoveryRequired: true, RecoveryReason: "stale witness",
			RecoverySourceHash: witness.SourceHash, ExpectedPollJSONHash: witness.PollJSONHash, HasExpectedPollJSONHash: true,
		})
		if !errors.Is(err, ErrChatPollRevisionChanged) {
			t.Fatalf("missing-poll stale update err=%v, want ErrChatPollRevisionChanged", err)
		}
	})

	t.Run("replacement row", func(t *testing.T) {
		store := newTestStore(t)
		const sessionID = "session-replaced-poll-witness"
		const chatID = "chat-replaced-poll-witness"
		if err := store.Update(ctx, func(state *State) error {
			state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now}
			state.ChatPolls[chatID] = ChatPollState{ChatID: chatID, Seeded: true, PollState: chatPollStateWarm, UpdatedAt: now}
			return nil
		}); err != nil {
			t.Fatalf("seed replacement-poll fixture: %v", err)
		}
		migrateStoreToSQLiteForTest(t, store)
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ?, projection_trusted = 0 WHERE id = ?`, []byte(`{"id":"session-replaced-poll-witness","teams_chat_id":"chat-replaced-poll-witness","status":"active","model_generation":"bad"}`), sessionID)
			return err
		})
		admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
		if err != nil || len(admission.CorruptSessions) != 1 {
			t.Fatalf("replacement-poll admission=%#v err=%v", admission, err)
		}
		witness := admission.CorruptSessions[0]
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, []byte(`{"chat_id":"chat-replaced-poll-witness","seeded":true,"poll_state":"cold"}`), chatID)
			return err
		})
		_, err = store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
			ChatID: chatID, SetRecoveryRequired: true, RecoveryReason: "stale witness",
			RecoverySourceHash: witness.SourceHash, ExpectedPollJSONHash: witness.PollJSONHash, HasExpectedPollJSONHash: true,
		})
		if !errors.Is(err, ErrChatPollRevisionChanged) {
			t.Fatalf("replacement-poll stale update err=%v, want ErrChatPollRevisionChanged", err)
		}
	})
}

func TestSQLiteHotPollCorruptSessionWithOpaquePollIsFencedAcrossReopen(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 13, 10, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		corruptSessionID = "session-opaque-poll-recovery"
		corruptChatID    = "chat-opaque-poll-recovery"
		healthySessionID = "session-opaque-poll-healthy"
		healthyChatID    = "chat-opaque-poll-healthy"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[corruptSessionID] = SessionContext{
			ID: corruptSessionID, Status: SessionStatusActive, TeamsChatID: corruptChatID, UpdatedAt: now,
		}
		state.ChatPolls[corruptChatID] = ChatPollState{
			ChatID: corruptChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		state.Sessions[healthySessionID] = SessionContext{
			ID: healthySessionID, Status: SessionStatusActive, TeamsChatID: healthyChatID, UpdatedAt: now,
		}
		state.ChatPolls[healthyChatID] = ChatPollState{
			ChatID: healthyChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed opaque-poll recovery fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// The session is invalid, while the matching poll is valid JSON with an
		// unusable nested state value.  This combination is important: the raw
		// poll must remain forensic, so the session hash cannot be written into
		// the poll JSON itself.
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ?, projection_trusted = 0 WHERE id = ?`,
			[]byte(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":"active","model_generation":"bad"}`, corruptSessionID, corruptChatID)), corruptSessionID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`,
			[]byte(fmt.Sprintf(`{"chat_id":%q,"seeded":true,"state":123}`, corruptChatID)), corruptChatID)
		return err
	})

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil || len(admission.CorruptSessions) != 1 {
		t.Fatalf("initial opaque-poll admission=%#v err=%v, want one corrupt witness", admission, err)
	}
	witness := admission.CorruptSessions[0]
	if !witness.HasPollJSON || witness.PollJSONHash == "" || witness.HasPoll {
		t.Fatalf("initial opaque-poll witness=%#v, want opaque poll hash without typed poll", witness)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID: corruptChatID, PollState: chatPollStateBlocked,
		NextPollAt: now.Add(time.Minute), BlockedUntil: now.Add(time.Minute),
		SetRecoveryRequired: true, RecoveryReason: witness.Reason,
		RecoverySourceHash:   witness.SourceHash,
		ExpectedPollJSONHash: witness.PollJSONHash, HasExpectedPollJSONHash: true,
	}); err != nil {
		t.Fatalf("persist opaque-poll recovery disposition: %v", err)
	}

	assertFenced := func(label string) {
		t.Helper()
		admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
		if err != nil {
			t.Fatalf("%s admission: %v", label, err)
		}
		if len(admission.CorruptSessions) != 0 {
			t.Fatalf("%s repeated corrupt recovery=%#v, want durable fence", label, admission.CorruptSessions)
		}
		foundHealthy := false
		for _, candidate := range admission.Candidates {
			if candidate.TeamsChatID == healthyChatID {
				foundHealthy = true
				break
			}
		}
		if !foundHealthy {
			t.Fatalf("%s healthy tail was starved: candidates=%#v admission=%#v", label, admission.Candidates, admission)
		}
	}
	assertFenced("same process")
	if err := store.Close(); err != nil {
		t.Fatalf("close opaque-poll recovery fixture: %v", err)
	}
	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("reopen opaque-poll recovery fixture: %v", err)
	}
	defer reopened.Close()
	store = reopened
	assertFenced("after reopen")
}

func TestSQLiteHotPollReadGateExcludesGraphBoundPrefixBeyondHeadroom(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	const tailChatID = "read-gate-local-tail"
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: "read-gate-control", UpdatedAt: now}
		state.ChatPolls["read-gate-control"] = ChatPollState{
			ChatID: "read-gate-control", Seeded: true, PollState: "warm",
			NextPollAt: now.Add(time.Hour), UpdatedAt: now,
		}
		for i := 0; i < 257; i++ {
			sessionID := fmt.Sprintf("read-gate-prefix-session-%03d", i)
			chatID := fmt.Sprintf("read-gate-prefix-chat-%03d", i)
			state.Sessions[sessionID] = SessionContext{
				ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID,
				CodexThreadID: "thread-" + sessionID, UpdatedAt: now,
			}
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: "warm",
				NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
				PendingPage: &ChatPollPendingPage{
					ChatID: chatID, ReceiptID: "receipt-" + chatID,
					RequestPath: "/chats/" + chatID + "/messages", Frontier: "head",
					Records: []json.RawMessage{json.RawMessage(`{}`)}, RecordIDs: []string{"m1"},
					RecordHashes: []string{"h1"}, Dispositions: []string{"invalid_record"},
					RefetchFailures: []int{1},
				},
			}
		}
		state.Sessions["read-gate-tail-session"] = SessionContext{
			ID: "read-gate-tail-session", Status: SessionStatusActive, TeamsChatID: tailChatID,
			CodexThreadID: "thread-read-gate-tail", UpdatedAt: now.Add(time.Second),
		}
		state.ChatPolls[tailChatID] = ChatPollState{
			ChatID: tailChatID, Seeded: true, PollState: "warm",
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now.Add(time.Second),
			PendingPage: &ChatPollPendingPage{
				ChatID: tailChatID, ReceiptID: "receipt-" + tailChatID,
				RequestPath: "/chats/" + tailChatID + "/messages", Frontier: "head",
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed read-gate prefix: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAtLimitAndReadGate(
		ctx, "read-gate-control", now.Add(-time.Hour), now, 1, true,
	)
	if err != nil {
		t.Fatalf("read-gated admission: %v", err)
	}
	if admission.Disposition == HotPollWorkAdmissionLegacyCompatible {
		t.Fatal("read-gated admission unexpectedly used legacy compatibility path")
	}
	if len(admission.Candidates) != 1 || admission.Candidates[0].TeamsChatID != tailChatID {
		t.Fatalf("read-gated candidates = %#v, want only local tail %q", admission.Candidates, tailChatID)
	}
}

func TestSQLiteHotPollReadGateCanonicalFallbackKeepsLocalReceiptWithStaleScalar(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	const (
		controlChatID = "read-gate-stale-scalar-control"
		localChatID   = "read-gate-stale-scalar-local"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: controlChatID, UpdatedAt: now}
		state.ChatPolls[controlChatID] = ChatPollState{
			ChatID: controlChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(time.Hour), UpdatedAt: now,
		}
		state.Sessions["read-gate-stale-scalar-session"] = SessionContext{
			ID: "read-gate-stale-scalar-session", Status: SessionStatusActive,
			TeamsChatID: localChatID, CodexThreadID: "thread-read-gate-stale-scalar", UpdatedAt: now,
		}
		state.ChatPolls[localChatID] = ChatPollState{
			ChatID: localChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(time.Hour), UpdatedAt: now,
			PendingPage: &ChatPollPendingPage{
				ChatID: localChatID, ReceiptID: "receipt-read-gate-stale-scalar",
				RequestPath: "/chats/" + localChatID + "/messages", Frontier: "head",
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale-scalar read-gate fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// A legacy/raw writer can leave the scalar presence bit stale. The
		// admission trigger revokes projection trust, forcing the canonical
		// fallback; the fallback must still see the local receipt while the
		// account/global read gate is active.
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET pending_page_active = 0 WHERE chat_id = ?`, localChatID)
		return err
	})

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAtLimitAndReadGate(
		ctx, controlChatID, time.Time{}, now, 1, true,
	)
	if err != nil {
		t.Fatalf("stale-scalar read-gated admission: %v", err)
	}
	if admission.Disposition == HotPollWorkAdmissionLegacyCompatible {
		t.Fatal("stale-scalar read-gated admission used legacy compatibility path")
	}
	if len(admission.Candidates) != 1 || admission.Candidates[0].TeamsChatID != localChatID {
		t.Fatalf("stale-scalar read-gated candidates = %#v, want local receipt %q", admission.Candidates, localChatID)
	}
}

// A Graph-dependent pending page is operational, but its provider retry
// deadline still applies.  It must not consume the bounded operational SQL
// prefix while a later due chat waits behind it; otherwise the bridge drops the
// whole prefix during scheduler re-evaluation and has no durable keyset tail
// from which to refill this cycle.
func TestSQLiteHotPollWorkAdmissionSkipsFutureGraphPendingPrefix(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		controlChatID = "future-graph-prefix-control"
		tailSessionID = "future-graph-prefix-tail-session"
		tailChatID    = "future-graph-prefix-tail-chat"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: controlChatID, UpdatedAt: now}
		state.ChatPolls[controlChatID] = ChatPollState{
			ChatID: controlChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now, UpdatedAt: now,
		}
		for i := 0; i < 65; i++ {
			sessionID := fmt.Sprintf("future-graph-prefix-session-%03d", i)
			chatID := fmt.Sprintf("future-graph-prefix-chat-%03d", i)
			updated := now.Add(-time.Hour + time.Duration(i)*time.Millisecond)
			state.Sessions[sessionID] = SessionContext{
				ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID,
				CodexThreadID: "thread-" + sessionID, UpdatedAt: updated,
			}
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(time.Hour), BlockedUntil: now.Add(2 * time.Hour),
				LastActivityAt: updated, UpdatedAt: updated,
				PendingPage: &ChatPollPendingPage{
					ChatID: chatID, ReceiptID: "receipt-" + chatID,
					RequestPath: "/chats/" + chatID + "/messages", Frontier: "head",
					Records:   []json.RawMessage{json.RawMessage(`{}`)},
					RecordIDs: []string{"exceptional"}, RecordHashes: []string{"hash"},
					Dispositions: []string{"invalid_record"}, RefetchFailures: []int{0},
				},
			}
		}
		state.Sessions[tailSessionID] = SessionContext{
			ID: tailSessionID, Status: SessionStatusActive, TeamsChatID: tailChatID,
			CodexThreadID: "thread-" + tailSessionID, UpdatedAt: now,
		}
		state.ChatPolls[tailChatID] = ChatPollState{
			ChatID: tailChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed future Graph-bound prefix: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAtLimit(
		ctx, controlChatID, time.Time{}, now, 8,
	)
	if err != nil {
		t.Fatalf("future Graph-bound prefix admission: %v", err)
	}
	if admission.Disposition == HotPollWorkAdmissionLegacyCompatible {
		t.Fatal("future Graph-bound prefix unexpectedly used legacy compatibility path")
	}
	for _, candidate := range admission.Candidates {
		if candidate.TeamsChatID == tailChatID {
			return
		}
	}
	t.Fatalf("due tail was hidden behind future Graph-bound prefix: candidates=%#v", admission.Candidates)
}

func TestSQLiteHotPollMalformedPollIsNotHiddenByStaleBlockedDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		sessionID = "malformed-blocked-session"
		chatID    = "malformed-blocked-chat"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{
			ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
		}
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed malformed-blocked fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Keep the session executable, but replace only the canonical poll payload
	// with a type-corrupt object and leave a deliberately unusable scalar retry
	// deadline. The old malformedPollReady predicate treated that deadline as a
	// permanent admission filter, so this otherwise repairable chat vanished
	// from every canonical fallback.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls
SET json = ?, poll_state = ?, blocked_until = ?, admission_valid = 0, projection_trusted = 0
WHERE chat_id = ?`,
			[]byte(`{"chat_id":"malformed-blocked-chat","state":123}`), chatPollStateBlocked, int64(^uint64(0)>>1), chatID)
		return err
	})

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("malformed-blocked admission: %v", err)
	}
	if admission.Disposition != HotPollWorkAdmissionAuthoritative || len(admission.CorruptSessions) != 0 {
		t.Fatalf("malformed-blocked admission = %#v, want authoritative runnable repair candidate", admission)
	}
	if len(admission.Candidates) != 1 || admission.Candidates[0].ID != sessionID || admission.Candidates[0].TeamsChatID != chatID {
		t.Fatalf("malformed-blocked candidates = %#v, want session-local repair candidate", admission.Candidates)
	}
}

func TestSQLiteHotPollAdmissionMarkerSnapshotFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["marker-snapshot-session"] = SessionContext{
			ID: "marker-snapshot-session", Status: SessionStatusActive,
			TeamsChatID: "marker-snapshot-chat", UpdatedAt: time.Now().UTC(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed marker snapshot fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	readCurrent := func(workCandidates bool) bool {
		t.Helper()
		var current bool
		if err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return sql.ErrNoRows
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			current, err = sqliteHotPollAdmissionVersionsCurrent(ctx, db, workCandidates)
			return err
		}); err != nil {
			t.Fatalf("read admission marker snapshot (work=%v): %v", workCandidates, err)
		}
		return current
	}

	if !readCurrent(false) || !readCurrent(true) {
		t.Fatal("current admission markers were not accepted")
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, "0", sqliteTurnProjectionVersionKey)
		return err
	})
	if readCurrent(true) {
		t.Fatal("mismatched work marker was accepted")
	}
	// Ready admission does not depend on the turn marker and should remain
	// eligible for the scalar lane; this also proves the marker set is scoped.
	if !readCurrent(false) {
		t.Fatal("unrelated turn marker mismatch disabled ready admission")
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteChatPollProjectionVersionKey)
		return err
	})
	if readCurrent(false) {
		t.Fatal("missing ready marker was accepted")
	}
}

func TestSQLiteHotPollScalarProjectionTrustIsFencedAndFallsBack(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	session := SessionContext{
		ID: "scalar-trust-session", Status: SessionStatusActive,
		TeamsChatID: "scalar-trust-chat", UpdatedAt: now,
	}
	poll := ChatPollState{
		ChatID: session.TeamsChatID, Seeded: true, PollState: chatPollStateWarm,
		NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		state.ChatPolls[poll.ChatID] = poll
		state.Turns["scalar-trust-turn"] = Turn{
			ID: "scalar-trust-turn", SessionID: session.ID, Status: TurnStatusCompleted,
			CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed scalar trust fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	readProjection := func(label string) (int64, int64, int64, error) {
		var trusted, canonicalRevision, projectionRevision int64
		err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%s: sqlite pointer missing", label)
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			return db.QueryRowContext(ctx, `SELECT projection_trusted, canonical_revision, projection_revision
FROM chat_polls WHERE chat_id = ?`, poll.ChatID).Scan(&trusted, &canonicalRevision, &projectionRevision)
		})
		return trusted, canonicalRevision, projectionRevision, err
	}

	trusted, beforeRevision, beforeProjection, err := readProjection("initial")
	if err != nil {
		t.Fatal(err)
	}
	if trusted != 1 || beforeRevision <= 0 || beforeProjection != beforeRevision {
		t.Fatalf("initial poll projection trust=%d canonical=%d projection=%d, want trusted matching positive generation", trusted, beforeRevision, beforeProjection)
	}
	var initialTrusted []string
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		initialTrusted, err = loadSQLiteHotPollReadyChatIDsTrusted(ctx, db, "control-chat", now, sqliteHotPollReadyLimit)
		return err
	}); err != nil {
		t.Fatalf("trusted scalar ready admission: %v", err)
	}
	if len(initialTrusted) != 1 || initialTrusted[0] != poll.ChatID {
		t.Fatalf("trusted scalar ready ids=%#v, want %q", initialTrusted, poll.ChatID)
	}

	// A raw JSON-first writer changes the canonical envelope without advancing
	// the projection generation. The trigger must revoke trust, and the public
	// admission API must still find the due row through its JSON oracle.
	raw := sqliteRawChatPollJSONForTest(t, store, poll.ChatID)
	var changed map[string]any
	if err := json.Unmarshal(raw, &changed); err != nil {
		t.Fatalf("decode poll JSON: %v", err)
	}
	changed["state"] = chatPollStateCold
	changedRaw, err := json.Marshal(changed)
	if err != nil {
		t.Fatalf("marshal changed poll JSON: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, changedRaw, poll.ChatID)
		return err
	})
	trusted, afterRawRevision, afterRawProjection, err := readProjection("raw write")
	if err != nil {
		t.Fatal(err)
	}
	if trusted != 0 || afterRawRevision != beforeRevision || afterRawProjection != beforeProjection {
		t.Fatalf("raw JSON write projection trust=%d canonical=%d projection=%d, want trust revoked without generation rewrite", trusted, afterRawRevision, afterRawProjection)
	}
	var fallbackIDs []string
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		fallbackIDs, err = loadSQLiteHotPollReadyChatIDs(ctx, db, "control-chat", now, sqliteHotPollReadyLimit)
		return err
	}); err != nil {
		t.Fatalf("JSON fallback ready admission: %v", err)
	}
	if len(fallbackIDs) != 1 || fallbackIDs[0] != poll.ChatID {
		t.Fatalf("fallback ready ids=%#v, want %q", fallbackIDs, poll.ChatID)
	}

	// A known writer advances the row-local generation and republishes a
	// matching trusted projection. This is separate from the business poll
	// revisions and proves the normal path can re-enter the fast lane.
	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID: poll.ChatID, PollState: chatPollStateWarm,
		NextPollAt: now.Add(-2 * time.Minute), LastActivityAt: now,
	}); err != nil {
		t.Fatalf("known schedule writer: %v", err)
	}
	trusted, finalRevision, finalProjection, err := readProjection("known writer")
	if err != nil {
		t.Fatal(err)
	}
	if trusted != 1 || finalRevision <= afterRawRevision || finalProjection != finalRevision {
		t.Fatalf("known writer projection trust=%d canonical=%d projection=%d, want trusted generation > %d", trusted, finalRevision, finalProjection, afterRawRevision)
	}
}

func TestSQLiteSessionBackfillRevokesPretrustedMalformedRow(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["malformed-pretrusted-session"] = SessionContext{
			ID: "malformed-pretrusted-session", Status: SessionStatusActive,
			TeamsChatID: "malformed-pretrusted-chat", UpdatedAt: now,
		}
		state.ChatPolls["malformed-pretrusted-chat"] = ChatPollState{
			ChatID: "malformed-pretrusted-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed malformed pretrusted fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Model a legacy writer that left a matching positive generation after
	// replacing the canonical envelope. The normal JSON trigger revokes trust
	// on the replacement itself; the final projection_trusted update recreates
	// the adversarial state that the backfill must repair.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`,
			[]byte(`{"id":"malformed-pretrusted-session","teams_chat_id":`), "malformed-pretrusted-session"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET canonical_revision = 99, projection_revision = 99, projection_trusted = 0 WHERE id = ?`,
			"malformed-pretrusted-session"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET projection_trusted = 1 WHERE id = ?`, "malformed-pretrusted-session"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, "0", sqliteSessionProjectionVersionKey)
		return err
	})

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return backfillSQLiteSessionDerivedColumns(db)
	}); err != nil {
		t.Fatalf("malformed session backfill: %v", err)
	}
	var trusted int
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT projection_trusted FROM sessions WHERE id = ?`, "malformed-pretrusted-session").Scan(&trusted)
	}); err != nil {
		t.Fatalf("read malformed session trust: %v", err)
	}
	if trusted != 0 {
		t.Fatalf("malformed session projection_trusted=%d, want revoked", trusted)
	}
}

func TestSQLiteHotPollScheduleProjectionMarkerSurvivesFullStateWrite(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i <= 2*sqliteProjectionBackfillBatchSize; i++ {
			chatID := fmt.Sprintf("schedule-marker-chat-%03d", i)
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed schedule marker fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	// Migration/open validation drains this tiny fixture in short bounded pages
	// so the test starts from the normal trusted fast path. Rewind only the
	// schedule projection below to model an interrupted upgrade during a later
	// full-state write.
	for i := 0; i < 4; i++ {
		if err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return sql.ErrNoRows
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			return ensureSQLiteSchema(db)
		}); err != nil {
			t.Fatalf("finish schedule projection backfill: %v", err)
		}
	}

	readMeta := func(key string) string {
		t.Helper()
		var value string
		if err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return sql.ErrNoRows
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			err = db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, key).Scan(&value)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}); err != nil {
			t.Fatalf("read state metadata %q: %v", key, err)
		}
		return value
	}
	if got := readMeta(sqliteChatPollScheduleProjectionVersionKey); got != sqliteChatPollScheduleProjectionVersion {
		t.Fatalf("initial schedule marker=%q, want %q", got, sqliteChatPollScheduleProjectionVersion)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ServiceControl.Paused = !state.ServiceControl.Paused
		return nil
	}); err != nil {
		t.Fatalf("first unrelated full-state write: %v", err)
	}
	if got := readMeta(sqliteChatPollScheduleProjectionVersionKey); got != sqliteChatPollScheduleProjectionVersion {
		t.Fatalf("completed schedule marker after full-state write=%q, want %q", got, sqliteChatPollScheduleProjectionVersion)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, "0", sqliteChatPollScheduleProjectionVersionKey); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteBackfillCursorKey(sqliteChatPollScheduleProjectionVersionKey), sqliteBackfillCursorValue(sqliteChatPollScheduleProjectionVersion, "schedule-marker-chat-000"))
		return err
	})
	if err := store.Update(ctx, func(state *State) error {
		state.ServiceControl.Paused = !state.ServiceControl.Paused
		return nil
	}); err != nil {
		t.Fatalf("unrelated full-state write: %v", err)
	}
	if got := readMeta(sqliteChatPollScheduleProjectionVersionKey); got != "0" {
		t.Fatalf("in-progress schedule marker after full-state write=%q, want 0", got)
	}
	if got := readMeta(sqliteBackfillCursorKey(sqliteChatPollScheduleProjectionVersionKey)); got != sqliteBackfillCursorValue(sqliteChatPollScheduleProjectionVersion, "schedule-marker-chat-000") {
		t.Fatalf("in-progress schedule cursor after full-state write=%q, want original cursor preserved without hot-path backfill", got)
	}
	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("explicit schema preparation after preserved cursor: %v", err)
	}
	if got := readMeta(sqliteChatPollScheduleProjectionVersionKey); got != sqliteChatPollScheduleProjectionVersion {
		t.Fatalf("explicit schema preparation did not drain the preserved schedule cursor: %q", got)
	}
	if got := readMeta(sqliteBackfillCursorKey(sqliteChatPollScheduleProjectionVersionKey)); got != "" {
		t.Fatalf("completed schedule preparation retained cursor: %q", got)
	}
}

func TestSQLiteHotPollTrustedWorkCandidatesMatchJSONOracle(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["scalar-ordinary"] = SessionContext{ID: "scalar-ordinary", Status: SessionStatusActive, TeamsChatID: "scalar-ordinary-chat", CodexThreadID: "scalar-thread-must-hydrate-later", UpdatedAt: now}
		state.ChatPolls["scalar-ordinary-chat"] = ChatPollState{ChatID: "scalar-ordinary-chat", Seeded: true, PollState: chatPollStateWarm, NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now}
		state.Sessions["scalar-retry"] = SessionContext{ID: "scalar-retry", Status: SessionStatusActive, TeamsChatID: "scalar-retry-chat", UpdatedAt: now}
		state.ChatPolls["scalar-retry-chat"] = ChatPollState{ChatID: "scalar-retry-chat", Seeded: true, PollState: chatPollStateCold, NextPollAt: now.Add(-time.Minute), LastActivityAt: now, FailureCount: 1, UpdatedAt: now}
		state.Sessions["scalar-operational"] = SessionContext{ID: "scalar-operational", Status: SessionStatusActive, TeamsChatID: "scalar-operational-chat", UpdatedAt: now}
		state.ChatPolls["scalar-operational-chat"] = ChatPollState{ChatID: "scalar-operational-chat", Seeded: true, PollState: chatPollStateCold, NextPollAt: now.Add(-time.Minute), LastActivityAt: now, ContinuationPath: "/chats/scalar-operational-chat/messages?$skiptoken=next", UpdatedAt: now}
		state.Sessions["scalar-pending"] = SessionContext{ID: "scalar-pending", Status: SessionStatusActive, TeamsChatID: "scalar-pending-chat", UpdatedAt: now}
		state.ChatPolls["scalar-pending-chat"] = ChatPollState{ChatID: "scalar-pending-chat", Seeded: true, PollState: chatPollStateCold, NextPollAt: now.Add(-time.Minute), LastActivityAt: now, PendingPage: &ChatPollPendingPage{ReceiptID: "scalar-pending-receipt", ChatID: "scalar-pending-chat", RequestPath: "/chats/scalar-pending-chat/messages?$top=1", Frontier: "head"}, UpdatedAt: now}
		state.Sessions["scalar-idle"] = SessionContext{ID: "scalar-idle", Status: SessionStatusActive, TeamsChatID: "scalar-idle-chat", UpdatedAt: now.Add(-2 * time.Hour)}
		state.ChatPolls["scalar-idle-chat"] = ChatPollState{ChatID: "scalar-idle-chat", Seeded: true, PollState: chatPollStateCold, NextPollAt: now.Add(-time.Minute), LastActivityAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)}
		state.Sessions["scalar-running"] = SessionContext{ID: "scalar-running", Status: SessionStatusActive, TeamsChatID: "scalar-running-chat", UpdatedAt: now.Add(-2 * time.Hour)}
		state.ChatPolls["scalar-running-chat"] = ChatPollState{ChatID: "scalar-running-chat", Seeded: true, PollState: chatPollStateCold, NextPollAt: now.Add(-time.Minute), LastActivityAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)}
		state.Turns["scalar-running-turn"] = Turn{ID: "scalar-running-turn", SessionID: "scalar-running", Status: TurnStatusRunning, CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute)}
		// There is no durable poll row for this active session. It remains an
		// ordinary candidate and exercises the NULL-side of the scalar join.
		state.Sessions["scalar-no-poll"] = SessionContext{ID: "scalar-no-poll", Status: SessionStatusActive, TeamsChatID: "scalar-no-poll-chat", UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed scalar work fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var trusted, oracle []SessionContext
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		trusted, err = loadSQLiteHotPollWorkCandidatesTrusted(ctx, db, "control-chat", now.Add(-time.Hour), now, sqliteHotPollReadyLimit)
		if err != nil {
			return err
		}
		oracle, err = loadSQLiteHotPollWorkCandidatesLegacy(ctx, db, "control-chat", now.Add(-time.Hour), now, sqliteHotPollReadyLimit)
		return err
	}); err != nil {
		t.Fatalf("load trusted/oracle candidates: %v", err)
	}
	set := func(values []SessionContext) map[string]bool {
		out := make(map[string]bool, len(values))
		for _, value := range values {
			out[value.ID] = true
		}
		return out
	}
	got, want := set(trusted), set(oracle)
	if len(got) != len(want) {
		t.Fatalf("trusted candidates=%#v oracle=%#v", got, want)
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("trusted candidates=%#v omitted oracle candidate %q", got, id)
		}
	}
	for i := range trusted {
		if trusted[i].ID != oracle[i].ID {
			t.Fatalf("trusted candidate order=%v, oracle order=%v", candidateIDs(trusted), candidateIDs(oracle))
		}
	}
	for _, candidate := range trusted {
		if candidate.ID == "scalar-ordinary" && candidate.CodexThreadID != "" {
			t.Fatalf("trusted candidate decoded canonical session JSON before final selection: %#v", candidate)
		}
	}
	if got["scalar-idle"] {
		t.Fatalf("trusted candidates admitted idle chat: %#v", got)
	}
	for _, id := range []string{"scalar-ordinary", "scalar-retry", "scalar-operational", "scalar-pending", "scalar-running", "scalar-no-poll"} {
		if !got[id] {
			t.Fatalf("trusted candidates omitted expected %q: %#v", id, got)
		}
	}
}

func candidateIDs(candidates []SessionContext) []string {
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.ID)
	}
	return ids
}

func TestSQLiteHotPollSelectedRefreshHydratesFreshSession(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["selected-session-fresh"] = SessionContext{
			ID: "selected-session-fresh", Status: SessionStatusActive,
			TeamsChatID: "selected-chat-fresh", UpdatedAt: now,
		}
		state.ChatPolls["selected-chat-fresh"] = ChatPollState{
			ChatID: "selected-chat-fresh", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed selected fresh session: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.Update(ctx, func(state *State) error {
		session := state.Sessions["selected-session-fresh"]
		session.Status = SessionStatusClosed
		session.TeamsChatID = "selected-chat-fresh-closed"
		session.UpdatedAt = now.Add(time.Minute)
		state.Sessions[session.ID] = session
		return nil
	}); err != nil {
		t.Fatalf("mutate selected session: %v", err)
	}
	refreshed, handled, err := store.HotPollSelectedStateForChatsAndSessions(ctx,
		[]string{"selected-chat-fresh"}, []string{"selected-session-fresh"})
	if err != nil || !handled {
		t.Fatalf("selected fresh session refresh handled=%v err=%v", handled, err)
	}
	got, ok := refreshed.Sessions["selected-session-fresh"]
	if !ok || got.Status != SessionStatusClosed || got.TeamsChatID != "selected-chat-fresh-closed" {
		t.Fatalf("selected fresh session=%#v present=%v, want closed/rebound session", got, ok)
	}
}

func TestSQLiteTurnProjectionBackfillStopsAtDurablePageBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	const total = sqliteProjectionBackfillBatchSize + 1
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < total; i++ {
			id := fmt.Sprintf("turn-backfill-%03d", i)
			state.Turns[id] = Turn{ID: id, SessionID: "turn-backfill-session", Status: TurnStatusCompleted, CreatedAt: now, UpdatedAt: now}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed turn backfill rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE turns SET projection_trusted = 0, projection_revision = 0, canonical_revision = 0`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteTurnProjectionVersionKey, "0")
		return err
	})
	// Re-run the bounded migration directly. One page must leave a cursor and
	// not claim completion while the final row remains.
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return backfillSQLiteTurnDerivedColumns(db)
	}); err != nil {
		t.Fatalf("first bounded turn backfill: %v", err)
	}
	var marker string
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteTurnProjectionVersionKey).Scan(&marker)
	}); err != nil {
		t.Fatal(err)
	}
	if marker == sqliteTurnProjectionVersion {
		t.Fatalf("turn projection marker reached %q before final row", marker)
	}
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return backfillSQLiteTurnDerivedColumns(db)
	}); err != nil {
		t.Fatalf("second bounded turn backfill: %v", err)
	}
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteTurnProjectionVersionKey).Scan(&marker)
	}); err != nil {
		t.Fatal(err)
	}
	if marker != sqliteTurnProjectionVersion {
		t.Fatalf("turn projection marker=%q after final page, want %q", marker, sqliteTurnProjectionVersion)
	}
}

func TestSQLiteProjectionBackfillRepublishesPositiveRevisionAsTrusted(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["repair-session"] = SessionContext{
			ID: "repair-session", Status: SessionStatusActive,
			TeamsChatID: "repair-chat", UpdatedAt: now,
		}
		state.ChatPolls["repair-chat"] = ChatPollState{
			ChatID: "repair-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		state.Turns["repair-turn"] = Turn{
			ID: "repair-turn", SessionID: "repair-session",
			Status: TurnStatusCompleted, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed projection repair fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Simulate a legacy projection publication that has a positive row-local
	// generation but lost its trust bit. Backfill must publish a new row-local
	// generation; reusing the old generation would trip the admission trigger
	// and leave the valid row permanently on the JSON fallback.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, key := range []string{
			sqliteSessionProjectionVersionKey,
			sqliteTurnProjectionVersionKey,
			sqliteChatPollFrontierHintVersionKey,
			sqliteChatPollProjectionVersionKey,
		} {
			if _, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, "0"); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET projection_trusted = 0
WHERE id = ?`, "repair-session")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE turns SET projection_trusted = 0
WHERE id = ?`, "repair-turn"); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE chat_polls SET projection_trusted = 0
WHERE chat_id = ?`, "repair-chat")
		return err
	})

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if err := backfillSQLiteSessionDerivedColumns(db); err != nil {
			return err
		}
		if err := backfillSQLiteTurnDerivedColumns(db); err != nil {
			return err
		}
		return backfillSQLiteChatPollDerivedColumns(db)
	}); err != nil {
		t.Fatalf("republish positive-revision projections: %v", err)
	}

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		assertTrusted := func(table, keyColumn, key string) error {
			var trusted, canonical, projection int64
			if err := db.QueryRowContext(ctx, `SELECT projection_trusted, canonical_revision, projection_revision
FROM `+table+` WHERE `+keyColumn+` = ?`, key).Scan(&trusted, &canonical, &projection); err != nil {
				return err
			}
			if trusted != 1 || canonical <= 0 || projection != canonical {
				return fmt.Errorf("%s %s=%q trusted=%d canonical=%d projection=%d; want trusted matching positive generation",
					table, keyColumn, key, trusted, canonical, projection)
			}
			return nil
		}
		if err := assertTrusted("sessions", "id", "repair-session"); err != nil {
			return err
		}
		if err := assertTrusted("turns", "id", "repair-turn"); err != nil {
			return err
		}
		return assertTrusted("chat_polls", "chat_id", "repair-chat")
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHotPollScalarProjectionTriggerDefinitionsAreCurrent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["trigger-session"] = SessionContext{ID: "trigger-session", Status: SessionStatusActive, TeamsChatID: "trigger-chat"}
		state.ChatPolls["trigger-chat"] = ChatPollState{ChatID: "trigger-chat", Seeded: true}
		state.Turns["trigger-turn"] = Turn{ID: "trigger-turn", SessionID: "trigger-session", Status: TurnStatusCompleted}
		return nil
	}); err != nil {
		t.Fatalf("seed trigger state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		current, err := sqliteAdmissionProjectionTriggersCurrent(db)
		if err != nil {
			return err
		}
		if !current {
			return fmt.Errorf("admission projection trigger definition is stale")
		}
		var turnTrusted int64
		if err := db.QueryRowContext(ctx, `SELECT projection_trusted FROM turns WHERE id = ?`, "trigger-turn").Scan(&turnTrusted); err != nil {
			return err
		}
		if turnTrusted != 1 {
			return fmt.Errorf("turn projection trust=%d, want 1", turnTrusted)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(sqliteTurnProjectionVersion) == "" {
		t.Fatal("turn projection version must be non-empty")
	}
}

func TestSQLiteHotPollScalarProjectionRepairsWrongTriggerBody(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["trigger-repair-session"] = SessionContext{
			ID: "trigger-repair-session", Status: SessionStatusActive, TeamsChatID: "trigger-repair-chat",
		}
		state.ChatPolls["trigger-repair-chat"] = ChatPollState{ChatID: "trigger-repair-chat", Seeded: true}
		return nil
	}); err != nil {
		t.Fatalf("seed trigger repair state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if _, err := db.Exec(`DROP TRIGGER IF EXISTS sessions_admission_projection_v1`); err != nil {
			return err
		}
		if _, err := db.Exec(`CREATE TRIGGER sessions_admission_projection_v1 AFTER UPDATE OF json ON sessions BEGIN SELECT 1; END`); err != nil {
			return err
		}
		current, err := sqliteAdmissionProjectionTriggersCurrent(db)
		if err != nil {
			return err
		}
		if current {
			return fmt.Errorf("wrong trigger body was accepted as current")
		}
		if err := ensureSQLiteAdmissionProjectionTriggers(db); err != nil {
			return err
		}
		current, err = sqliteAdmissionProjectionTriggersCurrent(db)
		if err != nil {
			return err
		}
		if !current {
			return fmt.Errorf("wrong trigger body was not repaired")
		}
		for _, key := range []string{
			sqliteSessionProjectionVersionKey,
			sqliteTurnProjectionVersionKey,
			sqliteChatPollFrontierHintVersionKey,
			sqliteChatPollProjectionVersionKey,
			sqliteChatPollScheduleProjectionVersionKey,
			sqliteInboundProjectionVersionKey,
		} {
			var marker string
			if markerErr := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, key).Scan(&marker); !errors.Is(markerErr, sql.ErrNoRows) {
				return fmt.Errorf("trigger repair left projection marker %q=%q/%v; want fail-closed invalidation", key, marker, markerErr)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHotPollScalarTrustRevokesContradictoryJSONWithMatchingRevision(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["semantic-trust-session"] = SessionContext{
			ID: "semantic-trust-session", Status: SessionStatusActive,
			TeamsChatID: "semantic-trust-chat", UpdatedAt: now,
		}
		state.ChatPolls["semantic-trust-chat"] = ChatPollState{
			ChatID: "semantic-trust-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		state.Turns["semantic-trust-turn"] = Turn{
			ID: "semantic-trust-turn", SessionID: "semantic-trust-session",
			Status: TurnStatusCompleted, CreatedAt: now, UpdatedAt: now,
		}
		state.Sessions["semantic-key-session"] = SessionContext{
			ID: "semantic-key-session", Status: SessionStatusActive,
			TeamsChatID: "semantic-key-chat", UpdatedAt: now,
		}
		state.ChatPolls["semantic-key-chat"] = ChatPollState{
			ChatID: "semantic-key-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		state.Turns["semantic-key-turn"] = Turn{
			ID: "semantic-key-turn", SessionID: "semantic-key-session",
			Status: TurnStatusCompleted, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed semantic trust fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// Each update models a mixed-version writer that deliberately republishes
		// matching positive generations and the trust bit. Revision equality by
		// itself must not authorize a scalar row whose canonical identity/status
		// or admission state disagrees with the indexed projection.
		if _, err := tx.ExecContext(ctx, `UPDATE sessions
SET json = json_set(json, '$.status', 'closed'),
    canonical_revision = canonical_revision + 1,
    projection_revision = canonical_revision + 1,
    projection_trusted = 1
WHERE id = ?`, "semantic-trust-session"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE chat_polls
SET json = json_set(json, '$.state', 'cold'),
    canonical_revision = canonical_revision + 1,
    projection_revision = canonical_revision + 1,
    projection_trusted = 1
WHERE chat_id = ?`, "semantic-trust-chat"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE turns
SET json = json_set(json, '$.session_id', 'foreign-session'),
    canonical_revision = canonical_revision + 1,
    projection_revision = canonical_revision + 1,
    projection_trusted = 1
WHERE id = ?`, "semantic-trust-turn")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions
SET id = ?, canonical_revision = canonical_revision + 1,
    projection_revision = canonical_revision + 1, projection_trusted = 1
WHERE id = ?`, "semantic-key-session-renamed", "semantic-key-session"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE chat_polls
SET chat_id = ?, canonical_revision = canonical_revision + 1,
    projection_revision = canonical_revision + 1, projection_trusted = 1
WHERE chat_id = ?`, "semantic-key-chat-renamed", "semantic-key-chat"); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE turns
SET id = ?, canonical_revision = canonical_revision + 1,
    projection_revision = canonical_revision + 1, projection_trusted = 1
WHERE id = ?`, "semantic-key-turn-renamed", "semantic-key-turn")
		return err
	})

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		checks := []struct {
			table, keyColumn, key string
		}{
			{"sessions", "id", "semantic-trust-session"},
			{"chat_polls", "chat_id", "semantic-trust-chat"},
			{"turns", "id", "semantic-trust-turn"},
			{"sessions", "id", "semantic-key-session-renamed"},
			{"chat_polls", "chat_id", "semantic-key-chat-renamed"},
			{"turns", "id", "semantic-key-turn-renamed"},
		}
		for _, check := range checks {
			var trusted int
			if err := db.QueryRowContext(ctx, `SELECT projection_trusted FROM `+check.table+` WHERE `+check.keyColumn+` = ?`, check.key).Scan(&trusted); err != nil {
				return err
			}
			if trusted != 0 {
				return fmt.Errorf("%s %s=%q remained trusted after contradictory JSON", check.table, check.keyColumn, check.key)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHotPollTrustedReadyUsesCanonicalTimeForDueGate(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls["canonical-time-chat"] = ChatPollState{
			ChatID: "canonical-time-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed canonical time fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// Keep scalar schedule columns and matching generations unchanged while
		// changing only the canonical deadline. The trusted query must use the
		// canonical due expression for correctness; scalar ordering columns are
		// still only a fairness hint.
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls
SET json = json_set(json, '$.next_poll_at', '2026-09-13T13:00:00Z'),
    canonical_revision = canonical_revision + 1,
    projection_revision = canonical_revision + 1,
    projection_trusted = 1
WHERE chat_id = ?`, "canonical-time-chat")
		return err
	})

	var ids []string
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		ids, err = loadSQLiteHotPollReadyChatIDsTrusted(ctx, db, "", now, sqliteHotPollReadyLimit)
		return err
	}); err != nil {
		t.Fatalf("trusted canonical time admission: %v", err)
	}
	for _, id := range ids {
		if id == "canonical-time-chat" {
			t.Fatalf("future canonical deadline entered trusted ready lane: %#v", ids)
		}
	}
}

func TestSQLiteHotPollAdmissionFallbackKeepsHealthyRowsOnScalarLane(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		for _, suffix := range []string{"healthy", "legacy"} {
			sessionID := "fallback-session-" + suffix
			chatID := "fallback-chat-" + suffix
			state.Sessions[sessionID] = SessionContext{
				ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
			}
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed mixed fallback fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	raw := sqliteRawChatPollJSONForTest(t, store, "fallback-chat-legacy")
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return err
		}
		object["state"] = json.RawMessage(`"cold"`)
		changed, err := json.Marshal(object)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, changed, "fallback-chat-legacy")
		return err
	})

	candidates, handled, err := store.HotPollWorkCandidates(ctx, "control-chat")
	if err != nil || !handled {
		t.Fatalf("mixed fallback candidates handled=%v err=%v", handled, err)
	}
	seen := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		seen[candidate.ID] = true
	}
	for _, id := range []string{"fallback-session-healthy", "fallback-session-legacy"} {
		if !seen[id] {
			t.Fatalf("mixed fallback omitted %q: %#v", id, seen)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("mixed fallback returned duplicate or unrelated candidates: %#v", seen)
	}
}

func TestSQLiteHotPollWorkAdmissionIgnoresUnroutableUntrustedSessions(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	const (
		healthySessionID = "work-admission-healthy-session"
		healthyChatID    = "work-admission-healthy-chat"
		stagingSessionID = "work-admission-staging-session"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[healthySessionID] = SessionContext{
			ID: healthySessionID, Status: SessionStatusActive, TeamsChatID: healthyChatID, UpdatedAt: now,
		}
		state.ChatPolls[healthyChatID] = ChatPollState{
			ChatID: healthyChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		// Fork/control staging sessions are durable scheduler metadata, but they
		// intentionally have no Teams chat identity until publication. They must
		// not force every work admission into the table-sized JSON compatibility
		// scan.
		state.Sessions[stagingSessionID] = SessionContext{
			ID: stagingSessionID, Status: SessionStatusStaging, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed unroutable staging fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// Make the auxiliary row explicitly enter the residual compatibility
		// lane. The test must prove that an untrusted row with no routable
		// identity is still irrelevant to work-chat admission.
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET projection_trusted = 0 WHERE id = ?`, stagingSessionID)
		return err
	})

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		versionsCurrent, err := sqliteHotPollAdmissionVersionsCurrent(ctx, db, true)
		if err != nil {
			return err
		}
		if !versionsCurrent {
			return errors.New("test fixture did not publish current hot-poll projection markers")
		}
		needsFallback, err := sqliteHotPollAdmissionNeedsFallbackWithVersions(ctx, db, true, versionsCurrent)
		if err != nil {
			return err
		}
		if needsFallback {
			return errors.New("unroutable staging session forced work-candidate fallback")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("work admission: %v", err)
	}
	if len(admission.Candidates) != 1 || admission.Candidates[0].ID != healthySessionID {
		t.Fatalf("work candidates=%#v, want only healthy session %q", admission.Candidates, healthySessionID)
	}
}

func TestSQLiteHotPollWorkAdmissionKeepsRoutableUntrustedSessionFallback(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	const (
		sessionID = "work-admission-routable-untrusted-session"
		chatID    = "work-admission-routable-untrusted-chat"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{
			ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
		}
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed routable session fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// The canonical identity remains routable while its scalar binding is
		// blank. This is exactly the mixed-version case that must keep the JSON
		// oracle alive; otherwise the scalar lane could silently omit the chat.
		raw := []byte(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":"active"}`, sessionID, chatID))
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ?, teams_chat_id = NULL, projection_trusted = 0 WHERE id = ?`, raw, sessionID)
		return err
	})

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		versionsCurrent, err := sqliteHotPollAdmissionVersionsCurrent(ctx, db, true)
		if err != nil {
			return err
		}
		if !versionsCurrent {
			return errors.New("test fixture did not publish current hot-poll projection markers")
		}
		needsFallback, err := sqliteHotPollAdmissionNeedsFallbackWithVersions(ctx, db, true, versionsCurrent)
		if err != nil {
			return err
		}
		if !needsFallback {
			return errors.New("routable untrusted session bypassed work-candidate fallback")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHotPollCanonicalFallbackReleasesStoreLockDuringRead(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["hot-poll-fallback-healthy-session"] = SessionContext{
			ID: "hot-poll-fallback-healthy-session", Status: SessionStatusActive,
			TeamsChatID: "hot-poll-fallback-healthy-chat", UpdatedAt: now,
		}
		state.ChatPolls["hot-poll-fallback-healthy-chat"] = ChatPollState{
			ChatID: "hot-poll-fallback-healthy-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		state.Sessions["hot-poll-fallback-legacy-session"] = SessionContext{
			ID: "hot-poll-fallback-legacy-session", Status: SessionStatusActive,
			TeamsChatID: "hot-poll-fallback-legacy-chat", UpdatedAt: now,
		}
		state.ChatPolls["hot-poll-fallback-legacy-chat"] = ChatPollState{
			ChatID: "hot-poll-fallback-legacy-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(time.Hour), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed canonical fallback lock fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	raw := sqliteRawChatPollJSONForTest(t, store, "hot-poll-fallback-legacy-chat")
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode legacy poll JSON: %v", err)
	}
	object["legacy_noise"] = json.RawMessage(`"mixed-version"`)
	changed, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal legacy poll JSON: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, changed, "hot-poll-fallback-legacy-chat")
		return err
	})

	opened := make(chan struct{})
	release := make(chan struct{})
	var openedOnce sync.Once
	var releaseOnce sync.Once
	previousHook := sqliteHotPollCanonicalFallbackTestHook
	sqliteHotPollCanonicalFallbackTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		openedOnce.Do(func() {
			close(opened)
			<-release
		})
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		sqliteHotPollCanonicalFallbackTestHook = previousHook
	})

	admissionDone := make(chan struct{})
	var admissionState State
	var admissionCandidates []SessionContext
	var admissionHandled bool
	var admissionErr error
	go func() {
		admissionState, admissionCandidates, admissionHandled, admissionErr = store.HotPollScheduleAndWorkCandidatesExcludingIdleAt(
			ctx, "", now.Add(-time.Hour), now,
		)
		close(admissionDone)
	}()
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("canonical fallback did not open an independent reader")
	}

	// The canonical scan is held before its first query. A durable schedule write
	// must still complete; if this blocks, the fallback has retained Store.mu or
	// the cross-process state lock despite using a query-only handle.
	updateDone := make(chan error, 1)
	go func() {
		_, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
			ChatID: "hot-poll-fallback-healthy-chat", PollState: chatPollStateWarm,
			NextPollAt: now.Add(-2 * time.Minute), LastActivityAt: now,
		})
		updateDone <- err
	}()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("durable schedule write during canonical fallback: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("durable schedule write waited behind canonical fallback")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-admissionDone:
	case <-time.After(10 * time.Second):
		t.Fatal("canonical fallback admission did not finish")
	}
	if admissionErr != nil || !admissionHandled {
		t.Fatalf("canonical fallback admission: %v", admissionErr)
	}
	if len(admissionCandidates) != 1 || admissionCandidates[0].TeamsChatID != "hot-poll-fallback-healthy-chat" {
		t.Fatalf("canonical fallback admission candidates = %#v, want healthy candidate", admissionCandidates)
	}
	if _, ok := admissionState.ChatPolls["hot-poll-fallback-healthy-chat"]; !ok {
		t.Fatalf("canonical fallback admission omitted healthy schedule: %#v", admissionState.ChatPolls)
	}
}

func TestSQLiteHotPollReadyCanonicalFallbackReleasesStoreLockDuringRead(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	const healthyChat = "ready-fallback-healthy-chat"
	const legacyChat = "ready-fallback-legacy-chat"
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["ready-fallback-healthy-session"] = SessionContext{
			ID: "ready-fallback-healthy-session", Status: SessionStatusActive,
			TeamsChatID: healthyChat, UpdatedAt: now,
		}
		state.ChatPolls[healthyChat] = ChatPollState{
			ChatID: healthyChat, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		state.Sessions["ready-fallback-legacy-session"] = SessionContext{
			ID: "ready-fallback-legacy-session", Status: SessionStatusActive,
			TeamsChatID: legacyChat, UpdatedAt: now,
		}
		state.ChatPolls[legacyChat] = ChatPollState{
			ChatID: legacyChat, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(time.Hour), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed ready fallback fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	raw := sqliteRawChatPollJSONForTest(t, store, legacyChat)
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode legacy ready poll: %v", err)
	}
	object["legacy_noise"] = json.RawMessage(`"mixed-version"`)
	changed, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal legacy ready poll: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, changed, legacyChat)
		return err
	})

	opened := make(chan struct{})
	release := make(chan struct{})
	var openedOnce sync.Once
	previousHook := sqliteHotPollCanonicalFallbackTestHook
	sqliteHotPollCanonicalFallbackTestHook = func(stage string) {
		if stage != "ready-opened" {
			return
		}
		openedOnce.Do(func() {
			close(opened)
			<-release
		})
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		sqliteHotPollCanonicalFallbackTestHook = previousHook
	})

	resultDone := make(chan struct{})
	var result State
	var resultErr error
	go func() {
		result, resultErr = store.HotPollReadyScheduleState(ctx, "control-chat", now)
		close(resultDone)
	}()
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("ready canonical fallback did not open an independent reader")
	}
	updateDone := make(chan error, 1)
	go func() {
		_, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
			ChatID: healthyChat, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-2 * time.Minute), LastActivityAt: now,
		})
		updateDone <- err
	}()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("ready schedule write during canonical fallback: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ready schedule write waited behind canonical fallback")
	}
	close(release)
	select {
	case <-resultDone:
	case <-time.After(10 * time.Second):
		t.Fatal("ready canonical fallback did not finish")
	}
	if resultErr != nil {
		t.Fatalf("ready canonical fallback: %v", resultErr)
	}
	if _, ok := result.ChatPolls[healthyChat]; !ok {
		t.Fatalf("ready canonical fallback omitted healthy chat: %#v", result.ChatPolls)
	}
}

func TestSQLiteHotPollStandaloneCanonicalFallbackReleasesStoreLockDuringRead(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	const healthyChat = "standalone-fallback-healthy-chat"
	const legacyChat = "standalone-fallback-legacy-chat"
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["standalone-fallback-healthy-session"] = SessionContext{
			ID: "standalone-fallback-healthy-session", Status: SessionStatusActive,
			TeamsChatID: healthyChat, UpdatedAt: now,
		}
		state.ChatPolls[healthyChat] = ChatPollState{
			ChatID: healthyChat, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		state.Sessions["standalone-fallback-legacy-session"] = SessionContext{
			ID: "standalone-fallback-legacy-session", Status: SessionStatusActive,
			TeamsChatID: legacyChat, UpdatedAt: now,
		}
		state.ChatPolls[legacyChat] = ChatPollState{
			ChatID: legacyChat, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(time.Hour), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed standalone fallback fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	raw := sqliteRawChatPollJSONForTest(t, store, legacyChat)
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode standalone legacy poll: %v", err)
	}
	object["legacy_noise"] = json.RawMessage(`"mixed-version"`)
	changed, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal standalone legacy poll: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, changed, legacyChat)
		return err
	})

	opened := make(chan struct{})
	release := make(chan struct{})
	var openedOnce sync.Once
	previousHook := sqliteHotPollCanonicalFallbackTestHook
	sqliteHotPollCanonicalFallbackTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		openedOnce.Do(func() {
			close(opened)
			<-release
		})
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		sqliteHotPollCanonicalFallbackTestHook = previousHook
	})

	admissionDone := make(chan struct{})
	var admission HotPollWorkAdmission
	var admissionErr error
	go func() {
		admission, admissionErr = store.HotPollWorkCandidatesWithDispositionExcludingIdleAtLimit(
			ctx, "control-chat", time.Time{}, now, 1,
		)
		close(admissionDone)
	}()
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("standalone canonical fallback did not open an independent reader")
	}
	updateDone := make(chan error, 1)
	go func() {
		_, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
			ChatID: healthyChat, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-2 * time.Minute), LastActivityAt: now,
		})
		updateDone <- err
	}()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("standalone schedule write during canonical fallback: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("standalone schedule write waited behind canonical fallback")
	}
	close(release)
	select {
	case <-admissionDone:
	case <-time.After(10 * time.Second):
		t.Fatal("standalone canonical fallback did not finish")
	}
	if admissionErr != nil {
		t.Fatalf("standalone canonical fallback: %v", admissionErr)
	}
	if admission.Disposition == HotPollWorkAdmissionLegacyCompatible || len(admission.Candidates) != 1 || admission.Candidates[0].TeamsChatID != healthyChat {
		t.Fatalf("standalone canonical fallback admission = %#v, want healthy candidate", admission)
	}
}

func TestSQLiteHotPollTrustedWorkCandidatesQuarantinesBadRowsWithoutDeadlock(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["trusted-deadlock-session"] = SessionContext{
			ID: "trusted-deadlock-session", Status: SessionStatusActive,
			TeamsChatID: "trusted-deadlock-chat", UpdatedAt: now,
		}
		state.ChatPolls["trusted-deadlock-chat"] = ChatPollState{
			ChatID: "trusted-deadlock-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed trusted deadlock fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Simulate a mixed-version writer that changed the canonical session JSON.
	// The existing projection trigger revokes trust; the scalar lane must
	// consequently return no candidate and must not try to write while its
	// SELECT is open. Final selected hydration remains the canonical safety
	// barrier for any stale trust bit restored by an older writer.
	raw := sqliteRawSessionJSONForTest(t, store, "trusted-deadlock-session")
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode trusted deadlock session: %v", err)
	}
	object["teams_chat_id"] = "trusted-deadlock-chat-replaced"
	changed, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal trusted deadlock session: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, changed, "trusted-deadlock-session"); err != nil {
			return err
		}
		return nil
	})

	deadlineCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var candidates []SessionContext
	if err := store.withStateLock(deadlineCtx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		candidates, err = loadSQLiteHotPollWorkCandidatesTrusted(deadlineCtx, db, "control-chat", now.Add(-time.Hour), now, sqliteHotPollReadyLimit)
		return err
	}); err != nil {
		t.Fatalf("trusted candidate quarantine: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("quarantined mismatched session was admitted: %#v", candidates)
	}
	var trusted int64
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT projection_trusted FROM sessions WHERE id = ?`, "trusted-deadlock-session").Scan(&trusted)
	}); err != nil {
		t.Fatal(err)
	}
	if trusted != 0 {
		t.Fatalf("mismatched session trust=%d, want 0", trusted)
	}
}

func TestSQLiteHotPollTrustedWorkCandidatesContinuesPastInvalidPage(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	_, ordinaryLimit := sqliteHotPollLaneLimits(sqliteHotPollReadyLimit)
	badCount := hotPollInvalidPageBadCount()
	healthyCount := hotPollInvalidPageHealthyCount()
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < badCount+healthyCount; i++ {
			id := fmt.Sprintf("invalid-page-session-%03d", i)
			chatID := fmt.Sprintf("invalid-page-chat-%03d", i)
			state.Sessions[id] = SessionContext{
				ID: id, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
			}
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed invalid-page fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Make the first keyset page consist entirely of rows whose scalar shape
	// still looks admissible but whose canonical JSON is unusable. The canonical
	// write revokes trust, so the bounded JSON recovery lane must skip these rows
	// and still reach healthy rows after the malformed prefix.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for i := 0; i < badCount; i++ {
			id := fmt.Sprintf("invalid-page-session-%03d", i)
			if _, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, []byte(`[]`), id); err != nil {
				return err
			}
		}
		return nil
	})
	candidates, handled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, "control-chat", now.Add(-time.Hour), now)
	if err != nil || !handled {
		t.Fatalf("invalid-page candidates handled=%v err=%v", handled, err)
	}
	badIDs := make(map[string]bool, badCount)
	for i := 0; i < badCount; i++ {
		badIDs[fmt.Sprintf("invalid-page-session-%03d", i)] = true
	}
	seen := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		seen[candidate.ID] = true
		if badIDs[candidate.ID] {
			t.Fatalf("invalid canonical session was admitted: %#v", candidate)
		}
	}
	wantHealthy := healthyCount
	if wantHealthy > ordinaryLimit {
		wantHealthy = ordinaryLimit
	}
	if len(seen) != wantHealthy {
		t.Fatalf("healthy candidates after invalid page=%d, want %d: %#v", len(seen), wantHealthy, seen)
	}
	for i := badCount; i < badCount+wantHealthy; i++ {
		id := fmt.Sprintf("invalid-page-session-%03d", i)
		if !seen[id] {
			t.Fatalf("invalid page prevented healthy candidate %q from admission: %#v", id, seen)
		}
	}
}

func TestSQLiteHotPollAdmissionFallbackProbeUsesPartialIndexes(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["fallback-index-session"] = SessionContext{
			ID: "fallback-index-session", Status: SessionStatusActive,
			TeamsChatID: "fallback-index-chat",
		}
		state.ChatPolls["fallback-index-chat"] = ChatPollState{
			ChatID: "fallback-index-chat", Seeded: true, PollState: chatPollStateWarm,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed fallback index fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		checks := []struct {
			name, query, index string
		}{
			{
				name:  "sessions",
				query: `EXPLAIN QUERY PLAN SELECT 1 FROM sessions WHERE COALESCE(projection_trusted, 0) != 1 OR COALESCE(canonical_revision, 0) <= 0 OR COALESCE(projection_revision, 0) != COALESCE(canonical_revision, 0) LIMIT 1`,
				index: "sessions_untrusted_generation_v2_idx",
			},
			{
				name:  "chat_polls",
				query: `EXPLAIN QUERY PLAN SELECT 1 FROM chat_polls WHERE (COALESCE(projection_trusted, 0) != 1 OR COALESCE(canonical_revision, 0) <= 0 OR COALESCE(projection_revision, 0) != COALESCE(canonical_revision, 0)) OR COALESCE(admission_valid, 0) != 1 LIMIT 1`,
				index: "chat_polls_untrusted_generation_v2_idx",
			},
		}
		for _, check := range checks {
			rows, err := db.QueryContext(ctx, check.query)
			if err != nil {
				return fmt.Errorf("%s explain: %w", check.name, err)
			}
			found := false
			details := make([]string, 0)
			for rows.Next() {
				var id, parent, detail int
				var description string
				if err := rows.Scan(&id, &parent, &detail, &description); err != nil {
					_ = rows.Close()
					return err
				}
				details = append(details, description)
				if strings.Contains(description, check.index) {
					found = true
				}
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return err
			}
			if err := rows.Close(); err != nil {
				return err
			}
			t.Logf("%s query plan: %s", check.name, strings.Join(details, " | "))
			if !found {
				return fmt.Errorf("%s fallback probe did not use %s (plan=%q)", check.name, check.index, details)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHotPollTrustedAdmissionUsesTrustedPartialIndexes(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["trusted-plan-session"] = SessionContext{
			ID: "trusted-plan-session", Status: SessionStatusActive,
			TeamsChatID: "trusted-plan-chat",
		}
		state.ChatPolls["trusted-plan-chat"] = ChatPollState{
			ChatID: "trusted-plan-chat", Seeded: true, PollState: chatPollStateWarm,
		}
		state.Turns["trusted-plan-turn"] = Turn{
			ID: "trusted-plan-turn", SessionID: "trusted-plan-session",
			Status: TurnStatusRunning,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed trusted planner fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		checks := []struct {
			name, query string
			indexes     []string
			args        []any
		}{
			{
				name: "operational chat polls",
				query: `EXPLAIN QUERY PLAN SELECT chat_id FROM chat_polls
WHERE projection_trusted = 1 AND canonical_revision > 0
  AND projection_revision = canonical_revision AND admission_valid = 1
  AND frontier_active = 1
ORDER BY updated_at, next_poll_at, last_activity_at, chat_id LIMIT 8`,
				indexes: []string{"chat_polls_trusted_operational_idx", "chat_polls_admission_idx"},
			},
			{
				name: "ordinary chat polls",
				query: `EXPLAIN QUERY PLAN SELECT chat_id FROM chat_polls
WHERE projection_trusted = 1 AND canonical_revision > 0
  AND projection_revision = canonical_revision AND admission_valid = 1
  AND frontier_active = 0
ORDER BY updated_at, next_poll_at, last_activity_at, chat_id LIMIT 8`,
				indexes: []string{"chat_polls_trusted_ordinary_idx", "chat_polls_admission_idx"},
			},
			{
				name: "trusted turns by session and status",
				query: `EXPLAIN QUERY PLAN SELECT id FROM turns
WHERE session_id = ? AND projection_trusted = 1
  AND canonical_revision > 0 AND projection_revision = canonical_revision
  AND status IN (?, ?)
ORDER BY session_id, queued_at, created_at, id`,
				indexes: []string{"turns_trusted_session_status_idx", "turns_session_status_idx"},
				args:    []any{"trusted-plan-session", TurnStatusRunning, TurnStatusQueued},
			},
		}
		for _, check := range checks {
			rows, err := db.QueryContext(ctx, check.query, check.args...)
			if err != nil {
				return fmt.Errorf("%s explain: %w", check.name, err)
			}
			found := false
			details := make([]string, 0)
			for rows.Next() {
				var id, parent, detail int
				var description string
				if err := rows.Scan(&id, &parent, &detail, &description); err != nil {
					_ = rows.Close()
					return err
				}
				details = append(details, description)
				for _, index := range check.indexes {
					if strings.Contains(description, index) {
						found = true
						break
					}
				}
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return err
			}
			if err := rows.Close(); err != nil {
				return err
			}
			t.Logf("%s query plan: %s", check.name, strings.Join(details, " | "))
			if !found {
				return fmt.Errorf("%s trusted query did not use a scalar admission index %v (plan=%q)", check.name, check.indexes, details)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHotPollCorruptWorkProbeUsesResidualSessionIndex(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["corrupt-probe-session"] = SessionContext{
			ID: "corrupt-probe-session", Status: SessionStatusActive,
			TeamsChatID: "corrupt-probe-chat", UpdatedAt: now,
		}
		state.ChatPolls["corrupt-probe-chat"] = ChatPollState{
			ChatID: "corrupt-probe-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed corrupt-probe fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = errors.New("sqlite pointer missing")
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		rows, err := db.QueryContext(ctx, `EXPLAIN QUERY PLAN `+sqliteHotPollCorruptWorkSessionsQuery(),
			"control-chat", "control-chat", string(SessionStatusActive), sqliteTime(now), sqliteTime(now), sqliteHotPollMalformedLimit)
		if err != nil {
			return err
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var selectID, parentID, notUsed int
			var detail string
			if err := rows.Scan(&selectID, &parentID, &notUsed, &detail); err != nil {
				return err
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		plan := strings.Join(details, "\n")
		if !strings.Contains(plan, "sessions_untrusted_generation_v2_idx") {
			return fmt.Errorf("corrupt-work probe did not use residual session index (plan=%q)", plan)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHotPollAdmissionRejectsStaleProjectionGeneration(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["stale-generation-session"] = SessionContext{
			ID: "stale-generation-session", Status: SessionStatusActive,
			TeamsChatID: "stale-generation-chat", UpdatedAt: now,
		}
		state.ChatPolls["stale-generation-chat"] = ChatPollState{
			ChatID: "stale-generation-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale generation fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Model a legacy/mixed-version writer that restored the trust bit while
	// leaving the scalar generation one publication behind. The current trigger
	// prevents this in normal operation; dropping it here makes the admission
	// predicate itself independently testable and proves the JSON compatibility
	// lane still recovers the canonical row.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS chat_polls_admission_projection_v1`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls
SET projection_revision = canonical_revision - 1, projection_trusted = 1
WHERE chat_id = ?`, "stale-generation-chat")
		return err
	})

	var trustedIDs, admittedIDs []string
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		trustedIDs, err = loadSQLiteHotPollReadyChatIDsTrusted(ctx, db, "control-chat", now, sqliteHotPollReadyLimit)
		if err != nil {
			return err
		}
		admittedIDs, err = loadSQLiteHotPollReadyChatIDs(ctx, db, "control-chat", now, sqliteHotPollReadyLimit)
		return err
	}); err != nil {
		t.Fatalf("stale generation admission: %v", err)
	}
	if len(trustedIDs) != 0 {
		t.Fatalf("stale generation entered scalar trusted lane: %#v", trustedIDs)
	}
	if len(admittedIDs) != 1 || admittedIDs[0] != "stale-generation-chat" {
		t.Fatalf("stale generation was not recovered by canonical lane: %#v", admittedIDs)
	}
}

func TestSQLiteHotPollWorkAdmissionRecoversStaleProjectionGeneration(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["stale-work-generation-session"] = SessionContext{
			ID: "stale-work-generation-session", Status: SessionStatusActive,
			TeamsChatID: "stale-work-generation-chat", UpdatedAt: now,
		}
		state.ChatPolls["stale-work-generation-chat"] = ChatPollState{
			ChatID: "stale-work-generation-chat", Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale work-generation fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Keep both rows' trust bits positive, but make the session publication one
	// generation behind. The ordinary legacy lane intentionally only recognizes
	// trust=0, so the dedicated stale-generation lane must be what recovers it.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS sessions_admission_projection_v1`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE sessions
SET projection_revision = canonical_revision - 1, projection_trusted = 1
WHERE id = ?`, "stale-work-generation-session")
		return err
	})

	var candidates []SessionContext
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		candidates, err = loadSQLiteHotPollWorkCandidates(ctx, db, "control-chat", now.Add(-time.Hour), now, sqliteHotPollReadyLimit)
		return err
	}); err != nil {
		t.Fatalf("stale work-generation admission: %v", err)
	}
	if len(candidates) != 1 || candidates[0].ID != "stale-work-generation-session" {
		t.Fatalf("stale work-generation candidate=%#v, want one canonical recovery candidate", candidates)
	}
}

func TestSQLiteHotPollReadyAdmissionDoesNotReserveEmptyRecoverySlot(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newTestStore(t)
	healthyCount := hotPollReadyAdmissionHealthyCount()
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < healthyCount; i++ {
			chatID := fmt.Sprintf("ready-quota-healthy-%03d", i)
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
			}
		}
		// This row makes the compatibility probe necessary, but its canonical
		// schedule is explicitly blocked in the future, so it is not an
		// admissible recovery candidate for this ready cycle.
		state.ChatPolls["ready-quota-future-corrupt"] = ChatPollState{
			ChatID: "ready-quota-future-corrupt", Seeded: true,
			PollState: chatPollStateBlocked, NextPollAt: now.Add(time.Hour),
			BlockedUntil: now.Add(time.Hour), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed ready quota fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`,
			[]byte(`{"chat_id":`), "ready-quota-future-corrupt")
		return err
	})

	var ids []string
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		ids, err = loadSQLiteHotPollReadyChatIDs(ctx, db, "control-chat", now, sqliteHotPollReadyLimit)
		return err
	}); err != nil {
		t.Fatalf("ready quota admission: %v", err)
	}
	_, ordinaryLimit := sqliteHotPollLaneLimits(sqliteHotPollReadyLimit)
	wantHealthy := healthyCount
	if wantHealthy > ordinaryLimit {
		wantHealthy = ordinaryLimit
	}
	if len(ids) != wantHealthy {
		t.Fatalf("healthy ready IDs=%d, want %d: %#v", len(ids), wantHealthy, ids)
	}
	for _, id := range ids {
		if id == "ready-quota-future-corrupt" {
			t.Fatalf("future corrupt row consumed a recovery slot: %#v", ids)
		}
	}
}

func TestSQLiteHotPollReadyStaleGenerationHonorsDueAndControlRecovery(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	t.Run("future ordinary row is not admitted", func(t *testing.T) {
		store := newTestStore(t)
		if err := store.Update(ctx, func(state *State) error {
			state.ChatPolls["stale-future-ready"] = ChatPollState{
				ChatID: "stale-future-ready", Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(time.Hour), UpdatedAt: now,
			}
			state.ChatPolls["stale-due-ready"] = ChatPollState{
				ChatID: "stale-due-ready", Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
			}
			return nil
		}); err != nil {
			t.Fatalf("seed future stale fixture: %v", err)
		}
		migrateStoreToSQLiteForTest(t, store)
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS chat_polls_admission_projection_v1`); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `UPDATE chat_polls
SET projection_revision = canonical_revision - 1, projection_trusted = 1
WHERE chat_id IN (?, ?)`, "stale-future-ready", "stale-due-ready")
			return err
		})

		var ids []string
		if err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				if err == nil {
					err = sql.ErrNoRows
				}
				return err
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			ids, err = loadSQLiteHotPollReadyChatIDs(ctx, db, "control-chat", now, sqliteHotPollReadyLimit)
			return err
		}); err != nil {
			t.Fatalf("future stale admission: %v", err)
		}
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			seen[id] = true
		}
		if seen["stale-future-ready"] {
			t.Fatalf("future stale row was admitted: %#v", ids)
		}
		if !seen["stale-due-ready"] {
			t.Fatalf("due stale row was not admitted for recovery: %#v", ids)
		}
	})

	t.Run("stale control row remains visible", func(t *testing.T) {
		store := newTestStore(t)
		if err := store.Update(ctx, func(state *State) error {
			state.ChatPolls["stale-control-ready"] = ChatPollState{
				ChatID: "stale-control-ready", Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(time.Hour), UpdatedAt: now,
			}
			return nil
		}); err != nil {
			t.Fatalf("seed stale control fixture: %v", err)
		}
		migrateStoreToSQLiteForTest(t, store)
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS chat_polls_admission_projection_v1`); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `UPDATE chat_polls
SET projection_revision = canonical_revision - 1, projection_trusted = 1
WHERE chat_id = ?`, "stale-control-ready")
			return err
		})

		var ids []string
		if err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				if err == nil {
					err = sql.ErrNoRows
				}
				return err
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			ids, err = loadSQLiteHotPollReadyChatIDs(ctx, db, "stale-control-ready", now, sqliteHotPollReadyLimit)
			return err
		}); err != nil {
			t.Fatalf("stale control admission: %v", err)
		}
		if len(ids) != 1 || ids[0] != "stale-control-ready" {
			t.Fatalf("stale control row was hidden: %#v", ids)
		}
	})
}
