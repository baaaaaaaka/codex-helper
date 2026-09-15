package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestDecodeChatPollStateRetainsPendingReceiptAcrossUnrelatedTypeError(t *testing.T) {
	const chatID = "chat-decode-pending-receipt"
	pending := ChatPollPendingPage{
		ChatID: chatID, RequestPath: "/chats/" + chatID + "/messages?$top=20",
		ReceiptID: "receipt-decode-pending", Frontier: "head", PollRole: "work",
	}
	pendingRaw, err := json.Marshal(pending)
	if err != nil {
		t.Fatalf("marshal pending receipt: %v", err)
	}
	raw, err := json.Marshal(map[string]json.RawMessage{
		"chat_id":      json.RawMessage(`"` + chatID + `"`),
		"seeded":       json.RawMessage(`true`),
		"state":        json.RawMessage(`123`),
		"pending_page": pendingRaw,
	})
	if err != nil {
		t.Fatalf("marshal partially corrupt poll: %v", err)
	}
	got, ok := decodeChatPollState(chatID, raw)
	if !ok || got.PendingPage == nil {
		t.Fatalf("decoded poll = %#v ok=%v, want retained pending receipt", got, ok)
	}
	if got.PendingPage.ReceiptID != pending.ReceiptID || got.PendingPage.RequestPath != pending.RequestPath {
		t.Fatalf("decoded pending receipt = %#v, want %#v", got.PendingPage, pending)
	}
	if !chatPollHasOpaqueRecoveryEvidence(got) {
		t.Fatalf("partially corrupt poll lost opaque recovery fence: %#v", got)
	}
}

func TestDecodeChatPollStateSyntaxErrorIsOpaque(t *testing.T) {
	const chatID = "chat-decode-syntax-error"
	raw := []byte(`{"chat_id":"chat-decode-syntax-error","state":`)
	got, ok := decodeChatPollState(chatID, raw)
	if !ok {
		t.Fatal("syntax-invalid poll was discarded instead of retained as recovery evidence")
	}
	if got.ChatID != chatID || !got.Seeded || !got.RecoveryRequired || !chatPollHasOpaqueRecoveryEvidence(got) {
		t.Fatalf("syntax-invalid poll = %#v, want opaque seeded recovery placeholder", got)
	}
	if got.RecoverySourceHash != sha256Bytes(raw) {
		t.Fatalf("syntax-invalid source hash = %q, want %q", got.RecoverySourceHash, sha256Bytes(raw))
	}
	if got.PendingPage != nil || got.ContinuationPath != "" || !got.LastModifiedCursor.IsZero() {
		t.Fatalf("syntax-invalid poll invented executable frontier: %#v", got)
	}
}

func TestChatPollSuccessDoesNotOverwriteOpaqueRecoveryEvidence(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			const chatID = "chat-opaque-success-fence"
			now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.ChatPolls[chatID] = ChatPollState{
					ChatID: chatID, Seeded: true, PollState: chatPollStateWarm, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed chat poll: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			raw := []byte(fmt.Sprintf(`{"chat_id":%q,"seeded":true,"state":123,"pending_page":{"receipt_id":"receipt-opaque-success","chat_id":%q,"request_path":"/chats/%s/messages?$top=20","frontier":"head","poll_role":"work","records":[]}}`, chatID, chatID, chatID))
			if useSQLite {
				sqliteWriteRawChatPollJSONForTest(t, store, chatID, raw)
			} else {
				stateRaw, err := os.ReadFile(store.Path())
				if err != nil {
					t.Fatalf("read JSON state: %v", err)
				}
				var root map[string]json.RawMessage
				if err := json.Unmarshal(stateRaw, &root); err != nil {
					t.Fatalf("decode JSON state: %v", err)
				}
				var polls map[string]json.RawMessage
				if err := json.Unmarshal(root["chat_polls"], &polls); err != nil {
					t.Fatalf("decode JSON chat polls: %v", err)
				}
				polls[chatID] = raw
				root["chat_polls"], err = json.Marshal(polls)
				if err != nil {
					t.Fatalf("encode JSON chat polls: %v", err)
				}
				stateRaw, err = json.Marshal(root)
				if err != nil {
					t.Fatalf("encode JSON state: %v", err)
				}
				if err := os.WriteFile(store.Path(), append(stateRaw, '\n'), 0o600); err != nil {
					t.Fatalf("write opaque JSON poll: %v", err)
				}
			}

			for _, call := range []struct {
				name string
				fn   func() error
			}{
				{name: "success", fn: func() error {
					_, err := store.RecordChatPollSuccessWithContinuation(ctx, chatID, now.Add(time.Minute), true, false, 1, "")
					return err
				}},
				{name: "success-and-schedule", fn: func() error {
					_, err := store.RecordChatPollSuccessWithContinuationAndSchedule(ctx, chatID, now.Add(time.Minute), true, false, 1, "", nil)
					return err
				}},
			} {
				t.Run(call.name, func(t *testing.T) {
					if err := call.fn(); !errors.Is(err, ErrChatPollOpaqueRecoveryRequired) {
						t.Fatalf("opaque success error = %v, want ErrChatPollOpaqueRecoveryRequired", err)
					}
					if useSQLite {
						if got := sqliteRawChatPollJSONForTest(t, store, chatID); !bytes.Equal(got, raw) {
							t.Fatalf("opaque SQLite poll changed after rejected success: got %q want %q", got, raw)
						}
					} else {
						state, err := store.Load(ctx)
						if err != nil {
							t.Fatalf("reload JSON state: %v", err)
						}
						poll, ok := state.ChatPolls[chatID]
						if !ok || !chatPollHasOpaqueRecoveryEvidence(poll) || poll.PendingPage == nil || poll.PendingPage.ReceiptID != "receipt-opaque-success" {
							t.Fatalf("rejected JSON success changed opaque poll: %#v present=%v", poll, ok)
						}
					}
				})
			}
		})
	}
}

// A valid canonical row can be classified as opaque solely because a mixed
// version writer left one scalar projection stale. An unrelated rewrite must
// retain the exact raw row, but an explicit typed update for that ID must not
// be silently discarded behind the stale projection.
func TestSQLiteFullRewriteAppliesChangedTypedSessionOverStaleProjection(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := SessionContext{
		ID: "opaque-session-typed-update", Status: SessionStatusActive,
		TeamsChatID: "opaque-session-chat", UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		return nil
	}); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET updated_at = ? WHERE id = ?`, sqliteTime(now.Add(-time.Hour)), session.ID)
		return err
	})

	if err := store.Update(ctx, func(state *State) error {
		current := state.Sessions[session.ID]
		current.Status = SessionStatusClosed
		current.UpdatedAt = now.Add(time.Minute)
		state.Sessions[session.ID] = current
		return nil
	}); err != nil {
		t.Fatalf("typed session update over stale projection: %v", err)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load updated session: %v", err)
	}
	got, ok := loaded.Sessions[session.ID]
	if !ok || got.Status != SessionStatusClosed || !got.UpdatedAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("updated session = %#v present=%v, want closed typed update", got, ok)
	}
}

// Outbox rows use the same opaque-preservation rule. A stale scalar must not
// prevent an explicit typed update from repairing the row, while malformed
// JSON remains protected by the existing opaque path.
func TestSQLiteFullRewriteAppliesChangedTypedOutboxOverStaleProjection(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	message := OutboxMessage{
		ID: "opaque-outbox-typed-update", SessionID: "opaque-outbox-session",
		TeamsChatID: "opaque-outbox-chat", Kind: "progress", Body: "before",
		Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[message.ID] = message
		return nil
	}); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET status = ? WHERE id = ?`, string(OutboxStatusSent), message.ID)
		return err
	})

	var rawBefore []byte
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT json FROM outbox_messages WHERE id = ?`, message.ID).Scan(&rawBefore)
	})

	err := store.Update(ctx, func(state *State) error {
		// The normal hot loader conservatively omits a contradictory outbox
		// projection. Supplying the complete typed value here models an
		// accidental generic update; it must fail closed rather than silently
		// discard the update or replace an unknown remote outcome.
		current := message
		current.Body = "after"
		current.UpdatedAt = now.Add(time.Minute)
		state.OutboxMessages[message.ID] = current
		return nil
	})
	if !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("typed outbox update over stale projection error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
	var rawAfter []byte
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT json FROM outbox_messages WHERE id = ?`, message.ID).Scan(&rawAfter)
	})
	if !bytes.Equal(rawAfter, rawBefore) {
		t.Fatalf("failed opaque outbox update changed raw JSON: before=%q after=%q", rawBefore, rawAfter)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load updated outbox: %v", err)
	}
	if _, ok := loaded.OutboxMessages[message.ID]; ok {
		t.Fatalf("opaque outbox unexpectedly became typed state after rejected update")
	}
}

func TestSQLiteOpaquePollFinalAnswerBoostUpdatesSidecarWithoutReplacingRaw(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	seedFinalAnswerPollBoostState(t, store, now, nil)
	migrateStoreToSQLiteForTest(t, store)
	raw := []byte(`{"chat_id":"chat-1","seeded":true,"state":"cold","continuation_path":"/chats/chat-1/messages?$skiptoken=old","pending_page":{}}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, raw, "chat-1")
		return err
	})

	boostAt := now.Add(3 * time.Minute)
	got, changed, err := store.BoostChatPollAfterFinalAnswer(ctx, FinalAnswerPollBoostRequest{
		SessionID: "s001", TeamsChatID: "chat-1", NextPollAt: boostAt, LastActivityAt: boostAt,
	})
	if err != nil {
		t.Fatalf("opaque final-answer boost: %v", err)
	}
	if !changed || got.PollState != chatPollStateHot || !got.RecoveryRequired || !got.NextPollAt.Equal(boostAt) {
		t.Fatalf("opaque final-answer boost result = %#v changed=%v", got, changed)
	}
	if gotRaw := sqliteRawChatPollJSONForTest(t, store, "chat-1"); !bytes.Equal(gotRaw, raw) {
		t.Fatalf("opaque final-answer boost replaced raw evidence: got %q want %q", gotRaw, raw)
	}
}

// An owner-bound retry gate is a real durable sidecar mutation even when the
// raw chat-poll receipt is opaque.  The SQLite path must report that mutation
// as changed; otherwise the bridge mistakes a successful current-owner write
// for a lost lease and can immediately repeat the same Graph read.
func TestSQLiteOpaquePollOwnerBoundRetryGateReportsDurableChange(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	const chatID = "chat-opaque-owner-retry-gate"
	scope := ScopeIdentity{ID: "scope-opaque-owner-retry-gate"}
	machine := MachineRecord{ID: "machine-opaque-owner-retry-gate", ScopeID: scope.ID, Kind: MachineKindPrimary}
	owner := testOwner("opaque-owner-retry-session", "opaque-owner-retry-turn", now)
	owner.MachineID = machine.ID
	owner.ScopeID = scope.ID
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = scope
		state.Machines[machine.ID] = machine
		state.ChatPolls[chatID] = ChatPollState{ChatID: chatID, Seeded: true, PollState: chatPollStateHot, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed opaque owner retry-gate state: %v", err)
	}
	lease, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope: scope, Machine: machine, Owner: owner, Duration: time.Minute, Now: now,
	})
	if err != nil || lease.Mode != LeaseModeActive || lease.Lease.Generation <= 0 {
		t.Fatalf("claim owner lease = %#v err=%v, want active lease", lease, err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Keep a syntactically valid but partially malformed receipt.  The typed
	// sidecar can carry a retry schedule, but the original bytes remain the
	// recovery evidence until an explicit repair replaces them.
	raw := []byte(fmt.Sprintf(`{"chat_id":%q,"seeded":true,"state":123,"pending_page":{}}`, chatID))
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, raw, chatID)
		return err
	})

	blockedUntil := now.Add(5 * time.Minute)
	if err := store.RecordChatPollErrorWithBlockForOwner(ctx, chatID, "Graph read 429", blockedUntil, machine.ID, lease.Lease.Generation); err != nil {
		t.Fatalf("current-owner opaque retry gate: %v", err)
	}
	if gotRaw := sqliteRawChatPollJSONForTest(t, store, chatID); !bytes.Equal(gotRaw, raw) {
		t.Fatalf("owner-bound opaque retry gate replaced raw evidence: got %q want %q", gotRaw, raw)
	}
	poll, found, err := store.ChatPoll(ctx, chatID)
	if err != nil || !found {
		t.Fatalf("read owner-bound opaque retry gate: found=%v err=%v poll=%#v", found, err, poll)
	}
	if !poll.RecoveryRequired || !poll.BlockedUntil.Equal(blockedUntil) || !poll.NextPollAt.Equal(blockedUntil) {
		t.Fatalf("owner-bound opaque retry gate = %#v, want durable blocked sidecar", poll)
	}
	var blockedNanos int64
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT blocked_until FROM chat_polls WHERE chat_id = ?`, chatID).Scan(&blockedNanos)
	})
	if blockedNanos != sqliteTime(blockedUntil) {
		t.Fatalf("owner-bound opaque blocked_until scalar = %d, want %d", blockedNanos, sqliteTime(blockedUntil))
	}
}

func TestSQLiteOpaquePollFullRewriteToleratesInvalidNumericSidecar(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	const chatID = "chat-opaque-invalid-sidecar"
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[chatID] = ChatPollState{ChatID: chatID, Seeded: true, PollState: chatPollStateHot, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed invalid-sidecar poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	raw := []byte(fmt.Sprintf(`{"chat_id":%q,"seeded":true,"state":"hot","pending_page":{}}`, chatID))
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ?, next_poll_at = ? WHERE chat_id = ?`, raw, "not-a-number", chatID); err != nil {
			return err
		}
		return nil
	})

	if err := store.Update(ctx, func(state *State) error {
		state.ServiceControl.Paused = !state.ServiceControl.Paused
		return nil
	}); err != nil {
		t.Fatalf("unrelated full-state rewrite with invalid sidecar: %v", err)
	}
	if got := sqliteRawChatPollJSONForTest(t, store, chatID); !bytes.Equal(got, raw) {
		t.Fatalf("invalid-sidecar rewrite changed opaque raw evidence: got %q want %q", got, raw)
	}
}
