package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A truncated outbox payload is one chat-local corruption. It must not make
// the SQLite store fail to open, hide healthy pending work, or disappear when
// an unrelated full-state update rewrites the typed projections.
func TestSQLiteMalformedOutboxRowIsolatedAndPreserved(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	healthy := OutboxMessage{
		ID:          "outbox:healthy-after-corrupt",
		SessionID:   "session:healthy-after-corrupt",
		TeamsChatID: "chat:healthy-after-corrupt",
		Kind:        "helper-status",
		Body:        "healthy pending message",
		Status:      OutboxStatusQueued,
		Sequence:    2,
		CreatedAt:   now.Add(time.Second),
		UpdatedAt:   now.Add(time.Second),
	}
	if _, _, err := store.QueueOutbox(ctx, healthy); err != nil {
		t.Fatalf("QueueOutbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	const corruptID = "outbox:malformed-before-healthy"
	corruptRaw := []byte(`{"id":"outbox:malformed-before-healthy"`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			corruptID, "session:corrupt", "turn:corrupt", "chat:healthy-after-corrupt", "", string(OutboxStatusQueued), 1, sqliteTime(now), 0, nil, corruptRaw)
		return err
	})

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with malformed outbox row: %v", err)
	}
	if _, ok := loaded.OutboxMessages[corruptID]; ok {
		t.Fatalf("malformed outbox row was exposed as typed state: %#v", loaded.OutboxMessages[corruptID])
	}
	if got := loaded.OutboxMessages[healthy.ID].Body; got != healthy.Body {
		t.Fatalf("healthy outbox body = %q, want %q", got, healthy.Body)
	}

	pending, err := store.PendingOutboxAt(ctx, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("PendingOutboxAt with malformed row: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != healthy.ID {
		t.Fatalf("pending outbox = %#v, want only healthy row %q", pending, healthy.ID)
	}

	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatID = "unrelated-control-chat"
		return nil
	}); err != nil {
		t.Fatalf("unrelated full-state update: %v", err)
	}
	if got := sqliteRawOutboxJSONForTest(t, store, corruptID); string(got) != string(corruptRaw) {
		t.Fatalf("malformed outbox raw payload changed: got %q want %q", got, corruptRaw)
	}
	loaded, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after preserving malformed outbox row: %v", err)
	}
	if got := loaded.OutboxMessages[healthy.ID].Body; got != healthy.Body {
		t.Fatalf("healthy outbox after rewrite = %q, want %q", got, healthy.Body)
	}
}

// Valid JSON is not necessarily a valid outbox row.  A type error or a
// mismatch between the indexed SQLite identity and the embedded payload must
// be treated as local quarantine evidence before SQL LIMIT is applied; it must
// not abort a pending page or route a healthy message to another chat.
func TestSQLiteSemanticallyMalformedOutboxRowsDoNotHideHealthyWork(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	healthy := OutboxMessage{
		ID:          "outbox:semantic-healthy",
		SessionID:   "session:semantic-healthy",
		TeamsChatID: "chat:semantic",
		Kind:        "helper-status",
		Body:        "healthy semantic pending message",
		Status:      OutboxStatusQueued,
		Sequence:    3,
		CreatedAt:   now.Add(3 * time.Second),
		UpdatedAt:   now.Add(3 * time.Second),
	}
	if _, _, err := store.QueueOutbox(ctx, healthy); err != nil {
		t.Fatalf("QueueOutbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	insertRaw := func(id string, sessionID string, chatID string, raw []byte) {
		t.Helper()
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				id, sessionID, "", chatID, "", string(OutboxStatusQueued), 1, sqliteTime(now), 0, 0, raw)
			return err
		})
	}
	// This payload is syntactically valid, but status has the wrong JSON type.
	insertRaw("outbox:semantic-type-error", "session:semantic-type-error", "chat:semantic", []byte(`{"id":"outbox:semantic-type-error","session_id":"session:semantic-type-error","teams_chat_id":"chat:semantic","status":17,"sequence":1,"created_at":"2026-08-31T12:00:00Z","body":"bad status type"}`))
	// This payload decodes as an OutboxMessage, but its embedded chat identity
	// disagrees with the indexed SQL projection.
	insertRaw("outbox:semantic-identity-error", "session:semantic-identity-error", "chat:semantic", []byte(`{"id":"outbox:semantic-identity-error","session_id":"session:semantic-identity-error","teams_chat_id":"chat:other","status":"queued","sequence":1,"created_at":"2026-08-31T12:00:00Z","body":"wrong chat identity"}`))

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with semantically malformed outbox rows: %v", err)
	}
	if _, ok := loaded.OutboxMessages["outbox:semantic-type-error"]; ok {
		t.Fatal("type-invalid outbox row was exposed as typed state")
	}
	if _, ok := loaded.OutboxMessages["outbox:semantic-identity-error"]; ok {
		t.Fatal("identity-conflicting outbox row was exposed as typed state")
	}
	if got := loaded.OutboxMessages[healthy.ID].Body; got != healthy.Body {
		t.Fatalf("healthy outbox body = %q, want %q", got, healthy.Body)
	}

	pending, err := store.PendingOutboxAt(ctx, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("PendingOutboxAt with semantic corruption: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != healthy.ID {
		t.Fatalf("pending outbox = %#v, want only healthy row %q", pending, healthy.ID)
	}

	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatID = "chat:unrelated-control"
		return nil
	}); err != nil {
		t.Fatalf("unrelated full-state update: %v", err)
	}
	for _, id := range []string{"outbox:semantic-type-error", "outbox:semantic-identity-error"} {
		if got := sqliteRawOutboxJSONForTest(t, store, id); len(got) == 0 {
			t.Fatalf("opaque semantic row %q was lost during full-state rewrite", id)
		}
	}
}

// A valid JSON envelope can still contain a field whose Go type is invalid.
// The post-send replay lane must skip that local row and continue to the next
// durable Sent row instead of turning the malformed row into a process-wide
// error or hiding all later side effects behind a SQL LIMIT.
func TestSQLiteSemanticallyMalformedSentSideEffectRowDoesNotHideHealthyWork(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	healthy := OutboxMessage{
		ID: "outbox:side-effect-healthy", TeamsChatID: "chat:side-effects",
		Kind: "helper-final", Body: "healthy sent message", Status: OutboxStatusSent,
		TeamsMessageID: "teams:side-effect-healthy", PostSendEffectsPending: true,
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[healthy.ID] = healthy
		return nil
	}); err != nil {
		t.Fatalf("seed healthy sent side-effect row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	const corruptID = "outbox:side-effect-semantic-corrupt"
	corruptRaw := []byte(`{"id":"outbox:side-effect-semantic-corrupt","teams_chat_id":"chat:side-effects","status":"sent","post_send_effects_pending":true,"last_send_error":17,"created_at":"2026-08-31T12:00:00Z"}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			corruptID, "", "", "chat:side-effects", "", string(OutboxStatusSent), 0, sqliteTime(now), 0, 1, corruptRaw)
		return err
	})

	pending, err := store.PendingSentOutboxSideEffects(ctx, 1)
	if err != nil {
		t.Fatalf("pending side effects with semantic corruption: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != healthy.ID {
		t.Fatalf("pending side effects = %#v, want only healthy row %q", pending, healthy.ID)
	}
}

// Echo recovery is bounded per status, but the SQL LIMIT must not be applied
// before the local JSON decode. A row with a valid projection and a malformed
// optional field can otherwise hide the only usable candidate behind it.
func TestSQLiteMalformedEchoRowDoesNotHideLaterCandidate(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 8, 31, 13, 0, 0, 0, time.UTC)
	valid := []OutboxMessage{
		{ID: "outbox:echo-sending-valid", TeamsChatID: "chat:echo", Status: OutboxStatusSending, CreatedAt: now.Add(time.Second)},
		{ID: "outbox:echo-accepted-valid", TeamsChatID: "chat:echo", Status: OutboxStatusAccepted, CreatedAt: now.Add(time.Second)},
		{ID: "outbox:echo-sent-valid", TeamsChatID: "chat:echo", Status: OutboxStatusSent, CreatedAt: now.Add(time.Second)},
	}
	if err := store.Update(ctx, func(state *State) error {
		for _, msg := range valid {
			state.OutboxMessages[msg.ID] = msg
		}
		return nil
	}); err != nil {
		t.Fatalf("seed echo candidates: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	malformed := []struct {
		id     string
		status OutboxStatus
		at     time.Time
	}{
		{id: "outbox:echo-sending-bad", status: OutboxStatusSending, at: now},
		{id: "outbox:echo-accepted-bad", status: OutboxStatusAccepted, at: now.Add(2 * time.Second)},
		{id: "outbox:echo-sent-bad", status: OutboxStatusSent, at: now.Add(2 * time.Second)},
	}
	for _, row := range malformed {
		raw := []byte(`{"id":"` + row.id + `","teams_chat_id":"chat:echo","status":"` + string(row.status) + `","created_at":"` + row.at.Format(time.RFC3339Nano) + `","last_send_error":17}`)
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				row.id, "", "", "chat:echo", "", string(row.status), 0, sqliteTime(row.at), 0, 0, raw)
			return err
		})
	}

	candidates, err := store.RecentOutboxEchoCandidates(ctx, OutboxEchoCandidateQuery{TeamsChatID: "chat:echo", LimitPerStatus: 1})
	if err != nil {
		t.Fatalf("RecentOutboxEchoCandidates with malformed rows: %v", err)
	}
	got := map[OutboxStatus]string{}
	for _, candidate := range candidates {
		got[candidate.Status] = candidate.ID
	}
	for _, want := range valid {
		if got[want.Status] != want.ID {
			t.Fatalf("echo candidate for %s = %q, want %q; all=%#v", want.Status, got[want.Status], want.ID, candidates)
		}
	}
}

// A JSON row can pass the cheap SQLite projection predicate and still fail the
// Go decoder. Echo recovery must continue the keyset scan past an arbitrary
// malformed prefix; an eight-page cap would make the valid receipt below
// permanently invisible.
func TestSQLiteMalformedEchoPrefixBeyondLegacyPageCapDoesNotHideCandidate(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 1, 13, 30, 0, 0, time.UTC)
	const chatID = "chat:echo-long-malformed-prefix"
	const malformedCount = 65
	valid := OutboxMessage{
		ID: "outbox:echo-long-valid", TeamsChatID: chatID, Status: OutboxStatusSending,
		Body: "usable echo receipt", CreatedAt: now.Add(malformedCount * time.Second),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[valid.ID] = valid
		return nil
	}); err != nil {
		t.Fatalf("seed valid echo receipt: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	for i := 0; i < malformedCount; i++ {
		id := fmt.Sprintf("outbox:echo-long-bad-%03d", i)
		createdAt := now.Add(time.Duration(i) * time.Second)
		raw := []byte(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":%q,"created_at":%q,"body":17}`,
			id, chatID, string(OutboxStatusSending), createdAt.Format(time.RFC3339Nano)))
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				id, "", "", chatID, "", string(OutboxStatusSending), 0, sqliteTime(createdAt), 0, 0, raw)
			return err
		})
	}

	candidates, err := store.RecentOutboxEchoCandidates(ctx, OutboxEchoCandidateQuery{
		TeamsChatID: chatID, LimitPerStatus: 1,
	})
	if err != nil {
		t.Fatalf("RecentOutboxEchoCandidates with long malformed prefix: %v", err)
	}
	if len(candidates) != 1 || candidates[0].ID != valid.ID {
		t.Fatalf("echo candidates = %#v, want valid receipt %q after %d malformed rows", candidates, valid.ID, malformedCount)
	}
}

// Missing compatibility values are safe to hydrate from canonical JSON. A
// mixed-version writer may commit the JSON envelope before refreshing the
// nullable indexed columns; treating blank strings as contradictions would
// silently hide ordinary delivery work until an unrelated rewrite.
func TestSQLiteBlankOutboxCompatibilityProjectionUsesCanonicalJSON(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 1, 14, 0, 0, 0, time.UTC)
	want := OutboxMessage{
		ID: "outbox:blank-compatibility-projection", SessionID: "session:blank-compatibility",
		TurnID: "turn:blank-compatibility", TeamsChatID: "chat:blank-compatibility",
		Kind: "helper-status", Body: "canonical JSON remains runnable", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[want.ID] = want
		return nil
	}); err != nil {
		t.Fatalf("seed canonical outbox row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages
SET session_id = '', turn_id = '', teams_chat_id = '', status = ''
WHERE id = ?`, want.ID)
		return err
	})

	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 1})
	if err != nil {
		t.Fatalf("PendingOutboxPageAt with blank compatibility projection: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != want.ID || page.Messages[0].TeamsChatID != want.TeamsChatID {
		t.Fatalf("pending page = %#v, want canonical row %#v", page.Messages, want)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with blank compatibility projection: %v", err)
	}
	if got, ok := loaded.OutboxMessages[want.ID]; !ok || got.SessionID != want.SessionID || got.TurnID != want.TurnID {
		t.Fatalf("loaded canonical outbox = %#v present=%v, want %#v", got, ok, want)
	}
}

// Canonical numeric delivery fields remain authoritative when a mixed-version
// writer has not populated their nullable compatibility columns yet. The hot
// page, chat preflight, and side-effect lane must all see the same durable row.
func TestSQLiteCanonicalNumericProjectionSurvivesNullCompatibilityColumns(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 1, 14, 30, 0, 0, time.UTC)
	queued := OutboxMessage{
		ID: "outbox:null-numeric-queued", SessionID: "session:null-numeric",
		TurnID: "turn:null-numeric", TeamsChatID: "chat:null-numeric",
		Kind: "helper-status", Body: "canonical sequence", Status: OutboxStatusQueued,
		Sequence: 17, CreatedAt: now, UpdatedAt: now,
	}
	sent := OutboxMessage{
		ID: "outbox:null-numeric-sent", SessionID: "session:null-numeric",
		TeamsChatID: "chat:null-numeric", Kind: "helper-final",
		Body: "canonical side effect", Status: OutboxStatusSent,
		TeamsMessageID: "teams:null-numeric", PostSendEffectsPending: true,
		Sequence: 18, CreatedAt: now.Add(time.Second), UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[queued.ID] = queued
		state.OutboxMessages[sent.ID] = sent
		return nil
	}); err != nil {
		t.Fatalf("seed numeric compatibility rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages
SET sequence = NULL, post_send_effects_pending = NULL WHERE id = ?`, queued.ID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages
SET sequence = NULL, post_send_effects_pending = NULL WHERE id = ?`, sent.ID)
		return err
	})

	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 1})
	if err != nil {
		t.Fatalf("pending page with NULL numeric projections: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != queued.ID || page.Messages[0].Sequence != queued.Sequence {
		t.Fatalf("pending page = %#v, want canonical queued row %#v", page.Messages, queued)
	}
	chatIDs, err := store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 1)
	if err != nil {
		t.Fatalf("pending chat IDs with NULL numeric projections: %v", err)
	}
	if len(chatIDs) != 1 || chatIDs[0] != queued.TeamsChatID {
		t.Fatalf("pending chat IDs = %#v, want %q", chatIDs, queued.TeamsChatID)
	}
	sideEffects, err := store.PendingSentOutboxSideEffects(ctx, 1)
	if err != nil {
		t.Fatalf("pending side effects with NULL numeric projections: %v", err)
	}
	if len(sideEffects) != 1 || sideEffects[0].ID != sent.ID || !sideEffects[0].PostSendEffectsPending {
		t.Fatalf("pending side effects = %#v, want canonical sent row %#v", sideEffects, sent)
	}
}

// A malformed scalar projection is local quarantine evidence, not a reason for
// database/sql to abort a query containing healthy rows. This also proves that
// the opaque-row capture used by an unrelated full-state update can read the
// damaged scalar without turning it into a synthetic zero.
func TestSQLiteMalformedNumericProjectionDoesNotAbortHealthyOutboxQueries(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 1, 15, 0, 0, 0, time.UTC)
	healthy := OutboxMessage{
		ID: "outbox:after-malformed-numeric", SessionID: "session:after-malformed-numeric",
		TeamsChatID: "chat:after-malformed-numeric", Kind: "helper-status",
		Body: "healthy row", Status: OutboxStatusQueued, Sequence: 2,
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[healthy.ID] = healthy
		return nil
	}); err != nil {
		t.Fatalf("seed healthy numeric row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	const badID = "outbox:malformed-numeric-prefix"
	badRaw := []byte(`{"id":"outbox:malformed-numeric-prefix","teams_chat_id":"chat:malformed-numeric","status":"queued","created_at":"2026-09-01T15:00:00Z","body":"opaque"}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages
(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			badID, "", "", "chat:malformed-numeric", "", string(OutboxStatusQueued), "bad-sequence",
			sqliteTime(now), "bad-deliver-after", "bad-pending", badRaw)
		return err
	})

	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 1})
	if err != nil {
		t.Fatalf("pending page with malformed numeric projection: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != healthy.ID {
		t.Fatalf("pending page = %#v, want healthy row %q", page.Messages, healthy.ID)
	}
	chatIDs, err := store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 2)
	if err != nil {
		t.Fatalf("pending chat IDs with malformed numeric projection: %v", err)
	}
	if len(chatIDs) != 1 || chatIDs[0] != healthy.TeamsChatID {
		t.Fatalf("pending chat IDs = %#v, want only %q", chatIDs, healthy.TeamsChatID)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatID = "chat:unrelated-after-malformed-numeric"
		return nil
	}); err != nil {
		t.Fatalf("full-state rewrite with malformed numeric projection: %v", err)
	}
	if got := sqliteRawOutboxJSONForTest(t, store, badID); string(got) != string(badRaw) {
		t.Fatalf("malformed numeric row changed during rewrite: got %q want %q", got, badRaw)
	}
}

// PendingOutboxChatIDsAt is a chat-level hint. It must group after SQLite has
// applied the same primitive JSON admission contract as the typed sender, so a
// large malformed prefix in one chat cannot force Go to decode every row before
// a later healthy chat becomes visible.
func TestSQLitePendingOutboxChatIDsGroupsBeforeMalformedSameChatPrefix(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 1, 16, 0, 0, 0, time.UTC)
	healthy := OutboxMessage{
		ID: "outbox:grouping-healthy", TeamsChatID: "chat:grouping-healthy",
		Kind: "helper-status", Body: "healthy", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: now.Add(time.Hour), UpdatedAt: now.Add(time.Hour),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[healthy.ID] = healthy
		return nil
	}); err != nil {
		t.Fatalf("seed grouping row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	const malformedCount = 1000
	for i := 0; i < malformedCount; i++ {
		id := fmt.Sprintf("outbox:grouping-bad-%04d", i)
		raw := []byte(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":"queued","sequence":%d,"created_at":%q,"body":17}`,
			id, "chat:grouping-bad", i+1, now.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano)))
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages
(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				id, "", "", "chat:grouping-bad", "", string(OutboxStatusQueued), i+1,
				sqliteTime(now.Add(time.Duration(i)*time.Second)), 0, 0, raw)
			return err
		})
	}

	chatIDs, err := store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 2)
	if err != nil {
		t.Fatalf("pending chat IDs with malformed same-chat prefix: %v", err)
	}
	if len(chatIDs) != 1 || chatIDs[0] != healthy.TeamsChatID {
		t.Fatalf("pending chat IDs = %#v, want only healthy chat %q", chatIDs, healthy.TeamsChatID)
	}
}

func TestSQLiteExplicitNullNextAttemptAtIsImmediatelyDue(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:migration-seed", TeamsChatID: "chat:migration-seed", Status: OutboxStatusSent, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("seed migration state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	const id = "outbox:explicit-null-schedule"
	raw := []byte(`{"id":"outbox:explicit-null-schedule","teams_chat_id":"chat:null-schedule","status":"queued","sequence":1,"created_at":"2026-09-01T12:00:00Z","next_attempt_at":null,"body":"must run"}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, "", "", "chat:null-schedule", "", string(OutboxStatusQueued), 1, sqliteTime(now), now.Add(time.Hour).UnixNano(), 0, raw)
		return err
	})

	pending, err := store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("PendingOutboxAt: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != id {
		t.Fatalf("pending = %#v, want explicit-null row %q", pending, id)
	}
}

func TestSQLiteOutboxProjectionMismatchIsOpaqueToEveryHotLane(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 1, 13, 0, 0, 0, time.UTC)
	healthy := OutboxMessage{
		ID: "outbox:projection-healthy", TeamsChatID: "chat:projection", Status: OutboxStatusQueued,
		Sequence: 4, CreatedAt: now.Add(4 * time.Second), UpdatedAt: now.Add(4 * time.Second),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[healthy.ID] = healthy
		return nil
	}); err != nil {
		t.Fatalf("seed healthy outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	insert := func(id string, sequence int64, createdAt time.Time, pending int64, body string) {
		t.Helper()
		raw := []byte(`{"id":"` + id + `","teams_chat_id":"chat:projection","status":"queued","sequence":` + fmt.Sprintf("%d", sequence) + `,"created_at":"` + createdAt.Format(time.RFC3339Nano) + `","body":"` + body + `"}`)
		if pending == 1 {
			raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"post_send_effects_pending":true}`)
		}
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				id, "", "", "chat:projection", "", string(OutboxStatusQueued), sequence, sqliteTime(createdAt), 0, pending, raw)
			return err
		})
	}
	// Each row has one contradictory indexed projection: sequence, created_at,
	// or post-send pending. None may be admitted as runnable work.
	insert("outbox:projection-bad-sequence", 9, now.Add(time.Second), 0, "bad-sequence")
	insert("outbox:projection-bad-created", 2, now.Add(2*time.Second), 0, "bad-created")
	insert("outbox:projection-bad-pending", 3, now.Add(3*time.Second), 1, "bad-pending")
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET sequence = 1 WHERE id = ?`, "outbox:projection-bad-sequence")
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE outbox_messages SET created_at = ? WHERE id = ?`, sqliteTime(now), "outbox:projection-bad-created")
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE outbox_messages SET post_send_effects_pending = 0, status = ? WHERE id = ?`, string(OutboxStatusSent), "outbox:projection-bad-pending")
		return err
	})

	pending, err := store.PendingOutboxAt(ctx, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("PendingOutboxAt: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != healthy.ID {
		t.Fatalf("pending = %#v, want only healthy row %q", pending, healthy.ID)
	}
	for _, id := range []string{
		"outbox:projection-bad-sequence",
		"outbox:projection-bad-created",
		"outbox:projection-bad-pending",
	} {
		if _, err := store.OutboxMessageByID(ctx, id); !errors.Is(err, ErrOutboxNotFound) {
			t.Fatalf("projection-mismatched point read %q error = %v, want quarantine as ErrOutboxNotFound", id, err)
		}
	}
}

// A Graph response is the authoritative external delivery receipt. Optional
// local delivery/artifact projections may be malformed independently; they
// must remain opaque and must not leave the already accepted outbox row in
// Sending forever, which would cause exact-recovery retries to spin without a
// durable local terminal state.
func TestSQLiteAcceptedOutboxSettlesWithMalformedLinkedRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:accepted-with-corrupt-links",
		TeamsChatID: "chat:accepted-with-corrupt-links",
		Kind:        "helper-final",
		Body:        "already accepted by Graph",
		Status:      OutboxStatusQueued,
		ArtifactIDs: []string{"artifact:corrupt-link"},
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)

	linkedRows := []struct {
		table string
		id    string
		raw   []byte
	}{
		{table: "transcript_deliveries", id: "delivery:corrupt-link", raw: []byte(`{"id":"delivery:corrupt-link","outbox_id":"` + msg.ID + `","status":17}`)},
		{table: "helper_deliveries", id: "helper:corrupt-link", raw: []byte(`{"id":"helper:corrupt-link","outbox_id":"` + msg.ID + `","status":17}`)},
		{table: "artifact_records", id: "artifact:corrupt-link", raw: []byte(`{"id":"artifact:corrupt-link","outbox_id":"` + msg.ID + `","status":17}`)},
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, row := range linkedRows {
			var query string
			switch row.table {
			case "transcript_deliveries":
				query = `INSERT INTO transcript_deliveries(id, session_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?)`
			case "helper_deliveries":
				query = `INSERT INTO helper_deliveries(id, session_id, turn_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)`
			case "artifact_records":
				query = `INSERT INTO artifact_records(id, session_id, turn_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)`
			}
			var err error
			if row.table == "transcript_deliveries" {
				_, err = tx.ExecContext(ctx, query, row.id, "", msg.ID, "queued", sqliteTime(now), row.raw)
			} else {
				_, err = tx.ExecContext(ctx, query, row.id, "", "", msg.ID, "queued", sqliteTime(now), row.raw)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})

	sent, err := store.MarkOutboxSent(ctx, msg.ID, "teams:accepted-with-corrupt-links")
	if err != nil {
		t.Fatalf("MarkOutboxSent with malformed linked rows: %v", err)
	}
	if sent.Status != OutboxStatusSent || sent.TeamsMessageID != "teams:accepted-with-corrupt-links" || !sent.PostSendEffectsPending {
		t.Fatalf("settled outbox = %#v, want Sent with replay marker", sent)
	}
	if got := sqliteRawTranscriptDeliveryJSONForTest(t, store, "delivery:corrupt-link"); string(got) != string(linkedRows[0].raw) {
		t.Fatalf("malformed transcript delivery changed: got=%q want=%q", got, linkedRows[0].raw)
	}
	if got := sqliteRawHelperDeliveryByOutboxForTest(t, store, msg.ID); string(got) != string(linkedRows[1].raw) {
		t.Fatalf("malformed helper delivery changed: got=%q want=%q", got, linkedRows[1].raw)
	}
	if got := sqliteRawArtifactRecordForTest(t, store, "artifact:corrupt-link"); string(got) != string(linkedRows[2].raw) {
		t.Fatalf("malformed artifact record changed: got=%q want=%q", got, linkedRows[2].raw)
	}
}

// Older SQLite writers stored the delivery fields in indexed columns but did
// not repeat all of them in the canonical JSON envelope. Every hot read must
// hydrate that compatibility projection before applying status/FIFO or
// post-send admission; otherwise a queued row can look terminal and a sent
// row can lose its Teams ID or replay marker after restart.
func TestSQLiteLegacyOutboxProjectionHydratesEveryDeliveryLane(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 7, 14, 0, 0, 0, time.UTC)
	queuedID := "outbox:legacy-projection-queued"
	sentID := "outbox:legacy-projection-sent"
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[queuedID] = OutboxMessage{
			ID: queuedID, SessionID: "session:legacy-projection", TurnID: "turn:legacy-projection",
			TeamsChatID: "chat:legacy-projection", Kind: "helper", Body: "queued legacy body",
			Status: OutboxStatusQueued, Sequence: 7, CreatedAt: now, UpdatedAt: now,
		}
		state.OutboxMessages[sentID] = OutboxMessage{
			ID: sentID, SessionID: "session:legacy-projection", TurnID: "turn:legacy-projection",
			TeamsChatID: "chat:legacy-projection", TeamsMessageID: "teams:legacy-projection",
			Kind: "helper-final", Body: "sent legacy body", Status: OutboxStatusSent,
			Sequence: 8, PostSendEffectsPending: true, CreatedAt: now.Add(time.Second), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy projection rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// Keep only fields that an old JSON writer was known to persist. The
		// indexed columns retain the identity, status, sequence and replay data.
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`,
			[]byte(`{"id":"outbox:legacy-projection-queued","kind":"helper","body":"queued legacy body","created_at":"2026-09-07T14:00:00Z"}`), queuedID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`,
			[]byte(`{"id":"outbox:legacy-projection-sent","kind":"helper-final","body":"sent legacy body","created_at":"2026-09-07T14:00:01Z"}`), sentID)
		return err
	})

	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("legacy queued pending page: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != queuedID {
		t.Fatalf("legacy queued page = %#v, want %q", page.Messages, queuedID)
	}
	queued := page.Messages[0]
	if queued.SessionID != "session:legacy-projection" || queued.TurnID != "turn:legacy-projection" ||
		queued.TeamsChatID != "chat:legacy-projection" || queued.Status != OutboxStatusQueued || queued.Sequence != 7 {
		t.Fatalf("hydrated queued projection = %#v", queued)
	}

	byID, err := store.OutboxMessageByID(ctx, queuedID)
	if err != nil {
		t.Fatalf("legacy queued OutboxMessageByID: %v", err)
	}
	if byID.SessionID != queued.SessionID || byID.TurnID != queued.TurnID || byID.Sequence != queued.Sequence {
		t.Fatalf("hydrated queued by-ID projection = %#v, want %#v", byID, queued)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return nil
	})

	sideEffects, err := store.PendingSentOutboxSideEffects(ctx, 10)
	if err != nil {
		t.Fatalf("legacy sent side-effect lane: %v", err)
	}
	if len(sideEffects) != 1 || sideEffects[0].ID != sentID {
		t.Fatalf("legacy side-effect rows = %#v, want %q", sideEffects, sentID)
	}
	sent := sideEffects[0]
	if sent.TeamsChatID != "chat:legacy-projection" || sent.TeamsMessageID != "teams:legacy-projection" ||
		sent.Status != OutboxStatusSent || sent.Sequence != 8 || !sent.PostSendEffectsPending {
		t.Fatalf("hydrated sent projection = %#v", sent)
	}

	sentMessages, err := store.SentOutboxMessagesForChat(ctx, "chat:legacy-projection")
	if err != nil {
		t.Fatalf("legacy sent chat lane: %v", err)
	}
	if len(sentMessages) != 1 || sentMessages[0].ID != sentID || sentMessages[0].TeamsMessageID != sent.TeamsMessageID {
		t.Fatalf("legacy sent chat messages = %#v, want hydrated %q", sentMessages, sentID)
	}
}

// A malformed optional retry timestamp must be isolated as opaque evidence;
// it must not hide a healthy later row. A blank legacy timestamp has the
// opposite compatibility meaning: it is an immediate retry, even if the old
// scalar deliver_after column is stale and in the future.
func TestSQLiteOutboxRetryScheduleMalformedAndBlankValuesAreBounded(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 7, 15, 0, 0, 0, time.UTC)
	badID := "outbox:invalid-retry-schedule"
	blankID := "outbox:blank-retry-schedule"
	healthyID := "outbox:healthy-after-invalid-retry-schedule"
	if err := store.Update(ctx, func(state *State) error {
		for _, msg := range []OutboxMessage{
			{ID: badID, TeamsChatID: "chat:retry-bad", Kind: "helper", Body: "bad", Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now},
			{ID: blankID, TeamsChatID: "chat:retry-blank", Kind: "helper", Body: "blank", Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now.Add(time.Second), UpdatedAt: now},
			{ID: healthyID, TeamsChatID: "chat:retry-healthy", Kind: "helper", Body: "healthy", Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now.Add(2 * time.Second), UpdatedAt: now},
		} {
			state.OutboxMessages[msg.ID] = msg
		}
		return nil
	}); err != nil {
		t.Fatalf("seed retry schedule rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		badRaw := []byte(`{"id":"outbox:invalid-retry-schedule","teams_chat_id":"chat:retry-bad","status":"queued","sequence":1,"created_at":"2026-09-07T15:00:00Z","next_attempt_at":"not-a-time"}`)
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, badRaw, badID); err != nil {
			return err
		}
		blankRaw := []byte(`{"id":"outbox:blank-retry-schedule","teams_chat_id":"chat:retry-blank","status":"queued","sequence":1,"created_at":"2026-09-07T15:00:01Z","next_attempt_at":""}`)
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ?, deliver_after = ? WHERE id = ?`, blankRaw, now.Add(time.Hour).UnixNano(), blankID); err != nil {
			return err
		}
		return nil
	})

	pending, err := store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("pending rows with malformed retry schedule: %v", err)
	}
	ids := make([]string, 0, len(pending))
	for _, msg := range pending {
		ids = append(ids, msg.ID)
		if msg.ID == blankID && !msg.NextAttemptAt.IsZero() {
			t.Fatalf("blank legacy retry schedule hydrated as future: %#v", msg)
		}
	}
	if len(ids) != 2 || ids[0] != blankID || ids[1] != healthyID {
		t.Fatalf("pending retry IDs = %v, want [%s %s] (invalid row isolated)", ids, blankID, healthyID)
	}
	if _, err := store.OutboxMessageByID(ctx, badID); !errors.Is(err, ErrOutboxNotFound) {
		t.Fatalf("invalid retry row by-ID error = %v, want ErrOutboxNotFound quarantine", err)
	}
}

// FIFO admission must not skip over an untrusted predecessor merely because
// a bounded SQL page contains many malformed rows. Returning an indeterminate
// result is safe and retryable; sending a later sequence would be irreversible.
func TestSQLiteOutboxFIFOIndeterminateWhenBadPredecessorFillsBoundedPage(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 7, 16, 0, 0, 0, time.UTC)
	const chatID = "chat:fifo-indeterminate"
	const laterID = "outbox:fifo-later"
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[laterID] = OutboxMessage{
			ID: laterID, TeamsChatID: chatID, Kind: "helper", Body: "later", Status: OutboxStatusQueued,
			Sequence: 100, CreatedAt: now.Add(2 * time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed FIFO later row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for i := 1; i <= 64; i++ {
			id := fmt.Sprintf("outbox:fifo-bad-%03d", i)
			stamp := now.Add(time.Duration(i) * time.Second)
			raw := []byte(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":"queued","sequence":%d,"created_at":%q,"last_send_error":17}`, id, chatID, i, stamp.Format(time.RFC3339Nano)))
			if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, id, "", "", chatID, "", string(OutboxStatusQueued), i, sqliteTime(stamp), 0, 0, raw); err != nil {
				return err
			}
		}
		return nil
	})

	later, err := store.OutboxMessageByID(ctx, laterID)
	if err != nil {
		t.Fatalf("load FIFO later row: %v", err)
	}
	if earlier, found, err := store.EarlierUnsentOutbox(ctx, later); !errors.Is(err, ErrOutboxPredecessorIndeterminate) || found || earlier.ID != "" {
		t.Fatalf("bounded FIFO predecessor = %#v found=%v err=%v, want indeterminate", earlier, found, err)
	}
	if all, err := store.EarlierUnsentOutboxes(ctx, later); !errors.Is(err, ErrOutboxPredecessorIndeterminate) || len(all) != 0 {
		t.Fatalf("full FIFO predecessor list = %#v err=%v, want indeterminate", all, err)
	}
}

func TestSQLiteOutboxFIFOIndeterminateWhenJSONSequenceIsHiddenByScalar(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 7, 16, 30, 0, 0, time.UTC)
	const chatID = "chat:fifo-hidden-sequence"
	const earlierID = "outbox:fifo-hidden-earlier"
	const laterID = "outbox:fifo-hidden-later"
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[earlierID] = OutboxMessage{
			ID: earlierID, TeamsChatID: chatID, Kind: "helper", Body: "earlier", Status: OutboxStatusQueued,
			Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		state.OutboxMessages[laterID] = OutboxMessage{
			ID: laterID, TeamsChatID: chatID, Kind: "helper", Body: "later", Status: OutboxStatusQueued,
			Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed hidden-sequence FIFO rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// The canonical JSON still proves sequence 1, but the legacy indexed
		// projection says 99. SQL must consider both representations before the
		// Go decoder returns an indeterminate predecessor result.
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET sequence = ? WHERE id = ?`, 99, earlierID)
		return err
	})
	later, err := store.OutboxMessageByID(ctx, laterID)
	if err != nil {
		t.Fatalf("load hidden-sequence later row: %v", err)
	}
	if earlier, found, err := store.EarlierUnsentOutbox(ctx, later); !errors.Is(err, ErrOutboxPredecessorIndeterminate) || found || earlier.ID != "" {
		t.Fatalf("hidden-sequence single FIFO predecessor = %#v found=%v err=%v, want indeterminate", earlier, found, err)
	}
	if all, err := store.EarlierUnsentOutboxes(ctx, later); !errors.Is(err, ErrOutboxPredecessorIndeterminate) || len(all) != 0 {
		t.Fatalf("hidden-sequence batch FIFO predecessors = %#v err=%v, want indeterminate", all, err)
	}
}

// SQLite TEXT PRIMARY KEY does not reject a NULL key on every supported
// SQLite build. Preserve such a corrupt row byte-for-byte during an unrelated
// full-state rewrite instead of failing the rewrite or silently deleting the
// evidence needed for repair.
func TestSQLiteNullOutboxIDIsPreservedAcrossFullRewrite(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 7, 17, 0, 0, 0, time.UTC)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:null-id-seed", TeamsChatID: "chat:null-id", Status: OutboxStatusSent, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("seed null-id rewrite store: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	raw := []byte(`{"teams_chat_id":"chat:null-id","status":"sent"}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, "", "", "chat:null-id", "", string(OutboxStatusSent), 0, 0, 0, 0, raw)
		return err
	})
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatID = "unrelated-null-id-control"
		return nil
	}); err != nil {
		t.Fatalf("full rewrite with NULL outbox ID: %v", err)
	}
	var got []byte
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT json FROM outbox_messages WHERE id IS NULL`).Scan(&got)
	})
	if string(got) != string(raw) {
		t.Fatalf("NULL outbox ID raw payload = %q, want %q", got, raw)
	}
}
