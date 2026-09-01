package store

import (
	"context"
	"database/sql"
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
