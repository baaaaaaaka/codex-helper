package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadMissingReturnsEmptyState(t *testing.T) {
	store := newTestStore(t)

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", state.SchemaVersion, SchemaVersion)
	}
	if len(state.Sessions) != 0 || len(state.Turns) != 0 || len(state.InboundEvents) != 0 || len(state.OutboxMessages) != 0 {
		t.Fatalf("missing store should be empty: %#v", state)
	}
	if _, err := os.Stat(store.Path()); !os.IsNotExist(err) {
		t.Fatalf("Load should not create state file, stat err = %v", err)
	}
}

func TestSaveWritesCompactStateJSON(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", time.Now(), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatal("state JSON should keep a trailing newline")
	}
	if bytes.Contains(bytes.TrimSuffix(data, []byte("\n")), []byte("\n")) {
		t.Fatalf("state JSON contains interior newlines; want compact JSON: %q", data[:min(len(data), 120)])
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("compact state JSON is not readable: %v", err)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load compact state JSON error: %v", err)
	}
	if _, ok := loaded.ChatPolls["chat-1"]; !ok {
		t.Fatalf("Load compact state JSON missed chat-1: %#v", loaded.ChatPolls)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.SetPaused(ctx, true, "operator maintenance"); err != nil {
		t.Fatalf("SetPaused error: %v", err)
	}
	if _, err := store.SetDraining(ctx, "upgrade"); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}
	inbound, created, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	if !created {
		t.Fatal("PersistInbound created = false")
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if !created {
		t.Fatal("QueueTurn created = false")
	}
	if _, err := store.MarkTurnCompleted(ctx, turn.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnCompleted error: %v", err)
	}
	if _, created, err := store.QueueOutbox(ctx, OutboxMessage{
		SessionID:   "s1",
		TurnID:      turn.ID,
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done",
	}); err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	} else if !created {
		t.Fatal("QueueOutbox created = false")
	}

	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	state, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load reopened error: %v", err)
	}
	if got := state.Sessions["s1"].CodexThreadID; got != "thread-1" {
		t.Fatalf("CodexThreadID = %q, want thread-1", got)
	}
	if got := state.Sessions["s1"].LatestCodexTurnID; got != "codex-turn-1" {
		t.Fatalf("LatestCodexTurnID = %q, want codex-turn-1", got)
	}
	if got := state.Turns[turn.ID].Status; got != TurnStatusCompleted {
		t.Fatalf("turn status = %q, want %q", got, TurnStatusCompleted)
	}
	if got := len(state.OutboxMessages); got != 1 {
		t.Fatalf("outbox count = %d, want 1", got)
	}
	if !state.ServiceControl.Paused || !state.ServiceControl.Draining {
		t.Fatalf("service control flags did not roundtrip: %#v", state.ServiceControl)
	}
	if got := state.ServiceControl.Reason; got != "upgrade" {
		t.Fatalf("service control reason = %q, want upgrade", got)
	}
	if state.ServiceControl.UpdatedAt.IsZero() {
		t.Fatal("service control UpdatedAt is zero after roundtrip")
	}
}

func TestSQLiteMigrationRoundTripAndHotPaths(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.SetPaused(ctx, true, "sqlite migration test"); err != nil {
		t.Fatalf("SetPaused error: %v", err)
	}
	session := testSession()
	if _, created, err := store.CreateSession(ctx, session); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}
	inbound, created, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	if !created {
		t.Fatal("PersistInbound created = false")
	}
	historical := testInbound()
	historical.ID = "historical-inbound"
	historical.TeamsMessageID = "historical-message"
	historical.Text = "historical completed request"
	historical, created, err = store.PersistInbound(ctx, historical)
	if err != nil {
		t.Fatalf("PersistInbound historical error: %v", err)
	}
	if !created {
		t.Fatal("PersistInbound historical created = false")
	}
	historicalTurn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: historical.ID})
	if err != nil {
		t.Fatalf("QueueTurn historical error: %v", err)
	}
	if !created {
		t.Fatal("QueueTurn historical created = false")
	}
	if _, err := store.MarkTurnCompleted(ctx, historicalTurn.ID, "thread-historical", "codex-turn-historical"); err != nil {
		t.Fatalf("MarkTurnCompleted historical error: %v", err)
	}
	for i := 0; i < 16; i++ {
		other := testInbound()
		other.ID = fmt.Sprintf("other-inbound-%02d", i)
		other.SessionID = "other-session"
		other.TeamsChatID = "other-chat"
		other.TeamsMessageID = fmt.Sprintf("other-message-%02d", i)
		other.Text = strings.Repeat("long-message-", 128)
		if _, _, err := store.PersistInbound(ctx, other); err != nil {
			t.Fatalf("PersistInbound other %d error: %v", i, err)
		}
	}
	before, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load before migration error: %v", err)
	}
	result, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("MigrateLargeStateToSQLite error: %v", err)
	}
	if !result.Migrated || result.Path == "" {
		t.Fatalf("migration result = %#v, want migrated path", result)
	}
	after, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after migration error: %v", err)
	}
	if !stateLogicalEqual(before, after) {
		t.Fatalf("sqlite migration changed logical state: %s", sqliteStateSummaryDiff(before, after))
	}
	again, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("idempotent MigrateLargeStateToSQLite error: %v", err)
	}
	if !again.AlreadyDB {
		t.Fatalf("second migration result = %#v, want AlreadyDB", again)
	}
	loadedInbound, ok, err := store.InboundEventByID(ctx, inbound.ID)
	if err != nil || !ok || loadedInbound.ID != inbound.ID {
		t.Fatalf("InboundEventByID = %#v ok %v err %v, want %q", loadedInbound, ok, err, inbound.ID)
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn sqlite error: %v", err)
	}
	if !created {
		t.Fatal("QueueTurn sqlite created = false")
	}
	claimed, ok, err := store.ClaimNextQueuedTurn(ctx, session.ID)
	if err != nil || !ok {
		t.Fatalf("ClaimNextQueuedTurn sqlite ok %v err %v", ok, err)
	}
	if claimed.ID != turn.ID || claimed.Status != TurnStatusRunning {
		t.Fatalf("claimed turn = %#v, want running %q", claimed, turn.ID)
	}
	if again, ok, err := store.ClaimNextQueuedTurn(ctx, session.ID); err != nil || ok {
		t.Fatalf("second ClaimNextQueuedTurn = %#v ok %v err %v, want no claim", again, ok, err)
	}
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		SessionID:   session.ID,
		TurnID:      turn.ID,
		TeamsChatID: session.TeamsChatID,
		Kind:        "final",
		Body:        "done",
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox sqlite created %v err %v", created, err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); err != nil {
		t.Fatalf("MarkOutboxSendAttempt sqlite error: %v", err)
	}
	accepted, err := store.MarkOutboxAccepted(ctx, msg.ID, "teams-reply-1")
	if err != nil {
		t.Fatalf("MarkOutboxAccepted sqlite error: %v", err)
	}
	if accepted.Status != OutboxStatusAccepted || accepted.TeamsMessageID != "teams-reply-1" {
		t.Fatalf("accepted outbox = %#v", accepted)
	}
	pending, err := store.PendingOutboxAt(ctx, time.Now())
	if err != nil {
		t.Fatalf("PendingOutboxAt sqlite error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != msg.ID {
		t.Fatalf("pending after accept = %#v, want accepted message", pending)
	}
	if _, err := store.MarkOutboxSent(ctx, msg.ID, "teams-reply-1"); err != nil {
		t.Fatalf("MarkOutboxSent sqlite error: %v", err)
	}
	lookup, err := store.MessageLookup(ctx, session.TeamsChatID, "teams-reply-1")
	if err != nil {
		t.Fatalf("MessageLookup sqlite error: %v", err)
	}
	if !lookup.HasDeliveredOutbox || lookup.Provenance.OutboxID != msg.ID {
		t.Fatalf("MessageLookup = %#v, want delivered outbox %q", lookup, msg.ID)
	}
	store.mu.Lock()
	sqliteLookupBuiltFullCache := store.messageLookup.Valid
	store.mu.Unlock()
	if sqliteLookupBuiltFullCache {
		t.Fatal("SQLite MessageLookup built the full state lookup cache")
	}
	inboundLookup, err := store.MessageLookup(ctx, inbound.TeamsChatID, inbound.TeamsMessageID)
	if err != nil {
		t.Fatalf("MessageLookup sqlite inbound error: %v", err)
	}
	if !inboundLookup.HasInbound || inboundLookup.Provenance.InboundID != inbound.ID {
		t.Fatalf("MessageLookup inbound = %#v, want inbound %q", inboundLookup, inbound.ID)
	}
	activeQueueState, err := store.SessionActiveTurnQueueSnapshot(ctx, session.ID)
	if err != nil {
		t.Fatalf("SessionActiveTurnQueueSnapshot sqlite error: %v", err)
	}
	if _, ok := activeQueueState.Turns[turn.ID]; !ok {
		t.Fatalf("active session queue snapshot missing running turn %q", turn.ID)
	}
	if _, ok := activeQueueState.InboundEvents[inbound.ID]; !ok {
		t.Fatalf("active session queue snapshot missing running inbound %q", inbound.ID)
	}
	if _, ok := activeQueueState.InboundEvents[historical.ID]; ok {
		t.Fatal("active session queue snapshot included completed historical inbound")
	}
	fullQueueState, err := store.SessionTurnQueueSnapshot(ctx, session.ID)
	if err != nil {
		t.Fatalf("SessionTurnQueueSnapshot sqlite full error: %v", err)
	}
	if _, ok := fullQueueState.InboundEvents[historical.ID]; !ok {
		t.Fatal("full session queue snapshot lost historical inbound needed for duplicate detection")
	}
	if _, err := store.MarkTurnCompleted(ctx, turn.ID, "thread-sqlite", "codex-turn-sqlite"); err != nil {
		t.Fatalf("MarkTurnCompleted sqlite error: %v", err)
	}
	queueState, err := store.SessionTurnQueueSnapshot(ctx, session.ID)
	if err != nil {
		t.Fatalf("SessionTurnQueueSnapshot sqlite error: %v", err)
	}
	if _, ok := queueState.InboundEvents[inbound.ID]; !ok {
		t.Fatalf("session queue snapshot missing inbound %q", inbound.ID)
	}
	if _, ok := queueState.InboundEvents["other-inbound-00"]; ok {
		t.Fatal("session queue snapshot included unrelated inbound")
	}
	if _, err := store.RecordChatPollSuccessWithContinuationAndSchedule(ctx, session.TeamsChatID, time.Now(), true, false, 1, "", func(ChatPollState) (ChatPollScheduleUpdate, error) {
		return ChatPollScheduleUpdate{ChatID: session.TeamsChatID, PollState: "warm", NextPollAt: time.Now().Add(time.Second), ResetFailures: true, ClearBlockedUntil: true}, nil
	}); err != nil {
		t.Fatalf("RecordChatPollSuccessWithContinuationAndSchedule sqlite error: %v", err)
	}
	if limit, err := store.SetChatRateLimit(ctx, session.TeamsChatID, time.Now().Add(time.Minute), "429"); err != nil || limit.ChatID != session.TeamsChatID {
		t.Fatalf("SetChatRateLimit sqlite = %#v err %v", limit, err)
	}
	if err := store.ClearChatRateLimit(ctx, session.TeamsChatID); err != nil {
		t.Fatalf("ClearChatRateLimit sqlite error: %v", err)
	}
	finalState, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load final sqlite error: %v", err)
	}
	if got := finalState.Sessions[session.ID].CodexThreadID; got != "thread-sqlite" {
		t.Fatalf("session CodexThreadID = %q, want thread-sqlite", got)
	}
}

func TestSQLiteMigrationUpgradesLegacySchemaWithRecoverableBackup(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	source := []byte(`{
		"schema_version": 1,
		"created_at": "2026-04-30T01:00:00Z",
		"updated_at": "2026-04-30T01:01:00Z",
		"service_owner": {
			"pid": 4242,
			"hostname": "legacy-host",
			"executable_path": "/usr/local/bin/codex-helper",
			"helper_version": "v0.1.0",
			"active_session_id": "s1",
			"active_turn_id": "turn:legacy"
		},
		"sessions": {
			"s1": {
				"id": "s1",
				"status": "active",
				"teams_chat_id": "chat-1",
				"teams_chat_url": "https://teams.example/chat-1",
				"teams_topic": "legacy topic",
				"codex_thread_id": "thread-legacy",
				"latest_turn_id": "turn:legacy",
				"cwd": "/workspace/legacy"
			}
		},
		"turns": {
			"turn:legacy": {
				"id": "turn:legacy",
				"session_id": "s1",
				"inbound_event_id": "inbound:legacy",
				"status": "completed",
				"codex_thread_id": "thread-legacy"
			}
		},
		"inbound_events": {
			"inbound:legacy": {
				"id": "inbound:legacy",
				"session_id": "s1",
				"teams_chat_id": "chat-1",
				"teams_message_id": "message-legacy",
				"source": "teams",
				"status": "queued",
				"turn_id": "turn:legacy",
				"text": "legacy prompt"
			}
		},
		"outbox_messages": {
			"outbox:accepted": {
				"id": "outbox:accepted",
				"session_id": "s1",
				"turn_id": "turn:legacy",
				"teams_chat_id": "chat-1",
				"kind": "final",
				"body": "legacy answer",
				"status": "accepted",
				"teams_message_id": "message-helper"
			}
		},
		"chat_polls": {
			"chat-1": {
				"chat_id": "chat-1",
				"seeded": true
			}
		}
	}`)
	writeRawStoreStateForTest(t, store, source)

	expected, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load expected migrated legacy state error: %v", err)
	}
	sourceOnDisk, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read legacy source before migration: %v", err)
	}
	if !bytes.Equal(sourceOnDisk, source) {
		t.Fatalf("Load should not rewrite legacy state before SQLite migration")
	}

	result, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("MigrateLargeStateToSQLite legacy error: %v", err)
	}
	if !result.Migrated || result.AlreadyDB || result.Path == "" || result.MigrationID == "" {
		t.Fatalf("migration result = %#v, want migrated SQLite store", result)
	}
	if !stateLogicalEqual(expected, result.State) {
		t.Fatalf("migration result changed legacy state: %s", sqliteStateSummaryDiff(expected, result.State))
	}
	pointerData, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read sqlite pointer: %v", err)
	}
	pointer, ok, err := storeSQLitePointerFromData(pointerData)
	if err != nil || !ok {
		t.Fatalf("state file is not sqlite pointer: ok=%v err=%v data=%s", ok, err, string(pointerData))
	}
	if pointer.MigrationID != result.MigrationID {
		t.Fatalf("pointer migration id = %q, want %q", pointer.MigrationID, result.MigrationID)
	}
	if pointer.SourceSchemaVersion != 1 {
		t.Fatalf("source schema version = %d, want legacy version 1", pointer.SourceSchemaVersion)
	}
	if pointer.SourceSHA256 != sha256Bytes(sourceOnDisk) {
		t.Fatalf("source sha = %q, want %q", pointer.SourceSHA256, sha256Bytes(sourceOnDisk))
	}
	backupData, err := os.ReadFile(store.Path() + ".bak.sqlite." + result.MigrationID)
	if err != nil {
		t.Fatalf("read migration backup: %v", err)
	}
	if !bytes.Equal(backupData, sourceOnDisk) {
		t.Fatalf("migration backup differs from legacy source")
	}
	if _, err := os.Stat(result.Path); err != nil {
		t.Fatalf("sqlite db was not created at %q: %v", result.Path, err)
	}
	dbState, err := loadSQLiteStateFile(result.Path)
	if err != nil {
		t.Fatalf("load sqlite db file directly: %v", err)
	}
	if !stateLogicalEqual(expected, dbState) {
		t.Fatalf("sqlite db state differs from migrated legacy state: %s", sqliteStateSummaryDiff(expected, dbState))
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after legacy sqlite migration error: %v", err)
	}
	if !stateLogicalEqual(expected, loaded) {
		t.Fatalf("loaded sqlite legacy state differs: %s", sqliteStateSummaryDiff(expected, loaded))
	}
	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open migrated sqlite store error: %v", err)
	}
	reopenedState, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load reopened migrated sqlite store error: %v", err)
	}
	if !stateLogicalEqual(expected, reopenedState) {
		t.Fatalf("reopened sqlite legacy state differs: %s", sqliteStateSummaryDiff(expected, reopenedState))
	}
}

func TestSQLiteMigrationThresholdSkipLeavesLegacyStateUntouched(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", time.Now(), true, false, 1); err != nil {
		t.Fatalf("seed legacy state: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read legacy state before threshold migration: %v", err)
	}

	result, err := store.MigrateLargeStateToSQLite(ctx, int64(len(before)+1))
	if err != nil {
		t.Fatalf("threshold MigrateLargeStateToSQLite error: %v", err)
	}
	if result.Migrated || result.AlreadyDB || result.Path != "" || result.MigrationID != "" {
		t.Fatalf("threshold migration result = %#v, want no migration", result)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read legacy state after threshold migration: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("threshold migration modified legacy state")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)); !os.IsNotExist(err) {
		t.Fatalf("threshold migration should not create sqlite db, stat err = %v", err)
	}
}

func TestSQLiteMigrationMissingStateDoesNotCreatePointerOrDB(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	result, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("missing-state MigrateLargeStateToSQLite error: %v", err)
	}
	if result.Migrated || result.AlreadyDB || result.Path != "" || result.MigrationID != "" {
		t.Fatalf("missing-state migration result = %#v, want no migration", result)
	}
	if result.State.SchemaVersion != SchemaVersion {
		t.Fatalf("missing-state result schema = %d, want %d", result.State.SchemaVersion, SchemaVersion)
	}
	if _, err := os.Stat(store.Path()); !os.IsNotExist(err) {
		t.Fatalf("missing-state migration should not create state pointer, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)); !os.IsNotExist(err) {
		t.Fatalf("missing-state migration should not create sqlite db, stat err = %v", err)
	}
}

func TestSQLiteMigrationUnsupportedFutureSchemaFailsClosed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	source := []byte(`{"schema_version":999,"sessions":{"s1":{"id":"s1","teams_chat_id":"chat-1"}}}`)
	writeRawStoreStateForTest(t, store, source)

	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); !errors.Is(err, ErrUnsupportedSchemaVersion) || !strings.Contains(err.Error(), "999") {
		t.Fatalf("future schema migration error = %v, want unsupported schema version 999", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read future schema after failed migration: %v", err)
	}
	if !bytes.Equal(after, source) {
		t.Fatalf("future schema was modified after failed migration")
	}
	backups, err := filepath.Glob(store.Path() + ".bak.sqlite.*")
	if err != nil {
		t.Fatalf("glob migration backups: %v", err)
	}
	if len(backups) != 0 {
		t.Fatalf("failed migration wrote backups: %v", backups)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)); !os.IsNotExist(err) {
		t.Fatalf("failed migration should not create sqlite db, stat err = %v", err)
	}
}

func TestSQLiteMigrationRetryReplacesStaleDBAndSidecars(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	expected, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load expected stale-db migration state: %v", err)
	}
	dbPath := filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)
	if err := os.WriteFile(dbPath, []byte("stale sqlite from interrupted migration"), 0o600); err != nil {
		t.Fatalf("write stale sqlite db: %v", err)
	}
	if err := os.WriteFile(dbPath+"-wal", []byte("stale wal"), 0o600); err != nil {
		t.Fatalf("write stale sqlite wal: %v", err)
	}
	if err := os.WriteFile(dbPath+"-shm", []byte("stale shm"), 0o600); err != nil {
		t.Fatalf("write stale sqlite shm: %v", err)
	}

	result, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("retry MigrateLargeStateToSQLite with stale db error: %v", err)
	}
	if !result.Migrated || result.Path != dbPath {
		t.Fatalf("retry migration result = %#v, want migrated stale db replacement", result)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(dbPath + suffix); !os.IsNotExist(err) {
			t.Fatalf("stale sqlite sidecar %s remained after migration, stat err = %v", suffix, err)
		}
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after stale db retry migration: %v", err)
	}
	if !stateLogicalEqual(expected, loaded) {
		t.Fatalf("stale db retry migration changed state: %s", sqliteStateSummaryDiff(expected, loaded))
	}
}

func TestSQLiteMigrationFailureLeavesLegacyStateLoadableAndRetryable(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	expected, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load expected failure migration state: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read legacy state before failed migration: %v", err)
	}
	dbPath := filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)
	if err := os.Mkdir(dbPath, 0o700); err != nil {
		t.Fatalf("create blocking sqlite db directory: %v", err)
	}

	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err == nil {
		t.Fatal("MigrateLargeStateToSQLite succeeded with sqlite db path blocked by directory")
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read legacy state after failed migration: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("failed migration modified legacy state")
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after failed migration should still read legacy state: %v", err)
	}
	if !stateLogicalEqual(expected, loaded) {
		t.Fatalf("failed migration changed loadable legacy state: %s", sqliteStateSummaryDiff(expected, loaded))
	}
	if err := os.RemoveAll(dbPath); err != nil {
		t.Fatalf("remove blocking sqlite db directory: %v", err)
	}
	result, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("retry MigrateLargeStateToSQLite after clearing failure: %v", err)
	}
	if !result.Migrated {
		t.Fatalf("retry migration result = %#v, want migrated", result)
	}
	reloaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after retry migration: %v", err)
	}
	if !stateLogicalEqual(expected, reloaded) {
		t.Fatalf("retry migration changed state: %s", sqliteStateSummaryDiff(expected, reloaded))
	}
}

func TestSQLiteMigrationCrashStageMatrixLeavesLegacyStateRetryable(t *testing.T) {
	stages := []string{
		sqliteMigrationStageAfterBackup,
		sqliteMigrationStageAfterTempVerified,
		sqliteMigrationStageAfterDBReplace,
	}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			seedComplexLegacyStateForSQLiteMigrationTest(t, store)
			expected, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load expected state: %v", err)
			}
			before, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read legacy state before injected crash: %v", err)
			}
			withSQLiteMigrationTestHook(t, func(got string) error {
				if got == stage {
					return fmt.Errorf("injected sqlite migration crash at %s", stage)
				}
				return nil
			})

			if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err == nil || !strings.Contains(err.Error(), stage) {
				t.Fatalf("MigrateLargeStateToSQLite error = %v, want injected stage %q", err, stage)
			}
			after, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read legacy state after injected crash: %v", err)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("injected crash at %s modified state pointer/legacy json", stage)
			}
			legacyState, legacy, err := store.LoadLegacyJSONState(ctx)
			if err != nil || !legacy {
				t.Fatalf("LoadLegacyJSONState after injected crash = legacy %v err %v, want legacy", legacy, err)
			}
			if !stateLogicalEqual(expected, legacyState) {
				t.Fatalf("legacy state changed after injected crash: %s", sqliteStateSummaryDiff(expected, legacyState))
			}
			if stage == sqliteMigrationStageAfterDBReplace {
				dbState, err := loadSQLiteStateFile(filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
				if err != nil {
					t.Fatalf("load sqlite db left by after-db-replace crash: %v", err)
				}
				if !stateLogicalEqual(expected, dbState) {
					t.Fatalf("db left by after-db-replace crash differs: %s", sqliteStateSummaryDiff(expected, dbState))
				}
			}

			sqliteMigrationTestHook = nil
			result, err := store.MigrateLargeStateToSQLite(ctx, 0)
			if err != nil {
				t.Fatalf("retry migration after injected crash: %v", err)
			}
			if !result.Migrated {
				t.Fatalf("retry result = %#v, want migrated", result)
			}
			reloaded, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after retry migration: %v", err)
			}
			if !stateLogicalEqual(expected, reloaded) {
				t.Fatalf("retry after injected crash changed state: %s", sqliteStateSummaryDiff(expected, reloaded))
			}
		})
	}
}

func TestSQLiteMigrationProcessBoundaryStressAfterUpgrade(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), storeConcurrentTestTimeout(30*time.Second))
	defer cancel()
	seedComplexLegacyStateForSQLiteMigrationTest(t, store)
	result := migrateStoreToSQLiteForTest(t, store)
	if result.Path == "" {
		t.Fatalf("migration result missing sqlite path: %#v", result)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close original migrated store: %v", err)
	}

	const workers = 8
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerStore, err := Open(store.Path())
			if err != nil {
				errCh <- fmt.Errorf("open upgraded store worker %d: %w", worker, err)
				return
			}
			defer workerStore.Close()
			sessionID := fmt.Sprintf("upgrade-session-%02d", worker%4)
			chatID := fmt.Sprintf("upgrade-chat-%02d", worker%4)
			inbound, created, err := workerStore.PersistInbound(ctx, InboundEvent{
				SessionID:      sessionID,
				TeamsChatID:    chatID,
				TeamsMessageID: fmt.Sprintf("upgrade-message-%02d", worker),
				Source:         "teams",
				Text:           fmt.Sprintf("prompt from upgraded worker %02d", worker),
			})
			if err != nil {
				errCh <- fmt.Errorf("persist inbound worker %d: %w", worker, err)
				return
			}
			if !created {
				errCh <- fmt.Errorf("persist inbound worker %d was not created", worker)
				return
			}
			turn, created, err := workerStore.QueueTurn(ctx, Turn{SessionID: sessionID, InboundEventID: inbound.ID})
			if err != nil {
				errCh <- fmt.Errorf("queue turn worker %d: %w", worker, err)
				return
			}
			if !created {
				errCh <- fmt.Errorf("queue turn worker %d was not created", worker)
				return
			}
			outbox, created, err := workerStore.QueueOutbox(ctx, OutboxMessage{
				ID:          fmt.Sprintf("upgrade-outbox-%02d", worker),
				SessionID:   sessionID,
				TurnID:      turn.ID,
				TeamsChatID: chatID,
				Kind:        "final",
				Body:        fmt.Sprintf("answer from upgraded worker %02d", worker),
			})
			if err != nil {
				errCh <- fmt.Errorf("queue outbox worker %d: %w", worker, err)
				return
			}
			if !created {
				errCh <- fmt.Errorf("queue outbox worker %d was not created", worker)
				return
			}
			if _, err := workerStore.MarkTurnCompleted(ctx, turn.ID, "thread-upgrade", "codex-turn-upgrade"); err != nil {
				errCh <- fmt.Errorf("mark turn completed worker %d: %w", worker, err)
				return
			}
			if _, err := workerStore.MarkOutboxSent(ctx, outbox.ID, fmt.Sprintf("teams-upgrade-%02d", worker)); err != nil {
				errCh <- fmt.Errorf("mark outbox sent worker %d: %w", worker, err)
				return
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open upgraded store after concurrent workers: %v", err)
	}
	defer reopened.Close()
	state, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load upgraded store after concurrent workers: %v", err)
	}
	for worker := 0; worker < workers; worker++ {
		messageID := fmt.Sprintf("upgrade-message-%02d", worker)
		lookup, err := reopened.MessageLookup(ctx, fmt.Sprintf("upgrade-chat-%02d", worker%4), messageID)
		if err != nil {
			t.Fatalf("MessageLookup worker %d: %v", worker, err)
		}
		if !lookup.HasInbound {
			t.Fatalf("MessageLookup worker %d missing inbound for %q: %#v", worker, messageID, lookup)
		}
		outboxID := fmt.Sprintf("upgrade-outbox-%02d", worker)
		outbox, ok := state.OutboxMessages[outboxID]
		if !ok || outbox.Status != OutboxStatusSent || outbox.TeamsMessageID == "" {
			t.Fatalf("outbox %q after upgrade workers = %#v ok=%v", outboxID, outbox, ok)
		}
	}
}

func TestSQLitePointerMissingDBFailsClosedAndDoesNotCreateEmptyStore(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	result := migrateStoreToSQLiteForTest(t, store)
	if err := store.Close(); err != nil {
		t.Fatalf("Close migrated store: %v", err)
	}
	if err := removeSQLiteSidecarFiles(result.Path); err != nil {
		t.Fatalf("remove sqlite sidecars: %v", err)
	}
	if err := os.Remove(result.Path); err != nil {
		t.Fatalf("remove migrated sqlite db: %v", err)
	}

	if _, err := store.Load(ctx); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Load missing sqlite db error = %v, want fail-closed missing-db error", err)
	}
	if _, err := store.PollStateSnapshot(ctx); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("PollStateSnapshot missing sqlite db error = %v, want fail-closed missing-db error", err)
	}
	if _, _, err := store.PersistInbound(ctx, testInbound()); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("PersistInbound missing sqlite db error = %v, want fail-closed missing-db error", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("MigrateLargeStateToSQLite missing sqlite db error = %v, want fail-closed missing-db error", err)
	}
	if _, err := os.Stat(result.Path); !os.IsNotExist(err) {
		t.Fatalf("missing sqlite db was recreated, stat err = %v", err)
	}
}

func TestSQLitePointerUninitializedOrCorruptDBFailsClosed(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		seed func(t *testing.T, path string)
		want string
	}{
		{
			name: "empty file",
			seed: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatalf("write empty sqlite db: %v", err)
				}
			},
			want: "not initialized",
		},
		{
			name: "schema without state_json",
			seed: func(t *testing.T, path string) {
				t.Helper()
				db, err := openSQLiteStore(path, true)
				if err != nil {
					t.Fatalf("open sqlite db without state_json: %v", err)
				}
				defer db.Close()
				if err := ensureSQLiteSchema(db); err != nil {
					t.Fatalf("ensure sqlite schema without state_json: %v", err)
				}
			},
			want: "state metadata",
		},
		{
			name: "corrupt file",
			seed: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("not a sqlite database"), 0o600); err != nil {
					t.Fatalf("write corrupt sqlite db: %v", err)
				}
			},
			want: "database",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			writeSQLitePointerForTest(t, store, storeSQLiteFileName)
			dbPath := filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)
			tc.seed(t, dbPath)
			before, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read sqlite pointer before Load: %v", err)
			}
			if _, err := store.Load(ctx); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("Load %s error = %v, want substring %q", tc.name, err, tc.want)
			}
			after, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read sqlite pointer after Load: %v", err)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("Load %s modified sqlite pointer", tc.name)
			}
		})
	}
}

func TestSQLitePointerPathValidationFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{name: "absolute", path: filepath.Join(t.TempDir(), "external.sqlite"), want: "absolute paths"},
		{name: "parent", path: "../store.sqlite", want: "expected"},
		{name: "subdir", path: "subdir/store.sqlite", want: "expected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			writeSQLitePointerForTest(t, store, tc.path)
			before, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read invalid pointer before Load: %v", err)
			}
			if _, err := store.Load(context.Background()); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load invalid pointer path error = %v, want substring %q", err, tc.want)
			}
			if _, err := store.SetPaused(context.Background(), true, "must not write"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("SetPaused invalid pointer path error = %v, want substring %q", err, tc.want)
			}
			after, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read invalid pointer after failed update: %v", err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("failed invalid-pointer update modified state pointer")
			}
		})
	}
}

func TestSQLitePointerSchemaRejectsLegacyV5Loaders(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)

	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read sqlite pointer: %v", err)
	}
	var pointer storeSQLitePointer
	if err := json.Unmarshal(data, &pointer); err != nil {
		t.Fatalf("unmarshal sqlite pointer: %v", err)
	}
	if pointer.SchemaVersion <= SchemaVersion {
		t.Fatalf("sqlite pointer schema_version = %d, want greater than legacy schema %d", pointer.SchemaVersion, SchemaVersion)
	}
	if _, err := legacyV5LoadStateDataForTest(data); !errors.Is(err, ErrUnsupportedSchemaVersion) || !strings.Contains(err.Error(), fmt.Sprint(pointer.SchemaVersion)) {
		t.Fatalf("legacy v5 loader error = %v, want unsupported pointer schema %d", err, pointer.SchemaVersion)
	}
	if _, err := store.Load(ctx); err != nil {
		t.Fatalf("current loader should still read sqlite pointer: %v", err)
	}
}

func TestSQLitePointerUnsupportedSchemaFailsClosed(t *testing.T) {
	for _, schemaVersion := range []int{0, storeSQLitePointerSchemaVersion - 1, storeSQLitePointerSchemaVersion + 1} {
		t.Run(fmt.Sprintf("schema=%d", schemaVersion), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			seedLegacyStateFileForSQLiteMigrationTest(t, store)
			migrateStoreToSQLiteForTest(t, store)
			data, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read sqlite pointer: %v", err)
			}
			var pointer map[string]any
			if err := json.Unmarshal(data, &pointer); err != nil {
				t.Fatalf("unmarshal sqlite pointer: %v", err)
			}
			pointer["schema_version"] = schemaVersion
			data, err = json.Marshal(pointer)
			if err != nil {
				t.Fatalf("marshal sqlite pointer: %v", err)
			}
			data = append(data, '\n')
			if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
				t.Fatalf("write unsupported sqlite pointer: %v", err)
			}
			before := append([]byte(nil), data...)
			if _, err := store.Load(ctx); !errors.Is(err, ErrUnsupportedSchemaVersion) || !strings.Contains(err.Error(), fmt.Sprint(schemaVersion)) {
				t.Fatalf("Load unsupported pointer schema error = %v, want schema %d", err, schemaVersion)
			}
			if _, err := store.SetPaused(ctx, true, "must not write"); !errors.Is(err, ErrUnsupportedSchemaVersion) || !strings.Contains(err.Error(), fmt.Sprint(schemaVersion)) {
				t.Fatalf("SetPaused unsupported pointer schema error = %v, want schema %d", err, schemaVersion)
			}
			if _, err := store.MigrateLargeStateToSQLite(ctx, 0); !errors.Is(err, ErrUnsupportedSchemaVersion) || !strings.Contains(err.Error(), fmt.Sprint(schemaVersion)) {
				t.Fatalf("MigrateLargeStateToSQLite unsupported pointer schema error = %v, want schema %d", err, schemaVersion)
			}
			after, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read unsupported sqlite pointer after failed updates: %v", err)
			}
			if !bytes.Equal(after, before) {
				t.Fatalf("unsupported pointer schema state was modified")
			}
		})
	}
}

func TestSQLiteMigrationFailureOnStaleSidecarLeavesLegacyState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read legacy state before sidecar failure: %v", err)
	}
	dbPath := filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)
	sidecarDir := dbPath + "-wal"
	if err := os.Mkdir(sidecarDir, 0o700); err != nil {
		t.Fatalf("create stale sidecar dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sidecarDir, "locked"), []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale sidecar child: %v", err)
	}

	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err == nil || !strings.Contains(err.Error(), "remove sqlite sidecar") {
		t.Fatalf("MigrateLargeStateToSQLite stale sidecar error = %v, want sidecar removal error", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read legacy state after sidecar failure: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("sidecar migration failure modified legacy state")
	}
	if _, legacy, err := store.LoadLegacyJSONState(ctx); err != nil || !legacy {
		t.Fatalf("LoadLegacyJSONState after sidecar failure = legacy %v err %v, want legacy", legacy, err)
	}
}

func TestSQLiteExistingDBMissingHotTablesFailsClosed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	dbPath := filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		t.Fatalf("create sqlite dir: %v", err)
	}
	db, err := openSQLiteStore(dbPath, true)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE state_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)`); err != nil {
		_ = db.Close()
		t.Fatalf("create state_meta: %v", err)
	}
	cold, err := json.Marshal(coldSQLiteState(newState()))
	if err != nil {
		_ = db.Close()
		t.Fatalf("marshal cold state: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_meta(key, value) VALUES ('state_json', ?)`, cold); err != nil {
		_ = db.Close()
		t.Fatalf("insert state_meta: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close partial sqlite db: %v", err)
	}
	writeSQLitePointerForTest(t, store, storeSQLiteFileName)

	if _, err := store.Load(ctx); err == nil || !strings.Contains(err.Error(), `missing required table "sessions"`) {
		t.Fatalf("Load partial sqlite db error = %v, want missing required table", err)
	}
	if _, err := store.SetPaused(ctx, true, "must not write"); err == nil || !strings.Contains(err.Error(), `missing required table "sessions"`) {
		t.Fatalf("SetPaused partial sqlite db error = %v, want missing required table", err)
	}
}

func TestSQLiteStoreUsesFullSynchronousMode(t *testing.T) {
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	result := migrateStoreToSQLiteForTest(t, store)
	db, err := openExistingSQLiteStore(result.Path)
	if err != nil {
		t.Fatalf("open migrated sqlite db: %v", err)
	}
	defer db.Close()
	var synchronous string
	if err := db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatalf("read synchronous pragma: %v", err)
	}
	if synchronous != "2" && !strings.EqualFold(synchronous, "FULL") {
		t.Fatalf("PRAGMA synchronous = %q, want FULL/2", synchronous)
	}
}

func TestSQLiteFileURIWindowsPaths(t *testing.T) {
	query := url.Values{"mode": []string{"rwc"}}
	cases := []struct {
		name string
		path string
		want string
	}{
		{
			name: "drive absolute",
			path: `D:\a\codex helper\store.sqlite`,
			want: "file:///D:/a/codex%20helper/store.sqlite?mode=rwc",
		},
		{
			name: "unc path",
			path: `\\server\share\codex helper\store.sqlite`,
			want: "file://server/share/codex%20helper/store.sqlite?mode=rwc",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := sqliteWindowsFileURL(tc.path)
			u.RawQuery = query.Encode()
			got := u.String()
			if got != tc.want {
				t.Fatalf("sqliteWindowsFileURL(%q) = %q, want %q", tc.path, got, tc.want)
			}
			if strings.Contains(got, `\`) || strings.Contains(got, "%5C") {
				t.Fatalf("sqlite Windows file URI should not contain raw or escaped backslashes: %q", got)
			}
		})
	}
}

func TestSQLiteRecordMessageProvenanceNoopMatchesLegacy(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			if _, err := store.SetPaused(ctx, true, "seed no-op provenance"); err != nil {
				t.Fatalf("seed no-op provenance state: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			before, _ := store.Load(ctx)
			record, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
				TeamsChatID:    "  ",
				TeamsMessageID: "\t",
				Origin:         MessageOriginHelperOutbox,
			})
			if err != nil {
				t.Fatalf("RecordMessageProvenance no-op error: %v", err)
			}
			if record.ID != "" {
				t.Fatalf("no-op RecordMessageProvenance returned %#v, want zero record", record)
			}
			after, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after no-op provenance: %v", err)
			}
			if !stateLogicalEqual(before, after) {
				t.Fatalf("no-op provenance changed state: %s", sqliteStateSummaryDiff(before, after))
			}
		})
	}
}

func TestSQLiteTrimmedMessageIndexesMatchLegacyLookupSemantics(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.InboundEvents["inbound-spaced"] = InboundEvent{
			ID:             "inbound-spaced",
			SessionID:      "session-spaced",
			TeamsChatID:    " chat-spaced ",
			TeamsMessageID: " message-spaced ",
			Status:         InboundStatusPersisted,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		state.OutboxMessages["outbox-spaced"] = OutboxMessage{
			ID:             "outbox-spaced",
			SessionID:      "session-spaced",
			TeamsChatID:    " chat-outbox ",
			TeamsMessageID: " message-outbox ",
			Status:         OutboxStatusSent,
			CreatedAt:      now,
			UpdatedAt:      now,
			SentAt:         now,
		}
		state.MessageProvenance["legacy-provenance-spaced"] = MessageProvenanceRecord{
			ID:             "legacy-provenance-spaced",
			TeamsChatID:    " chat-provenance ",
			TeamsMessageID: " message-provenance ",
			Origin:         MessageOriginUserInbound,
			InboundID:      "inbound-provenance",
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed spaced legacy state: %v", err)
	}
	legacyInbound, err := store.MessageLookup(ctx, "chat-spaced", "message-spaced")
	if err != nil || !legacyInbound.HasInbound {
		t.Fatalf("legacy spaced inbound lookup = %#v err=%v", legacyInbound, err)
	}
	legacyOutbox, err := store.MessageLookup(ctx, "chat-outbox", "message-outbox")
	if err != nil || !legacyOutbox.HasDeliveredOutbox {
		t.Fatalf("legacy spaced outbox lookup = %#v err=%v", legacyOutbox, err)
	}
	legacyProvenance, err := store.MessageLookup(ctx, "chat-provenance", "message-provenance")
	if err != nil || !legacyProvenance.HasProvenance || legacyProvenance.Provenance.ID != "legacy-provenance-spaced" {
		t.Fatalf("legacy spaced provenance lookup = %#v err=%v", legacyProvenance, err)
	}

	migrateStoreToSQLiteForTest(t, store)
	sqliteInbound, err := store.MessageLookup(ctx, "chat-spaced", "message-spaced")
	if err != nil || !messageLookupEqual(sqliteInbound, legacyInbound) {
		t.Fatalf("sqlite spaced inbound lookup = %#v err=%v, want %#v", sqliteInbound, err, legacyInbound)
	}
	sqliteOutbox, err := store.MessageLookup(ctx, "chat-outbox", "message-outbox")
	if err != nil || !messageLookupEqual(sqliteOutbox, legacyOutbox) {
		t.Fatalf("sqlite spaced outbox lookup = %#v err=%v, want %#v", sqliteOutbox, err, legacyOutbox)
	}
	sqliteProvenance, err := store.MessageLookup(ctx, "chat-provenance", "message-provenance")
	if err != nil || !messageLookupEqual(sqliteProvenance, legacyProvenance) {
		t.Fatalf("sqlite spaced provenance lookup = %#v err=%v, want %#v", sqliteProvenance, err, legacyProvenance)
	}
	duplicate, created, err := store.PersistInbound(ctx, InboundEvent{
		ID:             "new-duplicate-spaced",
		SessionID:      "session-spaced",
		TeamsChatID:    "chat-spaced",
		TeamsMessageID: "message-spaced",
		Text:           "duplicate after trim",
	})
	if err != nil || created || duplicate.ID != "inbound-spaced" {
		t.Fatalf("sqlite trimmed duplicate inbound = %#v created=%v err=%v", duplicate, created, err)
	}
	if _, _, err := store.PersistInbound(ctx, InboundEvent{
		ID:             "helper-echo",
		SessionID:      "session-spaced",
		TeamsChatID:    "chat-outbox",
		TeamsMessageID: "message-outbox",
		Text:           "helper echo",
	}); !errors.Is(err, ErrInboundMessageFromHelperOutbox) {
		t.Fatalf("sqlite trimmed helper echo PersistInbound error = %v, want ErrInboundMessageFromHelperOutbox", err)
	}
}

func TestSQLiteHotPathCreatePersistAndTurnLifecycle(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)

	session := testSession()
	session.ID = "sqlite-session"
	session.TeamsChatID = "sqlite-chat"
	session.CodexThreadID = "thread-session"
	createdSession, created, err := store.CreateSession(ctx, session)
	if err != nil || !created {
		t.Fatalf("CreateSession sqlite created=%v err=%v", created, err)
	}
	againSession, created, err := store.CreateSession(ctx, session)
	if err != nil || created || againSession.ID != createdSession.ID {
		t.Fatalf("duplicate CreateSession sqlite = %#v created=%v err=%v", againSession, created, err)
	}

	inbound := testInbound()
	inbound.ID = "sqlite-inbound-1"
	inbound.SessionID = session.ID
	inbound.TeamsChatID = session.TeamsChatID
	inbound.TeamsMessageID = "sqlite-message-1"
	inbound.Text = "run sqlite lifecycle"
	persisted, created, err := store.PersistInbound(ctx, inbound)
	if err != nil || !created {
		t.Fatalf("PersistInbound sqlite created=%v err=%v", created, err)
	}
	duplicateInbound, created, err := store.PersistInbound(ctx, inbound)
	if err != nil || created || duplicateInbound.ID != persisted.ID {
		t.Fatalf("duplicate PersistInbound sqlite = %#v created=%v err=%v", duplicateInbound, created, err)
	}
	if lookup, err := store.MessageLookup(ctx, session.TeamsChatID, inbound.TeamsMessageID); err != nil || !lookup.HasInbound || lookup.Provenance.InboundID != inbound.ID {
		t.Fatalf("MessageLookup persisted inbound = %#v err=%v", lookup, err)
	}

	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil || !created {
		t.Fatalf("QueueTurn sqlite created=%v err=%v", created, err)
	}
	duplicateTurn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil || created || duplicateTurn.ID != turn.ID {
		t.Fatalf("duplicate QueueTurn sqlite = %#v created=%v err=%v", duplicateTurn, created, err)
	}
	running, err := store.MarkTurnRunning(ctx, turn.ID, "thread-running", "codex-running")
	if err != nil {
		t.Fatalf("MarkTurnRunning sqlite error: %v", err)
	}
	if running.Status != TurnStatusRunning || running.CodexThreadID != "thread-running" || running.CodexTurnID != "codex-running" {
		t.Fatalf("running turn = %#v", running)
	}
	failed, err := store.MarkTurnFailedWithCodexIDs(ctx, turn.ID, "synthetic failure", "thread-failed", "codex-failed")
	if err != nil {
		t.Fatalf("MarkTurnFailedWithCodexIDs sqlite error: %v", err)
	}
	if failed.Status != TurnStatusFailed || failed.FailureMessage != "synthetic failure" || failed.CodexThreadID != "thread-failed" || failed.CodexTurnID != "codex-failed" {
		t.Fatalf("failed turn = %#v", failed)
	}
	if byID, ok, err := store.TurnByID(ctx, turn.ID); err != nil || !ok || byID.Status != TurnStatusFailed {
		t.Fatalf("TurnByID failed turn = %#v ok=%v err=%v", byID, ok, err)
	}

	interruptedInbound := inbound
	interruptedInbound.ID = "sqlite-inbound-interrupted"
	interruptedInbound.TeamsMessageID = "sqlite-message-interrupted"
	interruptedInbound.Text = "interrupt me"
	if _, created, err := store.PersistInbound(ctx, interruptedInbound); err != nil || !created {
		t.Fatalf("PersistInbound interrupted created=%v err=%v", created, err)
	}
	interruptedTurn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: interruptedInbound.ID})
	if err != nil || !created {
		t.Fatalf("QueueTurn interrupted created=%v err=%v", created, err)
	}
	interrupted, err := store.MarkTurnInterrupted(ctx, interruptedTurn.ID, "operator stop")
	if err != nil {
		t.Fatalf("MarkTurnInterrupted sqlite error: %v", err)
	}
	if interrupted.Status != TurnStatusInterrupted || interrupted.RecoveryReason != "operator stop" {
		t.Fatalf("interrupted turn = %#v", interrupted)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after sqlite lifecycle error: %v", err)
	}
	if state.InboundEvents[interruptedInbound.ID].Status != InboundStatusIgnored {
		t.Fatalf("interrupted inbound status = %q, want ignored", state.InboundEvents[interruptedInbound.ID].Status)
	}
	if state.Sessions[session.ID].LatestTurnID != interruptedTurn.ID {
		t.Fatalf("latest turn = %q, want %q", state.Sessions[session.ID].LatestTurnID, interruptedTurn.ID)
	}
}

func TestSQLiteOutboxErrorDriveArtifactAndHelperSideEffects(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)
	session := testSession()
	session.ID = "sqlite-outbox-session"
	session.TeamsChatID = "sqlite-outbox-chat"
	session.CodexThreadID = "thread-outbox"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession sqlite outbox created=%v err=%v", created, err)
	}
	inbound := testInbound()
	inbound.ID = "sqlite-outbox-inbound"
	inbound.SessionID = session.ID
	inbound.TeamsChatID = session.TeamsChatID
	inbound.TeamsMessageID = "sqlite-outbox-user-message"
	if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
		t.Fatalf("PersistInbound sqlite outbox created=%v err=%v", created, err)
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil || !created {
		t.Fatalf("QueueTurn sqlite outbox created=%v err=%v", created, err)
	}
	if _, err := store.MarkTurnRunning(ctx, turn.ID, "thread-outbox", "codex-outbox"); err != nil {
		t.Fatalf("MarkTurnRunning sqlite outbox error: %v", err)
	}
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:                     "sqlite-outbox-status",
		SessionID:              session.ID,
		TurnID:                 turn.ID,
		TeamsChatID:            session.TeamsChatID,
		Kind:                   "status-progress",
		Body:                   "uploading artifact",
		AttachmentName:         "report.txt",
		AttachmentUploadName:   "report-upload.txt",
		ArtifactIDs:            []string{"artifact:sqlite"},
		Status:                 OutboxStatusQueued,
		MentionOwner:           true,
		AttachmentUploadFolder: "folder",
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox sqlite side-effect created=%v err=%v", created, err)
	}
	duplicate, created, err := store.QueueOutbox(ctx, msg)
	if err != nil || created || duplicate.ID != msg.ID {
		t.Fatalf("duplicate QueueOutbox sqlite = %#v created=%v err=%v", duplicate, created, err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); err != nil {
		t.Fatalf("MarkOutboxSendAttempt sqlite side-effect error: %v", err)
	}
	uploaded, err := store.MarkOutboxDriveItem(ctx, msg.ID, " drive-item-1 ", " report.txt ", " etag-1 ", " https://sharepoint/report ", " dav://report ")
	if err != nil {
		t.Fatalf("MarkOutboxDriveItem sqlite side-effect error: %v", err)
	}
	if uploaded.DriveItemID != "drive-item-1" || uploaded.DriveItemName != "report.txt" || uploaded.LastSendError != "" {
		t.Fatalf("uploaded outbox = %#v", uploaded)
	}
	errored, err := store.MarkOutboxSendError(ctx, msg.ID, "synthetic send failure")
	if err != nil {
		t.Fatalf("MarkOutboxSendError sqlite side-effect error: %v", err)
	}
	if errored.Status != OutboxStatusQueued || errored.LastSendError != "synthetic send failure" {
		t.Fatalf("errored outbox = %#v", errored)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); err != nil {
		t.Fatalf("second MarkOutboxSendAttempt sqlite side-effect error: %v", err)
	}
	sent, err := store.MarkOutboxSent(ctx, msg.ID, "teams-side-effect")
	if err != nil {
		t.Fatalf("MarkOutboxSent sqlite side-effect error: %v", err)
	}
	if sent.Status != OutboxStatusSent || sent.TeamsMessageID != "teams-side-effect" || sent.SentAt.IsZero() {
		t.Fatalf("sent side-effect outbox = %#v", sent)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load sqlite side-effect state error: %v", err)
	}
	artifact := state.ArtifactRecords["artifact:sqlite"]
	if artifact.Status != "uploaded" || artifact.OutboxID != msg.ID || artifact.DriveItemID != "drive-item-1" || artifact.TeamsMessageID != "teams-side-effect" || artifact.UploadedAt.IsZero() || artifact.SentAt.IsZero() {
		t.Fatalf("artifact side effects = %#v", artifact)
	}
	if len(state.HelperDeliveries) != 1 {
		t.Fatalf("helper deliveries = %#v, want one stable record", state.HelperDeliveries)
	}
	for _, delivery := range state.HelperDeliveries {
		if delivery.OutboxID != msg.ID || delivery.Status != HelperDeliveryStatusSent || delivery.TeamsMessageID != "teams-side-effect" || delivery.SentAt.IsZero() {
			t.Fatalf("helper delivery side effects = %#v", delivery)
		}
	}
	if lookup, err := store.MessageLookup(ctx, session.TeamsChatID, "teams-side-effect"); err != nil || !lookup.HasDeliveredOutbox || lookup.Provenance.OutboxID != msg.ID {
		t.Fatalf("MessageLookup sent side-effect = %#v err=%v", lookup, err)
	}
}

func TestSQLiteSelectedSnapshotsMatchExpectedFields(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 24, 1, 2, 3, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ServiceOwner = &OwnerMetadata{PID: 4242, Hostname: "host", HelperVersion: "v0.1.0", LastHeartbeat: now}
		state.ControlChat = ControlChatBinding{TeamsChatID: "control-chat", UpdatedAt: now}
		state.Workflow = WorkflowNotificationConfig{Enabled: true, ControlChatID: "control-chat", UpdatedAt: now}
		state.Sessions["s1"] = SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1", LatestTurnID: "turn-running", CreatedAt: now, UpdatedAt: now}
		state.Sessions["s2"] = SessionContext{ID: "s2", Status: SessionStatusActive, TeamsChatID: "chat-2", LatestTurnID: "turn-other", CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-queued"] = Turn{ID: "turn-queued", SessionID: "s1", InboundEventID: "inbound-queued", Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-running"] = Turn{ID: "turn-running", SessionID: "s1", InboundEventID: "inbound-running", Status: TurnStatusRunning, QueuedAt: now, StartedAt: now, CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-completed"] = Turn{ID: "turn-completed", SessionID: "s1", InboundEventID: "inbound-completed", Status: TurnStatusCompleted, QueuedAt: now, CompletedAt: now, CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-other"] = Turn{ID: "turn-other", SessionID: "s2", InboundEventID: "inbound-other", Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now}
		state.InboundEvents["inbound-queued"] = InboundEvent{ID: "inbound-queued", SessionID: "s1", TeamsChatID: "chat-1", TeamsMessageID: "message-queued", Status: InboundStatusQueued, TurnID: "turn-queued", CreatedAt: now, UpdatedAt: now}
		state.InboundEvents["inbound-running"] = InboundEvent{ID: "inbound-running", SessionID: "s1", TeamsChatID: "chat-1", TeamsMessageID: "message-running", Status: InboundStatusQueued, TurnID: "turn-running", CreatedAt: now, UpdatedAt: now}
		state.InboundEvents["inbound-completed"] = InboundEvent{ID: "inbound-completed", SessionID: "s1", TeamsChatID: "chat-1", TeamsMessageID: "message-completed", Status: InboundStatusIgnored, TurnID: "turn-completed", CreatedAt: now, UpdatedAt: now}
		state.InboundEvents["inbound-other"] = InboundEvent{ID: "inbound-other", SessionID: "s2", TeamsChatID: "chat-2", TeamsMessageID: "message-other", Status: InboundStatusQueued, TurnID: "turn-other", CreatedAt: now, UpdatedAt: now}
		state.OutboxMessages["outbox-1"] = OutboxMessage{ID: "outbox-1", SessionID: "s1", TurnID: "turn-running", TeamsChatID: "chat-1", Kind: "final", Body: "pending", Status: OutboxStatusQueued, CreatedAt: now, UpdatedAt: now}
		state.ImportCheckpoints["import-1"] = ImportCheckpoint{ID: "import-1", SessionID: "s1", LastRecordID: "record-1", Status: "complete", UpdatedAt: now}
		state.ChatPolls["chat-1"] = ChatPollState{ChatID: "chat-1", Seeded: true, PollState: "warm", NextPollAt: now.Add(time.Minute), UpdatedAt: now}
		state.ChatRateLimits["chat-1"] = ChatRateLimitState{ChatID: "chat-1", BlockedUntil: now.Add(time.Hour), Reason: "429", UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed selected snapshot state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	pollState, err := store.PollStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("PollStateSnapshot sqlite error: %v", err)
	}
	if len(pollState.Sessions) != 2 || len(pollState.Turns) != 4 || len(pollState.InboundEvents) != 4 || pollState.ChatPolls["chat-1"].PollState != "warm" || pollState.ServiceOwner == nil {
		t.Fatalf("poll snapshot missing selected fields: %#v", pollState)
	}
	if len(pollState.OutboxMessages) != 0 || len(pollState.ChatRateLimits) != 0 {
		t.Fatalf("poll snapshot included unselected fields: outbox=%d rate_limits=%d", len(pollState.OutboxMessages), len(pollState.ChatRateLimits))
	}
	active, err := store.SessionActiveTurnQueueSnapshot(ctx, "s1")
	if err != nil {
		t.Fatalf("SessionActiveTurnQueueSnapshot sqlite error: %v", err)
	}
	if len(active.Turns) != 2 || active.Turns["turn-queued"].ID == "" || active.Turns["turn-running"].ID == "" || active.Turns["turn-completed"].ID != "" || active.Turns["turn-other"].ID != "" {
		t.Fatalf("active queue snapshot = %#v", active.Turns)
	}
	if len(active.InboundEvents) != 2 || active.InboundEvents["inbound-queued"].ID == "" || active.InboundEvents["inbound-running"].ID == "" {
		t.Fatalf("active queue inbound snapshot = %#v", active.InboundEvents)
	}
	fullSession, err := store.SessionTurnQueueSnapshot(ctx, "s1")
	if err != nil {
		t.Fatalf("SessionTurnQueueSnapshot sqlite error: %v", err)
	}
	if len(fullSession.Turns) != 3 || fullSession.Turns["turn-other"].ID != "" || fullSession.InboundEvents["inbound-completed"].ID == "" {
		t.Fatalf("full session queue snapshot = turns %#v inbound %#v", fullSession.Turns, fullSession.InboundEvents)
	}
	workflow, err := store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("WorkflowNotificationStateSnapshot sqlite error: %v", err)
	}
	if workflow.ControlChat.TeamsChatID != "control-chat" || !workflow.Workflow.Enabled {
		t.Fatalf("workflow notification snapshot = %#v", workflow)
	}
	outbox, err := store.OutboxStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("OutboxStateSnapshot sqlite error: %v", err)
	}
	if len(outbox.OutboxMessages) != 1 || outbox.OutboxMessages["outbox-1"].ID == "" || len(outbox.Turns) != 0 {
		t.Fatalf("outbox snapshot = %#v", outbox)
	}
	if poll, ok, err := store.ChatPoll(ctx, "chat-1"); err != nil || !ok || poll.PollState != "warm" {
		t.Fatalf("ChatPoll sqlite = %#v ok=%v err=%v", poll, ok, err)
	}
	if limit, ok, err := store.ChatRateLimit(ctx, "chat-1"); err != nil || !ok || limit.Reason != "429" {
		t.Fatalf("ChatRateLimit sqlite = %#v ok=%v err=%v", limit, ok, err)
	}
}

func TestSQLiteConcurrentDuplicateInboundAndTurnCreation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)
	session := testSession()
	session.ID = "sqlite-concurrent-session"
	session.TeamsChatID = "sqlite-concurrent-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession sqlite concurrent created=%v err=%v", created, err)
	}
	inbound := testInbound()
	inbound.ID = "sqlite-concurrent-inbound"
	inbound.SessionID = session.ID
	inbound.TeamsChatID = session.TeamsChatID
	inbound.TeamsMessageID = "sqlite-concurrent-message"
	inbound.Text = "same message"

	var inboundCreated int32
	var turnCreated int32
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker, err := Open(store.Path())
			if err != nil {
				errs <- err
				return
			}
			if _, created, err := worker.PersistInbound(ctx, inbound); err != nil {
				errs <- err
				return
			} else if created {
				atomic.AddInt32(&inboundCreated, 1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent PersistInbound sqlite error: %v", err)
	}
	if got := atomic.LoadInt32(&inboundCreated); got != 1 {
		t.Fatalf("concurrent PersistInbound created = %d, want 1", got)
	}

	errs = make(chan error, 32)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker, err := Open(store.Path())
			if err != nil {
				errs <- err
				return
			}
			if _, created, err := worker.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID}); err != nil {
				errs <- err
				return
			} else if created {
				atomic.AddInt32(&turnCreated, 1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent QueueTurn sqlite error: %v", err)
	}
	if got := atomic.LoadInt32(&turnCreated); got != 1 {
		t.Fatalf("concurrent QueueTurn created = %d, want 1", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load concurrent sqlite state error: %v", err)
	}
	if len(state.InboundEvents) != 1 || state.InboundEvents[inbound.ID].ID == "" {
		t.Fatalf("concurrent inbound state = %#v", state.InboundEvents)
	}
	var turnsForInbound int
	for _, turn := range state.Turns {
		if turn.InboundEventID == inbound.ID {
			turnsForInbound++
		}
	}
	if turnsForInbound != 1 {
		t.Fatalf("turns for inbound = %d, want 1: %#v", turnsForInbound, state.Turns)
	}
}

func TestSQLiteConcurrentClaimNextQueuedTurnAllowsOneRunner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)
	session := testSession()
	session.ID = "sqlite-claim-session"
	session.TeamsChatID = "sqlite-claim-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession sqlite claim created=%v err=%v", created, err)
	}
	for i := 0; i < 8; i++ {
		inbound := testInbound()
		inbound.ID = fmt.Sprintf("sqlite-claim-inbound-%02d", i)
		inbound.SessionID = session.ID
		inbound.TeamsChatID = session.TeamsChatID
		inbound.TeamsMessageID = fmt.Sprintf("sqlite-claim-message-%02d", i)
		if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
			t.Fatalf("PersistInbound sqlite claim %d created=%v err=%v", i, created, err)
		}
		if _, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID}); err != nil || !created {
			t.Fatalf("QueueTurn sqlite claim %d created=%v err=%v", i, created, err)
		}
	}

	var claimed int32
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker, err := Open(store.Path())
			if err != nil {
				errs <- err
				return
			}
			defer worker.Close()
			turn, ok, err := worker.ClaimNextQueuedTurn(ctx, session.ID)
			if err != nil {
				errs <- err
				return
			}
			if ok {
				if turn.Status != TurnStatusRunning {
					errs <- fmt.Errorf("claimed turn status = %q, want running", turn.Status)
					return
				}
				atomic.AddInt32(&claimed, 1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent ClaimNextQueuedTurn sqlite error: %v", err)
	}
	if got := atomic.LoadInt32(&claimed); got != 1 {
		t.Fatalf("concurrent ClaimNextQueuedTurn claims = %d, want 1", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load concurrent claim state: %v", err)
	}
	var running, queued int
	for _, turn := range state.Turns {
		if turn.SessionID != session.ID {
			continue
		}
		switch turn.Status {
		case TurnStatusRunning:
			running++
		case TurnStatusQueued:
			queued++
		}
	}
	if running != 1 || queued != 7 {
		t.Fatalf("turn statuses after concurrent claim: running=%d queued=%d turns=%#v", running, queued, state.Turns)
	}
}

func TestSQLiteConcurrentQueueOutboxSequencesAreUnique(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)
	session := testSession()
	session.ID = "sqlite-outbox-sequence-session"
	session.TeamsChatID = "sqlite-outbox-sequence-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession sqlite outbox sequence created=%v err=%v", created, err)
	}

	const workers = 24
	var created int32
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker, err := Open(store.Path())
			if err != nil {
				errs <- err
				return
			}
			defer worker.Close()
			_, ok, err := worker.QueueOutbox(ctx, OutboxMessage{
				ID:          fmt.Sprintf("sqlite-concurrent-outbox-%02d", i),
				SessionID:   session.ID,
				TeamsChatID: session.TeamsChatID,
				Kind:        "status",
				Body:        fmt.Sprintf("concurrent outbox %02d", i),
			})
			if err != nil {
				errs <- err
				return
			}
			if ok {
				atomic.AddInt32(&created, 1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent QueueOutbox sqlite error: %v", err)
	}
	if got := atomic.LoadInt32(&created); got != workers {
		t.Fatalf("concurrent QueueOutbox created = %d, want %d", got, workers)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load concurrent outbox sequence state: %v", err)
	}
	seen := map[int64]bool{}
	for i := 0; i < workers; i++ {
		msg := state.OutboxMessages[fmt.Sprintf("sqlite-concurrent-outbox-%02d", i)]
		if msg.ID == "" {
			t.Fatalf("missing concurrent outbox %02d", i)
		}
		if msg.Sequence <= 0 {
			t.Fatalf("outbox %s sequence = %d, want positive", msg.ID, msg.Sequence)
		}
		if seen[msg.Sequence] {
			t.Fatalf("duplicate outbox sequence %d in %#v", msg.Sequence, state.OutboxMessages)
		}
		seen[msg.Sequence] = true
	}
}

func TestLoadMigratesV1StateToV2SemanticBackbone(t *testing.T) {
	store := newTestStore(t)
	data := []byte(`{
		"schema_version": 1,
		"service_owner": {
			"pid": 4242,
			"hostname": "legacy-host",
			"executable_path": "/usr/local/bin/codex-helper",
			"helper_version": "v0.0.legacy",
			"active_session_id": "s1",
			"active_turn_id": "turn:legacy"
		},
		"sessions": {
			"s1": {
				"id": "s1",
				"status": "active",
				"teams_chat_id": "chat-1",
				"teams_chat_url": "https://teams.example/chat-1",
				"teams_topic": "topic",
				"runner_kind": "exec",
				"codex_version": "1.2.3",
				"cwd": "/workspace/project",
				"codex_home": "/home/user/.codex",
				"profile": "default",
				"model": "gpt-test",
				"sandbox": "workspace-write",
				"proxy_mode": "on",
				"yolo_mode": "off",
				"codex_thread_id": "thread-0"
			}
		},
		"turns": {
			"turn:legacy": {
				"id": "turn:legacy",
				"session_id": "s1",
				"status": "completed",
				"codex_thread_id": "thread-0"
			}
		},
		"inbound_events": {
			"inbound:chat-1:message-1": {
				"id": "inbound:chat-1:message-1",
				"session_id": "s1",
				"teams_chat_id": "chat-1",
				"teams_message_id": "message-1",
				"source": "teams",
				"status": "queued",
				"turn_id": "turn:legacy"
			}
		},
		"outbox_messages": {
			"outbox:legacy": {
				"id": "outbox:legacy",
				"session_id": "s1",
				"teams_chat_id": "chat-1",
				"kind": "final",
				"body": "legacy body",
				"status": "queued"
			},
			"outbox:accepted": {
				"id": "outbox:accepted",
				"session_id": "s1",
				"teams_chat_id": "chat-1",
				"kind": "final",
				"body": "accepted body",
				"status": "accepted",
				"teams_message_id": "teams-accepted"
			}
		},
		"chat_polls": {
			"chat-1": {
				"chat_id": "chat-1",
				"seeded": true
			}
		}
	}`)
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatalf("write old state: %v", err)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load migrated state error: %v", err)
	}
	if state.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", state.SchemaVersion, SchemaVersion)
	}
	if state.Sessions["s1"].TeamsChatID != "chat-1" {
		t.Fatalf("session missing after migration: %#v", state.Sessions["s1"])
	}
	msg := state.OutboxMessages["outbox:legacy"]
	if msg.Sequence <= 0 || msg.PartIndex != 1 || msg.PartCount != 1 || msg.RenderedHash == "" {
		t.Fatalf("legacy outbox metadata not initialized: %#v", msg)
	}
	accepted := state.OutboxMessages["outbox:accepted"]
	if accepted.Sequence <= 0 || accepted.Sequence == msg.Sequence || accepted.PartIndex != 1 || accepted.PartCount != 1 || accepted.RenderedHash == "" || accepted.TeamsMessageID != "teams-accepted" {
		t.Fatalf("accepted legacy outbox metadata not initialized: %#v", accepted)
	}
	if state.ChatSequences["chat-1"].Next != 3 {
		t.Fatalf("chat sequence next = %d, want 3", state.ChatSequences["chat-1"].Next)
	}
	if state.Workspaces == nil || state.DashboardViews == nil || state.DashboardNumbers == nil || state.TranscriptLedger == nil || state.TranscriptDeliveries == nil || state.HelperDeliveries == nil || state.ImportCheckpoints == nil || state.HistoryWatch == nil || state.ChatRateLimits == nil || state.ArtifactRecords == nil || state.Notifications == nil {
		t.Fatalf("semantic v2 maps were not initialized: %#v", state)
	}
	if state.ServiceOwner == nil || state.ServiceOwner.ActiveTurnID != "turn:legacy" {
		t.Fatalf("legacy owner not preserved: %#v", state.ServiceOwner)
	}
}

func TestLoadMigratesLocalPOCV1StateShape(t *testing.T) {
	store := newTestStore(t)
	data := []byte(`{
		"schema_version": 1,
		"created_at": "2026-04-30T13:48:19+08:00",
		"updated_at": "2026-04-30T14:06:44+08:00",
		"service_control": {
			"updated_at": "2026-04-30T14:06:44+08:00"
		},
		"sessions": {
			"s001": {
				"id": "s001",
				"status": "active",
				"teams_chat_id": "19:work-chat@thread.v2",
				"teams_chat_url": "https://teams.example/l/chat/19%3Awork-chat%40thread.v2/0",
				"teams_topic": "codex poc test",
				"codex_thread_id": "thread-poc",
				"latest_turn_id": "turn:inbound:19:work-chat@thread.v2:2",
				"runner_kind": "executor",
				"created_at": "2026-04-30T10:56:00+08:00",
				"updated_at": "2026-04-30T13:49:12+08:00"
			}
		},
		"turns": {
			"turn:inbound:19:work-chat@thread.v2:1": {
				"id": "turn:inbound:19:work-chat@thread.v2:1",
				"session_id": "s001",
				"inbound_event_id": "inbound:19:work-chat@thread.v2:1",
				"status": "completed",
				"codex_thread_id": "thread-poc",
				"queued_at": "2026-04-30T13:48:19+08:00",
				"started_at": "2026-04-30T13:48:19+08:00",
				"completed_at": "2026-04-30T13:48:19+08:00"
			},
			"turn:inbound:19:work-chat@thread.v2:2": {
				"id": "turn:inbound:19:work-chat@thread.v2:2",
				"session_id": "s001",
				"inbound_event_id": "inbound:19:work-chat@thread.v2:2",
				"status": "completed",
				"codex_thread_id": "thread-poc",
				"queued_at": "2026-04-30T13:48:47+08:00",
				"started_at": "2026-04-30T13:48:47+08:00",
				"completed_at": "2026-04-30T13:48:47+08:00"
			}
		},
		"inbound_events": {
			"inbound:19:work-chat@thread.v2:1": {
				"id": "inbound:19:work-chat@thread.v2:1",
				"session_id": "s001",
				"teams_chat_id": "19:work-chat@thread.v2",
				"teams_message_id": "1",
				"source": "teams",
				"status": "queued",
				"turn_id": "turn:inbound:19:work-chat@thread.v2:1"
			},
			"inbound:19:work-chat@thread.v2:2": {
				"id": "inbound:19:work-chat@thread.v2:2",
				"session_id": "s001",
				"teams_chat_id": "19:work-chat@thread.v2",
				"teams_message_id": "2",
				"source": "teams",
				"status": "queued",
				"turn_id": "turn:inbound:19:work-chat@thread.v2:2"
			}
		}
	}`)
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatalf("write local poc state: %v", err)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load local poc state error: %v", err)
	}
	if state.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", state.SchemaVersion, SchemaVersion)
	}
	session := state.Sessions["s001"]
	if session.TeamsChatID != "19:work-chat@thread.v2" || session.LatestTurnID != "turn:inbound:19:work-chat@thread.v2:2" {
		t.Fatalf("local poc session not preserved: %#v", session)
	}
	if got := state.Turns["turn:inbound:19:work-chat@thread.v2:2"].Status; got != TurnStatusCompleted {
		t.Fatalf("latest turn status = %q, want completed", got)
	}
	if state.Workspaces == nil || state.DashboardViews == nil || state.DashboardNumbers == nil || state.ChatSequences == nil || state.TranscriptDeliveries == nil || state.HelperDeliveries == nil || state.HistoryWatch == nil || state.ChatRateLimits == nil || state.Notifications == nil {
		t.Fatalf("semantic maps were not initialized for local poc shape: %#v", state)
	}
	if state.ServiceControl.UpdatedAt.IsZero() {
		t.Fatalf("service control timestamp not preserved: %#v", state.ServiceControl)
	}
}

func TestLoadUnsupportedFutureSchemaFailsClosed(t *testing.T) {
	store := newTestStore(t)
	data := []byte(`{"schema_version":999,"sessions":{"s1":{"id":"s1","teams_chat_id":"chat-1"}}}`)
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatalf("write future state: %v", err)
	}

	if _, err := store.Load(context.Background()); !errors.Is(err, ErrUnsupportedSchemaVersion) || !strings.Contains(err.Error(), "999") {
		t.Fatalf("Load future schema error = %v, want unsupported schema version 999", err)
	}
	if _, err := store.SetPaused(context.Background(), true, "should not write"); !errors.Is(err, ErrUnsupportedSchemaVersion) {
		t.Fatalf("SetPaused future schema error = %v, want unsupported schema", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read future state after failed update: %v", err)
	}
	if string(after) != string(data) {
		t.Fatalf("future state was modified after failed update:\n%s", string(after))
	}
}

func TestUnsupportedStorageBackendFailsClosed(t *testing.T) {
	store := newTestStore(t)
	data := []byte(fmt.Sprintf(`{"schema_version":%d,"storage_backend":"store-v9","path":"state.v9"}`, SchemaVersion))
	writeRawStoreStateForTest(t, store, data)

	if _, err := store.Load(context.Background()); err == nil || !strings.Contains(err.Error(), `unsupported teams store backend "store-v9"`) {
		t.Fatalf("Load unsupported backend error = %v, want unsupported backend", err)
	}
	if _, err := store.SetPaused(context.Background(), true, "should not write"); err == nil || !strings.Contains(err.Error(), `unsupported teams store backend "store-v9"`) {
		t.Fatalf("SetPaused unsupported backend error = %v, want unsupported backend", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read unsupported backend state after failed update: %v", err)
	}
	if !bytes.Equal(after, data) {
		t.Fatalf("unsupported backend state was modified after failed update:\n%s", string(after))
	}
}

func TestAcceptedOutboxIsPromotedWithoutResend(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:accepted",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "already accepted",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	if _, err := store.MarkOutboxAccepted(ctx, msg.ID, "teams-message-1"); err != nil {
		t.Fatalf("MarkOutboxAccepted error: %v", err)
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 1 || pending[0].Status != OutboxStatusAccepted || pending[0].TeamsMessageID != "teams-message-1" {
		t.Fatalf("pending accepted outbox = %#v, want one accepted message", pending)
	}
	if _, err := store.MarkOutboxSent(ctx, msg.ID, "teams-message-1"); err != nil {
		t.Fatalf("MarkOutboxSent error: %v", err)
	}
	pending, err = store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox after sent error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after sent = %#v, want none", pending)
	}
}

func TestOutboxStatusUpdateHonorsSessionLock(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:single-load",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "single load",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	release := holdSessionLockForTest(t, store, "s1")
	defer release()

	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := store.MarkOutboxSendAttempt(timeoutCtx, msg.ID); err == nil {
		t.Fatal("MarkOutboxSendAttempt completed while session lock was held")
	} else if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "was not acquired") {
		t.Fatalf("MarkOutboxSendAttempt error = %v, want session lock wait failure", err)
	}
}

func TestSQLiteOutboxStatusUpdatesHonorSessionLock(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}
	for _, id := range []string{"outbox:attempt", "outbox:accepted", "outbox:sent"} {
		if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
			ID:          id,
			SessionID:   "s1",
			TeamsChatID: "chat-1",
			Kind:        "helper",
			Body:        id,
		}); err != nil {
			t.Fatalf("QueueOutbox %s error: %v", id, err)
		}
	}
	migrateStoreToSQLiteForTest(t, store)
	release := holdSessionLockForTest(t, store, "s1")
	defer release()

	assertSessionLockWaitFailure := func(name string, fn func(context.Context) error) {
		t.Helper()
		timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if err := fn(timeoutCtx); err == nil {
			t.Fatalf("%s completed while session lock was held", name)
		} else if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "was not acquired") {
			t.Fatalf("%s error = %v, want session lock wait failure", name, err)
		}
	}
	assertSessionLockWaitFailure("MarkOutboxSendAttempt", func(ctx context.Context) error {
		_, err := store.MarkOutboxSendAttempt(ctx, "outbox:attempt")
		return err
	})
	assertSessionLockWaitFailure("MarkOutboxAccepted", func(ctx context.Context) error {
		_, err := store.MarkOutboxAccepted(ctx, "outbox:accepted", "teams-accepted")
		return err
	})
	assertSessionLockWaitFailure("MarkOutboxSent", func(ctx context.Context) error {
		_, err := store.MarkOutboxSent(ctx, "outbox:sent", "teams-sent")
		return err
	})
}

func TestTurnStatusUpdateHonorsSessionLockAcrossStoreInstances(t *testing.T) {
	store := newTestStore(t)
	other, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open second store error: %v", err)
	}
	ctx := context.Background()
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}
	inbound, _, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	release := holdSessionLockForTest(t, store, "s1")
	defer release()

	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := other.MarkTurnRunning(timeoutCtx, turn.ID, "thread-1", "codex-turn-1"); err == nil {
		t.Fatal("MarkTurnRunning completed while session lock was held by another store instance")
	} else if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "was not acquired") {
		t.Fatalf("MarkTurnRunning error = %v, want session lock wait failure", err)
	}
}

func TestSQLiteTurnStatusUpdatesHonorSessionLockAcrossStoreInstances(t *testing.T) {
	store := newTestStore(t)
	other, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open second store error: %v", err)
	}
	ctx := context.Background()
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}
	makeTurn := func(id string) string {
		t.Helper()
		inbound, _, err := store.PersistInbound(ctx, InboundEvent{
			ID:             "inbound:" + id,
			SessionID:      "s1",
			TeamsChatID:    "chat-1",
			TeamsMessageID: "message-" + id,
			Text:           id,
			Status:         InboundStatusPersisted,
		})
		if err != nil {
			t.Fatalf("PersistInbound %s error: %v", id, err)
		}
		turn, _, err := store.QueueTurn(ctx, Turn{ID: "turn:" + id, SessionID: "s1", InboundEventID: inbound.ID})
		if err != nil {
			t.Fatalf("QueueTurn %s error: %v", id, err)
		}
		return turn.ID
	}
	runningID := makeTurn("running")
	completedID := makeTurn("completed")
	failedID := makeTurn("failed")
	interruptedID := makeTurn("interrupted")
	migrateStoreToSQLiteForTest(t, store)
	release := holdSessionLockForTest(t, store, "s1")
	defer release()

	assertSessionLockWaitFailure := func(name string, fn func(context.Context) error) {
		t.Helper()
		timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if err := fn(timeoutCtx); err == nil {
			t.Fatalf("%s completed while session lock was held", name)
		} else if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "was not acquired") {
			t.Fatalf("%s error = %v, want session lock wait failure", name, err)
		}
	}
	assertSessionLockWaitFailure("MarkTurnRunning", func(ctx context.Context) error {
		_, err := other.MarkTurnRunning(ctx, runningID, "thread-1", "codex-turn-1")
		return err
	})
	assertSessionLockWaitFailure("MarkTurnCompleted", func(ctx context.Context) error {
		_, err := other.MarkTurnCompleted(ctx, completedID, "thread-1", "codex-turn-2")
		return err
	})
	assertSessionLockWaitFailure("MarkTurnFailed", func(ctx context.Context) error {
		_, err := other.MarkTurnFailed(ctx, failedID, "failed")
		return err
	})
	assertSessionLockWaitFailure("MarkTurnInterrupted", func(ctx context.Context) error {
		_, err := other.MarkTurnInterrupted(ctx, interruptedID, "interrupted")
		return err
	})
}

func TestPendingOutboxStatusMatrix(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 2, 9, 30, 0, 0, time.UTC)
	messages := []OutboxMessage{
		{ID: "queued", TeamsChatID: "chat-1", Status: OutboxStatusQueued},
		{ID: "accepted-with-teams-id", TeamsChatID: "chat-1", Status: OutboxStatusAccepted, TeamsMessageID: "teams-accepted"},
		{ID: "accepted-without-teams-id", TeamsChatID: "chat-1", Status: OutboxStatusAccepted},
		{ID: "fresh-sending", TeamsChatID: "chat-1", Status: OutboxStatusSending, LastSendAttempt: now.Add(-outboxSendLease + time.Second)},
		{ID: "stale-sending", TeamsChatID: "chat-1", Status: OutboxStatusSending, LastSendAttempt: now.Add(-outboxSendLease - time.Second)},
		{ID: "sent", TeamsChatID: "chat-1", Status: OutboxStatusSent, TeamsMessageID: "teams-sent"},
		{ID: "skipped", TeamsChatID: "chat-1", Status: OutboxStatusSkipped},
	}
	for _, msg := range messages {
		msg.Kind = "helper"
		msg.Body = msg.ID
		if _, _, err := store.QueueOutbox(ctx, msg); err != nil {
			t.Fatalf("QueueOutbox %s error: %v", msg.ID, err)
		}
	}
	pending, err := store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("PendingOutboxAt error: %v", err)
	}
	got := make(map[string]bool)
	for _, msg := range pending {
		got[msg.ID] = true
	}
	want := map[string]bool{
		"queued":                 true,
		"accepted-with-teams-id": true,
		"stale-sending":          true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pending status matrix = %#v, want %#v; pending=%#v", got, want, pending)
	}
}

func TestMarkTurnCompletedWithEmptyCodexTurnIDPreservesLatestKnownCodexTurnID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}

	firstInbound, _, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound first error: %v", err)
	}
	firstTurn, _, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: firstInbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn first error: %v", err)
	}
	if _, err := store.MarkTurnCompleted(ctx, firstTurn.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnCompleted first error: %v", err)
	}

	secondInbound := testInbound()
	secondInbound.ID = "inbound-2"
	secondInbound.TeamsMessageID = "message-2"
	secondInbound, _, err = store.PersistInbound(ctx, secondInbound)
	if err != nil {
		t.Fatalf("PersistInbound second error: %v", err)
	}
	secondTurn, _, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: secondInbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn second error: %v", err)
	}
	completed, err := store.MarkTurnCompleted(ctx, secondTurn.ID, "thread-1", "")
	if err != nil {
		t.Fatalf("MarkTurnCompleted second error: %v", err)
	}
	if completed.CodexTurnID != "" {
		t.Fatalf("second turn CodexTurnID = %q, want empty", completed.CodexTurnID)
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	session := state.Sessions["s1"]
	if session.LatestTurnID != secondTurn.ID {
		t.Fatalf("LatestTurnID = %q, want %q", session.LatestTurnID, secondTurn.ID)
	}
	if session.CodexThreadID != "thread-1" {
		t.Fatalf("CodexThreadID = %q, want thread-1", session.CodexThreadID)
	}
	if session.LatestCodexTurnID != "codex-turn-1" {
		t.Fatalf("LatestCodexTurnID = %q, want latest known codex-turn-1", session.LatestCodexTurnID)
	}
}

func TestQueueOutboxAssignsPerChatSequenceAndMetadata(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	first, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:first",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final-001",
		Body:        "first",
		PartIndex:   1,
		PartCount:   2,
	})
	if err != nil {
		t.Fatalf("QueueOutbox first error: %v", err)
	}
	if !created {
		t.Fatal("first QueueOutbox created = false")
	}
	second, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:second",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final-002",
		Body:        "second",
		PartIndex:   2,
		PartCount:   2,
	})
	if err != nil {
		t.Fatalf("QueueOutbox second error: %v", err)
	}
	other, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:other",
		TeamsChatID: "chat-2",
		Kind:        "control",
		Body:        "other",
	})
	if err != nil {
		t.Fatalf("QueueOutbox other chat error: %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || other.Sequence != 1 {
		t.Fatalf("unexpected sequences: first=%d second=%d other=%d", first.Sequence, second.Sequence, other.Sequence)
	}
	if first.RenderedHash == "" || first.PartIndex != 1 || first.PartCount != 2 {
		t.Fatalf("first outbox metadata mismatch: %#v", first)
	}
	if other.PartIndex != 1 || other.PartCount != 1 {
		t.Fatalf("default part metadata mismatch: %#v", other)
	}
}

func TestQueueOutboxRecordsHelperDeliveryLedger(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:status",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "codex-progress-003",
		Body:        "live status",
	})
	if err != nil {
		t.Fatalf("QueueOutbox status error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after queue error: %v", err)
	}
	if len(state.HelperDeliveries) != 1 {
		t.Fatalf("helper deliveries after queue = %#v, want one", state.HelperDeliveries)
	}
	var delivery HelperDeliveryRecord
	for _, record := range state.HelperDeliveries {
		delivery = record
	}
	if delivery.Status != HelperDeliveryStatusQueued || delivery.KindFamily != "status" || delivery.VisibleHash != bodyHash("live status") || delivery.CodexThreadID != "thread-0" {
		t.Fatalf("queued helper delivery mismatch: %#v", delivery)
	}
	if _, err := store.MarkOutboxSent(ctx, msg.ID, "teams-status"); err != nil {
		t.Fatalf("MarkOutboxSent status error: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after sent error: %v", err)
	}
	if len(state.HelperDeliveries) != 1 {
		t.Fatalf("helper deliveries after sent = %#v, want one stable record", state.HelperDeliveries)
	}
	for _, record := range state.HelperDeliveries {
		delivery = record
	}
	if delivery.Status != HelperDeliveryStatusSent || delivery.TeamsMessageID != "teams-status" || delivery.SentAt.IsZero() {
		t.Fatalf("sent helper delivery mismatch: %#v", delivery)
	}
}

func TestOutboxArtifactRecordsFollowOutboxSendState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, err := store.UpsertArtifactRecord(ctx, ArtifactRecord{
		ID:         "artifact:one",
		SessionID:  "s1",
		TurnID:     "turn-1",
		Path:       "artifact.txt",
		UploadName: "codex-artifact.txt",
		Status:     "queued",
	}); err != nil {
		t.Fatalf("UpsertArtifactRecord error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:                   "outbox:artifact",
		SessionID:            "s1",
		TurnID:               "turn-1",
		TeamsChatID:          "chat-1",
		Kind:                 "artifact",
		Body:                 "artifact attached",
		AttachmentName:       "artifact.txt",
		AttachmentUploadName: "codex-artifact.txt",
		ArtifactIDs:          []string{"artifact:one"},
	})
	if err != nil {
		t.Fatalf("QueueOutbox artifact error: %v", err)
	}
	if _, err := store.MarkOutboxDriveItem(ctx, msg.ID, "drive-item-1", "codex-artifact.txt", "", "", ""); err != nil {
		t.Fatalf("MarkOutboxDriveItem error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after drive item error: %v", err)
	}
	artifact := state.ArtifactRecords["artifact:one"]
	if artifact.Status != "drive_uploaded" || artifact.OutboxID != msg.ID || artifact.DriveItemID != "drive-item-1" || artifact.UploadedAt.IsZero() {
		t.Fatalf("artifact after drive upload mismatch: %#v", artifact)
	}
	if _, err := store.MarkOutboxSent(ctx, msg.ID, "teams-artifact"); err != nil {
		t.Fatalf("MarkOutboxSent artifact error: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after sent error: %v", err)
	}
	artifact = state.ArtifactRecords["artifact:one"]
	if artifact.Status != "uploaded" || artifact.TeamsMessageID != "teams-artifact" || artifact.Error != "" || artifact.SentAt.IsZero() {
		t.Fatalf("artifact after sent mismatch: %#v", artifact)
	}
}

func TestStoreConcurrentSessionAndOutboxWritersPreserveState(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), storeConcurrentTestTimeout(30*time.Second))
	defer cancel()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}

	const workers = 6
	const perWorker = 12
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerStore, err := Open(store.Path())
			if err != nil {
				errCh <- fmt.Errorf("open worker store %d: %w", worker, err)
				return
			}
			for i := 0; i < perWorker; i++ {
				idx := worker*perWorker + i
				inbound, created, err := workerStore.PersistInbound(ctx, InboundEvent{
					ID:             fmt.Sprintf("inbound:stress:%03d", idx),
					SessionID:      "s1",
					TeamsChatID:    "chat-1",
					TeamsMessageID: fmt.Sprintf("teams-message-%03d", idx),
					Source:         "teams",
				})
				if err != nil {
					errCh <- fmt.Errorf("persist inbound %d: %w", idx, err)
					return
				}
				if !created {
					errCh <- fmt.Errorf("inbound %d unexpectedly deduplicated", idx)
					return
				}
				turn, created, err := workerStore.QueueTurn(ctx, Turn{
					SessionID:      "s1",
					InboundEventID: inbound.ID,
				})
				if err != nil {
					errCh <- fmt.Errorf("queue turn %d: %w", idx, err)
					return
				}
				if !created {
					errCh <- fmt.Errorf("turn %d unexpectedly deduplicated", idx)
					return
				}
				if _, err := workerStore.MarkTurnRunning(ctx, turn.ID, "thread-stress", fmt.Sprintf("codex-turn-%03d", idx)); err != nil {
					errCh <- fmt.Errorf("mark turn running %d: %w", idx, err)
					return
				}
				if idx%5 == 0 {
					if _, err := workerStore.MarkTurnFailed(ctx, turn.ID, "synthetic failure"); err != nil {
						errCh <- fmt.Errorf("mark turn failed %d: %w", idx, err)
						return
					}
				} else if _, err := workerStore.MarkTurnCompleted(ctx, turn.ID, "thread-stress", fmt.Sprintf("codex-turn-%03d", idx)); err != nil {
					errCh <- fmt.Errorf("mark turn completed %d: %w", idx, err)
					return
				}

				chatID := fmt.Sprintf("chat-%d", 1+(idx%3))
				outbox, created, err := workerStore.QueueOutbox(ctx, OutboxMessage{
					ID:          fmt.Sprintf("outbox:stress:%03d", idx),
					SessionID:   "s1",
					TurnID:      turn.ID,
					TeamsChatID: chatID,
					Kind:        "stress",
					Body:        fmt.Sprintf("stress body %03d", idx),
				})
				if err != nil {
					errCh <- fmt.Errorf("queue outbox %d: %w", idx, err)
					return
				}
				if !created {
					errCh <- fmt.Errorf("outbox %d unexpectedly deduplicated", idx)
					return
				}
				switch idx % 4 {
				case 0:
					if _, err := workerStore.MarkOutboxSendAttempt(ctx, outbox.ID); err != nil {
						errCh <- fmt.Errorf("mark outbox send attempt %d: %w", idx, err)
						return
					}
					if _, err := workerStore.MarkOutboxAccepted(ctx, outbox.ID, fmt.Sprintf("teams-sent-%03d", idx)); err != nil {
						errCh <- fmt.Errorf("mark outbox accepted %d: %w", idx, err)
						return
					}
					if _, err := workerStore.MarkOutboxSent(ctx, outbox.ID, fmt.Sprintf("teams-sent-%03d", idx)); err != nil {
						errCh <- fmt.Errorf("mark outbox sent %d: %w", idx, err)
						return
					}
				case 1:
					if _, err := workerStore.MarkOutboxSendAttempt(ctx, outbox.ID); err != nil {
						errCh <- fmt.Errorf("mark outbox failed attempt %d: %w", idx, err)
						return
					}
					if _, err := workerStore.MarkOutboxSendError(ctx, outbox.ID, "synthetic 429"); err != nil {
						errCh <- fmt.Errorf("mark outbox send error %d: %w", idx, err)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	want := workers * perWorker
	if got := len(state.InboundEvents); got != want {
		t.Fatalf("inbound count = %d, want %d", got, want)
	}
	if got := len(state.Turns); got != want {
		t.Fatalf("turn count = %d, want %d", got, want)
	}
	if got := len(state.OutboxMessages); got != want {
		t.Fatalf("outbox count = %d, want %d", got, want)
	}
	sequencesByChat := map[string]map[int64]string{}
	countByChat := map[string]int{}
	for _, msg := range state.OutboxMessages {
		if msg.Sequence <= 0 {
			t.Fatalf("outbox %s has non-positive sequence: %#v", msg.ID, msg)
		}
		if sequencesByChat[msg.TeamsChatID] == nil {
			sequencesByChat[msg.TeamsChatID] = map[int64]string{}
		}
		if previous := sequencesByChat[msg.TeamsChatID][msg.Sequence]; previous != "" {
			t.Fatalf("duplicate sequence %d for chat %s: %s and %s", msg.Sequence, msg.TeamsChatID, previous, msg.ID)
		}
		sequencesByChat[msg.TeamsChatID][msg.Sequence] = msg.ID
		countByChat[msg.TeamsChatID]++
	}
	for chatID, count := range countByChat {
		for seq := int64(1); seq <= int64(count); seq++ {
			if sequencesByChat[chatID][seq] == "" {
				t.Fatalf("chat %s missing sequence %d in %#v", chatID, seq, sequencesByChat[chatID])
			}
		}
		if next := state.ChatSequences[chatID].Next; next != int64(count+1) {
			t.Fatalf("chat %s next sequence = %d, want %d", chatID, next, count+1)
		}
	}
}

func TestStoreConcurrentDistinctSessionWritersDoNotCrossMutate(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), storeConcurrentTestTimeout(30*time.Second))
	defer cancel()

	const sessions = 5
	const perSession = 16
	for i := 1; i <= sessions; i++ {
		sessionID := fmt.Sprintf("s%03d", i)
		if _, _, err := store.CreateSession(ctx, SessionContext{
			ID:          sessionID,
			Status:      SessionStatusActive,
			TeamsChatID: fmt.Sprintf("chat-%d", i),
		}); err != nil {
			t.Fatalf("CreateSession %s error: %v", sessionID, err)
		}
	}

	errCh := make(chan error, sessions)
	var wg sync.WaitGroup
	for i := 1; i <= sessions; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			workerStore, err := Open(store.Path())
			if err != nil {
				errCh <- fmt.Errorf("open worker store %d: %w", i, err)
				return
			}
			sessionID := fmt.Sprintf("s%03d", i)
			chatID := fmt.Sprintf("chat-%d", i)
			for j := 0; j < perSession; j++ {
				inboundID := fmt.Sprintf("inbound:%s:%02d", sessionID, j)
				inbound, created, err := workerStore.PersistInbound(ctx, InboundEvent{
					ID:             inboundID,
					SessionID:      sessionID,
					TeamsChatID:    chatID,
					TeamsMessageID: fmt.Sprintf("teams-%s-%02d", sessionID, j),
					Text:           fmt.Sprintf("prompt %s %02d", sessionID, j),
					Source:         "teams",
				})
				if err != nil {
					errCh <- fmt.Errorf("persist inbound %s: %w", inboundID, err)
					return
				}
				if !created {
					errCh <- fmt.Errorf("inbound %s unexpectedly deduplicated", inboundID)
					return
				}
				turn, created, err := workerStore.QueueTurn(ctx, Turn{
					SessionID:      sessionID,
					InboundEventID: inbound.ID,
				})
				if err != nil {
					errCh <- fmt.Errorf("queue turn %s: %w", inboundID, err)
					return
				}
				if !created {
					errCh <- fmt.Errorf("turn for %s unexpectedly deduplicated", inboundID)
					return
				}
				if _, err := workerStore.MarkTurnCompleted(ctx, turn.ID, "thread-"+sessionID, fmt.Sprintf("codex-%s-%02d", sessionID, j)); err != nil {
					errCh <- fmt.Errorf("complete turn %s: %w", turn.ID, err)
					return
				}
				if _, _, err := workerStore.QueueOutbox(ctx, OutboxMessage{
					ID:          fmt.Sprintf("outbox:%s:%02d", sessionID, j),
					SessionID:   sessionID,
					TurnID:      turn.ID,
					TeamsChatID: chatID,
					Kind:        "final",
					Body:        fmt.Sprintf("answer %s %02d", sessionID, j),
				}); err != nil {
					errCh <- fmt.Errorf("queue outbox %s: %w", turn.ID, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	for i := 1; i <= sessions; i++ {
		sessionID := fmt.Sprintf("s%03d", i)
		chatID := fmt.Sprintf("chat-%d", i)
		session := state.Sessions[sessionID]
		if session.LatestTurnID == "" || !strings.Contains(session.LatestTurnID, sessionID) {
			t.Fatalf("session %s LatestTurnID = %q, want own latest turn", sessionID, session.LatestTurnID)
		}
		var turns, outbox int
		sequences := map[int64]bool{}
		for _, turn := range state.Turns {
			if turn.SessionID == sessionID {
				turns++
			}
		}
		for _, msg := range state.OutboxMessages {
			if msg.SessionID == sessionID {
				outbox++
				if msg.TeamsChatID != chatID {
					t.Fatalf("session %s outbox %s chat = %q, want %q", sessionID, msg.ID, msg.TeamsChatID, chatID)
				}
				if sequences[msg.Sequence] {
					t.Fatalf("session %s duplicate sequence %d", sessionID, msg.Sequence)
				}
				sequences[msg.Sequence] = true
			}
		}
		if turns != perSession || outbox != perSession {
			t.Fatalf("session %s turns/outbox = %d/%d, want %d/%d", sessionID, turns, outbox, perSession, perSession)
		}
		for seq := int64(1); seq <= perSession; seq++ {
			if !sequences[seq] {
				t.Fatalf("session %s missing chat sequence %d in %#v", sessionID, seq, sequences)
			}
		}
	}
}

func TestStoreConcurrentInboundTurnDedupAcrossHandles(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), storeConcurrentTestTimeout(10*time.Second))
	defer cancel()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}

	const workers = 12
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerStore, err := Open(store.Path())
			if err != nil {
				errCh <- fmt.Errorf("open worker store %d: %w", worker, err)
				return
			}
			inbound, _, err := workerStore.PersistInbound(ctx, InboundEvent{
				SessionID:      "s1",
				TeamsChatID:    "chat-1",
				TeamsMessageID: "duplicate-message",
				Source:         "teams",
			})
			if err != nil {
				errCh <- fmt.Errorf("persist duplicate inbound %d: %w", worker, err)
				return
			}
			if _, _, err := workerStore.QueueTurn(ctx, Turn{
				SessionID:      "s1",
				InboundEventID: inbound.ID,
			}); err != nil {
				errCh <- fmt.Errorf("queue duplicate turn %d: %w", worker, err)
				return
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 1 {
		t.Fatalf("inbound count = %d, want one deduped event: %#v", got, state.InboundEvents)
	}
	if got := len(state.Turns); got != 1 {
		t.Fatalf("turn count = %d, want one deduped turn: %#v", got, state.Turns)
	}
	for _, inbound := range state.InboundEvents {
		if inbound.TurnID == "" || inbound.Status != InboundStatusQueued {
			t.Fatalf("deduped inbound not linked to queued turn: %#v", inbound)
		}
	}
}

func storeConcurrentTestTimeout(base time.Duration) time.Duration {
	if runtime.GOOS == "windows" {
		return 90 * time.Second
	}
	return base
}

func storeTestHelperBinaryName() string {
	if runtime.GOOS == "windows" {
		return "codex-proxy.exe"
	}
	return "codex-proxy"
}

func TestPendingOutboxSkipsRateLimitedChatWithoutBlockingOthers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:blocked", SessionID: "s1", TeamsChatID: "chat-1", Kind: "final", Body: "blocked"}); err != nil {
		t.Fatalf("QueueOutbox blocked error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:open", TeamsChatID: "chat-2", Kind: "control", Body: "open"}); err != nil {
		t.Fatalf("QueueOutbox open error: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-1", time.Now().Add(time.Minute), "429 Retry-After"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 1 || pending[0].TeamsChatID != "chat-2" {
		t.Fatalf("pending outbox = %#v, want only unblocked chat-2", pending)
	}
	if err := store.ClearChatRateLimit(ctx, "chat-1"); err != nil {
		t.Fatalf("ClearChatRateLimit error: %v", err)
	}
	pending, err = store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox after clear error: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending after clear = %#v, want both messages", pending)
	}
}

func TestPendingOutboxAtRespectsRateLimitExpiry(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:blocked", TeamsChatID: "chat-1", Kind: "helper", Body: "blocked"}); err != nil {
		t.Fatalf("QueueOutbox blocked error: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-1", now.Add(time.Minute), "429"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	pending, err := store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("PendingOutboxAt before expiry error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending before expiry = %#v, want none", pending)
	}
	pending, err = store.PendingOutboxAt(ctx, now.Add(time.Minute+time.Nanosecond))
	if err != nil {
		t.Fatalf("PendingOutboxAt after expiry error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "outbox:blocked" {
		t.Fatalf("pending after expiry = %#v, want blocked outbox", pending)
	}
}

func TestPendingOutboxCrashRecoveryPromotesAcceptedDespiteRateLimitedChat(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 2, 0, 0, 0, time.UTC)
	accepted, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:accepted-before-crash",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "already accepted by Graph",
	})
	if err != nil {
		t.Fatalf("QueueOutbox accepted error: %v", err)
	}
	if _, err := store.MarkOutboxAccepted(ctx, accepted.ID, "teams-message-1"); err != nil {
		t.Fatalf("MarkOutboxAccepted error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:queued-after-crash",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "must wait for retry-after",
	}); err != nil {
		t.Fatalf("QueueOutbox queued error: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-1", now.Add(time.Hour), "429 after accepted send"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}

	pending, err := store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("PendingOutboxAt error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != accepted.ID || pending[0].TeamsMessageID != "teams-message-1" {
		t.Fatalf("pending during rate limit = %#v, want only accepted message for local promotion", pending)
	}
	if _, err := store.MarkOutboxSent(ctx, accepted.ID, "teams-message-1"); err != nil {
		t.Fatalf("MarkOutboxSent accepted error: %v", err)
	}
	pending, err = store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("PendingOutboxAt after promotion error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after promotion while still rate limited = %#v, want queued message still blocked", pending)
	}
}

func TestEarlierUnsentOutboxPreservesSameChatOrdering(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 30, 0, 0, time.UTC)
	messages := []OutboxMessage{
		{ID: "outbox:sent", TeamsChatID: "chat-1", Sequence: 1, Kind: "helper", Body: "already sent", CreatedAt: now},
		{ID: "outbox:blocking", TeamsChatID: "chat-1", Sequence: 2, Kind: "helper", Body: "must send first", CreatedAt: now.Add(time.Second)},
		{ID: "outbox:other-chat", TeamsChatID: "chat-2", Sequence: 1, Kind: "helper", Body: "other chat", CreatedAt: now.Add(2 * time.Second)},
		{ID: "outbox:current", TeamsChatID: "chat-1", Sequence: 3, Kind: "helper", Body: "current", CreatedAt: now.Add(3 * time.Second)},
	}
	for _, msg := range messages {
		if _, _, err := store.QueueOutbox(ctx, msg); err != nil {
			t.Fatalf("QueueOutbox(%s) error: %v", msg.ID, err)
		}
	}
	if _, err := store.MarkOutboxSent(ctx, "outbox:sent", "teams-sent"); err != nil {
		t.Fatalf("MarkOutboxSent error: %v", err)
	}
	current := messages[3]
	earlier, ok, err := store.EarlierUnsentOutbox(ctx, current)
	if err != nil {
		t.Fatalf("EarlierUnsentOutbox error: %v", err)
	}
	if !ok || earlier.ID != "outbox:blocking" {
		t.Fatalf("earlier unsent = %#v ok=%v, want outbox:blocking", earlier, ok)
	}
	if earlier.TeamsChatID != current.TeamsChatID || earlier.Sequence >= current.Sequence {
		t.Fatalf("earlier outbox does not preserve same-chat ordering: earlier=%#v current=%#v", earlier, current)
	}
	if _, ok, err := store.EarlierUnsentOutbox(ctx, messages[1]); err != nil {
		t.Fatalf("EarlierUnsentOutbox for first pending error: %v", err)
	} else if ok {
		t.Fatal("first pending same-chat message should not have an earlier unsent blocker")
	}
}

func TestEarlierUnsentOutboxStatusMatrix(t *testing.T) {
	now := time.Date(2026, 4, 30, 12, 40, 0, 0, time.UTC)
	tests := []struct {
		name      string
		status    OutboxStatus
		teamsID   string
		wantBlock bool
	}{
		{name: "queued blocks", status: OutboxStatusQueued, wantBlock: true},
		{name: "sending blocks", status: OutboxStatusSending, wantBlock: true},
		{name: "accepted blocks until promoted", status: OutboxStatusAccepted, teamsID: "teams-accepted", wantBlock: true},
		{name: "sent does not block", status: OutboxStatusSent, teamsID: "teams-sent"},
		{name: "skipped does not block", status: OutboxStatusSkipped},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			earlier := OutboxMessage{
				ID:             "outbox:earlier",
				TeamsChatID:    "chat-1",
				TeamsMessageID: tc.teamsID,
				Sequence:       1,
				Kind:           "helper",
				Body:           "earlier",
				Status:         tc.status,
				CreatedAt:      now,
			}
			later := OutboxMessage{
				ID:          "outbox:later",
				TeamsChatID: "chat-1",
				Sequence:    2,
				Kind:        "helper",
				Body:        "later",
				CreatedAt:   now.Add(time.Second),
			}
			if _, _, err := store.QueueOutbox(ctx, earlier); err != nil {
				t.Fatalf("QueueOutbox earlier error: %v", err)
			}
			if _, _, err := store.QueueOutbox(ctx, later); err != nil {
				t.Fatalf("QueueOutbox later error: %v", err)
			}
			got, ok, err := store.EarlierUnsentOutbox(ctx, later)
			if err != nil {
				t.Fatalf("EarlierUnsentOutbox error: %v", err)
			}
			if ok != tc.wantBlock {
				t.Fatalf("EarlierUnsentOutbox ok = %v, want %v; earlier=%#v", ok, tc.wantBlock, got)
			}
			if tc.wantBlock && got.ID != earlier.ID {
				t.Fatalf("EarlierUnsentOutbox = %#v, want %#v", got, earlier)
			}
		})
	}
}

func TestEarlierUnsentOutboxIgnoresSkippedMessages(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 45, 0, 0, time.UTC)
	messages := []OutboxMessage{
		{ID: "outbox:skipped", TeamsChatID: "chat-1", Sequence: 1, Kind: "codex-status-001", Body: "superseded", Status: OutboxStatusSkipped, CreatedAt: now},
		{ID: "outbox:current", TeamsChatID: "chat-1", Sequence: 2, Kind: "queued-wait", Body: "current", CreatedAt: now.Add(time.Second)},
	}
	for _, msg := range messages {
		if _, _, err := store.QueueOutbox(ctx, msg); err != nil {
			t.Fatalf("QueueOutbox(%s) error: %v", msg.ID, err)
		}
	}
	if _, ok, err := store.EarlierUnsentOutbox(ctx, messages[1]); err != nil {
		t.Fatalf("EarlierUnsentOutbox error: %v", err)
	} else if ok {
		t.Fatal("skipped same-chat message should not block later outbox delivery")
	}

	if _, err := store.MarkOutboxSent(ctx, "outbox:current", "teams-current"); err != nil {
		t.Fatalf("MarkOutboxSent current error: %v", err)
	}
	blocking, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:blocking",
		TeamsChatID: "chat-1",
		Sequence:    3,
		Kind:        "helper",
		Body:        "must still block",
		CreatedAt:   now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("QueueOutbox blocking error: %v", err)
	}
	later, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:later",
		TeamsChatID: "chat-1",
		Sequence:    4,
		Kind:        "helper",
		Body:        "later",
		CreatedAt:   now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("QueueOutbox later error: %v", err)
	}
	earlier, ok, err := store.EarlierUnsentOutbox(ctx, later)
	if err != nil {
		t.Fatalf("EarlierUnsentOutbox later error: %v", err)
	}
	if !ok || earlier.ID != blocking.ID {
		t.Fatalf("earlier unsent = %#v ok=%v, want queued blocker %#v", earlier, ok, blocking)
	}
}

func TestMarkOutboxDriveItemPersistsUploadMetadataAndClearsError(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:            "outbox:attachment",
		TeamsChatID:   "chat-1",
		Kind:          "attachment",
		Body:          "artifact",
		LastSendError: "previous upload failed",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}

	updated, err := store.MarkOutboxDriveItem(ctx, msg.ID, " drive-item-1 ", " report.txt ", " \"{1176C944-0CB9-4304-974C-5837185EFD6A},1\" ", " https://sharepoint.example/report.txt ", " dav://report ")
	if err != nil {
		t.Fatalf("MarkOutboxDriveItem error: %v", err)
	}
	if updated.DriveItemID != "drive-item-1" ||
		updated.DriveItemName != "report.txt" ||
		updated.DriveItemETag != "\"{1176C944-0CB9-4304-974C-5837185EFD6A},1\"" ||
		updated.DriveItemWebURL != "https://sharepoint.example/report.txt" ||
		updated.DriveItemWebDav != "dav://report" ||
		updated.LastSendError != "" {
		t.Fatalf("unexpected DriveItem metadata: %#v", updated)
	}
	reloaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := reloaded.OutboxMessages[msg.ID]; got.DriveItemID != "drive-item-1" || got.DriveItemETag == "" || got.LastSendError != "" {
		t.Fatalf("DriveItem metadata was not durable: %#v", got)
	}
}

func TestChatRateLimitReturnsTrimmedStateAndRejectsEmptyChatID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	blockedUntil := time.Date(2026, 4, 30, 13, 0, 0, 0, time.UTC)
	if _, err := store.SetChatRateLimit(ctx, " chat-1 ", blockedUntil, "429 Retry-After"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	limit, ok, err := store.ChatRateLimit(ctx, " chat-1 ")
	if err != nil {
		t.Fatalf("ChatRateLimit error: %v", err)
	}
	if !ok || limit.ChatID != "chat-1" || !limit.BlockedUntil.Equal(blockedUntil) || limit.Reason != "429 Retry-After" {
		t.Fatalf("unexpected chat rate limit: %#v ok=%v", limit, ok)
	}
	if _, _, err := store.ChatRateLimit(ctx, " "); err == nil {
		t.Fatal("expected empty chat id rejection")
	}
}

func TestServiceControlPauseResumeIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	initial, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if initial.Paused || initial.Draining || initial.Reason != "" || !initial.UpdatedAt.IsZero() {
		t.Fatalf("initial control = %#v, want zero value", initial)
	}

	paused, err := store.SetPaused(ctx, true, " maintenance ")
	if err != nil {
		t.Fatalf("SetPaused true error: %v", err)
	}
	if !paused.Paused || paused.Draining || paused.Reason != "maintenance" || paused.UpdatedAt.IsZero() {
		t.Fatalf("paused control mismatch: %#v", paused)
	}
	again, err := store.SetPaused(ctx, true, "maintenance")
	if err != nil {
		t.Fatalf("idempotent SetPaused true error: %v", err)
	}
	if !sameControl(again, paused) {
		t.Fatalf("idempotent pause changed control: got %#v want %#v", again, paused)
	}

	resumed, err := store.SetPaused(ctx, false, "")
	if err != nil {
		t.Fatalf("SetPaused false error: %v", err)
	}
	if resumed.Paused || resumed.Draining || resumed.Reason != "" || resumed.UpdatedAt.IsZero() {
		t.Fatalf("resumed control mismatch: %#v", resumed)
	}
	resumedAgain, err := store.SetPaused(ctx, false, "")
	if err != nil {
		t.Fatalf("idempotent SetPaused false error: %v", err)
	}
	if !sameControl(resumedAgain, resumed) {
		t.Fatalf("idempotent resume changed control: got %#v want %#v", resumedAgain, resumed)
	}
}

func TestServiceControlDrainSetAndClear(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	draining, err := store.SetDraining(ctx, " upgrade ")
	if err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	if draining.Paused || !draining.Draining || draining.Reason != "upgrade" || draining.UpdatedAt.IsZero() {
		t.Fatalf("draining control mismatch: %#v", draining)
	}
	drainingAgain, err := store.SetDraining(ctx, "upgrade")
	if err != nil {
		t.Fatalf("idempotent SetDraining error: %v", err)
	}
	if !sameControl(drainingAgain, draining) {
		t.Fatalf("idempotent drain changed control: got %#v want %#v", drainingAgain, draining)
	}

	cleared, err := store.ClearDrain(ctx)
	if err != nil {
		t.Fatalf("ClearDrain error: %v", err)
	}
	if cleared.Paused || cleared.Draining || cleared.Reason != "" || cleared.UpdatedAt.IsZero() {
		t.Fatalf("cleared drain control mismatch: %#v", cleared)
	}
	clearedAgain, err := store.ClearDrain(ctx)
	if err != nil {
		t.Fatalf("idempotent ClearDrain error: %v", err)
	}
	if !sameControl(clearedAgain, cleared) {
		t.Fatalf("idempotent drain clear changed control: got %#v want %#v", clearedAgain, cleared)
	}
}

func TestUpgradeLifecycleRestoresControl(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	req, err := store.BeginUpgrade(ctx, HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if req.ID == "" || req.Phase != UpgradePhaseDraining || req.Reason != HelperUpgradeReason || req.DeadlineAt.IsZero() {
		t.Fatalf("upgrade request mismatch: %#v", req)
	}
	control, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if !control.Draining || control.Reason != HelperUpgradeReason {
		t.Fatalf("control after BeginUpgrade = %#v, want helper drain", control)
	}

	ready, err := store.MarkUpgradeReady(ctx, req.ID)
	if err != nil {
		t.Fatalf("MarkUpgradeReady error: %v", err)
	}
	if ready.Phase != UpgradePhaseReady || ready.ReadyAt.IsZero() {
		t.Fatalf("ready request mismatch: %#v", ready)
	}
	completed, err := store.CompleteUpgrade(ctx, req.ID, "v1.2.4")
	if err != nil {
		t.Fatalf("CompleteUpgrade error: %v", err)
	}
	if completed.Phase != UpgradePhaseCompleted || completed.CompletedAt.IsZero() {
		t.Fatalf("completed request mismatch: %#v", completed)
	}
	if completed.InstalledTag != "v1.2.4" {
		t.Fatalf("InstalledTag = %q, want v1.2.4", completed.InstalledTag)
	}
	notified, err := store.MarkUpgradeCompletionNoticeQueued(ctx, req.ID, "outbox-upgrade-complete")
	if err != nil {
		t.Fatalf("MarkUpgradeCompletionNoticeQueued error: %v", err)
	}
	if notified.CompletionNoticeID != "outbox-upgrade-complete" || notified.CompletionNoticeAt.IsZero() {
		t.Fatalf("completion notice fields mismatch: %#v", notified)
	}
	control, err = store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl after complete error: %v", err)
	}
	if control.Paused || control.Draining || control.Reason != "" {
		t.Fatalf("control after complete = %#v, want restored running", control)
	}
}

func TestHelperUpgradeDrainExpired(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	base := State{
		ServiceControl: ServiceControl{Draining: true, Reason: HelperUpgradeReason},
		Upgrade: &UpgradeRequest{
			ID:         "upgrade-1",
			Phase:      UpgradePhaseDraining,
			Reason:     HelperUpgradeReason,
			DeadlineAt: now.Add(-time.Second),
		},
	}
	if !HelperUpgradeDrainExpired(base, now) {
		t.Fatal("expired helper upgrade drain was not detected")
	}

	future := base
	future.Upgrade = cloneUpgradeForTest(base.Upgrade)
	future.Upgrade.DeadlineAt = now.Add(time.Second)
	if HelperUpgradeDrainExpired(future, now) {
		t.Fatal("future helper upgrade drain reported expired")
	}

	completed := base
	completed.Upgrade = cloneUpgradeForTest(base.Upgrade)
	completed.Upgrade.Phase = UpgradePhaseCompleted
	if HelperUpgradeDrainExpired(completed, now) {
		t.Fatal("completed helper upgrade reported expired")
	}

	plainDrain := base
	plainDrain.Upgrade = nil
	if HelperUpgradeDrainExpired(plainDrain, now) {
		t.Fatal("plain helper drain without upgrade request reported expired")
	}

	codexUpgrade := base
	codexUpgrade.ServiceControl.Reason = CodexUpgradeReason
	codexUpgrade.Upgrade = cloneUpgradeForTest(base.Upgrade)
	codexUpgrade.Upgrade.Reason = CodexUpgradeReason
	if HelperUpgradeDrainExpired(codexUpgrade, now) {
		t.Fatal("Codex upgrade drain reported as helper upgrade expired")
	}
}

func TestHelperReloadDrainStale(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	staleAfter := 6 * time.Minute
	base := State{
		ServiceControl: ServiceControl{
			Draining:  true,
			Reason:    HelperReloadReason,
			UpdatedAt: now.Add(-staleAfter - time.Second),
		},
	}
	if !HelperReloadDrainStale(base, now, staleAfter) {
		t.Fatal("stale helper reload drain was not detected")
	}

	fresh := base
	fresh.ServiceControl.UpdatedAt = now.Add(-staleAfter + time.Second)
	if HelperReloadDrainStale(fresh, now, staleAfter) {
		t.Fatal("fresh helper reload drain reported stale")
	}

	unknownUpdatedAt := base
	unknownUpdatedAt.ServiceControl.UpdatedAt = time.Time{}
	if !HelperReloadDrainStale(unknownUpdatedAt, now, staleAfter) {
		t.Fatal("helper reload drain without timestamp should be recoverable")
	}

	future := base
	future.ServiceControl.UpdatedAt = now.Add(time.Second)
	if HelperReloadDrainStale(future, now, staleAfter) {
		t.Fatal("future helper reload drain timestamp reported stale")
	}

	upgradeDrain := base
	upgradeDrain.ServiceControl.Reason = HelperUpgradeReason
	if HelperReloadDrainStale(upgradeDrain, now, staleAfter) {
		t.Fatal("helper upgrade drain reported as stale reload drain")
	}

	notDraining := base
	notDraining.ServiceControl.Draining = false
	if HelperReloadDrainStale(notDraining, now, staleAfter) {
		t.Fatal("non-draining service control reported as stale reload drain")
	}

	if HelperReloadDrainStale(base, now, 0) {
		t.Fatal("zero stale threshold must not report stale")
	}
}

func cloneUpgradeForTest(req *UpgradeRequest) *UpgradeRequest {
	if req == nil {
		return nil
	}
	out := *req
	return &out
}

func TestUpgradeRescueInterruptsRunningPreservesQueuedAndSkipsTransientOutbox(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, _, err := store.CreateSession(ctx, SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1"}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.QueueTurn(ctx, Turn{ID: "turn:queued", SessionID: "s1", Status: TurnStatusQueued}); err != nil {
		t.Fatalf("QueueTurn queued error: %v", err)
	}
	if _, _, err := store.QueueTurn(ctx, Turn{ID: "turn:running", SessionID: "s1", Status: TurnStatusRunning}); err != nil {
		t.Fatalf("QueueTurn running error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:status",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "codex-status-001",
		Status:      OutboxStatusQueued,
	}); err != nil {
		t.Fatalf("QueueOutbox status error: %v", err)
	}

	report, err := store.RescueForUpgrade(ctx, UpgradeRescueOptions{Reason: HelperUpgradeReason})
	if err != nil {
		t.Fatalf("RescueForUpgrade error: %v", err)
	}
	if report.Upgrade.Phase != UpgradePhaseDraining || report.Upgrade.RescueStartedAt.IsZero() || report.Upgrade.RescueCompletedAt.IsZero() {
		t.Fatalf("upgrade rescue metadata mismatch: %#v", report.Upgrade)
	}
	if got := report.PreservedQueuedTurnIDs; !reflect.DeepEqual(got, []string{"turn:queued"}) {
		t.Fatalf("PreservedQueuedTurnIDs = %#v, want queued turn", got)
	}
	if got := report.InterruptedTurnIDs; !reflect.DeepEqual(got, []string{"turn:running"}) {
		t.Fatalf("InterruptedTurnIDs = %#v, want running turn", got)
	}
	if got := report.SupersededOutboxIDs; !reflect.DeepEqual(got, []string{"outbox:status"}) {
		t.Fatalf("SupersededOutboxIDs = %#v, want transient outbox", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns["turn:queued"].Status; got != TurnStatusQueued {
		t.Fatalf("queued turn status = %q, want queued", got)
	}
	running := state.Turns["turn:running"]
	if running.Status != TurnStatusInterrupted || running.RecoveryReason != "interrupted by helper upgrade rescue" {
		t.Fatalf("running turn after rescue = %#v", running)
	}
	if got := state.OutboxMessages["outbox:status"].Status; got != OutboxStatusSkipped {
		t.Fatalf("transient outbox status = %q, want skipped", got)
	}
}

func TestUpgradeRescuePreservesProtectedOutboxBlocker(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:answer",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "answer",
		Body:        "saved answer",
		Status:      OutboxStatusQueued,
	}); err != nil {
		t.Fatalf("QueueOutbox protected error: %v", err)
	}
	report, err := store.RescueForUpgrade(ctx, UpgradeRescueOptions{Reason: HelperUpgradeReason})
	if err != nil {
		t.Fatalf("RescueForUpgrade error: %v", err)
	}
	if got := report.PreservedOutboxBlockerIDs; !reflect.DeepEqual(got, []string{"outbox:answer"}) {
		t.Fatalf("PreservedOutboxBlockerIDs = %#v, want protected outbox", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.OutboxMessages["outbox:answer"].Status; got != OutboxStatusQueued {
		t.Fatalf("protected outbox status = %q, want queued", got)
	}
	if !OutboxBlocksUpgrade(state, state.OutboxMessages["outbox:answer"], time.Now()) {
		t.Fatal("protected outbox should still block upgrade after rescue")
	}
}

func TestUpgradeRescueFencesStaleOwnerButRefusesLiveOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	live := testOwner("s1", "turn:running", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, live, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat live error: %v", err)
	}
	if _, err := store.RescueForUpgrade(ctx, UpgradeRescueOptions{Reason: HelperUpgradeReason, StaleAfter: time.Minute}); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("live owner rescue error = %v, want ErrOwnerLive", err)
	}
	if err := store.ClearOwner(ctx); err != nil {
		t.Fatalf("ClearOwner after live refusal error: %v", err)
	}
	staleHeartbeat := time.Now().Add(-2 * time.Minute)
	stale := testOwner("s1", "turn:running", staleHeartbeat)
	if _, err := store.RecordOwnerHeartbeat(ctx, stale, time.Minute, staleHeartbeat); err != nil {
		t.Fatalf("RecordOwnerHeartbeat stale error: %v", err)
	}
	report, err := store.RescueForUpgrade(ctx, UpgradeRescueOptions{Reason: HelperUpgradeReason, StaleAfter: time.Minute})
	if err != nil {
		t.Fatalf("stale RescueForUpgrade error: %v", err)
	}
	if report.ClearedOwner == nil {
		t.Fatal("stale owner should be cleared")
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	} else if ok {
		t.Fatal("owner should be absent after stale rescue")
	}
}

func TestUpgradeNotificationTargetsDeduplicate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	req, err := store.BeginUpgrade(ctx, CodexUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	target := UpgradeNotificationTarget{SessionID: "s001", TurnID: "turn-1", TeamsChatID: "chat-1"}
	updated, err := store.AddUpgradeNotificationTarget(ctx, req.ID, target)
	if err != nil {
		t.Fatalf("AddUpgradeNotificationTarget error: %v", err)
	}
	updated, err = store.AddUpgradeNotificationTarget(ctx, req.ID, target)
	if err != nil {
		t.Fatalf("duplicate AddUpgradeNotificationTarget error: %v", err)
	}
	updated, err = store.AddUpgradeNotificationTarget(ctx, req.ID, UpgradeNotificationTarget{SessionID: "s002", TurnID: "turn-2", TeamsChatID: "chat-2"})
	if err != nil {
		t.Fatalf("second AddUpgradeNotificationTarget error: %v", err)
	}
	if len(updated.NotificationTargets) != 2 {
		t.Fatalf("notification targets = %#v, want two unique targets", updated.NotificationTargets)
	}
	if updated.NotificationTargets[0].CreatedAt.IsZero() || updated.NotificationTargets[1].CreatedAt.IsZero() {
		t.Fatalf("notification targets missing CreatedAt: %#v", updated.NotificationTargets)
	}
}

func TestUpgradeAbortPreservesPreviousDrain(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.SetDraining(ctx, "maintenance"); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	req, err := store.BeginUpgrade(ctx, HelperUpgradeReason, 0)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if !req.PreviousControl.Draining || req.PreviousControl.Reason != "maintenance" {
		t.Fatalf("previous control mismatch: %#v", req.PreviousControl)
	}
	if _, err := store.AbortUpgrade(ctx, req.ID, "download failed"); err != nil {
		t.Fatalf("AbortUpgrade error: %v", err)
	}
	control, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl after abort error: %v", err)
	}
	if !control.Draining || control.Reason != "maintenance" {
		t.Fatalf("control after abort = %#v, want previous drain", control)
	}
	read, ok, err := store.ReadUpgrade(ctx)
	if err != nil {
		t.Fatalf("ReadUpgrade error: %v", err)
	}
	if !ok || read.Phase != UpgradePhaseAborted || read.AbortReason != "download failed" {
		t.Fatalf("aborted upgrade mismatch: %#v ok=%v", read, ok)
	}
}

func TestAutoUpdateLifecyclePersistsCandidateAndClearsAfterInstall(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	published := now.Add(-time.Hour)
	eligible := published

	checked, err := store.RecordAutoUpdateCheck(ctx, AutoUpdateRecord{
		Now:                  now,
		NextCheckAt:          now.Add(30 * time.Minute),
		CandidateTag:         " v1.2.4 ",
		CandidateVersion:     " 1.2.4 ",
		CandidatePriority:    " p0 ",
		CandidateAsset:       " codex-proxy_1.2.4_linux_amd64 ",
		CandidatePublishedAt: published,
		CandidateEligibleAt:  eligible,
	})
	if err != nil {
		t.Fatalf("RecordAutoUpdateCheck error: %v", err)
	}
	if checked.CandidateTag != "v1.2.4" || checked.CandidateVersion != "1.2.4" || checked.CandidatePriority != "p0" {
		t.Fatalf("candidate was not normalized: %#v", checked)
	}
	if checked.LastSuccessAt.IsZero() || !checked.LastErrorAt.IsZero() {
		t.Fatalf("success/error timestamps mismatch after check: %#v", checked)
	}
	if _, err := store.RecordAutoUpdateAttempt(ctx, " v1.2.4 ", now.Add(time.Minute)); err != nil {
		t.Fatalf("RecordAutoUpdateAttempt error: %v", err)
	}
	installed, err := store.RecordAutoUpdateInstalled(ctx, " v1.2.4 ", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("RecordAutoUpdateInstalled error: %v", err)
	}
	if installed.LastInstalledTag != "v1.2.4" || installed.LastInstalledAt.IsZero() {
		t.Fatalf("installed state mismatch: %#v", installed)
	}
	if installed.CandidateTag != "" || installed.CandidateVersion != "" || !installed.CandidatePublishedAt.IsZero() {
		t.Fatalf("candidate should be cleared after install: %#v", installed)
	}

	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	read, err := reopened.ReadAutoUpdate(ctx)
	if err != nil {
		t.Fatalf("ReadAutoUpdate error: %v", err)
	}
	if read.LastInstalledTag != "v1.2.4" || read.LastAttemptTag != "v1.2.4" {
		t.Fatalf("reopened auto-update state mismatch: %#v", read)
	}
}

func TestAutoUpdateCheckRecordsTrimmedErrorAndBackoff(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	longErr := strings.Repeat("x", 400)

	state, err := store.RecordAutoUpdateCheck(ctx, AutoUpdateRecord{
		Now:          now,
		NextCheckAt:  now.Add(30 * time.Minute),
		BackoffUntil: now.Add(10 * time.Minute),
		LastError:    longErr,
	})
	if err != nil {
		t.Fatalf("RecordAutoUpdateCheck error: %v", err)
	}
	if state.LastError == "" || len(state.LastError) > 240 {
		t.Fatalf("LastError was not trimmed: len=%d value=%q", len(state.LastError), state.LastError)
	}
	if state.LastErrorAt.IsZero() || !state.BackoffUntil.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("error/backoff timestamps mismatch: %#v", state)
	}

	cleared, err := store.RecordAutoUpdateCheck(ctx, AutoUpdateRecord{
		Now:         now.Add(time.Hour),
		NextCheckAt: now.Add(90 * time.Minute),
	})
	if err != nil {
		t.Fatalf("RecordAutoUpdateCheck clear error: %v", err)
	}
	if cleared.LastError != "" || !cleared.LastErrorAt.IsZero() || cleared.LastSuccessAt.IsZero() {
		t.Fatalf("success check should clear prior error: %#v", cleared)
	}
}

func TestDeferredInboundIsDurableAndListed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	event, created, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-deferred",
		Source:         "teams",
		Status:         InboundStatusDeferred,
	})
	if err != nil {
		t.Fatalf("PersistInbound deferred error: %v", err)
	}
	if !created || event.Status != InboundStatusDeferred {
		t.Fatalf("deferred event mismatch: %#v created=%v", event, created)
	}
	deferred, err := store.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].ID != event.ID {
		t.Fatalf("deferred inbound = %#v, want %s", deferred, event.ID)
	}
	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	deferred, err = reopened.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound reopened error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].Status != InboundStatusDeferred {
		t.Fatalf("reopened deferred inbound = %#v", deferred)
	}
}

func TestHasUpgradeBlockingWorkAllowsDeferredAndRateLimitedOutbox(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "deferred-only",
		Status:         InboundStatusDeferred,
	}); err != nil {
		t.Fatalf("PersistInbound deferred error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load deferred-only error: %v", err)
	}
	if HasUpgradeBlockingWork(state, now) {
		t.Fatal("deferred inbound should not block upgrade")
	}

	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:blocked",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "blocked",
	}); err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load queued outbox error: %v", err)
	}
	if !HasUpgradeBlockingWork(state, now) {
		t.Fatal("queued outbox without rate limit should block upgrade")
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-1", now.Add(time.Minute), "429"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load rate-limited outbox error: %v", err)
	}
	if HasUpgradeBlockingWork(state, now) {
		t.Fatal("rate-limited durable outbox should not block upgrade")
	}

	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:                 "outbox:nonblocking-ack",
		TeamsChatID:        "chat-2",
		Kind:               "ack",
		Body:               "upgrade notice",
		UpgradeNonBlocking: true,
	}); err != nil {
		t.Fatalf("QueueOutbox nonblocking ack error: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load nonblocking ack error: %v", err)
	}
	if HasUpgradeBlockingWork(state, now) {
		t.Fatal("upgrade non-blocking ACK outbox should not block upgrade")
	}
}

func TestOutboxBlocksUpgradeStatusMatrix(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	base := State{
		ChatRateLimits: map[string]ChatRateLimitState{},
	}
	freshAttempt := now.Add(-outboxSendLease + time.Second)
	staleAttempt := now.Add(-outboxSendLease - time.Second)
	tests := []struct {
		name  string
		msg   OutboxMessage
		limit ChatRateLimitState
		want  bool
	}{
		{
			name: "queued blocks",
			msg:  OutboxMessage{ID: "queued", TeamsChatID: "chat-1", Status: OutboxStatusQueued},
			want: true,
		},
		{
			name: "legacy queued codex status does not block",
			msg:  OutboxMessage{ID: "queued-status", TeamsChatID: "chat-1", Kind: "codex-status-001", Status: OutboxStatusQueued},
			want: false,
		},
		{
			name: "queued codex compact status does not block",
			msg:  OutboxMessage{ID: "queued-compact", TeamsChatID: "chat-1", Kind: "codex-compact-001", Status: OutboxStatusQueued},
			want: false,
		},
		{
			name: "legacy queued interrupted notice does not block",
			msg:  OutboxMessage{ID: "queued-interrupted", TeamsChatID: "chat-1", Kind: "interrupted", Status: OutboxStatusQueued},
			want: false,
		},
		{
			name: "generic error still blocks",
			msg:  OutboxMessage{ID: "queued-error", TeamsChatID: "chat-1", Kind: "error", Status: OutboxStatusQueued},
			want: true,
		},
		{
			name: "generic recovery still blocks",
			msg:  OutboxMessage{ID: "queued-recovery", TeamsChatID: "chat-1", Kind: "recovery-missing-message", Status: OutboxStatusQueued},
			want: true,
		},
		{
			name: "final overrides nonblocking hint",
			msg:  OutboxMessage{ID: "queued-final", TeamsChatID: "chat-1", Kind: "final", Status: OutboxStatusQueued, UpgradeNonBlocking: true},
			want: true,
		},
		{
			name:  "final overrides rate limit",
			msg:   OutboxMessage{ID: "queued-rate-limited-final", TeamsChatID: "chat-1", Kind: "final", Status: OutboxStatusQueued},
			limit: ChatRateLimitState{ChatID: "chat-1", BlockedUntil: now.Add(time.Minute)},
			want:  true,
		},
		{
			name: "attachment overrides nonblocking hint",
			msg:  OutboxMessage{ID: "queued-attachment", TeamsChatID: "chat-1", Kind: "helper", AttachmentPath: "/tmp/report.txt", Status: OutboxStatusQueued, UpgradeNonBlocking: true},
			want: true,
		},
		{
			name: "turn completed notification overrides nonblocking hint",
			msg:  OutboxMessage{ID: "queued-completed", TeamsChatID: "chat-1", Kind: "helper", NotificationKind: "turn_completed", Status: OutboxStatusQueued, UpgradeNonBlocking: true},
			want: true,
		},
		{
			name:  "queued rate limited does not block",
			msg:   OutboxMessage{ID: "queued-rate-limited", TeamsChatID: "chat-1", Status: OutboxStatusQueued},
			limit: ChatRateLimitState{ChatID: "chat-1", BlockedUntil: now.Add(time.Minute)},
			want:  false,
		},
		{
			name: "fresh sending blocks",
			msg:  OutboxMessage{ID: "fresh-sending", TeamsChatID: "chat-1", Status: OutboxStatusSending, LastSendAttempt: freshAttempt},
			want: true,
		},
		{
			name: "sending with missing attempt blocks",
			msg:  OutboxMessage{ID: "missing-attempt", TeamsChatID: "chat-1", Status: OutboxStatusSending},
			want: true,
		},
		{
			name: "stale sending does not block",
			msg:  OutboxMessage{ID: "stale-sending", TeamsChatID: "chat-1", Status: OutboxStatusSending, LastSendAttempt: staleAttempt},
			want: false,
		},
		{
			name: "accepted does not block",
			msg:  OutboxMessage{ID: "accepted", TeamsChatID: "chat-1", Status: OutboxStatusAccepted, TeamsMessageID: "teams-1"},
			want: false,
		},
		{
			name: "sent does not block",
			msg:  OutboxMessage{ID: "sent", TeamsChatID: "chat-1", Status: OutboxStatusSent, TeamsMessageID: "teams-1"},
			want: false,
		},
		{
			name: "skipped does not block",
			msg:  OutboxMessage{ID: "skipped", TeamsChatID: "chat-1", Status: OutboxStatusSkipped},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := base
			state.ChatRateLimits = map[string]ChatRateLimitState{}
			if !tc.limit.BlockedUntil.IsZero() {
				state.ChatRateLimits[tc.limit.ChatID] = tc.limit
			}
			if got := OutboxBlocksUpgrade(state, tc.msg, now); got != tc.want {
				t.Fatalf("OutboxBlocksUpgrade = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestQueueOutboxMarksNotificationKindsUpgradeNonBlocking(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:status",
		TeamsChatID: "chat-1",
		Kind:        "codex-status-001",
		Body:        "still working",
	}); err != nil {
		t.Fatalf("QueueOutbox status error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:compact",
		TeamsChatID: "chat-1",
		Kind:        "codex-compact-001",
		Body:        "context compacted",
	}); err != nil {
		t.Fatalf("QueueOutbox compact error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:final",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "answer",
	}); err != nil {
		t.Fatalf("QueueOutbox final error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if !state.OutboxMessages["outbox:status"].UpgradeNonBlocking {
		t.Fatalf("status outbox should be upgrade non-blocking: %#v", state.OutboxMessages["outbox:status"])
	}
	if !state.OutboxMessages["outbox:compact"].UpgradeNonBlocking {
		t.Fatalf("compact status outbox should be upgrade non-blocking: %#v", state.OutboxMessages["outbox:compact"])
	}
	if state.OutboxMessages["outbox:final"].UpgradeNonBlocking {
		t.Fatalf("final outbox should still block upgrade: %#v", state.OutboxMessages["outbox:final"])
	}
}

func TestMarkTurnInterruptedSkipsPendingTransientOutbox(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, Turn{ID: "turn:streaming", SessionID: "s1"})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, err := store.MarkTurnRunning(ctx, turn.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnRunning error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:status",
		SessionID:   "s1",
		TurnID:      turn.ID,
		TeamsChatID: "chat-1",
		Kind:        "codex-status-001",
		Body:        "stale status",
	}); err != nil {
		t.Fatalf("QueueOutbox status error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:final",
		SessionID:   "s1",
		TurnID:      turn.ID,
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "protected final",
	}); err != nil {
		t.Fatalf("QueueOutbox final error: %v", err)
	}

	if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "canceled by user"); err != nil {
		t.Fatalf("MarkTurnInterrupted error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.OutboxMessages["outbox:status"].Status; got != OutboxStatusSkipped {
		t.Fatalf("status outbox status = %q, want skipped", got)
	}
	if got := state.OutboxMessages["outbox:final"].Status; got != OutboxStatusQueued {
		t.Fatalf("final outbox status = %q, want queued", got)
	}
}

func TestRecordTranscriptDeliveryAdvancesCheckpointPosition(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	checkpointID := "checkpoint:s1"
	if _, _, err := store.RecordTranscriptDelivery(ctx, TranscriptDeliveryRecord{
		ID:        "delivery:s1:r1",
		SessionID: "s1",
		Status:    TranscriptDeliveryStatusSkipped,
	}, ImportCheckpoint{
		ID:             checkpointID,
		SessionID:      "s1",
		SourcePath:     "/tmp/session.jsonl",
		LastRecordID:   "r1",
		LastSourceLine: 42,
		LastOffset:     2048,
		Status:         "complete",
	}); err != nil {
		t.Fatalf("RecordTranscriptDelivery error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.LastRecordID != "r1" || checkpoint.LastSourceLine != 42 || checkpoint.LastOffset != 2048 {
		t.Fatalf("checkpoint = %#v, want full source position for skipped delivery", checkpoint)
	}
}

func TestRecoverSupersedesTransientOutboxButPreservesProtectedDelivery(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	for _, msg := range []OutboxMessage{
		{ID: "outbox:status", SessionID: "s1", TeamsChatID: "chat-1", Kind: "codex-status-001", Status: OutboxStatusQueued},
		{ID: "outbox:interrupted", SessionID: "s1", TeamsChatID: "chat-1", Kind: "interrupted", Status: OutboxStatusSending},
		{ID: "outbox:final", SessionID: "s1", TeamsChatID: "chat-1", Kind: "final", Status: OutboxStatusQueued, UpgradeNonBlocking: true},
		{ID: "outbox:artifact", SessionID: "s1", TeamsChatID: "chat-1", Kind: "helper", AttachmentPath: "/tmp/report.txt", Status: OutboxStatusQueued, UpgradeNonBlocking: true},
	} {
		if _, _, err := store.QueueOutbox(ctx, msg); err != nil {
			t.Fatalf("QueueOutbox %s error: %v", msg.ID, err)
		}
	}

	report, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover error: %v", err)
	}
	if got, want := report.SupersededOutboxIDs, []string{"outbox:interrupted", "outbox:status"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SupersededOutboxIDs = %#v, want %#v", got, want)
	}
	if got, want := report.PreservedOutboxBlockerIDs, []string{"outbox:artifact", "outbox:final"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PreservedOutboxBlockerIDs = %#v, want %#v", got, want)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	for _, id := range []string{"outbox:status", "outbox:interrupted"} {
		if got := state.OutboxMessages[id].Status; got != OutboxStatusSkipped {
			t.Fatalf("%s status = %q, want %q", id, got, OutboxStatusSkipped)
		}
		if OutboxBlocksUpgrade(state, state.OutboxMessages[id], time.Now()) {
			t.Fatalf("%s should not block upgrade after recover", id)
		}
	}
	for _, id := range []string{"outbox:final", "outbox:artifact"} {
		if got := state.OutboxMessages[id].Status; got != OutboxStatusQueued {
			t.Fatalf("%s status = %q, want queued", id, got)
		}
		if !OutboxBlocksUpgrade(state, state.OutboxMessages[id], time.Now()) {
			t.Fatalf("%s should still block upgrade after recover", id)
		}
	}
}

func TestRecoverSkipsSaveWhenOnlyProtectedOutboxReportsBlockers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:                 "outbox:protected-final",
		SessionID:          "s1",
		TeamsChatID:        "chat-1",
		Kind:               "final",
		Status:             OutboxStatusQueued,
		UpgradeNonBlocking: true,
	}); err != nil {
		t.Fatalf("QueueOutbox protected error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read store before recover: %v", err)
	}

	report, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover error: %v", err)
	}
	if got, want := report.PreservedOutboxBlockerIDs, []string{"outbox:protected-final"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PreservedOutboxBlockerIDs = %#v, want %#v", got, want)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read store after recover: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("Recover rewrote state even though it only reported protected blockers")
	}
}

func TestHasDeliveredOutboxMessageStatusMatrix(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, msg := range []OutboxMessage{
		{ID: "queued", TeamsChatID: "chat-1", TeamsMessageID: "teams-queued", Status: OutboxStatusQueued},
		{ID: "sending", TeamsChatID: "chat-1", TeamsMessageID: "teams-sending", Status: OutboxStatusSending},
		{ID: "accepted", TeamsChatID: "chat-1", TeamsMessageID: "teams-accepted", Status: OutboxStatusAccepted},
		{ID: "sent", TeamsChatID: "chat-1", TeamsMessageID: "teams-sent", Status: OutboxStatusSent},
		{ID: "other-chat", TeamsChatID: "chat-2", TeamsMessageID: "teams-other", Status: OutboxStatusSent},
	} {
		if _, _, err := store.QueueOutbox(ctx, msg); err != nil {
			t.Fatalf("QueueOutbox %s error: %v", msg.ID, err)
		}
	}
	tests := []struct {
		name      string
		chatID    string
		messageID string
		want      bool
	}{
		{name: "queued", chatID: "chat-1", messageID: "teams-queued", want: false},
		{name: "sending", chatID: "chat-1", messageID: "teams-sending", want: false},
		{name: "accepted", chatID: "chat-1", messageID: "teams-accepted", want: true},
		{name: "sent", chatID: "chat-1", messageID: "teams-sent", want: true},
		{name: "other chat", chatID: "chat-1", messageID: "teams-other", want: false},
		{name: "missing", chatID: "chat-1", messageID: "missing", want: false},
		{name: "empty", chatID: "", messageID: "teams-sent", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.HasDeliveredOutboxMessage(ctx, tc.chatID, tc.messageID)
			if err != nil {
				t.Fatalf("HasDeliveredOutboxMessage error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("HasDeliveredOutboxMessage = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasUpgradeBlockingWorkTurnStatusMatrix(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		status TurnStatus
		want   bool
	}{
		{name: "queued", status: TurnStatusQueued, want: true},
		{name: "running", status: TurnStatusRunning, want: true},
		{name: "completed", status: TurnStatusCompleted, want: false},
		{name: "failed", status: TurnStatusFailed, want: false},
		{name: "interrupted", status: TurnStatusInterrupted, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := State{
				Turns: map[string]Turn{
					"turn-1": {ID: "turn-1", SessionID: "s1", Status: tc.status},
				},
				OutboxMessages: map[string]OutboxMessage{},
				ChatRateLimits: map[string]ChatRateLimitState{},
			}
			if got := HasUpgradeBlockingWork(state, now); got != tc.want {
				t.Fatalf("HasUpgradeBlockingWork = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUpgradeBlockersDescribeBlockingWork(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	state := State{
		Turns: map[string]Turn{
			"turn-running": {ID: "turn-running", SessionID: "s1", Status: TurnStatusRunning},
			"turn-done":    {ID: "turn-done", SessionID: "s1", Status: TurnStatusCompleted},
		},
		OutboxMessages: map[string]OutboxMessage{
			"outbox-blocked": {ID: "outbox-blocked", SessionID: "s2", TeamsChatID: "chat-2", Kind: "answer", Status: OutboxStatusQueued},
			"outbox-sent":    {ID: "outbox-sent", SessionID: "s2", TeamsChatID: "chat-2", Kind: "answer", Status: OutboxStatusSent},
		},
		ChatRateLimits: map[string]ChatRateLimitState{},
		ImportCheckpoints: map[string]ImportCheckpoint{
			"transcript:s1": {ID: "transcript:s1", SessionID: "s1", Status: "importing"},
		},
	}
	blockers := UpgradeBlockers(state, now)
	if len(blockers) != 2 {
		t.Fatalf("UpgradeBlockers len = %d, want 2: %#v", len(blockers), blockers)
	}
	if blockers[0].Kind != "outbox" || blockers[0].ID != "outbox-blocked" || blockers[0].Status != string(OutboxStatusQueued) || blockers[0].Detail != "answer" {
		t.Fatalf("outbox blocker mismatch: %#v", blockers[0])
	}
	if blockers[1].Kind != "turn" || blockers[1].ID != "turn-running" || blockers[1].Status != string(TurnStatusRunning) {
		t.Fatalf("turn blocker mismatch: %#v", blockers[1])
	}
}

func TestHasUpgradeBlockingWorkIgnoresImportCheckpointWithoutWork(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	state := State{
		Turns:          map[string]Turn{},
		OutboxMessages: map[string]OutboxMessage{},
		ChatRateLimits: map[string]ChatRateLimitState{},
		ImportCheckpoints: map[string]ImportCheckpoint{
			"transcript:s1": {
				ID:           "transcript:s1",
				SessionID:    "s1",
				Status:       "importing",
				ImportTurnID: "import:s1",
			},
		},
	}
	if HasUpgradeBlockingWork(state, now) {
		t.Fatalf("import checkpoint without queued/running turn or blocking outbox should not block upgrade")
	}
	if blockers := UpgradeBlockers(state, now); len(blockers) != 0 {
		t.Fatalf("UpgradeBlockers = %#v, want none", blockers)
	}
}

func TestPermissionsUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix file mode assertion")
	}
	store := newTestStore(t)
	if _, _, err := store.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}

	dirInfo, err := os.Stat(filepath.Dir(store.Path()))
	if err != nil {
		t.Fatalf("stat state dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("dir mode = %o, want 700", got)
	}
	fileInfo, err := os.Stat(store.Path())
	if err != nil {
		t.Fatalf("stat state file: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("file mode = %o, want 600", got)
	}
}

func TestDuplicateInboundIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}

	first, created, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("first PersistInbound error: %v", err)
	}
	if !created {
		t.Fatal("first inbound created = false")
	}
	beforeDuplicate, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before duplicate inbound: %v", err)
	}
	second, created, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Source:         "teams",
	})
	if err != nil {
		t.Fatalf("second PersistInbound error: %v", err)
	}
	if created {
		t.Fatal("duplicate inbound created = true")
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate inbound ID = %q, want %q", second.ID, first.ID)
	}
	afterDuplicate, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after duplicate inbound: %v", err)
	}
	if !bytes.Equal(beforeDuplicate, afterDuplicate) {
		t.Fatal("duplicate PersistInbound rewrote state file")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 1 {
		t.Fatalf("inbound count = %d, want 1", got)
	}
}

func TestHasInboundMessage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.PersistInbound(ctx, testInbound()); err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	got, err := store.HasInboundMessage(ctx, "chat-1", "message-1")
	if err != nil {
		t.Fatalf("HasInboundMessage error: %v", err)
	}
	if !got {
		t.Fatal("HasInboundMessage = false, want true")
	}
	got, err = store.HasInboundMessage(ctx, "chat-1", "message-missing")
	if err != nil {
		t.Fatalf("HasInboundMessage missing error: %v", err)
	}
	if got {
		t.Fatal("HasInboundMessage missing = true, want false")
	}
	record, ok, err := store.MessageProvenance(ctx, "chat-1", "message-1")
	if err != nil {
		t.Fatalf("MessageProvenance inbound error: %v", err)
	}
	if !ok || record.Origin != MessageOriginUserInbound || record.InboundID == "" {
		t.Fatalf("inbound message provenance = %#v, ok=%v", record, ok)
	}
	lookup, err := store.MessageLookup(ctx, "chat-1", "message-1")
	if err != nil {
		t.Fatalf("MessageLookup inbound error: %v", err)
	}
	if !lookup.HasInbound || !lookup.HasProvenance || lookup.Provenance.Origin != MessageOriginUserInbound || lookup.HasDeliveredOutbox {
		t.Fatalf("MessageLookup inbound = %#v", lookup)
	}
}

func TestMessageProvenanceRecordsHelperOutbox(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	record, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginHelperOutbox,
		SessionID:      "s1",
		TurnID:         "turn-1",
		OutboxID:       "outbox-1",
		Kind:           "helper",
		RenderedHash:   "hash-1",
	})
	if err != nil {
		t.Fatalf("RecordMessageProvenance error: %v", err)
	}
	if record.ID == "" || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		t.Fatalf("record metadata was not initialized: %#v", record)
	}
	got, ok, err := store.MessageProvenance(ctx, "chat-1", "teams-helper-1")
	if err != nil {
		t.Fatalf("MessageProvenance error: %v", err)
	}
	if !ok || got.Origin != MessageOriginHelperOutbox || got.OutboxID != "outbox-1" || got.RenderedHash != "hash-1" {
		t.Fatalf("message provenance = %#v, ok=%v", got, ok)
	}
	delivered, err := store.HasDeliveredOutboxMessage(ctx, "chat-1", "teams-helper-1")
	if err != nil {
		t.Fatalf("HasDeliveredOutboxMessage error: %v", err)
	}
	if !delivered {
		t.Fatal("HasDeliveredOutboxMessage should use helper provenance")
	}
	lookup, err := store.MessageLookup(ctx, "chat-1", "teams-helper-1")
	if err != nil {
		t.Fatalf("MessageLookup helper error: %v", err)
	}
	if !lookup.HasDeliveredOutbox || !lookup.HasProvenance || lookup.Provenance.Origin != MessageOriginHelperOutbox || lookup.HasInbound {
		t.Fatalf("MessageLookup helper = %#v", lookup)
	}
	missing, ok, err := store.MessageProvenance(ctx, "chat-1", "missing")
	if err != nil {
		t.Fatalf("MessageProvenance missing error: %v", err)
	}
	if ok || missing.ID != "" {
		t.Fatalf("missing provenance = %#v, ok=%v", missing, ok)
	}
}

func TestDuplicateMessageProvenanceDoesNotRewriteState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	record := MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginHelperOutbox,
		SessionID:      "s1",
		TurnID:         "turn-1",
		OutboxID:       "outbox-1",
		Kind:           "helper",
		RenderedHash:   "hash-1",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if _, err := store.RecordMessageProvenance(ctx, record); err != nil {
		t.Fatalf("initial RecordMessageProvenance error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before duplicate provenance: %v", err)
	}
	again, err := store.RecordMessageProvenance(ctx, record)
	if err != nil {
		t.Fatalf("duplicate RecordMessageProvenance error: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after duplicate provenance: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("duplicate RecordMessageProvenance rewrote state")
	}
	if again.ID == "" || again.Origin != MessageOriginHelperOutbox || again.OutboxID != "outbox-1" {
		t.Fatalf("duplicate provenance returned unexpected record: %#v", again)
	}
}

func TestDuplicateMessageProvenanceFromSentOutboxDoesNotRewriteState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, created, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:           "outbox:sent-provenance",
		SessionID:    "s1",
		TurnID:       "turn-1",
		TeamsChatID:  "chat-1",
		Kind:         "answer",
		RenderedHash: "hash-1",
		Body:         "sent provenance",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	sent, err := store.MarkOutboxSent(ctx, msg.ID, "teams-helper-1")
	if err != nil {
		t.Fatalf("MarkOutboxSent error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before duplicate sent provenance: %v", err)
	}
	again, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    sent.TeamsChatID,
		TeamsMessageID: sent.TeamsMessageID,
		Origin:         MessageOriginHelperOutbox,
		SessionID:      sent.SessionID,
		TurnID:         sent.TurnID,
		OutboxID:       sent.ID,
		Kind:           sent.Kind,
		RenderedHash:   sent.RenderedHash,
		CreatedAt:      sent.CreatedAt,
		UpdatedAt:      firstStoreNonZeroTime(sent.SentAt, sent.UpdatedAt, sent.CreatedAt),
	})
	if err != nil {
		t.Fatalf("duplicate sent RecordMessageProvenance error: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after duplicate sent provenance: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("duplicate sent RecordMessageProvenance rewrote state")
	}
	if again.OutboxID != sent.ID || again.Origin != MessageOriginHelperOutbox {
		t.Fatalf("duplicate sent provenance returned unexpected record: %#v", again)
	}
}

func TestLargeMessageLookupPressureMatchesStateSemantics(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLargeMessageLookupState(t, store, 14000, 9000, 512)

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load large state: %v", err)
	}
	cases := []struct {
		name      string
		chatID    string
		messageID string
	}{
		{name: "direct inbound", chatID: largeInboundChatID(13999), messageID: largeInboundMessageID(13999)},
		{name: "helper provenance", chatID: "provenance-helper-chat", messageID: largeHelperProvenanceMessageID(8999)},
		{name: "user provenance", chatID: "provenance-user-chat", messageID: largeUserProvenanceMessageID(8998)},
		{name: "sent outbox", chatID: "outbox-chat", messageID: largeOutboxMessageID(511)},
		{name: "queued outbox", chatID: "outbox-chat", messageID: largeOutboxMessageID(510)},
		{name: "missing", chatID: "missing-chat", messageID: "missing-message"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := legacyMessageLookupFromState(state, tc.chatID, tc.messageID)
			got, err := store.MessageLookup(ctx, tc.chatID, tc.messageID)
			if err != nil {
				t.Fatalf("MessageLookup error: %v", err)
			}
			if !messageLookupEqual(got, want) {
				t.Fatalf("MessageLookup = %#v, want %#v", got, want)
			}
			inbound, err := store.HasInboundMessage(ctx, tc.chatID, tc.messageID)
			if err != nil {
				t.Fatalf("HasInboundMessage error: %v", err)
			}
			if inbound != want.HasInbound {
				t.Fatalf("HasInboundMessage = %v, want %v", inbound, want.HasInbound)
			}
			delivered, err := store.HasDeliveredOutboxMessage(ctx, tc.chatID, tc.messageID)
			if err != nil {
				t.Fatalf("HasDeliveredOutboxMessage error: %v", err)
			}
			if delivered != want.HasDeliveredOutbox {
				t.Fatalf("HasDeliveredOutboxMessage = %v, want %v", delivered, want.HasDeliveredOutbox)
			}
			provenance, ok, err := store.MessageProvenance(ctx, tc.chatID, tc.messageID)
			if err != nil {
				t.Fatalf("MessageProvenance error: %v", err)
			}
			if ok != want.HasProvenance || provenance.ID != want.Provenance.ID || provenance.Origin != want.Provenance.Origin {
				t.Fatalf("MessageProvenance = %#v ok=%v, want %#v ok=%v", provenance, ok, want.Provenance, want.HasProvenance)
			}
		})
	}
}

func TestLargeDuplicateInboundPressureDoesNotRewriteState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLargeMessageLookupState(t, store, 14000, 9000, 512)
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read large state before duplicate: %v", err)
	}
	inbound, created, err := store.PersistInbound(ctx, InboundEvent{
		TeamsChatID:    largeInboundChatID(13999),
		TeamsMessageID: largeInboundMessageID(13999),
		Source:         "teams",
	})
	if err != nil {
		t.Fatalf("duplicate PersistInbound error: %v", err)
	}
	if created {
		t.Fatal("duplicate PersistInbound created = true")
	}
	if inbound.ID == "" || inbound.TeamsMessageID != largeInboundMessageID(13999) {
		t.Fatalf("duplicate inbound mismatch: %#v", inbound)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read large state after duplicate: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("large duplicate PersistInbound rewrote state file")
	}
}

func TestMessageLookupCacheTracksSameStoreUpdates(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	missing, err := store.MessageLookup(ctx, "chat-1", "message-1")
	if err != nil {
		t.Fatalf("warm missing MessageLookup error: %v", err)
	}
	if missing.HasInbound || missing.HasProvenance || missing.HasDeliveredOutbox {
		t.Fatalf("warm missing MessageLookup = %#v", missing)
	}
	if _, created, err := store.PersistInbound(ctx, InboundEvent{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Source:         "teams",
	}); err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	} else if !created {
		t.Fatal("PersistInbound created = false")
	}
	lookup, err := store.MessageLookup(ctx, "chat-1", "message-1")
	if err != nil {
		t.Fatalf("MessageLookup after same-store update error: %v", err)
	}
	if !lookup.HasInbound || !lookup.HasProvenance || lookup.Provenance.Origin != MessageOriginUserInbound || lookup.HasDeliveredOutbox {
		t.Fatalf("MessageLookup after same-store update = %#v", lookup)
	}
}

func TestMessageLookupCacheIsLazyBeforeFirstLookup(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, created, err := store.PersistInbound(ctx, InboundEvent{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Source:         "teams",
	}); err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	} else if !created {
		t.Fatal("PersistInbound created = false")
	}
	store.mu.Lock()
	valid := store.messageLookup.Valid
	store.mu.Unlock()
	if valid {
		t.Fatal("message lookup cache was built before the first MessageLookup")
	}
}

func TestMessageLookupCacheTracksExternalStoreUpdates(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	lookup, err := store.MessageLookup(ctx, "chat-1", "message-1")
	if err != nil {
		t.Fatalf("warm MessageLookup error: %v", err)
	}
	if lookup.HasDeliveredOutbox {
		t.Fatalf("warm MessageLookup = %#v", lookup)
	}
	other, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open second store: %v", err)
	}
	if _, err := other.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Origin:         MessageOriginHelperOutbox,
		OutboxID:       "outbox-1",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance through second store: %v", err)
	}
	lookup, err = store.MessageLookup(ctx, "chat-1", "message-1")
	if err != nil {
		t.Fatalf("MessageLookup after external update error: %v", err)
	}
	if !lookup.HasDeliveredOutbox || !lookup.HasProvenance || lookup.Provenance.Origin != MessageOriginHelperOutbox || lookup.HasInbound {
		t.Fatalf("MessageLookup after external update = %#v", lookup)
	}
}

func TestMessageLookupCacheRefreshesOutboxBackfillAfterSameStoreUpdate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	lookup, err := store.MessageLookup(ctx, "chat-1", "teams-outbox-1")
	if err != nil {
		t.Fatalf("warm MessageLookup error: %v", err)
	}
	if lookup.HasProvenance || lookup.HasDeliveredOutbox || lookup.HasInbound {
		t.Fatalf("warm MessageLookup = %#v", lookup)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox-1"] = OutboxMessage{
			ID:             "outbox-1",
			TeamsChatID:    "chat-1",
			TeamsMessageID: "teams-outbox-1",
			Kind:           "answer",
			Status:         OutboxStatusSent,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
			SentAt:         time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("Update outbox error: %v", err)
	}
	lookup, err = store.MessageLookup(ctx, "chat-1", "teams-outbox-1")
	if err != nil {
		t.Fatalf("MessageLookup after outbox update error: %v", err)
	}
	if !lookup.HasProvenance || lookup.Provenance.Origin != MessageOriginHelperOutbox || !lookup.HasDeliveredOutbox || lookup.HasInbound {
		t.Fatalf("MessageLookup after outbox update = %#v", lookup)
	}
}

func TestMessageLookupCachePrefersCanonicalProvenanceForFlags(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	chatID := "chat-1"
	messageID := "message-1"
	canonicalID := messageProvenanceID(chatID, messageID)
	if err := store.Update(ctx, func(state *State) error {
		state.MessageProvenance["legacy-user-record"] = MessageProvenanceRecord{
			ID:             "legacy-user-record",
			TeamsChatID:    chatID,
			TeamsMessageID: messageID,
			Origin:         MessageOriginUserInbound,
			InboundID:      "legacy-inbound",
		}
		state.MessageProvenance[canonicalID] = MessageProvenanceRecord{
			ID:             canonicalID,
			TeamsChatID:    chatID,
			TeamsMessageID: messageID,
			Origin:         MessageOriginHelperOutbox,
			OutboxID:       "canonical-outbox",
		}
		return nil
	}); err != nil {
		t.Fatalf("Update provenance records error: %v", err)
	}
	lookup, err := store.MessageLookup(ctx, chatID, messageID)
	if err != nil {
		t.Fatalf("MessageLookup error: %v", err)
	}
	if !lookup.HasProvenance || lookup.Provenance.ID != canonicalID || lookup.Provenance.Origin != MessageOriginHelperOutbox {
		t.Fatalf("MessageLookup provenance = %#v", lookup)
	}
	if lookup.HasInbound || !lookup.HasDeliveredOutbox {
		t.Fatalf("MessageLookup flags = inbound:%v delivered:%v, want inbound:false delivered:true", lookup.HasInbound, lookup.HasDeliveredOutbox)
	}
	inbound, err := store.HasInboundMessage(ctx, chatID, messageID)
	if err != nil {
		t.Fatalf("HasInboundMessage error: %v", err)
	}
	if inbound {
		t.Fatal("HasInboundMessage used non-canonical user provenance")
	}
}

func TestMessageLookupCachePreservesUnknownProvenanceWithoutFlags(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	chatID := "chat-1"
	messageID := "message-unknown"
	if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    chatID,
		TeamsMessageID: messageID,
		Origin:         "unknown",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance error: %v", err)
	}
	lookup, err := store.MessageLookup(ctx, chatID, messageID)
	if err != nil {
		t.Fatalf("MessageLookup error: %v", err)
	}
	if !lookup.HasProvenance || lookup.Provenance.Origin != "unknown" || lookup.HasInbound || lookup.HasDeliveredOutbox {
		t.Fatalf("MessageLookup unknown provenance = %#v", lookup)
	}
}

func TestMessageLookupCacheInvalidatesSameSizeSameModTimeReplace(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	chatID := "chat-1"
	firstMessageID := "message-a"
	secondMessageID := "message-b"
	firstData := stateJSONWithProvenance(t, chatID, firstMessageID, MessageOriginHelperOutbox)
	secondData := stateJSONWithProvenance(t, chatID, secondMessageID, MessageOriginHelperOutbox)
	if len(firstData) != len(secondData) {
		t.Fatalf("test state sizes differ: %d != %d", len(firstData), len(secondData))
	}
	fixedTime := time.Unix(1_700_000_100, 987_654_321)
	if err := ensurePrivateDir(filepath.Dir(store.Path())); err != nil {
		t.Fatalf("ensure state dir: %v", err)
	}
	if err := os.WriteFile(store.Path(), firstData, fileMode); err != nil {
		t.Fatalf("write first state: %v", err)
	}
	if err := os.Chtimes(store.Path(), fixedTime, fixedTime); err != nil {
		t.Fatalf("chtimes first state: %v", err)
	}
	lookup, err := store.MessageLookup(ctx, chatID, firstMessageID)
	if err != nil {
		t.Fatalf("warm MessageLookup error: %v", err)
	}
	if !lookup.HasDeliveredOutbox {
		t.Fatalf("warm MessageLookup = %#v", lookup)
	}
	tmp, err := os.CreateTemp(filepath.Dir(store.Path()), ".state.json.tmp-*")
	if err != nil {
		t.Fatalf("create replacement: %v", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(secondData); err != nil {
		_ = tmp.Close()
		t.Fatalf("write replacement: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close replacement: %v", err)
	}
	if err := os.Chmod(tmpName, fileMode); err != nil {
		t.Fatalf("chmod replacement: %v", err)
	}
	if err := os.Chtimes(tmpName, fixedTime, fixedTime); err != nil {
		t.Fatalf("chtimes replacement: %v", err)
	}
	if err := durableReplaceFile(tmpName, store.Path()); err != nil {
		t.Fatalf("durableReplaceFile: %v", err)
	}
	if err := os.Chtimes(store.Path(), fixedTime, fixedTime); err != nil {
		t.Fatalf("chtimes replaced state: %v", err)
	}
	lookup, err = store.MessageLookup(ctx, chatID, firstMessageID)
	if err != nil {
		t.Fatalf("MessageLookup first after replace error: %v", err)
	}
	if lookup.HasProvenance || lookup.HasDeliveredOutbox || lookup.HasInbound {
		t.Fatalf("first message lookup after replace = %#v, want missing", lookup)
	}
	lookup, err = store.MessageLookup(ctx, chatID, secondMessageID)
	if err != nil {
		t.Fatalf("MessageLookup second after replace error: %v", err)
	}
	if !lookup.HasProvenance || lookup.Provenance.TeamsMessageID != secondMessageID || !lookup.HasDeliveredOutbox {
		t.Fatalf("second message lookup after replace = %#v", lookup)
	}
}

func TestLargeMessageLookupHotCacheDoesNotReloadState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLargeMessageLookupState(t, store, 14000, 9000, 512)
	queries := largeMessageLookupQueries()
	if _, err := store.MessageLookup(ctx, queries[0].chatID, queries[0].messageID); err != nil {
		t.Fatalf("warm MessageLookup error: %v", err)
	}
	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})
	for i := 0; i < 1000; i++ {
		query := queries[i%len(queries)]
		if _, err := store.MessageLookup(ctx, query.chatID, query.messageID); err != nil {
			t.Fatalf("hot MessageLookup %d error: %v", i, err)
		}
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("hot MessageLookup reloaded state %d times", got)
	}
}

func TestLargeMessageLookupHotCacheAllocationPressure(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedLargeMessageLookupState(t, store, 14000, 9000, 512)
	queries := largeMessageLookupQueries()
	if _, err := store.MessageLookup(ctx, queries[0].chatID, queries[0].messageID); err != nil {
		t.Fatalf("warm MessageLookup error: %v", err)
	}
	var lookupErr error
	allocsPerRun := testing.AllocsPerRun(1000, func() {
		for _, query := range queries {
			if _, err := store.MessageLookup(ctx, query.chatID, query.messageID); err != nil {
				lookupErr = err
				return
			}
		}
	})
	if lookupErr != nil {
		t.Fatalf("MessageLookup error: %v", lookupErr)
	}
	allocsPerLookup := allocsPerRun / float64(len(queries))
	if allocsPerLookup > 8 {
		t.Fatalf("hot MessageLookup allocations = %.2f per lookup, want <= 8", allocsPerLookup)
	}
}

func TestStateFileStampDetectsSameSizeSameModTimeReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	firstData := []byte("alpha\n")
	secondData := []byte("bravo\n")
	if len(firstData) != len(secondData) {
		t.Fatalf("test data size mismatch: %d != %d", len(firstData), len(secondData))
	}
	fixedTime := time.Unix(1_700_000_000, 123_456_789)
	if err := os.WriteFile(path, firstData, fileMode); err != nil {
		t.Fatalf("write first file: %v", err)
	}
	if err := os.Chtimes(path, fixedTime, fixedTime); err != nil {
		t.Fatalf("chtimes first file: %v", err)
	}
	firstStamp, err := stateFileStampForPath(path)
	if err != nil {
		t.Fatalf("first stateFileStampForPath: %v", err)
	}
	tmp, err := os.CreateTemp(dir, ".state.json.tmp-*")
	if err != nil {
		t.Fatalf("create temp replacement: %v", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(secondData); err != nil {
		_ = tmp.Close()
		t.Fatalf("write temp replacement: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp replacement: %v", err)
	}
	if err := os.Chmod(tmpName, fileMode); err != nil {
		t.Fatalf("chmod temp replacement: %v", err)
	}
	if err := os.Chtimes(tmpName, fixedTime, fixedTime); err != nil {
		t.Fatalf("chtimes temp replacement: %v", err)
	}
	if err := durableReplaceFile(tmpName, path); err != nil {
		t.Fatalf("durableReplaceFile: %v", err)
	}
	if err := os.Chtimes(path, fixedTime, fixedTime); err != nil {
		t.Fatalf("chtimes replaced file: %v", err)
	}
	secondStamp, err := stateFileStampForPath(path)
	if err != nil {
		t.Fatalf("second stateFileStampForPath: %v", err)
	}
	if firstStamp.Size != secondStamp.Size {
		t.Fatalf("replacement size = %d, want %d", secondStamp.Size, firstStamp.Size)
	}
	if !firstStamp.ModTime.Equal(secondStamp.ModTime) {
		t.Fatalf("replacement modtime = %s, want %s", secondStamp.ModTime, firstStamp.ModTime)
	}
	if firstStamp.equal(secondStamp) {
		t.Fatal("state file stamp treated same-size same-mtime replacement as unchanged")
	}
}

func largeMessageLookupQueries() []struct {
	chatID    string
	messageID string
} {
	return []struct {
		chatID    string
		messageID string
	}{
		{chatID: largeInboundChatID(13999), messageID: largeInboundMessageID(13999)},
		{chatID: "provenance-helper-chat", messageID: largeHelperProvenanceMessageID(8999)},
		{chatID: "provenance-user-chat", messageID: largeUserProvenanceMessageID(8998)},
		{chatID: "outbox-chat", messageID: largeOutboxMessageID(511)},
		{chatID: "outbox-chat", messageID: largeOutboxMessageID(510)},
		{chatID: "missing-chat", messageID: "missing-message"},
	}
}

func stateJSONWithProvenance(t *testing.T, chatID string, messageID string, origin string) []byte {
	t.Helper()
	state := newState()
	fixed := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)
	state.CreatedAt = fixed
	state.UpdatedAt = fixed
	recordMessageProvenanceLocked(&state, MessageProvenanceRecord{
		TeamsChatID:    chatID,
		TeamsMessageID: messageID,
		Origin:         origin,
		OutboxID:       "outbox-1",
	}, time.Time{})
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent state: %v", err)
	}
	return append(data, '\n')
}

func BenchmarkLargeMessageLookup(b *testing.B) {
	store := newBenchmarkStore(b)
	ctx := context.Background()
	seedLargeMessageLookupState(b, store, 14000, 9000, 512)
	if _, err := store.MessageLookup(ctx, largeInboundChatID(13999), largeInboundMessageID(13999)); err != nil {
		b.Fatalf("warm MessageLookup error: %v", err)
	}
	queries := largeMessageLookupQueries()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		if _, err := store.MessageLookup(ctx, query.chatID, query.messageID); err != nil {
			b.Fatalf("MessageLookup error: %v", err)
		}
	}
}

func BenchmarkLargeMessageLookupCacheRefresh(b *testing.B) {
	store := newBenchmarkStore(b)
	ctx := context.Background()
	seedLargeMessageLookupState(b, store, 14000, 9000, 512)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.mu.Lock()
		store.invalidateMessageLookupCacheLocked()
		store.mu.Unlock()
		if _, err := store.MessageLookup(ctx, largeInboundChatID(13999), largeInboundMessageID(13999)); err != nil {
			b.Fatalf("MessageLookup error: %v", err)
		}
	}
}

func BenchmarkHighChurnNoopStoreUpdates(b *testing.B) {
	store := newBenchmarkStore(b)
	ctx := context.Background()
	seedLargeMessageLookupState(b, store, 4000, 3000, 256)
	chatCount := 256
	base := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	successAt := time.Now().Add(24 * time.Hour)
	nextPollAt := successAt.Add(time.Minute)
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < chatCount; i++ {
			chatID := fmt.Sprintf("bench-chat-%03d", i)
			state.ChatPolls[chatID] = ChatPollState{
				ChatID:               chatID,
				Seeded:               true,
				PollState:            "warm",
				NextPollAt:           nextPollAt,
				LastModifiedCursor:   base,
				LastSuccessfulPollAt: successAt,
				UpdatedAt:            successAt,
			}
			recordMessageProvenanceLocked(state, MessageProvenanceRecord{
				TeamsChatID:    chatID,
				TeamsMessageID: fmt.Sprintf("bench-message-%03d", i),
				Origin:         MessageOriginHelperOutbox,
				OutboxID:       fmt.Sprintf("bench-outbox-%03d", i),
				CreatedAt:      base,
				UpdatedAt:      base,
			}, time.Time{})
		}
		return nil
	}); err != nil {
		b.Fatalf("seed high-churn no-op state: %v", err)
	}
	records := make([]MessageProvenanceRecord, 0, chatCount)
	for i := 0; i < chatCount; i++ {
		records = append(records, MessageProvenanceRecord{
			TeamsChatID:    fmt.Sprintf("bench-chat-%03d", i),
			TeamsMessageID: fmt.Sprintf("bench-message-%03d", i),
			Origin:         MessageOriginHelperOutbox,
			OutboxID:       fmt.Sprintf("bench-outbox-%03d", i),
			CreatedAt:      base,
			UpdatedAt:      base,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := records[i%len(records)]
		if _, err := store.RecordChatPollSuccess(ctx, record.TeamsChatID, base, true, false, 0); err != nil {
			b.Fatalf("RecordChatPollSuccess error: %v", err)
		}
		if _, err := store.RecordMessageProvenance(ctx, record); err != nil {
			b.Fatalf("RecordMessageProvenance error: %v", err)
		}
		if _, err := store.RecordChatPollSuccessWithContinuationAndSchedule(ctx, record.TeamsChatID, base, true, false, 0, "", func(ChatPollState) (ChatPollScheduleUpdate, error) {
			return ChatPollScheduleUpdate{
				ChatID:     record.TeamsChatID,
				PollState:  "warm",
				NextPollAt: nextPollAt,
			}, nil
		}); err != nil {
			b.Fatalf("RecordChatPollSuccessWithContinuationAndSchedule error: %v", err)
		}
	}
}

func TestMessageProvenanceDoesNotDowngradeHelperOutboxToInbound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginHelperOutbox,
		OutboxID:       "outbox-1",
		Kind:           "codex-progress-001",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance helper error: %v", err)
	}
	if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginUserInbound,
		InboundID:      "inbound-1",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance inbound error: %v", err)
	}
	got, ok, err := store.MessageProvenance(ctx, "chat-1", "teams-helper-1")
	if err != nil {
		t.Fatalf("MessageProvenance error: %v", err)
	}
	if !ok || got.Origin != MessageOriginHelperOutbox || got.OutboxID != "outbox-1" || got.InboundID != "" {
		t.Fatalf("provenance was downgraded to inbound: %#v ok=%v", got, ok)
	}
	if !strings.Contains(got.Diagnostic, "ignored user_inbound") {
		t.Fatalf("diagnostic = %q, want ignored user_inbound", got.Diagnostic)
	}
}

func TestMessageProvenanceAllowsHelperOutboxToReplaceEarlyInbound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginUserInbound,
		InboundID:      "inbound-1",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance inbound error: %v", err)
	}
	if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginHelperOutbox,
		OutboxID:       "outbox-1",
		Kind:           "codex-progress-001",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance helper error: %v", err)
	}
	got, ok, err := store.MessageProvenance(ctx, "chat-1", "teams-helper-1")
	if err != nil {
		t.Fatalf("MessageProvenance error: %v", err)
	}
	if !ok || got.Origin != MessageOriginHelperOutbox || got.OutboxID != "outbox-1" {
		t.Fatalf("helper provenance did not replace inbound: %#v ok=%v", got, ok)
	}
	if !strings.Contains(got.Diagnostic, "replaced user_inbound") {
		t.Fatalf("diagnostic = %q, want replaced user_inbound", got.Diagnostic)
	}
}

func TestPersistInboundRejectsHelperOutboxMessageID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginHelperOutbox,
		OutboxID:       "outbox-1",
		Kind:           "codex-status-001",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance helper error: %v", err)
	}
	if _, created, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Text:           "should not become a prompt",
		Status:         InboundStatusQueued,
	}); !errors.Is(err, ErrInboundMessageFromHelperOutbox) {
		t.Fatalf("PersistInbound error = %v, want ErrInboundMessageFromHelperOutbox", err)
	} else if created {
		t.Fatal("PersistInbound created helper outbox as inbound")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 0 {
		t.Fatalf("inbound events = %d, want none: %#v", got, state.InboundEvents)
	}
	record, ok, err := store.MessageProvenance(ctx, "chat-1", "teams-helper-1")
	if err != nil {
		t.Fatalf("MessageProvenance error: %v", err)
	}
	if !ok || record.Origin != MessageOriginHelperOutbox || record.InboundID != "" {
		t.Fatalf("provenance = %#v ok=%v, want helper outbox only", record, ok)
	}
}

func TestPersistInboundRejectsDeliveredOutboxWithoutProvenance(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:             "outbox-1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Kind:           "helper",
		Body:           "sent before provenance write",
		Status:         OutboxStatusSent,
	}); err != nil {
		t.Fatalf("QueueOutbox helper error: %v", err)
	}
	if _, created, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Text:           "should not become a prompt",
		Status:         InboundStatusQueued,
	}); !errors.Is(err, ErrInboundMessageFromHelperOutbox) {
		t.Fatalf("PersistInbound error = %v, want ErrInboundMessageFromHelperOutbox", err)
	} else if created {
		t.Fatal("PersistInbound created delivered outbox as inbound")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 0 {
		t.Fatalf("inbound events = %d, want none: %#v", got, state.InboundEvents)
	}
}

func TestHelperOutboxProvenanceSuppressesEarlyQueuedInbound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	inbound, created, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Text:           "should be suppressed",
		Status:         InboundStatusQueued,
	})
	if err != nil {
		t.Fatalf("PersistInbound early error: %v", err)
	}
	if !created {
		t.Fatal("early inbound created = false")
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if !created {
		t.Fatal("QueueTurn created = false")
	}
	if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginHelperOutbox,
		OutboxID:       "outbox-1",
		Kind:           "codex-status-001",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance helper error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.InboundEvents[inbound.ID].Status; got != InboundStatusIgnored {
		t.Fatalf("inbound status = %q, want ignored", got)
	}
	if got := state.Turns[turn.ID].Status; got != TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if reason := state.Turns[turn.ID].RecoveryReason; !strings.Contains(reason, "helper_outbox provenance") {
		t.Fatalf("turn recovery reason = %q, want helper provenance reason", reason)
	}
	record, ok, err := store.MessageProvenance(ctx, "chat-1", "teams-helper-1")
	if err != nil {
		t.Fatalf("MessageProvenance error: %v", err)
	}
	if !ok || record.Origin != MessageOriginHelperOutbox || record.InboundID != "" || record.OutboxID != "outbox-1" {
		t.Fatalf("provenance = %#v ok=%v, want helper outbox replacement", record, ok)
	}
}

func TestSQLiteHelperOutboxProvenanceSuppressesEarlyQueuedInbound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	inbound, created, err := store.PersistInbound(ctx, InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Text:           "should be suppressed",
		Status:         InboundStatusQueued,
	})
	if err != nil {
		t.Fatalf("PersistInbound early error: %v", err)
	}
	if !created {
		t.Fatal("early inbound created = false")
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if !created {
		t.Fatal("QueueTurn created = false")
	}
	migrateStoreToSQLiteForTest(t, store)
	if _, err := store.RecordMessageProvenance(ctx, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginHelperOutbox,
		OutboxID:       "outbox-1",
		Kind:           "codex-status-001",
	}); err != nil {
		t.Fatalf("RecordMessageProvenance helper error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.InboundEvents[inbound.ID].Status; got != InboundStatusIgnored {
		t.Fatalf("inbound status = %q, want ignored", got)
	}
	if got := state.Turns[turn.ID].Status; got != TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if reason := state.Turns[turn.ID].RecoveryReason; !strings.Contains(reason, "helper_outbox provenance") {
		t.Fatalf("turn recovery reason = %q, want helper provenance reason", reason)
	}
	record, ok, err := store.MessageProvenance(ctx, "chat-1", "teams-helper-1")
	if err != nil {
		t.Fatalf("MessageProvenance error: %v", err)
	}
	if !ok || record.Origin != MessageOriginHelperOutbox || record.InboundID != "" || record.OutboxID != "outbox-1" {
		t.Fatalf("provenance = %#v ok=%v, want helper outbox replacement", record, ok)
	}
}

func TestBackfillMessageProvenancePreservesCurrentRecordsAndRefreshesChangedFields(t *testing.T) {
	state := newState()
	created := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	updated := created.Add(time.Minute)
	inboundID := inboundID("chat-1", "message-1")
	state.InboundEvents[inboundID] = InboundEvent{
		ID:             inboundID,
		SessionID:      "s1",
		TurnID:         "turn-1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Status:         InboundStatusQueued,
		TextHash:       "hash-1",
		CreatedAt:      created,
		UpdatedAt:      created,
	}

	backfillMessageProvenance(&state)
	provenanceID := messageProvenanceID("chat-1", "message-1")
	first := state.MessageProvenance[provenanceID]
	backfillMessageProvenance(&state)
	if got := state.MessageProvenance[provenanceID]; !reflect.DeepEqual(got, first) {
		t.Fatalf("stable backfill changed provenance:\nbefore=%#v\nafter=%#v", first, got)
	}

	inbound := state.InboundEvents[inboundID]
	inbound.UpdatedAt = updated
	inbound.TextHash = "hash-2"
	state.InboundEvents[inboundID] = inbound
	backfillMessageProvenance(&state)
	refreshed := state.MessageProvenance[provenanceID]
	if !refreshed.UpdatedAt.Equal(updated) || refreshed.RenderedHash != "hash-2" {
		t.Fatalf("changed inbound did not refresh provenance: %#v", refreshed)
	}
}

func TestBackfillDerivedRecordsIsStableForDuplicateCandidates(t *testing.T) {
	state := newState()
	now := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	for _, item := range []struct {
		id     string
		status InboundStatus
		hash   string
	}{
		{id: "inbound:b", status: InboundStatusPersisted, hash: "hash-b"},
		{id: "inbound:a", status: InboundStatusQueued, hash: "hash-a"},
	} {
		state.InboundEvents[item.id] = InboundEvent{
			ID:             item.id,
			SessionID:      "s1",
			TurnID:         "turn-1",
			TeamsChatID:    "chat-1",
			TeamsMessageID: "message-1",
			Status:         item.status,
			TextHash:       item.hash,
			CreatedAt:      now,
			UpdatedAt:      now.Add(time.Duration(len(item.id)) * time.Second),
		}
	}
	state.Sessions["s1"] = SessionContext{ID: "s1", TeamsChatID: "chat-1", CodexThreadID: "thread-1"}
	for _, item := range []struct {
		id   string
		kind string
		at   time.Time
	}{
		{id: "prov:b", kind: "sync-status-b", at: now.Add(2 * time.Second)},
		{id: "prov:a", kind: "sync-status-a", at: now.Add(time.Second)},
	} {
		state.MessageProvenance[item.id] = MessageProvenanceRecord{
			ID:             item.id,
			TeamsChatID:    "chat-1",
			TeamsMessageID: "helper-message-" + item.id,
			Origin:         MessageOriginHelperOutbox,
			SessionID:      "s1",
			Kind:           item.kind,
			RenderedHash:   "same-visible-hash",
			CreatedAt:      item.at,
			UpdatedAt:      item.at,
		}
	}

	backfillMessageProvenance(&state)
	backfillHelperDeliveries(&state)
	firstProvenance := state.MessageProvenance[messageProvenanceID("chat-1", "message-1")]
	firstHelpers, err := json.Marshal(state.HelperDeliveries)
	if err != nil {
		t.Fatalf("marshal helper deliveries: %v", err)
	}
	for i := 0; i < 10; i++ {
		backfillMessageProvenance(&state)
		backfillHelperDeliveries(&state)
		if got := state.MessageProvenance[messageProvenanceID("chat-1", "message-1")]; !reflect.DeepEqual(got, firstProvenance) {
			t.Fatalf("duplicate provenance backfill changed on iteration %d:\nbefore=%#v\nafter=%#v", i, firstProvenance, got)
		}
		gotHelpers, err := json.Marshal(state.HelperDeliveries)
		if err != nil {
			t.Fatalf("marshal helper deliveries after iteration %d: %v", i, err)
		}
		if string(gotHelpers) != string(firstHelpers) {
			t.Fatalf("duplicate helper backfill changed on iteration %d", i)
		}
	}
}

func TestBackfillMessageProvenanceSuppressesHelperReplacementWithDirectIDs(t *testing.T) {
	state := newState()
	now := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	inboundID := inboundID("chat-1", "message-1")
	state.InboundEvents[inboundID] = InboundEvent{
		ID:             inboundID,
		SessionID:      "s1",
		TurnID:         "turn-1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Status:         InboundStatusQueued,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	state.Turns["turn-1"] = Turn{
		ID:             "turn-1",
		SessionID:      "s1",
		InboundEventID: inboundID,
		Status:         TurnStatusQueued,
		CreatedAt:      now,
	}

	backfillMessageProvenance(&state)
	state.OutboxMessages["outbox-1"] = OutboxMessage{
		ID:             "outbox-1",
		SessionID:      "s1",
		TurnID:         "turn-1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Kind:           "codex-status-001",
		Status:         OutboxStatusSent,
		CreatedAt:      now,
		UpdatedAt:      now,
		SentAt:         now,
	}
	backfillMessageProvenance(&state)

	if got := state.InboundEvents[inboundID].Status; got != InboundStatusIgnored {
		t.Fatalf("inbound status = %q, want ignored", got)
	}
	if got := state.Turns["turn-1"].Status; got != TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	record := state.MessageProvenance[messageProvenanceID("chat-1", "message-1")]
	if record.Origin != MessageOriginHelperOutbox || record.OutboxID != "outbox-1" || !strings.Contains(record.Diagnostic, "replaced user_inbound") {
		t.Fatalf("replacement provenance = %#v", record)
	}
}

func TestSuppressInboundExecutionForHelperOutboxFallsBackForLegacyInboundID(t *testing.T) {
	state := newState()
	now := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	state.InboundEvents["legacy-inbound-1"] = InboundEvent{
		ID:             "legacy-inbound-1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Status:         InboundStatusQueued,
		CreatedAt:      now,
	}
	state.Turns["turn-legacy"] = Turn{
		ID:             "turn-legacy",
		InboundEventID: "legacy-inbound-1",
		Status:         TurnStatusQueued,
		CreatedAt:      now,
	}

	suppressInboundExecutionForHelperOutboxLocked(&state, MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		InboundID:      "stale-inbound-id",
	}, now.Add(time.Minute))

	if got := state.InboundEvents["legacy-inbound-1"].Status; got != InboundStatusIgnored {
		t.Fatalf("legacy inbound status = %q, want ignored", got)
	}
	if got := state.Turns["turn-legacy"].Status; got != TurnStatusInterrupted {
		t.Fatalf("legacy turn status = %q, want interrupted", got)
	}
}

func TestRecoverInterruptsAmbiguousTurns(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	running, _, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn running error: %v", err)
	}
	if _, err := store.MarkTurnRunning(ctx, running.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnRunning error: %v", err)
	}
	queued, _, err := store.QueueTurn(ctx, Turn{ID: "turn:manual", SessionID: "s1"})
	if err != nil {
		t.Fatalf("QueueTurn queued error: %v", err)
	}

	report, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover error: %v", err)
	}
	if got := len(report.InterruptedTurnIDs); got != 2 {
		t.Fatalf("interrupted turn count = %d, want 2 (%v)", got, report.InterruptedTurnIDs)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	for _, id := range []string{running.ID, queued.ID} {
		turn := state.Turns[id]
		if turn.Status != TurnStatusInterrupted {
			t.Fatalf("turn %s status = %q, want %q", id, turn.Status, TurnStatusInterrupted)
		}
		if turn.RecoveryReason == "" {
			t.Fatalf("turn %s recovery reason is empty", id)
		}
	}
	if got := state.InboundEvents[inbound.ID].Status; got != InboundStatusIgnored {
		t.Fatalf("interrupted turn inbound status = %q, want %q", got, InboundStatusIgnored)
	}
}

func TestClaimNextQueuedTurnSerializesPerSession(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	first, _, err := store.QueueTurn(ctx, Turn{ID: "turn:first", SessionID: "s1", QueuedAt: time.Date(2026, 5, 3, 1, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("QueueTurn first error: %v", err)
	}
	if _, _, err := store.QueueTurn(ctx, Turn{ID: "turn:second", SessionID: "s1", QueuedAt: time.Date(2026, 5, 3, 1, 1, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("QueueTurn second error: %v", err)
	}
	claimed, ok, err := store.ClaimNextQueuedTurn(ctx, "s1")
	if err != nil {
		t.Fatalf("ClaimNextQueuedTurn error: %v", err)
	}
	if !ok || claimed.ID != first.ID || claimed.Status != TurnStatusRunning || claimed.StartedAt.IsZero() {
		t.Fatalf("claimed first queued turn mismatch: ok=%v claimed=%#v", ok, claimed)
	}
	if again, ok, err := store.ClaimNextQueuedTurn(ctx, "s1"); err != nil || ok {
		t.Fatalf("ClaimNextQueuedTurn while running = ok %v turn %#v err %v, want no claim", ok, again, err)
	}
	if _, err := store.MarkTurnCompleted(ctx, claimed.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnCompleted error: %v", err)
	}
	second, ok, err := store.ClaimNextQueuedTurn(ctx, "s1")
	if err != nil {
		t.Fatalf("ClaimNextQueuedTurn second error: %v", err)
	}
	if !ok || second.ID != "turn:second" || second.Status != TurnStatusRunning {
		t.Fatalf("claimed second queued turn mismatch: ok=%v second=%#v", ok, second)
	}
}

func TestInterruptedTurnCannotBeCompletedOrFailed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, Turn{ID: "turn:manual", SessionID: "s1"})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "forced recovery"); err != nil {
		t.Fatalf("MarkTurnInterrupted error: %v", err)
	}
	if _, err := store.MarkTurnCompleted(ctx, turn.ID, "thread-1", "codex-turn-1"); err == nil || !strings.Contains(err.Error(), "cannot be completed") {
		t.Fatalf("MarkTurnCompleted error = %v, want interrupted guard", err)
	}
	if _, err := store.MarkTurnFailed(ctx, turn.ID, "failed after recovery"); err == nil || !strings.Contains(err.Error(), "cannot be failed") {
		t.Fatalf("MarkTurnFailed error = %v, want interrupted guard", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
}

func TestMarkTurnFailedWithCodexIDsPersistsDiagnostics(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, Turn{ID: "turn:manual", SessionID: "s1"})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}

	failed, err := store.MarkTurnFailedWithCodexIDs(ctx, turn.ID, "model policy failed", "thread-1", "codex-turn-failed")
	if err != nil {
		t.Fatalf("MarkTurnFailedWithCodexIDs error: %v", err)
	}
	if failed.Status != TurnStatusFailed || failed.CodexThreadID != "thread-1" || failed.CodexTurnID != "codex-turn-failed" {
		t.Fatalf("failed turn mismatch: %#v", failed)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	session := state.Sessions["s1"]
	if session.CodexThreadID != "thread-1" || session.LatestCodexTurnID != "codex-turn-failed" || session.LatestTurnID != turn.ID {
		t.Fatalf("session codex diagnostics mismatch: %#v", session)
	}
}

func TestOutboxResendDoesNotCreateNewTurn(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if !created {
		t.Fatal("initial QueueTurn created = false")
	}
	outbox := OutboxMessage{
		SessionID:   "s1",
		TurnID:      turn.ID,
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done",
	}
	if _, created, err := store.QueueOutbox(ctx, outbox); err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	} else if !created {
		t.Fatal("initial QueueOutbox created = false")
	}

	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	duplicateInbound, created, err := reopened.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("duplicate PersistInbound error: %v", err)
	}
	if created {
		t.Fatal("duplicate inbound created = true")
	}
	duplicateTurn, created, err := reopened.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: duplicateInbound.ID})
	if err != nil {
		t.Fatalf("duplicate QueueTurn error: %v", err)
	}
	if created {
		t.Fatal("duplicate QueueTurn created = true")
	}
	if duplicateTurn.ID != turn.ID {
		t.Fatalf("duplicate turn ID = %q, want %q", duplicateTurn.ID, turn.ID)
	}
	if _, created, err := reopened.QueueOutbox(ctx, outbox); err != nil {
		t.Fatalf("duplicate QueueOutbox error: %v", err)
	} else if created {
		t.Fatal("duplicate QueueOutbox created = true")
	}
	state, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.Turns); got != 1 {
		t.Fatalf("turn count = %d, want 1", got)
	}
	if got := len(state.OutboxMessages); got != 1 {
		t.Fatalf("outbox count = %d, want 1", got)
	}
	pending, err := reopened.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if got := len(pending); got != 1 {
		t.Fatalf("pending outbox count = %d, want 1", got)
	}
}

func TestMarkOutboxSentIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:anchor",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "anchor",
		Body:        "ready",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	first, err := store.MarkOutboxSent(ctx, msg.ID, "teams-message-1")
	if err != nil {
		t.Fatalf("first MarkOutboxSent error: %v", err)
	}
	second, err := store.MarkOutboxSent(ctx, msg.ID, "teams-message-1")
	if err != nil {
		t.Fatalf("second MarkOutboxSent error: %v", err)
	}
	if first.ID != second.ID || second.Status != OutboxStatusSent || second.TeamsMessageID != "teams-message-1" {
		t.Fatalf("unexpected sent outbox after duplicate update: %#v", second)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.OutboxMessages); got != 1 {
		t.Fatalf("outbox count = %d, want 1", got)
	}
	record, ok, err := store.MessageProvenance(ctx, "chat-1", "teams-message-1")
	if err != nil {
		t.Fatalf("MessageProvenance error: %v", err)
	}
	if !ok || record.Origin != MessageOriginHelperOutbox || record.OutboxID != msg.ID {
		t.Fatalf("message provenance = %#v, ok=%v", record, ok)
	}
	if err := store.Update(ctx, func(state *State) error {
		delete(state.OutboxMessages, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("delete outbox from state: %v", err)
	}
	delivered, err := store.HasDeliveredOutboxMessage(ctx, "chat-1", "teams-message-1")
	if err != nil {
		t.Fatalf("HasDeliveredOutboxMessage error: %v", err)
	}
	if !delivered {
		t.Fatal("HasDeliveredOutboxMessage should use provenance after outbox pruning")
	}
}

func TestOutboxSendAttemptClaimsQueuedMessage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:claim",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	claimed, err := store.MarkOutboxSendAttempt(ctx, msg.ID)
	if err != nil {
		t.Fatalf("MarkOutboxSendAttempt error: %v", err)
	}
	if claimed.Status != OutboxStatusSending || claimed.LastSendAttempt.IsZero() {
		t.Fatalf("unexpected claimed outbox: %#v", claimed)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); !errors.Is(err, ErrOutboxSendNotClaimed) {
		t.Fatalf("second MarkOutboxSendAttempt error = %v, want ErrOutboxSendNotClaimed", err)
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("fresh sending outbox should not be pending: %#v", pending)
	}
}

func TestPendingOutboxIncludesStaleSendingMessage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:stale-send",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); err != nil {
		t.Fatalf("MarkOutboxSendAttempt error: %v", err)
	}
	if err := store.UpdateSession(ctx, "s1", func(state *State) error {
		stale := state.OutboxMessages[msg.ID]
		stale.LastSendAttempt = time.Now().Add(-outboxSendLease - time.Second)
		state.OutboxMessages[msg.ID] = stale
		return nil
	}); err != nil {
		t.Fatalf("make outbox send attempt stale: %v", err)
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != msg.ID {
		t.Fatalf("pending outbox = %#v, want stale sending message", pending)
	}
}

func TestSavePrunesOldSentOutboxMessages(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	err := store.Update(ctx, func(state *State) error {
		for i := 0; i < maxRetainedSentOutboxMessages+20; i++ {
			id := fmt.Sprintf("outbox:sent:%03d", i)
			state.OutboxMessages[id] = OutboxMessage{
				ID:          id,
				TeamsChatID: "chat-1",
				Status:      OutboxStatusSent,
				Kind:        "helper",
				Body:        "sent",
				SentAt:      now.Add(time.Duration(i) * time.Second),
				CreatedAt:   now.Add(time.Duration(i) * time.Second),
			}
		}
		state.OutboxMessages["outbox:queued"] = OutboxMessage{
			ID:          "outbox:queued",
			TeamsChatID: "chat-1",
			Status:      OutboxStatusQueued,
			Kind:        "helper",
			Body:        "queued",
			CreatedAt:   now,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.OutboxMessages); got != maxRetainedSentOutboxMessages {
		t.Fatalf("outbox len = %d, want %d", got, maxRetainedSentOutboxMessages)
	}
	if _, ok := state.OutboxMessages["outbox:queued"]; !ok {
		t.Fatal("queued outbox was pruned")
	}
	if _, ok := state.OutboxMessages["outbox:sent:000"]; ok {
		t.Fatal("oldest sent outbox was not pruned")
	}
	if _, ok := state.OutboxMessages[fmt.Sprintf("outbox:sent:%03d", maxRetainedSentOutboxMessages+19)]; !ok {
		t.Fatal("newest sent outbox was pruned")
	}
}

func TestSavePrunesOldTranscriptLedgerRecords(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	err := store.Update(ctx, func(state *State) error {
		for i := 0; i < maxRetainedTranscriptLedgerRecords+20; i++ {
			id := fmt.Sprintf("ledger:s1:%04d", i)
			state.TranscriptLedger[id] = TranscriptLedgerRecord{
				ID:             id,
				SessionID:      "s1",
				SourceRecordID: fmt.Sprintf("record-%04d", i),
				UpdatedAt:      now.Add(time.Duration(i) * time.Second),
				CreatedAt:      now.Add(time.Duration(i) * time.Second),
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.TranscriptLedger); got != maxRetainedTranscriptLedgerRecords {
		t.Fatalf("ledger len = %d, want %d", got, maxRetainedTranscriptLedgerRecords)
	}
	if _, ok := state.TranscriptLedger["ledger:s1:0000"]; ok {
		t.Fatal("oldest transcript ledger record was not pruned")
	}
	if _, ok := state.TranscriptLedger[fmt.Sprintf("ledger:s1:%04d", maxRetainedTranscriptLedgerRecords+19)]; !ok {
		t.Fatal("newest transcript ledger record was pruned")
	}
}

func TestTeamsBackgroundKeepaliveClockSkewOutboxAndRateLimitCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:future-send", TeamsChatID: "chat-1", Kind: "helper", Body: "sending"}); err != nil {
		t.Fatalf("QueueOutbox future-send error: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, "outbox:future-send"); err != nil {
		t.Fatalf("MarkOutboxSendAttempt error: %v", err)
	}
	if err := store.UpdateSession(ctx, "clock skew", func(state *State) error {
		msg := state.OutboxMessages["outbox:future-send"]
		msg.LastSendAttempt = now.Add(30 * time.Second)
		state.OutboxMessages[msg.ID] = msg
		return nil
	}); err != nil {
		t.Fatalf("set future send attempt: %v", err)
	}
	pending, err := store.PendingOutboxAt(ctx, now)
	if err != nil {
		t.Fatalf("PendingOutboxAt before future attempt error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("future send attempt should remain leased after backward clock skew: %#v", pending)
	}
	pending, err = store.PendingOutboxAt(ctx, now.Add(30*time.Second+outboxSendLease+time.Nanosecond))
	if err != nil {
		t.Fatalf("PendingOutboxAt after future lease expiry error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "outbox:future-send" {
		t.Fatalf("future send attempt should become pending after lease expiry: %#v", pending)
	}

	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{ID: "outbox:rate-limited", TeamsChatID: "chat-2", Kind: "helper", Body: "blocked"}); err != nil {
		t.Fatalf("QueueOutbox rate-limited error: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-2", now.Add(10*time.Minute), "429 after sleep"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	pending, err = store.PendingOutboxAt(ctx, now.Add(9*time.Minute))
	if err != nil {
		t.Fatalf("PendingOutboxAt before rate limit expiry error: %v", err)
	}
	for _, msg := range pending {
		if msg.ID == "outbox:rate-limited" {
			t.Fatalf("rate-limited outbox should not be pending before Retry-After expires: %#v", pending)
		}
	}
	pending, err = store.PendingOutboxAt(ctx, now.Add(10*time.Minute+time.Nanosecond))
	if err != nil {
		t.Fatalf("PendingOutboxAt after rate limit expiry error: %v", err)
	}
	found := false
	for _, msg := range pending {
		if msg.ID == "outbox:rate-limited" {
			found = true
		}
	}
	if !found {
		t.Fatalf("rate-limited outbox should return after Retry-After expires: %#v", pending)
	}
}

func TestTeamsBackgroundKeepaliveLogoutResumePreservesQueuedOutboxCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	startedAt := testOwnerStart()
	heartbeatAt := startedAt.Add(10 * time.Second)

	staleOwner := testOwner("session-old", "turn-old", startedAt)
	if _, err := store.RecordOwnerHeartbeat(ctx, staleOwner, time.Minute, heartbeatAt); err != nil {
		t.Fatalf("stale RecordOwnerHeartbeat error: %v", err)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:queued-during-logout",
		SessionID:   "session-old",
		TurnID:      "turn-old",
		TeamsChatID: "chat-1",
		Kind:        "answer",
		Body:        "queued before helper takeover",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); err != nil {
		t.Fatalf("MarkOutboxSendAttempt error: %v", err)
	}

	blockedUntil := heartbeatAt.Add(4 * time.Minute)
	if _, err := store.SetChatRateLimit(ctx, "chat-1", blockedUntil, "429 while old helper was disconnecting"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}
	if err := store.UpdateSession(ctx, "model logout-equivalent send lease", func(state *State) error {
		queued := state.OutboxMessages[msg.ID]
		queued.LastSendAttempt = heartbeatAt.Add(15 * time.Second)
		state.OutboxMessages[msg.ID] = queued
		return nil
	}); err != nil {
		t.Fatalf("rewrite send attempt time: %v", err)
	}

	recoveryAt := heartbeatAt.Add(2 * time.Minute)
	nextOwner := testOwner("session-new", "turn-new", recoveryAt)
	nextOwner.PID = 5252
	recoveredOwner, recovered, err := store.RecoverStaleOwner(ctx, nextOwner, time.Minute, recoveryAt)
	if err != nil {
		t.Fatalf("RecoverStaleOwner error: %v", err)
	}
	if !recovered {
		t.Fatal("RecoverStaleOwner recovered = false")
	}
	if recoveredOwner.ActiveSessionID != "session-new" || recoveredOwner.ActiveTurnID != "turn-new" {
		t.Fatalf("recovered owner active fields = %q/%q, want new turn", recoveredOwner.ActiveSessionID, recoveredOwner.ActiveTurnID)
	}

	pending, err := store.PendingOutboxAt(ctx, blockedUntil.Add(-time.Nanosecond))
	if err != nil {
		t.Fatalf("PendingOutboxAt before Retry-After error: %v", err)
	}
	for _, candidate := range pending {
		if candidate.ID == msg.ID {
			t.Fatalf("outbox should stay suppressed until Retry-After even after helper takeover: %#v", pending)
		}
	}

	pending, err = store.PendingOutboxAt(ctx, blockedUntil.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("PendingOutboxAt after Retry-After error: %v", err)
	}
	var found *OutboxMessage
	for i := range pending {
		if pending[i].ID == msg.ID {
			found = &pending[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("outbox queued by stale owner should still be sendable after takeover and Retry-After: %#v", pending)
	}
	if found.Body != "queued before helper takeover" || found.SessionID != "session-old" || found.TurnID != "turn-old" {
		t.Fatalf("queued outbox mutated during recovery: %#v", *found)
	}

	duplicate, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          msg.ID,
		SessionID:   "session-old",
		TurnID:      "turn-old",
		TeamsChatID: "chat-1",
		Kind:        "answer",
		Body:        "duplicate after takeover should not replace original",
	})
	if err != nil {
		t.Fatalf("duplicate QueueOutbox error: %v", err)
	}
	if created {
		t.Fatal("duplicate QueueOutbox created = true after takeover")
	}
	if duplicate.Body != "queued before helper takeover" {
		t.Fatalf("duplicate QueueOutbox replaced original body: %#v", duplicate)
	}

	later, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:later-after-takeover",
		SessionID:   "session-new",
		TurnID:      "turn-new",
		TeamsChatID: "chat-1",
		Kind:        "answer",
		Body:        "later answer after helper takeover",
	})
	if err != nil {
		t.Fatalf("later QueueOutbox error: %v", err)
	}
	earlier, ok, err := store.EarlierUnsentOutbox(ctx, later)
	if err != nil {
		t.Fatalf("EarlierUnsentOutbox error: %v", err)
	}
	if !ok || earlier.ID != msg.ID {
		t.Fatalf("later outbox should be blocked behind unsent pre-takeover outbox; earlier=%#v ok=%v", earlier, ok)
	}
}

func TestChatPollStateTracksCursorAndErrors(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	cursor := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	poll, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, true, 50)
	if err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	if !poll.Seeded || !poll.LastModifiedCursor.Equal(cursor) || poll.LastSuccessfulPollAt.IsZero() {
		t.Fatalf("unexpected poll success state: %#v", poll)
	}
	if poll.LastWindowFullAt.IsZero() || !strings.Contains(poll.LastWindowFullMessage, "full message window") {
		t.Fatalf("expected window diagnostic, got %#v", poll)
	}
	poll, err = store.RecordChatPollSuccessWithContinuation(ctx, "chat-1", cursor, true, true, 50, "/chats/chat-1/messages?$skiptoken=next")
	if err != nil {
		t.Fatalf("RecordChatPollSuccessWithContinuation error: %v", err)
	}
	if poll.ContinuationPath != "/chats/chat-1/messages?$skiptoken=next" {
		t.Fatalf("continuation path = %q", poll.ContinuationPath)
	}

	if err := store.RecordChatPollError(ctx, "chat-1", strings.Repeat("x", 300)); err != nil {
		t.Fatalf("RecordChatPollError error: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil {
		t.Fatalf("ChatPoll error: %v", err)
	}
	if !ok {
		t.Fatal("expected chat poll state")
	}
	if len(poll.LastError) != 240 || poll.LastErrorAt.IsZero() {
		t.Fatalf("unexpected poll error state: %#v", poll)
	}
	if poll.FailureCount != 1 {
		t.Fatalf("failure count = %d, want 1", poll.FailureCount)
	}

	later := cursor.Add(time.Minute)
	poll, err = store.RecordChatPollSuccess(ctx, "chat-1", later, false, false, 1)
	if err != nil {
		t.Fatalf("second RecordChatPollSuccess error: %v", err)
	}
	if !poll.Seeded || !poll.LastModifiedCursor.Equal(later) || poll.LastError != "" || poll.LastWindowFullMessage != "" || poll.ContinuationPath != "" || poll.FailureCount != 0 {
		t.Fatalf("unexpected recovered poll state: %#v", poll)
	}
}

func TestChatPollSuccessTimestampOnlyNoopDoesNotRewriteState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	cursor := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, false, 0); err != nil {
		t.Fatalf("initial RecordChatPollSuccess error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before timestamp-only success: %v", err)
	}
	pollBefore, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll before timestamp-only success ok=%v err=%v", ok, err)
	}
	pollAfter, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, false, 0)
	if err != nil {
		t.Fatalf("timestamp-only RecordChatPollSuccess error: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after timestamp-only success: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("timestamp-only RecordChatPollSuccess rewrote state")
	}
	if !pollAfter.LastSuccessfulPollAt.Equal(pollBefore.LastSuccessfulPollAt) {
		t.Fatalf("timestamp-only success changed LastSuccessfulPollAt: before=%s after=%s", pollBefore.LastSuccessfulPollAt, pollAfter.LastSuccessfulPollAt)
	}

	if err := store.RecordChatPollError(ctx, "chat-1", "temporary graph error"); err != nil {
		t.Fatalf("RecordChatPollError error: %v", err)
	}
	beforeRecovery, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before recovery success: %v", err)
	}
	recovered, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, false, 0)
	if err != nil {
		t.Fatalf("recovery RecordChatPollSuccess error: %v", err)
	}
	afterRecovery, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after recovery success: %v", err)
	}
	if bytes.Equal(beforeRecovery, afterRecovery) {
		t.Fatal("RecordChatPollSuccess did not rewrite while clearing poll error")
	}
	if recovered.LastError != "" || recovered.FailureCount != 0 {
		t.Fatalf("recovery success did not clear error state: %#v", recovered)
	}
}

func TestChatPollSuccessHeartbeatBoundary(t *testing.T) {
	cursor := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	lastSuccess := cursor.Add(time.Minute)
	state := newState()
	state.ChatPolls["chat-1"] = ChatPollState{
		ChatID:               "chat-1",
		Seeded:               true,
		LastModifiedCursor:   cursor,
		LastSuccessfulPollAt: lastSuccess,
		UpdatedAt:            lastSuccess,
	}
	poll, changed := applyChatPollSuccessLocked(&state, "chat-1", cursor, true, false, 0, "", lastSuccess.Add(chatPollSuccessHeartbeatWriteInterval-time.Nanosecond))
	if changed {
		t.Fatal("poll success changed before heartbeat interval elapsed")
	}
	if !poll.LastSuccessfulPollAt.Equal(lastSuccess) {
		t.Fatalf("pre-boundary LastSuccessfulPollAt = %s, want %s", poll.LastSuccessfulPollAt, lastSuccess)
	}
	poll, changed = applyChatPollSuccessLocked(&state, "chat-1", cursor, true, false, 0, "", lastSuccess.Add(chatPollSuccessHeartbeatWriteInterval))
	if !changed {
		t.Fatal("poll success did not refresh at heartbeat boundary")
	}
	if !poll.LastSuccessfulPollAt.Equal(lastSuccess.Add(chatPollSuccessHeartbeatWriteInterval)) {
		t.Fatalf("boundary LastSuccessfulPollAt = %s, want %s", poll.LastSuccessfulPollAt, lastSuccess.Add(chatPollSuccessHeartbeatWriteInterval))
	}
}

func TestChatPollSuccessAndScheduleUsesSingleStateLoad(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	cursor := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	next := cursor.Add(time.Minute)
	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})
	poll, err := store.RecordChatPollSuccessWithContinuationAndSchedule(ctx, "chat-1", cursor, true, false, 0, "", func(poll ChatPollState) (ChatPollScheduleUpdate, error) {
		if !poll.Seeded || !poll.LastModifiedCursor.Equal(cursor) || poll.LastSuccessfulPollAt.IsZero() {
			t.Fatalf("schedule callback saw unexpected success poll: %#v", poll)
		}
		return ChatPollScheduleUpdate{
			ChatID:            "chat-1",
			PollState:         "warm",
			NextPollAt:        next,
			ClearBlockedUntil: true,
			ResetFailures:     true,
		}, nil
	})
	if err != nil {
		t.Fatalf("RecordChatPollSuccessWithContinuationAndSchedule error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 1 {
		t.Fatalf("combined chat poll update loaded state %d times, want 1", got)
	}
	if poll.PollState != "warm" || !poll.NextPollAt.Equal(next) || !poll.LastModifiedCursor.Equal(cursor) {
		t.Fatalf("combined chat poll state = %#v", poll)
	}
}

func TestChatPollSuccessAndScheduleWritesWhenSuccessNoopButScheduleChanges(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	cursor := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, false, 0); err != nil {
		t.Fatalf("initial RecordChatPollSuccess error: %v", err)
	}
	beforePoll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll before combined update ok=%v err=%v", ok, err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before schedule-only change: %v", err)
	}
	next := cursor.Add(2 * time.Minute)
	poll, err := store.RecordChatPollSuccessWithContinuationAndSchedule(ctx, "chat-1", cursor, true, false, 0, "", func(ChatPollState) (ChatPollScheduleUpdate, error) {
		return ChatPollScheduleUpdate{
			ChatID:     "chat-1",
			PollState:  "warm",
			NextPollAt: next,
		}, nil
	})
	if err != nil {
		t.Fatalf("RecordChatPollSuccessWithContinuationAndSchedule error: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after schedule-only change: %v", err)
	}
	if bytes.Equal(before, after) {
		t.Fatal("schedule-only combined update did not rewrite state")
	}
	if !poll.LastSuccessfulPollAt.Equal(beforePoll.LastSuccessfulPollAt) {
		t.Fatalf("schedule-only combined update changed LastSuccessfulPollAt: before=%s after=%s", beforePoll.LastSuccessfulPollAt, poll.LastSuccessfulPollAt)
	}
	if poll.PollState != "warm" || !poll.NextPollAt.Equal(next) {
		t.Fatalf("schedule-only combined poll = %#v", poll)
	}
}

func TestChatPollSuccessAndScheduleRejectsMismatchedChat(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	cursor := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, false, 0); err != nil {
		t.Fatalf("initial RecordChatPollSuccess error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before mismatched schedule: %v", err)
	}
	_, err = store.RecordChatPollSuccessWithContinuationAndSchedule(ctx, "chat-1", cursor.Add(time.Minute), true, false, 1, "", func(ChatPollState) (ChatPollScheduleUpdate, error) {
		return ChatPollScheduleUpdate{
			ChatID:     "chat-2",
			PollState:  "warm",
			NextPollAt: cursor.Add(time.Minute),
		}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched schedule error = %v, want chat mismatch", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after mismatched schedule: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("mismatched combined schedule rewrote state")
	}
}

func TestHighChurnNoopPressureDoesNotRewriteState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	cursor := time.Date(2026, 5, 23, 1, 0, 0, 0, time.UTC)
	record := MessageProvenanceRecord{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-1",
		Origin:         MessageOriginHelperOutbox,
		OutboxID:       "outbox-1",
		CreatedAt:      cursor,
		UpdatedAt:      cursor,
	}
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, false, 0); err != nil {
		t.Fatalf("initial RecordChatPollSuccess error: %v", err)
	}
	if _, err := store.RecordMessageProvenance(ctx, record); err != nil {
		t.Fatalf("initial RecordMessageProvenance error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before pressure: %v", err)
	}
	for i := 0; i < 100; i++ {
		if _, err := store.RecordChatPollSuccess(ctx, "chat-1", cursor, true, false, 0); err != nil {
			t.Fatalf("pressure RecordChatPollSuccess %d error: %v", i, err)
		}
		if _, err := store.RecordMessageProvenance(ctx, record); err != nil {
			t.Fatalf("pressure RecordMessageProvenance %d error: %v", i, err)
		}
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after pressure: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("high-churn no-op pressure rewrote state")
	}
}

func TestChatPollScheduleCanClearContinuationPath(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, "chat-1", now.Add(-time.Hour), true, true, 50, "/chats/chat-1/messages?$skiptoken=stale"); err != nil {
		t.Fatalf("RecordChatPollSuccessWithContinuation error: %v", err)
	}

	poll, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:                "chat-1",
		PollState:             "hot",
		NextPollAt:            now,
		LastActivityAt:        now,
		ClearContinuationPath: true,
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedule clear continuation error: %v", err)
	}
	if poll.ContinuationPath != "" {
		t.Fatalf("continuation path was not cleared: %#v", poll)
	}
}

func TestChatPollScheduleStatePersistsParkAndBlock(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Add(time.Hour)

	poll, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      "hot",
		NextPollAt:     now.Add(time.Second),
		LastActivityAt: now,
		ResetFailures:  true,
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedule hot error: %v", err)
	}
	if poll.PollState != "hot" || !poll.NextPollAt.Equal(now.Add(time.Second)) || !poll.LastActivityAt.Equal(now) {
		t.Fatalf("hot poll schedule mismatch: %#v", poll)
	}

	blockedUntil := now.Add(time.Minute)
	if err := store.RecordChatPollErrorWithBlock(ctx, "chat-1", "429", blockedUntil); err != nil {
		t.Fatalf("RecordChatPollErrorWithBlock error: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll after block ok=%v err=%v", ok, err)
	}
	if poll.PollState != "blocked" || poll.PreviousPollState != "hot" || !poll.BlockedUntil.Equal(blockedUntil) || !poll.NextPollAt.Equal(blockedUntil) {
		t.Fatalf("blocked poll schedule mismatch: %#v", poll)
	}

	poll, err = store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:            "chat-1",
		PollState:         "parked",
		PreviousPollState: "",
		ClearBlockedUntil: true,
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedule parked error: %v", err)
	}
	if poll.PollState != "parked" || poll.ParkedAt.IsZero() || !poll.BlockedUntil.IsZero() {
		t.Fatalf("parked poll schedule mismatch: %#v", poll)
	}
	poll, err = store.MarkChatPollParkNoticeSent(ctx, "chat-1", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("MarkChatPollParkNoticeSent error: %v", err)
	}
	if poll.ParkNoticeSentAt.IsZero() {
		t.Fatalf("park notice timestamp missing: %#v", poll)
	}

	poll, err = store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:            "chat-1",
		PollState:         "hot",
		NextPollAt:        now,
		LastActivityAt:    now.Add(3 * time.Minute),
		ClearBlockedUntil: true,
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedule resume error: %v", err)
	}
	if poll.PollState != "hot" || !poll.ParkedAt.IsZero() || !poll.ParkNoticeSentAt.IsZero() {
		t.Fatalf("resume should clear park markers: %#v", poll)
	}
}

func TestChatPollScheduleNoopDoesNotRewriteState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	update := ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      "warm",
		NextPollAt:     now.Add(time.Minute),
		LastActivityAt: now.Add(-10 * time.Minute),
	}
	if _, err := store.UpdateChatPollSchedule(ctx, update); err != nil {
		t.Fatalf("initial UpdateChatPollSchedule error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before noop: %v", err)
	}
	pollBefore, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll before noop ok=%v err=%v", ok, err)
	}
	pollAfter, err := store.UpdateChatPollSchedule(ctx, update)
	if err != nil {
		t.Fatalf("noop UpdateChatPollSchedule error: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after noop: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("noop UpdateChatPollSchedule rewrote state file")
	}
	if !pollAfter.UpdatedAt.Equal(pollBefore.UpdatedAt) {
		t.Fatalf("noop UpdateChatPollSchedule changed UpdatedAt: before=%s after=%s", pollBefore.UpdatedAt, pollAfter.UpdatedAt)
	}
}

func TestUpdateChatPollSchedulesEmptyBatchDoesNotLoadState(t *testing.T) {
	store := newTestStore(t)
	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	polls, err := store.UpdateChatPollSchedules(context.Background(), nil)
	if err != nil {
		t.Fatalf("UpdateChatPollSchedules empty error: %v", err)
	}
	if len(polls) != 0 {
		t.Fatalf("empty batch returned polls: %#v", polls)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("empty batch loaded state %d times, want 0", got)
	}
}

func TestUpdateChatPollSchedulesBatchesInSingleLoad(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	polls, err := store.UpdateChatPollSchedules(ctx, []ChatPollScheduleUpdate{
		{
			ChatID:         "chat-1",
			PollState:      "warm",
			NextPollAt:     now.Add(time.Minute),
			LastActivityAt: now.Add(-10 * time.Minute),
		},
		{
			ChatID:         "chat-2",
			PollState:      "hot",
			NextPollAt:     now.Add(2 * time.Minute),
			LastActivityAt: now.Add(-5 * time.Minute),
		},
		{
			ChatID:         "chat-3",
			PollState:      "blocked",
			NextPollAt:     now.Add(5 * time.Minute),
			LastActivityAt: now.Add(-time.Minute),
			BlockedUntil:   now.Add(5 * time.Minute),
		},
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedules error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 1 {
		t.Fatalf("batch schedule update loaded state %d times, want 1", got)
	}
	if len(polls) != 3 {
		t.Fatalf("batch returned %d polls, want 3: %#v", len(polls), polls)
	}
	if poll := polls["chat-1"]; poll.PollState != "warm" || !poll.NextPollAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("chat-1 poll = %#v", poll)
	}
	if poll := polls["chat-2"]; poll.PollState != "hot" || !poll.LastActivityAt.Equal(now.Add(-5*time.Minute)) {
		t.Fatalf("chat-2 poll = %#v", poll)
	}
	if poll := polls["chat-3"]; poll.PollState != "blocked" || !poll.BlockedUntil.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("chat-3 poll = %#v", poll)
	}
}

func TestUpdateChatPollSchedulesAppliesDuplicateChatInOrder(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	polls, err := store.UpdateChatPollSchedules(ctx, []ChatPollScheduleUpdate{
		{
			ChatID:         "chat-1",
			PollState:      "warm",
			NextPollAt:     now.Add(time.Minute),
			LastActivityAt: now.Add(-10 * time.Minute),
		},
		{
			ChatID:         "chat-1",
			PollState:      "blocked",
			NextPollAt:     now.Add(5 * time.Minute),
			BlockedUntil:   now.Add(5 * time.Minute),
			LastActivityAt: now.Add(-10 * time.Minute),
		},
	})
	if err != nil {
		t.Fatalf("UpdateChatPollSchedules duplicate chat error: %v", err)
	}
	poll := polls["chat-1"]
	if poll.PollState != "blocked" || poll.PreviousPollState != "warm" || !poll.BlockedUntil.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("duplicate chat final poll = %#v, want blocked with previous warm", poll)
	}
}

func TestUpdateChatPollSchedulesNoopDoesNotRewriteState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	updates := []ChatPollScheduleUpdate{
		{
			ChatID:         "chat-1",
			PollState:      "warm",
			NextPollAt:     now.Add(time.Minute),
			LastActivityAt: now.Add(-10 * time.Minute),
		},
		{
			ChatID:         "chat-2",
			PollState:      "hot",
			NextPollAt:     now.Add(2 * time.Minute),
			LastActivityAt: now.Add(-5 * time.Minute),
		},
	}
	beforePolls, err := store.UpdateChatPollSchedules(ctx, updates)
	if err != nil {
		t.Fatalf("initial UpdateChatPollSchedules error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before noop batch: %v", err)
	}

	afterPolls, err := store.UpdateChatPollSchedules(ctx, updates)
	if err != nil {
		t.Fatalf("noop UpdateChatPollSchedules error: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after noop batch: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("noop UpdateChatPollSchedules rewrote state file")
	}
	for _, chatID := range []string{"chat-1", "chat-2"} {
		if !afterPolls[chatID].UpdatedAt.Equal(beforePolls[chatID].UpdatedAt) {
			t.Fatalf("%s noop batch changed UpdatedAt: before=%s after=%s", chatID, beforePolls[chatID].UpdatedAt, afterPolls[chatID].UpdatedAt)
		}
	}
}

func TestUpdateChatPollSchedulesRejectsInvalidBatchAtomically(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      "warm",
		NextPollAt:     now.Add(time.Minute),
		LastActivityAt: now.Add(-10 * time.Minute),
	}); err != nil {
		t.Fatalf("initial UpdateChatPollSchedule error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before invalid batch: %v", err)
	}

	_, err = store.UpdateChatPollSchedules(ctx, []ChatPollScheduleUpdate{
		{
			ChatID:         "chat-2",
			PollState:      "hot",
			NextPollAt:     now.Add(2 * time.Minute),
			LastActivityAt: now.Add(-5 * time.Minute),
		},
		{
			ChatID:     " ",
			PollState:  "warm",
			NextPollAt: now.Add(3 * time.Minute),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "chat id is required") {
		t.Fatalf("invalid batch error = %v, want chat id error", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after invalid batch: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("invalid UpdateChatPollSchedules batch partially rewrote state")
	}
	if _, ok, err := store.ChatPoll(ctx, "chat-2"); err != nil || ok {
		t.Fatalf("chat-2 after invalid batch ok=%v err=%v, want absent", ok, err)
	}
}

func TestChatPollScheduleBlockedNoopPreservesPreviousState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      "warm",
		NextPollAt:     now.Add(time.Minute),
		LastActivityAt: now.Add(-10 * time.Minute),
	}); err != nil {
		t.Fatalf("initial warm schedule error: %v", err)
	}
	blockedUpdate := ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      "blocked",
		NextPollAt:     now.Add(5 * time.Minute),
		LastActivityAt: now.Add(-10 * time.Minute),
		BlockedUntil:   now.Add(5 * time.Minute),
	}
	blocked, err := store.UpdateChatPollSchedule(ctx, blockedUpdate)
	if err != nil {
		t.Fatalf("blocked schedule error: %v", err)
	}
	if blocked.PreviousPollState != "warm" {
		t.Fatalf("blocked previous state = %q, want warm", blocked.PreviousPollState)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before blocked noop: %v", err)
	}

	blockedAgain, err := store.UpdateChatPollSchedule(ctx, blockedUpdate)
	if err != nil {
		t.Fatalf("blocked noop schedule error: %v", err)
	}
	if blockedAgain.PreviousPollState != "warm" {
		t.Fatalf("blocked noop previous state = %q, want warm", blockedAgain.PreviousPollState)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after blocked noop: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("blocked noop UpdateChatPollSchedule rewrote state file")
	}

	resumed, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:            "chat-1",
		PollState:         "warm",
		NextPollAt:        now.Add(6 * time.Minute),
		LastActivityAt:    now.Add(-10 * time.Minute),
		ClearBlockedUntil: true,
	})
	if err != nil {
		t.Fatalf("resume schedule error: %v", err)
	}
	if resumed.PollState != "warm" || resumed.PreviousPollState != "" || !resumed.BlockedUntil.IsZero() {
		t.Fatalf("resume did not clear blocked metadata: %#v", resumed)
	}
}

func TestChatPollScheduleParkedNoopPreservesParkedAt(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	update := ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      "parked",
		NextPollAt:     now.Add(48 * time.Hour),
		LastActivityAt: now.Add(-48 * time.Hour),
	}
	parked, err := store.UpdateChatPollSchedule(ctx, update)
	if err != nil {
		t.Fatalf("parked schedule error: %v", err)
	}
	if parked.ParkedAt.IsZero() {
		t.Fatal("parked schedule did not set ParkedAt")
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before parked noop: %v", err)
	}

	parkedAgain, err := store.UpdateChatPollSchedule(ctx, update)
	if err != nil {
		t.Fatalf("parked noop schedule error: %v", err)
	}
	if !parkedAgain.ParkedAt.Equal(parked.ParkedAt) {
		t.Fatalf("parked noop changed ParkedAt: before=%s after=%s", parked.ParkedAt, parkedAgain.ParkedAt)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after parked noop: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("parked noop UpdateChatPollSchedule rewrote state file")
	}
}

func TestRecordOwnerHeartbeatWritesAndReadsOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	owner := testOwner("session-1", "turn-1", now)

	recorded, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now)
	if err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	if recorded.LastHeartbeat != now {
		t.Fatalf("LastHeartbeat = %s, want %s", recorded.LastHeartbeat, now)
	}
	read, ok, err := store.ReadOwner(ctx)
	if err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	}
	if !ok {
		t.Fatal("ReadOwner ok = false")
	}
	if read.PID != owner.PID || read.Hostname != owner.Hostname || read.ExecutablePath != owner.ExecutablePath {
		t.Fatalf("owner identity mismatch: %#v", read)
	}
	if read.HelperVersion != "v-test" || read.ActiveSessionID != "session-1" || read.ActiveTurnID != "turn-1" {
		t.Fatalf("owner payload mismatch: %#v", read)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.ServiceOwner == nil || state.LockOwner == nil {
		t.Fatalf("state owner metadata missing: service=%#v lock=%#v", state.ServiceOwner, state.LockOwner)
	}
	if *state.ServiceOwner != *state.LockOwner {
		t.Fatalf("service owner and lock owner diverged: service=%#v lock=%#v", *state.ServiceOwner, *state.LockOwner)
	}
}

func TestDeferredInboundSortedByChatAndMessageTime(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	events := []InboundEvent{
		{SessionID: "s2", TeamsChatID: "chat-b", TeamsMessageID: "b2", Text: "b2", Status: InboundStatusDeferred, CreatedAt: base.Add(2 * time.Second)},
		{SessionID: "s1", TeamsChatID: "chat-a", TeamsMessageID: "a2", Text: "a2", Status: InboundStatusDeferred, CreatedAt: base.Add(2 * time.Second)},
		{SessionID: "s1", TeamsChatID: "chat-a", TeamsMessageID: "a1", Text: "a1", Status: InboundStatusDeferred, CreatedAt: base.Add(time.Second)},
	}
	for _, event := range events {
		if _, _, err := store.PersistInbound(ctx, event); err != nil {
			t.Fatalf("PersistInbound error: %v", err)
		}
	}

	deferred, err := store.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound error: %v", err)
	}
	var got []string
	for _, event := range deferred {
		got = append(got, event.TeamsMessageID)
	}
	want := []string{"a1", "a2", "b2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deferred order = %#v, want %#v", got, want)
	}
}

func TestClaimControlLeasePrimaryBeatsEphemeralAndEphemeralStaysStandby(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}

	ephemeral := MachineRecord{ID: "machine-temp", ScopeID: scope.ID, Kind: MachineKindEphemeral, Label: "temp"}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ephemeral ClaimControlLease error: %v", err)
	}
	if first.Mode != LeaseModeActive || first.Lease.HolderMachineID != ephemeral.ID || first.Lease.Generation != 1 {
		t.Fatalf("ephemeral decision = %#v, want active generation 1", first)
	}

	primary := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary, Label: "primary"}
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: primary, Duration: time.Minute, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("primary ClaimControlLease error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.HolderMachineID != primary.ID || second.Lease.Generation != 2 {
		t.Fatalf("primary decision = %#v, want preempted active generation 2", second)
	}
	stateAfterPreempt, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after primary preempt error: %v", err)
	}
	if got := stateAfterPreempt.Machines[ephemeral.ID].Status; got != MachineStatusStandby {
		t.Fatalf("preempted ephemeral status = %q, want standby", got)
	}

	third, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatalf("second ephemeral ClaimControlLease error: %v", err)
	}
	if third.Mode != LeaseModeStandby || third.Lease.HolderMachineID != primary.ID {
		t.Fatalf("second ephemeral decision = %#v, want standby behind primary", third)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Machines[ephemeral.ID].Status; got != MachineStatusStandby {
		t.Fatalf("ephemeral status = %q, want standby", got)
	}
	if got := state.Machines[primary.ID].Status; got != MachineStatusActive {
		t.Fatalf("primary status = %q, want active", got)
	}
}

func TestClaimControlLeaseDoesNotPreemptLiveIdleOwner(t *testing.T) {
	prevHostname := ownerHostname
	prevAlive := ownerProcessAlive
	t.Cleanup(func() {
		ownerHostname = prevHostname
		ownerProcessAlive = prevAlive
	})
	ownerHostname = func() (string, error) { return "host-a", nil }
	ownerProcessAlive = func(pid int) bool { return pid == 4242 }

	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	ephemeral := MachineRecord{ID: "machine-temp", ScopeID: scope.ID, Kind: MachineKindEphemeral, Label: "temp"}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ephemeral ClaimControlLease error: %v", err)
	}
	owner := testOwner("", "", now)
	owner.ScopeID = scope.ID
	owner.MachineID = ephemeral.ID
	owner.LeaseGeneration = first.Lease.Generation
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	primary := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary, Label: "primary"}
	blocked, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: primary, Duration: time.Minute, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("primary ClaimControlLease error: %v", err)
	}
	if blocked.Mode != LeaseModeStandby || blocked.Lease.HolderMachineID != ephemeral.ID {
		t.Fatalf("primary decision = %#v, want standby behind live owner", blocked)
	}

	cleared, err := store.ClearOwnerIfSame(ctx, owner)
	if err != nil {
		t.Fatalf("ClearOwnerIfSame error: %v", err)
	}
	if !cleared {
		t.Fatal("ClearOwnerIfSame cleared = false, want true")
	}
	released, err := store.ReleaseControlLeaseIfHolder(ctx, ephemeral.ID, first.Lease.Generation)
	if err != nil {
		t.Fatalf("ReleaseControlLeaseIfHolder error: %v", err)
	}
	if !released {
		t.Fatal("ReleaseControlLeaseIfHolder released = false, want true")
	}
	claimed, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: primary, Duration: time.Minute, Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatalf("primary ClaimControlLease after release error: %v", err)
	}
	if claimed.Mode != LeaseModeActive || claimed.Lease.HolderMachineID != primary.ID {
		t.Fatalf("primary decision after release = %#v, want active", claimed)
	}
}

func TestClaimControlLeaseSameMachineDuplicateStaysStandbyBehindLiveOwner(t *testing.T) {
	prevHostname := ownerHostname
	prevAlive := ownerProcessAlive
	t.Cleanup(func() {
		ownerHostname = prevHostname
		ownerProcessAlive = prevAlive
	})
	ownerHostname = func() (string, error) { return "host-a", nil }
	ownerProcessAlive = func(pid int) bool { return pid == 4242 }

	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	machine := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary, Label: "primary"}
	owner := testOwner("", "", now)
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Owner: owner, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("initial ClaimControlLease error: %v", err)
	}
	owner.ScopeID = scope.ID
	owner.MachineID = machine.ID
	owner.LeaseGeneration = first.Lease.Generation
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	duplicateOwner := owner
	duplicateOwner.PID = 7777
	duplicate, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Owner: duplicateOwner, Duration: time.Minute, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("duplicate ClaimControlLease error: %v", err)
	}
	if duplicate.Mode != LeaseModeStandby || duplicate.Lease.HolderMachineID != machine.ID {
		t.Fatalf("duplicate decision = %#v, want standby behind existing owner", duplicate)
	}

	beforeRefresh, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before same owner refresh: %v", err)
	}
	refreshed, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Owner: owner, Duration: time.Minute, Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatalf("same owner ClaimControlLease error: %v", err)
	}
	if refreshed.Mode != LeaseModeActive || refreshed.Lease.HolderMachineID != machine.ID {
		t.Fatalf("same owner decision = %#v, want active refresh", refreshed)
	}
	afterRefresh, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after same owner refresh: %v", err)
	}
	if bytes.Equal(beforeRefresh, afterRefresh) {
		t.Fatal("same owner refresh did not rewrite standby holder machine status")
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after same owner refresh: %v", err)
	}
	if got := loaded.Machines[machine.ID].Status; got != MachineStatusActive {
		t.Fatalf("persisted holder machine status = %q, want %q", got, MachineStatusActive)
	}
}

func TestClaimControlLeaseSameMachineReloadBackupPathRefreshesActiveOwner(t *testing.T) {
	prevHostname := ownerHostname
	prevAlive := ownerProcessAlive
	t.Cleanup(func() {
		ownerHostname = prevHostname
		ownerProcessAlive = prevAlive
	})
	ownerHostname = func() (string, error) { return "host-a", nil }
	ownerProcessAlive = func(pid int) bool { return pid == 4242 }

	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	machine := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary, Label: "primary"}
	owner := testOwner("", "", now)
	owner.ExecutablePath = "/usr/local/bin/codex-helper.reload-backup-4242-100"
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Owner: owner, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("initial ClaimControlLease error: %v", err)
	}
	owner.ScopeID = scope.ID
	owner.MachineID = machine.ID
	owner.LeaseGeneration = first.Lease.Generation
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	reloadedOwner := owner
	reloadedOwner.ExecutablePath = "/usr/local/bin/codex-helper.reload-backup-4242-100.reload-backup-4242-200"
	refreshed, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Owner: reloadedOwner, Duration: time.Minute, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("reload-backup ClaimControlLease error: %v", err)
	}
	if refreshed.Mode != LeaseModeActive || refreshed.Lease.HolderMachineID != machine.ID {
		t.Fatalf("reload-backup decision = %#v, want active refresh", refreshed)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Machines[machine.ID].Status; got != MachineStatusActive {
		t.Fatalf("machine status = %q, want active", got)
	}
}

func TestSameOwnerProcessCanonicalizesNFSSillyRename(t *testing.T) {
	dir := t.TempDir()
	stable := filepath.Join(dir, storeTestHelperBinaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	a := OwnerMetadata{PID: 1234, Hostname: "host-a", ExecutablePath: stable}
	b := OwnerMetadata{PID: 1234, Hostname: "host-a", ExecutablePath: filepath.Join(dir, ".nfs802014de01c482a800000492")}
	if !sameOwnerProcess(a, b) {
		t.Fatalf("sameOwnerProcess should canonicalize NFS silly rename: a=%#v b=%#v", a, b)
	}
}

func TestCurrentOwnerStoresStableExecutableWhenRawPathIsTransient(t *testing.T) {
	prevExecutable := currentOwnerExecutable
	prevArgv0 := currentOwnerArgv0
	t.Cleanup(func() {
		currentOwnerExecutable = prevExecutable
		currentOwnerArgv0 = prevArgv0
	})

	dir := t.TempDir()
	stable := filepath.Join(dir, storeTestHelperBinaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	currentOwnerExecutable = func() (string, error) {
		return filepath.Join(dir, ".nfs802014de01c482a800000492"), nil
	}

	owner, err := CurrentOwner("0.1.0-test", "session-1", "turn-1", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if owner.ExecutablePath != stable {
		t.Fatalf("owner executable = %q, want stable %q", owner.ExecutablePath, stable)
	}
}

func TestCurrentOwnerFallsBackToStableArgv0WhenRawPathIsGoBuild(t *testing.T) {
	prevExecutable := currentOwnerExecutable
	prevArgv0 := currentOwnerArgv0
	t.Cleanup(func() {
		currentOwnerExecutable = prevExecutable
		currentOwnerArgv0 = prevArgv0
	})

	dir := t.TempDir()
	name := storeTestHelperBinaryName()
	stable := filepath.Join(dir, name)
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	currentOwnerExecutable = func() (string, error) {
		return filepath.Join(t.TempDir(), "go-build123", "b001", "exe", name), nil
	}
	currentOwnerArgv0 = func() string { return stable }

	owner, err := CurrentOwner("0.1.0-test", "", "", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if owner.ExecutablePath != stable {
		t.Fatalf("owner executable = %q, want argv0 stable %q", owner.ExecutablePath, stable)
	}
}

func TestCurrentOwnerRejectsUnresolvedTransientExecutable(t *testing.T) {
	prevExecutable := currentOwnerExecutable
	prevArgv0 := currentOwnerArgv0
	t.Cleanup(func() {
		currentOwnerExecutable = prevExecutable
		currentOwnerArgv0 = prevArgv0
	})

	currentOwnerExecutable = func() (string, error) {
		return filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492"), nil
	}
	currentOwnerArgv0 = func() string { return "" }

	owner, err := CurrentOwner("0.1.0-test", "", "", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "stable owner executable") {
		t.Fatalf("CurrentOwner error = %v, want unresolved stable owner executable failure", err)
	}
	if owner.ExecutablePath != "" {
		t.Fatalf("owner executable = %q, want empty on failure", owner.ExecutablePath)
	}
}

func TestCurrentOwnerRejectsUnresolvedGoBuildHelperExecutable(t *testing.T) {
	prevExecutable := currentOwnerExecutable
	prevArgv0 := currentOwnerArgv0
	t.Cleanup(func() {
		currentOwnerExecutable = prevExecutable
		currentOwnerArgv0 = prevArgv0
	})

	name := storeTestHelperBinaryName()
	currentOwnerExecutable = func() (string, error) {
		return filepath.Join(t.TempDir(), "go-build123", "b001", "exe", name), nil
	}
	currentOwnerArgv0 = func() string { return "" }

	_, err := CurrentOwner("0.1.0-test", "", "", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "stable owner executable") {
		t.Fatalf("CurrentOwner error = %v, want unresolved go-build helper executable failure", err)
	}
}

func TestCurrentOwnerAllowsUnresolvedWindowsGoTestExecutable(t *testing.T) {
	prevExecutable := currentOwnerExecutable
	prevArgv0 := currentOwnerArgv0
	t.Cleanup(func() {
		currentOwnerExecutable = prevExecutable
		currentOwnerArgv0 = prevArgv0
	})

	raw := filepath.Join(t.TempDir(), "go-build123", "b001", "cli.test.exe")
	currentOwnerExecutable = func() (string, error) { return raw, nil }
	currentOwnerArgv0 = func() string { return "" }

	owner, err := CurrentOwner("0.1.0-test", "", "", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if owner.ExecutablePath != raw {
		t.Fatalf("owner executable = %q, want test executable %q", owner.ExecutablePath, raw)
	}
}

func TestClaimControlLeaseDoesNotPreemptActiveTurn(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	ephemeral := MachineRecord{ID: "machine-temp", ScopeID: scope.ID, Kind: MachineKindEphemeral}
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ephemeral ClaimControlLease error: %v", err)
	}
	owner := OwnerMetadata{PID: 123, Hostname: "temp", ExecutablePath: "/bin/helper", MachineID: ephemeral.ID, LeaseGeneration: decision.Lease.Generation, ActiveTurnID: "turn-1", StartedAt: now}
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	primary := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	primaryDecision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: primary, Duration: time.Minute, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("primary ClaimControlLease error: %v", err)
	}
	if primaryDecision.Mode != LeaseModeStandby || primaryDecision.Lease.HolderMachineID != ephemeral.ID {
		t.Fatalf("primary decision during active turn = %#v, want standby behind active ephemeral turn", primaryDecision)
	}
}

func TestClaimControlLeaseSameHolderEarlyRefreshDoesNotRewriteState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	machine := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	owner := OwnerMetadata{PID: 123, Hostname: "remote-host", ExecutablePath: "/bin/helper", StartedAt: now}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Owner: owner, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("initial ClaimControlLease error: %v", err)
	}
	owner.MachineID = machine.ID
	owner.LeaseGeneration = first.Lease.Generation
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before early refresh: %v", err)
	}
	refreshed, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Owner: owner, Duration: time.Minute, Now: now.Add(5 * time.Second)})
	if err != nil {
		t.Fatalf("early refresh ClaimControlLease error: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after early refresh: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("same-holder early ClaimControlLease refresh rewrote state")
	}
	if refreshed.Mode != LeaseModeActive || refreshed.Lease.Generation != first.Lease.Generation || !refreshed.Lease.LeaseUntil.Equal(first.Lease.LeaseUntil) {
		t.Fatalf("early refresh decision = %#v, want unchanged active lease %#v", refreshed, first.Lease)
	}

	changedScope := scope
	changedScope.Profile = "review"
	beforeScopeChange, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before scope metadata refresh: %v", err)
	}
	if _, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: changedScope, Machine: machine, Owner: owner, Duration: time.Minute, Now: now.Add(8 * time.Second)}); err != nil {
		t.Fatalf("scope metadata refresh ClaimControlLease error: %v", err)
	}
	afterScopeChange, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after scope metadata refresh: %v", err)
	}
	if bytes.Equal(beforeScopeChange, afterScopeChange) {
		t.Fatal("same-holder scope metadata refresh did not rewrite state")
	}
	loadedAfterScopeChange, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after scope metadata refresh: %v", err)
	}
	if loadedAfterScopeChange.Scope.Profile != "review" {
		t.Fatalf("scope profile after metadata refresh = %q, want review", loadedAfterScopeChange.Scope.Profile)
	}

	changedMachine := machine
	changedMachine.Label = "renamed-machine"
	beforeMetadataChange, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state before metadata refresh: %v", err)
	}
	metadataRefresh, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: changedScope, Machine: changedMachine, Owner: owner, Duration: time.Minute, Now: now.Add(10 * time.Second)})
	if err != nil {
		t.Fatalf("metadata refresh ClaimControlLease error: %v", err)
	}
	afterMetadataChange, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read state after metadata refresh: %v", err)
	}
	if bytes.Equal(beforeMetadataChange, afterMetadataChange) {
		t.Fatal("same-holder metadata refresh did not rewrite state")
	}
	if metadataRefresh.Holder.Label != "renamed-machine" {
		t.Fatalf("metadata refresh holder label = %q, want renamed-machine", metadataRefresh.Holder.Label)
	}

	later, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: changedScope, Machine: changedMachine, Owner: owner, Duration: time.Minute, Now: now.Add(40 * time.Second)})
	if err != nil {
		t.Fatalf("late refresh ClaimControlLease error: %v", err)
	}
	if !later.Lease.LeaseUntil.After(metadataRefresh.Lease.LeaseUntil) {
		t.Fatalf("late refresh did not extend lease: metadata=%s later=%s", metadataRefresh.Lease.LeaseUntil, later.Lease.LeaseUntil)
	}
}

func TestControlLeaseForwardJumpAllowsTakeoverAfterSleep(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	ephemeral := MachineRecord{ID: "machine-temp", ScopeID: scope.ID, Kind: MachineKindEphemeral}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: ephemeral, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ephemeral ClaimControlLease error: %v", err)
	}
	owner := OwnerMetadata{PID: 123, Hostname: "temp", ExecutablePath: "/bin/helper", MachineID: ephemeral.ID, LeaseGeneration: first.Lease.Generation, ActiveTurnID: "turn-1", StartedAt: now}
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	primary := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	wake := now.Add(2 * time.Minute)
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: primary, Duration: time.Minute, Now: wake})
	if err != nil {
		t.Fatalf("primary ClaimControlLease after sleep error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.HolderMachineID != primary.ID {
		t.Fatalf("primary decision after sleep = %#v, want takeover", second)
	}
	if _, err := store.ValidateControlLease(ctx, ephemeral.ID, first.Lease.Generation, wake); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("old sleepy holder ValidateControlLease error = %v, want ErrControlLeaseNotHeld", err)
	}
}

func TestClaimControlLeaseConcurrentManyMachinesSingleActive(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	errs := make(chan error, 80)
	for i := 0; i < 40; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			kind := MachineKindEphemeral
			if i%5 == 0 {
				kind = MachineKindPrimary
			}
			machine := MachineRecord{
				ID:      fmt.Sprintf("machine-%02d", i),
				ScopeID: scope.ID,
				Kind:    kind,
				Label:   fmt.Sprintf("machine %02d", i),
			}
			for j := 0; j < 5; j++ {
				now := start.Add(time.Duration(i*5+j) * time.Millisecond)
				if _, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Minute, Now: now}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("ClaimControlLease concurrent error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.ControlLease.HolderMachineID == "" || state.ControlLease.Generation <= 0 {
		t.Fatalf("missing active control lease after concurrent claims: %#v", state.ControlLease)
	}
	active := 0
	for _, machine := range state.Machines {
		if machine.Status == MachineStatusActive {
			active++
			if machine.ID != state.ControlLease.HolderMachineID {
				t.Fatalf("machine %s is active but lease holder is %s", machine.ID, state.ControlLease.HolderMachineID)
			}
		}
	}
	if active != 1 {
		t.Fatalf("active machine count = %d, want 1; machines=%#v lease=%#v", active, state.Machines, state.ControlLease)
	}
}

func TestTeamsBackgroundKeepaliveOldGenerationCannotReleaseNewHolderCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	firstMachine := MachineRecord{ID: "machine-first", ScopeID: scope.ID, Kind: MachineKindPrimary}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: firstMachine, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("first ClaimControlLease error: %v", err)
	}
	secondMachine := MachineRecord{ID: "machine-second", ScopeID: scope.ID, Kind: MachineKindPrimary}
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: secondMachine, Duration: time.Minute, Now: now.Add(2 * time.Minute)})
	if err != nil {
		t.Fatalf("second ClaimControlLease after lease expiry error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.HolderMachineID != secondMachine.ID || second.Lease.Generation <= first.Lease.Generation {
		t.Fatalf("second decision = %#v, want newer active generation", second)
	}
	released, err := store.ReleaseControlLeaseIfHolder(ctx, firstMachine.ID, first.Lease.Generation)
	if err != nil {
		t.Fatalf("old generation ReleaseControlLeaseIfHolder error: %v", err)
	}
	if released {
		t.Fatal("old generation release cleared the new holder")
	}
	if _, err := store.ValidateControlLease(ctx, secondMachine.ID, second.Lease.Generation, now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatalf("new holder lease was not preserved: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.ControlLease.HolderMachineID != secondMachine.ID || state.Machines[firstMachine.ID].Status != MachineStatusStandby {
		t.Fatalf("unexpected state after stale release: lease=%#v machines=%#v", state.ControlLease, state.Machines)
	}
}

func TestTeamsBackgroundKeepaliveSharedHomeTakeoverPreservesQueuedWorkAndRejectsOldMachineCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}

	machineA := MachineRecord{ID: "machine-a", ScopeID: scope.ID, Kind: MachineKindPrimary, Hostname: "host-a", Priority: 10}
	ownerA := testOwner("session-a", "turn-a", now)
	ownerA.PID = 1111
	ownerA.Hostname = "host-a"
	ownerA.ScopeID = scope.ID
	ownerA.MachineID = machineA.ID
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("machine A ClaimControlLease error: %v", err)
	}
	if first.Mode != LeaseModeActive || first.Lease.Generation != 1 {
		t.Fatalf("machine A decision = %#v, want active generation 1", first)
	}
	ownerA.LeaseGeneration = first.Lease.Generation
	if _, err := store.RecordOwnerHeartbeat(ctx, ownerA, time.Minute, now); err != nil {
		t.Fatalf("machine A RecordOwnerHeartbeat error: %v", err)
	}
	queued, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:              "outbox:machine-a-before-takeover",
		SessionID:       "session-a",
		TurnID:          "turn-a",
		TeamsChatID:     "chat-1",
		ScopeID:         scope.ID,
		MachineID:       machineA.ID,
		LeaseGeneration: first.Lease.Generation,
		Kind:            "final",
		Body:            "queued before shared-home takeover",
	})
	if err != nil {
		t.Fatalf("QueueOutbox before takeover error: %v", err)
	}

	takeoverAt := now.Add(2 * time.Minute)
	machineB := MachineRecord{ID: "machine-b", ScopeID: scope.ID, Kind: MachineKindPrimary, Hostname: "host-b", Priority: 10}
	ownerB := testOwner("session-b", "turn-b", takeoverAt)
	ownerB.PID = 2222
	ownerB.Hostname = "host-b"
	ownerB.ScopeID = scope.ID
	ownerB.MachineID = machineB.ID
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Minute, Now: takeoverAt})
	if err != nil {
		t.Fatalf("machine B ClaimControlLease error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.HolderMachineID != machineB.ID || second.Lease.Generation <= first.Lease.Generation {
		t.Fatalf("machine B decision = %#v, want active newer generation", second)
	}
	ownerB.LeaseGeneration = second.Lease.Generation
	if _, recovered, err := store.RecoverStaleOwner(ctx, ownerB, time.Minute, takeoverAt); err != nil {
		t.Fatalf("machine B RecoverStaleOwner error: %v", err)
	} else if !recovered {
		t.Fatal("machine B RecoverStaleOwner recovered = false")
	}

	if _, err := store.ValidateControlLease(ctx, machineA.ID, first.Lease.Generation, takeoverAt.Add(time.Second)); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("machine A old generation ValidateControlLease error = %v, want ErrControlLeaseNotHeld", err)
	}
	released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, first.Lease.Generation)
	if err != nil {
		t.Fatalf("machine A stale ReleaseControlLeaseIfHolder error: %v", err)
	}
	if released {
		t.Fatal("machine A stale generation released machine B lease")
	}

	wokenA := ownerA
	wokenA.ActiveSessionID = "session-a-resumed"
	wokenA.ActiveTurnID = "turn-a-resumed"
	if _, err := store.RecordOwnerHeartbeat(ctx, wokenA, time.Minute, takeoverAt.Add(10*time.Second)); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("machine A stale RecordOwnerHeartbeat error = %v, want ErrOwnerLive", err)
	}

	pending, err := store.PendingOutboxAt(ctx, takeoverAt.Add(10*time.Second))
	if err != nil {
		t.Fatalf("PendingOutboxAt after takeover error: %v", err)
	}
	var preserved *OutboxMessage
	for i := range pending {
		if pending[i].ID == queued.ID {
			preserved = &pending[i]
			break
		}
	}
	if preserved == nil {
		t.Fatalf("pre-takeover outbox missing after shared-home takeover: %#v", pending)
	}
	if preserved.Body != queued.Body || preserved.MachineID != machineA.ID || preserved.LeaseGeneration != first.Lease.Generation {
		t.Fatalf("pre-takeover outbox mutated: %#v", *preserved)
	}

	later, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:              "outbox:machine-b-after-takeover",
		SessionID:       "session-b",
		TurnID:          "turn-b",
		TeamsChatID:     "chat-1",
		ScopeID:         scope.ID,
		MachineID:       machineB.ID,
		LeaseGeneration: second.Lease.Generation,
		Kind:            "final",
		Body:            "queued after shared-home takeover",
	})
	if err != nil {
		t.Fatalf("QueueOutbox after takeover error: %v", err)
	}
	earlier, ok, err := store.EarlierUnsentOutbox(ctx, later)
	if err != nil {
		t.Fatalf("EarlierUnsentOutbox error: %v", err)
	}
	if !ok || earlier.ID != queued.ID {
		t.Fatalf("post-takeover outbox should remain ordered behind pre-takeover outbox; earlier=%#v ok=%v", earlier, ok)
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load final state error: %v", err)
	}
	if state.ControlLease.HolderMachineID != machineB.ID || state.ControlLease.Generation != second.Lease.Generation {
		t.Fatalf("final control lease = %#v, want machine B generation %d", state.ControlLease, second.Lease.Generation)
	}
	if state.Machines[machineA.ID].Status != MachineStatusStandby || state.Machines[machineB.ID].Status != MachineStatusActive {
		t.Fatalf("final machine statuses unexpected: %#v", state.Machines)
	}
	if owner, ok := state.readOwner(); !ok || owner.MachineID != machineB.ID || owner.LeaseGeneration != second.Lease.Generation {
		t.Fatalf("final owner = %#v ok=%v, want machine B generation %d", owner, ok, second.Lease.Generation)
	}
}

func TestTeamsBackgroundKeepaliveSameHolderRefreshPreservesGenerationCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	machine := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("first ClaimControlLease error: %v", err)
	}
	second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Minute, Now: now.Add(30 * time.Second)})
	if err != nil {
		t.Fatalf("refresh ClaimControlLease error: %v", err)
	}
	if second.Mode != LeaseModeActive || second.Lease.Generation != first.Lease.Generation {
		t.Fatalf("same holder refresh = %#v, want active generation %d", second, first.Lease.Generation)
	}
	if !second.Lease.LeaseUntil.After(first.Lease.LeaseUntil) {
		t.Fatalf("same holder refresh did not extend lease: before=%s after=%s", first.Lease.LeaseUntil, second.Lease.LeaseUntil)
	}
}

func TestTeamsBackgroundKeepaliveScopeIsolationRejectsSharedStateReuseCI(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	scopeA := ScopeIdentity{ID: "scope-a", AccountID: "user-a", OSUser: "alice", Profile: "default"}
	scopeB := ScopeIdentity{ID: "scope-b", AccountID: "user-b", OSUser: "bob", Profile: "default"}
	if _, err := store.RecordScope(ctx, scopeA); err != nil {
		t.Fatalf("RecordScope A error: %v", err)
	}
	if _, err := store.RecordScope(ctx, scopeB); err == nil || !strings.Contains(err.Error(), `belongs to scope "scope-a"`) {
		t.Fatalf("RecordScope B error = %v, want scope isolation guard", err)
	}
	_, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:   scopeB,
		Machine: MachineRecord{ID: "machine-b", ScopeID: scopeB.ID, Kind: MachineKindPrimary},
		Now:     time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
	})
	if err == nil || !strings.Contains(err.Error(), `belongs to scope "scope-a"`) {
		t.Fatalf("ClaimControlLease for scope B error = %v, want scope isolation guard", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.Scope.ID != scopeA.ID || len(state.Machines) != 0 {
		t.Fatalf("scope isolation failure mutated state: %#v", state)
	}
}

func TestRecordScopeSkipsSaveForUnchangedScope(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	scope := ScopeIdentity{ID: "scope-a", AccountID: "user-a", OSUser: "alice", Profile: "default"}
	if _, err := store.RecordScope(ctx, scope); err != nil {
		t.Fatalf("RecordScope initial error: %v", err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read store before unchanged scope: %v", err)
	}
	if _, err := store.RecordScope(ctx, scope); err != nil {
		t.Fatalf("RecordScope unchanged error: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read store after unchanged scope: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("RecordScope rewrote state for an unchanged scope claim")
	}

	if err := store.Update(ctx, func(state *State) error {
		state.Scope.CreatedAt = time.Time{}
		state.Scope.UpdatedAt = time.Time{}
		return nil
	}); err != nil {
		t.Fatalf("clear scope timestamps: %v", err)
	}
	legacyTimestamps, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read store with missing scope timestamps: %v", err)
	}
	if _, err := store.RecordScope(ctx, scope); err != nil {
		t.Fatalf("RecordScope timestamp backfill error: %v", err)
	}
	backfilled, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read store after scope timestamp backfill: %v", err)
	}
	if bytes.Equal(backfilled, legacyTimestamps) {
		t.Fatal("RecordScope did not rewrite state to backfill missing timestamps")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load backfilled scope: %v", err)
	}
	if state.Scope.CreatedAt.IsZero() || state.Scope.UpdatedAt.IsZero() {
		t.Fatalf("scope timestamps were not backfilled: %#v", state.Scope)
	}
	after = backfilled

	changed := scope
	changed.Profile = "work"
	if _, err := store.RecordScope(ctx, changed); err != nil {
		t.Fatalf("RecordScope changed metadata error: %v", err)
	}
	afterChanged, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read store after changed scope: %v", err)
	}
	if bytes.Equal(afterChanged, after) {
		t.Fatal("RecordScope did not rewrite state for changed scope metadata")
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load changed scope: %v", err)
	}
	if state.Scope.Profile != "work" {
		t.Fatalf("scope profile = %q, want work", state.Scope.Profile)
	}
}

func TestIsStaleHandlesFutureHeartbeatAndThresholdBoundary(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	staleAfter := time.Minute
	owner := OwnerMetadata{LastHeartbeat: now.Add(30 * time.Second)}
	if IsStale(owner, staleAfter, now) {
		t.Fatal("future heartbeat should not be stale")
	}
	owner.LastHeartbeat = now.Add(-staleAfter)
	if IsStale(owner, staleAfter, now) {
		t.Fatal("heartbeat exactly at stale threshold should not be stale")
	}
	owner.LastHeartbeat = now.Add(-staleAfter - time.Nanosecond)
	if !IsStale(owner, staleAfter, now) {
		t.Fatal("heartbeat older than stale threshold should be stale")
	}
}

func TestValidateAndReleaseControlLease(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	machine := MachineRecord{ID: "machine-primary", ScopeID: scope.ID, Kind: MachineKindPrimary}
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ClaimControlLease error: %v", err)
	}
	if _, err := store.ValidateControlLease(ctx, machine.ID, decision.Lease.Generation, now.Add(30*time.Second)); err != nil {
		t.Fatalf("ValidateControlLease held error: %v", err)
	}
	if _, err := store.ValidateControlLease(ctx, "other-machine", decision.Lease.Generation, now.Add(30*time.Second)); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("ValidateControlLease wrong holder error = %v, want ErrControlLeaseNotHeld", err)
	}
	released, err := store.ReleaseControlLeaseIfHolder(ctx, machine.ID, decision.Lease.Generation)
	if err != nil {
		t.Fatalf("ReleaseControlLeaseIfHolder error: %v", err)
	}
	if !released {
		t.Fatal("ReleaseControlLeaseIfHolder released = false, want true")
	}
	if _, err := store.ValidateControlLease(ctx, machine.ID, decision.Lease.Generation, now.Add(30*time.Second)); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("ValidateControlLease after release error = %v, want ErrControlLeaseNotHeld", err)
	}
}

func TestRecordOwnerHeartbeatUpdatesExistingOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	startedAt := testOwnerStart()
	firstHeartbeat := startedAt.Add(5 * time.Second)
	secondHeartbeat := startedAt.Add(20 * time.Second)
	owner := testOwner("session-1", "turn-1", startedAt)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, firstHeartbeat); err != nil {
		t.Fatalf("first RecordOwnerHeartbeat error: %v", err)
	}

	owner.ActiveSessionID = "session-2"
	owner.ActiveTurnID = "turn-2"
	owner.HelperVersion = "v-test.2"
	updated, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, secondHeartbeat)
	if err != nil {
		t.Fatalf("second RecordOwnerHeartbeat error: %v", err)
	}
	if updated.StartedAt != startedAt {
		t.Fatalf("StartedAt = %s, want %s", updated.StartedAt, startedAt)
	}
	if updated.LastHeartbeat != secondHeartbeat {
		t.Fatalf("LastHeartbeat = %s, want %s", updated.LastHeartbeat, secondHeartbeat)
	}
	if updated.ActiveSessionID != "session-2" || updated.ActiveTurnID != "turn-2" || updated.HelperVersion != "v-test.2" {
		t.Fatalf("updated owner payload mismatch: %#v", updated)
	}
}

func TestRecordOwnerHeartbeatTreatsExecRestartAsSameOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	startedAt := testOwnerStart()
	firstHeartbeat := startedAt.Add(5 * time.Second)
	secondHeartbeat := startedAt.Add(30 * time.Second)
	owner := testOwner("session-old", "turn-old", startedAt)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, firstHeartbeat); err != nil {
		t.Fatalf("first RecordOwnerHeartbeat error: %v", err)
	}

	execOwner := owner
	execOwner.StartedAt = startedAt.Add(20 * time.Second)
	execOwner.ActiveSessionID = ""
	execOwner.ActiveTurnID = ""
	updated, err := store.RecordOwnerHeartbeat(ctx, execOwner, time.Minute, secondHeartbeat)
	if err != nil {
		t.Fatalf("exec restart RecordOwnerHeartbeat error: %v", err)
	}
	if !updated.StartedAt.Equal(startedAt) {
		t.Fatalf("StartedAt = %s, want original %s", updated.StartedAt, startedAt)
	}
	if !updated.LastHeartbeat.Equal(secondHeartbeat) {
		t.Fatalf("LastHeartbeat = %s, want %s", updated.LastHeartbeat, secondHeartbeat)
	}
	if updated.ActiveSessionID != "" || updated.ActiveTurnID != "" {
		t.Fatalf("active owner fields = %q/%q, want cleared after restart", updated.ActiveSessionID, updated.ActiveTurnID)
	}
}

func TestRecordOwnerHeartbeatRefusesLiveOwnerWithDiagnostic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	existing := testOwner("session-live", "turn-live", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, existing, time.Minute, now); err != nil {
		t.Fatalf("existing RecordOwnerHeartbeat error: %v", err)
	}

	contender := testOwner("session-new", "turn-new", now.Add(10*time.Second))
	contender.PID = existing.PID + 1
	_, err := store.RecordOwnerHeartbeat(ctx, contender, time.Minute, now.Add(15*time.Second))
	if !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("RecordOwnerHeartbeat error = %v, want ErrOwnerLive", err)
	}
	msg := err.Error()
	for _, want := range []string{
		"pid=4242",
		`host="host-a"`,
		`executable="/usr/local/bin/codex-helper"`,
		`helper_version="v-test"`,
		`active_session_id="session-live"`,
		`active_turn_id="turn-live"`,
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("diagnostic %q missing from error: %s", want, msg)
		}
	}
	read, ok, err := store.ReadOwner(ctx)
	if err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	}
	if !ok {
		t.Fatal("ReadOwner ok = false")
	}
	if read.PID != existing.PID || read.ActiveSessionID != "session-live" {
		t.Fatalf("live owner was unexpectedly replaced: %#v", read)
	}
}

func TestRecordOwnerHeartbeatReplacesDeadLocalOwnerBeforeStale(t *testing.T) {
	prevHostname := ownerHostname
	prevAlive := ownerProcessAlive
	t.Cleanup(func() {
		ownerHostname = prevHostname
		ownerProcessAlive = prevAlive
	})
	ownerHostname = func() (string, error) { return "host-a", nil }
	ownerProcessAlive = func(pid int) bool { return pid != 4242 }

	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	existing := testOwner("session-live", "turn-live", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, existing, time.Minute, now); err != nil {
		t.Fatalf("existing RecordOwnerHeartbeat error: %v", err)
	}

	contender := testOwner("session-new", "turn-new", now.Add(10*time.Second))
	contender.PID = 5252
	recorded, err := store.RecordOwnerHeartbeat(ctx, contender, time.Minute, now.Add(15*time.Second))
	if err != nil {
		t.Fatalf("contender RecordOwnerHeartbeat error: %v", err)
	}
	if recorded.PID != contender.PID || recorded.ActiveSessionID != "session-new" || recorded.ActiveTurnID != "turn-new" {
		t.Fatalf("dead local owner was not replaced: %#v", recorded)
	}
}

func TestOwnerAppearsLocal(t *testing.T) {
	prevHostname := ownerHostname
	t.Cleanup(func() { ownerHostname = prevHostname })
	ownerHostname = func() (string, error) { return "host-a", nil }

	if !OwnerAppearsLocal(OwnerMetadata{Hostname: "host-a"}) {
		t.Fatal("owner on current host was not recognized as local")
	}
	if !OwnerAppearsLocal(OwnerMetadata{Hostname: " HOST-A "}) {
		t.Fatal("owner hostname comparison should ignore case and surrounding space")
	}
	if OwnerAppearsLocal(OwnerMetadata{Hostname: "host-b"}) {
		t.Fatal("owner on another host was recognized as local")
	}
	if OwnerAppearsLocal(OwnerMetadata{}) {
		t.Fatal("owner without hostname was recognized as local")
	}
}

func TestRecoverStaleOwnerReplacesStaleOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	startedAt := testOwnerStart()
	staleHeartbeat := startedAt.Add(10 * time.Second)
	staleOwner := testOwner("session-old", "turn-old", startedAt)
	if _, err := store.RecordOwnerHeartbeat(ctx, staleOwner, time.Minute, staleHeartbeat); err != nil {
		t.Fatalf("stale RecordOwnerHeartbeat error: %v", err)
	}

	now := staleHeartbeat.Add(2 * time.Minute)
	if !IsStale(staleOwner.withLastHeartbeat(staleHeartbeat), time.Minute, now) {
		t.Fatal("owner should be stale")
	}
	next := testOwner("session-new", "turn-new", now)
	next.PID = 5252
	recoveredOwner, recovered, err := store.RecoverStaleOwner(ctx, next, time.Minute, now)
	if err != nil {
		t.Fatalf("RecoverStaleOwner error: %v", err)
	}
	if !recovered {
		t.Fatal("RecoverStaleOwner recovered = false")
	}
	if recoveredOwner.PID != next.PID || recoveredOwner.ActiveSessionID != "session-new" || recoveredOwner.ActiveTurnID != "turn-new" {
		t.Fatalf("recovered owner mismatch: %#v", recoveredOwner)
	}
}

func TestRecoverStaleOwnerTreatsExecRestartAsSameOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	startedAt := testOwnerStart()
	firstHeartbeat := startedAt.Add(5 * time.Second)
	secondHeartbeat := startedAt.Add(30 * time.Second)
	owner := testOwner("session-old", "turn-old", startedAt)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, firstHeartbeat); err != nil {
		t.Fatalf("first RecordOwnerHeartbeat error: %v", err)
	}

	execOwner := owner
	execOwner.StartedAt = startedAt.Add(20 * time.Second)
	execOwner.ActiveSessionID = ""
	execOwner.ActiveTurnID = ""
	recoveredOwner, recovered, err := store.RecoverStaleOwner(ctx, execOwner, time.Minute, secondHeartbeat)
	if err != nil {
		t.Fatalf("RecoverStaleOwner exec restart error: %v", err)
	}
	if recovered {
		t.Fatal("RecoverStaleOwner recovered = true for same exec-restarted owner")
	}
	if !recoveredOwner.StartedAt.Equal(startedAt) {
		t.Fatalf("StartedAt = %s, want original %s", recoveredOwner.StartedAt, startedAt)
	}
	if !recoveredOwner.LastHeartbeat.Equal(secondHeartbeat) {
		t.Fatalf("LastHeartbeat = %s, want %s", recoveredOwner.LastHeartbeat, secondHeartbeat)
	}
	if recoveredOwner.ActiveSessionID != "" || recoveredOwner.ActiveTurnID != "" {
		t.Fatalf("active owner fields = %q/%q, want cleared after restart", recoveredOwner.ActiveSessionID, recoveredOwner.ActiveTurnID)
	}
}

func TestRecoverStaleOwnerReplacesDeadLocalOwnerBeforeStale(t *testing.T) {
	prevHostname := ownerHostname
	prevAlive := ownerProcessAlive
	t.Cleanup(func() {
		ownerHostname = prevHostname
		ownerProcessAlive = prevAlive
	})
	ownerHostname = func() (string, error) { return "host-a", nil }
	ownerProcessAlive = func(pid int) bool { return pid != 4242 }

	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	liveHeartbeat := now.Add(10 * time.Second)
	existing := testOwner("session-live", "turn-live", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, existing, time.Minute, liveHeartbeat); err != nil {
		t.Fatalf("existing RecordOwnerHeartbeat error: %v", err)
	}

	next := testOwner("session-new", "turn-new", now.Add(20*time.Second))
	next.PID = 5252
	recoveredOwner, recovered, err := store.RecoverStaleOwner(ctx, next, time.Minute, now.Add(30*time.Second))
	if err != nil {
		t.Fatalf("RecoverStaleOwner error: %v", err)
	}
	if !recovered {
		t.Fatal("RecoverStaleOwner recovered = false for dead local owner")
	}
	if recoveredOwner.PID != next.PID || recoveredOwner.ActiveSessionID != "session-new" || recoveredOwner.ActiveTurnID != "turn-new" {
		t.Fatalf("recovered owner mismatch: %#v", recoveredOwner)
	}
}

func TestRecoverStaleOwnerRefusesLiveOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	liveOwner := testOwner("session-live", "turn-live", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, liveOwner, time.Minute, now); err != nil {
		t.Fatalf("live RecordOwnerHeartbeat error: %v", err)
	}

	next := testOwner("session-new", "turn-new", now.Add(20*time.Second))
	next.PID = 5252
	_, recovered, err := store.RecoverStaleOwner(ctx, next, time.Minute, now.Add(30*time.Second))
	if !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("RecoverStaleOwner error = %v, want ErrOwnerLive", err)
	}
	if recovered {
		t.Fatal("RecoverStaleOwner recovered = true for live owner")
	}
}

func TestClearOwnerIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.ClearOwner(ctx); err != nil {
		t.Fatalf("first empty ClearOwner error: %v", err)
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after empty clear error: %v", err)
	} else if ok {
		t.Fatal("ReadOwner ok = true after empty clear")
	}
	now := testOwnerStart()
	if _, err := store.RecordOwnerHeartbeat(ctx, testOwner("session-1", "turn-1", now), time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	if err := store.ClearOwner(ctx); err != nil {
		t.Fatalf("ClearOwner error: %v", err)
	}
	if err := store.ClearOwner(ctx); err != nil {
		t.Fatalf("second ClearOwner error: %v", err)
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after clear error: %v", err)
	} else if ok {
		t.Fatal("ReadOwner ok = true after clear")
	}
}

func TestClearOwnerIfSameDoesNotClearDifferentOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	owner := testOwner("session-1", "turn-1", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	other := owner
	other.PID++
	cleared, err := store.ClearOwnerIfSame(ctx, other)
	if err != nil {
		t.Fatalf("ClearOwnerIfSame different owner error: %v", err)
	}
	if cleared {
		t.Fatal("ClearOwnerIfSame cleared different owner")
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after different clear error: %v", err)
	} else if !ok {
		t.Fatal("owner missing after different clear")
	}
	cleared, err = store.ClearOwnerIfSame(ctx, owner)
	if err != nil {
		t.Fatalf("ClearOwnerIfSame same owner error: %v", err)
	}
	if !cleared {
		t.Fatal("ClearOwnerIfSame did not clear same owner")
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after same clear error: %v", err)
	} else if ok {
		t.Fatal("owner still present after same clear")
	}
}

func TestClearOwnerIfSameDoesNotClearSameProcessDifferentInstance(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := testOwnerStart()
	owner := testOwner("session-1", "turn-1", now)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	other := owner
	other.StartedAt = now.Add(time.Minute)
	cleared, err := store.ClearOwnerIfSame(ctx, other)
	if err != nil {
		t.Fatalf("ClearOwnerIfSame different instance error: %v", err)
	}
	if cleared {
		t.Fatal("ClearOwnerIfSame cleared same pid/executable with different started_at")
	}
	if _, ok, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("ReadOwner after different instance clear error: %v", err)
	} else if !ok {
		t.Fatal("owner missing after different instance clear")
	}
}

func TestReplaceFileWithRetryRetriesRetryableErrors(t *testing.T) {
	retryErr := errors.New("sharing violation")
	attempts := 0
	err := replaceFileWithRetry("tmp", "state.json", func(string, string) error {
		attempts++
		if attempts < 3 {
			return retryErr
		}
		return nil
	}, func(err error) bool {
		return errors.Is(err, retryErr)
	})
	if err != nil {
		t.Fatalf("replaceFileWithRetry error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestReplaceFileWithRetryStopsOnPermanentError(t *testing.T) {
	permanentErr := errors.New("permission denied")
	attempts := 0
	err := replaceFileWithRetry("tmp", "state.json", func(string, string) error {
		attempts++
		return permanentErr
	}, func(error) bool { return false })
	if !errors.Is(err, permanentErr) {
		t.Fatalf("replaceFileWithRetry error = %v, want permanent error", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 for permanent error", attempts)
	}
}

func TestReplaceFileWithRetryBoundsRetryableErrors(t *testing.T) {
	retryErr := errors.New("sharing violation")
	attempts := 0
	err := replaceFileWithRetry("tmp", "state.json", func(string, string) error {
		attempts++
		return retryErr
	}, func(err error) bool {
		return errors.Is(err, retryErr)
	})
	if !errors.Is(err, retryErr) {
		t.Fatalf("replaceFileWithRetry error = %v, want retry error", err)
	}
	if attempts != durableReplaceAttempts {
		t.Fatalf("attempts = %d, want bounded attempts %d", attempts, durableReplaceAttempts)
	}
}

func TestAtomicWriteFileUsesTempAndCleansFailedReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	replaceErr := errors.New("replace failed")
	prev := durableReplaceFile
	t.Cleanup(func() { durableReplaceFile = prev })
	var tempPath string
	durableReplaceFile = func(src string, dst string) error {
		tempPath = src
		if dst != path {
			t.Fatalf("replace dst = %q, want %q", dst, path)
		}
		if filepath.Dir(src) != filepath.Dir(path) {
			t.Fatalf("replace src dir = %q, want %q", filepath.Dir(src), filepath.Dir(path))
		}
		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("read temp during replace: %v", err)
		}
		if string(data) != "new state" {
			t.Fatalf("temp data = %q, want new state", data)
		}
		return replaceErr
	}

	err := atomicWriteFile(path, []byte("new state"), 0o600)
	if !errors.Is(err, replaceErr) {
		t.Fatalf("atomicWriteFile error = %v, want replace error", err)
	}
	if tempPath == "" {
		t.Fatal("durableReplaceFile was not called")
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file still exists after replace failure: stat err=%v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target should not exist after failed replace: stat err=%v", err)
	}
}

func seedLargeMessageLookupState(t testing.TB, store *Store, inboundCount int, provenanceCount int, outboxCount int) {
	t.Helper()
	ctx := context.Background()
	base := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < inboundCount; i++ {
			chatID := largeInboundChatID(i)
			messageID := largeInboundMessageID(i)
			event := InboundEvent{
				ID:             inboundID(chatID, messageID),
				SessionID:      fmt.Sprintf("session-%03d", i%128),
				TeamsChatID:    chatID,
				TeamsMessageID: messageID,
				Text:           fmt.Sprintf("inbound body %05d", i),
				TextHash:       fmt.Sprintf("inbound-hash-%05d", i),
				Source:         "teams",
				Status:         InboundStatusPersisted,
				ReceivedAt:     base.Add(time.Duration(i) * time.Second),
				CreatedAt:      base.Add(time.Duration(i) * time.Second),
				UpdatedAt:      base.Add(time.Duration(i) * time.Second),
			}
			state.InboundEvents[event.ID] = event
			recordMessageProvenanceLocked(state, MessageProvenanceRecord{
				TeamsChatID:    event.TeamsChatID,
				TeamsMessageID: event.TeamsMessageID,
				Origin:         MessageOriginUserInbound,
				SessionID:      event.SessionID,
				InboundID:      event.ID,
				Kind:           string(event.Status),
				RenderedHash:   event.TextHash,
				CreatedAt:      event.CreatedAt,
				UpdatedAt:      event.UpdatedAt,
			}, time.Time{})
		}
		for i := 0; i < provenanceCount; i++ {
			origin := MessageOriginUserInbound
			chatID := "provenance-user-chat"
			messageID := largeUserProvenanceMessageID(i)
			if i%2 == 1 {
				origin = MessageOriginHelperOutbox
				chatID = "provenance-helper-chat"
				messageID = largeHelperProvenanceMessageID(i)
			}
			recordMessageProvenanceLocked(state, MessageProvenanceRecord{
				TeamsChatID:    chatID,
				TeamsMessageID: messageID,
				Origin:         origin,
				SessionID:      fmt.Sprintf("provenance-session-%03d", i%128),
				TurnID:         fmt.Sprintf("provenance-turn-%05d", i),
				OutboxID:       fmt.Sprintf("provenance-outbox-%05d", i),
				InboundID:      fmt.Sprintf("provenance-inbound-%05d", i),
				Kind:           fmt.Sprintf("kind-%03d", i%32),
				RenderedHash:   fmt.Sprintf("provenance-hash-%05d", i),
				CreatedAt:      base.Add(2*time.Hour + time.Duration(i)*time.Second),
				UpdatedAt:      base.Add(2*time.Hour + time.Duration(i)*time.Second),
			}, time.Time{})
		}
		for i := 0; i < outboxCount; i++ {
			status := OutboxStatusSent
			if i%2 == 0 {
				status = OutboxStatusQueued
			}
			id := fmt.Sprintf("large-outbox-%05d", i)
			state.OutboxMessages[id] = OutboxMessage{
				ID:             id,
				TeamsChatID:    "outbox-chat",
				TeamsMessageID: largeOutboxMessageID(i),
				Kind:           "answer",
				Body:           fmt.Sprintf("outbox body %05d", i),
				Status:         status,
				CreatedAt:      base.Add(4*time.Hour + time.Duration(i)*time.Second),
				UpdatedAt:      base.Add(4*time.Hour + time.Duration(i)*time.Second),
				SentAt:         base.Add(4*time.Hour + time.Duration(i)*time.Second),
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed large message lookup state: %v", err)
	}
}

func largeInboundChatID(i int) string {
	return fmt.Sprintf("inbound-chat-%03d", i%97)
}

func largeInboundMessageID(i int) string {
	return fmt.Sprintf("inbound-message-%05d", i)
}

func largeUserProvenanceMessageID(i int) string {
	return fmt.Sprintf("user-provenance-message-%05d", i)
}

func largeHelperProvenanceMessageID(i int) string {
	return fmt.Sprintf("helper-provenance-message-%05d", i)
}

func largeOutboxMessageID(i int) string {
	return fmt.Sprintf("outbox-message-%05d", i)
}

func legacyMessageLookupFromState(state State, chatID string, teamsMessageID string) MessageLookup {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	var out MessageLookup
	for _, record := range state.MessageProvenance {
		if strings.TrimSpace(record.TeamsChatID) != chatID || strings.TrimSpace(record.TeamsMessageID) != teamsMessageID {
			continue
		}
		out.Provenance = record
		out.HasProvenance = true
		switch strings.TrimSpace(record.Origin) {
		case MessageOriginUserInbound:
			out.HasInbound = true
		case MessageOriginHelperOutbox:
			out.HasDeliveredOutbox = true
		}
		break
	}
	for _, event := range state.InboundEvents {
		if strings.TrimSpace(event.TeamsChatID) == chatID && strings.TrimSpace(event.TeamsMessageID) == teamsMessageID {
			out.HasInbound = true
			break
		}
	}
	for _, msg := range state.OutboxMessages {
		if strings.TrimSpace(msg.TeamsChatID) != chatID || strings.TrimSpace(msg.TeamsMessageID) != teamsMessageID {
			continue
		}
		switch msg.Status {
		case OutboxStatusAccepted, OutboxStatusSent:
			out.HasDeliveredOutbox = true
		}
		break
	}
	return out
}

func messageLookupEqual(left MessageLookup, right MessageLookup) bool {
	return left.HasProvenance == right.HasProvenance &&
		left.HasInbound == right.HasInbound &&
		left.HasDeliveredOutbox == right.HasDeliveredOutbox &&
		left.Provenance.ID == right.Provenance.ID &&
		left.Provenance.TeamsChatID == right.Provenance.TeamsChatID &&
		left.Provenance.TeamsMessageID == right.Provenance.TeamsMessageID &&
		left.Provenance.Origin == right.Provenance.Origin &&
		left.Provenance.InboundID == right.Provenance.InboundID &&
		left.Provenance.OutboxID == right.Provenance.OutboxID
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "teams-state", "state.json"))
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close test store: %v", err)
		}
	})
	return store
}

func writeRawStoreStateForTest(t *testing.T, store *Store, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatalf("write raw state: %v", err)
	}
}

func seedLegacyStateFileForSQLiteMigrationTest(t *testing.T, store *Store) {
	t.Helper()
	if _, err := store.SetPaused(context.Background(), true, "seed sqlite migration"); err != nil {
		t.Fatalf("seed legacy state before sqlite migration: %v", err)
	}
}

func seedComplexLegacyStateForSQLiteMigrationTest(t *testing.T, store *Store) {
	t.Helper()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *State) error {
		state.ServiceControl = ServiceControl{Paused: true, Draining: true, Reason: "upgrade", UpdatedAt: now}
		state.ControlChat = ControlChatBinding{TeamsChatID: "control-chat", TeamsChatURL: "https://teams.example/control", TeamsChatTopic: "control", UpdatedAt: now}
		for i := 0; i < 4; i++ {
			sessionID := fmt.Sprintf("upgrade-session-%02d", i)
			chatID := fmt.Sprintf("upgrade-chat-%02d", i)
			state.Sessions[sessionID] = SessionContext{
				ID:            sessionID,
				Status:        SessionStatusActive,
				TeamsChatID:   chatID,
				TeamsChatURL:  "https://teams.example/" + chatID,
				TeamsTopic:    "upgrade topic",
				CodexThreadID: fmt.Sprintf("thread-upgrade-%02d", i),
				Cwd:           fmt.Sprintf("/workspace/upgrade-%02d", i),
				CreatedAt:     now,
				UpdatedAt:     now,
			}
			state.ChatPolls[chatID] = ChatPollState{ChatID: chatID, Seeded: true, PollState: "warm", NextPollAt: now.Add(time.Minute), LastSuccessfulPollAt: now.Add(-time.Minute), UpdatedAt: now}
			state.ChatRateLimits[chatID] = ChatRateLimitState{ChatID: chatID, BlockedUntil: now.Add(time.Minute), Reason: "seeded"}
			for j := 0; j < 3; j++ {
				inboundID := fmt.Sprintf("upgrade-inbound-%02d-%02d", i, j)
				turnID := fmt.Sprintf("upgrade-turn-%02d-%02d", i, j)
				messageID := fmt.Sprintf("upgrade-message-seed-%02d-%02d", i, j)
				status := TurnStatusCompleted
				if j == 2 {
					status = TurnStatusQueued
				}
				state.InboundEvents[inboundID] = InboundEvent{
					ID:             inboundID,
					SessionID:      sessionID,
					TeamsChatID:    chatID,
					TeamsMessageID: messageID,
					Source:         "teams",
					Status:         InboundStatusQueued,
					TurnID:         turnID,
					Text:           strings.Repeat(fmt.Sprintf("seed prompt %02d %02d ", i, j), 16),
					CreatedAt:      now.Add(time.Duration(i*10+j) * time.Second),
					UpdatedAt:      now.Add(time.Duration(i*10+j) * time.Second),
				}
				state.Turns[turnID] = Turn{
					ID:             turnID,
					SessionID:      sessionID,
					InboundEventID: inboundID,
					Status:         status,
					QueuedAt:       now.Add(time.Duration(i*10+j) * time.Second),
					CreatedAt:      now.Add(time.Duration(i*10+j) * time.Second),
					UpdatedAt:      now.Add(time.Duration(i*10+j) * time.Second),
				}
				state.MessageProvenance[fmt.Sprintf("upgrade-prov-in-%02d-%02d", i, j)] = MessageProvenanceRecord{
					ID:             fmt.Sprintf("upgrade-prov-in-%02d-%02d", i, j),
					TeamsChatID:    chatID,
					TeamsMessageID: messageID,
					Origin:         MessageOriginUserInbound,
					SessionID:      sessionID,
					InboundID:      inboundID,
					CreatedAt:      now,
					UpdatedAt:      now,
				}
				if j < 2 {
					outboxID := fmt.Sprintf("upgrade-seed-outbox-%02d-%02d", i, j)
					teamsMessageID := fmt.Sprintf("upgrade-helper-message-%02d-%02d", i, j)
					state.OutboxMessages[outboxID] = OutboxMessage{
						ID:             outboxID,
						SessionID:      sessionID,
						TurnID:         turnID,
						TeamsChatID:    chatID,
						TeamsMessageID: teamsMessageID,
						Kind:           "final",
						Body:           strings.Repeat("seed answer ", 12),
						Status:         OutboxStatusSent,
						Sequence:       int64(j + 1),
						CreatedAt:      now,
						UpdatedAt:      now,
						SentAt:         now,
					}
					state.MessageProvenance[fmt.Sprintf("upgrade-prov-out-%02d-%02d", i, j)] = MessageProvenanceRecord{
						ID:             fmt.Sprintf("upgrade-prov-out-%02d-%02d", i, j),
						TeamsChatID:    chatID,
						TeamsMessageID: teamsMessageID,
						Origin:         MessageOriginHelperOutbox,
						SessionID:      sessionID,
						OutboxID:       outboxID,
						CreatedAt:      now,
						UpdatedAt:      now,
					}
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed complex legacy state before sqlite migration: %v", err)
	}
}

func withSQLiteMigrationTestHook(t *testing.T, hook func(stage string) error) {
	t.Helper()
	prev := sqliteMigrationTestHook
	sqliteMigrationTestHook = hook
	t.Cleanup(func() {
		sqliteMigrationTestHook = prev
	})
}

func migrateStoreToSQLiteForTest(t *testing.T, store *Store) StoreSQLiteMigrationResult {
	t.Helper()
	result, err := store.MigrateLargeStateToSQLite(context.Background(), 0)
	if err != nil {
		t.Fatalf("MigrateLargeStateToSQLite error: %v", err)
	}
	if !result.Migrated && !result.AlreadyDB {
		t.Fatalf("MigrateLargeStateToSQLite result = %#v, want migrated or already DB", result)
	}
	return result
}

func writeSQLitePointerForTest(t *testing.T, store *Store, path string) {
	t.Helper()
	pointer := storeSQLitePointer{
		SchemaVersion:       storeSQLitePointerSchemaVersion,
		StorageBackend:      storeSQLiteBackend,
		StorageVersion:      storeSQLiteVersion,
		Path:                path,
		MigrationID:         "test-migration",
		SourceSchemaVersion: SchemaVersion,
		CreatedAt:           time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC),
	}
	if err := store.writeSQLitePointerUnlocked(pointer); err != nil {
		t.Fatalf("write sqlite pointer: %v", err)
	}
}

func legacyV5LoadStateDataForTest(data []byte) (State, error) {
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, err
	}
	if state.SchemaVersion >= 0 && state.SchemaVersion < SchemaVersion {
		state = migrateStateToCurrent(state)
		return state, nil
	}
	if state.SchemaVersion != SchemaVersion {
		return State{}, &UnsupportedSchemaVersionError{Version: state.SchemaVersion}
	}
	normalizeLoadedState(&state)
	return state, nil
}

func holdSessionLockForTest(t *testing.T, store *Store, sessionID string) func() {
	t.Helper()
	release := make(chan struct{})
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- store.withSessionLock(context.Background(), sessionID, func() error {
			close(ready)
			<-release
			return nil
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("hold session lock %q error: %v", sessionID, err)
	case <-time.After(time.Second):
		t.Fatalf("timed out acquiring session lock %q", sessionID)
	}
	released := false
	return func() {
		if released {
			return
		}
		released = true
		close(release)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("release session lock %q error: %v", sessionID, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out releasing session lock %q", sessionID)
		}
	}
}

func newBenchmarkStore(b *testing.B) *Store {
	b.Helper()
	store, err := Open(filepath.Join(b.TempDir(), "teams-state", "state.json"))
	if err != nil {
		b.Fatalf("Open error: %v", err)
	}
	return store
}

func testSession() SessionContext {
	return SessionContext{
		ID:            "s1",
		TeamsChatID:   "chat-1",
		TeamsChatURL:  "https://teams.example/chat-1",
		TeamsTopic:    "topic",
		RunnerKind:    "exec",
		CodexVersion:  "1.2.3",
		Cwd:           "/workspace/project",
		CodexHome:     "/home/user/.codex",
		Profile:       "default",
		Model:         "gpt-test",
		Sandbox:       "workspace-write",
		ProxyMode:     "on",
		YoloMode:      "off",
		CodexThreadID: "thread-0",
	}
}

func testInbound() InboundEvent {
	return InboundEvent{
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Source:         "teams",
	}
}

func testOwnerStart() time.Time {
	return time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
}

func testOwner(activeSessionID string, activeTurnID string, startedAt time.Time) OwnerMetadata {
	return OwnerMetadata{
		PID:             4242,
		Hostname:        "host-a",
		ExecutablePath:  "/usr/local/bin/codex-helper",
		HelperVersion:   "v-test",
		StartedAt:       startedAt,
		ActiveSessionID: activeSessionID,
		ActiveTurnID:    activeTurnID,
	}
}

func sameControl(a ServiceControl, b ServiceControl) bool {
	return a.Paused == b.Paused &&
		a.Draining == b.Draining &&
		a.Reason == b.Reason &&
		a.UpdatedAt.Equal(b.UpdatedAt)
}

func (owner OwnerMetadata) withLastHeartbeat(lastHeartbeat time.Time) OwnerMetadata {
	owner.LastHeartbeat = lastHeartbeat
	return owner
}
