package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
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

func TestLoadPathReadOnlyJSONDoesNotCreateLockOrModifyFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := newState()
	state.Sessions["s001"] = SessionContext{ID: "s001", Status: SessionStatusActive, TeamsChatID: "chat-1"}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("Marshal state: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o640); err != nil {
		t.Fatalf("write JSON store: %v", err)
	}
	mtime := time.Unix(1_700_000_000, 123_000_000)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("Chtimes JSON store: %v", err)
	}
	before := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(path))

	loaded, err := LoadPathReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("LoadPathReadOnly JSON: %v", err)
	}
	if loaded.Sessions["s001"].TeamsChatID != "chat-1" {
		t.Fatalf("read-only JSON state = %#v", loaded.Sessions)
	}
	after := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(path))
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("read-only JSON load changed files:\nbefore=%#v\nafter=%#v", before, after)
	}
	if _, err := os.Stat(path + ".lock"); !os.IsNotExist(err) {
		t.Fatalf("read-only JSON load created lock file: %v", err)
	}
}

func TestLoadPathReadOnlySQLiteDoesNotModifyDatabaseFamily(t *testing.T) {
	store := newTestStore(t)
	if err := store.Update(context.Background(), func(state *State) error {
		state.Sessions["s001"] = SessionContext{ID: "s001", Status: SessionStatusActive, TeamsChatID: "chat-1"}
		state.ChatPolls["chat-1"] = ChatPollState{ChatID: "chat-1", PollState: "warm", Seeded: true}
		return nil
	}); err != nil {
		t.Fatalf("seed SQLite fixture: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close SQLite fixture: %v", err)
	}
	before := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(path))
	for i := 0; i < 5; i++ {
		loaded, err := LoadPathReadOnly(context.Background(), path)
		if err != nil {
			t.Fatalf("LoadPathReadOnly SQLite iteration %d: %v", i, err)
		}
		if loaded.Sessions["s001"].TeamsChatID != "chat-1" || loaded.ChatPolls["chat-1"].PollState != "warm" {
			t.Fatalf("read-only SQLite state iteration %d = sessions:%#v polls:%#v", i, loaded.Sessions, loaded.ChatPolls)
		}
	}
	after := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(path))
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("read-only SQLite load changed files:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestLoadPathReadOnlySQLiteSeesLiveWALWithoutModifyingPersistentFiles(t *testing.T) {
	store := newTestStore(t)
	if err := store.Update(context.Background(), func(state *State) error {
		state.Sessions["s001"] = SessionContext{ID: "s001", Status: SessionStatusActive, TeamsChatID: "chat-before"}
		return nil
	}); err != nil {
		t.Fatalf("seed SQLite fixture: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	if err := store.Update(context.Background(), func(state *State) error {
		session := state.Sessions["s001"]
		session.TeamsChatID = "chat-in-wal"
		state.Sessions["s001"] = session
		return nil
	}); err != nil {
		t.Fatalf("update live SQLite fixture: %v", err)
	}
	pointer, ok, err := store.currentSQLitePointerUnlocked()
	if err != nil || !ok {
		t.Fatalf("current SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	walInfo, err := os.Stat(dbPath + "-wal")
	if err != nil {
		t.Fatalf("stat live WAL: %v", err)
	}
	if walInfo.Size() == 0 {
		t.Fatal("live WAL is empty; fixture did not exercise uncheckpointed state")
	}
	before := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(dbPath))
	if _, ok := before[filepath.Base(dbPath)+"-shm"]; !ok {
		t.Fatal("live fixture has no SHM; read-only WAL test requires an existing shared-memory index")
	}
	loaded, err := LoadPathReadOnly(context.Background(), store.Path())
	if err != nil {
		t.Fatalf("LoadPathReadOnly live SQLite: %v", err)
	}
	if got := loaded.Sessions["s001"].TeamsChatID; got != "chat-in-wal" {
		t.Fatalf("live SQLite chat = %q, want uncheckpointed WAL value", got)
	}
	after := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(dbPath))
	// A WAL reader updates transient read marks in the pre-existing SHM index.
	// Those bytes are coordination state, not durable store data. Main DB, WAL,
	// pointer, lock, and backup files must remain byte-for-byte unchanged.
	delete(before, filepath.Base(dbPath)+"-shm")
	delete(after, filepath.Base(dbPath)+"-shm")
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("read-only live SQLite load changed files:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestLoadPathReadOnlySQLiteRetriesWhenWALAppearsDuringImmutableRead(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s001"] = SessionContext{ID: "s001", Status: SessionStatusActive, TeamsChatID: "chat-before"}
		return nil
	}); err != nil {
		t.Fatalf("seed SQLite fixture: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	if _, err := store.CheckpointSQLiteWAL(ctx, 0); err != nil {
		t.Fatalf("CheckpointSQLiteWAL: %v", err)
	}
	pointer, ok, err := store.currentSQLitePointerUnlocked()
	if err != nil || !ok {
		t.Fatalf("current SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	hookCalls := 0
	loaded, err := loadSQLiteStateFileReadOnlyWithHook(ctx, dbPath, func(attempt int, immutable bool) {
		if attempt != 0 || hookCalls != 0 {
			return
		}
		hookCalls++
		if !immutable {
			t.Fatal("first diagnostic attempt should be immutable after checkpoint")
		}
		if updateErr := store.Update(ctx, func(state *State) error {
			session := state.Sessions["s001"]
			session.TeamsChatID = "chat-raced-into-wal"
			state.Sessions["s001"] = session
			return nil
		}); updateErr != nil {
			t.Fatalf("write WAL during diagnostic read: %v", updateErr)
		}
	})
	if err != nil {
		t.Fatalf("load raced SQLite snapshot: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("snapshot hook calls = %d, want 1", hookCalls)
	}
	if got := loaded.Sessions["s001"].TeamsChatID; got != "chat-raced-into-wal" {
		t.Fatalf("raced SQLite chat = %q, want latest WAL value", got)
	}
}

func TestLoadPathReadOnlySQLiteLiveWALWithoutSHMFailsWithoutCreatingSidecar(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "store.sqlite")
	if err := os.WriteFile(dbPath, []byte("database fixture"), 0o600); err != nil {
		t.Fatalf("write database fixture: %v", err)
	}
	if err := os.WriteFile(dbPath+"-wal", []byte("uncheckpointed WAL"), 0o600); err != nil {
		t.Fatalf("write WAL fixture: %v", err)
	}
	_, err := loadSQLiteStateFileReadOnly(context.Background(), dbPath)
	if err == nil || !strings.Contains(err.Error(), "without creating SHM") {
		t.Fatalf("missing-SHM live WAL error = %v", err)
	}
	if _, statErr := os.Stat(dbPath + "-shm"); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("read-only live WAL check created SHM: %v", statErr)
	}
}

type readOnlyFileSnapshot struct {
	Mode    os.FileMode
	Size    int64
	ModTime int64
	SHA256  string
}

func snapshotRegularFilesForReadOnlyTest(t *testing.T, dir string) map[string]readOnlyFileSnapshot {
	t.Helper()
	out := make(map[string]readOnlyFileSnapshot)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("Info %s: %v", path, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		out[entry.Name()] = readOnlyFileSnapshot{Mode: info.Mode(), Size: info.Size(), ModTime: info.ModTime().UnixNano(), SHA256: sha256Bytes(data)}
	}
	return out
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
	defer reopened.Close()
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

func TestQueueTurnPinsSessionModelProfileSnapshot(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			snapshot := modelprofile.Snapshot{
				Name:               "mimo25",
				Provider:           "mimo",
				APIKeyRef:          "secret:model-profile/mimo25/api-key",
				Revision:           3,
				KeyFingerprint:     "key:test",
				BaseURLHash:        "url:test",
				CatalogFingerprint: "catalog:test",
			}
			session := testSession()
			session.ModelProfile = snapshot
			if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			inbound, created, err := store.PersistInbound(ctx, testInbound())
			if err != nil || !created {
				t.Fatalf("PersistInbound created=%v err=%v", created, err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			turn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
			if err != nil || !created {
				t.Fatalf("QueueTurn created=%v err=%v", created, err)
			}
			if turn.ModelProfile != snapshot {
				t.Fatalf("queued turn model profile = %#v, want %#v", turn.ModelProfile, snapshot)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := state.Turns[turn.ID].ModelProfile; got != snapshot {
				t.Fatalf("stored turn model profile = %#v, want %#v", got, snapshot)
			}
		})
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
	defer reopened.Close()
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

func TestRecentSessionInboundTurnSnapshotSQLiteFiltersOldEvents(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	since := now.Add(-3*time.Minute + 500*time.Millisecond)
	recentReceivedAt := since.Add(250 * time.Millisecond)
	oldReceivedAt := since.Add(-250 * time.Millisecond)
	session := testSession()
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		recentInbound := testInbound()
		recentInbound.ID = "recent-inbound"
		recentInbound.TurnID = "recent-turn"
		recentInbound.CreatedAt = now.Add(-time.Minute)
		recentInbound.UpdatedAt = recentInbound.CreatedAt
		state.InboundEvents[recentInbound.ID] = recentInbound
		state.Turns["recent-turn"] = Turn{ID: "recent-turn", SessionID: session.ID, InboundEventID: recentInbound.ID, Status: TurnStatusCompleted, CreatedAt: recentInbound.CreatedAt, UpdatedAt: recentInbound.CreatedAt}

		recentReceivedInbound := testInbound()
		recentReceivedInbound.ID = "recent-received-inbound"
		recentReceivedInbound.TeamsMessageID = "recent-received-message"
		recentReceivedInbound.TurnID = "recent-received-turn"
		recentReceivedInbound.ReceivedAt = recentReceivedAt
		recentReceivedInbound.CreatedAt = now.Add(-10 * time.Minute)
		recentReceivedInbound.UpdatedAt = recentReceivedInbound.CreatedAt
		state.InboundEvents[recentReceivedInbound.ID] = recentReceivedInbound
		state.Turns["recent-received-turn"] = Turn{ID: "recent-received-turn", SessionID: session.ID, InboundEventID: recentReceivedInbound.ID, Status: TurnStatusCompleted, CreatedAt: recentReceivedInbound.CreatedAt, UpdatedAt: recentReceivedInbound.CreatedAt}

		oldReceivedInbound := testInbound()
		oldReceivedInbound.ID = "old-received-inbound"
		oldReceivedInbound.TeamsMessageID = "old-received-message"
		oldReceivedInbound.TurnID = "old-received-turn"
		oldReceivedInbound.ReceivedAt = oldReceivedAt
		oldReceivedInbound.CreatedAt = now.Add(-10 * time.Minute)
		oldReceivedInbound.UpdatedAt = now.Add(-time.Minute)
		state.InboundEvents[oldReceivedInbound.ID] = oldReceivedInbound
		state.Turns["old-received-turn"] = Turn{ID: "old-received-turn", SessionID: session.ID, InboundEventID: oldReceivedInbound.ID, Status: TurnStatusCompleted, CreatedAt: oldReceivedInbound.CreatedAt, UpdatedAt: oldReceivedInbound.UpdatedAt}

		oldInbound := testInbound()
		oldInbound.ID = "old-inbound"
		oldInbound.TeamsMessageID = "old-message"
		oldInbound.TurnID = "old-turn"
		oldInbound.CreatedAt = now.Add(-10 * time.Minute)
		oldInbound.UpdatedAt = oldInbound.CreatedAt
		state.InboundEvents[oldInbound.ID] = oldInbound
		state.Turns["old-turn"] = Turn{ID: "old-turn", SessionID: session.ID, InboundEventID: oldInbound.ID, Status: TurnStatusCompleted, CreatedAt: oldInbound.CreatedAt, UpdatedAt: oldInbound.CreatedAt}

		otherInbound := testInbound()
		otherInbound.ID = "other-inbound"
		otherInbound.SessionID = "other-session"
		otherInbound.TeamsChatID = "other-chat"
		otherInbound.TeamsMessageID = "other-message"
		otherInbound.TurnID = "other-turn"
		otherInbound.CreatedAt = now.Add(-time.Minute)
		otherInbound.UpdatedAt = otherInbound.CreatedAt
		state.InboundEvents[otherInbound.ID] = otherInbound
		state.Turns["other-turn"] = Turn{ID: "other-turn", SessionID: otherInbound.SessionID, InboundEventID: otherInbound.ID, Status: TurnStatusCompleted, CreatedAt: otherInbound.CreatedAt, UpdatedAt: otherInbound.CreatedAt}
		return nil
	}); err != nil {
		t.Fatalf("seed recent inbound snapshot state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("sqlite pointer not available")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `UPDATE inbound_events SET received_at = ? WHERE id = ?`, sqliteTime(recentReceivedAt.Truncate(time.Second)), "recent-received-inbound"); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `UPDATE inbound_events SET received_at = ? WHERE id = ?`, sqliteTime(oldReceivedAt.Truncate(time.Second)), "old-received-inbound"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("simulate legacy received_at backfill precision: %v", err)
	}

	snapshot, err := store.RecentSessionInboundTurnSnapshot(ctx, session.ID, since)
	if err != nil {
		t.Fatalf("RecentSessionInboundTurnSnapshot sqlite error: %v", err)
	}
	if snapshot.InboundEvents["recent-inbound"].ID == "" || snapshot.Turns["recent-turn"].ID == "" {
		t.Fatalf("recent snapshot missing recent inbound/turn: %#v %#v", snapshot.InboundEvents, snapshot.Turns)
	}
	if snapshot.InboundEvents["recent-received-inbound"].ID == "" || snapshot.Turns["recent-received-turn"].ID == "" {
		t.Fatalf("recent snapshot missing received-at recent inbound/turn: %#v %#v", snapshot.InboundEvents, snapshot.Turns)
	}
	if snapshot.InboundEvents["old-inbound"].ID != "" || snapshot.Turns["old-turn"].ID != "" {
		t.Fatalf("recent snapshot included old inbound/turn: %#v %#v", snapshot.InboundEvents, snapshot.Turns)
	}
	if snapshot.InboundEvents["old-received-inbound"].ID != "" || snapshot.Turns["old-received-turn"].ID != "" {
		t.Fatalf("recent snapshot included received-at old inbound/turn: %#v %#v", snapshot.InboundEvents, snapshot.Turns)
	}
	if snapshot.InboundEvents["other-inbound"].ID != "" || snapshot.Turns["other-turn"].ID != "" {
		t.Fatalf("recent snapshot included other session: %#v %#v", snapshot.InboundEvents, snapshot.Turns)
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

func TestSQLiteOpenMigratesLegacyChatPollColumns(t *testing.T) {
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
	if err := ensureSQLiteSchema(db); err != nil {
		_ = db.Close()
		t.Fatalf("ensure sqlite schema: %v", err)
	}
	if _, err := db.Exec(`DROP TABLE chat_polls`); err != nil {
		_ = db.Close()
		t.Fatalf("drop current chat_polls: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE chat_polls (chat_id TEXT PRIMARY KEY, next_poll_at INTEGER, poll_state TEXT, updated_at INTEGER, json BLOB NOT NULL)`); err != nil {
		_ = db.Close()
		t.Fatalf("create legacy chat_polls: %v", err)
	}
	now := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	legacyPoll := ChatPollState{
		ChatID:           "chat-legacy",
		Seeded:           true,
		PollState:        "parked",
		NextPollAt:       now.Add(time.Hour),
		ParkedAt:         now.Add(-48 * time.Hour),
		ParkNoticeSentAt: now.Add(-47 * time.Hour),
		UpdatedAt:        now,
	}
	legacyJSON, err := json.Marshal(legacyPoll)
	if err != nil {
		_ = db.Close()
		t.Fatalf("marshal legacy poll: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO chat_polls(chat_id, next_poll_at, poll_state, updated_at, json) VALUES (?, ?, ?, ?, ?)`,
		legacyPoll.ChatID, sqliteTime(legacyPoll.NextPollAt), legacyPoll.PollState, sqliteTime(legacyPoll.UpdatedAt), legacyJSON); err != nil {
		_ = db.Close()
		t.Fatalf("insert legacy poll: %v", err)
	}
	cold, err := json.Marshal(coldSQLiteState(newState()))
	if err != nil {
		_ = db.Close()
		t.Fatalf("marshal cold state: %v", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO state_meta(key, value) VALUES ('state_json', ?)`, cold); err != nil {
		_ = db.Close()
		t.Fatalf("insert state meta: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy sqlite db: %v", err)
	}
	writeSQLitePointerForTest(t, store, storeSQLiteFileName)

	if _, _, err := store.HotPollScheduleSnapshot(ctx); err != nil {
		t.Fatalf("HotPollScheduleSnapshot should migrate legacy chat_polls columns: %v", err)
	}
	if _, err := store.RecordChatPollSuccess(ctx, "chat-new", now, true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess after legacy chat_polls migration: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err = openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("reopen sqlite db: %v", err)
	}
	defer db.Close()
	var lastActivity, parkNotice, parkedSkip sql.NullInt64
	if err := db.QueryRow(`SELECT last_activity_at, park_notice_sent_at, parked_skip_eligible FROM chat_polls WHERE chat_id = ?`, "chat-new").Scan(&lastActivity, &parkNotice, &parkedSkip); err != nil {
		t.Fatalf("query migrated chat-new derived columns: %v", err)
	}
	if !parkedSkip.Valid || parkedSkip.Int64 != 0 {
		t.Fatalf("chat-new parked_skip_eligible = %#v, want false", parkedSkip)
	}
	if err := db.QueryRow(`SELECT parked_skip_eligible FROM chat_polls WHERE chat_id = ?`, "chat-legacy").Scan(&parkedSkip); err != nil {
		t.Fatalf("query migrated legacy parked_skip_eligible: %v", err)
	}
	if !parkedSkip.Valid || parkedSkip.Int64 != 1 {
		t.Fatalf("chat-legacy parked_skip_eligible = %#v, want true", parkedSkip)
	}
}

type officialReleaseFixtureKind string

const (
	officialReleaseFixtureNoTeamsStore officialReleaseFixtureKind = "no-teams-store"
	officialReleaseFixtureLegacyJSONV4 officialReleaseFixtureKind = "legacy-json-v4"
	officialReleaseFixtureLegacyJSONV5 officialReleaseFixtureKind = "legacy-json-v5"
	officialReleaseFixtureSQLiteV5     officialReleaseFixtureKind = "sqlite-v5"
)

type officialReleaseFixtureCase struct {
	tag  string
	kind officialReleaseFixtureKind
}

func officialReleaseUpgradeFixtureCasesForTest() []officialReleaseFixtureCase {
	return []officialReleaseFixtureCase{
		// v0.0.48 is the last stable release before the Teams helper store existed.
		{tag: "v0.0.48", kind: officialReleaseFixtureNoTeamsStore},
		// Cover every stable Teams-helper release from v0.1.0 onward. Keep the
		// list explicit so a regression points at the affected upgrade source
		// version, and add each new final release here when it ships.
		{tag: "v0.1.0", kind: officialReleaseFixtureLegacyJSONV4},
		{tag: "v0.1.1", kind: officialReleaseFixtureLegacyJSONV5},
		{tag: "v0.1.2", kind: officialReleaseFixtureLegacyJSONV5},
		// v0.1.3 is the first stable release whose Teams store can already be a
		// SQLite sidecar.
		{tag: "v0.1.3", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.4", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.5", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.6", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.7", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.8", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.9", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.10", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.11", kind: officialReleaseFixtureSQLiteV5},
		{tag: "v0.1.12", kind: officialReleaseFixtureSQLiteV5},
	}
}

func TestSQLiteOfficialReleaseStoresUpgradeToCurrent(t *testing.T) {
	for _, tc := range officialReleaseUpgradeFixtureCasesForTest() {
		t.Run(tc.tag, func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			switch tc.kind {
			case officialReleaseFixtureNoTeamsStore:
			case officialReleaseFixtureLegacyJSONV4:
				seedOfficialReleaseLegacyJSONStoreForTest(t, store, tc.tag, 4)
			case officialReleaseFixtureLegacyJSONV5:
				seedOfficialReleaseLegacyJSONStoreForTest(t, store, tc.tag, 5)
			case officialReleaseFixtureSQLiteV5:
				seedOfficialReleaseSQLiteStoreForTest(t, store, tc.tag)
			default:
				t.Fatalf("unknown official release fixture kind %q", tc.kind)
			}

			loaded, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load %s fixture: %v", tc.tag, err)
			}
			if loaded.SchemaVersion != SchemaVersion {
				t.Fatalf("Load %s schema = %d, want %d", tc.tag, loaded.SchemaVersion, SchemaVersion)
			}
			if tc.kind == officialReleaseFixtureNoTeamsStore {
				assertOfficialReleaseNoTeamsStoreLoaded(t, tc.tag, loaded)
			} else {
				assertOfficialReleaseFixtureLoaded(t, tc.tag, loaded)
			}

			result, err := store.MigrateLargeStateToSQLite(ctx, 0)
			if err != nil {
				t.Fatalf("MigrateLargeStateToSQLite %s fixture: %v", tc.tag, err)
			}
			if tc.kind == officialReleaseFixtureNoTeamsStore {
				if result.Migrated || result.AlreadyDB || result.Path != "" || result.MigrationID != "" {
					t.Fatalf("%s migration result = %#v, want no migration for absent Teams store", tc.tag, result)
				}
				if _, err := os.Stat(store.Path()); !os.IsNotExist(err) {
					t.Fatalf("%s absent Teams store migration created state pointer, stat err = %v", tc.tag, err)
				}
				return
			}
			if tc.kind == officialReleaseFixtureSQLiteV5 {
				if !result.AlreadyDB || result.Migrated {
					t.Fatalf("%s migration result = %#v, want already-db", tc.tag, result)
				}
			} else if !result.Migrated || result.AlreadyDB {
				t.Fatalf("%s migration result = %#v, want migrated legacy JSON", tc.tag, result)
			}
			assertOfficialReleaseSQLiteSchemaForTest(t, store, tc.tag)
			assertOfficialReleaseHotPathsForTest(t, store, tc.tag)
		})
	}
}

func TestSQLiteOfficialReleaseFixtureListCoversStableTags(t *testing.T) {
	if os.Getenv("CODEX_HELPER_REQUIRE_RELEASE_TAG_FIXTURES") != "1" {
		t.Skip("set CODEX_HELPER_REQUIRE_RELEASE_TAG_FIXTURES=1 in CI after fetching tags")
	}
	tags := gitStableReleaseTagsForTest(t)
	want := officialReleaseUpgradeFixtureTagsForStableTags(tags)
	got := map[string]bool{}
	for _, tc := range officialReleaseUpgradeFixtureCasesForTest() {
		got[tc.tag] = true
	}
	var missing []string
	for _, tag := range want {
		if !got[tag] {
			missing = append(missing, tag)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("official release upgrade fixtures missing stable tags: %v", missing)
	}
}

func TestSQLiteOfficialReleaseLegacyOutboxColumnsUpgradeToCurrent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedOfficialReleaseSQLiteStoreForTestWithOptions(t, store, "v0.1.7-legacy-outbox-columns", officialReleaseSQLiteFixtureOptions{
		LegacyOutboxColumns: true,
	})

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load legacy outbox fixture: %v", err)
	}
	assertOfficialReleaseFixtureLoaded(t, "v0.1.7-legacy-outbox-columns", loaded)
	result, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("MigrateLargeStateToSQLite legacy outbox fixture: %v", err)
	}
	if !result.AlreadyDB || result.Migrated {
		t.Fatalf("legacy outbox migration result = %#v, want already-db", result)
	}
	assertOfficialReleaseSQLiteSchemaForTest(t, store, "v0.1.7-legacy-outbox-columns")
	assertOfficialReleaseHotPathsForTest(t, store, "v0.1.7-legacy-outbox-columns")
}

func TestSQLiteStoreUsesNormalSynchronousModeAndManualWALCheckpoint(t *testing.T) {
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	result := migrateStoreToSQLiteForTest(t, store)

	ctx := context.Background()
	err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		var synchronous string
		if err := db.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&synchronous); err != nil {
			return fmt.Errorf("read synchronous pragma: %w", err)
		}
		if synchronous != "1" && !strings.EqualFold(synchronous, "NORMAL") {
			return fmt.Errorf("PRAGMA synchronous = %q, want NORMAL/1", synchronous)
		}
		var autocheckpoint int
		if err := db.QueryRowContext(ctx, `PRAGMA wal_autocheckpoint`).Scan(&autocheckpoint); err != nil {
			return fmt.Errorf("read wal_autocheckpoint pragma: %w", err)
		}
		if autocheckpoint != sqliteWALAutocheckpointPages {
			return fmt.Errorf("PRAGMA wal_autocheckpoint = %d, want %d", autocheckpoint, sqliteWALAutocheckpointPages)
		}
		return seedLargeSQLiteWALTxForTest(ctx, db, 512, 4096)
	})
	if err != nil {
		t.Fatalf("seed sqlite WAL: %v", err)
	}
	walPath := result.Path + "-wal"
	beforeInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL before checkpoint: %v", err)
	}
	if beforeInfo.Size() < 512*1024 {
		t.Fatalf("seeded WAL size = %d, want at least 512 KiB", beforeInfo.Size())
	}
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, `INSERT OR REPLACE INTO state_meta(key, value) VALUES ('wal_checkpoint_test_hot_write', ?)`, []byte(`{"ok":true}`))
		return err
	}); err != nil {
		t.Fatalf("small hot write after large WAL: %v", err)
	}
	hotInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL after hot write: %v", err)
	}
	if hotInfo.Size() < beforeInfo.Size() {
		t.Fatalf("small hot write checkpointed WAL from %d to %d bytes", beforeInfo.Size(), hotInfo.Size())
	}
	if growth := hotInfo.Size() - beforeInfo.Size(); growth > 256*1024 {
		t.Fatalf("small hot write grew WAL by %d bytes, want <= 256 KiB", growth)
	}
	skipped, err := store.CheckpointSQLiteWAL(ctx, hotInfo.Size()+1)
	if err != nil {
		t.Fatalf("skip manual checkpoint: %v", err)
	}
	if !skipped.SQLite || skipped.Attempted {
		t.Fatalf("checkpoint below threshold = %#v, want sqlite skip without attempt", skipped)
	}
	ran, err := store.CheckpointSQLiteWAL(ctx, 1)
	if err != nil {
		t.Fatalf("manual checkpoint: %v", err)
	}
	if !ran.SQLite || !ran.Attempted || ran.Busy != 0 {
		t.Fatalf("checkpoint result = %#v, want successful attempt", ran)
	}
	afterSize := int64(0)
	if afterInfo, err := os.Stat(walPath); err == nil {
		afterSize = afterInfo.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat WAL after checkpoint: %v", err)
	}
	if afterSize > 32*1024 {
		t.Fatalf("WAL size after checkpoint = %d, want <= 32 KiB", afterSize)
	}
}

func BenchmarkSQLiteManualWALCheckpointHotWrite(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, err := Open(filepath.Join(b.TempDir(), "teams-state", "state.json"))
		if err != nil {
			b.Fatalf("Open error: %v", err)
		}
		if _, err := store.SetPaused(ctx, true, "seed sqlite benchmark"); err != nil {
			_ = store.Close()
			b.Fatalf("seed legacy state: %v", err)
		}
		result, err := store.MigrateLargeStateToSQLite(ctx, 0)
		if err != nil {
			_ = store.Close()
			b.Fatalf("migrate sqlite state: %v", err)
		}
		if err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			return seedLargeSQLiteWALTxForTest(ctx, db, 512, 4096)
		}); err != nil {
			_ = store.Close()
			b.Fatalf("seed sqlite WAL: %v", err)
		}
		walPath := result.Path + "-wal"
		beforeInfo, err := os.Stat(walPath)
		if err != nil {
			_ = store.Close()
			b.Fatalf("stat WAL before hot write: %v", err)
		}
		b.StartTimer()
		if err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, `INSERT OR REPLACE INTO state_meta(key, value) VALUES ('wal_checkpoint_benchmark_hot_write', ?)`, []byte(`{"ok":true}`))
			return err
		}); err != nil {
			b.Fatalf("small hot write after large WAL: %v", err)
		}
		b.StopTimer()
		hotInfo, err := os.Stat(walPath)
		if err != nil {
			_ = store.Close()
			b.Fatalf("stat WAL after hot write: %v", err)
		}
		if hotInfo.Size() < beforeInfo.Size() {
			_ = store.Close()
			b.Fatalf("small hot write checkpointed WAL from %d to %d bytes", beforeInfo.Size(), hotInfo.Size())
		}
		b.ReportMetric(float64(beforeInfo.Size()), "wal_before_bytes")
		b.ReportMetric(float64(hotInfo.Size()-beforeInfo.Size()), "hot_wal_growth_bytes")
		ran, err := store.CheckpointSQLiteWAL(ctx, 1)
		if err != nil {
			_ = store.Close()
			b.Fatalf("manual checkpoint: %v", err)
		}
		if !ran.Attempted || ran.Busy != 0 {
			_ = store.Close()
			b.Fatalf("checkpoint result = %#v, want successful attempt", ran)
		}
		_ = store.Close()
	}
}

func BenchmarkSQLiteManualWALCheckpointWriteAmplification(b *testing.B) {
	ctx := context.Background()
	cases := []struct {
		name         string
		rows         int
		payloadBytes int
	}{
		{name: "wal-2MiB", rows: 512, payloadBytes: 4096},
		{name: "wal-64MiB", rows: 8192, payloadBytes: 8192},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var totalIO sqliteTestProcIO
			var totalWALBefore int64
			var totalWALAfter int64
			var totalLogFrames int64
			var totalCheckpointedFrames int64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store, err := Open(filepath.Join(b.TempDir(), "teams-state", "state.json"))
				if err != nil {
					b.Fatalf("Open error: %v", err)
				}
				if _, err := store.SetPaused(ctx, true, "seed sqlite checkpoint benchmark"); err != nil {
					_ = store.Close()
					b.Fatalf("seed legacy state: %v", err)
				}
				result, err := store.MigrateLargeStateToSQLite(ctx, 0)
				if err != nil {
					_ = store.Close()
					b.Fatalf("migrate sqlite state: %v", err)
				}
				if err := store.withStateLock(ctx, func() error {
					pointer, ok, err := store.currentSQLitePointerUnlocked()
					if err != nil || !ok {
						return err
					}
					db, err := store.sqliteDBUnlocked(pointer)
					if err != nil {
						return err
					}
					return seedLargeSQLiteWALTxForTest(ctx, db, tc.rows, tc.payloadBytes)
				}); err != nil {
					_ = store.Close()
					b.Fatalf("seed sqlite WAL: %v", err)
				}
				walPath := result.Path + "-wal"
				beforeInfo, err := os.Stat(walPath)
				if err != nil {
					_ = store.Close()
					b.Fatalf("stat WAL before checkpoint: %v", err)
				}
				beforeIO, beforeIOOK := readSQLiteTestProcSelfIO()
				b.StartTimer()
				ran, err := store.CheckpointSQLiteWAL(ctx, 1)
				b.StopTimer()
				afterIO, afterIOOK := readSQLiteTestProcSelfIO()
				if err != nil {
					_ = store.Close()
					b.Fatalf("manual checkpoint: %v", err)
				}
				if !ran.Attempted || ran.Busy != 0 {
					_ = store.Close()
					b.Fatalf("checkpoint result = %#v, want successful attempt", ran)
				}
				if beforeIOOK && afterIOOK {
					totalIO.add(beforeIO.delta(afterIO))
				}
				afterSize := int64(0)
				if afterInfo, err := os.Stat(walPath); err == nil {
					afterSize = afterInfo.Size()
				} else if !errors.Is(err, os.ErrNotExist) {
					_ = store.Close()
					b.Fatalf("stat WAL after checkpoint: %v", err)
				}
				totalWALBefore += beforeInfo.Size()
				totalWALAfter += afterSize
				totalLogFrames += int64(ran.LogFrames)
				totalCheckpointedFrames += int64(ran.CheckpointedFrames)
				_ = store.Close()
			}
			reportSQLiteTestProcIO(b, totalIO, b.N)
			if b.N > 0 {
				denom := float64(b.N)
				b.ReportMetric(float64(totalWALBefore)/denom, "wal_before_bytes")
				b.ReportMetric(float64(totalWALAfter)/denom, "wal_after_bytes")
				b.ReportMetric(float64(totalLogFrames)/denom, "checkpoint_log_frames/op")
				b.ReportMetric(float64(totalCheckpointedFrames)/denom, "checkpointed_frames/op")
			}
		})
	}
}

func seedLargeSQLiteWALTxForTest(ctx context.Context, db *sql.DB, rows int, payloadBytes int) error {
	if rows <= 0 {
		rows = 1
	}
	if payloadBytes <= 0 {
		payloadBytes = 1
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS wal_checkpoint_test(id INTEGER PRIMARY KEY, payload TEXT)`); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	payload := strings.Repeat("x", payloadBytes)
	for i := 0; i < rows; i++ {
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO wal_checkpoint_test(id, payload) VALUES (?, ?)`, i, payload); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type sqliteTestProcIO struct {
	rchar               uint64
	wchar               uint64
	readBytes           uint64
	writeBytes          uint64
	cancelledWriteBytes uint64
}

func readSQLiteTestProcSelfIO() (sqliteTestProcIO, bool) {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return sqliteTestProcIO{}, false
	}
	var ioState sqliteTestProcIO
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "rchar":
			ioState.rchar = n
		case "wchar":
			ioState.wchar = n
		case "read_bytes":
			ioState.readBytes = n
		case "write_bytes":
			ioState.writeBytes = n
		case "cancelled_write_bytes":
			ioState.cancelledWriteBytes = n
		}
	}
	return ioState, true
}

func (ioState sqliteTestProcIO) delta(after sqliteTestProcIO) sqliteTestProcIO {
	return sqliteTestProcIO{
		rchar:               sqliteTestSaturatingSub(after.rchar, ioState.rchar),
		wchar:               sqliteTestSaturatingSub(after.wchar, ioState.wchar),
		readBytes:           sqliteTestSaturatingSub(after.readBytes, ioState.readBytes),
		writeBytes:          sqliteTestSaturatingSub(after.writeBytes, ioState.writeBytes),
		cancelledWriteBytes: sqliteTestSaturatingSub(after.cancelledWriteBytes, ioState.cancelledWriteBytes),
	}
}

func (ioState *sqliteTestProcIO) add(other sqliteTestProcIO) {
	ioState.rchar += other.rchar
	ioState.wchar += other.wchar
	ioState.readBytes += other.readBytes
	ioState.writeBytes += other.writeBytes
	ioState.cancelledWriteBytes += other.cancelledWriteBytes
}

func sqliteTestSaturatingSub(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func reportSQLiteTestProcIO(b *testing.B, total sqliteTestProcIO, n int) {
	b.Helper()
	if n <= 0 {
		return
	}
	denom := float64(n)
	b.ReportMetric(float64(total.readBytes)/denom, "disk_read_B/op")
	b.ReportMetric(float64(total.writeBytes)/denom, "disk_write_B/op")
	b.ReportMetric(float64(total.cancelledWriteBytes)/denom, "cancelled_write_B/op")
	b.ReportMetric(float64(total.rchar)/denom, "logical_read_B/op")
	b.ReportMetric(float64(total.wchar)/denom, "logical_write_B/op")
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

func TestSQLiteMarkOutboxSendErrorDoesNotLoadColdState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	session := testSession()
	session.ID = "send-error-session"
	session.TeamsChatID = "send-error-chat"
	session.CodexThreadID = "send-error-thread"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	artifactCreatedAt := time.Date(2026, 6, 16, 7, 45, 0, 0, time.UTC)
	if _, err := store.UpsertArtifactRecord(ctx, ArtifactRecord{
		ID:         "artifact:send-error",
		SessionID:  session.ID,
		TurnID:     "turn:artifact-preplan",
		Path:       "canonical/send-error.txt",
		UploadName: "canonical-send-error-upload.txt",
		Status:     "queued",
		CreatedAt:  artifactCreatedAt,
		UpdatedAt:  artifactCreatedAt,
	}); err != nil {
		t.Fatalf("seed preplanned artifact: %v", err)
	}
	inbound := testInbound()
	inbound.ID = "send-error-inbound"
	inbound.SessionID = session.ID
	inbound.TeamsChatID = session.TeamsChatID
	inbound.TeamsMessageID = "send-error-user-message"
	if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
		t.Fatalf("PersistInbound created=%v err=%v", created, err)
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil || !created {
		t.Fatalf("QueueTurn created=%v err=%v", created, err)
	}
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:                   "send-error-outbox",
		SessionID:            session.ID,
		TurnID:               turn.ID,
		TeamsChatID:          session.TeamsChatID,
		Kind:                 "status-progress",
		Body:                 "uploading",
		ArtifactIDs:          []string{"artifact:send-error"},
		AttachmentName:       "report.txt",
		AttachmentUploadName: "report-upload.txt",
		DriveItemID:          "drive-item-1",
		Status:               OutboxStatusSending,
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)
	sqliteWriteRawStateJSONForTest(t, store, []byte(`{"broken"`))

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	errored, err := store.MarkOutboxSendError(ctx, msg.ID, strings.Repeat("x", 300))
	if err != nil {
		t.Fatalf("MarkOutboxSendError sqlite error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("MarkOutboxSendError loaded full state %d times", got)
	}
	if errored.Status != OutboxStatusQueued || len(errored.LastSendError) != 240 || errored.LastSendAttempt.IsZero() {
		t.Fatalf("errored outbox = %#v", errored)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
	outboxRaw := sqliteRawOutboxJSONForTest(t, store, msg.ID)
	var outbox OutboxMessage
	if err := json.Unmarshal(outboxRaw, &outbox); err != nil {
		t.Fatalf("unmarshal outbox: %v", err)
	}
	if outbox.Status != OutboxStatusQueued || outbox.LastSendError == "" {
		t.Fatalf("stored outbox after send error = %#v", outbox)
	}
	helperRaw := sqliteRawHelperDeliveryByOutboxForTest(t, store, msg.ID)
	var helper HelperDeliveryRecord
	if err := json.Unmarshal(helperRaw, &helper); err != nil {
		t.Fatalf("unmarshal helper delivery: %v", err)
	}
	if helper.Status != HelperDeliveryStatusFailed || helper.OutboxID != msg.ID {
		t.Fatalf("helper delivery after send error = %#v", helper)
	}
	artifactRaw := sqliteRawArtifactRecordForTest(t, store, "artifact:send-error")
	var artifact ArtifactRecord
	if err := json.Unmarshal(artifactRaw, &artifact); err != nil {
		t.Fatalf("unmarshal artifact: %v", err)
	}
	if artifact.Status != "message_failed" || artifact.OutboxID != msg.ID || artifact.DriveItemID != "drive-item-1" || artifact.Error == "" {
		t.Fatalf("artifact after send error = %#v", artifact)
	}
	if artifact.Path != "canonical/send-error.txt" || artifact.UploadName != "canonical-send-error-upload.txt" || !artifact.CreatedAt.Equal(artifactCreatedAt) {
		t.Fatalf("artifact preplanned fields were not preserved after send error: %#v", artifact)
	}
}

func TestSQLiteSuppressOutboxOwnerMentionDoesNotLoadColdState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	session := testSession()
	session.ID = "suppress-owner-session"
	session.TeamsChatID = "suppress-owner-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:           "suppress-owner-outbox",
		SessionID:    session.ID,
		TeamsChatID:  session.TeamsChatID,
		Kind:         "workflow-notification",
		Body:         "workflow update",
		Status:       OutboxStatusQueued,
		MentionOwner: true,
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)
	sqliteWriteRawStateJSONForTest(t, store, []byte(`{"broken"`))

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	updated, err := store.SuppressOutboxOwnerMention(ctx, msg.ID)
	if err != nil {
		t.Fatalf("SuppressOutboxOwnerMention sqlite error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("SuppressOutboxOwnerMention loaded full state %d times", got)
	}
	if updated.MentionOwner {
		t.Fatalf("updated outbox still mentions owner: %#v", updated)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
	outboxRaw := sqliteRawOutboxJSONForTest(t, store, msg.ID)
	var outbox OutboxMessage
	if err := json.Unmarshal(outboxRaw, &outbox); err != nil {
		t.Fatalf("unmarshal outbox: %v", err)
	}
	if outbox.MentionOwner {
		t.Fatalf("stored outbox still mentions owner: %#v", outbox)
	}
}

func TestSQLiteMarkOutboxDriveItemDoesNotLoadColdState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	session := testSession()
	session.ID = "drive-item-session"
	session.TeamsChatID = "drive-item-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	artifactCreatedAt := time.Date(2026, 6, 16, 7, 45, 0, 0, time.UTC)
	if _, err := store.UpsertArtifactRecord(ctx, ArtifactRecord{
		ID:         "artifact:drive-item",
		SessionID:  session.ID,
		TurnID:     "turn:artifact-preplan",
		Path:       "canonical/drive-item.txt",
		UploadName: "canonical-drive-item-upload.txt",
		Status:     "queued",
		CreatedAt:  artifactCreatedAt,
		UpdatedAt:  artifactCreatedAt,
	}); err != nil {
		t.Fatalf("seed preplanned artifact: %v", err)
	}
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:                   "drive-item-outbox",
		SessionID:            session.ID,
		TeamsChatID:          session.TeamsChatID,
		Kind:                 "status-progress",
		Body:                 "uploading",
		ArtifactIDs:          []string{"artifact:drive-item"},
		AttachmentName:       "report.txt",
		AttachmentUploadName: "report-upload.txt",
		Status:               OutboxStatusSending,
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)
	sqliteWriteRawStateJSONForTest(t, store, []byte(`{"broken"`))

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	updated, err := store.MarkOutboxDriveItem(ctx, msg.ID, " drive-item-2 ", " report.txt ", " etag-2 ", " https://sharepoint/report ", " dav://report ")
	if err != nil {
		t.Fatalf("MarkOutboxDriveItem sqlite error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("MarkOutboxDriveItem loaded full state %d times", got)
	}
	if updated.DriveItemID != "drive-item-2" || updated.DriveItemName != "report.txt" || updated.DriveItemETag != "etag-2" || updated.LastSendError != "" {
		t.Fatalf("updated outbox = %#v", updated)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
	atomic.StoreInt64(&loads, 0)
	artifactRaw := sqliteRawArtifactRecordForTest(t, store, "artifact:drive-item")
	var artifact ArtifactRecord
	if err := json.Unmarshal(artifactRaw, &artifact); err != nil {
		t.Fatalf("unmarshal artifact: %v", err)
	}
	if artifact.Status != "drive_uploaded" || artifact.OutboxID != msg.ID || artifact.DriveItemID != "drive-item-2" || artifact.UploadedAt.IsZero() || artifact.Error != "" {
		t.Fatalf("artifact after drive item update = %#v", artifact)
	}
	if artifact.Path != "canonical/drive-item.txt" || artifact.UploadName != "canonical-drive-item-upload.txt" || !artifact.CreatedAt.Equal(artifactCreatedAt) {
		t.Fatalf("artifact preplanned fields were not preserved after drive item update: %#v", artifact)
	}
	if _, err := store.MarkOutboxSent(ctx, msg.ID, "teams-drive-item"); err != nil {
		t.Fatalf("MarkOutboxSent sqlite artifact error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("artifact outbox send path loaded full state %d times", got)
	}
	artifactRaw = sqliteRawArtifactRecordForTest(t, store, "artifact:drive-item")
	if err := json.Unmarshal(artifactRaw, &artifact); err != nil {
		t.Fatalf("unmarshal sent artifact: %v", err)
	}
	if artifact.Status != "uploaded" || artifact.TeamsMessageID != "teams-drive-item" || artifact.SentAt.IsZero() || artifact.Error != "" {
		t.Fatalf("artifact after sent = %#v", artifact)
	}
	if artifact.Path != "canonical/drive-item.txt" || artifact.UploadName != "canonical-drive-item-upload.txt" || !artifact.CreatedAt.Equal(artifactCreatedAt) {
		t.Fatalf("artifact preplanned fields were not preserved after sent: %#v", artifact)
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
		state.OutboxMessages["outbox-other"] = OutboxMessage{ID: "outbox-other", SessionID: "s2", TurnID: "turn-other", TeamsChatID: "chat-2", Kind: "final", Body: "other", Status: OutboxStatusSent, CreatedAt: now, UpdatedAt: now}
		state.ImportCheckpoints["import-active"] = ImportCheckpoint{ID: "import-active", SessionID: "s1", LastRecordID: "record-active", Status: sqliteImportCheckpointImporting, UpdatedAt: now}
		state.ImportCheckpoints["import-1"] = ImportCheckpoint{ID: "import-1", SessionID: "s1", LastRecordID: "record-1", Status: "complete", UpdatedAt: now}
		state.ImportCheckpoints["import-other"] = ImportCheckpoint{ID: "import-other", SessionID: "s2", LastRecordID: "record-other", Status: "complete", UpdatedAt: now}
		state.TranscriptDeliveries["transcript-delivery-1"] = TranscriptDeliveryRecord{ID: "transcript-delivery-1", SessionID: "s1", OutboxID: "outbox-1", Kind: "final", TextHash: "hash-1", Status: TranscriptDeliveryStatusSent, CreatedAt: now, UpdatedAt: now}
		state.TranscriptDeliveries["transcript-delivery-other"] = TranscriptDeliveryRecord{ID: "transcript-delivery-other", SessionID: "s2", OutboxID: "outbox-other", Kind: "final", TextHash: "hash-other", Status: TranscriptDeliveryStatusSent, CreatedAt: now, UpdatedAt: now}
		state.HelperDeliveries["helper-delivery-1"] = HelperDeliveryRecord{ID: "helper-delivery-1", SessionID: "s1", TurnID: "turn-running", OutboxID: "outbox-1", Kind: "final", Status: HelperDeliveryStatusSent, CreatedAt: now, UpdatedAt: now}
		state.HelperDeliveries["helper-delivery-other"] = HelperDeliveryRecord{ID: "helper-delivery-other", SessionID: "s2", TurnID: "turn-other", OutboxID: "outbox-other", Kind: "final", Status: HelperDeliveryStatusSent, CreatedAt: now, UpdatedAt: now}
		state.ChatPolls["chat-1"] = ChatPollState{ChatID: "chat-1", Seeded: true, PollState: "warm", NextPollAt: now.Add(time.Minute), UpdatedAt: now}
		state.ChatPolls["chat-parked-clean"] = ChatPollState{ChatID: "chat-parked-clean", Seeded: true, PollState: "parked", ParkedAt: now.Add(-48 * time.Hour), ParkNoticeSentAt: now.Add(-47 * time.Hour), UpdatedAt: now}
		state.ChatPolls["chat-parked-stale"] = ChatPollState{ChatID: "chat-parked-stale", Seeded: true, PollState: "parked", ParkedAt: now.Add(-48 * time.Hour), ParkNoticeSentAt: now.Add(-47 * time.Hour), ContinuationPath: "/chats/chat-parked-stale/messages?$skiptoken=stale", UpdatedAt: now}
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
	pollSchedule, err := store.PollScheduleSnapshot(ctx)
	if err != nil {
		t.Fatalf("PollScheduleSnapshot sqlite error: %v", err)
	}
	if pollSchedule.ControlChat.TeamsChatID != "control-chat" || len(pollSchedule.Sessions) != 2 || pollSchedule.ChatPolls["chat-1"].PollState != "warm" || pollSchedule.ServiceOwner == nil {
		t.Fatalf("poll schedule snapshot missing selected fields: %#v", pollSchedule)
	}
	if pollSchedule.ChatPolls["chat-parked-clean"].PollState != "parked" || pollSchedule.ChatPolls["chat-parked-stale"].ContinuationPath == "" {
		t.Fatalf("poll schedule snapshot should include parked rows: %#v", pollSchedule.ChatPolls)
	}
	if len(pollSchedule.InboundEvents) != 0 || len(pollSchedule.OutboxMessages) != 0 || len(pollSchedule.ChatRateLimits) != 0 {
		t.Fatalf("poll schedule snapshot included unselected hot fields: inbound=%d outbox=%d rate_limits=%d", len(pollSchedule.InboundEvents), len(pollSchedule.OutboxMessages), len(pollSchedule.ChatRateLimits))
	}
	hotPollState, err := store.HotPollScheduleState(ctx)
	if err != nil {
		t.Fatalf("HotPollScheduleState sqlite error: %v", err)
	}
	if _, ok := hotPollState.ChatPolls["chat-parked-clean"]; ok || hotPollState.ChatPolls["chat-parked-stale"].ContinuationPath == "" {
		t.Fatalf("hot poll state should skip only clean parked rows: %#v", hotPollState.ChatPolls)
	}
	if len(hotPollState.Sessions) != 0 {
		t.Fatalf("hot poll state should not load sessions eagerly: %d", len(hotPollState.Sessions))
	}
	if hotPollState.Turns["turn-queued"].ID == "" || hotPollState.Turns["turn-running"].ID == "" || hotPollState.Turns["turn-other"].ID == "" || hotPollState.Turns["turn-completed"].ID != "" {
		t.Fatalf("hot poll state should load only active turns: %#v", hotPollState.Turns)
	}
	if hotPollState.ImportCheckpoints["import-active"].ID == "" || hotPollState.ImportCheckpoints["import-1"].ID != "" || hotPollState.ImportCheckpoints["import-other"].ID != "" {
		t.Fatalf("hot poll state should load only importing checkpoints: %#v", hotPollState.ImportCheckpoints)
	}
	hotPollSchedule, parkedSkip, err := store.HotPollScheduleSnapshot(ctx)
	if err != nil {
		t.Fatalf("HotPollScheduleSnapshot sqlite error: %v", err)
	}
	if _, ok := hotPollSchedule.ChatPolls["chat-parked-clean"]; ok || !parkedSkip["chat-parked-clean"] {
		t.Fatalf("hot poll schedule should skip clean parked row: polls=%#v skip=%#v", hotPollSchedule.ChatPolls, parkedSkip)
	}
	if hotPollSchedule.ChatPolls["chat-parked-stale"].ContinuationPath == "" || parkedSkip["chat-parked-stale"] {
		t.Fatalf("hot poll schedule should retain stale parked row for maintenance: polls=%#v skip=%#v", hotPollSchedule.ChatPolls, parkedSkip)
	}
	if hotPollSchedule.ChatPolls["chat-1"].PollState != "warm" || hotPollSchedule.ServiceOwner == nil {
		t.Fatalf("hot poll schedule missing non-parked selected fields: %#v", hotPollSchedule)
	}
	if len(hotPollSchedule.Sessions) != 0 {
		t.Fatalf("hot poll schedule should not load sessions eagerly: %d", len(hotPollSchedule.Sessions))
	}
	if hotPollSchedule.Turns["turn-queued"].ID == "" || hotPollSchedule.Turns["turn-running"].ID == "" || hotPollSchedule.Turns["turn-other"].ID == "" || hotPollSchedule.Turns["turn-completed"].ID != "" {
		t.Fatalf("hot poll schedule should load only active turns: %#v", hotPollSchedule.Turns)
	}
	if hotPollSchedule.ImportCheckpoints["import-active"].ID == "" || hotPollSchedule.ImportCheckpoints["import-1"].ID != "" || hotPollSchedule.ImportCheckpoints["import-other"].ID != "" {
		t.Fatalf("hot poll schedule should load only importing checkpoints: %#v", hotPollSchedule.ImportCheckpoints)
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
	workflowTurn, err := store.SessionWorkflowEventSnapshotForTurn(ctx, "s1", "turn-running")
	if err != nil {
		t.Fatalf("SessionWorkflowEventSnapshotForTurn sqlite error: %v", err)
	}
	if len(workflowTurn.Sessions) != 1 || workflowTurn.Sessions["s1"].ID == "" {
		t.Fatalf("workflow turn snapshot sessions = %#v", workflowTurn.Sessions)
	}
	if len(workflowTurn.Turns) != 1 || workflowTurn.Turns["turn-running"].ID == "" {
		t.Fatalf("workflow turn snapshot turns = %#v", workflowTurn.Turns)
	}
	if len(workflowTurn.InboundEvents) != 1 || workflowTurn.InboundEvents["inbound-running"].ID == "" {
		t.Fatalf("workflow turn snapshot inbound = %#v", workflowTurn.InboundEvents)
	}
	threadResolution, err := store.SessionThreadResolutionSnapshot(ctx, "s1")
	if err != nil {
		t.Fatalf("SessionThreadResolutionSnapshot sqlite error: %v", err)
	}
	if len(threadResolution.Sessions) != 1 || len(threadResolution.Turns) != 3 || len(threadResolution.InboundEvents) != 0 {
		t.Fatalf("thread resolution snapshot = sessions %#v turns %#v inbound %#v", threadResolution.Sessions, threadResolution.Turns, threadResolution.InboundEvents)
	}
	transcriptDedupe, err := store.SessionTranscriptDedupeSnapshot(ctx, "s1", "import-1")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot sqlite error: %v", err)
	}
	if transcriptDedupe.ServiceOwner == nil || transcriptDedupe.ImportCheckpoints["import-1"].ID == "" || transcriptDedupe.ImportCheckpoints["import-other"].ID != "" {
		t.Fatalf("transcript dedupe checkpoint/runtime snapshot = %#v owner=%#v", transcriptDedupe.ImportCheckpoints, transcriptDedupe.ServiceOwner)
	}
	if len(transcriptDedupe.Turns) != 3 || transcriptDedupe.Turns["turn-other"].ID != "" || len(transcriptDedupe.InboundEvents) != 3 || transcriptDedupe.InboundEvents["inbound-other"].ID != "" {
		t.Fatalf("transcript dedupe turn/inbound snapshot = turns %#v inbound %#v", transcriptDedupe.Turns, transcriptDedupe.InboundEvents)
	}
	if len(transcriptDedupe.OutboxMessages) != 1 || transcriptDedupe.OutboxMessages["outbox-1"].ID == "" || transcriptDedupe.OutboxMessages["outbox-other"].ID != "" {
		t.Fatalf("transcript dedupe outbox snapshot = %#v", transcriptDedupe.OutboxMessages)
	}
	if len(transcriptDedupe.TranscriptDeliveries) != 1 || transcriptDedupe.TranscriptDeliveries["transcript-delivery-1"].ID == "" || transcriptDedupe.TranscriptDeliveries["transcript-delivery-other"].ID != "" {
		t.Fatalf("transcript dedupe transcript deliveries = %#v", transcriptDedupe.TranscriptDeliveries)
	}
	if len(transcriptDedupe.HelperDeliveries) != 1 || transcriptDedupe.HelperDeliveries["helper-delivery-1"].ID == "" || transcriptDedupe.HelperDeliveries["helper-delivery-other"].ID != "" {
		t.Fatalf("transcript dedupe helper deliveries = %#v", transcriptDedupe.HelperDeliveries)
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
	if len(outbox.OutboxMessages) != 2 || outbox.OutboxMessages["outbox-1"].ID == "" || outbox.OutboxMessages["outbox-other"].ID == "" || len(outbox.Turns) != 0 {
		t.Fatalf("outbox snapshot = %#v", outbox)
	}
	if poll, ok, err := store.ChatPoll(ctx, "chat-1"); err != nil || !ok || poll.PollState != "warm" {
		t.Fatalf("ChatPoll sqlite = %#v ok=%v err=%v", poll, ok, err)
	}
	if limit, ok, err := store.ChatRateLimit(ctx, "chat-1"); err != nil || !ok || limit.Reason != "429" {
		t.Fatalf("ChatRateLimit sqlite = %#v ok=%v err=%v", limit, ok, err)
	}
}

func TestImportCheckpointUsesNarrowSQLiteLookup(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 24, 3, 4, 5, 0, time.UTC)
	checkpoint := ImportCheckpoint{
		ID:             "transcript:session-1",
		SessionID:      "session-1",
		SourcePath:     "/tmp/cxp/session-1.jsonl",
		LastRecordID:   "record-1",
		LastSourceLine: 17,
		LastOffset:     4096,
		SourceSize:     8192,
		SourceModTime:  now.Add(-time.Minute),
		ImportTurnID:   "turn-1",
		KindPrefix:     "assistant",
		Status:         "complete",
		UpdatedAt:      now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		state.ImportCheckpoints["transcript:broken"] = ImportCheckpoint{ID: "transcript:broken", SessionID: "broken", LastRecordID: "broken", Status: "complete", UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed import checkpoints: %v", err)
	}

	legacy, found, err := store.ImportCheckpoint(ctx, checkpoint.ID)
	if err != nil {
		t.Fatalf("ImportCheckpoint legacy error: %v", err)
	}
	if !found || !reflect.DeepEqual(legacy, checkpoint) {
		t.Fatalf("ImportCheckpoint legacy = %#v found=%v, want %#v true", legacy, found, checkpoint)
	}
	if missing, found, err := store.ImportCheckpoint(ctx, "transcript:missing"); err != nil || found || missing.ID != "" {
		t.Fatalf("ImportCheckpoint legacy missing = %#v found=%v err=%v, want missing nil", missing, found, err)
	}

	migrateStoreToSQLiteForTest(t, store)
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	res, err := db.ExecContext(ctx, `UPDATE import_checkpoints SET json = ? WHERE id = ?`, []byte(`{"broken"`), "transcript:broken")
	if err != nil {
		t.Fatalf("corrupt unrelated import checkpoint: %v", err)
	}
	if rows, err := res.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("corrupt unrelated import checkpoint rows affected = %d err=%v, want 1", rows, err)
	}

	got, found, err := store.ImportCheckpoint(ctx, checkpoint.ID)
	if err != nil {
		t.Fatalf("ImportCheckpoint sqlite narrow lookup error: %v", err)
	}
	if !found || !reflect.DeepEqual(got, checkpoint) {
		t.Fatalf("ImportCheckpoint sqlite = %#v found=%v, want %#v true", got, found, checkpoint)
	}
	if _, _, err := store.ImportCheckpoint(ctx, "transcript:broken"); err == nil {
		t.Fatal("ImportCheckpoint corrupt requested row error = nil, want JSON error")
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt unrelated import checkpoint")
	}
}

func TestUpdateImportCheckpointUsesNarrowSQLiteWrite(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 10, 7, 0, 0, 0, time.UTC)
	checkpoint := ImportCheckpoint{
		ID:             "transcript:session-1",
		SessionID:      "session-1",
		SourcePath:     "/tmp/cxp/session-1.jsonl",
		LastRecordID:   "record-1",
		LastSourceLine: 10,
		LastOffset:     100,
		Status:         "complete",
		UpdatedAt:      now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		state.ImportCheckpoints["transcript:broken"] = ImportCheckpoint{ID: "transcript:broken", SessionID: "broken", LastRecordID: "broken", Status: "complete", UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed import checkpoints: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `UPDATE import_checkpoints SET json = ? WHERE id = ?`, []byte(`{"broken"`), "transcript:broken"); err != nil {
		t.Fatalf("corrupt unrelated import checkpoint: %v", err)
	}

	updated, changed, err := store.UpdateImportCheckpoint(ctx, checkpoint.ID, func(current ImportCheckpoint, found bool, updateTime time.Time) (ImportCheckpoint, bool, error) {
		if !found || current.LastRecordID != "record-1" {
			t.Fatalf("current checkpoint = %#v found=%v, want record-1 true", current, found)
		}
		current.LastRecordID = "record-2"
		current.LastSourceLine = 20
		current.LastOffset = 200
		current.UpdatedAt = updateTime
		return current, true, nil
	})
	if err != nil {
		t.Fatalf("UpdateImportCheckpoint sqlite narrow write error: %v", err)
	}
	if !changed || updated.LastRecordID != "record-2" || updated.LastSourceLine != 20 || updated.LastOffset != 200 {
		t.Fatalf("updated checkpoint = %#v changed=%v, want record-2 changed", updated, changed)
	}
	got, found, err := store.ImportCheckpoint(ctx, checkpoint.ID)
	if err != nil {
		t.Fatalf("ImportCheckpoint updated row error: %v", err)
	}
	if !found || got.LastRecordID != "record-2" || got.LastSourceLine != 20 || got.LastOffset != 200 {
		t.Fatalf("ImportCheckpoint after update = %#v found=%v", got, found)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt unrelated import checkpoint")
	}
}

func TestUpdateImportCheckpointSQLiteNoopDoesNotRewriteRow(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 10, 7, 30, 0, 0, time.UTC)
	checkpoint := ImportCheckpoint{
		ID:             "transcript:session-noop",
		SessionID:      "session-noop",
		SourcePath:     "/tmp/cxp/session-noop.jsonl",
		LastRecordID:   "record-1",
		LastSourceLine: 10,
		LastOffset:     100,
		Status:         "complete",
		UpdatedAt:      now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed import checkpoint: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	var before []byte
	if err := db.QueryRowContext(ctx, `SELECT json FROM import_checkpoints WHERE id = ?`, checkpoint.ID).Scan(&before); err != nil {
		t.Fatalf("read checkpoint before noop: %v", err)
	}

	updated, changed, err := store.UpdateImportCheckpoint(ctx, checkpoint.ID, func(current ImportCheckpoint, found bool, updateTime time.Time) (ImportCheckpoint, bool, error) {
		if !found || current.LastRecordID != checkpoint.LastRecordID {
			t.Fatalf("current checkpoint = %#v found=%v, want seeded checkpoint", current, found)
		}
		current.UpdatedAt = updateTime
		return current, false, nil
	})
	if err != nil {
		t.Fatalf("UpdateImportCheckpoint noop error: %v", err)
	}
	if changed {
		t.Fatalf("UpdateImportCheckpoint noop changed=true, updated=%#v", updated)
	}
	var after []byte
	if err := db.QueryRowContext(ctx, `SELECT json FROM import_checkpoints WHERE id = ?`, checkpoint.ID).Scan(&after); err != nil {
		t.Fatalf("read checkpoint after noop: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("noop checkpoint update rewrote row:\nbefore=%s\nafter=%s", string(before), string(after))
	}
}

func TestUpdateImportCheckpointSQLiteCreatesMissingRow(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-created"] = SessionContext{ID: "session-created", Status: SessionStatusActive, TeamsChatID: "chat-created", UpdatedAt: time.Now()}
		return nil
	}); err != nil {
		t.Fatalf("seed store before sqlite migration: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	created, changed, err := store.UpdateImportCheckpoint(ctx, "transcript:created", func(current ImportCheckpoint, found bool, updateTime time.Time) (ImportCheckpoint, bool, error) {
		if found || current.ID != "" {
			t.Fatalf("current checkpoint = %#v found=%v, want missing", current, found)
		}
		return ImportCheckpoint{
			SessionID:      "session-created",
			SourcePath:     "/tmp/cxp/session-created.jsonl",
			LastRecordID:   "record-created",
			LastSourceLine: 42,
			LastOffset:     4200,
			Status:         "complete",
			UpdatedAt:      updateTime,
		}, true, nil
	})
	if err != nil {
		t.Fatalf("UpdateImportCheckpoint create error: %v", err)
	}
	if !changed || created.ID != "transcript:created" || created.LastRecordID != "record-created" {
		t.Fatalf("created checkpoint = %#v changed=%v", created, changed)
	}
	got, found, err := store.ImportCheckpoint(ctx, "transcript:created")
	if err != nil {
		t.Fatalf("ImportCheckpoint created row error: %v", err)
	}
	if !found || got.SessionID != "session-created" || got.LastSourceLine != 42 || got.LastOffset != 4200 {
		t.Fatalf("created row = %#v found=%v", got, found)
	}
}

func TestSQLiteHotPollWorkCandidatesReturnsOnlyPositiveDurableCandidates(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 9, 11, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s-clean"] = SessionContext{ID: "s-clean", Status: SessionStatusActive, TeamsChatID: "chat-clean", UpdatedAt: now.Add(-time.Hour)}
		state.Sessions["s-dirty"] = SessionContext{ID: "s-dirty", Status: SessionStatusActive, TeamsChatID: "chat-dirty", UpdatedAt: now.Add(-2 * time.Hour)}
		state.Sessions["s-warm"] = SessionContext{ID: "s-warm", Status: SessionStatusActive, TeamsChatID: "chat-warm", UpdatedAt: now}
		state.Sessions["s-missing"] = SessionContext{ID: "s-missing", Status: SessionStatusActive, TeamsChatID: "chat-missing", UpdatedAt: now.Add(time.Minute)}
		state.Sessions["s-closed"] = SessionContext{ID: "s-closed", Status: SessionStatusClosed, TeamsChatID: "chat-closed", UpdatedAt: now.Add(2 * time.Minute)}
		state.Sessions["s-control"] = SessionContext{ID: "s-control", Status: SessionStatusActive, TeamsChatID: "control-chat", UpdatedAt: now.Add(3 * time.Minute)}
		state.ChatPolls["chat-clean"] = ChatPollState{ChatID: "chat-clean", Seeded: true, PollState: "parked", ParkedAt: now.Add(-48 * time.Hour), ParkNoticeSentAt: now.Add(-47 * time.Hour), UpdatedAt: now}
		state.ChatPolls["chat-dirty"] = ChatPollState{ChatID: "chat-dirty", Seeded: true, PollState: "parked", ParkedAt: now.Add(-48 * time.Hour), ParkNoticeSentAt: now.Add(-47 * time.Hour), ContinuationPath: "/chats/chat-dirty/messages?$skiptoken=stale", UpdatedAt: now}
		state.ChatPolls["chat-warm"] = ChatPollState{ChatID: "chat-warm", Seeded: true, PollState: "warm", NextPollAt: now.Add(-time.Second), UpdatedAt: now}
		state.ChatPolls["chat-closed"] = ChatPollState{ChatID: "chat-closed", Seeded: true, PollState: "warm", NextPollAt: now.Add(-time.Second), UpdatedAt: now}
		state.ChatPolls["control-chat"] = ChatPollState{ChatID: "control-chat", Seeded: true, PollState: "warm", NextPollAt: now.Add(-time.Second), UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed work candidates: %v", err)
	}

	if candidates, handled, err := store.HotPollWorkCandidates(ctx, "control-chat"); err != nil || handled || len(candidates) != 0 {
		t.Fatalf("json HotPollWorkCandidates = candidates=%#v handled=%v err=%v, want legacy fallback", candidates, handled, err)
	}

	migrateStoreToSQLiteForTest(t, store)
	candidates, handled, err := store.HotPollWorkCandidates(ctx, "control-chat")
	if err != nil {
		t.Fatalf("sqlite HotPollWorkCandidates error: %v", err)
	}
	if !handled {
		t.Fatal("sqlite HotPollWorkCandidates was not handled")
	}
	got := make(map[string]bool, len(candidates))
	for _, session := range candidates {
		got[session.ID] = true
	}
	want := map[string]bool{"s-dirty": true, "s-warm": true, "s-missing": true}
	if len(got) != len(want) {
		t.Fatalf("candidate ids = %#v, want %#v", got, want)
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("candidate ids = %#v, missing %s", got, id)
		}
	}
	for _, id := range []string{"s-clean", "s-closed", "s-control"} {
		if got[id] {
			t.Fatalf("candidate ids = %#v, should not include %s", got, id)
		}
	}
}

func TestSQLiteHotPollWorkCandidatesCanExcludeIdleAutoParkCandidates(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 9, 11, 0, 0, 0, time.UTC)
	oldActivity := now.Add(-49 * time.Hour)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s-idle"] = SessionContext{ID: "s-idle", Status: SessionStatusActive, TeamsChatID: "chat-idle", UpdatedAt: oldActivity}
		state.Sessions["s-running"] = SessionContext{ID: "s-running", Status: SessionStatusActive, TeamsChatID: "chat-running", UpdatedAt: oldActivity}
		state.Sessions["s-recent"] = SessionContext{ID: "s-recent", Status: SessionStatusActive, TeamsChatID: "chat-recent", UpdatedAt: now}
		state.ChatPolls["chat-idle"] = ChatPollState{ChatID: "chat-idle", Seeded: true, PollState: chatPollStateCold, LastActivityAt: oldActivity, NextPollAt: now.Add(-time.Minute), UpdatedAt: now}
		state.ChatPolls["chat-running"] = ChatPollState{ChatID: "chat-running", Seeded: true, PollState: chatPollStateCold, LastActivityAt: oldActivity, NextPollAt: now.Add(-time.Minute), UpdatedAt: now}
		state.ChatPolls["chat-recent"] = ChatPollState{ChatID: "chat-recent", Seeded: true, PollState: chatPollStateCold, LastActivityAt: oldActivity, NextPollAt: now.Add(-time.Minute), UpdatedAt: now}
		state.Turns["turn-running"] = Turn{ID: "turn-running", SessionID: "s-running", Status: TurnStatusRunning, StartedAt: now.Add(-time.Minute), CreatedAt: now.Add(-time.Minute)}
		return nil
	}); err != nil {
		t.Fatalf("seed idle candidates: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	candidates, handled, err := store.HotPollWorkCandidatesExcludingIdle(ctx, "control-chat", now.Add(-48*time.Hour))
	if err != nil {
		t.Fatalf("HotPollWorkCandidatesExcludingIdle error: %v", err)
	}
	if !handled {
		t.Fatal("HotPollWorkCandidatesExcludingIdle was not handled")
	}
	got := make(map[string]bool, len(candidates))
	for _, session := range candidates {
		got[session.ID] = true
	}
	if got["s-idle"] {
		t.Fatalf("idle auto-park candidate stayed in hot poll candidates: %#v", got)
	}
	for _, id := range []string{"s-running", "s-recent"} {
		if !got[id] {
			t.Fatalf("candidate ids = %#v, missing %s", got, id)
		}
	}

	parkCandidates, handled, err := store.IdleWorkChatParkCandidates(ctx, "control-chat", now.Add(-48*time.Hour), 16)
	if err != nil {
		t.Fatalf("IdleWorkChatParkCandidates error: %v", err)
	}
	if !handled {
		t.Fatal("IdleWorkChatParkCandidates was not handled")
	}
	if len(parkCandidates) != 1 || parkCandidates[0].Session.ID != "s-idle" || parkCandidates[0].Poll.ChatID != "chat-idle" {
		t.Fatalf("park candidates = %#v, want only s-idle", parkCandidates)
	}
}

func TestStoreSessionsByIDFiltersRequestedSessions(t *testing.T) {
	for _, migrate := range []bool{false, true} {
		name := "json"
		if migrate {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			now := time.Date(2026, 6, 9, 10, 30, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["s1"] = SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1", UpdatedAt: now}
				state.Sessions["s2"] = SessionContext{ID: "s2", Status: SessionStatusClosed, TeamsChatID: "chat-2", UpdatedAt: now}
				state.Sessions["s3"] = SessionContext{ID: "s3", Status: SessionStatusActive, TeamsChatID: "chat-3", UpdatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed sessions: %v", err)
			}
			if migrate {
				migrateStoreToSQLiteForTest(t, store)
			}

			sessions, err := store.SessionsByID(ctx, []string{"s2", "missing", "s1", "s1", ""})
			if err != nil {
				t.Fatalf("SessionsByID: %v", err)
			}
			if len(sessions) != 2 || sessions["s1"].TeamsChatID != "chat-1" || sessions["s2"].Status != SessionStatusClosed {
				t.Fatalf("SessionsByID returned %#v", sessions)
			}
			if _, ok := sessions["s3"]; ok {
				t.Fatalf("SessionsByID loaded unrequested session: %#v", sessions["s3"])
			}
		})
	}
}

func TestSQLiteHotPollScheduleSnapshotBackfillsParkedSkipColumns(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls["chat-parked-clean"] = ChatPollState{ChatID: "chat-parked-clean", Seeded: true, PollState: "parked", ParkedAt: now.Add(-48 * time.Hour), ParkNoticeSentAt: now.Add(-47 * time.Hour), UpdatedAt: now}
		state.ChatPolls["chat-parked-stale"] = ChatPollState{ChatID: "chat-parked-stale", Seeded: true, PollState: "parked", ParkedAt: now.Add(-48 * time.Hour), ParkNoticeSentAt: now.Add(-47 * time.Hour), ContinuationPath: "/chats/chat-parked-stale/messages?$skiptoken=stale", UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed parked polls: %v", err)
	}
	result := migrateStoreToSQLiteForTest(t, store)
	if err := store.Close(); err != nil {
		t.Fatalf("close migrated store: %v", err)
	}
	db, err := openSQLiteHandle(result.Path, false)
	if err != nil {
		t.Fatalf("open migrated sqlite db: %v", err)
	}
	if _, err := db.Exec(`UPDATE chat_polls SET park_notice_sent_at = NULL, parked_skip_eligible = NULL`); err != nil {
		_ = db.Close()
		t.Fatalf("clear derived columns: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite db after clearing columns: %v", err)
	}
	reopened, err := Open(store.Path())
	if err != nil {
		t.Fatalf("reopen sqlite-backed store: %v", err)
	}
	defer reopened.Close()

	hotPollSchedule, parkedSkip, err := reopened.HotPollScheduleSnapshot(ctx)
	if err != nil {
		t.Fatalf("HotPollScheduleSnapshot after derived-column backfill: %v", err)
	}
	if _, ok := hotPollSchedule.ChatPolls["chat-parked-clean"]; ok || !parkedSkip["chat-parked-clean"] {
		t.Fatalf("backfill should make clean parked row skippable: polls=%#v skip=%#v", hotPollSchedule.ChatPolls, parkedSkip)
	}
	if hotPollSchedule.ChatPolls["chat-parked-stale"].ContinuationPath == "" || parkedSkip["chat-parked-stale"] {
		t.Fatalf("backfill should keep stale parked row in hot snapshot: polls=%#v skip=%#v", hotPollSchedule.ChatPolls, parkedSkip)
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
			defer worker.Close()
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
			defer worker.Close()
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
	defer other.Close()
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
	defer other.Close()
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

func TestBindSessionCodexThreadSQLiteUpdatesOnlySessionAndTurn(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, created, err := store.CreateSession(ctx, SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1"}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	} else if !created {
		t.Fatal("CreateSession created = false")
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
		t.Fatal("QueueTurn created = false")
	}
	if err := store.Update(ctx, func(state *State) error {
		state.DashboardNumbers["large"] = DashboardNumberRecord{
			ID:        "large",
			ChatID:    "control-chat",
			Kind:      "workspace",
			Number:    1,
			Label:     strings.Repeat("dashboard ", 4096),
			UpdatedAt: time.Date(2026, 6, 11, 9, 30, 0, 0, time.UTC),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cold state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	before := sqliteRawStateJSONForTest(t, store)

	session, changed, err := store.BindSessionCodexThread(ctx, "s1", turn.ID, "thread-fast")
	if err != nil {
		t.Fatalf("BindSessionCodexThread error: %v", err)
	}
	if !changed {
		t.Fatal("BindSessionCodexThread changed = false")
	}
	if session.CodexThreadID != "thread-fast" {
		t.Fatalf("returned session CodexThreadID = %q, want thread-fast", session.CodexThreadID)
	}
	after := sqliteRawStateJSONForTest(t, store)
	if !bytes.Equal(before, after) {
		t.Fatalf("BindSessionCodexThread rewrote cold state_json: before=%d after=%d", len(before), len(after))
	}
	noOpBefore := sqliteRawStateJSONForTest(t, store)
	session, changed, err = store.BindSessionCodexThread(ctx, "s1", turn.ID, "thread-fast")
	if err != nil {
		t.Fatalf("BindSessionCodexThread no-op error: %v", err)
	}
	if changed {
		t.Fatal("BindSessionCodexThread no-op changed = true")
	}
	if session.CodexThreadID != "thread-fast" {
		t.Fatalf("no-op returned session CodexThreadID = %q, want thread-fast", session.CodexThreadID)
	}
	noOpAfter := sqliteRawStateJSONForTest(t, store)
	if !bytes.Equal(noOpBefore, noOpAfter) {
		t.Fatalf("BindSessionCodexThread no-op rewrote cold state_json: before=%d after=%d", len(noOpBefore), len(noOpAfter))
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after bind error: %v", err)
	}
	if got := state.Sessions["s1"].CodexThreadID; got != "thread-fast" {
		t.Fatalf("session CodexThreadID = %q, want thread-fast", got)
	}
	if got := state.Turns[turn.ID].CodexThreadID; got != "thread-fast" {
		t.Fatalf("turn CodexThreadID = %q, want thread-fast", got)
	}
}

func TestBindSessionCodexThreadSQLiteRejectsConflicts(t *testing.T) {
	ctx := context.Background()
	t.Run("session", func(t *testing.T) {
		store := newTestStore(t)
		session := SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1", CodexThreadID: "thread-existing"}
		if _, created, err := store.CreateSession(ctx, session); err != nil {
			t.Fatalf("CreateSession error: %v", err)
		} else if !created {
			t.Fatal("CreateSession created = false")
		}
		migrateStoreToSQLiteForTest(t, store)

		_, _, err := store.BindSessionCodexThread(ctx, "s1", "", "thread-new")
		var conflict CodexThreadBindingConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("BindSessionCodexThread error = %v, want CodexThreadBindingConflictError", err)
		}
		state, loadErr := store.Load(ctx)
		if loadErr != nil {
			t.Fatalf("Load after conflict error: %v", loadErr)
		}
		if got := state.Sessions["s1"].CodexThreadID; got != "thread-existing" {
			t.Fatalf("session CodexThreadID after conflict = %q, want thread-existing", got)
		}
	})

	t.Run("turn", func(t *testing.T) {
		store := newTestStore(t)
		if _, created, err := store.CreateSession(ctx, SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1"}); err != nil {
			t.Fatalf("CreateSession error: %v", err)
		} else if !created {
			t.Fatal("CreateSession created = false")
		}
		inbound, _, err := store.PersistInbound(ctx, testInbound())
		if err != nil {
			t.Fatalf("PersistInbound error: %v", err)
		}
		turn, created, err := store.QueueTurn(ctx, Turn{SessionID: "s1", InboundEventID: inbound.ID, CodexThreadID: "thread-existing"})
		if err != nil {
			t.Fatalf("QueueTurn error: %v", err)
		}
		if !created {
			t.Fatal("QueueTurn created = false")
		}
		migrateStoreToSQLiteForTest(t, store)

		_, _, err = store.BindSessionCodexThread(ctx, "s1", turn.ID, "thread-new")
		var conflict CodexThreadBindingConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("BindSessionCodexThread error = %v, want CodexThreadBindingConflictError", err)
		}
		state, loadErr := store.Load(ctx)
		if loadErr != nil {
			t.Fatalf("Load after conflict error: %v", loadErr)
		}
		if got := state.Sessions["s1"].CodexThreadID; got != "" {
			t.Fatalf("session CodexThreadID after turn conflict = %q, want empty", got)
		}
		if got := state.Turns[turn.ID].CodexThreadID; got != "thread-existing" {
			t.Fatalf("turn CodexThreadID after conflict = %q, want thread-existing", got)
		}
	})
}

func TestSQLiteQueueOutboxSequenceDoesNotRewriteColdState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC)
	if _, _, err := store.CreateSession(ctx, SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1"}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ChatSequences["chat-1"] = ChatSequenceState{ChatID: "chat-1", Next: 7, UpdatedAt: now}
		state.DashboardNumbers["large"] = DashboardNumberRecord{
			ID:        "large",
			ChatID:    "control-chat",
			Kind:      "workspace",
			Number:    1,
			Label:     strings.Repeat("dashboard ", 4096),
			UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed state error: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	before := sqliteRawStateJSONForTest(t, store)
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:seq",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "codex-progress-001",
		Body:        "progress",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	if !created {
		t.Fatal("QueueOutbox created = false")
	}
	if msg.Sequence != 7 {
		t.Fatalf("queued sequence = %d, want 7", msg.Sequence)
	}
	after := sqliteRawStateJSONForTest(t, store)
	if !bytes.Equal(before, after) {
		t.Fatalf("QueueOutbox rewrote cold state_json: before=%d after=%d", len(before), len(after))
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after QueueOutbox error: %v", err)
	}
	if next := state.ChatSequences["chat-1"].Next; next != 8 {
		t.Fatalf("chat sequence next = %d, want 8", next)
	}
	if state.OutboxMessages["outbox:seq"].Sequence != 7 {
		t.Fatalf("outbox sequence after load = %d, want 7", state.OutboxMessages["outbox:seq"].Sequence)
	}
	if len(state.HelperDeliveries) == 0 {
		t.Fatal("helper delivery ledger was not persisted")
	}
}

func TestSQLiteMarkTurnInterruptedPreservesChatSequences(t *testing.T) {
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
	migrateStoreToSQLiteForTest(t, store)

	if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "canceled by user"); err != nil {
		t.Fatalf("MarkTurnInterrupted error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after interrupt error: %v", err)
	}
	if got := state.OutboxMessages["outbox:status"].Status; got != OutboxStatusSkipped {
		t.Fatalf("status outbox status = %q, want skipped", got)
	}
	if got := state.OutboxMessages["outbox:final"].Status; got != OutboxStatusQueued {
		t.Fatalf("final outbox status = %q, want queued", got)
	}
	statusHelper := helperDeliveryForOutboxForTest(state, "outbox:status")
	if statusHelper.Status != HelperDeliveryStatusSkipped {
		t.Fatalf("status helper delivery = %#v, want skipped", statusHelper)
	}
	if finalHelper := helperDeliveryForOutboxForTest(state, "outbox:final"); finalHelper.ID != "" {
		t.Fatalf("final helper delivery = %#v, want no synthesized ledger for protected final", finalHelper)
	}
	if next := state.ChatSequences["chat-1"].Next; next != 3 {
		t.Fatalf("chat sequence next after interrupt = %d, want 3", next)
	}
}

func TestSQLiteMarkTurnLifecycleDoesNotLoadColdState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(ctx, testInbound())
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, Turn{ID: "turn:lifecycle", SessionID: "s1", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if err := store.UpdateSession(ctx, "s1", func(state *State) error {
		state.DashboardNumbers["cold"] = DashboardNumberRecord{ID: "cold", ChatID: "control-chat", Kind: "session", Number: 1, Label: strings.Repeat("cold ", 4096)}
		return nil
	}); err != nil {
		t.Fatalf("seed cold state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	corruptState := []byte(`{"broken"`)
	sqliteWriteRawStateJSONForTest(t, store, corruptState)

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	if _, err := store.MarkTurnRunning(ctx, turn.ID, "thread-1", "codex-turn-started"); err != nil {
		t.Fatalf("MarkTurnRunning error: %v", err)
	}
	completed, err := store.MarkTurnCompleted(ctx, turn.ID, "thread-1", "codex-turn-completed")
	if err != nil {
		t.Fatalf("MarkTurnCompleted error: %v", err)
	}
	if completed.Status != TurnStatusCompleted || completed.CodexTurnID != "codex-turn-completed" {
		t.Fatalf("completed turn = %#v", completed)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("turn lifecycle loaded full state %d times", got)
	}
	if afterState := sqliteRawStateJSONForTest(t, store); !bytes.Equal(corruptState, afterState) {
		t.Fatalf("turn lifecycle rewrote cold state_json: %s", afterState)
	}
	var rawTurn Turn
	if raw := sqliteRawTurnJSONForTest(t, store, turn.ID); json.Unmarshal(raw, &rawTurn) != nil {
		t.Fatalf("unmarshal raw turn")
	}
	if rawTurn.Status != TurnStatusCompleted || rawTurn.CodexThreadID != "thread-1" || rawTurn.CodexTurnID != "codex-turn-completed" {
		t.Fatalf("raw turn = %#v", rawTurn)
	}
	var session SessionContext
	if raw := sqliteRawSessionJSONForTest(t, store, "s1"); json.Unmarshal(raw, &session) != nil {
		t.Fatalf("unmarshal raw session")
	}
	if session.CodexThreadID != "thread-1" || session.LatestCodexTurnID != "codex-turn-completed" || session.LatestTurnID != turn.ID {
		t.Fatalf("raw session = %#v", session)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
}

func TestSQLiteOpenBackfillsLegacyChatSequencesTable(t *testing.T) {
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
	if err := ensureSQLiteSchema(db); err != nil {
		_ = db.Close()
		t.Fatalf("ensure sqlite schema: %v", err)
	}
	if _, err := db.Exec(`DROP TABLE chat_sequences`); err != nil {
		_ = db.Close()
		t.Fatalf("drop current chat_sequences: %v", err)
	}
	legacy := newState()
	legacy.ChatSequences["chat-legacy"] = ChatSequenceState{ChatID: "chat-legacy", Next: 42, UpdatedAt: time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)}
	raw, err := json.Marshal(legacy)
	if err != nil {
		_ = db.Close()
		t.Fatalf("marshal legacy state: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_meta(key, value) VALUES ('state_json', ?)`, raw); err != nil {
		_ = db.Close()
		t.Fatalf("insert legacy state_json: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy sqlite db: %v", err)
	}
	writeSQLitePointerForTest(t, store, storeSQLiteFileName)

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load legacy sqlite with chat sequences error: %v", err)
	}
	if next := state.ChatSequences["chat-legacy"].Next; next != 42 {
		t.Fatalf("loaded legacy chat sequence next = %d, want 42", next)
	}
	msg, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:legacy-seq",
		TeamsChatID: "chat-legacy",
		Kind:        "control",
		Body:        "legacy",
	})
	if err != nil {
		t.Fatalf("QueueOutbox legacy sequence error: %v", err)
	}
	if msg.Sequence != 42 {
		t.Fatalf("legacy queued sequence = %d, want 42", msg.Sequence)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after legacy QueueOutbox error: %v", err)
	}
	if next := state.ChatSequences["chat-legacy"].Next; next != 43 {
		t.Fatalf("legacy chat sequence next after queue = %d, want 43", next)
	}
}

func TestSQLiteOpenBackfillsLegacyChatSequencesWhenTableAlreadyExists(t *testing.T) {
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
	if err := ensureSQLiteSchema(db); err != nil {
		_ = db.Close()
		t.Fatalf("ensure sqlite schema: %v", err)
	}
	legacy := newState()
	legacy.ChatSequences["chat-existing-table"] = ChatSequenceState{ChatID: "chat-existing-table", Next: 17, UpdatedAt: time.Date(2026, 6, 10, 8, 30, 0, 0, time.UTC)}
	raw, err := json.Marshal(legacy)
	if err != nil {
		_ = db.Close()
		t.Fatalf("marshal legacy state: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_meta(key, value) VALUES ('state_json', ?)`, raw); err != nil {
		_ = db.Close()
		t.Fatalf("insert legacy state_json: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy sqlite db: %v", err)
	}
	writeSQLitePointerForTest(t, store, storeSQLiteFileName)

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load legacy sqlite with existing chat_sequences table error: %v", err)
	}
	if next := state.ChatSequences["chat-existing-table"].Next; next != 17 {
		t.Fatalf("loaded chat sequence next = %d, want 17", next)
	}
}

func TestSQLiteQueueOutboxSelfHealsMissingChatSequenceFromOutbox(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s1"] = SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1", CreatedAt: now, UpdatedAt: now}
		state.OutboxMessages["outbox:existing"] = OutboxMessage{
			ID:          "outbox:existing",
			SessionID:   "s1",
			TeamsChatID: "chat-1",
			Kind:        "final",
			Body:        "existing",
			Status:      OutboxStatusSent,
			Sequence:    9,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy outbox state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:next",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "next",
	})
	if err != nil {
		t.Fatalf("QueueOutbox after missing chat sequence error: %v", err)
	}
	if !created {
		t.Fatal("QueueOutbox created = false")
	}
	if msg.Sequence != 10 {
		t.Fatalf("self-healed sequence = %d, want 10", msg.Sequence)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after self-heal error: %v", err)
	}
	if next := state.ChatSequences["chat-1"].Next; next != 11 {
		t.Fatalf("chat sequence next after self-heal = %d, want 11", next)
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

func TestSQLiteUpsertArtifactRecordDoesNotLoadColdStateAndPreservesMergeFields(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	createdAt := time.Date(2026, 6, 16, 8, 0, 0, 0, time.UTC)
	uploadedAt := createdAt.Add(time.Minute)
	sentAt := createdAt.Add(2 * time.Minute)
	if _, err := store.UpsertArtifactRecord(ctx, ArtifactRecord{
		ID:             "artifact:merge",
		SessionID:      "s1",
		TurnID:         "turn-1",
		Path:           "artifact.txt",
		UploadName:     "codex-artifact.txt",
		OutboxID:       "outbox-1",
		DriveItemID:    "drive-item-1",
		TeamsMessageID: "teams-message-1",
		Status:         "uploaded",
		UploadedAt:     uploadedAt,
		SentAt:         sentAt,
		CreatedAt:      createdAt,
		UpdatedAt:      createdAt,
	}); err != nil {
		t.Fatalf("seed UpsertArtifactRecord error: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	sqliteWriteRawStateJSONForTest(t, store, []byte(`{"broken"`))

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	updated, err := store.UpsertArtifactRecord(ctx, ArtifactRecord{
		ID:        "artifact:merge",
		SessionID: "s1",
		Status:    "failed",
		Error:     strings.Repeat("e", 300),
	})
	if err != nil {
		t.Fatalf("UpsertArtifactRecord sqlite error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("UpsertArtifactRecord loaded full state %d times", got)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
	if !updated.CreatedAt.Equal(createdAt) || updated.OutboxID != "outbox-1" || updated.DriveItemID != "drive-item-1" || updated.TeamsMessageID != "teams-message-1" || !updated.UploadedAt.Equal(uploadedAt) || !updated.SentAt.Equal(sentAt) {
		t.Fatalf("updated artifact did not preserve merge fields: %#v", updated)
	}
	if updated.Status != "failed" || len(updated.Error) != 240 {
		t.Fatalf("updated artifact status/error mismatch: %#v", updated)
	}
	raw := sqliteRawArtifactRecordForTest(t, store, "artifact:merge")
	var stored ArtifactRecord
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("unmarshal stored artifact: %v", err)
	}
	if stored.ID != updated.ID || stored.SessionID != updated.SessionID || stored.Status != updated.Status || stored.Error != updated.Error ||
		stored.OutboxID != updated.OutboxID || stored.DriveItemID != updated.DriveItemID || stored.TeamsMessageID != updated.TeamsMessageID ||
		!stored.CreatedAt.Equal(updated.CreatedAt) || !stored.UpdatedAt.Equal(updated.UpdatedAt) || !stored.UploadedAt.Equal(updated.UploadedAt) || !stored.SentAt.Equal(updated.SentAt) {
		t.Fatalf("stored artifact differs from returned artifact:\nstored=%#v\nupdated=%#v", stored, updated)
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
			defer workerStore.Close()
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
			defer workerStore.Close()
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
			defer workerStore.Close()
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

func TestPendingOutboxPageMatchesFullPendingAcrossBackends(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
			messages := []OutboxMessage{
				{ID: "outbox:queued-a", SessionID: "s1", TurnID: "turn-1", TeamsChatID: "chat-open", Kind: "helper", Body: "queued-a", CreatedAt: now.Add(time.Millisecond)},
				{ID: "outbox:blocked", SessionID: "s1", TurnID: "turn-1", TeamsChatID: "chat-blocked", Kind: "helper", Body: "blocked", CreatedAt: now.Add(2 * time.Millisecond)},
				{ID: "outbox:accepted-with-id", SessionID: "s1", TurnID: "turn-1", TeamsChatID: "chat-blocked", Kind: "helper", Body: "accepted", Status: OutboxStatusAccepted, TeamsMessageID: "teams-accepted", CreatedAt: now.Add(3 * time.Millisecond)},
				{ID: "outbox:accepted-without-id", SessionID: "s1", TurnID: "turn-1", TeamsChatID: "chat-open", Kind: "helper", Body: "accepted-without-id", Status: OutboxStatusAccepted, CreatedAt: now.Add(4 * time.Millisecond)},
				{ID: "outbox:fresh-sending", SessionID: "s1", TurnID: "turn-1", TeamsChatID: "chat-open", Kind: "helper", Body: "fresh", Status: OutboxStatusSending, LastSendAttempt: now.Add(-outboxSendLease + time.Second), CreatedAt: now.Add(5 * time.Millisecond)},
				{ID: "outbox:stale-sending", SessionID: "s1", TurnID: "turn-1", TeamsChatID: "chat-open", Kind: "helper", Body: "stale", Status: OutboxStatusSending, LastSendAttempt: now.Add(-outboxSendLease - time.Second), CreatedAt: now.Add(6 * time.Millisecond)},
				{ID: "outbox:sent", SessionID: "s1", TurnID: "turn-1", TeamsChatID: "chat-open", Kind: "helper", Body: "sent", Status: OutboxStatusSent, TeamsMessageID: "teams-sent", CreatedAt: now.Add(7 * time.Millisecond)},
				{ID: "outbox:queued-b", SessionID: "s2", TurnID: "turn-2", TeamsChatID: "chat-open", Kind: "helper", Body: "queued-b", CreatedAt: now.Add(8 * time.Millisecond)},
			}
			for _, msg := range messages {
				if _, _, err := store.QueueOutbox(ctx, msg); err != nil {
					t.Fatalf("QueueOutbox %s error: %v", msg.ID, err)
				}
			}
			if _, err := store.SetChatRateLimit(ctx, "chat-blocked", now.Add(time.Hour), "429"); err != nil {
				t.Fatalf("SetChatRateLimit error: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}

			full, err := store.PendingOutboxAt(ctx, now)
			if err != nil {
				t.Fatalf("PendingOutboxAt error: %v", err)
			}
			gotIDs := collectPendingOutboxPageIDsForTest(t, store, PendingOutboxQuery{Now: now, Limit: 2})
			wantIDs := outboxIDsForTest(full)
			if !reflect.DeepEqual(gotIDs, wantIDs) {
				t.Fatalf("paged pending IDs = %#v, want full pending IDs %#v; full=%#v", gotIDs, wantIDs, full)
			}
			filteredIDs := collectPendingOutboxPageIDsForTest(t, store, PendingOutboxQuery{
				Now:         now,
				SessionID:   "s1",
				TurnID:      "turn-1",
				TeamsChatID: "chat-open",
				Limit:       1,
			})
			wantFiltered := []string{"outbox:queued-a", "outbox:stale-sending"}
			if !reflect.DeepEqual(filteredIDs, wantFiltered) {
				t.Fatalf("filtered paged pending IDs = %#v, want %#v", filteredIDs, wantFiltered)
			}
		})
	}
}

func TestPendingOutboxPageDoesNotStarveBehindFreshSendingRowsAcrossBackends(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			now := time.Date(2026, 5, 30, 11, 0, 0, 0, time.UTC)
			for i := 0; i < 5; i++ {
				msg := OutboxMessage{
					ID:              fmt.Sprintf("outbox:fresh-%02d", i),
					TeamsChatID:     "chat-1",
					Kind:            "helper",
					Body:            "fresh sending lease",
					Status:          OutboxStatusSending,
					LastSendAttempt: now.Add(-time.Second),
					CreatedAt:       now.Add(time.Duration(i) * time.Millisecond),
				}
				if _, _, err := store.QueueOutbox(ctx, msg); err != nil {
					t.Fatalf("QueueOutbox fresh %d error: %v", i, err)
				}
			}
			due := OutboxMessage{
				ID:          "outbox:due-behind-fresh",
				TeamsChatID: "chat-1",
				Kind:        "helper",
				Body:        "due",
				CreatedAt:   now.Add(10 * time.Millisecond),
			}
			if _, _, err := store.QueueOutbox(ctx, due); err != nil {
				t.Fatalf("QueueOutbox due error: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}

			gotIDs := collectPendingOutboxPageIDsForTest(t, store, PendingOutboxQuery{Now: now, Limit: 2})
			if !reflect.DeepEqual(gotIDs, []string{due.ID}) {
				t.Fatalf("paged pending behind fresh sending = %#v, want only %s", gotIDs, due.ID)
			}
		})
	}
}

func TestSetChatRateLimitForOutboxPreservesPoisonAcrossBackends(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			if sqlite {
				if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
					ID:          "outbox:migration-seed",
					TeamsChatID: "migration-chat",
					Kind:        "helper",
					Body:        "migration seed",
				}); err != nil {
					t.Fatalf("QueueOutbox migration seed error: %v", err)
				}
				migrateStoreToSQLiteForTest(t, store)
			}
			blockedUntil := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
			limit, err := store.SetChatRateLimitForOutbox(ctx, " chat-1 ", blockedUntil, "429", " outbox:poison ")
			if err != nil {
				t.Fatalf("SetChatRateLimitForOutbox error: %v", err)
			}
			if limit.ChatID != "chat-1" || limit.PoisonOutboxID != "outbox:poison" {
				t.Fatalf("rate limit with poison = %#v, want trimmed chat and poison outbox", limit)
			}
			limit, err = store.SetChatRateLimit(ctx, "chat-1", blockedUntil.Add(time.Minute), "429 again")
			if err != nil {
				t.Fatalf("SetChatRateLimit without outbox error: %v", err)
			}
			if limit.PoisonOutboxID != "outbox:poison" {
				t.Fatalf("rate limit poison after ordinary update = %q, want preserved poison", limit.PoisonOutboxID)
			}
		})
	}
}

func collectPendingOutboxPageIDsForTest(t *testing.T, store *Store, query PendingOutboxQuery) []string {
	t.Helper()
	ctx := context.Background()
	var ids []string
	seen := map[string]bool{}
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatalf("pending outbox pagination did not finish; ids=%#v cursor=%#v", ids, query.After)
		}
		page, err := store.PendingOutboxPageAt(ctx, query)
		if err != nil {
			t.Fatalf("PendingOutboxPageAt error: %v", err)
		}
		for _, msg := range page.Messages {
			if seen[msg.ID] {
				t.Fatalf("duplicate outbox %q across pending pages; ids=%#v", msg.ID, ids)
			}
			seen[msg.ID] = true
			ids = append(ids, msg.ID)
		}
		if !page.More {
			return ids
		}
		if page.NextCursor.IsZero() {
			t.Fatalf("pending outbox page had More=true without cursor; ids=%#v", ids)
		}
		query.After = page.NextCursor
	}
}

func outboxIDsForTest(messages []OutboxMessage) []string {
	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		ids = append(ids, msg.ID)
	}
	return ids
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

func TestServiceControlDrainOperationFence(t *testing.T) {
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			acquired, err := store.SetDrainingOperation(ctx, "chat recreate", "operation-1")
			if err != nil {
				t.Fatalf("SetDrainingOperation: %v", err)
			}
			if !acquired.Draining || acquired.DrainOperationID != "operation-1" || acquired.LastDrainOperationID != "operation-1" || acquired.LastDrainOperationAt.IsZero() {
				t.Fatalf("acquired control = %#v", acquired)
			}
			beforeIdempotent := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(store.Path()))
			idempotent, err := store.SetDrainingOperation(ctx, "chat recreate", "operation-1")
			if err != nil {
				t.Fatalf("idempotent SetDrainingOperation: %v", err)
			}
			if !sameControl(idempotent, acquired) {
				t.Fatalf("idempotent fence changed control: got %#v want %#v", idempotent, acquired)
			}
			afterIdempotent := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(store.Path()))
			for name := range beforeIdempotent {
				if strings.HasSuffix(name, "-shm") {
					delete(beforeIdempotent, name)
					delete(afterIdempotent, name)
				}
			}
			if !reflect.DeepEqual(beforeIdempotent, afterIdempotent) {
				t.Fatalf("idempotent fence rewrote persistent files:\nbefore=%#v\nafter=%#v", beforeIdempotent, afterIdempotent)
			}
			reopened, err := Open(store.Path())
			if err != nil {
				t.Fatalf("reopen fenced store: %v", err)
			}
			persisted, err := reopened.ReadControl(ctx)
			if closeErr := reopened.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
			if err != nil {
				t.Fatalf("read persisted fence: %v", err)
			}
			if !persisted.Draining || persisted.DrainOperationID != "operation-1" || persisted.LastDrainOperationID != "operation-1" || persisted.LastDrainOperationAt.IsZero() {
				t.Fatalf("persisted fence = %#v", persisted)
			}
			if _, err := store.SetDrainingOperation(ctx, "chat recreate", "operation-2"); !errors.Is(err, ErrDrainOperationConflict) {
				t.Fatalf("competing SetDrainingOperation error = %v", err)
			}
			if _, err := store.SetDraining(ctx, "upgrade"); !errors.Is(err, ErrDrainOperationConflict) {
				t.Fatalf("unfenced SetDraining error = %v", err)
			}
			if _, err := store.BeginUpgrade(ctx, HelperUpgradeReason, time.Minute); !errors.Is(err, ErrDrainOperationConflict) {
				t.Fatalf("BeginUpgrade while fenced error = %v", err)
			}
			if _, err := store.ClearDrainOperation(ctx, "operation-2"); !errors.Is(err, ErrDrainOperationConflict) {
				t.Fatalf("stale ClearDrainOperation error = %v", err)
			}
			stillFenced, err := store.ReadControl(ctx)
			if err != nil {
				t.Fatalf("ReadControl: %v", err)
			}
			if !stillFenced.Draining || stillFenced.DrainOperationID != "operation-1" {
				t.Fatalf("stale cleanup changed fence: %#v", stillFenced)
			}
			cleared, err := store.ClearDrainOperation(ctx, "operation-1")
			if err != nil {
				t.Fatalf("ClearDrainOperation owner: %v", err)
			}
			if cleared.Draining || cleared.DrainOperationID != "" || cleared.LastDrainOperationID != "operation-1" || cleared.LastDrainOperationAt.IsZero() {
				t.Fatalf("cleared control = %#v", cleared)
			}
			if _, err := store.ClearDrainOperation(ctx, "operation-1"); err != nil {
				t.Fatalf("idempotent ClearDrainOperation: %v", err)
			}
		})
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

func TestAbortExpiredHelperUpgradeDrainPreservesPreviousDrain(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)

	if _, err := store.SetDraining(ctx, "maintenance"); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	req, err := store.BeginUpgrade(ctx, HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Upgrade.DeadlineAt = now.Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatalf("expire upgrade error: %v", err)
	}

	aborted, changed, err := store.AbortExpiredHelperUpgradeDrain(ctx, req.ID, now, time.Minute, "watchdog reconciled expired helper upgrade")
	if err != nil {
		t.Fatalf("AbortExpiredHelperUpgradeDrain error: %v", err)
	}
	if !changed || aborted.Phase != UpgradePhaseAborted {
		t.Fatalf("aborted=%#v changed=%v, want aborted change", aborted, changed)
	}
	control, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if !control.Draining || control.Reason != "maintenance" {
		t.Fatalf("control after guarded abort = %#v, want previous maintenance drain", control)
	}
}

func TestAbortExpiredHelperUpgradeDrainPreservesFreshActiveOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	req, err := store.BeginUpgrade(ctx, HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	owner := OwnerMetadata{
		PID:           os.Getpid(),
		Hostname:      hostname,
		StartedAt:     now.Add(-time.Hour),
		LastHeartbeat: now.Add(-time.Second),
		ActiveTurnID:  "turn-live",
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Upgrade.DeadlineAt = now.Add(-time.Minute)
		state.ServiceOwner = &owner
		state.LockOwner = &owner
		return nil
	}); err != nil {
		t.Fatalf("seed active owner error: %v", err)
	}

	aborted, changed, err := store.AbortExpiredHelperUpgradeDrain(ctx, req.ID, now, time.Minute, "watchdog reconciled expired helper upgrade")
	if err != nil {
		t.Fatalf("AbortExpiredHelperUpgradeDrain error: %v", err)
	}
	if changed || aborted.ID != "" {
		t.Fatalf("aborted=%#v changed=%v, want no change while local active turn owns drain", aborted, changed)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if !state.ServiceControl.Draining || state.Upgrade == nil || state.Upgrade.Phase != UpgradePhaseDraining {
		t.Fatalf("state was cleared despite fresh active owner: control=%#v upgrade=%#v", state.ServiceControl, state.Upgrade)
	}
}

func TestClearStaleHelperReloadDrainPreservesFreshDrain(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ServiceControl = ServiceControl{
			Draining:  true,
			Reason:    HelperReloadReason,
			UpdatedAt: now.Add(-time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed reload drain error: %v", err)
	}

	control, changed, err := store.ClearStaleHelperReloadDrain(ctx, now, 6*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("ClearStaleHelperReloadDrain error: %v", err)
	}
	if changed || !control.Draining || control.Reason != HelperReloadReason {
		t.Fatalf("control=%#v changed=%v, want fresh reload drain preserved", control, changed)
	}
}

func TestClearStaleHelperReloadDrainPreservesFreshRemoteOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	owner := OwnerMetadata{
		PID:           4242,
		Hostname:      "remote-shared-home-host",
		StartedAt:     now.Add(-time.Hour),
		LastHeartbeat: now.Add(-time.Second),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ServiceControl = ServiceControl{
			Draining:  true,
			Reason:    HelperReloadReason,
			UpdatedAt: now.Add(-10 * time.Minute),
		}
		state.ServiceOwner = &owner
		state.LockOwner = &owner
		return nil
	}); err != nil {
		t.Fatalf("seed stale reload drain error: %v", err)
	}

	control, changed, err := store.ClearStaleHelperReloadDrain(ctx, now, 6*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("ClearStaleHelperReloadDrain error: %v", err)
	}
	if changed || !control.Draining || control.Reason != HelperReloadReason {
		t.Fatalf("control=%#v changed=%v, want fresh remote reload drain preserved", control, changed)
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
	defer reopened.Close()
	read, err := reopened.ReadAutoUpdate(ctx)
	if err != nil {
		t.Fatalf("ReadAutoUpdate error: %v", err)
	}
	if read.LastInstalledTag != "v1.2.4" || read.LastAttemptTag != "v1.2.4" {
		t.Fatalf("reopened auto-update state mismatch: %#v", read)
	}
}

func TestReadAutoUpdateControlSQLiteDoesNotLoadHotTables(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now()
	if err := store.Update(ctx, func(state *State) error {
		state.ServiceControl = ServiceControl{
			Paused:    true,
			Reason:    "maintenance",
			UpdatedAt: now,
		}
		state.AutoUpdate = AutoUpdateState{
			NextCheckAt: now.Add(30 * time.Minute),
			LastCheckAt: now.Add(-time.Minute),
		}
		for i := 0; i < 128; i++ {
			id := fmt.Sprintf("auto-update-hot-%03d", i)
			state.InboundEvents[id] = InboundEvent{
				ID:        id,
				SessionID: "s001",
				Text:      strings.Repeat("hot payload ", 256),
				Status:    InboundStatusPersisted,
				CreatedAt: now,
				UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed auto-update state: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	res, err := db.ExecContext(ctx, `UPDATE inbound_events SET json = ? WHERE id = ?`, []byte(`{"broken"`), "auto-update-hot-000")
	if err != nil {
		t.Fatalf("corrupt hot inbound row: %v", err)
	}
	if rows, err := res.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("corrupt hot inbound rows affected = %d err=%v, want 1", rows, err)
	}

	auto, control, err := store.ReadAutoUpdateControl(ctx)
	if err != nil {
		t.Fatalf("ReadAutoUpdateControl should not load corrupt hot inbound row: %v", err)
	}
	if !auto.NextCheckAt.Equal(now.Add(30 * time.Minute)) {
		t.Fatalf("NextCheckAt = %s, want %s", auto.NextCheckAt, now.Add(30*time.Minute))
	}
	if !control.Paused || control.Reason != "maintenance" {
		t.Fatalf("control = %#v, want paused maintenance", control)
	}
	if _, err := store.ReadAutoUpdate(ctx); err != nil {
		t.Fatalf("ReadAutoUpdate should not load corrupt hot inbound row: %v", err)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt hot inbound row")
	}
}

func TestSQLiteAutoUpdateRecordsUseRuntimeRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 6, 17, 9, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.AutoUpdate = AutoUpdateState{
			NextCheckAt: now.Add(-time.Minute),
			LastCheckAt: now.Add(-time.Hour),
		}
		state.InboundEvents["auto-update-hot"] = InboundEvent{
			ID:        "auto-update-hot",
			SessionID: "s001",
			Text:      strings.Repeat("hot payload ", 512),
			Status:    InboundStatusPersisted,
			CreatedAt: now,
			UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed auto-update runtime state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeStateJSON := sqliteRawStateJSONForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyAutoUpdate); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE inbound_events SET json = ? WHERE id = ?`, []byte(`{"broken"`), "auto-update-hot")
		if err != nil {
			return err
		}
		if rows, err := res.RowsAffected(); err != nil {
			return err
		} else if rows != 1 {
			return fmt.Errorf("corrupt hot rows = %d, want 1", rows)
		}
		return nil
	})

	checked, err := store.RecordAutoUpdateCheck(ctx, AutoUpdateRecord{
		Now:              now,
		NextCheckAt:      now.Add(30 * time.Minute),
		CandidateTag:     " v9.9.9 ",
		CandidateVersion: " 9.9.9 ",
		CandidateAsset:   " asset ",
	})
	if err != nil {
		t.Fatalf("RecordAutoUpdateCheck should not load corrupt hot rows: %v", err)
	}
	if checked.CandidateTag != "v9.9.9" || !checked.NextCheckAt.Equal(now.Add(30*time.Minute)) {
		t.Fatalf("checked auto-update state mismatch: %#v", checked)
	}
	if _, err := store.RecordAutoUpdateAttempt(ctx, " v9.9.9 ", now.Add(time.Minute)); err != nil {
		t.Fatalf("RecordAutoUpdateAttempt should not load corrupt hot rows: %v", err)
	}
	installed, err := store.RecordAutoUpdateInstalled(ctx, " v9.9.9 ", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("RecordAutoUpdateInstalled should not load corrupt hot rows: %v", err)
	}
	if installed.LastInstalledTag != "v9.9.9" || installed.CandidateTag != "" {
		t.Fatalf("installed auto-update state mismatch: %#v", installed)
	}
	afterStateJSON := sqliteRawStateJSONForTest(t, store)
	if !bytes.Equal(afterStateJSON, beforeStateJSON) {
		t.Fatal("auto-update runtime row update rewrote state_meta state_json")
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyAutoUpdate).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("auto_update runtime rows = %d, want 1", count)
		}
		return nil
	})
	read, err := store.ReadAutoUpdate(ctx)
	if err != nil {
		t.Fatalf("ReadAutoUpdate should not load corrupt hot rows: %v", err)
	}
	if read.LastInstalledTag != "v9.9.9" || read.LastAttemptTag != "v9.9.9" {
		t.Fatalf("read auto-update state mismatch: %#v", read)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt hot inbound row")
	}
}

func TestSQLiteServiceControlAndUpgradeUseRuntimeRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ServiceControl = ServiceControl{Paused: true, Reason: "seeded maintenance", UpdatedAt: now.Add(-time.Hour)}
		state.InboundEvents["upgrade-hot"] = InboundEvent{
			ID:        "upgrade-hot",
			SessionID: "s001",
			Text:      strings.Repeat("hot payload ", 512),
			Status:    InboundStatusPersisted,
			CreatedAt: now,
			UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed service control runtime state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeStateJSON := sqliteRawStateJSONForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_state WHERE key IN (?, ?)`, sqliteRuntimeKeyServiceControl, sqliteRuntimeKeyUpgrade); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE inbound_events SET json = ? WHERE id = ?`, []byte(`{"broken"`), "upgrade-hot")
		if err != nil {
			return err
		}
		if rows, err := res.RowsAffected(); err != nil {
			return err
		} else if rows != 1 {
			return fmt.Errorf("corrupt hot rows = %d, want 1", rows)
		}
		return nil
	})

	control, err := store.SetDraining(ctx, "manual drain")
	if err != nil {
		t.Fatalf("SetDraining should not load corrupt hot rows: %v", err)
	}
	if !control.Paused || !control.Draining || control.Reason != "manual drain" {
		t.Fatalf("draining control mismatch: %#v", control)
	}
	req, err := store.BeginUpgrade(ctx, HelperUpgradeReason, 10*time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade should not load corrupt hot rows: %v", err)
	}
	if req.ID == "" || req.Phase != UpgradePhaseDraining || req.PreviousControl.Reason != "manual drain" {
		t.Fatalf("upgrade request mismatch after begin: %#v", req)
	}
	if _, err := store.MarkUpgradeReady(ctx, req.ID); err != nil {
		t.Fatalf("MarkUpgradeReady should not load corrupt hot rows: %v", err)
	}
	completed, err := store.CompleteUpgrade(ctx, req.ID, "v9.9.9")
	if err != nil {
		t.Fatalf("CompleteUpgrade should not load corrupt hot rows: %v", err)
	}
	if completed.Phase != UpgradePhaseCompleted || completed.InstalledTag != "v9.9.9" {
		t.Fatalf("completed upgrade mismatch: %#v", completed)
	}
	afterStateJSON := sqliteRawStateJSONForTest(t, store)
	if !bytes.Equal(afterStateJSON, beforeStateJSON) {
		t.Fatal("service-control/upgrade runtime row update rewrote state_meta state_json")
	}
	control, err = store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("ReadControl should not load corrupt hot rows: %v", err)
	}
	if !control.Paused || !control.Draining || control.Reason != "manual drain" {
		t.Fatalf("restored control mismatch after complete: %#v", control)
	}
	readUpgrade, ok, err := store.ReadUpgrade(ctx)
	if err != nil {
		t.Fatalf("ReadUpgrade should not load corrupt hot rows: %v", err)
	}
	if !ok || readUpgrade.Phase != UpgradePhaseCompleted || readUpgrade.InstalledTag != "v9.9.9" {
		t.Fatalf("read upgrade mismatch: %#v ok=%v", readUpgrade, ok)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt hot inbound row")
	}
}

func TestUpgradeBlockingStateSnapshotSQLiteDoesNotLoadUnneededHotTables(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now()
	if err := store.Update(ctx, func(state *State) error {
		state.ServiceControl = ServiceControl{
			Draining:  true,
			Reason:    HelperReloadReason,
			UpdatedAt: now,
		}
		state.Turns["turn-running"] = Turn{
			ID:        "turn-running",
			SessionID: "s001",
			Status:    TurnStatusRunning,
			StartedAt: now,
			UpdatedAt: now,
		}
		for i := 0; i < 128; i++ {
			id := fmt.Sprintf("upgrade-hot-inbound-%03d", i)
			state.InboundEvents[id] = InboundEvent{
				ID:        id,
				SessionID: "s001",
				Text:      strings.Repeat("hot payload ", 256),
				Status:    InboundStatusPersisted,
				CreatedAt: now,
				UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed upgrade blocking state: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	res, err := db.ExecContext(ctx, `UPDATE inbound_events SET json = ? WHERE id = ?`, []byte(`{"broken"`), "upgrade-hot-inbound-000")
	if err != nil {
		t.Fatalf("corrupt hot inbound row: %v", err)
	}
	if rows, err := res.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("corrupt hot inbound rows affected = %d err=%v, want 1", rows, err)
	}
	state, err := store.UpgradeBlockingStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("UpgradeBlockingStateSnapshot should not load corrupt inbound row: %v", err)
	}
	if !HasUpgradeBlockingWork(state, now) {
		t.Fatal("UpgradeBlockingStateSnapshot lost running turn blocker")
	}
	if !state.ServiceControl.Draining || state.ServiceControl.Reason != HelperReloadReason {
		t.Fatalf("UpgradeBlockingStateSnapshot lost service control: %#v", state.ServiceControl)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt hot inbound row")
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
	defer reopened.Close()
	deferred, err = reopened.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound reopened error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].Status != InboundStatusDeferred {
		t.Fatalf("reopened deferred inbound = %#v", deferred)
	}
}

func TestSQLiteDeferredInboundFiltersAndSortsLargeTable(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 24, 11, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s1"] = SessionContext{ID: "s1", TeamsChatID: "chat-a", Status: SessionStatusActive, CreatedAt: now, UpdatedAt: now}
		state.Sessions["s2"] = SessionContext{ID: "s2", TeamsChatID: "chat-b", Status: SessionStatusActive, CreatedAt: now, UpdatedAt: now}
		for i := 0; i < 1200; i++ {
			status := InboundStatusPersisted
			if i%5 == 0 {
				status = InboundStatusIgnored
			}
			sessionID := "s1"
			chatID := "chat-a"
			if i%2 == 0 {
				sessionID = "s2"
				chatID = "chat-b"
			}
			id := fmt.Sprintf("non-deferred-%04d", i)
			state.InboundEvents[id] = InboundEvent{
				ID:             id,
				SessionID:      sessionID,
				TeamsChatID:    chatID,
				TeamsMessageID: fmt.Sprintf("message-%04d", i),
				Text:           "not deferred",
				Status:         status,
				CreatedAt:      now.Add(time.Duration(i) * time.Millisecond),
				UpdatedAt:      now.Add(time.Duration(i) * time.Millisecond),
			}
		}
		state.InboundEvents["deferred-b2"] = InboundEvent{ID: "deferred-b2", SessionID: "s2", TeamsChatID: "chat-b", TeamsMessageID: "b2", Status: InboundStatusDeferred, CreatedAt: now.Add(2 * time.Second), UpdatedAt: now}
		state.InboundEvents["deferred-a2"] = InboundEvent{ID: "deferred-a2", SessionID: "s1", TeamsChatID: "chat-a", TeamsMessageID: "a2", Status: InboundStatusDeferred, CreatedAt: now.Add(2 * time.Second), UpdatedAt: now}
		state.InboundEvents["deferred-a1"] = InboundEvent{ID: "deferred-a1", SessionID: "s1", TeamsChatID: "chat-a", TeamsMessageID: "a1", Status: InboundStatusDeferred, CreatedAt: now.Add(time.Second), UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed inbound events: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	deferred, err := store.DeferredInbound(ctx)
	if err != nil {
		t.Fatalf("DeferredInbound sqlite error: %v", err)
	}
	got := make([]string, 0, len(deferred))
	for _, event := range deferred {
		if event.Status != InboundStatusDeferred {
			t.Fatalf("DeferredInbound returned non-deferred event: %#v", event)
		}
		got = append(got, event.ID)
	}
	want := []string{"deferred-a1", "deferred-a2", "deferred-b2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deferred sqlite order = %#v, want %#v", got, want)
	}
}

func TestHasQueuedTurnsAcrossBackends(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		sqlite := sqlite
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["s1"] = SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1", CreatedAt: now, UpdatedAt: now}
				for i := 0; i < 1200; i++ {
					status := TurnStatusCompleted
					if i%257 == 0 {
						status = TurnStatusRunning
					}
					id := fmt.Sprintf("turn:%04d", i)
					state.Turns[id] = Turn{ID: id, SessionID: "s1", Status: status, CreatedAt: now.Add(time.Duration(i) * time.Millisecond), UpdatedAt: now}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed turns: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			hasQueued, err := store.HasQueuedTurns(ctx)
			if err != nil {
				t.Fatalf("HasQueuedTurns without queued turn error: %v", err)
			}
			if hasQueued {
				t.Fatal("HasQueuedTurns returned true without queued turns")
			}
			running, err := store.RunningTurnSessionIDs(ctx)
			if err != nil {
				t.Fatalf("RunningTurnSessionIDs error: %v", err)
			}
			if !running["s1"] || len(running) != 1 {
				t.Fatalf("RunningTurnSessionIDs = %#v, want only s1", running)
			}

			if _, _, err := store.QueueTurn(ctx, Turn{ID: "turn:queued", SessionID: "s1", Status: TurnStatusQueued, QueuedAt: now.Add(time.Hour)}); err != nil {
				t.Fatalf("QueueTurn queued error: %v", err)
			}
			hasQueued, err = store.HasQueuedTurns(ctx)
			if err != nil {
				t.Fatalf("HasQueuedTurns with queued turn error: %v", err)
			}
			if !hasQueued {
				t.Fatal("HasQueuedTurns returned false with a queued turn")
			}
		})
	}
}

func TestHasPendingWorkflowNotificationsAcrossBackends(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		sqlite := sqlite
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			now := time.Date(2026, 5, 24, 13, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				for i := 0; i < 1200; i++ {
					status := NotificationStatusSent
					if i%113 == 0 {
						status = NotificationStatusUnknown
					}
					id := fmt.Sprintf("notification:%04d", i)
					state.Notifications[id] = NotificationRecord{ID: id, Status: status, CreatedAt: now.Add(time.Duration(i) * time.Millisecond), UpdatedAt: now}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed sent notifications: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			hasPending, err := store.HasPendingWorkflowNotifications(ctx)
			if err != nil {
				t.Fatalf("HasPendingWorkflowNotifications without pending error: %v", err)
			}
			if hasPending {
				t.Fatal("HasPendingWorkflowNotifications returned true for sent/unknown notifications")
			}

			if err := store.Update(ctx, func(state *State) error {
				state.Notifications["notification:queued"] = NotificationRecord{ID: "notification:queued", Status: NotificationStatusQueued, CreatedAt: now.Add(time.Hour), UpdatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed queued notification: %v", err)
			}
			hasPending, err = store.HasPendingWorkflowNotifications(ctx)
			if err != nil {
				t.Fatalf("HasPendingWorkflowNotifications with pending error: %v", err)
			}
			if !hasPending {
				t.Fatal("HasPendingWorkflowNotifications returned false with a queued notification")
			}
		})
	}
}

func TestPendingWorkflowNotificationsAcrossBackendsFiltersAndOrders(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		sqlite := sqlite
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			now := time.Date(2026, 5, 25, 9, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Notifications["sent"] = NotificationRecord{ID: "sent", Status: NotificationStatusSent, CreatedAt: now}
				state.Notifications["unknown"] = NotificationRecord{ID: "unknown", Status: NotificationStatusUnknown, CreatedAt: now}
				state.Notifications["custom"] = NotificationRecord{ID: "custom", Status: "custom", CreatedAt: now}
				state.Notifications["queued"] = NotificationRecord{ID: "queued", Status: NotificationStatusQueued, CreatedAt: now.Add(1 * time.Second)}
				state.Notifications["empty"] = NotificationRecord{ID: "empty", CreatedAt: now.Add(2 * time.Second)}
				state.Notifications["failed"] = NotificationRecord{ID: "failed", Status: NotificationStatusFailed, CreatedAt: now.Add(3 * time.Second)}
				state.Notifications["sending"] = NotificationRecord{ID: "sending", Status: NotificationStatusSending, CreatedAt: now.Add(4 * time.Second)}
				for i := 0; i < 500; i++ {
					id := fmt.Sprintf("sent-bulk-%03d", i)
					state.Notifications[id] = NotificationRecord{ID: id, Status: NotificationStatusSent, CreatedAt: now.Add(time.Duration(i) * time.Millisecond)}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed notifications: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			pending, err := store.PendingWorkflowNotifications(ctx)
			if err != nil {
				t.Fatalf("PendingWorkflowNotifications error: %v", err)
			}
			var got []string
			for _, rec := range pending {
				got = append(got, rec.ID)
			}
			want := []string{"queued", "empty", "failed", "sending"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("pending ids = %#v, want %#v", got, want)
			}
		})
	}
}

func TestUpdateNotificationAcrossBackendsTouchesOnlyNotification(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		sqlite := sqlite
		t.Run(fmt.Sprintf("sqlite=%v", sqlite), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			now := time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.InboundEvents["inbound-large"] = InboundEvent{
					ID:        "inbound-large",
					Text:      strings.Repeat("payload ", 4096),
					Status:    InboundStatusPersisted,
					CreatedAt: now,
					UpdatedAt: now,
				}
				state.Notifications["existing"] = NotificationRecord{
					ID:        "existing",
					Status:    NotificationStatusQueued,
					Title:     "old",
					ButtonURL: "https://example.test/old",
					CreatedAt: now,
					UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			updated, changed, err := store.UpdateNotification(ctx, "existing", func(rec NotificationRecord, found bool, updateNow time.Time) (NotificationRecord, bool, error) {
				if !found {
					t.Fatal("existing notification was not found")
				}
				rec.Status = NotificationStatusSent
				rec.SentAt = updateNow
				rec.UpdatedAt = updateNow
				return rec, true, nil
			})
			if err != nil {
				t.Fatalf("UpdateNotification existing error: %v", err)
			}
			if !changed || updated.Status != NotificationStatusSent || updated.SentAt.IsZero() {
				t.Fatalf("updated = %#v changed=%v, want sent", updated, changed)
			}
			created, changed, err := store.UpdateNotification(ctx, "created", func(rec NotificationRecord, found bool, updateNow time.Time) (NotificationRecord, bool, error) {
				if found {
					t.Fatal("created notification unexpectedly existed")
				}
				return NotificationRecord{ID: "created", Status: NotificationStatusQueued, CreatedAt: updateNow, UpdatedAt: updateNow}, true, nil
			})
			if err != nil {
				t.Fatalf("UpdateNotification created error: %v", err)
			}
			if !changed || created.ID != "created" || created.Status != NotificationStatusQueued {
				t.Fatalf("created = %#v changed=%v, want queued", created, changed)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after UpdateNotification: %v", err)
			}
			if state.InboundEvents["inbound-large"].Text == "" {
				t.Fatalf("UpdateNotification lost unrelated inbound event: %#v", state.InboundEvents)
			}
			if state.Notifications["existing"].Status != NotificationStatusSent || state.Notifications["created"].Status != NotificationStatusQueued {
				t.Fatalf("notifications after update = %#v", state.Notifications)
			}
		})
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

func TestSQLiteRecordTranscriptDeliveryDoesNotLoadColdStateAndPreservesDedupe(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	checkpointID := "checkpoint:s1"
	if _, created, err := store.RecordTranscriptDelivery(ctx, TranscriptDeliveryRecord{
		ID:             "delivery:s1:r1",
		SessionID:      "s1",
		OutboxID:       "outbox-1",
		TeamsMessageID: "teams-message-original",
		Status:         TranscriptDeliveryStatusSent,
		CreatedAt:      time.Date(2026, 6, 16, 8, 0, 0, 0, time.UTC),
	}, ImportCheckpoint{
		ID:             checkpointID,
		SessionID:      "s1",
		SourcePath:     "/tmp/session.jsonl",
		LastRecordID:   "r1",
		LastSourceLine: 42,
		LastOffset:     2048,
		Status:         "complete",
	}); err != nil || !created {
		t.Fatalf("seed RecordTranscriptDelivery created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeDeliveryRaw := sqliteRawTranscriptDeliveryJSONForTest(t, store, "delivery:s1:r1")
	sqliteWriteRawStateJSONForTest(t, store, []byte(`{"broken"`))

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	delivery, created, err := store.RecordTranscriptDelivery(ctx, TranscriptDeliveryRecord{
		ID:             "delivery:s1:r1",
		SessionID:      "s1",
		OutboxID:       "outbox-overwrite",
		TeamsMessageID: "teams-message-overwrite",
		Status:         TranscriptDeliveryStatusSkipped,
	}, ImportCheckpoint{
		ID:             checkpointID,
		SessionID:      "s1",
		SourcePath:     "/tmp/session.jsonl",
		LastRecordID:   "r2",
		LastSourceLine: 84,
		LastOffset:     4096,
		Status:         "complete",
	})
	if err != nil {
		t.Fatalf("RecordTranscriptDelivery sqlite duplicate error: %v", err)
	}
	if created {
		t.Fatal("duplicate transcript delivery was reported as created")
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("RecordTranscriptDelivery loaded full state %d times", got)
	}
	if delivery.OutboxID != "outbox-1" || delivery.TeamsMessageID != "teams-message-original" || delivery.Status != TranscriptDeliveryStatusSent {
		t.Fatalf("duplicate returned overwritten delivery: %#v", delivery)
	}
	if afterDeliveryRaw := sqliteRawTranscriptDeliveryJSONForTest(t, store, "delivery:s1:r1"); !bytes.Equal(beforeDeliveryRaw, afterDeliveryRaw) {
		t.Fatalf("duplicate transcript delivery rewrote delivery row:\nbefore=%s\nafter=%s", beforeDeliveryRaw, afterDeliveryRaw)
	}
	var checkpoint ImportCheckpoint
	if raw := sqliteRawImportCheckpointJSONForTest(t, store, checkpointID); json.Unmarshal(raw, &checkpoint) != nil {
		t.Fatalf("unmarshal checkpoint from raw %s", raw)
	}
	if checkpoint.LastRecordID != "r2" || checkpoint.LastSourceLine != 84 || checkpoint.LastOffset != 4096 {
		t.Fatalf("checkpoint did not advance on duplicate delivery: %#v", checkpoint)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
	atomic.StoreInt64(&loads, 0)

	newDelivery, created, err := store.RecordTranscriptDelivery(ctx, TranscriptDeliveryRecord{
		ID:        "delivery:s1:r3",
		SessionID: "s1",
		Status:    TranscriptDeliveryStatusSkipped,
	}, ImportCheckpoint{
		ID:             checkpointID,
		SessionID:      "s1",
		SourcePath:     "/tmp/session.jsonl",
		LastRecordID:   "r3",
		LastSourceLine: 126,
		LastOffset:     8192,
		Status:         "complete",
	})
	if err != nil || !created {
		t.Fatalf("RecordTranscriptDelivery sqlite new delivery created=%v err=%v", created, err)
	}
	if newDelivery.ID != "delivery:s1:r3" || newDelivery.Status != TranscriptDeliveryStatusSkipped || newDelivery.CreatedAt.IsZero() {
		t.Fatalf("new delivery mismatch: %#v", newDelivery)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("RecordTranscriptDelivery loaded full state after new delivery %d times", got)
	}
	if raw := sqliteRawTranscriptDeliveryJSONForTest(t, store, "delivery:s1:r3"); len(raw) == 0 {
		t.Fatal("new transcript delivery row missing")
	}
	if raw := sqliteRawImportCheckpointJSONForTest(t, store, checkpointID); json.Unmarshal(raw, &checkpoint) != nil {
		t.Fatalf("unmarshal final checkpoint from raw %s", raw)
	}
	if checkpoint.LastRecordID != "r3" || checkpoint.LastSourceLine != 126 || checkpoint.LastOffset != 8192 {
		t.Fatalf("checkpoint did not advance on new delivery: %#v", checkpoint)
	}
}

func TestRecordTranscriptCheckpointPreservesLegacySemantics(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	previousMod := time.Date(2026, 6, 16, 7, 30, 0, 0, time.UTC)
	if err := store.UpdateSession(ctx, "s1", func(state *State) error {
		state.ImportCheckpoints["checkpoint:s1"] = ImportCheckpoint{
			ID:             "checkpoint:s1",
			SessionID:      "s1",
			SourcePath:     "/tmp/old.jsonl",
			LastRecordID:   "old-record",
			LastSourceLine: 12,
			LastOffset:     4096,
			SourceSize:     8192,
			SourceModTime:  previousMod,
			ImportTurnID:   "import-turn-1",
			KindPrefix:     "assistant",
			Status:         importCheckpointStatusBlocked,
			UpdatedAt:      previousMod,
		}
		state.ImportCheckpoints["checkpoint:failed"] = ImportCheckpoint{
			ID:             "checkpoint:failed",
			SessionID:      "s1",
			LastRecordID:   "failed-record",
			LastSourceLine: 3,
			Status:         "failed",
			UpdatedAt:      previousMod,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoints: %v", err)
	}

	newMod := time.Date(2026, 6, 16, 8, 0, 0, 0, time.UTC)
	if err := store.RecordTranscriptCheckpoint(ctx, ImportCheckpoint{
		ID:             "checkpoint:s1",
		SessionID:      "s1",
		SourcePath:     "/tmp/new.jsonl",
		LastRecordID:   "new-record",
		LastSourceLine: 24,
		LastOffset:     0,
		SourceSize:     16384,
		SourceModTime:  newMod,
	}, TranscriptLedgerRecord{
		ID:             "ledger:s1:new-record",
		SessionID:      "s1",
		CodexThreadID:  "thread-1",
		SourcePath:     "/tmp/new.jsonl",
		SourceLine:     24,
		SourceRecordID: "new-record",
	}); err != nil {
		t.Fatalf("RecordTranscriptCheckpoint error: %v", err)
	}
	if err := store.RecordTranscriptCheckpoint(ctx, ImportCheckpoint{
		ID:             "checkpoint:failed",
		SessionID:      "s1",
		SourcePath:     "/tmp/failed.jsonl",
		LastRecordID:   "new-failed-record",
		LastSourceLine: 4,
		LastOffset:     128,
		SourceSize:     256,
		SourceModTime:  newMod,
	}, TranscriptLedgerRecord{
		ID:             "ledger:s1:new-failed-record",
		SessionID:      "s1",
		SourcePath:     "/tmp/failed.jsonl",
		SourceLine:     4,
		SourceRecordID: "new-failed-record",
	}); err != nil {
		t.Fatalf("RecordTranscriptCheckpoint failed status error: %v", err)
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	checkpoint := state.ImportCheckpoints["checkpoint:s1"]
	if checkpoint.LastRecordID != "new-record" || checkpoint.LastSourceLine != 24 || checkpoint.LastOffset != 4096 {
		t.Fatalf("checkpoint position = %#v, want new record/line and previous nonzero offset", checkpoint)
	}
	if checkpoint.SourcePath != "/tmp/new.jsonl" || checkpoint.SourceSize != 16384 || !checkpoint.SourceModTime.Equal(newMod) {
		t.Fatalf("checkpoint source metadata = %#v, want new source metadata", checkpoint)
	}
	if checkpoint.ImportTurnID != "import-turn-1" || checkpoint.KindPrefix != "assistant" || checkpoint.Status != importCheckpointStatusComplete {
		t.Fatalf("checkpoint preserved fields/status = %#v", checkpoint)
	}
	if ledger := state.TranscriptLedger["ledger:s1:new-record"]; ledger.ID == "" || ledger.SourceRecordID != "new-record" || ledger.ImportedAt.IsZero() || ledger.CreatedAt.IsZero() || ledger.UpdatedAt.IsZero() {
		t.Fatalf("ledger not recorded with timestamps: %#v", ledger)
	}
	if failed := state.ImportCheckpoints["checkpoint:failed"]; failed.Status != "failed" || failed.LastRecordID != "new-failed-record" {
		t.Fatalf("failed checkpoint status should be preserved while advancing position: %#v", failed)
	}
}

func TestSQLiteRecordTranscriptCheckpointDoesNotLoadColdStateAndTouchesOnlyCheckpointAndLedger(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	previousMod := time.Date(2026, 6, 16, 7, 30, 0, 0, time.UTC)
	if err := store.UpdateSession(ctx, "s1", func(state *State) error {
		state.ImportCheckpoints["checkpoint:s1"] = ImportCheckpoint{
			ID:             "checkpoint:s1",
			SessionID:      "s1",
			SourcePath:     "/tmp/old.jsonl",
			LastRecordID:   "old-record",
			LastSourceLine: 12,
			LastOffset:     4096,
			SourceSize:     8192,
			SourceModTime:  previousMod,
			ImportTurnID:   "import-turn-1",
			KindPrefix:     "assistant",
			Status:         importCheckpointStatusBlocked,
			UpdatedAt:      previousMod,
		}
		state.TranscriptLedger["ledger:unrelated"] = TranscriptLedgerRecord{
			ID:             "ledger:unrelated",
			SessionID:      "s-other",
			SourcePath:     "/tmp/other.jsonl",
			SourceLine:     1,
			SourceRecordID: "other-record",
			ImportedAt:     previousMod,
			CreatedAt:      previousMod,
			UpdatedAt:      previousMod,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeUnrelated := sqliteRawTranscriptLedgerJSONForTest(t, store, "ledger:unrelated")
	corruptState := []byte(`{"broken"`)
	sqliteWriteRawStateJSONForTest(t, store, corruptState)

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	newMod := time.Date(2026, 6, 16, 8, 0, 0, 0, time.UTC)
	err := store.RecordTranscriptCheckpoint(ctx, ImportCheckpoint{
		ID:             "checkpoint:s1",
		SessionID:      "s1",
		SourcePath:     "/tmp/new.jsonl",
		LastRecordID:   "new-record",
		LastSourceLine: 24,
		LastOffset:     0,
		SourceSize:     16384,
		SourceModTime:  newMod,
	}, TranscriptLedgerRecord{
		ID:             "ledger:s1:new-record",
		SessionID:      "s1",
		CodexThreadID:  "thread-1",
		SourcePath:     "/tmp/new.jsonl",
		SourceLine:     24,
		SourceRecordID: "new-record",
	})
	if err != nil {
		t.Fatalf("RecordTranscriptCheckpoint sqlite error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("RecordTranscriptCheckpoint loaded full state %d times", got)
	}
	if afterState := sqliteRawStateJSONForTest(t, store); !bytes.Equal(corruptState, afterState) {
		t.Fatalf("RecordTranscriptCheckpoint rewrote cold state_json: %s", afterState)
	}
	if afterUnrelated := sqliteRawTranscriptLedgerJSONForTest(t, store, "ledger:unrelated"); !bytes.Equal(beforeUnrelated, afterUnrelated) {
		t.Fatalf("RecordTranscriptCheckpoint rewrote unrelated ledger row:\nbefore=%s\nafter=%s", beforeUnrelated, afterUnrelated)
	}
	var checkpoint ImportCheckpoint
	if raw := sqliteRawImportCheckpointJSONForTest(t, store, "checkpoint:s1"); json.Unmarshal(raw, &checkpoint) != nil {
		t.Fatalf("unmarshal checkpoint raw %s", raw)
	}
	if checkpoint.LastRecordID != "new-record" || checkpoint.LastSourceLine != 24 || checkpoint.LastOffset != 4096 {
		t.Fatalf("checkpoint position = %#v, want new record/line and previous nonzero offset", checkpoint)
	}
	if checkpoint.ImportTurnID != "import-turn-1" || checkpoint.KindPrefix != "assistant" || checkpoint.Status != importCheckpointStatusComplete {
		t.Fatalf("checkpoint preserved fields/status = %#v", checkpoint)
	}
	if checkpoint.SourcePath != "/tmp/new.jsonl" || checkpoint.SourceSize != 16384 || !checkpoint.SourceModTime.Equal(newMod) {
		t.Fatalf("checkpoint source metadata = %#v, want new source metadata", checkpoint)
	}
	var ledger TranscriptLedgerRecord
	if raw := sqliteRawTranscriptLedgerJSONForTest(t, store, "ledger:s1:new-record"); json.Unmarshal(raw, &ledger) != nil {
		t.Fatalf("unmarshal ledger raw %s", raw)
	}
	if ledger.SourceRecordID != "new-record" || ledger.SourceLine != 24 || ledger.CodexThreadID != "thread-1" || ledger.ImportedAt.IsZero() {
		t.Fatalf("ledger = %#v, want inserted record with timestamps", ledger)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
}

func TestSQLiteSessionAndInboundRowUpdatesDoNotTouchColdState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 18, 9, 0, 0, 0, time.UTC)
	if _, _, err := store.CreateSession(ctx, SessionContext{
		ID:          "s1",
		Status:      SessionStatusActive,
		TeamsChatID: "chat-1",
		TeamsTopic:  "before",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("CreateSession s1: %v", err)
	}
	if _, _, err := store.CreateSession(ctx, SessionContext{
		ID:          "s-unrelated",
		Status:      SessionStatusActive,
		TeamsChatID: "chat-unrelated",
		TeamsTopic:  "unrelated",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("CreateSession unrelated: %v", err)
	}
	if _, _, err := store.PersistInbound(ctx, InboundEvent{
		ID:             "inbound-1",
		SessionID:      "s1",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Status:         InboundStatusDeferred,
		Source:         "teams",
		Text:           "deferred",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PersistInbound: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeUnrelated := sqliteRawSessionJSONForTest(t, store, "s-unrelated")
	corruptState := []byte(`{"broken"`)
	sqliteWriteRawStateJSONForTest(t, store, corruptState)

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	session, changed, err := store.UpdateSessionContext(ctx, "s1", func(current SessionContext, found bool, updateNow time.Time) (SessionContext, bool, error) {
		if !found {
			return current, false, fmt.Errorf("session missing")
		}
		current.TeamsTopic = "after"
		current.UpdatedAt = updateNow
		return current, true, nil
	})
	if err != nil || !changed || session.TeamsTopic != "after" {
		t.Fatalf("UpdateSessionContext = %#v changed=%v err=%v, want updated", session, changed, err)
	}
	inbound, changed, err := store.UpdateInboundEvent(ctx, "inbound-1", func(current InboundEvent, found bool, updateNow time.Time) (InboundEvent, bool, error) {
		if !found {
			return current, false, fmt.Errorf("inbound missing")
		}
		if current.Status != InboundStatusDeferred {
			return current, false, nil
		}
		current.Status = InboundStatusIgnored
		current.Source = "teams row-update"
		current.UpdatedAt = updateNow
		return current, true, nil
	})
	if err != nil || !changed || inbound.Status != InboundStatusIgnored {
		t.Fatalf("UpdateInboundEvent = %#v changed=%v err=%v, want ignored", inbound, changed, err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("row-level updates loaded full state %d times", got)
	}
	if afterState := sqliteRawStateJSONForTest(t, store); !bytes.Equal(corruptState, afterState) {
		t.Fatalf("row-level updates rewrote cold state_json: %s", afterState)
	}
	if afterUnrelated := sqliteRawSessionJSONForTest(t, store, "s-unrelated"); !bytes.Equal(beforeUnrelated, afterUnrelated) {
		t.Fatalf("row-level updates rewrote unrelated session row:\nbefore=%s\nafter=%s", beforeUnrelated, afterUnrelated)
	}
	var rawSession SessionContext
	if raw := sqliteRawSessionJSONForTest(t, store, "s1"); json.Unmarshal(raw, &rawSession) != nil {
		t.Fatalf("unmarshal session raw %s", raw)
	}
	if rawSession.TeamsTopic != "after" {
		t.Fatalf("session topic = %q, want after", rawSession.TeamsTopic)
	}
	var rawInbound InboundEvent
	if raw := sqliteRawInboundJSONForTest(t, store, "inbound-1"); json.Unmarshal(raw, &rawInbound) != nil {
		t.Fatalf("unmarshal inbound raw %s", raw)
	}
	if rawInbound.Status != InboundStatusIgnored || rawInbound.Source != "teams row-update" {
		t.Fatalf("inbound row = %#v, want ignored row-update", rawInbound)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
}

func TestSQLiteColdMetadataUpdatesDoNotRewriteHotRows(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 18, 9, 30, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: "control-chat", TeamsChatTopic: "control", UpdatedAt: now}
		state.Sessions["s1"] = SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1", TeamsTopic: "session", CreatedAt: now, UpdatedAt: now}
		state.InboundEvents["inbound-unrelated"] = InboundEvent{ID: "inbound-unrelated", SessionID: "s1", TeamsChatID: "chat-1", TeamsMessageID: "m1", Status: InboundStatusPersisted, CreatedAt: now, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeSession := sqliteRawSessionJSONForTest(t, store, "s1")
	beforeInbound := sqliteRawInboundJSONForTest(t, store, "inbound-unrelated")

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	if err := store.UpdateDashboardRecords(ctx, func(records *DashboardStoreRecords, updateNow time.Time) (bool, error) {
		records.Views["control-chat"] = DashboardViewRecord{ID: "dashboard:control-chat", ChatID: "control-chat", Kind: "sessions", CreatedAt: updateNow, UpdatedAt: updateNow}
		records.Numbers["n1"] = DashboardNumberRecord{ID: "n1", ChatID: "control-chat", Kind: "session", Number: 1, SessionID: "s1", UpdatedAt: updateNow}
		records.Workspaces["w1"] = WorkspaceRecord{ID: "w1", Path: "/workspace", Number: 1, UpdatedAt: updateNow}
		return true, nil
	}); err != nil {
		t.Fatalf("UpdateDashboardRecords: %v", err)
	}
	if _, err := store.UpdateWorkflowConfig(ctx, func(current WorkflowNotificationConfig, control ControlChatBinding, updateNow time.Time) (WorkflowNotificationConfig, bool, error) {
		current.Enabled = true
		current.ControlChatID = control.TeamsChatID
		current.ControlWebhookURLFile = "/tmp/workflow-webhook-url"
		current.UpdatedAt = updateNow
		return current, true, nil
	}); err != nil {
		t.Fatalf("UpdateWorkflowConfig: %v", err)
	}
	if err := store.UpsertModelProfileKeyIntake(ctx, ModelProfileKeyIntake{
		ID:           "intake-1",
		TeamsChatID:  "control-chat",
		AuthorUserID: "user-1",
		ProfileName:  "openai",
		Status:       ModelProfileKeyIntakePending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("UpsertModelProfileKeyIntake: %v", err)
	}
	if err := store.UpdateModelProfileKeyIntakes(ctx, func(intakes map[string]ModelProfileKeyIntake, updateNow time.Time) (bool, error) {
		intake := intakes["intake-1"]
		intake.Status = ModelProfileKeyIntakeConfirmed
		intake.UpdatedAt = updateNow
		intakes[intake.ID] = intake
		return true, nil
	}); err != nil {
		t.Fatalf("UpdateModelProfileKeyIntakes: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("cold metadata updates loaded full state %d times", got)
	}
	if afterSession := sqliteRawSessionJSONForTest(t, store, "s1"); !bytes.Equal(beforeSession, afterSession) {
		t.Fatalf("cold metadata updates rewrote session row:\nbefore=%s\nafter=%s", beforeSession, afterSession)
	}
	if afterInbound := sqliteRawInboundJSONForTest(t, store, "inbound-unrelated"); !bytes.Equal(beforeInbound, afterInbound) {
		t.Fatalf("cold metadata updates rewrote inbound row:\nbefore=%s\nafter=%s", beforeInbound, afterInbound)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after cold metadata updates: %v", err)
	}
	if _, ok := state.DashboardViews["control-chat"]; !ok {
		t.Fatalf("dashboard view missing after cold update: %#v", state.DashboardViews)
	}
	if !state.Workflow.Enabled || state.Workflow.ControlChatID != "control-chat" {
		t.Fatalf("workflow = %#v, want enabled control-chat", state.Workflow)
	}
	if got := state.ModelProfileKeyIntakes["intake-1"].Status; got != ModelProfileKeyIntakeConfirmed {
		t.Fatalf("model key intake status = %q, want confirmed", got)
	}
}

func TestSQLiteControlChatBindingDoesNotRewriteHotRows(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 18, 9, 45, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s1"] = SessionContext{ID: "s1", Status: SessionStatusActive, TeamsChatID: "chat-1", TeamsTopic: "session", CreatedAt: now, UpdatedAt: now}
		state.InboundEvents["inbound-1"] = InboundEvent{ID: "inbound-1", SessionID: "s1", TeamsChatID: "chat-1", TeamsMessageID: "m1", Status: InboundStatusPersisted, CreatedAt: now, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeSession := sqliteRawSessionJSONForTest(t, store, "s1")
	beforeInbound := sqliteRawInboundJSONForTest(t, store, "inbound-1")

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	changed, err := store.RecordControlChatBinding(ctx, ControlChatBindingUpdate{
		ScopeID:              "scope-1",
		AccountID:            "account-1",
		UserPrincipal:        "user@example.test",
		Profile:              "default",
		MachineID:            "machine-1",
		MachineLabel:         "machine label",
		MachineHostname:      "machine label",
		MachineKind:          MachineKindPrimary,
		MachinePriority:      7,
		TeamsChatID:          "control-chat",
		TeamsChatURL:         "https://teams.example/control",
		TeamsChatTopic:       "control topic",
		UserTitle:            "Control title",
		TitleSource:          "user",
		UpdateTitleIfPresent: true,
	})
	if err != nil || !changed {
		t.Fatalf("RecordControlChatBinding changed=%v err=%v, want changed", changed, err)
	}
	changed, err = store.RecordControlChatBinding(ctx, ControlChatBindingUpdate{
		ScopeID:              "scope-1",
		AccountID:            "account-1",
		UserPrincipal:        "user@example.test",
		Profile:              "default",
		MachineID:            "machine-1",
		MachineLabel:         "machine label",
		MachineHostname:      "machine label",
		MachineKind:          MachineKindPrimary,
		MachinePriority:      7,
		TeamsChatID:          "control-chat",
		TeamsChatURL:         "https://teams.example/control",
		TeamsChatTopic:       "control topic updated",
		UpdateTitleIfPresent: true,
	})
	if err != nil || !changed {
		t.Fatalf("RecordControlChatBinding topic-only changed=%v err=%v, want changed", changed, err)
	}
	changed, err = store.RecordControlChatBinding(ctx, ControlChatBindingUpdate{
		ScopeID:              "scope-1",
		AccountID:            "account-1",
		UserPrincipal:        "user@example.test",
		Profile:              "default",
		MachineID:            "machine-1",
		MachineLabel:         "machine label",
		MachineHostname:      "machine label",
		MachineKind:          MachineKindPrimary,
		MachinePriority:      7,
		TeamsChatID:          "control-chat",
		TeamsChatURL:         "https://teams.example/control",
		TeamsChatTopic:       "control topic updated",
		UpdateTitleIfPresent: true,
	})
	if err != nil || changed {
		t.Fatalf("RecordControlChatBinding no-op changed=%v err=%v, want no change", changed, err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("control binding update loaded full state %d times", got)
	}
	if afterSession := sqliteRawSessionJSONForTest(t, store, "s1"); !bytes.Equal(beforeSession, afterSession) {
		t.Fatalf("control binding rewrote session row:\nbefore=%s\nafter=%s", beforeSession, afterSession)
	}
	if afterInbound := sqliteRawInboundJSONForTest(t, store, "inbound-1"); !bytes.Equal(beforeInbound, afterInbound) {
		t.Fatalf("control binding rewrote inbound row:\nbefore=%s\nafter=%s", beforeInbound, afterInbound)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after control binding: %v", err)
	}
	if state.MachineIdentity.ID != "machine-1" || state.MachineIdentity.Label != "machine label" || state.MachineIdentity.Kind != MachineKindPrimary || state.MachineIdentity.Priority != 7 {
		t.Fatalf("machine identity = %#v, want recorded machine", state.MachineIdentity)
	}
	if state.ControlChat.TeamsChatID != "control-chat" || state.ControlChat.TeamsChatTopic != "control topic updated" || state.ControlChat.UserTitle != "Control title" || state.ControlChat.TitleSource != "user" {
		t.Fatalf("control chat = %#v, want recorded binding/title", state.ControlChat)
	}
}

func TestSQLiteQueueTranscriptDeliveryOutboxDoesNotLoadColdStateAndTouchesOnlyQueueRows(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if err := store.UpdateSession(ctx, "s1", func(state *State) error {
		state.ChatSequences["chat-1"] = ChatSequenceState{ChatID: "chat-1", Next: 7, UpdatedAt: time.Date(2026, 6, 16, 8, 0, 0, 0, time.UTC)}
		state.DashboardNumbers["cold"] = DashboardNumberRecord{ID: "cold", ChatID: "control-chat", Kind: "session", Number: 1, Label: strings.Repeat("cold ", 4096)}
		return nil
	}); err != nil {
		t.Fatalf("seed chat sequence: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	corruptState := []byte(`{"broken"`)
	sqliteWriteRawStateJSONForTest(t, store, corruptState)

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	out, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
		Message: OutboxMessage{
			ID:          "outbox:transcript:s1:r1",
			SessionID:   "s1",
			TurnID:      "turn-sync",
			TeamsChatID: "chat-1",
			Kind:        "status-progress",
			Body:        "status update text",
		},
		Delivery: TranscriptDeliveryRecord{
			ID:             "delivery:s1:r1",
			SessionID:      "s1",
			SourcePath:     "/tmp/session.jsonl",
			SourceLine:     100,
			SourceRecordID: "r1",
			Kind:           "status-progress",
			TextHash:       "hash-r1",
		},
	})
	if err != nil {
		t.Fatalf("QueueTranscriptDeliveryOutbox sqlite error: %v", err)
	}
	if !created || alreadyDelivered {
		t.Fatalf("QueueTranscriptDeliveryOutbox created=%v alreadyDelivered=%v, want true false", created, alreadyDelivered)
	}
	if out.ID != "outbox:transcript:s1:r1" || out.Sequence != 7 || out.CodexThreadID != "thread-0" {
		t.Fatalf("queued outbox = %#v, want existing sequence and session codex thread", out)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("QueueTranscriptDeliveryOutbox loaded full state %d times", got)
	}
	if afterState := sqliteRawStateJSONForTest(t, store); !bytes.Equal(corruptState, afterState) {
		t.Fatalf("QueueTranscriptDeliveryOutbox rewrote cold state_json: %s", afterState)
	}
	var delivery TranscriptDeliveryRecord
	if raw := sqliteRawTranscriptDeliveryJSONForTest(t, store, "delivery:s1:r1"); json.Unmarshal(raw, &delivery) != nil {
		t.Fatalf("unmarshal transcript delivery raw")
	}
	if delivery.OutboxID != out.ID || delivery.Status != TranscriptDeliveryStatusQueued || delivery.SourceRecordID != "r1" {
		t.Fatalf("delivery row = %#v, want queued row linked to outbox", delivery)
	}
	var helper HelperDeliveryRecord
	if raw := sqliteRawHelperDeliveryByOutboxForTest(t, store, out.ID); json.Unmarshal(raw, &helper) != nil {
		t.Fatalf("unmarshal helper delivery raw")
	}
	if helper.OutboxID != out.ID || helper.CodexThreadID != "thread-0" || helper.Status != HelperDeliveryStatusQueued {
		t.Fatalf("helper delivery row = %#v, want queued helper delivery", helper)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
	}
}

func TestSQLiteQueueTranscriptDeliveryOutboxLinksExistingOutboxWithoutAllocatingSequence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	existing, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:existing-transcript",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "status-progress",
		Body:        "already queued status",
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox seed created=%v err=%v", created, err)
	}
	if err := store.UpdateSession(ctx, "s1", func(state *State) error {
		state.TranscriptDeliveries["delivery:s1:pending-link"] = TranscriptDeliveryRecord{
			ID:             "delivery:s1:pending-link",
			SessionID:      "s1",
			SourceRecordID: "r1",
			Status:         TranscriptDeliveryStatusQueued,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed pending-link delivery: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeSequence := sqliteRawChatSequenceJSONForTest(t, store, "chat-1")
	beforeOutbox := sqliteRawOutboxJSONForTest(t, store, existing.ID)
	corruptState := []byte(`{"broken"`)
	sqliteWriteRawStateJSONForTest(t, store, corruptState)

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	out, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
		Message: OutboxMessage{
			ID:          existing.ID,
			SessionID:   "s1",
			TeamsChatID: "chat-1",
			Kind:        "status-progress",
			Body:        "already queued status",
		},
		Delivery: TranscriptDeliveryRecord{
			ID:        "delivery:s1:pending-link",
			SessionID: "s1",
			Status:    TranscriptDeliveryStatusQueued,
		},
	})
	if err != nil {
		t.Fatalf("QueueTranscriptDeliveryOutbox existing outbox error: %v", err)
	}
	if out.ID != existing.ID || created || alreadyDelivered {
		t.Fatalf("QueueTranscriptDeliveryOutbox existing outbox out=%#v created=%v alreadyDelivered=%v", out, created, alreadyDelivered)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("QueueTranscriptDeliveryOutbox existing outbox loaded full state %d times", got)
	}
	if afterSequence := sqliteRawChatSequenceJSONForTest(t, store, "chat-1"); !bytes.Equal(beforeSequence, afterSequence) {
		t.Fatalf("existing outbox path allocated chat sequence:\nbefore=%s\nafter=%s", beforeSequence, afterSequence)
	}
	if afterOutbox := sqliteRawOutboxJSONForTest(t, store, existing.ID); !bytes.Equal(beforeOutbox, afterOutbox) {
		t.Fatalf("existing outbox path rewrote outbox:\nbefore=%s\nafter=%s", beforeOutbox, afterOutbox)
	}
	var delivery TranscriptDeliveryRecord
	if raw := sqliteRawTranscriptDeliveryJSONForTest(t, store, "delivery:s1:pending-link"); json.Unmarshal(raw, &delivery) != nil {
		t.Fatalf("unmarshal pending-link delivery raw")
	}
	if delivery.OutboxID != existing.ID || delivery.Status != TranscriptDeliveryStatusQueued {
		t.Fatalf("pending-link delivery = %#v, want linked to existing outbox", delivery)
	}
	if afterState := sqliteRawStateJSONForTest(t, store); !bytes.Equal(corruptState, afterState) {
		t.Fatalf("QueueTranscriptDeliveryOutbox existing outbox rewrote cold state_json: %s", afterState)
	}
}

func TestSQLiteQueueTranscriptDeliveryOutboxSuppressesDeliveredWithoutColdLoad(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	now := time.Date(2026, 6, 16, 8, 0, 0, 0, time.UTC)
	if err := store.UpdateSession(ctx, "s1", func(state *State) error {
		state.TranscriptDeliveries["delivery:s1:sent"] = TranscriptDeliveryRecord{
			ID:             "delivery:s1:sent",
			SessionID:      "s1",
			SourceRecordID: "r1",
			Status:         TranscriptDeliveryStatusSent,
			CreatedAt:      now,
			UpdatedAt:      now,
			SentAt:         now,
		}
		state.ImportCheckpoints["checkpoint:s1"] = ImportCheckpoint{
			ID:             "checkpoint:s1",
			SessionID:      "s1",
			LastRecordID:   "r1",
			LastSourceLine: 10,
			LastOffset:     256,
			Status:         importCheckpointStatusBlocked,
			UpdatedAt:      now,
		}
		state.ChatSequences["chat-1"] = ChatSequenceState{ChatID: "chat-1", Next: 11, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed delivered transcript: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeDelivery := sqliteRawTranscriptDeliveryJSONForTest(t, store, "delivery:s1:sent")
	corruptState := []byte(`{"broken"`)
	sqliteWriteRawStateJSONForTest(t, store, corruptState)

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	out, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
		Message: OutboxMessage{
			ID:          "outbox:should-not-be-created",
			SessionID:   "s1",
			TeamsChatID: "chat-1",
			Kind:        "final",
			Body:        "duplicate final answer text",
		},
		Delivery: TranscriptDeliveryRecord{
			ID:        "delivery:s1:sent",
			SessionID: "s1",
			Status:    TranscriptDeliveryStatusQueued,
		},
		Checkpoint: ImportCheckpoint{
			ID:             "checkpoint:s1",
			SessionID:      "s1",
			SourcePath:     "/tmp/session.jsonl",
			LastRecordID:   "r2",
			LastSourceLine: 20,
			LastOffset:     512,
		},
	})
	if err != nil {
		t.Fatalf("QueueTranscriptDeliveryOutbox suppress sqlite error: %v", err)
	}
	if out.ID != "" || created || !alreadyDelivered {
		t.Fatalf("QueueTranscriptDeliveryOutbox suppress out=%#v created=%v alreadyDelivered=%v, want empty false true", out, created, alreadyDelivered)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("QueueTranscriptDeliveryOutbox suppress loaded full state %d times", got)
	}
	if afterDelivery := sqliteRawTranscriptDeliveryJSONForTest(t, store, "delivery:s1:sent"); !bytes.Equal(beforeDelivery, afterDelivery) {
		t.Fatalf("suppress rewrote delivered transcript delivery:\nbefore=%s\nafter=%s", beforeDelivery, afterDelivery)
	}
	var checkpoint ImportCheckpoint
	if raw := sqliteRawImportCheckpointJSONForTest(t, store, "checkpoint:s1"); json.Unmarshal(raw, &checkpoint) != nil {
		t.Fatalf("unmarshal checkpoint raw")
	}
	if checkpoint.LastRecordID != "r2" || checkpoint.LastSourceLine != 20 || checkpoint.LastOffset != 512 || checkpoint.Status != importCheckpointStatusComplete {
		t.Fatalf("checkpoint after suppress = %#v, want advanced complete checkpoint", checkpoint)
	}
	if afterState := sqliteRawStateJSONForTest(t, store); !bytes.Equal(corruptState, afterState) {
		t.Fatalf("QueueTranscriptDeliveryOutbox suppress rewrote cold state_json: %s", afterState)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt cold state_json")
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
	defer other.Close()
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
	defer reopened.Close()
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

func TestSQLiteRecordTranscriptCheckpointPrunesSplitTranscriptLedger(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	base := time.Date(2026, 6, 16, 8, 0, 0, 0, time.UTC)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for i := 0; i < maxRetainedTranscriptLedgerRecords+20; i++ {
			record := TranscriptLedgerRecord{
				ID:             fmt.Sprintf("ledger:old:%04d", i),
				SessionID:      "s1",
				SourceRecordID: fmt.Sprintf("old-record-%04d", i),
				ImportedAt:     base.Add(time.Duration(i) * time.Second),
				CreatedAt:      base.Add(time.Duration(i) * time.Second),
				UpdatedAt:      base.Add(time.Duration(i) * time.Second),
			}
			if err := upsertSQLiteTranscriptLedgerTx(ctx, tx, record); err != nil {
				return err
			}
		}
		return nil
	})
	if got := sqliteTableRowCountForTest(t, store, "transcript_ledger"); got != maxRetainedTranscriptLedgerRecords+20 {
		t.Fatalf("seeded transcript_ledger rows = %d, want %d", got, maxRetainedTranscriptLedgerRecords+20)
	}

	if err := store.RecordTranscriptCheckpoint(ctx, ImportCheckpoint{
		ID:             "checkpoint:s1",
		SessionID:      "s1",
		SourcePath:     "/tmp/session.jsonl",
		LastRecordID:   "new-record",
		LastSourceLine: 99,
		LastOffset:     12345,
		Status:         importCheckpointStatusComplete,
	}, TranscriptLedgerRecord{
		ID:             "ledger:new",
		SessionID:      "s1",
		SourcePath:     "/tmp/session.jsonl",
		SourceLine:     99,
		SourceRecordID: "new-record",
		ImportedAt:     base.Add(24 * time.Hour),
		CreatedAt:      base.Add(24 * time.Hour),
		UpdatedAt:      base.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("RecordTranscriptCheckpoint error: %v", err)
	}
	if got := sqliteTableRowCountForTest(t, store, "transcript_ledger"); got != maxRetainedTranscriptLedgerRecords {
		t.Fatalf("pruned transcript_ledger rows = %d, want %d", got, maxRetainedTranscriptLedgerRecords)
	}
	if sqliteRowExistsForTest(t, store, "transcript_ledger", "ledger:old:0000") {
		t.Fatal("oldest transcript ledger row was not pruned")
	}
	if !sqliteRowExistsForTest(t, store, "transcript_ledger", "ledger:new") {
		t.Fatal("new transcript ledger row was pruned")
	}
}

func TestSQLiteTranscriptDeliveryPruneKeepsNewestRows(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, testSession()); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	base := time.Date(2026, 6, 16, 8, 0, 0, 0, time.UTC)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for i := 0; i < 5; i++ {
			delivery := TranscriptDeliveryRecord{
				ID:             fmt.Sprintf("delivery:prune:%02d", i),
				SessionID:      "s1",
				SourceRecordID: fmt.Sprintf("record-%02d", i),
				Status:         TranscriptDeliveryStatusSkipped,
				CreatedAt:      base.Add(time.Duration(i) * time.Second),
				UpdatedAt:      base.Add(time.Duration(i) * time.Second),
			}
			if err := upsertSQLiteTranscriptDeliveryTx(ctx, tx, delivery); err != nil {
				return err
			}
		}
		return pruneSQLiteTranscriptDeliveriesTx(ctx, tx, 3)
	})
	if got := sqliteTableRowCountForTest(t, store, "transcript_deliveries"); got != 3 {
		t.Fatalf("pruned transcript_deliveries rows = %d, want 3", got)
	}
	for _, id := range []string{"delivery:prune:00", "delivery:prune:01"} {
		if sqliteRowExistsForTest(t, store, "transcript_deliveries", id) {
			t.Fatalf("old delivery row %q was not pruned", id)
		}
	}
	for _, id := range []string{"delivery:prune:02", "delivery:prune:03", "delivery:prune:04"} {
		if !sqliteRowExistsForTest(t, store, "transcript_deliveries", id) {
			t.Fatalf("new delivery row %q was pruned", id)
		}
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

func TestBoostChatPollAfterFinalAnswerGuardsAndSQLiteParity(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 15, 8, 0, 0, 0, time.UTC)
	boostAt := now.Add(3 * time.Minute)
	answerAt := now.Add(2 * time.Minute)
	cases := []struct {
		name        string
		mutate      func(*State)
		wantChanged bool
	}{
		{
			name:        "eligible cold continuation",
			wantChanged: true,
		},
		{
			name: "parked poll",
			mutate: func(state *State) {
				poll := state.ChatPolls["chat-1"]
				poll.PollState = chatPollStateParked
				poll.ParkedAt = now.Add(-49 * time.Hour)
				poll.ParkNoticeSentAt = now.Add(-48 * time.Hour)
				state.ChatPolls["chat-1"] = poll
			},
		},
		{
			name: "blocked poll state",
			mutate: func(state *State) {
				poll := state.ChatPolls["chat-1"]
				poll.PollState = chatPollStateBlocked
				poll.BlockedUntil = now.Add(10 * time.Minute)
				state.ChatPolls["chat-1"] = poll
			},
		},
		{
			name: "blocked until future",
			mutate: func(state *State) {
				poll := state.ChatPolls["chat-1"]
				poll.BlockedUntil = now.Add(10 * time.Minute)
				state.ChatPolls["chat-1"] = poll
			},
		},
		{
			name: "catchup poll",
			mutate: func(state *State) {
				poll := state.ChatPolls["chat-1"]
				poll.PollState = chatPollStateCatchup
				state.ChatPolls["chat-1"] = poll
			},
		},
		{
			name: "active transcript import",
			mutate: func(state *State) {
				state.ServiceOwner = &OwnerMetadata{StartedAt: now.Add(-time.Hour), LastHeartbeat: now}
				state.ImportCheckpoints[transcriptCheckpointIDForSession("s001")] = ImportCheckpoint{
					ID:        transcriptCheckpointIDForSession("s001"),
					SessionID: "s001",
					Status:    importCheckpointStatusImporting,
					UpdatedAt: now.Add(-time.Minute),
				}
			},
		},
		{
			name: "orphan transcript import",
			mutate: func(state *State) {
				state.ServiceOwner = &OwnerMetadata{StartedAt: now.Add(time.Minute), LastHeartbeat: now.Add(time.Minute)}
				state.ImportCheckpoints[transcriptCheckpointIDForSession("s001")] = ImportCheckpoint{
					ID:        transcriptCheckpointIDForSession("s001"),
					SessionID: "s001",
					Status:    importCheckpointStatusImporting,
					UpdatedAt: now.Add(-time.Minute),
				}
			},
			wantChanged: true,
		},
		{
			name: "closed session",
			mutate: func(state *State) {
				session := state.Sessions["s001"]
				session.Status = SessionStatusClosed
				state.Sessions["s001"] = session
			},
		},
		{
			name: "session chat mismatch",
			mutate: func(state *State) {
				session := state.Sessions["s001"]
				session.TeamsChatID = "chat-other"
				state.Sessions["s001"] = session
			},
		},
		{
			name: "unseeded poll",
			mutate: func(state *State) {
				poll := state.ChatPolls["chat-1"]
				poll.Seeded = false
				state.ChatPolls["chat-1"] = poll
			},
		},
	}
	for _, tc := range cases {
		for _, backend := range []string{"json", "sqlite"} {
			t.Run(tc.name+"/"+backend, func(t *testing.T) {
				store := newTestStore(t)
				seedFinalAnswerPollBoostState(t, store, now, tc.mutate)
				if backend == "sqlite" {
					migrateStoreToSQLiteForTest(t, store)
				}
				before, ok, err := store.ChatPoll(ctx, "chat-1")
				if err != nil || !ok {
					t.Fatalf("ChatPoll before boost ok=%v err=%v", ok, err)
				}

				got, changed, err := store.BoostChatPollAfterFinalAnswer(ctx, FinalAnswerPollBoostRequest{
					SessionID:      "s001",
					TeamsChatID:    "chat-1",
					NextPollAt:     boostAt,
					LastActivityAt: answerAt,
				})
				if err != nil {
					t.Fatalf("BoostChatPollAfterFinalAnswer error: %v", err)
				}
				if changed != tc.wantChanged {
					t.Fatalf("BoostChatPollAfterFinalAnswer changed=%v, want %v; got=%#v", changed, tc.wantChanged, got)
				}
				after, ok, err := store.ChatPoll(ctx, "chat-1")
				if err != nil || !ok {
					t.Fatalf("ChatPoll after boost ok=%v err=%v", ok, err)
				}
				if !tc.wantChanged {
					if !reflect.DeepEqual(after, before) {
						t.Fatalf("no-op boost changed poll:\nbefore=%#v\nafter=%#v", before, after)
					}
					return
				}
				if got != after {
					t.Fatalf("returned poll differs from stored poll:\ngot=%#v\nafter=%#v", got, after)
				}
				if after.PollState != chatPollStateHot || !after.NextPollAt.Equal(boostAt) || !after.LastActivityAt.Equal(answerAt) || !after.UpdatedAt.Equal(boostAt) {
					t.Fatalf("boosted poll schedule mismatch: %#v", after)
				}
				if after.ContinuationPath != before.ContinuationPath || after.ContinuationPath == "" {
					t.Fatalf("boost should preserve continuation path: before=%#v after=%#v", before, after)
				}
				if !after.ParkedAt.IsZero() || !after.ParkNoticeSentAt.IsZero() {
					t.Fatalf("boost should not leave park markers: %#v", after)
				}
			})
		}
	}
}

func TestBoostChatPollAfterFinalAnswerSQLiteUsesNarrowRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 6, 15, 9, 0, 0, 0, time.UTC)
	seedFinalAnswerPollBoostState(t, store, now, func(state *State) {
		state.Sessions["broken"] = SessionContext{ID: "broken", Status: SessionStatusActive, TeamsChatID: "chat-broken", UpdatedAt: now}
		state.ChatPolls["chat-broken"] = ChatPollState{ChatID: "chat-broken", Seeded: true, PollState: chatPollStateCold, UpdatedAt: now}
		state.ImportCheckpoints["transcript:broken"] = ImportCheckpoint{ID: "transcript:broken", SessionID: "broken", Status: "complete", UpdatedAt: now}
	})
	migrateStoreToSQLiteForTest(t, store)
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, []byte(`{"broken"`), "chat-broken"); err != nil {
		t.Fatalf("corrupt unrelated chat poll: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE import_checkpoints SET json = ? WHERE id = ?`, []byte(`{"broken"`), "transcript:broken"); err != nil {
		t.Fatalf("corrupt unrelated import checkpoint: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, []byte(`{"broken"`), sqliteRuntimeKeyServiceOwner); err != nil {
		t.Fatalf("corrupt service owner: %v", err)
	}

	boostAt := now.Add(3 * time.Minute)
	answerAt := now.Add(2 * time.Minute)
	got, changed, err := store.BoostChatPollAfterFinalAnswer(ctx, FinalAnswerPollBoostRequest{
		SessionID:      "s001",
		TeamsChatID:    "chat-1",
		NextPollAt:     boostAt,
		LastActivityAt: answerAt,
	})
	if err != nil {
		t.Fatalf("BoostChatPollAfterFinalAnswer narrow sqlite error: %v", err)
	}
	if !changed || got.PollState != chatPollStateHot || !got.NextPollAt.Equal(boostAt) || got.ContinuationPath == "" {
		t.Fatalf("narrow sqlite boost result = %#v changed=%v", got, changed)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt unrelated rows")
	}
}

func seedFinalAnswerPollBoostState(t *testing.T, store *Store, now time.Time, mutate func(*State)) {
	t.Helper()
	if err := store.Update(context.Background(), func(state *State) error {
		state.Sessions["s001"] = SessionContext{
			ID:          "s001",
			Status:      SessionStatusActive,
			TeamsChatID: "chat-1",
			UpdatedAt:   now.Add(-time.Hour),
		}
		state.ChatPolls["chat-1"] = ChatPollState{
			ChatID:           "chat-1",
			Seeded:           true,
			PollState:        chatPollStateCold,
			NextPollAt:       now.Add(time.Hour),
			LastActivityAt:   now.Add(-time.Hour),
			ContinuationPath: "/chats/chat-1/messages?$skiptoken=old",
			UpdatedAt:        now.Add(-time.Hour),
		}
		if mutate != nil {
			mutate(state)
		}
		return nil
	}); err != nil {
		t.Fatalf("seed final answer poll boost state: %v", err)
	}
}

func TestChatPollCleanupClearsContinuationBackoffAndErrorForParkedAndClosed(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name          string
		sessionID     string
		chatID        string
		sessionStatus SessionStatus
		pollState     string
	}{
		{name: "parked", sessionID: "s-parked", chatID: "chat-parked", sessionStatus: SessionStatusActive, pollState: "parked"},
		{name: "closed", sessionID: "s-closed", chatID: "chat-closed", sessionStatus: SessionStatusClosed, pollState: "cold"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := store.CreateSession(ctx, SessionContext{
				ID:          tc.sessionID,
				Status:      tc.sessionStatus,
				TeamsChatID: tc.chatID,
				UpdatedAt:   now.Add(-49 * time.Hour),
			}); err != nil {
				t.Fatalf("CreateSession error: %v", err)
			}
			if _, err := store.RecordChatPollSuccessWithContinuation(ctx, tc.chatID, now.Add(-time.Hour), true, true, 50, "/chats/"+tc.chatID+"/messages?$skiptoken=stale"); err != nil {
				t.Fatalf("RecordChatPollSuccessWithContinuation error: %v", err)
			}
			if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
				ChatID:         tc.chatID,
				PollState:      tc.pollState,
				LastActivityAt: now.Add(-49 * time.Hour),
				BlockedUntil:   now.Add(time.Hour),
			}); err != nil {
				t.Fatalf("UpdateChatPollSchedule error: %v", err)
			}
			if err := store.RecordChatPollError(ctx, tc.chatID, "temporary graph error"); err != nil {
				t.Fatalf("RecordChatPollError error: %v", err)
			}
			poll, ok, err := store.ChatPoll(ctx, tc.chatID)
			if err != nil || !ok {
				t.Fatalf("ChatPoll before cleanup ok=%v err=%v", ok, err)
			}
			if poll.ContinuationPath == "" || poll.BlockedUntil.IsZero() || poll.LastError == "" || poll.LastErrorAt.IsZero() || poll.FailureCount == 0 {
				t.Fatalf("test poll was not dirty before cleanup: %#v", poll)
			}

			poll, err = store.ClearChatPollContinuationBackoffAndError(ctx, tc.chatID)
			if err != nil {
				t.Fatalf("ClearChatPollContinuationBackoffAndError error: %v", err)
			}
			if poll.PollState != tc.pollState || poll.ContinuationPath != "" || !poll.BlockedUntil.IsZero() || poll.LastError != "" || !poll.LastErrorAt.IsZero() || poll.FailureCount != 0 {
				t.Fatalf("cleanup poll mismatch: %#v", poll)
			}
			if tc.sessionStatus == SessionStatusClosed {
				state, err := store.Load(ctx)
				if err != nil {
					t.Fatalf("Load state: %v", err)
				}
				if got := state.Sessions[tc.sessionID].Status; got != SessionStatusClosed {
					t.Fatalf("closed session status changed to %q", got)
				}
			}

			before, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read state before idempotent cleanup: %v", err)
			}
			pollBefore, ok, err := store.ChatPoll(ctx, tc.chatID)
			if err != nil || !ok {
				t.Fatalf("ChatPoll before idempotent cleanup ok=%v err=%v", ok, err)
			}
			pollAfter, err := store.ClearChatPollContinuationBackoffAndError(ctx, tc.chatID)
			if err != nil {
				t.Fatalf("idempotent ClearChatPollContinuationBackoffAndError error: %v", err)
			}
			after, err := os.ReadFile(store.Path())
			if err != nil {
				t.Fatalf("read state after idempotent cleanup: %v", err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("idempotent cleanup rewrote state file")
			}
			if !pollAfter.UpdatedAt.Equal(pollBefore.UpdatedAt) {
				t.Fatalf("idempotent cleanup changed UpdatedAt: before=%s after=%s", pollBefore.UpdatedAt, pollAfter.UpdatedAt)
			}
		})
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

func TestRecordChatPollErrorWithBlockSQLiteUsesNarrowRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Add(time.Hour)
	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      chatPollStateHot,
		NextPollAt:     now.Add(time.Second),
		LastActivityAt: now,
		ResetFailures:  true,
	}); err != nil {
		t.Fatalf("seed target poll: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:         "chat-unrelated",
		PollState:      chatPollStateWarm,
		NextPollAt:     now.Add(10 * time.Minute),
		LastActivityAt: now.Add(-time.Hour),
		ResetFailures:  true,
	}); err != nil {
		t.Fatalf("seed unrelated poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	beforeStateJSON := sqliteRawStateJSONForTest(t, store)
	beforeUnrelated := sqliteRawChatPollJSONForTest(t, store, "chat-unrelated")
	corruptUnrelated := []byte(`{"broken"`)
	sqliteWriteRawChatPollJSONForTest(t, store, "chat-unrelated", corruptUnrelated)

	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	blockedUntil := now.Add(time.Minute)
	if err := store.RecordChatPollErrorWithBlock(ctx, "chat-1", "429 Too Many Requests", blockedUntil); err != nil {
		t.Fatalf("RecordChatPollErrorWithBlock sqlite error: %v", err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("RecordChatPollErrorWithBlock loaded full state %d times", got)
	}
	if afterStateJSON := sqliteRawStateJSONForTest(t, store); !bytes.Equal(beforeStateJSON, afterStateJSON) {
		t.Fatal("RecordChatPollErrorWithBlock rewrote cold state_json")
	}
	if afterUnrelated := sqliteRawChatPollJSONForTest(t, store, "chat-unrelated"); !bytes.Equal(corruptUnrelated, afterUnrelated) {
		t.Fatalf("RecordChatPollErrorWithBlock rewrote unrelated chat poll:\nbefore=%q\nafter=%q\noriginal=%q", corruptUnrelated, afterUnrelated, beforeUnrelated)
	}

	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll target after error ok=%v err=%v", ok, err)
	}
	if poll.ChatID != "chat-1" || poll.LastError != "429 Too Many Requests" || poll.FailureCount != 1 || poll.LastErrorAt.IsZero() {
		t.Fatalf("target poll error fields mismatch: %#v", poll)
	}
	if poll.PollState != chatPollStateBlocked || poll.PreviousPollState != chatPollStateHot || !poll.BlockedUntil.Equal(blockedUntil) || !poll.NextPollAt.Equal(blockedUntil) {
		t.Fatalf("target poll block fields mismatch: %#v", poll)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("full Load unexpectedly succeeded with corrupt unrelated chat poll row")
	}
}

func TestSQLiteMarkChatPollParkNoticeSentDoesNotLoadFullState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      "parked",
		LastActivityAt: now.Add(-49 * time.Hour),
	}); err != nil {
		t.Fatalf("seed parked poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	sentAt := now.Add(time.Minute)
	poll, err := store.MarkChatPollParkNoticeSent(ctx, "chat-1", sentAt)
	if err != nil {
		t.Fatalf("MarkChatPollParkNoticeSent sqlite error: %v", err)
	}
	if poll.ChatID != "chat-1" || !poll.ParkNoticeSentAt.Equal(sentAt) {
		t.Fatalf("park notice poll = %#v", poll)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("MarkChatPollParkNoticeSent loaded full state %d times", got)
	}
	loaded, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll after mark ok=%v err=%v", ok, err)
	}
	if !loaded.ParkNoticeSentAt.Equal(sentAt) {
		t.Fatalf("loaded park notice timestamp = %s, want %s", loaded.ParkNoticeSentAt, sentAt)
	}
}

func TestSQLiteChatSessionActivityDoesNotLoadFullState(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		sessions   []SessionContext
		wantMatch  bool
		wantActive bool
	}{
		{
			name: "active shared chat",
			sessions: []SessionContext{
				{ID: "s-active", TeamsChatID: "chat-1", Status: SessionStatusActive},
				{ID: "s-closed", TeamsChatID: "chat-1", Status: SessionStatusClosed},
			},
			wantMatch:  true,
			wantActive: true,
		},
		{
			name: "terminal shared chat",
			sessions: []SessionContext{
				{ID: "s-closed-a", TeamsChatID: "chat-1", Status: SessionStatusClosed},
				{ID: "s-closed-b", TeamsChatID: "chat-1", Status: SessionStatusArchived},
			},
			wantMatch:  true,
			wantActive: false,
		},
		{
			name:       "no matching chat",
			sessions:   []SessionContext{{ID: "s-other", TeamsChatID: "chat-other", Status: SessionStatusActive}},
			wantMatch:  false,
			wantActive: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				for _, session := range tc.sessions {
					state.Sessions[session.ID] = session
				}
				return nil
			}); err != nil {
				t.Fatalf("seed sessions: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			var loads int64
			prev := loadUnlockedTestHook
			loadUnlockedTestHook = func() {
				atomic.AddInt64(&loads, 1)
			}
			t.Cleanup(func() {
				loadUnlockedTestHook = prev
			})

			matched, active, err := store.ChatSessionActivity(ctx, "chat-1")
			if err != nil {
				t.Fatalf("ChatSessionActivity sqlite error: %v", err)
			}
			if matched != tc.wantMatch || active != tc.wantActive {
				t.Fatalf("ChatSessionActivity = matched=%v active=%v, want matched=%v active=%v", matched, active, tc.wantMatch, tc.wantActive)
			}
			if got := atomic.LoadInt64(&loads); got != 0 {
				t.Fatalf("ChatSessionActivity loaded full state %d times", got)
			}
		})
	}
}

func TestSQLiteChatRateLimitDoesNotLoadFullState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	blockedUntil := time.Date(2026, 6, 9, 9, 30, 0, 0, time.UTC)
	if _, err := store.SetChatRateLimitForOutbox(ctx, "chat-1", blockedUntil, "429", "outbox-1"); err != nil {
		t.Fatalf("seed chat rate limit: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, "chat-other", blockedUntil.Add(time.Hour), "other"); err != nil {
		t.Fatalf("seed other chat rate limit: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	limit, ok, err := store.ChatRateLimit(ctx, "chat-1")
	if err != nil {
		t.Fatalf("ChatRateLimit sqlite error: %v", err)
	}
	if !ok || limit.ChatID != "chat-1" || !limit.BlockedUntil.Equal(blockedUntil) || limit.Reason != "429" || limit.PoisonOutboxID != "outbox-1" {
		t.Fatalf("ChatRateLimit sqlite = %#v ok=%v", limit, ok)
	}
	if _, ok, err := store.ChatRateLimit(ctx, "missing"); err != nil || ok {
		t.Fatalf("ChatRateLimit missing ok=%v err=%v, want false nil", ok, err)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("ChatRateLimit loaded full state %d times", got)
	}
}

func TestSQLiteMarkOutboxSendAttemptDoesNotReadColdState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 9, 45, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s1"] = SessionContext{ID: "s1", TeamsChatID: "chat-1", CodexThreadID: "thread-1", CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-1"] = Turn{ID: "turn-1", SessionID: "s1", Status: TurnStatusRunning, CodexThreadID: "thread-1", CreatedAt: now, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed session and turn: %v", err)
	}
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox-1",
		SessionID:   "s1",
		TurnID:      "turn-1",
		TeamsChatID: "chat-1",
		Kind:        "queued-status",
		Body:        "working",
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox created=%v err=%v", created, err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load queued outbox state: %v", err)
	}
	var queuedDelivery HelperDeliveryRecord
	for _, delivery := range state.HelperDeliveries {
		if delivery.OutboxID == msg.ID {
			queuedDelivery = delivery
			break
		}
	}
	if queuedDelivery.ID == "" || queuedDelivery.Status != HelperDeliveryStatusQueued {
		t.Fatalf("queued helper delivery = %#v", queuedDelivery)
	}
	migrateStoreToSQLiteForTest(t, store)
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, []byte(`{"broken"`)); err != nil {
		t.Fatalf("corrupt cold state: %v", err)
	}

	claimed, err := store.MarkOutboxSendAttempt(ctx, msg.ID)
	if err != nil {
		t.Fatalf("MarkOutboxSendAttempt with corrupt cold state: %v", err)
	}
	if claimed.Status != OutboxStatusSending || claimed.LastSendAttempt.IsZero() || claimed.LastSendError != "" {
		t.Fatalf("claimed outbox = %#v", claimed)
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT json FROM helper_deliveries WHERE outbox_id = ?`, msg.ID).Scan(&raw); err != nil {
		t.Fatalf("load helper delivery row: %v", err)
	}
	var delivery HelperDeliveryRecord
	if err := json.Unmarshal(raw, &delivery); err != nil {
		t.Fatalf("unmarshal helper delivery row: %v", err)
	}
	if delivery.Status != HelperDeliveryStatusSending || delivery.ID != queuedDelivery.ID || !delivery.CreatedAt.Equal(queuedDelivery.CreatedAt) || delivery.CodexThreadID != "thread-1" {
		t.Fatalf("helper delivery after send attempt = %#v, queued was %#v", delivery, queuedDelivery)
	}
}

func TestSQLiteMarkOutboxDeliveredDoesNotReadColdState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s1"] = SessionContext{ID: "s1", TeamsChatID: "chat-1", CodexThreadID: "thread-1", CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-1"] = Turn{ID: "turn-1", SessionID: "s1", Status: TurnStatusRunning, CodexThreadID: "thread-1", CreatedAt: now, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed session and turn: %v", err)
	}
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox-1",
		SessionID:   "s1",
		TurnID:      "turn-1",
		TeamsChatID: "chat-1",
		Kind:        "queued-status",
		Body:        "done",
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, []byte(`{"broken"`)); err != nil {
		t.Fatalf("corrupt cold state: %v", err)
	}

	accepted, err := store.MarkOutboxAccepted(ctx, msg.ID, "teams-message-1")
	if err != nil {
		t.Fatalf("MarkOutboxAccepted with corrupt cold state: %v", err)
	}
	if accepted.Status != OutboxStatusAccepted || accepted.TeamsMessageID != "teams-message-1" {
		t.Fatalf("accepted outbox = %#v", accepted)
	}
	sent, err := store.MarkOutboxSent(ctx, msg.ID, "teams-message-1")
	if err != nil {
		t.Fatalf("MarkOutboxSent with corrupt cold state: %v", err)
	}
	if sent.Status != OutboxStatusSent || sent.TeamsMessageID != "teams-message-1" || sent.SentAt.IsZero() {
		t.Fatalf("sent outbox = %#v", sent)
	}
	var provenanceRaw []byte
	if err := db.QueryRowContext(ctx, `SELECT json FROM message_provenance WHERE teams_chat_id = ? AND teams_message_id = ?`, "chat-1", "teams-message-1").Scan(&provenanceRaw); err != nil {
		t.Fatalf("load message provenance row: %v", err)
	}
	var provenance MessageProvenanceRecord
	if err := json.Unmarshal(provenanceRaw, &provenance); err != nil {
		t.Fatalf("unmarshal message provenance row: %v", err)
	}
	if provenance.Origin != MessageOriginHelperOutbox || provenance.OutboxID != msg.ID || provenance.SessionID != "s1" {
		t.Fatalf("message provenance = %#v", provenance)
	}
	var deliveryRaw []byte
	if err := db.QueryRowContext(ctx, `SELECT json FROM helper_deliveries WHERE outbox_id = ?`, msg.ID).Scan(&deliveryRaw); err != nil {
		t.Fatalf("load helper delivery row: %v", err)
	}
	var delivery HelperDeliveryRecord
	if err := json.Unmarshal(deliveryRaw, &delivery); err != nil {
		t.Fatalf("unmarshal helper delivery row: %v", err)
	}
	if delivery.Status != HelperDeliveryStatusSent || delivery.TeamsMessageID != "teams-message-1" || delivery.CodexThreadID != "thread-1" || delivery.SentAt.IsZero() {
		t.Fatalf("helper delivery after sent = %#v", delivery)
	}
}

func TestSQLiteSentOutboxMessagesForChatDoesNotLoadFullState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 9, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["sent-1"] = OutboxMessage{
			ID:          "sent-1",
			TeamsChatID: "chat-1",
			Status:      OutboxStatusSent,
			CreatedAt:   now,
			UpdatedAt:   now,
			SentAt:      now,
		}
		state.OutboxMessages["accepted-1"] = OutboxMessage{
			ID:          "accepted-1",
			TeamsChatID: "chat-1",
			Status:      OutboxStatusAccepted,
			CreatedAt:   now.Add(time.Second),
			UpdatedAt:   now.Add(time.Second),
		}
		state.OutboxMessages["sent-other"] = OutboxMessage{
			ID:          "sent-other",
			TeamsChatID: "chat-other",
			Status:      OutboxStatusSent,
			CreatedAt:   now.Add(2 * time.Second),
			UpdatedAt:   now.Add(2 * time.Second),
			SentAt:      now.Add(2 * time.Second),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	var loads int64
	prev := loadUnlockedTestHook
	loadUnlockedTestHook = func() {
		atomic.AddInt64(&loads, 1)
	}
	t.Cleanup(func() {
		loadUnlockedTestHook = prev
	})

	messages, err := store.SentOutboxMessagesForChat(ctx, " chat-1 ")
	if err != nil {
		t.Fatalf("SentOutboxMessagesForChat sqlite error: %v", err)
	}
	if len(messages) != 1 || messages[0].ID != "sent-1" {
		t.Fatalf("SentOutboxMessagesForChat = %#v, want only sent-1", messages)
	}
	if got := atomic.LoadInt64(&loads); got != 0 {
		t.Fatalf("SentOutboxMessagesForChat loaded full state %d times", got)
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

func TestSQLiteOwnerLeaseColdUpdatesPreserveHotTables(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 24, 9, 30, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-1"] = SessionContext{ID: "session-1", TeamsChatID: "chat-1", Status: SessionStatusActive, LatestTurnID: "turn-1", CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-1"] = Turn{ID: "turn-1", SessionID: "session-1", Status: TurnStatusQueued, CreatedAt: now, UpdatedAt: now}
		state.OutboxMessages["outbox-1"] = OutboxMessage{ID: "outbox-1", SessionID: "session-1", TurnID: "turn-1", TeamsChatID: "chat-1", Status: OutboxStatusQueued, Body: "queued message", CreatedAt: now, UpdatedAt: now}
		state.MessageProvenance["prov-1"] = MessageProvenanceRecord{ID: "prov-1", TeamsChatID: "chat-1", TeamsMessageID: "msg-1", Origin: MessageOriginHelperOutbox, SessionID: "session-1", OutboxID: "outbox-1", CreatedAt: now, UpdatedAt: now}
		state.ImportCheckpoints["transcript:session-1"] = ImportCheckpoint{ID: "transcript:session-1", SessionID: "session-1", SourcePath: "/tmp/session.jsonl", LastRecordID: "record-1", Status: "complete", UpdatedAt: now}
		state.TranscriptLedger["ledger-1"] = TranscriptLedgerRecord{ID: "ledger-1", SessionID: "session-1", SourceRecordID: "record-1", CreatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	scope := ScopeIdentity{ID: "scope-1", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	machine := MachineRecord{ID: "machine-1", ScopeID: scope.ID, Kind: MachineKindPrimary, Hostname: "host-a", Priority: 10}
	owner := testOwner("session-1", "turn-1", now)
	owner.ScopeID = scope.ID
	owner.MachineID = machine.ID
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Owner: owner, Duration: time.Minute, Now: now})
	if err != nil {
		t.Fatalf("ClaimControlLease error: %v", err)
	}
	if decision.Mode != LeaseModeActive || decision.Lease.HolderMachineID != machine.ID {
		t.Fatalf("ClaimControlLease decision = %#v, want active machine", decision)
	}
	owner.LeaseGeneration = decision.Lease.Generation
	heartbeatAt := now.Add(10 * time.Second)
	updated, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, heartbeatAt)
	if err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	if !updated.LastHeartbeat.Equal(heartbeatAt) || updated.MachineID != machine.ID || updated.LeaseGeneration != decision.Lease.Generation {
		t.Fatalf("updated owner = %#v", updated)
	}
	read, ok, err := store.ReadOwner(ctx)
	if err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	}
	if !ok || read.MachineID != machine.ID || read.ActiveTurnID != "turn-1" {
		t.Fatalf("ReadOwner = %#v ok=%v", read, ok)
	}
	if _, err := store.ValidateControlLease(ctx, machine.ID, decision.Lease.Generation, now.Add(20*time.Second)); err != nil {
		t.Fatalf("ValidateControlLease error: %v", err)
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if _, ok := state.Sessions["session-1"]; !ok {
		t.Fatalf("session hot table row was lost: %#v", state.Sessions)
	}
	if _, ok := state.Turns["turn-1"]; !ok {
		t.Fatalf("turn hot table row was lost: %#v", state.Turns)
	}
	if outbox := state.OutboxMessages["outbox-1"]; outbox.Body != "queued message" || outbox.Status != OutboxStatusQueued {
		t.Fatalf("outbox hot table row mutated: %#v", outbox)
	}
	if _, ok := state.MessageProvenance["prov-1"]; !ok {
		t.Fatalf("provenance hot table row was lost: %#v", state.MessageProvenance)
	}
	if checkpoint := state.ImportCheckpoints["transcript:session-1"]; checkpoint.LastRecordID != "record-1" || checkpoint.Status != "complete" {
		t.Fatalf("cold checkpoint mutated unexpectedly: %#v", checkpoint)
	}
	if owner, ok := state.readOwner(); !ok || owner.MachineID != machine.ID || !owner.LastHeartbeat.Equal(heartbeatAt) {
		t.Fatalf("loaded owner = %#v ok=%v", owner, ok)
	}
}

func TestSQLiteHistoryWatchColdUpdatePreservesSplitTables(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	checkpointID := "history-watch:test"
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-1"] = SessionContext{ID: "session-1", TeamsChatID: "chat-1", Status: SessionStatusActive, LatestTurnID: "turn-1", CreatedAt: now, UpdatedAt: now}
		state.InboundEvents["inbound-1"] = InboundEvent{ID: "inbound-1", SessionID: "session-1", TeamsChatID: "chat-1", TeamsMessageID: "message-1", Status: InboundStatusPersisted, CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-1"] = Turn{ID: "turn-1", SessionID: "session-1", InboundEventID: "inbound-1", Status: TurnStatusQueued, CreatedAt: now, UpdatedAt: now}
		state.OutboxMessages["outbox-1"] = OutboxMessage{ID: "outbox-1", SessionID: "session-1", TurnID: "turn-1", TeamsChatID: "chat-1", Status: OutboxStatusQueued, Body: "queued message", CreatedAt: now, UpdatedAt: now}
		state.MessageProvenance["prov-1"] = MessageProvenanceRecord{ID: "prov-1", TeamsChatID: "chat-1", TeamsMessageID: "message-1", Origin: MessageOriginUserInbound, SessionID: "session-1", InboundID: "inbound-1", CreatedAt: now, UpdatedAt: now}
		state.ImportCheckpoints["transcript:session-1"] = ImportCheckpoint{ID: "transcript:session-1", SessionID: "session-1", SourcePath: "/tmp/session.jsonl", LastRecordID: "record-1", Status: "complete", UpdatedAt: now}
		state.TranscriptLedger["ledger-1"] = TranscriptLedgerRecord{ID: "ledger-1", SessionID: "session-1", SourceRecordID: "record-1", CreatedAt: now}
		state.TranscriptDeliveries["delivery-1"] = TranscriptDeliveryRecord{ID: "delivery-1", SessionID: "session-1", OutboxID: "outbox-1", Status: TranscriptDeliveryStatusSent, CreatedAt: now}
		state.HelperDeliveries["helper-1"] = HelperDeliveryRecord{ID: "helper-1", SessionID: "session-1", TurnID: "turn-1", OutboxID: "outbox-1", Status: HelperDeliveryStatusSent, CreatedAt: now}
		state.ArtifactRecords["artifact-1"] = ArtifactRecord{ID: "artifact-1", SessionID: "session-1", TurnID: "turn-1", OutboxID: "outbox-1", Status: "uploaded", CreatedAt: now}
		state.Notifications["notification-1"] = NotificationRecord{ID: "notification-1", SessionID: "session-1", TurnID: "turn-1", Status: NotificationStatusSent, CreatedAt: now}
		state.HistoryWatch[checkpointID] = HistoryWatchCheckpoint{ID: checkpointID, Path: "/tmp/session.jsonl", Size: 100, Offset: 100, Line: 10, SessionID: "session-1", ThreadID: "thread-1", TeamsOriginThreadID: "thread-1", TurnID: "turn-1", TeamsOriginTurnID: "turn-1", UpdatedAt: now}
		state.HistoryWatchReady = now
		return nil
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	tables := []string{"sessions", "inbound_events", "turns", "outbox_messages", "message_provenance", "import_checkpoints", "transcript_ledger", "transcript_deliveries", "helper_deliveries", "artifact_records", "notifications"}
	beforeCounts := sqliteTableCountsForTest(t, store, tables...)
	watchState, err := store.HistoryWatchState(ctx)
	if err != nil {
		t.Fatalf("HistoryWatchState sqlite error: %v", err)
	}
	if watchState.HistoryWatch[checkpointID].Path == "" || watchState.HistoryWatchReady.IsZero() {
		t.Fatalf("HistoryWatchState missed checkpoint metadata: %#v", watchState)
	}
	if watchState.HistoryWatch[checkpointID].TeamsOriginTurnID != "turn-1" {
		t.Fatalf("HistoryWatchState missed Teams-origin checkpoint turn: %#v", watchState.HistoryWatch[checkpointID])
	}
	if watchState.HistoryWatch[checkpointID].TeamsOriginThreadID != "thread-1" {
		t.Fatalf("HistoryWatchState missed Teams-origin checkpoint thread: %#v", watchState.HistoryWatch[checkpointID])
	}
	if len(watchState.Sessions) != 0 || len(watchState.InboundEvents) != 0 || len(watchState.Turns) != 0 || len(watchState.OutboxMessages) != 0 {
		t.Fatalf("HistoryWatchState should not load hot tables: sessions=%d inbound=%d turns=%d outbox=%d", len(watchState.Sessions), len(watchState.InboundEvents), len(watchState.Turns), len(watchState.OutboxMessages))
	}

	updatedAt := now.Add(2 * time.Minute)
	if err := store.UpdateHistoryWatch(ctx, func(historyWatch map[string]HistoryWatchCheckpoint, ready *time.Time) error {
		checkpoint := historyWatch[checkpointID]
		checkpoint.Size = 128
		checkpoint.Offset = 128
		checkpoint.Line = 12
		checkpoint.LastFinalID = "final-1"
		checkpoint.TeamsOriginThreadID = "thread-teams-origin"
		checkpoint.TeamsOriginTurnID = "turn-teams-origin"
		checkpoint.UpdatedAt = updatedAt
		historyWatch[checkpointID] = checkpoint
		*ready = updatedAt
		return nil
	}); err != nil {
		t.Fatalf("UpdateHistoryWatch sqlite error: %v", err)
	}
	afterCounts := sqliteTableCountsForTest(t, store, tables...)
	if !reflect.DeepEqual(afterCounts, beforeCounts) {
		t.Fatalf("split table counts changed after history watch update: before=%#v after=%#v", beforeCounts, afterCounts)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after history watch update: %v", err)
	}
	if checkpoint := state.HistoryWatch[checkpointID]; checkpoint.Offset != 128 || checkpoint.LastFinalID != "final-1" || checkpoint.TeamsOriginThreadID != "thread-teams-origin" || checkpoint.TeamsOriginTurnID != "turn-teams-origin" || !checkpoint.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("history watch checkpoint not updated: %#v", checkpoint)
	}
	if state.Sessions["session-1"].ID == "" || state.InboundEvents["inbound-1"].ID == "" || state.Turns["turn-1"].ID == "" || state.OutboxMessages["outbox-1"].ID == "" {
		t.Fatalf("hot rows were lost: sessions=%#v inbound=%#v turns=%#v outbox=%#v", state.Sessions, state.InboundEvents, state.Turns, state.OutboxMessages)
	}
	if state.TranscriptDeliveries["delivery-1"].ID == "" || state.HelperDeliveries["helper-1"].ID == "" || state.Notifications["notification-1"].ID == "" {
		t.Fatalf("split side tables were lost: transcript=%#v helper=%#v notifications=%#v", state.TranscriptDeliveries, state.HelperDeliveries, state.Notifications)
	}
}

func TestSQLiteColdSaveBackfillsSplitTablesFromLegacyStateJSON(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 24, 10, 15, 0, 0, time.UTC)
	var helperDeliveryID string
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-1"] = SessionContext{ID: "session-1", TeamsChatID: "chat-1", Status: SessionStatusActive, CreatedAt: now, UpdatedAt: now}
		state.Turns["turn-1"] = Turn{ID: "turn-1", SessionID: "session-1", Status: TurnStatusQueued, CreatedAt: now, UpdatedAt: now}
		msg := OutboxMessage{ID: "outbox-1", SessionID: "session-1", TurnID: "turn-1", TeamsChatID: "chat-1", Kind: "codex-status-001", Status: OutboxStatusQueued, Body: "queued message", CreatedAt: now, UpdatedAt: now}
		state.OutboxMessages[msg.ID] = msg
		state.ImportCheckpoints["transcript:session-1"] = ImportCheckpoint{ID: "transcript:session-1", SessionID: "session-1", SourcePath: "/tmp/session.jsonl", LastRecordID: "record-1", Status: "complete", UpdatedAt: now}
		state.TranscriptLedger["ledger-1"] = TranscriptLedgerRecord{ID: "ledger-1", SessionID: "session-1", SourceRecordID: "record-1", CreatedAt: now}
		state.TranscriptDeliveries["delivery-1"] = TranscriptDeliveryRecord{ID: "delivery-1", SessionID: "session-1", OutboxID: "outbox-1", Status: TranscriptDeliveryStatusQueued, CreatedAt: now}
		helperDelivery, ok := helperDeliveryRecordFromOutboxLocked(state, msg, HelperDeliveryStatusQueued, now)
		if !ok {
			return errors.New("helper delivery could not be derived from seeded outbox")
		}
		helperDeliveryID = helperDelivery.ID
		state.HelperDeliveries[helperDelivery.ID] = helperDelivery
		state.ArtifactRecords["artifact-1"] = ArtifactRecord{ID: "artifact-1", SessionID: "session-1", TurnID: "turn-1", OutboxID: "outbox-1", Status: "queued", CreatedAt: now}
		state.Notifications["notification-1"] = NotificationRecord{ID: "notification-1", SessionID: "session-1", TurnID: "turn-1", Status: NotificationStatusQueued, CreatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	migrated := migrateStoreToSQLiteForTest(t, store).State
	legacyJSON, err := json.Marshal(migrated)
	if err != nil {
		t.Fatalf("marshal legacy state json: %v", err)
	}
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("sqlite pointer missing")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, table := range []string{"import_checkpoints", "transcript_ledger", "transcript_deliveries", "helper_deliveries", "artifact_records", "notifications"} {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, legacyJSON); err != nil {
			return err
		}
		return tx.Commit()
	}); err != nil {
		t.Fatalf("simulate legacy sqlite state json: %v", err)
	}

	if _, err := store.MarkOutboxSendAttempt(ctx, "outbox-1"); err != nil {
		t.Fatalf("MarkOutboxSendAttempt error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if _, ok := state.ImportCheckpoints["transcript:session-1"]; !ok {
		t.Fatalf("import checkpoint was not preserved: %#v", state.ImportCheckpoints)
	}
	if _, ok := state.TranscriptLedger["ledger-1"]; !ok {
		t.Fatalf("transcript ledger was not preserved: %#v", state.TranscriptLedger)
	}
	if delivery := state.TranscriptDeliveries["delivery-1"]; delivery.ID == "" || delivery.Status != TranscriptDeliveryStatusQueued {
		t.Fatalf("transcript delivery mismatch: %#v", delivery)
	}
	if delivery := state.HelperDeliveries[helperDeliveryID]; delivery.ID == "" || delivery.Status != HelperDeliveryStatusSending {
		t.Fatalf("helper delivery mismatch: %#v", delivery)
	}
	if _, ok := state.ArtifactRecords["artifact-1"]; !ok {
		t.Fatalf("artifact record was not preserved: %#v", state.ArtifactRecords)
	}
	if _, ok := state.Notifications["notification-1"]; !ok {
		t.Fatalf("notification was not preserved: %#v", state.Notifications)
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
	prevSleep := durableReplaceSleep
	var sleeps []time.Duration
	durableReplaceSleep = func(delay time.Duration) {
		sleeps = append(sleeps, delay)
	}
	t.Cleanup(func() { durableReplaceSleep = prevSleep })

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
	wantSleeps := []time.Duration{
		25 * time.Millisecond,
		50 * time.Millisecond,
		75 * time.Millisecond,
		100 * time.Millisecond,
		125 * time.Millisecond,
		150 * time.Millisecond,
		175 * time.Millisecond,
	}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("retry sleeps = %v, want %v", sleeps, wantSleeps)
	}
	for i := range sleeps {
		if sleeps[i] != wantSleeps[i] {
			t.Fatalf("retry sleeps = %v, want %v", sleeps, wantSleeps)
		}
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

func sqliteTableCountsForTest(t *testing.T, store *Store, tables ...string) map[string]int {
	t.Helper()
	allowed := map[string]bool{
		"sessions":              true,
		"inbound_events":        true,
		"turns":                 true,
		"outbox_messages":       true,
		"message_provenance":    true,
		"import_checkpoints":    true,
		"transcript_ledger":     true,
		"transcript_deliveries": true,
		"helper_deliveries":     true,
		"artifact_records":      true,
		"notifications":         true,
	}
	ctx := context.Background()
	counts := make(map[string]int, len(tables))
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		for _, table := range tables {
			if !allowed[table] {
				return fmt.Errorf("sqlite table count test does not allow table %q", table)
			}
			var count int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
				return err
			}
			counts[table] = count
		}
		return nil
	}); err != nil {
		t.Fatalf("count sqlite tables: %v", err)
	}
	return counts
}

func seedOfficialReleaseLegacyJSONStoreForTest(t *testing.T, store *Store, tag string, schemaVersion int) {
	t.Helper()
	state := officialReleaseUpgradeFixtureState(tag, schemaVersion, false)
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal %s legacy JSON fixture: %v", tag, err)
	}
	data = append(data, '\n')
	writeRawStoreStateForTest(t, store, data)
}

func seedOfficialReleaseSQLiteStoreForTest(t *testing.T, store *Store, tag string) {
	t.Helper()
	seedOfficialReleaseSQLiteStoreForTestWithOptions(t, store, tag, officialReleaseSQLiteFixtureOptions{})
}

type officialReleaseSQLiteFixtureOptions struct {
	LegacyOutboxColumns bool
}

func seedOfficialReleaseSQLiteStoreForTestWithOptions(t *testing.T, store *Store, tag string, opts officialReleaseSQLiteFixtureOptions) {
	t.Helper()
	state := officialReleaseUpgradeFixtureState(tag, SchemaVersion, true)
	dbPath := filepath.Join(filepath.Dir(store.Path()), storeSQLiteFileName)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		t.Fatalf("create %s sqlite dir: %v", tag, err)
	}
	db, err := openSQLiteStore(dbPath, true)
	if err != nil {
		t.Fatalf("open %s sqlite fixture: %v", tag, err)
	}
	if err := createOfficialReleaseSQLiteSchemaForTest(db); err != nil {
		_ = db.Close()
		t.Fatalf("create %s sqlite fixture schema: %v", tag, err)
	}
	if opts.LegacyOutboxColumns {
		if err := createOfficialReleaseLegacyOutboxSchemaForTest(db); err != nil {
			_ = db.Close()
			t.Fatalf("create %s legacy outbox schema: %v", tag, err)
		}
	}
	if err := insertOfficialReleaseSQLiteStateForTestWithOptions(db, state, opts); err != nil {
		_ = db.Close()
		t.Fatalf("insert %s sqlite fixture state: %v", tag, err)
	}
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint %s sqlite fixture: %v", tag, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close %s sqlite fixture: %v", tag, err)
	}
	writeSQLitePointerForTest(t, store, storeSQLiteFileName)
}

var stableReleaseTagPatternForTest = regexp.MustCompile(`^v([0-9]+)\.([0-9]+)\.([0-9]+)$`)

type stableReleaseVersionForTest struct {
	major int
	minor int
	patch int
}

func gitStableReleaseTagsForTest(t *testing.T) []string {
	t.Helper()
	cmd := exec.Command("git", "tag", "--list", "v*")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("list git tags: %v", err)
	}
	var tags []string
	for _, line := range strings.Split(string(out), "\n") {
		tag := strings.TrimSpace(line)
		if tag == "" {
			continue
		}
		if _, ok := parseStableReleaseTagForTest(tag); ok {
			tags = append(tags, tag)
		}
	}
	if len(tags) == 0 {
		t.Fatal("no stable release tags found; CI must fetch release tags before running this test")
	}
	sort.Strings(tags)
	return tags
}

func officialReleaseUpgradeFixtureTagsForStableTags(tags []string) []string {
	var lastV00 string
	var lastV00Version stableReleaseVersionForTest
	include := map[string]bool{}
	for _, tag := range tags {
		version, ok := parseStableReleaseTagForTest(tag)
		if !ok {
			continue
		}
		if version.major == 0 && version.minor == 0 {
			if lastV00 == "" || stableReleaseVersionLessForTest(lastV00Version, version) {
				lastV00 = tag
				lastV00Version = version
			}
			continue
		}
		if !stableReleaseVersionLessForTest(version, stableReleaseVersionForTest{major: 0, minor: 1, patch: 0}) {
			include[tag] = true
		}
	}
	if lastV00 != "" {
		include[lastV00] = true
	}
	out := make([]string, 0, len(include))
	for tag := range include {
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

func parseStableReleaseTagForTest(tag string) (stableReleaseVersionForTest, bool) {
	match := stableReleaseTagPatternForTest.FindStringSubmatch(tag)
	if match == nil {
		return stableReleaseVersionForTest{}, false
	}
	major, err := strconv.Atoi(match[1])
	if err != nil {
		return stableReleaseVersionForTest{}, false
	}
	minor, err := strconv.Atoi(match[2])
	if err != nil {
		return stableReleaseVersionForTest{}, false
	}
	patch, err := strconv.Atoi(match[3])
	if err != nil {
		return stableReleaseVersionForTest{}, false
	}
	return stableReleaseVersionForTest{major: major, minor: minor, patch: patch}, true
}

func stableReleaseVersionLessForTest(a, b stableReleaseVersionForTest) bool {
	if a.major != b.major {
		return a.major < b.major
	}
	if a.minor != b.minor {
		return a.minor < b.minor
	}
	return a.patch < b.patch
}

func officialReleaseUpgradeFixtureState(tag string, schemaVersion int, includeProvenance bool) State {
	now := time.Date(2026, 6, 10, 10, 30, 0, 0, time.UTC)
	state := State{
		SchemaVersion: schemaVersion,
		CreatedAt:     now.Add(-time.Hour),
		UpdatedAt:     now,
		ServiceOwner: &OwnerMetadata{
			PID:             4242,
			Hostname:        "official-release-host",
			ExecutablePath:  "/usr/local/bin/codex-proxy",
			HelperVersion:   tag,
			ActiveSessionID: "official-session",
			ActiveTurnID:    "official-turn",
			LastHeartbeat:   now,
		},
		Sessions:             map[string]SessionContext{},
		Turns:                map[string]Turn{},
		InboundEvents:        map[string]InboundEvent{},
		OutboxMessages:       map[string]OutboxMessage{},
		MessageProvenance:    map[string]MessageProvenanceRecord{},
		ChatPolls:            map[string]ChatPollState{},
		ChatSequences:        map[string]ChatSequenceState{},
		ChatRateLimits:       map[string]ChatRateLimitState{},
		TranscriptLedger:     map[string]TranscriptLedgerRecord{},
		TranscriptDeliveries: map[string]TranscriptDeliveryRecord{},
		HelperDeliveries:     map[string]HelperDeliveryRecord{},
		ImportCheckpoints:    map[string]ImportCheckpoint{},
		ArtifactRecords:      map[string]ArtifactRecord{},
		Notifications:        map[string]NotificationRecord{},
	}
	state.Sessions["official-session"] = SessionContext{
		ID:            "official-session",
		Status:        SessionStatusActive,
		TeamsChatID:   "official-chat",
		TeamsChatURL:  "https://teams.example/official-chat",
		TeamsTopic:    "official upgrade fixture",
		CodexThreadID: "official-thread",
		LatestTurnID:  "official-turn",
		Cwd:           "/workspace/official-upgrade",
		CreatedAt:     now.Add(-50 * time.Minute),
		UpdatedAt:     now.Add(-5 * time.Minute),
	}
	state.InboundEvents["official-inbound"] = InboundEvent{
		ID:             "official-inbound",
		SessionID:      "official-session",
		TeamsChatID:    "official-chat",
		TeamsMessageID: "official-user-message",
		Source:         "teams",
		Status:         InboundStatusQueued,
		TurnID:         "official-turn",
		Text:           "upgrade from " + tag,
		CreatedAt:      now.Add(-45 * time.Minute),
		UpdatedAt:      now.Add(-45 * time.Minute),
	}
	state.Turns["official-turn"] = Turn{
		ID:             "official-turn",
		SessionID:      "official-session",
		InboundEventID: "official-inbound",
		Status:         TurnStatusCompleted,
		CodexThreadID:  "official-thread",
		CodexTurnID:    "official-codex-turn",
		QueuedAt:       now.Add(-44 * time.Minute),
		CreatedAt:      now.Add(-44 * time.Minute),
		CompletedAt:    now.Add(-40 * time.Minute),
		UpdatedAt:      now.Add(-40 * time.Minute),
	}
	state.OutboxMessages["official-outbox"] = OutboxMessage{
		ID:             "official-outbox",
		SessionID:      "official-session",
		TurnID:         "official-turn",
		CodexThreadID:  "official-thread",
		TeamsChatID:    "official-chat",
		TeamsMessageID: "official-helper-message",
		Kind:           "final",
		Body:           "legacy answer from " + tag,
		Status:         OutboxStatusSent,
		Sequence:       9,
		ArtifactIDs:    []string{"official-artifact"},
		CreatedAt:      now.Add(-39 * time.Minute),
		UpdatedAt:      now.Add(-39 * time.Minute),
		SentAt:         now.Add(-39 * time.Minute),
	}
	state.ImportCheckpoints[transcriptCheckpointIDForSession("official-session")] = ImportCheckpoint{
		ID:             transcriptCheckpointIDForSession("official-session"),
		SessionID:      "official-session",
		SourcePath:     "/tmp/official-session.jsonl",
		LastRecordID:   "official-record",
		LastSourceLine: 77,
		LastOffset:     4096,
		ImportTurnID:   "official-import-turn",
		KindPrefix:     "assistant",
		Status:         importCheckpointStatusComplete,
		UpdatedAt:      now.Add(-38 * time.Minute),
	}
	state.TranscriptLedger["official-ledger"] = TranscriptLedgerRecord{
		ID:             "official-ledger",
		SessionID:      "official-session",
		CodexThreadID:  "official-thread",
		SourcePath:     "/tmp/official-session.jsonl",
		SourceLine:     77,
		SourceRecordID: "official-record",
		Kind:           "final",
		TeamsOriginID:  "official-helper-message",
		OutboxID:       "official-outbox",
		ImportedAt:     now.Add(-38 * time.Minute),
		CreatedAt:      now.Add(-38 * time.Minute),
		UpdatedAt:      now.Add(-38 * time.Minute),
	}
	state.TranscriptDeliveries["official-delivery"] = TranscriptDeliveryRecord{
		ID:             "official-delivery",
		SessionID:      "official-session",
		CodexThreadID:  "official-thread",
		SourcePath:     "/tmp/official-session.jsonl",
		SourceLine:     77,
		SourceRecordID: "official-record",
		Kind:           "final",
		TextHash:       "official-text-hash",
		OutboxID:       "official-outbox",
		TeamsMessageID: "official-helper-message",
		Status:         TranscriptDeliveryStatusSent,
		CreatedAt:      now.Add(-38 * time.Minute),
		UpdatedAt:      now.Add(-38 * time.Minute),
		SentAt:         now.Add(-38 * time.Minute),
	}
	state.HelperDeliveries["official-helper-delivery"] = HelperDeliveryRecord{
		ID:             "official-helper-delivery",
		SessionID:      "official-session",
		TeamsChatID:    "official-chat",
		CodexThreadID:  "official-thread",
		TurnID:         "official-turn",
		Kind:           "final",
		KindFamily:     "final",
		OutboxID:       "official-outbox",
		TeamsMessageID: "official-helper-message",
		Status:         HelperDeliveryStatusSent,
		CreatedAt:      now.Add(-39 * time.Minute),
		UpdatedAt:      now.Add(-39 * time.Minute),
		SentAt:         now.Add(-39 * time.Minute),
	}
	state.ArtifactRecords["official-artifact"] = ArtifactRecord{
		ID:             "official-artifact",
		SessionID:      "official-session",
		TurnID:         "official-turn",
		Path:           "official-report.txt",
		UploadName:     "official-report-upload.txt",
		DriveItemID:    "official-drive-item",
		OutboxID:       "official-outbox",
		TeamsMessageID: "official-helper-message",
		Status:         "uploaded",
		UploadedAt:     now.Add(-39 * time.Minute),
		SentAt:         now.Add(-39 * time.Minute),
		CreatedAt:      now.Add(-40 * time.Minute),
		UpdatedAt:      now.Add(-39 * time.Minute),
	}
	state.Notifications["official-notification"] = NotificationRecord{
		ID:        "official-notification",
		SessionID: "official-session",
		TurnID:    "official-turn",
		Kind:      "turn_completed",
		Status:    NotificationStatusSent,
		Title:     "official notification",
		CreatedAt: now.Add(-39 * time.Minute),
		UpdatedAt: now.Add(-39 * time.Minute),
		SentAt:    now.Add(-39 * time.Minute),
	}
	state.ChatSequences["official-chat"] = ChatSequenceState{
		ChatID:    "official-chat",
		Next:      10,
		UpdatedAt: now.Add(-39 * time.Minute),
	}
	state.ChatPolls["official-chat"] = ChatPollState{
		ChatID:           "official-chat",
		Seeded:           true,
		PollState:        chatPollStateParked,
		NextPollAt:       now.Add(time.Hour),
		ParkedAt:         now.Add(-30 * time.Minute),
		ParkNoticeSentAt: now.Add(-29 * time.Minute),
		UpdatedAt:        now.Add(-29 * time.Minute),
	}
	state.ChatRateLimits["official-chat"] = ChatRateLimitState{
		ChatID:       "official-chat",
		BlockedUntil: now.Add(10 * time.Minute),
		Reason:       "official-release-fixture",
	}
	if includeProvenance {
		state.MessageProvenance["official-provenance-inbound"] = MessageProvenanceRecord{
			ID:             "official-provenance-inbound",
			TeamsChatID:    "official-chat",
			TeamsMessageID: "official-user-message",
			Origin:         MessageOriginUserInbound,
			SessionID:      "official-session",
			TurnID:         "official-turn",
			InboundID:      "official-inbound",
			CreatedAt:      now.Add(-45 * time.Minute),
			UpdatedAt:      now.Add(-45 * time.Minute),
		}
		state.MessageProvenance["official-provenance-outbox"] = MessageProvenanceRecord{
			ID:             "official-provenance-outbox",
			TeamsChatID:    "official-chat",
			TeamsMessageID: "official-helper-message",
			Origin:         MessageOriginHelperOutbox,
			SessionID:      "official-session",
			TurnID:         "official-turn",
			OutboxID:       "official-outbox",
			CreatedAt:      now.Add(-39 * time.Minute),
			UpdatedAt:      now.Add(-39 * time.Minute),
		}
	}
	return state
}

func createOfficialReleaseSQLiteSchemaForTest(db *sql.DB) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS state_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS runtime_state (key TEXT PRIMARY KEY, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, teams_chat_id TEXT, status TEXT, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS sessions_chat_idx ON sessions(teams_chat_id)`,
		`CREATE TABLE IF NOT EXISTS inbound_events (id TEXT PRIMARY KEY, session_id TEXT, teams_chat_id TEXT, teams_message_id TEXT, status TEXT, created_at INTEGER, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS inbound_session_idx ON inbound_events(session_id, status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS inbound_status_idx ON inbound_events(status, teams_chat_id, created_at, teams_message_id)`,
		`CREATE INDEX IF NOT EXISTS inbound_message_idx ON inbound_events(teams_chat_id, teams_message_id)`,
		`CREATE TABLE IF NOT EXISTS turns (id TEXT PRIMARY KEY, session_id TEXT, status TEXT, queued_at INTEGER, created_at INTEGER, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS turns_ready_idx ON turns(status, session_id, queued_at, id)`,
		`CREATE INDEX IF NOT EXISTS turns_session_status_idx ON turns(session_id, status, queued_at, id)`,
		`CREATE TABLE IF NOT EXISTS outbox_messages (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, teams_chat_id TEXT, teams_message_id TEXT, status TEXT, sequence INTEGER, created_at INTEGER, deliver_after INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_messages(status, teams_chat_id, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_session_idx ON outbox_messages(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS message_provenance (id TEXT PRIMARY KEY, teams_chat_id TEXT, teams_message_id TEXT, origin TEXT, session_id TEXT, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS message_provenance_lookup_idx ON message_provenance(teams_chat_id, teams_message_id, origin)`,
		`CREATE TABLE IF NOT EXISTS chat_polls (chat_id TEXT PRIMARY KEY, next_poll_at INTEGER, poll_state TEXT, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS chat_rate_limits (chat_id TEXT PRIMARY KEY, blocked_until INTEGER, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS import_checkpoints (id TEXT PRIMARY KEY, session_id TEXT, status TEXT, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS import_checkpoints_session_idx ON import_checkpoints(session_id, status, updated_at, id)`,
		`CREATE TABLE IF NOT EXISTS transcript_ledger (id TEXT PRIMARY KEY, session_id TEXT, imported_at INTEGER, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS transcript_ledger_session_idx ON transcript_ledger(session_id, imported_at, id)`,
		`CREATE TABLE IF NOT EXISTS transcript_deliveries (id TEXT PRIMARY KEY, session_id TEXT, outbox_id TEXT, status TEXT, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS transcript_deliveries_session_idx ON transcript_deliveries(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS helper_deliveries (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, outbox_id TEXT, status TEXT, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS helper_deliveries_session_idx ON helper_deliveries(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS artifact_records (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, outbox_id TEXT, status TEXT, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS artifact_records_session_idx ON artifact_records(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS notifications (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, status TEXT, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS notifications_session_idx ON notifications(session_id, status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS notifications_status_idx ON notifications(status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_turn_idx ON outbox_messages(turn_id, status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_message_lookup_idx ON outbox_messages(teams_chat_id, teams_message_id, status)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func createOfficialReleaseLegacyOutboxSchemaForTest(db *sql.DB) error {
	for _, stmt := range []string{
		`DROP INDEX IF EXISTS outbox_pending_idx`,
		`DROP INDEX IF EXISTS outbox_session_idx`,
		`DROP INDEX IF EXISTS outbox_turn_idx`,
		`DROP INDEX IF EXISTS outbox_message_lookup_idx`,
		`DROP TABLE IF EXISTS outbox_messages`,
		`CREATE TABLE outbox_messages (id TEXT PRIMARY KEY, session_id TEXT, teams_chat_id TEXT, status TEXT, sequence INTEGER, created_at INTEGER, deliver_after INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_messages(status, teams_chat_id, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_session_idx ON outbox_messages(session_id, status, created_at, id)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func insertOfficialReleaseSQLiteStateForTest(db *sql.DB, state State) error {
	return insertOfficialReleaseSQLiteStateForTestWithOptions(db, state, officialReleaseSQLiteFixtureOptions{})
}

func insertOfficialReleaseSQLiteStateForTestWithOptions(db *sql.DB, state State, opts officialReleaseSQLiteFixtureOptions) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	cold, err := json.Marshal(coldSQLiteState(state))
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES ('state_json', ?)`, cold); err != nil {
		return err
	}
	if err := saveSQLiteRuntimeStateTx(ctx, tx, state); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO sessions(id, teams_chat_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)`, state.Sessions, func(v SessionContext) []any {
		return []any{v.ID, v.TeamsChatID, string(v.Status), sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO inbound_events(id, session_id, teams_chat_id, teams_message_id, status, created_at, updated_at, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, state.InboundEvents, func(v InboundEvent) []any {
		return []any{v.ID, v.SessionID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), string(v.Status), sqliteTime(v.CreatedAt), sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO turns(id, session_id, status, queued_at, created_at, updated_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)`, state.Turns, func(v Turn) []any {
		return []any{v.ID, v.SessionID, string(v.Status), sqliteTime(queuedTurnSortTime(v)), sqliteTime(v.CreatedAt), sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	outboxInsert := `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	outboxValues := func(v OutboxMessage) []any {
		return []any{v.ID, v.SessionID, v.TurnID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), string(v.Status), v.Sequence, sqliteTime(v.CreatedAt), int64(0)}
	}
	if opts.LegacyOutboxColumns {
		outboxInsert = `INSERT INTO outbox_messages(id, session_id, teams_chat_id, status, sequence, created_at, deliver_after, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
		outboxValues = func(v OutboxMessage) []any {
			return []any{v.ID, v.SessionID, strings.TrimSpace(v.TeamsChatID), string(v.Status), v.Sequence, sqliteTime(v.CreatedAt), int64(0)}
		}
	}
	if err := writeSQLiteMap(ctx, tx, outboxInsert, state.OutboxMessages, outboxValues); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO message_provenance(id, teams_chat_id, teams_message_id, origin, session_id, json) VALUES (?, ?, ?, ?, ?, ?)`, state.MessageProvenance, func(v MessageProvenanceRecord) []any {
		return []any{v.ID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), v.Origin, v.SessionID}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO chat_polls(chat_id, next_poll_at, poll_state, updated_at, json) VALUES (?, ?, ?, ?, ?)`, state.ChatPolls, func(v ChatPollState) []any {
		return []any{v.ChatID, sqliteTime(v.NextPollAt), v.PollState, sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO chat_rate_limits(chat_id, blocked_until, json) VALUES (?, ?, ?)`, state.ChatRateLimits, func(v ChatRateLimitState) []any {
		return []any{v.ChatID, sqliteTime(v.BlockedUntil)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)`, state.ImportCheckpoints, func(v ImportCheckpoint) []any {
		return []any{v.ID, v.SessionID, v.Status, sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO transcript_ledger(id, session_id, imported_at, created_at, json) VALUES (?, ?, ?, ?, ?)`, state.TranscriptLedger, func(v TranscriptLedgerRecord) []any {
		return []any{v.ID, v.SessionID, sqliteTime(v.ImportedAt), sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO transcript_deliveries(id, session_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?)`, state.TranscriptDeliveries, func(v TranscriptDeliveryRecord) []any {
		return []any{v.ID, v.SessionID, v.OutboxID, string(v.Status), sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO helper_deliveries(id, session_id, turn_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)`, state.HelperDeliveries, func(v HelperDeliveryRecord) []any {
		return []any{v.ID, v.SessionID, v.TurnID, v.OutboxID, string(v.Status), sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO artifact_records(id, session_id, turn_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)`, state.ArtifactRecords, func(v ArtifactRecord) []any {
		return []any{v.ID, v.SessionID, v.TurnID, v.OutboxID, v.Status, sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO notifications(id, session_id, turn_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?)`, state.Notifications, func(v NotificationRecord) []any {
		return []any{v.ID, v.SessionID, v.TurnID, string(v.Status), sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	return tx.Commit()
}

func assertOfficialReleaseNoTeamsStoreLoaded(t *testing.T, tag string, state State) {
	t.Helper()
	if len(state.Sessions) != 0 || len(state.Turns) != 0 || len(state.InboundEvents) != 0 || len(state.OutboxMessages) != 0 {
		t.Fatalf("%s should load as empty Teams state before Teams helper existed: %#v", tag, state)
	}
	if len(state.ChatPolls) != 0 || len(state.ImportCheckpoints) != 0 || len(state.TranscriptLedger) != 0 || len(state.ArtifactRecords) != 0 {
		t.Fatalf("%s should not fabricate Teams hot state before Teams helper existed: %#v", tag, state)
	}
}

func assertOfficialReleaseFixtureLoaded(t *testing.T, tag string, state State) {
	t.Helper()
	if _, ok := state.Sessions["official-session"]; !ok {
		t.Fatalf("%s upgraded state missing official session", tag)
	}
	if _, ok := state.InboundEvents["official-inbound"]; !ok {
		t.Fatalf("%s upgraded state missing official inbound", tag)
	}
	if turn, ok := state.Turns["official-turn"]; !ok || turn.Status != TurnStatusCompleted {
		t.Fatalf("%s upgraded turn = %#v ok=%v, want completed", tag, turn, ok)
	}
	if _, ok := state.OutboxMessages["official-outbox"]; !ok {
		t.Fatalf("%s upgraded state missing official outbox", tag)
	}
	if poll, ok := state.ChatPolls["official-chat"]; !ok || poll.PollState != chatPollStateParked {
		t.Fatalf("%s upgraded poll = %#v ok=%v, want parked", tag, poll, ok)
	}
	if checkpoint, ok := state.ImportCheckpoints[transcriptCheckpointIDForSession("official-session")]; !ok || checkpoint.LastRecordID != "official-record" || checkpoint.Status != importCheckpointStatusComplete {
		t.Fatalf("%s upgraded checkpoint = %#v ok=%v, want complete official checkpoint", tag, checkpoint, ok)
	}
	if ledger, ok := state.TranscriptLedger["official-ledger"]; !ok || ledger.SourceRecordID != "official-record" {
		t.Fatalf("%s upgraded transcript ledger = %#v ok=%v, want official ledger", tag, ledger, ok)
	}
	if delivery, ok := state.TranscriptDeliveries["official-delivery"]; !ok || delivery.Status != TranscriptDeliveryStatusSent || delivery.OutboxID != "official-outbox" {
		t.Fatalf("%s upgraded transcript delivery = %#v ok=%v, want sent official delivery", tag, delivery, ok)
	}
	if helper, ok := state.HelperDeliveries["official-helper-delivery"]; !ok || helper.Status != HelperDeliveryStatusSent || helper.OutboxID != "official-outbox" {
		t.Fatalf("%s upgraded helper delivery = %#v ok=%v, want sent official helper delivery", tag, helper, ok)
	}
	if artifact, ok := state.ArtifactRecords["official-artifact"]; !ok || artifact.Status != "uploaded" || artifact.OutboxID != "official-outbox" {
		t.Fatalf("%s upgraded artifact = %#v ok=%v, want uploaded official artifact", tag, artifact, ok)
	}
	if notification, ok := state.Notifications["official-notification"]; !ok || notification.Status != NotificationStatusSent {
		t.Fatalf("%s upgraded notification = %#v ok=%v, want sent official notification", tag, notification, ok)
	}
}

func assertOfficialReleaseSQLiteSchemaForTest(t *testing.T, store *Store, tag string) {
	t.Helper()
	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read %s sqlite pointer: %v", tag, err)
	}
	pointer, ok, err := storeSQLitePointerFromData(data)
	if err != nil || !ok {
		t.Fatalf("%s store did not upgrade to sqlite pointer: ok=%v err=%v data=%s", tag, ok, err, string(data))
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve %s sqlite path: %v", tag, err)
	}
	db, err := openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("open %s upgraded sqlite: %v", tag, err)
	}
	defer db.Close()
	columns := sqliteColumnSetForTest(t, db, "chat_polls")
	for _, column := range []string{"last_activity_at", "park_notice_sent_at", "parked_skip_eligible"} {
		if !columns[column] {
			t.Fatalf("%s upgraded chat_polls missing column %q; columns=%v", tag, column, columns)
		}
	}
	inboundColumns := sqliteColumnSetForTest(t, db, "inbound_events")
	if !inboundColumns["received_at"] {
		t.Fatalf("%s upgraded inbound_events missing column received_at; columns=%v", tag, inboundColumns)
	}
	indexes := sqliteIndexSetForTest(t, db)
	for _, index := range []string{"inbound_session_received_idx", "chat_polls_parked_skip_idx", "chat_polls_auto_park_idx", "outbox_chat_sequence_idx", "transcript_deliveries_outbox_idx", "helper_deliveries_outbox_idx", "artifact_records_outbox_idx"} {
		if !indexes[index] {
			t.Fatalf("%s upgraded sqlite missing index %q; indexes=%v", tag, index, indexes)
		}
	}
	sequenceColumns := sqliteColumnSetForTest(t, db, "chat_sequences")
	for _, column := range []string{"chat_id", "next_sequence", "updated_at", "json"} {
		if !sequenceColumns[column] {
			t.Fatalf("%s upgraded chat_sequences missing column %q; columns=%v", tag, column, sequenceColumns)
		}
	}
	var parkedSkip sql.NullInt64
	if err := db.QueryRow(`SELECT parked_skip_eligible FROM chat_polls WHERE chat_id = ?`, "official-chat").Scan(&parkedSkip); err != nil {
		t.Fatalf("query %s upgraded parked_skip_eligible: %v", tag, err)
	}
	if !parkedSkip.Valid || parkedSkip.Int64 != 1 {
		t.Fatalf("%s upgraded official-chat parked_skip_eligible = %#v, want true", tag, parkedSkip)
	}
}

func sqliteColumnSetForTest(t *testing.T, db *sql.DB, table string) map[string]bool {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatalf("pragma table_info(%s): %v", table, err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan table_info(%s): %v", table, err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info(%s): %v", table, err)
	}
	return columns
}

func sqliteIndexSetForTest(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'index'`)
	if err != nil {
		t.Fatalf("query sqlite indexes: %v", err)
	}
	defer rows.Close()
	indexes := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan sqlite index: %v", err)
		}
		indexes[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite indexes: %v", err)
	}
	return indexes
}

func assertOfficialReleaseHotPathsForTest(t *testing.T, store *Store, tag string) {
	t.Helper()
	ctx := context.Background()
	if lookup, err := store.MessageLookup(ctx, "official-chat", "official-user-message"); err != nil || !lookup.HasInbound {
		t.Fatalf("%s upgraded inbound lookup = %#v err=%v, want inbound", tag, lookup, err)
	}
	if lookup, err := store.MessageLookup(ctx, "official-chat", "official-helper-message"); err != nil || !lookup.HasDeliveredOutbox {
		t.Fatalf("%s upgraded outbox lookup = %#v err=%v, want delivered outbox", tag, lookup, err)
	}
	now := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)
	inbound := InboundEvent{
		ID:             "post-upgrade-inbound",
		SessionID:      "official-session",
		TeamsChatID:    "official-chat",
		TeamsMessageID: "post-upgrade-user-message",
		Source:         "teams",
		Status:         InboundStatusQueued,
		Text:           "post-upgrade prompt",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
		t.Fatalf("%s PersistInbound after upgrade created=%v err=%v", tag, created, err)
	}
	turn, created, err := store.QueueTurn(ctx, Turn{SessionID: "official-session", InboundEventID: inbound.ID, CreatedAt: now, UpdatedAt: now})
	if err != nil || !created {
		t.Fatalf("%s QueueTurn after upgrade created=%v err=%v", tag, created, err)
	}
	claimed, ok, err := store.ClaimNextQueuedTurn(ctx, "official-session")
	if err != nil || !ok || claimed.ID != turn.ID {
		t.Fatalf("%s ClaimNextQueuedTurn after upgrade = %#v ok=%v err=%v, want %q", tag, claimed, ok, err, turn.ID)
	}
	outbox, created, err := store.QueueOutbox(ctx, OutboxMessage{
		SessionID:   "official-session",
		TurnID:      turn.ID,
		TeamsChatID: "official-chat",
		Kind:        "final",
		Body:        "post-upgrade answer",
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	if err != nil || !created {
		t.Fatalf("%s QueueOutbox after upgrade created=%v err=%v", tag, created, err)
	}
	if outbox.Sequence != 10 {
		t.Fatalf("%s QueueOutbox after upgrade sequence = %d, want 10", tag, outbox.Sequence)
	}
	if _, err := store.MarkOutboxAccepted(ctx, outbox.ID, "post-upgrade-helper-message"); err != nil {
		t.Fatalf("%s MarkOutboxAccepted after upgrade: %v", tag, err)
	}
	if _, err := store.MarkOutboxSent(ctx, outbox.ID, "post-upgrade-helper-message"); err != nil {
		t.Fatalf("%s MarkOutboxSent after upgrade: %v", tag, err)
	}
	if lookup, err := store.MessageLookup(ctx, "official-chat", "post-upgrade-helper-message"); err != nil || !lookup.HasDeliveredOutbox {
		t.Fatalf("%s post-upgrade outbox lookup = %#v err=%v, want delivered outbox", tag, lookup, err)
	}
	if _, err := store.RecordChatPollSuccess(ctx, "official-chat", now, true, false, 1); err != nil {
		t.Fatalf("%s RecordChatPollSuccess after upgrade: %v", tag, err)
	}
	if session, changed, err := store.UpdateSessionContext(ctx, "official-session", func(current SessionContext, found bool, updateNow time.Time) (SessionContext, bool, error) {
		if !found {
			return current, false, fmt.Errorf("official-session missing")
		}
		current.TeamsTopic = "post-upgrade session topic"
		current.UpdatedAt = updateNow
		return current, true, nil
	}); err != nil || !changed || session.TeamsTopic != "post-upgrade session topic" {
		t.Fatalf("%s UpdateSessionContext after upgrade = %#v changed=%v err=%v", tag, session, changed, err)
	}
	if updatedInbound, changed, err := store.UpdateInboundEvent(ctx, inbound.ID, func(current InboundEvent, found bool, updateNow time.Time) (InboundEvent, bool, error) {
		if !found {
			return current, false, fmt.Errorf("%s missing", inbound.ID)
		}
		current.Source = "post-upgrade-row-update"
		current.UpdatedAt = updateNow
		return current, true, nil
	}); err != nil || !changed || updatedInbound.Source != "post-upgrade-row-update" {
		t.Fatalf("%s UpdateInboundEvent after upgrade = %#v changed=%v err=%v", tag, updatedInbound, changed, err)
	}
	if err := store.UpdateDashboardRecords(ctx, func(records *DashboardStoreRecords, updateNow time.Time) (bool, error) {
		records.Views["official-control-chat"] = DashboardViewRecord{ID: "dashboard:official-control-chat", ChatID: "official-control-chat", Kind: "sessions", CreatedAt: updateNow, UpdatedAt: updateNow}
		records.Numbers["official-dashboard-number"] = DashboardNumberRecord{ID: "official-dashboard-number", ChatID: "official-control-chat", Kind: "session", Number: 1, SessionID: "official-session", UpdatedAt: updateNow}
		records.Workspaces["official-workspace"] = WorkspaceRecord{ID: "official-workspace", Path: "/workspace/official", Number: 1, UpdatedAt: updateNow}
		return true, nil
	}); err != nil {
		t.Fatalf("%s UpdateDashboardRecords after upgrade: %v", tag, err)
	}
	if _, err := store.UpdateWorkflowConfig(ctx, func(current WorkflowNotificationConfig, _ ControlChatBinding, updateNow time.Time) (WorkflowNotificationConfig, bool, error) {
		current.Enabled = true
		current.ControlChatID = "official-control-chat"
		current.ControlWebhookURLFile = "/tmp/official-workflow-webhook"
		current.UpdatedAt = updateNow
		return current, true, nil
	}); err != nil {
		t.Fatalf("%s UpdateWorkflowConfig after upgrade: %v", tag, err)
	}
	if changed, err := store.RecordControlChatBinding(ctx, ControlChatBindingUpdate{
		ScopeID:              "official-scope",
		AccountID:            "official-account",
		UserPrincipal:        "official@example.test",
		Profile:              "default",
		MachineID:            "official-machine",
		MachineLabel:         "official-machine-label",
		MachineHostname:      "official-machine-label",
		MachineKind:          MachineKindPrimary,
		MachinePriority:      11,
		TeamsChatID:          "official-control-chat",
		TeamsChatURL:         "https://teams.example/official-control",
		TeamsChatTopic:       "official control",
		UserTitle:            "Official control",
		TitleSource:          "user",
		UpdateTitleIfPresent: true,
	}); err != nil || !changed {
		t.Fatalf("%s RecordControlChatBinding after upgrade changed=%v err=%v", tag, changed, err)
	}
	if err := store.UpsertModelProfileKeyIntake(ctx, ModelProfileKeyIntake{ID: "post-upgrade-model-key", TeamsChatID: "official-control-chat", AuthorUserID: "official-user", ProfileName: "openai", Status: ModelProfileKeyIntakePending, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("%s UpsertModelProfileKeyIntake after upgrade: %v", tag, err)
	}
	if err := store.UpdateModelProfileKeyIntakes(ctx, func(intakes map[string]ModelProfileKeyIntake, updateNow time.Time) (bool, error) {
		intake := intakes["post-upgrade-model-key"]
		intake.Status = ModelProfileKeyIntakeConfirmed
		intake.UpdatedAt = updateNow
		intakes[intake.ID] = intake
		return true, nil
	}); err != nil {
		t.Fatalf("%s UpdateModelProfileKeyIntakes after upgrade: %v", tag, err)
	}
	if _, _, err := store.HotPollScheduleSnapshot(ctx); err != nil {
		t.Fatalf("%s HotPollScheduleSnapshot after upgrade: %v", tag, err)
	}
	if err := store.RecordTranscriptCheckpoint(ctx, ImportCheckpoint{
		ID:             transcriptCheckpointIDForSession("official-session"),
		SessionID:      "official-session",
		SourcePath:     "/tmp/official-session.jsonl",
		LastRecordID:   "post-upgrade-record",
		LastSourceLine: 101,
		LastOffset:     8192,
		Status:         importCheckpointStatusBlocked,
	}, TranscriptLedgerRecord{
		ID:             "post-upgrade-ledger",
		SessionID:      "official-session",
		CodexThreadID:  "official-thread",
		SourcePath:     "/tmp/official-session.jsonl",
		SourceLine:     101,
		SourceRecordID: "post-upgrade-record",
		Kind:           "final",
	}); err != nil {
		t.Fatalf("%s RecordTranscriptCheckpoint after upgrade: %v", tag, err)
	}
	if delivery, created, err := store.RecordTranscriptDelivery(ctx, TranscriptDeliveryRecord{
		ID:             "post-upgrade-skipped-delivery",
		SessionID:      "official-session",
		CodexThreadID:  "official-thread",
		SourcePath:     "/tmp/official-session.jsonl",
		SourceLine:     102,
		SourceRecordID: "post-upgrade-skipped",
		Kind:           "status-progress",
		TextHash:       "post-upgrade-skipped-hash",
		Status:         TranscriptDeliveryStatusSkipped,
	}, ImportCheckpoint{
		ID:             transcriptCheckpointIDForSession("official-session"),
		SessionID:      "official-session",
		SourcePath:     "/tmp/official-session.jsonl",
		LastRecordID:   "post-upgrade-skipped",
		LastSourceLine: 102,
		LastOffset:     9000,
	}); err != nil || !created || delivery.Status != TranscriptDeliveryStatusSkipped {
		t.Fatalf("%s RecordTranscriptDelivery after upgrade = %#v created=%v err=%v, want skipped created", tag, delivery, created, err)
	}
	transcriptOutbox, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
		Message: OutboxMessage{
			ID:          "post-upgrade-transcript-outbox",
			SessionID:   "official-session",
			TurnID:      turn.ID,
			TeamsChatID: "official-chat",
			Kind:        "status-progress",
			Body:        "post-upgrade transcript delivery",
		},
		Delivery: TranscriptDeliveryRecord{
			ID:             "post-upgrade-queued-delivery",
			SessionID:      "official-session",
			CodexThreadID:  "official-thread",
			SourcePath:     "/tmp/official-session.jsonl",
			SourceLine:     103,
			SourceRecordID: "post-upgrade-queued",
			Kind:           "status-progress",
			TextHash:       "post-upgrade-queued-hash",
			Status:         TranscriptDeliveryStatusQueued,
		},
		Checkpoint: ImportCheckpoint{
			ID:             transcriptCheckpointIDForSession("official-session"),
			SessionID:      "official-session",
			SourcePath:     "/tmp/official-session.jsonl",
			LastRecordID:   "post-upgrade-queued",
			LastSourceLine: 103,
			LastOffset:     10000,
		},
	})
	if err != nil || !created || alreadyDelivered || transcriptOutbox.ID == "" {
		t.Fatalf("%s QueueTranscriptDeliveryOutbox after upgrade out=%#v created=%v alreadyDelivered=%v err=%v", tag, transcriptOutbox, created, alreadyDelivered, err)
	}
	if err := store.RecordTranscriptCheckpoint(ctx, ImportCheckpoint{
		ID:             transcriptCheckpointIDForSession("official-session"),
		SessionID:      "official-session",
		SourcePath:     "/tmp/official-session.jsonl",
		LastRecordID:   "post-upgrade-queued",
		LastSourceLine: 103,
		LastOffset:     10000,
	}, TranscriptLedgerRecord{
		ID:             "post-upgrade-queued-ledger",
		SessionID:      "official-session",
		CodexThreadID:  "official-thread",
		SourcePath:     "/tmp/official-session.jsonl",
		SourceLine:     103,
		SourceRecordID: "post-upgrade-queued",
		Kind:           "status-progress",
		OutboxID:       transcriptOutbox.ID,
	}); err != nil {
		t.Fatalf("%s RecordTranscriptCheckpoint queued after upgrade: %v", tag, err)
	}
	if _, err := store.UpsertArtifactRecord(ctx, ArtifactRecord{
		ID:         "post-upgrade-artifact",
		SessionID:  "official-session",
		TurnID:     turn.ID,
		Path:       "post-upgrade-artifact.txt",
		UploadName: "post-upgrade-artifact-upload.txt",
		Status:     "queued",
	}); err != nil {
		t.Fatalf("%s UpsertArtifactRecord after upgrade: %v", tag, err)
	}
	artifactOutbox, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:                   "post-upgrade-artifact-outbox",
		SessionID:            "official-session",
		TurnID:               turn.ID,
		TeamsChatID:          "official-chat",
		Kind:                 "artifact",
		Body:                 "post-upgrade artifact",
		AttachmentName:       "outbox-fallback-name.txt",
		AttachmentUploadName: "outbox-fallback-upload.txt",
		ArtifactIDs:          []string{"post-upgrade-artifact"},
	})
	if err != nil || !created {
		t.Fatalf("%s QueueOutbox artifact after upgrade created=%v err=%v", tag, created, err)
	}
	if _, err := store.MarkOutboxDriveItem(ctx, artifactOutbox.ID, "post-upgrade-drive-item", "artifact.txt", "etag", "https://sharepoint.example/artifact", "dav://artifact"); err != nil {
		t.Fatalf("%s MarkOutboxDriveItem after upgrade: %v", tag, err)
	}
	if _, err := store.MarkOutboxSendError(ctx, artifactOutbox.ID, "post-upgrade send retry"); err != nil {
		t.Fatalf("%s MarkOutboxSendError after upgrade: %v", tag, err)
	}
	finalState, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("%s Load after post-upgrade hot paths: %v", tag, err)
	}
	if checkpoint := finalState.ImportCheckpoints[transcriptCheckpointIDForSession("official-session")]; checkpoint.LastRecordID != "post-upgrade-queued" || checkpoint.Status != importCheckpointStatusComplete {
		t.Fatalf("%s final checkpoint = %#v, want queued record complete", tag, checkpoint)
	}
	if _, ok := finalState.TranscriptLedger["post-upgrade-ledger"]; !ok {
		t.Fatalf("%s post-upgrade ledger missing: %#v", tag, finalState.TranscriptLedger)
	}
	if delivery := finalState.TranscriptDeliveries["post-upgrade-queued-delivery"]; delivery.OutboxID != transcriptOutbox.ID || delivery.Status != TranscriptDeliveryStatusQueued {
		t.Fatalf("%s queued transcript delivery = %#v", tag, delivery)
	}
	artifact := finalState.ArtifactRecords["post-upgrade-artifact"]
	if artifact.OutboxID != artifactOutbox.ID || artifact.DriveItemID != "post-upgrade-drive-item" || artifact.Status != "message_failed" || artifact.Path != "post-upgrade-artifact.txt" || artifact.UploadName != "post-upgrade-artifact-upload.txt" {
		t.Fatalf("%s post-upgrade artifact side effects = %#v", tag, artifact)
	}
	if finalState.Sessions["official-session"].TeamsTopic != "post-upgrade session topic" {
		t.Fatalf("%s final session topic = %q", tag, finalState.Sessions["official-session"].TeamsTopic)
	}
	if finalState.InboundEvents[inbound.ID].Source != "post-upgrade-row-update" {
		t.Fatalf("%s final inbound source = %q", tag, finalState.InboundEvents[inbound.ID].Source)
	}
	if _, ok := finalState.DashboardViews["official-control-chat"]; !ok {
		t.Fatalf("%s post-upgrade dashboard view missing: %#v", tag, finalState.DashboardViews)
	}
	if !finalState.Workflow.Enabled || finalState.Workflow.ControlChatID != "official-control-chat" {
		t.Fatalf("%s post-upgrade workflow = %#v", tag, finalState.Workflow)
	}
	if finalState.MachineIdentity.ID != "official-machine" || finalState.ControlChat.TeamsChatID != "official-control-chat" || finalState.ControlChat.UserTitle != "Official control" {
		t.Fatalf("%s post-upgrade control binding machine=%#v control=%#v", tag, finalState.MachineIdentity, finalState.ControlChat)
	}
	if got := finalState.ModelProfileKeyIntakes["post-upgrade-model-key"].Status; got != ModelProfileKeyIntakeConfirmed {
		t.Fatalf("%s post-upgrade model key status = %q", tag, got)
	}
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

func TestSQLitePointerCacheRefreshesWhenPointerFileChanges(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: "control-chat"}
		return nil
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	result := migrateStoreToSQLiteForTest(t, store)
	if _, err := store.Load(ctx); err != nil {
		t.Fatalf("initial cached sqlite load: %v", err)
	}
	if !store.sqlitePointerCached {
		t.Fatal("sqlite pointer was not cached after migration/load")
	}
	if result.Path == "" {
		t.Fatal("migration did not return sqlite path")
	}

	writeRawStoreStateForTest(t, store, []byte(`{"storage_backend":`+"\n"))
	err := store.withStateLock(ctx, func() error {
		_, ok, err := store.currentSQLitePointerUnlocked()
		if ok {
			t.Fatal("currentSQLitePointerUnlocked ok = true after pointer file changed to invalid JSON")
		}
		if err != nil {
			t.Fatalf("currentSQLitePointerUnlocked error = %v, want invalid JSON treated as non-pointer", err)
		}
		if store.sqlitePointerCached {
			t.Fatal("sqlite pointer cache was not cleared after pointer file changed")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withStateLock: %v", err)
	}
}

func TestSQLitePointerCacheRevalidatesAfterLoadBeforeHotPath(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)
	if _, err := store.Load(ctx); err != nil {
		t.Fatalf("initial sqlite load: %v", err)
	}
	if !store.sqlitePointerCached {
		t.Fatal("sqlite pointer was not cached after load")
	}
	if store.sqlitePointerTrusted {
		t.Fatal("sqlite pointer cache should require hot-path revalidation after load")
	}

	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read sqlite pointer: %v", err)
	}
	var pointer map[string]any
	if err := json.Unmarshal(data, &pointer); err != nil {
		t.Fatalf("unmarshal sqlite pointer: %v", err)
	}
	unsupportedSchemaVersion := storeSQLitePointerSchemaVersion + 1
	pointer["schema_version"] = unsupportedSchemaVersion
	data, err = json.Marshal(pointer)
	if err != nil {
		t.Fatalf("marshal sqlite pointer: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatalf("write unsupported sqlite pointer: %v", err)
	}

	err = store.withStateLock(ctx, func() error {
		_, ok, err := store.currentSQLitePointerUnlocked()
		if ok {
			t.Fatal("currentSQLitePointerUnlocked ok = true after same-size unsupported pointer rewrite")
		}
		if !errors.Is(err, ErrUnsupportedSchemaVersion) || !strings.Contains(err.Error(), fmt.Sprint(unsupportedSchemaVersion)) {
			t.Fatalf("currentSQLitePointerUnlocked error = %v, want unsupported schema %d", err, unsupportedSchemaVersion)
		}
		if store.sqlitePointerCached {
			t.Fatal("sqlite pointer cache was not cleared after unsupported pointer rewrite")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withStateLock: %v", err)
	}
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

func sqliteRawStateJSONForTest(t *testing.T, store *Store) []byte {
	t.Helper()
	ctx := context.Background()
	var raw []byte
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&raw)
	}); err != nil {
		t.Fatalf("read sqlite state_json: %v", err)
	}
	return append([]byte(nil), raw...)
}

func withSQLiteTxForTest(t *testing.T, store *Store, fn func(*sql.Tx) error) {
	t.Helper()
	ctx := context.Background()
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit()
	}); err != nil {
		t.Fatalf("with sqlite tx: %v", err)
	}
}

func sqliteTableRowCountForTest(t *testing.T, store *Store, table string) int {
	t.Helper()
	ctx := context.Background()
	var count int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, query).Scan(&count)
	}); err != nil {
		t.Fatalf("count sqlite table %s: %v", table, err)
	}
	return count
}

func sqliteRowExistsForTest(t *testing.T, store *Store, table string, id string) bool {
	t.Helper()
	ctx := context.Background()
	var count int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE id = ?`, table)
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, query, id).Scan(&count)
	}); err != nil {
		t.Fatalf("query sqlite row %s.%s: %v", table, id, err)
	}
	return count > 0
}

func sqliteRawChatPollJSONForTest(t *testing.T, store *Store, chatID string) []byte {
	t.Helper()
	ctx := context.Background()
	var raw []byte
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID).Scan(&raw)
	}); err != nil {
		t.Fatalf("read sqlite chat poll %q: %v", chatID, err)
	}
	return append([]byte(nil), raw...)
}

func sqliteWriteRawChatPollJSONForTest(t *testing.T, store *Store, chatID string, raw []byte) {
	t.Helper()
	ctx := context.Background()
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		result, err := db.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, raw, chatID)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil {
			return err
		} else if rows != 1 {
			return fmt.Errorf("updated %d chat_polls rows, want 1", rows)
		}
		return nil
	}); err != nil {
		t.Fatalf("write sqlite chat poll %q: %v", chatID, err)
	}
}

func sqliteWriteRawStateJSONForTest(t *testing.T, store *Store, raw []byte) {
	t.Helper()
	ctx := context.Background()
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		result, err := db.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, raw)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil {
			return err
		} else if rows != 1 {
			return fmt.Errorf("updated %d state_meta rows, want 1", rows)
		}
		return nil
	}); err != nil {
		t.Fatalf("write sqlite state_json: %v", err)
	}
}

func sqliteRawOutboxJSONForTest(t *testing.T, store *Store, outboxID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM outbox_messages WHERE id = ?`, outboxID, "outbox "+outboxID)
}

func sqliteRawSessionJSONForTest(t *testing.T, store *Store, sessionID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM sessions WHERE id = ?`, sessionID, "session "+sessionID)
}

func sqliteRawInboundJSONForTest(t *testing.T, store *Store, inboundID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM inbound_events WHERE id = ?`, inboundID, "inbound "+inboundID)
}

func sqliteRawTurnJSONForTest(t *testing.T, store *Store, turnID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM turns WHERE id = ?`, turnID, "turn "+turnID)
}

func sqliteRawHelperDeliveryByOutboxForTest(t *testing.T, store *Store, outboxID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM helper_deliveries WHERE outbox_id = ?`, outboxID, "helper delivery for outbox "+outboxID)
}

func helperDeliveryForOutboxForTest(state State, outboxID string) HelperDeliveryRecord {
	for _, delivery := range state.HelperDeliveries {
		if delivery.OutboxID == outboxID {
			return delivery
		}
	}
	return HelperDeliveryRecord{}
}

func sqliteRawArtifactRecordForTest(t *testing.T, store *Store, artifactID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM artifact_records WHERE id = ?`, artifactID, "artifact "+artifactID)
}

func sqliteRawTranscriptDeliveryJSONForTest(t *testing.T, store *Store, deliveryID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM transcript_deliveries WHERE id = ?`, deliveryID, "transcript delivery "+deliveryID)
}

func sqliteRawImportCheckpointJSONForTest(t *testing.T, store *Store, checkpointID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM import_checkpoints WHERE id = ?`, checkpointID, "import checkpoint "+checkpointID)
}

func sqliteRawChatSequenceJSONForTest(t *testing.T, store *Store, chatID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM chat_sequences WHERE chat_id = ?`, chatID, "chat sequence "+chatID)
}

func sqliteRawTranscriptLedgerJSONForTest(t *testing.T, store *Store, ledgerID string) []byte {
	t.Helper()
	return sqliteRawJSONByKeyForTest(t, store, `SELECT json FROM transcript_ledger WHERE id = ?`, ledgerID, "transcript ledger "+ledgerID)
}

func sqliteRawJSONByKeyForTest(t *testing.T, store *Store, query string, arg string, label string) []byte {
	t.Helper()
	ctx := context.Background()
	var raw []byte
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, query, arg).Scan(&raw)
	}); err != nil {
		t.Fatalf("read sqlite %s: %v", label, err)
	}
	return append([]byte(nil), raw...)
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
		a.DrainOperationID == b.DrainOperationID &&
		a.LastDrainOperationID == b.LastDrainOperationID &&
		a.LastDrainOperationAt.Equal(b.LastDrainOperationAt) &&
		a.UpdatedAt.Equal(b.UpdatedAt)
}

func (owner OwnerMetadata) withLastHeartbeat(lastHeartbeat time.Time) OwnerMetadata {
	owner.LastHeartbeat = lastHeartbeat
	return owner
}
