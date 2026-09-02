package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestJSONMalformedCanonicalCheckpointIsIsolatedAtStartup(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	badSession := testSession()
	badSession.ID = "json-malformed-checkpoint-session"
	badSession.TeamsChatID = "json-malformed-checkpoint-chat"
	goodSession := testSession()
	goodSession.ID = "json-healthy-checkpoint-session"
	goodSession.TeamsChatID = "json-healthy-checkpoint-chat"
	for _, session := range []SessionContext{badSession, goodSession} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	badID := sessionTranscriptCheckpointID(badSession.ID)
	goodID := sessionTranscriptCheckpointID(goodSession.ID)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[badID] = ImportCheckpoint{
			ID: badID, SessionID: badSession.ID, LastRecordID: "bad-record",
			LastOffset: 128, LastOffsetKnown: true, Status: importCheckpointStatusComplete,
		}
		state.ImportCheckpoints[goodID] = ImportCheckpoint{
			ID: goodID, SessionID: goodSession.ID, LastRecordID: "healthy-record",
			LastOffset: 256, LastOffsetKnown: true, Status: importCheckpointStatusComplete,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoints: %v", err)
	}

	rawState, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read JSON state: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rawState, &root); err != nil {
		t.Fatalf("decode JSON state: %v", err)
	}
	var rows map[string]json.RawMessage
	if err := json.Unmarshal(root["import_checkpoints"], &rows); err != nil {
		t.Fatalf("decode checkpoint map: %v", err)
	}
	var badFields map[string]json.RawMessage
	if err := json.Unmarshal(rows[badID], &badFields); err != nil {
		t.Fatalf("decode bad checkpoint fields: %v", err)
	}
	badFields["last_offset_known"] = json.RawMessage("1")
	rows[badID], err = json.Marshal(badFields)
	if err != nil {
		t.Fatalf("encode malformed checkpoint: %v", err)
	}
	root["import_checkpoints"], err = json.Marshal(rows)
	if err != nil {
		t.Fatalf("encode checkpoint map: %v", err)
	}
	corrupted, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode corrupted state: %v", err)
	}
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before malformed-state reopen: %v", err)
	}
	writeRawStoreStateForTest(t, store, corrupted)
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen malformed JSON store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	loaded, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load malformed JSON store: %v", err)
	}
	bad := loaded.ImportCheckpoints[badID]
	if bad.ID != badID || bad.SessionID != badSession.ID || bad.TranscriptQuarantine == nil || bad.TranscriptQuarantine.Kind != malformedCanonicalCheckpointKind {
		t.Fatalf("malformed JSON checkpoint = %#v, want isolated canonical fence", bad)
	}
	if bad.LastRecordID != "" || bad.LastOffset != 0 || bad.LastOffsetKnown || bad.SourcePath != "" || bad.SourceFingerprint != "" || bad.UnresolvedExecution == nil || !bad.LegacySourceUnverified || !bad.RecoveryProofUnusable {
		t.Fatalf("malformed JSON fallback retained untrusted data: %#v", bad)
	}
	if got := loaded.ImportCheckpoints[goodID].LastRecordID; got != "healthy-record" {
		t.Fatalf("healthy JSON checkpoint = %q, want intact row", got)
	}
	if _, ok := loaded.Sessions[badSession.ID]; !ok {
		t.Fatalf("malformed JSON load lost bad session")
	}
	if _, ok := loaded.Sessions[goodSession.ID]; !ok {
		t.Fatalf("malformed JSON load lost healthy session")
	}
}

func TestJSONMalformedCanonicalCheckpointRawSurvivesUnrelatedSave(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "json-opaque-preservation-session"
	session.TeamsChatID = "json-opaque-preservation-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	checkpointID := sessionTranscriptCheckpointID(session.ID)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
			ID: checkpointID, SessionID: session.ID, LastRecordID: "must-survive-raw",
			LastOffset: 128, LastOffsetKnown: true, Status: importCheckpointStatusComplete,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read JSON state: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state: %v", err)
	}
	var rows map[string]json.RawMessage
	if err := json.Unmarshal(root["import_checkpoints"], &rows); err != nil {
		t.Fatalf("decode checkpoint map: %v", err)
	}
	malformed := json.RawMessage(`{"id":"` + checkpointID + `","session_id":"` + session.ID + `","last_offset":128,"last_offset_known":1,"opaque_marker":"retain-me"}`)
	rows[checkpointID] = malformed
	root["import_checkpoints"], err = json.Marshal(rows)
	if err != nil {
		t.Fatalf("encode checkpoint map: %v", err)
	}
	corrupted, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode corrupted state: %v", err)
	}
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before malformed-state reopen: %v", err)
	}
	writeRawStoreStateForTest(t, store, corrupted)
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen malformed JSON store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Load(ctx); err != nil {
		t.Fatalf("load malformed JSON store: %v", err)
	}

	rawBefore := jsonCheckpointRawForTest(t, path, checkpointID)
	if string(rawBefore) != string(malformed) {
		t.Fatalf("malformed raw checkpoint before save = %q, want %q", rawBefore, malformed)
	}
	if err := reopened.Update(ctx, func(state *State) error {
		session.UserTitle = "unrelated-json-update"
		state.Sessions[session.ID] = session
		return nil
	}); err != nil {
		t.Fatalf("unrelated JSON update: %v", err)
	}
	rawAfter := jsonCheckpointRawForTest(t, path, checkpointID)
	if string(rawAfter) != string(rawBefore) {
		t.Fatalf("opaque JSON checkpoint changed across unrelated update: before=%q after=%q", rawBefore, rawAfter)
	}

	replacement := ImportCheckpoint{
		ID: checkpointID, SessionID: session.ID, Status: importCheckpointStatusComplete,
		SourcePath: "/tmp/json-opaque-repaired.jsonl", SourceGeneration: "generation-1",
		SourceFingerprint: "fingerprint-1", LastRecordID: "repaired-record",
		LastSourceLine: 4, LastOffset: 40, LastOffsetKnown: true, SourceSize: 40,
	}
	if _, changed, err := reopened.UpdateImportCheckpoint(ctx, checkpointID, func(current ImportCheckpoint, found bool, now time.Time) (ImportCheckpoint, bool, error) {
		if !found || current.TranscriptQuarantine == nil {
			t.Fatalf("opaque update current=%#v found=%v, want fallback", current, found)
		}
		replacement.UpdatedAt = now
		return replacement, true, nil
	}); err != nil || !changed {
		t.Fatalf("explicit JSON checkpoint replacement changed=%v err=%v", changed, err)
	}
	if got := jsonCheckpointRawForTest(t, path, checkpointID); string(got) == string(malformed) {
		t.Fatalf("explicit JSON checkpoint replacement retained opaque raw=%q", got)
	}
}

func TestJSONMalformedChatPollIsIsolatedAtStartupAndPreserved(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	good := testSession()
	good.ID = "json-healthy-poll-session"
	good.TeamsChatID = "json-healthy-poll-chat"
	bad := testSession()
	bad.ID = "json-malformed-poll-session"
	bad.TeamsChatID = "json-malformed-poll-chat"
	for _, session := range []SessionContext{good, bad} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[good.TeamsChatID] = ChatPollState{ChatID: good.TeamsChatID, Seeded: true, PollState: chatPollStateHot}
		state.ChatPolls[bad.TeamsChatID] = ChatPollState{ChatID: bad.TeamsChatID, Seeded: true, PollState: chatPollStateHot}
		return nil
	}); err != nil {
		t.Fatalf("seed chat polls: %v", err)
	}

	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read JSON state: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state: %v", err)
	}
	var rows map[string]json.RawMessage
	if err := json.Unmarshal(root["chat_polls"], &rows); err != nil {
		t.Fatalf("decode chat poll map: %v", err)
	}
	malformed := json.RawMessage(`{"chat_id":"` + bad.TeamsChatID + `","state":123,"opaque_marker":"retain-me"}`)
	rows[bad.TeamsChatID] = malformed
	root["chat_polls"], err = json.Marshal(rows)
	if err != nil {
		t.Fatalf("encode chat poll map: %v", err)
	}
	corrupted, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode corrupted state: %v", err)
	}
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before malformed poll reopen: %v", err)
	}
	writeRawStoreStateForTest(t, store, corrupted)
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen malformed poll store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load malformed poll store: %v", err)
	}
	if _, ok := loaded.ChatPolls[good.TeamsChatID]; !ok {
		t.Fatal("healthy chat poll was lost while isolating malformed poll")
	}
	badPoll, ok := loaded.ChatPolls[bad.TeamsChatID]
	if !ok || !badPoll.RecoveryRequired || badPoll.ChatID != bad.TeamsChatID || badPoll.RecoverySourceHash == "" {
		t.Fatalf("malformed chat poll recovery placeholder = %#v found=%v, want chat-local recovery marker", badPoll, ok)
	}
	if _, err := reopened.SetPaused(ctx, true, "unrelated JSON update"); err != nil {
		t.Fatalf("unrelated JSON update: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read JSON state after update: %v", err)
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state after update: %v", err)
	}
	if err := json.Unmarshal(root["chat_polls"], &rows); err != nil {
		t.Fatalf("decode chat poll map after update: %v", err)
	}
	if string(rows[bad.TeamsChatID]) != string(malformed) {
		t.Fatalf("malformed chat poll raw changed across unrelated update: got=%q want=%q", rows[bad.TeamsChatID], malformed)
	}
}

func TestJSONMalformedSessionAndActiveTurnAreIsolatedAtFullLoad(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	good := testSession()
	good.ID = "json-healthy-session-row"
	good.TeamsChatID = "json-healthy-session-chat"
	bad := testSession()
	bad.ID = "json-malformed-session-row"
	bad.TeamsChatID = "json-malformed-session-chat"
	for _, session := range []SessionContext{good, bad} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	turnID := "json-malformed-active-turn"
	if err := store.Update(ctx, func(state *State) error {
		now := time.Now()
		state.Turns[turnID] = Turn{
			ID: turnID, SessionID: bad.ID, Status: TurnStatusQueued,
			QueuedAt: now, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed active turn: %v", err)
	}

	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read JSON state: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state: %v", err)
	}
	var sessions map[string]json.RawMessage
	if err := json.Unmarshal(root["sessions"], &sessions); err != nil {
		t.Fatalf("decode JSON sessions: %v", err)
	}
	var turns map[string]json.RawMessage
	if err := json.Unmarshal(root["turns"], &turns); err != nil {
		t.Fatalf("decode JSON turns: %v", err)
	}
	// The malformed session has a typed-field error, while the active turn has
	// a malformed timestamp but retains the SQL/JSON identity and queued status
	// needed for a conservative execution hold.
	sessions[bad.ID] = json.RawMessage(`{"id":"` + bad.ID + `","teams_chat_id":"` + bad.TeamsChatID + `","status":123}`)
	turns[turnID] = json.RawMessage(`{"id":"` + turnID + `","session_id":"` + bad.ID + `","status":"queued","queued_at":123}`)
	root["sessions"], err = json.Marshal(sessions)
	if err != nil {
		t.Fatalf("encode malformed JSON sessions: %v", err)
	}
	root["turns"], err = json.Marshal(turns)
	if err != nil {
		t.Fatalf("encode malformed JSON turns: %v", err)
	}
	corrupted, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode malformed JSON state: %v", err)
	}
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before malformed session/turn reopen: %v", err)
	}
	writeRawStoreStateForTest(t, store, corrupted)
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen malformed session/turn store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	loaded, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load malformed JSON session/turn rows: %v", err)
	}
	if _, ok := loaded.Sessions[good.ID]; !ok {
		t.Fatal("healthy JSON session was lost while isolating malformed session")
	}
	if _, ok := loaded.Sessions[bad.ID]; ok {
		t.Fatal("malformed JSON session was admitted into typed state")
	}
	turn, ok := loaded.Turns[turnID]
	if !ok || turn.ID != turnID || turn.SessionID != bad.ID || turn.Status != TurnStatusQueued {
		t.Fatalf("malformed JSON active turn = %#v found=%v, want queued safety hold", turn, ok)
	}

	// A second open/load is the startup boundary that matters to the listener;
	// the malformed row must remain locally held rather than poisoning healthy
	// state on the next process generation.
	if err := reopened.Close(); err != nil {
		t.Fatalf("close after malformed JSON load: %v", err)
	}
	reopened, err = Open(path)
	if err != nil {
		t.Fatalf("reopen malformed JSON session/turn store second generation: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err = reopened.Load(ctx)
	if err != nil {
		t.Fatalf("second Load malformed JSON session/turn rows: %v", err)
	}
	if _, ok := loaded.Sessions[good.ID]; !ok {
		t.Fatal("healthy JSON session was lost on second startup")
	}
	turn, ok = loaded.Turns[turnID]
	if !ok || turn.Status != TurnStatusQueued || turn.SessionID != bad.ID {
		t.Fatalf("second-generation malformed JSON active turn = %#v found=%v, want queued safety hold", turn, ok)
	}
}

func TestJSONMalformedOutboxAndHistoryRowsAreIsolatedAndPreserved(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	goodOutboxID := "outbox:json-healthy-row"
	badOutboxID := "outbox:json-malformed-row"
	goodHistoryID := "history-watch:json-healthy-row"
	badHistoryID := "history-watch:json-malformed-row"
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[goodOutboxID] = OutboxMessage{
			ID: goodOutboxID, TeamsChatID: "chat:json-healthy-row", Kind: "status",
			Body: "healthy outbox row", Status: OutboxStatusQueued, CreatedAt: now, UpdatedAt: now,
		}
		state.HistoryWatch[goodHistoryID] = HistoryWatchCheckpoint{
			ID: goodHistoryID, Path: "/tmp/json-healthy-row.jsonl", Offset: 7, Line: 1, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed JSON outbox/history rows: %v", err)
	}

	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read JSON state: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state: %v", err)
	}
	var outboxRows map[string]json.RawMessage
	if err := json.Unmarshal(root["outbox_messages"], &outboxRows); err != nil {
		t.Fatalf("decode JSON outbox map: %v", err)
	}
	var historyRows map[string]json.RawMessage
	if err := json.Unmarshal(root["history_watch"], &historyRows); err != nil {
		t.Fatalf("decode JSON history map: %v", err)
	}
	malformedOutbox := json.RawMessage(`{"id":"` + badOutboxID + `","teams_chat_id":"chat:json-malformed-row","status":17,"opaque_marker":"retain-outbox"}`)
	malformedHistory := json.RawMessage(`{"id":"` + badHistoryID + `","path":123,"opaque_marker":"retain-history"}`)
	outboxRows[badOutboxID] = malformedOutbox
	historyRows[badHistoryID] = malformedHistory
	root["outbox_messages"], err = json.Marshal(outboxRows)
	if err != nil {
		t.Fatalf("encode malformed JSON outbox map: %v", err)
	}
	root["history_watch"], err = json.Marshal(historyRows)
	if err != nil {
		t.Fatalf("encode malformed JSON history map: %v", err)
	}
	corrupted, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode malformed JSON state: %v", err)
	}
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before malformed outbox/history reopen: %v", err)
	}
	writeRawStoreStateForTest(t, store, corrupted)
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen malformed outbox/history store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load malformed outbox/history rows: %v", err)
	}
	if got := loaded.OutboxMessages[goodOutboxID].Body; got != "healthy outbox row" {
		t.Fatalf("healthy JSON outbox row = %q, want intact row", got)
	}
	if _, ok := loaded.OutboxMessages[badOutboxID]; ok {
		t.Fatalf("malformed JSON outbox row was exposed as typed state")
	}
	if got := loaded.HistoryWatch[goodHistoryID].Path; got != "/tmp/json-healthy-row.jsonl" {
		t.Fatalf("healthy JSON history row = %#v, want intact row", loaded.HistoryWatch[goodHistoryID])
	}
	if _, ok := loaded.HistoryWatch[badHistoryID]; ok {
		t.Fatalf("malformed JSON history row was exposed as typed state")
	}

	if err := reopened.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatID = "chat:unrelated-json-row-update"
		return nil
	}); err != nil {
		t.Fatalf("unrelated JSON update: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read JSON state after unrelated update: %v", err)
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state after unrelated update: %v", err)
	}
	if err := json.Unmarshal(root["outbox_messages"], &outboxRows); err != nil {
		t.Fatalf("decode outbox map after unrelated update: %v", err)
	}
	if err := json.Unmarshal(root["history_watch"], &historyRows); err != nil {
		t.Fatalf("decode history map after unrelated update: %v", err)
	}
	if string(outboxRows[badOutboxID]) != string(malformedOutbox) {
		t.Fatalf("malformed JSON outbox raw changed: got=%q want=%q", outboxRows[badOutboxID], malformedOutbox)
	}
	if string(historyRows[badHistoryID]) != string(malformedHistory) {
		t.Fatalf("malformed JSON history raw changed: got=%q want=%q", historyRows[badHistoryID], malformedHistory)
	}

	if err := reopened.Update(ctx, func(state *State) error {
		state.OutboxMessages[badOutboxID] = OutboxMessage{
			ID: badOutboxID, TeamsChatID: "chat:json-repaired", Kind: "status",
			Body: "repaired outbox row", Status: OutboxStatusQueued, CreatedAt: now, UpdatedAt: now,
		}
		state.HistoryWatch[badHistoryID] = HistoryWatchCheckpoint{
			ID: badHistoryID, Path: "/tmp/json-repaired-row.jsonl", Offset: 9, Line: 1, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("explicit JSON outbox/history replacement: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read JSON state after explicit replacement: %v", err)
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state after explicit replacement: %v", err)
	}
	if err := json.Unmarshal(root["outbox_messages"], &outboxRows); err != nil {
		t.Fatalf("decode outbox map after explicit replacement: %v", err)
	}
	if err := json.Unmarshal(root["history_watch"], &historyRows); err != nil {
		t.Fatalf("decode history map after explicit replacement: %v", err)
	}
	if string(outboxRows[badOutboxID]) == string(malformedOutbox) || string(historyRows[badHistoryID]) == string(malformedHistory) {
		t.Fatalf("explicit typed replacement did not retire opaque JSON rows: outbox=%q history=%q", outboxRows[badOutboxID], historyRows[badHistoryID])
	}
}

func TestSQLiteMalformedChatPollIsIsolatedAtFullLoadAndPreserved(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	good := testSession()
	good.ID = "sqlite-healthy-poll-session"
	good.TeamsChatID = "sqlite-healthy-poll-chat"
	bad := testSession()
	bad.ID = "sqlite-malformed-poll-session"
	bad.TeamsChatID = "sqlite-malformed-poll-chat"
	for _, session := range []SessionContext{good, bad} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[good.TeamsChatID] = ChatPollState{ChatID: good.TeamsChatID, Seeded: true, PollState: chatPollStateHot}
		state.ChatPolls[bad.TeamsChatID] = ChatPollState{ChatID: bad.TeamsChatID, Seeded: true, PollState: chatPollStateHot}
		return nil
	}); err != nil {
		t.Fatalf("seed chat polls: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// The cold state document in a SQLite store normally excludes chat_polls.
	// Put one malformed per-chat row in it as well as in the materialized table
	// so both the startup state decoder and the hot table loader are exercised.
	malformed := json.RawMessage(`{"chat_id":"` + bad.TeamsChatID + `","state":123,"opaque_marker":"retain-me"}`)
	stateRaw := sqliteRawStateJSONForTest(t, store)
	var root map[string]json.RawMessage
	if err := json.Unmarshal(stateRaw, &root); err != nil {
		t.Fatalf("decode sqlite cold state: %v", err)
	}
	encodedPolls, err := json.Marshal(map[string]json.RawMessage{
		good.TeamsChatID: mustJSONRaw(t, ChatPollState{ChatID: good.TeamsChatID, Seeded: true, PollState: chatPollStateHot}),
		bad.TeamsChatID:  malformed,
	})
	if err != nil {
		t.Fatalf("encode sqlite cold poll map: %v", err)
	}
	root["chat_polls"] = encodedPolls
	corruptedState, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode sqlite malformed cold state: %v", err)
	}
	sqliteWriteRawStateJSONForTest(t, store, corruptedState)
	sqliteWriteRawChatPollJSONForTest(t, store, bad.TeamsChatID, malformed)

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load malformed SQLite chat poll: %v", err)
	}
	if _, ok := loaded.ChatPolls[good.TeamsChatID]; !ok {
		t.Fatal("healthy SQLite chat poll was lost while isolating malformed poll")
	}
	badPoll, ok := loaded.ChatPolls[bad.TeamsChatID]
	if !ok || !badPoll.RecoveryRequired || badPoll.ChatID != bad.TeamsChatID || badPoll.RecoverySourceHash == "" {
		t.Fatalf("malformed SQLite chat poll recovery placeholder = %#v found=%v, want chat-local recovery marker", badPoll, ok)
	}

	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before malformed SQLite poll reopen: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen malformed SQLite poll store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err = reopened.Load(ctx)
	if err != nil {
		t.Fatalf("startup Load malformed SQLite chat poll: %v", err)
	}
	if _, ok := loaded.ChatPolls[good.TeamsChatID]; !ok {
		t.Fatal("healthy SQLite chat poll was lost after reopening")
	}
	badPoll, ok = loaded.ChatPolls[bad.TeamsChatID]
	if !ok || !badPoll.RecoveryRequired || badPoll.ChatID != bad.TeamsChatID || badPoll.RecoverySourceHash == "" {
		t.Fatalf("reopened malformed SQLite chat poll recovery placeholder = %#v found=%v, want marker", badPoll, ok)
	}
	rawBefore := sqliteRawChatPollJSONForTest(t, reopened, bad.TeamsChatID)
	if string(rawBefore) != string(malformed) {
		t.Fatalf("malformed SQLite poll raw before update = %q, want %q", rawBefore, malformed)
	}
	if _, err := reopened.SetPaused(ctx, true, "unrelated SQLite update"); err != nil {
		t.Fatalf("unrelated SQLite update: %v", err)
	}
	rawAfter := sqliteRawChatPollJSONForTest(t, reopened, bad.TeamsChatID)
	if string(rawAfter) != string(rawBefore) {
		t.Fatalf("malformed SQLite poll raw changed across unrelated update: before=%q after=%q", rawBefore, rawAfter)
	}
}

// Point reads must use the SQL chat key as the recovery identity even when a
// projection contains valid JSON with no usable ChatID. Otherwise {}, null, or
// a foreign embedded chat_id is interpreted as "no checkpoint" and the next
// live poll can silently consume the current Graph head as a new baseline.
func TestSQLitePointChatPollReadQuarantinesSemanticallyInvalidJSON(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "sqlite-point-poll-session"
	session.TeamsChatID = "sqlite-point-poll-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[session.TeamsChatID] = ChatPollState{
			ChatID: session.TeamsChatID, Seeded: true, PollState: "hot",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed chat poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	for _, raw := range [][]byte{
		[]byte(`{}`),
		[]byte(`null`),
		[]byte(`{"chat_id":"sqlite-foreign-chat","state":"hot"}`),
	} {
		sqliteWriteRawChatPollJSONForTest(t, store, session.TeamsChatID, raw)
		poll, found, err := store.ChatPoll(ctx, session.TeamsChatID)
		if err != nil {
			t.Fatalf("ChatPoll raw=%q: %v", raw, err)
		}
		if !found || poll.ChatID != session.TeamsChatID || !poll.RecoveryRequired || poll.RecoverySourceHash == "" {
			t.Fatalf("ChatPoll raw=%q = %#v found=%v, want chat-local recovery placeholder", raw, poll, found)
		}
	}
}

// Poll admission must not decode the growing cold state document on every
// listener tick. The runtime projection contains the small control binding
// needed by this path, so even a temporarily torn state_json row must not hide
// an otherwise healthy due chat.
func TestSQLiteHotPollAdmissionDoesNotDependOnColdStateJSON(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 8, 31, 14, 0, 0, 0, time.UTC)
	session := testSession()
	session.ID = "sqlite-hot-admission-session"
	session.TeamsChatID = "sqlite-hot-admission-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[session.TeamsChatID] = ChatPollState{
			ChatID: session.TeamsChatID, Seeded: true, PollState: "hot", NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed hot chat poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	sqliteWriteRawStateJSONForTest(t, store, []byte(`{"state_json":"torn"`))

	loaded, err := store.HotPollReadyScheduleState(ctx, "", now)
	if err != nil {
		t.Fatalf("HotPollReadyScheduleState with torn cold state: %v", err)
	}
	if poll, ok := loaded.ChatPolls[session.TeamsChatID]; !ok || poll.ChatID != session.TeamsChatID {
		t.Fatalf("hot admission chat poll = %#v found=%v, want healthy due chat", poll, ok)
	}
}

func TestSQLiteMalformedSessionAndActiveTurnAreIsolatedAtFullLoad(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	good := testSession()
	good.ID = "sqlite-healthy-session-row"
	good.TeamsChatID = "sqlite-healthy-session-chat"
	bad := testSession()
	bad.ID = "sqlite-malformed-session-row"
	bad.TeamsChatID = "sqlite-malformed-session-chat"
	for _, session := range []SessionContext{good, bad} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	turnID := "sqlite-malformed-active-turn"
	if err := store.Update(ctx, func(state *State) error {
		state.Turns[turnID] = Turn{ID: turnID, SessionID: bad.ID, Status: TurnStatusQueued, QueuedAt: time.Now(), CreatedAt: time.Now(), UpdatedAt: time.Now()}
		return nil
	}); err != nil {
		t.Fatalf("seed active turn: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	malformedSession := json.RawMessage(`{"id":"` + bad.ID + `","teams_chat_id":"` + bad.TeamsChatID + `","status":123}`)
	malformedTurn := json.RawMessage(`{"id":"` + turnID + `","session_id":"` + bad.ID + `","status":123}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, malformedSession, bad.ID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE turns SET json = ? WHERE id = ?`, malformedTurn, turnID)
		return err
	})

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load malformed session/turn rows: %v", err)
	}
	if _, ok := loaded.Sessions[good.ID]; !ok {
		t.Fatal("healthy session was lost while isolating malformed session")
	}
	if _, ok := loaded.Sessions[bad.ID]; ok {
		t.Fatal("malformed session was admitted into typed state")
	}
	turn, ok := loaded.Turns[turnID]
	if !ok || turn.ID != turnID || turn.SessionID != bad.ID || turn.Status != TurnStatusQueued {
		t.Fatalf("malformed active turn = %#v found=%v, want SQL-backed queued safety hold", turn, ok)
	}

	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before malformed session/turn reopen: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen malformed session/turn store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err = reopened.Load(ctx)
	if err != nil {
		t.Fatalf("startup Load malformed session/turn rows: %v", err)
	}
	if _, ok := loaded.Sessions[good.ID]; !ok {
		t.Fatal("healthy session was lost after reopening")
	}
	turn, ok = loaded.Turns[turnID]
	if !ok || turn.Status != TurnStatusQueued || turn.SessionID != bad.ID {
		t.Fatalf("reopened malformed active turn = %#v found=%v, want queued safety hold", turn, ok)
	}
}

func TestSQLiteMalformedSessionDoesNotDisableHealthyHotPollAdmission(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	good := testSession()
	good.ID = "sqlite-hot-healthy-session"
	good.TeamsChatID = "sqlite-hot-healthy-chat"
	bad := testSession()
	bad.ID = "sqlite-hot-malformed-session"
	bad.TeamsChatID = "sqlite-hot-malformed-chat"
	for _, session := range []SessionContext{good, bad} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	now := time.Now().UTC().Add(-time.Minute)
	if err := store.Update(ctx, func(state *State) error {
		for _, chatID := range []string{good.TeamsChatID, bad.TeamsChatID} {
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateHot,
				NextPollAt: now, LastActivityAt: now, UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed hot poll rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	malformed := []byte(`{"id":"` + bad.ID + `","teams_chat_id":"` + bad.TeamsChatID + `","status":123,"opaque_marker":"retain-session"}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, malformed, bad.ID)
		return err
	})

	candidates, handled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("HotPollWorkCandidates with malformed session: %v", err)
	}
	if !handled {
		t.Fatal("malformed session made SQLite admission fall back instead of serving the healthy session")
	}
	foundHealthy := false
	for _, candidate := range candidates {
		if candidate.ID == good.ID && candidate.TeamsChatID == good.TeamsChatID {
			foundHealthy = true
		}
		if candidate.ID == bad.ID {
			t.Fatalf("malformed session was admitted as runnable candidate: %#v", candidate)
		}
	}
	if !foundHealthy {
		t.Fatalf("healthy session disappeared behind malformed session: candidates=%#v", candidates)
	}

	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatID = "unrelated-session-preservation-update"
		return nil
	}); err != nil {
		t.Fatalf("unrelated SQLite update: %v", err)
	}
	if got := sqliteRawSessionJSONForTest(t, store, bad.ID); string(got) != string(malformed) {
		t.Fatalf("malformed session raw changed across unrelated update: got=%q want=%q", got, malformed)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after preserving malformed session: %v", err)
	}
	if _, ok := loaded.Sessions[good.ID]; !ok {
		t.Fatal("healthy session was lost after preserving malformed session")
	}
	if _, ok := loaded.Sessions[bad.ID]; ok {
		t.Fatal("malformed session became typed state after unrelated update")
	}
}

// A mixed-version writer can update the indexed session status separately from
// the canonical JSON. The active JSON must keep the chat visible to admission;
// otherwise one stale SQL projection can make a healthy conversation vanish
// until an unrelated full rewrite happens.
func TestSQLiteActiveJSONSessionSurvivesStaleSQLStatus(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "sqlite-stale-session-status"
	session.TeamsChatID = "sqlite-stale-session-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[session.TeamsChatID] = ChatPollState{
			ChatID: session.TeamsChatID, Seeded: true, PollState: "hot", NextPollAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale-status poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET status = ? WHERE id = ?`, "closed", session.ID)
		return err
	})

	candidates, handled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, "unrelated-control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("HotPollWorkCandidates with stale SQL status: %v", err)
	}
	if !handled {
		t.Fatal("stale SQL session status made SQLite admission fall back")
	}
	for _, candidate := range candidates {
		if candidate.ID == session.ID {
			return
		}
	}
	t.Fatalf("active JSON session was hidden by stale SQL status: candidates=%#v", candidates)
}

func TestSQLiteMalformedHistoryWatchProjectionFallsBackAndRematerializes(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	checkpointID := "history-watch:malformed-projection"
	now := time.Date(2026, 9, 1, 13, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.HistoryWatch[checkpointID] = HistoryWatchCheckpoint{
			ID: checkpointID, Path: "/tmp/malformed-projection.jsonl", SessionID: "projection-session",
			Offset: 64, Size: 64, Line: 4, UpdatedAt: now,
		}
		state.HistoryWatchReady = now
		return nil
	}); err != nil {
		t.Fatalf("seed history-watch projection: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	corruptProjection := []byte(`{"history_watch":`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, corruptProjection, sqliteHistoryWatchProjectionKey)
		return err
	})

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with malformed history-watch projection: %v", err)
	}
	if got := loaded.HistoryWatch[checkpointID].Offset; got != 64 {
		t.Fatalf("canonical history-watch offset = %d, want 64", got)
	}

	if err := store.UpdateHistoryWatch(ctx, func(history map[string]HistoryWatchCheckpoint, ready *time.Time) error {
		checkpoint := history[checkpointID]
		checkpoint.Offset = 96
		checkpoint.Size = 96
		checkpoint.Line = 6
		history[checkpointID] = checkpoint
		*ready = now.Add(time.Minute)
		return nil
	}); err != nil {
		t.Fatalf("rematerialize malformed history-watch projection: %v", err)
	}
	loaded, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after rematerializing history-watch projection: %v", err)
	}
	checkpoint := loaded.HistoryWatch[checkpointID]
	if checkpoint.Offset != 96 || checkpoint.Size != 96 || checkpoint.Line != 6 {
		t.Fatalf("rematerialized history-watch checkpoint = %#v, want updated canonical state", checkpoint)
	}
	var projection sqliteHistoryWatchProjection
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var raw []byte
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteHistoryWatchProjectionKey).Scan(&raw); err != nil {
			return err
		}
		return json.Unmarshal(raw, &projection)
	})
	if got := projection.HistoryWatch[checkpointID].Offset; got != 96 {
		t.Fatalf("rematerialized projection offset = %d, want 96", got)
	}
}

func mustJSONRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test JSON: %v", err)
	}
	return raw
}

func jsonCheckpointRawForTest(t *testing.T, path string, checkpointID string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read JSON state for checkpoint %s: %v", checkpointID, err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state for checkpoint %s: %v", checkpointID, err)
	}
	var rows map[string]json.RawMessage
	if err := json.Unmarshal(root["import_checkpoints"], &rows); err != nil {
		t.Fatalf("decode JSON checkpoints for %s: %v", checkpointID, err)
	}
	raw, ok := rows[checkpointID]
	if !ok {
		t.Fatalf("JSON checkpoint %s missing", checkpointID)
	}
	return append([]byte(nil), raw...)
}

func TestSQLiteMalformedCanonicalCheckpointIsIsolatedFromScopedReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := newTestStore(t)
	badSession := testSession()
	badSession.ID = "malformed-checkpoint-session"
	badSession.TeamsChatID = "malformed-checkpoint-chat"
	goodSession := testSession()
	goodSession.ID = "healthy-checkpoint-session"
	goodSession.TeamsChatID = "healthy-checkpoint-chat"
	for _, session := range []SessionContext{badSession, goodSession} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	badID := sessionTranscriptCheckpointID(badSession.ID)
	goodID := sessionTranscriptCheckpointID(goodSession.ID)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[badID] = ImportCheckpoint{
			ID: badID, SessionID: badSession.ID, LastRecordID: "bad-session-last-record",
			LastOffset: 128, LastOffsetKnown: true, Status: importCheckpointStatusComplete, UpdatedAt: time.Now(),
		}
		state.ImportCheckpoints[goodID] = ImportCheckpoint{
			ID: goodID, SessionID: goodSession.ID, LastRecordID: "healthy-session-last-record",
			LastOffset: 256, LastOffsetKnown: true, Status: importCheckpointStatusComplete, UpdatedAt: time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoints: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	badRaw, err := json.Marshal(ImportCheckpoint{ID: badID, SessionID: badSession.ID, LastRecordID: "bad-session-last-record", LastOffset: 128, LastOffsetKnown: true, Status: importCheckpointStatusComplete})
	if err != nil {
		t.Fatalf("marshal checkpoint: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(badRaw, &fields); err != nil {
		t.Fatalf("unmarshal checkpoint fields: %v", err)
	}
	fields["last_offset_known"] = json.RawMessage("1")
	badRaw, err = json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal malformed checkpoint: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE import_checkpoints SET json = ? WHERE id = ?`, badRaw, badID)
		return err
	})

	assertMalformedFallback := func(stage string, checkpoint ImportCheckpoint, found bool) {
		t.Helper()
		if !found {
			t.Fatalf("%s found=%v, want malformed checkpoint to remain addressable", stage, found)
		}
		if checkpoint.ID != badID || checkpoint.SessionID != badSession.ID {
			t.Fatalf("%s checkpoint identity = %#v, want id=%q session=%q", stage, checkpoint, badID, badSession.ID)
		}
		if checkpoint.TranscriptQuarantine == nil || checkpoint.TranscriptQuarantine.Kind != malformedCanonicalCheckpointKind {
			t.Fatalf("%s checkpoint quarantine = %#v, want %q", stage, checkpoint.TranscriptQuarantine, malformedCanonicalCheckpointKind)
		}
		if checkpoint.UnresolvedExecution == nil || !checkpoint.LegacySourceUnverified {
			t.Fatalf("%s checkpoint = %#v, want malformed unresolved fallback", stage, checkpoint)
		}
		if checkpoint.LastRecordID != "" || checkpoint.LastSourceLine != 0 || checkpoint.LastOffset != 0 || checkpoint.LastOffsetKnown ||
			checkpoint.SourcePath != "" || checkpoint.SourceGeneration != "" || checkpoint.SourceFingerprint != "" ||
			checkpoint.PendingHistoryRange != nil || checkpoint.ContextGap != nil || checkpoint.TerminalBoundary != nil {
			t.Fatalf("%s opaque fallback retained unproven cursor/proof: %#v", stage, checkpoint)
		}
	}

	checkpoint, found, err := store.ImportCheckpoint(ctx, badID)
	if err != nil {
		t.Fatalf("ImportCheckpoint malformed row: %v", err)
	}
	assertMalformedFallback("ImportCheckpoint", checkpoint, found)
	selected, err := store.ImportCheckpointsForSessions(ctx, []string{badSession.ID, goodSession.ID})
	if err != nil {
		t.Fatalf("ImportCheckpointsForSessions with one malformed row: %v", err)
	}
	assertMalformedFallback("ImportCheckpointsForSessions", selected[badID], true)
	if selected[goodID].LastRecordID != "healthy-session-last-record" {
		t.Fatalf("healthy selected checkpoint = %#v, want intact good row", selected[goodID])
	}
	snapshot, err := store.LinkedTranscriptSessionSnapshot(ctx, []string{badSession.ID, goodSession.ID})
	if err != nil {
		t.Fatalf("LinkedTranscriptSessionSnapshot with one malformed row: %v", err)
	}
	assertMalformedFallback("LinkedTranscriptSessionSnapshot", snapshot.Checkpoints[badID], true)
	if snapshot.Checkpoints[goodID].LastRecordID != "healthy-session-last-record" {
		t.Fatalf("healthy snapshot checkpoint = %#v, want intact good row", snapshot.Checkpoints[goodID])
	}
	state, err := store.TranscriptImportStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("TranscriptImportStateSnapshot with one malformed row: %v", err)
	}
	assertMalformedFallback("TranscriptImportStateSnapshot", state.ImportCheckpoints[badID], true)
	if state.ImportCheckpoints[goodID].LastRecordID != "healthy-session-last-record" {
		t.Fatalf("healthy import state checkpoint = %#v, want intact good row", state.ImportCheckpoints[goodID])
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with one malformed row: %v", err)
	}
	assertMalformedFallback("Load", loaded.ImportCheckpoints[badID], true)
	if loaded.ImportCheckpoints[goodID].LastRecordID != "healthy-session-last-record" {
		t.Fatalf("healthy loaded checkpoint = %#v, want intact good row", loaded.ImportCheckpoints[goodID])
	}
	rawBefore := sqliteRawImportCheckpointJSONForTest(t, store, badID)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[goodSession.ID] = goodSession
		return nil
	}); err != nil {
		t.Fatalf("unrelated update with opaque checkpoint: %v", err)
	}
	rawAfter := sqliteRawImportCheckpointJSONForTest(t, store, badID)
	if string(rawAfter) != string(rawBefore) {
		t.Fatalf("opaque checkpoint raw JSON changed across unrelated update: before=%q after=%q", rawBefore, rawAfter)
	}
}

func TestSQLiteOpaqueCheckpointRepairUsesRawCAS(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "opaque-repair-session"
	session.TeamsChatID = "opaque-repair-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	checkpointID := sessionTranscriptCheckpointID(session.ID)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
			ID: checkpointID, SessionID: session.ID, Status: importCheckpointStatusComplete,
			LastRecordID: "before-repair", LastOffset: 10, LastOffsetKnown: true,
			UpdatedAt: time.Date(2026, 8, 27, 1, 2, 3, 0, time.UTC),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	badRaw := []byte(`{"last_offset_known":1}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE import_checkpoints SET json = ? WHERE id = ?`, badRaw, checkpointID)
		return err
	})
	expectedRaw := sqliteRawImportCheckpointJSONForTest(t, store, checkpointID)
	if string(expectedRaw) != string(badRaw) {
		t.Fatalf("stored opaque raw = %q, want %q", expectedRaw, badRaw)
	}

	_, _, err := store.UpdateImportCheckpoint(ctx, checkpointID, func(current ImportCheckpoint, found bool, updateTime time.Time) (ImportCheckpoint, bool, error) {
		if !found || current.TranscriptQuarantine == nil {
			t.Fatalf("opaque update current=%#v found=%v, want deterministic fallback", current, found)
		}
		current.LastRecordID = "must-not-overwrite-raw"
		return current, true, nil
	})
	if !errors.Is(err, ErrOpaqueCheckpoint) {
		t.Fatalf("ordinary opaque update error = %v, want ErrOpaqueCheckpoint", err)
	}
	if got := sqliteRawImportCheckpointJSONForTest(t, store, checkpointID); string(got) != string(expectedRaw) {
		t.Fatalf("ordinary update changed opaque raw: got=%q want=%q", got, expectedRaw)
	}

	replacement := ImportCheckpoint{
		ID: checkpointID, SessionID: session.ID, Status: importCheckpointStatusComplete,
		SourcePath: "/tmp/opaque-repair.jsonl", LastRecordID: "after-repair",
		LastSourceLine: 4, LastOffset: 40, LastOffsetKnown: true, SourceSize: 40,
		UpdatedAt: time.Date(2026, 8, 27, 2, 3, 4, 0, time.UTC),
	}
	if err := store.RepairOpaqueImportCheckpoint(ctx, checkpointID, expectedRaw, replacement); err != nil {
		t.Fatalf("RepairOpaqueImportCheckpoint: %v", err)
	}
	got, found, err := store.ImportCheckpoint(ctx, checkpointID)
	if err != nil || !found || got.LastRecordID != replacement.LastRecordID || got.RecoveryProofUnusable {
		t.Fatalf("repaired checkpoint=%#v found=%v err=%v, want trusted replacement", got, found, err)
	}
	if err := store.RepairOpaqueImportCheckpoint(ctx, checkpointID, expectedRaw, replacement); err != nil {
		t.Fatalf("idempotent RepairOpaqueImportCheckpoint retry: %v", err)
	}
	staleReplacement := replacement
	staleReplacement.LastRecordID = "stale-replacement"
	if err := store.RepairOpaqueImportCheckpoint(ctx, checkpointID, []byte(`{"other":true}`), staleReplacement); !errors.Is(err, ErrCheckpointRepairConflict) {
		t.Fatalf("stale RepairOpaqueImportCheckpoint error = %v, want CAS conflict", err)
	}
}

func TestSQLiteOperationCheckpointUpdateAcceptsValidNonCanonicalRow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "operation-checkpoint-session"
	session.TeamsChatID = "operation-checkpoint-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate store: %v", err)
	}
	operationID := sessionTranscriptCheckpointID(session.ID) + ":subagent:child"
	if _, changed, err := store.UpdateImportCheckpoint(ctx, operationID, func(current ImportCheckpoint, found bool, now time.Time) (ImportCheckpoint, bool, error) {
		if found {
			t.Fatalf("new operation checkpoint found=%v, want false", found)
		}
		return ImportCheckpoint{
			ID: operationID, SessionID: session.ID, Status: importCheckpointStatusComplete,
			LastRecordID: "marker-1", UpdatedAt: now,
		}, true, nil
	}); err != nil || !changed {
		t.Fatalf("create operation checkpoint changed=%v err=%v", changed, err)
	}
	if _, changed, err := store.UpdateImportCheckpoint(ctx, operationID, func(current ImportCheckpoint, found bool, now time.Time) (ImportCheckpoint, bool, error) {
		if !found || current.ID != operationID || current.SessionID != session.ID {
			t.Fatalf("existing operation checkpoint=%#v found=%v, want valid non-canonical row", current, found)
		}
		current.LastRecordID = "marker-2"
		current.UpdatedAt = now
		return current, true, nil
	}); err != nil || !changed {
		t.Fatalf("update operation checkpoint changed=%v err=%v", changed, err)
	}
	got, found, err := store.ImportCheckpoint(ctx, operationID)
	if err != nil || !found || got.LastRecordID != "marker-2" {
		t.Fatalf("operation checkpoint=%#v found=%v err=%v, want updated valid row", got, found, err)
	}
}

func TestSQLiteNullableCheckpointMetadataIsolatedAndPreserved(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "nullable-checkpoint-session"
	session.TeamsChatID = "nullable-checkpoint-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)
	canonicalID := sessionTranscriptCheckpointID("empty-identity-session")
	nullRaw := []byte(`{}`)
	canonicalRaw := []byte(`{}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES(NULL, NULL, NULL, NULL, ?)`, nullRaw); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES(?, ?, NULL, NULL, ?)`, canonicalID, "empty-identity-session", canonicalRaw)
		return err
	})

	checkpoint, found, err := store.ImportCheckpoint(ctx, canonicalID)
	if err != nil || !found || checkpoint.TranscriptQuarantine == nil || checkpoint.TranscriptQuarantine.Kind != invalidCanonicalCheckpointKind {
		t.Fatalf("empty-identity checkpoint=%#v found=%v err=%v, want deterministic opaque fallback", checkpoint, found, err)
	}
	if checkpoint.LastRecordID != "" || checkpoint.LastOffset != 0 || checkpoint.LastOffsetKnown || checkpoint.SourcePath != "" || checkpoint.SourceGeneration != "" {
		t.Fatalf("empty-identity fallback retained unproven cursor/proof: %#v", checkpoint)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with nullable checkpoint metadata: %v", err)
	}
	if got := loaded.ImportCheckpoints[canonicalID]; got.TranscriptQuarantine == nil || got.TranscriptQuarantine.Kind != invalidCanonicalCheckpointKind {
		t.Fatalf("loaded empty-identity checkpoint=%#v, want opaque fallback", got)
	}

	if err := store.Update(ctx, func(state *State) error {
		state.SchemaVersion++
		return nil
	}); err != nil {
		t.Fatalf("unrelated update with nullable checkpoint metadata: %v", err)
	}
	var gotID, gotSession, gotStatus sql.NullString
	var gotUpdated sql.NullInt64
	var gotRaw []byte
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
		return db.QueryRowContext(ctx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id IS NULL`).Scan(&gotID, &gotSession, &gotStatus, &gotUpdated, &gotRaw)
	}); err != nil {
		t.Fatalf("read nullable checkpoint row: %v", err)
	}
	if gotID.Valid || gotSession.Valid || gotStatus.Valid || gotUpdated.Valid || string(gotRaw) != string(nullRaw) {
		t.Fatalf("nullable checkpoint row changed: id=%#v session=%#v status=%#v updated=%#v raw=%q", gotID, gotSession, gotStatus, gotUpdated, gotRaw)
	}
	if got := sqliteRawImportCheckpointJSONForTest(t, store, canonicalID); string(got) != string(canonicalRaw) {
		t.Fatalf("empty-identity raw JSON changed: got=%q want=%q", got, canonicalRaw)
	}
}

func TestSQLiteCanonicalCheckpointSQLIdentityConflictIsolated(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := testSession()
	owner.ID = "canonical-key-owner"
	owner.TeamsChatID = "canonical-key-owner-chat"
	foreign := testSession()
	foreign.ID = "canonical-sql-owner"
	foreign.TeamsChatID = "canonical-sql-owner-chat"
	for _, session := range []SessionContext{owner, foreign} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	keyID := sessionTranscriptCheckpointID(owner.ID)
	foreignID := sessionTranscriptCheckpointID(foreign.ID)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[keyID] = ImportCheckpoint{ID: keyID, SessionID: owner.ID, Status: importCheckpointStatusComplete}
		state.ImportCheckpoints[foreignID] = ImportCheckpoint{ID: foreignID, SessionID: foreign.ID, Status: importCheckpointStatusComplete}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoints: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	foreignPayload, err := json.Marshal(ImportCheckpoint{ID: sessionTranscriptCheckpointID(foreign.ID), SessionID: foreign.ID, Status: importCheckpointStatusComplete, LastRecordID: "foreign-payload"})
	if err != nil {
		t.Fatalf("marshal foreign payload: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE import_checkpoints SET session_id = ?, json = ? WHERE id = ?`, foreign.ID, foreignPayload, keyID)
		return err
	})

	if checkpoint, found, err := store.ImportCheckpoint(ctx, keyID); !errors.Is(err, ErrSessionStateProvenanceMismatch) || found || checkpoint.ID != "" {
		t.Fatalf("canonical SQL identity conflict = %#v found=%v err=%v, want isolated provenance error", checkpoint, found, err)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with canonical SQL identity conflict: %v", err)
	}
	if _, found := loaded.ImportCheckpoints[keyID]; found {
		t.Fatalf("Load exposed canonical SQL identity conflict as typed checkpoint: %#v", loaded.ImportCheckpoints[keyID])
	}
	importState, err := store.TranscriptImportStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("TranscriptImportStateSnapshot with canonical SQL identity conflict: %v", err)
	}
	if _, found := importState.ImportCheckpoints[keyID]; found {
		t.Fatalf("TranscriptImportStateSnapshot exposed canonical SQL identity conflict: %#v", importState.ImportCheckpoints[keyID])
	}
	if got := loaded.ImportCheckpoints[foreignID].SessionID; got != foreign.ID {
		t.Fatalf("healthy foreign checkpoint session=%q, want %q", got, foreign.ID)
	}
}
