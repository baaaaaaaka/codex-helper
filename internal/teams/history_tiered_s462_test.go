package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// S462 produced ordinary-looking task_started records for persistent goal
// continuations.  They carry the same metadata as a root task, so checking
// model_context_window/collaboration_mode_kind cannot establish ownership.
// The child final must remain quarantined after the prior terminal boundary.
func TestHistoryTieredScanTailQuarantinesRealGoalContinuationTaskStarted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := []byte(`{"type":"session_meta","payload":{"id":"thread-s462"}}
{"type":"response_item","payload":{"id":"outer-final","type":"message","role":"assistant","turn_id":"outer-turn","phase":"final_answer","content":[{"type":"output_text","text":"outer"}]}}
{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}
{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-turn","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}
{"type":"response_item","payload":{"id":"goal-context","type":"message","role":"user","content":[{"type":"input_text","text":"<codex_internal_context source=\"goal\">Continue working toward the active thread goal.</codex_internal_context>"}]}}
{"type":"response_item","payload":{"id":"child-final","type":"message","role":"assistant","phase":"final_answer","internal_chat_message_metadata_passthrough":{"turn_id":"child-turn"},"content":[{"type":"output_text","text":"child continuation answer"}]}}
`)
	if err := os.WriteFile(path, transcript, 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("goal continuation was accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "outer" {
		t.Fatalf("finals=%#v, want only the outer final", result.Finals)
	}
}

func TestHistoryWatchAdvancesPhysicalCursorPastContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zero-offset-watch.jsonl")
	transcript := "{" + `"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"outer"}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-turn"}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"child"}}` + "\n"
	if err := os.WriteFile(path, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	store := newBridgeTestStore(t)
	defer store.Close()
	bridge := &Bridge{store: store}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now()); err != nil {
		t.Fatalf("sync zero-offset history watch: %v", err)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load history watch state: %v", err)
	}
	checkpoint, ok := state.HistoryWatch[historyWatchCheckpointID(path)]
	if !ok {
		t.Fatalf("zero-offset history watch checkpoint missing: %#v", state.HistoryWatch)
	}
	if !checkpoint.UnresolvedContinuation {
		t.Fatalf("zero-offset child continuation was not blocked: %#v", checkpoint)
	}
	if checkpoint.LastFinalStartOffset != 0 || !checkpoint.LastFinalStartOffsetKnown {
		t.Fatalf("zero-offset final boundary = offset %d known=%v, want 0/true", checkpoint.LastFinalStartOffset, checkpoint.LastFinalStartOffsetKnown)
	}
	if checkpoint.Offset <= 0 || checkpoint.Offset != int64(len(transcript)) {
		t.Fatalf("history watch did not commit the physical cursor: offset=%d checkpoint=%#v", checkpoint.Offset, checkpoint)
	}
	if checkpoint.PendingHistoryRange == nil || checkpoint.PendingHistoryRange.StartOffset <= 0 || checkpoint.PendingHistoryRange.ExclusiveEnd != checkpoint.Offset {
		t.Fatalf("history watch missing bounded pending range: %#v", checkpoint)
	}
}

func TestHistoryTieredScanUnresolvedContinuationReleasesAfterLaterPrompt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "continuation-recovery.jsonl")
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-continuation-recovery"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-turn","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"child-turn","phase":"final_answer","message":"child must wait"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if !first.State.UnresolvedContinuation || first.State.Offset != int64(len(initial)) {
		t.Fatalf("first scan state=%#v, want unresolved at physical EOF", first.State)
	}

	appendLine(t, path, `{"type":"response_item","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"new explicit Teams request"}]}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"new-root","started_at":1786181090,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"new-root","phase":"final_answer","message":"new root answer"}}`)
	second, err := historyTieredScanTail(path, first.State, 1<<20)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if second.State.UnresolvedContinuation || second.State.PendingHistoryRange != nil || second.State.TranscriptQuarantine != nil {
		t.Fatalf("later prompt did not release unresolved range: state=%#v", second.State)
	}
	if len(second.Finals) != 1 || second.Finals[0].Record.Text != "new root answer" {
		t.Fatalf("second scan finals=%#v, want only new root answer", second.Finals)
	}

	repeat, err := historyTieredScanTail(path, second.State, 1<<20)
	if err != nil {
		t.Fatalf("repeat scan: %v", err)
	}
	if len(repeat.Finals) != 0 {
		t.Fatalf("repeat scan finals=%#v, want no duplicate", repeat.Finals)
	}
}

func TestHistoryWatchQuarantinesMirrorAmbiguityWithoutExecutionFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mirror-watch.jsonl")
	transcript := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-watch-mirror"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"anonymous mirror one"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"anonymous mirror two"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	store := newBridgeTestStore(t)
	bridge := &Bridge{store: store}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now()); err != nil {
		t.Fatalf("sync mirror history watch: %v", err)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load mirror history watch state: %v", err)
	}
	checkpoint, ok := state.HistoryWatch[historyWatchCheckpointID(path)]
	if !ok {
		t.Fatalf("mirror history-watch checkpoint missing: %#v", state.HistoryWatch)
	}
	if checkpoint.TranscriptQuarantine == nil || checkpoint.UnresolvedContinuation {
		t.Fatalf("mirror ambiguity became an execution fence: %#v", checkpoint)
	}
	if checkpoint.Offset <= 0 || checkpoint.PendingHistoryRange == nil {
		t.Fatalf("mirror ambiguity did not retain a physical cursor plus pending range: %#v", checkpoint)
	}
}

func TestHistoryWatchAdvancesPastMalformedRecordWithoutLivelock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed-watch.jsonl")
	contents := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-watch-malformed"}}`,
		`{"broken":`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write malformed transcript: %v", err)
	}
	store := newBridgeTestStore(t)
	bridge := &Bridge{store: store}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now()); err != nil {
		t.Fatalf("first malformed history-watch sync: %v", err)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load first history-watch state: %v", err)
	}
	checkpoint, ok := state.HistoryWatch[historyWatchCheckpointID(path)]
	if !ok {
		t.Fatalf("malformed history-watch checkpoint missing: %#v", state.HistoryWatch)
	}
	if checkpoint.Offset != int64(len(contents)) || checkpoint.Line != 2 {
		t.Fatalf("malformed history-watch cursor = %#v, want offset %d/line 2", checkpoint, len(contents))
	}
	if checkpoint.TranscriptQuarantine == nil || checkpoint.TranscriptQuarantine.Kind != "malformed_record" || checkpoint.UnresolvedContinuation {
		t.Fatalf("malformed history-watch disposition = %#v, want opaque history-only quarantine", checkpoint)
	}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("repeat malformed history-watch sync: %v", err)
	}
	repeated, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load repeated history-watch state: %v", err)
	}
	checkpoint = repeated.HistoryWatch[historyWatchCheckpointID(path)]
	if checkpoint.Offset != int64(len(contents)) || checkpoint.Line != 2 {
		t.Fatalf("repeat malformed history-watch cursor = %#v, want stable consumed boundary", checkpoint)
	}
}

func TestHistoryWatchChangedPathsChecksLegacyProofOnNormalPoll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-watch.jsonl")
	original := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("write original transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat original transcript: %v", err)
	}
	state := teamstore.State{HistoryWatch: map[string]teamstore.HistoryWatchCheckpoint{
		historyWatchCheckpointID(path): {
			ID:      historyWatchCheckpointID(path),
			Path:    path,
			Size:    info.Size(),
			ModTime: info.ModTime(),
			Offset:  info.Size(),
		},
	}}
	rewritten := strings.Replace(original, "old answer", "new answer", 1)
	if len(rewritten) != len(original) {
		t.Fatalf("test rewrite changed size")
	}
	if err := os.WriteFile(path, []byte(rewritten), 0o600); err != nil {
		t.Fatalf("rewrite transcript: %v", err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("restore transcript mtime: %v", err)
	}
	changes, err := historyWatchChangedPaths([]string{path}, state, false)
	if err != nil {
		t.Fatalf("historyWatchChangedPaths: %v", err)
	}
	if len(changes) != 1 || cleanComparablePath(changes[0]) != cleanComparablePath(path) {
		t.Fatalf("legacy same-size rewrite changes = %#v, want %q", changes, path)
	}
}

func TestHistoryWatchChangedPathsSkipsSourceRewriteBlockedRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocked-watch.jsonl")
	if err := os.WriteFile(path, []byte(`{"id":"rewritten","role":"assistant"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write blocked transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat blocked transcript: %v", err)
	}
	state := teamstore.State{HistoryWatch: map[string]teamstore.HistoryWatchCheckpoint{
		historyWatchCheckpointID(path): {
			ID:                   historyWatchCheckpointID(path),
			Path:                 path,
			Size:                 info.Size(),
			ModTime:              info.ModTime(),
			Offset:               info.Size(),
			SourceRewriteBlocked: true,
		},
	}}
	changes, err := historyWatchChangedPaths([]string{path}, state, false)
	if err != nil {
		t.Fatalf("historyWatchChangedPaths: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("blocked checkpoint changes = %#v, want no automatic poll", changes)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove blocked transcript: %v", err)
	}
	changes, err = historyWatchChangedPaths(nil, state, false)
	if err != nil {
		t.Fatalf("historyWatchChangedPaths after removal: %v", err)
	}
	if len(changes) != 1 || changes[0] != path {
		t.Fatalf("removed blocked checkpoint changes = %#v, want cleanup path", changes)
	}
}

func TestHistoryTieredScanTailQuarantinesNoIDChildEventMessageAfterTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := []byte(`{"type":"session_meta","payload":{"id":"thread-s462-no-id"}}
{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}
{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}
{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"anonymous child answer"}}
`)
	if err := os.WriteFile(path, transcript, 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("anonymous child final was accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "outer" {
		t.Fatalf("finals=%#v, want only the outer final", result.Finals)
	}
}

func TestHistoryTieredScanTailQuarantinesTwoAnonymousFinalsAfterTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "two-anonymous-finals.jsonl")
	transcript := []byte(`{"type":"session_meta","payload":{"id":"thread-two-anonymous"}}
{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}
{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}
{"type":"event_msg","payload":{"type":"agent_message","thread_id":"thread-two-anonymous","phase":"final_answer","message":"anonymous child one"}}
{"type":"response_item","thread_id":"thread-two-anonymous","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"anonymous child two"}]}}
`)
	if err := os.WriteFile(path, transcript, 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation || result.State.TranscriptQuarantine == nil {
		t.Fatalf("two anonymous finals were not transcript-quarantined: state=%#v finals=%#v", result.State, result.Finals)
	}
	for _, final := range result.Finals {
		if strings.Contains(final.Record.Text, "anonymous child") {
			t.Fatalf("anonymous child final leaked: %#v", result.Finals)
		}
	}
}

func TestHistoryTieredScanTailQuarantinesAnonymousFinalPairBeforeTerminal(t *testing.T) {
	for _, tc := range []struct {
		name string
		gap  string
	}{
		{name: "adjacent", gap: ""},
		{name: "intervening-status", gap: `{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"still working"}}` + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "two-anonymous-before-terminal.jsonl")
			transcript := `{"type":"session_meta","payload":{"id":"thread-two-anonymous-before"}}
{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"anonymous first"}}
` + tc.gap + `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"anonymous second"}]}}
{"type":"event_msg","payload":{"type":"task_complete"}}
`
			if err := os.WriteFile(path, []byte(transcript), 0o600); err != nil {
				t.Fatalf("write transcript: %v", err)
			}
			result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
			if err != nil {
				t.Fatalf("historyTieredScanTail: %v", err)
			}
			if result.State.UnresolvedContinuation || result.State.TranscriptQuarantine == nil {
				t.Fatalf("anonymous pre-terminal pair was not transcript-quarantined: state=%#v finals=%#v", result.State, result.Finals)
			}
			for _, final := range result.Finals {
				if strings.Contains(final.Record.Text, "anonymous ") {
					t.Fatalf("anonymous pre-terminal final leaked: %#v", result.Finals)
				}
			}
		})
	}
}

func TestHistoryTieredScanTailQuarantinesAnonymousFinalPairAcrossPolls(t *testing.T) {
	path := filepath.Join(t.TempDir(), "anonymous-across-polls.jsonl")
	first := []byte(`{"type":"session_meta","payload":{"id":"thread-anonymous-polls"}}
{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"anonymous first"}}
`)
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatalf("write first transcript: %v", err)
	}
	state, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if state.State.UnresolvedContinuation || len(state.Finals) != 1 {
		t.Fatalf("first anonymous final = state=%#v finals=%#v, want one pending final", state.State, state.Finals)
	}
	if err := os.WriteFile(path, append(first, []byte(`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"anonymous second"}]}}
`)...), 0o600); err != nil {
		t.Fatalf("append second transcript: %v", err)
	}
	second, err := historyTieredScanTail(path, state.State, 1<<20)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if second.State.UnresolvedContinuation || second.State.TranscriptQuarantine == nil {
		t.Fatalf("second anonymous final was not transcript-quarantined: state=%#v finals=%#v", second.State, second.Finals)
	}
}

func TestHistoryWatchDeletedSourceRewriteBlockedCheckpointIsCleanedAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "deleted-blocked-watch.jsonl")
			if err := os.WriteFile(path, []byte(`{"id":"rewritten"}`+"\n"), 0o600); err != nil {
				t.Fatalf("write blocked transcript: %v", err)
			}
			store := newBridgeTestStore(t)
			defer store.Close()
			checkpointID := historyWatchCheckpointID(path)
			if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
				history[checkpointID] = teamstore.HistoryWatchCheckpoint{
					ID: checkpointID, Path: path, Size: 64, Offset: 64,
					SourceRewriteBlocked: true, UpdatedAt: time.Now(),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed blocked history-watch checkpoint: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			if err := os.Remove(path); err != nil {
				t.Fatalf("remove transcript: %v", err)
			}
			bridge := &Bridge{store: store}
			if err := bridge.syncCodexHistoryWatchPath(ctx, path, time.Now()); err != nil {
				t.Fatalf("sync deleted blocked checkpoint: %v", err)
			}
			state, err := store.HistoryWatchState(ctx)
			if err != nil {
				t.Fatalf("read history-watch state after cleanup: %v", err)
			}
			if _, found := state.HistoryWatch[checkpointID]; found {
				t.Fatalf("deleted blocked checkpoint survived cleanup: %#v", state.HistoryWatch[checkpointID])
			}
			// Cleanup is idempotent: a second poll has no checkpoint to rewrite.
			if err := bridge.syncCodexHistoryWatchPath(ctx, path, time.Now()); err != nil {
				t.Fatalf("repeat deleted blocked checkpoint cleanup: %v", err)
			}
		})
	}
}

func TestHistoryWatchLegacyCheckpointWithoutFingerprintDefersSilently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-history-watch.jsonl")
	body := []byte(`{"type":"session_meta","payload":{"id":"legacy-watch"}}
{"thread_id":"legacy-watch","turn_id":"turn-old","id":"old-final","role":"assistant","text":"old answer"}
{"type":"turn.completed","thread_id":"legacy-watch","turn_id":"turn-old"}
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat transcript: %v", err)
	}
	store := newBridgeTestStore(t)
	defer store.Close()
	checkpointID := historyWatchCheckpointID(path)
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		history[checkpointID] = teamstore.HistoryWatchCheckpoint{
			ID:          checkpointID,
			Path:        path,
			Size:        info.Size(),
			ModTime:     info.ModTime(),
			Offset:      info.Size(),
			LastFinalID: "old-final",
		}
		*ready = time.Now()
		return nil
	}); err != nil {
		t.Fatalf("seed legacy history-watch checkpoint: %v", err)
	}
	appendLine(t, path, `{"thread_id":"legacy-watch","turn_id":"turn-new","id":"new-final","role":"assistant","text":"new answer from an untrusted legacy boundary"}`)
	appendLine(t, path, `{"type":"turn.completed","thread_id":"legacy-watch","turn_id":"turn-new"}`)
	bridge := &Bridge{store: store}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now()); err != nil {
		t.Fatalf("sync legacy history-watch checkpoint: %v", err)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load history-watch state: %v", err)
	}
	checkpoint := state.HistoryWatch[checkpointID]
	if checkpoint.SourceRewriteBlocked || !checkpoint.LegacySourceUnverified {
		t.Fatalf("legacy checkpoint was not migrated to a silent history-only boundary: %#v", checkpoint)
	}
	if checkpoint.Offset != info.Size() {
		t.Fatalf("legacy checkpoint cursor changed while blocked: got %d want %d", checkpoint.Offset, info.Size())
	}
	if checkpoint.SourceFingerprint != "" {
		t.Fatalf("legacy checkpoint unexpectedly acquired a fingerprint from an untrusted prefix: %#v", checkpoint)
	}
}

func TestExplicitHistoryClearsLegacySourceUnverifiedMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-explicit-recovery.jsonl")
	body := `{"id":"recovered-final","role":"assistant","text":"recovered answer"}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	store := newBridgeTestStore(t)
	defer store.Close()
	checkpointID := "transcript:legacy-explicit-recovery"
	if _, _, err := store.UpdateImportCheckpoint(context.Background(), checkpointID, func(checkpoint teamstore.ImportCheckpoint, _ bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		checkpoint.ID = checkpointID
		checkpoint.SessionID = "legacy-explicit-recovery"
		checkpoint.SourcePath = path
		checkpoint.LastRecordID = "old-final"
		checkpoint.LegacySourceUnverified = true
		checkpoint.ImportTurnID = "publish-history:legacy-explicit-recovery"
		checkpoint.Status = importCheckpointStatusImporting
		checkpoint.UpdatedAt = now
		return checkpoint, true, nil
	}); err != nil {
		t.Fatalf("seed legacy checkpoint: %v", err)
	}
	bridge := &Bridge{store: store}
	if err := bridge.markTranscriptImportCompleteDetailedWithID(context.Background(), Session{ID: "legacy-explicit-recovery"}, path, "recovered-final", 1, checkpointID, true); err != nil {
		t.Fatalf("complete explicit history: %v", err)
	}
	checkpoint, found, err := store.ImportCheckpoint(context.Background(), checkpointID)
	if err != nil || !found {
		t.Fatalf("load recovered checkpoint: found=%v err=%v", found, err)
	}
	if checkpoint.LegacySourceUnverified || checkpoint.SourceFingerprint == "" || checkpoint.Status != importCheckpointStatusComplete {
		t.Fatalf("explicit recovery left an unusable legacy checkpoint: %#v", checkpoint)
	}
}

func TestHistoryTieredScanTailQuarantinesSourceIDChildEventMessageWithoutTurnIDAfterTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := []byte(`{"type":"session_meta","payload":{"id":"thread-s462-source-id"}}
{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}
{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}
{"type":"event_msg","payload":{"id":"child-final","type":"agent_message","phase":"final_answer","message":"source-id child answer without turn provenance"}}
`)
	if err := os.WriteFile(path, transcript, 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("source-ID child final was accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "outer" {
		t.Fatalf("finals=%#v, want only the outer final", result.Finals)
	}
}

func TestHistoryTieredScanTailQuarantinesSourceIDChildAfterFinalWithoutTaskTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := []byte(`{"type":"session_meta","payload":{"id":"thread-s462-source-id-no-terminal"}}
{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}
{"type":"event_msg","payload":{"id":"child-final","type":"agent_message","phase":"final_answer","message":"source-id child after final without task terminal"}}
`)
	if err := os.WriteFile(path, transcript, 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("source-ID child final was accepted without explicit terminal: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "outer" {
		t.Fatalf("finals=%#v, want only the outer final", result.Finals)
	}
}

func TestReadLinkedTranscriptDeltaLegacyNoOffsetQuarantinesSourceIDChildEventMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-s462-legacy-no-offset"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}`,
		`{"type":"event_msg","payload":{"id":"child-final","type":"agent_message","phase":"final_answer","message":"legacy source-id child answer"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath: path, LastRecordID: "outer-final",
	}, "thread-s462-legacy-no-offset", "thread-s462-legacy-no-offset")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !result.UnresolvedContinuation && !result.PendingContinuation {
		t.Fatalf("legacy no-offset child continuation was not blocked: %#v", result)
	}
	for _, record := range result.Records {
		if strings.Contains(record.Text, "legacy source-id child answer") {
			t.Fatalf("legacy no-offset child answer leaked: %#v", result.Records)
		}
	}
}

func TestReadLinkedTranscriptDeltaLegacyNoOffsetQuarantinesDifferentTurnFinal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-s462-legacy-no-offset-turn"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer without turn provenance"}}`,
		`{"type":"response_item","payload":{"id":"child-final","type":"message","role":"assistant","turn_id":"child-turn","phase":"final_answer","content":[{"type":"output_text","text":"legacy different-turn child answer"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath: path, LastRecordID: "outer-final",
	}, "thread-s462-legacy-no-offset-turn", "thread-s462-legacy-no-offset-turn")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !result.UnresolvedContinuation && !result.PendingContinuation {
		t.Fatalf("legacy no-offset different-turn continuation was not blocked: %#v", result)
	}
	for _, record := range result.Records {
		if strings.Contains(record.Text, "legacy different-turn child answer") {
			t.Fatalf("legacy no-offset different-turn answer leaked: %#v", result.Records)
		}
	}
}

func TestReadLinkedTranscriptDeltaQuarantinesNoIDChildEventMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-s462-linked-no-id"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat initial transcript: %v", err)
	}
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"anonymous child answer"}}`)
	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath: path, LastRecordID: "outer-final", LastSourceLine: 3,
		LastOffset: info.Size(), SourceSize: info.Size(), SourceModTime: info.ModTime(),
	}, "thread-s462-linked-no-id", "thread-s462-linked-no-id")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !transcript.UnresolvedContinuation && !transcript.PendingContinuation {
		t.Fatalf("anonymous child continuation was not blocked: %#v", transcript)
	}
	for _, record := range transcript.Records {
		if strings.Contains(record.Text, "anonymous child answer") {
			t.Fatalf("anonymous child answer leaked into linked records: %#v", transcript.Records)
		}
	}
}

func TestReadLinkedTranscriptDeltaRejectsNonEOFPrefixedSourceRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	oldLine := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	trustedTail := `{"id":"trusted-tail","role":"assistant","text":"already trusted"}` + "\n"
	if err := os.WriteFile(path, []byte(oldLine+trustedTail), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat initial transcript: %v", err)
	}
	offset := int64(len(oldLine))
	checkpoint := teamstore.ImportCheckpoint{
		SourcePath:        path,
		SessionID:         "thread-non-eof-rewrite",
		LastRecordID:      "old",
		LastSourceLine:    1,
		LastOffset:        offset,
		LastOffsetKnown:   true,
		SourceSize:        info.Size(),
		SourceModTime:     info.ModTime(),
		SourceFingerprint: transcriptCheckpointSourceFingerprint(path, offset),
	}
	if checkpoint.SourceFingerprint == "" {
		t.Fatal("initial checkpoint fingerprint is empty")
	}
	// Rewrite bytes inside the trusted window without changing the line size,
	// then append a plausible final. The append must not make the rewritten
	// prefix look like an ordinary incremental tail.
	rewritten := `{"id":"old","role":"assistant","text":"new answer"}` + "\n"
	if len(rewritten) != len(oldLine) {
		t.Fatalf("rewrite fixture changed size: old=%d new=%d", len(oldLine), len(rewritten))
	}
	if err := os.WriteFile(path, []byte(rewritten+trustedTail+`{"id":"new-final","role":"assistant","text":"must stay blocked"}`+"\n"), 0o600); err != nil {
		t.Fatalf("rewrite transcript: %v", err)
	}
	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, checkpoint, "thread-non-eof-rewrite", "thread-non-eof-rewrite")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !transcriptHasDiagnostic(transcript, "source_rewritten") {
		t.Fatalf("diagnostics = %#v, want source_rewritten", transcript.Diagnostics)
	}
	if len(transcript.Records) != 0 || transcriptRecordsContainText(transcript.Records, "must stay blocked") {
		t.Fatalf("rewritten non-EOF delta leaked records: %#v", transcript.Records)
	}
}

func TestReadLinkedTranscriptDeltaRejectsSamePrefixSourceReplacement(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	oldLine := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	trustedTail := `{"id":"trusted-tail","role":"assistant","text":"already trusted"}` + "\n"
	if err := os.WriteFile(path, []byte(oldLine+trustedTail), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat initial transcript: %v", err)
	}
	offset := int64(len(oldLine))
	checkpoint := teamstore.ImportCheckpoint{
		SourcePath:        path,
		SessionID:         "thread-same-prefix-replacement",
		LastRecordID:      "old",
		LastSourceLine:    1,
		LastOffset:        offset,
		LastOffsetKnown:   true,
		SourceSize:        info.Size(),
		SourceModTime:     info.ModTime(),
		SourceFingerprint: transcriptCheckpointSourceFingerprint(path, offset),
	}
	if checkpoint.SourceFingerprint == "" {
		t.Fatal("initial checkpoint fingerprint is empty")
	}

	// Keep the trusted prefix byte-for-byte identical, but replace the path with
	// a different inode and a plausible new suffix. A bounded content hash alone
	// would accept this; automatic linked delivery must reject it.
	replacement := filepath.Join(dir, "replacement.jsonl")
	if err := os.WriteFile(replacement, []byte(oldLine+trustedTail+`{"id":"new-final","role":"assistant","text":"must stay blocked"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write replacement transcript: %v", err)
	}
	oldPath := filepath.Join(dir, "old.jsonl")
	if err := os.Rename(path, oldPath); err != nil {
		t.Fatalf("move old transcript: %v", err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatalf("install replacement transcript: %v", err)
	}

	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, checkpoint, "thread-same-prefix-replacement", "thread-same-prefix-replacement")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !transcriptHasDiagnostic(transcript, "source_rewritten") {
		t.Fatalf("diagnostics = %#v, want source_rewritten", transcript.Diagnostics)
	}
	if len(transcript.Records) != 0 || transcriptRecordsContainText(transcript.Records, "must stay blocked") {
		t.Fatalf("replacement delta leaked records: %#v", transcript.Records)
	}
}

func TestReadLinkedTranscriptDeltaRejectsKnownZeroOffsetWithRecordID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(`{"id":"new-final","role":"assistant","text":"must stay blocked"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath:      path,
		LastRecordID:    "stale-record",
		LastOffset:      0,
		LastOffsetKnown: true,
	}, "thread-zero-inconsistent", "thread-zero-inconsistent")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !transcriptHasDiagnostic(transcript, "source_rewritten") {
		t.Fatalf("diagnostics = %#v, want source_rewritten", transcript.Diagnostics)
	}
	if len(transcript.Records) != 0 {
		t.Fatalf("inconsistent zero-offset checkpoint returned records: %#v", transcript.Records)
	}
}

func TestTranscriptCheckpointKnownZeroOffsetDoesNotMeanEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte("prefix\n"), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	zero := transcriptCheckpointSourceFingerprint(path, 0)
	eof := transcriptCheckpointSourceFingerprint(path, int64(len("prefix\n")))
	if zero == "" || eof == "" || zero == eof {
		t.Fatalf("fingerprints at zero/EOF = %q/%q, want distinct non-empty proofs", zero, eof)
	}
	if !linkedCheckpointOffsetKnown(teamstore.ImportCheckpoint{LastOffset: 0, LastOffsetKnown: true}) {
		t.Fatal("known zero offset was not recognized")
	}
	if linkedCheckpointOffsetKnown(teamstore.ImportCheckpoint{LastOffset: 0}) {
		t.Fatal("legacy zero offset was treated as known")
	}
}

func TestBridgeSyncLinkedTranscriptQuarantinesNoIDChildEventMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-s462-linked-bridge-no-id"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-s462-linked-bridge-no-id", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-s462-linked-bridge-no-id")
	*sent = nil
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"anonymous child answer"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("sync linked transcript: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "anonymous child answer") {
		t.Fatalf("anonymous child answer leaked through bridge: %#v", *sent)
	}
	if len(*sent) != 0 {
		t.Fatalf("history-only ambiguity emitted a live execution notice: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.Status == importCheckpointStatusBlocked || checkpoint.LastRecordID == "" || checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine == nil || checkpoint.PendingHistoryRange == nil || checkpoint.LastOffset <= int64(len(initial)) {
		t.Fatalf("checkpoint = %#v, want non-blocking history-only quarantine after physical progress", checkpoint)
	}
	firstNoticeCount := strings.Count(sentPlainJoined(*sent), "helper publish-history")
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("repeat linked transcript sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if got := strings.Count(sentPlainJoined(*sent), "helper publish-history"); got != firstNoticeCount {
		t.Fatalf("history-only quarantine emitted a repeat notice: before=%d after=%d sent=%#v", firstNoticeCount, got, *sent)
	}
}

func TestBridgeSyncLinkedTranscriptQuarantinesSourceIDChildEventMessageWithoutTurnID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-s462-linked-source-id"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-s462-linked-source-id", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-s462-linked-source-id")
	*sent = nil
	appendLine(t, path, `{"type":"event_msg","payload":{"id":"child-final","type":"agent_message","phase":"final_answer","message":"source-id child answer without turn provenance"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("sync linked transcript: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "source-id child answer without turn provenance") {
		t.Fatalf("source-ID child answer leaked through bridge: %#v", *sent)
	}
	if len(*sent) != 0 {
		t.Fatalf("history-only ambiguity emitted a live execution notice: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine == nil {
		t.Fatalf("checkpoint = %#v, want history-only quarantine", checkpoint)
	}
}

func TestBridgeSyncLinkedTranscriptAdvancesOversizedTailIncrementally(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-s462-linked-large"}}` + "\n" +
		`{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-s462-linked-large", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-s462-linked-large")
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load seeded state: %v", err)
	}
	before := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	*sent = nil
	largeStatus := `{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"` + strings.Repeat("x", historyTieredMaxTailBytes+1024) + `"}}` + "\n"
	largeFinal := `{"type":"event_msg","payload":{"type":"agent_message","id":"large-final","turn_id":"child-turn","phase":"final_answer","message":"large tail final"}}` + "\n"
	if err := os.WriteFile(path, []byte(initial+largeStatus+largeFinal), 0o600); err != nil {
		t.Fatalf("write oversized transcript: %v", err)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync oversized linked transcript: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "large tail final") {
		t.Fatalf("oversized tail final leaked: %#v", *sent)
	}
	if !sentPlainContains(*sent, strings.Repeat("x", 40)) {
		t.Fatalf("bounded oversized-tail prefix was not delivered: %#v", *sent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	after := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if after.Status == importCheckpointStatusBlocked || after.LastOffset <= before.LastOffset || after.LastOffset >= after.SourceSize {
		t.Fatalf("oversized-tail checkpoint did not make a bounded resumable advance: before=%#v after=%#v", before, after)
	}
	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("resume oversized linked transcript: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if !sentPlainContains(*sent, "large tail final") {
		t.Fatalf("automatic bounded resume did not release oversized backlog: %#v", *sent)
	}
}

func TestBridgeSyncLinkedTranscriptPublishesSafePrefixBeforePendingRootMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-s512-safe-prefix"}}` + "\n" +
		`{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-s512-safe-prefix", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-s512-safe-prefix")
	*sent = nil

	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","id":"safe-prefix-final","turn_id":"outer-turn","phase":"final_answer","message":"safe prefix answer must be delivered"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"pending-root","started_at":1786181089,"model_context_window":128000,"collaboration_mode_kind":"default"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync pending-root transcript: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if !sentPlainContains(*sent, "safe prefix answer must be delivered") {
		t.Fatalf("safe prefix final was lost when the scan ended at task_started: %#v", *sent)
	}
	if sentPlainContains(*sent, "previous Codex execution is still unconfirmed") || sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("pending root marker emitted an execution/history gate for a safe prefix: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load pending-root state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.LastOffset <= 0 || checkpoint.LastOffset != checkpoint.SourceSize || checkpoint.Status == importCheckpointStatusBlocked || checkpoint.UnresolvedExecution != nil {
		t.Fatalf("checkpoint after safe prefix = %#v, want progress through the ignored marker without an owner fence", checkpoint)
	}
	first := sentPlainJoined(*sent)
	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("repeat pending-root transcript sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if strings.Contains(sentPlainJoined(*sent), "safe prefix answer must be delivered") {
		t.Fatalf("safe prefix final was replayed on the pending marker poll: first=%s repeat=%#v", first, *sent)
	}
}

func TestBridgeSyncLinkedTranscriptAdvancesPastOversizedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-s462-linked-oversized-record"}}` + "\n" +
		`{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-s462-linked-oversized-record", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-s462-linked-oversized-record")
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load seeded state: %v", err)
	}
	before := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	giantRecord := `{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"` + strings.Repeat("x", historyTieredMaxRecordBytes+1) + `"}}` + "\n"
	finalRecord := `{"type":"event_msg","payload":{"type":"agent_message","id":"after-oversized-record","phase":"final_answer","message":"must wait for recovery"}}` + "\n"
	if err := os.WriteFile(path, []byte(initial+giantRecord+finalRecord), 0o600); err != nil {
		t.Fatalf("write oversized-record transcript: %v", err)
	}
	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("first oversized-record sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "must wait for recovery") || sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("automatic oversized-record scan emitted user-visible output: %#v", *sent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load quarantined state: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.OversizedRecordBlocked || checkpoint.Status == importCheckpointStatusBlocked {
		t.Fatalf("checkpoint = %#v, want a resumable bounded cursor", checkpoint)
	}
	if checkpoint.LastOffset <= before.LastOffset || checkpoint.LastOffset >= checkpoint.SourceSize {
		t.Fatalf("checkpoint = %#v, want the cursor after the giant record and before the final", checkpoint)
	}
	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("resume unchanged oversized-record sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if !sentPlainContains(*sent, "must wait for recovery") {
		t.Fatalf("resumable oversized-record scan did not deliver the following final: %#v", *sent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("reload quarantined state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[checkpointID]
	if checkpoint.OversizedRecordBlocked || checkpoint.LastOffset < checkpoint.SourceSize {
		t.Fatalf("resumed oversized-record checkpoint = %#v, want EOF cursor", checkpoint)
	}

	// A later append remains ordinary incremental history; the old oversized
	// record does not turn the source into a permanent history gate.
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"new safe record"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("changed oversized-record sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("changed oversized-record sync emitted a recovery gate: %#v", *sent)
	}
}

func TestBridgeSyncLinkedTranscriptAdvancesPastMalformedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-s520-malformed"
	initial := `{"type":"session_meta","payload":{"id":"` + threadID + `"}}` + "\n" +
		`{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, threadID, path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, threadID)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load seeded state: %v", err)
	}
	before := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	malformed := `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"broken-turn","message":"broken"}` + "\n"
	after := `{"type":"event_msg","payload":{"id":"after-malformed","type":"agent_message","thread_id":"` + threadID + `","turn_id":"new-turn","phase":"final_answer","message":"after malformed"}}` + "\n"
	if err := os.WriteFile(path, []byte(initial+malformed+after), 0o600); err != nil {
		t.Fatalf("write malformed continuation: %v", err)
	}
	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("first malformed sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "after malformed") || sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("malformed boundary emitted later history or a gate: %#v", *sent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load malformed checkpoint: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.LastOffset <= before.LastOffset || checkpoint.LastOffset >= checkpoint.SourceSize {
		t.Fatalf("malformed checkpoint = %#v, want cursor after malformed line and before final", checkpoint)
	}
	if checkpoint.TranscriptQuarantine == nil || checkpoint.TranscriptQuarantine.Kind != "malformed_record" {
		t.Fatalf("malformed checkpoint quarantine = %#v", checkpoint.TranscriptQuarantine)
	}
	if checkpoint.UnresolvedExecution != nil {
		t.Fatalf("malformed history created live execution owner: %#v", checkpoint.UnresolvedExecution)
	}

	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("resume after malformed sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if !sentPlainContains(*sent, "after malformed") {
		t.Fatalf("later final was not delivered after malformed boundary: %#v", *sent)
	}
}

func TestBridgeSyncLinkedTranscriptAdvancesPastLargeInvisibleCompleteRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-s519-invisible"}}` + "\n" +
		`{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-s519-invisible", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-s519-invisible")
	*sent = nil

	// This models the image/tool record reported by s519: it is a complete
	// newline-bounded JSON object, larger than the normal scan budget but still
	// below the opaque-record cap, and its envelope has no visible text.
	invisible := `{"type":"item.completed","item":{"type":"message","role":"assistant","metadata":"` +
		strings.Repeat("x", historyTieredMaxRecordBytes-512*1024) + `"}}` + "\n"
	final := `{"type":"event_msg","payload":{"type":"agent_message","id":"s519-final","phase":"final_answer","message":"final after invisible image"}}` + "\n"
	if err := os.WriteFile(path, []byte(initial+invisible+final), 0o600); err != nil {
		t.Fatalf("write invisible-record transcript: %v", err)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("first invisible-record sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "final after invisible image") {
		t.Fatalf("first scan reached final beyond the bounded invisible record: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load invisible-record checkpoint: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.Status == importCheckpointStatusBlocked || checkpoint.LastOffset <= 0 || checkpoint.LastOffset >= checkpoint.SourceSize {
		t.Fatalf("invisible complete record did not create a resumable ignored cursor: %#v", checkpoint)
	}

	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("resume invisible-record sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if !sentPlainContains(*sent, "final after invisible image") {
		t.Fatalf("final after invisible record was not delivered: %#v", *sent)
	}
}

func TestReadLinkedTranscriptDeltaLegacyCheckpointResumesBoundedFullRescan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := `{"type":"session_meta","payload":{"id":"thread-s462-legacy-large"}}` + "\n" +
		`{"id":"checkpoint","role":"assistant","text":"old answer"}` + "\n" +
		`{"id":"tail","role":"assistant","text":"` + strings.Repeat("x", historyTieredMaxTailBytes+1024) + `"}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write oversized legacy transcript: %v", err)
	}
	delta, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath: path, LastRecordID: "checkpoint",
	}, "thread-s462-legacy-large", "thread-s462-legacy-large")
	if err != nil {
		t.Fatalf("read legacy oversized delta: %v", err)
	}
	if transcriptHasDiagnostic(delta, "tail_too_large") || len(delta.Records) != 1 || delta.Records[0].ItemID != "tail" {
		t.Fatalf("legacy oversized delta = %#v, want the bounded consumed record", delta)
	}
}

func TestReadLinkedTranscriptDeltaLegacyCheckpointPreservesCompletePrefixBeforePartial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := strings.Join([]string{
		`{"id":"checkpoint","role":"assistant","text":"old answer"}`,
		`{"id":"new-final","role":"assistant","text":"complete answer before partial tail"}`,
	}, "\n") + "\n"
	partial := `{"id":"partial","role":"assistant","text":"still being written`
	if err := os.WriteFile(path, []byte(prefix+partial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}

	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath:   path,
		LastRecordID: "checkpoint",
	}, "thread-legacy-partial", "thread-legacy-partial")
	if err != nil {
		t.Fatalf("read legacy transcript delta: %v", err)
	}
	if !transcriptHasDiagnostic(transcript, "incomplete_tail") {
		t.Fatalf("delta diagnostics = %#v, want incomplete_tail", transcript.Diagnostics)
	}
	if len(transcript.Records) != 1 || transcript.Records[0].ItemID != "new-final" || transcript.Records[0].Text != "complete answer before partial tail" {
		t.Fatalf("delta records = %#v, want the complete record before the partial tail", transcript.Records)
	}
}

func TestReadLinkedTranscriptDeltaLegacyCheckpointRefusesMissingKeyBeforePartial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := `{"id":"new-final","role":"assistant","text":"complete answer"}` + "\n" +
		`{"id":"partial","role":"assistant","text":"still being written`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath:   path,
		LastRecordID: "missing-checkpoint",
	}, "thread-legacy-partial-missing", "thread-legacy-partial-missing")
	if err != nil {
		t.Fatalf("read legacy transcript delta: %v", err)
	}
	if !transcriptHasDiagnostic(transcript, "checkpoint_not_found") || len(transcript.Records) != 0 {
		t.Fatalf("delta = %#v, want checkpoint_not_found with no records", transcript)
	}
}

func TestHistoryTieredScanTailQuarantinesAdjacentMixedIDFinalCandidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"event-final","type":"agent_message","phase":"final_answer","message":"mixed-id mirror"}}`,
		`{"type":"response_item","thread_id":"thread-mixed","payload":{"type":"message","role":"assistant","phase":"final_answer","turn":{"id":"turn-mixed"},"content":[{"type":"output_text","text":"mixed-id mirror"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation || result.State.TranscriptQuarantine == nil || len(result.Finals) != 0 {
		t.Fatalf("adjacent mixed-ID candidates escaped transcript quarantine: state=%#v finals=%#v", result.State, result.Finals)
	}
	if result.State.TranscriptQuarantine.FrontierOffset != 0 || result.State.TranscriptQuarantine.FrontierLine != 1 {
		t.Fatalf("mixed-ID quarantine frontier = %#v, want the first record at offset 0/line 1", result.State.TranscriptQuarantine)
	}
}

func TestHistoryTieredScanTailQuarantinesMixedIDMirrorAfterTerminalBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "terminal-mixed-id-mirror.jsonl")
	body := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"event_msg","payload":{"id":"event-final","type":"agent_message","phase":"final_answer","message":"same terminal mirror"}}`,
		`{"type":"response_item","thread_id":"thread-terminal-mirror","payload":{"type":"message","role":"assistant","phase":"final_answer","turn":{"id":"mirror-turn"},"content":[{"type":"output_text","text":"same terminal mirror"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation || result.State.TranscriptQuarantine == nil || len(result.Finals) != 1 || result.Finals[0].Record.Text != "outer answer" {
		t.Fatalf("terminal mixed-ID mirror escaped transcript quarantine: state=%#v finals=%#v quarantined=%#v", result.State, result.Finals, result.QuarantinedFinals)
	}
}

func TestHistoryTieredScanTailQuarantinesSameSourceMixedIDFinalCandidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"event-final","type":"agent_message","phase":"final_answer","message":"same-source ambiguous answer"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_tokens":1}}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"same-source ambiguous answer","turn_id":"turn-mixed"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{ThreadID: "thread-same-source"}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation || result.State.TranscriptQuarantine == nil || len(result.Finals) != 0 {
		t.Fatalf("same-source mixed-ID candidates escaped transcript quarantine: state=%#v finals=%#v", result.State, result.Finals)
	}
}

func TestHistoryTieredScanTailQuarantinesNonAdjacentMixedIDFinalCandidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"event-final","type":"agent_message","phase":"final_answer","message":"ambiguous repeated answer"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_tokens":1}}}`,
		`{"type":"response_item","thread_id":"thread-mixed","payload":{"type":"message","role":"assistant","phase":"final_answer","turn":{"id":"turn-mixed"},"content":[{"type":"output_text","text":"ambiguous repeated answer"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{ThreadID: "thread-mixed"}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation || result.State.TranscriptQuarantine == nil || len(result.Finals) != 0 {
		t.Fatalf("ambiguous mixed-ID candidates escaped transcript quarantine: state=%#v finals=%#v", result.State, result.Finals)
	}
}

func TestHistoryTieredScanTailQuarantinesSameTextAnonymousFinalCandidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"anonymous-first","type":"agent_message","phase":"final_answer","message":"repeated anonymous answer"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_tokens":1}}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"repeated anonymous answer"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:                 path,
		TerminalBoundarySeen: true,
		LastFinalID:          "old-final",
		LastFinalTextHash:    normalizedTextHash("old answer"),
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation || result.State.TranscriptQuarantine == nil || len(result.Finals) != 0 || len(result.QuarantinedFinals) != 2 {
		t.Fatalf("same-text anonymous candidates escaped transcript quarantine: state=%#v finals=%#v quarantined=%#v", result.State, result.Finals, result.QuarantinedFinals)
	}
}

func TestHistoryTieredScanTailQuarantinesCrossPollMixedIDMirror(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cross-poll-mirror.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"event_msg","payload":{"id":"event-final","type":"agent_message","phase":"final_answer","message":"cross-poll mirror"}}`+"\n"), 0o600); err != nil {
		t.Fatalf("write first transcript surface: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{Path: path}, 1<<20)
	if err != nil {
		t.Fatalf("first historyTieredScanTail: %v", err)
	}
	if first.State.UnresolvedContinuation || first.State.TranscriptQuarantine != nil || len(first.Finals) != 1 {
		t.Fatalf("first surface = state=%#v finals=%#v quarantine=%#v, want one provisional final", first.State, first.Finals, first.State.TranscriptQuarantine)
	}
	appendLine(t, path, `{"type":"response_item","thread_id":"thread-cross-poll","payload":{"type":"message","role":"assistant","phase":"final_answer","turn":{"id":"typed-mirror-turn"},"content":[{"type":"output_text","text":"cross-poll mirror"}]}}`)
	second, err := historyTieredScanTail(path, first.State, 1<<20)
	if err != nil {
		t.Fatalf("second historyTieredScanTail: %v", err)
	}
	if second.State.UnresolvedContinuation || second.State.TranscriptQuarantine == nil || len(second.Finals) != 0 {
		t.Fatalf("cross-poll mirror escaped transcript quarantine: state=%#v finals=%#v quarantined=%#v", second.State, second.Finals, second.QuarantinedFinals)
	}
}

func TestBridgeSyncLinkedTranscriptQuarantinesCrossPollMixedIDMirror(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cross-poll-bridge.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-cross-poll-bridge"}}` + "\n" +
		`{"type":"event_msg","payload":{"id":"old-final","type":"agent_message","phase":"final_answer","message":"old answer"}}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-cross-poll-bridge", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-cross-poll-bridge")
	*sent = nil
	firstAnswer := "cross-poll answer"
	appendLine(t, path, `{"type":"event_msg","payload":{"id":"event-final","type":"agent_message","phase":"final_answer","message":"`+firstAnswer+`"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("sync first surface: %v", err)
	}
	if strings.Count(sentPlainJoined(*sent), firstAnswer) != 1 {
		t.Fatalf("first surface output = %#v, want one answer", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load after first surface: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.LastFinalID == "" || !checkpoint.LastFinalStartOffsetKnown || checkpoint.LastFinalTextHash == "" {
		t.Fatalf("first surface did not persist final boundary provenance: %#v", checkpoint)
	}
	if len(state.TranscriptLedger) == 0 {
		t.Fatalf("first surface did not retain transcript delivery ledger")
	}
	appendLine(t, path, `{"type":"response_item","thread_id":"thread-cross-poll-bridge","payload":{"type":"message","role":"assistant","phase":"final_answer","turn":{"id":"typed-mirror-turn"},"content":[{"type":"output_text","text":"`+firstAnswer+`"}]}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("sync second surface: %v", err)
	}
	if strings.Count(sentPlainJoined(*sent), firstAnswer) != 1 {
		t.Fatalf("cross-poll mirror was delivered twice: %#v", *sent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load after second surface: %v", err)
	}
	checkpoint = state.ImportCheckpoints[checkpointID]
	if checkpoint.TranscriptQuarantine == nil || checkpoint.UnresolvedExecution != nil || checkpoint.LastRecordID == "" && checkpoint.LastOffset != 0 {
		t.Fatalf("cross-poll checkpoint = %#v, want retained history-only quarantine", checkpoint)
	}
}

func TestHistoryTieredScanTailKeepsSameTextAcrossExplicitTurns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"first-final","type":"agent_message","turn_id":"turn-one","phase":"final_answer","message":"same text"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-one"}}`,
		`{"type":"response_item","thread_id":"thread-same-text","payload":{"id":"next-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"a distinct next request"}]}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-two","started_at":1786181089,"model_context_window":128000,"collaboration_mode_kind":"default"}}`,
		`{"type":"response_item","thread_id":"thread-same-text","payload":{"type":"message","role":"assistant","turn":{"id":"turn-two"},"phase":"final_answer","content":[{"type":"output_text","text":"same text"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{ThreadID: "thread-same-text"}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation || len(result.Finals) != 2 {
		t.Fatalf("distinct explicit turns were deduped or quarantined: state=%#v finals=%#v", result.State, result.Finals)
	}
}

func TestHistoryTieredScanTailDoesNotCompactSameTextAcrossPromptScopes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-text-prompt-scopes.jsonl")
	body := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"anonymous-final","type":"agent_message","phase":"final_answer","message":"same scoped text"}}`,
		`{"type":"response_item","thread_id":"thread-same-scope","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"new outer request"}]}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"new-turn","started_at":1786181089,"model_context_window":128000,"collaboration_mode_kind":"default"}}`,
		`{"type":"response_item","thread_id":"thread-same-scope","payload":{"type":"message","role":"assistant","turn":{"id":"new-turn"},"phase":"final_answer","content":[{"type":"output_text","text":"same scoped text"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:                      path,
		ThreadID:                  "thread-same-scope",
		TerminalBoundarySeen:      true,
		LastFinalID:               "prior-final",
		LastFinalStartOffsetKnown: true,
		LastFinalStartOffset:      1,
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation {
		t.Fatalf("same-text finals across a visible prompt were gated as an execution owner: state=%#v quarantined=%#v", result.State, result.QuarantinedFinals)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "same scoped text" || result.Finals[0].Record.TurnID != "new-turn" {
		t.Fatalf("same-text prompt scopes finals = %#v, want the new request final", result.Finals)
	}
}

func TestBridgeSyncLinkedTranscriptQuarantinesNonAdjacentMixedIDMirror(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-mixed-bridge"}}` + "\n" +
		`{"type":"event_msg","payload":{"id":"old-final","type":"agent_message","phase":"final_answer","message":"old answer"}}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-mixed-bridge", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-mixed-bridge")
	*sent = nil
	appendLine(t, path, `{"type":"event_msg","payload":{"id":"mixed-event","type":"agent_message","phase":"final_answer","message":"same ambiguous answer"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"token_count","info":{"total_tokens":1}}}`)
	appendLine(t, path, `{"type":"response_item","thread_id":"thread-mixed-bridge","payload":{"type":"message","role":"assistant","turn":{"id":"mixed-turn"},"phase":"final_answer","content":[{"type":"output_text","text":"same ambiguous answer"}]}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("sync mixed-ID transcript: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if strings.Contains(joined, "same ambiguous answer") {
		t.Fatalf("ambiguous mixed-ID answer leaked through bridge: %#v", *sent)
	}
	if strings.Contains(joined, "helper publish-history") {
		t.Fatalf("transcript-only ambiguity emitted a recovery notice: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load mixed-ID state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.Status == importCheckpointStatusBlocked || checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine == nil || checkpoint.PendingHistoryRange == nil {
		t.Fatalf("mixed-ID checkpoint = %#v, want non-blocking silent transcript quarantine", checkpoint)
	}
}

func TestBridgeSyncLinkedTranscriptQuarantinesSameSourceMixedIDMirror(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-same-source-bridge"}}` + "\n" +
		`{"type":"event_msg","payload":{"id":"old-final","type":"agent_message","phase":"final_answer","message":"old answer"}}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-same-source-bridge", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-same-source-bridge")
	*sent = nil
	appendLine(t, path, `{"type":"event_msg","payload":{"id":"mixed-event","type":"agent_message","phase":"final_answer","message":"same-source ambiguous answer"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"token_count","info":{"total_tokens":1}}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"same-source ambiguous answer","turn_id":"mixed-turn"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("sync same-source mixed-ID transcript: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if strings.Contains(joined, "same-source ambiguous answer") {
		t.Fatalf("same-source mixed-ID answer leaked through bridge: %#v", *sent)
	}
	if strings.Contains(joined, "helper publish-history") {
		t.Fatalf("transcript-only ambiguity emitted a recovery notice: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load same-source mixed-ID state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.Status == importCheckpointStatusBlocked || checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine == nil || checkpoint.PendingHistoryRange == nil {
		t.Fatalf("same-source mixed-ID checkpoint = %#v, want non-blocking silent transcript quarantine", checkpoint)
	}
}

func TestBridgeWorkPublishHistoryRecoversTranscriptQuarantine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-explicit-quarantine"}}` + "\n" +
		`{"id":"old-final","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-explicit-quarantine", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-explicit-quarantine")
	*sent = nil
	appendLine(t, path, `{"type":"event_msg","payload":{"id":"mirror-event","type":"agent_message","phase":"final_answer","message":"quarantined answer"}}`)
	appendLine(t, path, `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"quarantined answer"}]}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("automatic quarantine sync: %v", err)
	}
	if sentPlainContains(*sent, "quarantined answer") || sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("automatic quarantine produced user-visible output: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load quarantined state: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.TranscriptQuarantine == nil || checkpoint.UnresolvedExecution != nil {
		t.Fatalf("automatic mirror state = %#v, want history-only quarantine", checkpoint)
	}

	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, bridgePollMessage("publish-quarantine", "2026-08-22T02:00:00Z", "helper publish-history"), "helper publish-history"); err != nil {
		t.Fatalf("explicit publish-history recovery: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if strings.Count(joined, "quarantined answer") != 1 || !strings.Contains(joined, "Import complete") {
		t.Fatalf("explicit recovery output = %q, want one answer and completion", joined)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load recovered state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[checkpointID]
	if checkpoint.Status != importCheckpointStatusComplete || checkpoint.TranscriptQuarantine != nil || checkpoint.UnresolvedExecution != nil {
		t.Fatalf("explicit recovery checkpoint = %#v, want complete without quarantine/fence", checkpoint)
	}
}

func TestPersistTranscriptQuarantineDoesNotOverwriteNewerBranchOrRecovery(t *testing.T) {
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			graph, _ := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			session := bridge.reg.SessionByChatID("chat-1")
			checkpointID := transcriptCheckpointID(session.ID)
			quarantine := &teamstore.TranscriptQuarantine{Kind: "mixed_id_final_mirror", SourcePath: "/old/rollout.jsonl", FrontierOffset: 128}
			if err := store.Update(context.Background(), func(state *teamstore.State) error {
				state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{ID: checkpointID, SessionID: session.ID, SourcePath: quarantine.SourcePath, Status: importCheckpointStatusBlocked, TranscriptQuarantine: quarantine}
				return nil
			}); err != nil {
				t.Fatalf("seed quarantine: %v", err)
			}
			stale, found, err := store.ImportCheckpoint(context.Background(), checkpointID)
			if err != nil || !found {
				t.Fatalf("load stale checkpoint: %#v found=%v err=%v", stale, found, err)
			}
			if err := store.Update(context.Background(), func(state *teamstore.State) error {
				current := state.ImportCheckpoints[checkpointID]
				current.TranscriptQuarantine = &teamstore.TranscriptQuarantine{Kind: quarantine.Kind, SourcePath: quarantine.SourcePath, FrontierOffset: quarantine.FrontierOffset, LiveBranchThreadID: "thread-live"}
				state.ImportCheckpoints[checkpointID] = current
				return nil
			}); err != nil {
				t.Fatalf("seed live branch winner: %v", err)
			}
			if err := bridge.persistTranscriptQuarantine(context.Background(), *session, quarantine.SourcePath, stale, quarantine); err != nil {
				t.Fatalf("stale quarantine persistence after branch bind: %v", err)
			}
			current, _, err := store.ImportCheckpoint(context.Background(), checkpointID)
			if err != nil || current.TranscriptQuarantine == nil || current.TranscriptQuarantine.LiveBranchThreadID != "thread-live" {
				t.Fatalf("stale writer erased live branch: %#v err=%v", current, err)
			}

			stale, _, err = store.ImportCheckpoint(context.Background(), checkpointID)
			if err != nil {
				t.Fatalf("reload checkpoint before explicit clear: %v", err)
			}
			if err := store.Update(context.Background(), func(state *teamstore.State) error {
				current := state.ImportCheckpoints[checkpointID]
				current.TranscriptQuarantine = nil
				current.Status = importCheckpointStatusComplete
				state.ImportCheckpoints[checkpointID] = current
				return nil
			}); err != nil {
				t.Fatalf("seed explicit recovery winner: %v", err)
			}
			if err := bridge.persistTranscriptQuarantine(context.Background(), *session, quarantine.SourcePath, stale, quarantine); err != nil {
				t.Fatalf("stale quarantine persistence after explicit recovery: %v", err)
			}
			current, _, err = store.ImportCheckpoint(context.Background(), checkpointID)
			if err != nil || current.TranscriptQuarantine != nil || current.Status != importCheckpointStatusComplete {
				t.Fatalf("stale writer resurrected quarantine: %#v err=%v", current, err)
			}
		})
	}
}
