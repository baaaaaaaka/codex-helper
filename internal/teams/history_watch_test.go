package teams

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type historyWatchEventSourceStub struct {
	paths     []string
	prune     bool
	dirty     []string
	uncertain bool
}

func (s *historyWatchEventSourceStub) Update(paths []string, prune bool) ([]string, bool, error) {
	s.paths = append([]string(nil), paths...)
	s.prune = prune
	dirty := append([]string(nil), s.dirty...)
	s.dirty = nil
	return dirty, s.uncertain, nil
}

func (s *historyWatchEventSourceStub) Close() error { return nil }

func TestHistoryWatchTrustedCleanCycleDoesNotSelectAllHistoryFiles(t *testing.T) {
	root := t.TempDir()
	paths := []string{
		filepath.Join(root, "cold-1.jsonl"),
		filepath.Join(root, "cold-2.jsonl"),
		filepath.Join(root, "cold-3.jsonl"),
	}
	state := teamstore.State{HistoryWatch: make(map[string]teamstore.HistoryWatchCheckpoint, len(paths))}
	for _, path := range paths {
		state.HistoryWatch[historyWatchCheckpointID(path)] = teamstore.HistoryWatchCheckpoint{Path: path}
	}
	stub := &historyWatchEventSourceStub{}
	bridge := &Bridge{historyWatchEvents: stub}

	dirty, uncertain := bridge.historyWatchDirtyPaths(paths, nil, false)
	if uncertain {
		t.Fatal("trusted clean event source became uncertain")
	}
	if len(dirty) != 0 {
		t.Fatalf("trusted clean dirty paths = %#v, want empty", dirty)
	}
	if stub.prune || len(stub.paths) != 0 {
		t.Fatalf("trusted clean watcher paths=%#v prune=%t, want no full history set", stub.paths, stub.prune)
	}
	scanPaths := historyWatchScanPaths(paths, nil, dirty, state, false, uncertain)
	if len(scanPaths) != 0 {
		t.Fatalf("trusted clean scan paths = %#v, want empty", scanPaths)
	}
}

func TestHistoryWatchUncertainCycleSelectsCompleteHistorySet(t *testing.T) {
	root := t.TempDir()
	paths := []string{
		filepath.Join(root, "cold-1.jsonl"),
		filepath.Join(root, "cold-2.jsonl"),
	}
	state := teamstore.State{HistoryWatch: make(map[string]teamstore.HistoryWatchCheckpoint, len(paths))}
	for _, path := range paths {
		state.HistoryWatch[historyWatchCheckpointID(path)] = teamstore.HistoryWatchCheckpoint{Path: path}
	}
	scanPaths := historyWatchScanPaths(paths, nil, nil, state, false, true)
	if !reflect.DeepEqual(scanPaths, uniqueSortedCleanPaths(paths)) {
		t.Fatalf("uncertain scan paths = %#v, want complete set %#v", scanPaths, paths)
	}
}

func TestHistoryWatchTrustedIdleCycleReselectsCheckpointedUnfinishedTail(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "unfinished.jsonl")
	blockedPath := filepath.Join(root, "blocked.jsonl")
	legacyPath := filepath.Join(root, "legacy.jsonl")
	state := teamstore.State{HistoryWatch: map[string]teamstore.HistoryWatchCheckpoint{
		historyWatchCheckpointID(path): {
			Path:   path,
			Offset: 128,
			Size:   256,
		},
		historyWatchCheckpointID(blockedPath): {
			Path:                 blockedPath,
			Offset:               128,
			Size:                 256,
			SourceRewriteBlocked: true,
		},
		historyWatchCheckpointID(legacyPath): {
			Path:                   legacyPath,
			Offset:                 128,
			Size:                   256,
			LegacySourceUnverified: true,
		},
	}}

	// The watcher has already consumed the original dirty event.  The durable
	// cursor is still before the observed EOF, so a clean/no-event cycle must
	// retain the path for another bounded scan instead of treating the chat as
	// fully caught up.
	got := historyWatchScanPaths([]string{path, blockedPath, legacyPath}, nil, nil, state, false, false)
	if !reflect.DeepEqual(got, []string{path}) {
		t.Fatalf("trusted idle scan paths = %#v, want unfinished path %q", got, path)
	}
}

func TestHistoryWatchMandatoryRecoveryIncludesUnprovedAndRecoveryFlags(t *testing.T) {
	base := teamstore.HistoryWatchCheckpoint{Path: "/sessions/quiet.jsonl", Size: 100, Offset: 100}
	tests := []struct {
		name string
		edit func(*teamstore.HistoryWatchCheckpoint)
	}{
		{
			name: "missing source proof at caught up cursor",
		},
		{
			name: "oversized record",
			edit: func(checkpoint *teamstore.HistoryWatchCheckpoint) { checkpoint.OversizedRecordBlocked = true },
		},
		{
			name: "incomplete source rewrite scan",
			edit: func(checkpoint *teamstore.HistoryWatchCheckpoint) { checkpoint.SourceRewriteRecoveryScanPending = true },
		},
		{
			name: "unresolved continuation",
			edit: func(checkpoint *teamstore.HistoryWatchCheckpoint) { checkpoint.UnresolvedContinuation = true },
		},
		{
			name: "pending root task",
			edit: func(checkpoint *teamstore.HistoryWatchCheckpoint) { checkpoint.PendingRootTaskStarted = true },
		},
		{
			name: "pending assistant",
			edit: func(checkpoint *teamstore.HistoryWatchCheckpoint) {
				checkpoint.PendingAssistantSourceID = "assistant-record"
			},
		},
		{
			name: "context gap",
			edit: func(checkpoint *teamstore.HistoryWatchCheckpoint) {
				checkpoint.ContextGap = &teamstore.ContextGapState{}
			},
		},
		{
			name: "pending history range",
			edit: func(checkpoint *teamstore.HistoryWatchCheckpoint) {
				checkpoint.PendingHistoryRange = &teamstore.HistoryPendingRange{}
			},
		},
		{
			name: "quarantined frontier",
			edit: func(checkpoint *teamstore.HistoryWatchCheckpoint) {
				checkpoint.TranscriptQuarantine = &teamstore.TranscriptQuarantine{Kind: "test", FrontierRecordID: "record"}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checkpoint := base
			if test.edit != nil {
				test.edit(&checkpoint)
			}
			if !historyWatchCheckpointNeedsMandatoryMaintenance(checkpoint) {
				t.Fatalf("checkpoint %#v was not selected for mandatory maintenance", checkpoint)
			}
		})
	}
	if historyWatchCheckpointNeedsMandatoryMaintenance(teamstore.HistoryWatchCheckpoint{Path: "/sessions/clean.jsonl", Size: 100, Offset: 100, SourceFingerprint: "trusted"}) {
		t.Fatal("trusted caught-up checkpoint was selected for mandatory maintenance")
	}
}

func TestHistoryWatchDirtyPathsRetainsFailedPathUntilAcknowledged(t *testing.T) {
	changedPath := filepath.Join(t.TempDir(), "changed.jsonl")
	recentPath := filepath.Join(t.TempDir(), "recent.jsonl")
	stub := &historyWatchEventSourceStub{dirty: []string{changedPath}}
	bridge := &Bridge{historyWatchEvents: stub}

	dirty, uncertain := bridge.historyWatchDirtyPaths([]string{"/cold/indexed.jsonl"}, []string{recentPath}, false)
	if uncertain || !reflect.DeepEqual(dirty, []string{changedPath}) {
		t.Fatalf("initial dirty paths = %#v uncertain=%t", dirty, uncertain)
	}
	if stub.prune || !reflect.DeepEqual(stub.paths, []string{recentPath}) {
		t.Fatalf("incremental watcher update paths=%#v prune=%t", stub.paths, stub.prune)
	}

	// A failed publish must be retried even when the watcher has no duplicate
	// event. This is the production behavior after the existing history-watch
	// worker returns an error.
	bridge.historyWatchRetryDirtyPath(changedPath)
	dirty, uncertain = bridge.historyWatchDirtyPaths(nil, nil, false)
	if uncertain || !reflect.DeepEqual(dirty, []string{changedPath}) {
		t.Fatalf("retained dirty paths = %#v uncertain=%t", dirty, uncertain)
	}
	bridge.historyWatchAckDirtyPath(changedPath)
	dirty, uncertain = bridge.historyWatchDirtyPaths(nil, nil, false)
	if uncertain || len(dirty) != 0 {
		t.Fatalf("acknowledged dirty paths = %#v uncertain=%t, want empty", dirty, uncertain)
	}
}

func TestHistoryWatchDirtyHintWorkerFailureRetainsRetryBoundary(t *testing.T) {
	changedPath := filepath.Join(t.TempDir(), "changed.jsonl")
	wantErr := errors.New("history worker failed")
	stub := &historyWatchEventSourceStub{dirty: []string{changedPath}}
	bridge := &Bridge{
		historyWatchEvents:        stub,
		transcriptSyncWorkerCount: 1,
	}
	var attempts int
	bridge.historyWatchPathHook = func(context.Context, string) error {
		attempts++
		return wantErr
	}

	dirty, uncertain := bridge.historyWatchDirtyPaths(nil, []string{changedPath}, false)
	if uncertain || !reflect.DeepEqual(dirty, []string{changedPath}) {
		t.Fatalf("dirty hint = %#v uncertain=%t, want the changed path", dirty, uncertain)
	}
	if err := bridge.runHistoryWatchSyncJobs(context.Background(), dirty, time.Now()); !errors.Is(err, wantErr) {
		t.Fatalf("history worker error = %v, want %v", err, wantErr)
	}
	if attempts != 1 {
		t.Fatalf("history worker attempts = %d, want one bounded attempt", attempts)
	}

	// The worker failure is not an acknowledgement.  The production cycle adds
	// the path back to the retry lane after the aggregate worker error; this
	// assertion exercises that event-hint/worker/retry boundary together.
	bridge.historyWatchRetryDirtyPaths(dirty)
	retained, uncertain := bridge.historyWatchDirtyPaths(nil, nil, false)
	if uncertain || !reflect.DeepEqual(retained, []string{changedPath}) {
		t.Fatalf("retained retry boundary = %#v uncertain=%t, want %q", retained, uncertain, changedPath)
	}
	bridge.historyWatchAckDirtyPaths(retained)
	if retained, uncertain := bridge.historyWatchDirtyPaths(nil, nil, false); uncertain || len(retained) != 0 {
		t.Fatalf("acknowledged retry boundary = %#v uncertain=%t, want empty", retained, uncertain)
	}
}

func TestHistoryWatchWorkerStopsQueuedJobsAfterProcessWideFailure(t *testing.T) {
	bridge := &Bridge{transcriptSyncWorkerCount: 1}
	paths := make([]string, 8)
	for i := range paths {
		paths[i] = filepath.Join(t.TempDir(), "queued", fmt.Sprintf("%d.jsonl", i))
	}
	attempts := 0
	bridge.historyWatchPathHook = func(context.Context, string) error {
		attempts++
		return teamstore.ErrControlLeaseNotHeld
	}
	err := bridge.runHistoryWatchSyncJobs(context.Background(), paths, time.Now())
	if !teamstore.IsProcessWideStateError(err) {
		t.Fatalf("history worker error = %v, want process-wide lease error", err)
	}
	if attempts != 1 {
		t.Fatalf("history workers started %d queued jobs after lease loss, want 1", attempts)
	}
}

func TestBacklogHistoryFairCursorPersistsAfterOrdinaryFailure(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	root := t.TempDir()
	now := time.Now().UTC()
	paths := make([]string, 5)
	for i := range paths {
		paths[i] = filepath.Join(root, fmt.Sprintf("recovery-%d.jsonl", i))
		if err := os.WriteFile(paths[i], []byte("history tail\n"), 0o600); err != nil {
			t.Fatalf("write recovery path %d: %v", i, err)
		}
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlLease = teamstore.ControlLease{
			HolderMachineID: "history-fair-owner", Generation: 12,
			Status: teamstore.ControlLeaseStatusActive, LeaseUntil: now.Add(time.Hour),
		}
		for _, path := range paths {
			state.HistoryWatch[historyWatchCheckpointID(path)] = teamstore.HistoryWatchCheckpoint{
				ID: historyWatchCheckpointID(path), Path: path, Size: 0, Offset: 0,
				PendingRootTaskStarted: true, UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed backlog history recovery state: %v", err)
	}
	bridge := &Bridge{store: store, transcriptSyncWorkerCount: 1}
	wantErr := errors.New("ordinary history recovery failure")
	attempts := 0
	bridge.historyWatchPathHook = func(context.Context, string) error {
		attempts++
		return wantErr
	}
	ownerCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{
		MachineID: "history-fair-owner", LeaseGeneration: 12,
	})

	if err := bridge.syncCodexHistoryFinalsForBacklogWithDiscovery(ownerCtx, now, false); !errors.Is(err, wantErr) {
		t.Fatalf("backlog recovery error = %v, want ordinary worker error", err)
	}
	if attempts != maxBacklogHistoryRecoveryJobs {
		t.Fatalf("backlog recovery attempted %d paths, want bounded quantum %d", attempts, maxBacklogHistoryRecoveryJobs)
	}
	control, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("read persisted backlog fairness cursor: %v", err)
	}
	wantCursor := uniqueSortedCleanPaths(paths)[maxBacklogHistoryRecoveryJobs-1]
	if control.BacklogHistoryRecoveryFairCursor != wantCursor {
		t.Fatalf("persisted history recovery cursor = %q, want %q", control.BacklogHistoryRecoveryFairCursor, wantCursor)
	}
	if dirty := bridge.historyWatchPendingDirtyPaths(); len(dirty) != maxBacklogHistoryRecoveryJobs {
		t.Fatalf("ordinary failure retained %d selected dirty paths, want %d: %v", len(dirty), maxBacklogHistoryRecoveryJobs, dirty)
	}
}

func TestLinkedTranscriptWorkerStopsQueuedJobsAfterProcessWideFailure(t *testing.T) {
	bridge := &Bridge{transcriptSyncWorkerCount: 1}
	jobs := make([]linkedTranscriptSyncJob, 8)
	for i := range jobs {
		jobs[i].session = Session{ID: fmt.Sprintf("linked-queued-%d", i)}
	}
	attempts := 0
	bridge.linkedTranscriptSessionHook = func(context.Context, Session) error {
		attempts++
		return teamstore.ErrControlLeaseNotHeld
	}
	err := bridge.runLinkedTranscriptSyncJobs(context.Background(), jobs, func(context.Context, Session, teamstore.ImportCheckpoint) (teamstore.State, error) {
		t.Fatal("loadState ran after process-wide linked worker failure")
		return teamstore.State{}, nil
	})
	if !teamstore.IsProcessWideStateError(err) {
		t.Fatalf("linked worker error = %v, want process-wide lease error", err)
	}
	if attempts != 1 {
		t.Fatalf("linked workers started %d queued jobs after lease loss, want 1", attempts)
	}
}

func TestHistoryWatchWorkerFailureRetriesWithoutSecondEvent(t *testing.T) {
	root := newBridgeTestCodexRoot(t)
	now := time.Now().UTC()
	path := filepath.Join(root, "sessions", now.Format("2006"), now.Format("01"), now.Format("02"), "retry-without-event.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create history directory: %v", err)
	}
	initial := listenerRecoveryTranscriptLine("retry-initial", "baseline")
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat initial transcript: %v", err)
	}
	checkpoint := listenerRecoveryHistoryCheckpoint(path, "retry-session", "retry-thread", info)
	store := newBridgeTestStore(t)
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		history[checkpoint.ID] = checkpoint
		*ready = now.Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatalf("seed history-watch checkpoint: %v", err)
	}
	if err := os.WriteFile(path, []byte(initial+listenerRecoveryTranscriptLine("retry-tail", "tail after first scan")), 0o600); err != nil {
		t.Fatalf("append transcript: %v", err)
	}

	stub := &historyWatchEventSourceStub{dirty: []string{path}}
	bridge := &Bridge{
		store:                     store,
		scope:                     teamstore.ScopeIdentity{CodexHome: root},
		historyWatchEvents:        stub,
		transcriptSyncWorkerCount: 1,
	}
	wantErr := errors.New("transient history worker failure")
	attempts := 0
	bridge.historyWatchPathHook = func(context.Context, string) error {
		attempts++
		if attempts == 1 {
			return wantErr
		}
		return nil
	}

	if err := bridge.syncCodexHistoryFinals(context.Background(), now, false); !errors.Is(err, wantErr) {
		t.Fatalf("first history-watch cycle error = %v, want %v", err, wantErr)
	}
	first, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read checkpoint after failed cycle: %v", err)
	}
	if got := first.HistoryWatch[checkpoint.ID].Offset; got != checkpoint.Offset {
		t.Fatalf("failed cycle advanced offset to %d, want %d", got, checkpoint.Offset)
	}

	// No second event is injected. The retry lane plus the durable offset<size
	// rule must make the unchanged path eligible for the next cycle.
	if err := bridge.syncCodexHistoryFinals(context.Background(), now.Add(time.Second), false); err != nil {
		t.Fatalf("retry history-watch cycle: %v", err)
	}
	second, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read checkpoint after retry cycle: %v", err)
	}
	finalInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat completed transcript: %v", err)
	}
	if got := second.HistoryWatch[checkpoint.ID].Offset; got != finalInfo.Size() {
		t.Fatalf("retry cycle offset = %d, want EOF %d", got, finalInfo.Size())
	}
	if attempts != 2 {
		t.Fatalf("history worker attempts = %d, want failed attempt plus retry", attempts)
	}
	if dirty, uncertain := bridge.historyWatchDirtyPaths(nil, nil, false); uncertain || len(dirty) != 0 {
		t.Fatalf("successful retry left dirty paths=%#v uncertain=%t", dirty, uncertain)
	}
}
