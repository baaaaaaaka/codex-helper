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

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func writeMissingHistoryFixture(t *testing.T, path, threadID, prompt, answer string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir fixture directory: %v", err)
	}
	body := fmt.Sprintf(
		`{"type":"session_meta","payload":{"id":%q}}`+"\n"+
			`{"thread_id":%q,"turn_id":"turn-1","id":"u1","role":"user","text":%q}`+"\n"+
			`{"thread_id":%q,"turn_id":"turn-1","id":"a1","role":"assistant","text":%q}`+"\n"+
			`{"type":"turn.completed","thread_id":%q,"turn_id":"turn-1"}`+"\n",
		threadID, threadID, prompt, threadID, answer, threadID,
	)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

func TestHistoryWatchReconcileDiscoversMissedMonthButBaselinesOlderHistory(t *testing.T) {
	now := time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
	root := newBridgeTestCodexRoot(t)
	monthPath := filepath.Join(root, "sessions", "2026", "05", "01", "rollout-month-thread-month.jsonl")
	oldPath := filepath.Join(root, "sessions", "2026", "04", "01", "rollout-old-thread-old.jsonl")
	writeMissingHistoryFixture(t, monthPath, "thread-month", "month prompt", "month final")
	writeMissingHistoryFixture(t, oldPath, "thread-old", "old prompt", "old final")

	previousDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(context.Context, string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "fixture",
			Path: "/home/test/project",
			Sessions: []codexhistory.Session{
				{SessionID: "thread-month", ProjectPath: "/home/test/project", FilePath: monthPath, ModifiedAt: now.Add(-10 * 24 * time.Hour)},
				{SessionID: "thread-old", ProjectPath: "/home/test/project", FilePath: oldPath, ModifiedAt: now.Add(-40 * 24 * time.Hour)},
			},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = previousDiscover })

	graph, sent := newBridgeCreateChatGraph(t, nil)
	store := newBridgeTestStore(t)
	if err := store.UpdateHistoryWatch(context.Background(), func(_ map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		*ready = now.Add(-time.Hour)
		return nil
	}); err != nil {
		t.Fatalf("seed initialized history-watch state: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.scope.CodexHome = root

	if err := bridge.syncCodexHistoryFinals(context.Background(), now, true); err != nil {
		t.Fatalf("reconcile missed-month history: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)

	if bridge.reg.SessionByCodexThreadID("thread-month") == nil {
		t.Fatal("month-window session was baselined or otherwise not discovered")
	}
	if sentPlainContains(*sent, "old final") {
		t.Fatalf("history older than recovery window was published: %#v", *sent)
	}
	if !sentPlainContains(*sent, "month final") {
		t.Fatalf("month-window final was not published: %#v", *sent)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read history-watch state: %v", err)
	}
	if _, ok := state.HistoryWatch[historyWatchCheckpointID(monthPath)]; !ok {
		t.Fatalf("month-window checkpoint missing: %#v", state.HistoryWatch)
	}
	oldCheckpoint, ok := state.HistoryWatch[historyWatchCheckpointID(oldPath)]
	if !ok || oldCheckpoint.Offset != oldCheckpoint.Size {
		t.Fatalf("older history was not EOF-baselined: %#v", state.HistoryWatch)
	}
}

func TestHistoryWatchReconcileRecoveryBatchCompletesAcrossCyclesWithoutDuplicate(t *testing.T) {
	now := time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
	root := newBridgeTestCodexRoot(t)
	projects := make([]codexhistory.Session, 0, maxHistoryRecoveryDiscoveryJobs+1)
	for i := 0; i < maxHistoryRecoveryDiscoveryJobs+1; i++ {
		threadID := fmt.Sprintf("thread-recovery-%d", i)
		path := filepath.Join(root, "sessions", "2026", "05", fmt.Sprintf("%02d", 1+i), fmt.Sprintf("rollout-%s.jsonl", threadID))
		answer := "answer-" + threadID
		writeMissingHistoryFixture(t, path, threadID, "prompt-"+threadID, answer)
		projects = append(projects, codexhistory.Session{SessionID: threadID, ProjectPath: "/home/test/project", FilePath: path, ModifiedAt: now.Add(-10 * 24 * time.Hour)})
	}
	previousDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(context.Context, string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{Key: "fixture", Path: "/home/test/project", Sessions: projects}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = previousDiscover })

	graph, sent := newBridgeCreateChatGraph(t, nil)
	store := newBridgeTestStore(t)
	if err := store.UpdateHistoryWatch(context.Background(), func(_ map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		*ready = now.Add(-time.Hour)
		return nil
	}); err != nil {
		t.Fatalf("seed initialized history-watch state: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.scope.CodexHome = root

	for cycle := 0; cycle < 2; cycle++ {
		if err := bridge.syncCodexHistoryFinals(context.Background(), now.Add(time.Duration(cycle)*time.Minute), true); err != nil {
			t.Fatalf("reconcile recovery cycle %d: %v", cycle+1, err)
		}
		flushBridgeQueuedNotificationsForTest(t, bridge)
	}

	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read completed recovery state: %v", err)
	}
	for _, session := range projects {
		checkpoint := state.HistoryWatch[historyWatchCheckpointID(session.FilePath)]
		if checkpoint.Path != cleanComparablePath(session.FilePath) || checkpoint.Offset != checkpoint.Size {
			t.Fatalf("recovery checkpoint for %q = %#v, want EOF", session.SessionID, checkpoint)
		}
		if bridge.reg.SessionByCodexThreadID(session.SessionID) == nil {
			t.Fatalf("recovery session %q was not registered", session.SessionID)
		}
		if got := countSentPlainContaining(*sent, "answer-"+session.SessionID); got != 1 {
			t.Fatalf("recovery answer %q was sent %d times, want exactly once", session.SessionID, got)
		}
	}
	registry := bridge.registrySnapshot()
	for _, session := range projects {
		count := 0
		for _, registered := range registry.Sessions {
			if registered.CodexThreadID == session.SessionID {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("recovery thread %q has %d registry sessions, want exactly one", session.SessionID, count)
		}
	}
}

func TestHistoryWatchReconcileDiscoveryIsBoundedAndFair(t *testing.T) {
	now := time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
	root := newBridgeTestCodexRoot(t)
	projects := make([]codexhistory.Session, 0, maxHistoryRecoveryDiscoveryJobs+1)
	for i := 0; i < maxHistoryRecoveryDiscoveryJobs+1; i++ {
		threadID := fmt.Sprintf("thread-month-%d", i)
		path := filepath.Join(root, "sessions", "2026", "05", fmt.Sprintf("%02d", 1+i), fmt.Sprintf("rollout-month-%s.jsonl", threadID))
		writeMissingHistoryFixture(t, path, threadID, "prompt", "answer")
		projects = append(projects, codexhistory.Session{SessionID: threadID, ProjectPath: "/home/test/project", FilePath: path, ModifiedAt: now.Add(-10 * 24 * time.Hour)})
	}
	previousDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(context.Context, string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{Key: "fixture", Path: "/home/test/project", Sessions: projects}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = previousDiscover })

	store := newBridgeTestStore(t)
	if err := store.UpdateHistoryWatch(context.Background(), func(_ map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		*ready = now.Add(-time.Hour)
		return nil
	}); err != nil {
		t.Fatalf("seed initialized history-watch state: %v", err)
	}
	bridge := &Bridge{store: store, scope: teamstore.ScopeIdentity{CodexHome: root}, transcriptSyncWorkerCount: 1}
	var attempted []string
	bridge.historyWatchPathHook = func(_ context.Context, path string) error {
		attempted = append(attempted, path)
		return errors.New("bounded fixture stop")
	}

	err := bridge.syncCodexHistoryFinals(context.Background(), now, true)
	if err == nil || len(attempted) != maxHistoryRecoveryDiscoveryJobs {
		t.Fatalf("reconcile discovery attempts=%d err=%v, want bounded batch=%d", len(attempted), err, maxHistoryRecoveryDiscoveryJobs)
	}
	if !reflect.DeepEqual(attempted, uniqueSortedCleanPaths(attempted)) {
		t.Fatalf("discovery attempts were not deterministic/unique: %#v", attempted)
	}
}

func TestHistoryWatchReconcilePrioritizesUnindexedRecoveryOverChangedColdPaths(t *testing.T) {
	now := time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
	root := newBridgeTestCodexRoot(t)
	coldPath := filepath.Join(root, "sessions", "2026", "05", "01", "rollout-cold.jsonl")
	targetPath := filepath.Join(root, "sessions", "2026", "05", "05", "rollout-target.jsonl")
	writeMissingHistoryFixture(t, coldPath, "thread-cold", "cold prompt", "cold answer")
	writeMissingHistoryFixture(t, targetPath, "thread-target", "target prompt", "target answer")

	previousDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(context.Context, string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{Key: "fixture", Path: "/home/test/project", Sessions: []codexhistory.Session{
			{SessionID: "thread-cold", ProjectPath: "/home/test/project", FilePath: coldPath, ModifiedAt: now.Add(-10 * 24 * time.Hour)},
			{SessionID: "thread-target", ProjectPath: "/home/test/project", FilePath: targetPath, ModifiedAt: now.Add(-6 * 24 * time.Hour)},
		}}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = previousDiscover })

	store := newBridgeTestStore(t)
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		history[historyWatchCheckpointID(coldPath)] = teamstore.HistoryWatchCheckpoint{
			ID:   historyWatchCheckpointID(coldPath),
			Path: coldPath,
		}
		*ready = now.Add(-time.Hour)
		return nil
	}); err != nil {
		t.Fatalf("seed initialized history-watch state: %v", err)
	}
	bridge := &Bridge{store: store, scope: teamstore.ScopeIdentity{CodexHome: root}, transcriptSyncWorkerCount: 1}
	var attempted []string
	bridge.historyWatchPathHook = func(_ context.Context, path string) error {
		attempted = append(attempted, path)
		return errors.New("bounded fixture stop")
	}

	err := bridge.syncCodexHistoryFinals(context.Background(), now, true)
	if err == nil || len(attempted) < 2 {
		t.Fatalf("reconcile attempts=%v err=%v, want both changed cold and unindexed recovery paths", attempted, err)
	}
	if attempted[0] != cleanComparablePath(targetPath) {
		t.Fatalf("first reconcile attempt=%q, want unindexed recovery path %q before cold path %q", attempted[0], cleanComparablePath(targetPath), cleanComparablePath(coldPath))
	}
}

func TestBacklogHistoryFairnessDiscoversMonthWindowPath(t *testing.T) {
	now := time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
	root := newBridgeTestCodexRoot(t)
	path := filepath.Join(root, "sessions", "2026", "05", "01", "rollout-month-backlog.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir month-window directory: %v", err)
	}
	if err := os.WriteFile(path, []byte("month-window\n"), 0o600); err != nil {
		t.Fatalf("write month-window fixture: %v", err)
	}
	store := newBridgeTestStore(t)
	bridge := &Bridge{store: store, scope: teamstore.ScopeIdentity{CodexHome: root}, transcriptSyncWorkerCount: 1}
	var selected string
	bridge.historyWatchPathHook = func(_ context.Context, path string) error {
		selected = path
		return context.Canceled
	}
	err := bridge.syncCodexHistoryFinalsForBacklogWithDiscovery(context.Background(), now, true)
	if selected != cleanComparablePath(path) {
		t.Fatalf("backlog fairness selected %q, want month-window path %q; err=%v", selected, cleanComparablePath(path), err)
	}
}

func TestBacklogFairCursorUsesLexicalSuccessorWhenCursorWasRemoved(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlLease = teamstore.ControlLease{HolderMachineID: "cursor-owner", Generation: 7, Status: teamstore.ControlLeaseStatusActive, LeaseUntil: now.Add(time.Hour)}
		state.ServiceControl.BacklogHistoryDiscoveryFairCursor = filepath.Join(root, "b")
		return nil
	}); err != nil {
		t.Fatalf("seed cursor state: %v", err)
	}
	ownerCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{MachineID: "cursor-owner", LeaseGeneration: 7})
	bridge := &Bridge{store: store}
	keys := []string{filepath.Join(root, "a"), filepath.Join(root, "c"), filepath.Join(root, "d")}
	if err := bridge.restoreBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneHistoryDiscovery, keys); err != nil {
		t.Fatalf("restore removed cursor: %v", err)
	}
	want := []string{cleanComparablePath(filepath.Join(root, "c"))}
	if got := bridge.selectBacklogHistoryDiscoveryPaths(keys, 1); !reflect.DeepEqual(got, want) {
		t.Fatalf("selected successor = %#v, want [c]", got)
	}
}

func TestSeedHistoryWatchWakeCheckpointsIsExplicitIdempotentAndDoesNotRewriteExisting(t *testing.T) {
	root := newBridgeTestCodexRoot(t)
	first := filepath.Join(root, "sessions", "2026", "05", "01", "rollout-first.jsonl")
	second := filepath.Join(root, "sessions", "2026", "05", "02", "rollout-second.jsonl")
	writeMissingHistoryFixture(t, first, "thread-first", "first prompt", "first answer")
	writeMissingHistoryFixture(t, second, "thread-second", "second prompt", "second answer")
	store := newBridgeTestStore(t)
	now := time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
	if err := store.UpdateHistoryWatch(context.Background(), func(_ map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		*ready = now.Add(-time.Hour)
		return nil
	}); err != nil {
		t.Fatalf("initialize history-watch state: %v", err)
	}

	report, err := seedHistoryWatchWakeCheckpoints(context.Background(), store, root, []string{second, first, first}, now)
	if err != nil {
		t.Fatalf("seed history wake checkpoints: %v", err)
	}
	if !reflect.DeepEqual(report.Added, []string{cleanComparablePath(first), cleanComparablePath(second)}) || len(report.Existing) != 0 {
		t.Fatalf("first wake report = %#v, want two added paths", report)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read first wake state: %v", err)
	}
	for _, path := range []string{first, second} {
		checkpoint, ok := state.HistoryWatch[historyWatchCheckpointID(path)]
		if !ok || checkpoint.Path != cleanComparablePath(path) || checkpoint.Offset != 0 || checkpoint.Size != 0 {
			t.Fatalf("wake checkpoint for %q = %#v, want zero physical cursor", path, checkpoint)
		}
	}

	const preservedOffset = 19
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		checkpoint := history[historyWatchCheckpointID(first)]
		checkpoint.Offset = preservedOffset
		checkpoint.Size = preservedOffset
		history[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed existing checkpoint mutation: %v", err)
	}
	report, err = seedHistoryWatchWakeCheckpoints(context.Background(), store, root, []string{first, second}, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("repeat history wake checkpoints: %v", err)
	}
	if len(report.Added) != 0 || !reflect.DeepEqual(report.Existing, []string{cleanComparablePath(first), cleanComparablePath(second)}) {
		t.Fatalf("repeat wake report = %#v, want only existing paths", report)
	}
	state, err = store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read repeated wake state: %v", err)
	}
	if got := state.HistoryWatch[historyWatchCheckpointID(first)].Offset; got != preservedOffset {
		t.Fatalf("repeat wake rewrote existing offset to %d, want %d", got, preservedOffset)
	}

	before := len(state.HistoryWatch)
	if _, err := seedHistoryWatchWakeCheckpoints(context.Background(), store, root, []string{filepath.Join(root, "outside.jsonl")}, now); err == nil {
		t.Fatal("outside/missing path was accepted by history wake repair")
	}
	after, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read invalid-path wake state: %v", err)
	}
	if len(after.HistoryWatch) != before {
		t.Fatalf("invalid-path repair changed history-watch count from %d to %d", before, len(after.HistoryWatch))
	}

	uninitializedStore := newBridgeTestStore(t)
	if _, err := seedHistoryWatchWakeCheckpoints(context.Background(), uninitializedStore, root, []string{first}, now); err == nil {
		t.Fatal("uninitialized history-watch store accepted a wake repair")
	}
	uninitializedState, err := uninitializedStore.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read uninitialized wake state: %v", err)
	}
	if len(uninitializedState.HistoryWatch) != 0 || !uninitializedState.HistoryWatchReady.IsZero() {
		t.Fatalf("uninitialized wake repair changed state: ready=%s history=%#v", uninitializedState.HistoryWatchReady, uninitializedState.HistoryWatch)
	}
}
