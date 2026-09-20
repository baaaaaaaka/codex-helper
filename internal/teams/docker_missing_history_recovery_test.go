package teams

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// TestDockerMissingHistoryRecovery is an opt-in acceptance test for the
// copied real-data fixture. It uses the real SQLite schema/projection and real
// Codex JSONL files, but a local Graph recorder; no Teams credential or real
// POST can enter this test. The runner must mount the copied Codex tree at
// /home/baka/.codex so persisted source paths stay unchanged.
func TestDockerMissingHistoryRecovery(t *testing.T) {
	if os.Getenv("CXP_TEAMS_DOCKER_MISSING_HISTORY_RECOVERY") != "1" {
		t.Skip("set CXP_TEAMS_DOCKER_MISSING_HISTORY_RECOVERY=1 to run the copied real-data recovery acceptance test")
	}
	fixtureRoot := dockerTeamsFixtureRoot(t)
	store, _ := prepareDockerFixtureStore(t, fixtureRoot)
	t.Log("Docker missing-history fixture store prepared")
	dockerFixtureRemapCodexPaths(t, store)
	t.Log("Docker missing-history fixture paths sanitized")
	const codexHome = "/home/baka/.codex"
	now := time.Date(2026, 9, 22, 9, 0, 0, 0, time.UTC)
	paths := []string{
		filepath.Join(codexHome, "sessions", "2026", "09", "18", "rollout-2026-09-18T14-57-10-01a0b34e-0898-78e2-b28f-d01814db435a.jsonl"),
		filepath.Join(codexHome, "sessions", "2026", "09", "18", "rollout-2026-09-18T16-11-23-01a0b391-f857-7861-a9ba-3a8eec856b00.jsonl"),
		filepath.Join(codexHome, "sessions", "2026", "09", "18", "rollout-2026-09-18T16-23-05-01a0b39c-b167-7ac2-92e9-e843067d2abb.jsonl"),
	}
	threads := []string{
		"01a0b34e-0898-78e2-b28f-d01814db435a",
		"01a0b391-f857-7861-a9ba-3a8eec856b00",
		"01a0b39c-b167-7ac2-92e9-e843067d2abb",
	}
	// The copied JSONL files necessarily have new inodes. Rebind the durable
	// source proofs before deleting the three target checkpoints; otherwise the
	// test would turn every inherited checkpoint into a synthetic source-rewrite
	// event and measure thousands of fixture-only checkpoint writes instead of
	// the missing-history recovery lane.
	allowedSourcePaths := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		allowedSourcePaths[filepath.Clean(path)] = struct{}{}
	}
	dockerFixtureRebindSourceProofsForPaths(t, store, allowedSourcePaths)
	t.Log("Docker missing-history witness source proofs rebound")
	discoveredPaths := make(map[string]bool, len(paths))
	discoveryCalls := 0
	discoverySessions := 0
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat copied missing-history witness %q: %v", path, err)
		}
		// The deterministic clock is intentionally four days after the source
		// files' 2026-09-18 session date. This keeps the acceptance test focused
		// on the recovery window boundary instead of depending on the wall clock
		// at the time the Docker fixture happens to run.
		if !info.ModTime().Before(now.Add(-3 * 24 * time.Hour)) {
			t.Fatalf("missing-history witness %q is not older than the three-day hot window: mtime=%s now=%s", path, info.ModTime().UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
		}
	}
	previousDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(ctx context.Context, codexHome string) ([]codexhistory.Project, error) {
		discoveryCalls++
		projects, err := previousDiscover(ctx, codexHome)
		if err != nil {
			return projects, err
		}
		for _, project := range projects {
			for _, session := range project.Sessions {
				discoverySessions++
				discoveredPaths[filepath.Clean(session.FilePath)] = true
			}
		}
		t.Logf("Docker recovery discovery call=%d returned projects=%d cumulative_sessions=%d targets=%d", discoveryCalls, len(projects), discoverySessions, len(discoveredPaths))
		return projects, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = previousDiscover })

	// This focused witness fixture deliberately keeps the operational tables from
	// the real database, but narrows the HistoryWatch projection to the three
	// copied witnesses. The shell runner does not copy the other historical
	// JSONLs, so leaving their durable paths in this projection would make the
	// first reconcile scan hundreds of absent files and spend the test's budget
	// on expected fixture omissions. Removing those projection rows is a
	// disposable runtime-only setup operation; it does not touch the source
	// fixture or the live database. The full chat-coverage and throughput modes
	// retain the complete referenced history corpus and exercise the full
	// projection.
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		for id := range history {
			delete(history, id)
		}
		*ready = now.Add(-time.Hour)
		return nil
	}); err != nil {
		t.Fatalf("reset disposable history-watch projection: %v", err)
	}
	t.Log("Docker missing-history witness checkpoints removed")
	graph, _ := newBridgeCreateChatGraph(t, nil)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.scope.CodexHome = codexHome
	// The real copied store contains other old, currently-unindexed JSONL files.
	// Position only the in-memory fair cursor at this deterministic three-file
	// witness so the first bounded recovery quantum exercises the target route;
	// this does not bypass discovery, source proofs, session publication, or CAS.
	// Subsequent cycles use the ordinary hot watcher and therefore do not pay a
	// second full reconcile merely to re-assert an already durable checkpoint.
	root, err := codexhistory.ResolveCodexDir(codexHome)
	if err != nil {
		t.Fatalf("resolve copied Codex root for recovery cursor setup: %v", err)
	}
	recoveryFiles, err := historyTieredListSessionFilesInDirsContext(context.Background(), historyWatchRecentSessionDirs(root, now, historyWatchRecoveryDays))
	if err != nil {
		t.Fatalf("list copied recovery-window files for recovery cursor setup: %v", err)
	}
	stateBeforeDiscovery, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("read copied history-watch state for recovery cursor setup: %v", err)
	}
	candidates := historyWatchUnindexedPaths(recoveryFiles, stateBeforeDiscovery)
	cursor := -1
	for index, candidate := range candidates {
		if candidate == cleanComparablePath(paths[0]) {
			cursor = index
			break
		}
	}
	if cursor < 0 {
		t.Fatalf("copied recovery-window discovery candidates do not contain witness %q", paths[0])
	}
	bridge.backlogMaintenanceMu.Lock()
	bridge.backlogHistoryDiscoveryCursor = cursor
	bridge.backlogMaintenanceMu.Unlock()
	checkpointsAtStart := make(map[string]int64, len(paths))
	for cycle := 0; cycle < 3; cycle++ {
		reconcile := cycle == 0
		if err := bridge.syncCodexHistoryFinals(context.Background(), now.Add(time.Duration(cycle)*10*time.Second), reconcile); err != nil {
			if remaining := suppressHistoryWatchJobDeferrals(err); remaining != nil {
				t.Fatalf("Docker missing-history reconcile cycle %d had non-deferred error: %v", cycle+1, remaining)
			}
			t.Logf("Docker missing-history reconcile cycle %d had bounded path deferrals; continuing to verify target progress: %v", cycle+1, err)
		}
		state, err := store.HistoryWatchState(context.Background())
		if err != nil {
			t.Fatalf("read Docker recovery history-watch state after cycle %d: %v", cycle+1, err)
		}
		for i, path := range paths {
			checkpoint, ok := state.HistoryWatch[historyWatchCheckpointID(path)]
			if !ok || checkpoint.Offset <= 0 || checkpoint.Offset > checkpoint.Size || checkpoint.SourceRewriteBlocked {
				t.Fatalf("Docker recovery checkpoint %q after cycle %d = %#v, want bounded forward progress", path, cycle+1, checkpoint)
			}
			if bridge.reg.SessionByCodexThreadID(threads[i]) == nil {
				t.Fatalf("Docker recovery did not register thread %q", threads[i])
			}
			if cycle == 0 {
				checkpointsAtStart[path] = checkpoint.Offset
			} else if checkpoint.Offset < checkpointsAtStart[path] {
				t.Fatalf("Docker recovery checkpoint %q regressed from %d to %d", path, checkpointsAtStart[path], checkpoint.Offset)
			}
		}
	}
	// Do not run the global test flush against this production-sized fixture:
	// its inherited sent history intentionally exceeds the bounded canonical
	// fallback window. Flush only the three newly-created target chats, which
	// preserves the real per-chat FIFO boundary without allowing unrelated old
	// rows to decide whether discovery succeeded.
	for _, threadID := range threads {
		session := bridge.reg.SessionByCodexThreadID(threadID)
		if session == nil {
			t.Fatalf("Docker recovery did not register thread %q before targeted flush", threadID)
		}
		if err := bridge.flushPendingOutboxForChat(context.Background(), session.ChatID); err != nil && !isOutboxDeliveryDeferred(err) {
			t.Fatalf("flush recovered chat %q: %v", session.ChatID, err)
		}
	}
	for i, path := range paths {
		if !discoveredPaths[filepath.Clean(path)] {
			t.Fatalf("Docker real-data discovery did not return copied session %q", path)
		}
		session := bridge.reg.SessionByCodexThreadID(threads[i])
		if session == nil || strings.TrimSpace(session.ChatID) == "" {
			t.Fatalf("Docker real-data discovery did not create a usable Teams chat for %q: %#v", threads[i], session)
		}
		checkpoint, found, err := store.ImportCheckpoint(context.Background(), transcriptCheckpointID(session.ID))
		if err != nil {
			t.Fatalf("read import checkpoint for discovered thread %q: %v", threads[i], err)
		}
		if !found || checkpoint.SessionID != session.ID {
			t.Fatalf("discovered thread %q has no durable transcript checkpoint: found=%t checkpoint=%#v", threads[i], found, checkpoint)
		}
	}
	if discoveryCalls > 12 {
		t.Fatalf("Docker recovery repeated full Codex discovery %d times for three bounded cycles; want batch-scoped discovery, not one scan per history path", discoveryCalls)
	}
	registry := bridge.registrySnapshot()
	for _, threadID := range threads {
		count := 0
		for _, session := range registry.Sessions {
			if session.CodexThreadID == threadID {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("Docker real-data discovery registered thread %q %d times, want exactly once", threadID, count)
		}
	}
}
