package teams

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// runHistoryWatchSyncJobs gives each changed transcript path its own bounded
// worker. HistoryWatch is a cold path, so this is intentionally a small pool;
// its purpose is fairness and fault isolation, not unrestricted parallelism.
func (b *Bridge) runHistoryWatchSyncJobs(ctx context.Context, paths []string, now time.Time) error {
	if len(paths) == 0 {
		return nil
	}
	workerCount := b.transcriptSyncWorkerCount
	if workerCount <= 0 || workerCount > maxConcurrentTranscriptSyncs {
		workerCount = maxConcurrentTranscriptSyncs
	}
	if workerCount > len(paths) {
		workerCount = len(paths)
	}
	type result struct {
		index int
		err   error
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int, len(paths))
	results := make(chan result, len(paths))
	for index := range paths {
		jobs <- index
	}
	close(jobs)
	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				path := paths[index]
				jobCtx, jobCancel := boundedTeamsPhaseJobContext(workCtx)
				var err error
				if b.historyWatchPathHook != nil {
					err = b.historyWatchPathHook(jobCtx, path)
				}
				if err == nil {
					err = b.syncCodexHistoryWatchPath(jobCtx, path, now)
				}
				jobCancel()
				results <- result{index: index, err: err}
				if teamstore.IsProcessWideStateError(err) {
					cancel()
				}
			}
		}()
	}
	workers.Wait()
	close(results)
	errs := make([]error, len(paths))
	for item := range results {
		if item.index >= 0 && item.index < len(errs) {
			errs[item.index] = item.err
		}
	}
	joined := make([]error, 0, len(errs))
	for index, err := range errs {
		if err != nil {
			joined = append(joined, fmt.Errorf("history watch path %q: %w", strings.TrimSpace(paths[index]), err))
		}
	}
	return errors.Join(joined...)
}

// history-watch scans are performed outside the store transaction and may
// overlap a control-lease takeover.  Use the same owner capability as the
// live transcript path at every durable history-watch boundary; direct unit
// callers without an active listener lease retain the unscoped compatibility
// path.
func (b *Bridge) updateHistoryWatch(ctx context.Context, fn func(map[string]teamstore.HistoryWatchCheckpoint, *time.Time) error) error {
	if machineID, generation, ownerBound := b.transcriptCheckpointOwnerCapabilityForContext(ctx); ownerBound {
		return b.store.UpdateHistoryWatchForOwner(ctx, machineID, generation, fn)
	}
	return b.store.UpdateHistoryWatch(ctx, fn)
}

func (b *Bridge) updateHistoryWatchCheckpointIfCurrent(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, next teamstore.HistoryWatchCheckpoint) error {
	if machineID, generation, ownerBound := b.transcriptCheckpointOwnerCapabilityForContext(ctx); ownerBound {
		return b.store.UpdateHistoryWatchCheckpointIfCurrentForOwner(ctx, id, expected, next, machineID, generation)
	}
	return b.store.UpdateHistoryWatchCheckpointIfCurrent(ctx, id, expected, next)
}

func (b *Bridge) deleteHistoryWatchCheckpointIfCurrent(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint) error {
	if machineID, generation, ownerBound := b.transcriptCheckpointOwnerCapabilityForContext(ctx); ownerBound {
		return b.store.DeleteHistoryWatchCheckpointIfCurrentForOwner(ctx, id, expected, machineID, generation)
	}
	return b.store.DeleteHistoryWatchCheckpointIfCurrent(ctx, id, expected)
}

func (b *Bridge) historyWatchDirtyPaths(fullPaths, incrementalPaths []string, prune bool) ([]string, bool) {
	if b == nil {
		return nil, true
	}
	b.historyWatchEventsMu.Lock()
	defer b.historyWatchEventsMu.Unlock()
	if b.historyWatchEvents == nil {
		b.historyWatchEvents = newHistoryWatchEventSource()
		// A newly created watcher has no state to retain. It must receive the
		// complete indexed set even during an otherwise incremental cycle.
		prune = true
		incrementalPaths = fullPaths
	}
	if b.historyWatchPendingDirty == nil {
		b.historyWatchPendingDirty = make(map[string]struct{})
	}
	paths := incrementalPaths
	if prune {
		paths = fullPaths
	}
	dirty, uncertain, err := b.historyWatchEvents.Update(paths, prune)
	if err != nil {
		_ = b.historyWatchEvents.Close()
		b.historyWatchEvents = nil
		return nil, true
	}
	for _, path := range dirty {
		if path = cleanComparablePath(path); path != "" {
			b.historyWatchPendingDirty[path] = struct{}{}
		}
	}
	if len(b.historyWatchPendingDirty) == 0 {
		return nil, uncertain
	}
	allDirty := make([]string, 0, len(b.historyWatchPendingDirty))
	for path := range b.historyWatchPendingDirty {
		allDirty = append(allDirty, path)
	}
	sort.Strings(allDirty)
	return allDirty, uncertain
}

func (b *Bridge) closeHistoryWatchEvents() error {
	if b == nil {
		return nil
	}
	b.historyWatchEventsMu.Lock()
	defer b.historyWatchEventsMu.Unlock()
	if b.historyWatchEvents == nil {
		b.historyWatchPendingDirty = nil
		return nil
	}
	err := b.historyWatchEvents.Close()
	b.historyWatchEvents = nil
	b.historyWatchPendingDirty = nil
	return err
}

func (b *Bridge) historyWatchAckDirtyPath(path string) {
	if b == nil {
		return
	}
	path = cleanComparablePath(path)
	if path == "" {
		return
	}
	b.historyWatchEventsMu.Lock()
	defer b.historyWatchEventsMu.Unlock()
	delete(b.historyWatchPendingDirty, path)
}

func (b *Bridge) historyWatchAckDirtyPaths(paths []string) {
	for _, path := range paths {
		b.historyWatchAckDirtyPath(path)
	}
}

func (b *Bridge) historyWatchRetryDirtyPath(path string) {
	if b == nil {
		return
	}
	path = cleanComparablePath(path)
	if path == "" {
		return
	}
	b.historyWatchEventsMu.Lock()
	defer b.historyWatchEventsMu.Unlock()
	if b.historyWatchPendingDirty == nil {
		b.historyWatchPendingDirty = make(map[string]struct{})
	}
	b.historyWatchPendingDirty[path] = struct{}{}
}

func (b *Bridge) historyWatchRetryDirtyPaths(paths []string) {
	for _, path := range paths {
		b.historyWatchRetryDirtyPath(path)
	}
}

func (b *Bridge) syncCodexHistoryFinalsIfDue(ctx context.Context, now time.Time) error {
	if b == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	if !b.lastHistoryWatchSync.IsZero() && now.Sub(b.lastHistoryWatchSync) < historyWatchSyncMinInterval {
		return nil
	}
	reconcile := b.lastHistoryWatchReconcile.IsZero() || now.Sub(b.lastHistoryWatchReconcile) >= historyWatchReconcileInterval
	if reconcile && !b.lastHistoryWatchReconcileAttempt.IsZero() && now.Sub(b.lastHistoryWatchReconcileAttempt) < historyWatchReconcileRetryInterval {
		reconcile = false
	}
	if reconcile {
		// A failed reconcile must not make the full project/session discovery run
		// on every 10-second watcher tick. Keep the attempt timestamp separate
		// from the last successful baseline so a transient failure retries soon,
		// while the normal per-path watcher continues to process known files.
		b.lastHistoryWatchReconcileAttempt = now
	}
	b.lastHistoryWatchSync = now
	err := b.syncCodexHistoryFinals(ctx, now, reconcile)
	if err == nil && reconcile {
		b.lastHistoryWatchReconcile = now
	}
	return err
}

func (b *Bridge) syncCodexHistoryFinals(ctx context.Context, now time.Time, reconcile bool) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	state, err := b.store.HistoryWatchState(ctx)
	if err != nil {
		return err
	}
	initialized := !state.HistoryWatchReady.IsZero()
	paths := historyWatchPathsFromState(state)
	var recent []string
	sessionsRootMissing := false
	var firstErr error
	root, rootErr := codexhistory.ResolveCodexDir(b.scope.CodexHome)
	if rootErr != nil {
		// Keep processing already checkpointed paths when the discovery root is
		// temporarily unavailable.  Returning the error is important: a silent
		// success here makes the listener look healthy while it performs no work.
		firstErr = fmt.Errorf("resolve Codex history directory: %w", rootErr)
	} else {
		var recentErr error
		recent, recentErr = historyTieredListSessionFilesInDirs(historyWatchRecentSessionDirs(root, now, historyWatchRecentDays))
		if recentErr != nil {
			firstErr = fmt.Errorf("list recent Codex history sessions: %w", recentErr)
		} else {
			recentSet := historyWatchPathSet(recent)
			paths = append(paths, recent...)
			if reconcile {
				reconciled, reconcileErr := b.historyWatchReconcilePaths(ctx)
				if codexhistory.IsSessionsDirNotFound(reconcileErr) {
					reconcileErr = nil
					reconciled = nil
					sessionsRootMissing = true
				}
				if reconcileErr != nil {
					firstErr = joinHistoryWatchErrors(firstErr, reconcileErr)
				} else {
					if initialized {
						baseline := historyWatchMissingNonRecentPaths(reconciled, recentSet, state)
						if len(baseline) > 0 {
							if baselineErr := b.baselineCodexHistoryWatch(ctx, baseline, now); baselineErr != nil {
								firstErr = joinHistoryWatchErrors(firstErr, baselineErr)
							} else if refreshed, refreshErr := b.store.HistoryWatchState(ctx); refreshErr != nil {
								firstErr = joinHistoryWatchErrors(firstErr, refreshErr)
							} else {
								state = refreshed
							}
						}
					}
					paths = append(paths, reconciled...)
				}
			}
		}
	}
	paths = uniqueSortedCleanPaths(paths)
	if !initialized {
		if firstErr != nil {
			return firstErr
		}
		if sessionsRootMissing {
			return nil
		}
		if !reconcile {
			// The initial project discovery failed recently. Do not mark a partial
			// recent-files list as the durable baseline while the retry cooldown is
			// active; otherwise older sessions could be silently forgotten.
			return nil
		}
		return b.baselineCodexHistoryWatch(ctx, paths, now)
	}
	// The event stream is only a dirty hint. A trusted stream lets the normal
	// cycle inspect dirty paths (plus recent files that have not received a
	// durable checkpoint); startup, reconciliation, watch registration, queue
	// overflow, and recreated directories all force the complete indexed set.
	dirtyPaths, watchUncertain := b.historyWatchDirtyPaths(paths, recent, reconcile)
	scanPaths := historyWatchScanPaths(paths, recent, dirtyPaths, state, reconcile, watchUncertain)
	verifyUnchanged := reconcile || watchUncertain || len(dirtyPaths) > 0
	changes, scanErr := historyWatchChangedPaths(scanPaths, state, verifyUnchanged)
	if scanErr != nil {
		firstErr = joinHistoryWatchErrors(firstErr, scanErr)
	}
	if scanErr == nil {
		changed := make(map[string]struct{}, len(changes))
		for _, path := range changes {
			changed[cleanComparablePath(path)] = struct{}{}
		}
		for _, path := range dirtyPaths {
			if _, ok := changed[cleanComparablePath(path)]; !ok {
				b.historyWatchAckDirtyPath(path)
			}
		}
	}
	syncErr := b.runHistoryWatchSyncJobs(ctx, changes, now)
	if syncErr != nil {
		b.historyWatchRetryDirtyPaths(dirtyPaths)
		if watchUncertain {
			// A full scan caused by an uncertain watcher may discover a changed
			// path without a corresponding event. Keep that path selected until
			// its existing worker/CAS path succeeds.
			b.historyWatchRetryDirtyPaths(changes)
		}
	}
	if scanErr == nil && syncErr == nil {
		b.historyWatchAckDirtyPaths(dirtyPaths)
	}
	if syncErr != nil {
		if firstErr == nil {
			firstErr = syncErr
		} else {
			firstErr = errors.Join(firstErr, syncErr)
		}
	}
	return firstErr
}

func historyWatchScanPaths(fullPaths, recentPaths, dirtyPaths []string, state teamstore.State, reconcile, uncertain bool) []string {
	selected := make([]string, 0, len(fullPaths)+len(dirtyPaths))
	if reconcile || uncertain {
		selected = append(selected, fullPaths...)
	} else {
		selected = append(selected, dirtyPaths...)
	}
	// A bounded or partial scan may have durably retained a physical cursor
	// before EOF even though its event was already consumed. Keep such a path in
	// the next scan quantum without requiring another filesystem event; otherwise
	// a quiet writer can leave the checkpoint permanently stranded at the same
	// offset. historyWatchChangedPaths still applies the source-proof and
	// blocked/deferred filters before the worker runs.
	selected = append(selected, historyWatchResumablePaths(state)...)
	selected = append(selected, historyWatchUnindexedPaths(recentPaths, state)...)
	return uniqueSortedCleanPaths(selected)
}

func historyWatchResumablePaths(state teamstore.State) []string {
	out := make([]string, 0)
	for _, checkpoint := range state.HistoryWatch {
		path := cleanComparablePath(checkpoint.Path)
		if path == "" {
			continue
		}
		if checkpoint.SourceRewriteBlocked || checkpoint.LegacySourceUnverified || checkpoint.RecoveryProofUnusable {
			continue
		}
		if checkpoint.Offset >= 0 && checkpoint.Offset < checkpoint.Size {
			out = append(out, path)
		}
	}
	return out
}

func historyWatchUnindexedPaths(paths []string, state teamstore.State) []string {
	known := make(map[string]struct{}, len(state.HistoryWatch))
	for _, checkpoint := range state.HistoryWatch {
		if path := cleanComparablePath(checkpoint.Path); path != "" {
			known[path] = struct{}{}
		}
	}
	var out []string
	for _, path := range uniqueSortedCleanPaths(paths) {
		if _, ok := known[path]; !ok {
			out = append(out, path)
		}
	}
	return out
}

func joinHistoryWatchErrors(first error, next error) error {
	if first == nil {
		return next
	}
	if next == nil {
		return first
	}
	return errors.Join(first, next)
}

func (b *Bridge) historyWatchReconcilePaths(ctx context.Context) ([]string, error) {
	projects, err := discoverCodexProjectsForTeams(ctx, b.scope.CodexHome)
	if err != nil {
		return nil, err
	}
	projects = codexhistory.FilterUserVisibleProjects(projects)
	var paths []string
	for _, project := range projects {
		for _, session := range project.Sessions {
			if strings.TrimSpace(session.FilePath) != "" {
				paths = append(paths, session.FilePath)
			}
		}
	}
	return paths, nil
}

func historyWatchPathsFromState(state teamstore.State) []string {
	out := make([]string, 0, len(state.HistoryWatch))
	for _, checkpoint := range state.HistoryWatch {
		if strings.TrimSpace(checkpoint.Path) != "" {
			out = append(out, checkpoint.Path)
		}
	}
	return out
}

func historyWatchPathSet(paths []string) map[string]bool {
	out := make(map[string]bool, len(paths))
	for _, path := range uniqueSortedCleanPaths(paths) {
		out[path] = true
	}
	return out
}

func historyWatchMissingNonRecentPaths(paths []string, recent map[string]bool, state teamstore.State) []string {
	known := make(map[string]bool, len(state.HistoryWatch))
	for _, checkpoint := range state.HistoryWatch {
		if path := cleanComparablePath(checkpoint.Path); path != "" {
			known[path] = true
		}
	}
	var out []string
	for _, path := range uniqueSortedCleanPaths(paths) {
		if path == "" || recent[path] || known[path] {
			continue
		}
		out = append(out, path)
	}
	return out
}

func historyWatchRecentSessionDirs(root string, now time.Time, days int) []string {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	if days <= 0 {
		days = 1
	}
	sessionsRoot := filepath.Join(root, "sessions")
	dirs := []string{sessionsRoot}
	for i := 0; i < days; i++ {
		day := now.AddDate(0, 0, -i)
		dirs = append(dirs, filepath.Join(sessionsRoot, day.Format("2006"), day.Format("01"), day.Format("02")))
	}
	return uniqueSortedCleanPaths(dirs)
}

func historyWatchChangedPaths(paths []string, state teamstore.State, verifyUnchanged bool) ([]string, error) {
	states := make(map[string]historyTieredFileState, len(state.HistoryWatch))
	blockedPaths := make(map[string]bool)
	missingBlockedPaths := make(map[string]bool)
	rebasePaths := make(map[string]bool)
	deferredPaths := make(map[string]bool)
	missingDeferredPaths := make(map[string]bool)
	// Legacy HistoryWatch rows have no bounded prefix proof.  Even on the
	// normal (non-reconcile) poll, verify those rows before treating an equal
	// size/mtime as unchanged; otherwise a same-size rewrite can remain
	// invisible until the next reconcile and an old source-less outbox may be
	// flushed first.  Modern rows retain the cheap stat-only idle path.
	legacyPaths := make(map[string]bool)
	selectedOnly := paths != nil
	selectedPaths := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path = cleanComparablePath(path); path != "" {
			selectedPaths[path] = struct{}{}
		}
	}
	var firstErr error
	for _, checkpoint := range state.HistoryWatch {
		if path := cleanComparablePath(checkpoint.Path); path != "" {
			fileState := historyTieredFileStateFromHistoryWatch(checkpoint)
			fileState.Path = path
			states[path] = fileState
			if fileState.SourceRewriteBlocked {
				// A blocked checkpoint is an explicit manual-recovery boundary.
				// Until publish-history/baseline replaces it, no stat change can
				// make the path automatically trustworthy again. Excluding it here
				// avoids a repeated stat/lock/sync cycle; the sync path already
				// returns without removing or advancing such a checkpoint.
				blockedPaths[path] = true
				// Codex publishes a migrated rollout by atomically replacing the
				// same path. Only a new paginated file identity is an automatic
				// recovery candidate; legacy files retain the explicit recovery
				// boundary and are not repeatedly scanned.
				if fileState.SourceRewriteBlocked {
					if identity, ok := codexPaginatedHistoryIdentity(path, firstNonEmptyString(fileState.ThreadID, fileState.SessionID)); ok &&
						identity != strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) {
						rebasePaths[path] = true
					}
				}
				continue
			}
			if fileState.LegacySourceUnverified {
				// An inherited cursor is a silent, history-only boundary. Do not
				// rescan it on every append; explicit publish-history chooses the
				// recovery position. Keep a deletion probe so stale rows do not
				// accumulate forever.
				deferredPaths[path] = true
				continue
			}
			if fileState.RecoveryProofUnusable {
				// A malformed optional proof is retained as a silent, history-only
				// boundary. Do not let the watcher reinterpret the current file or
				// emit a live-chat blocker until explicit recovery replaces it.
				deferredPaths[path] = true
				continue
			}
			if fileState.Offset > 0 && strings.TrimSpace(fileState.SourceFingerprint) == "" {
				legacyPaths[path] = true
			}
		}
	}
	// A blocked row must not be rescanned merely because its metadata changed,
	// but a deleted source still needs one cleanup pass so its durable checkpoint
	// does not become an unbounded orphan.  This is the only filesystem probe we
	// perform for an explicitly blocked row.
	for path := range blockedPaths {
		if selectedOnly {
			if _, selected := selectedPaths[path]; !selected {
				continue
			}
		}
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			missingBlockedPaths[path] = true
			continue
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if info.IsDir() {
			continue
		}
		// A source generation is stable across append-only writes and in-place
		// repairs. If the last rebase attempt recorded only this same generation
		// but the file snapshot changed, schedule another bounded scan. Without
		// this check the marker would permanently hide a repaired anchor while
		// still correctly suppressing unchanged blocked sources.
		if !rebasePaths[path] {
			fileState := states[path]
			if marker := strings.TrimSpace(fileState.SourceRewriteRecoveryIdentity); marker != "" {
				if identity, ok := codexPaginatedHistoryIdentity(path, firstNonEmptyString(fileState.ThreadID, fileState.SessionID)); ok &&
					identity == marker && !historyRewriteRecoverySnapshotMatches(fileState, info) {
					rebasePaths[path] = true
				}
			}
		}
	}
	for path := range deferredPaths {
		if selectedOnly {
			if _, selected := selectedPaths[path]; !selected {
				continue
			}
		}
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			missingDeferredPaths[path] = true
			continue
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if info.IsDir() {
			continue
		}
	}
	if len(blockedPaths) > 0 || len(deferredPaths) > 0 {
		filtered := make([]string, 0, len(paths))
		for _, path := range paths {
			cleanPath := cleanComparablePath(path)
			if (blockedPaths[cleanPath] && !missingBlockedPaths[cleanPath] && !rebasePaths[cleanPath]) ||
				(deferredPaths[cleanPath] && !missingDeferredPaths[cleanPath]) {
				continue
			}
			filtered = append(filtered, path)
		}
		paths = filtered
	}
	for path := range missingBlockedPaths {
		paths = append(paths, path)
	}
	for path := range missingDeferredPaths {
		paths = append(paths, path)
	}
	for path := range rebasePaths {
		paths = append(paths, path)
	}
	if len(paths) == 0 {
		return nil, firstErr
	}
	if !verifyUnchanged && len(legacyPaths) > 0 {
		// Keep modern sessions on the stat-only path while strictly checking
		// only the legacy paths that lack a source proof.
		legacy := make([]string, 0, len(legacyPaths))
		modern := make([]string, 0, len(paths))
		for _, path := range paths {
			if legacyPaths[cleanComparablePath(path)] {
				legacy = append(legacy, path)
			} else {
				modern = append(modern, path)
			}
		}
		legacyChanges, err := historyTieredDetectStatChanges(legacy, states, true)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		modernChanges, err := historyTieredDetectStatChanges(modern, states, false)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		legacyChanges = append(legacyChanges, modernChanges...)
		changes := legacyChanges
		out := make([]string, 0, len(changes))
		for _, change := range changes {
			out = append(out, change.Path)
		}
		for path := range missingBlockedPaths {
			out = append(out, path)
		}
		for path := range missingDeferredPaths {
			out = append(out, path)
		}
		for path := range rebasePaths {
			out = append(out, path)
		}
		return uniqueSortedCleanPaths(out), firstErr
	}
	changes, err := historyTieredDetectStatChanges(paths, states, verifyUnchanged)
	if err != nil && firstErr == nil {
		firstErr = err
	}
	out := make([]string, 0, len(changes))
	for _, change := range changes {
		out = append(out, change.Path)
	}
	for path := range missingBlockedPaths {
		out = append(out, path)
	}
	for path := range missingDeferredPaths {
		out = append(out, path)
	}
	for path := range rebasePaths {
		out = append(out, path)
	}
	return uniqueSortedCleanPaths(out), firstErr
}

func uniqueSortedCleanPaths(paths []string) []string {
	seen := make(map[string]bool, len(paths))
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		path = filepath.Clean(path)
		if seen[path] {
			continue
		}
		seen[path] = true
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func (b *Bridge) baselineCodexHistoryWatch(ctx context.Context, paths []string, now time.Time) error {
	return b.updateHistoryWatch(ctx, func(historyWatch map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		for _, path := range paths {
			info, err := os.Stat(path)
			if err != nil || info.IsDir() {
				continue
			}
			id := historyWatchCheckpointID(path)
			boundary, err := historyWatchBaselineBoundary(path, info.Size())
			if err != nil {
				return err
			}
			fingerprint := strings.TrimSpace(transcriptCheckpointSourceFingerprint(path, boundary.Offset))
			generation := historyTieredSourceIdentity(path, info)
			checkpoint := teamstore.HistoryWatchCheckpoint{
				ID:                   id,
				Path:                 path,
				Size:                 info.Size(),
				ModTime:              info.ModTime(),
				SourceGeneration:     generation,
				SourceFingerprint:    fingerprint,
				SourceChangeTime:     teamstore.SourceFileChangeTimeFromFileInfo(info),
				SourceRewriteBlocked: info.Size() > 0 && fingerprint == "",
				Offset:               boundary.Offset,
				Line:                 boundary.Line,
				UpdatedAt:            now,
			}
			if boundary.Partial {
				checkpoint.PartialLineStartOffset = boundary.Offset
				checkpoint.PartialReadOffset = info.Size()
				checkpoint.PartialObservedSize = info.Size()
				checkpoint.PartialLine = boundary.Line + 1
				checkpoint.PartialStartedAt = info.ModTime()
				checkpoint.PartialSourceIdentity = generation
				checkpoint.PartialSourceChangeTime = teamstore.SourceFileChangeTimeFromFileInfo(info)
				checkpoint.PartialReplayOffset = boundary.Offset
				checkpoint.PartialReplayLine = boundary.Line
				checkpoint.PartialLastProgressAt = now
			}
			historyWatch[id] = checkpoint
		}
		*ready = now
		return nil
	})
}

type historyWatchBaselineBoundaryResult struct {
	Offset  int64
	Line    int
	Partial bool
}

// historyWatchBaselineBoundary returns the last durable JSONL delimiter. A
// startup baseline may skip existing complete records, but must not skip an
// unterminated record that a live Codex writer can finish after startup. A
// valid JSON value at EOF is complete even without a final delimiter.
func historyWatchBaselineBoundary(path string, size int64) (historyWatchBaselineBoundaryResult, error) {
	boundary := historyWatchBaselineBoundaryResult{}
	if strings.TrimSpace(path) == "" || size <= 0 {
		return boundary, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return boundary, err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(io.LimitReader(f, size), historyTieredTailReaderSize)
	var offset int64
	var suffix []byte
	suffixTooLarge := false
	for offset < size {
		chunk, readErr := reader.ReadSlice('\n')
		offset += int64(len(chunk))
		if bytes.HasSuffix(chunk, []byte{'\n'}) {
			boundary.Offset = offset
			boundary.Line++
			suffix = suffix[:0]
			suffixTooLarge = false
		} else if !suffixTooLarge {
			if len(suffix)+len(chunk) <= historyTieredMaxRecordBytes {
				suffix = append(suffix, chunk...)
			} else {
				suffixTooLarge = true
				suffix = nil
			}
		}
		if readErr != nil {
			if readErr == bufio.ErrBufferFull {
				continue
			}
			if readErr == io.EOF {
				break
			}
			return boundary, readErr
		}
	}
	if boundary.Offset == size {
		return boundary, nil
	}
	trimmed := bytes.TrimSpace(suffix)
	if !suffixTooLarge && len(trimmed) > 0 && json.Valid(trimmed) {
		boundary.Offset = size
		boundary.Line++
		return boundary, nil
	}
	boundary.Partial = size > boundary.Offset
	return boundary, nil
}

func (b *Bridge) syncCodexHistoryWatchPath(ctx context.Context, path string, now time.Time) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	id := historyWatchCheckpointID(path)
	state, err := b.store.HistoryWatchState(ctx)
	if err != nil {
		return err
	}
	checkpoint := state.HistoryWatch[id]
	var expectedCheckpoint *teamstore.HistoryWatchCheckpoint
	if current, ok := state.HistoryWatch[id]; ok {
		currentCopy := current
		expectedCheckpoint = &currentCopy
	}
	previous := historyTieredFileStateFromHistoryWatch(checkpoint)
	if strings.TrimSpace(previous.Path) == "" {
		previous.Path = path
	}
	if previous.LegacySourceUnverified {
		if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
			return b.removeHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint)
		} else if statErr != nil {
			return statErr
		}
		// This is deliberately a no-op for automatic history polling. The
		// inherited cursor has no source identity proof, so appending or rewriting
		// the file cannot safely establish a new suffix boundary. The explicit
		// publish-history command is the recovery operation.
		return nil
	}
	if previous.RecoveryProofUnusable {
		if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
			return b.removeHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint)
		} else if statErr != nil {
			return statErr
		}
		// The row is still usable for live state discovery, but its optional
		// source proof is not safe for automatic suffix scanning. Keep the
		// physical cursor untouched and wait for explicit recovery.
		return nil
	}
	if previous.SourceRewriteBlocked {
		// A source rewrite is an explicit safety boundary. A Codex migration may
		// safely rebase it once the same path is a paginated rollout and the old
		// final can be located by stable identity. All delivery state remains
		// untouched by this operation.
		if rebased, err := b.rebaseHistoryWatchSourceRewrite(ctx, id, expectedCheckpoint, previous, path, now); err != nil {
			return err
		} else if rebased {
			return nil
		}
		if !b.historyWatchDeletedSourceProbeDue(path, now) {
			return nil
		}
		if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
			b.clearHistoryWatchDeletedSourceProbe(path)
			// A blocked checkpoint for a deleted transcript is no longer useful.
			// Remove it with the same expected-checkpoint CAS used by the normal
			// deletion path; a concurrent explicit recovery must win over cleanup.
			return b.removeHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint)
		} else if statErr != nil {
			return statErr
		}
		return nil
	}
	blockSourceRewrite := func() error {
		info, statErr := os.Stat(path)
		if os.IsNotExist(statErr) {
			return b.removeHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint)
		}
		if statErr != nil {
			return statErr
		}
		blocked := previous
		blocked.Path = path
		blocked.Size = info.Size()
		blocked.ModTime = info.ModTime()
		blocked.SourceRewriteBlocked = true
		blocked.SourceRewriteRecoveryIdentity = ""
		return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, blocked, now)
	}
	legacyFingerprintMissing := previous.Size > 0 && strings.TrimSpace(previous.SourceFingerprint) == ""
	if legacyFingerprintMissing {
		// A legacy cursor has no proof that the bytes before Offset still belong
		// to this source. Migrate it to a silent history-only boundary instead of
		// marking the whole chat blocked or emitting a recovery notice. Do not
		// update the cursor to the current file size: that would silently skip a
		// replacement suffix. Explicit publish-history establishes the next
		// content-bound boundary.
		migrated := previous
		migrated.Path = path
		migrated.LegacySourceUnverified = true
		migrated.SourceRewriteBlocked = false
		migrated.OversizedRecordBlocked = false
		return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, migrated, now)
	}
	if previous.Size > 0 && previous.Offset >= 0 && strings.TrimSpace(previous.SourceFingerprint) != "" {
		if !historyWatchSourcePrefixMatches(path, previous) {
			return blockSourceRewrite()
		}
	}
	result, err := historyTieredScanTail(path, previous, historyTieredMaxTailBytes)
	if os.IsNotExist(err) {
		return b.removeHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint)
	}
	if err != nil {
		return err
	}
	if result.Incomplete && historyWatchPartialIsStale(previous, result.State, now) {
		if recovered, released, releaseErr := historyWatchReleaseStalePartial(path, previous); releaseErr != nil {
			return releaseErr
		} else if released {
			// Release only the complete prefix. The unterminated line remains a
			// resumable tail at the same source offset and will be parsed again if
			// the writer later completes it; no opaque gap or execution fence is
			// manufactured from elapsed time alone.
			result = recovered
		}
	}
	// The scan holds an open descriptor only for its read.  Re-validate the
	// trusted prefix before any session/outbox publish because the pathname may
	// have been atomically replaced while the tail was being parsed.  The
	// fingerprint includes the opened file identity, so a replacement with the
	// same bytes and metadata is still rejected.
	if strings.TrimSpace(previous.SourceFingerprint) != "" && !historyWatchSourcePrefixMatches(path, previous) {
		return blockSourceRewrite()
	}
	if result.Incomplete || result.Truncated {
		// Do not publish a final observed before an unterminated/truncated
		// record.  The scanner state contains metadata for complete records
		// before the incomplete line, but those records have not been published
		// by this path.  Persisting a complete-prefix cursor would make the next
		// pass skip a valid final permanently.  A truncated source is already reset
		// by the scanner; an incomplete append retains only its bounded read hint.
		if result.Incomplete && !result.Truncated {
			// The scanner may have observed complete records before the partial
			// line, but this watcher has not published them yet.  Keep the
			// previous semantic cursor/final boundary and persist only the
			// bounded partial-line observation.  Otherwise a final before the
			// incomplete tail can be recorded as already delivered while the
			// logical cursor still points before it.
			partial := previous
			partial.Path = path
			partial.Size = result.State.Size
			partial.ModTime = result.State.ModTime
			// HistoryWatch does not publish the complete records observed before
			// the partial line.  Reuse the existing partial-read hint, but make its
			// replay origin the last durable newline; otherwise completion would
			// resume at the partial line and permanently skip an earlier final.
			partial.PartialLineStartOffset = result.State.PartialLineStartOffset
			partial.PartialReadOffset = result.State.PartialReadOffset
			partial.PartialObservedSize = result.State.PartialObservedSize
			partial.PartialLine = result.State.PartialLine
			partial.PartialStartedAt = result.State.PartialStartedAt
			partial.PartialSourceIdentity = result.State.PartialSourceIdentity
			partial.PartialSourceChangeTime = result.State.PartialSourceChangeTime
			partial.PartialReplayOffset = previous.Offset
			partial.PartialReplayLine = previous.Line
			if partial.PartialLastProgressAt.IsZero() ||
				partial.PartialObservedSize != previous.PartialObservedSize ||
				partial.PartialReadOffset != previous.PartialReadOffset ||
				partial.PartialSourceIdentity != previous.PartialSourceIdentity ||
				partial.PartialSourceChangeTime != previous.PartialSourceChangeTime ||
				!partial.ModTime.Equal(previous.ModTime) {
				partial.PartialLastProgressAt = now
			}
			return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, partial, now)
		}
		return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, result.State, now)
	}
	historyOnlyDisposition := false
	if result.State.TranscriptQuarantine != nil && !result.State.UnresolvedContinuation &&
		!historyTieredQuarantineIsContextGap(result.State.TranscriptQuarantine) {
		// A mirror/anonymous-final ambiguity is a history frontier, not proof
		// that a Codex process still owns the thread. Keep the physical cursor
		// monotonic and retain one bounded semantic range; do not create an
		// execution anchor or emit a blocker notice. Continue through the normal
		// safe-final path below so a final before the ambiguous range is not lost.
		historyOnlyDisposition = true
		if result.State.PendingHistoryRange == nil {
			start := result.State.TranscriptQuarantine.FrontierOffset
			end := result.State.Offset
			result.State.PendingHistoryRange = historyTieredPendingHistoryRange(result.State, start, end, result.State.TranscriptQuarantine.Kind)
		}
	}
	// A malformed or complete oversized JSONL record is an opaque byte-range
	// disposition, not an execution boundary. The scanner has already consumed
	// its newline and reset parser context, so keep the physical cursor and let
	// the normal final-publish path handle any safe records before that gap. A
	// later poll resumes after the gap instead of retrying it forever.
	if result.State.UnresolvedContinuation {
		// Do not publish the ambiguous child tail, but do commit the physical
		// cursor and its exact bounded pending range. Rewinding to the marker was
		// the s512 livelock: every poll rediscovered the same final/marker pair.
		// Safe finals before that boundary continue through the normal publish path.
		historyOnlyDisposition = true
		if result.State.PendingHistoryRange == nil {
			result.State.PendingHistoryRange = historyTieredPendingHistoryRange(result.State, result.State.UnresolvedContinuationOffset, result.State.Offset, "unresolved_continuation")
		}
	}
	// The scanner's bounded read proof covers the bytes that produced the
	// records below.  Prefix validation alone is insufficient here: an
	// in-place rewrite in the newly-read tail could leave the old checkpoint
	// prefix intact while changing the final that is about to be published.
	// HistoryWatch is a cold periodic path, so re-reading this bounded range is
	// preferable to adding another read or lock to ordinary message delivery.
	if result.BytesRead > 0 || len(result.Records) > 0 || len(result.Finals) > 0 {
		if !historyWatchReadProofMatches(path, result) {
			return blockSourceRewrite()
		}
	}
	// Keep this second guard immediately adjacent to the first publish call.
	// It narrows the scan-to-publish window without changing the durable final
	// CAS semantics below.
	if strings.TrimSpace(previous.SourceFingerprint) != "" && !historyWatchSourcePrefixMatches(path, previous) {
		return blockSourceRewrite()
	}
	start := historyWatchSessionStartResult{}
	if !historyOnlyDisposition {
		var err error
		start, err = b.publishHistoryWatchSessionStart(ctx, path, result)
		if err != nil {
			return err
		}
	}
	if start.blocked {
		return nil
	}
	if start.clearTeamsOrigin {
		result.State.TeamsOriginThreadID = ""
		result.State.TeamsOriginTurnID = ""
	}
	if strings.TrimSpace(start.teamsOriginTurnID) != "" {
		result.State.TeamsOriginTurnID = strings.TrimSpace(start.teamsOriginTurnID)
	}
	if strings.TrimSpace(start.teamsOriginThreadID) != "" {
		result.State.TeamsOriginThreadID = strings.TrimSpace(start.teamsOriginThreadID)
	}
	for _, final := range result.Finals {
		if strings.TrimSpace(final.Key) == "" || final.Key == checkpoint.LastFinalID {
			continue
		}
		if historyWatchFinalMatchesTeamsOrigin(final, result.State.TeamsOriginThreadID, result.State.TeamsOriginTurnID) {
			continue
		}
		handled, err := b.publishHistoryWatchFinal(ctx, path, final, publishHistoryWatchFinalOptions{
			ForceDetectedNotification: start.sessionStarted,
		})
		if err != nil {
			return err
		}
		if !handled {
			if historyOnlyDisposition {
				// Without a discovered destination there is no safe way to claim
				// this final as delivered. Preserve the durable history-only
				// boundary, matching the old watcher behavior, and let a later
				// discovery or explicit import choose the destination.
				return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, result.State, now)
			}
			return nil
		}
		if !historyWatchReadProofMatches(path, result) {
			return blockSourceRewrite()
		}
	}
	if len(result.Finals) == 0 && (result.BytesRead > 0 || len(result.Records) > 0) {
		if !historyWatchReadProofMatches(path, result) {
			return blockSourceRewrite()
		}
	}
	return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, result.State, now)
}

func historyWatchPartialIsStale(previous, observed historyTieredFileState, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	if previous.PartialPrefixReleased || previous.PartialLineStartOffset < previous.Offset || previous.PartialReadOffset <= previous.PartialLineStartOffset {
		return false
	}
	if observed.PartialLineStartOffset != previous.PartialLineStartOffset ||
		observed.PartialReadOffset != previous.PartialReadOffset ||
		observed.PartialObservedSize != previous.PartialObservedSize ||
		observed.Size != previous.PartialObservedSize ||
		!observed.ModTime.Equal(previous.ModTime) {
		return false
	}
	if previous.PartialLastProgressAt.IsZero() || now.Before(previous.PartialLastProgressAt) {
		return false
	}
	return now.Sub(previous.PartialLastProgressAt) >= historyTieredStalePartialAfter
}

// historyWatchReleaseStalePartial releases only the newline-complete prefix of
// an unchanged partial source. The partial bytes are deliberately retained in
// the checkpoint: elapsed time is not proof that the writer crashed, and
// advancing the cursor to EOF would lose a record if the writer resumes. The
// returned result is safe for the ordinary final/outbox path because its read
// proof ends at the partial line start, never inside the mutable tail.
func historyWatchReleaseStalePartial(path string, previous historyTieredFileState) (historyTieredTailResult, bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return historyTieredTailResult{}, false, nil
	}
	rescan := previous
	rescan.PartialLineStartOffset = 0
	rescan.PartialReadOffset = 0
	rescan.PartialObservedSize = 0
	rescan.PartialLine = 0
	rescan.PartialStartedAt = time.Time{}
	rescan.PartialSourceIdentity = ""
	rescan.PartialSourceChangeTime = 0
	rescan.PartialReplayOffset = 0
	rescan.PartialReplayLine = 0
	rescan.PartialLastProgressAt = time.Time{}
	rescan.PartialPrefixReleased = false
	prefix, err := historyTieredScanTail(path, rescan, historyTieredMaxTailBytes)
	if err != nil || prefix.Truncated || !prefix.Incomplete {
		return prefix, false, err
	}
	start := prefix.State.PartialLineStartOffset
	if start < previous.Offset || start > prefix.State.Size {
		return prefix, false, nil
	}
	size := prefix.State.Size
	pathInfo, err := os.Stat(path)
	if err != nil || pathInfo.IsDir() || pathInfo.Size() != size {
		return prefix, false, err
	}
	f, err := os.Open(path)
	if err != nil {
		return prefix, false, err
	}
	defer f.Close()
	fdInfo, err := f.Stat()
	if err != nil || fdInfo.IsDir() || !os.SameFile(pathInfo, fdInfo) || fdInfo.Size() != size {
		return prefix, false, nil
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, fdInfo)
	if err != nil || strings.TrimSpace(identity) == "" {
		return prefix, false, nil
	}
	next := prefix.State
	next.Offset = start
	next.Line = prefix.State.PartialLine - 1
	if next.Line < previous.Line {
		next.Line = previous.Line
	}
	next.PartialReplayOffset = start
	next.PartialReplayLine = next.Line
	next.PartialLastProgressAt = previous.PartialLastProgressAt
	next.PartialPrefixReleased = true
	next.TranscriptQuarantine = previous.TranscriptQuarantine
	next.ContextGap = previous.ContextGap
	fdPost, fdPostErr := f.Stat()
	post, postErr := os.Stat(path)
	if fdPostErr != nil || postErr != nil || !os.SameFile(fdInfo, fdPost) || !os.SameFile(pathInfo, post) || post.Size() != prefix.State.Size {
		return prefix, false, nil
	}
	prefix.State = next
	prefix.Incomplete = false
	prefix.ReadProofFingerprint = transcriptSourceRangeFingerprint(path, previous.Offset, start)
	prefix.ReadProofStartOffset = previous.Offset
	prefix.ReadProofEndOffset = start
	prefix.ReadProofRangeKnown = strings.TrimSpace(prefix.ReadProofFingerprint) != ""
	prefix.BytesRead = start - previous.Offset
	if prefix.BytesRead < 0 {
		return prefix, false, nil
	}
	return prefix, true, nil
}

// historyWatchDeletedSourceProbeDue keeps the compatibility cleanup for
// deleted blocked checkpoints out of the normal per-poll file-stat path. A
// blocked checkpoint is already fail-closed; probing it again is only useful
// occasionally to retire a transcript that disappeared after the rewrite.
// The in-memory gate is deliberately bounded and is lost on restart, where a
// single startup probe is cheap and useful.
func (b *Bridge) historyWatchDeletedSourceProbeDue(path string, now time.Time) bool {
	if b == nil {
		return true
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	b.historyWatchProbeMu.Lock()
	defer b.historyWatchProbeMu.Unlock()
	if b.historyWatchDeletedProbeAt == nil {
		b.historyWatchDeletedProbeAt = make(map[string]time.Time)
	}
	if next, ok := b.historyWatchDeletedProbeAt[path]; ok && now.Before(next) {
		return false
	}
	if len(b.historyWatchDeletedProbeAt) >= 1024 {
		for key := range b.historyWatchDeletedProbeAt {
			delete(b.historyWatchDeletedProbeAt, key)
			break
		}
	}
	b.historyWatchDeletedProbeAt[path] = now.Add(historyWatchDeletedProbeInterval)
	return true
}

func (b *Bridge) clearHistoryWatchDeletedSourceProbe(path string) {
	if b == nil {
		return
	}
	b.historyWatchProbeMu.Lock()
	delete(b.historyWatchDeletedProbeAt, strings.TrimSpace(path))
	b.historyWatchProbeMu.Unlock()
}

// historyWatchSourcePrefixMatches verifies the source identity and bounded
// prefix proof captured by a prior HistoryWatch checkpoint.  It deliberately
// uses stat/open/fstat/post-stat around the bounded read: path replacement
// between any two steps must fail closed rather than allowing a stale scan to
// publish records from a new inode.
func historyWatchSourcePrefixMatches(path string, previous historyTieredFileState) bool {
	path = strings.TrimSpace(path)
	expected := strings.TrimSpace(previous.SourceFingerprint)
	if path == "" || expected == "" || previous.Offset < 0 {
		return false
	}
	pathInfo, err := os.Stat(path)
	if err != nil || pathInfo.IsDir() || pathInfo.Size() < previous.Offset {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	fdInfo, err := f.Stat()
	if err != nil || fdInfo.IsDir() || fdInfo.Size() < previous.Offset || !os.SameFile(pathInfo, fdInfo) {
		return false
	}
	actual := strings.TrimSpace(transcriptCheckpointSourceFingerprintFromReader(f, path, fdInfo.Size(), previous.Offset))
	if actual == "" || actual != expected {
		return false
	}
	postInfo, err := os.Stat(path)
	return err == nil && !postInfo.IsDir() && os.SameFile(pathInfo, postInfo) && postInfo.Size() >= previous.Offset
}

// historyWatchReadProofMatches verifies the exact bounded range consumed by
// historyTieredScanTail.  It is intentionally used only by HistoryWatch's
// cold scan-to-publish path; the normal Teams outbox path has its own
// source-proof cache and final CAS.
func historyWatchReadProofMatches(path string, result historyTieredTailResult) bool {
	if !result.ReadProofRangeKnown || strings.TrimSpace(result.ReadProofFingerprint) == "" ||
		result.ReadProofStartOffset < 0 || result.ReadProofEndOffset < result.ReadProofStartOffset {
		return false
	}
	return transcriptSourceRangeFingerprint(path, result.ReadProofStartOffset, result.ReadProofEndOffset) == strings.TrimSpace(result.ReadProofFingerprint)
}

func historyTieredFileStateFromHistoryWatch(checkpoint teamstore.HistoryWatchCheckpoint) historyTieredFileState {
	return historyTieredFileState{
		Path:                               strings.TrimSpace(checkpoint.Path),
		Size:                               checkpoint.Size,
		ModTime:                            checkpoint.ModTime,
		SourceGeneration:                   strings.TrimSpace(checkpoint.SourceGeneration),
		SourceFingerprint:                  strings.TrimSpace(checkpoint.SourceFingerprint),
		SourceChangeTime:                   checkpoint.SourceChangeTime,
		SourceRewriteBlocked:               checkpoint.SourceRewriteBlocked,
		LegacySourceUnverified:             checkpoint.LegacySourceUnverified,
		RecoveryProofUnusable:              checkpoint.RecoveryProofUnusable,
		OversizedRecordBlocked:             checkpoint.OversizedRecordBlocked,
		SourceRewriteRecoveryIdentity:      strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity),
		SourceRewriteRecoverySize:          checkpoint.SourceRewriteRecoverySize,
		SourceRewriteRecoveryModTime:       checkpoint.SourceRewriteRecoveryModTime,
		SourceRewriteRecoveryChangeTime:    checkpoint.SourceRewriteRecoveryChangeTime,
		SourceRewriteRecoveryScanPending:   checkpoint.SourceRewriteRecoveryScanPending,
		SourceRewriteRecoveryScanOffset:    checkpoint.SourceRewriteRecoveryScanOffset,
		SourceRewriteRecoveryScanLine:      checkpoint.SourceRewriteRecoveryScanLine,
		SourceRewriteRecoveryScanSessionID: strings.TrimSpace(checkpoint.SourceRewriteRecoveryScanSessionID),
		SourceRewriteRecoveryScanThreadID:  strings.TrimSpace(checkpoint.SourceRewriteRecoveryScanThreadID),
		SourceRewriteRecoveryScanTurnID:    strings.TrimSpace(checkpoint.SourceRewriteRecoveryScanTurnID),
		Offset:                             checkpoint.Offset,
		Line:                               checkpoint.Line,
		PartialLineStartOffset:             checkpoint.PartialLineStartOffset,
		PartialReadOffset:                  checkpoint.PartialReadOffset,
		PartialObservedSize:                checkpoint.PartialObservedSize,
		PartialLine:                        checkpoint.PartialLine,
		PartialStartedAt:                   checkpoint.PartialStartedAt,
		PartialSourceIdentity:              strings.TrimSpace(checkpoint.PartialSourceIdentity),
		PartialSourceChangeTime:            checkpoint.PartialSourceChangeTime,
		PartialReplayOffset:                checkpoint.PartialReplayOffset,
		PartialReplayLine:                  checkpoint.PartialReplayLine,
		PartialLastProgressAt:              checkpoint.PartialLastProgressAt,
		PartialPrefixReleased:              checkpoint.PartialPrefixReleased,
		PendingOpaqueRecordStartOffset:     checkpoint.PendingOpaqueRecordStartOffset,
		PendingOpaqueRecordEndOffset:       checkpoint.PendingOpaqueRecordEndOffset,
		PendingOpaqueRecordLine:            checkpoint.PendingOpaqueRecordLine,
		PendingOpaqueRecordID:              strings.TrimSpace(checkpoint.PendingOpaqueRecordID),
		SessionID:                          strings.TrimSpace(checkpoint.SessionID),
		ThreadID:                           strings.TrimSpace(checkpoint.ThreadID),
		TeamsOriginThreadID:                strings.TrimSpace(checkpoint.TeamsOriginThreadID),
		TurnID:                             strings.TrimSpace(checkpoint.TurnID),
		TeamsOriginTurnID:                  strings.TrimSpace(checkpoint.TeamsOriginTurnID),
		ExternalUserPromptSeen:             checkpoint.ExternalUserPromptSeen,
		LastFinalID:                        strings.TrimSpace(checkpoint.LastFinalID),
		LastFinalLine:                      checkpoint.LastFinalLine,
		LastFinalStartOffset:               checkpoint.LastFinalStartOffset,
		LastFinalStartOffsetKnown:          checkpoint.LastFinalStartOffsetKnown,
		LastFinalThreadID:                  strings.TrimSpace(checkpoint.LastFinalThreadID),
		LastFinalTurnID:                    strings.TrimSpace(checkpoint.LastFinalTurnID),
		LastFinalTextHash:                  strings.TrimSpace(checkpoint.LastFinalTextHash),
		// Older history-watch checkpoints persisted only LastFinalID.  In this
		// watcher namespace that ID is a real final boundary (unlike linked
		// transcript checkpoints, whose LastRecordID may be any record), so
		// hydrate the explicit boundary bit for incremental ownership checks.
		TerminalBoundarySeen:         checkpoint.TerminalBoundarySeen || checkpoint.TerminalBoundary != nil || strings.TrimSpace(checkpoint.LastFinalID) != "",
		TerminalBoundaryLine:         checkpoint.TerminalBoundaryLine,
		TerminalBoundary:             checkpoint.TerminalBoundary,
		UnresolvedContinuation:       checkpoint.UnresolvedContinuation,
		UnresolvedContinuationLine:   checkpoint.UnresolvedContinuationLine,
		UnresolvedContinuationOffset: checkpoint.UnresolvedContinuationOffset,
		PendingRootTaskStarted:       checkpoint.PendingRootTaskStarted,
		PendingRootTaskStartedLine:   checkpoint.PendingRootTaskStartedLine,
		PendingRootTaskStartedOffset: checkpoint.PendingRootTaskStartedOffset,
		TranscriptQuarantine:         checkpoint.TranscriptQuarantine,
		ContextGap:                   checkpoint.ContextGap,
		PendingHistoryRange:          checkpoint.PendingHistoryRange,
		pendingAssistant:             historyWatchPendingAssistantFromCheckpoint(checkpoint),
	}
}

func historyWatchCheckpointID(path string) string {
	if abs, err := filepath.Abs(strings.TrimSpace(path)); err == nil {
		path = abs
	}
	return "history-watch:" + shortStableID(filepath.Clean(path))
}

func (b *Bridge) recordHistoryWatchCheckpoint(ctx context.Context, id string, state historyTieredFileState, now time.Time) error {
	return b.updateHistoryWatch(ctx, func(historyWatch map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		checkpoint := historyWatchCheckpointFromState(id, state, now)
		historyWatch[id] = checkpoint
		return nil
	})
}

func (b *Bridge) recordHistoryWatchCheckpointIfCurrent(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, state historyTieredFileState, now time.Time) error {
	checkpoint := historyWatchCheckpointFromState(id, state, now)
	if err := b.updateHistoryWatchCheckpointIfCurrent(ctx, id, expected, checkpoint); errors.Is(err, teamstore.ErrHistoryWatchCheckpointConflict) {
		// Another watcher won the cursor CAS. The newer checkpoint is already
		// durable; discard this stale scan and let the next poll start fresh.
		return nil
	} else {
		return err
	}
}

func historyWatchCheckpointFromState(id string, state historyTieredFileState, now time.Time) teamstore.HistoryWatchCheckpoint {
	if !state.SourceRewriteBlocked && !state.LegacySourceUnverified {
		if fingerprint := strings.TrimSpace(transcriptCheckpointSourceFingerprint(state.Path, state.Offset)); fingerprint != "" {
			state.SourceFingerprint = fingerprint
		} else if state.Size > 0 {
			state.SourceRewriteBlocked = true
		}
	}
	checkpoint := teamstore.HistoryWatchCheckpoint{
		ID:                     id,
		Path:                   strings.TrimSpace(state.Path),
		Size:                   state.Size,
		ModTime:                state.ModTime,
		SourceGeneration:       strings.TrimSpace(state.SourceGeneration),
		SourceFingerprint:      strings.TrimSpace(state.SourceFingerprint),
		SourceChangeTime:       state.SourceChangeTime,
		SourceRewriteBlocked:   state.SourceRewriteBlocked,
		LegacySourceUnverified: state.LegacySourceUnverified,
		RecoveryProofUnusable:  state.RecoveryProofUnusable,
		// Complete oversized JSONL records are now advanced as opaque ignored
		// dispositions; retain the field only for old on-disk compatibility.
		OversizedRecordBlocked:             false,
		SourceRewriteRecoveryIdentity:      strings.TrimSpace(state.SourceRewriteRecoveryIdentity),
		SourceRewriteRecoverySize:          state.SourceRewriteRecoverySize,
		SourceRewriteRecoveryModTime:       state.SourceRewriteRecoveryModTime,
		SourceRewriteRecoveryChangeTime:    state.SourceRewriteRecoveryChangeTime,
		SourceRewriteRecoveryScanPending:   state.SourceRewriteRecoveryScanPending,
		SourceRewriteRecoveryScanOffset:    state.SourceRewriteRecoveryScanOffset,
		SourceRewriteRecoveryScanLine:      state.SourceRewriteRecoveryScanLine,
		SourceRewriteRecoveryScanSessionID: strings.TrimSpace(state.SourceRewriteRecoveryScanSessionID),
		SourceRewriteRecoveryScanThreadID:  strings.TrimSpace(state.SourceRewriteRecoveryScanThreadID),
		SourceRewriteRecoveryScanTurnID:    strings.TrimSpace(state.SourceRewriteRecoveryScanTurnID),
		Offset:                             state.Offset,
		Line:                               state.Line,
		PartialLineStartOffset:             state.PartialLineStartOffset,
		PartialReadOffset:                  state.PartialReadOffset,
		PartialObservedSize:                state.PartialObservedSize,
		PartialLine:                        state.PartialLine,
		PartialStartedAt:                   state.PartialStartedAt,
		PartialSourceIdentity:              strings.TrimSpace(state.PartialSourceIdentity),
		PartialSourceChangeTime:            state.PartialSourceChangeTime,
		PartialReplayOffset:                state.PartialReplayOffset,
		PartialReplayLine:                  state.PartialReplayLine,
		PartialLastProgressAt:              state.PartialLastProgressAt,
		PartialPrefixReleased:              state.PartialPrefixReleased,
		PendingOpaqueRecordStartOffset:     state.PendingOpaqueRecordStartOffset,
		PendingOpaqueRecordEndOffset:       state.PendingOpaqueRecordEndOffset,
		PendingOpaqueRecordLine:            state.PendingOpaqueRecordLine,
		PendingOpaqueRecordID:              strings.TrimSpace(state.PendingOpaqueRecordID),
		SessionID:                          strings.TrimSpace(state.SessionID),
		ThreadID:                           strings.TrimSpace(state.ThreadID),
		TeamsOriginThreadID:                strings.TrimSpace(state.TeamsOriginThreadID),
		TurnID:                             strings.TrimSpace(state.TurnID),
		TeamsOriginTurnID:                  strings.TrimSpace(state.TeamsOriginTurnID),
		ExternalUserPromptSeen:             state.ExternalUserPromptSeen,
		LastFinalID:                        strings.TrimSpace(state.LastFinalID),
		LastFinalLine:                      state.LastFinalLine,
		LastFinalStartOffset:               state.LastFinalStartOffset,
		LastFinalStartOffsetKnown:          state.LastFinalStartOffsetKnown,
		LastFinalThreadID:                  strings.TrimSpace(state.LastFinalThreadID),
		LastFinalTurnID:                    strings.TrimSpace(state.LastFinalTurnID),
		LastFinalTextHash:                  strings.TrimSpace(state.LastFinalTextHash),
		TerminalBoundarySeen:               state.TerminalBoundarySeen || state.TerminalBoundary != nil,
		TerminalBoundaryLine:               state.TerminalBoundaryLine,
		TerminalBoundary:                   state.TerminalBoundary,
		UnresolvedContinuation:             state.UnresolvedContinuation,
		UnresolvedContinuationLine:         state.UnresolvedContinuationLine,
		UnresolvedContinuationOffset:       state.UnresolvedContinuationOffset,
		PendingRootTaskStarted:             state.PendingRootTaskStarted,
		PendingRootTaskStartedLine:         state.PendingRootTaskStartedLine,
		PendingRootTaskStartedOffset:       state.PendingRootTaskStartedOffset,
		TranscriptQuarantine:               state.TranscriptQuarantine,
		ContextGap:                         state.ContextGap,
		PendingHistoryRange:                state.PendingHistoryRange,
		UpdatedAt:                          now,
	}
	applyHistoryWatchPendingAssistant(&checkpoint, state.pendingAssistant)
	return checkpoint
}

func (b *Bridge) removeHistoryWatchCheckpoint(ctx context.Context, id string) error {
	return b.updateHistoryWatch(ctx, func(historyWatch map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		delete(historyWatch, id)
		return nil
	})
}

func (b *Bridge) removeHistoryWatchCheckpointIfCurrent(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint) error {
	if expected == nil {
		return nil
	}
	if err := b.deleteHistoryWatchCheckpointIfCurrent(ctx, id, expected); errors.Is(err, teamstore.ErrHistoryWatchCheckpointConflict) {
		return nil
	} else {
		return err
	}
}

type historyWatchSessionStartResult struct {
	sessionStarted      bool
	teamsOriginThreadID string
	teamsOriginTurnID   string
	clearTeamsOrigin    bool
	blocked             bool
}

func (b *Bridge) publishHistoryWatchSessionStart(ctx context.Context, path string, result historyTieredTailResult) (historyWatchSessionStartResult, error) {
	record, ok := historyTieredFirstVisibleUserPromptRecord(result.Records)
	if !ok {
		return historyWatchSessionStartResult{}, nil
	}
	if isSubagent, err := codexhistory.SessionFileIsSubagentContext(ctx, path); err == nil && isSubagent {
		return historyWatchSessionStartResult{}, nil
	} else if err != nil && !os.IsNotExist(err) {
		return historyWatchSessionStartResult{}, err
	}
	if b.historyWatchRecordLooksTeamsOrigin(ctx, record) {
		return historyWatchSessionStartResult{
			teamsOriginThreadID: strings.TrimSpace(firstNonEmptyString(record.ThreadID, result.State.ThreadID)),
			teamsOriginTurnID:   strings.TrimSpace(firstNonEmptyString(record.TurnID, result.State.TurnID)),
		}, nil
	}
	threadID := strings.TrimSpace(firstNonEmptyString(record.ThreadID, result.State.ThreadID))
	local, project, ok, err := b.findHistoryWatchCodexSession(ctx, path, threadID)
	if err != nil || !ok {
		return historyWatchSessionStartResult{blocked: true}, err
	}
	if existing := b.reg.SessionByCodexThreadID(local.SessionID); existing != nil && isActiveSessionStatus(existing.Status) {
		if err := b.ensureDurableSession(ctx, existing); err != nil {
			return historyWatchSessionStartResult{}, err
		}
		return historyWatchSessionStartResult{clearTeamsOrigin: true}, nil
	}
	if strings.TrimSpace(local.FirstPrompt) == "" {
		local.FirstPrompt = formatTranscriptRecordForTeams(record)
	}
	_, err = b.publishCodexSessionLocalWithOptions(ctx, local, project, publishCodexSessionOptions{
		ChatCreatedNotification:         false,
		ChatCreatedNoticeAfterImport:    true,
		LocalSessionStartedNotification: true,
		BackgroundImport:                true,
	})
	if err != nil {
		return historyWatchSessionStartResult{}, err
	}
	return historyWatchSessionStartResult{sessionStarted: true, clearTeamsOrigin: true}, nil
}

func historyWatchFinalMatchesTeamsOrigin(final historyTieredFinal, teamsOriginThreadID string, teamsOriginTurnID string) bool {
	teamsOriginThreadID = strings.TrimSpace(teamsOriginThreadID)
	teamsOriginTurnID = strings.TrimSpace(teamsOriginTurnID)
	finalTurnID := strings.TrimSpace(final.Record.TurnID)
	if teamsOriginTurnID != "" && finalTurnID == teamsOriginTurnID {
		return true
	}
	finalThreadID := strings.TrimSpace(final.Record.ThreadID)
	return teamsOriginThreadID != "" && finalThreadID == teamsOriginThreadID && (teamsOriginTurnID == "" || finalTurnID == "")
}

func historyTieredFirstVisibleUserPromptRecord(records []TranscriptRecord) (TranscriptRecord, bool) {
	for _, record := range records {
		if record.Kind != TranscriptKindUser {
			continue
		}
		if strings.TrimSpace(formatTranscriptRecordForTeams(record)) == "" {
			continue
		}
		if codexhistory.ShouldSkipSystemInjectedUserPrompt(record.Text) {
			continue
		}
		return record, true
	}
	return TranscriptRecord{}, false
}

func historyWatchPromptLooksTeamsOrigin(text string) bool {
	text = strings.TrimSpace(text)
	return strings.Contains(text, teamsHelperSafetyInstructionLead) &&
		strings.Contains(text, "Codex turn launched by the Teams helper")
}

func (b *Bridge) historyWatchRecordLooksTeamsOrigin(ctx context.Context, record TranscriptRecord) bool {
	if historyWatchPromptLooksTeamsOrigin(record.Text) {
		return true
	}
	if b == nil || b.store == nil || record.Kind != TranscriptKindUser {
		return false
	}
	body := formatTranscriptRecordForTeams(record)
	if strings.TrimSpace(body) == "" {
		return false
	}
	threadID := strings.TrimSpace(record.ThreadID)
	if threadID == "" {
		return false
	}
	state, err := b.store.HistoryWatchOriginState(ctx, threadID)
	if err != nil {
		return false
	}
	return shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginTextHashesForHistoryWatch(state, threadID))
}

func teamsOriginTextHashesForHistoryWatch(state teamstore.State, threadID string) map[string]bool {
	hashes := make(map[string]bool)
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return hashes
	}
	for _, inbound := range state.InboundEvents {
		if inbound.TurnID == "" {
			continue
		}
		if !inboundSourceIsTeamsOrigin(inbound.Source) {
			continue
		}
		if !teamsOriginInboundMatchesHistoryThread(state, inbound, threadID) {
			continue
		}
		addTeamsOriginInboundTextHashes(hashes, inbound)
	}
	return hashes
}

func teamsOriginInboundMatchesHistoryThread(state teamstore.State, inbound teamstore.InboundEvent, threadID string) bool {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return false
	}
	if turn, ok := state.Turns[inbound.TurnID]; ok && strings.TrimSpace(turn.CodexThreadID) == threadID {
		return true
	}
	if session, ok := state.Sessions[inbound.SessionID]; ok && strings.TrimSpace(session.CodexThreadID) == threadID {
		return true
	}
	return false
}

type publishHistoryWatchFinalOptions struct {
	ForceDetectedNotification bool
}

func (b *Bridge) publishHistoryWatchFinal(ctx context.Context, path string, final historyTieredFinal, opts publishHistoryWatchFinalOptions) (bool, error) {
	if isSubagent, err := codexhistory.SessionFileIsSubagentContext(ctx, path); err == nil && isSubagent {
		return true, nil
	} else if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	threadID := strings.TrimSpace(final.Record.ThreadID)
	local, project, ok, err := b.findHistoryWatchCodexSession(ctx, path, threadID)
	if err != nil || !ok {
		return false, err
	}
	if existing := b.reg.SessionByCodexThreadID(local.SessionID); existing != nil && isActiveSessionStatus(existing.Status) {
		if err := b.ensureDurableSession(ctx, existing); err != nil {
			return false, err
		}
		if opts.ForceDetectedNotification && !b.sessionHasTeamsManagedTurns(ctx, existing.ID) {
			b.queueWorkflowNotificationForDetectedCodexAnswer(ctx, existing, final.Key)
			return true, nil
		}
		if !b.sessionHasTeamsManagedTurns(ctx, existing.ID) {
			if err := b.syncSessionTranscript(ctx, *existing, local); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	_, err = b.publishCodexSessionLocalWithOptions(ctx, local, project, publishCodexSessionOptions{
		ChatCreatedNotification: !b.workflowUserAttentionAvailable(ctx),
		BackgroundImport:        true,
	})
	if err != nil {
		return false, err
	}
	if session := b.reg.SessionByCodexThreadID(local.SessionID); session != nil {
		b.queueWorkflowNotificationForDetectedCodexAnswer(ctx, session, final.Key)
	}
	return true, nil
}

func (b *Bridge) sessionHasTeamsManagedTurns(ctx context.Context, sessionID string) bool {
	if b == nil || b.store == nil {
		return true
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return true
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return true
	}
	if durable, ok := state.Sessions[sessionID]; ok && strings.TrimSpace(durable.LatestTurnID) != "" {
		return true
	}
	for _, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) == sessionID {
			return true
		}
	}
	return false
}

func (b *Bridge) findHistoryWatchCodexSession(ctx context.Context, path string, threadID string) (codexhistory.Session, codexhistory.Project, bool, error) {
	projects, err := discoverCodexProjectsForTeams(ctx, b.scope.CodexHome)
	if err != nil {
		// A fresh installation may not have created ~/.codex/sessions yet.  That
		// is a normal "nothing discoverable" result for HistoryWatch, not a
		// corrupt source or a process-wide failure.  Keep the old quiet behavior
		// for this specific absence while surfacing all other discovery failures so
		// the listener cannot report a false successful no-op.
		if codexhistory.IsSessionsDirNotFound(err) {
			return codexhistory.Session{}, codexhistory.Project{}, false, nil
		}
		return codexhistory.Session{}, codexhistory.Project{}, false, fmt.Errorf("discover Codex history session: %w", err)
	}
	projects = codexhistory.FilterUserVisibleProjects(projects)
	cleanPath := cleanComparablePath(path)
	threadID = strings.TrimSpace(threadID)
	for _, project := range projects {
		for _, local := range project.Sessions {
			if local.ProjectPath == "" {
				local.ProjectPath = project.Path
			}
			if threadID != "" && local.SessionID == threadID {
				return local, project, true, nil
			}
			if cleanPath != "" && cleanComparablePath(local.FilePath) == cleanPath {
				return local, project, true, nil
			}
		}
	}
	return codexhistory.Session{}, codexhistory.Project{}, false, nil
}

func historyWatchPendingAssistantFromCheckpoint(checkpoint teamstore.HistoryWatchCheckpoint) historyTieredAssistantCandidate {
	if strings.TrimSpace(checkpoint.PendingAssistantText) == "" {
		return historyTieredAssistantCandidate{}
	}
	return historyTieredAssistantCandidate{Record: TranscriptRecord{
		SourceItemID:      strings.TrimSpace(checkpoint.PendingAssistantSourceID),
		ThreadID:          strings.TrimSpace(checkpoint.PendingAssistantThreadID),
		TurnID:            strings.TrimSpace(checkpoint.PendingAssistantTurnID),
		Kind:              TranscriptKindAssistant,
		Text:              strings.TrimSpace(checkpoint.PendingAssistantText),
		CreatedAt:         checkpoint.PendingAssistantCreatedAt,
		SourceLine:        checkpoint.PendingAssistantSourceLine,
		SourceStartOffset: checkpoint.PendingAssistantStartOffset,
		SourceOffset:      checkpoint.PendingAssistantOffset,
		SourceType:        strings.TrimSpace(checkpoint.PendingAssistantSourceType),
	}}
}

func applyHistoryWatchPendingAssistant(checkpoint *teamstore.HistoryWatchCheckpoint, pending historyTieredAssistantCandidate) {
	if checkpoint == nil {
		return
	}
	record := pending.Record
	if strings.TrimSpace(record.Text) == "" {
		checkpoint.PendingAssistantSourceID = ""
		checkpoint.PendingAssistantThreadID = ""
		checkpoint.PendingAssistantTurnID = ""
		checkpoint.PendingAssistantText = ""
		checkpoint.PendingAssistantCreatedAt = time.Time{}
		checkpoint.PendingAssistantSourceLine = 0
		checkpoint.PendingAssistantStartOffset = 0
		checkpoint.PendingAssistantOffset = 0
		checkpoint.PendingAssistantSourceType = ""
		return
	}
	checkpoint.PendingAssistantSourceID = strings.TrimSpace(record.SourceItemID)
	checkpoint.PendingAssistantThreadID = strings.TrimSpace(record.ThreadID)
	checkpoint.PendingAssistantTurnID = strings.TrimSpace(record.TurnID)
	checkpoint.PendingAssistantText = strings.TrimSpace(record.Text)
	checkpoint.PendingAssistantCreatedAt = record.CreatedAt
	checkpoint.PendingAssistantSourceLine = record.SourceLine
	checkpoint.PendingAssistantStartOffset = record.SourceStartOffset
	checkpoint.PendingAssistantOffset = record.SourceOffset
	checkpoint.PendingAssistantSourceType = strings.TrimSpace(record.SourceType)
}

func cleanComparablePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return filepath.Clean(path)
}
