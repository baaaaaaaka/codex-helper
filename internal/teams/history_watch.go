package teams

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

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
	root, err := codexhistory.ResolveCodexDir(b.scope.CodexHome)
	if err != nil {
		return nil
	}
	state, err := b.store.HistoryWatchState(ctx)
	if err != nil {
		return err
	}
	initialized := !state.HistoryWatchReady.IsZero()
	paths := historyWatchPathsFromState(state)
	recent, err := historyTieredListSessionFilesInDirs(historyWatchRecentSessionDirs(root, now, historyWatchRecentDays))
	if err != nil {
		return err
	}
	recentSet := historyWatchPathSet(recent)
	paths = append(paths, recent...)
	sessionsRootMissing := false
	if reconcile {
		reconciled, err := b.historyWatchReconcilePaths(ctx)
		if codexhistory.IsSessionsDirNotFound(err) {
			err = nil
			reconciled = nil
			sessionsRootMissing = true
		}
		if err != nil {
			if !initialized {
				return err
			}
		} else {
			if initialized {
				baseline := historyWatchMissingNonRecentPaths(reconciled, recentSet, state)
				if len(baseline) > 0 {
					if err := b.baselineCodexHistoryWatch(ctx, baseline, now); err != nil {
						return err
					}
					state, err = b.store.HistoryWatchState(ctx)
					if err != nil {
						return err
					}
				}
			}
			paths = append(paths, reconciled...)
		}
	}
	paths = uniqueSortedCleanPaths(paths)
	if !initialized {
		if sessionsRootMissing {
			return nil
		}
		return b.baselineCodexHistoryWatch(ctx, paths, now)
	}
	changes, err := historyWatchChangedPaths(paths, state, reconcile)
	if err != nil {
		return err
	}
	for _, path := range changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.syncCodexHistoryWatchPath(ctx, path, now); err != nil {
			return err
		}
	}
	return nil
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
	// Legacy HistoryWatch rows have no bounded prefix proof.  Even on the
	// normal (non-reconcile) poll, verify those rows before treating an equal
	// size/mtime as unchanged; otherwise a same-size rewrite can remain
	// invisible until the next reconcile and an old source-less outbox may be
	// flushed first.  Modern rows retain the cheap stat-only idle path.
	legacyPaths := make(map[string]bool)
	for _, checkpoint := range state.HistoryWatch {
		if path := cleanComparablePath(checkpoint.Path); path != "" {
			fileState := historyTieredFileStateFromHistoryWatch(checkpoint)
			fileState.Path = path
			states[path] = fileState
			if fileState.SourceRewriteBlocked || fileState.OversizedRecordBlocked {
				// A blocked checkpoint is an explicit manual-recovery boundary.
				// Until publish-history/baseline replaces it, no stat change can
				// make the path automatically trustworthy again. Excluding it here
				// avoids a repeated stat/lock/sync cycle; the sync path already
				// returns without removing or advancing such a checkpoint.
				blockedPaths[path] = true
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
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			missingBlockedPaths[path] = true
			continue
		}
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			continue
		}
	}
	if len(blockedPaths) > 0 {
		filtered := make([]string, 0, len(paths))
		for _, path := range paths {
			cleanPath := cleanComparablePath(path)
			if blockedPaths[cleanPath] && !missingBlockedPaths[cleanPath] {
				continue
			}
			filtered = append(filtered, path)
		}
		paths = filtered
	}
	for path := range missingBlockedPaths {
		paths = append(paths, path)
	}
	if len(paths) == 0 {
		return nil, nil
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
		if err != nil {
			return nil, err
		}
		modernChanges, err := historyTieredDetectStatChanges(modern, states, false)
		if err != nil {
			return nil, err
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
		return uniqueSortedCleanPaths(out), nil
	}
	changes, err := historyTieredDetectStatChanges(paths, states, verifyUnchanged)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(changes))
	for _, change := range changes {
		out = append(out, change.Path)
	}
	for path := range missingBlockedPaths {
		out = append(out, path)
	}
	return uniqueSortedCleanPaths(out), nil
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
	return b.store.UpdateHistoryWatch(ctx, func(historyWatch map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		for _, path := range paths {
			info, err := os.Stat(path)
			if err != nil || info.IsDir() {
				continue
			}
			id := historyWatchCheckpointID(path)
			fingerprint := strings.TrimSpace(transcriptCheckpointSourceFingerprint(path, info.Size()))
			historyWatch[id] = teamstore.HistoryWatchCheckpoint{
				ID:                   id,
				Path:                 path,
				Size:                 info.Size(),
				ModTime:              info.ModTime(),
				SourceFingerprint:    fingerprint,
				SourceRewriteBlocked: info.Size() > 0 && fingerprint == "",
				Offset:               info.Size(),
				UpdatedAt:            now,
			}
		}
		*ready = now
		return nil
	})
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
	if previous.SourceRewriteBlocked {
		// A source rewrite is an explicit safety boundary.  Do not let a later
		// append make the watcher reinterpret the rewritten prefix; explicit
		// history import is required to establish a new trusted cursor.
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
		return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, blocked, now)
	}
	legacyFingerprintMissing := previous.Size > 0 && strings.TrimSpace(previous.SourceFingerprint) == ""
	if legacyFingerprintMissing {
		// A legacy cursor has no proof that the bytes before Offset still belong
		// to this source.  Scanning even a commentary-only suffix would mint a
		// new fingerprint from the rewritten file and make a later final look
		// trusted.  Require an explicit history publication/baseline to establish
		// the first content-bound cursor; never infer it from the current EOF.
		return blockSourceRewrite()
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
	// The scan holds an open descriptor only for its read.  Re-validate the
	// trusted prefix before any session/outbox publish because the pathname may
	// have been atomically replaced while the tail was being parsed.  The
	// fingerprint includes the opened file identity, so a replacement with the
	// same bytes and metadata is still rejected.
	if strings.TrimSpace(previous.SourceFingerprint) != "" && !historyWatchSourcePrefixMatches(path, previous) {
		return blockSourceRewrite()
	}
	if (result.OversizedRecord || result.TooLarge && !result.BudgetExhausted) && !result.Incomplete {
		// History watch is a periodic discovery path.  Do not turn a large
		// append into an unbounded read. Record the observed file stat while
		// keeping the logical cursor untouched; a single pathological record still
		// requires explicit history import, while a normal bounded prefix remains
		// resumable through the BudgetExhausted path above.
		if info, statErr := os.Stat(path); statErr != nil {
			return statErr
		} else {
			blockedState := result.State
			if result.OversizedRecord {
				// Do not skip complete records before the pathological line: they
				// were not published by this pass and must remain in the explicit
				// recovery range.
				blockedState = previous
			}
			blockedState.Path = path
			blockedState.Size = info.Size()
			blockedState.ModTime = info.ModTime()
			blockedState.OversizedRecordBlocked = result.OversizedRecord
			return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, blockedState, now)
		}
	}
	if result.Incomplete || result.Truncated {
		// Do not publish a final observed before an unterminated/truncated
		// record.  The scanner state contains metadata for complete records
		// before the incomplete line, but those records have not been published
		// by this path.  Persisting that state would make the next pass skip a
		// valid final permanently.  A truncated source is already reset by the
		// scanner; an incomplete append must roll back to the previous trusted
		// cursor and retry the complete prefix after the writer settles.
		if result.Incomplete && !result.Truncated {
			rollback := previous
			rollback.Path = path
			return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, rollback, now)
		}
		return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, result.State, now)
	}
	if result.State.UnresolvedContinuation {
		// Do not publish finals from a tail that contains execution-bearing work
		// after a previous terminal boundary. Persist the marker before returning
		// so a helper restart cannot forget this fail-closed decision.
		blockedState := result.State
		if blockedState.UnresolvedContinuationOffset >= 0 && blockedState.UnresolvedContinuationOffset < blockedState.Offset {
			blockedState.Offset = blockedState.UnresolvedContinuationOffset
			blockedState.Line = blockedState.UnresolvedContinuationLine
			if blockedState.LastFinalStartOffsetKnown && blockedState.LastFinalStartOffset < blockedState.Offset {
				blockedState.Offset = blockedState.LastFinalStartOffset
				blockedState.Line = blockedState.LastFinalLine - 1
				if blockedState.Line < 0 {
					blockedState.Line = 0
				}
			}
			if strings.TrimSpace(blockedState.LastFinalID) == "" {
				blockedState.LastFinalID = previous.LastFinalID
			}
			if !blockedState.LastFinalStartOffsetKnown {
				blockedState.LastFinalStartOffset = previous.LastFinalStartOffset
				blockedState.LastFinalStartOffsetKnown = previous.LastFinalStartOffsetKnown
			}
			if blockedState.LastFinalLine == 0 {
				blockedState.LastFinalLine = previous.LastFinalLine
			}
			if blockedState.SessionID == "" {
				blockedState.SessionID = previous.SessionID
			}
			if blockedState.ThreadID == "" {
				blockedState.ThreadID = previous.ThreadID
			}
			if blockedState.TurnID == "" {
				blockedState.TurnID = previous.TurnID
			}
			if blockedState.pendingAssistant.Record.ItemID == "" {
				blockedState.pendingAssistant = previous.pendingAssistant
			}
		}
		return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expectedCheckpoint, blockedState, now)
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
	start, err := b.publishHistoryWatchSessionStart(ctx, path, result)
	if err != nil {
		return err
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
		Path:                      strings.TrimSpace(checkpoint.Path),
		Size:                      checkpoint.Size,
		ModTime:                   checkpoint.ModTime,
		SourceFingerprint:         strings.TrimSpace(checkpoint.SourceFingerprint),
		SourceRewriteBlocked:      checkpoint.SourceRewriteBlocked,
		OversizedRecordBlocked:    checkpoint.OversizedRecordBlocked,
		Offset:                    checkpoint.Offset,
		Line:                      checkpoint.Line,
		SessionID:                 strings.TrimSpace(checkpoint.SessionID),
		ThreadID:                  strings.TrimSpace(checkpoint.ThreadID),
		TeamsOriginThreadID:       strings.TrimSpace(checkpoint.TeamsOriginThreadID),
		TurnID:                    strings.TrimSpace(checkpoint.TurnID),
		TeamsOriginTurnID:         strings.TrimSpace(checkpoint.TeamsOriginTurnID),
		ExternalUserPromptSeen:    checkpoint.ExternalUserPromptSeen,
		LastFinalID:               strings.TrimSpace(checkpoint.LastFinalID),
		LastFinalLine:             checkpoint.LastFinalLine,
		LastFinalStartOffset:      checkpoint.LastFinalStartOffset,
		LastFinalStartOffsetKnown: checkpoint.LastFinalStartOffsetKnown,
		LastFinalThreadID:         strings.TrimSpace(checkpoint.LastFinalThreadID),
		LastFinalTurnID:           strings.TrimSpace(checkpoint.LastFinalTurnID),
		LastFinalTextHash:         strings.TrimSpace(checkpoint.LastFinalTextHash),
		// Older history-watch checkpoints persisted only LastFinalID.  In this
		// watcher namespace that ID is a real final boundary (unlike linked
		// transcript checkpoints, whose LastRecordID may be any record), so
		// hydrate the explicit boundary bit for incremental ownership checks.
		TerminalBoundarySeen:         checkpoint.TerminalBoundarySeen || strings.TrimSpace(checkpoint.LastFinalID) != "",
		TerminalBoundaryLine:         checkpoint.TerminalBoundaryLine,
		UnresolvedContinuation:       checkpoint.UnresolvedContinuation,
		UnresolvedContinuationLine:   checkpoint.UnresolvedContinuationLine,
		UnresolvedContinuationOffset: checkpoint.UnresolvedContinuationOffset,
		PendingRootTaskStarted:       checkpoint.PendingRootTaskStarted,
		PendingRootTaskStartedLine:   checkpoint.PendingRootTaskStartedLine,
		PendingRootTaskStartedOffset: checkpoint.PendingRootTaskStartedOffset,
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
	return b.store.UpdateHistoryWatch(ctx, func(historyWatch map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		checkpoint := historyWatchCheckpointFromState(id, state, now)
		historyWatch[id] = checkpoint
		return nil
	})
}

func (b *Bridge) recordHistoryWatchCheckpointIfCurrent(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, state historyTieredFileState, now time.Time) error {
	checkpoint := historyWatchCheckpointFromState(id, state, now)
	if err := b.store.UpdateHistoryWatchCheckpointIfCurrent(ctx, id, expected, checkpoint); errors.Is(err, teamstore.ErrHistoryWatchCheckpointConflict) {
		// Another watcher won the cursor CAS. The newer checkpoint is already
		// durable; discard this stale scan and let the next poll start fresh.
		return nil
	} else {
		return err
	}
}

func historyWatchCheckpointFromState(id string, state historyTieredFileState, now time.Time) teamstore.HistoryWatchCheckpoint {
	if !state.SourceRewriteBlocked {
		if fingerprint := strings.TrimSpace(transcriptCheckpointSourceFingerprint(state.Path, state.Offset)); fingerprint != "" {
			state.SourceFingerprint = fingerprint
		} else if state.Size > 0 {
			state.SourceRewriteBlocked = true
		}
	}
	checkpoint := teamstore.HistoryWatchCheckpoint{
		ID:                           id,
		Path:                         strings.TrimSpace(state.Path),
		Size:                         state.Size,
		ModTime:                      state.ModTime,
		SourceFingerprint:            strings.TrimSpace(state.SourceFingerprint),
		SourceRewriteBlocked:         state.SourceRewriteBlocked,
		OversizedRecordBlocked:       state.OversizedRecordBlocked,
		Offset:                       state.Offset,
		Line:                         state.Line,
		SessionID:                    strings.TrimSpace(state.SessionID),
		ThreadID:                     strings.TrimSpace(state.ThreadID),
		TeamsOriginThreadID:          strings.TrimSpace(state.TeamsOriginThreadID),
		TurnID:                       strings.TrimSpace(state.TurnID),
		TeamsOriginTurnID:            strings.TrimSpace(state.TeamsOriginTurnID),
		ExternalUserPromptSeen:       state.ExternalUserPromptSeen,
		LastFinalID:                  strings.TrimSpace(state.LastFinalID),
		LastFinalLine:                state.LastFinalLine,
		LastFinalStartOffset:         state.LastFinalStartOffset,
		LastFinalStartOffsetKnown:    state.LastFinalStartOffsetKnown,
		LastFinalThreadID:            strings.TrimSpace(state.LastFinalThreadID),
		LastFinalTurnID:              strings.TrimSpace(state.LastFinalTurnID),
		LastFinalTextHash:            strings.TrimSpace(state.LastFinalTextHash),
		TerminalBoundarySeen:         state.TerminalBoundarySeen,
		TerminalBoundaryLine:         state.TerminalBoundaryLine,
		UnresolvedContinuation:       state.UnresolvedContinuation,
		UnresolvedContinuationLine:   state.UnresolvedContinuationLine,
		UnresolvedContinuationOffset: state.UnresolvedContinuationOffset,
		PendingRootTaskStarted:       state.PendingRootTaskStarted,
		PendingRootTaskStartedLine:   state.PendingRootTaskStartedLine,
		PendingRootTaskStartedOffset: state.PendingRootTaskStartedOffset,
		UpdatedAt:                    now,
	}
	applyHistoryWatchPendingAssistant(&checkpoint, state.pendingAssistant)
	return checkpoint
}

func (b *Bridge) removeHistoryWatchCheckpoint(ctx context.Context, id string) error {
	return b.store.UpdateHistoryWatch(ctx, func(historyWatch map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		delete(historyWatch, id)
		return nil
	})
}

func (b *Bridge) removeHistoryWatchCheckpointIfCurrent(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint) error {
	if expected == nil {
		return nil
	}
	if err := b.store.DeleteHistoryWatchCheckpointIfCurrent(ctx, id, expected); errors.Is(err, teamstore.ErrHistoryWatchCheckpointConflict) {
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
		return codexhistory.Session{}, codexhistory.Project{}, false, nil
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
