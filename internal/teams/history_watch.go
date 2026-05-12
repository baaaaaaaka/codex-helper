package teams

import (
	"context"
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
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	initialized := !state.HistoryWatchReady.IsZero()
	paths := historyWatchPathsFromState(state)
	recent, err := historyTieredListSessionFilesInDirs(historyWatchRecentSessionDirs(root, now, historyWatchRecentDays))
	if err != nil {
		return err
	}
	paths = append(paths, recent...)
	if reconcile {
		reconciled, err := b.historyWatchReconcilePaths(ctx)
		if err != nil {
			if !initialized {
				return err
			}
		} else {
			paths = append(paths, reconciled...)
		}
	}
	paths = uniqueSortedCleanPaths(paths)
	if !initialized {
		return b.baselineCodexHistoryWatch(ctx, paths, now)
	}
	changes, err := historyWatchChangedPaths(paths, state)
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

func historyWatchChangedPaths(paths []string, state teamstore.State) ([]string, error) {
	states := make(map[string]historyTieredFileState, len(state.HistoryWatch))
	for _, checkpoint := range state.HistoryWatch {
		if path := cleanComparablePath(checkpoint.Path); path != "" {
			fileState := historyTieredFileStateFromHistoryWatch(checkpoint)
			fileState.Path = path
			states[path] = fileState
		}
	}
	changes, err := historyTieredDetectStatChanges(paths, states)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(changes))
	for _, change := range changes {
		out = append(out, change.Path)
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
	return b.store.Update(ctx, func(state *teamstore.State) error {
		if state.HistoryWatch == nil {
			state.HistoryWatch = make(map[string]teamstore.HistoryWatchCheckpoint)
		}
		for _, path := range paths {
			info, err := os.Stat(path)
			if err != nil || info.IsDir() {
				continue
			}
			id := historyWatchCheckpointID(path)
			state.HistoryWatch[id] = teamstore.HistoryWatchCheckpoint{
				ID:        id,
				Path:      path,
				Size:      info.Size(),
				ModTime:   info.ModTime(),
				Offset:    info.Size(),
				UpdatedAt: now,
			}
		}
		state.HistoryWatchReady = now
		return nil
	})
}

func (b *Bridge) syncCodexHistoryWatchPath(ctx context.Context, path string, now time.Time) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	id := historyWatchCheckpointID(path)
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	checkpoint := state.HistoryWatch[id]
	previous := historyTieredFileStateFromHistoryWatch(checkpoint)
	if strings.TrimSpace(previous.Path) == "" {
		previous.Path = path
	}
	result, err := historyTieredScanTail(path, previous, historyTieredMaxTailBytes)
	if os.IsNotExist(err) {
		return b.removeHistoryWatchCheckpoint(ctx, id)
	}
	if err != nil {
		return err
	}
	if result.TooLarge {
		result, err = historyTieredScanTail(path, previous, 0)
		if err != nil {
			return err
		}
	}
	sessionStarted, blocked, err := b.publishHistoryWatchSessionStart(ctx, path, result)
	if err != nil {
		return err
	}
	if blocked {
		return nil
	}
	for _, final := range result.Finals {
		if strings.TrimSpace(final.Key) == "" || final.Key == checkpoint.LastFinalID {
			continue
		}
		handled, err := b.publishHistoryWatchFinal(ctx, path, final, publishHistoryWatchFinalOptions{
			ForceDetectedNotification: sessionStarted,
		})
		if err != nil {
			return err
		}
		if !handled {
			return nil
		}
	}
	return b.recordHistoryWatchCheckpoint(ctx, id, result.State, now)
}

func historyTieredFileStateFromHistoryWatch(checkpoint teamstore.HistoryWatchCheckpoint) historyTieredFileState {
	return historyTieredFileState{
		Path:             strings.TrimSpace(checkpoint.Path),
		Size:             checkpoint.Size,
		ModTime:          checkpoint.ModTime,
		Offset:           checkpoint.Offset,
		Line:             checkpoint.Line,
		SessionID:        strings.TrimSpace(checkpoint.SessionID),
		ThreadID:         strings.TrimSpace(checkpoint.ThreadID),
		TurnID:           strings.TrimSpace(checkpoint.TurnID),
		LastFinalID:      strings.TrimSpace(checkpoint.LastFinalID),
		pendingAssistant: historyWatchPendingAssistantFromCheckpoint(checkpoint),
	}
}

func historyWatchCheckpointID(path string) string {
	if abs, err := filepath.Abs(strings.TrimSpace(path)); err == nil {
		path = abs
	}
	return "history-watch:" + shortStableID(filepath.Clean(path))
}

func (b *Bridge) recordHistoryWatchCheckpoint(ctx context.Context, id string, state historyTieredFileState, now time.Time) error {
	return b.store.Update(ctx, func(storeState *teamstore.State) error {
		if storeState.HistoryWatch == nil {
			storeState.HistoryWatch = make(map[string]teamstore.HistoryWatchCheckpoint)
		}
		checkpoint := teamstore.HistoryWatchCheckpoint{
			ID:          id,
			Path:        strings.TrimSpace(state.Path),
			Size:        state.Size,
			ModTime:     state.ModTime,
			Offset:      state.Offset,
			Line:        state.Line,
			SessionID:   strings.TrimSpace(state.SessionID),
			ThreadID:    strings.TrimSpace(state.ThreadID),
			TurnID:      strings.TrimSpace(state.TurnID),
			LastFinalID: strings.TrimSpace(state.LastFinalID),
			UpdatedAt:   now,
		}
		applyHistoryWatchPendingAssistant(&checkpoint, state.pendingAssistant)
		storeState.HistoryWatch[id] = checkpoint
		return nil
	})
}

func (b *Bridge) removeHistoryWatchCheckpoint(ctx context.Context, id string) error {
	return b.store.Update(ctx, func(state *teamstore.State) error {
		delete(state.HistoryWatch, id)
		return nil
	})
}

func (b *Bridge) publishHistoryWatchSessionStart(ctx context.Context, path string, result historyTieredTailResult) (bool, bool, error) {
	if isSubagent, err := codexhistory.SessionFileIsSubagentContext(ctx, path); err == nil && isSubagent {
		return false, false, nil
	} else if err != nil && !os.IsNotExist(err) {
		return false, false, err
	}
	record, ok := historyTieredFirstVisibleUserPromptRecord(result.Records)
	if !ok {
		return false, false, nil
	}
	if historyWatchPromptLooksTeamsOrigin(record.Text) {
		return false, false, nil
	}
	threadID := strings.TrimSpace(firstNonEmptyString(record.ThreadID, result.State.ThreadID))
	local, project, ok, err := b.findHistoryWatchCodexSession(ctx, path, threadID)
	if err != nil || !ok {
		return false, true, err
	}
	if existing := b.reg.SessionByCodexThreadID(local.SessionID); existing != nil && isActiveSessionStatus(existing.Status) {
		if err := b.ensureDurableSession(ctx, existing); err != nil {
			return false, false, err
		}
		return false, false, nil
	}
	if strings.TrimSpace(local.FirstPrompt) == "" {
		local.FirstPrompt = formatTranscriptRecordForTeams(record)
	}
	_, err = b.publishCodexSessionLocalWithOptions(ctx, local, project, publishCodexSessionOptions{
		ChatCreatedNotification:         false,
		ChatCreatedNoticeAfterImport:    true,
		LocalSessionStartedNotification: true,
	})
	if err != nil {
		return false, false, err
	}
	return true, false, nil
}

func historyTieredFirstVisibleUserPromptRecord(records []TranscriptRecord) (TranscriptRecord, bool) {
	for _, record := range records {
		if record.Kind != TranscriptKindUser {
			continue
		}
		if strings.TrimSpace(formatTranscriptRecordForTeams(record)) == "" {
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
