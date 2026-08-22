package teams

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	executionAnchorStateUnresolved  = "unresolved"
	executionAnchorObservedTaskCap  = 32
	executionFenceReconcileInterval = 15 * time.Second
	executionFenceReconcileTimeout  = 1 * time.Second
	// A probe is only a best-effort retry throttle.  Bounding this map keeps a
	// stream of legacy/orphan anchors from turning one bridge process into an
	// unbounded memory cache; eviction can at worst cause one extra safe probe.
	executionFenceProbeCacheLimit = 1024
)

const (
	executionAnchorProofFence = "app_server_fence"
	executionAnchorProofTurn  = "outer_turn_terminal"
)

// executionAnchorProof is a capability to clear one specific anchor.  It is
// intentionally a value snapshot rather than a turn ID: a late callback must
// not clear a newer anchor for the same outer turn after a clear/recreate race.
type executionAnchorProof struct {
	Kind              string
	SessionID         string
	ThreadID          string
	SourcePath        string
	SourceFingerprint string
	OuterTurnID       string
	CodexTurnID       string
	Generation        int64
	CutoffRecordID    string
	CutoffLine        int
	CutoffOffset      int64
}

func executionAnchorProofFromAnchor(sessionID string, anchor teamstore.ExecutionAnchor, kind string) executionAnchorProof {
	return executionAnchorProof{
		Kind:              strings.TrimSpace(kind),
		SessionID:         strings.TrimSpace(firstNonEmptyString(sessionID, anchor.SessionID)),
		ThreadID:          strings.TrimSpace(anchor.ThreadID),
		SourcePath:        cleanComparablePath(anchor.SourcePath),
		SourceFingerprint: strings.TrimSpace(anchor.SourceFingerprint),
		OuterTurnID:       strings.TrimSpace(anchor.OuterTurnID),
		CodexTurnID:       strings.TrimSpace(anchor.CodexTurnID),
		Generation:        anchor.Generation,
		CutoffRecordID:    strings.TrimSpace(anchor.CutoffRecordID),
		CutoffLine:        anchor.CutoffLine,
		CutoffOffset:      anchor.CutoffOffset,
	}
}

func executionAnchorProofMatchesAnchor(proof executionAnchorProof, anchor *teamstore.ExecutionAnchor, sessionID string) bool {
	if anchor == nil || strings.TrimSpace(anchor.State) == "resolved" {
		return false
	}
	if strings.TrimSpace(proof.SessionID) == "" || strings.TrimSpace(proof.SessionID) != strings.TrimSpace(sessionID) {
		return false
	}
	if strings.TrimSpace(anchor.SessionID) != "" && strings.TrimSpace(anchor.SessionID) != strings.TrimSpace(proof.SessionID) {
		return false
	}
	if strings.TrimSpace(anchor.ThreadID) != strings.TrimSpace(proof.ThreadID) ||
		strings.TrimSpace(anchor.OuterTurnID) != strings.TrimSpace(proof.OuterTurnID) ||
		strings.TrimSpace(anchor.CodexTurnID) != strings.TrimSpace(proof.CodexTurnID) ||
		cleanComparablePath(anchor.SourcePath) != strings.TrimSpace(proof.SourcePath) ||
		strings.TrimSpace(anchor.SourceFingerprint) != strings.TrimSpace(proof.SourceFingerprint) ||
		anchor.CutoffRecordID != proof.CutoffRecordID ||
		anchor.CutoffLine != proof.CutoffLine ||
		anchor.CutoffOffset != proof.CutoffOffset {
		return false
	}
	// Generation zero is a pre-anchor legacy value.  It is accepted only for
	// the exact same legacy snapshot; newly created anchors are always assigned
	// a positive generation before any automatic proof can clear them.
	return anchor.Generation == proof.Generation
}

type executionAnchorTailObservation struct {
	TaskIDs        []string
	Continuation   bool
	Unknown        bool
	SourceObserved bool
	SourceSize     int64
	SourceModTime  time.Time
	Scanned        bool
}

func executionAnchorActive(anchor *teamstore.ExecutionAnchor) bool {
	return anchor != nil && strings.TrimSpace(anchor.State) != "resolved"
}

func executionAnchorForLegacyTurn(session Session, local codexhistory.Session, checkpoint teamstore.ImportCheckpoint, turn teamstore.Turn) teamstore.ExecutionAnchor {
	now := time.Now()
	sourcePath := strings.TrimSpace(firstNonEmptyString(checkpoint.SourcePath, local.FilePath))
	threadID := strings.TrimSpace(firstNonEmptyString(turn.CodexThreadID, session.CodexThreadID, local.SessionID))
	anchor := teamstore.ExecutionAnchor{
		SessionID:         strings.TrimSpace(session.ID),
		ThreadID:          threadID,
		OuterTurnID:       strings.TrimSpace(turn.ID),
		CodexTurnID:       strings.TrimSpace(turn.CodexTurnID),
		SourcePath:        sourcePath,
		SourceFingerprint: strings.TrimSpace(checkpoint.SourceFingerprint),
		Reason:            strings.TrimSpace(turn.RecoveryReason),
		State:             executionAnchorStateUnresolved,
		Generation:        1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	// A Running turn recovered after a helper restart has no verified source
	// boundary: the checkpoint may have advanced while the old app-server
	// execution was still alive. Starting the proof at that cursor could skip
	// the only terminal record that can establish ownership, so force the
	// conservative beginning-of-source boundary for this recovery reason.
	if turn.Status == teamstore.TurnStatusRunning && strings.TrimSpace(turn.RecoveryReason) == recoveryReasonAmbiguousAfterHelperRestart {
		return anchor
	}
	// A checkpoint updated after the interruption may already include the
	// orphan continuation. Never use that cursor as the safety boundary. A
	// fresh interruption normally has a checkpoint from before the turn, so it
	// remains a useful bounded scan start in that case.
	if !turn.InterruptedAt.IsZero() && !checkpoint.UpdatedAt.IsZero() && checkpoint.UpdatedAt.After(turn.InterruptedAt) {
		return anchor
	}
	anchor.CutoffRecordID = strings.TrimSpace(checkpoint.LastRecordID)
	anchor.CutoffLine = checkpoint.LastSourceLine
	anchor.CutoffOffset = checkpoint.LastOffset
	return anchor
}

func executionAnchorTurn(state teamstore.State, anchor teamstore.ExecutionAnchor, session Session) teamstore.Turn {
	if id := strings.TrimSpace(anchor.OuterTurnID); id != "" {
		if turn, ok := state.Turns[id]; ok {
			return turn
		}
		return teamstore.Turn{
			ID:             id,
			SessionID:      strings.TrimSpace(session.ID),
			CodexThreadID:  strings.TrimSpace(anchor.ThreadID),
			CodexTurnID:    strings.TrimSpace(anchor.CodexTurnID),
			RecoveryReason: strings.TrimSpace(anchor.Reason),
			Status:         teamstore.TurnStatusInterrupted,
		}
	}
	return teamstore.Turn{
		SessionID:      strings.TrimSpace(session.ID),
		CodexThreadID:  strings.TrimSpace(anchor.ThreadID),
		CodexTurnID:    strings.TrimSpace(anchor.CodexTurnID),
		RecoveryReason: strings.TrimSpace(anchor.Reason),
		Status:         teamstore.TurnStatusInterrupted,
	}
}

func executionTurnBoundaryTime(turn teamstore.Turn) time.Time {
	var preferred time.Time
	switch turn.Status {
	case teamstore.TurnStatusInterrupted:
		preferred = turn.InterruptedAt
	case teamstore.TurnStatusCompleted:
		preferred = turn.CompletedAt
	case teamstore.TurnStatusFailed:
		preferred = turn.FailedAt
	case teamstore.TurnStatusRunning:
		preferred = turn.StartedAt
	case teamstore.TurnStatusQueued:
		preferred = turn.QueuedAt
	}
	if !preferred.IsZero() {
		return preferred
	}
	for _, value := range []time.Time{turn.StartedAt, turn.QueuedAt, turn.CreatedAt, turn.UpdatedAt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func executionAnchorTurnOwnershipConfirmed(state teamstore.State, anchor teamstore.ExecutionAnchor) bool {
	turnID := strings.TrimSpace(anchor.OuterTurnID)
	if turnID == "" {
		return false
	}
	turn, ok := state.Turns[turnID]
	return ok && strings.TrimSpace(turn.RecoveryReason) == recoveryReasonCodexExecutionConfirmed
}

func (b *Bridge) ensureUnresolvedExecutionAnchor(ctx context.Context, session Session, local codexhistory.Session, checkpoint teamstore.ImportCheckpoint, turn teamstore.Turn) (teamstore.ImportCheckpoint, error) {
	checkpointID := transcriptCheckpointID(session.ID)
	updated, _, err := b.store.UpdateImportCheckpoint(ctx, checkpointID, func(current teamstore.ImportCheckpoint, _ bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		if executionAnchorActive(current.UnresolvedExecution) {
			next := *current.UnresolvedExecution
			changed := false
			if next.Generation <= 0 {
				next.Generation = current.ExecutionAnchorGeneration
				if next.Generation <= 0 {
					next.Generation = 1
				}
				changed = true
			}
			if current.ExecutionAnchorGeneration > next.Generation {
				next.Generation = current.ExecutionAnchorGeneration
				changed = true
			}
			if current.ExecutionAnchorGeneration < next.Generation {
				current.ExecutionAnchorGeneration = next.Generation
				changed = true
			}
			if strings.TrimSpace(next.SourcePath) == "" {
				if path := strings.TrimSpace(firstNonEmptyString(current.SourcePath, checkpoint.SourcePath, local.FilePath)); path != "" {
					next.SourcePath = path
					changed = true
				}
			}
			if strings.TrimSpace(next.SourceFingerprint) == "" {
				if fingerprint := strings.TrimSpace(firstNonEmptyString(current.SourceFingerprint, checkpoint.SourceFingerprint)); fingerprint != "" {
					next.SourceFingerprint = fingerprint
					changed = true
				}
			}
			if strings.TrimSpace(next.ThreadID) == "" {
				if thread := strings.TrimSpace(firstNonEmptyString(turn.CodexThreadID, session.CodexThreadID, local.SessionID)); thread != "" {
					next.ThreadID = thread
					changed = true
				}
			}
			if strings.TrimSpace(next.CodexTurnID) == "" && strings.TrimSpace(turn.CodexTurnID) != "" {
				next.CodexTurnID = strings.TrimSpace(turn.CodexTurnID)
				changed = true
			}
			if changed {
				next.UpdatedAt = now
				current.UnresolvedExecution = &next
			}
			return current, changed, nil
		}
		if strings.TrimSpace(current.SourcePath) == "" {
			current.SourcePath = strings.TrimSpace(firstNonEmptyString(checkpoint.SourcePath, local.FilePath))
		}
		anchor := executionAnchorForLegacyTurn(session, local, current, turn)
		anchor.Generation = current.ExecutionAnchorGeneration + 1
		if anchor.Generation <= 0 {
			anchor.Generation = 1
		}
		if strings.TrimSpace(anchor.SourcePath) == "" {
			anchor.SourcePath = strings.TrimSpace(firstNonEmptyString(checkpoint.SourcePath, local.FilePath))
		}
		if strings.TrimSpace(anchor.SourceFingerprint) == "" {
			anchor.SourceFingerprint = strings.TrimSpace(firstNonEmptyString(current.SourceFingerprint, checkpoint.SourceFingerprint))
		}
		if !turn.InterruptedAt.IsZero() && !current.UpdatedAt.IsZero() && current.UpdatedAt.After(turn.InterruptedAt) {
			anchor.CutoffRecordID = ""
			anchor.CutoffLine = 0
			anchor.CutoffOffset = 0
		}
		if anchor.CreatedAt.IsZero() {
			anchor.CreatedAt = now
		}
		anchor.UpdatedAt = now
		current.ID = checkpointID
		current.SessionID = strings.TrimSpace(session.ID)
		current.ExecutionAnchorGeneration = anchor.Generation
		current.UnresolvedExecution = &anchor
		return current, true, nil
	})
	if err != nil {
		return teamstore.ImportCheckpoint{}, err
	}
	return updated, nil
}

func (b *Bridge) persistUnresolvedExecutionAnchorForTurn(ctx context.Context, session Session, turn teamstore.Turn) (teamstore.PersistInterruptedTurnWithAnchorResult, error) {
	if b == nil || b.store == nil {
		return teamstore.PersistInterruptedTurnWithAnchorResult{}, nil
	}
	threadID := firstNonEmptyString(turn.CodexThreadID, session.CodexThreadID)
	request := teamstore.PersistInterruptedTurnWithAnchorRequest{
		SessionID:          strings.TrimSpace(session.ID),
		TurnID:             strings.TrimSpace(turn.ID),
		CheckpointID:       transcriptCheckpointID(session.ID),
		CodexThreadID:      threadID,
		CodexTurnID:        strings.TrimSpace(turn.CodexTurnID),
		RecoveryReason:     strings.TrimSpace(turn.RecoveryReason),
		ConservativeCutoff: strings.TrimSpace(turn.RecoveryReason) == recoveryReasonAmbiguousAfterHelperRestart,
		Anchor: teamstore.ExecutionAnchor{
			SessionID:   strings.TrimSpace(session.ID),
			ThreadID:    threadID,
			OuterTurnID: strings.TrimSpace(turn.ID),
			CodexTurnID: strings.TrimSpace(turn.CodexTurnID),
			Reason:      strings.TrimSpace(turn.RecoveryReason),
			State:       executionAnchorStateUnresolved,
		},
	}
	result, err := b.store.PersistInterruptedTurnWithAnchor(ctx, request)
	if err != nil {
		return result, err
	}
	// A durable terminal status alone is not app-server ownership proof.  The
	// only automatic resolver is the typed outer-turn callback (or the
	// app-server cancellation fence); ordinary callbacks must leave the anchor
	// unresolved so an orphan continuation cannot be published.
	return result, nil
}

func (b *Bridge) clearUnresolvedExecutionAnchorWithProof(ctx context.Context, proof executionAnchorProof) error {
	if b == nil || b.store == nil || strings.TrimSpace(proof.SessionID) == "" || strings.TrimSpace(proof.OuterTurnID) == "" {
		return nil
	}
	// Only typed ownership proofs can clear an anchor.  In particular, a
	// transcript terminal is diagnostic/quarantine evidence, never an automatic
	// resolver: an internal child can produce a matching terminal record while
	// the outer app-server execution remains alive.  A fence confirmation is
	// authoritative even for an old checkpoint that did not persist the optional
	// Codex turn ID, but it still has to match the full anchor snapshot and
	// generation.
	switch proof.Kind {
	case executionAnchorProofFence:
	case executionAnchorProofTurn:
		if strings.TrimSpace(proof.CodexTurnID) == "" {
			return nil
		}
	default:
		return nil
	}
	checkpointID := transcriptCheckpointID(proof.SessionID)
	checkpoint, found, err := b.store.ImportCheckpoint(ctx, checkpointID)
	if err != nil || !found || !executionAnchorProofMatchesAnchor(proof, checkpoint.UnresolvedExecution, proof.SessionID) {
		return err
	}
	resolvedTurnID := strings.TrimSpace(proof.OuterTurnID)
	turn, turnFound, err := b.store.TurnByID(ctx, resolvedTurnID)
	if err != nil {
		return err
	}
	if !turnFound {
		// An anchor without a durable owner cannot be cleared by a transcript
		// guess.  An app-server fence proof can still clear it only when the
		// fence supplied the exact outer identity; missing owner data remains
		// fail-closed for old/orphan checkpoints.
		return nil
	}
	if turn.Status != teamstore.TurnStatusInterrupted && turn.Status != teamstore.TurnStatusCompleted && turn.Status != teamstore.TurnStatusFailed {
		return nil
	}
	if strings.TrimSpace(proof.ThreadID) != "" && strings.TrimSpace(turn.CodexThreadID) != strings.TrimSpace(proof.ThreadID) {
		return nil
	}
	if strings.TrimSpace(proof.CodexTurnID) != "" && strings.TrimSpace(turn.CodexTurnID) != strings.TrimSpace(proof.CodexTurnID) {
		return nil
	}
	if turn.Status == teamstore.TurnStatusInterrupted && strings.TrimSpace(turn.RecoveryReason) != recoveryReasonCodexExecutionConfirmed && !isUnresolvedAmbiguousCodexRecoveryReason(turn.RecoveryReason) {
		return nil
	}
	return b.store.ClearExecutionAnchorAndConfirmTurn(ctx, teamstore.ExecutionAnchorClearRequest{
		CheckpointID:       checkpointID,
		SessionID:          proof.SessionID,
		ThreadID:           proof.ThreadID,
		SourcePath:         proof.SourcePath,
		SourceFingerprint:  proof.SourceFingerprint,
		OuterTurnID:        resolvedTurnID,
		CodexTurnID:        proof.CodexTurnID,
		Generation:         proof.Generation,
		CutoffRecordID:     proof.CutoffRecordID,
		CutoffLine:         proof.CutoffLine,
		CutoffOffset:       proof.CutoffOffset,
		RecoveryReasonFrom: turn.RecoveryReason,
		RecoveryReasonTo:   recoveryReasonCodexExecutionConfirmed,
	})
}

func (b *Bridge) reconcileExecutionFence(ctx context.Context, session Session) (bool, error) {
	if b == nil || b.executor == nil {
		return false, nil
	}
	if strings.TrimSpace(session.CodexThreadID) == "" {
		return false, nil
	}
	reconciler, ok := b.executor.(ExecutionFenceReconciler)
	if !ok {
		return false, nil
	}
	return reconciler.ReconcileExecutionFence(ctx, &session)
}

func executionFenceReconcileDue(anchor teamstore.ExecutionAnchor, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	return anchor.LastFenceCheckAt.IsZero() || now.Sub(anchor.LastFenceCheckAt) >= executionFenceReconcileInterval
}

func executionFenceProbeKey(sessionID string, anchor teamstore.ExecutionAnchor) string {
	return strings.Join([]string{
		strings.TrimSpace(sessionID),
		strings.TrimSpace(anchor.OuterTurnID),
		strings.TrimSpace(anchor.CodexTurnID),
		strconv.FormatInt(anchor.Generation, 10),
	}, "\x00")
}

// claimExecutionFenceProbe keeps the retry throttle in memory. Persisting a
// LastFenceCheckAt timestamp before every probe rewrites the whole JSON state
// (or opens a checkpoint transaction) even when the probe fails and the
// ownership state is unchanged. A restart may perform one extra probe, which
// is safe; the final resolver still requires the typed app-server proof.
func (b *Bridge) claimExecutionFenceProbe(sessionID string, anchor teamstore.ExecutionAnchor, now time.Time) bool {
	if b == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	key := executionFenceProbeKey(sessionID, anchor)
	b.executionFenceProbeMu.Lock()
	defer b.executionFenceProbeMu.Unlock()
	if b.executionFenceProbeAt == nil {
		b.executionFenceProbeAt = make(map[string]time.Time)
	}
	if last, ok := b.executionFenceProbeAt[key]; ok && now.Sub(last) < executionFenceReconcileInterval {
		return false
	}
	if len(b.executionFenceProbeAt) >= executionFenceProbeCacheLimit {
		var oldestKey string
		var oldestAt time.Time
		for candidate, at := range b.executionFenceProbeAt {
			if oldestKey == "" || at.Before(oldestAt) {
				oldestKey = candidate
				oldestAt = at
			}
		}
		if oldestKey != "" {
			delete(b.executionFenceProbeAt, oldestKey)
		}
	}
	// Preserve a recent durable timestamp written by an older helper version
	// during the migration window, while avoiding any new write on this path.
	if executionFenceReconcileDue(anchor, now) {
		b.executionFenceProbeAt[key] = now
		return true
	}
	b.executionFenceProbeAt[key] = anchor.LastFenceCheckAt
	return false
}

func (b *Bridge) reconcileExecutionFenceIfDue(ctx context.Context, session Session, anchor teamstore.ExecutionAnchor) (bool, error) {
	if b == nil || b.executor == nil {
		return false, nil
	}
	if _, ok := b.executor.(ExecutionFenceReconciler); !ok {
		return false, nil
	}
	if !b.claimExecutionFenceProbe(session.ID, anchor, time.Now()) {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, executionFenceReconcileTimeout)
	defer cancel()
	if strings.TrimSpace(session.CodexThreadID) == "" {
		session.CodexThreadID = strings.TrimSpace(anchor.ThreadID)
	}
	return b.reconcileExecutionFence(probeCtx, session)
}

func observeUnresolvedExecutionTail(anchor teamstore.ExecutionAnchor) executionAnchorTailObservation {
	path := strings.TrimSpace(anchor.SourcePath)
	if path == "" {
		return executionAnchorTailObservation{}
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return executionAnchorTailObservation{Unknown: true}
	}
	out := executionAnchorTailObservation{
		SourceObserved: true,
		SourceSize:     info.Size(),
		SourceModTime:  info.ModTime(),
	}
	// Stat is intentionally the cheap fast path. The bridge invokes this guard
	// before each sync stage, so an unchanged source must not reread the bounded
	// tail (or issue a second JSON parse) on every invocation.
	if anchor.ObservedSourceSize == info.Size() && !anchor.ObservedSourceModTime.IsZero() && anchor.ObservedSourceModTime.Equal(info.ModTime()) {
		return out
	}
	if anchor.CutoffOffset <= 0 || info.Size() <= anchor.CutoffOffset {
		return out
	}
	if info.Size()-anchor.CutoffOffset > historyTieredMaxTailBytes {
		out.Unknown = true
		out.Scanned = true
		return out
	}
	f, err := os.Open(path)
	if err != nil {
		out.Unknown = true
		out.Scanned = true
		return out
	}
	defer f.Close()
	if _, err := f.Seek(anchor.CutoffOffset, io.SeekStart); err != nil {
		out.Unknown = true
		out.Scanned = true
		return out
	}
	reader := bufio.NewReaderSize(f, historyTieredTailReaderSize)
	out.Scanned = true
	seen := make(map[string]struct{})
	for {
		read, readErr := historyTieredReadJSONLRecord(reader, historyTieredMaxRecordBytes, historyTieredMaxRecordReadBytes)
		complete := read.Complete || (readErr == io.EOF && read.BytesRead > 0)
		if read.BytesRead > 0 {
			if !complete || read.Oversized {
				out.Unknown = true
			} else {
				var obj map[string]json.RawMessage
				if err := json.Unmarshal(read.Line, &obj); err != nil {
					out.Unknown = true
				} else if task, thread, ok := executionAnchorLineSignal(obj); ok {
					if expected := strings.TrimSpace(anchor.ThreadID); expected != "" && thread != "" && thread != expected {
						// A shared Codex home can contain unrelated records. Keep the
						// anchor unresolved, but do not attribute another thread's ID.
					} else {
						out.Continuation = true
						if task != "" {
							if _, exists := seen[task]; !exists && len(out.TaskIDs) < executionAnchorObservedTaskCap {
								seen[task] = struct{}{}
								out.TaskIDs = append(out.TaskIDs, task)
							}
						}
					}
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			if readErr != io.EOF {
				out.Unknown = true
			}
			break
		}
	}
	sort.Strings(out.TaskIDs)
	return out
}

func executionAnchorLineSignal(obj map[string]json.RawMessage) (taskID string, threadID string, ok bool) {
	if obj == nil {
		return "", "", false
	}
	topType := strings.ToLower(strings.TrimSpace(jsonStringField(obj, "type", "method")))
	threadID = firstNonEmptyString(jsonStringField(obj, "thread_id", "threadId", "conversation_id", "conversationId"))
	taskID = firstNonEmptyString(jsonStringField(obj, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(obj, "turn"))
	if payload, has := jsonObjectField(obj, "payload"); has {
		threadID = firstNonEmptyString(jsonStringField(payload, "thread_id", "threadId"), threadID)
		taskID = firstNonEmptyString(jsonStringField(payload, "turn_id", "turnId", "task_id", "taskId", "id"), nestedJSONID(payload, "turn"), taskID)
		payloadType := strings.ToLower(strings.TrimSpace(jsonStringField(payload, "type", "event")))
		if payloadType == "task_started" || payloadType == "turn_context" || payloadType == "turn_started" || payloadType == "goal_continuation" {
			return firstNonEmptyString(taskID, jsonStringField(payload, "id")), threadID, true
		}
	}
	if params, has := jsonObjectField(obj, "params"); has {
		threadID = firstNonEmptyString(jsonStringField(params, "thread_id", "threadId"), threadID)
		taskID = firstNonEmptyString(jsonStringField(params, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(params, "turn"), taskID)
	}
	switch topType {
	case "task_started", "turn_context", "turn.started", "thread.started", "goal_continuation":
		return firstNonEmptyString(taskID, jsonStringField(obj, "id")), threadID, true
	case "response_item":
		// A response_item id is a message identity, not an execution identity.
		// Only treat it as an anchor observation when the protocol supplies an
		// explicit turn/task id; ordinary user prompts and tool items must not
		// make a valid outer terminal look like an orphan continuation.
		if strings.TrimSpace(firstNonEmptyString(jsonStringField(obj, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(obj, "turn"))) == "" {
			if payload, ok := jsonObjectField(obj, "payload"); ok && strings.TrimSpace(firstNonEmptyString(jsonStringField(payload, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(payload, "turn"))) == "" {
				return "", threadID, false
			}
		}
		return taskID, threadID, true
	case "event_msg":
		return "", threadID, false
	default:
		return "", threadID, false
	}
}

func (b *Bridge) observeUnresolvedExecutionAnchor(ctx context.Context, sessionID string, anchor teamstore.ExecutionAnchor) error {
	observation := observeUnresolvedExecutionTail(anchor)
	if !observation.SourceObserved && len(observation.TaskIDs) == 0 {
		return nil
	}
	checkpointID := transcriptCheckpointID(sessionID)
	_, _, err := b.store.UpdateImportCheckpoint(ctx, checkpointID, func(current teamstore.ImportCheckpoint, _ bool, _ time.Time) (teamstore.ImportCheckpoint, bool, error) {
		if !executionAnchorActive(current.UnresolvedExecution) {
			return current, false, nil
		}
		next := *current.UnresolvedExecution
		seen := make(map[string]struct{}, len(next.ObservedTaskIDs)+len(observation.TaskIDs))
		for _, id := range next.ObservedTaskIDs {
			if id = strings.TrimSpace(id); id != "" {
				seen[id] = struct{}{}
			}
		}
		changed := false
		for _, id := range observation.TaskIDs {
			if _, ok := seen[id]; ok || len(next.ObservedTaskIDs) >= executionAnchorObservedTaskCap {
				continue
			}
			next.ObservedTaskIDs = append(next.ObservedTaskIDs, id)
			seen[id] = struct{}{}
			changed = true
		}
		if len(next.ObservedTaskIDs) > 0 {
			sort.Strings(next.ObservedTaskIDs)
		}
		if observation.SourceObserved && (next.ObservedSourceSize != observation.SourceSize || !next.ObservedSourceModTime.Equal(observation.SourceModTime)) {
			next.ObservedSourceSize = observation.SourceSize
			next.ObservedSourceModTime = observation.SourceModTime
			changed = true
		}
		if !changed {
			return current, false, nil
		}
		current.UnresolvedExecution = &next
		return current, true, nil
	})
	return err
}

func (b *Bridge) recheckExecutionOwnershipAfterAnchorClear(ctx context.Context, session Session) (bool, error) {
	state, err := b.store.SessionExecutionStateSnapshot(ctx, session.ID, transcriptCheckpointID(session.ID))
	if err != nil {
		return true, err
	}
	if checkpoint, ok := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]; ok && executionAnchorActive(checkpoint.UnresolvedExecution) {
		return true, nil
	}
	turn, ambiguous := unresolvedAmbiguousCodexTurn(state, session)
	if !ambiguous {
		return false, nil
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if _, err := b.ensureUnresolvedExecutionAnchor(ctx, session, codexhistory.Session{}, checkpoint, turn); err != nil {
		return true, err
	}
	return true, nil
}

func (b *Bridge) guardAutomaticTranscriptSync(ctx context.Context, session Session, local codexhistory.Session, checkpoint teamstore.ImportCheckpoint, hasCheckpoint bool) (bool, teamstore.ImportCheckpoint, bool, error) {
	checkpointID := transcriptCheckpointID(session.ID)
	executionState, err := b.store.SessionExecutionStateSnapshot(ctx, session.ID, checkpointID)
	if err != nil {
		return true, checkpoint, hasCheckpoint, err
	}
	if current, ok := executionState.ImportCheckpoints[checkpointID]; ok {
		checkpoint = current
		hasCheckpoint = true
	}
	for _, turn := range executionState.Turns {
		if strings.TrimSpace(turn.SessionID) == strings.TrimSpace(session.ID) && turn.Status == teamstore.TurnStatusRunning {
			return true, checkpoint, hasCheckpoint, nil
		}
	}
	anchor := checkpoint.UnresolvedExecution
	var ambiguousTurn teamstore.Turn
	var ambiguous bool
	if !executionAnchorActive(anchor) {
		ambiguousTurn, ambiguous = unresolvedAmbiguousCodexTurn(executionState, session)
	}
	if executionAnchorActive(anchor) {
		// Cancellation paths may persist the anchor before a linked transcript
		// is discovered. Hydrate source/thread provenance as soon as the watcher
		// has it, so a helper restart can still prove a terminal boundary.
		hydrated, hydrateErr := b.ensureUnresolvedExecutionAnchor(ctx, session, local, checkpoint, executionAnchorTurn(executionState, *anchor, session))
		if hydrateErr != nil {
			return true, checkpoint, hasCheckpoint, hydrateErr
		}
		checkpoint = hydrated
		anchor = checkpoint.UnresolvedExecution
	}
	// Transcript inspection is diagnostic-only.  A matching terminal record is
	// not sufficient ownership provenance because an internal child can emit a
	// terminal before the outer app-server execution does.  Automatic clearing
	// therefore accepts only the typed outer-turn callback or the app-server
	// cancellation fence below.
	if executionAnchorActive(anchor) && executionAnchorTurnOwnershipConfirmed(executionState, *anchor) {
		if err := b.clearUnresolvedExecutionAnchorWithProof(ctx, executionAnchorProofFromAnchor(session.ID, *anchor, executionAnchorProofTurn)); err != nil {
			return true, checkpoint, hasCheckpoint, err
		}
		if unresolved, err := b.recheckExecutionOwnershipAfterAnchorClear(ctx, session); err != nil {
			return true, checkpoint, hasCheckpoint, err
		} else if unresolved {
			return true, checkpoint, hasCheckpoint, nil
		}
		return true, checkpoint, hasCheckpoint, nil
	}
	if executionAnchorActive(anchor) {
		if confirmed, err := b.reconcileExecutionFenceIfDue(ctx, session, *anchor); err != nil {
			// Keep the fail-closed behavior, but do not turn a transient probe
			// failure into a hot-looping sync error.
			_ = err
		} else if confirmed {
			if err := b.clearUnresolvedExecutionAnchorWithProof(ctx, executionAnchorProofFromAnchor(session.ID, *anchor, executionAnchorProofFence)); err != nil {
				return true, checkpoint, hasCheckpoint, err
			}
			if unresolved, err := b.recheckExecutionOwnershipAfterAnchorClear(ctx, session); err != nil {
				return true, checkpoint, hasCheckpoint, err
			} else if unresolved {
				return true, checkpoint, hasCheckpoint, nil
			}
			// Do not reuse a snapshot taken before the fence was cleared. The next
			// scheduled sync will start from the unchanged checkpoint.
			return true, checkpoint, hasCheckpoint, nil
		}
	}
	if !executionAnchorActive(anchor) && !ambiguous {
		return false, checkpoint, hasCheckpoint, nil
	}
	if !executionAnchorActive(anchor) {
		checkpoint, err = b.ensureUnresolvedExecutionAnchor(ctx, session, local, checkpoint, ambiguousTurn)
		if err != nil {
			return true, checkpoint, hasCheckpoint, err
		}
		anchor = checkpoint.UnresolvedExecution
	}
	if executionAnchorActive(anchor) {
		_ = b.observeUnresolvedExecutionAnchor(ctx, session.ID, *anchor)
		if observed, found, err := b.store.ImportCheckpoint(ctx, checkpointID); err == nil && found {
			checkpoint = observed
			anchor = checkpoint.UnresolvedExecution
		}
	}
	turn := ambiguousTurn
	if turn.ID == "" && executionAnchorActive(anchor) {
		turn = executionAnchorTurn(executionState, *anchor, session)
	}
	if err := b.blockAutomaticTranscriptSyncForAmbiguousExecution(ctx, session, local.FilePath, checkpoint, turn); err != nil {
		return true, checkpoint, hasCheckpoint, err
	}
	return true, checkpoint, hasCheckpoint, nil
}

func (b *Bridge) sessionExecutionOwnershipUnresolved(ctx context.Context, session Session) (bool, error) {
	unresolved, _, err := b.sessionExecutionOwnershipState(ctx, session)
	return unresolved, err
}

// sessionExecutionOwnershipState returns the strict execution-fence result and
// the durable snapshot used to reach it. Live-turn admission can inspect a
// history-only transcript quarantine from this snapshot without an additional
// store read on the common trusted path.
func (b *Bridge) sessionExecutionOwnershipState(ctx context.Context, session Session) (bool, teamstore.State, error) {
	checkpointID := transcriptCheckpointID(session.ID)
	state, err := b.store.SessionExecutionStateSnapshot(ctx, session.ID, checkpointID)
	if err != nil {
		return true, state, err
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	if executionAnchorActive(checkpoint.UnresolvedExecution) {
		// Do not use transcript-only terminal probes as an automatic resolver;
		// they cannot establish that an assistant final belongs to this outer
		// Teams execution.  Only a typed owner callback or the app-server fence
		// may clear the anchor.
		if executionAnchorTurnOwnershipConfirmed(state, *checkpoint.UnresolvedExecution) {
			if err := b.clearUnresolvedExecutionAnchorWithProof(ctx, executionAnchorProofFromAnchor(session.ID, *checkpoint.UnresolvedExecution, executionAnchorProofTurn)); err != nil {
				return true, state, err
			}
			unresolved, err := b.recheckExecutionOwnershipAfterAnchorClear(ctx, session)
			return unresolved, state, err
		}
		if confirmed, err := b.reconcileExecutionFenceIfDue(ctx, session, *checkpoint.UnresolvedExecution); err == nil && confirmed {
			if err := b.clearUnresolvedExecutionAnchorWithProof(ctx, executionAnchorProofFromAnchor(session.ID, *checkpoint.UnresolvedExecution, executionAnchorProofFence)); err != nil {
				return true, state, err
			}
			unresolved, err := b.recheckExecutionOwnershipAfterAnchorClear(ctx, session)
			return unresolved, state, err
		}
		return true, state, nil
	}
	turn, ambiguous := unresolvedAmbiguousCodexTurn(state, session)
	if !ambiguous {
		return false, state, nil
	}
	if _, err := b.ensureUnresolvedExecutionAnchor(ctx, session, codexhistory.Session{}, checkpoint, turn); err != nil {
		return true, state, err
	}
	return true, state, nil
}

// sessionLiveExecutionOwnershipUnresolved is the live-turn view of the
// execution fence. The strict view above remains authoritative for transcript
// recovery and old outbox delivery. Once a fresh thread has been durably
// recorded as the session's live branch, that old history fence must no longer
// reject ordinary new Teams turns on the fresh thread.
func (b *Bridge) sessionLiveExecutionOwnershipUnresolved(ctx context.Context, session Session) (bool, error) {
	unresolved, state, err := b.sessionExecutionOwnershipState(ctx, session)
	if err != nil {
		return true, err
	}
	checkpointID := transcriptCheckpointID(session.ID)
	if !unresolved {
		checkpoint := state.ImportCheckpoints[checkpointID]
		quarantine := checkpoint.TranscriptQuarantine
		if quarantine == nil {
			return false, nil
		}
		if strings.TrimSpace(quarantine.LiveBranchThreadID) == "" {
			return true, nil
		}
		liveSessionThreadID, err := b.durableSessionThreadID(ctx, session)
		if err != nil {
			return true, err
		}
		return strings.TrimSpace(quarantine.LiveBranchThreadID) != liveSessionThreadID, nil
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	anchor := checkpoint.UnresolvedExecution
	liveSessionThreadID, err := b.durableSessionThreadID(ctx, session)
	if err != nil {
		return true, err
	}
	if executionAnchorActive(anchor) && strings.TrimSpace(anchor.LiveBranchThreadID) != "" &&
		strings.TrimSpace(anchor.LiveBranchThreadID) == liveSessionThreadID {
		return false, nil
	}
	return true, nil
}

func (b *Bridge) liveTurnExecutionOwnershipUnresolved(ctx context.Context, session Session, turn teamstore.Turn) (bool, error) {
	if turn.StartNewCodexThread {
		return false, nil
	}
	unresolved, state, err := b.sessionExecutionOwnershipState(ctx, session)
	if err != nil {
		return true, err
	}
	checkpointID := transcriptCheckpointID(session.ID)
	if !unresolved {
		checkpoint := state.ImportCheckpoints[checkpointID]
		quarantine := checkpoint.TranscriptQuarantine
		if quarantine == nil {
			return false, nil
		}
		if strings.TrimSpace(quarantine.LiveBranchThreadID) == "" {
			return true, nil
		}
		liveSessionThreadID, err := b.durableSessionThreadID(ctx, session)
		if err != nil {
			return true, err
		}
		liveThread := strings.TrimSpace(quarantine.LiveBranchThreadID)
		if liveThread == liveSessionThreadID || liveThread == strings.TrimSpace(turn.CodexThreadID) {
			return false, nil
		}
		return true, nil
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	anchor := checkpoint.UnresolvedExecution
	liveSessionThreadID, err := b.durableSessionThreadID(ctx, session)
	if err != nil {
		return true, err
	}
	if executionAnchorActive(anchor) && strings.TrimSpace(anchor.LiveBranchThreadID) != "" {
		liveThread := strings.TrimSpace(anchor.LiveBranchThreadID)
		if liveThread == liveSessionThreadID || liveThread == strings.TrimSpace(turn.CodexThreadID) {
			return false, nil
		}
	}
	return true, nil
}

func (b *Bridge) durableSessionThreadID(ctx context.Context, session Session) (string, error) {
	liveSessionThreadID := strings.TrimSpace(session.CodexThreadID)
	if durableSessions, err := b.store.SessionsByID(ctx, []string{session.ID}); err != nil {
		return liveSessionThreadID, err
	} else if durable, ok := durableSessions[strings.TrimSpace(session.ID)]; ok && strings.TrimSpace(durable.CodexThreadID) != "" {
		liveSessionThreadID = strings.TrimSpace(durable.CodexThreadID)
	}
	return liveSessionThreadID, nil
}
