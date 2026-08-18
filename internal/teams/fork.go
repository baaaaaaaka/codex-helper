package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const forkHistoryCompleteMarker = "History import complete. The new Codex chat is ready."

const forkNativeReconcileWindow = 15 * time.Minute

type forkHistorySnapshot struct {
	Items    []teamstore.ForkHistoryItem
	Metadata teamstore.ForkManifestMetadata
}

func isForkDeliveryOutbox(msg teamstore.OutboxMessage) bool {
	return strings.TrimSpace(msg.ForkOperationID) != ""
}

func isForkHistoryOutbox(msg teamstore.OutboxMessage) bool {
	return isForkDeliveryOutbox(msg) &&
		strings.TrimSpace(msg.ForkRole) != "" &&
		strings.TrimSpace(msg.ForkRole) != "link" &&
		strings.TrimSpace(msg.ForkRole) != "complete-marker" &&
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(msg.Kind)), "fork-history-")
}

func (b *Bridge) forkWorkSession(ctx context.Context, parent *Session, command ChatMessage) error {
	if parent == nil {
		return fmt.Errorf("fork parent session is required")
	}
	if !isActiveSessionStatus(parent.Status) {
		return fmt.Errorf("session %q is not active", parent.ID)
	}
	if strings.TrimSpace(parent.CodexThreadID) == "" {
		return fmt.Errorf("session %q does not have a native Codex thread to fork", parent.ID)
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	unresolved, err := b.sessionExecutionOwnershipUnresolved(ctx, *parent)
	if err != nil {
		return err
	}
	operationID := forkOperationID(parent.ID, command.ID, command.Body.Content)
	childID := "fork-session:" + shortStableID(operationID)
	now := time.Now()
	child := teamstore.SessionContext{
		ID:                          childID,
		Status:                      teamstore.SessionStatusStaging,
		TeamsTopic:                  forkChildTopic(parent.Topic),
		TitleSource:                 parent.TitleSource,
		Cwd:                         parent.Cwd,
		RunnerKind:                  "executor",
		ModelGeneration:             parent.ModelGeneration,
		ModelProfile:                parent.ModelProfile,
		ModelSelectionSource:        parent.ModelSelectionSource,
		PendingModelProfile:         parent.PendingModelProfile,
		PendingModelSelectionSource: parent.PendingModelSelectionSource,
		PendingModelRequestedAt:     parent.PendingModelRequestedAt,
		PendingReasoningEffort:      parent.PendingReasoningEffort,
		PendingReasoningSource:      parent.PendingReasoningSource,
		ReasoningEffort:             parent.ReasoningEffort,
		ReasoningEffortSource:       parent.ReasoningEffortSource,
		CreatedAt:                   now,
		UpdatedAt:                   now,
	}
	op, created, err := b.store.BeginFork(ctx, teamstore.ForkBeginRequest{
		OperationID:          operationID,
		CommandInboundID:     command.ID,
		ParentSessionID:      parent.ID,
		ParentChatID:         parent.ChatID,
		ParentThreadID:       parent.CodexThreadID,
		ChildSession:         child,
		OwnerMachineID:       b.machine.ID,
		OwnerLeaseGeneration: b.currentLeaseGeneration(),
		HistoryPlanVersion:   2,
		ForkWindowStart:      now,
		ForkWindowEnd:        now.Add(forkNativeReconcileWindow),
		Now:                  now,
	})
	if err != nil {
		if errors.Is(err, teamstore.ErrForkAlreadyInProgress) {
			existing, fenced, lookupErr := b.store.ParentFork(ctx, parent.ID)
			if lookupErr != nil {
				return lookupErr
			}
			if fenced && strings.TrimSpace(existing.ID) != "" {
				return b.queueForkNotice(ctx, parent.ID, existing.ID, parent.ChatID, "fork-pending", "⏳ A fork is already in progress for this chat. I will keep the existing operation and send its link when the history is confirmed; no duplicate fork was started.")
			}
		}
		return err
	}
	if !created {
		if unresolved {
			// The durable operation is already staged; reconciliation will resume
			// only after the parent execution fence is resolved.
			return nil
		}
		if teamstore.ForkPhaseTerminal(op.Phase) {
			return b.resumeForkOperation(ctx, parent, op)
		}
		return b.queueForkNotice(ctx, parent.ID, op.ID, parent.ChatID, "fork-pending", "⏳ A fork is already in progress for this chat. I will keep the existing operation and send its link when the history is confirmed; no duplicate fork was started.")
	}
	if claimed, err := b.claimForkOperation(ctx, op); err != nil {
		return err
	} else {
		op = claimed
	}
	if unresolved {
		if err := b.sendToChat(ctx, parent.ChatID, "⏳ Fork staged. The parent Codex execution is still unconfirmed; I will continue the fork only after that ownership fence is resolved."); err != nil {
			return err
		}
		return nil
	}
	_, ok, err := b.store.ForkCutoff(ctx, op.ID)
	if err != nil {
		return err
	}
	if !ok {
		return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, fmt.Errorf("fork cutoff for operation %q is not durable", op.ID))
	}
	return b.queueForkNotice(ctx, parent.ID, op.ID, parent.ChatID, "fork-progress", "⏳ Fork requested. I fixed the parent cutoff and am preparing the new chat; I will send its link only after the visible history is confirmed.")
}

// queueForkNotice is deliberately queue-only. Fork commands are handled by
// the Teams polling path; doing a synchronous Graph flush here would make a
// large parent-chat backlog (or a slow Graph request) block all subsequent
// polling. The owner loop sends the notice under the normal bounded outbox
// budget.
func (b *Bridge) queueForkNotice(ctx context.Context, sessionID string, operationID string, chatID string, kind string, body string) error {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return fmt.Errorf("fork notice operation id is required")
	}
	return b.queueOrSendOutboxChunks(ctx, sessionID, "fork:"+operationID, chatID, kind, body, outboxQueueOptions{}, true)
}

func (b *Bridge) deferSessionMessageDuringFork(ctx context.Context, session *Session, msg ChatMessage) error {
	if session == nil {
		return nil
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	inbound, created, err := b.persistInboundWithStatusAndSource(ctx, session, msg, teamstore.InboundStatusDeferred, "teams_session_fork_deferred")
	if err != nil {
		return err
	}
	if created {
		return b.sendExternalDeferredReceipt(ctx, session.ChatID, inbound, "⏳ I received this message. The parent chat is being forked; I will process it after the fork operation finishes.")
	}
	return nil
}

func (b *Bridge) deferForkChildMessage(ctx context.Context, session *Session, msg ChatMessage, _ string) error {
	if session == nil {
		return nil
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	_, _, err := b.persistInboundWithStatusAndSource(ctx, session, msg, teamstore.InboundStatusDeferred, "teams_fork_child_deferred")
	return err
}

// pollStagedForkChildren gives a child chat a durable input path before the
// child session enters the normal active registry projection. Messages are
// persisted as deferred input and are replayed only after ActivateFork.
func (b *Bridge) pollStagedForkChildren(ctx context.Context, top int) error {
	if b == nil || b.store == nil {
		return nil
	}
	state, err := b.store.ForkPollingSnapshot(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	polled := 0
	limit := b.effectiveMaxWorkChatPollsPerCycle()
	if limit <= 0 {
		limit = 1
	}
	var firstErr error
	for _, op := range state.ForkOperations {
		if polled >= limit {
			break
		}
		switch op.Phase {
		case teamstore.ForkPhaseChildChatStaged, teamstore.ForkPhaseHistoryPublishing, teamstore.ForkPhaseHistoryVerified:
		default:
			continue
		}
		child, ok := state.Sessions[op.ChildSessionID]
		if !ok || isActiveSessionStatus(string(child.Status)) || strings.TrimSpace(child.TeamsChatID) == "" {
			continue
		}
		poll, hasPoll := state.ChatPolls[child.TeamsChatID]
		decision := decideInboundPoll(inboundPollInput{
			ChatID:           child.TeamsChatID,
			Role:             inboundPollRoleWork,
			Poll:             poll,
			HasPoll:          hasPoll,
			SessionUpdatedAt: child.UpdatedAt,
			Now:              now,
		})
		if !decision.Due {
			continue
		}
		childSession := registrySessionFromDurable(child)
		if _, err := b.pollChatWithRoleStateOptions(ctx, child.TeamsChatID, effectiveOwnerPollTop(top), inboundPollRoleWork, false, poll, hasPoll, pollChatWithRoleOptions{
			AllowBacklogDrain:        true,
			MaxBacklogActions:        1,
			RecoverStaleContinuation: true,
		}, func(ctx context.Context, msg ChatMessage, text string) error {
			return b.deferForkChildMessage(ctx, &childSession, msg, text)
		}); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		polled++
	}
	return firstErr
}

func (b *Bridge) resumeForkOperation(ctx context.Context, parent *Session, op teamstore.ForkOperation) error {
	if teamstore.ForkPhaseTerminal(op.Phase) {
		if op.Phase == teamstore.ForkPhaseLinkSent {
			return nil
		}
		return fmt.Errorf("fork operation %q is already %s", op.ID, op.Phase)
	}
	if err := b.reconcileForkOperation(ctx, op); err != nil {
		return err
	}
	updated, ok, err := b.store.ForkOperation(ctx, op.ID)
	if err != nil {
		return err
	}
	if !ok {
		return teamstore.ErrForkNotFound
	}
	if teamstore.ForkPhaseTerminal(updated.Phase) && updated.Phase != teamstore.ForkPhaseLinkSent {
		if strings.TrimSpace(updated.LastError) != "" {
			return fmt.Errorf("fork operation %q is already %s: %s", updated.ID, updated.Phase, strings.TrimSpace(updated.LastError))
		}
		return fmt.Errorf("fork operation %q is already %s", updated.ID, updated.Phase)
	}
	// Recovery is driven by the owner loop. A fork may legitimately remain
	// pending while its history or link outbox is waiting for the next bounded
	// flush cycle; that is not a command failure and must not trigger a retry
	// that creates another operation.
	if updated.Phase != teamstore.ForkPhaseLinkSent {
		return nil
	}
	return nil
}

// reconcileForkOperations advances only durable phases whose external action
// is either idempotent (Graph CreateOrGet/outbox reconciliation) or already
// proven. It is intentionally called by the owner loop, so a process restart
// cannot leave a staged child permanently invisible.
func (b *Bridge) reconcileForkOperations(ctx context.Context) error {
	if b == nil || b.store == nil {
		return nil
	}
	ops, err := b.store.ForkOperations(ctx)
	if err != nil {
		return err
	}
	op, ok := b.nextForkOperation(ops)
	if !ok {
		return nil
	}
	if err := b.reconcileForkOperation(ctx, op); err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams fork recovery %s: %v\n", op.ID, err)
		}
		latest, found, loadErr := b.store.ForkOperation(ctx, op.ID)
		if loadErr != nil {
			if b.out != nil {
				_, _ = fmt.Fprintf(b.out, "Teams fork recovery state %s: %v\n", op.ID, loadErr)
			}
			return nil
		}
		if found {
			if latest.Phase == teamstore.ForkPhaseFailed {
				_ = b.queueForkNotice(ctx, latest.ParentSessionID, latest.ID, latest.ParentChatID, "fork-failure", "❌ Fork failed: "+firstNonEmptyString(strings.TrimSpace(latest.LastError), "the operation could not be completed")+". The parent chat remains available.")
			} else if latest.Phase == teamstore.ForkPhaseBlockedAmbiguous && strings.TrimSpace(latest.ChildThreadID) == "" {
				_ = b.queueForkNotice(ctx, latest.ParentSessionID, latest.ID, latest.ParentChatID, "fork-blocked", "⚠️ Fork is blocked because the native child response was ambiguous. I did not retry `thread/fork`; inspect the operation before continuing.")
			}
		}
	}
	return nil
}

// nextForkOperation keeps the owner loop bounded and fair. A single fork may
// still need several durable phases, but one loop pass must not walk every
// unfinished operation and issue an unbounded sequence of Graph calls.
func (b *Bridge) nextForkOperation(ops []teamstore.ForkOperation) (teamstore.ForkOperation, bool) {
	candidates := make([]teamstore.ForkOperation, 0, len(ops))
	for _, op := range ops {
		if !teamstore.ForkPhaseTerminal(op.Phase) {
			candidates = append(candidates, op)
		}
	}
	if len(candidates) == 0 {
		return teamstore.ForkOperation{}, false
	}
	b.forkReconcileMu.Lock()
	defer b.forkReconcileMu.Unlock()
	start := 0
	if after := strings.TrimSpace(b.forkReconcileAfterID); after != "" {
		for i, op := range candidates {
			if op.ID == after {
				start = (i + 1) % len(candidates)
				break
			}
		}
	}
	op := candidates[start]
	b.forkReconcileAfterID = op.ID
	return op, true
}

func (b *Bridge) reconcileForkOperation(ctx context.Context, op teamstore.ForkOperation) error {
	if teamstore.ForkPhaseTerminal(op.Phase) {
		return nil
	}
	if claimed, err := b.claimForkOperation(ctx, op); err != nil {
		return err
	} else {
		op = claimed
	}
	parent, cutoff, err := b.forkParentAndCutoff(ctx, op)
	if err != nil {
		return err
	}
	if op.Phase == teamstore.ForkPhaseParentFenced || (op.Phase == teamstore.ForkPhaseSnapshotMaterialized && op.NativeForkIntentAt.IsZero()) {
		unresolved, err := b.sessionExecutionOwnershipUnresolved(ctx, *parent)
		if err != nil {
			return err
		}
		if unresolved {
			// Fork staging is durable, but history materialization and native fork
			// execution must not consume an ambiguous parent transcript.
			return teamstore.ErrUnresolvedExecution
		}
	}
	if op.Phase == teamstore.ForkPhaseSnapshotMaterialized {
		if op.NativeForkIntentAt.IsZero() {
			// The manifest is already durable. The next phase can safely resume
			// from it; re-materializing the full transcript would add latency and
			// could create a different snapshot after a restart.
		} else {
			op.Phase = teamstore.ForkPhaseBlockedAmbiguous
			if _, err := b.updateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
				current.Phase = teamstore.ForkPhaseBlockedAmbiguous
				current.LastError = "native fork intent was durably recorded before the response was lost; automatic retry is disabled"
				current.LastErrorAt = time.Now()
				return nil
			}); err != nil {
				return err
			}
		}
	}
	if op.Phase == teamstore.ForkPhaseParentFenced && !op.NativeForkIntentAt.IsZero() && strings.TrimSpace(op.ChildThreadID) == "" {
		// A crash can occur after the durable native-fork intent and before the
		// child ID is persisted. Treat that boundary exactly like an ambiguous
		// response; retrying thread/fork could create a second child.
		updated, err := b.updateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
			current.Phase = teamstore.ForkPhaseBlockedAmbiguous
			current.LastError = "native fork intent was durably recorded before the child ID; automatic retry is disabled"
			current.LastErrorAt = time.Now()
			return nil
		})
		if err != nil {
			return err
		}
		op = updated
	}
	if op.Phase == teamstore.ForkPhaseBlockedAmbiguous && strings.TrimSpace(op.ChildThreadID) == "" {
		return b.reconcileAmbiguousNativeFork(ctx, parent, op)
	}
	if op.Phase == teamstore.ForkPhaseBlockedAmbiguous && strings.TrimSpace(op.ChildThreadID) != "" {
		// A Graph/create-chat failure is retryable because the operation ID is
		// the external idempotency key. Restore the last durable Codex milestone
		// before retrying that external action; otherwise a blocked operation
		// would be treated as an unknown phase and remain stuck forever.
		updated, err := b.updateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
			current.Phase = teamstore.ForkPhaseCodexForked
			return nil
		})
		if err != nil {
			return err
		}
		op = updated
	}
	if op.Phase == teamstore.ForkPhaseParentFenced {
		snapshot, err := b.materializeForkHistoryForVersion(ctx, parent, cutoff, op.HistoryPlanVersion)
		if err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, err)
		}
		manifest, err := b.saveForkManifest(ctx, op.ID, snapshot.Items, snapshot.Metadata)
		if err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, err)
		}
		if err := b.validateForkBoundary(ctx, parent, cutoff, manifest); err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, err)
		}
		// Keep transcript planning and the native side effect in separate owner
		// loop passes. The command path is already queue-only, and this durable
		// boundary prevents one reconcile pass from doing both a large local
		// render and an external Codex request.
		return nil
	}
	if op.Phase == teamstore.ForkPhaseSnapshotMaterialized {
		if err := b.validateForkBoundary(ctx, parent, cutoff, op); err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, err)
		}
		forger, ok := b.executor.(ForkExecutor)
		if !ok {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, codexrunner.UnsupportedError("thread/fork"))
		}
		if _, err := b.markForkNativeIntentIfParentExecutionClear(ctx, op.ID); err != nil {
			if errors.Is(err, teamstore.ErrForkParentFenced) || errors.Is(err, teamstore.ErrUnresolvedExecution) {
				return err
			}
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, err)
		}
		if err := b.validateForkOwner(ctx, op.ID); err != nil {
			return err
		}
		child, err := forger.ForkThread(ctx, parent, cutoff.CodexTurnID)
		if err != nil {
			phase := teamstore.ForkPhaseFailed
			if codexrunner.IsKind(err, codexrunner.ErrorAmbiguous) || codexrunner.IsKind(err, codexrunner.ErrorParse) {
				phase = teamstore.ForkPhaseBlockedAmbiguous
			}
			return b.failFork(ctx, op.ID, phase, err)
		}
		if _, err := b.recordForkCodexChild(ctx, op.ID, child.CodexThreadID); err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseBlockedAmbiguous, err)
		}
		// Leave Graph chat creation to the next bounded owner-loop pass.
		return nil
	}
	return b.resumeForkAfterCodexChild(ctx, parent, op)
}

func (b *Bridge) reconcileAmbiguousNativeFork(ctx context.Context, parent *Session, op teamstore.ForkOperation) error {
	if strings.TrimSpace(op.ParentThreadID) == "" || strings.TrimSpace(parent.CodexThreadID) != strings.TrimSpace(op.ParentThreadID) {
		return b.noteForkBlocked(ctx, op.ID, "native fork reconciliation source thread no longer matches the durably fenced parent")
	}
	reconciler, ok := b.executor.(ForkReconciler)
	if !ok {
		return b.noteForkBlocked(ctx, op.ID, "native fork response was ambiguous and the configured executor cannot reconcile child threads")
	}
	if op.NativeForkWindowStart.IsZero() || op.NativeForkWindowEnd.IsZero() {
		return b.noteForkBlocked(ctx, op.ID, "native fork response was ambiguous but its durable creation window is missing")
	}
	result, err := reconciler.ReconcileForkThread(ctx, parent, op.CutoffCodexTurnID, op.NativeForkWindowStart, op.NativeForkWindowEnd)
	if err != nil {
		return b.noteForkBlocked(ctx, op.ID, "native fork reconciliation failed: "+err.Error())
	}
	if result.MatchCount != 1 || strings.TrimSpace(result.Result.CodexThreadID) == "" {
		if result.MatchCount > 1 {
			return b.noteForkBlocked(ctx, op.ID, fmt.Sprintf("native fork reconciliation found %d possible child threads; refusing to choose", result.MatchCount))
		}
		return b.noteForkBlocked(ctx, op.ID, "native fork reconciliation found no unique child thread")
	}
	updated, err := b.recordForkCodexChild(ctx, op.ID, result.Result.CodexThreadID)
	if err != nil {
		return err
	}
	return b.resumeForkAfterCodexChild(ctx, parent, updated)
}

func (b *Bridge) noteForkBlocked(ctx context.Context, operationID string, message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "fork remains blocked pending safe reconciliation"
	}
	if _, err := b.updateForkOperation(ctx, operationID, func(op *teamstore.ForkOperation) error {
		op.Phase = teamstore.ForkPhaseBlockedAmbiguous
		op.LastError = message
		op.LastErrorAt = time.Now()
		return nil
	}); err != nil {
		return err
	}
	return fmt.Errorf("fork operation %q blocked: %s", operationID, message)
}

func (b *Bridge) forkParentAndCutoff(ctx context.Context, op teamstore.ForkOperation) (*Session, teamstore.Turn, error) {
	// Recovery needs the durable session binding as well as its turns. The
	// queue-only snapshot intentionally omits sessions on SQLite, so use the
	// workflow snapshot here rather than relying on the registry projection.
	state, err := b.store.SessionWorkflowEventSnapshot(ctx, op.ParentSessionID)
	if err != nil {
		return nil, teamstore.Turn{}, err
	}
	durable, ok := state.Sessions[op.ParentSessionID]
	if !ok {
		return nil, teamstore.Turn{}, fmt.Errorf("fork parent session %q not found", op.ParentSessionID)
	}
	parent := registrySessionFromDurable(durable)
	parent.Status = string(teamstore.SessionStatusActive)
	cutoff, ok := state.Turns[op.CutoffTurnID]
	if !ok && strings.TrimSpace(op.CutoffCodexTurnID) != "" {
		for _, candidate := range state.Turns {
			if candidate.SessionID == op.ParentSessionID && candidate.CodexTurnID == op.CutoffCodexTurnID {
				cutoff = candidate
				ok = true
				break
			}
		}
	}
	if !ok || strings.TrimSpace(cutoff.CodexTurnID) == "" {
		return nil, teamstore.Turn{}, fmt.Errorf("fork cutoff for operation %q is not durable", op.ID)
	}
	return &parent, cutoff, nil
}

func (b *Bridge) resumeForkAfterCodexChild(ctx context.Context, parent *Session, op teamstore.ForkOperation) error {
	if parent == nil {
		return fmt.Errorf("fork parent session is required")
	}
	var err error
	if op.Phase == teamstore.ForkPhaseCodexForked {
		start := firstNonZeroTime(op.ChatStart, op.CreatedAt, time.Now()).UTC()
		end := firstNonZeroTime(op.ChatEnd, start.Add(24*time.Hour)).UTC()
		if _, err := b.updateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
			current.GraphExternalID = firstNonEmptyString(current.GraphExternalID, op.ID)
			current.ChatStart = start
			current.ChatEnd = end
			return nil
		}); err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseBlockedAmbiguous, err)
		}
		if b.graph == nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseBlockedAmbiguous, fmt.Errorf("Teams Graph client is not configured"))
		}
		if err := b.validateForkOwner(ctx, op.ID); err != nil {
			return err
		}
		chat, _, err := b.graph.CreateOrGetMeetingChatWindow(ctx, forkChildTopic(parent.Topic), op.ID, start, end)
		if err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseBlockedAmbiguous, fmt.Errorf("fork child chat could not be confirmed: %w", err))
		}
		if _, err := b.stageForkChat(ctx, op.ID, chat.ID, chat.WebURL, chat.Topic, op.ID, start, end); err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseBlockedAmbiguous, err)
		}
		op, _, err = b.store.ForkOperation(ctx, op.ID)
		if err != nil {
			return err
		}
		// The next owner-loop pass queues the immutable history. Keep Graph chat
		// creation as the only external action in this pass.
		return nil
	}
	if op.Phase == teamstore.ForkPhaseChildChatStaged {
		if _, err := b.queueForkHistory(ctx, op.ID, forkHistoryCompleteMarker); err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseBlockedAmbiguous, err)
		}
		op, _, err = b.store.ForkOperation(ctx, op.ID)
		if err != nil {
			return err
		}
		return nil
	}
	if op.Phase == teamstore.ForkPhaseHistoryPublishing {
		current, verified, err := b.refreshForkHistory(ctx, op.ID)
		if err != nil {
			return err
		}
		if !verified {
			return nil
		}
		op = current
		return nil
	}
	if op.Phase == teamstore.ForkPhaseHistoryVerified {
		linkBody := fmt.Sprintf("✅ Fork complete. Open the new Codex chat:\n%s", strings.TrimSpace(op.ChildChatURL))
		if _, err := b.activateFork(ctx, op.ID, teamstore.OutboxMessage{
			ID:              "fork-link:" + op.ID,
			SessionID:       parent.ID,
			TeamsChatID:     parent.ChatID,
			Kind:            "fork-link",
			Body:            linkBody,
			ForkOperationID: op.ID,
			ForkRole:        "link",
		}); err != nil {
			return err
		}
		op, _, err = b.store.ForkOperation(ctx, op.ID)
		if err != nil {
			return err
		}
		return nil
	}
	if op.Phase == teamstore.ForkPhaseActivated {
		link, err := b.store.OutboxMessageByID(ctx, op.LinkOutboxID)
		if err != nil {
			return err
		}
		if link.Status != teamstore.OutboxStatusSent || strings.TrimSpace(link.TeamsMessageID) == "" {
			// A fresh sending lease can survive a process crash and is normally
			// excluded from PendingOutboxPageAt until it expires. Fork release
			// cannot wait for that timeout: reconcile the provenance-marked link
			// immediately, while keeping the large history delivery queue-only.
			if err := b.sendQueuedOutboxWithOptions(ctx, link, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true, AllowAmbiguousRetry: true}); err != nil {
				return err
			}
			link, err = b.store.OutboxMessageByID(ctx, op.LinkOutboxID)
			if err != nil {
				return err
			}
		}
		if link.Status != teamstore.OutboxStatusSent || strings.TrimSpace(link.TeamsMessageID) == "" {
			return nil
		}
		_, err = b.markForkLinkSent(ctx, op.ID)
		return err
	}
	return nil
}

func (b *Bridge) claimForkOperation(ctx context.Context, op teamstore.ForkOperation) (teamstore.ForkOperation, error) {
	if b == nil || b.store == nil {
		return op, nil
	}
	machineID := strings.TrimSpace(b.machine.ID)
	generation := b.currentLeaseGeneration()
	if machineID == "" || generation <= 0 {
		// Unit-level callers and one-shot recovery tools may not participate in
		// the listener lease. The production owner loop always supplies both.
		return op, nil
	}
	return b.store.ClaimForkOperation(ctx, op.ID, machineID, generation, time.Now())
}

func (b *Bridge) forkOwnerLease() (teamstore.ForkOwnerLease, bool) {
	if b == nil || b.store == nil {
		return teamstore.ForkOwnerLease{}, false
	}
	owner := teamstore.ForkOwnerLease{
		MachineID:  strings.TrimSpace(b.machine.ID),
		Generation: b.currentLeaseGeneration(),
	}
	return owner, owner.MachineID != "" && owner.Generation > 0
}

func (b *Bridge) validateForkOwner(ctx context.Context, operationID string) error {
	owner, ok := b.forkOwnerLease()
	if !ok {
		return nil
	}
	return b.store.ValidateForkOperationOwner(ctx, operationID, owner)
}

func (b *Bridge) updateForkOperation(ctx context.Context, operationID string, fn func(*teamstore.ForkOperation) error) (teamstore.ForkOperation, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.UpdateForkOperationOwned(ctx, operationID, owner, fn)
	}
	return b.store.UpdateForkOperation(ctx, operationID, fn)
}

func (b *Bridge) markForkNativeIntentIfParentExecutionClear(ctx context.Context, operationID string) (teamstore.ForkOperation, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.MarkForkNativeIntentIfParentExecutionClearOwned(ctx, operationID, owner)
	}
	return b.store.MarkForkNativeIntentIfParentExecutionClear(ctx, operationID)
}

func (b *Bridge) saveForkManifest(ctx context.Context, operationID string, items []teamstore.ForkHistoryItem, metadata teamstore.ForkManifestMetadata) (teamstore.ForkOperation, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.SaveForkManifestWithMetadataOwned(ctx, operationID, items, metadata, owner)
	}
	return b.store.SaveForkManifestWithMetadata(ctx, operationID, items, metadata)
}

func (b *Bridge) recordForkCodexChild(ctx context.Context, operationID string, childThreadID string) (teamstore.ForkOperation, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.RecordForkCodexChildOwned(ctx, operationID, childThreadID, owner)
	}
	return b.store.RecordForkCodexChild(ctx, operationID, childThreadID)
}

func (b *Bridge) stageForkChat(ctx context.Context, operationID string, chatID string, chatURL string, topic string, externalID string, start time.Time, end time.Time) (teamstore.ForkOperation, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.StageForkChatOwned(ctx, operationID, chatID, chatURL, topic, externalID, start, end, owner)
	}
	return b.store.StageForkChat(ctx, operationID, chatID, chatURL, topic, externalID, start, end)
}

func (b *Bridge) queueForkHistory(ctx context.Context, operationID string, markerBody string) ([]teamstore.OutboxMessage, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.QueueForkHistoryOwned(ctx, operationID, markerBody, owner)
	}
	return b.store.QueueForkHistory(ctx, operationID, markerBody)
}

func (b *Bridge) refreshForkHistory(ctx context.Context, operationID string) (teamstore.ForkOperation, bool, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.RefreshForkHistoryOwned(ctx, operationID, owner)
	}
	return b.store.RefreshForkHistory(ctx, operationID)
}

func (b *Bridge) activateFork(ctx context.Context, operationID string, link teamstore.OutboxMessage) (teamstore.ForkOperation, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.ActivateForkOwned(ctx, operationID, link, owner)
	}
	return b.store.ActivateFork(ctx, operationID, link)
}

func (b *Bridge) markForkLinkSent(ctx context.Context, operationID string) (teamstore.ForkOperation, error) {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.MarkForkLinkSentOwned(ctx, operationID, owner)
	}
	return b.store.MarkForkLinkSent(ctx, operationID)
}

func (b *Bridge) markForkHistoryDuplicateSettled(ctx context.Context, operationID string, outboxID string, teamsMessageID string) error {
	if owner, ok := b.forkOwnerLease(); ok {
		return b.store.MarkForkHistoryDuplicateSettledOwned(ctx, operationID, outboxID, teamsMessageID, owner)
	}
	return b.store.MarkForkHistoryDuplicateSettled(ctx, operationID, outboxID, teamsMessageID)
}

func (b *Bridge) failFork(ctx context.Context, operationID string, phase teamstore.ForkOperationPhase, cause error) error {
	message := "fork failed"
	if cause != nil {
		message = cause.Error()
	}
	_, updateErr := b.updateForkOperation(ctx, operationID, func(op *teamstore.ForkOperation) error {
		op.Phase = phase
		op.LastError = message
		op.LastErrorAt = time.Now()
		op.RetryCount++
		return nil
	})
	if updateErr != nil {
		return errors.Join(cause, updateErr)
	}
	if b != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams fork %s: %s\n", operationID, message)
	}
	return fmt.Errorf("%s", message)
}

func (b *Bridge) materializeForkHistory(ctx context.Context, parent *Session, cutoff teamstore.Turn) (forkHistorySnapshot, error) {
	return b.materializeForkHistoryForVersion(ctx, parent, cutoff, 2)
}

func (b *Bridge) materializeForkHistoryForVersion(ctx context.Context, parent *Session, cutoff teamstore.Turn, planVersion int) (forkHistorySnapshot, error) {
	if planVersion <= 0 {
		planVersion = 1
	}
	local, ok, err := b.localCodexSessionForTeamsSessionWithDiscovery(ctx, *parent)
	if err != nil {
		return forkHistorySnapshot{}, err
	}
	if !ok || strings.TrimSpace(local.FilePath) == "" {
		return forkHistorySnapshot{}, fmt.Errorf("source Codex transcript for session %q could not be located; refusing to publish an empty fork chat", parent.ID)
	}
	transcript, err := ReadSessionTranscript(local.FilePath)
	if err != nil {
		return forkHistorySnapshot{}, err
	}
	last := -1
	for i, record := range transcript.Records {
		if strings.TrimSpace(record.TurnID) == strings.TrimSpace(cutoff.CodexTurnID) {
			last = i
		}
	}
	if last < 0 {
		return forkHistorySnapshot{}, fmt.Errorf("fork cutoff turn %q was not found in the transcript; refusing to guess a boundary", cutoff.CodexTurnID)
	}
	cutoffRecord := transcript.Records[last]
	if cutoffRecord.SourceOffset <= 0 {
		return forkHistorySnapshot{}, fmt.Errorf("fork cutoff turn %q has no durable transcript byte offset; refusing to guess a boundary", cutoff.CodexTurnID)
	}
	prefixHash, err := hashForkTranscriptPrefix(local.FilePath, cutoffRecord.SourceOffset)
	if err != nil {
		return forkHistorySnapshot{}, err
	}
	records := transcript.Records[:last+1]
	items := make([]teamstore.ForkHistoryItem, 0, len(records))
	appendHistoryBody := func(sourceRecordID string, sourceLine int, sourceStartOffset int64, sourceOffset int64, sourceTurnID string, kind string, body string) {
		chunks := splitForkHistoryBody(kind, body)
		for _, chunk := range chunks {
			partSourceID := sourceRecordID
			if len(chunks) > 1 {
				partSourceID = fmt.Sprintf("%s:part:%d", firstNonEmptyString(sourceRecordID, "record"), chunk.PartIndex)
			}
			items = append(items, teamstore.ForkHistoryItem{
				Ordinal:           len(items),
				SourceRecordID:    partSourceID,
				SourceLine:        sourceLine,
				SourceStartOffset: sourceStartOffset,
				SourceOffset:      sourceOffset,
				SourceTurnID:      sourceTurnID,
				Kind:              kind,
				RenderedBody:      strings.TrimSpace(chunk.Text),
				PartIndex:         chunk.PartIndex,
				PartCount:         chunk.PartCount,
				RenderedBytes:     chunk.ByteLength,
				BodyHash:          forkBodyHash(chunk.Text),
				DeliveryStatus:    teamstore.ForkHistoryDeliveryQueued,
			})
		}
	}
	if planVersion <= 1 {
		for _, record := range records {
			if err := ctx.Err(); err != nil {
				return forkHistorySnapshot{}, err
			}
			if record.Internal || record.Kind == TranscriptKindTool || record.Kind == TranscriptKindArtifact {
				continue
			}
			body := strings.TrimSpace(formatTranscriptRecordForTeams(record))
			if body == "" {
				continue
			}
			if record.Kind != TranscriptKindUser && record.Kind != TranscriptKindAssistant && record.Kind != TranscriptKindStatus && record.Kind != TranscriptKindCompact {
				continue
			}
			appendHistoryBody(
				firstNonEmptyString(record.ItemID, record.DedupeKey),
				record.SourceLine,
				record.SourceStartOffset,
				record.SourceOffset,
				record.TurnID,
				string(record.Kind),
				body,
			)
		}
	} else {
		dedupe := newTranscriptDedupeState()
		plannedRecords := make([]transcriptImportBatchRecord, 0, len(records))
		for i, record := range records {
			if err := ctx.Err(); err != nil {
				return forkHistorySnapshot{}, err
			}
			line, offset := transcriptCheckpointPositionForRecord(transcript.Records, i)
			planned, _, _, included := planTranscriptImportRecord(record, line, offset, "fork", i+1, dedupe, transcriptPlanOptions{ForkVisibleOnly: true})
			if !included {
				continue
			}
			plannedRecords = append(plannedRecords, planned)
		}
		for _, batch := range planTranscriptHistoryBatches(plannedRecords) {
			firstID := transcriptRecordCheckpointKey(batch.First)
			lastID := transcriptRecordCheckpointKey(batch.Last)
			sourceRecordID := firstNonEmptyString(firstID, lastID, "record")
			if lastID != "" && lastID != firstID {
				sourceRecordID += ".." + lastID
			}
			if batch.PartCount > 1 {
				sourceRecordID = fmt.Sprintf("%s:part:%d", sourceRecordID, batch.PartIndex)
			}
			items = append(items, teamstore.ForkHistoryItem{
				Ordinal:           len(items),
				SourceRecordID:    sourceRecordID,
				SourceEndRecordID: lastID,
				SourceLine:        batch.First.SourceLine,
				SourceStartOffset: batch.First.SourceStartOffset,
				SourceOffset:      batch.Last.SourceOffset,
				SourceTurnID:      firstNonEmptyString(batch.Last.TurnID, batch.First.TurnID),
				Kind:              "batch",
				RenderedBody:      batch.HTML,
				PartIndex:         batch.PartIndex,
				PartCount:         batch.PartCount,
				RenderedBytes:     len(batch.HTML),
				BodyHash:          forkBodyHash(batch.HTML),
				DeliveryStatus:    teamstore.ForkHistoryDeliveryQueued,
			})
		}
	}
	for _, subagent := range local.Subagents {
		if !forkSubagentIsBeforeCutoff(subagent, cutoff.CompletedAt) {
			continue
		}
		marker := strings.TrimSpace(formatSubagentImportMarker(local, subagent))
		if marker == "" {
			continue
		}
		key := subagentImportKey(subagent, len(items)+1)
		appendHistoryBody("subagent:"+key, 0, 0, 0, cutoff.CodexTurnID, "subagent-marker", marker)
	}
	if len(items) == 0 {
		return forkHistorySnapshot{}, fmt.Errorf("source Codex transcript for session %q contains no visible history before the fork cutoff", parent.ID)
	}
	return forkHistorySnapshot{
		Items: items,
		Metadata: teamstore.ForkManifestMetadata{
			SourcePath:              local.FilePath,
			SourceFingerprint:       transcript.FileFingerprint,
			HistoryPlanVersion:      planVersion,
			CutoffSourceRecordID:    firstNonEmptyString(cutoffRecord.ItemID, cutoffRecord.DedupeKey),
			CutoffSourceLine:        cutoffRecord.SourceLine,
			CutoffSourceStartOffset: cutoffRecord.SourceStartOffset,
			CutoffSourceOffset:      cutoffRecord.SourceOffset,
			SourcePrefixHash:        prefixHash,
		},
	}, nil
}

// validateForkBoundary re-reads the authoritative parent/cutoff and the
// immutable transcript prefix immediately before the native fork intent is
// recorded. The parent is fenced by this point, but this check still closes
// races with a stale registry projection or a transcript writer that advanced
// the source unexpectedly. The manifest already contains the rendered item
// hash, so re-rendering the entire transcript here would only add latency and
// memory pressure; the byte prefix hash is the source-of-truth check.
func (b *Bridge) validateForkBoundary(ctx context.Context, parent *Session, cutoff teamstore.Turn, manifest teamstore.ForkOperation) error {
	if parent == nil {
		return fmt.Errorf("fork parent is required for boundary validation")
	}
	if strings.TrimSpace(parent.CodexThreadID) == "" || strings.TrimSpace(parent.CodexThreadID) != strings.TrimSpace(manifest.ParentThreadID) {
		return fmt.Errorf("fork parent Codex thread changed before native fork")
	}
	authoritativeCutoff, ok, err := b.store.ForkCutoff(ctx, manifest.ID)
	if err != nil {
		return err
	}
	if !ok || authoritativeCutoff.ID != cutoff.ID || authoritativeCutoff.CodexTurnID != cutoff.CodexTurnID {
		return fmt.Errorf("fork cutoff changed before native fork")
	}
	local, ok, err := b.localCodexSessionForTeamsSessionWithDiscovery(ctx, *parent)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(local.FilePath) == "" {
		return fmt.Errorf("fork transcript is no longer available before native fork")
	}
	if filepath.Clean(local.FilePath) != filepath.Clean(manifest.SourceTranscriptPath) {
		return fmt.Errorf("fork transcript path changed before native fork")
	}
	if manifest.CutoffSourceOffset <= 0 || strings.TrimSpace(manifest.SourcePrefixHash) == "" {
		return fmt.Errorf("fork cutoff prefix proof is incomplete before native fork")
	}
	prefixHash, err := hashForkTranscriptPrefix(local.FilePath, manifest.CutoffSourceOffset)
	if err != nil {
		return fmt.Errorf("fork transcript changed before native fork: %w", err)
	}
	if prefixHash != manifest.SourcePrefixHash {
		return fmt.Errorf("fork transcript prefix changed before native fork")
	}
	return nil
}

func splitForkHistoryBody(kind string, body string) []TeamsRenderedChunk {
	return PlanTeamsHTMLChunks(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    renderKindForOutbox("fork-history-" + kind),
		Text:    body,
	}, TeamsRenderOptions{
		HardLimitBytes:   safeTeamsHTMLContentBytes,
		TargetLimitBytes: teamsChunkHTMLContentBytes,
	})
}

func hashForkTranscriptPrefix(path string, offset int64) (string, error) {
	if strings.TrimSpace(path) == "" || offset < 0 {
		return "", fmt.Errorf("fork transcript prefix requires a valid path and offset")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	read, err := io.Copy(h, io.LimitReader(f, offset))
	if err != nil {
		return "", err
	}
	if read != offset {
		return "", fmt.Errorf("fork transcript prefix ended at %d bytes, want %d", read, offset)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func forkSubagentIsBeforeCutoff(subagent codexhistory.SubagentSession, cutoff time.Time) bool {
	created := firstNonZeroTime(subagent.CreatedAt, subagent.ModifiedAt)
	return !created.IsZero() && !cutoff.IsZero() && !created.After(cutoff)
}

func forkOperationID(sessionID string, commandID string, body string) string {
	seed := strings.TrimSpace(sessionID) + "\x00" + strings.TrimSpace(commandID) + "\x00" + strings.TrimSpace(body)
	sum := sha256.Sum256([]byte(seed))
	return "fork:" + hex.EncodeToString(sum[:])[:24]
}

func forkBodyHash(body string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(body)))
	return hex.EncodeToString(sum[:])
}

func forkChildTopic(parent string) string {
	parent = strings.TrimSpace(parent)
	if parent == "" {
		parent = "Codex"
	}
	return SanitizeTopic(parent + " (fork)")
}
