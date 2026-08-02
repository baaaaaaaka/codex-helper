package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
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
		ForkWindowStart:      now,
		ForkWindowEnd:        now.Add(forkNativeReconcileWindow),
		Now:                  now,
	})
	if err != nil {
		return err
	}
	if !created {
		return b.resumeForkOperation(ctx, parent, op)
	}
	if claimed, err := b.claimForkOperation(ctx, op); err != nil {
		return err
	} else {
		op = claimed
	}
	cutoff, ok, err := b.store.ForkCutoff(ctx, op.ID)
	if err != nil {
		return err
	}
	if !ok {
		return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, fmt.Errorf("fork cutoff for operation %q is not durable", op.ID))
	}
	if err := b.sendToChat(ctx, parent.ChatID, "⏳ Fork requested. I fixed the parent cutoff and am preparing the new chat; I will send its link only after the visible history is confirmed."); err != nil {
		return err
	}

	snapshot, err := b.materializeForkHistory(ctx, parent, cutoff)
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

	forger, ok := b.executor.(ForkExecutor)
	if !ok {
		err := codexrunner.UnsupportedError("thread/fork")
		return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, err)
	}
	if _, err := b.updateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
		if current.NativeForkIntentAt.IsZero() {
			current.NativeForkIntentAt = time.Now()
		}
		return nil
	}); err != nil {
		// The intent is written before the external request. If this durable
		// boundary cannot be recorded, do not issue the native fork call.
		return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, err)
	}
	if err := b.validateForkOwner(ctx, op.ID); err != nil {
		return err
	}
	childResult, err := forger.ForkThread(ctx, parent, cutoff.CodexTurnID)
	if err != nil {
		phase := teamstore.ForkPhaseFailed
		if codexrunner.IsKind(err, codexrunner.ErrorAmbiguous) || codexrunner.IsKind(err, codexrunner.ErrorParse) {
			phase = teamstore.ForkPhaseBlockedAmbiguous
		}
		return b.failFork(ctx, op.ID, phase, err)
	}
	if _, err := b.recordForkCodexChild(ctx, op.ID, childResult.CodexThreadID); err != nil {
		return b.failFork(ctx, op.ID, teamstore.ForkPhaseBlockedAmbiguous, err)
	}
	current, ok, err := b.store.ForkOperation(ctx, op.ID)
	if err != nil || !ok {
		if err != nil {
			return err
		}
		return teamstore.ErrForkNotFound
	}
	return b.resumeForkAfterCodexChild(ctx, parent, current)
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
	if !ok || updated.Phase != teamstore.ForkPhaseLinkSent {
		return fmt.Errorf("fork operation %q remains in %s", op.ID, updated.Phase)
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
	for _, op := range ops {
		if teamstore.ForkPhaseTerminal(op.Phase) {
			continue
		}
		if err := b.reconcileForkOperation(ctx, op); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams fork recovery %s: %v\n", op.ID, err)
		}
	}
	return nil
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
	if op.Phase == teamstore.ForkPhaseSnapshotMaterialized {
		if op.NativeForkIntentAt.IsZero() {
			// The intent is the durable happens-before marker for the external
			// request. Without it, the previous process could not have reached
			// the request call, so recovery may safely resume the fork.
			updated, err := b.updateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
				current.Phase = teamstore.ForkPhaseParentFenced
				return nil
			})
			if err != nil {
				return err
			}
			op = updated
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
		snapshot, err := b.materializeForkHistory(ctx, parent, cutoff)
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
		forger, ok := b.executor.(ForkExecutor)
		if !ok {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseFailed, codexrunner.UnsupportedError("thread/fork"))
		}
		if _, err := b.updateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
			if current.NativeForkIntentAt.IsZero() {
				current.NativeForkIntentAt = time.Now()
			}
			return nil
		}); err != nil {
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
		op, _, err = b.store.ForkOperation(ctx, op.ID)
		if err != nil {
			return err
		}
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
	}
	if op.Phase == teamstore.ForkPhaseChildChatStaged {
		if _, err := b.queueForkHistory(ctx, op.ID, forkHistoryCompleteMarker); err != nil {
			return b.failFork(ctx, op.ID, teamstore.ForkPhaseBlockedAmbiguous, err)
		}
		op, _, err = b.store.ForkOperation(ctx, op.ID)
		if err != nil {
			return err
		}
	}
	if op.Phase == teamstore.ForkPhaseHistoryPublishing {
		if err := b.flushPendingOutboxForChat(ctx, op.ChildChatID); err != nil {
			return err
		}
		current, verified, err := b.refreshForkHistory(ctx, op.ID)
		if err != nil {
			return err
		}
		if !verified {
			return fmt.Errorf("fork history for %q is not durably verified; link remains gated", op.ID)
		}
		op = current
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
	}
	if op.Phase == teamstore.ForkPhaseActivated {
		if err := b.flushPendingOutboxForChat(ctx, parent.ChatID); err != nil {
			return err
		}
		link, err := b.store.OutboxMessageByID(ctx, op.LinkOutboxID)
		if err != nil {
			return err
		}
		if link.Status != teamstore.OutboxStatusSent || strings.TrimSpace(link.TeamsMessageID) == "" {
			// A fresh sending lease can survive a process crash and is normally
			// excluded from PendingOutboxPageAt until its lease expires. Fork
			// release cannot wait for that timeout: the new owner must immediately
			// reconcile the provenance-marked link against Graph before deciding
			// that the URL is still withheld.
			if err := b.sendQueuedOutboxWithOptions(ctx, link, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true, AllowAmbiguousRetry: true}); err != nil {
				return err
			}
			link, err = b.store.OutboxMessageByID(ctx, op.LinkOutboxID)
			if err != nil {
				return err
			}
		}
		if link.Status != teamstore.OutboxStatusSent || strings.TrimSpace(link.TeamsMessageID) == "" {
			return fmt.Errorf("fork link for %q is not durably sent; URL remains withheld", op.ID)
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
			CutoffSourceRecordID:    firstNonEmptyString(cutoffRecord.ItemID, cutoffRecord.DedupeKey),
			CutoffSourceLine:        cutoffRecord.SourceLine,
			CutoffSourceStartOffset: cutoffRecord.SourceStartOffset,
			CutoffSourceOffset:      cutoffRecord.SourceOffset,
			SourcePrefixHash:        prefixHash,
		},
	}, nil
}

// validateForkBoundary re-reads the authoritative parent/cutoff and transcript
// immediately before the native fork intent is recorded. The parent is fenced
// by this point, but this check still closes races with a stale registry
// projection or a transcript writer that advanced the source unexpectedly.
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
	snapshot, err := b.materializeForkHistory(ctx, parent, cutoff)
	if err != nil {
		return fmt.Errorf("fork transcript changed before native fork: %w", err)
	}
	metadata := snapshot.Metadata
	if metadata.SourcePath != manifest.SourceTranscriptPath ||
		metadata.SourceFingerprint != manifest.SourceFingerprint ||
		metadata.CutoffSourceRecordID != manifest.CutoffSourceRecordID ||
		metadata.CutoffSourceLine != manifest.CutoffSourceLine ||
		metadata.CutoffSourceStartOffset != manifest.CutoffSourceStartOffset ||
		metadata.CutoffSourceOffset != manifest.CutoffSourceOffset ||
		metadata.SourcePrefixHash != manifest.SourcePrefixHash ||
		len(snapshot.Items) != manifest.ManifestCount ||
		teamstore.ForkHistoryManifestHash(snapshot.Items) != manifest.ManifestHash {
		return fmt.Errorf("fork transcript fingerprint or cutoff manifest changed before native fork")
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
