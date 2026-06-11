package teams

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type threadResolveAction string

const (
	threadResolveStartNew threadResolveAction = "startNew"
	threadResolveBind     threadResolveAction = "bind"
	threadResolveBlock    threadResolveAction = "block"
)

const (
	codexThreadConflictKind = "codex_thread_conflict"
	codexThreadMissingKind  = "codex_thread_missing"
)

type threadResolveDecision struct {
	Action   threadResolveAction
	ThreadID string
	Kind     string
	Message  string
}

type codexThreadConflictError struct {
	SessionID string
	Existing  string
	Observed  string
	Source    string
}

func (e codexThreadConflictError) Error() string {
	return fmt.Sprintf("Codex thread conflict for session %s: existing=%s observed=%s source=%s", e.SessionID, e.Existing, e.Observed, e.Source)
}

func (b *Bridge) resolveCodexThreadBeforeRun(ctx context.Context, session *Session, turn teamstore.Turn) (bool, error) {
	decision, err := b.resolveCodexThreadDecision(ctx, session, turn)
	if err != nil {
		return true, b.interruptTurnForThreadRecovery(ctx, session, turn, codexThreadMissingKind, "Codex thread recovery could not read durable state: "+err.Error())
	}
	switch decision.Action {
	case threadResolveStartNew:
		return false, nil
	case threadResolveBind:
		if err := b.bindSessionCodexThreadIfSafe(ctx, session, turn.ID, decision.ThreadID, "resolver"); err != nil {
			return true, b.interruptTurnForThreadRecovery(ctx, session, turn, codexThreadConflictKind, err.Error())
		}
		return false, nil
	case threadResolveBlock:
		return true, b.interruptTurnForThreadRecovery(ctx, session, turn, decision.Kind, decision.Message)
	default:
		return true, b.interruptTurnForThreadRecovery(ctx, session, turn, codexThreadMissingKind, "Codex thread recovery returned an unknown decision.")
	}
}

func (b *Bridge) resolveCodexThreadDecision(ctx context.Context, session *Session, turn teamstore.Turn) (threadResolveDecision, error) {
	if b == nil || b.store == nil || session == nil {
		return threadResolveDecision{Action: threadResolveStartNew}, nil
	}
	sessionID := strings.TrimSpace(session.ID)
	state, err := b.store.SessionThreadResolutionSnapshot(ctx, sessionID)
	if err != nil {
		return threadResolveDecision{}, err
	}
	var candidates []string
	if durable, ok := state.Sessions[sessionID]; ok {
		if threadID := strings.TrimSpace(durable.CodexThreadID); threadID != "" {
			candidates = appendUniqueString(candidates, threadID)
		}
	}
	for _, prior := range state.Turns {
		if strings.TrimSpace(prior.SessionID) != sessionID || strings.TrimSpace(prior.ID) == strings.TrimSpace(turn.ID) {
			continue
		}
		if threadID := strings.TrimSpace(prior.CodexThreadID); threadID != "" {
			candidates = appendUniqueString(candidates, threadID)
		}
	}
	journal, err := b.readThreadLinkJournal(ctx, sessionID)
	if err != nil {
		return threadResolveDecision{}, err
	}
	for _, rec := range journal {
		if !threadLinkJournalRecordMatchesSession(b, session, rec) {
			continue
		}
		candidates = appendUniqueString(candidates, strings.TrimSpace(rec.CodexThreadID))
	}
	if len(candidates) > 1 {
		return threadResolveDecision{
			Action:  threadResolveBlock,
			Kind:    codexThreadConflictKind,
			Message: "Codex thread conflict: durable state has multiple candidate threads for this Teams session. I did not start a new thread because that could lose context.",
		}, nil
	}
	if len(candidates) == 1 {
		return threadResolveDecision{Action: threadResolveBind, ThreadID: candidates[0]}, nil
	}
	if weakThreadID := strings.TrimSpace(session.CodexThreadID); weakThreadID != "" {
		return threadResolveDecision{
			Action:  threadResolveBlock,
			Kind:    codexThreadMissingKind,
			Message: "Codex thread is only present in the legacy registry projection, not durable state. I did not resume it automatically because that could attach this Teams session to the wrong context. Use `helper restore-thread " + weakThreadID + "` in this Work chat if this is the correct thread, then retry the interrupted turn.",
		}, nil
	}
	if sessionHasPriorCodexAcceptedEvidence(state, sessionID, turn.ID) {
		return threadResolveDecision{
			Action:  threadResolveBlock,
			Kind:    codexThreadMissingKind,
			Message: "Codex thread is missing for this Teams session, but prior Codex activity exists. I did not start a new thread because that could lose context. Use `helper restore-thread <thread-id>` in this Work chat, then retry the interrupted turn.",
		}, nil
	}
	return threadResolveDecision{Action: threadResolveStartNew}, nil
}

func sessionHasPriorCodexAcceptedEvidence(state teamstore.State, sessionID string, currentTurnID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	currentTurnID = strings.TrimSpace(currentTurnID)
	if session, ok := state.Sessions[sessionID]; ok {
		if strings.TrimSpace(session.LatestCodexTurnID) != "" || strings.TrimSpace(session.CodexThreadID) != "" {
			return true
		}
	}
	for _, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) != sessionID || strings.TrimSpace(turn.ID) == currentTurnID {
			continue
		}
		if strings.TrimSpace(turn.CodexThreadID) != "" || strings.TrimSpace(turn.CodexTurnID) != "" {
			return true
		}
	}
	return false
}

func (b *Bridge) bindSessionCodexThreadIfSafe(ctx context.Context, session *Session, turnID string, threadID string, source string) error {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" || session == nil {
		return nil
	}
	sessionID := strings.TrimSpace(session.ID)
	if b == nil || b.store == nil {
		session.CodexThreadID = threadID
		return nil
	}
	alreadyCurrent, err := b.codexThreadBindingAlreadyCurrent(ctx, sessionID, turnID, threadID, source)
	if err != nil {
		return err
	}
	if alreadyCurrent {
		if b.updateSessionCodexThreadProjection(session, sessionID, threadID) && strings.TrimSpace(b.registryPath) != "" {
			if err := b.Save(); err != nil && b.out != nil {
				_, _ = fmt.Fprintf(b.out, "Teams registry thread projection save skipped for %s: %v\n", sessionID, err)
			}
		}
		return nil
	}
	_, _, err = b.store.BindSessionCodexThread(ctx, sessionID, turnID, threadID)
	if err != nil {
		var conflict teamstore.CodexThreadBindingConflictError
		if errors.As(err, &conflict) {
			observed := strings.TrimSpace(conflict.Observed)
			if observed == "" {
				observed = threadID
			}
			return codexThreadConflictError{SessionID: sessionID, Existing: conflict.Existing, Observed: observed, Source: source}
		}
		return err
	}
	projectionChanged := b.updateSessionCodexThreadProjection(session, sessionID, threadID)
	if projectionChanged && strings.TrimSpace(b.registryPath) != "" {
		if err := b.Save(); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams registry thread projection save skipped for %s: %v\n", sessionID, err)
		}
	}
	_ = b.appendThreadLinkJournal(ctx, threadLinkJournalRecord{
		Source:        source,
		ScopeID:       b.scope.ID,
		MachineID:     b.machine.ID,
		SessionID:     sessionID,
		ChatID:        session.ChatID,
		TeamsTurnID:   turnID,
		CodexThreadID: threadID,
		Cwd:           session.Cwd,
	})
	return nil
}

func (b *Bridge) codexThreadBindingAlreadyCurrent(ctx context.Context, sessionID string, turnID string, threadID string, source string) (bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	turnID = strings.TrimSpace(turnID)
	threadID = strings.TrimSpace(threadID)
	if b == nil || b.store == nil || sessionID == "" || threadID == "" {
		return false, nil
	}
	var state teamstore.State
	if turnID != "" {
		var err error
		state, err = b.store.SessionWorkflowEventSnapshotForTurn(ctx, sessionID, turnID)
		if err != nil {
			return false, err
		}
	} else {
		sessions, err := b.store.SessionsByID(ctx, []string{sessionID})
		if err != nil {
			return false, err
		}
		state = teamstore.State{
			SchemaVersion: teamstore.SchemaVersion,
			Sessions:      sessions,
			Turns:         map[string]teamstore.Turn{},
		}
	}
	durable, ok := state.Sessions[sessionID]
	if !ok || durable.ID == "" {
		return false, nil
	}
	existing := strings.TrimSpace(durable.CodexThreadID)
	if existing != "" && existing != threadID {
		return false, codexThreadConflictError{SessionID: sessionID, Existing: existing, Observed: threadID, Source: source}
	}
	if existing != threadID {
		return false, nil
	}
	if turnID != "" {
		if turn, ok := state.Turns[turnID]; ok && strings.TrimSpace(turn.SessionID) == sessionID {
			turnThread := strings.TrimSpace(turn.CodexThreadID)
			if turnThread != "" && turnThread != threadID {
				return false, codexThreadConflictError{SessionID: sessionID, Existing: turnThread, Observed: threadID, Source: source}
			}
			if turnThread != threadID {
				return false, nil
			}
		}
	}
	return true, nil
}

func (b *Bridge) updateSessionCodexThreadProjection(session *Session, sessionID string, threadID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	threadID = strings.TrimSpace(threadID)
	changed := false
	if b != nil {
		if current := b.reg.SessionByID(sessionID); current != nil {
			if strings.TrimSpace(current.CodexThreadID) != threadID {
				current.CodexThreadID = threadID
				current.UpdatedAt = time.Now()
				b.markRegistryProjectionDirty()
				changed = true
			}
		}
	}
	if session != nil {
		session.CodexThreadID = threadID
	}
	return changed
}

func (b *Bridge) bindObservedCodexThreadOrInterrupt(ctx context.Context, session *Session, turn teamstore.Turn, threadID string, source string) (bool, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return false, nil
	}
	if err := b.bindSessionCodexThreadIfSafe(ctx, session, turn.ID, threadID, source); err != nil {
		return true, b.interruptTurnForThreadRecovery(ctx, session, turn, codexThreadConflictKind, err.Error())
	}
	return false, nil
}

func (b *Bridge) interruptTurnForThreadRecovery(ctx context.Context, session *Session, turn teamstore.Turn, kind string, message string) error {
	if session == nil {
		return fmt.Errorf("%s", message)
	}
	if kind == "" {
		kind = codexThreadMissingKind
	}
	if strings.TrimSpace(message) == "" {
		message = "Codex thread recovery blocked this request."
	}
	if b.store != nil && strings.TrimSpace(turn.ID) != "" {
		if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, kind+": "+message); err != nil {
			return err
		}
	}
	return b.queueAndSendOutboxChunksWithOptions(ctx, session.ID, turn.ID, session.ChatID, kind, message, outboxQueueOptions{
		MentionOwner:     true,
		NotificationKind: "needs_attention",
	})
}

func threadLinkJournalRecordMatchesSession(b *Bridge, session *Session, rec threadLinkJournalRecord) bool {
	if session == nil {
		return false
	}
	if recScope := strings.TrimSpace(rec.ScopeID); recScope != "" && b != nil && strings.TrimSpace(b.scope.ID) != "" && recScope != strings.TrimSpace(b.scope.ID) {
		return false
	}
	if recChat := strings.TrimSpace(rec.ChatID); recChat != "" && strings.TrimSpace(session.ChatID) != "" && recChat != strings.TrimSpace(session.ChatID) {
		return false
	}
	return true
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
