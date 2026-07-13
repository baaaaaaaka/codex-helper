package teams

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// Model selection sources are persisted separately from the resolved snapshot.
// A snapshot tells us what will run; the source tells the user why it was
// selected and whether changing the global default will affect this chat.
const (
	modelSelectionSourceGlobalDefault = "global_default"
	modelSelectionSourceChatOverride  = "chat_override"
	modelSelectionSourceLegacy        = "legacy"
)

func modelSelectionSourceLabel(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case modelSelectionSourceGlobalDefault:
		return "global default"
	case modelSelectionSourceChatOverride:
		return "chat override"
	case modelSelectionSourceLegacy:
		return "legacy session (selection source unavailable)"
	default:
		return "selection source unavailable"
	}
}

func modelSnapshotExactLabel(snapshot modelprofile.Snapshot) string {
	if snapshot.IsZero() {
		return "unresolved model"
	}
	model := strings.TrimSpace(firstNonEmptyString(snapshot.Model, snapshot.DefaultModel))
	if snapshot.IsDefault() {
		if model == "" {
			return "Codex Official (account default; exact slug unavailable)"
		}
		return fmt.Sprintf("Codex Official (`%s`)", model)
	}
	name := strings.TrimSpace(snapshot.Name)
	if name == "" {
		name = strings.TrimSpace(snapshot.Provider)
	}
	provider := strings.TrimSpace(snapshot.Provider)
	if provider == "" || strings.EqualFold(provider, name) {
		provider = ""
	}
	label := name
	if provider != "" {
		label += " (" + provider + ")"
	}
	if model != "" {
		label += " (`" + model + "`)"
	}
	if label == "" {
		label = "configured model"
	}
	return label
}

func modelSnapshotSelectorLabel(snapshot modelprofile.Snapshot) string {
	if snapshot.IsZero() {
		return "unresolved"
	}
	if snapshot.IsDefault() {
		if model := strings.TrimSpace(firstNonEmptyString(snapshot.Model, snapshot.DefaultModel)); model != "" {
			return "official:" + model
		}
		return "default (dynamic Codex account default)"
	}
	if name := strings.TrimSpace(snapshot.Name); name != "" {
		return "profile:" + name
	}
	return "configured profile"
}

func (b *Bridge) currentChatModelSummaryLines(ctx context.Context, session *Session, chatKind string) []string {
	chatKind = firstNonEmptyString(strings.TrimSpace(chatKind), "Chat")
	if session == nil {
		return []string{"Current chat model (" + chatKind + "): session not found."}
	}
	source := strings.TrimSpace(session.ModelSelectionSource)
	if source == "" {
		source = modelSelectionSourceLegacy
	}
	effort, effortSource := b.chatReasoningEffortSummary(session)
	lines := []string{
		"Current chat model (" + chatKind + ")",
		"Session: `" + strings.TrimSpace(session.ID) + "`",
		"Effective: " + modelSnapshotExactLabel(session.ModelProfile),
		"Selector: `" + modelSnapshotSelectorLabel(session.ModelProfile) + "`",
		"Selection: " + modelSelectionSourceLabel(source),
		"Effective effort: `" + effort + "` (" + reasoningEffortSourceLabel(effortSource) + ")",
		"Applies to: new turns in this chat",
	}
	if global, err := b.resolveNewSessionModelProfile(ctx, ""); err == nil && !global.IsZero() {
		lines = append(lines, "Global default for new chats: "+modelSnapshotExactLabel(global)+" (existing chats are unchanged)")
	} else {
		lines = append(lines, "Global default for new chats: unavailable (use `default model status` in Control)")
	}
	if pending := session.PendingModelProfile; !pending.IsZero() {
		pendingSource := strings.TrimSpace(session.PendingModelSelectionSource)
		if pendingSource == "" {
			pendingSource = modelSelectionSourceChatOverride
		}
		lines = append(lines, "Pending next-turn model: "+modelSnapshotExactLabel(pending)+" ("+modelSelectionSourceLabel(pendingSource)+", applies when this chat is idle)")
	}
	lines = append(lines, b.currentTurnModelSummaryLines(ctx, session.ID)...)
	return lines
}

func (b *Bridge) chatReasoningEffortSummary(session *Session) (string, string) {
	if session == nil {
		return DefaultSessionReasoningEffort, reasoningEffortSourceHelperDefault
	}
	executor := Executor(nil)
	if b != nil {
		executor = b.executor
		if isControlFallbackSessionID(session.ID) {
			executor = b.effectiveControlFallbackExecutor()
		}
	}
	effort := effectiveSessionReasoningEffortWithExecutor(session, executor)
	source := strings.TrimSpace(session.ReasoningEffortSource)
	if source == "" {
		if provider, ok := executor.(ReasoningEffortDefaultProvider); ok && strings.TrimSpace(provider.DefaultReasoningEffort()) != "" {
			source = reasoningEffortSourceExecutorDefault
		} else if isControlFallbackSessionID(session.ID) {
			source = reasoningEffortSourceHelperDefault
		} else {
			source = reasoningEffortSourceHelperDefault
		}
	}
	return effort, source
}

func (b *Bridge) currentTurnModelSummaryLines(ctx context.Context, sessionID string) []string {
	if b == nil || b.store == nil || strings.TrimSpace(sessionID) == "" {
		return nil
	}
	state, err := b.store.SessionWorkflowEventSnapshot(ctx, sessionID)
	if err != nil {
		return nil
	}
	turns := make([]teamstore.Turn, 0, len(state.Turns))
	for _, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) != strings.TrimSpace(sessionID) {
			continue
		}
		if turn.Status == teamstore.TurnStatusRunning || turn.Status == teamstore.TurnStatusQueued {
			turns = append(turns, turn)
		}
	}
	sort.SliceStable(turns, func(i, j int) bool {
		left := turns[i].StartedAt
		if left.IsZero() {
			left = turns[i].QueuedAt
		}
		right := turns[j].StartedAt
		if right.IsZero() {
			right = turns[j].QueuedAt
		}
		if left.Equal(right) {
			return turns[i].ID < turns[j].ID
		}
		return left.Before(right)
	})
	lines := make([]string, 0, 2)
	for _, turn := range turns {
		kind := "Queued turn"
		if turn.Status == teamstore.TurnStatusRunning {
			kind = "Active turn"
		}
		effort := strings.TrimSpace(turn.ReasoningEffort)
		if effort == "" {
			effort = "runtime fallback"
		}
		lines = append(lines, fmt.Sprintf("%s: %s; effort `%s` (captured when queued)", kind, modelSnapshotExactLabel(turn.ModelProfile), effort))
		if len(lines) == 2 {
			break
		}
	}
	return lines
}

func reasoningEffortSourceLabel(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case reasoningEffortSourceExplicit:
		return "chat override"
	case reasoningEffortSourceGlobalDefault:
		return "global default"
	case reasoningEffortSourceModelDefault:
		return "model-advertised default"
	case reasoningEffortSourceExecutorDefault:
		return "Codex runtime default"
	case reasoningEffortSourceHelperDefault:
		return "helper fallback"
	default:
		return "runtime fallback"
	}
}
