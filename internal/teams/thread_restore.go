package teams

import (
	"context"
	"fmt"
	"strings"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func (b *Bridge) restoreThreadCommand(ctx context.Context, session *Session, arg string) error {
	if session == nil {
		return nil
	}
	fields := strings.Fields(strings.TrimSpace(arg))
	if len(fields) == 0 {
		return b.sendToChat(ctx, session.ChatID, "usage: `helper restore-thread <thread-id>`")
	}
	if len(fields) != 1 || containsRestoreThreadForceFlag(fields) {
		return b.sendToChat(ctx, session.ChatID, "`helper restore-thread` accepts exactly one thread id and does not support `--force`.")
	}
	threadID := strings.TrimSpace(fields[0])
	if threadID == "" {
		return b.sendToChat(ctx, session.ChatID, "usage: `helper restore-thread <thread-id>`")
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	if current, ok := state.Sessions[session.ID]; ok {
		if existing := strings.TrimSpace(current.CodexThreadID); existing != "" && existing != threadID {
			return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("restore-thread refused: this Teams session is already bound to Codex thread `%s`.", existing))
		}
	}
	if other, ok := activeDurableSessionForCodexThread(state, session.ID, threadID); ok {
		return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("restore-thread refused: Codex thread `%s` is already bound to active Teams session `%s`.", threadID, other.ID))
	}
	if err := b.bindSessionCodexThreadIfSafe(ctx, session, "", threadID, "restore-thread"); err != nil {
		return b.sendToChat(ctx, session.ChatID, "restore-thread refused: "+err.Error())
	}
	body := fmt.Sprintf("Restored Codex thread `%s` for this Teams session.", threadID)
	if turnID, ok := latestRetryableTurnID(state, session.ID); ok {
		body += "\n\nRetry the interrupted turn with `helper retry " + turnID + "`."
	}
	return b.sendToChat(ctx, session.ChatID, body)
}

func containsRestoreThreadForceFlag(fields []string) bool {
	for _, field := range fields {
		if strings.EqualFold(strings.TrimSpace(field), "--force") || strings.EqualFold(strings.TrimSpace(field), "-f") {
			return true
		}
	}
	return false
}

func activeDurableSessionForCodexThread(state teamstore.State, currentSessionID string, threadID string) (teamstore.SessionContext, bool) {
	currentSessionID = strings.TrimSpace(currentSessionID)
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return teamstore.SessionContext{}, false
	}
	for _, session := range state.Sessions {
		if strings.TrimSpace(session.ID) == currentSessionID {
			continue
		}
		if strings.TrimSpace(session.CodexThreadID) != threadID {
			continue
		}
		if isActiveSessionStatus(string(session.Status)) {
			return session, true
		}
	}
	return teamstore.SessionContext{}, false
}
