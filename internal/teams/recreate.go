package teams

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type RecreatedChat struct {
	SessionID string
	OldChat   Chat
	NewChat   Chat
}

type RecreateSessionChatOptions struct {
	ImportHistory bool
	Activate      bool
}

func (b *Bridge) RecreateControlChat(ctx context.Context) (RecreatedChat, error) {
	if b == nil {
		return RecreatedChat{}, fmt.Errorf("Teams bridge is not configured")
	}
	if err := b.prepareSessionMaintenanceState(ctx); err != nil {
		return RecreatedChat{}, err
	}
	old := Chat{ID: b.reg.ControlChatID, Topic: b.reg.ControlChatTopic, WebURL: b.reg.ControlChatURL, ChatType: "meeting"}
	topic := b.desiredControlChatTopic()
	chat, err := b.createMeetingChat(ctx, topic)
	if err != nil {
		return RecreatedChat{}, err
	}
	if err := b.sendRecreatedChatMovedNotice(ctx, old, chat, "Control chat", ""); err != nil {
		return RecreatedChat{}, err
	}
	b.reg.ensureMaps()
	b.reg.ControlChatID = chat.ID
	b.reg.ControlChatTopic = chat.Topic
	b.reg.ControlChatURL = chat.WebURL
	if old.ID != "" && old.ID != chat.ID {
		delete(b.reg.Chats, old.ID)
	}
	b.reg.Chats[chat.ID] = ChatState{}
	b.markRegistryProjectionDirty()
	if err := b.recordControlChatBinding(ctx, chat); err != nil {
		return RecreatedChat{}, err
	}
	if err := b.resetRecreatedControlChatState(ctx, old.ID, chat); err != nil {
		return RecreatedChat{}, err
	}
	if err := b.Save(); err != nil {
		return RecreatedChat{}, err
	}
	if err := b.sendChatCreatedMention(ctx, "", chat.ID, "Control chat recreated."); err != nil {
		return RecreatedChat{}, err
	}
	if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          directOutboxID(chat.ID, "control-ready", "control chat is ready"),
		TeamsChatID: chat.ID,
		Kind:        "control",
		Body:        "control chat is ready.\n\n" + controlHelpText(),
	}); err != nil {
		return RecreatedChat{}, err
	}
	return RecreatedChat{OldChat: old, NewChat: chat}, nil
}

func (b *Bridge) prepareSessionMaintenanceState(ctx context.Context) error {
	if !b.maintenanceStorePinned {
		if err := b.migrateRegistryProjectionToStore(ctx); err != nil {
			return err
		}
	}
	return b.restoreRegistryFromStore(ctx)
}

func (b *Bridge) RecreateSessionChat(ctx context.Context, selector string, opts RecreateSessionChatOptions) (RecreatedChat, error) {
	if b == nil {
		return RecreatedChat{}, fmt.Errorf("Teams bridge is not configured")
	}
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return RecreatedChat{}, fmt.Errorf("session id, Codex thread id, or Teams chat id is required")
	}
	if err := b.prepareSessionMaintenanceState(ctx); err != nil {
		return RecreatedChat{}, err
	}
	session := b.sessionForRecreateSelector(selector)
	if session == nil {
		return RecreatedChat{}, fmt.Errorf("Teams session not found for %q", selector)
	}
	switch teamstore.SessionStatus(strings.TrimSpace(session.Status)) {
	case "", teamstore.SessionStatusActive:
	case teamstore.SessionStatusQuarantined:
		if !opts.Activate {
			return RecreatedChat{}, fmt.Errorf("Teams session %s is quarantined; rerun with --activate to recreate and reactivate it", session.ID)
		}
	case teamstore.SessionStatusClosed, teamstore.SessionStatusArchived:
		return RecreatedChat{}, fmt.Errorf("Teams session %s has status %q and cannot be recreated", session.ID, session.Status)
	default:
		return RecreatedChat{}, fmt.Errorf("Teams session %s has unsupported status %q", session.ID, session.Status)
	}
	old := Chat{ID: session.ChatID, Topic: session.Topic, WebURL: session.ChatURL, ChatType: "meeting"}
	topic := strings.TrimSpace(session.Topic)
	if topic == "" {
		topic = WorkChatTitle(ChatTitleOptions{
			MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
			Profile:      b.scope.Profile,
			SessionID:    session.ID,
			Cwd:          session.Cwd,
		})
	}
	chat, err := b.createMeetingChat(ctx, topic)
	if err != nil {
		return RecreatedChat{}, err
	}
	now := time.Now()
	next := *session
	next.ChatID = chat.ID
	next.ChatURL = chat.WebURL
	next.Topic = chat.Topic
	next.Status = "active"
	next.UpdatedAt = now
	if err := b.resetRecreatedSessionChatState(ctx, old.ID, next, opts.Activate); err != nil {
		return RecreatedChat{}, fmt.Errorf("new Teams chat %s was created but durable session rebind failed: %w", chat.WebURL, err)
	}
	postBindError := func(stage string, err error) (RecreatedChat, error) {
		return RecreatedChat{}, fmt.Errorf("new Teams chat %s is durably bound, but %s failed; the new binding was retained: %w", chat.WebURL, stage, err)
	}
	*session = next
	b.reg.ensureMaps()
	if old.ID != "" && old.ID != chat.ID {
		delete(b.reg.Chats, old.ID)
	}
	b.reg.Chats[chat.ID] = ChatState{}
	b.markRegistryProjectionDirty()
	if err := b.Save(); err != nil {
		return postBindError("saving the local registry projection", err)
	}
	noticeErr := b.sendRecreatedChatMovedNotice(ctx, old, chat, "Work chat", session.ID)
	var retireErr error
	if old.ID != "" && old.ID != chat.ID {
		retireErr = b.retireRecreatedSessionChatRouting(ctx, session.ID, old.ID, chat.ID)
		if retireErr == nil {
			delete(b.reg.Chats, old.ID)
			b.markRegistryProjectionDirty()
			retireErr = b.Save()
		}
	}
	if err := errors.Join(noticeErr, retireErr); err != nil {
		return postBindError("notifying or retiring the previous chat", err)
	}
	if opts.ImportHistory {
		local, ok, err := b.localCodexSessionForTeamsSessionWithDiscovery(ctx, *session)
		if err != nil {
			return postBindError("discovering the local Codex transcript", err)
		}
		if !ok {
			return postBindError("discovering the local Codex transcript", fmt.Errorf("local Codex transcript not found for session %s", session.ID))
		}
		if err := b.importFullCodexTranscriptToTeams(ctx, *session, local, ""); err != nil {
			return postBindError("publishing full history (retry with `helper publish-history full`)", err)
		}
	}
	if err := b.sendChatCreatedMention(ctx, session.ID, chat.ID, workChatLifecycleNotice("Work chat recreated", *session)); err != nil {
		return postBindError("sending the recreated-chat lifecycle notice", err)
	}
	if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + session.ID + ":recreate-anchor:" + normalizedTextHash(chat.ID),
		SessionID:   session.ID,
		TeamsChatID: chat.ID,
		Kind:        "anchor",
		Body:        sessionReadyMessage(*session, ""),
	}); err != nil {
		return postBindError("sending the recreated-chat ready anchor", err)
	}
	return RecreatedChat{SessionID: session.ID, OldChat: old, NewChat: chat}, nil
}

func (b *Bridge) retireRecreatedSessionChatRouting(ctx context.Context, sessionID string, oldChatID string, newChatID string) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	return b.store.UpdateSession(ctx, sessionID, func(state *teamstore.State) error {
		current, ok := state.Sessions[sessionID]
		if !ok || strings.TrimSpace(current.TeamsChatID) != strings.TrimSpace(newChatID) {
			return fmt.Errorf("durable Teams session %s no longer points to recreated chat %q", sessionID, newChatID)
		}
		retireRecreatedChatState(state, oldChatID)
		return nil
	})
}

func (b *Bridge) sessionForRecreateSelector(selector string) *Session {
	if session := b.reg.SessionByID(selector); session != nil {
		return session
	}
	if session := b.reg.SessionByCodexThreadID(selector); session != nil {
		return session
	}
	if session := b.reg.SessionByChatID(selector); session != nil {
		return session
	}
	for i := range b.reg.Sessions {
		if strings.EqualFold(recreateShortGraphID(b.reg.Sessions[i].ChatID), selector) {
			return &b.reg.Sessions[i]
		}
	}
	return nil
}

func recreateShortGraphID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

func (b *Bridge) sendRecreatedChatMovedNotice(ctx context.Context, oldChat Chat, newChat Chat, label string, sessionID string) error {
	oldID := strings.TrimSpace(oldChat.ID)
	newID := strings.TrimSpace(newChat.ID)
	if oldID == "" || newID == "" || oldID == newID {
		return nil
	}
	body := recreatedChatMovedNoticeText(label, sessionID, newChat.WebURL)
	if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:               directOutboxID(oldID, "chat-moved", body),
		SessionID:        sessionID,
		TeamsChatID:      oldID,
		Kind:             "chat-moved",
		Body:             body,
		MentionOwner:     true,
		NotificationKind: "chat_recreated",
	}); err != nil {
		return fmt.Errorf("send migration link to old Teams chat: %w", err)
	}
	return nil
}

func recreatedChatMovedNoticeText(label string, sessionID string, newURL string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		label = "chat"
	}
	sessionID = strings.TrimSpace(sessionID)
	target := "the new " + label
	if sessionID != "" {
		target += " for " + sessionID
	}
	lines := []string{
		"🔁 This chat moved",
		"",
		"Open " + target + ":",
	}
	if link := strings.TrimSpace(newURL); link != "" {
		lines = append(lines, link)
	}
	lines = append(lines,
		"",
		"Messages here may not be handled after the switch.",
	)
	return strings.Join(lines, "\n")
}

func (b *Bridge) resetRecreatedControlChatState(ctx context.Context, oldChatID string, chat Chat) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		now := time.Now()
		retireRecreatedChatState(state, oldChatID)
		retireRecreatedChatState(state, chat.ID)
		state.ControlChat.MachineID = b.machine.ID
		state.ControlChat.ScopeID = b.scope.ID
		state.ControlChat.AccountID = b.user.ID
		state.ControlChat.Profile = b.scope.Profile
		state.ControlChat.TeamsChatID = chat.ID
		state.ControlChat.TeamsChatURL = chat.WebURL
		state.ControlChat.TeamsChatTopic = chat.Topic
		state.ControlChat.UserTitle = b.reg.ControlChatUserTitle
		state.ControlChat.TitleSource = b.reg.ControlChatTitleSource
		state.ControlChat.BoundAt = now
		state.ControlChat.UpdatedAt = now
		if current := state.Sessions[controlFallbackSessionID]; current.ID != "" {
			current.TeamsChatID = chat.ID
			current.TeamsChatURL = chat.WebURL
			current.TeamsTopic = chat.Topic
			current.UpdatedAt = now
			state.Sessions[controlFallbackSessionID] = current
		}
		return nil
	})
}

func (b *Bridge) resetRecreatedSessionChatState(ctx context.Context, oldChatID string, session Session, activate bool) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		current, ok := state.Sessions[session.ID]
		if !ok || strings.TrimSpace(current.ID) == "" {
			return fmt.Errorf("durable Teams session %s was not found", session.ID)
		}
		if strings.TrimSpace(current.TeamsChatID) != strings.TrimSpace(oldChatID) {
			return fmt.Errorf("durable Teams session %s moved concurrently: expected chat %q, found %q", session.ID, oldChatID, current.TeamsChatID)
		}
		switch current.Status {
		case "", teamstore.SessionStatusActive:
		case teamstore.SessionStatusQuarantined:
			if !activate {
				return fmt.Errorf("durable Teams session %s is quarantined", session.ID)
			}
		case teamstore.SessionStatusClosed, teamstore.SessionStatusArchived:
			return fmt.Errorf("durable Teams session %s has status %q and cannot be recreated", session.ID, current.Status)
		default:
			return fmt.Errorf("durable Teams session %s has unsupported status %q", session.ID, current.Status)
		}
		retireRecreatedChatState(state, oldChatID)
		retireRecreatedChatState(state, session.ChatID)
		current.Status = teamstore.SessionStatusActive
		if activate {
			current.QuarantinedAt = time.Time{}
			current.QuarantineReason = ""
			current.QuarantineSource = ""
			current.QuarantineMessageIDs = nil
		}
		current.TeamsChatID = session.ChatID
		current.TeamsChatURL = session.ChatURL
		current.TeamsTopic = session.Topic
		current.UserTitle = session.UserTitle
		current.TitleSource = session.TitleSource
		current.CodexThreadID = session.CodexThreadID
		current.Cwd = session.Cwd
		if current.CreatedAt.IsZero() {
			current.CreatedAt = session.CreatedAt
		}
		current.UpdatedAt = now
		state.Sessions[session.ID] = current
		return nil
	})
}

func retireRecreatedChatState(state *teamstore.State, chatID string) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return
	}
	delete(state.ChatPolls, chatID)
	delete(state.ChatSequences, chatID)
	delete(state.ChatRateLimits, chatID)
	for id, view := range state.DashboardViews {
		if view.ChatID == chatID {
			delete(state.DashboardViews, id)
		}
	}
	for id, number := range state.DashboardNumbers {
		if number.ChatID == chatID {
			delete(state.DashboardNumbers, id)
		}
	}
}
