package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	workflowNotificationMaxSummaryRunes = 120
	workflowNotificationMaxTitleRunes   = 96
	workflowNotificationSendTimeout     = 8 * time.Second
	workflowNotificationSendLease       = 2 * time.Minute
	workflowNotificationConfigFileName  = "workflow-notifications.json"
	workflowWebhookURLFileName          = "workflow-webhook-url"
)

type WorkflowNotificationEvent struct {
	ID             string
	SessionID      string
	TurnID         string
	OutboxID       string
	Kind           string
	Title          string
	ChatTitle      string
	RequestSummary string
	Hint           string
	ButtonTitle    string
	ButtonURL      string
}

type workflowNotificationConfigFile struct {
	Version  int                                  `json:"version"`
	Workflow teamstore.WorkflowNotificationConfig `json:"workflow"`
}

func (b *Bridge) SendWorkflowNotificationTest(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return err
	}
	cfg, err := b.effectiveWorkflowNotificationConfig(state)
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return fmt.Errorf("Teams workflow notifications are disabled")
	}
	controlURL := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatURL, state.ControlChat.TeamsChatURL))
	if !safeTeamsOpenURL(controlURL) {
		tenantID := ""
		if b.graph != nil {
			tenantID = b.graph.tenantID()
		}
		controlURL = TeamsChatURL(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID), tenantID)
	}
	if controlURL == "" {
		return fmt.Errorf("Teams control chat is not configured")
	}
	event := WorkflowNotificationEvent{
		ID:          "workflow:test:" + shortStableID(time.Now().Format(time.RFC3339Nano)+controlURL),
		Kind:        "test",
		Title:       "✅ Codex helper notification test",
		ChatTitle:   workflowControlChatTitle(b, state),
		Hint:        "Workflow card delivery is configured for this control chat.",
		ButtonTitle: "Open Control",
		ButtonURL:   controlURL,
	}
	if err := b.queueWorkflowNotification(ctx, event); err != nil {
		return err
	}
	return b.flushPendingWorkflowNotifications(ctx)
}

func ValidateWorkflowWebhookURLFile(path string) error {
	_, err := readWorkflowWebhookURLFile(path)
	return err
}

func (b *Bridge) ConfigureWorkflowNotificationsFromWebhookURL(ctx context.Context, webhookURL string) (teamstore.WorkflowNotificationConfig, error) {
	webhookURL = strings.TrimSpace(webhookURL)
	if !safeWorkflowWebhookURL(webhookURL) {
		return teamstore.WorkflowNotificationConfig{}, fmt.Errorf("workflow webhook URL must be an absolute https URL")
	}
	path, err := b.workflowWebhookURLFilePath()
	if err != nil {
		return teamstore.WorkflowNotificationConfig{}, err
	}
	if err := writeWorkflowWebhookURLSecretFile(path, webhookURL); err != nil {
		return teamstore.WorkflowNotificationConfig{}, err
	}
	return b.ConfigureWorkflowNotifications(ctx, path, true)
}

func (b *Bridge) ConfigureWorkflowNotifications(ctx context.Context, webhookURLFile string, enabled bool) (teamstore.WorkflowNotificationConfig, error) {
	if err := b.ensureStore(); err != nil {
		return teamstore.WorkflowNotificationConfig{}, err
	}
	webhookURLFile = strings.TrimSpace(webhookURLFile)
	if enabled {
		if webhookURLFile == "" {
			return teamstore.WorkflowNotificationConfig{}, fmt.Errorf("workflow webhook URL file is required")
		}
		if !filepath.IsAbs(webhookURLFile) {
			return teamstore.WorkflowNotificationConfig{}, fmt.Errorf("workflow webhook URL file must be an absolute path")
		}
		if _, err := readWorkflowWebhookURLFile(webhookURLFile); err != nil {
			return teamstore.WorkflowNotificationConfig{}, err
		}
	}
	var out teamstore.WorkflowNotificationConfig
	err := b.store.Update(ctx, func(state *teamstore.State) error {
		next := teamstore.WorkflowNotificationConfig{UpdatedAt: time.Now()}
		if enabled {
			controlChatID := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
			if controlChatID == "" {
				return fmt.Errorf("Teams control chat must be configured before enabling workflow notifications")
			}
			next = state.Workflow
			next.Enabled = true
			next.ControlWebhookURLFile = webhookURLFile
			next.ControlChatID = controlChatID
			next.UpdatedAt = time.Now()
		}
		state.Workflow = next
		out = next
		return nil
	})
	if err != nil {
		return out, err
	}
	if err := b.saveWorkflowNotificationConfigFile(out); err != nil {
		return out, err
	}
	return out, nil
}

func (b *Bridge) queueWorkflowNotificationForSentOutbox(ctx context.Context, outbox teamstore.OutboxMessage) {
	if b == nil || b.store == nil {
		return
	}
	event, ok, err := b.workflowNotificationEventForOutbox(ctx, outbox)
	if err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow notification planning error: %v\n", err)
		}
		return
	}
	if !ok {
		return
	}
	if b.outboxAlreadyMentionedControlOwner(ctx, outbox) && !b.workflowCardAvailable(ctx) {
		return
	}
	if err := b.queueUserAttentionNotification(ctx, event, ""); err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow notification queue error: %v\n", err)
		}
		return
	}
}

func (b *Bridge) queueWorkflowNotificationForDetectedCodexAnswer(ctx context.Context, session *Session, sourceKey string) {
	if b == nil || b.store == nil || session == nil {
		return
	}
	sourceKey = strings.TrimSpace(sourceKey)
	if sourceKey == "" || strings.TrimSpace(session.ChatID) == "" {
		return
	}
	outboxID := "detected-codex-answer:" + shortStableID(session.ID+":"+sourceKey)
	event, ok, err := b.workflowNotificationEventForOutbox(ctx, teamstore.OutboxMessage{
		ID:               outboxID,
		SessionID:        session.ID,
		TurnID:           sourceKey,
		TeamsChatID:      session.ChatID,
		Kind:             "detected-codex-answer",
		NotificationKind: "turn_completed",
	})
	if err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow notification planning error: %v\n", err)
		}
		return
	}
	if !ok {
		return
	}
	event.ID = "workflow:detected-codex-answer:" + shortStableID(session.ID)
	event.OutboxID = ""
	event.TurnID = ""
	if err := b.queueDetectedCodexAnswerNotification(ctx, event); err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow notification queue error: %v\n", err)
		}
		return
	}
}

func (b *Bridge) queueDetectedCodexAnswerNotification(ctx context.Context, event WorkflowNotificationEvent) error {
	if b == nil || b.store == nil {
		return nil
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return err
	}
	cfg, cfgErr := b.effectiveWorkflowNotificationConfig(state)
	if cfgErr == nil && cfg.Enabled {
		currentControlChatID := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
		if workflowConfigMatchesCurrentControl(cfg, currentControlChatID) {
			if _, err := readWorkflowWebhookURLFile(cfg.ControlWebhookURLFile); err == nil {
				return b.queueWorkflowNotification(ctx, event)
			}
		}
	}
	_, err = b.queueWorkflowNotificationFallbackMentionOnly(ctx, state, event, "")
	return err
}

func (b *Bridge) workflowNotificationEventForOutbox(ctx context.Context, outbox teamstore.OutboxMessage) (WorkflowNotificationEvent, bool, error) {
	if outbox.ID == "" || outbox.TeamsChatID == "" {
		return WorkflowNotificationEvent{}, false, nil
	}
	if !outboxHasWorkflowNotificationCandidate(outbox) {
		return WorkflowNotificationEvent{}, false, nil
	}
	if shouldSkipWorkflowNotificationOutbox(outbox) {
		return WorkflowNotificationEvent{}, false, nil
	}
	state, err := b.store.SessionWorkflowEventSnapshotForTurn(ctx, outbox.SessionID, outbox.TurnID)
	if err != nil {
		return WorkflowNotificationEvent{}, false, err
	}
	session := workflowSessionForOutbox(b, state, outbox)
	chatTitle := workflowNotificationChatTitle(b, session, outbox)
	requestSummary := workflowNotificationRequestSummary(state, outbox, session)
	buttonURL := workflowNotificationButtonURL(b, session, outbox)
	if buttonURL == "" {
		return WorkflowNotificationEvent{}, false, nil
	}
	event := WorkflowNotificationEvent{
		ID:             "workflow:" + shortStableID(outbox.ID),
		SessionID:      outbox.SessionID,
		TurnID:         outbox.TurnID,
		OutboxID:       outbox.ID,
		Kind:           "workflow",
		ChatTitle:      chatTitle,
		RequestSummary: requestSummary,
		ButtonURL:      buttonURL,
	}
	kind := strings.ToLower(strings.TrimSpace(outbox.Kind))
	notificationKind := strings.ToLower(strings.TrimSpace(outbox.NotificationKind))
	switch {
	case notificationKind == "chat_created" || kind == "chat-created":
		event.Kind = "chat_ready"
		event.Title = "💬 Codex chat ready"
		event.Hint = "Open this chat to start or continue the task."
		event.ButtonTitle = "Open chat"
		if event.RequestSummary == "" {
			event.RequestSummary = workflowFallbackRequestForSession(session)
		}
	case notificationKind == "local_session_started" || kind == "local-session-started":
		event.Kind = "local_session_started"
		event.Title = "💬 New local Codex chat detected"
		event.Hint = "Open this chat to watch the answer and continue from the full local context."
		event.ButtonTitle = "Open chat"
		if event.RequestSummary == "" {
			event.RequestSummary = workflowFallbackRequestForSession(session)
		}
	case notificationKind == "chat_recreated" || kind == "chat-moved":
		event.Kind = "chat_moved"
		event.Title = "🔁 Codex chat moved"
		event.Hint = "Open the new chat to continue."
		event.ButtonTitle = "Open new chat"
		if event.RequestSummary == "" {
			event.RequestSummary = workflowFallbackRequestForSession(session)
		}
	case notificationKind == "turn_completed":
		event.Kind = "turn_completed"
		event.Title = "✅ Codex finished"
		event.Hint = "The answer is ready in the work chat."
		event.ButtonTitle = "Open answer"
	case notificationKind == "helper_upgrade_completed":
		event.Kind = "helper_upgrade_completed"
		event.Title = "✅ Helper update completed"
		event.ChatTitle = workflowControlChatTitle(b, state)
		event.RequestSummary = ""
		event.Hint = workflowMachineLabel(b) + " is back online and running the updated helper."
		event.ButtonTitle = "Open Control"
	case notificationKind == helperUpgradeActivationFailedNotificationKind:
		event.Kind = helperUpgradeActivationFailedNotificationKind
		event.Title = "⚠️ Helper update activation failed"
		event.ChatTitle = workflowControlChatTitle(b, state)
		event.RequestSummary = ""
		event.Hint = workflowMachineLabel(b) + " could not activate the downloaded helper. Open Control for the diagnostic."
		event.ButtonTitle = "Open Control"
	case notificationKind == helperUpgradeActivationActionRequiredNotificationKind:
		event.Kind = helperUpgradeActivationActionRequiredNotificationKind
		event.Title = "⚠️ Helper update needs attention"
		event.ChatTitle = workflowControlChatTitle(b, state)
		event.RequestSummary = ""
		event.Hint = workflowMachineLabel(b) + " reports activation completed, but the running helper version still does not match the target."
		event.ButtonTitle = "Open Control"
	case strings.Contains(kind, "reload-complete"):
		event.Kind = "helper_reload_completed"
		event.Title = "✅ Helper reload completed"
		event.ChatTitle = workflowControlChatTitle(b, state)
		event.RequestSummary = ""
		event.Hint = workflowMachineLabel(b) + " is back online and running the reloaded code."
		event.ButtonTitle = "Open Control"
	case strings.Contains(kind, "restart-complete"):
		event.Kind = "helper_restart_completed"
		event.Title = "✅ Helper restart completed"
		event.ChatTitle = workflowControlChatTitle(b, state)
		event.RequestSummary = ""
		event.Hint = workflowMachineLabel(b) + " is back online after restart."
		event.ButtonTitle = "Open Control"
	case strings.HasPrefix(outbox.ID, "outbox:codex-upgrade-target:"):
		event.Kind = "codex_upgrade"
		if strings.Contains(strings.ToLower(outbox.Body), "failed") {
			event.Title = "⚠️ Codex upgrade failed"
			event.Hint = "Open the work chat to check the upgrade error."
		} else {
			event.Title = "⬆️ Codex upgraded"
			event.Hint = "Open the work chat and retry the request when ready."
		}
		event.ButtonTitle = "Open chat"
	case notificationKind == "needs_attention" || workflowOutboxNeedsAttention(kind):
		event.Kind = "needs_attention"
		event.Title = "⚠️ Codex needs attention"
		event.Hint = "Open the work chat to retry or check what happened."
		event.ButtonTitle = "Open chat"
	default:
		return WorkflowNotificationEvent{}, false, nil
	}
	if strings.TrimSpace(event.ChatTitle) == "" {
		event.ChatTitle = workflowNotificationChatTitle(b, session, outbox)
	}
	if strings.TrimSpace(event.ChatTitle) == "" {
		event.ChatTitle = workflowMachineLabel(b)
	}
	return event, true, nil
}

func (b *Bridge) workflowNotificationsEnabled(ctx context.Context) bool {
	if b == nil || b.store == nil {
		return false
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return false
	}
	cfg, err := b.effectiveWorkflowNotificationConfig(state)
	if err != nil {
		return false
	}
	return cfg.Enabled
}

func (b *Bridge) workflowUserAttentionAvailable(ctx context.Context) bool {
	if b == nil || b.store == nil {
		return false
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return false
	}
	cfg, err := b.effectiveWorkflowNotificationConfig(state)
	if err == nil && cfg.Enabled {
		currentControlChatID := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
		if workflowConfigMatchesCurrentControl(cfg, currentControlChatID) {
			if _, err := readWorkflowWebhookURLFile(cfg.ControlWebhookURLFile); err == nil {
				return true
			}
		}
	}
	return workflowFallbackControlChatID(state) != ""
}

func (b *Bridge) workflowCardAvailable(ctx context.Context) bool {
	if b == nil || b.store == nil {
		return false
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return false
	}
	cfg, err := b.effectiveWorkflowNotificationConfig(state)
	if err != nil {
		return false
	}
	return b.workflowCardAvailableFromState(state, cfg)
}

func (b *Bridge) workflowCardAvailableFromState(state teamstore.State, cfg teamstore.WorkflowNotificationConfig) bool {
	if b == nil || !cfg.Enabled {
		return false
	}
	currentControlChatID := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
	if !workflowConfigMatchesCurrentControl(cfg, currentControlChatID) {
		return false
	}
	_, err := readWorkflowWebhookURLFile(cfg.ControlWebhookURLFile)
	return err == nil
}

func workflowConfigMatchesCurrentControl(cfg teamstore.WorkflowNotificationConfig, currentControlChatID string) bool {
	configControlChatID := strings.TrimSpace(cfg.ControlChatID)
	currentControlChatID = strings.TrimSpace(currentControlChatID)
	if configControlChatID == "" {
		return true
	}
	return currentControlChatID != "" && configControlChatID == currentControlChatID
}

func (b *Bridge) queueUserAttentionNotification(ctx context.Context, event WorkflowNotificationEvent, fallbackReason string) error {
	if b == nil || b.store == nil {
		return nil
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return err
	}
	cfg, cfgErr := b.effectiveWorkflowNotificationConfig(state)
	if cfgErr != nil {
		return b.queueWorkflowNotificationFallbackMention(ctx, state, event, "Workflow card is unavailable: "+redactWorkflowNotificationError(cfgErr))
	}
	if !cfg.Enabled {
		return b.queueWorkflowNotificationFallbackMention(ctx, state, event, fallbackReason)
	}
	currentControlChatID := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
	if !workflowConfigMatchesCurrentControl(cfg, currentControlChatID) {
		return b.queueWorkflowNotificationFallbackMention(ctx, state, event, "Workflow card is unavailable because the control chat changed.")
	}
	if _, err := readWorkflowWebhookURLFile(cfg.ControlWebhookURLFile); err != nil {
		return b.queueWorkflowNotificationFallbackMention(ctx, state, event, "Workflow card is unavailable: "+redactWorkflowNotificationError(err))
	}
	if err := b.queueWorkflowNotification(ctx, event); err != nil {
		return err
	}
	if err := b.flushPendingWorkflowNotifications(ctx); err != nil {
		if fallbackErr := b.queueWorkflowNotificationFallbackForDefiniteSendFailure(ctx, event, err); fallbackErr != nil {
			if b.out != nil {
				_, _ = fmt.Fprintf(b.out, "Teams workflow fallback queue error: %v\n", fallbackErr)
			}
		}
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow notification send error: %v\n", err)
		}
	}
	return nil
}

func (b *Bridge) queueWorkflowNotificationFallbackForDefiniteSendFailure(ctx context.Context, event WorkflowNotificationEvent, sendErr error) error {
	if b == nil || b.store == nil || strings.TrimSpace(event.ID) == "" {
		return nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	rec, ok := state.Notifications[event.ID]
	if !ok || rec.Status != teamstore.NotificationStatusFailed || rec.LastErrorRetryable || rec.DeliveryUncertain {
		return nil
	}
	reason := strings.TrimSpace(rec.LastError)
	if reason == "" && sendErr != nil {
		reason = redactWorkflowNotificationError(sendErr)
	}
	if reason != "" {
		reason = "Workflow card send failed: " + reason
	}
	return b.queueWorkflowNotificationFallbackMention(ctx, state, event, reason)
}

func (b *Bridge) queueWorkflowNotificationFallbackMention(ctx context.Context, state teamstore.State, event WorkflowNotificationEvent, reason string) error {
	queued, err := b.queueWorkflowNotificationFallbackMentionOnly(ctx, state, event, reason)
	if err != nil || !queued {
		return err
	}
	if workChatID, ok, err := b.queueWorkflowNotificationWorkChatAttentionNotice(ctx, state, event); err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow work-chat attention notice queue error: %v\n", err)
		}
	} else if ok {
		if err := b.flushPendingOutboxForChat(ctx, workChatID); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow work-chat attention notice send error: %v\n", err)
		}
	}
	if err := b.flushPendingOutboxForChat(ctx, workflowFallbackControlChatID(state)); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams workflow fallback send error: %v\n", err)
	}
	return nil
}

func (b *Bridge) queueWorkflowNotificationFallbackMentionOnly(ctx context.Context, state teamstore.State, event WorkflowNotificationEvent, reason string) (bool, error) {
	if b == nil || b.store == nil {
		return false, nil
	}
	controlChatID := workflowFallbackControlChatID(state)
	if controlChatID == "" {
		return false, nil
	}
	body := workflowNotificationFallbackBody(event, reason)
	if strings.TrimSpace(body) == "" {
		return false, nil
	}
	if _, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:workflow-fallback:" + shortStableID(event.ID),
		SessionID:        controlFallbackSessionID,
		TeamsChatID:      controlChatID,
		Kind:             "workflow-fallback",
		Body:             body,
		MentionOwner:     true,
		NotificationKind: "owner_notification",
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Bridge) queueWorkflowNotificationWorkChatAttentionNotice(ctx context.Context, state teamstore.State, event WorkflowNotificationEvent) (string, bool, error) {
	if b == nil || b.store == nil || !workflowNotificationShouldMirrorAttentionToWorkChat(event) {
		return "", false, nil
	}
	eventID := strings.TrimSpace(event.ID)
	if eventID == "" {
		return "", false, nil
	}
	controlChatID := workflowFallbackControlChatID(state)
	if controlChatID == "" {
		return "", false, nil
	}
	session, ok := state.Sessions[strings.TrimSpace(event.SessionID)]
	if !ok {
		return "", false, nil
	}
	workChatID := strings.TrimSpace(session.TeamsChatID)
	if workChatID == "" || workChatID == controlChatID {
		return "", false, nil
	}
	body := workflowNotificationWorkChatAttentionBody(event)
	if strings.TrimSpace(body) == "" {
		return "", false, nil
	}
	queued, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:workflow-work-attention:" + shortStableID(eventID),
		SessionID:   strings.TrimSpace(event.SessionID),
		TurnID:      strings.TrimSpace(event.TurnID),
		TeamsChatID: workChatID,
		Kind:        "helper",
		Body:        body,
	})
	if err != nil {
		return workChatID, false, err
	}
	if strings.TrimSpace(queued.TeamsChatID) != "" {
		workChatID = strings.TrimSpace(queued.TeamsChatID)
	}
	return workChatID, true, nil
}

func workflowNotificationShouldMirrorAttentionToWorkChat(event WorkflowNotificationEvent) bool {
	if strings.EqualFold(strings.TrimSpace(event.Kind), "needs_attention") {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(event.Title), "⚠️ Codex needs attention")
}

func workflowNotificationWorkChatAttentionBody(event WorkflowNotificationEvent) string {
	title := strings.TrimSpace(event.Title)
	if title == "" {
		title = "⚠️ Codex needs attention"
	}
	return title + "\n\nCodex hit a problem in this work chat. Check the previous status/error message here, or open the control notification for details."
}

func workflowNotificationFallbackBody(event WorkflowNotificationEvent, reason string) string {
	var out strings.Builder
	appendParagraph := func(inner string) {
		inner = strings.TrimSpace(inner)
		if inner == "" {
			return
		}
		out.WriteString("<p>")
		out.WriteString(inner)
		out.WriteString("</p>")
	}
	if title := strings.TrimSpace(event.Title); title != "" {
		appendParagraph("<strong>" + html.EscapeString(title) + "</strong>")
	}
	chatTitle := workflowNotificationFallbackCompactChatTitle(event.ChatTitle)
	summary := strings.TrimSpace(event.RequestSummary)
	if workflowNotificationFallbackTextRedundant(summary, chatTitle) {
		summary = ""
	}
	details := make([]string, 0, 2)
	if chatTitle != "" {
		details = append(details, "<strong>Chat:</strong> "+html.EscapeString(chatTitle))
	}
	if summary != "" {
		details = append(details, "<strong>Request:</strong> "+html.EscapeString(summary))
	}
	if len(details) > 0 {
		appendParagraph(strings.Join(details, "<br>"))
	}
	if hint := strings.TrimSpace(event.Hint); hint != "" {
		appendParagraph("<strong>Next:</strong> " + html.EscapeString(hint))
	}
	if reason = strings.TrimSpace(reason); reason != "" {
		appendParagraph("<strong>Notice:</strong> " + html.EscapeString(reason))
	}
	if url := strings.TrimSpace(event.ButtonURL); url != "" {
		button := strings.TrimSpace(firstNonEmptyString(event.ButtonTitle, "Open chat"))
		if safeTeamsOpenURL(url) {
			appendParagraph(`<strong>Open:</strong> <a href="` + html.EscapeString(url) + `">` + html.EscapeString(button) + `</a>`)
		} else {
			appendParagraph("<strong>" + html.EscapeString(button) + ":</strong> " + html.EscapeString(url))
		}
	}
	return strings.TrimSpace(out.String())
}

func workflowNotificationFallbackCompactChatTitle(title string) string {
	title = strings.Join(strings.Fields(strings.TrimSpace(title)), " ")
	if title == "" {
		return ""
	}
	for {
		parts := strings.Split(title, " - ")
		if len(parts) < 3 {
			return title
		}
		first := strings.TrimSpace(parts[0])
		second := strings.TrimSpace(parts[1])
		if first == "" || first != second {
			return title
		}
		next := append([]string{first}, parts[2:]...)
		title = strings.Join(next, " - ")
	}
}

func workflowNotificationFallbackTextRedundant(summary string, chatTitle string) bool {
	summaryKey := workflowNotificationFallbackTextKey(summary)
	if summaryKey == "" {
		return true
	}
	chatKey := workflowNotificationFallbackTextKey(chatTitle)
	if chatKey == "" {
		return false
	}
	if summaryKey == chatKey {
		return true
	}
	return len([]rune(summaryKey)) >= 8 && strings.Contains(chatKey, summaryKey)
}

func workflowNotificationFallbackTextKey(text string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(text)), " "))
}

func (b *Bridge) outboxAlreadyMentionedControlOwner(ctx context.Context, outbox teamstore.OutboxMessage) bool {
	if b == nil || b.store == nil || !outbox.MentionOwner {
		return false
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return false
	}
	controlChatID := workflowFallbackControlChatID(state)
	return controlChatID != "" && strings.TrimSpace(outbox.TeamsChatID) == controlChatID
}

func workflowFallbackControlChatID(state teamstore.State) string {
	return strings.TrimSpace(state.ControlChat.TeamsChatID)
}

func shouldSkipWorkflowNotificationOutbox(outbox teamstore.OutboxMessage) bool {
	kind := strings.ToLower(strings.TrimSpace(outbox.Kind))
	if kind == "" {
		return true
	}
	notificationKind := strings.ToLower(strings.TrimSpace(outbox.NotificationKind))
	if notificationKind == "turn_completed" ||
		notificationKind == "helper_upgrade_completed" ||
		notificationKind == helperUpgradeActivationFailedNotificationKind ||
		notificationKind == helperUpgradeActivationActionRequiredNotificationKind {
		return false
	}
	if strings.HasPrefix(kind, "import-") || strings.HasPrefix(kind, "sync-") || isTranscriptImportBatchOutboxKind(kind) {
		return true
	}
	if strings.HasPrefix(kind, "final-") && strings.TrimSpace(outbox.NotificationKind) == "" {
		return true
	}
	return false
}

func workflowOutboxNeedsAttention(kind string) bool {
	switch {
	case kind == "error" || strings.HasPrefix(kind, "error-"):
		return true
	case kind == "interrupted" || strings.HasPrefix(kind, "interrupted-"):
		return true
	case kind == "failed" || strings.HasPrefix(kind, "failed-"):
		return true
	case strings.Contains(kind, "queued-turn-error"):
		return true
	case strings.Contains(kind, "recovery-missing-message"):
		return true
	case strings.Contains(kind, "attachment-download"):
		return true
	default:
		return false
	}
}

func (b *Bridge) queueWorkflowNotification(ctx context.Context, event WorkflowNotificationEvent) error {
	if strings.TrimSpace(event.ID) == "" {
		return fmt.Errorf("workflow notification event id is required")
	}
	_, _, err := b.store.UpdateNotification(ctx, event.ID, func(rec teamstore.NotificationRecord, found bool, now time.Time) (teamstore.NotificationRecord, bool, error) {
		if found {
			switch rec.Status {
			case teamstore.NotificationStatusSending, teamstore.NotificationStatusSent, teamstore.NotificationStatusUnknown:
				return rec, false, nil
			}
		}
		if rec.ID == "" {
			rec.ID = event.ID
			rec.CreatedAt = now
			rec.Status = teamstore.NotificationStatusQueued
		}
		if rec.Status == "" {
			rec.Status = teamstore.NotificationStatusQueued
		}
		rec.SessionID = strings.TrimSpace(event.SessionID)
		rec.TurnID = strings.TrimSpace(event.TurnID)
		rec.OutboxID = strings.TrimSpace(event.OutboxID)
		rec.Kind = strings.TrimSpace(event.Kind)
		rec.Title = workflowLimitRunes(event.Title, workflowNotificationMaxTitleRunes)
		rec.ChatTitle = workflowLimitRunes(event.ChatTitle, workflowNotificationMaxTitleRunes)
		rec.RequestSummary = workflowLimitRunes(event.RequestSummary, workflowNotificationMaxSummaryRunes)
		rec.Hint = workflowLimitRunes(event.Hint, workflowNotificationMaxSummaryRunes)
		rec.ButtonTitle = workflowLimitRunes(event.ButtonTitle, 32)
		rec.ButtonURL = strings.TrimSpace(event.ButtonURL)
		rec.UpdatedAt = now
		return rec, true, nil
	})
	return err
}

func (b *Bridge) flushPendingWorkflowNotifications(ctx context.Context) error {
	return b.flushPendingWorkflowNotificationsWithLimit(ctx, 0)
}

func (b *Bridge) flushPendingWorkflowNotificationsWithLimit(ctx context.Context, maxNotifications int) error {
	if b == nil || b.store == nil {
		return nil
	}
	hasPending, err := b.store.HasPendingWorkflowNotifications(ctx)
	if err != nil {
		return err
	}
	if !hasPending {
		return nil
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return err
	}
	cfg, err := b.effectiveWorkflowNotificationConfig(state)
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return nil
	}
	currentControlChatID := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
	if strings.TrimSpace(cfg.ControlChatID) != "" && currentControlChatID != "" && strings.TrimSpace(cfg.ControlChatID) != currentControlChatID {
		return nil
	}
	pending, err := b.store.PendingWorkflowNotifications(ctx)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	webhookURL, err := readWorkflowWebhookURLFile(cfg.ControlWebhookURLFile)
	if err != nil {
		return err
	}
	now := time.Now()
	var firstErr error
	attempts := 0
	for _, rec := range pending {
		if rec.Status == teamstore.NotificationStatusSent || rec.Status == teamstore.NotificationStatusUnknown {
			continue
		}
		if rec.Status == teamstore.NotificationStatusSending {
			if err := b.markStaleWorkflowNotificationUnknown(ctx, rec, now); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		if rec.Status != "" && rec.Status != teamstore.NotificationStatusQueued && rec.Status != teamstore.NotificationStatusFailed {
			continue
		}
		if !workflowNotificationRetryDue(rec, now) {
			continue
		}
		if strings.TrimSpace(rec.Title) == "" || strings.TrimSpace(rec.ButtonURL) == "" {
			continue
		}
		claimed, ok, err := b.claimWorkflowNotificationForSend(ctx, rec.ID, now)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !ok {
			continue
		}
		attempts++
		if err := b.sendWorkflowNotificationRecord(ctx, webhookURL, claimed); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if maxNotifications > 0 && attempts >= maxNotifications {
				break
			}
			continue
		}
		if maxNotifications > 0 && attempts >= maxNotifications {
			break
		}
	}
	return firstErr
}

func workflowNotificationRetryDue(rec teamstore.NotificationRecord, now time.Time) bool {
	if rec.Status != teamstore.NotificationStatusFailed || rec.LastAttemptAt.IsZero() {
		return true
	}
	if !rec.LastErrorRetryable {
		return false
	}
	delay := time.Duration(rec.Attempts) * 30 * time.Second
	if delay < 30*time.Second {
		delay = 30 * time.Second
	}
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	return !now.Before(rec.LastAttemptAt.Add(delay))
}

func (b *Bridge) claimWorkflowNotificationForSend(ctx context.Context, id string, now time.Time) (teamstore.NotificationRecord, bool, error) {
	var out teamstore.NotificationRecord
	claimed := false
	if now.IsZero() {
		now = time.Now()
	}
	updated, _, err := b.store.UpdateNotification(ctx, id, func(rec teamstore.NotificationRecord, found bool, _ time.Time) (teamstore.NotificationRecord, bool, error) {
		if !found {
			return rec, false, nil
		}
		switch rec.Status {
		case teamstore.NotificationStatusSent, teamstore.NotificationStatusUnknown:
			return rec, false, nil
		case teamstore.NotificationStatusSending:
			if rec.LastAttemptAt.IsZero() || now.Sub(rec.LastAttemptAt) <= workflowNotificationSendLease {
				return rec, false, nil
			}
			rec.Status = teamstore.NotificationStatusUnknown
			rec.DeliveryUncertain = true
			rec.LastErrorRetryable = false
			rec.LastError = "previous Teams workflow webhook attempt did not finish; delivery is uncertain"
			rec.UpdatedAt = now
			return rec, true, nil
		case teamstore.NotificationStatusFailed:
			if !workflowNotificationRetryDue(rec, now) {
				return rec, false, nil
			}
		case "", teamstore.NotificationStatusQueued:
		default:
			return rec, false, nil
		}
		if strings.TrimSpace(rec.Title) == "" || strings.TrimSpace(rec.ButtonURL) == "" {
			return rec, false, nil
		}
		rec.Status = teamstore.NotificationStatusSending
		rec.Attempts++
		rec.LastAttemptAt = now
		rec.LastError = ""
		rec.LastErrorRetryable = false
		rec.DeliveryUncertain = false
		rec.UpdatedAt = now
		claimed = true
		return rec, true, nil
	})
	if claimed {
		out = updated
	}
	return out, claimed, err
}

func (b *Bridge) markStaleWorkflowNotificationUnknown(ctx context.Context, rec teamstore.NotificationRecord, now time.Time) error {
	_, _, err := b.store.UpdateNotification(ctx, rec.ID, func(current teamstore.NotificationRecord, found bool, _ time.Time) (teamstore.NotificationRecord, bool, error) {
		if !found || current.Status != teamstore.NotificationStatusSending {
			return current, false, nil
		}
		if current.LastAttemptAt.IsZero() || now.Sub(current.LastAttemptAt) <= workflowNotificationSendLease {
			return current, false, nil
		}
		current.Status = teamstore.NotificationStatusUnknown
		current.DeliveryUncertain = true
		current.LastErrorRetryable = false
		current.LastError = "previous Teams workflow webhook attempt did not finish; delivery is uncertain"
		current.UpdatedAt = now
		return current, true, nil
	})
	return err
}

func (b *Bridge) sendWorkflowNotificationRecord(ctx context.Context, webhookURL string, rec teamstore.NotificationRecord) error {
	sendCtx, cancel := context.WithTimeout(ctx, workflowNotificationSendTimeout)
	defer cancel()
	_, err := PostWorkflowWebhookWithoutRateLimitRetry(sendCtx, b.httpClient, webhookURL, WorkflowWebhookMessage{
		Title:          rec.Title,
		ChatTitle:      rec.ChatTitle,
		RequestSummary: rec.RequestSummary,
		Hint:           rec.Hint,
		Actions: []OpenURLCardAction{{
			Title: firstNonEmptyString(strings.TrimSpace(rec.ButtonTitle), "Open chat"),
			URL:   rec.ButtonURL,
		}},
	})
	if err != nil {
		retryable := workflowWebhookRetryable(err)
		uncertain := workflowWebhookDeliveryUncertain(err)
		_, _, _ = b.store.UpdateNotification(context.Background(), rec.ID, func(current teamstore.NotificationRecord, found bool, now time.Time) (teamstore.NotificationRecord, bool, error) {
			if !found {
				current = rec
			}
			current.Status = teamstore.NotificationStatusFailed
			if uncertain {
				current.Status = teamstore.NotificationStatusUnknown
			}
			current.LastError = redactWorkflowNotificationError(err)
			current.LastErrorRetryable = retryable
			current.DeliveryUncertain = uncertain
			current.UpdatedAt = now
			return current, true, nil
		})
		return err
	}
	_, _, updateErr := b.store.UpdateNotification(ctx, rec.ID, func(current teamstore.NotificationRecord, found bool, now time.Time) (teamstore.NotificationRecord, bool, error) {
		if !found {
			current = rec
		}
		current.Status = teamstore.NotificationStatusSent
		current.LastError = ""
		current.LastErrorRetryable = false
		current.DeliveryUncertain = false
		current.SentAt = now
		current.UpdatedAt = current.SentAt
		return current, true, nil
	})
	return updateErr
}

func readWorkflowWebhookURLFile(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("workflow webhook URL file is not configured")
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("workflow webhook URL file must be an absolute path")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("workflow webhook URL file must not be a symlink")
	}
	if info.IsDir() {
		return "", fmt.Errorf("workflow webhook URL file must be a file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("workflow webhook URL file permissions must be 0600 or stricter")
	}
	if runtime.GOOS != "windows" {
		if err := validateWorkflowWebhookURLFileParents(path); err != nil {
			return "", err
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	webhookURL := strings.TrimSpace(string(raw))
	if !safeWorkflowWebhookURL(webhookURL) {
		return "", fmt.Errorf("workflow webhook URL must be an absolute https URL")
	}
	return webhookURL, nil
}

func writeWorkflowWebhookURLSecretFile(path string, webhookURL string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("workflow webhook URL file is required")
	}
	if !filepath.IsAbs(path) {
		return fmt.Errorf("workflow webhook URL file must be an absolute path")
	}
	webhookURL = strings.TrimSpace(webhookURL)
	if !safeWorkflowWebhookURL(webhookURL) {
		return fmt.Errorf("workflow webhook URL must be an absolute https URL")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		if err := validateWorkflowWebhookURLFileParents(path); err != nil {
			return err
		}
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("workflow webhook URL file must not be a symlink")
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	if err := os.WriteFile(tmp, []byte(webhookURL+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if _, err := readWorkflowWebhookURLFile(path); err != nil {
		return err
	}
	return nil
}

func validateWorkflowWebhookURLFileParents(path string) error {
	dir := filepath.Dir(path)
	for {
		info, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("workflow webhook URL file parent directories must not be symlinks")
		}
		if info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("workflow webhook URL file parent directories must not be group/world-writable")
		}
		if info.Mode().Perm()&0o077 == 0 {
			return nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}

func (b *Bridge) effectiveWorkflowNotificationConfig(state teamstore.State) (teamstore.WorkflowNotificationConfig, error) {
	if state.Workflow.Enabled || strings.TrimSpace(state.Workflow.ControlWebhookURLFile) != "" || strings.TrimSpace(state.Workflow.ControlChatID) != "" {
		return state.Workflow, nil
	}
	cfg, ok, err := b.loadWorkflowNotificationConfigFile()
	if err != nil {
		return teamstore.WorkflowNotificationConfig{}, err
	}
	if ok {
		return cfg, nil
	}
	return teamstore.WorkflowNotificationConfig{}, nil
}

func (b *Bridge) workflowNotificationConfigFilePath() (string, error) {
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	return WorkflowNotificationConfigFilePathForScope(b.scope.ID)
}

func (b *Bridge) workflowWebhookURLFilePath() (string, error) {
	configPath, err := b.workflowNotificationConfigFilePath()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(configPath), workflowWebhookURLFileName), nil
}

func (b *Bridge) saveWorkflowNotificationConfigFile(cfg teamstore.WorkflowNotificationConfig) error {
	path, err := b.workflowNotificationConfigFilePath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(workflowNotificationConfigFile{Version: 1, Workflow: cfg}, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (b *Bridge) loadWorkflowNotificationConfigFile() (teamstore.WorkflowNotificationConfig, bool, error) {
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	return LoadWorkflowNotificationConfigFileForScope(b.scope.ID)
}

func WorkflowNotificationConfigFilePathForScope(scopeID string) (string, error) {
	storePath, err := DefaultStorePathForScope(scopeID)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(storePath), workflowNotificationConfigFileName), nil
}

func LoadWorkflowNotificationConfigFileForScope(scopeID string) (teamstore.WorkflowNotificationConfig, bool, error) {
	path, err := WorkflowNotificationConfigFilePathForScope(scopeID)
	if err != nil {
		return teamstore.WorkflowNotificationConfig{}, false, err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return teamstore.WorkflowNotificationConfig{}, false, nil
	}
	if err != nil {
		return teamstore.WorkflowNotificationConfig{}, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return teamstore.WorkflowNotificationConfig{}, false, fmt.Errorf("workflow notification config file must not be a symlink")
	}
	if info.IsDir() {
		return teamstore.WorkflowNotificationConfig{}, false, fmt.Errorf("workflow notification config file must be a file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return teamstore.WorkflowNotificationConfig{}, false, fmt.Errorf("workflow notification config file permissions must be 0600 or stricter")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return teamstore.WorkflowNotificationConfig{}, false, err
	}
	var file workflowNotificationConfigFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return teamstore.WorkflowNotificationConfig{}, false, err
	}
	return file.Workflow, true, nil
}

func workflowSessionForOutbox(b *Bridge, state teamstore.State, outbox teamstore.OutboxMessage) *Session {
	if b == nil {
		return nil
	}
	if session := b.sessionForIDState(state, outbox.SessionID); session != nil {
		return session
	}
	for _, session := range b.reg.Sessions {
		if strings.TrimSpace(session.ChatID) == strings.TrimSpace(outbox.TeamsChatID) {
			copy := session
			return &copy
		}
	}
	return nil
}

func workflowNotificationChatTitle(b *Bridge, session *Session, outbox teamstore.OutboxMessage) string {
	if session == nil {
		return workflowLimitRunes(strings.TrimSpace(outbox.TeamsChatID), workflowNotificationMaxTitleRunes)
	}
	title := WorkChatTitle(ChatTitleOptions{
		MachineLabel: workflowMachineLabel(b),
		SessionID:    session.ID,
		UserTitle:    session.UserTitle,
		Topic:        session.Topic,
		Cwd:          session.Cwd,
	})
	if title == "" {
		title = session.ID
	}
	return workflowLimitRunes(title, workflowNotificationMaxTitleRunes)
}

func workflowControlChatTitle(b *Bridge, state teamstore.State) string {
	title := strings.TrimSpace(firstNonEmptyString(state.ControlChat.TeamsChatTopic, b.reg.ControlChatTopic))
	if title != "" {
		return workflowLimitRunes(title, workflowNotificationMaxTitleRunes)
	}
	return workflowLimitRunes(ControlChatTitle(ChatTitleOptions{MachineLabel: workflowMachineLabel(b)}), workflowNotificationMaxTitleRunes)
}

func workflowNotificationButtonURL(b *Bridge, session *Session, outbox teamstore.OutboxMessage) string {
	if movedURL := workflowNotificationMovedChatURL(outbox); movedURL != "" {
		return movedURL
	}
	if session != nil && safeTeamsOpenURL(session.ChatURL) {
		return strings.TrimSpace(session.ChatURL)
	}
	if b != nil && strings.TrimSpace(outbox.TeamsChatID) == strings.TrimSpace(b.reg.ControlChatID) && safeTeamsOpenURL(b.reg.ControlChatURL) {
		return strings.TrimSpace(b.reg.ControlChatURL)
	}
	tenantID := ""
	if b != nil && b.graph != nil {
		tenantID = b.graph.tenantID()
	}
	return TeamsChatURL(outbox.TeamsChatID, tenantID)
}

func workflowNotificationMovedChatURL(outbox teamstore.OutboxMessage) string {
	kind := strings.ToLower(strings.TrimSpace(outbox.Kind))
	notificationKind := strings.ToLower(strings.TrimSpace(outbox.NotificationKind))
	if notificationKind != "chat_recreated" && kind != "chat-moved" {
		return ""
	}
	for _, field := range strings.Fields(outbox.Body) {
		candidate := strings.Trim(field, "<>()[]{}\"'.,;")
		if safeTeamsOpenURL(candidate) {
			return candidate
		}
	}
	return ""
}

func workflowNotificationRequestSummary(state teamstore.State, outbox teamstore.OutboxMessage, session *Session) string {
	if turn, ok := state.Turns[outbox.TurnID]; ok {
		if inbound, ok := state.InboundEvents[turn.InboundEventID]; ok {
			if summary := workflowRequestSummary(inbound.Text); summary != "" {
				return summary
			}
		}
	}
	return workflowFallbackRequestForSession(session)
}

func workflowFallbackRequestForSession(session *Session) string {
	if session == nil {
		return ""
	}
	if strings.TrimSpace(session.UserTitle) != "" {
		return workflowRequestSummary(session.UserTitle)
	}
	if strings.TrimSpace(session.Topic) != "" {
		return workflowRequestSummary(session.Topic)
	}
	if strings.TrimSpace(session.Cwd) != "" {
		return "Workspace: " + filepath.Base(strings.TrimSpace(session.Cwd))
	}
	return ""
}

func workflowRequestSummary(text string) string {
	text = strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
	return workflowLimitRunes(text, workflowNotificationMaxSummaryRunes)
}

func workflowLimitRunes(text string, limit int) string {
	text = strings.TrimSpace(text)
	if limit <= 0 {
		return text
	}
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	if limit == 1 {
		return "…"
	}
	return strings.TrimSpace(string(runes[:limit-1])) + "…"
}

func workflowMachineLabel(b *Bridge) string {
	if b != nil {
		if strings.TrimSpace(b.machine.Label) != "" {
			return strings.TrimSpace(b.machine.Label)
		}
	}
	return machineLabel()
}

func redactWorkflowNotificationError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if strings.Contains(strings.ToLower(text), "https://") {
		text = "Teams workflow webhook request failed"
	}
	if len(text) > 240 {
		text = text[:240] + "…"
	}
	return text
}
