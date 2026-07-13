package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func (b *Bridge) handleModelControlCommand(ctx context.Context, msg ChatMessage, arg string) (string, error) {
	if modelNaturalLanguageListRequest(arg) {
		return b.modelManagerList(ctx)
	}
	sub, rest := modelCommandParts(arg)
	if sub == "" {
		sub = "status"
	}
	switch sub {
	case "status", "current":
		session, err := b.ensureControlFallbackSession(ctx)
		if err != nil {
			return "", err
		}
		return b.formatChatModelStatus(ctx, session, "Control")
	case "list", "ls", "profiles":
		return b.modelManagerList(ctx)
	case "providers", "provider":
		return b.modelManagerProviders(ctx)
	case "setup", "guide", "add", "create":
		if modelProfileSetupRequestsTeamsKeyIntake(rest) {
			return b.startModelProfileKeyIntake(ctx, msg, rest)
		}
		if _, ok := modelprofile.LookupModelChoice(rest); ok {
			result, err := b.modelManagerSetupModel(ctx, rest, false)
			if err != nil {
				return "", err
			}
			if result.NeedsAPIKey {
				return b.startModelProfileKeyIntake(ctx, msg, rest)
			}
			return formatTeamsModelSetupResult(result), nil
		}
		return b.modelManagerSetupGuide(ctx, rest)
	case "key", "api-key", "apikey":
		return modelProfileKeyIntakeUsage(), nil
	case "doctor", "check":
		return b.modelManagerDoctor(ctx, rest)
	case "switch", "use", "set":
		if strings.TrimSpace(rest) == "" {
			return "", fmt.Errorf("usage: `model switch <model>`")
		}
		session, err := b.ensureControlFallbackSession(ctx)
		if err != nil {
			return "", err
		}
		return b.switchChatModelProfile(ctx, session, rest, "Control")
	case "reset":
		session, err := b.ensureControlFallbackSession(ctx)
		if err != nil {
			return "", err
		}
		return b.resetChatModelProfile(ctx, session, "Control")
	case "default", "set-default":
		return "Global defaults moved to the Control-only `default` command. Use `default model set <model>` or `default model reset`.", nil
	case "delete", "remove", "rm":
		name, confirm := parseModelDeleteArgs(rest)
		if strings.TrimSpace(name) == "" {
			return "", fmt.Errorf("usage: `model delete <profile> --confirm`")
		}
		return b.modelManagerDelete(ctx, name, confirm)
	case "effort", "reasoning-effort", "thinking-effort":
		return b.handleReasoningEffortControlCommand(ctx, rest)
	default:
		return modelControlUsage(), nil
	}
}

func (b *Bridge) handleModelWorkCommand(ctx context.Context, session *Session, arg string) (string, error) {
	if modelNaturalLanguageListRequest(arg) {
		return b.modelManagerList(ctx)
	}
	sub, rest := modelCommandParts(arg)
	if sub == "" {
		sub = "status"
	}
	switch sub {
	case "status", "current":
		return b.formatChatModelStatus(ctx, session, "Work")
	case "list", "ls", "profiles":
		return b.modelManagerList(ctx)
	case "providers", "provider":
		return b.modelManagerProviders(ctx)
	case "setup", "guide", "add", "create":
		return b.modelManagerSetupGuide(ctx, rest)
	case "doctor", "check":
		return b.modelManagerDoctor(ctx, firstNonEmptyString(rest, sessionModelProfileName(session)))
	case "switch", "use":
		if strings.TrimSpace(rest) == "" {
			return "", fmt.Errorf("usage: `model switch <profile>`")
		}
		return b.switchWorkModelProfile(ctx, session, rest)
	case "reset":
		return b.resetChatModelProfile(ctx, session, "Work")
	case "fork", "new":
		if strings.TrimSpace(rest) == "" {
			return "", fmt.Errorf("usage: `model fork <profile>`")
		}
		return b.forkWorkChatWithModelProfile(ctx, session, rest)
	case "effort", "reasoning-effort", "thinking-effort":
		return b.handleReasoningEffortWorkCommand(ctx, session, rest)
	default:
		return modelWorkUsage(), nil
	}
}

func modelNaturalLanguageListRequest(arg string) bool {
	value := strings.ToLower(strings.TrimSpace(arg))
	if value == "" {
		return false
	}
	// Exact command forms are handled by the normal parser below. This helper
	// only recognizes read-only natural-language questions; mutations continue
	// to require explicit switch/use/fork commands.
	for _, exact := range []string{"list", "ls", "profiles", "status", "current", "providers", "provider", "setup", "doctor"} {
		if value == exact || strings.HasPrefix(value, exact+" ") {
			return false
		}
	}
	listSignals := []string{
		"列出", "列出来", "列表", "有哪些", "有什么模型", "哪些模型", "可用模型", "所有模型", "全部模型",
		"what model", "which model", "models are available", "available model", "list model", "show model", "all model",
	}
	for _, signal := range listSignals {
		if strings.Contains(value, signal) {
			return true
		}
	}
	return false
}

func modelCommandParts(arg string) (string, string) {
	name, rest := splitDashboardCommandBody(strings.TrimSpace(arg))
	return strings.ToLower(strings.TrimSpace(name)), strings.TrimSpace(rest)
}

func parseModelDeleteArgs(arg string) (string, bool) {
	words := strings.Fields(arg)
	confirm := false
	var keep []string
	for _, word := range words {
		switch strings.ToLower(strings.TrimSpace(word)) {
		case "--yes", "--confirm", "confirm":
			confirm = true
		default:
			keep = append(keep, word)
		}
	}
	return strings.Join(keep, " "), confirm
}

func (b *Bridge) modelManagerList(ctx context.Context) (string, error) {
	if b == nil || b.modelProfileManager == nil {
		return modelProfileManagerUnavailable(), nil
	}
	return b.modelProfileManager.ListModelProfiles(ctx)
}

func (b *Bridge) modelManagerProviders(ctx context.Context) (string, error) {
	if b == nil || b.modelProfileManager == nil {
		return modelProfileManagerUnavailable(), nil
	}
	return b.modelProfileManager.ModelProfileProviders(ctx)
}

func (b *Bridge) modelManagerSetupGuide(ctx context.Context, arg string) (string, error) {
	if b == nil || b.modelProfileManager == nil {
		return modelProfileManagerUnavailable(), nil
	}
	return b.modelProfileManager.ModelProfileSetupGuide(ctx, arg)
}

func (b *Bridge) modelManagerSetupModel(ctx context.Context, model string, setDefault bool) (ModelProfileSetupResult, error) {
	if b == nil || b.modelProfileManager == nil {
		return ModelProfileSetupResult{}, fmt.Errorf("%s", modelProfileManagerUnavailable())
	}
	return b.modelProfileManager.SetupModelProfile(ctx, ModelProfileSetupRequest{Model: strings.TrimSpace(model), SetDefault: setDefault})
}

func (b *Bridge) modelManagerDoctor(ctx context.Context, name string) (string, error) {
	if b == nil || b.modelProfileManager == nil {
		return modelProfileManagerUnavailable(), nil
	}
	return b.modelProfileManager.ModelProfileDoctor(ctx, strings.TrimSpace(name))
}

func (b *Bridge) modelManagerDelete(ctx context.Context, name string, confirm bool) (string, error) {
	if b == nil || b.modelProfileManager == nil {
		return modelProfileManagerUnavailable(), nil
	}
	return b.modelProfileManager.DeleteModelProfile(ctx, strings.TrimSpace(name), confirm)
}

func modelProfileManagerUnavailable() string {
	return "Model management is not configured for this Teams service. Use the local CLI to inspect or configure availability: `cxp model list` and `cxp model setup <model> --no-default`."
}

func modelControlUsage() string {
	return strings.Join([]string{
		"Model commands:",
		"- `model list` - list available models and setup status",
		"- `model setup` - list model choices",
		"- `model setup <model>` - configure a model",
		"- `model key confirm <code>` then `model key <code> <api-key>` - finish Teams key intake",
		"- `model doctor <model>` - validate the model backing profile",
		"- `model status` - show this Control chat's model",
		"- `model switch <model>` - switch this Control chat",
		"- `model reset` - switch this Control chat to the global default",
		"- `default model set <model>` - set the global default",
		"- `new <directory> --model <model>` - create a chat pinned to a model",
		"- `effort list|status|set|reset` - inspect or change Control chat effort",
	}, "\n")
}

func modelWorkUsage() string {
	return strings.Join([]string{
		"Work chat model commands:",
		"- `model status` - show this chat's pinned profile",
		"- `model switch <name>` - switch this chat while preserving its Codex thread",
		"- `model reset` - switch this chat to the global default",
		"- `model fork <name>` - create a new Work chat with another profile",
		"- `model list` - list profiles",
		"- `effort list|status|set|reset` - inspect or change this chat's effort",
	}, "\n")
}

func formatTeamsModelSetupResult(result ModelProfileSetupResult) string {
	if result.ProfileName == config.DefaultModelProfileName || result.Provider == modelprofile.DefaultProvider {
		return "Model available: Codex Official\n\nCurrent chats and global defaults are unchanged."
	}
	lines := []string{
		fmt.Sprintf("Saved model %s as `%s`.", result.DisplayName, result.ProfileName),
	}
	if result.ReusedAPIKey {
		lines = append(lines, fmt.Sprintf("Using existing %s API key %s.", result.Provider, modelprofile.MaskRef(result.APIKeyRef)))
	}
	lines = append(lines, "Current chats and global defaults are unchanged.")
	lines = append(lines, "Existing Work chats keep their pinned model.")
	return strings.Join(lines, "\n")
}

func (b *Bridge) formatChatModelStatus(ctx context.Context, session *Session, chatKind string) (string, error) {
	if session == nil {
		return "Model profile: session not found.", nil
	}
	chatKind = firstNonEmptyString(strings.TrimSpace(chatKind), "Chat")
	lines := []string{
		"Chat: " + chatKind,
		"Model profile: " + modelProfileDisplayName(session.ModelProfile),
	}
	if model := strings.TrimSpace(firstNonEmptyString(session.ModelProfile.Model, session.ModelProfile.DefaultModel)); model != "" {
		lines = append(lines, "Model: "+model)
	}
	if switchable, reason := b.workModelProfileSwitchability(ctx, session); switchable {
		lines = append(lines, "Switchable: yes. Switching preserves the existing Codex thread and portable context, if present.")
	} else if strings.TrimSpace(reason) != "" {
		lines = append(lines, "Switchable: no - "+reason)
	}
	if strings.TrimSpace(session.CodexThreadID) != "" {
		lines = append(lines, "Codex thread: "+session.CodexThreadID)
	}
	return strings.Join(lines, "\n"), nil
}

func (b *Bridge) switchWorkModelProfile(ctx context.Context, session *Session, ref string) (string, error) {
	return b.switchChatModelProfile(ctx, session, ref, "Work")
}

func (b *Bridge) switchChatModelProfile(ctx context.Context, session *Session, ref string, chatKind string) (string, error) {
	if session == nil {
		return "", fmt.Errorf("session not found")
	}
	chatKind = firstNonEmptyString(strings.TrimSpace(chatKind), "Chat")
	b.modelProfileMu.Lock()
	defer b.modelProfileMu.Unlock()
	snapshot, err := b.resolveNewSessionModelProfile(ctx, ref)
	if err != nil {
		return "", err
	}
	if modelProfileSnapshotsSameRuntime(session.ModelProfile, snapshot) {
		return "Model profile already selected for this " + chatKind + " chat: " + modelProfileDisplayName(snapshot), nil
	}
	if switchable, reason := b.workModelProfileSwitchability(ctx, session); !switchable {
		if strings.Contains(reason, "queued or running") {
			effort, source, _ := b.reasoningEffortForModelSwitch(ctx, session, snapshot)
			if err := b.setPendingSessionModelProfile(ctx, session, snapshot, effort, source); err != nil {
				return "", err
			}
			return "Model profile switch scheduled for this " + chatKind + " chat: " + modelProfileDisplayName(snapshot) + "\n\nCurrent queued or running work keeps its captured model. The switch will apply automatically when this chat becomes idle.", nil
		}
		return "", fmt.Errorf("cannot switch this %s chat's model profile: %s", chatKind, reason)
	}
	effort, effortSource, effortNotice := b.reasoningEffortForModelSwitch(ctx, session, snapshot)
	if err := b.setSessionModelProfile(ctx, session, snapshot, effort, effortSource); err != nil {
		return "", err
	}
	threadNotice := "The next message will start a Codex thread with the selected model."
	if strings.TrimSpace(session.CodexThreadID) != "" {
		threadNotice = "The next message will continue the current Codex thread with its portable context preserved."
	}
	message := "Model profile switched for this " + chatKind + " chat: " + modelProfileDisplayName(snapshot) + "\n\n" + threadNotice
	if effortNotice != "" {
		message += "\n\n" + effortNotice
	}
	return message, nil
}

func (b *Bridge) resetChatModelProfile(ctx context.Context, session *Session, chatKind string) (string, error) {
	return b.switchChatModelProfile(ctx, session, "", chatKind)
}

func (b *Bridge) setPendingSessionModelProfile(ctx context.Context, session *Session, snapshot modelprofile.Snapshot, effort string, source string) error {
	now := time.Now()
	if b.store != nil {
		if _, _, err := b.store.UpdateSessionContext(ctx, session.ID, func(current teamstore.SessionContext, found bool, _ time.Time) (teamstore.SessionContext, bool, error) {
			if !found || current.ID == "" {
				return current, false, fmt.Errorf("session %q not found", session.ID)
			}
			current.PendingModelProfile = snapshot
			current.PendingModelRequestedAt = now
			current.PendingReasoningEffort = effort
			current.PendingReasoningSource = source
			current.UpdatedAt = now
			return current, true, nil
		}); err != nil {
			return err
		}
	}
	session.PendingModelProfile = snapshot
	session.PendingModelRequestedAt = now
	session.PendingReasoningEffort = effort
	session.PendingReasoningSource = source
	if current := b.reg.SessionByID(session.ID); current != nil {
		current.PendingModelProfile = snapshot
		current.PendingModelRequestedAt = now
		current.PendingReasoningEffort = effort
		current.PendingReasoningSource = source
		current.UpdatedAt = now
		b.markRegistryProjectionDirty()
	}
	if strings.TrimSpace(b.registryPath) != "" {
		if err := b.Save(); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams registry pending model projection save skipped for %s: %v\n", session.ID, err)
		}
	}
	return nil
}

func (b *Bridge) applyPendingSessionModelProfileIfIdle(ctx context.Context, session *Session) error {
	if session == nil || session.PendingModelProfile.IsZero() {
		return nil
	}
	b.modelProfileMu.Lock()
	defer b.modelProfileMu.Unlock()
	if switchable, _ := b.workModelProfileSwitchability(ctx, session); !switchable {
		return nil
	}
	return b.activatePendingSessionModelProfileLocked(ctx, session)
}

func (b *Bridge) activatePendingSessionModelProfileForQueuedTurn(ctx context.Context, session *Session, turn teamstore.Turn) error {
	if session == nil || session.PendingModelProfile.IsZero() || turn.ModelGeneration != session.ModelGeneration+1 || !modelProfileSnapshotsSameRuntime(turn.ModelProfile, session.PendingModelProfile) {
		return nil
	}
	b.modelProfileMu.Lock()
	defer b.modelProfileMu.Unlock()
	return b.activatePendingSessionModelProfileLocked(ctx, session)
}

func (b *Bridge) activatePendingSessionModelProfileLocked(ctx context.Context, session *Session) error {
	snapshot := session.PendingModelProfile
	effort := session.PendingReasoningEffort
	source := session.PendingReasoningSource
	if strings.TrimSpace(effort) == "" {
		effort, source, _ = b.reasoningEffortForModelSwitch(ctx, session, snapshot)
	}
	if err := b.setSessionModelProfile(ctx, session, snapshot, effort, source); err != nil {
		return err
	}
	return nil
}

func (b *Bridge) workModelProfileSwitchability(ctx context.Context, session *Session) (bool, string) {
	if b == nil || b.store == nil || session == nil {
		return false, "durable state is not available"
	}
	state, err := b.store.SessionWorkflowEventSnapshot(ctx, session.ID)
	if err != nil {
		return false, "could not inspect queued turns: " + err.Error()
	}
	for _, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) != strings.TrimSpace(session.ID) {
			continue
		}
		switch turn.Status {
		case teamstore.TurnStatusQueued, teamstore.TurnStatusRunning:
			return false, "a turn is queued or running"
		default:
			continue
		}
	}
	return true, ""
}

func (b *Bridge) setSessionModelProfile(ctx context.Context, session *Session, snapshot modelprofile.Snapshot, effort string, effortSource string) error {
	now := time.Now()
	nextGeneration := session.ModelGeneration + 1
	preservedThreadID := strings.TrimSpace(session.CodexThreadID)
	preservedLatestTurnID := ""
	if b.store != nil {
		updated, _, err := b.store.UpdateSessionContext(ctx, session.ID, func(current teamstore.SessionContext, _ bool, _ time.Time) (teamstore.SessionContext, bool, error) {
			if current.ID == "" {
				return current, false, fmt.Errorf("session %q not found", session.ID)
			}
			current.ModelProfile = snapshot
			current.ModelGeneration++
			current.ReasoningEffort = effort
			current.ReasoningEffortSource = effortSource
			current.PendingModelProfile = modelprofile.Snapshot{}
			current.PendingModelRequestedAt = time.Time{}
			current.PendingReasoningEffort = ""
			current.PendingReasoningSource = ""
			current.UpdatedAt = now
			return current, true, nil
		})
		if err != nil {
			return err
		}
		nextGeneration = updated.ModelGeneration
		preservedThreadID = strings.TrimSpace(updated.CodexThreadID)
		preservedLatestTurnID = strings.TrimSpace(updated.LatestCodexTurnID)
	}
	session.ModelProfile = snapshot
	session.ModelGeneration = nextGeneration
	session.CodexThreadID = preservedThreadID
	session.ReasoningEffort = effort
	session.ReasoningEffortSource = effortSource
	session.PendingModelProfile = modelprofile.Snapshot{}
	session.PendingModelRequestedAt = time.Time{}
	session.PendingReasoningEffort = ""
	session.PendingReasoningSource = ""
	session.UpdatedAt = now
	if current := b.reg.SessionByID(session.ID); current != nil {
		current.ModelProfile = snapshot
		current.ModelGeneration = session.ModelGeneration
		current.CodexThreadID = preservedThreadID
		current.ReasoningEffort = effort
		current.ReasoningEffortSource = effortSource
		current.PendingModelProfile = modelprofile.Snapshot{}
		current.PendingModelRequestedAt = time.Time{}
		current.PendingReasoningEffort = ""
		current.PendingReasoningSource = ""
		current.UpdatedAt = now
		b.markRegistryProjectionDirty()
	}
	if preservedThreadID != "" {
		if err := b.appendThreadLinkJournal(ctx, threadLinkJournalRecord{
			Source:          "model_switch",
			ScopeID:         b.scope.ID,
			MachineID:       b.machine.ID,
			SessionID:       session.ID,
			ChatID:          session.ChatID,
			CodexThreadID:   preservedThreadID,
			CodexTurnID:     preservedLatestTurnID,
			ModelGeneration: session.ModelGeneration,
			ModelProfile:    snapshot,
			Cwd:             session.Cwd,
		}); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams model-switch thread journal update skipped for %s: %v\n", session.ID, err)
		}
	}
	if strings.TrimSpace(b.registryPath) != "" {
		if err := b.Save(); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams registry model projection save skipped for %s: %v\n", session.ID, err)
		}
	}
	return nil
}

func modelProfileSnapshotsSameRuntime(left, right modelprofile.Snapshot) bool {
	left.CapturedAt = time.Time{}
	right.CapturedAt = time.Time{}
	return modelProfileSnapshotsEqual(left, right)
}

func (b *Bridge) reasoningEffortForModelSwitch(ctx context.Context, session *Session, snapshot modelprofile.Snapshot) (string, string, string) {
	current := strings.TrimSpace(session.ReasoningEffort)
	source := strings.TrimSpace(session.ReasoningEffortSource)
	wasExplicit := source == reasoningEffortSourceExplicit && current != ""
	target := *session
	target.ModelProfile = snapshot
	target.CodexThreadID = ""
	executor := b.executor
	if isControlFallbackSessionID(session.ID) {
		executor = b.effectiveControlFallbackExecutor()
	}
	catalog, err := reasoningEffortCatalog(ctx, executor, &target)
	options := make([]string, 0, len(catalog.Options))
	for _, option := range catalog.Options {
		if value := strings.TrimSpace(option.Effort); value != "" {
			options = append(options, value)
		}
	}
	defaultEffort := strings.TrimSpace(catalog.DefaultEffort)
	if err != nil {
		_ = json.Unmarshal([]byte(snapshot.SupportedReasoningEffortsJSON), &options)
		defaultEffort = strings.TrimSpace(snapshot.DefaultReasoningEffort)
	}
	supported := func(value string) bool {
		for _, option := range options {
			if strings.EqualFold(strings.TrimSpace(option), strings.TrimSpace(value)) {
				return true
			}
		}
		return false
	}
	if wasExplicit && (len(options) == 0 || supported(current)) {
		return current, source, ""
	}
	if defaultEffort == "" {
		if current != "" && (len(options) == 0 || supported(current)) {
			return current, source, ""
		}
		return "", reasoningEffortSourceModelDefault, "Reasoning effort will use the target model default."
	}
	notice := ""
	if wasExplicit && !strings.EqualFold(current, defaultEffort) {
		notice = "Your explicit reasoning effort `" + current + "` is not supported by the target model; it was reset to `" + defaultEffort + "`."
	}
	return defaultEffort, reasoningEffortSourceModelDefault, notice
}

func (b *Bridge) forkWorkChatWithModelProfile(ctx context.Context, source *Session, ref string) (string, error) {
	if source == nil {
		return "", fmt.Errorf("session not found")
	}
	if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
		return "", err
	} else if blocked {
		return serviceControlBlockedMessage(control, "forking Work chats"), nil
	}
	snapshot, err := b.resolveNewSessionModelProfile(ctx, ref)
	if err != nil {
		return "", err
	}
	effort, effortSource, err := b.reasoningEffortFromGlobalDefault(ctx, snapshot)
	if err != nil {
		return "", err
	}
	now := time.Now()
	sessionID := b.reg.NextSessionID()
	topic := WorkChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		SessionID:    sessionID,
		Topic:        firstNonEmptyString(source.UserTitle, source.Topic),
		Cwd:          source.Cwd,
	})
	chat, err := b.createMeetingChat(ctx, topic)
	if err != nil {
		return "", err
	}
	session := Session{
		ID:                    sessionID,
		ChatID:                chat.ID,
		ChatURL:               chat.WebURL,
		Topic:                 chat.Topic,
		TitleSource:           sessionTitleSourceAuto,
		Status:                "active",
		Cwd:                   source.Cwd,
		ModelProfile:          snapshot,
		ReasoningEffort:       effort,
		ReasoningEffortSource: effortSource,
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	b.reg.Sessions = append(b.reg.Sessions, session)
	b.markRegistryProjectionDirty()
	if err := b.ensureDurableSession(ctx, &session); err != nil {
		return "", err
	}
	if strings.TrimSpace(b.registryPath) != "" {
		if err := b.Save(); err != nil {
			return "", err
		}
	}
	body := sessionReadyMessage(session, "", "model:"+modelProfileDisplayName(snapshot), "")
	if err := b.sendChatCreatedMention(ctx, session.ID, chat.ID, workChatCreatedNotice(session)); err != nil {
		return "", err
	}
	if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + session.ID + ":anchor",
		SessionID:   session.ID,
		TeamsChatID: chat.ID,
		Kind:        "anchor",
		Body:        body,
	}); err != nil {
		return "", err
	}
	return fmt.Sprintf("Forked Work chat %s with model profile %s:\n%s", session.ID, modelProfileDisplayName(snapshot), session.ChatURL), nil
}

func sessionModelProfileName(session *Session) string {
	if session == nil {
		return ""
	}
	return strings.TrimSpace(session.ModelProfile.Name)
}
