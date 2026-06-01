package teams

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const modelProfileKeyIntakeTTL = 10 * time.Minute

var (
	modelProfileKeyIntakeNow     = time.Now
	newModelProfileKeyIntakeCode = randomModelProfileKeyIntakeCode
)

type modelProfileKeyIntakeSetupOptions struct {
	Provider       string
	ProfileName    string
	Model          string
	SSHProxy       string
	SetDefault     bool
	TeamsKeyIntake bool
}

func modelProfileSetupRequestsTeamsKeyIntake(arg string) bool {
	opts, _ := parseModelProfileKeyIntakeSetupOptions(arg)
	return opts.TeamsKeyIntake
}

func parseModelProfileKeyIntakeSetupOptions(arg string) (modelProfileKeyIntakeSetupOptions, error) {
	var opts modelProfileKeyIntakeSetupOptions
	fields := strings.Fields(strings.TrimSpace(arg))
	var positional []string
	providerWasFlag := false
	for i := 0; i < len(fields); i++ {
		word := strings.TrimSpace(fields[i])
		lower := strings.ToLower(word)
		switch {
		case lower == "--teams-key-intake" || lower == "--teams-key" || lower == "--key-intake":
			opts.TeamsKeyIntake = true
		case lower == "--set-default" || lower == "--default":
			opts.SetDefault = true
		case lower == "--provider":
			if i+1 >= len(fields) {
				return opts, fmt.Errorf("--provider requires a value")
			}
			i++
			opts.Provider = strings.TrimSpace(fields[i])
			providerWasFlag = true
		case strings.HasPrefix(lower, "--provider="):
			opts.Provider = strings.TrimSpace(word[len("--provider="):])
			providerWasFlag = true
		case lower == "--model":
			if i+1 >= len(fields) {
				return opts, fmt.Errorf("--model requires a value")
			}
			i++
			opts.Model = strings.TrimSpace(fields[i])
		case strings.HasPrefix(lower, "--model="):
			opts.Model = strings.TrimSpace(word[len("--model="):])
		case lower == "--ssh-proxy":
			if i+1 >= len(fields) {
				return opts, fmt.Errorf("--ssh-proxy requires a value")
			}
			i++
			opts.SSHProxy = strings.TrimSpace(fields[i])
		case strings.HasPrefix(lower, "--ssh-proxy="):
			opts.SSHProxy = strings.TrimSpace(word[len("--ssh-proxy="):])
		case lower == "--no-ssh-proxy" || lower == "--ssh-proxy=none":
			opts.SSHProxy = ""
		case strings.HasPrefix(lower, "-"):
			return opts, fmt.Errorf("unknown model setup option %q", word)
		default:
			positional = append(positional, word)
		}
	}
	if providerWasFlag {
		if len(positional) > 0 {
			opts.ProfileName = strings.TrimSpace(positional[0])
		}
		if len(positional) > 1 {
			return opts, fmt.Errorf("too many model profile setup arguments")
		}
	} else {
		if len(positional) > 0 {
			opts.Provider = strings.TrimSpace(positional[0])
		}
		if len(positional) > 1 {
			opts.ProfileName = strings.TrimSpace(positional[1])
		}
		if len(positional) > 2 {
			return opts, fmt.Errorf("too many model profile setup arguments")
		}
	}
	opts.Provider = modelprofile.NormalizeProvider(opts.Provider)
	opts.ProfileName = strings.TrimSpace(opts.ProfileName)
	opts.Model = strings.TrimSpace(opts.Model)
	opts.SSHProxy = strings.TrimSpace(opts.SSHProxy)
	if strings.EqualFold(opts.SSHProxy, "none") {
		opts.SSHProxy = ""
	}
	return opts, nil
}

func defaultTeamsModelProfileNameForProvider(provider string) string {
	provider = modelprofile.NormalizeProvider(provider)
	switch provider {
	case "":
		return ""
	case "mimo":
		return "mimo25"
	default:
		return provider
	}
}

func (b *Bridge) startModelProfileKeyIntake(ctx context.Context, msg ChatMessage, arg string) (string, error) {
	if b == nil || b.store == nil {
		return "", fmt.Errorf("Teams durable state is required for model key intake")
	}
	if b.modelProfileManager == nil {
		return modelProfileManagerUnavailable(), nil
	}
	if !messageAuthoredByCurrentUser(msg, b.user) {
		return "Only the Teams helper owner can start model profile API key intake.", nil
	}
	opts, err := parseModelProfileKeyIntakeSetupOptions(arg)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(opts.Provider) == "" {
		return modelProfileKeyIntakeSetupUsage(), nil
	}
	spec, err := modelprofile.MustLookupProvider(opts.Provider)
	if err != nil {
		return "", err
	}
	if spec.ID == modelprofile.DefaultProvider || !spec.UsesAdapter {
		return "The built-in `default` profile uses official Codex auth and does not need an API key. Use `model default default` for future Work chats.", nil
	}
	modelID := ""
	if strings.TrimSpace(opts.Model) != "" {
		selectedModel, err := spec.MustResolveModel(opts.Model)
		if err != nil {
			return "", err
		}
		modelID = selectedModel.PublicID()
	}
	name := strings.TrimSpace(opts.ProfileName)
	if name == "" {
		name = defaultTeamsModelProfileNameForProvider(spec.ID)
	}
	if strings.EqualFold(name, config.DefaultModelProfileName) {
		return "", fmt.Errorf("the built-in default model profile cannot store a third-party API key")
	}
	code, err := newModelProfileKeyIntakeCode()
	if err != nil {
		return "", err
	}
	now := modelProfileKeyIntakeNow().UTC()
	chatID := firstNonEmptyString(msg.ChatID, b.reg.ControlChatID)
	intake := teamstore.ModelProfileKeyIntake{
		ID:               "model-key-intake:" + shortStableID(strings.Join([]string{chatID, chatMessageAuthorUserID(msg), msg.ID, name, spec.ID, modelID, fmt.Sprint(now.UnixNano())}, "\n")),
		ScopeID:          b.scope.ID,
		TeamsChatID:      chatID,
		RequestMessageID: msg.ID,
		AuthorUserID:     chatMessageAuthorUserID(msg),
		AuthorName:       chatMessageAuthorDisplayName(msg),
		ProfileName:      name,
		Provider:         spec.ID,
		Model:            modelID,
		SSHProxy:         opts.SSHProxy,
		SetDefault:       opts.SetDefault,
		Status:           teamstore.ModelProfileKeyIntakePending,
		CreatedAt:        now,
		UpdatedAt:        now,
		ExpiresAt:        now.Add(modelProfileKeyIntakeTTL),
	}
	intake.CodeHash = modelProfileKeyIntakeCodeHash(code, intake)
	if err := b.store.Update(ctx, func(state *teamstore.State) error {
		state.ModelProfileKeyIntakes[intake.ID] = intake
		return nil
	}); err != nil {
		return "", err
	}
	modelLabel := modelID
	if modelLabel == "" {
		modelLabel = "existing/default"
	}
	lines := []string{
		fmt.Sprintf("Teams API key intake started for model profile `%s` (provider `%s`, model `%s`).", name, spec.ID, modelLabel),
		"",
		"Security note: the next message containing the API key may still be retained by Microsoft Teams/Graph/audit systems. I will not write that raw key to local Teams state, control history, or helper outbox; it will only be saved in the local model profile secret store.",
		"",
		fmt.Sprintf("Expires: %s", intake.ExpiresAt.Format(time.RFC3339)),
		fmt.Sprintf("Step 1: send `model key confirm %s`", code),
		fmt.Sprintf("Step 2: then send `model key %s <api-key>`", code),
		"Cancel: `model key cancel " + code + "`",
	}
	if opts.SetDefault {
		lines = append(lines, "", "After the key is saved, this profile will become the default for future Work chats.")
	}
	return strings.Join(lines, "\n"), nil
}

func isModelProfileKeyIntakeControlRoute(routeText string) bool {
	arg, ok := modelProfileKeyIntakeControlArgument(routeText)
	if !ok {
		return false
	}
	sub, _ := modelCommandParts(arg)
	switch sub {
	case "key", "api-key", "apikey":
		return true
	default:
		return false
	}
}

func modelProfileKeyIntakeControlArgument(routeText string) (string, bool) {
	parsed := ParseDashboardCommand(ChatScopeControl, routeText)
	if !parsed.HelperCommand || parsed.Name != DashboardCommandModel {
		return "", false
	}
	return parsed.Argument, true
}

func (b *Bridge) handleModelProfileKeyIntakeControlMessage(ctx context.Context, msg ChatMessage, routeText string) error {
	if !messageAuthoredByCurrentUser(msg, b.user) {
		return b.sendControl(ctx, "Only the Teams helper owner can enter model profile API keys in Teams.")
	}
	arg, ok := modelProfileKeyIntakeControlArgument(routeText)
	if !ok {
		return b.sendControl(ctx, modelProfileKeyIntakeUsage())
	}
	sub, rest := modelCommandParts(arg)
	if sub != "key" && sub != "api-key" && sub != "apikey" {
		return b.sendControl(ctx, modelProfileKeyIntakeUsage())
	}
	action, value := splitModelProfileKeyIntakeAction(rest)
	switch strings.ToLower(action) {
	case "":
		return b.sendControl(ctx, modelProfileKeyIntakeUsage())
	case "confirm":
		code, extra := splitModelProfileKeyIntakeAction(value)
		if code == "" || strings.TrimSpace(extra) != "" {
			return b.sendControl(ctx, "Usage: `model key confirm <code>`")
		}
		message, err := b.confirmModelProfileKeyIntake(ctx, msg, code)
		if err != nil {
			return b.sendControl(ctx, controlCommandErrorMessage(err))
		}
		return b.sendControl(ctx, message)
	case "cancel":
		code, extra := splitModelProfileKeyIntakeAction(value)
		if code == "" || strings.TrimSpace(extra) != "" {
			return b.sendControl(ctx, "Usage: `model key cancel <code>`")
		}
		message, err := b.cancelModelProfileKeyIntake(ctx, msg, code)
		if err != nil {
			return b.sendControl(ctx, controlCommandErrorMessage(err))
		}
		return b.sendControl(ctx, message)
	default:
		code := strings.TrimSpace(action)
		apiKey, extra := splitModelProfileKeyIntakeAction(value)
		if code == "" || apiKey == "" || strings.TrimSpace(extra) != "" {
			return b.sendControl(ctx, "Usage: `model key <code> <api-key>`")
		}
		message, err := b.completeModelProfileKeyIntake(ctx, msg, code, apiKey)
		if err != nil {
			return b.sendControl(ctx, controlCommandErrorMessage(err))
		}
		return b.sendControl(ctx, message)
	}
}

func splitModelProfileKeyIntakeAction(text string) (string, string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", ""
	}
	name, rest, ok := strings.Cut(text, " ")
	if !ok {
		return strings.TrimSpace(name), ""
	}
	return strings.TrimSpace(name), strings.TrimSpace(rest)
}

func (b *Bridge) confirmModelProfileKeyIntake(ctx context.Context, msg ChatMessage, code string) (string, error) {
	now := modelProfileKeyIntakeNow().UTC()
	var out teamstore.ModelProfileKeyIntake
	found := false
	err := b.updateModelProfileKeyIntakeByCode(ctx, msg, code, now, func(intake *teamstore.ModelProfileKeyIntake) (bool, error) {
		found = true
		out = *intake
		switch intake.Status {
		case teamstore.ModelProfileKeyIntakePending:
			intake.Status = teamstore.ModelProfileKeyIntakeConfirmed
			intake.ConfirmedAt = now
			intake.UpdatedAt = now
			out = *intake
			return true, nil
		case teamstore.ModelProfileKeyIntakeConfirmed:
			return false, nil
		default:
			return false, nil
		}
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "No active model API key intake matched that code for this Teams chat and user.", nil
	}
	if out.Status != teamstore.ModelProfileKeyIntakeConfirmed {
		return "That model API key intake is no longer active. Start again with `model setup <provider> [name] --teams-key-intake`.", nil
	}
	return fmt.Sprintf("Confirmed model profile `%s`. Now send `model key %s <api-key>` before %s.", out.ProfileName, strings.TrimSpace(code), out.ExpiresAt.Format(time.RFC3339)), nil
}

func (b *Bridge) cancelModelProfileKeyIntake(ctx context.Context, msg ChatMessage, code string) (string, error) {
	now := modelProfileKeyIntakeNow().UTC()
	var out teamstore.ModelProfileKeyIntake
	found := false
	err := b.updateModelProfileKeyIntakeByCode(ctx, msg, code, now, func(intake *teamstore.ModelProfileKeyIntake) (bool, error) {
		found = true
		out = *intake
		if intake.Status == teamstore.ModelProfileKeyIntakeCompleted || intake.Status == teamstore.ModelProfileKeyIntakeCanceled || intake.Status == teamstore.ModelProfileKeyIntakeExpired {
			return false, nil
		}
		if intake.Status == teamstore.ModelProfileKeyIntakeSaving {
			return false, nil
		}
		intake.Status = teamstore.ModelProfileKeyIntakeCanceled
		intake.CanceledAt = now
		intake.UpdatedAt = now
		out = *intake
		return true, nil
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "No active model API key intake matched that code for this Teams chat and user.", nil
	}
	if out.Status == teamstore.ModelProfileKeyIntakeSaving {
		return fmt.Sprintf("Model API key intake for `%s` is already saving and cannot be canceled now.", out.ProfileName), nil
	}
	return fmt.Sprintf("Canceled model API key intake for `%s`.", out.ProfileName), nil
}

func (b *Bridge) completeModelProfileKeyIntake(ctx context.Context, msg ChatMessage, code string, apiKey string) (string, error) {
	if b.modelProfileManager == nil {
		return modelProfileManagerUnavailable(), nil
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return "", fmt.Errorf("API key is empty")
	}
	now := modelProfileKeyIntakeNow().UTC()
	var intake teamstore.ModelProfileKeyIntake
	found := false
	claimed := false
	err := b.updateModelProfileKeyIntakeByCode(ctx, msg, code, now, func(candidate *teamstore.ModelProfileKeyIntake) (bool, error) {
		found = true
		intake = *candidate
		if candidate.Status != teamstore.ModelProfileKeyIntakeConfirmed {
			return false, nil
		}
		candidate.Status = teamstore.ModelProfileKeyIntakeSaving
		candidate.UpdatedAt = now
		candidate.LastError = ""
		intake = *candidate
		claimed = true
		return true, nil
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "No active model API key intake matched that code for this Teams chat and user.", nil
	}
	if intake.Status == teamstore.ModelProfileKeyIntakePending {
		return fmt.Sprintf("Run `model key confirm %s` before sending the API key.", strings.TrimSpace(code)), nil
	}
	if !claimed {
		if intake.Status == teamstore.ModelProfileKeyIntakeSaving {
			return fmt.Sprintf("Model API key intake for `%s` is already being saved. Wait for the current attempt to finish.", intake.ProfileName), nil
		}
		return "That model API key intake is no longer active. Start again with `model setup <provider> [name] --teams-key-intake`.", nil
	}
	result, saveErr := b.modelProfileManager.SaveModelProfileAPIKey(ctx, ModelProfileAPIKeySaveRequest{
		ProfileName: intake.ProfileName,
		Provider:    intake.Provider,
		Model:       intake.Model,
		APIKey:      apiKey,
		SSHProxy:    intake.SSHProxy,
		SetDefault:  intake.SetDefault,
	})
	var safeSaveErr error
	if saveErr != nil {
		safeSaveErr = fmt.Errorf("%s", sanitizeModelProfileKeyIntakeError(saveErr, apiKey))
	}
	updateErr := b.store.Update(ctx, func(state *teamstore.State) error {
		current := state.ModelProfileKeyIntakes[intake.ID]
		if current.ID == "" {
			return nil
		}
		if current.Status != teamstore.ModelProfileKeyIntakeSaving {
			return nil
		}
		current.UpdatedAt = now
		if safeSaveErr != nil {
			current.Status = teamstore.ModelProfileKeyIntakeConfirmed
			current.LastError = safeSaveErr.Error()
		} else {
			current.Status = teamstore.ModelProfileKeyIntakeCompleted
			current.CompletedAt = now
			current.LastError = ""
		}
		state.ModelProfileKeyIntakes[current.ID] = current
		return nil
	})
	if updateErr != nil {
		return "", updateErr
	}
	if safeSaveErr != nil {
		return "", safeSaveErr
	}
	lines := []string{
		fmt.Sprintf("Saved model profile `%s` (provider `%s`, model `%s`, api_key=%s, fingerprint=%s, revision=%d).", result.ProfileName, result.Provider, result.Model, modelprofile.MaskRef(result.APIKeyRef), result.Fingerprint, result.Revision),
		"Raw key was not written to local Teams state, control history, or helper outbox.",
	}
	if result.SetDefault {
		lines = append(lines, "Default model profile for future Work chats: "+result.ProfileName)
	}
	lines = append(lines, "Use `model doctor "+result.ProfileName+"` to validate it.")
	return strings.Join(lines, "\n"), nil
}

func (b *Bridge) updateModelProfileKeyIntakeByCode(ctx context.Context, msg ChatMessage, code string, now time.Time, fn func(*teamstore.ModelProfileKeyIntake) (bool, error)) error {
	if b == nil || b.store == nil {
		return fmt.Errorf("Teams durable state is required for model key intake")
	}
	code = strings.TrimSpace(code)
	if code == "" {
		return nil
	}
	chatID := firstNonEmptyString(msg.ChatID, b.reg.ControlChatID)
	authorID := chatMessageAuthorUserID(msg)
	return b.store.Update(ctx, func(state *teamstore.State) error {
		for id, intake := range state.ModelProfileKeyIntakes {
			if !modelProfileKeyIntakeMatches(intake, code, chatID, authorID) {
				continue
			}
			if intake.Status != teamstore.ModelProfileKeyIntakeSaving && modelProfileKeyIntakeExpired(intake, now) {
				intake.Status = teamstore.ModelProfileKeyIntakeExpired
				intake.UpdatedAt = now
				state.ModelProfileKeyIntakes[id] = intake
				return nil
			}
			changed, err := fn(&intake)
			if err != nil {
				return err
			}
			if changed {
				state.ModelProfileKeyIntakes[id] = intake
			}
			return nil
		}
		return nil
	})
}

func modelProfileKeyIntakeMatches(intake teamstore.ModelProfileKeyIntake, code string, chatID string, authorID string) bool {
	if strings.TrimSpace(intake.ID) == "" || strings.TrimSpace(code) == "" {
		return false
	}
	if strings.TrimSpace(intake.TeamsChatID) != "" && !strings.EqualFold(strings.TrimSpace(intake.TeamsChatID), strings.TrimSpace(chatID)) {
		return false
	}
	if strings.TrimSpace(intake.AuthorUserID) != "" && !strings.EqualFold(strings.TrimSpace(intake.AuthorUserID), strings.TrimSpace(authorID)) {
		return false
	}
	switch intake.Status {
	case teamstore.ModelProfileKeyIntakePending, teamstore.ModelProfileKeyIntakeConfirmed, teamstore.ModelProfileKeyIntakeSaving:
	default:
		return false
	}
	return modelProfileKeyIntakeCodeHash(code, intake) == intake.CodeHash
}

func modelProfileKeyIntakeExpired(intake teamstore.ModelProfileKeyIntake, now time.Time) bool {
	return !intake.ExpiresAt.IsZero() && !now.IsZero() && !intake.ExpiresAt.After(now)
}

func modelProfileKeyIntakeCodeHash(code string, intake teamstore.ModelProfileKeyIntake) string {
	material := strings.Join([]string{
		strings.TrimSpace(intake.ID),
		strings.TrimSpace(intake.TeamsChatID),
		strings.TrimSpace(intake.AuthorUserID),
		strings.TrimSpace(code),
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return "code:" + hex.EncodeToString(sum[:])[:24]
}

func randomModelProfileKeyIntakeCode() (string, error) {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	var b strings.Builder
	for _, v := range raw {
		b.WriteByte(alphabet[int(v)%len(alphabet)])
	}
	return b.String(), nil
}

func sanitizeModelProfileKeyIntakeError(err error, apiKey string) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	apiKey = strings.TrimSpace(apiKey)
	if apiKey != "" {
		msg = strings.ReplaceAll(msg, apiKey, "<redacted-api-key>")
	}
	return msg
}

func modelProfileKeyIntakeSetupUsage() string {
	return "Usage: `model setup <provider> [name] --teams-key-intake [--model <model>] [--set-default] [--ssh-proxy <profile>]`"
}

func modelProfileKeyIntakeUsage() string {
	return strings.Join([]string{
		"Model API key intake commands:",
		"- Start: `model setup <provider> [name] --teams-key-intake [--model <model>]`",
		"- Confirm: `model key confirm <code>`",
		"- Save: `model key <code> <api-key>`",
		"- Cancel: `model key cancel <code>`",
	}, "\n")
}
