package teams

import (
	"context"
	"fmt"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	reasoningEffortSourceExplicit        = "explicit"
	reasoningEffortSourceModelDefault    = "model_default"
	reasoningEffortSourceExecutorDefault = "executor_default"
	reasoningEffortSourceHelperDefault   = "helper_default"
	reasoningEffortSourceGlobalDefault   = "global_default"
	reasoningEffortSourceRuntimeDefault  = ReasoningEffortSourceRuntimeDefault
	reasoningEffortSourceLegacy          = "legacy"
)

func (b *Bridge) handleReasoningEffortControlCommand(ctx context.Context, arg string) (string, error) {
	session, err := b.ensureControlFallbackSession(ctx)
	if err != nil {
		return "", err
	}
	return b.handleReasoningEffortCommand(ctx, session, b.effectiveControlFallbackExecutor(), arg)
}

func (b *Bridge) handleReasoningEffortWorkCommand(ctx context.Context, session *Session, arg string) (string, error) {
	return b.handleReasoningEffortCommand(ctx, session, b.executor, arg)
}

func (b *Bridge) handleReasoningEffortCommand(ctx context.Context, session *Session, executor Executor, arg string) (string, error) {
	if session == nil {
		return "", fmt.Errorf("session not found")
	}
	sub, rest := modelCommandParts(arg)
	if sub == "" {
		sub = "status"
	}
	switch sub {
	case "status", "current":
		// Prefer a cached read-only catalog so status does not start or
		// reconfigure a runner. Preserve the cold-start fallback when no cache is
		// available; read-only commands must remain compatible with old adapters.
		catalog, err := reasoningEffortCatalogReadOnly(ctx, executor, session)
		if err != nil {
			catalog, err = reasoningEffortCatalog(ctx, executor, session)
		}
		if err != nil {
			return formatReasoningEffortStatus(session, executor, ReasoningEffortCatalog{}, err), nil
		}
		return formatReasoningEffortStatus(session, executor, catalog, nil), nil
	case "list", "ls", "options", "choices":
		catalog, err := reasoningEffortCatalogReadOnly(ctx, executor, session)
		if err != nil {
			catalog, err = reasoningEffortCatalog(ctx, executor, session)
		}
		if err != nil {
			return "", err
		}
		return formatReasoningEffortCatalog(session, executor, catalog), nil
	case "set", "switch", "use":
		if strings.TrimSpace(rest) == "" {
			return "", fmt.Errorf("usage: `effort set <value>`")
		}
		return b.setReasoningEffortFromCatalog(ctx, session, executor, rest, reasoningEffortSourceExplicit)
	case "reset", "default":
		if b != nil && b.defaultManager != nil {
			effort, source, err := b.defaultManager.ResolveDefaultReasoningEffort(ctx, session.ModelProfile)
			if err != nil {
				return "", err
			}
			source = normalizedReasoningEffortSource(effort, source)
			if strings.TrimSpace(effort) != "" {
				return b.setReasoningEffortFromCatalog(ctx, session, executor, effort, firstNonEmptyString(source, reasoningEffortSourceGlobalDefault))
			}
			if source == reasoningEffortSourceRuntimeDefault && !isControlFallbackSessionID(session.ID) {
				return b.setSessionReasoningEffort(ctx, session, "", source, ReasoningEffortCatalog{})
			}
		}
		catalog, err := reasoningEffortCatalog(ctx, executor, session)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(catalog.DefaultEffort) == "" {
			return "", fmt.Errorf("model %q did not advertise a default reasoning effort", catalog.Model)
		}
		return b.setReasoningEffortFromCatalog(ctx, session, executor, catalog.DefaultEffort, reasoningEffortSourceModelDefault)
	case "help", "?":
		return reasoningEffortUsage(), nil
	default:
		if strings.TrimSpace(rest) == "" {
			return b.setReasoningEffortFromCatalog(ctx, session, executor, sub, reasoningEffortSourceExplicit)
		}
		return reasoningEffortUsage(), nil
	}
}

func reasoningEffortCatalog(ctx context.Context, executor Executor, session *Session) (ReasoningEffortCatalog, error) {
	provider, ok := executor.(ReasoningEffortCatalogProvider)
	if !ok {
		return ReasoningEffortCatalog{}, fmt.Errorf("the active Codex executor does not expose model/list")
	}
	catalog, err := provider.ReasoningEffortCatalog(ctx, session)
	if err != nil {
		return ReasoningEffortCatalog{}, fmt.Errorf("could not list reasoning efforts: %w", err)
	}
	if len(catalog.Options) == 0 {
		return ReasoningEffortCatalog{}, fmt.Errorf("model %q did not advertise any reasoning efforts", catalog.Model)
	}
	return catalog, nil
}

func reasoningEffortCatalogReadOnly(ctx context.Context, executor Executor, session *Session) (ReasoningEffortCatalog, error) {
	if provider, ok := executor.(ReadOnlyReasoningEffortCatalogProvider); ok {
		catalog, err := provider.CachedReasoningEffortCatalog(ctx, session)
		if err != nil {
			return ReasoningEffortCatalog{}, fmt.Errorf("could not list cached reasoning efforts: %w", err)
		}
		if len(catalog.Options) == 0 {
			return ReasoningEffortCatalog{}, fmt.Errorf("model %q did not advertise any reasoning efforts", catalog.Model)
		}
		return catalog, nil
	}
	return ReasoningEffortCatalog{}, fmt.Errorf("the active Codex executor does not expose a cached model/list; status/list will not start or reconfigure a runner")
}

func (b *Bridge) setReasoningEffortFromCatalog(ctx context.Context, session *Session, executor Executor, requested string, source string) (string, error) {
	catalog, err := reasoningEffortCatalog(ctx, executor, session)
	if err != nil {
		return "", err
	}
	requested = strings.TrimSpace(requested)
	for _, option := range catalog.Options {
		if strings.EqualFold(strings.TrimSpace(option.Effort), requested) {
			return b.setSessionReasoningEffort(ctx, session, option.Effort, source, catalog)
		}
	}
	return "", fmt.Errorf("reasoning effort %q is not supported by model %q; available values: %s", requested, catalog.Model, reasoningEffortOptionNames(catalog.Options))
}

func (b *Bridge) setSessionReasoningEffort(ctx context.Context, session *Session, effort string, source string, catalog ReasoningEffortCatalog) (string, error) {
	effort = strings.TrimSpace(effort)
	source = strings.TrimSpace(source)
	if source == "" {
		source = reasoningEffortSourceExplicit
	}
	if effort == "" && source != reasoningEffortSourceRuntimeDefault {
		return "", fmt.Errorf("reasoning effort is required")
	}
	b.reasoningEffortMu.Lock()
	defer b.reasoningEffortMu.Unlock()
	previous := strings.TrimSpace(session.ReasoningEffort)
	now := time.Now()
	if b.store != nil {
		updated, _, err := b.store.UpdateSessionContext(ctx, session.ID, func(current teamstore.SessionContext, found bool, mutationTime time.Time) (teamstore.SessionContext, bool, error) {
			if !found || current.ID == "" {
				return current, false, fmt.Errorf("session %q not found", session.ID)
			}
			if current.ReasoningEffort == effort && current.ReasoningEffortSource == source {
				return current, false, nil
			}
			current.ReasoningEffort = effort
			current.ReasoningEffortSource = source
			current.UpdatedAt = mutationTime
			return current, true, nil
		})
		if err != nil {
			return "", err
		}
		effort = strings.TrimSpace(updated.ReasoningEffort)
		source = strings.TrimSpace(updated.ReasoningEffortSource)
		if !updated.UpdatedAt.IsZero() {
			now = updated.UpdatedAt
		}
	}
	updatedRegistrySession := false
	b.regMu.Lock()
	if current := b.reg.SessionByID(session.ID); current != nil {
		current.ReasoningEffort = effort
		current.ReasoningEffortSource = source
		current.UpdatedAt = now
		b.registryProjectionDirty = true
		updatedRegistrySession = current == session
	}
	b.regMu.Unlock()
	if !updatedRegistrySession {
		session.ReasoningEffort = effort
		session.ReasoningEffortSource = source
		session.UpdatedAt = now
	}
	if strings.TrimSpace(b.registryPath) != "" && !isControlFallbackSessionID(session.ID) {
		if err := b.Save(); err != nil {
			return "", err
		}
	}
	model := strings.TrimSpace(catalog.Model)
	if model == "" {
		model = "current model"
	}
	previousLabel := "`" + firstNonEmptyString(previous, "unset") + "`"
	return fmt.Sprintf("Reasoning effort updated for this %s chat\nPrevious: %s\nEffective for next turn: %s\nModel: `%s`\nSource: %s\nApplies to new turns in this chat; running or already queued turns keep their captured effort.", reasoningEffortChatKind(session), previousLabel, reasoningEffortDisplayValue(effort), model, reasoningEffortSourceLabel(source)), nil
}

func formatReasoningEffortStatus(session *Session, executor Executor, catalog ReasoningEffortCatalog, catalogErr error) string {
	effort, source := effectiveSessionReasoningEffortResolution(session, executor)
	lines := []string{
		"Current chat effort (" + reasoningEffortChatKind(session) + ")",
		"Effective for next turn: " + reasoningEffortDisplayValue(effort),
		"Source: " + reasoningEffortSourceLabel(source),
		"Applies to: new turns in this chat; running and already queued turns keep their captured effort.",
	}
	if model := strings.TrimSpace(catalog.Model); model != "" {
		lines = append(lines, "Model for this chat: `"+model+"`")
	}
	if catalog.DefaultEffort != "" {
		lines = append(lines, "Model-advertised default: `"+catalog.DefaultEffort+"`")
	}
	if len(catalog.Options) > 0 {
		lines = append(lines, "Available: "+reasoningEffortOptionNames(catalog.Options))
	}
	if catalogErr != nil {
		lines = append(lines, "Dynamic list unavailable: "+catalogErr.Error())
	}
	return strings.Join(lines, "\n")
}

func formatReasoningEffortCatalog(session *Session, executor Executor, catalog ReasoningEffortCatalog) string {
	model := firstNonEmptyString(catalog.DisplayName, catalog.Model, "current model")
	lines := []string{"Current chat effort choices for " + model + ":"}
	current, _ := effectiveSessionReasoningEffortResolution(session, executor)
	for _, option := range catalog.Options {
		markers := []string{}
		if strings.EqualFold(option.Effort, current) {
			markers = append(markers, "current")
		}
		if strings.EqualFold(option.Effort, catalog.DefaultEffort) {
			markers = append(markers, "model-advertised default")
		}
		line := "- `" + option.Effort + "`"
		if len(markers) > 0 {
			line += " (" + strings.Join(markers, ", ") + ")"
		}
		if option.Description != "" {
			line += " - " + option.Description
		}
		lines = append(lines, line)
	}
	lines = append(lines, "", "Use `effort set <value>` to switch this chat.")
	return strings.Join(lines, "\n")
}

func normalizedReasoningEffortSource(effort string, source string) string {
	source = strings.TrimSpace(source)
	if source != "" {
		return source
	}
	if strings.TrimSpace(effort) == "" {
		return reasoningEffortSourceRuntimeDefault
	}
	return reasoningEffortSourceLegacy
}

func reasoningEffortDisplayValue(effort string) string {
	effort = strings.TrimSpace(effort)
	if effort == "" {
		return "Codex runtime fallback"
	}
	return "`" + effort + "`"
}

func reasoningEffortOptionNames(options []ReasoningEffortOption) string {
	names := make([]string, 0, len(options))
	for _, option := range options {
		if effort := strings.TrimSpace(option.Effort); effort != "" {
			names = append(names, "`"+effort+"`")
		}
	}
	return strings.Join(names, ", ")
}

func reasoningEffortChatKind(session *Session) string {
	if session != nil && isControlFallbackSessionID(session.ID) {
		return "Control"
	}
	return "Work"
}

func reasoningEffortUsage() string {
	return strings.Join([]string{
		"Reasoning effort commands:",
		"- `effort status` - show this chat's current effort",
		"- `effort list` - list model-advertised choices",
		"- `effort set <value>` - switch future turns in this chat",
		"- `effort reset` - use the current model's advertised default",
	}, "\n")
}
