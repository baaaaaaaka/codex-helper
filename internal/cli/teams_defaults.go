package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

type teamsDefaultSettingSpec struct {
	key         string
	aliases     []string
	description string
	status      func(context.Context) (string, error)
	list        func(context.Context) (string, error)
	set         func(context.Context, string) (string, error)
	reset       func(context.Context) (string, error)
}

func (m teamsModelProfileManager) defaultSettingSpecs() []teamsDefaultSettingSpec {
	return []teamsDefaultSettingSpec{
		{
			key: "model", aliases: []string{"model-profile"}, description: "model used by future launches and sessions",
			status: m.globalDefaultModelStatus, list: m.ListModelProfiles, set: m.setGlobalDefaultModel, reset: m.resetGlobalDefaultModel,
		},
		{
			key: "effort", aliases: []string{"reasoning-effort", "thinking-effort"}, description: "reasoning effort used by future launches and sessions",
			status: m.globalDefaultEffortStatus, list: m.globalDefaultEffortList, set: m.setGlobalDefaultEffort, reset: m.resetGlobalDefaultEffort,
		},
	}
}

func (m teamsModelProfileManager) HandleDefaultCommand(ctx context.Context, command teams.DefaultCommand) (string, error) {
	specs := m.defaultSettingSpecs()
	if command.Action == teams.DefaultCommandHelp {
		return globalDefaultCommandUsage(specs, strings.TrimSpace(command.Setting)), nil
	}
	if strings.TrimSpace(command.Setting) == "" {
		if command.Action != teams.DefaultCommandStatus {
			return globalDefaultCommandUsage(specs, ""), nil
		}
		lines := []string{"Global defaults", "Applies to future launches and newly created chats; existing chats remain unchanged."}
		for _, spec := range specs {
			status, err := spec.status(ctx)
			if err != nil {
				return "", err
			}
			lines = append(lines, "", status)
		}
		return strings.Join(lines, "\n"), nil
	}
	spec, ok := findTeamsDefaultSetting(specs, command.Setting)
	if !ok {
		return "", fmt.Errorf("unknown global default %q; available settings: %s", command.Setting, teamsDefaultSettingNames(specs))
	}
	switch command.Action {
	case teams.DefaultCommandStatus:
		return spec.status(ctx)
	case teams.DefaultCommandList:
		if spec.list == nil {
			return "", fmt.Errorf("global default %q does not expose a value list", spec.key)
		}
		return spec.list(ctx)
	case teams.DefaultCommandSet:
		if spec.set == nil {
			return "", fmt.Errorf("global default %q cannot be changed", spec.key)
		}
		return spec.set(ctx, command.Value)
	case teams.DefaultCommandReset:
		if spec.reset == nil {
			return "", fmt.Errorf("global default %q cannot be reset", spec.key)
		}
		return spec.reset(ctx)
	default:
		return globalDefaultCommandUsage(specs, spec.key), nil
	}
}

func findTeamsDefaultSetting(specs []teamsDefaultSettingSpec, ref string) (teamsDefaultSettingSpec, bool) {
	ref = strings.ToLower(strings.TrimSpace(ref))
	for _, spec := range specs {
		if ref == spec.key {
			return spec, true
		}
		for _, alias := range spec.aliases {
			if ref == strings.ToLower(strings.TrimSpace(alias)) {
				return spec, true
			}
		}
	}
	return teamsDefaultSettingSpec{}, false
}

func teamsDefaultSettingNames(specs []teamsDefaultSettingSpec) string {
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		names = append(names, "`"+spec.key+"`")
	}
	return strings.Join(names, ", ")
}

func globalDefaultCommandUsage(specs []teamsDefaultSettingSpec, setting string) string {
	setting = strings.TrimSpace(setting)
	if setting != "" {
		return strings.Join([]string{
			"Global default `" + setting + "` commands:",
			"- `default " + setting + " status`",
			"- `default " + setting + " list`",
			"- `default " + setting + " set <value>`",
			"- `default " + setting + " reset`",
		}, "\n")
	}
	lines := []string{
		"Global default commands (Control chat only):",
		"- `default status` - show every registered global default",
	}
	for _, spec := range specs {
		lines = append(lines, "- `default "+spec.key+" status|list|set|reset` - "+spec.description)
	}
	return strings.Join(lines, "\n")
}

func (m teamsModelProfileManager) defaultModelResolver() teams.ModelProfileResolver {
	if m.defaultResolver != nil {
		return m.defaultResolver
	}
	if m.runtimeResolver != nil {
		return newTeamsModelProfileResolverWithRuntime(m.root, m.runtimeResolver)
	}
	return newTeamsModelProfileResolver(m.root, m.codexPath)
}

func (m teamsModelProfileManager) resolveGlobalDefaultModel(ctx context.Context) (config.Config, modelprofile.Snapshot, string, string, error) {
	store, err := m.store()
	if err != nil {
		return config.Config{}, modelprofile.Snapshot{}, "", "", err
	}
	cfg, err := store.Load()
	if err != nil {
		return config.Config{}, modelprofile.Snapshot{}, "", "", err
	}
	selector := cfg.EffectiveDefaultModelSelector()
	source := "built_in"
	if cfg.Defaults != nil && strings.TrimSpace(cfg.Defaults.Model) != "" {
		source = "explicit"
	} else if strings.TrimSpace(cfg.DefaultModelProfile) != "" {
		source = "legacy_default_model_profile"
	}
	snapshot, err := m.defaultModelResolver()(ctx, selector)
	if err != nil {
		return config.Config{}, modelprofile.Snapshot{}, "", "", err
	}
	return cfg, snapshot, selector, source, nil
}

func (m teamsModelProfileManager) globalDefaultModelStatus(ctx context.Context) (string, error) {
	_, snapshot, selector, source, err := m.resolveGlobalDefaultModel(ctx)
	if err != nil {
		return "", err
	}
	return strings.Join([]string{
		"Model",
		"Configured: `" + selector + "`",
		"Effective: `" + defaultSnapshotLabel(snapshot) + "`",
		"Source: `" + source + "`",
	}, "\n"), nil
}

func defaultSnapshotLabel(snapshot modelprofile.Snapshot) string {
	return firstNonEmptyCLI(strings.TrimSpace(snapshot.Model), strings.TrimSpace(snapshot.DefaultModel), strings.TrimSpace(snapshot.Name), config.DefaultModelProfileName)
}

func canonicalGlobalModelSelector(snapshot modelprofile.Snapshot) string {
	if snapshot.IsDefault() {
		if model := strings.TrimSpace(firstNonEmptyCLI(snapshot.Model, snapshot.DefaultModel)); model != "" {
			return "official:" + model
		}
		return config.DefaultModelProfileName
	}
	return "profile:" + strings.TrimSpace(snapshot.Name)
}

func defaultProfileConfigFingerprint(cfg config.Config, selector string) string {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(selector)), "profile:") {
		return ""
	}
	name := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(selector), "profile:"))
	profile := config.ModelProfile{}
	present := false
	for candidate, value := range cfg.ModelProfiles {
		if strings.EqualFold(strings.TrimSpace(candidate), name) {
			profile = value
			present = true
			break
		}
	}
	payload := struct {
		Present            bool                              `json:"present"`
		Profile            config.ModelProfile               `json:"profile"`
		ModelConfigVersion int                               `json:"modelConfigVersion"`
		Credentials        map[string]config.ModelCredential `json:"credentials"`
		Providers          map[string]config.ModelProvider   `json:"providers"`
		Models             map[string]config.ModelDefinition `json:"models"`
		SSHProfiles        []config.Profile                  `json:"sshProfiles"`
	}{
		Present: present, Profile: profile, ModelConfigVersion: cfg.ModelConfigVersion,
		Credentials: cfg.ModelCredentials, Providers: cfg.ModelProviders, Models: cfg.Models, SSHProfiles: cfg.Profiles,
	}
	raw, _ := json.Marshal(payload)
	return string(raw)
}

func validateDefaultProfileFingerprint(cfg config.Config, selector, validated string) error {
	if current := defaultProfileConfigFingerprint(cfg, selector); current != validated {
		return fmt.Errorf("global default model profile %q changed concurrently; retry the command", selector)
	}
	return nil
}

func (m teamsModelProfileManager) setGlobalDefaultModel(ctx context.Context, value string) (string, error) {
	requested := strings.TrimSpace(value)
	store, err := m.store()
	if err != nil {
		return "", err
	}
	validatedConfig, err := store.Load()
	if err != nil {
		return "", err
	}
	snapshot, err := m.defaultModelResolver()(ctx, requested)
	if err != nil {
		return "", err
	}
	selector := canonicalGlobalModelSelector(snapshot)
	if strings.EqualFold(requested, config.DefaultModelProfileName) {
		// `default model set default` is an explicit, dynamic account-default
		// selection, not a request to freeze today's resolved official slug.
		// `default model reset` remains distinct: it removes the typed override
		// and restores the legacy consumer-specific fallback.
		selector = config.DefaultModelProfileName
	}
	validatedProfileFingerprint := defaultProfileConfigFingerprint(validatedConfig, selector)
	effortNotice := ""
	if err := store.Update(func(updated *config.Config) error {
		if err := validateDefaultProfileFingerprint(*updated, selector, validatedProfileFingerprint); err != nil {
			return err
		}
		nextEffort, notice, err := reconcileDefaultEffortWithSnapshot(updated.ExplicitDefaultReasoningEffort(), snapshot)
		if err != nil {
			return err
		}
		effortNotice = notice
		defaults := updated.EnsureGlobalDefaults()
		defaults.Model = selector
		defaults.ReasoningEffort = nextEffort
		switch {
		case strings.HasPrefix(strings.ToLower(selector), "profile:"):
			updated.DefaultModelProfile = strings.TrimSpace(strings.TrimPrefix(selector, "profile:"))
		case strings.EqualFold(selector, config.DefaultModelProfileName):
			updated.DefaultModelProfile = ""
		default:
			updated.DefaultModelProfile = ""
		}
		return nil
	}); err != nil {
		return "", err
	}
	lines := []string{
		"Global default model: `" + selector + "`",
		"Applies to future launches and newly created chats; this Control chat is unchanged.",
	}
	if effortNotice != "" {
		lines = append(lines, effortNotice)
	}
	return strings.Join(lines, "\n"), nil
}

func (m teamsModelProfileManager) resetGlobalDefaultModel(ctx context.Context) (string, error) {
	snapshot, err := m.defaultModelResolver()(ctx, config.DefaultModelProfileName)
	if err != nil {
		return "", err
	}
	store, err := m.store()
	if err != nil {
		return "", err
	}
	effortNotice := ""
	if err := store.Update(func(cfg *config.Config) error {
		nextEffort, notice, err := reconcileDefaultEffortWithSnapshot(cfg.ExplicitDefaultReasoningEffort(), snapshot)
		if err != nil {
			return err
		}
		effortNotice = notice
		cfg.DefaultModelProfile = ""
		if cfg.Defaults != nil {
			cfg.Defaults.Model = ""
			cfg.Defaults.ReasoningEffort = nextEffort
			cfg.PruneEmptyGlobalDefaults()
		}
		return nil
	}); err != nil {
		return "", err
	}
	message := "Global default model reset to the built-in Codex default. Existing chats are unchanged."
	if effortNotice != "" {
		message += "\n" + effortNotice
	}
	return message, nil
}

func snapshotReasoningEfforts(snapshot modelprofile.Snapshot) ([]string, string) {
	var options []string
	_ = json.Unmarshal([]byte(snapshot.SupportedReasoningEffortsJSON), &options)
	clean := make([]string, 0, len(options))
	for _, option := range options {
		if option = strings.TrimSpace(option); option != "" {
			clean = append(clean, option)
		}
	}
	return clean, strings.TrimSpace(snapshot.DefaultReasoningEffort)
}

func effortSupported(options []string, effort string) bool {
	for _, option := range options {
		if strings.EqualFold(strings.TrimSpace(option), strings.TrimSpace(effort)) {
			return true
		}
	}
	return false
}

func reconcileDefaultEffortWithSnapshot(current string, snapshot modelprofile.Snapshot) (string, string, error) {
	current = strings.TrimSpace(current)
	if current == "" {
		return "", "", nil
	}
	options, modelDefault := snapshotReasoningEfforts(snapshot)
	if effortSupported(options, current) {
		return current, "", nil
	}
	if len(options) == 0 && modelDefault == "" {
		if snapshot.IsDefault() && strings.TrimSpace(firstNonEmptyCLI(snapshot.Model, snapshot.DefaultModel)) == "" {
			return "", "", fmt.Errorf("cannot validate global default effort %q because the official model catalog is unavailable", current)
		}
		return "", "Global default effort `" + current + "` was cleared because the selected model does not advertise reasoning effort.", nil
	}
	if modelDefault == "" || !effortSupported(options, modelDefault) {
		return "", "", fmt.Errorf("model %q does not advertise a valid default reasoning effort", defaultSnapshotLabel(snapshot))
	}
	return modelDefault, "Global default effort `" + current + "` is not supported by the selected model and was reset to `" + modelDefault + "`.", nil
}

func (m teamsModelProfileManager) globalDefaultEffortStatus(ctx context.Context) (string, error) {
	cfg, snapshot, _, _, err := m.resolveGlobalDefaultModel(ctx)
	if err != nil {
		return "", err
	}
	explicit := cfg.ExplicitDefaultReasoningEffort()
	_, modelDefault := snapshotReasoningEfforts(snapshot)
	effective := explicit
	source := "explicit"
	if effective == "" {
		hasExplicitModel := cfg.Defaults != nil && strings.TrimSpace(cfg.Defaults.Model) != ""
		if hasExplicitModel && modelDefault != "" {
			effective = modelDefault
			source = "model_default"
		} else {
			effective = "runtime fallback"
			source = "inherited_runtime"
		}
	}
	return strings.Join([]string{
		"Effort",
		"Configured: `" + firstNonEmptyCLI(explicit, "unset") + "`",
		"Effective: `" + effective + "`",
		"Model: `" + defaultSnapshotLabel(snapshot) + "`",
		"Source: `" + source + "`",
	}, "\n"), nil
}

func (m teamsModelProfileManager) globalDefaultEffortList(ctx context.Context) (string, error) {
	cfg, snapshot, _, _, err := m.resolveGlobalDefaultModel(ctx)
	if err != nil {
		return "", err
	}
	options, modelDefault := snapshotReasoningEfforts(snapshot)
	if len(options) == 0 {
		return "", fmt.Errorf("model %q did not advertise reasoning effort choices", defaultSnapshotLabel(snapshot))
	}
	current := cfg.ExplicitDefaultReasoningEffort()
	lines := []string{"Global default effort choices for `" + defaultSnapshotLabel(snapshot) + "`:"}
	for _, option := range options {
		markers := []string{}
		if strings.EqualFold(option, current) {
			markers = append(markers, "configured")
		}
		if strings.EqualFold(option, modelDefault) {
			markers = append(markers, "model default")
		}
		line := "- `" + option + "`"
		if len(markers) > 0 {
			line += " (" + strings.Join(markers, ", ") + ")"
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), nil
}

func (m teamsModelProfileManager) setGlobalDefaultEffort(ctx context.Context, value string) (string, error) {
	validatedConfig, snapshot, validatedSelector, _, err := m.resolveGlobalDefaultModel(ctx)
	if err != nil {
		return "", err
	}
	options, _ := snapshotReasoningEfforts(snapshot)
	requested := strings.TrimSpace(value)
	var canonical string
	for _, option := range options {
		if strings.EqualFold(option, requested) {
			canonical = option
			break
		}
	}
	if canonical == "" {
		return "", fmt.Errorf("reasoning effort %q is not supported by global default model %q; available values: %s", requested, defaultSnapshotLabel(snapshot), strings.Join(options, ", "))
	}
	store, err := m.store()
	if err != nil {
		return "", err
	}
	validatedProfileFingerprint := defaultProfileConfigFingerprint(validatedConfig, validatedSelector)
	if err := store.Update(func(cfg *config.Config) error {
		if current := cfg.EffectiveDefaultModelSelector(); !strings.EqualFold(strings.TrimSpace(current), strings.TrimSpace(validatedSelector)) {
			return fmt.Errorf("global default model changed concurrently from %q to %q; retry `default effort set %s`", validatedSelector, current, canonical)
		}
		if err := validateDefaultProfileFingerprint(*cfg, validatedSelector, validatedProfileFingerprint); err != nil {
			return err
		}
		cfg.EnsureGlobalDefaults().ReasoningEffort = canonical
		return nil
	}); err != nil {
		return "", err
	}
	return "Global default effort: `" + canonical + "`\nApplies to future launches and newly created chats; existing chats are unchanged.", nil
}

func (m teamsModelProfileManager) resetGlobalDefaultEffort(ctx context.Context) (string, error) {
	store, err := m.store()
	if err != nil {
		return "", err
	}
	if err := store.Update(func(cfg *config.Config) error {
		if cfg.Defaults != nil {
			cfg.Defaults.ReasoningEffort = ""
			cfg.PruneEmptyGlobalDefaults()
		}
		return nil
	}); err != nil {
		return "", err
	}
	_ = ctx
	return "Global default effort reset to the inherited runtime/model fallback. Existing chats are unchanged.", nil
}

func (m teamsModelProfileManager) ResolveDefaultReasoningEffort(ctx context.Context, snapshot modelprofile.Snapshot) (string, string, error) {
	store, err := m.store()
	if err != nil {
		return "", "", err
	}
	cfg, err := store.Load()
	if err != nil {
		return "", "", err
	}
	effort := cfg.ExplicitDefaultReasoningEffort()
	if effort == "" {
		if cfg.Defaults != nil && strings.TrimSpace(cfg.Defaults.Model) != "" {
			options, modelDefault := snapshotReasoningEfforts(snapshot)
			if modelDefault != "" && effortSupported(options, modelDefault) {
				return modelDefault, "model_default", nil
			}
		}
		return "", "", nil
	}
	options, modelDefault := snapshotReasoningEfforts(snapshot)
	if effortSupported(options, effort) {
		return effort, "global_default", nil
	}
	if modelDefault != "" && effortSupported(options, modelDefault) {
		return modelDefault, "model_default", nil
	}
	return "", "", nil
}
