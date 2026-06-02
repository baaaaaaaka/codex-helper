package cli

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

type teamsModelProfileManager struct {
	root *rootOptions
}

func newTeamsModelProfileManager(root *rootOptions) teamsModelProfileManager {
	return teamsModelProfileManager{root: root}
}

func (m teamsModelProfileManager) store() (*config.Store, error) {
	store, _, err := newRootStore(m.root, "")
	return store, err
}

func (m teamsModelProfileManager) ListModelProfiles(ctx context.Context) (string, error) {
	store, err := m.store()
	if err != nil {
		return "", err
	}
	cfg, err := store.Load()
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	printModelChoices(&out, cfg, secretStore)
	_ = ctx
	return strings.TrimSpace(out.String()) + "\n\nUse `model setup <model>` to configure a model, or `model use <model>` for future Work chats.", nil
}

func (m teamsModelProfileManager) ModelProfileProviders(ctx context.Context) (string, error) {
	ids := modelprofile.ProviderIDs()
	sort.Strings(ids)
	lines := []string{"Model profile providers"}
	for _, id := range ids {
		spec, ok := modelprofile.LookupProvider(id)
		if !ok {
			continue
		}
		if spec.ID == modelprofile.DefaultProvider {
			lines = append(lines, "- default: Codex official API")
			continue
		}
		features := []string{}
		if spec.SupportsTools {
			features = append(features, "tools")
		}
		if spec.SupportsVision {
			features = append(features, "vision")
		}
		if spec.SupportsReason {
			features = append(features, "reasoning")
		}
		if len(features) == 0 {
			features = append(features, "chat")
		}
		models := []string{}
		for _, model := range spec.ModelCatalog() {
			models = append(models, "`"+model.PublicID()+"`"+modelAliasSummary(model))
		}
		modelText := "`" + spec.DefaultPublicModel() + "`"
		if len(models) > 1 {
			modelText = strings.Join(models, ", ")
		}
		lines = append(lines, fmt.Sprintf("- %s: %s, default `%s`, models %s, env `%s`", spec.ID, strings.Join(features, "/"), spec.DefaultPublicModel(), modelText, spec.RecommendedEnv))
	}
	_ = ctx
	return strings.Join(lines, "\n"), nil
}

func (m teamsModelProfileManager) ModelProfileSetupGuide(ctx context.Context, arg string) (string, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		store, err := m.store()
		if err != nil {
			return "", err
		}
		cfg, err := store.Load()
		if err != nil {
			return "", err
		}
		var out bytes.Buffer
		secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
		printModelChoices(&out, cfg, secretStore)
		return strings.TrimSpace(out.String()) + "\n\nReply with `model setup <model>`, for example `model setup mimo-v2.5-pro`.", nil
	}
	if choice, ok := modelprofile.LookupModelChoice(arg); ok {
		if !choice.RequiresAPIKey {
			return "Use `model use default` to switch future Work chats back to the Codex official API.", nil
		}
		return fmt.Sprintf("Set up %s with `model setup %s`. If no %s API key is configured yet, I will start a one-time Teams key intake.", choice.DisplayName, choice.ID, choice.ProviderDisplayName), nil
	}
	provider, name := parseTeamsModelSetupGuideArg(arg)
	if provider == "" {
		return "Usage: `model setup <model>`\n\nRun `model setup` to see all available models.", nil
	}
	spec, err := modelprofile.MustLookupProvider(provider)
	if err != nil {
		return "", err
	}
	if name == "" {
		name = spec.ID
		if spec.ID == "mimo" {
			name = "mimo25"
		}
	}
	if spec.ID == modelprofile.DefaultProvider {
		return "Use the built-in official Codex API profile with:\n\n`cxp model-profile set-default default`", nil
	}
	envName := strings.TrimSpace(spec.RecommendedEnv)
	if envName == "" {
		envName = strings.ToUpper(spec.ID) + "_API_KEY"
	}
	lines := []string{
		"Run one of these commands in a local terminal:",
		"",
		fmt.Sprintf("`cxp model-profile setup %s --provider %s --api-key-stdin --set-default`", shellQuoteForTeams(name), shellQuoteForTeams(spec.ID)),
		fmt.Sprintf("`cxp model-profile setup %s --provider %s --api-key-env %s --set-default`", shellQuoteForTeams(name), shellQuoteForTeams(spec.ID), shellQuoteForTeams(envName)),
		"",
		"Or, if you cannot access a terminal, use the explicit Teams key intake flow:",
		fmt.Sprintf("`model setup %s %s --teams-key-intake --set-default`", shellQuoteForTeams(spec.ID), shellQuoteForTeams(name)),
		"",
		"After that, use `model list` in Teams, `model default <name>` for future Work chats, or `new <directory> --model <name>`.",
	}
	if models := spec.ModelCatalog(); len(models) > 1 {
		lines = append(lines, "", "Optional model choices:")
		for _, model := range models {
			lines = append(lines, fmt.Sprintf("- `%s` - %s%s", model.PublicID(), model.Label(), modelAliasSummary(model)))
		}
		lines = append(lines,
			"",
			fmt.Sprintf("To pin a non-default model, add `--model <model>`, for example: `cxp model-profile setup %s --provider %s --model %s --api-key-stdin --set-default`", shellQuoteForTeams(name), shellQuoteForTeams(spec.ID), shellQuoteForTeams(models[len(models)-1].PublicID())),
			fmt.Sprintf("Teams key intake also supports it: `model setup %s %s --model %s --teams-key-intake --set-default`", shellQuoteForTeams(spec.ID), shellQuoteForTeams(name), shellQuoteForTeams(models[len(models)-1].PublicID())),
		)
	}
	_ = ctx
	return strings.Join(lines, "\n"), nil
}

func (m teamsModelProfileManager) SetupModelProfile(ctx context.Context, req teams.ModelProfileSetupRequest) (teams.ModelProfileSetupResult, error) {
	choice, err := modelprofile.MustLookupModelChoice(req.Model)
	if err != nil {
		return teams.ModelProfileSetupResult{}, err
	}
	if !choice.RequiresAPIKey {
		store, err := m.store()
		if err != nil {
			return teams.ModelProfileSetupResult{}, err
		}
		if err := store.Update(func(cfg *config.Config) error {
			cfg.DefaultModelProfile = ""
			return nil
		}); err != nil {
			return teams.ModelProfileSetupResult{}, err
		}
		_ = ctx
		return teams.ModelProfileSetupResult{
			ProfileName: config.DefaultModelProfileName,
			Provider:    modelprofile.DefaultProvider,
			Model:       modelprofile.DefaultProvider,
			DisplayName: choice.DisplayName,
			SetDefault:  true,
		}, nil
	}
	store, err := m.store()
	if err != nil {
		return teams.ModelProfileSetupResult{}, err
	}
	cfg, err := store.Load()
	if err != nil {
		return teams.ModelProfileSetupResult{}, err
	}
	sshProxy := strings.TrimSpace(req.SSHProxy)
	if strings.EqualFold(sshProxy, "none") {
		sshProxy = ""
	}
	if sshProxy != "" {
		if _, ok := cfg.FindProfile(sshProxy); !ok {
			return teams.ModelProfileSetupResult{}, fmt.Errorf("ssh proxy profile %q not found", sshProxy)
		}
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	apiKeyRef := reusableAPIKeyRef(cfg, secretStore, choice)
	if apiKeyRef == "" {
		return teams.ModelProfileSetupResult{
			ProfileName:     choice.RecommendedProfile,
			Provider:        choice.ProviderID,
			Model:           choice.PublicModel,
			DisplayName:     choice.DisplayName,
			NeedsAPIKey:     true,
			CredentialScope: choice.CredentialScope,
			SetDefault:      req.SetDefault,
		}, nil
	}
	profileName := choice.RecommendedProfile
	existing, existed := cfg.FindModelProfile(profileName)
	if sshProxy == "" && existed {
		sshProxy = existing.SSHProxy
	}
	now := time.Now().UTC()
	revision := existing.Revision
	if revision <= 0 {
		revision = 1
	} else if modelProfileSetupChanges(existing, choice.ProviderID, choice.PublicModel, apiKeyRef, sshProxy) {
		revision++
	}
	createdAt := existing.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	profile := config.ModelProfile{
		Provider:  choice.ProviderID,
		Model:     choice.PublicModel,
		APIKeyRef: apiKeyRef,
		SSHProxy:  sshProxy,
		Revision:  revision,
		CreatedAt: createdAt,
		UpdatedAt: now,
	}
	if err := store.Update(func(cfg *config.Config) error {
		cfg.UpsertModelProfile(profileName, profile)
		if req.SetDefault {
			cfg.DefaultModelProfile = profileName
		}
		return nil
	}); err != nil {
		return teams.ModelProfileSetupResult{}, err
	}
	_ = ctx
	return teams.ModelProfileSetupResult{
		ProfileName:     profileName,
		Provider:        choice.ProviderID,
		Model:           choice.PublicModel,
		DisplayName:     choice.DisplayName,
		APIKeyRef:       apiKeyRef,
		ReusedAPIKey:    true,
		CredentialScope: choice.CredentialScope,
		SetDefault:      req.SetDefault,
	}, nil
}

func (m teamsModelProfileManager) ModelProfileDoctor(ctx context.Context, name string) (string, error) {
	store, err := m.store()
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	if err := runModelDoctor(&out, store, strings.TrimSpace(name)); err != nil {
		return "", err
	}
	_ = ctx
	return strings.TrimSpace(out.String()), nil
}

func (m teamsModelProfileManager) SetDefaultModelProfile(ctx context.Context, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("model is required")
	}
	if choice, ok := modelprofile.LookupModelChoice(name); ok {
		result, err := m.SetupModelProfile(ctx, teams.ModelProfileSetupRequest{Model: choice.ID, SetDefault: true})
		if err != nil {
			return "", err
		}
		if result.NeedsAPIKey {
			return "", fmt.Errorf("%s is not configured yet; run `model setup %s` first", choice.ID, choice.ID)
		}
		return fmt.Sprintf("Default model for future Work chats: %s\n\nExisting Work chats keep their pinned model.", result.DisplayName), nil
	}
	store, err := m.store()
	if err != nil {
		return "", err
	}
	var canonical string
	if err := store.Update(func(cfg *config.Config) error {
		if _, err := modelprofile.Resolve(*cfg, name); err != nil {
			return err
		}
		if strings.EqualFold(name, config.DefaultModelProfileName) {
			cfg.DefaultModelProfile = ""
			canonical = config.DefaultModelProfileName
			return nil
		}
		found, _, ok := findModelProfileForCLI(*cfg, name)
		if !ok {
			return fmt.Errorf("model profile %q not found", name)
		}
		cfg.DefaultModelProfile = found
		canonical = found
		return nil
	}); err != nil {
		return "", err
	}
	_ = ctx
	return fmt.Sprintf("Default model profile for future Work chats: %s\n\nExisting Work chats keep their pinned profile.", canonical), nil
}

func (m teamsModelProfileManager) DeleteModelProfile(ctx context.Context, name string, confirm bool) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("model profile name is required")
	}
	if !confirm {
		return fmt.Sprintf("This only deletes the profile config, not Teams chats. To confirm, send `model delete %s --confirm`.", name), nil
	}
	if strings.EqualFold(name, config.DefaultModelProfileName) {
		return "", fmt.Errorf("default model profile is built in and cannot be deleted")
	}
	store, err := m.store()
	if err != nil {
		return "", err
	}
	removed := false
	if err := store.Update(func(cfg *config.Config) error {
		removed = cfg.RemoveModelProfile(name)
		return nil
	}); err != nil {
		return "", err
	}
	if !removed {
		return "", fmt.Errorf("model profile %q not found", name)
	}
	_ = ctx
	return fmt.Sprintf("Deleted model profile %q. Existing Work chats that were pinned to it may no longer launch until they are forked or recreated.", name), nil
}

func (m teamsModelProfileManager) SaveModelProfileAPIKey(ctx context.Context, req teams.ModelProfileAPIKeySaveRequest) (teams.ModelProfileAPIKeySaveResult, error) {
	name := strings.TrimSpace(req.ProfileName)
	if name == "" {
		return teams.ModelProfileAPIKeySaveResult{}, fmt.Errorf("model profile name is required")
	}
	if strings.EqualFold(name, config.DefaultModelProfileName) {
		return teams.ModelProfileAPIKeySaveResult{}, fmt.Errorf("the built-in default model profile cannot store a third-party API key")
	}
	spec, err := modelprofile.MustLookupProvider(req.Provider)
	if err != nil {
		return teams.ModelProfileAPIKeySaveResult{}, err
	}
	if spec.ID == modelprofile.DefaultProvider || !spec.UsesAdapter {
		return teams.ModelProfileAPIKeySaveResult{}, fmt.Errorf("provider %q does not require a third-party API key", spec.ID)
	}
	apiKey := strings.TrimSpace(req.APIKey)
	if apiKey == "" {
		return teams.ModelProfileAPIKeySaveResult{}, fmt.Errorf("API key is empty")
	}
	store, err := m.store()
	if err != nil {
		return teams.ModelProfileAPIKeySaveResult{}, err
	}
	cfg, err := store.Load()
	if err != nil {
		return teams.ModelProfileAPIKeySaveResult{}, err
	}
	if canonical, _, ok := findModelProfileForCLI(cfg, name); ok {
		name = canonical
	}
	existing, existed := cfg.FindModelProfile(name)
	modelRef := strings.TrimSpace(req.Model)
	if modelRef == "" && existed && strings.EqualFold(existing.Provider, spec.ID) && strings.TrimSpace(existing.Model) != "" {
		modelRef = existing.Model
	}
	selectedModel, err := spec.MustResolveModel(modelRef)
	if err != nil {
		return teams.ModelProfileAPIKeySaveResult{}, err
	}
	modelID := selectedModel.PublicID()
	sshProxy := strings.TrimSpace(req.SSHProxy)
	if strings.EqualFold(sshProxy, "none") {
		sshProxy = ""
	}
	if sshProxy != "" {
		if _, ok := cfg.FindProfile(sshProxy); !ok {
			return teams.ModelProfileAPIKeySaveResult{}, fmt.Errorf("ssh proxy profile %q not found", sshProxy)
		}
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	apiKeyRef := modelprofile.SecretRefForProfile(name)
	if strings.TrimSpace(req.CredentialScope) != "" {
		apiKeyRef = modelprofile.SecretRefForCredentialScope(req.CredentialScope)
	}
	oldKey, oldKeyFound, err := secretStore.Get(apiKeyRef)
	if err != nil {
		return teams.ModelProfileAPIKeySaveResult{}, err
	}
	changed := !existed ||
		modelProfileSetupChanges(existing, spec.ID, modelID, apiKeyRef, sshProxy) ||
		!oldKeyFound ||
		strings.TrimSpace(oldKey) != apiKey
	revision := existing.Revision
	if revision <= 0 {
		revision = 1
	}
	if existed && changed {
		revision++
	}
	now := time.Now().UTC()
	createdAt := existing.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	if err := secretStore.Put(apiKeyRef, apiKey); err != nil {
		return teams.ModelProfileAPIKeySaveResult{}, err
	}
	profile := config.ModelProfile{
		Provider:  spec.ID,
		Model:     modelID,
		APIKeyRef: apiKeyRef,
		SSHProxy:  sshProxy,
		Revision:  revision,
		CreatedAt: createdAt,
		UpdatedAt: now,
	}
	if err := store.Update(func(cfg *config.Config) error {
		cfg.UpsertModelProfile(name, profile)
		if req.SetDefault {
			cfg.DefaultModelProfile = name
		}
		return nil
	}); err != nil {
		return teams.ModelProfileAPIKeySaveResult{}, err
	}
	_ = ctx
	return teams.ModelProfileAPIKeySaveResult{
		ProfileName: name,
		Provider:    spec.ID,
		Model:       modelID,
		APIKeyRef:   apiKeyRef,
		Fingerprint: modelprofile.Fingerprint(apiKey),
		Revision:    revision,
		SetDefault:  req.SetDefault,
	}, nil
}

func parseTeamsModelSetupGuideArg(arg string) (string, string) {
	fields := strings.Fields(strings.TrimSpace(arg))
	var provider, name string
	var positional []string
	providerWasFlag := false
	for i := 0; i < len(fields); i++ {
		word := strings.TrimSpace(fields[i])
		lower := strings.ToLower(word)
		switch {
		case lower == "--provider" && i+1 < len(fields):
			i++
			provider = fields[i]
			providerWasFlag = true
		case strings.HasPrefix(lower, "--provider="):
			provider = word[len("--provider="):]
			providerWasFlag = true
		case (lower == "--model" || lower == "--ssh-proxy") && i+1 < len(fields):
			i++
		case strings.HasPrefix(lower, "--model="), strings.HasPrefix(lower, "--ssh-proxy="):
		case strings.HasPrefix(lower, "-"):
		default:
			positional = append(positional, word)
		}
	}
	if providerWasFlag {
		if len(positional) > 0 {
			name = positional[0]
		}
	} else {
		if len(positional) > 0 {
			provider = positional[0]
		}
		if len(positional) > 1 {
			name = positional[1]
		}
	}
	return strings.TrimSpace(provider), strings.TrimSpace(name)
}

func splitWords2(s string) (string, string) {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) == 0 {
		return "", ""
	}
	if len(fields) == 1 {
		return fields[0], ""
	}
	return fields[0], strings.Join(fields[1:], " ")
}

func shellQuoteForTeams(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return `""`
	}
	if strings.ContainsAny(s, " \t'\"`$\\") {
		return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
	}
	return s
}
