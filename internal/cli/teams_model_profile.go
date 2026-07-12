package cli

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

type teamsModelProfileManager struct {
	root            *rootOptions
	codexPath       string
	runtimeResolver teamsCodexRuntimeResolver
}

var listTeamsOfficialModelsFn = listTeamsOfficialModels

var teamsOfficialModelCache = struct {
	sync.Mutex
	byPath map[string]teamsOfficialModelCacheEntry
}{byPath: map[string]teamsOfficialModelCacheEntry{}}

type teamsOfficialModelCacheEntry struct {
	models []teamsOfficialModel
	at     time.Time
}

const teamsOfficialModelCacheTTL = 30 * time.Minute

func invalidateTeamsOfficialModelCache() {
	teamsOfficialModelCache.Lock()
	teamsOfficialModelCache.byPath = map[string]teamsOfficialModelCacheEntry{}
	teamsOfficialModelCache.Unlock()
}

func verifyAndStampTeamsModelProfile(ctx context.Context, cfg *config.Config, name string, apiKey string) error {
	resolved, err := modelprofile.Resolve(*cfg, name)
	if err != nil {
		return err
	}
	if err := verifyConfiguredModelAuthenticationFn(ctx, resolved, apiKey); err != nil {
		profile := cfg.ModelProfiles[name]
		profile.VerificationFingerprint = ""
		profile.VerifiedAt = time.Time{}
		profile.VerificationError = compactVerificationError(err, apiKey)
		cfg.ModelProfiles[name] = profile
		return err
	}
	profile := cfg.ModelProfiles[name]
	profile.VerifiedAt = time.Now().UTC()
	profile.VerificationError = ""
	profile.VerificationFingerprint = modelVerificationFingerprint("", resolved, apiKey)
	cfg.ModelProfiles[name] = profile
	return nil
}

func newTeamsModelProfileManager(root *rootOptions, codexPath ...string) teamsModelProfileManager {
	path := "codex"
	if len(codexPath) > 0 && strings.TrimSpace(codexPath[0]) != "" {
		path = strings.TrimSpace(codexPath[0])
	}
	return teamsModelProfileManager{root: root, codexPath: path}
}

func newTeamsModelProfileManagerWithRuntime(root *rootOptions, resolver teamsCodexRuntimeResolver) teamsModelProfileManager {
	return teamsModelProfileManager{root: root, runtimeResolver: resolver}
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
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	probePath := m.codexPath
	var probeEnv []string
	if m.runtimeResolver != nil {
		contract, runtimeErr := m.runtimeResolver(ctx)
		if runtimeErr != nil {
			return printTeamsVerifiedModels(cfg, secretStore, false, nil, fmt.Errorf("official Codex runtime unavailable: %w", runtimeErr)) + "\n\nUse `model setup` to discover or verify additional models; only authentication-verified models appear above.", nil
		}
		ctx = withCodexInvocation(ctx, codexInvocationForRuntime(contract))
		probePath = contract.Runtime.Command
		probeEnv = contract.Runtime.Environment
	}
	officialReady := codexLoginStatusProbeFn(ctx, probePath, probeEnv, nil)
	var officialModels []teamsOfficialModel
	var officialCatalogErr error
	if officialReady {
		officialModels, officialCatalogErr = listTeamsOfficialModelsFn(ctx, probePath)
	} else {
		officialCatalogErr = fmt.Errorf("Codex login probe did not succeed for the Teams target account; verify login, executable access, and CODEX_HOME")
	}
	return printTeamsVerifiedModels(cfg, secretStore, officialReady, officialModels, officialCatalogErr) + "\n\nUse `model setup` to discover or verify additional models; only authentication-verified models appear above.", nil
}

func (m teamsModelProfileManager) ModelProfileRuntimeWarning(_ context.Context, snapshot modelprofile.Snapshot) (string, bool, error) {
	name := strings.TrimSpace(snapshot.Name)
	if name == "" || snapshot.IsDefault() {
		return "", false, nil
	}
	store, err := m.store()
	if err != nil {
		return "", false, err
	}
	cfg, err := store.Load()
	if err != nil {
		return "", false, err
	}
	profile, ok := cfg.FindModelProfile(name)
	if !ok || strings.TrimSpace(profile.Source) == "" {
		return "", false, nil
	}
	source, ok := findSource(cfg, profile.Source)
	if !ok || !source.BackupActive || strings.TrimSpace(source.Revision) == "" {
		return "", false, nil
	}
	lines := []string{
		"⚠️ **Backup JSON configuration is active.**",
		fmt.Sprintf("Profile `%s` is using last-known-good source `%s` revision `%s` because the latest JSON sync failed.", name, profile.Source, shortModelSourceRevision(source.Revision)),
	}
	if attempted := shortModelSourceRevision(source.BackupAttemptedRevision); attempted != "" {
		lines = append(lines, "Failed revision: `"+attempted+"`.")
	}
	if reason := strings.TrimSpace(source.BackupReason); reason != "" {
		lines = append(lines, "Reason: "+shortenModelProfileWarning(reason, 240))
	}
	lines = append(lines, "This warning is shown on every turn until a newer JSON revision syncs and verifies successfully.")
	return strings.Join(lines, "\n"), true, nil
}

func shortModelSourceRevision(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 12 {
		return value[:12]
	}
	return value
}

func shortenModelProfileWarning(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if limit > 0 && len(value) > limit {
		return value[:limit-1] + "…"
	}
	return value
}

type teamsOfficialModel struct {
	Slug                  string
	DisplayName           string
	IsDefault             bool
	DefaultReasoningLevel string
}

func listTeamsOfficialModels(ctx context.Context, codexPath string) ([]teamsOfficialModel, error) {
	cacheKey := strings.TrimSpace(codexPath)
	if invocation, ok := codexInvocationFromContext(ctx); ok && strings.TrimSpace(invocation.Fingerprint) != "" {
		cacheKey = invocation.Fingerprint
	}
	teamsOfficialModelCache.Lock()
	if cached, ok := teamsOfficialModelCache.byPath[cacheKey]; ok && time.Since(cached.at) < teamsOfficialModelCacheTTL {
		models := append([]teamsOfficialModel(nil), cached.models...)
		teamsOfficialModelCache.Unlock()
		return models, nil
	}
	teamsOfficialModelCache.Unlock()
	starter := codexrunner.AppServerTransportStarter(codexrunner.AppServerProcessStarter{})
	extraEnv := []string(nil)
	if invocation, ok := codexInvocationFromContext(ctx); ok {
		extraEnv = append([]string(nil), invocation.Environment...)
		starter = configureAppServerStarter{base: starter, configure: func(command *exec.Cmd) error {
			return configureCodexInvocationCommand(ctx, command)
		}}
	}
	runner := &codexrunner.AppServerRunner{
		Starter:       starter,
		Command:       strings.TrimSpace(codexPath),
		AppServerArgs: []string{"--analytics-default-enabled"},
		ExtraEnv:      extraEnv,
		Timeout:       10 * time.Second,
	}
	defer func() { _ = runner.Close() }()
	listed, err := runner.ListModels(ctx)
	if err != nil {
		return nil, fmt.Errorf("read current-account Codex model/list: %w", err)
	}
	models := make([]teamsOfficialModel, 0, len(listed))
	for _, listedModel := range listed {
		model := teamsOfficialModel{
			Slug:                  strings.TrimSpace(firstNonEmptyCLI(listedModel.Model, listedModel.ID)),
			DisplayName:           strings.TrimSpace(listedModel.DisplayName),
			IsDefault:             listedModel.IsDefault,
			DefaultReasoningLevel: strings.TrimSpace(listedModel.DefaultReasoningEffort),
		}
		if model.Slug == "" {
			continue
		}
		if model.DisplayName == "" {
			model.DisplayName = model.Slug
		}
		models = append(models, model)
	}
	if len(models) == 0 {
		return nil, fmt.Errorf("current-account Codex model/list contains no selectable models")
	}
	teamsOfficialModelCache.Lock()
	teamsOfficialModelCache.byPath[cacheKey] = teamsOfficialModelCacheEntry{models: append([]teamsOfficialModel(nil), models...), at: time.Now()}
	teamsOfficialModelCache.Unlock()
	return models, nil
}

func teamsOfficialDefaultModel(models []teamsOfficialModel) (teamsOfficialModel, int) {
	for index, model := range models {
		if model.IsDefault {
			return model, index
		}
	}
	if len(models) > 0 {
		return models[0], 0
	}
	return teamsOfficialModel{}, -1
}

func printTeamsVerifiedModels(cfg config.Config, secrets *modelprofile.SecretStore, officialReady bool, officialModels []teamsOfficialModel, officialCatalogErr error) string {
	lines := []string{"Verified models"}
	defaultName := cfg.EffectiveDefaultModelProfile()
	officialDefault, officialDefaultIndex := teamsOfficialDefaultModel(officialModels)
	if strings.EqualFold(defaultName, config.DefaultModelProfileName) && officialReady && officialDefaultIndex >= 0 {
		lines = append(lines, fmt.Sprintf("* current default: %s (`%s`)", officialDefault.DisplayName, officialDefault.Slug))
	} else if !strings.EqualFold(defaultName, config.DefaultModelProfileName) {
		if profile, ok := cfg.FindModelProfile(defaultName); ok && modelProfileVerificationCurrent(cfg, defaultName, profile, secrets) {
			if resolved, err := modelprofile.Resolve(cfg, defaultName); err == nil {
				lines = append(lines, fmt.Sprintf("* current default: %s (`%s`)", resolved.Model.Label(), defaultName))
			}
		}
	}
	if officialReady {
		if len(officialModels) > 0 {
			lines = append(lines, fmt.Sprintf("  official default: %s (`%s`)", officialDefault.DisplayName, officialDefault.Slug))
			lines = append(lines, "", "Official Codex models")
			for index, model := range officialModels {
				suffix := ""
				if index == officialDefaultIndex {
					suffix = " [official default]"
				}
				if model.DefaultReasoningLevel != "" {
					suffix += " effort=" + model.DefaultReasoningLevel
				}
				lines = append(lines, fmt.Sprintf("  - %s (`%s`)%s", model.DisplayName, model.Slug, suffix))
			}
		} else {
			lines = append(lines, fmt.Sprintf("  official default unavailable: %v", officialCatalogErr))
		}
	} else if officialCatalogErr != nil {
		lines = append(lines, fmt.Sprintf("  official catalog unavailable: %v", officialCatalogErr))
	}
	thirdPartyHeaderAdded := false
	names := make([]string, 0, len(cfg.ModelProfiles))
	for name := range cfg.ModelProfiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		profile := cfg.ModelProfiles[name]
		if !modelProfileVerificationCurrent(cfg, name, profile, secrets) {
			continue
		}
		resolved, err := modelprofile.Resolve(cfg, name)
		if err != nil || resolved.IsDefault() {
			continue
		}
		marker := " "
		if strings.EqualFold(defaultName, name) {
			marker = "*"
		}
		if !thirdPartyHeaderAdded {
			lines = append(lines, "", "Verified third-party profiles")
			thirdPartyHeaderAdded = true
		}
		lines = append(lines, fmt.Sprintf("%s %s: %s (%s)", marker, name, resolved.Model.Label(), resolved.Provider.DisplayName))
	}
	if len(lines) == 1 {
		lines = append(lines, "  none")
	}
	return strings.Join(lines, "\n")
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
	currentVerification := existed && !modelProfileSetupChanges(existing, choice.ProviderID, choice.PublicModel, apiKeyRef, sshProxy) && modelProfileVerificationCurrent(cfg, profileName, existing, secretStore)
	if currentVerification {
		profile.VerifiedAt = existing.VerifiedAt
		profile.VerificationFingerprint = existing.VerificationFingerprint
	}
	cfg.UpsertModelProfile(profileName, profile)
	if req.SetDefault {
		cfg.DefaultModelProfile = profileName
	}
	if !currentVerification {
		apiKey, resolveErr := modelprofile.ResolveAPIKey(apiKeyRef, secretStore, nil)
		if resolveErr != nil {
			return teams.ModelProfileSetupResult{}, resolveErr
		}
		verifyErr := verifyAndStampTeamsModelProfile(ctx, &cfg, profileName, apiKey)
		if saveErr := store.Save(cfg); saveErr != nil {
			return teams.ModelProfileSetupResult{}, saveErr
		}
		if verifyErr != nil {
			return teams.ModelProfileSetupResult{}, fmt.Errorf("model %s authentication verification failed and remains hidden: %w", choice.ID, verifyErr)
		}
	} else if err := store.Save(cfg); err != nil {
		return teams.ModelProfileSetupResult{}, err
	}
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
	currentVerification := existed && !changed && modelProfileVerificationCurrent(cfg, name, existing, secretStore)
	if currentVerification {
		profile.VerifiedAt = existing.VerifiedAt
		profile.VerificationFingerprint = existing.VerificationFingerprint
	}
	cfg.UpsertModelProfile(name, profile)
	if req.SetDefault {
		cfg.DefaultModelProfile = name
	}
	var verifyErr error
	if !currentVerification {
		verifyErr = verifyAndStampTeamsModelProfile(ctx, &cfg, name, apiKey)
	}
	if err := store.Save(cfg); err != nil {
		return teams.ModelProfileAPIKeySaveResult{}, err
	}
	if verifyErr != nil {
		return teams.ModelProfileAPIKeySaveResult{}, fmt.Errorf("model profile %q authentication verification failed and remains hidden: %w", name, verifyErr)
	}
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
