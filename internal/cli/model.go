package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

type modelSetupOptions struct {
	apiKeyEnv   string
	apiKeyStdin bool
	sshProxy    string
	noDefault   bool
	noDoctor    bool
}

func newModelCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "model",
		Short: "Choose the Codex model CXP should use",
	}
	cmd.AddCommand(
		newModelSetupCmd(root),
		newModelUseCmd(root),
		newModelListCmd(root),
		newModelDoctorCmd(root),
		newModelCatalogCmd(root),
		newModelProviderCmd(root),
	)
	return cmd
}

func newModelSetupCmd(root *rootOptions) *cobra.Command {
	opts := modelSetupOptions{}
	cmd := &cobra.Command{
		Use:   "setup [model]",
		Short: "Configure model availability without changing chat or global selection",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			modelRef := ""
			if len(args) == 1 {
				modelRef = args[0]
			}
			return runModelSetup(cmd, root, modelRef, opts)
		},
	}
	cmd.Flags().StringVar(&opts.apiKeyEnv, "api-key-env", "", "Environment variable containing the provider API key")
	cmd.Flags().BoolVar(&opts.apiKeyStdin, "api-key-stdin", false, "Read the provider API key from stdin and save it to the local secret store")
	cmd.Flags().StringVar(&opts.sshProxy, "ssh-proxy", "", "SSH proxy profile id or name to use for this model")
	cmd.Flags().BoolVar(&opts.noDefault, "no-default", false, "Deprecated: setup never changes the default")
	cmd.Flags().BoolVar(&opts.noDoctor, "no-doctor", false, "Skip static profile validation after saving")
	return cmd
}

func newModelUseCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "use <model>",
		Short: "Make a configured model the default for future Codex launches",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runModelUse(cmd, root, args[0])
		},
	}
}

func newModelListCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List available models and setup status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			cfg, err := store.Load()
			if err != nil {
				return err
			}
			secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
			printModelChoices(cmd.OutOrStdout(), cfg, secretStore)
			return nil
		},
	}
}

func newModelDoctorCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor [model]",
		Short: "Validate the profile backing a model",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref := ""
			if len(args) == 1 {
				ref = args[0]
			}
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			return runModelDoctor(cmd.OutOrStdout(), store, ref)
		},
	}
}

func runModelSetup(cmd *cobra.Command, root *rootOptions, modelRef string, opts modelSetupOptions) error {
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	if strings.Contains(strings.TrimSpace(modelRef), "/") {
		if profile, ok := cfg.FindModelProfile(modelRef); ok {
			if mapKeyFold(cfg.ModelCatalogs, profile.Source) != "" {
				providerOpts := modelProviderSetupOptions{apiKeyEnv: opts.apiKeyEnv, apiKeyStdin: opts.apiKeyStdin, timeout: 20 * time.Second}
				return runModelProviderSetup(cmd, root, profile.Provider, providerOpts)
			}
		}
		return fmt.Errorf("catalog selector %q is not configured; add or sync a model catalog first", strings.TrimSpace(modelRef))
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	choice, err := modelChoiceForCLI(cmd, modelRef, cfg, secretStore)
	if err != nil {
		return err
	}
	if !choice.RequiresAPIKey {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Model available: Codex Official")
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Current chats and global defaults are unchanged.")
		return nil
	}
	profileName := choice.RecommendedProfile
	existing, existed := cfg.FindModelProfile(profileName)
	sshProxy := strings.TrimSpace(opts.sshProxy)
	if sshProxy != "" {
		if _, ok := cfg.FindProfile(sshProxy); !ok {
			return fmt.Errorf("ssh proxy profile %q not found", sshProxy)
		}
	} else if existed {
		sshProxy = existing.SSHProxy
	}
	apiKeyRef, err := modelAPIKeyRefForSetup(cmd, cfg, secretStore, choice, existing, opts)
	if err != nil {
		return err
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
	// Availability and selection are separate operations. `model setup` never
	// changes a global default; use `model use` explicitly after verification.
	var verifyErr error
	if !currentVerification {
		apiKey, resolveErr := modelprofile.ResolveAPIKey(apiKeyRef, secretStore, os.Getenv)
		if resolveErr != nil {
			return resolveErr
		}
		verifyErr = verifyAndStampTeamsModelProfile(cmd.Context(), &cfg, profileName, apiKey)
	}
	if err := store.Save(cfg); err != nil {
		return err
	}
	if verifyErr != nil {
		return fmt.Errorf("model %s authentication verification failed and remains hidden: %w", choice.ID, verifyErr)
	}
	action := "Saved"
	if existed {
		action = "Updated"
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s model %s as profile %q (api_key=%s)\n", action, choice.DisplayName, profileName, modelprofile.MaskRef(apiKeyRef))
	if !opts.noDoctor {
		return runModelProfileDoctor(cmd.OutOrStdout(), store, profileName)
	}
	return nil
}

func runModelUse(cmd *cobra.Command, root *rootOptions, modelRef string) error {
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	// External catalogs use stable provider/model selectors. They are already
	// materialized as named profiles, so switching one must not fall through to
	// the built-in static model table (which would silently select another
	// family).
	if profile, ok := cfg.FindModelProfile(modelRef); ok && !strings.EqualFold(profile.Provider, modelprofile.DefaultProvider) {
		if mapKeyFold(cfg.ModelCatalogs, profile.Source) != "" {
			profileRef := canonicalConfiguredProfileName(cfg, modelRef)
			if profileRef == "" {
				profileRef = strings.TrimSpace(modelRef)
			}
			secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
			if !modelProfileVerificationCurrent(cfg, profileRef, profile, secrets) {
				return fmt.Errorf("model %s is not authentication-verified; run `cxp model provider setup %s --api-key-stdin`", modelRef, profile.Provider)
			}
			cfg.SetDefaultModelProfile(profileRef)
			if err := store.Save(cfg); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Default model: %s (provider %s)\n", profileRef, profile.Provider)
			return nil
		}
	}
	choice, err := modelprofile.MustLookupModelChoice(modelRef)
	if err != nil {
		return err
	}
	if !choice.RequiresAPIKey {
		if err := store.Update(func(cfg *config.Config) error {
			cfg.SetDefaultModelProfile("")
			return nil
		}); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Default model: Codex Official")
		return nil
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	profileName := verifiedModelChoiceProfileName(cfg, secretStore, choice)
	if profileName == "" {
		return fmt.Errorf("%s is not authentication-verified; run `cxp model setup %s --api-key-stdin`", choice.ID, choice.ID)
	}
	cfg.SetDefaultModelProfile(profileName)
	if err := store.Save(cfg); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Default model: %s\n", choice.DisplayName)
	return nil
}

func verifiedModelChoiceProfileName(cfg config.Config, secrets *modelprofile.SecretStore, choice modelprofile.ModelChoice) string {
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
		if err == nil && resolvedModelChoiceMatches(resolved, choice) {
			return name
		}
	}
	return ""
}

func canonicalConfiguredProfileName(cfg config.Config, ref string) string {
	for name := range cfg.ModelProfiles {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(ref)) {
			return name
		}
	}
	return ""
}

func modelChoiceForCLI(cmd *cobra.Command, modelRef string, cfg config.Config, secretStore *modelprofile.SecretStore) (modelprofile.ModelChoice, error) {
	modelRef = strings.TrimSpace(modelRef)
	if modelRef != "" {
		return modelprofile.MustLookupModelChoice(modelRef)
	}
	printModelChoices(cmd.OutOrStdout(), cfg, secretStore)
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return modelprofile.ModelChoice{}, fmt.Errorf("model is required in non-interactive mode")
	}
	reader := bufio.NewReader(cmd.InOrStdin())
	answer := prompt(reader, "Model", modelprofile.DefaultProvider)
	if index, ok := parseModelChoiceIndex(answer); ok {
		choices := modelprofile.ModelChoices()
		if index < 1 || index > len(choices) {
			return modelprofile.ModelChoice{}, fmt.Errorf("model choice %d is out of range", index)
		}
		return choices[index-1], nil
	}
	return modelprofile.MustLookupModelChoice(answer)
}

func modelAPIKeyRefForSetup(cmd *cobra.Command, cfg config.Config, secretStore *modelprofile.SecretStore, choice modelprofile.ModelChoice, existing config.ModelProfile, opts modelSetupOptions) (string, error) {
	switch {
	case strings.TrimSpace(opts.apiKeyEnv) != "":
		return modelprofile.EnvRefPrefix + strings.TrimSpace(opts.apiKeyEnv), nil
	case opts.apiKeyStdin:
		key, err := readModelProfileAPIKey(cmd.InOrStdin())
		if err != nil {
			return "", err
		}
		ref := modelprofile.SecretRefForCredentialScope(choice.CredentialScope)
		if err := secretStore.Put(ref, key); err != nil {
			return "", err
		}
		return ref, nil
	}
	if ref := reusableAPIKeyRef(cfg, secretStore, choice); ref != "" {
		return ref, nil
	}
	if strings.TrimSpace(existing.APIKeyRef) != "" {
		return existing.APIKeyRef, nil
	}
	if term.IsTerminal(int(os.Stdin.Fd())) {
		key, err := readModelProfileAPIKeyFromTerminal()
		if err != nil {
			return "", err
		}
		ref := modelprofile.SecretRefForCredentialScope(choice.CredentialScope)
		if err := secretStore.Put(ref, key); err != nil {
			return "", err
		}
		return ref, nil
	}
	return "", fmt.Errorf("%s requires an API key; pass --api-key-env or --api-key-stdin", choice.ID)
}

func reusableAPIKeyRef(cfg config.Config, secretStore *modelprofile.SecretStore, choice modelprofile.ModelChoice) string {
	if strings.TrimSpace(choice.CredentialScope) != "" && secretStore != nil {
		ref := modelprofile.SecretRefForCredentialScope(choice.CredentialScope)
		if _, ok, err := secretStore.Get(ref); err == nil && ok {
			return ref
		}
	}
	if existing, ok := cfg.FindModelProfile(choice.RecommendedProfile); ok && strings.TrimSpace(existing.APIKeyRef) != "" {
		return existing.APIKeyRef
	}
	names := make([]string, 0, len(cfg.ModelProfiles))
	for name := range cfg.ModelProfiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		profile := cfg.ModelProfiles[name]
		if !strings.EqualFold(profile.Provider, choice.ProviderID) {
			continue
		}
		if strings.TrimSpace(profile.APIKeyRef) != "" {
			return profile.APIKeyRef
		}
	}
	return ""
}

func runModelDoctor(out io.Writer, store *config.Store, ref string) error {
	ref = strings.TrimSpace(ref)
	choice, ok := modelprofile.LookupModelChoice(ref)
	if !ok {
		return runModelProfileDoctor(out, store, ref)
	}
	profileName := strings.TrimSpace(choice.RecommendedProfile)
	if profileName == "" {
		profileName = config.DefaultModelProfileName
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	if readyProfileName := modelChoiceReadyProfileName(cfg, choice); readyProfileName != "" {
		return runModelProfileDoctor(out, store, readyProfileName)
	}
	if !choice.RequiresAPIKey {
		return runModelProfileDoctor(out, store, profileName)
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	if ref := reusableAPIKeyRef(cfg, secretStore, choice); ref != "" {
		return fmt.Errorf("model %s has a saved %s API key (%s), but profile %q is not configured for %s yet; run `model setup %s`, then retry `model doctor %s`",
			choice.ID, choice.ProviderDisplayName, modelprofile.MaskRef(ref), profileName, choice.DisplayName, choice.ID, choice.ID)
	}
	return fmt.Errorf("model %s is not configured yet; run `model setup %s` to configure %s", choice.ID, choice.ID, choice.DisplayName)
}

func modelChoiceReadyProfileName(cfg config.Config, choice modelprofile.ModelChoice) string {
	if !choice.RequiresAPIKey {
		return config.DefaultModelProfileName
	}
	candidates := []string{}
	seen := map[string]bool{}
	addCandidate := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		key := strings.ToLower(name)
		if seen[key] {
			return
		}
		seen[key] = true
		candidates = append(candidates, name)
	}
	addCandidate(choice.RecommendedProfile)
	addCandidate(cfg.EffectiveDefaultModelProfile())
	names := make([]string, 0, len(cfg.ModelProfiles))
	for name := range cfg.ModelProfiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		addCandidate(name)
	}
	for _, name := range candidates {
		resolved, err := modelprofile.Resolve(cfg, name)
		if err != nil {
			continue
		}
		if resolvedModelChoiceMatches(resolved, choice) {
			return resolved.Name
		}
	}
	return ""
}

func resolvedModelChoiceMatches(resolved modelprofile.Resolved, choice modelprofile.ModelChoice) bool {
	if !choice.RequiresAPIKey {
		return strings.EqualFold(resolved.Provider.ID, modelprofile.DefaultProvider)
	}
	return strings.EqualFold(resolved.Provider.ID, choice.ProviderID) &&
		strings.EqualFold(resolved.SelectedPublicModel(), choice.PublicModel) &&
		strings.TrimSpace(resolved.Profile.APIKeyRef) != ""
}

func modelChoiceStatus(cfg config.Config, secretStore *modelprofile.SecretStore, choice modelprofile.ModelChoice) string {
	if !choice.RequiresAPIKey {
		return "ready"
	}
	if readyProfileName := modelChoiceReadyProfileName(cfg, choice); readyProfileName != "" {
		if strings.EqualFold(readyProfileName, strings.TrimSpace(choice.RecommendedProfile)) {
			return "ready"
		}
		return "ready (profile " + readyProfileName + ")"
	}
	if ref := reusableAPIKeyRef(cfg, secretStore, choice); ref != "" {
		return "needs setup (key " + modelprofile.MaskRef(ref) + ")"
	}
	return "needs key"
}

func printModelChoices(out io.Writer, cfg config.Config, secretStore *modelprofile.SecretStore) {
	if out == nil {
		return
	}
	defaultName := cfg.EffectiveDefaultModelProfile()
	_, _ = fmt.Fprintln(out, "Models (official and configured catalog selectors)")
	_, _ = fmt.Fprintln(out, "[global default] marks the model used by future launches; it does not describe the current chat")
	_, _ = fmt.Fprintln(out, "Catalog provider setup verifies all models under one provider")
	for i, choice := range modelprofile.ModelChoices() {
		status := modelChoiceStatus(cfg, secretStore, choice)
		marker := "-"
		if modelChoiceIsDefault(cfg, choice, defaultName) {
			marker = "[global default]"
		}
		_, _ = fmt.Fprintf(out, "%s %d. %-20s %-22s %s\n", marker, i+1, choice.ID, choice.DisplayName, status)
	}
	verified := make([]string, 0)
	catalogProfiles := make([]string, 0)
	for name, profile := range cfg.ModelProfiles {
		if mapKeyFold(cfg.ModelCatalogs, profile.Source) != "" {
			catalogProfiles = append(catalogProfiles, name)
			continue
		}
		if strings.TrimSpace(profile.Source) != "" && strings.TrimSpace(profile.VerificationFingerprint) != "" {
			verified = append(verified, name)
		}
	}
	sort.Strings(catalogProfiles)
	if len(catalogProfiles) > 0 {
		_, _ = fmt.Fprintln(out, "Catalog models (selector is provider/model)")
		for _, name := range catalogProfiles {
			profile := cfg.ModelProfiles[name]
			marker := "-"
			if strings.EqualFold(name, defaultName) || strings.EqualFold("profile:"+name, cfg.EffectiveDefaultModelSelector()) {
				marker = "[global default]"
			}
			status := "needs provider setup"
			if strings.TrimSpace(profile.VerificationFingerprint) != "" && modelProfileVerificationCurrent(cfg, name, profile, secretStore) {
				status = "ready"
			}
			label := name
			if resolved, err := modelprofile.Resolve(cfg, name); err == nil {
				label = resolved.Model.Label()
				label += " [" + modelRouteSummary(resolved) + "]"
			}
			_, _ = fmt.Fprintf(out, "%s %-30s %-22s catalog %s (%s)\n", marker, name, label, profile.Source, status)
		}
	}
	sort.Strings(verified)
	if len(verified) > 0 {
		_, _ = fmt.Fprintln(out, "Verified repository models")
		for _, name := range verified {
			resolved, err := modelprofile.Resolve(cfg, name)
			if err != nil {
				continue
			}
			marker := "-"
			if strings.EqualFold(name, defaultName) {
				marker = "[global default]"
			}
			_, _ = fmt.Fprintf(out, "%s %-20s %-22s verified (model `%s`, selector `profile:%s`, source %s)\n", marker, name, resolved.Model.Label(), resolved.Model.PublicID(), name, cfg.ModelProfiles[name].Source)
		}
	}
}

func modelRouteSummary(resolved modelprofile.Resolved) string {
	interfaceName := strings.TrimSpace(resolved.Provider.DefaultInterface)
	if interfaceName == "" {
		interfaceName = "default"
	}
	adapter := strings.TrimSpace(resolved.Provider.AdapterProfile)
	if adapter == "" {
		adapter = "generic"
	}
	parts := []string{"interface=" + interfaceName, "adapter=" + adapter}
	featureNames := make([]string, 0, len(resolved.Model.Features))
	for name := range resolved.Model.Features {
		featureNames = append(featureNames, name)
	}
	sort.Strings(featureNames)
	for _, name := range featureNames {
		feature := resolved.Model.Features[name]
		value := feature.Support
		if feature.Interface != "" {
			value += "@" + feature.Interface
		}
		if feature.NativeTool != nil {
			value += ":tool=" + feature.NativeTool.UpstreamType
		}
		if feature.Operation != "" {
			value += ":op=" + feature.Operation
		}
		if feature.Sources != nil {
			mode := strings.TrimSpace(feature.Sources.Mode)
			if mode == "" {
				mode = "default"
			}
			value += ":sources=" + mode
			if feature.Sources.RequireURL || feature.Sources.RequireSources || feature.RequireSources {
				value += "+required"
			}
		}
		parts = append(parts, name+"="+value)
	}
	return strings.Join(parts, ",")
}

func modelChoiceIsDefault(cfg config.Config, choice modelprofile.ModelChoice, defaultName string) bool {
	if strings.EqualFold(choice.RecommendedProfile, defaultName) {
		return true
	}
	profile, ok := cfg.FindModelProfile(defaultName)
	if !ok {
		return false
	}
	if !strings.EqualFold(profile.Provider, choice.ProviderID) {
		return false
	}
	if !choice.RequiresAPIKey {
		return strings.EqualFold(choice.ProviderID, modelprofile.DefaultProvider)
	}
	spec, ok := modelprofile.LookupProvider(profile.Provider)
	if !ok {
		return false
	}
	model, ok := spec.ResolveModel(profile.Model)
	if !ok {
		return false
	}
	return strings.EqualFold(model.PublicID(), choice.PublicModel)
}

func parseModelChoiceIndex(s string) (int, bool) {
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}
