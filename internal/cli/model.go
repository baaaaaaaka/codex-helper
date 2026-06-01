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
	)
	return cmd
}

func newModelSetupCmd(root *rootOptions) *cobra.Command {
	opts := modelSetupOptions{}
	cmd := &cobra.Command{
		Use:   "setup [model]",
		Short: "Set up a model by choosing the model first",
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
	cmd.Flags().BoolVar(&opts.noDefault, "no-default", false, "Save the model without making it the default")
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
			if choice, ok := modelprofile.LookupModelChoice(ref); ok {
				ref = choice.RecommendedProfile
			}
			return runModelProfileDoctor(cmd.OutOrStdout(), store, ref)
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
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	choice, err := modelChoiceForCLI(cmd, modelRef, cfg, secretStore)
	if err != nil {
		return err
	}
	if !choice.RequiresAPIKey {
		if err := store.Update(func(cfg *config.Config) error {
			cfg.DefaultModelProfile = ""
			return nil
		}); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Default model: Codex Official")
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
	cfg.UpsertModelProfile(profileName, profile)
	if !opts.noDefault {
		cfg.DefaultModelProfile = profileName
	}
	if err := store.Save(cfg); err != nil {
		return err
	}
	action := "Saved"
	if existed {
		action = "Updated"
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s model %s as profile %q (api_key=%s)\n", action, choice.DisplayName, profileName, modelprofile.MaskRef(apiKeyRef))
	if !opts.noDefault {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Default model: %s\n", choice.DisplayName)
	}
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
	choice, err := modelprofile.MustLookupModelChoice(modelRef)
	if err != nil {
		return err
	}
	if !choice.RequiresAPIKey {
		if err := store.Update(func(cfg *config.Config) error {
			cfg.DefaultModelProfile = ""
			return nil
		}); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Default model: Codex Official")
		return nil
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	profileName := choice.RecommendedProfile
	existing, existed := cfg.FindModelProfile(profileName)
	apiKeyRef := strings.TrimSpace(existing.APIKeyRef)
	if apiKeyRef == "" {
		apiKeyRef = reusableAPIKeyRef(cfg, secretStore, choice)
	}
	if apiKeyRef == "" {
		return fmt.Errorf("%s is not configured yet; run `cxp model setup %s --api-key-stdin`", choice.ID, choice.ID)
	}
	if !existed ||
		strings.TrimSpace(existing.APIKeyRef) == "" ||
		!strings.EqualFold(strings.TrimSpace(existing.Provider), choice.ProviderID) ||
		!strings.EqualFold(strings.TrimSpace(existing.Model), choice.PublicModel) {
		now := time.Now().UTC()
		revision := existing.Revision
		if revision <= 0 {
			revision = 1
		} else if modelProfileSetupChanges(existing, choice.ProviderID, choice.PublicModel, apiKeyRef, existing.SSHProxy) {
			revision++
		}
		createdAt := existing.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}
		cfg.UpsertModelProfile(profileName, config.ModelProfile{
			Provider:  choice.ProviderID,
			Model:     choice.PublicModel,
			APIKeyRef: apiKeyRef,
			SSHProxy:  existing.SSHProxy,
			Revision:  revision,
			CreatedAt: createdAt,
			UpdatedAt: now,
		})
	}
	cfg.DefaultModelProfile = profileName
	if err := store.Save(cfg); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Default model: %s\n", choice.DisplayName)
	return nil
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

func printModelChoices(out io.Writer, cfg config.Config, secretStore *modelprofile.SecretStore) {
	if out == nil {
		return
	}
	defaultName := cfg.EffectiveDefaultModelProfile()
	_, _ = fmt.Fprintln(out, "Models")
	for i, choice := range modelprofile.ModelChoices() {
		status := "ready"
		if choice.RequiresAPIKey {
			status = "needs key"
			if ref := reusableAPIKeyRef(cfg, secretStore, choice); ref != "" {
				status = "uses key " + modelprofile.MaskRef(ref)
			}
		}
		marker := " "
		if modelChoiceIsDefault(cfg, choice, defaultName) {
			marker = "*"
		}
		_, _ = fmt.Fprintf(out, "%s %d. %-20s %-22s %s\n", marker, i+1, choice.ID, choice.DisplayName, status)
	}
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
