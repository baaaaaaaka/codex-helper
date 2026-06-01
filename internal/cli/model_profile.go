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

type modelProfileSetupOptions struct {
	provider    string
	model       string
	apiKeyEnv   string
	apiKeyStdin bool
	sshProxy    string
	setDefault  bool
	noDoctor    bool
}

func newModelProfileCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "model-profile",
		Short: "Manage model profiles for Codex launches",
	}
	cmd.AddCommand(
		newModelProfileSetupCmd(root),
		newModelProfileListCmd(root),
		newModelProfileDoctorCmd(root),
		newModelProfileDeleteCmd(root),
		newModelProfileSetDefaultCmd(root),
	)
	return cmd
}

func newModelProfileSetupCmd(root *rootOptions) *cobra.Command {
	opts := modelProfileSetupOptions{}
	cmd := &cobra.Command{
		Use:   "setup [name]",
		Short: "Create or update a model profile",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := ""
			if len(args) == 1 {
				name = args[0]
			}
			return runModelProfileSetup(cmd, root, name, opts)
		},
	}
	cmd.Flags().StringVar(&opts.provider, "provider", "", "Model provider: default, deepseek, mimo, kimi, glm, minimax, qwen")
	cmd.Flags().StringVar(&opts.model, "model", "", "Provider model to use by default, for example pro or deepseek/deepseek-v4-pro")
	cmd.Flags().StringVar(&opts.apiKeyEnv, "api-key-env", "", "Environment variable containing the provider API key")
	cmd.Flags().BoolVar(&opts.apiKeyStdin, "api-key-stdin", false, "Read the provider API key from stdin and save it to the local secret store")
	cmd.Flags().StringVar(&opts.sshProxy, "ssh-proxy", "", "SSH proxy profile id or name to use for this model profile")
	cmd.Flags().BoolVar(&opts.setDefault, "set-default", false, "Make this the default model profile")
	cmd.Flags().BoolVar(&opts.noDoctor, "no-doctor", false, "Skip static profile validation after saving")
	return cmd
}

func newModelProfileListCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List model profiles",
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
			printModelProfiles(cmd.OutOrStdout(), cfg)
			return nil
		},
	}
}

func newModelProfileDoctorCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor [name]",
		Short: "Validate a model profile",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := ""
			if len(args) == 1 {
				name = args[0]
			}
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			return runModelProfileDoctor(cmd.OutOrStdout(), store, name)
		},
	}
}

func newModelProfileDeleteCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a model profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.EqualFold(strings.TrimSpace(args[0]), config.DefaultModelProfileName) {
				return fmt.Errorf("default model profile is built in and cannot be deleted")
			}
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			removed := false
			if err := store.Update(func(cfg *config.Config) error {
				removed = cfg.RemoveModelProfile(args[0])
				return nil
			}); err != nil {
				return err
			}
			if !removed {
				return fmt.Errorf("model profile %q not found", args[0])
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Deleted model profile %q\n", args[0])
			return nil
		},
	}
}

func newModelProfileSetDefaultCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "set-default <name>",
		Short: "Set the default model profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			name := strings.TrimSpace(args[0])
			if err := store.Update(func(cfg *config.Config) error {
				if _, err := modelprofile.Resolve(*cfg, name); err != nil {
					return err
				}
				if strings.EqualFold(name, config.DefaultModelProfileName) {
					cfg.DefaultModelProfile = ""
				} else {
					canonical, _, ok := findModelProfileForCLI(*cfg, name)
					if !ok {
						return fmt.Errorf("model profile %q not found", name)
					}
					cfg.DefaultModelProfile = canonical
				}
				return nil
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Default model profile: %s\n", name)
			return nil
		},
	}
}

func runModelProfileSetup(cmd *cobra.Command, root *rootOptions, name string, opts modelProfileSetupOptions) error {
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}

	reader := bufio.NewReader(cmd.InOrStdin())
	name = strings.TrimSpace(name)
	if name == "" {
		name = prompt(reader, "Model profile name", config.DefaultModelProfileName)
	}
	if name == "" {
		return fmt.Errorf("model profile name is required")
	}
	provider := strings.TrimSpace(opts.provider)
	if provider == "" {
		def := config.DefaultModelProfileName
		if existing, ok := cfg.FindModelProfile(name); ok && strings.TrimSpace(existing.Provider) != "" {
			def = existing.Provider
		}
		provider = prompt(reader, "Provider", def)
	}
	spec, err := modelprofile.MustLookupProvider(provider)
	if err != nil {
		return err
	}
	if strings.EqualFold(name, config.DefaultModelProfileName) && spec.ID != modelprofile.DefaultProvider {
		return fmt.Errorf("the built-in default model profile must use provider %q", modelprofile.DefaultProvider)
	}
	existing, existed := cfg.FindModelProfile(name)
	modelRef := strings.TrimSpace(opts.model)
	if modelRef == "" && strings.TrimSpace(existing.Model) != "" && strings.EqualFold(existing.Provider, spec.ID) {
		modelRef = existing.Model
	}
	if modelRef == "" && spec.UsesAdapter && len(spec.ModelCatalog()) > 1 && term.IsTerminal(int(os.Stdin.Fd())) {
		_, _ = fmt.Fprintf(os.Stderr, "Available models for %s:\n", spec.ID)
		for _, model := range spec.ModelCatalog() {
			_, _ = fmt.Fprintf(os.Stderr, "- %s (%s)%s\n", model.PublicID(), model.Label(), modelAliasSummary(model))
		}
		modelRef = prompt(reader, "Model", spec.DefaultPublicModel())
	}
	selectedModel, err := spec.MustResolveModel(modelRef)
	if spec.UsesAdapter && err != nil {
		return err
	}
	modelID := ""
	if spec.UsesAdapter {
		modelID = selectedModel.PublicID()
	}

	apiKeyRef := ""
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	if spec.UsesAdapter {
		switch {
		case strings.TrimSpace(opts.apiKeyEnv) != "":
			apiKeyRef = modelprofile.EnvRefPrefix + strings.TrimSpace(opts.apiKeyEnv)
		case opts.apiKeyStdin:
			key, err := readModelProfileAPIKey(cmd.InOrStdin())
			if err != nil {
				return err
			}
			apiKeyRef = modelprofile.SecretRefForProfile(name)
			if err := secretStore.Put(apiKeyRef, key); err != nil {
				return err
			}
		case strings.TrimSpace(existing.APIKeyRef) != "":
			apiKeyRef = existing.APIKeyRef
		default:
			if term.IsTerminal(int(os.Stdin.Fd())) {
				key, err := readModelProfileAPIKeyFromTerminal()
				if err != nil {
					return err
				}
				apiKeyRef = modelprofile.SecretRefForProfile(name)
				if err := secretStore.Put(apiKeyRef, key); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("provider %q requires an API key; pass --api-key-env or --api-key-stdin", spec.ID)
			}
		}
	}

	sshProxy := strings.TrimSpace(opts.sshProxy)
	if sshProxy != "" {
		if _, ok := cfg.FindProfile(sshProxy); !ok {
			return fmt.Errorf("ssh proxy profile %q not found", sshProxy)
		}
	}

	now := time.Now().UTC()
	revision := existing.Revision
	if revision <= 0 {
		revision = 1
	} else if modelProfileSetupChanges(existing, spec.ID, modelID, apiKeyRef, sshProxy) {
		revision++
	}
	createdAt := existing.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	if apiKeyRef == "" {
		apiKeyRef = existing.APIKeyRef
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

	if strings.EqualFold(name, config.DefaultModelProfileName) {
		if opts.setDefault {
			cfg.DefaultModelProfile = ""
		}
	} else {
		cfg.UpsertModelProfile(name, profile)
		if opts.setDefault {
			cfg.DefaultModelProfile = name
		}
	}
	if err := store.Save(cfg); err != nil {
		return err
	}
	action := "Saved"
	if existed {
		action = "Updated"
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s model profile %q (provider=%s, model=%s, api_key=%s, ssh_proxy=%s, revision=%d)\n",
		action, name, spec.ID, emptyAsNone(profile.Model), modelprofile.MaskRef(profile.APIKeyRef), emptyAsNone(profile.SSHProxy), profile.Revision)
	if !opts.noDoctor {
		return runModelProfileDoctor(cmd.OutOrStdout(), store, name)
	}
	return nil
}

func runModelProfileDoctor(out io.Writer, store *config.Store, name string) error {
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	resolved, err := modelprofile.Resolve(cfg, name)
	if err != nil {
		return err
	}
	if out != nil {
		_, _ = fmt.Fprintf(out, "OK  model profile %q\n", resolved.Name)
		_, _ = fmt.Fprintf(out, "OK  provider %s\n", resolved.Provider.ID)
		if resolved.Provider.UsesAdapter {
			_, _ = fmt.Fprintf(out, "OK  model %s\n", resolved.SelectedPublicModel())
		}
	}
	if resolved.Provider.UsesAdapter {
		secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
		apiKey, err := modelprofile.ResolveAPIKey(resolved.Profile.APIKeyRef, secrets, os.Getenv)
		if err != nil {
			return err
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "OK  api key %s fingerprint=%s\n", modelprofile.MaskRef(resolved.Profile.APIKeyRef), modelprofile.Fingerprint(apiKey))
		}
	} else if out != nil {
		_, _ = fmt.Fprintln(out, "OK  api key Codex official auth")
	}
	if resolved.SSHProfile != nil {
		if out != nil {
			_, _ = fmt.Fprintf(out, "OK  ssh proxy %s\n", resolved.SSHProfile.Name)
		}
	} else if out != nil {
		_, _ = fmt.Fprintln(out, "OK  ssh proxy none")
	}
	return nil
}

func printModelProfiles(out io.Writer, cfg config.Config) {
	if out == nil {
		return
	}
	defaultName := cfg.EffectiveDefaultModelProfile()
	_, _ = fmt.Fprintln(out, "Model profiles")
	printOne := func(name string, p config.ModelProfile) {
		marker := " "
		if strings.EqualFold(name, defaultName) {
			marker = "*"
		}
		provider := strings.TrimSpace(p.Provider)
		if provider == "" {
			provider = modelprofile.DefaultProvider
		}
		model := strings.TrimSpace(p.Model)
		if model == "" {
			if spec, ok := modelprofile.LookupProvider(provider); ok && spec.UsesAdapter {
				model = spec.DefaultPublicModel()
			}
		}
		_, _ = fmt.Fprintf(out, "%s %s: provider=%s model=%s api_key=%s ssh_proxy=%s revision=%d\n",
			marker, name, provider, emptyAsNone(model), modelprofile.MaskRef(p.APIKeyRef), emptyAsNone(p.SSHProxy), maxInt(p.Revision, 1))
	}
	printOne(config.DefaultModelProfileName, config.ModelProfile{Provider: modelprofile.DefaultProvider, Revision: 1})
	names := make([]string, 0, len(cfg.ModelProfiles))
	for name := range cfg.ModelProfiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		printOne(name, cfg.ModelProfiles[name])
	}
}

func findModelProfileForCLI(cfg config.Config, ref string) (string, config.ModelProfile, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", config.ModelProfile{}, false
	}
	for name, profile := range cfg.ModelProfiles {
		if strings.EqualFold(name, ref) {
			return name, profile, true
		}
	}
	return "", config.ModelProfile{}, false
}

func modelProfileSetupChanges(existing config.ModelProfile, provider string, model string, apiKeyRef string, sshProxy string) bool {
	if strings.TrimSpace(apiKeyRef) == "" {
		apiKeyRef = existing.APIKeyRef
	}
	return strings.TrimSpace(existing.Provider) != strings.TrimSpace(provider) ||
		strings.TrimSpace(existing.Model) != strings.TrimSpace(model) ||
		strings.TrimSpace(existing.APIKeyRef) != strings.TrimSpace(apiKeyRef) ||
		strings.TrimSpace(existing.SSHProxy) != strings.TrimSpace(sshProxy)
}

func modelAliasSummary(model modelprofile.ModelSpec) string {
	aliases := make([]string, 0, len(model.Aliases))
	seen := map[string]bool{}
	for _, alias := range model.Aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		key := strings.ToLower(alias)
		if seen[key] {
			continue
		}
		seen[key] = true
		aliases = append(aliases, alias)
	}
	if len(aliases) == 0 {
		return ""
	}
	return " (aliases: " + strings.Join(aliases, ", ") + ")"
}

func readModelProfileAPIKey(in io.Reader) (string, error) {
	raw, err := io.ReadAll(in)
	if err != nil {
		return "", err
	}
	key := strings.TrimSpace(string(raw))
	if key == "" {
		return "", fmt.Errorf("empty API key")
	}
	return key, nil
}

func readModelProfileAPIKeyFromTerminal() (string, error) {
	_, _ = fmt.Fprint(os.Stderr, "API key (input hidden): ")
	raw, err := term.ReadPassword(int(os.Stdin.Fd()))
	_, _ = fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}
	key := strings.TrimSpace(string(raw))
	if key == "" {
		return "", fmt.Errorf("empty API key")
	}
	return key, nil
}

func emptyAsNone(s string) string {
	if strings.TrimSpace(s) == "" {
		return "none"
	}
	return strings.TrimSpace(s)
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
