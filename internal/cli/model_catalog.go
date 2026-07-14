package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelcatalog"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

const (
	defaultModelCatalogManifest = "models.json"
	modelCatalogManagedDir      = "model-catalogs"
	modelCatalogGitCacheDir     = "model-catalog-cache"
)

// Both catalog types are refreshed by the same long-lived Teams process and
// persist through the same config store. Serialize their auto-sync passes so
// one pass cannot load a stale snapshot and overwrite the other pass's
// committed routes.
var modelSubscriptionAutoSyncMu sync.Mutex

type modelCatalogAddOptions struct {
	gitURL   string
	jsonFile string
	stdin    bool
	ref      string
	manifest string
	replace  bool
	autoSync bool
}

type modelProviderSetupOptions struct {
	apiKeyEnv     string
	apiKeyStdin   bool
	interfaceName string
	timeout       time.Duration
}

func newModelCatalogCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{Use: "catalog", Short: "Manage provider/model catalogs from Git or local JSON"}
	cmd.AddCommand(
		newModelCatalogAddCmd(root, false),
		newModelCatalogAddCmd(root, true),
		newModelCatalogSyncCmd(root),
		newModelCatalogListCmd(root),
		newModelCatalogRemoveCmd(root),
	)
	return cmd
}

func newModelCatalogAddCmd(root *rootOptions, replace bool) *cobra.Command {
	opts := modelCatalogAddOptions{manifest: defaultModelCatalogManifest}
	verb := "add"
	short := "Register a catalog and activate every provider/model route"
	if replace {
		verb = "replace"
		short = "Replace a catalog atomically and refresh its routes"
	}
	cmd := &cobra.Command{
		Use:   verb + " <name>",
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.replace = replace
			return runModelCatalogAdd(cmd, root, args[0], opts)
		},
	}
	cmd.Flags().StringVar(&opts.gitURL, "git", "", "Git repository URL (credentials must come from Git's credential helper or SSH agent)")
	cmd.Flags().StringVar(&opts.jsonFile, "json", "", "Local catalog JSON file to import and manage")
	cmd.Flags().BoolVar(&opts.stdin, "stdin", false, "Read catalog JSON from stdin")
	cmd.Flags().StringVar(&opts.ref, "ref", "", "Git branch, tag, or commit")
	cmd.Flags().StringVar(&opts.manifest, "manifest", defaultModelCatalogManifest, "Repository-relative manifest path for Git catalogs")
	cmd.Flags().BoolVar(&opts.autoSync, "auto-sync", false, "Allow long-lived services to refresh this Git catalog")
	return cmd
}

func newModelCatalogSyncCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use: "sync <name>", Short: "Refresh one Git catalog or re-read one managed JSON catalog", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error { return runModelCatalogSync(cmd, root, args[0]) },
	}
}

func newModelCatalogListCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use: "list", Short: "List catalogs and their provider/model routes", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			cfg, err := store.Load()
			if err != nil {
				return err
			}
			printModelCatalogs(cmd.OutOrStdout(), cfg)
			return nil
		},
	}
}

func newModelCatalogRemoveCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use: "remove <name>", Short: "Remove a catalog and all routes owned by it", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			name := strings.TrimSpace(args[0])
			err = store.Update(func(cfg *config.Config) error {
				canonicalName := mapKeyFold(cfg.ModelCatalogs, name)
				if canonicalName == "" {
					return fmt.Errorf("model catalog %q not found", name)
				}
				removeCatalogRuntime(cfg, canonicalName)
				return nil
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed model catalog %q and its provider/model routes.\n", name)
			return nil
		},
	}
}

func newModelProviderCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{Use: "provider", Short: "Bind one provider credential and activate all of its catalog models"}
	cmd.AddCommand(newModelProviderListCmd(root), newModelProviderSetupCmd(root), newModelProviderDoctorCmd(root))
	return cmd
}

func newModelProviderListCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use: "list", Short: "List catalog providers and activation status", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			cfg, err := store.Load()
			if err != nil {
				return err
			}
			secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
			printModelProvidersWithSecrets(cmd.OutOrStdout(), cfg, secrets)
			return nil
		},
	}
}

func newModelProviderSetupCmd(root *rootOptions) *cobra.Command {
	opts := modelProviderSetupOptions{timeout: 20 * time.Second}
	cmd := &cobra.Command{
		Use: "setup <provider>", Short: "Set one provider key and verify every model published by it", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error { return runModelProviderSetup(cmd, root, args[0], opts) },
	}
	cmd.Flags().StringVar(&opts.apiKeyEnv, "api-key-env", "", "Environment variable containing the provider API key")
	cmd.Flags().BoolVar(&opts.apiKeyStdin, "api-key-stdin", false, "Read the provider API key from stdin and save it locally")
	cmd.Flags().StringVar(&opts.interfaceName, "interface", "", "Bind a key for one catalog interface (for example anthropic or beta)")
	cmd.Flags().DurationVar(&opts.timeout, "timeout", 20*time.Second, "Verification timeout per model")
	return cmd
}

func newModelProviderDoctorCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use: "doctor <provider>", Short: "Explain which models of one provider are ready", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			cfg, err := store.Load()
			if err != nil {
				return err
			}
			return modelProviderDoctor(cmd.OutOrStdout(), cfg, args[0])
		},
	}
}

func runModelCatalogAdd(cmd *cobra.Command, root *rootOptions, rawName string, opts modelCatalogAddOptions) error {
	name := strings.TrimSpace(rawName)
	if err := config.ValidateModelCatalogID(name); err != nil {
		return err
	}
	gitURL := strings.TrimSpace(opts.gitURL)
	jsonFile := strings.TrimSpace(opts.jsonFile)
	hasGit, hasJSON := gitURL != "", jsonFile != ""
	if hasGit && (hasJSON || opts.stdin) || hasJSON && opts.stdin || !hasGit && !hasJSON && !opts.stdin {
		return fmt.Errorf("choose exactly one catalog input: --git, --json, or --stdin")
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	if existingName := mapKeyFold(cfg.ModelCatalogs, name); existingName != "" {
		if !opts.replace {
			return fmt.Errorf("model catalog %q already exists; use `model catalog replace`", existingName)
		}
		name = existingName
	}

	var catalog config.ModelCatalog
	var doc modelcatalog.Document
	var raw []byte
	if hasGit {
		parsed, parseErr := url.Parse(gitURL)
		scpLike := strings.HasPrefix(gitURL, "git@") && strings.Contains(gitURL, ":") && !strings.Contains(gitURL, "://") && !strings.ContainsAny(gitURL, " \t\r\n")
		if (parseErr != nil && !scpLike) || (parsed != nil && parsed.User != nil) {
			return fmt.Errorf("Git URL must not contain embedded credentials")
		}
		catalog = config.ModelCatalog{Type: config.ModelCatalogTypeGit, URL: gitURL, Ref: strings.TrimSpace(opts.ref), File: cleanCatalogManifest(opts.manifest), AutoSync: opts.autoSync}
		doc, catalog.Revision, err = fetchGitModelCatalog(cmd.Context(), store, catalog)
		if err != nil {
			return err
		}
	} else {
		catalog = config.ModelCatalog{Type: config.ModelCatalogTypeManagedJSON, ManagedFile: filepath.ToSlash(filepath.Join(modelCatalogManagedDir, name+".json"))}
		if opts.stdin {
			raw, err = io.ReadAll(cmd.InOrStdin())
		} else {
			raw, err = os.ReadFile(jsonFile)
		}
		if err != nil {
			return err
		}
		doc, err = modelcatalog.Parse(raw)
		if err != nil {
			return fmt.Errorf("parse catalog: %w", err)
		}
	}
	catalog.SyncedAt = time.Now().UTC()
	if err := catalog.Validate(name); err != nil {
		return err
	}
	if catalog.Type == config.ModelCatalogTypeManagedJSON {
		canonical, marshalErr := modelcatalog.Marshal(doc)
		if marshalErr != nil {
			return marshalErr
		}
		// Managed storage is canonicalized before it is written. Compute the
		// content revision from those exact bytes so the first explicit sync is
		// a true no-op rather than an artificial catalog replacement.
		catalog.Revision = modelCatalogDocumentRevision(canonical)
		// Validate route collisions before touching the managed file. If the
		// config commit races and fails after the file swap, restore the exact
		// previous bytes so replacement remains atomic from the user's point of
		// view.
		preview, cloneErr := cloneConfigForCatalog(cfg)
		if cloneErr != nil {
			return cloneErr
		}
		if err := installModelCatalog(&preview, name, catalog, doc); err != nil {
			return err
		}
		managedPath, pathErr := safeManagedCatalogPath(store.Path(), catalog.ManagedFile)
		if pathErr != nil {
			return pathErr
		}
		previous, readErr := os.ReadFile(managedPath)
		hadPrevious := readErr == nil
		if readErr != nil && !os.IsNotExist(readErr) {
			return readErr
		}
		if err := writeManagedCatalog(store.Path(), catalog.ManagedFile, canonical); err != nil {
			return err
		}
		if err := store.Update(func(current *config.Config) error {
			return installModelCatalog(current, name, catalog, doc)
		}); err != nil {
			if restoreErr := restoreManagedCatalog(managedPath, previous, hadPrevious); restoreErr != nil {
				return fmt.Errorf("install model catalog: %w (restore managed catalog: %v)", err, restoreErr)
			}
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Catalog %q installed: %d provider(s), %d model(s).\n", name, len(doc.Providers), len(doc.Routes()))
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Next: cxp model provider list; then `cxp model provider setup <provider> --api-key-stdin`.\n")
		return nil
	}
	if err := store.Update(func(current *config.Config) error {
		return installModelCatalog(current, name, catalog, doc)
	}); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Catalog %q installed: %d provider(s), %d model(s).\n", name, len(doc.Providers), len(doc.Routes()))
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Next: cxp model provider list; then `cxp model provider setup <provider> --api-key-stdin`.\n")
	return nil
}

func cloneConfigForCatalog(cfg config.Config) (config.Config, error) {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return config.Config{}, err
	}
	var clone config.Config
	if err := json.Unmarshal(raw, &clone); err != nil {
		return config.Config{}, err
	}
	return clone, nil
}

func restoreManagedCatalog(path string, previous []byte, hadPrevious bool) error {
	if !hadPrevious {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".catalog-restore-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(previous); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func runModelCatalogSync(cmd *cobra.Command, root *rootOptions, name string) error {
	name = strings.TrimSpace(name)
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	canonicalName := mapKeyFold(cfg.ModelCatalogs, name)
	catalog, ok := cfg.ModelCatalogs[canonicalName]
	if !ok {
		return fmt.Errorf("model catalog %q not found", name)
	}
	name = canonicalName
	var doc modelcatalog.Document
	if catalog.Type == config.ModelCatalogTypeGit {
		doc, catalog.Revision, err = fetchGitModelCatalog(cmd.Context(), store, catalog)
	} else {
		doc, err = readManagedCatalog(store.Path(), catalog.ManagedFile)
		if err == nil {
			canonical, marshalErr := modelcatalog.Marshal(doc)
			if marshalErr == nil {
				catalog.Revision = modelCatalogDocumentRevision(canonical)
			}
		}
	}
	if err != nil {
		return err
	}
	catalog.SyncedAt = time.Now().UTC()
	if err := store.Update(func(current *config.Config) error {
		return installModelCatalog(current, name, catalog, doc)
	}); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Catalog %q synchronized: %d provider(s), %d model(s).\n", name, len(doc.Providers), len(doc.Routes()))
	return nil
}

func runModelCatalogAutoSyncLoop(ctx context.Context, root *rootOptions, errOut io.Writer, interval time.Duration) {
	if interval <= 0 {
		interval = defaultModelSourceAutoSyncInterval
	}
	for {
		delay := runModelCatalogAutoSyncOnce(ctx, root, errOut, time.Now().UTC(), interval)
		if delay <= 0 {
			delay = interval
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}

func runModelCatalogAutoSyncOnce(ctx context.Context, root *rootOptions, errOut io.Writer, now time.Time, interval time.Duration) time.Duration {
	modelSubscriptionAutoSyncMu.Lock()
	defer modelSubscriptionAutoSyncMu.Unlock()
	if interval <= 0 {
		interval = defaultModelSourceAutoSyncInterval
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		modelCatalogAutoSyncWarning(errOut, "open config", err)
		return interval
	}
	cfg, err := store.Load()
	if err != nil {
		modelCatalogAutoSyncWarning(errOut, "load config", err)
		return interval
	}
	names := dueModelCatalogs(cfg, now, interval)
	hadFailure := false
	for _, name := range names {
		if ctx.Err() != nil {
			return interval
		}
		cmd := &cobra.Command{}
		cmd.SetContext(ctx)
		cmd.SetOut(io.Discard)
		cmd.SetErr(errOut)
		if err := runModelCatalogSync(cmd, root, name); err != nil {
			hadFailure = true
			modelCatalogAutoSyncWarning(errOut, "sync "+name, err)
		}
	}
	if hadFailure {
		// Do not retry a broken Git/network subscription in a tight loop. The
		// last successfully installed catalog remains active until the next
		// normal interval.
		return interval
	}
	if len(names) == 0 {
		return nextModelCatalogAutoSyncDelay(cfg, now, interval)
	}
	if refreshed, loadErr := store.Load(); loadErr == nil {
		return nextModelCatalogAutoSyncDelay(refreshed, time.Now().UTC(), interval)
	}
	return interval
}

func dueModelCatalogs(cfg config.Config, now time.Time, interval time.Duration) []string {
	if interval <= 0 {
		interval = defaultModelSourceAutoSyncInterval
	}
	names := make([]string, 0)
	for name, catalog := range cfg.ModelCatalogs {
		if !catalog.AutoSync || catalog.Type != config.ModelCatalogTypeGit {
			continue
		}
		if catalog.SyncedAt.IsZero() || !catalog.SyncedAt.Add(interval).After(now) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func nextModelCatalogAutoSyncDelay(cfg config.Config, now time.Time, interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = defaultModelSourceAutoSyncInterval
	}
	delay := interval
	found := false
	for _, catalog := range cfg.ModelCatalogs {
		if !catalog.AutoSync || catalog.Type != config.ModelCatalogTypeGit {
			continue
		}
		found = true
		if catalog.SyncedAt.IsZero() {
			return time.Millisecond
		}
		candidate := catalog.SyncedAt.Add(interval).Sub(now)
		if candidate <= 0 {
			return time.Millisecond
		}
		if candidate < delay {
			delay = candidate
		}
	}
	if !found {
		return interval
	}
	return delay
}

func modelCatalogAutoSyncWarning(out io.Writer, operation string, err error) {
	if out != nil && err != nil {
		_, _ = fmt.Fprintf(out, "Model catalog auto-sync warning: %s: %v\n", operation, err)
	}
}

func fetchGitModelCatalog(ctx context.Context, store *config.Store, catalog config.ModelCatalog) (modelcatalog.Document, string, error) {
	cacheRoot := filepath.Join(filepath.Dir(store.Path()), modelCatalogGitCacheDir)
	if err := os.MkdirAll(cacheRoot, 0o700); err != nil {
		return modelcatalog.Document{}, "", err
	}
	tmp, err := os.MkdirTemp(cacheRoot, ".sync-")
	if err != nil {
		return modelcatalog.Document{}, "", err
	}
	defer os.RemoveAll(tmp)
	repo := filepath.Join(tmp, "repo")
	args := []string{"clone", "--depth", "1", "--filter=blob:none", "--no-checkout", "--", catalog.URL, repo}
	if raw, cloneErr := exec.CommandContext(ctx, "git", args...).CombinedOutput(); cloneErr != nil {
		return modelcatalog.Document{}, "", fmt.Errorf("clone model catalog: %w: %s", cloneErr, strings.TrimSpace(string(raw)))
	}
	if strings.TrimSpace(catalog.Ref) != "" {
		if raw, fetchErr := exec.CommandContext(ctx, "git", "-C", repo, "fetch", "--depth", "1", "origin", catalog.Ref).CombinedOutput(); fetchErr != nil {
			return modelcatalog.Document{}, "", fmt.Errorf("fetch catalog ref %q: %w: %s", catalog.Ref, fetchErr, strings.TrimSpace(string(raw)))
		}
		if raw, checkoutErr := exec.CommandContext(ctx, "git", "-C", repo, "checkout", "--detach", "FETCH_HEAD").CombinedOutput(); checkoutErr != nil {
			return modelcatalog.Document{}, "", fmt.Errorf("checkout catalog ref %q: %w: %s", catalog.Ref, checkoutErr, strings.TrimSpace(string(raw)))
		}
	} else if raw, checkoutErr := exec.CommandContext(ctx, "git", "-C", repo, "checkout", "--detach", "HEAD").CombinedOutput(); checkoutErr != nil {
		return modelcatalog.Document{}, "", fmt.Errorf("checkout model catalog: %w: %s", checkoutErr, strings.TrimSpace(string(raw)))
	}
	revisionRaw, err := exec.CommandContext(ctx, "git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		return modelcatalog.Document{}, "", fmt.Errorf("read catalog revision: %w", err)
	}
	path, err := safeRepoFile(repo, catalog.File)
	if err != nil {
		return modelcatalog.Document{}, "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return modelcatalog.Document{}, "", fmt.Errorf("read catalog manifest %q: %w", catalog.File, err)
	}
	doc, err := modelcatalog.Parse(raw)
	if err != nil {
		return modelcatalog.Document{}, "", fmt.Errorf("parse catalog manifest %q: %w", catalog.File, err)
	}
	return doc, strings.TrimSpace(string(revisionRaw)), nil
}

func installModelCatalog(cfg *config.Config, name string, catalog config.ModelCatalog, doc modelcatalog.Document) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	if err := catalog.Validate(name); err != nil {
		return err
	}
	if err := doc.Validate(); err != nil {
		return err
	}
	routes := doc.Routes()
	if existingName := mapKeyFold(cfg.ModelCatalogs, name); existingName != "" {
		previous := cfg.ModelCatalogs[existingName]
		if modelCatalogContentUnchanged(previous, catalog) && modelCatalogRuntimeIntact(*cfg, existingName, routes) {
			// A periodic sync with the same source revision must not rebuild
			// profiles or advance their revisions. That would invalidate active
			// runtime/cache identities even though the effective route is
			// unchanged. Update only source metadata (notably SyncedAt and
			// AutoSync) and leave verification state untouched.
			catalogKey := existingName
			cfg.ModelCatalogs[catalogKey] = catalog
			cfg.ModelConfigVersion = config.CurrentModelConfigVersion
			return config.ValidateModelConfig(*cfg)
		}
	}
	oldProfiles := map[string]bool{}
	type catalogVerification struct {
		basis  string
		apiRef string
		proof  string
		at     time.Time
		err    string
	}
	oldVerification := map[string]catalogVerification{}
	oldProfileValues := map[string]config.ModelProfile{}
	oldProviders := map[string]bool{}
	oldModels := map[string]bool{}
	oldCredentials := map[string]bool{}
	oldBindings := map[string]config.ModelProviderBinding{}
	for profileName, profile := range cfg.ModelProfiles {
		if !strings.EqualFold(strings.TrimSpace(profile.Source), name) {
			continue
		}
		oldProfiles[profileName] = true
		oldProfileValues[profileName] = profile
		if strings.TrimSpace(profile.VerificationFingerprint) != "" {
			if resolved, resolveErr := modelprofile.Resolve(*cfg, profileName); resolveErr == nil {
				oldVerification[profileName] = catalogVerification{basis: modelVerificationConfigurationFingerprint(resolved), apiRef: strings.TrimSpace(resolved.Profile.APIKeyRef), proof: profile.VerificationFingerprint, at: profile.VerifiedAt, err: profile.VerificationError}
			}
		}
		if modelName, model, ok := config.FindModelDefinition(*cfg, profile.Model); ok {
			oldModels[modelName] = true
			oldProviders[model.Provider] = true
			if provider, ok := cfg.ModelProviders[model.Provider]; ok {
				oldCredentials[provider.Credential] = true
			}
		}
	}
	for provider, binding := range cfg.ModelProviderBindings {
		if strings.EqualFold(strings.TrimSpace(binding.Catalog), name) {
			oldBindings[provider] = binding
		}
	}
	for credentialName := range cfg.ModelCredentials {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(credentialName)), strings.ToLower("catalog/"+name+"/")) {
			oldCredentials[credentialName] = true
		}
	}
	for _, route := range routes {
		profileKey := mapKeyFold(cfg.ModelProfiles, route.Selector)
		if existing, ok := cfg.ModelProfiles[profileKey]; ok && mapKeyFold(oldProfiles, profileKey) == "" && strings.TrimSpace(existing.Source) != "" {
			return fmt.Errorf("catalog route %q conflicts with existing profile from %q", route.Selector, existing.Source)
		}
		providerKey := mapKeyFold(cfg.ModelProviders, route.ProviderID)
		if providerKey != "" && mapKeyFold(oldProviders, providerKey) == "" {
			return fmt.Errorf("catalog provider %q conflicts with an existing provider", route.ProviderID)
		}
		if profileKey != "" && mapKeyFold(oldProfiles, profileKey) == "" {
			return fmt.Errorf("catalog selector %q conflicts with an existing profile", route.Selector)
		}
	}
	if cfg.ModelCatalogs == nil {
		cfg.ModelCatalogs = map[string]config.ModelCatalog{}
	}
	if cfg.ModelProviderBindings == nil {
		cfg.ModelProviderBindings = map[string]config.ModelProviderBinding{}
	}
	if cfg.ModelProviders == nil {
		cfg.ModelProviders = map[string]config.ModelProvider{}
	}
	if cfg.Models == nil {
		cfg.Models = map[string]config.ModelDefinition{}
	}
	if cfg.ModelProfiles == nil {
		cfg.ModelProfiles = map[string]config.ModelProfile{}
	}
	if cfg.ModelCredentials == nil {
		cfg.ModelCredentials = map[string]config.ModelCredential{}
	}
	for name := range oldProfiles {
		delete(cfg.ModelProfiles, name)
	}
	for name := range oldModels {
		used := false
		for _, profile := range cfg.ModelProfiles {
			if strings.EqualFold(strings.TrimSpace(profile.Model), name) {
				used = true
				break
			}
			if resolvedName, _, ok := config.FindModelDefinition(*cfg, profile.Model); ok && strings.EqualFold(resolvedName, name) {
				used = true
				break
			}
		}
		if !used {
			delete(cfg.Models, name)
		}
	}
	for name := range oldProviders {
		used := false
		for _, model := range cfg.Models {
			if strings.EqualFold(strings.TrimSpace(model.Provider), name) {
				used = true
				break
			}
		}
		if !used {
			for _, profile := range cfg.ModelProfiles {
				if strings.EqualFold(strings.TrimSpace(profile.Provider), name) {
					used = true
					break
				}
			}
		}
		if !used {
			delete(cfg.ModelProviders, name)
		}
	}
	for name := range oldCredentials {
		used := false
		for _, provider := range cfg.ModelProviders {
			if strings.EqualFold(strings.TrimSpace(provider.Credential), name) {
				used = true
				break
			}
		}
		if !used {
			for _, profile := range cfg.ModelProfiles {
				if strings.EqualFold(strings.TrimSpace(profile.Credential), name) {
					used = true
					break
				}
			}
		}
		if !used {
			delete(cfg.ModelCredentials, name)
		}
	}
	for provider, binding := range cfg.ModelProviderBindings {
		if strings.EqualFold(strings.TrimSpace(binding.Catalog), name) {
			delete(cfg.ModelProviderBindings, provider)
		}
	}
	for _, route := range routes {
		bindingKey := mapKeyFold(oldBindings, route.ProviderID)
		binding := oldBindings[bindingKey]
		if binding.Catalog != "" && !strings.EqualFold(binding.Catalog, name) {
			return fmt.Errorf("provider %q is already bound to catalog %q", route.ProviderID, binding.Catalog)
		}
		binding.Catalog = name
		if strings.TrimSpace(binding.SecretRef) == "" {
			binding.SecretRef = providerSecretRef(route.ProviderID)
		}
		secretRef := strings.TrimSpace(binding.SecretRef)
		if configured, ok := binding.InterfaceSecrets[route.InterfaceID]; ok && strings.TrimSpace(configured) != "" {
			secretRef = strings.TrimSpace(configured)
		}
		// Catalog replacement invalidates every route's verification proof. A
		// provider is enabled again only after the batch setup command verifies
		// all of its current models.
		binding.Enabled = false
		cfg.ModelProviderBindings[route.ProviderID] = binding
		credentialName := catalogCredentialNameForInterface(name, route.ProviderID, route.InterfaceID, route.Provider.DefaultInterface)
		credential := cfg.ModelCredentials[credentialName]
		credential.APIKeyRef = secretRef
		credential.Pending = credential.APIKeyRef == ""
		credential.AuthType = strings.ToLower(strings.TrimSpace(route.AuthType))
		credential.Header = strings.TrimSpace(route.AuthHeader)
		cfg.ModelCredentials[credentialName] = credential
		provider := route.Provider
		if existingProvider, ok := cfg.ModelProviders[route.ProviderID]; ok {
			for interfaceName, existingCredential := range existingProvider.InterfaceCredentials {
				if provider.InterfaceCredentials == nil {
					provider.InterfaceCredentials = map[string]string{}
				}
				provider.InterfaceCredentials[interfaceName] = existingCredential
			}
		}
		provider.Credential = catalogCredentialName(name, route.ProviderID)
		if provider.InterfaceCredentials == nil {
			provider.InterfaceCredentials = map[string]string{}
		}
		provider.InterfaceCredentials[route.InterfaceID] = credentialName
		provider.InterfaceCredentials[provider.DefaultInterface] = provider.Credential
		// Keep the provider-level credential pointed at the default interface;
		// model profiles use the interface-specific credential when their default
		// route differs.
		if _, ok := cfg.ModelCredentials[provider.Credential]; !ok {
			defaultSecret := strings.TrimSpace(binding.SecretRef)
			if configured, ok := binding.InterfaceSecrets[provider.DefaultInterface]; ok && strings.TrimSpace(configured) != "" {
				defaultSecret = strings.TrimSpace(configured)
			}
			defaultIface := provider.Interfaces[provider.DefaultInterface]
			cfg.ModelCredentials[provider.Credential] = config.ModelCredential{APIKeyRef: defaultSecret, Pending: defaultSecret == "", AuthType: strings.ToLower(strings.TrimSpace(defaultIface.Auth.Type)), Header: strings.TrimSpace(defaultIface.Auth.Header)}
		}
		cfg.ModelProviders[route.ProviderID] = provider
		oldProfileName := mapKeyFold(oldProfileValues, route.Selector)
		old := oldProfileValues[oldProfileName]
		profile := config.ModelProfile{Provider: route.ProviderID, Model: route.Selector, Credential: credentialName, APIKeyRef: credential.APIKeyRef, Revision: old.Revision + 1, CreatedAt: old.CreatedAt, UpdatedAt: time.Now().UTC(), Source: name}
		if profile.Revision <= 1 {
			profile.Revision = 1
		}
		if profile.CreatedAt.IsZero() {
			profile.CreatedAt = profile.UpdatedAt
		}
		cfg.ModelProfiles[route.Selector] = profile
		model := route.Model
		model.Provider = route.ProviderID
		cfg.Models[route.Selector] = model
	}
	// Preserve a verification proof when a Git revision changes without
	// changing the effective provider/model policy or credential reference.
	// A changed route remains hidden until the provider-wide setup verifies it.
	verifiedProviders := map[string]bool{}
	for _, route := range routes {
		if _, ok := verifiedProviders[route.ProviderID]; !ok {
			verifiedProviders[route.ProviderID] = true
		}
	}
	for _, route := range routes {
		profile := cfg.ModelProfiles[route.Selector]
		oldProfileName := mapKeyFold(oldVerification, route.Selector)
		old, ok := oldVerification[oldProfileName]
		if !ok {
			verifiedProviders[route.ProviderID] = false
			continue
		}
		resolved, resolveErr := modelprofile.Resolve(*cfg, route.Selector)
		if resolveErr != nil || old.apiRef != strings.TrimSpace(resolved.Profile.APIKeyRef) || old.basis != modelVerificationConfigurationFingerprint(resolved) {
			verifiedProviders[route.ProviderID] = false
			continue
		}
		profile.VerifiedAt, profile.VerificationFingerprint, profile.VerificationError = old.at, old.proof, old.err
		cfg.ModelProfiles[route.Selector] = profile
	}
	for provider, binding := range cfg.ModelProviderBindings {
		if !strings.EqualFold(strings.TrimSpace(binding.Catalog), name) {
			continue
		}
		binding.Enabled = verifiedProviders[provider]
		cfg.ModelProviderBindings[provider] = binding
	}
	cfg.ModelCatalogs[name] = catalog
	cfg.ModelConfigVersion = config.CurrentModelConfigVersion
	repairCatalogDefault(cfg, oldProfiles)
	return config.ValidateModelConfig(*cfg)
}

func modelCatalogContentUnchanged(previous, current config.ModelCatalog) bool {
	return previous.Type == current.Type && previous.URL == current.URL && previous.Ref == current.Ref && previous.File == current.File && previous.ManagedFile == current.ManagedFile && strings.TrimSpace(previous.Revision) != "" && strings.TrimSpace(previous.Revision) == strings.TrimSpace(current.Revision)
}

func modelCatalogRuntimeIntact(cfg config.Config, catalogName string, routes []modelcatalog.Route) bool {
	for _, route := range routes {
		profileName := mapKeyFold(cfg.ModelProfiles, route.Selector)
		if profileName == "" {
			return false
		}
		profile := cfg.ModelProfiles[profileName]
		if !strings.EqualFold(strings.TrimSpace(profile.Source), strings.TrimSpace(catalogName)) {
			return false
		}
		modelName, model, ok := config.FindModelDefinition(cfg, profile.Model)
		if !ok || !strings.EqualFold(strings.TrimSpace(modelName), strings.TrimSpace(route.Selector)) || !strings.EqualFold(strings.TrimSpace(model.Provider), strings.TrimSpace(route.ProviderID)) {
			return false
		}
		providerName := mapKeyFold(cfg.ModelProviders, route.ProviderID)
		if providerName == "" {
			return false
		}
		bindingName := mapKeyFold(cfg.ModelProviderBindings, route.ProviderID)
		if bindingName == "" || !strings.EqualFold(strings.TrimSpace(cfg.ModelProviderBindings[bindingName].Catalog), strings.TrimSpace(catalogName)) {
			return false
		}
	}
	return len(routes) > 0
}

func removeCatalogRuntime(cfg *config.Config, name string) bool {
	removed := false
	profiles := map[string]bool{}
	providers := map[string]bool{}
	models := map[string]bool{}
	credentials := map[string]bool{}
	for profileName, profile := range cfg.ModelProfiles {
		if !strings.EqualFold(strings.TrimSpace(profile.Source), name) {
			continue
		}
		removed = true
		profiles[profileName] = true
		if modelName, model, ok := config.FindModelDefinition(*cfg, profile.Model); ok {
			models[modelName] = true
			providers[model.Provider] = true
			if provider, ok := cfg.ModelProviders[model.Provider]; ok {
				credentials[provider.Credential] = true
			}
		}
	}
	for name := range profiles {
		delete(cfg.ModelProfiles, name)
	}
	for name := range models {
		used := false
		for _, profile := range cfg.ModelProfiles {
			if strings.EqualFold(strings.TrimSpace(profile.Model), name) {
				used = true
				break
			}
			if resolvedName, _, ok := config.FindModelDefinition(*cfg, profile.Model); ok && strings.EqualFold(resolvedName, name) {
				used = true
				break
			}
		}
		if !used {
			delete(cfg.Models, name)
		}
	}
	for name := range providers {
		used := false
		for _, model := range cfg.Models {
			if strings.EqualFold(model.Provider, name) {
				used = true
				break
			}
		}
		if !used {
			for _, profile := range cfg.ModelProfiles {
				if strings.EqualFold(strings.TrimSpace(profile.Provider), name) {
					used = true
					break
				}
			}
		}
		if !used {
			delete(cfg.ModelProviders, name)
		}
	}
	for name := range credentials {
		used := false
		for _, provider := range cfg.ModelProviders {
			if strings.EqualFold(provider.Credential, name) {
				used = true
				break
			}
		}
		if !used {
			delete(cfg.ModelCredentials, name)
		}
	}
	for provider, binding := range cfg.ModelProviderBindings {
		if strings.EqualFold(binding.Catalog, name) {
			delete(cfg.ModelProviderBindings, provider)
		}
	}
	delete(cfg.ModelCatalogs, name)
	repairCatalogDefault(cfg, profiles)
	return removed
}

func repairCatalogDefault(cfg *config.Config, removed map[string]bool) {
	if cfg == nil {
		return
	}
	selector := strings.TrimSpace(cfg.EffectiveDefaultModelSelector())
	name := selector
	switch {
	case strings.HasPrefix(strings.ToLower(name), "official:"):
		// Official selectors do not point at a local catalog profile.
		return
	case strings.HasPrefix(strings.ToLower(name), "profile:"):
		_, name, _ = strings.Cut(name, ":")
	}
	if removed[name] || (name != "" && !strings.EqualFold(name, config.DefaultModelProfileName)) {
		if _, ok := cfg.FindModelProfile(name); !ok {
			cfg.SetDefaultModelProfile("")
		}
	}
}

// catalogProviderInterfaceUsage returns the catalog interfaces that are
// actually needed by each published model. Unused interfaces (for example a
// provider's optional Beta endpoint) do not block activation, while a
// feature-specific interface such as DeepSeek Anthropic must be verified with
// its own credential before the provider is exposed.
func catalogProviderInterfaceUsage(cfg config.Config, providerName string) map[string][]string {
	providerKey := mapKeyFold(cfg.ModelProviders, providerName)
	provider, ok := cfg.ModelProviders[providerKey]
	if !ok {
		return nil
	}
	usage := map[string][]string{}
	seen := map[string]map[string]bool{}
	add := func(interfaceName, profileName string) {
		interfaceName = mapKeyFold(provider.Interfaces, interfaceName)
		if interfaceName == "" {
			return
		}
		if seen[interfaceName] == nil {
			seen[interfaceName] = map[string]bool{}
		}
		if seen[interfaceName][profileName] {
			return
		}
		seen[interfaceName][profileName] = true
		usage[interfaceName] = append(usage[interfaceName], profileName)
	}
	for _, profileName := range catalogProviderProfiles(cfg, providerName) {
		profile := cfg.ModelProfiles[profileName]
		_, model, ok := config.FindModelDefinition(cfg, profile.Model)
		if !ok || !strings.EqualFold(strings.TrimSpace(model.Provider), strings.TrimSpace(providerName)) {
			continue
		}
		defaultInterface := firstNonEmptyCLI(model.DefaultInterface, provider.DefaultInterface)
		add(defaultInterface, profileName)
		for _, feature := range model.Features {
			if feature.Support != "native" && feature.Support != "translated" {
				continue
			}
			add(feature.Interface, profileName)
		}
	}
	for interfaceName := range usage {
		sort.Strings(usage[interfaceName])
	}
	return usage
}

// resolveCatalogProfileForInterface resolves a profile against an alternate
// catalog interface without mutating the canonical profile. The returned
// value is used only for a real provider verification request; ordinary
// launches continue to resolve the model's declared default interface.
func resolveCatalogProfileForInterface(cfg config.Config, profileName, interfaceName string) (modelprofile.Resolved, error) {
	clone, err := cloneConfigForCatalog(cfg)
	if err != nil {
		return modelprofile.Resolved{}, err
	}
	profile, ok := clone.ModelProfiles[profileName]
	if !ok {
		return modelprofile.Resolved{}, fmt.Errorf("model profile %q not found", profileName)
	}
	modelName, model, ok := config.FindModelDefinition(clone, profile.Model)
	if !ok {
		return modelprofile.Resolved{}, fmt.Errorf("model profile %q references missing model %q", profileName, profile.Model)
	}
	providerKey := mapKeyFold(clone.ModelProviders, model.Provider)
	provider, ok := clone.ModelProviders[providerKey]
	if !ok {
		return modelprofile.Resolved{}, fmt.Errorf("model provider %q not found", model.Provider)
	}
	interfaceKey := mapKeyFold(provider.Interfaces, interfaceName)
	if interfaceKey == "" {
		return modelprofile.Resolved{}, fmt.Errorf("model provider %q has no interface %q", model.Provider, interfaceName)
	}
	model.DefaultInterface = interfaceKey
	clone.Models[modelName] = model
	if interfaceCredentialKey := mapKeyFold(provider.InterfaceCredentials, interfaceKey); interfaceCredentialKey != "" {
		credentialName := strings.TrimSpace(provider.InterfaceCredentials[interfaceCredentialKey])
		profile.Credential = credentialName
		if credential, ok := clone.ModelCredentials[credentialName]; ok {
			profile.APIKeyRef = credential.APIKeyRef
		}
	}
	clone.ModelProfiles[profileName] = profile
	return modelprofile.Resolve(clone, profileName)
}

func runModelProviderSetup(cmd *cobra.Command, root *rootOptions, rawProvider string, opts modelProviderSetupOptions) error {
	provider := strings.TrimSpace(rawProvider)
	if opts.timeout <= 0 {
		opts.timeout = 20 * time.Second
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	providerKey := mapKeyFold(cfg.ModelProviderBindings, provider)
	binding, ok := cfg.ModelProviderBindings[providerKey]
	if !ok {
		return fmt.Errorf("catalog provider %q is not configured", provider)
	}
	secretStore := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	providerDefinition, hasProviderDefinition := cfg.ModelProviders[providerKey]
	interfaceName := ""
	if hasProviderDefinition && len(providerDefinition.Interfaces) > 0 {
		interfaceName = mapKeyFold(providerDefinition.Interfaces, providerDefinition.DefaultInterface)
		if strings.TrimSpace(opts.interfaceName) != "" {
			interfaceName = mapKeyFold(providerDefinition.Interfaces, opts.interfaceName)
			if interfaceName == "" {
				return fmt.Errorf("catalog provider %q has no interface %q", provider, opts.interfaceName)
			}
		}
		if interfaceName == "" {
			interfaceNames := make([]string, 0, len(providerDefinition.Interfaces))
			for name := range providerDefinition.Interfaces {
				interfaceNames = append(interfaceNames, name)
			}
			sort.Strings(interfaceNames)
			interfaceName = interfaceNames[0]
		}
	} else if strings.TrimSpace(opts.interfaceName) != "" {
		return fmt.Errorf("catalog provider %q has no materialized interface definitions", provider)
	}
	existingSecretRef := binding.SecretRef
	if interfaceName != "" {
		if configured := strings.TrimSpace(binding.InterfaceSecrets[interfaceName]); configured != "" {
			existingSecretRef = configured
		}
	}
	ref, key, err := resolveProviderAPIKey(cmd, secretStore, existingSecretRef, providerKey+interfaceScopeSuffix(interfaceName), opts)
	if err != nil {
		return err
	}
	targetCredential := ""
	if interfaceName != "" {
		if binding.InterfaceSecrets == nil {
			binding.InterfaceSecrets = map[string]string{}
		}
		binding.InterfaceSecrets[interfaceName] = ref
		providerDefinition := cfg.ModelProviders[providerKey]
		if strings.EqualFold(interfaceName, providerDefinition.DefaultInterface) {
			binding.SecretRef = ref
		}
		credentialName := catalogCredentialNameForInterface(binding.Catalog, providerKey, interfaceName, providerDefinition.DefaultInterface)
		credential := cfg.ModelCredentials[credentialName]
		credential.APIKeyRef = ref
		credential.Pending = false
		if iface, ok := providerDefinition.Interfaces[interfaceName]; ok {
			credential.AuthType = strings.ToLower(strings.TrimSpace(iface.Auth.Type))
			credential.Header = strings.TrimSpace(iface.Auth.Header)
		}
		cfg.ModelCredentials[credentialName] = credential
		if providerDefinition.InterfaceCredentials == nil {
			providerDefinition.InterfaceCredentials = map[string]string{}
		}
		providerDefinition.InterfaceCredentials[interfaceName] = credentialName
		cfg.ModelProviders[providerKey] = providerDefinition
		cfg.ModelProviderBindings[providerKey] = binding
		targetCredential = credentialName
	} else if hasProviderDefinition {
		targetCredential = strings.TrimSpace(providerDefinition.Credential)
	}
	profiles := catalogProviderProfiles(cfg, provider)
	if len(profiles) == 0 {
		return fmt.Errorf("catalog provider %q publishes no models", provider)
	}
	if interfaceName == "" || (hasProviderDefinition && strings.EqualFold(interfaceName, providerDefinition.DefaultInterface)) {
		binding.SecretRef = ref
	}
	binding.Enabled = false
	cfg.ModelProviderBindings[providerKey] = binding
	for _, name := range profiles {
		profile := cfg.ModelProfiles[name]
		if targetCredential != "" && !strings.EqualFold(strings.TrimSpace(profile.Credential), targetCredential) {
			continue
		}
		if credential := strings.TrimSpace(profile.Credential); credential != "" {
			value := cfg.ModelCredentials[credential]
			value.APIKeyRef, value.Pending = ref, false
			cfg.ModelCredentials[credential] = value
		}
		profile.APIKeyRef = ref
		cfg.ModelProfiles[name] = profile
	}
	verified, failures := 0, make([]string, 0)
	attemptedInterfaceProfiles := map[string]map[string]bool{}
	for _, name := range profiles {
		profile := cfg.ModelProfiles[name]
		if targetCredential != "" && !strings.EqualFold(strings.TrimSpace(profile.Credential), targetCredential) {
			continue
		}
		if interfaceName != "" {
			if attemptedInterfaceProfiles[interfaceName] == nil {
				attemptedInterfaceProfiles[interfaceName] = map[string]bool{}
			}
			attemptedInterfaceProfiles[interfaceName][name] = true
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
		verifyErr := verifyAndStampTeamsModelProfile(ctx, &cfg, name, key)
		cancel()
		if verifyErr != nil {
			failures = append(failures, fmt.Sprintf("%s: %s", name, compactVerificationError(verifyErr, key)))
		}
	}
	// Verify every configured interface used by the provider's models. The
	// profile fingerprint records the ordinary/default route; alternate routes
	// are verified in a cloned profile so a DeepSeek Anthropic/Beta request is
	// actually sent through its selected converter instead of the default Chat
	// adapter. A missing interface key keeps the whole provider hidden.
	usage := catalogProviderInterfaceUsage(cfg, provider)
	defaultInterface := ""
	if hasProviderDefinition {
		defaultInterface = mapKeyFold(providerDefinition.Interfaces, providerDefinition.DefaultInterface)
	}
	for usedInterface, interfaceProfiles := range usage {
		ref := ""
		if configured := mapKeyFold(binding.InterfaceSecrets, usedInterface); configured != "" {
			ref = binding.InterfaceSecrets[configured]
		}
		if ref == "" && strings.EqualFold(usedInterface, defaultInterface) {
			ref = binding.SecretRef
		}
		interfaceKey, keyErr := modelprofile.ResolveAPIKey(ref, secretStore, os.Getenv)
		if keyErr != nil || strings.TrimSpace(interfaceKey) == "" {
			failures = append(failures, fmt.Sprintf("interface %s: key is not configured", usedInterface))
			continue
		}
		for _, name := range interfaceProfiles {
			if attemptedInterfaceProfiles[usedInterface] != nil && attemptedInterfaceProfiles[usedInterface][name] {
				continue
			}
			resolved, resolveErr := resolveCatalogProfileForInterface(cfg, name, usedInterface)
			if resolveErr != nil {
				failures = append(failures, fmt.Sprintf("%s (%s): %v", name, usedInterface, resolveErr))
				continue
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
			verifyErr := verifyConfiguredModelAuthenticationFn(ctx, resolved, interfaceKey)
			cancel()
			if verifyErr != nil {
				failures = append(failures, fmt.Sprintf("%s (%s): %s", name, usedInterface, compactVerificationError(verifyErr, interfaceKey)))
			}
		}
	}
	for _, name := range profiles {
		profile := cfg.ModelProfiles[name]
		if modelProfileVerificationCurrentIgnoringBinding(cfg, name, profile, secretStore) {
			verified++
		}
	}
	binding.Enabled = len(failures) == 0 && verified == len(profiles)
	cfg.ModelProviderBindings[providerKey] = binding
	if err := store.Save(cfg); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Provider %q: %d/%d model(s) verified and available.\n", provider, verified, len(profiles))
	for _, failure := range failures {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Provider verification failed: %s\n", failure)
	}
	if len(failures) > 0 {
		return fmt.Errorf("provider %q has %d unverified model(s)", provider, len(failures))
	}
	return nil
}

func resolveProviderAPIKey(cmd *cobra.Command, secrets *modelprofile.SecretStore, existing, provider string, opts modelProviderSetupOptions) (string, string, error) {
	if strings.TrimSpace(opts.apiKeyEnv) != "" {
		ref := modelprofile.EnvRefPrefix + strings.TrimSpace(opts.apiKeyEnv)
		key, err := modelprofile.ResolveAPIKey(ref, secrets, os.Getenv)
		return ref, key, err
	}
	if opts.apiKeyStdin {
		key, err := readModelProfileAPIKey(cmd.InOrStdin())
		if err != nil {
			return "", "", err
		}
		ref := providerSecretRef(provider)
		if err := secrets.Put(ref, key); err != nil {
			return "", "", err
		}
		return ref, key, nil
	}
	ref := strings.TrimSpace(existing)
	if ref != "" {
		key, err := modelprofile.ResolveAPIKey(ref, secrets, os.Getenv)
		if err == nil {
			return ref, key, nil
		}
	}
	if term.IsTerminal(int(os.Stdin.Fd())) {
		key, err := readModelProfileAPIKeyFromTerminal()
		if err != nil {
			return "", "", err
		}
		ref = providerSecretRef(provider)
		if err := secrets.Put(ref, key); err != nil {
			return "", "", err
		}
		return ref, key, nil
	}
	return "", "", fmt.Errorf("provider %s requires an API key; pass --api-key-env or --api-key-stdin", provider)
}

func modelProviderDoctor(out io.Writer, cfg config.Config, provider string) error {
	profiles := catalogProviderProfiles(cfg, provider)
	if len(profiles) == 0 {
		return fmt.Errorf("catalog provider %q is not configured", provider)
	}
	for _, name := range profiles {
		profile := cfg.ModelProfiles[name]
		status := "needs verification"
		if strings.TrimSpace(profile.VerificationFingerprint) != "" {
			status = "verified"
		}
		_, _ = fmt.Fprintf(out, "%s\t%s\n", name, status)
	}
	return nil
}

func catalogProviderProfiles(cfg config.Config, provider string) []string {
	names := make([]string, 0)
	for name, profile := range cfg.ModelProfiles {
		if strings.EqualFold(strings.TrimSpace(profile.Provider), provider) {
			if mapKeyFold(cfg.ModelCatalogs, profile.Source) != "" {
				names = append(names, name)
			}
		}
	}
	sort.Strings(names)
	return names
}

func printModelCatalogs(out io.Writer, cfg config.Config) {
	names := make([]string, 0, len(cfg.ModelCatalogs))
	for name := range cfg.ModelCatalogs {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		_, _ = fmt.Fprintln(out, "No model catalogs configured.")
		return
	}
	for _, name := range names {
		catalog := cfg.ModelCatalogs[name]
		providers := map[string]bool{}
		models := 0
		interfaces := 0
		for profileName, profile := range cfg.ModelProfiles {
			if strings.EqualFold(profile.Source, name) {
				providers[profile.Provider] = true
				if provider, ok := cfg.ModelProviders[profile.Provider]; ok {
					interfaces = maxInt(interfaces, len(provider.Interfaces))
				}
				_ = profileName
				models++
			}
		}
		_, _ = fmt.Fprintf(out, "%s\ttype=%s\tproviders=%d\tmodels=%d\tinterfaces=%d\trevision=%s\n", name, catalog.Type, len(providers), models, interfaces, shortRevision(catalog.Revision))
	}
}

func printModelProviders(out io.Writer, cfg config.Config) {
	printModelProvidersWithSecrets(out, cfg, nil)
}

func printModelProvidersWithSecrets(out io.Writer, cfg config.Config, secrets *modelprofile.SecretStore) {
	providers := make([]string, 0, len(cfg.ModelProviderBindings))
	for provider := range cfg.ModelProviderBindings {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	if len(providers) == 0 {
		_, _ = fmt.Fprintln(out, "No catalog providers configured.")
		return
	}
	for _, provider := range providers {
		binding := cfg.ModelProviderBindings[provider]
		profiles := catalogProviderProfiles(cfg, provider)
		verified := 0
		for _, name := range profiles {
			profile := cfg.ModelProfiles[name]
			verifiedNow := strings.TrimSpace(profile.VerificationFingerprint) != ""
			if secrets != nil {
				verifiedNow = verifiedNow && modelProfileVerificationCurrent(cfg, name, profile, secrets)
			}
			if verifiedNow {
				verified++
			}
		}
		status := "needs setup"
		if binding.Enabled && verified == len(profiles) {
			status = "ready"
		} else if verified > 0 {
			status = fmt.Sprintf("partial (%d/%d verified)", verified, len(profiles))
		}
		_, _ = fmt.Fprintf(out, "%s\tcatalog=%s\tmodels=%d\tinterface-keys=%d\tstatus=%s\n", provider, binding.Catalog, len(profiles), len(binding.InterfaceSecrets), status)
	}
}

// The Teams Control chat can inspect and refresh catalogs without accepting a
// raw token in chat. Provider activation deliberately remains a local CLI
// operation unless a secret is already present in the machine's secret store.
func (m teamsModelProfileManager) ListModelCatalogs(ctx context.Context) (string, error) {
	store, err := m.store()
	if err != nil {
		return "", err
	}
	cfg, err := store.Load()
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	printModelCatalogs(&out, cfg)
	return strings.TrimSpace(out.String()), nil
}

func (m teamsModelProfileManager) SyncModelCatalog(ctx context.Context, name string) (string, error) {
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	if err := runModelCatalogSync(cmd, m.root, name); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

func (m teamsModelProfileManager) ListModelProviders(ctx context.Context) (string, error) {
	store, err := m.store()
	if err != nil {
		return "", err
	}
	cfg, err := store.Load()
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	printModelProvidersWithSecrets(&out, cfg, secrets)
	return strings.TrimSpace(out.String()), nil
}

func (m teamsModelProfileManager) SetupModelProvider(ctx context.Context, provider string) (string, error) {
	store, err := m.store()
	if err != nil {
		return "", err
	}
	cfg, err := store.Load()
	if err != nil {
		return "", err
	}
	providerKey := mapKeyFold(cfg.ModelProviderBindings, provider)
	binding, ok := cfg.ModelProviderBindings[providerKey]
	if !ok {
		return "", fmt.Errorf("catalog provider %q is not configured", strings.TrimSpace(provider))
	}
	secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	if _, err := modelprofile.ResolveAPIKey(binding.SecretRef, secrets, os.Getenv); err != nil {
		return fmt.Sprintf("Provider %q is not activated yet. For safety, enter the key locally with `cxp model provider setup %s --api-key-stdin`; raw API keys are not accepted in Teams chat.", providerKey, providerKey), nil
	}
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	if err := runModelProviderSetup(cmd, m.root, providerKey, modelProviderSetupOptions{timeout: 20 * time.Second}); err != nil {
		return strings.TrimSpace(out.String() + "\n" + errOut.String()), err
	}
	return strings.TrimSpace(out.String()), nil
}

func providerSecretRef(provider string) string {
	return modelprofile.SecretRefForCredentialScope("catalog-provider/" + strings.TrimSpace(provider))
}

func interfaceScopeSuffix(interfaceName string) string {
	if strings.TrimSpace(interfaceName) == "" {
		return ""
	}
	return "/" + strings.TrimSpace(interfaceName)
}
func catalogCredentialName(catalog, provider string) string {
	return "catalog/" + strings.TrimSpace(catalog) + "/" + strings.TrimSpace(provider)
}

func catalogCredentialNameForInterface(catalog, provider, interfaceName, defaultInterface string) string {
	base := catalogCredentialName(catalog, provider)
	if strings.TrimSpace(interfaceName) == "" || strings.EqualFold(strings.TrimSpace(interfaceName), strings.TrimSpace(defaultInterface)) {
		return base
	}
	return base + "/" + strings.TrimSpace(interfaceName)
}
func cleanCatalogManifest(value string) string {
	value = filepath.ToSlash(filepath.Clean(strings.TrimSpace(value)))
	if value == "." || value == "" {
		return defaultModelCatalogManifest
	}
	return value
}
func shortRevision(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 12 {
		return value[:12]
	}
	if value == "" {
		return "-"
	}
	return value
}

func mapKeyFold[T any](values map[string]T, ref string) string {
	for key := range values {
		if strings.EqualFold(strings.TrimSpace(key), strings.TrimSpace(ref)) {
			return key
		}
	}
	return ""
}

func modelCatalogDocumentRevision(raw []byte) string {
	// The revision is a content identity for managed JSON. It is deliberately
	// not a secret or an externally meaningful Git commit.
	return modelprofile.Fingerprint(string(raw))
}

func writeManagedCatalog(configPath, relative string, raw []byte) error {
	path, err := safeManagedCatalogPath(configPath, relative)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".catalog-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func readManagedCatalog(configPath, relative string) (modelcatalog.Document, error) {
	path, err := safeManagedCatalogPath(configPath, relative)
	if err != nil {
		return modelcatalog.Document{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return modelcatalog.Document{}, err
	}
	doc, err := modelcatalog.Parse(raw)
	if err != nil {
		return modelcatalog.Document{}, fmt.Errorf("parse managed catalog: %w", err)
	}
	return doc, nil
}

func safeManagedCatalogPath(configPath, relative string) (string, error) {
	root := filepath.Dir(configPath)
	relative = filepath.Clean(strings.TrimSpace(relative))
	if relative == "." || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("managed catalog file must stay below the config directory")
	}
	path := filepath.Join(root, relative)
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("managed catalog file escapes the config directory")
	}
	return path, nil
}
