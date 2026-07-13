package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
)

const defaultModelSourceFile = "cxp-models.json"

const defaultModelSourceAutoSyncInterval = 30 * time.Minute

var verifyConfiguredModelAuthenticationFn = verifySyncedModel
var runModelSourceSyncFn = runModelSourceSync

type modelSourceSyncOptions struct{ name, ref, file string }
type modelSourceBindOptions struct {
	apiKeyEnv   string
	apiKeyStdin bool
	timeout     time.Duration
}

func newModelSourceCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{Use: "model-source", Aliases: []string{"models-repo"}, Short: "Sync and activate model profiles from a Git repository"}
	cmd.AddCommand(newModelSourceSyncCmd(root), newModelSourceListCmd(root), newModelSourceBindCmd(root))
	return cmd
}

func newModelSourceSyncCmd(root *rootOptions) *cobra.Command {
	opts := modelSourceSyncOptions{file: defaultModelSourceFile}
	cmd := &cobra.Command{
		Use: "sync <repository-or-source>", Short: "Shallow-sync model candidates without requiring a key", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error { return runModelSourceSync(cmd, root, args[0], opts) },
	}
	cmd.Flags().StringVar(&opts.name, "name", "", "Local source name (derived from the repository when omitted)")
	cmd.Flags().StringVar(&opts.ref, "ref", "", "Git branch, tag, or commit (repository default when omitted)")
	cmd.Flags().StringVar(&opts.file, "file", defaultModelSourceFile, "Repository-relative model config file")
	return cmd
}

func newModelSourceListCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{Use: "list", Short: "List synced candidates and verification state", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		store, _, err := newRootStore(root, "")
		if err != nil {
			return err
		}
		cfg, err := store.Load()
		if err != nil {
			return err
		}
		printModelSources(cmd.OutOrStdout(), cfg)
		return nil
	}}
}

func newModelSourceBindCmd(root *rootOptions) *cobra.Command {
	opts := modelSourceBindOptions{timeout: 20 * time.Second}
	cmd := &cobra.Command{
		Use: "bind <source> <profile>", Short: "Bind a key and verify one synced profile before exposing it", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runModelSourceBind(cmd, root, args[0], args[1], opts)
		},
	}
	cmd.Flags().StringVar(&opts.apiKeyEnv, "api-key-env", "", "Environment variable containing the key")
	cmd.Flags().BoolVar(&opts.apiKeyStdin, "api-key-stdin", false, "Read and save the key from stdin")
	cmd.Flags().DurationVar(&opts.timeout, "timeout", 20*time.Second, "Verification timeout")
	return cmd
}

func runModelSourceSync(cmd *cobra.Command, root *rootOptions, ref string, opts modelSourceSyncOptions) (retErr error) {
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	name, source, err := resolveModelSource(cfg, ref, opts)
	if err != nil {
		return err
	}
	attemptedRevision := ""
	defer func() {
		if retErr != nil {
			_ = recordModelSourceBackupFailure(store, name, attemptedRevision, retErr)
		}
	}()
	cacheRoot := filepath.Join(filepath.Dir(store.Path()), "model-sources")
	if err := os.MkdirAll(cacheRoot, 0o700); err != nil {
		return err
	}
	syncLock := flock.New(filepath.Join(cacheRoot, "."+name+".sync.lock"))
	locked, err := syncLock.TryLockContext(cmd.Context(), 100*time.Millisecond)
	if err != nil {
		return fmt.Errorf("lock model source %q sync: %w", name, err)
	}
	if !locked {
		return fmt.Errorf("lock model source %q sync: context ended before the lock became available", name)
	}
	defer func() { _ = syncLock.Unlock() }()
	staging, err := os.MkdirTemp(cacheRoot, ".sync-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	repoDir := filepath.Join(staging, "repo")
	args := []string{"clone", "--depth", "1", "--filter=blob:none", "--no-checkout"}
	args = append(args, "--", source.URL, repoDir)
	git := exec.CommandContext(cmd.Context(), "git", args...)
	if raw, cloneErr := git.CombinedOutput(); cloneErr != nil {
		return fmt.Errorf("sync model source %q: %w: %s", name, cloneErr, strings.TrimSpace(string(raw)))
	}
	if source.Ref != "" {
		fetch := exec.CommandContext(cmd.Context(), "git", "-C", repoDir, "fetch", "--depth", "1", "origin", source.Ref)
		if raw, fetchErr := fetch.CombinedOutput(); fetchErr != nil {
			return fmt.Errorf("fetch ref %q for model source %q: %w: %s", source.Ref, name, fetchErr, strings.TrimSpace(string(raw)))
		}
		checkout := exec.CommandContext(cmd.Context(), "git", "-C", repoDir, "checkout", "--detach", "FETCH_HEAD")
		if raw, checkoutErr := checkout.CombinedOutput(); checkoutErr != nil {
			return fmt.Errorf("checkout ref %q for model source %q: %w: %s", source.Ref, name, checkoutErr, strings.TrimSpace(string(raw)))
		}
	} else {
		checkout := exec.CommandContext(cmd.Context(), "git", "-C", repoDir, "checkout", "--detach", "HEAD")
		if raw, checkoutErr := checkout.CombinedOutput(); checkoutErr != nil {
			return fmt.Errorf("checkout model source %q: %w: %s", name, checkoutErr, strings.TrimSpace(string(raw)))
		}
	}
	revisionRaw, err := exec.CommandContext(cmd.Context(), "git", "-C", repoDir, "rev-parse", "HEAD").Output()
	if err != nil {
		return fmt.Errorf("read synced revision: %w", err)
	}
	attemptedRevision = strings.TrimSpace(string(revisionRaw))
	manifestPath, err := safeRepoFile(repoDir, source.File)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", source.File, err)
	}
	fragment, err := config.ParseModelConfigFragment(raw)
	if err != nil {
		return fmt.Errorf("validate %s: %w", source.File, err)
	}
	source.Revision = attemptedRevision
	source.SyncedAt = time.Now().UTC()
	source.BackupActive = false
	source.BackupSince = time.Time{}
	source.BackupFailedAt = time.Time{}
	source.BackupAttemptedRevision = ""
	source.BackupReason = ""
	if err := mergeModelSource(&cfg, name, source, fragment); err != nil {
		return err
	}
	source = cfg.ModelSources[name]
	finalDir := filepath.Join(cacheRoot, name)
	backup := finalDir + ".old"
	_ = os.RemoveAll(backup)
	if _, statErr := os.Stat(finalDir); statErr == nil {
		if err := os.Rename(finalDir, backup); err != nil {
			return err
		}
	}
	if err := os.Rename(repoDir, finalDir); err != nil {
		_ = os.Rename(backup, finalDir)
		return err
	}
	var committed config.Config
	if err := store.Update(func(current *config.Config) error {
		if err := mergeModelSource(current, name, source, fragment); err != nil {
			return err
		}
		committed = *current
		return nil
	}); err != nil {
		_ = os.RemoveAll(finalDir)
		_ = os.Rename(backup, finalDir)
		return err
	}
	cfg = committed
	source = cfg.ModelSources[name]
	if warnings, verifyErr := reverifyUpdatedSourceProfiles(cmd.Context(), store, name, source.Revision); verifyErr != nil {
		return verifyErr
	} else {
		for _, warning := range warnings {
			_, _ = fmt.Fprintln(cmd.ErrOrStderr(), warning)
		}
	}
	if refreshed, loadErr := store.Load(); loadErr == nil {
		cfg = refreshed
		source = cfg.ModelSources[name]
	}
	_ = os.RemoveAll(backup)
	verified := 0
	pending := ""
	for _, profileName := range source.Profiles {
		if strings.TrimSpace(cfg.ModelProfiles[profileName].VerificationFingerprint) != "" {
			verified++
		} else if pending == "" {
			pending = profileName
		}
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Synced %s at %.12s: %d profile(s), %d verified and available.\n", name, source.Revision, len(source.Profiles), verified)
	if pending != "" {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Next: cxp model-source bind %s %s --api-key-stdin\n", name, pending)
	}
	return nil
}

func recordModelSourceBackupFailure(store *config.Store, name, attemptedRevision string, syncErr error) error {
	if store == nil || strings.TrimSpace(name) == "" || syncErr == nil {
		return nil
	}
	now := time.Now().UTC()
	return store.Update(func(cfg *config.Config) error {
		source, ok := cfg.ModelSources[name]
		// There is no backup JSON until at least one revision was successfully
		// activated. Initial sync failures remain ordinary setup failures.
		if !ok || strings.TrimSpace(source.Revision) == "" {
			return nil
		}
		if !source.BackupActive || source.BackupSince.IsZero() {
			source.BackupSince = now
		}
		source.BackupActive = true
		source.BackupFailedAt = now
		source.BackupAttemptedRevision = strings.TrimSpace(attemptedRevision)
		source.BackupReason = compactVerificationError(syncErr)
		cfg.ModelSources[name] = source
		return nil
	})
}

type sourceProfileVerificationResult struct {
	name        string
	verifiedAt  time.Time
	fingerprint string
	errorText   string
}

func reverifyUpdatedSourceProfiles(ctx context.Context, store *config.Store, sourceName, expectedRevision string) ([]string, error) {
	if store == nil {
		return nil, nil
	}
	cfg, err := store.Load()
	if err != nil {
		return nil, err
	}
	source, ok := cfg.ModelSources[sourceName]
	if !ok || source.Revision != expectedRevision {
		return nil, nil
	}
	secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	results := make([]sourceProfileVerificationResult, 0)
	var warnings []string
	for _, name := range source.Profiles {
		profile := cfg.ModelProfiles[name]
		if modelProfileVerificationCurrent(cfg, name, profile, secrets) {
			continue
		}
		resolved, resolveErr := modelprofile.Resolve(cfg, name)
		if resolveErr != nil {
			continue
		}
		apiKey, keyErr := modelprofile.ResolveAPIKey(resolved.Profile.APIKeyRef, secrets, os.Getenv)
		if keyErr != nil {
			// A freshly synced pending credential is expected. The user can bind
			// it later; do not turn normal key-less sync into an error.
			continue
		}
		verifyCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		verifyErr := verifyConfiguredModelAuthenticationFn(verifyCtx, resolved, apiKey)
		cancel()
		result := sourceProfileVerificationResult{name: name}
		if verifyErr != nil {
			result.errorText = compactVerificationError(verifyErr, apiKey)
			warnings = append(warnings, fmt.Sprintf("Model source auto-verification warning: %s remains hidden: %s", name, result.errorText))
		} else {
			result.verifiedAt = time.Now().UTC()
			result.fingerprint = modelVerificationFingerprint(expectedRevision, resolved, apiKey)
		}
		results = append(results, result)
	}
	err = store.Update(func(current *config.Config) error {
		currentSource, ok := current.ModelSources[sourceName]
		if !ok || currentSource.Revision != expectedRevision {
			return nil
		}
		for _, result := range results {
			profile, ok := current.ModelProfiles[result.name]
			if !ok || !strings.EqualFold(profile.Source, sourceName) {
				continue
			}
			profile.VerifiedAt = result.verifiedAt
			profile.VerificationFingerprint = result.fingerprint
			profile.VerificationError = result.errorText
			current.ModelProfiles[result.name] = profile
		}
		repairUnavailableSourceDefault(current)
		return nil
	})
	return warnings, err
}

// runModelSourceAutoSyncLoop keeps repository-backed model configuration fresh
// for long-lived processes. It deliberately reuses the interactive sync path:
// a failed clone, checkout, schema validation, merge, or config save therefore
// leaves the last successfully synced repository and config untouched.
func runModelSourceAutoSyncLoop(ctx context.Context, root *rootOptions, errOut io.Writer, interval time.Duration) {
	if interval <= 0 {
		interval = defaultModelSourceAutoSyncInterval
	}
	for {
		delay := runModelSourceAutoSyncOnce(ctx, root, errOut, time.Now().UTC(), interval)
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

func runModelSourceAutoSyncOnce(ctx context.Context, root *rootOptions, errOut io.Writer, now time.Time, interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = defaultModelSourceAutoSyncInterval
	}
	store, _, err := newRootStore(root, "")
	if err != nil {
		modelSourceAutoSyncWarning(errOut, "open config", err)
		return interval
	}
	cfg, err := store.Load()
	if err != nil {
		modelSourceAutoSyncWarning(errOut, "load config", err)
		return interval
	}
	names := dueModelSources(cfg, now, interval)
	hadFailure := false
	for _, name := range names {
		if ctx.Err() != nil {
			return interval
		}
		cmd := &cobra.Command{}
		cmd.SetContext(ctx)
		cmd.SetOut(io.Discard)
		cmd.SetErr(errOut)
		if err := runModelSourceSyncFn(cmd, root, name, modelSourceSyncOptions{}); err != nil {
			hadFailure = true
			if storeErr := recordModelSourceBackupFailure(store, name, "", err); storeErr != nil {
				modelSourceAutoSyncWarning(errOut, fmt.Sprintf("record backup state for %s", name), storeErr)
			}
			modelSourceAutoSyncWarning(errOut, fmt.Sprintf("sync %s", name), err)
		}
	}
	// Reload after successful syncs so the next timer is based on the SyncedAt
	// value committed by the sync path rather than the pre-sync snapshot.
	if len(names) > 0 {
		if refreshed, loadErr := store.Load(); loadErr == nil {
			cfg = refreshed
		} else {
			modelSourceAutoSyncWarning(errOut, "reload config", loadErr)
		}
	}
	if hadFailure {
		// A stale SyncedAt must not turn a transient Git/network failure into a
		// tight retry loop. The last-known-good config remains active and the
		// failed source is tried again on the next normal interval.
		return interval
	}
	return nextModelSourceAutoSyncDelay(cfg, time.Now().UTC(), interval)
}

func dueModelSources(cfg config.Config, now time.Time, interval time.Duration) []string {
	if interval <= 0 {
		interval = defaultModelSourceAutoSyncInterval
	}
	names := make([]string, 0, len(cfg.ModelSources))
	for name, source := range cfg.ModelSources {
		if source.SyncedAt.IsZero() || !source.SyncedAt.Add(interval).After(now) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func nextModelSourceAutoSyncDelay(cfg config.Config, now time.Time, interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = defaultModelSourceAutoSyncInterval
	}
	if len(cfg.ModelSources) == 0 {
		return interval
	}
	delay := interval
	for _, source := range cfg.ModelSources {
		if source.SyncedAt.IsZero() {
			return time.Millisecond
		}
		candidate := source.SyncedAt.Add(interval).Sub(now)
		if candidate <= 0 {
			return time.Millisecond
		}
		if candidate < delay {
			delay = candidate
		}
	}
	return delay
}

func modelSourceAutoSyncWarning(out io.Writer, operation string, err error) {
	if out == nil || err == nil {
		return
	}
	_, _ = fmt.Fprintf(out, "Model source auto-sync warning: %s: %v\n", operation, err)
}

func resolveModelSource(cfg config.Config, ref string, opts modelSourceSyncOptions) (string, config.ModelSource, error) {
	ref = strings.TrimSpace(ref)
	for name, source := range cfg.ModelSources {
		if strings.EqualFold(name, ref) {
			if opts.ref != "" {
				source.Ref = strings.TrimSpace(opts.ref)
			}
			if opts.file != "" {
				source.File = strings.TrimSpace(opts.file)
			}
			return name, source, nil
		}
	}
	if ref == "" || strings.HasPrefix(ref, "-") {
		return "", config.ModelSource{}, fmt.Errorf("repository URL is required")
	}
	if parsed, err := url.Parse(ref); err == nil && parsed.User != nil {
		return "", config.ModelSource{}, fmt.Errorf("repository URLs containing credentials are not accepted; use the Git credential helper or SSH authentication")
	}
	name := strings.TrimSpace(opts.name)
	if name == "" {
		name = modelSourceName(ref)
	}
	if err := validateModelSourceName(name); err != nil {
		return "", config.ModelSource{}, err
	}
	file := strings.TrimSpace(opts.file)
	if file == "" {
		file = defaultModelSourceFile
	}
	return name, config.ModelSource{URL: ref, Ref: strings.TrimSpace(opts.ref), File: file}, nil
}

func modelSourceName(raw string) string {
	value := strings.TrimSuffix(strings.TrimRight(raw, "/"), ".git")
	if i := strings.LastIndexAny(value, "/:"); i >= 0 {
		value = value[i+1:]
	}
	return strings.ToLower(strings.TrimSpace(value))
}

func validateModelSourceName(name string) error {
	if name == "" || name == "." || name == ".." {
		return fmt.Errorf("model source name is empty")
	}
	for _, r := range name {
		if !(r == '-' || r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return fmt.Errorf("model source name %q contains unsupported characters", name)
		}
	}
	return nil
}

func safeRepoFile(repoDir, name string) (string, error) {
	name = filepath.Clean(strings.TrimSpace(name))
	if name == "." || filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("model config file must be a safe repository-relative path")
	}
	path := filepath.Join(repoDir, name)
	realRepo, err := filepath.EvalSymlinks(repoDir)
	if err != nil {
		return "", err
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(realRepo, real)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("model config file escapes the repository")
	}
	return real, nil
}

func mergeModelSource(cfg *config.Config, name string, source config.ModelSource, fragment config.Config) error {
	if cfg.ModelSources == nil {
		cfg.ModelSources = map[string]config.ModelSource{}
	}
	old := cfg.ModelSources[name]
	type verificationBasis struct {
		modelFingerprint string
		apiKeyRef        string
	}
	oldVerificationBasis := map[string]verificationBasis{}
	for _, profileName := range old.Profiles {
		if resolved, err := modelprofile.Resolve(*cfg, profileName); err == nil {
			oldVerificationBasis[profileName] = verificationBasis{
				modelFingerprint: modelVerificationConfigurationFingerprint(resolved),
				apiKeyRef:        strings.TrimSpace(resolved.Profile.APIKeyRef),
			}
		}
	}
	source.Credentials, source.Providers, source.Models, source.Profiles = nil, nil, nil, nil
	owned := func(values []string, key string) bool {
		for _, value := range values {
			if strings.EqualFold(value, key) {
				return true
			}
		}
		return false
	}
	check := func(kind string, incoming any, existing bool, oldNames []string, key string) error {
		if existing && !owned(oldNames, key) {
			return fmt.Errorf("%s %q conflicts with local or another source", kind, key)
		}
		_ = incoming
		return nil
	}
	for key, value := range fragment.ModelCredentials {
		if err := check("credential", value, cfg.ModelCredentials != nil && hasCredential(cfg.ModelCredentials, key), old.Credentials, key); err != nil {
			return err
		}
	}
	for key, value := range fragment.ModelProviders {
		if err := check("provider", value, cfg.ModelProviders != nil && hasProvider(cfg.ModelProviders, key), old.Providers, key); err != nil {
			return err
		}
	}
	for key, value := range fragment.Models {
		if err := check("model", value, cfg.Models != nil && hasModel(cfg.Models, key), old.Models, key); err != nil {
			return err
		}
	}
	for key, value := range fragment.ModelProfiles {
		if err := check("profile", value, cfg.ModelProfiles != nil && hasProfile(cfg.ModelProfiles, key), old.Profiles, key); err != nil {
			return err
		}
	}
	oldCredentials := map[string]config.ModelCredential{}
	oldProfiles := map[string]config.ModelProfile{}
	for _, key := range old.Credentials {
		if value, ok := cfg.ModelCredentials[key]; ok {
			oldCredentials[key] = value
		}
		delete(cfg.ModelCredentials, key)
	}
	for _, key := range old.Providers {
		delete(cfg.ModelProviders, key)
	}
	for _, key := range old.Models {
		delete(cfg.Models, key)
	}
	for _, key := range old.Profiles {
		if value, ok := cfg.ModelProfiles[key]; ok {
			oldProfiles[key] = value
		}
		delete(cfg.ModelProfiles, key)
	}
	if cfg.ModelCredentials == nil {
		cfg.ModelCredentials = map[string]config.ModelCredential{}
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
	for key, value := range fragment.ModelCredentials {
		if previous, ok := oldCredentials[key]; ok && previous.APIKeyRef != "" {
			value.APIKeyRef = previous.APIKeyRef
			value.Pending = false
		}
		cfg.ModelCredentials[key] = value
		source.Credentials = append(source.Credentials, key)
	}
	for key, value := range fragment.ModelProviders {
		cfg.ModelProviders[key] = value
		source.Providers = append(source.Providers, key)
	}
	for key, value := range fragment.Models {
		cfg.Models[key] = value
		source.Models = append(source.Models, key)
	}
	for key, value := range fragment.ModelProfiles {
		value.Source = name
		if value.Revision <= 0 {
			value.Revision = 1
		}
		cfg.ModelProfiles[key] = value
		source.Profiles = append(source.Profiles, key)
	}
	// Preserve authentication proof across Git commits when the effective
	// provider/model configuration and credential binding are unchanged.
	for key, previous := range oldProfiles {
		oldBasis, ok := oldVerificationBasis[key]
		if !ok || strings.TrimSpace(previous.VerificationFingerprint) == "" {
			continue
		}
		resolved, err := modelprofile.Resolve(*cfg, key)
		if err != nil {
			continue
		}
		newBasis := verificationBasis{
			modelFingerprint: modelVerificationConfigurationFingerprint(resolved),
			apiKeyRef:        strings.TrimSpace(resolved.Profile.APIKeyRef),
		}
		if oldBasis != newBasis {
			continue
		}
		value := cfg.ModelProfiles[key]
		value.VerifiedAt = previous.VerifiedAt
		value.VerificationFingerprint = previous.VerificationFingerprint
		value.VerificationError = previous.VerificationError
		cfg.ModelProfiles[key] = value
	}
	sort.Strings(source.Credentials)
	sort.Strings(source.Providers)
	sort.Strings(source.Models)
	sort.Strings(source.Profiles)
	cfg.ModelConfigVersion = config.CurrentModelConfigVersion
	cfg.ModelSources[name] = source
	repairDefaultModelProfileAfterSourceMerge(cfg)
	return config.ValidateModelConfig(*cfg)
}

func repairDefaultModelProfileAfterSourceMerge(cfg *config.Config) {
	if cfg == nil {
		return
	}
	if name := typedDefaultModelProfileName(cfg); name != "" {
		if _, ok := cfg.FindModelProfile(name); !ok {
			clearTypedDefaultModelProfile(cfg, name)
			if strings.EqualFold(strings.TrimSpace(cfg.DefaultModelProfile), name) {
				cfg.DefaultModelProfile = config.DefaultModelProfileName
			}
		}
	}
	name := strings.TrimSpace(cfg.DefaultModelProfile)
	if name == "" || strings.EqualFold(name, config.DefaultModelProfileName) {
		return
	}
	_, ok := cfg.FindModelProfile(name)
	if !ok {
		cfg.DefaultModelProfile = config.DefaultModelProfileName
		clearTypedDefaultModelProfile(cfg, name)
	}
}

func repairUnavailableSourceDefault(cfg *config.Config) {
	if cfg == nil {
		return
	}
	if name := typedDefaultModelProfileName(cfg); name != "" {
		profile, ok := cfg.FindModelProfile(name)
		if !ok || (strings.TrimSpace(profile.Source) != "" && strings.TrimSpace(profile.VerificationFingerprint) == "") {
			clearTypedDefaultModelProfile(cfg, name)
			if strings.EqualFold(strings.TrimSpace(cfg.DefaultModelProfile), name) {
				cfg.DefaultModelProfile = config.DefaultModelProfileName
			}
		}
	}
	name := strings.TrimSpace(cfg.DefaultModelProfile)
	if name == "" || strings.EqualFold(name, config.DefaultModelProfileName) {
		return
	}
	profile, ok := cfg.FindModelProfile(name)
	if !ok || (strings.TrimSpace(profile.Source) != "" && strings.TrimSpace(profile.VerificationFingerprint) == "") {
		cfg.DefaultModelProfile = config.DefaultModelProfileName
		clearTypedDefaultModelProfile(cfg, name)
	}
}

func typedDefaultModelProfileName(cfg *config.Config) string {
	if cfg == nil || cfg.Defaults == nil {
		return ""
	}
	selector := strings.TrimSpace(cfg.Defaults.Model)
	if !strings.HasPrefix(strings.ToLower(selector), "profile:") {
		return ""
	}
	_, name, _ := strings.Cut(selector, ":")
	return strings.TrimSpace(name)
}

func clearTypedDefaultModelProfile(cfg *config.Config, name string) {
	if cfg == nil || cfg.Defaults == nil {
		return
	}
	selector := strings.TrimSpace(cfg.Defaults.Model)
	typedProfileMatches := false
	if strings.HasPrefix(strings.ToLower(selector), "profile:") {
		_, profileName, _ := strings.Cut(selector, ":")
		typedProfileMatches = strings.EqualFold(strings.TrimSpace(profileName), strings.TrimSpace(name))
	}
	if selector == "" || typedProfileMatches {
		cfg.Defaults.Model = ""
		cfg.Defaults.ReasoningEffort = ""
		cfg.PruneEmptyGlobalDefaults()
	}
}

// Lookup helpers intentionally use exact source-owned names; strict sync rejects collisions.
func hasCredential(m map[string]config.ModelCredential, k string) bool { _, ok := m[k]; return ok }
func hasProvider(m map[string]config.ModelProvider, k string) bool     { _, ok := m[k]; return ok }
func hasModel(m map[string]config.ModelDefinition, k string) bool      { _, ok := m[k]; return ok }
func hasProfile(m map[string]config.ModelProfile, k string) bool       { _, ok := m[k]; return ok }
func containsFold(values []string, value string) bool {
	for _, item := range values {
		if strings.EqualFold(item, value) {
			return true
		}
	}
	return false
}

func runModelSourceBind(cmd *cobra.Command, root *rootOptions, sourceName, profileName string, opts modelSourceBindOptions) error {
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	source, ok := findSource(cfg, sourceName)
	if !ok {
		return fmt.Errorf("model source %q not found; run `cxp model-source list`", sourceName)
	}
	canonical, profile, ok := findSourceProfile(cfg, source, profileName)
	if !ok {
		return fmt.Errorf("profile %q is not provided by source %q", profileName, sourceName)
	}
	_, definition, ok := config.FindModelDefinition(cfg, profile.Model)
	if !ok {
		return fmt.Errorf("profile %q references missing model %q", canonical, profile.Model)
	}
	provider, ok := cfg.ModelProviders[definition.Provider]
	if !ok {
		return fmt.Errorf("model provider %q not found", definition.Provider)
	}
	credentialName := strings.TrimSpace(profile.Credential)
	if credentialName == "" {
		credentialName = strings.TrimSpace(provider.Credential)
	}
	credential, ok := cfg.ModelCredentials[credentialName]
	if !ok {
		return fmt.Errorf("profile %q has no credential slot", canonical)
	}
	secrets := modelprofile.NewSecretStore(modelprofile.SecretPathForConfig(store.Path()))
	apiKeyRef, apiKey, err := modelSourceKey(cmd, secrets, sourceName, credentialName, credential.APIKeyRef, opts)
	if err != nil {
		return err
	}
	credential.APIKeyRef, credential.Pending = apiKeyRef, false
	cfg.ModelCredentials[credentialName] = credential
	profile.Credential, profile.Provider = credentialName, definition.Provider
	profile.VerificationFingerprint, profile.VerificationError = "", ""
	cfg.ModelProfiles[canonical] = profile
	resolved, err := modelprofile.Resolve(cfg, canonical)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
	defer cancel()
	if err := verifyConfiguredModelAuthenticationFn(ctx, resolved, apiKey); err != nil {
		profile.VerificationError = compactVerificationError(err, apiKey)
		cfg.ModelProfiles[canonical] = profile
		if saveErr := store.Save(cfg); saveErr != nil {
			return fmt.Errorf("verification failed (%v), then save failed: %w", err, saveErr)
		}
		return fmt.Errorf("profile %q verification failed and remains hidden: %w", canonical, err)
	}
	profile.VerifiedAt = time.Now().UTC()
	profile.VerificationError = ""
	profile.VerificationFingerprint = modelVerificationFingerprint(source.Revision, resolved, apiKey)
	profile.UpdatedAt = profile.VerifiedAt
	cfg.ModelProfiles[canonical] = profile
	if err := store.Save(cfg); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Verified %s/%s. It is now available in Codex CLI, App, and Teams model lists.\n", sourceName, canonical)
	return nil
}

func modelSourceKey(cmd *cobra.Command, secrets *modelprofile.SecretStore, source, credential, existing string, opts modelSourceBindOptions) (string, string, error) {
	if opts.apiKeyEnv != "" && opts.apiKeyStdin {
		return "", "", fmt.Errorf("pass only one of --api-key-env or --api-key-stdin")
	}
	if opts.apiKeyEnv != "" {
		ref := modelprofile.EnvRefPrefix + strings.TrimSpace(opts.apiKeyEnv)
		key, err := modelprofile.ResolveAPIKey(ref, secrets, os.Getenv)
		return ref, key, err
	}
	ref := modelprofile.SecretRefForCredentialScope("repo/" + source + "/" + credential)
	if opts.apiKeyStdin {
		key, err := readModelProfileAPIKey(cmd.InOrStdin())
		if err != nil {
			return "", "", err
		}
		if err := secrets.Put(ref, key); err != nil {
			return "", "", err
		}
		return ref, key, nil
	}
	if existing != "" {
		if key, err := modelprofile.ResolveAPIKey(existing, secrets, os.Getenv); err == nil {
			return existing, key, nil
		}
	}
	if term.IsTerminal(int(os.Stdin.Fd())) {
		key, err := readModelProfileAPIKeyFromTerminal()
		if err != nil {
			return "", "", err
		}
		if err := secrets.Put(ref, key); err != nil {
			return "", "", err
		}
		return ref, key, nil
	}
	return "", "", fmt.Errorf("a key is required; pass --api-key-stdin or --api-key-env <NAME>")
}

func verifySyncedModel(ctx context.Context, resolved modelprofile.Resolved, apiKey string) error {
	if resolved.Provider.DirectResponses {
		return verifyNativeResponsesModel(ctx, resolved, apiKey)
	}
	client := responsesadapter.NewUpstreamHTTPClient(nil)
	client.Timeout = 20 * time.Second
	max := 8
	adapter := responsesadapter.OpenAIChatAdapter{BaseURL: resolved.Provider.BaseURL, APIKey: apiKey, HTTPClient: client, MaxRetries: -1, MaxOutputTokens: max, Headers: resolved.Provider.Headers, AuthType: resolved.Provider.AuthType, AuthHeader: resolved.Provider.AuthHeader, StreamMode: resolved.Model.StreamPolicy.UpstreamMode, ReasoningDeltaPath: resolved.Model.StreamPolicy.ReasoningDeltaPath, CachedTokensPath: resolved.Model.StreamPolicy.CachedTokensPath, UsageField: resolved.Model.CachePolicy.UsageField, Profile: responsesadapter.ProfileForProvider(resolved.Provider.AdapterProfile).WithModelPolicies(resolved.Model.ReasoningPolicy, resolved.Model.ToolPolicy, resolved.Model.MessagePolicy, resolved.Model.SamplingPolicy)}
	stream, err := adapter.Stream(ctx, responsesadapter.ProviderRequest{Model: resolved.Model.UpstreamModel(), InputText: "Reply with OK.", MaxOutputTokens: &max, ReasoningEffort: resolved.Provider.DefaultReasoningEffort})
	if err != nil {
		return err
	}
	done := false
	for event := range stream {
		if event.Kind == responsesadapter.ProviderEventError {
			return event.Err
		}
		if event.Kind == responsesadapter.ProviderEventDone {
			done = true
		}
	}
	if !done {
		return fmt.Errorf("upstream stream ended without completion")
	}
	return nil
}

func verifyNativeResponsesModel(ctx context.Context, resolved modelprofile.Resolved, apiKey string) error {
	endpoint := strings.TrimRight(resolved.Provider.BaseURL, "/") + "/responses"
	payload, _ := json.Marshal(map[string]any{"model": resolved.Model.UpstreamModel(), "input": "Reply with OK.", "max_output_tokens": 8, "stream": false})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range resolved.Provider.Headers {
		req.Header.Set(key, value)
	}
	if strings.EqualFold(resolved.Provider.AuthType, "header") {
		header := resolved.Provider.AuthHeader
		if header == "" {
			header = "X-API-Key"
		}
		req.Header.Set(header, apiKey)
	} else {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("upstream returned %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return nil
}

func modelVerificationFingerprint(revision string, resolved modelprofile.Resolved, key string) string {
	// Repository revisions are intentionally excluded. A documentation-only or
	// unrelated profile update must not hide an otherwise unchanged, previously
	// verified model. ModelFingerprint still invalidates verification whenever
	// the effective provider/model compatibility configuration changes.
	_ = revision
	sum := sha256.Sum256([]byte(modelVerificationConfigurationFingerprint(resolved) + "\x00" + modelprofile.Fingerprint(key)))
	return hex.EncodeToString(sum[:])[:20]
}

func modelVerificationConfigurationFingerprint(resolved modelprofile.Resolved) string {
	provider := resolved.Provider
	provider.DefaultModel = resolved.Model.PublicID()
	provider.Models = []modelprofile.ModelSpec{resolved.Model}
	raw, err := json.Marshal(struct {
		Provider modelprofile.ProviderSpec `json:"provider"`
		SSHProxy string                    `json:"sshProxy,omitempty"`
	}{Provider: provider, SSHProxy: strings.TrimSpace(resolved.Profile.SSHProxy)})
	if err != nil {
		return modelprofile.ModelFingerprint(resolved.Provider, resolved.Model.PublicID())
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func modelProfileVerificationCurrent(cfg config.Config, name string, profile config.ModelProfile, secrets *modelprofile.SecretStore) bool {
	if strings.TrimSpace(profile.VerificationFingerprint) == "" {
		return false
	}
	resolved, err := modelprofile.Resolve(cfg, name)
	if err != nil {
		return false
	}
	apiKey, err := modelprofile.ResolveAPIKey(resolved.Profile.APIKeyRef, secrets, os.Getenv)
	if err != nil {
		return false
	}
	revision := ""
	if source, ok := findSource(cfg, profile.Source); ok {
		revision = source.Revision
	}
	return profile.VerificationFingerprint == modelVerificationFingerprint(revision, resolved, apiKey)
}
func compactVerificationError(err error, secrets ...string) string {
	value := err.Error()
	for _, secret := range secrets {
		if strings.TrimSpace(secret) != "" {
			value = strings.ReplaceAll(value, secret, "<redacted>")
		}
	}
	value = strings.Join(strings.Fields(value), " ")
	if len(value) > 300 {
		value = value[:300]
	}
	return value
}
func findSource(cfg config.Config, ref string) (config.ModelSource, bool) {
	for name, source := range cfg.ModelSources {
		if strings.EqualFold(name, strings.TrimSpace(ref)) {
			return source, true
		}
	}
	return config.ModelSource{}, false
}
func findSourceProfile(cfg config.Config, source config.ModelSource, ref string) (string, config.ModelProfile, bool) {
	for _, name := range source.Profiles {
		if strings.EqualFold(name, strings.TrimSpace(ref)) {
			profile, ok := cfg.ModelProfiles[name]
			return name, profile, ok
		}
	}
	return "", config.ModelProfile{}, false
}

func printModelSources(out io.Writer, cfg config.Config) {
	_, _ = fmt.Fprintln(out, "Model sources")
	names := make([]string, 0, len(cfg.ModelSources))
	for name := range cfg.ModelSources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		source := cfg.ModelSources[name]
		_, _ = fmt.Fprintf(out, "%s  revision=%.12s profiles=%d url=%s\n", name, source.Revision, len(source.Profiles), source.URL)
		if source.BackupActive {
			_, _ = fmt.Fprintf(out, "  WARNING: backup JSON active; attempted=%s reason=%s\n", firstNonEmptyCLI(shortModelSourceRevision(source.BackupAttemptedRevision), "unknown"), shortenModelProfileWarning(source.BackupReason, 240))
		}
		for _, profileName := range source.Profiles {
			profile := cfg.ModelProfiles[profileName]
			status := "needs key"
			if profile.VerificationFingerprint != "" {
				status = "verified"
			} else if profile.VerificationError != "" {
				status = "verification failed"
			}
			_, _ = fmt.Fprintf(out, "  - %s: %s\n", profileName, status)
		}
	}
}
