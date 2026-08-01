package teams

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/gofrs/flock"
)

// ScopeStoreMetadata is the bounded portion of a Teams store needed for cold
// migration discovery and takeover safety checks. It intentionally excludes
// sessions, turns, inbound rows, and outbox rows.
type ScopeStoreMetadata struct {
	Scope          teamstore.ScopeIdentity      `json:"scope"`
	ControlChat    teamstore.ControlChatBinding `json:"control_chat"`
	ServiceOwner   *teamstore.OwnerMetadata     `json:"service_owner,omitempty"`
	LockOwner      *teamstore.OwnerMetadata     `json:"lock_owner,omitempty"`
	ControlLease   teamstore.ControlLease       `json:"control_lease"`
	ServiceControl teamstore.ServiceControl     `json:"service_control"`
}

type RuntimeStoreAction string

const (
	RuntimeStoreActionReady            RuntimeStoreAction = "ready"
	RuntimeStoreActionCreate           RuntimeStoreAction = "create"
	RuntimeStoreActionMigrateLegacy    RuntimeStoreAction = "migrate-legacy"
	RuntimeStoreActionQuarantineLegacy RuntimeStoreAction = "quarantine-legacy"

	runtimeStoreDiscoveryTimeout        = 5 * time.Second
	runtimeStoreDiscoveryCandidateLimit = 512
	runtimeStoreDiscoveryEntryLimit     = 100_000
	runtimeStoreDiscoveryBatchSize      = 256

	runtimeStoreMigrationStageStagingReady        = "staging-ready"
	runtimeStoreMigrationStageStagingCopied       = "staging-copied"
	runtimeStoreMigrationStageStagingValidated    = "staging-validated"
	runtimeStoreMigrationStageReplayFencesMerged  = "replay-fences-merged"
	runtimeStoreMigrationStageCanonicalPublished  = "canonical-published"
	runtimeStoreMigrationStageRegistryQuarantined = "registry-quarantined"
	runtimeStoreMigrationCleanupFileName          = ".migration-cleanup.json"
)

// runtimeStoreMigrationTestHook is nil in production. Tests use it to inject
// failures only at durable cold-path migration boundaries.
var runtimeStoreMigrationTestHook func(stage string) error

var loadScopeMetadataOfflineRecoveryReadOnly = teamstore.LoadPathRuntimeMetadataOfflineRecoveryReadOnly

type RuntimeStorePlan struct {
	Action        RuntimeStoreAction
	Scope         teamstore.ScopeIdentity
	CanonicalPath string
	LegacyPath    string
	RegistryPath  string
}

type runtimeStoreMigrationCleanup struct {
	SourcePath string `json:"source_path"`
}

type migrationSourcePreflight struct {
	Layout        migrationSourceLayout
	Metadata      ScopeStoreMetadata
	SourceScopeID string
}

type runtimeStoreMigrationOptions struct {
	RegistryPath string
}

type RuntimeStoreActionRequiredError struct {
	Plan RuntimeStorePlan
}

func (e *RuntimeStoreActionRequiredError) Error() string {
	if e == nil {
		return "Teams runtime store action is required"
	}
	switch e.Plan.Action {
	case RuntimeStoreActionMigrateLegacy:
		return fmt.Sprintf("Teams runtime store migration is required: %s -> %s", e.Plan.LegacyPath, e.Plan.CanonicalPath)
	case RuntimeStoreActionQuarantineLegacy:
		return fmt.Sprintf("canonical and legacy Teams stores both exist: canonical=%s legacy=%s", e.Plan.CanonicalPath, e.Plan.LegacyPath)
	default:
		return "Teams runtime store action is required"
	}
}

func RuntimeStorePlanFromError(err error) (RuntimeStorePlan, bool) {
	var required *RuntimeStoreActionRequiredError
	if !errors.As(err, &required) || required == nil {
		return RuntimeStorePlan{}, false
	}
	return required.Plan, true
}

// inspectRuntimeStorePath is the single runtime path primitive. It deliberately
// uses Lstat so a retained or attacker-controlled symlink can never redirect a
// listener outside the selected state root.
func inspectRuntimeStorePath(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect Teams store %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("Teams store %s is not a regular file", path)
	}
	return true, nil
}

func normalizedExplicitRuntimeRegistryPath(scopeID string, registryPath string) string {
	registryPath = strings.TrimSpace(registryPath)
	if registryPath == "" {
		return ""
	}
	if defaultPath, err := DefaultRegistryPathForScope(scopeID); err == nil && samePath(registryPath, defaultPath) {
		return ""
	}
	return registryPath
}

func validateExplicitRuntimeRegistryReadOnly(registryPath string) error {
	registryPath = strings.TrimSpace(registryPath)
	if registryPath == "" {
		return nil
	}
	if _, err := LoadRegistry(registryPath); err != nil {
		return fmt.Errorf("validate explicit Teams registry %s before migration: %w", registryPath, err)
	}
	return nil
}

func migrationSourceLayoutForRuntimePlan(
	sourcePath string,
	options runtimeStoreMigrationOptions,
) (migrationSourceLayout, error) {
	layout, err := migrationSourceLayoutForStore(sourcePath)
	if err != nil {
		return migrationSourceLayout{}, err
	}
	registryPath := strings.TrimSpace(options.RegistryPath)
	if registryPath == "" {
		return layout, nil
	}
	if scopedStoreMigrationSource(layout.StorePath) &&
		pathWithinDirectory(registryPath, filepath.Dir(layout.StorePath)) {
		return migrationSourceLayout{}, fmt.Errorf(
			"explicit Teams registry %s is inside migration source %s; run `cxp teams service repair` before migration",
			registryPath,
			filepath.Dir(layout.StorePath),
		)
	}
	// An explicit registry is the caller's durable authority. Do not copy or
	// quarantine any inferred default registry; use the explicit path only for
	// replay-ledger identity and fences.
	layout.RegistryPath = ""
	layout.RegistryInScopeDir = false
	return layout, nil
}

func pathWithinDirectory(path string, directory string) bool {
	path = filepath.Clean(strings.TrimSpace(path))
	directory = filepath.Clean(strings.TrimSpace(directory))
	if path == "." || directory == "." {
		return false
	}
	rel, err := filepath.Rel(directory, path)
	if err != nil || filepath.IsAbs(rel) {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// ProbeScopeMetadataReadOnly reads only the bounded identity and authority
// projection from a JSON store or SQLite pointer. It does not acquire a flock,
// create a sidecar, migrate, chmod, or checkpoint WAL.
func ProbeScopeMetadataReadOnly(ctx context.Context, path string) (ScopeStoreMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	metadata, err := teamstore.LoadPathRuntimeMetadataReadOnly(ctx, path)
	if err != nil {
		return ScopeStoreMetadata{}, err
	}
	return ScopeStoreMetadata{
		Scope:          metadata.Scope,
		ControlChat:    metadata.ControlChat,
		ServiceOwner:   metadata.ServiceOwner,
		LockOwner:      metadata.LockOwner,
		ControlLease:   metadata.ControlLease,
		ServiceControl: metadata.ServiceControl,
	}, nil
}

func probeScopeMetadataOfflineRecoveryReadOnly(ctx context.Context, path string) (ScopeStoreMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	metadata, err := loadScopeMetadataOfflineRecoveryReadOnly(ctx, path)
	if err != nil {
		return ScopeStoreMetadata{}, err
	}
	return ScopeStoreMetadata{
		Scope:          metadata.Scope,
		ControlChat:    metadata.ControlChat,
		ServiceOwner:   metadata.ServiceOwner,
		LockOwner:      metadata.LockOwner,
		ControlLease:   metadata.ControlLease,
		ServiceControl: metadata.ServiceControl,
	}, nil
}

func discoverRuntimeScopeMigrationSource(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	currentPath string,
	registryPath string,
) (teamstore.ScopeIdentity, string, bool, error) {
	matches, err := discoverRuntimeScopeMigrationMatches(ctx, scope, currentPath, registryPath)
	if err != nil {
		return teamstore.ScopeIdentity{}, "", false, err
	}
	if len(matches) == 0 {
		return teamstore.ScopeIdentity{}, "", false, nil
	}
	if len(matches) > 1 {
		paths := make([]string, 0, len(matches))
		for _, match := range matches {
			paths = append(paths, match.path)
		}
		sort.Strings(paths)
		return teamstore.ScopeIdentity{}, "", false, fmt.Errorf(
			"multiple Teams migration candidates match scope %q; refusing to guess: %s",
			scope.ID,
			strings.Join(paths, ", "),
		)
	}
	return matches[0].scope, matches[0].path, true, nil
}

func discoverRuntimeScopeMigrationMatches(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	currentPath string,
	registryPath string,
) ([]resolvedScopeStoreCandidate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeStoreDiscoveryTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	paths, err := runtimeStoreMigrationCandidatePaths(ctx, currentPath, runtimeStoreDiscoveryCandidateLimit)
	if err != nil {
		return nil, err
	}
	var matches []resolvedScopeStoreCandidate
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if samePath(path, currentPath) {
			continue
		}
		exists, err := pathExists(path)
		if err != nil {
			return nil, fmt.Errorf("inspect Teams migration candidate %s: %w", path, err)
		}
		if !exists {
			continue
		}
		metadata, err := ProbeScopeMetadataReadOnly(ctx, path)
		if err != nil {
			// A concurrent migrator can quarantine the exact legacy path after
			// Lstat and before the metadata probe. Treat that as a vanished
			// candidate and let the migration coordinator/canonical path decide;
			// other read failures remain fail-closed.
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("probe Teams migration candidate %s: %w", path, err)
		}
		state := teamstore.State{
			Scope:          metadata.Scope,
			ControlChat:    metadata.ControlChat,
			ServiceOwner:   metadata.ServiceOwner,
			LockOwner:      metadata.LockOwner,
			ControlLease:   metadata.ControlLease,
			ServiceControl: metadata.ServiceControl,
		}
		if !scopeStateMatches(scope, state) &&
			!defaultGlobalStoreCanSeedScopeWithRegistry(scope, path, state, registryPath) {
			continue
		}
		candidateScope := metadata.Scope
		if strings.TrimSpace(candidateScope.ID) == "" {
			candidateScope = scope
		}
		matches = append(matches, resolvedScopeStoreCandidate{scope: candidateScope, path: path})
	}
	return matches, nil
}

// runtimeStoreMigrationCandidatePaths bounds the historical discovery cold
// path without changing the maintenance resolver. Only regular state.json
// files count as candidates; retained sidecar-only directories do not consume
// the candidate budget. A separate, much larger entry budget keeps a damaged
// directory from turning cold discovery into an unbounded scan.
func runtimeStoreMigrationCandidatePaths(ctx context.Context, currentPath string, limit int) ([]string, error) {
	return runtimeStoreMigrationCandidatePathsWithLimits(ctx, currentPath, limit, runtimeStoreDiscoveryEntryLimit)
}

func runtimeStoreMigrationCandidatePathsWithLimits(
	ctx context.Context,
	currentPath string,
	candidateLimit int,
	entryLimit int,
) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if candidateLimit <= 0 {
		return nil, fmt.Errorf("Teams runtime migration candidate limit must be positive")
	}
	if entryLimit <= 0 {
		return nil, fmt.Errorf("Teams runtime migration entry limit must be positive")
	}
	seen := map[string]bool{}
	paths := make([]string, 0, 8)
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" || seen[path] {
			return
		}
		seen[path] = true
		paths = append(paths, path)
	}
	add(currentPath)

	remainingCandidates := candidateLimit
	remainingEntries := entryLimit
	for _, rootPath := range runtimeStoreMigrationScopeRoots() {
		candidates, visited, err := readRuntimeStoreMigrationCandidates(
			ctx,
			rootPath,
			remainingCandidates,
			remainingEntries,
		)
		if err != nil {
			return nil, err
		}
		remainingEntries -= visited
		remainingCandidates -= len(candidates)
		for _, path := range candidates {
			add(path)
		}
	}
	if stateGlobalPath, err := appdirs.StatePath("teams", "state.json"); err == nil {
		add(stateGlobalPath)
	}
	if legacyGlobalPath, err := appdirs.LegacyConfigPath("teams", "state.json"); err == nil {
		add(legacyGlobalPath)
	}
	return paths, nil
}

func runtimeStoreMigrationScopeRoots() []string {
	var roots []string
	if path, err := appdirs.StatePath("teams", "scopes"); err == nil {
		roots = append(roots, path)
	}
	if path, err := appdirs.LegacyConfigPath("teams", "scopes"); err == nil {
		roots = append(roots, path)
	}
	return roots
}

func readRuntimeStoreMigrationCandidates(
	ctx context.Context,
	rootPath string,
	candidateLimit int,
	entryLimit int,
) ([]string, int, error) {
	root, err := os.OpenRoot(rootPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("open Teams runtime migration scope root %s: %w", rootPath, err)
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return nil, 0, fmt.Errorf("read Teams runtime migration scope root %s: %w", rootPath, err)
	}
	defer dir.Close()

	visited := 0
	candidates := make([]string, 0, 8)
	for {
		if err := ctx.Err(); err != nil {
			return nil, visited, err
		}
		entries, readErr := dir.ReadDir(runtimeStoreDiscoveryBatchSize)
		for _, entry := range entries {
			visited++
			if visited > entryLimit {
				return nil, visited, fmt.Errorf(
					"Teams runtime migration discovery exceeded %d scope directory entries; refusing unbounded scan",
					entryLimit,
				)
			}
			if err := ctx.Err(); err != nil {
				return nil, visited, err
			}
			name := entry.Name()
			info, err := root.Lstat(name)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, visited, fmt.Errorf("inspect Teams migration scope directory %s: %w", filepath.Join(rootPath, name), err)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return nil, visited, fmt.Errorf("Teams migration scope directory %s must not be a symlink", filepath.Join(rootPath, name))
			}
			if !info.IsDir() {
				continue
			}
			stateRel := filepath.Join(name, "state.json")
			stateInfo, err := root.Lstat(stateRel)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, visited, fmt.Errorf("inspect Teams migration candidate %s: %w", filepath.Join(rootPath, stateRel), err)
			}
			if stateInfo.Mode()&os.ModeSymlink != 0 || !stateInfo.Mode().IsRegular() {
				return nil, visited, fmt.Errorf("Teams migration candidate %s is not a regular file", filepath.Join(rootPath, stateRel))
			}
			if len(candidates) >= candidateLimit {
				return nil, visited, fmt.Errorf(
					"Teams runtime migration discovery exceeded %d scope candidates; refusing unbounded scan",
					candidateLimit,
				)
			}
			candidates = append(candidates, filepath.Join(rootPath, stateRel))
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, visited, fmt.Errorf("read Teams runtime migration scope root %s: %w", rootPath, readErr)
		}
	}
	sort.Strings(candidates)
	return candidates, visited, nil
}

func InspectRuntimeStoreForScope(ctx context.Context, scope teamstore.ScopeIdentity) (RuntimeStorePlan, error) {
	return InspectRuntimeStoreForScopeWithRegistry(ctx, scope, "")
}

func InspectRuntimeStoreForScopeWithRegistry(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	registryPath string,
) (RuntimeStorePlan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RuntimeStorePlan{}, err
	}
	scope = normalizeScopeForResolution(scope)
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return RuntimeStorePlan{}, err
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		return RuntimeStorePlan{}, err
	}
	plan := RuntimeStorePlan{
		Scope:         scope,
		CanonicalPath: canonicalPath,
		LegacyPath:    legacyPath,
		RegistryPath:  normalizedExplicitRuntimeRegistryPath(scope.ID, registryPath),
	}
	canonicalExists, err := inspectRuntimeStorePath(canonicalPath)
	if err != nil {
		return plan, err
	}
	legacyExists, err := inspectRuntimeStorePath(legacyPath)
	if err != nil {
		return plan, err
	}
	if canonicalExists {
		cleanupSource, cleanupPending, err := readRuntimeStoreMigrationCleanup(canonicalPath)
		if err != nil {
			return plan, err
		}
		if legacyExists {
			if cleanupPending && !samePath(cleanupSource, legacyPath) {
				return plan, fmt.Errorf(
					"multiple legacy Teams stores require cleanup: %s and %s",
					cleanupSource,
					legacyPath,
				)
			}
			plan.Action = RuntimeStoreActionQuarantineLegacy
			return plan, nil
		}
		if cleanupPending {
			plan.Action = RuntimeStoreActionQuarantineLegacy
			plan.LegacyPath = cleanupSource
			return plan, nil
		}
		plan.Action = RuntimeStoreActionReady
		return plan, nil
	}
	if legacyExists {
		plan.Action = RuntimeStoreActionMigrateLegacy
		return plan, nil
	}
	resolved, sourcePath, ok, err := discoverRuntimeScopeMigrationSource(
		ctx,
		scope,
		canonicalPath,
		plan.RegistryPath,
	)
	if err != nil {
		return plan, err
	}
	if !ok {
		plan.Action = RuntimeStoreActionCreate
		plan.LegacyPath = ""
		return plan, nil
	}
	if strings.TrimSpace(resolved.ID) != "" {
		plan.Scope = resolved
		if plan.Scope.ID != scope.ID {
			plan.Scope.ID = scope.ID
		}
	}
	plan.Action = RuntimeStoreActionMigrateLegacy
	plan.LegacyPath = sourcePath
	return plan, nil
}

func executeLegacyOnlyMigrationContext(ctx context.Context, scope teamstore.ScopeIdentity, sourcePath string) (string, error) {
	return executeLegacyOnlyMigrationContextWithOptions(
		ctx,
		scope,
		sourcePath,
		runtimeStoreMigrationOptions{},
	)
}

func executeLegacyOnlyMigrationContextWithOptions(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	sourcePath string,
	options runtimeStoreMigrationOptions,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	scope = normalizeScopeForResolution(scope)
	if err := validateExplicitRuntimeRegistryReadOnly(options.RegistryPath); err != nil {
		return "", err
	}
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return "", err
	}
	if samePath(sourcePath, canonicalPath) {
		return canonicalPath, nil
	}
	sourceLayout, err := migrationSourceLayoutForRuntimePlan(sourcePath, options)
	if err != nil {
		return "", err
	}
	canonicalDir := filepath.Dir(canonicalPath)
	canonicalParent := filepath.Dir(canonicalDir)
	if err := preflightMigrationQuarantineTargets(scope.ID, sourceLayout); err != nil {
		return "", err
	}
	if err := os.MkdirAll(canonicalParent, 0o700); err != nil {
		return "", fmt.Errorf("prepare canonical Teams migration directory: %w", err)
	}
	coordinator, err := acquireScopeMigrationCoordinatorLock(ctx, canonicalDir+".migration.lock")
	if err != nil {
		return "", err
	}
	defer releaseScopeTakeoverLocks([]heldScopeTakeoverLock{coordinator})

	canonicalExists, err := inspectRuntimeStorePath(canonicalPath)
	if err != nil {
		return "", fmt.Errorf("inspect canonical Teams migration target: %w", err)
	}
	sourceExists, err := inspectRuntimeStorePath(sourcePath)
	if err != nil {
		return "", fmt.Errorf("inspect legacy Teams migration source: %w", err)
	}
	if canonicalExists && !sourceExists {
		return canonicalPath, nil
	}
	if canonicalExists && sourceExists {
		return "", fmt.Errorf("canonical and legacy Teams stores both exist; retry with an offline quarantine plan")
	}
	if !sourceExists {
		return "", fmt.Errorf("legacy Teams migration source disappeared before migration: %s", sourcePath)
	}
	locks, err := acquireScopeTakeoverLocks(ctx, migrationLockPathsForStore(sourcePath))
	if err != nil {
		return "", deferRuntimeStoreTakeover("lock legacy Teams migration source: " + err.Error())
	}
	defer func() {
		releaseScopeTakeoverLocks(locks)
	}()

	// Another process can finish the migration between discovery and the
	// coordinator handoff. Recheck after all source-family locks are held so a
	// resumed copy never races a writer or replaces an already-complete target.
	canonicalExists, err = inspectRuntimeStorePath(canonicalPath)
	if err != nil {
		return "", fmt.Errorf("reinspect canonical Teams migration target: %w", err)
	}
	sourceExists, err = inspectRuntimeStorePath(sourcePath)
	if err != nil {
		return "", fmt.Errorf("reinspect legacy Teams migration source: %w", err)
	}
	if canonicalExists && !sourceExists {
		return canonicalPath, nil
	}
	if canonicalExists && sourceExists {
		return "", fmt.Errorf("canonical and legacy Teams stores both exist after acquiring migration locks; retry with an offline quarantine plan")
	}
	if !sourceExists {
		return "", fmt.Errorf("legacy Teams migration source disappeared while acquiring migration locks: %s", sourcePath)
	}
	sourcePreflight, err := preflightMigrationSourceWithRegistry(
		ctx,
		scope,
		sourceLayout,
		options.RegistryPath,
	)
	if err != nil {
		return "", err
	}
	replayPlan, err := buildLegacyReplayPlanReadOnlyWithRegistry(
		ctx,
		scope,
		sourcePreflight.SourceScopeID,
		sourcePreflight.Layout,
		options.RegistryPath,
	)
	if err != nil {
		return "", err
	}
	stagingDir := canonicalDir + ".migrating"
	if err := resetRuntimeStoreMigrationStagingDir(stagingDir); err != nil {
		return "", fmt.Errorf("prepare Teams migration staging directory: %w", err)
	}
	if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageStagingReady); err != nil {
		return "", err
	}
	stagingPath := filepath.Join(stagingDir, filepath.Base(canonicalPath))
	if err := copyMigrationSourceToTarget(scope.ID, sourcePreflight.SourceScopeID, stagingPath, sourcePreflight.Layout); err != nil {
		return "", fmt.Errorf("copy legacy Teams store to canonical path: %w", err)
	}
	if err := rebindRuntimeStoreMigrationScope(ctx, stagingPath, scope); err != nil {
		return "", fmt.Errorf("rebind migrated Teams scope identity: %w", err)
	}
	if err := clearConfirmedStoppedOwnerMetadataFromMigrationTarget(ctx, sourcePreflight.Metadata, stagingPath); err != nil {
		return "", fmt.Errorf("clear stopped Teams owner from migration staging: %w", err)
	}
	if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageStagingCopied); err != nil {
		return "", err
	}
	if err := applyLegacyReplayPlan(ctx, stagingPath, replayPlan); err != nil {
		return "", fmt.Errorf("merge legacy Teams replay fences: %w", err)
	}
	if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageReplayFencesMerged); err != nil {
		return "", err
	}
	if err := validateRuntimeStoreMigrationStagingWithLayout(ctx, scope, stagingPath, sourcePreflight.Layout); err != nil {
		return "", fmt.Errorf("validate Teams migration staging directory: %w", err)
	}
	if scopedStoreMigrationSource(sourcePath) {
		if err := writeRuntimeStoreMigrationCleanup(stagingPath, sourcePath); err != nil {
			return "", fmt.Errorf("record Teams migration cleanup source: %w", err)
		}
	}
	if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageStagingValidated); err != nil {
		return "", err
	}
	if err := os.Rename(stagingDir, canonicalDir); err != nil {
		return "", fmt.Errorf("publish canonical Teams migration directory: %w", err)
	}
	if err := syncRuntimeStoreRenameParents(stagingDir, canonicalDir); err != nil {
		return "", fmt.Errorf("sync canonical Teams migration directory: %w", err)
	}
	if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageCanonicalPublished); err != nil {
		return "", err
	}

	if scopedStoreMigrationSource(sourcePath) {
		if !sourcePreflight.Layout.RegistryInScopeDir && strings.TrimSpace(sourcePreflight.Layout.RegistryPath) != "" {
			if err := quarantineRelatedFileFamily(scope.ID, sourcePreflight.Layout.RegistryPath); err != nil {
				return "", fmt.Errorf("quarantine migrated legacy Teams registry: %w", err)
			}
			if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageRegistryQuarantined); err != nil {
				return "", err
			}
		}
		releaseScopeTakeoverLocksBeforeDirectoryRename(&locks)
		if handled, err := quarantineScopedStoreDirectory(scope.ID, sourcePath); err != nil {
			return "", fmt.Errorf("quarantine migrated legacy Teams scope: %w", err)
		} else if !handled {
			return "", fmt.Errorf("legacy Teams scope is not a scoped store: %s", sourcePath)
		}
	}
	if err := removeRuntimeStoreMigrationCleanup(canonicalPath); err != nil {
		return "", err
	}
	return canonicalPath, nil
}

func rebindRuntimeStoreMigrationScope(ctx context.Context, path string, scope teamstore.ScopeIdentity) error {
	store, err := teamstore.Open(path)
	if err != nil {
		return err
	}
	rebindErr := store.RebindScopeForMigration(ctx, scope)
	closeErr := store.Close()
	if rebindErr != nil {
		return rebindErr
	}
	return closeErr
}

func runtimeStoreMigrationCleanupPath(storePath string) string {
	return filepath.Join(filepath.Dir(storePath), runtimeStoreMigrationCleanupFileName)
}

func writeRuntimeStoreMigrationCleanup(storePath string, sourcePath string) error {
	data, err := json.Marshal(runtimeStoreMigrationCleanup{SourcePath: filepath.Clean(sourcePath)})
	if err != nil {
		return err
	}
	path := runtimeStoreMigrationCleanupPath(storePath)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	return syncRuntimeStoreRenameParents(path, path)
}

func readRuntimeStoreMigrationCleanup(storePath string) (string, bool, error) {
	path := runtimeStoreMigrationCleanupPath(storePath)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 4096 {
		return "", false, fmt.Errorf("invalid Teams migration cleanup marker %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false, err
	}
	var marker runtimeStoreMigrationCleanup
	if err := json.Unmarshal(data, &marker); err != nil {
		return "", false, fmt.Errorf("parse Teams migration cleanup marker %s: %w", path, err)
	}
	sourcePath := filepath.Clean(strings.TrimSpace(marker.SourcePath))
	if sourcePath == "." || !scopedStoreMigrationSource(sourcePath) || !runtimeStoreMigrationSourceAllowed(sourcePath) {
		return "", false, fmt.Errorf("invalid Teams migration cleanup source %q", marker.SourcePath)
	}
	return sourcePath, true, nil
}

func runtimeStoreMigrationSourceAllowed(sourcePath string) bool {
	sourceRoot := filepath.Clean(filepath.Dir(filepath.Dir(sourcePath)))
	for _, root := range runtimeStoreMigrationScopeRoots() {
		if samePath(sourceRoot, root) {
			return true
		}
	}
	return false
}

func removeRuntimeStoreMigrationCleanup(storePath string) error {
	path := runtimeStoreMigrationCleanupPath(storePath)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove Teams migration cleanup marker: %w", err)
	}
	return syncRuntimeStoreRenameParents(path, path)
}

func runRuntimeStoreMigrationTestHook(stage string) error {
	if runtimeStoreMigrationTestHook == nil {
		return nil
	}
	return runtimeStoreMigrationTestHook(stage)
}

func resetRuntimeStoreMigrationStagingDir(path string) error {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("refusing to replace non-directory Teams migration staging path %s", path)
		}
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.MkdirAll(path, 0o700)
}

func copyLegacyStoreToTarget(scopeID string, targetPath string, sourcePath string) error {
	layout, err := migrationSourceLayoutForStore(sourcePath)
	if err != nil {
		return err
	}
	return copyMigrationSourceToTarget(scopeID, scopeID, targetPath, layout)
}

func copyMigrationSourceToTarget(scopeID string, sourceScopeID string, targetPath string, layout migrationSourceLayout) error {
	sourcePath := layout.StorePath
	targetDir := filepath.Dir(targetPath)
	sourceDir := filepath.Dir(sourcePath)
	for _, suffix := range []string{"", "-wal"} {
		source := filepath.Join(sourceDir, teamstore.SQLiteFileName) + suffix
		if exists, err := pathExists(source); err != nil {
			return err
		} else if exists {
			if err := appdirs.CopyFileIfMissing(filepath.Join(targetDir, teamstore.SQLiteFileName)+suffix, source); err != nil {
				return err
			}
		}
	}
	for _, name := range storeSidecarNames() {
		source := filepath.Join(sourceDir, name)
		if exists, err := pathExists(source); err != nil {
			return err
		} else if exists {
			if err := appdirs.CopyFileIfMissing(filepath.Join(targetDir, name), source); err != nil {
				return err
			}
		}
	}
	if strings.TrimSpace(layout.RegistryPath) != "" {
		if err := copyRelatedFileFamilyIfPresent(filepath.Join(targetDir, "registry.json"), layout.RegistryPath); err != nil {
			return err
		}
	}
	if err := copyScopeOwnedMigrationSidecars(targetDir, sourceDir, sourceScopeID, scopeID); err != nil {
		return err
	}
	return appdirs.CopyFileReplacing(targetPath, sourcePath)
}

func copyScopeOwnedMigrationSidecars(targetDir string, sourceDir string, sourceScopeID string, targetScopeID string) error {
	sourceHistory := filepath.Join(sourceDir, controlChatHistoryFileName)
	targetHistory := filepath.Join(targetDir, controlChatHistoryFileName)
	if err := copyMigrationRegularFileIfPresent(targetHistory, sourceHistory); err != nil {
		return fmt.Errorf("copy Teams control history JSONL: %w", err)
	}
	sourceHistorySQLite := teamsLedgerSQLitePath(sourceHistory)
	targetHistorySQLite := teamsLedgerSQLitePath(targetHistory)
	for _, suffix := range []string{"", "-wal"} {
		if err := copyMigrationRegularFileIfPresent(targetHistorySQLite+suffix, sourceHistorySQLite+suffix); err != nil {
			return fmt.Errorf("copy Teams control history SQLite%s: %w", suffix, err)
		}
	}
	if relatedMigrationFamilyExists(targetHistory, targetHistorySQLite) {
		if _, err := readControlChatHistoryEntries(targetHistory); err != nil {
			return fmt.Errorf("validate migrated Teams control history: %w", err)
		}
	}
	if err := copyThreadLinkMigrationJournals(
		filepath.Join(targetDir, "thread-links"),
		filepath.Join(sourceDir, "thread-links"),
		sourceScopeID,
		targetScopeID,
	); err != nil {
		return fmt.Errorf("copy Teams thread-link journals: %w", err)
	}
	return nil
}

func copyMigrationRegularFileIfPresent(targetPath string, sourcePath string) error {
	info, err := os.Lstat(sourcePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("migration source %s is not a regular file", sourcePath)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return err
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	removeTarget := true
	defer func() {
		_ = target.Close()
		if removeTarget {
			_ = os.Remove(targetPath)
		}
	}()
	if _, err := io.Copy(target, source); err != nil {
		return err
	}
	if err := target.Sync(); err != nil {
		return err
	}
	if err := target.Close(); err != nil {
		return err
	}
	removeTarget = false
	return nil
}

func copyThreadLinkMigrationJournals(targetDir string, sourceDir string, sourceScopeID string, targetScopeID string) error {
	info, err := os.Lstat(sourceDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("thread-link migration source %s is not a directory", sourceDir)
	}
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		entryInfo, err := os.Lstat(filepath.Join(sourceDir, name))
		if err != nil {
			return err
		}
		if entryInfo.Mode()&os.ModeSymlink != 0 || !entryInfo.Mode().IsRegular() {
			return fmt.Errorf("thread-link migration source %s is not a regular file", filepath.Join(sourceDir, name))
		}
		if filepath.Ext(name) != ".jsonl" {
			continue
		}
		if err := os.MkdirAll(targetDir, 0o700); err != nil {
			return err
		}
		if err := copyThreadLinkMigrationJournal(
			filepath.Join(targetDir, name),
			filepath.Join(sourceDir, name),
			sourceScopeID,
			targetScopeID,
		); err != nil {
			return err
		}
	}
	if exists, err := pathExists(targetDir); err != nil {
		return err
	} else if exists {
		return syncRuntimeStoreRenameParents(filepath.Join(targetDir, ".sync"), filepath.Join(targetDir, ".sync"))
	}
	return nil
}

func copyThreadLinkMigrationJournal(targetPath string, sourcePath string, sourceScopeID string, targetScopeID string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	removeTarget := true
	defer func() {
		_ = target.Close()
		if removeTarget {
			_ = os.Remove(targetPath)
		}
	}()

	reader := bufio.NewReaderSize(source, threadLinkJournalMaxLineByte)
	total := 0
	for {
		line, readErr := reader.ReadBytes('\n')
		total += len(line)
		if total > threadLinkJournalMaxReplayByte {
			return errors.New("thread-link journal replay budget exceeded")
		}
		if len(line) > threadLinkJournalMaxLineByte {
			return fmt.Errorf("thread-link journal line exceeds %d bytes", threadLinkJournalMaxLineByte)
		}
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				return errors.New("thread-link journal ends with an incomplete record")
			}
			raw := strings.TrimSpace(string(line))
			if raw == "" {
				if _, err := target.Write([]byte{'\n'}); err != nil {
					return err
				}
			} else {
				var record map[string]json.RawMessage
				if err := json.Unmarshal([]byte(raw), &record); err != nil {
					return fmt.Errorf("parse thread-link journal %s: %w", sourcePath, err)
				}
				var recordScopeID string
				if rawScopeID, ok := record["scope_id"]; ok {
					if err := json.Unmarshal(rawScopeID, &recordScopeID); err != nil {
						return fmt.Errorf("parse thread-link journal scope_id in %s: %w", sourcePath, err)
					}
				}
				recordScopeID = strings.TrimSpace(recordScopeID)
				switch {
				case recordScopeID == "", recordScopeID == strings.TrimSpace(targetScopeID):
				case strings.TrimSpace(sourceScopeID) != "" && recordScopeID == strings.TrimSpace(sourceScopeID):
					record["scope_id"], _ = json.Marshal(strings.TrimSpace(targetScopeID))
				default:
					return fmt.Errorf(
						"thread-link journal %s contains unrelated scope_id %q",
						sourcePath,
						recordScopeID,
					)
				}
				encoded, err := json.Marshal(record)
				if err != nil {
					return err
				}
				encoded = append(encoded, '\n')
				if _, err := target.Write(encoded); err != nil {
					return err
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if err := target.Sync(); err != nil {
		return err
	}
	if err := target.Close(); err != nil {
		return err
	}
	removeTarget = false
	return nil
}

func validateRuntimeStoreMigrationStaging(ctx context.Context, scope teamstore.ScopeIdentity, stagingPath string, sourcePath string) error {
	layout, err := migrationSourceLayoutForStore(sourcePath)
	if err != nil {
		return err
	}
	return validateRuntimeStoreMigrationStagingWithLayout(ctx, scope, stagingPath, layout)
}

func validateRuntimeStoreMigrationStagingWithLayout(ctx context.Context, scope teamstore.ScopeIdentity, stagingPath string, layout migrationSourceLayout) error {
	stagingStore, err := teamstore.Open(stagingPath)
	if err != nil {
		return err
	}
	state, loadErr := stagingStore.Load(ctx)
	closeErr := stagingStore.Close()
	_ = os.Remove(stagingPath + ".lock")
	if loadErr != nil {
		return loadErr
	}
	if closeErr != nil {
		return closeErr
	}
	if !scopeStateMatches(scope, state) && !defaultGlobalStoreCanSeedScope(scope, layout.StorePath, state) {
		return fmt.Errorf("staging store identity does not match scope %q", scope.ID)
	}
	if strings.TrimSpace(layout.RegistryPath) != "" {
		if exists, err := pathExists(layout.RegistryPath); err != nil {
			return err
		} else if exists && !registryFileValid(filepath.Join(filepath.Dir(stagingPath), "registry.json")) {
			return fmt.Errorf("staging registry is not valid")
		}
	}
	if err := syncRuntimeStoreRenameParents(stagingPath, stagingPath); err != nil {
		return err
	}
	return nil
}

func acquireScopeMigrationCoordinatorLock(ctx context.Context, path string) (heldScopeTakeoverLock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	lock := flock.New(path)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil {
		return heldScopeTakeoverLock{}, fmt.Errorf("acquire Teams migration coordinator %s: %w", path, err)
	}
	if !locked {
		return heldScopeTakeoverLock{}, fmt.Errorf("acquire Teams migration coordinator %s: lock is busy", path)
	}
	return heldScopeTakeoverLock{path: path, lock: lock}, nil
}

func migrationLockPathsForStore(sourcePath string) []string {
	paths := []string{sourcePath + ".lock"}
	layout, err := migrationSourceLayoutForStore(sourcePath)
	if err != nil || strings.TrimSpace(layout.RegistryPath) == "" {
		return paths
	}
	if relatedMigrationFamilyExists(layout.RegistryPath) {
		paths = append(paths, layout.RegistryPath+".lock")
	}
	return paths
}

func relatedMigrationFamilyExists(paths ...string) bool {
	for _, path := range paths {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if _, err := os.Lstat(path + suffix); err == nil {
				return true
			}
		}
	}
	return false
}

type heldScopeTakeoverLock struct {
	path string
	lock *flock.Flock
}

func acquireScopeTakeoverLocks(ctx context.Context, paths []string) ([]heldScopeTakeoverLock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	seen := make(map[string]struct{}, len(paths))
	ordered := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	var held []heldScopeTakeoverLock
	for _, path := range ordered {
		lock := flock.New(path)
		lockCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		locked, err := lock.TryLockContext(lockCtx, 25*time.Millisecond)
		cancel()
		if err != nil || !locked {
			releaseScopeTakeoverLocks(held)
			if err != nil {
				return nil, fmt.Errorf("acquire %s: %w", path, err)
			}
			return nil, fmt.Errorf("acquire %s: lock is busy", path)
		}
		held = append(held, heldScopeTakeoverLock{path: path, lock: lock})
	}
	return held, nil
}

func releaseScopeTakeoverLocks(locks []heldScopeTakeoverLock) {
	for i := len(locks) - 1; i >= 0; i-- {
		_ = locks[i].lock.Unlock()
	}
}

func releaseScopeTakeoverLockPath(locks *[]heldScopeTakeoverLock, path string) {
	if locks == nil {
		return
	}
	for i := range *locks {
		if !samePath((*locks)[i].path, path) {
			continue
		}
		_ = (*locks)[i].lock.Unlock()
		*locks = append((*locks)[:i], (*locks)[i+1:]...)
		return
	}
}

func releaseScopeTakeoverLocksBeforeDirectoryRename(locks *[]heldScopeTakeoverLock) {
	if runtime.GOOS != "windows" || locks == nil || len(*locks) == 0 {
		return
	}
	// Windows does not allow a directory rename while flock keeps the
	// state.json.lock handle open inside that directory. The managed-service
	// coordinator and writer-exit checks still fence the cold-path handoff, and
	// Rename itself fails closed if another process opens the source meanwhile.
	releaseScopeTakeoverLocks(*locks)
	*locks = nil
}

func migrationBackupPath(sourcePath string, scopeID string) string {
	clean := filepath.Clean(sourcePath)
	sourceDir := filepath.Dir(clean)
	teamsRoot := sourceDir
	if filepath.Base(filepath.Dir(sourceDir)) == "scopes" {
		teamsRoot = filepath.Dir(filepath.Dir(sourceDir))
	}
	return filepath.Join(teamsRoot, "migration-backups", safeScopePathPart(scopeID), filepath.Base(clean))
}

func preflightLegacyQuarantineTargets(scopeID string, sourcePath string) error {
	layout, err := migrationSourceLayoutForStore(sourcePath)
	if err != nil {
		return err
	}
	return preflightMigrationQuarantineTargets(scopeID, layout)
}

func preflightMigrationQuarantineTargets(scopeID string, layout migrationSourceLayout) error {
	sourcePath := layout.StorePath
	if !scopedStoreMigrationSource(sourcePath) {
		return nil
	}
	sourceDir := filepath.Dir(filepath.Clean(sourcePath))
	backupDir := filepath.Dir(migrationBackupPath(sourcePath, scopeID))
	if err := preflightMigrationPathPair(sourceDir, backupDir, true); err != nil {
		return err
	}
	if !layout.RegistryInScopeDir && strings.TrimSpace(layout.RegistryPath) != "" {
		if err := preflightMigrationPathPair(layout.RegistryPath, migrationBackupPath(layout.RegistryPath, scopeID), false); err != nil {
			return err
		}
	}
	return nil
}

func scopedStoreMigrationSource(sourcePath string) bool {
	sourcePath = filepath.Clean(strings.TrimSpace(sourcePath))
	if filepath.Base(sourcePath) != "state.json" {
		return false
	}
	sourceDir := filepath.Dir(sourcePath)
	return filepath.Base(filepath.Dir(sourceDir)) == "scopes"
}

func preflightMigrationPathPair(source string, backup string, directory bool) error {
	sourceExists, err := inspectMigrationQuarantinePath(source, directory, "source")
	if err != nil {
		return err
	}
	backupExists, err := inspectMigrationQuarantinePath(backup, directory, "backup")
	if err != nil {
		return err
	}
	if sourceExists && backupExists {
		return fmt.Errorf("migration backup conflict: source %s and backup %s both exist", source, backup)
	}
	return nil
}

func inspectMigrationQuarantinePath(path string, directory bool, role string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("refusing to use symlink migration %s %s", role, path)
	}
	if directory {
		if !info.IsDir() {
			return false, fmt.Errorf("refusing to use non-directory migration %s %s", role, path)
		}
	} else if !info.Mode().IsRegular() {
		return false, fmt.Errorf("refusing to use non-regular migration %s %s", role, path)
	}
	return true, nil
}

func preflightMigrationSource(ctx context.Context, scope teamstore.ScopeIdentity, layout migrationSourceLayout) (migrationSourcePreflight, error) {
	return preflightMigrationSourceWithRegistry(ctx, scope, layout, "")
}

func preflightMigrationSourceWithRegistry(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	layout migrationSourceLayout,
	registryPath string,
) (migrationSourcePreflight, error) {
	if err := preflightMigrationQuarantineTargets(scope.ID, layout); err != nil {
		return migrationSourcePreflight{}, err
	}
	metadata, err := probeScopeMetadataOfflineRecoveryReadOnly(ctx, layout.StorePath)
	if err != nil {
		return migrationSourcePreflight{}, fmt.Errorf("probe legacy Teams writer metadata: %w", err)
	}
	if err := validateOfflineRuntimeStoreWriterMetadataWithRegistry(
		scope,
		layout.StorePath,
		metadata,
		registryPath,
	); err != nil {
		return migrationSourcePreflight{}, err
	}
	return migrationSourcePreflight{
		Layout:        layout,
		Metadata:      metadata,
		SourceScopeID: strings.TrimSpace(metadata.Scope.ID),
	}, nil
}

func validateOfflineRuntimeStoreWriterStopped(ctx context.Context, scope teamstore.ScopeIdentity, sourcePath string) error {
	layout, err := migrationSourceLayoutForStore(sourcePath)
	if err != nil {
		return err
	}
	_, err = preflightMigrationSource(ctx, scope, layout)
	return err
}

func validateOfflineRuntimeStoreWriterMetadata(scope teamstore.ScopeIdentity, sourcePath string, metadata ScopeStoreMetadata) error {
	return validateOfflineRuntimeStoreWriterMetadataWithRegistry(scope, sourcePath, metadata, "")
}

func validateOfflineRuntimeStoreWriterMetadataWithRegistry(
	scope teamstore.ScopeIdentity,
	sourcePath string,
	metadata ScopeStoreMetadata,
	registryPath string,
) error {
	state := teamstore.State{
		Scope:          metadata.Scope,
		ControlChat:    metadata.ControlChat,
		ServiceOwner:   metadata.ServiceOwner,
		LockOwner:      metadata.LockOwner,
		ControlLease:   metadata.ControlLease,
		ServiceControl: metadata.ServiceControl,
	}
	if !scopeStateMatches(scope, state) &&
		!defaultGlobalStoreCanSeedScopeWithRegistry(scope, sourcePath, state, registryPath) {
		return fmt.Errorf("legacy Teams store identity does not match scope %q", scope.ID)
	}
	return validateRuntimeStoreWriterStoppedMetadata("legacy", metadata)
}

func validateRuntimeStoreWriterStoppedMetadata(role string, metadata ScopeStoreMetadata) error {
	role = strings.TrimSpace(role)
	if role == "" {
		role = "Teams store"
	}
	for _, candidate := range []struct {
		name  string
		owner *teamstore.OwnerMetadata
	}{
		{name: "service owner", owner: metadata.ServiceOwner},
		{name: "lock owner", owner: metadata.LockOwner},
	} {
		if candidate.owner == nil || ownerMetadataEmpty(*candidate.owner) {
			continue
		}
		if runtimeStoreOwnerExitConfirmed(*candidate.owner) {
			continue
		}
		return deferRuntimeStoreTakeover(fmt.Sprintf(
			"%s %s is not confirmed stopped (pid=%d host=%q)",
			role,
			candidate.name,
			candidate.owner.PID,
			candidate.owner.Hostname,
		))
	}
	lease := metadata.ControlLease
	if lease.Status == teamstore.ControlLeaseStatusActive &&
		!lease.LeaseUntil.IsZero() &&
		lease.LeaseUntil.After(time.Now()) {
		return deferRuntimeStoreTakeover(fmt.Sprintf(
			"%s control lease remains active until %s",
			role,
			lease.LeaseUntil.UTC().Format(time.RFC3339Nano),
		))
	}
	return nil
}

func clearConfirmedStoppedOwnerFromMigrationTarget(ctx context.Context, sourcePath string, targetPath string) error {
	metadata, err := ProbeScopeMetadataReadOnly(ctx, sourcePath)
	if err != nil {
		return err
	}
	return clearConfirmedStoppedOwnerMetadataFromMigrationTarget(ctx, metadata, targetPath)
}

func clearConfirmedStoppedOwnerMetadataFromMigrationTarget(ctx context.Context, metadata ScopeStoreMetadata, targetPath string) error {
	var owner *teamstore.OwnerMetadata
	switch {
	case metadata.ServiceOwner != nil && !ownerMetadataEmpty(*metadata.ServiceOwner):
		owner = metadata.ServiceOwner
	case metadata.LockOwner != nil && !ownerMetadataEmpty(*metadata.LockOwner):
		owner = metadata.LockOwner
	}
	if owner == nil || !runtimeStoreOwnerExitConfirmed(*owner) {
		return nil
	}
	store, err := teamstore.Open(targetPath)
	if err != nil {
		return err
	}
	_, clearErr := store.ClearOwnerIfSame(ctx, *owner)
	closeErr := store.Close()
	if clearErr != nil {
		return clearErr
	}
	return closeErr
}

func ownerMetadataEmpty(owner teamstore.OwnerMetadata) bool {
	return owner.PID <= 0 &&
		strings.TrimSpace(owner.Hostname) == "" &&
		strings.TrimSpace(owner.ExecutablePath) == "" &&
		strings.TrimSpace(owner.MachineID) == "" &&
		owner.StartedAt.IsZero() &&
		owner.LastHeartbeat.IsZero()
}

func quarantineScopedStoreDirectory(scopeID string, sourcePath string) (bool, error) {
	sourcePath = filepath.Clean(strings.TrimSpace(sourcePath))
	if filepath.Base(sourcePath) != "state.json" {
		return false, nil
	}
	sourceDir := filepath.Dir(sourcePath)
	if filepath.Base(filepath.Dir(sourceDir)) != "scopes" {
		return false, nil
	}
	backupDir := filepath.Dir(migrationBackupPath(sourcePath, scopeID))

	sourceInfo, sourceErr := os.Lstat(sourceDir)
	backupInfo, backupErr := os.Lstat(backupDir)
	sourceExists := sourceErr == nil
	backupExists := backupErr == nil
	if sourceErr != nil && !os.IsNotExist(sourceErr) {
		return true, sourceErr
	}
	if backupErr != nil && !os.IsNotExist(backupErr) {
		return true, backupErr
	}
	if sourceExists && (sourceInfo.Mode()&os.ModeSymlink != 0 || !sourceInfo.IsDir()) {
		return true, fmt.Errorf("refusing to quarantine non-directory scoped migration source %s", sourceDir)
	}
	if backupExists && (backupInfo.Mode()&os.ModeSymlink != 0 || !backupInfo.IsDir()) {
		return true, fmt.Errorf("refusing to use non-directory scoped migration backup %s", backupDir)
	}
	if sourceExists && backupExists {
		return true, fmt.Errorf("migration backup conflict: source directory %s and backup directory %s both exist", sourceDir, backupDir)
	}
	if !sourceExists {
		return true, nil
	}
	if err := os.MkdirAll(filepath.Dir(backupDir), 0o700); err != nil {
		return true, err
	}
	if err := os.Rename(sourceDir, backupDir); err != nil {
		return true, err
	}
	if err := syncRuntimeStoreRenameParents(sourceDir, backupDir); err != nil {
		return true, err
	}
	return true, nil
}

func quarantineRelatedFileFamily(scopeID string, sourceBase string, suffixes ...string) error {
	var renamed [][2]string
	for _, suffix := range cleanupSuffixesWithBaseLast(suffixes...) {
		source := sourceBase + suffix
		backup := migrationBackupPath(source, scopeID)
		sourceInfo, sourceErr := os.Lstat(source)
		backupInfo, backupErr := os.Lstat(backup)
		sourceExists := sourceErr == nil
		backupExists := backupErr == nil
		if sourceErr != nil && !os.IsNotExist(sourceErr) {
			return sourceErr
		}
		if backupErr != nil && !os.IsNotExist(backupErr) {
			return backupErr
		}
		if sourceExists && (sourceInfo.Mode()&os.ModeSymlink != 0 || !sourceInfo.Mode().IsRegular()) {
			return fmt.Errorf("refusing to quarantine non-regular migration source %s", source)
		}
		if backupExists && (backupInfo.Mode()&os.ModeSymlink != 0 || !backupInfo.Mode().IsRegular()) {
			return fmt.Errorf("refusing to use non-regular migration backup %s", backup)
		}
		if sourceExists && backupExists {
			return fmt.Errorf("migration backup conflict: source %s and backup %s both exist", source, backup)
		}
		if !sourceExists {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(backup), 0o700); err != nil {
			return err
		}
		if err := os.Rename(source, backup); err != nil {
			return err
		}
		renamed = append(renamed, [2]string{source, backup})
	}
	for _, pair := range renamed {
		if err := syncRuntimeStoreRenameParents(pair[0], pair[1]); err != nil {
			return err
		}
	}
	return nil
}

type RuntimeStorePreparation struct {
	CanonicalPath    string
	LegacyPath       string
	TakeoverRequired bool
	Resolved         bool
}

// RuntimeStoreTakeoverDeferredError is safe for the service retry loop. It
// means takeover made no unsafe selection and should be retried after the
// existing listener has had time to drain or after a transient fence clears.
type RuntimeStoreTakeoverDeferredError struct {
	Reason string
}

func (e *RuntimeStoreTakeoverDeferredError) Error() string {
	if e == nil {
		return "automatic Teams runtime store takeover deferred"
	}
	reason := strings.TrimSpace(e.Reason)
	if reason == "" {
		return "automatic Teams runtime store takeover deferred"
	}
	return "automatic Teams runtime store takeover deferred: " + reason
}

func deferRuntimeStoreTakeover(reason string) error {
	return &RuntimeStoreTakeoverDeferredError{Reason: strings.TrimSpace(reason)}
}

// RuntimeStoreMigrationBlockedError keeps a deterministic cold-path failure in
// the managed child retry loop. This prevents a fixed supervisor restart cycle
// from repeatedly copying the store while preserving the original cause.
type RuntimeStoreMigrationBlockedError struct {
	Err error
}

func (e *RuntimeStoreMigrationBlockedError) Error() string {
	if e == nil || e.Err == nil {
		return "Teams runtime store migration is blocked"
	}
	return "Teams runtime store migration is blocked: " + e.Err.Error()
}

func (e *RuntimeStoreMigrationBlockedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// PrepareRuntimeStoreForListener is a pure path resolver. It never opens a
// store, writes migration state, drains a listener, fences a process, or
// moves files. A managed service may complete TakeoverRequired before entering
// Listen; foreground listeners fail closed.
func PrepareRuntimeStoreForListener(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
) (RuntimeStorePreparation, error) {
	plan, err := InspectRuntimeStoreForScope(ctx, scope)
	if err != nil {
		return RuntimeStorePreparation{}, err
	}
	result := RuntimeStorePreparation{
		CanonicalPath: plan.CanonicalPath,
		LegacyPath:    plan.LegacyPath,
	}
	switch plan.Action {
	case RuntimeStoreActionReady, RuntimeStoreActionCreate:
		result.Resolved = true
	case RuntimeStoreActionMigrateLegacy, RuntimeStoreActionQuarantineLegacy:
		result.TakeoverRequired = true
	}
	return result, nil
}

func CompleteOfflineRuntimeStorePlan(ctx context.Context, plan RuntimeStorePlan) error {
	var err error
	switch plan.Action {
	case RuntimeStoreActionReady, RuntimeStoreActionCreate:
		return nil
	case RuntimeStoreActionMigrateLegacy:
		_, err = executeLegacyOnlyMigrationContextWithOptions(
			ctx,
			plan.Scope,
			plan.LegacyPath,
			runtimeStoreMigrationOptions{RegistryPath: plan.RegistryPath},
		)
	case RuntimeStoreActionQuarantineLegacy:
		err = completeOfflineRuntimeStoreTakeoverWithOptions(
			ctx,
			plan.Scope,
			plan.LegacyPath,
			runtimeStoreMigrationOptions{RegistryPath: plan.RegistryPath},
		)
	default:
		return fmt.Errorf("unsupported Teams runtime store action %q", plan.Action)
	}
	if err == nil {
		return nil
	}
	var deferred *RuntimeStoreTakeoverDeferredError
	var blocked *RuntimeStoreMigrationBlockedError
	if errors.As(err, &deferred) ||
		errors.As(err, &blocked) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &RuntimeStoreMigrationBlockedError{Err: err}
}

// CompleteOfflineRuntimeStoreTakeover performs the dual-store filesystem
// handoff after the service layer has stopped every writer for this service.
// It never interprets persisted PID metadata or writes listener drain state.
// Shared replay ledgers are unioned monotonically and retained at their legacy
// paths; only scope-owned registry and store files are quarantined.
func CompleteOfflineRuntimeStoreTakeover(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	legacyPath string,
) error {
	return completeOfflineRuntimeStoreTakeoverWithOptions(
		ctx,
		scope,
		legacyPath,
		runtimeStoreMigrationOptions{},
	)
}

func completeOfflineRuntimeStoreTakeoverWithOptions(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	legacyPath string,
	options runtimeStoreMigrationOptions,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	scope = normalizeScopeForResolution(scope)
	if err := validateExplicitRuntimeRegistryReadOnly(options.RegistryPath); err != nil {
		return err
	}
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return err
	}
	canonicalExists, err := inspectRuntimeStorePath(canonicalPath)
	if err != nil {
		return err
	}
	if !canonicalExists {
		return fmt.Errorf("offline Teams store takeover requires canonical store %s", canonicalPath)
	}
	legacyPath = filepath.Clean(strings.TrimSpace(legacyPath))
	sourceLayout, err := migrationSourceLayoutForRuntimePlan(legacyPath, options)
	if err != nil {
		return err
	}
	expectedLegacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		return err
	}
	if !samePath(legacyPath, expectedLegacyPath) &&
		(!scopedStoreMigrationSource(legacyPath) || !runtimeStoreMigrationSourceAllowed(legacyPath)) {
		return fmt.Errorf("offline Teams store takeover expected legacy path %s, got %s", expectedLegacyPath, legacyPath)
	}
	if err := preflightMigrationQuarantineTargets(scope.ID, sourceLayout); err != nil {
		return err
	}
	legacyExists, err := inspectRuntimeStorePath(legacyPath)
	if err != nil {
		return err
	}
	backupDir := filepath.Dir(migrationBackupPath(legacyPath, scope.ID))
	storeAlreadyQuarantined := false
	if !legacyExists {
		backupStore := filepath.Join(backupDir, filepath.Base(legacyPath))
		if backupExists, backupErr := inspectRuntimeStorePath(backupStore); backupErr != nil {
			return backupErr
		} else if backupExists {
			storeAlreadyQuarantined = true
		} else {
			return fmt.Errorf("legacy Teams store disappeared before offline takeover: %s", legacyPath)
		}
	}

	lockPaths := append(migrationLockPathsForStore(legacyPath), canonicalPath+".lock")
	if storeAlreadyQuarantined && len(lockPaths) > 0 {
		filtered := lockPaths[:0]
		for _, path := range lockPaths {
			if samePath(path, legacyPath+".lock") {
				continue
			}
			filtered = append(filtered, path)
		}
		lockPaths = filtered
	}
	locks, err := acquireScopeTakeoverLocks(ctx, lockPaths)
	if err != nil {
		return deferRuntimeStoreTakeover("lock offline Teams store takeover source: " + err.Error())
	}
	coordinator, err := acquireScopeMigrationCoordinatorLock(ctx, canonicalPath+".takeover.lock")
	if err != nil {
		releaseScopeTakeoverLocks(locks)
		return deferRuntimeStoreTakeover(err.Error())
	}
	defer releaseScopeTakeoverLocks([]heldScopeTakeoverLock{coordinator})
	defer func() {
		releaseScopeTakeoverLocks(locks)
	}()
	if err := validateCanonicalRuntimeStoreForScopeOffline(ctx, scope, canonicalPath); err != nil {
		return err
	}
	var sourcePreflight migrationSourcePreflight
	if !storeAlreadyQuarantined {
		sourcePreflight, err = preflightMigrationSourceWithRegistry(
			ctx,
			scope,
			sourceLayout,
			options.RegistryPath,
		)
		if err != nil {
			return err
		}
	}
	if err := preflightMigrationQuarantineTargets(scope.ID, sourceLayout); err != nil {
		return err
	}
	if !storeAlreadyQuarantined {
		replayPlan, err := buildLegacyReplayPlanReadOnlyWithRegistry(
			ctx,
			scope,
			sourcePreflight.SourceScopeID,
			sourcePreflight.Layout,
			options.RegistryPath,
		)
		if err != nil {
			return err
		}
		// The offline recovery reader requires the canonical flock while it
		// rebuilds SHM. Store.ApplyOutboxReplayFences takes that same flock, so
		// hand it off immediately before the idempotent Store update. The
		// service coordinator and all source-family locks remain held.
		releaseScopeTakeoverLockPath(&locks, canonicalPath+".lock")
		if err := applyLegacyReplayPlan(ctx, canonicalPath, replayPlan); err != nil {
			return err
		}
		canonicalLocks, err := acquireScopeTakeoverLocks(ctx, []string{canonicalPath + ".lock"})
		if err != nil {
			return deferRuntimeStoreTakeover("relock canonical Teams store after replay fencing: " + err.Error())
		}
		locks = append(locks, canonicalLocks...)
		if err := validateCanonicalRuntimeStoreWriterStoppedOffline(ctx, scope, canonicalPath); err != nil {
			return err
		}
	}

	// Move the cross-root registry first. If the process stops between these two
	// operations, state.json remains discoverable and the next service start
	// retries the idempotent registry move before committing the scope handoff.
	if !sourceLayout.RegistryInScopeDir && strings.TrimSpace(sourceLayout.RegistryPath) != "" {
		if err := quarantineRelatedFileFamily(scope.ID, sourceLayout.RegistryPath); err != nil {
			return fmt.Errorf("quarantine legacy Teams registry: %w", err)
		}
	}
	if !storeAlreadyQuarantined {
		releaseScopeTakeoverLocksBeforeDirectoryRename(&locks)
		if handled, err := quarantineScopedStoreDirectory(scope.ID, legacyPath); err != nil {
			return fmt.Errorf("quarantine legacy Teams scope: %w", err)
		} else if !handled {
			return fmt.Errorf("legacy Teams scope is not a scoped store: %s", legacyPath)
		}
	}
	if err := removeRuntimeStoreMigrationCleanup(canonicalPath); err != nil {
		return err
	}
	return nil
}

func validateCanonicalRuntimeStoreForScope(ctx context.Context, scope teamstore.ScopeIdentity, path string) error {
	state, err := teamstore.LoadPathReadOnly(ctx, path)
	if err != nil {
		return fmt.Errorf("validate canonical Teams store %s: %w", path, err)
	}
	if strings.TrimSpace(state.Scope.ID) != strings.TrimSpace(scope.ID) || !scopeStateMatches(scope, state) {
		return fmt.Errorf("canonical Teams store %s identity does not match scope %q", path, scope.ID)
	}
	return nil
}

func validateCanonicalRuntimeStoreForScopeOffline(ctx context.Context, scope teamstore.ScopeIdentity, path string) error {
	state, err := teamstore.LoadPathOfflineRecoveryReadOnly(ctx, path)
	if err != nil {
		return fmt.Errorf("validate canonical Teams store %s: %w", path, err)
	}
	if strings.TrimSpace(state.Scope.ID) != strings.TrimSpace(scope.ID) || !scopeStateMatches(scope, state) {
		return fmt.Errorf("canonical Teams store %s identity does not match scope %q", path, scope.ID)
	}
	return validateRuntimeStoreWriterStoppedMetadata("canonical", scopeStoreMetadataFromState(state))
}

func validateCanonicalRuntimeStoreWriterStoppedOffline(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	path string,
) error {
	metadata, err := probeScopeMetadataOfflineRecoveryReadOnly(ctx, path)
	if err != nil {
		return fmt.Errorf("revalidate canonical Teams writer metadata %s: %w", path, err)
	}
	state := teamstore.State{
		Scope:       metadata.Scope,
		ControlChat: metadata.ControlChat,
	}
	if strings.TrimSpace(state.Scope.ID) != strings.TrimSpace(scope.ID) || !scopeStateMatches(scope, state) {
		return fmt.Errorf("canonical Teams store %s identity does not match scope %q", path, scope.ID)
	}
	return validateRuntimeStoreWriterStoppedMetadata("canonical", metadata)
}

func scopeStoreMetadataFromState(state teamstore.State) ScopeStoreMetadata {
	return ScopeStoreMetadata{
		Scope:          state.Scope,
		ControlChat:    state.ControlChat,
		ServiceOwner:   state.ServiceOwner,
		LockOwner:      state.LockOwner,
		ControlLease:   state.ControlLease,
		ServiceControl: state.ServiceControl,
	}
}

func readCompletedGlobalInboundForUnion(ctx context.Context, path string) (map[string]globalInboundItem, error) {
	items := make(map[string]globalInboundItem)
	jsonExists, err := inspectRuntimeStorePath(path)
	if err != nil {
		return nil, err
	}
	if jsonExists {
		jsonLedger, err := readGlobalInboundJSON(path)
		if err != nil {
			return nil, err
		}
		for _, item := range jsonLedger.Items {
			if item.Status != "done" || strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
				continue
			}
			items[globalInboundKey(item.ChatID, item.MessageID)] = item
		}
	}
	sqlitePath := teamsLedgerSQLitePath(path)
	if _, err := os.Lstat(sqlitePath); errors.Is(err, os.ErrNotExist) {
		return items, nil
	} else if err != nil {
		return nil, err
	}
	db, err := openTeamsLedgerSQLiteReadOnly(ctx, sqlitePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, `SELECT json FROM inbound_ledger WHERE status = 'done'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item globalInboundItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		if strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
			continue
		}
		items[globalInboundKey(item.ChatID, item.MessageID)] = item
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func readGlobalInboundForUnion(ctx context.Context, path string) (map[string]globalInboundItem, error) {
	items := make(map[string]globalInboundItem)
	jsonExists, err := inspectRuntimeStorePath(path)
	if err != nil {
		return nil, err
	}
	if jsonExists {
		jsonLedger, err := readGlobalInboundJSON(path)
		if err != nil {
			return nil, err
		}
		for _, item := range jsonLedger.Items {
			if strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
				continue
			}
			items[globalInboundKey(item.ChatID, item.MessageID)] = item
		}
	}
	sqlitePath := teamsLedgerSQLitePath(path)
	if _, err := os.Lstat(sqlitePath); errors.Is(err, os.ErrNotExist) {
		return items, nil
	} else if err != nil {
		return nil, err
	}
	db, err := openTeamsLedgerSQLiteReadOnly(ctx, sqlitePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, `SELECT json FROM inbound_ledger`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item globalInboundItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		if strings.TrimSpace(item.ChatID) != "" && strings.TrimSpace(item.MessageID) != "" {
			items[globalInboundKey(item.ChatID, item.MessageID)] = item
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func readGlobalOutboundForUnion(ctx context.Context, path string) (map[string]globalOutboundItem, error) {
	items := make(map[string]globalOutboundItem)
	jsonExists, err := inspectRuntimeStorePath(path)
	if err != nil {
		return nil, err
	}
	if jsonExists {
		jsonLedger, err := readGlobalOutboundJSON(path)
		if err != nil {
			return nil, err
		}
		for _, item := range jsonLedger.Items {
			if strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
				continue
			}
			items[globalOutboundKey(item.ChatID, item.MessageID)] = item
		}
	}
	sqlitePath := teamsLedgerSQLitePath(path)
	if _, err := os.Lstat(sqlitePath); errors.Is(err, os.ErrNotExist) {
		return items, nil
	} else if err != nil {
		return nil, err
	}
	db, err := openTeamsLedgerSQLiteReadOnly(ctx, sqlitePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, `SELECT json FROM outbound_ledger`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item globalOutboundItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		if strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
			continue
		}
		items[globalOutboundKey(item.ChatID, item.MessageID)] = item
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

type legacyReplayPlan struct {
	canonicalInboundPath  string
	canonicalOutboundPath string
	inboundDelta          []globalInboundItem
	outboundDelta         []globalOutboundItem
	outboxFences          []teamstore.OutboxReplayFence
}

func buildLegacyReplayPlanReadOnly(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	sourceScopeID string,
	sourceLayout migrationSourceLayout,
) (legacyReplayPlan, error) {
	return buildLegacyReplayPlanReadOnlyWithRegistry(
		ctx,
		scope,
		sourceScopeID,
		sourceLayout,
		"",
	)
}

func buildLegacyReplayPlanReadOnlyWithRegistry(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	sourceScopeID string,
	sourceLayout migrationSourceLayout,
	registryPath string,
) (legacyReplayPlan, error) {
	var plan legacyReplayPlan
	if ctx == nil {
		ctx = context.Background()
	}
	registryPath = strings.TrimSpace(registryPath)
	legacyRegistryPath := strings.TrimSpace(sourceLayout.RegistryPath)
	if registryPath != "" {
		legacyRegistryPath = registryPath
	}
	if legacyRegistryPath == "" {
		return plan, nil
	}
	sourceScopeID = strings.TrimSpace(sourceScopeID)
	canonicalRegistryPath := registryPath
	var err error
	if canonicalRegistryPath == "" {
		canonicalRegistryPath, err = DefaultRegistryPathForScope(scope.ID)
		if err != nil {
			return plan, err
		}
	}

	legacyInboundPath, legacyInboundOK := globalInboundLedgerPathForRegistry(legacyRegistryPath)
	canonicalInboundPath, canonicalInboundOK := globalInboundLedgerPathForRegistry(canonicalRegistryPath)
	if legacyInboundOK && canonicalInboundOK && !samePath(legacyInboundPath, canonicalInboundPath) {
		var locks []heldScopeTakeoverLock
		if relatedMigrationFamilyExists(legacyInboundPath, teamsLedgerSQLitePath(legacyInboundPath)) {
			locks, err = acquireScopeTakeoverLocks(ctx, []string{legacyInboundPath + ".lock"})
			if err != nil {
				return plan, fmt.Errorf("lock legacy global Teams inbound ledger: %w", err)
			}
		}
		legacyItems, readErr := readCompletedGlobalInboundForUnion(ctx, legacyInboundPath)
		releaseScopeTakeoverLocks(locks)
		if readErr != nil {
			return plan, fmt.Errorf("read legacy global inbound ledger: %w", readErr)
		}
		canonicalItems, err := readGlobalInboundForUnion(ctx, canonicalInboundPath)
		if err != nil {
			return plan, fmt.Errorf("read canonical global inbound ledger: %w", err)
		}
		plan.canonicalInboundPath = canonicalInboundPath
		for key, item := range legacyItems {
			if canonical, ok := canonicalItems[key]; ok && canonical.Status == "done" {
				continue
			}
			item.Status = "done"
			plan.inboundDelta = append(plan.inboundDelta, item)
		}
	}

	legacyOutboundPath, legacyOutboundOK := globalOutboundLedgerPathForRegistry(legacyRegistryPath)
	canonicalOutboundPath, canonicalOutboundOK := globalOutboundLedgerPathForRegistry(canonicalRegistryPath)
	if legacyOutboundOK && canonicalOutboundOK {
		var locks []heldScopeTakeoverLock
		if relatedMigrationFamilyExists(legacyOutboundPath, teamsLedgerSQLitePath(legacyOutboundPath)) {
			locks, err = acquireScopeTakeoverLocks(ctx, []string{legacyOutboundPath + ".lock"})
			if err != nil {
				return plan, fmt.Errorf("lock legacy global Teams outbound ledger: %w", err)
			}
		}
		legacyItems, readErr := readGlobalOutboundForUnion(ctx, legacyOutboundPath)
		releaseScopeTakeoverLocks(locks)
		if readErr != nil {
			return plan, fmt.Errorf("read legacy global outbound ledger: %w", readErr)
		}
		sameLedger := samePath(legacyOutboundPath, canonicalOutboundPath)
		canonicalItems := map[string]globalOutboundItem{}
		if !sameLedger {
			canonicalItems, err = readGlobalOutboundForUnion(ctx, canonicalOutboundPath)
			if err != nil {
				return plan, fmt.Errorf("read canonical global outbound ledger: %w", err)
			}
			plan.canonicalOutboundPath = canonicalOutboundPath
		}
		for key, item := range legacyItems {
			itemScopeID := strings.TrimSpace(item.ScopeID)
			if itemScopeID == "" || itemScopeID == strings.TrimSpace(scope.ID) ||
				(sourceScopeID != "" && itemScopeID == sourceScopeID) {
				plan.outboxFences = append(plan.outboxFences, teamstore.OutboxReplayFence{
					OutboxID:       item.OutboxID,
					TeamsChatID:    item.ChatID,
					TeamsMessageID: item.MessageID,
					SessionID:      item.SessionID,
					TurnID:         item.TurnID,
					Kind:           item.Kind,
				})
			}
			if !sameLedger {
				if _, ok := canonicalItems[key]; ok {
					continue
				}
				plan.outboundDelta = append(plan.outboundDelta, item)
			}
		}
	}
	sort.Slice(plan.inboundDelta, func(i, j int) bool {
		return globalInboundKey(plan.inboundDelta[i].ChatID, plan.inboundDelta[i].MessageID) <
			globalInboundKey(plan.inboundDelta[j].ChatID, plan.inboundDelta[j].MessageID)
	})
	sort.Slice(plan.outboundDelta, func(i, j int) bool {
		return globalOutboundKey(plan.outboundDelta[i].ChatID, plan.outboundDelta[i].MessageID) <
			globalOutboundKey(plan.outboundDelta[j].ChatID, plan.outboundDelta[j].MessageID)
	})
	sort.Slice(plan.outboxFences, func(i, j int) bool { return plan.outboxFences[i].OutboxID < plan.outboxFences[j].OutboxID })
	return plan, nil
}

func applyLegacyReplayPlan(ctx context.Context, targetStorePath string, plan legacyReplayPlan) error {
	if len(plan.outboxFences) > 0 {
		store, err := teamstore.Open(targetStorePath)
		if err != nil {
			return fmt.Errorf("open target Teams store for outbound replay fence: %w", err)
		}
		_, applyErr := store.ApplyOutboxReplayFences(ctx, plan.outboxFences)
		closeErr := store.Close()
		if applyErr != nil {
			return fmt.Errorf("apply legacy outbound replay fence: %w", applyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close target Teams store after outbound replay fence: %w", closeErr)
		}
	}
	if len(plan.inboundDelta) > 0 {
		if err := updateGlobalInboundSQLite(ctx, plan.canonicalInboundPath, func(tx *sql.Tx, _ time.Time) error {
			for _, item := range plan.inboundDelta {
				key := globalInboundKey(item.ChatID, item.MessageID)
				current, ok, err := loadGlobalInboundSQLiteItem(ctx, tx, key)
				if err != nil {
					return err
				}
				if ok && current.Status == "done" {
					continue
				}
				item.Status = "done"
				if err := upsertGlobalInboundSQLiteTx(ctx, tx, key, item); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("merge legacy global inbound ledger: %w", err)
		}
	}
	if len(plan.outboundDelta) > 0 {
		if err := recordMissingGlobalOutboundBatch(ctx, plan.canonicalOutboundPath, plan.outboundDelta, time.Now()); err != nil {
			return fmt.Errorf("merge legacy global outbound ledger: %w", err)
		}
	}
	return nil
}

// UnionLegacyGlobalLedgers copies only monotonic replay fences into the
// canonical ledgers. The legacy ledgers are shared by every legacy scope, so
// this function never moves, truncates, or deletes them.
func UnionLegacyGlobalLedgers(ctx context.Context, scope teamstore.ScopeIdentity, legacyStorePath string, targetStorePath string) error {
	layout, err := migrationSourceLayoutForStore(legacyStorePath)
	if err != nil {
		return err
	}
	metadata, err := ProbeScopeMetadataReadOnly(ctx, legacyStorePath)
	if err != nil {
		return err
	}
	plan, err := buildLegacyReplayPlanReadOnly(ctx, scope, strings.TrimSpace(metadata.Scope.ID), layout)
	if err != nil {
		return err
	}
	return applyLegacyReplayPlan(ctx, targetStorePath, plan)
}
