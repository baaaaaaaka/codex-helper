package teams

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	runtimeStoreMigrationStageStagingReady        = "staging-ready"
	runtimeStoreMigrationStageStagingCopied       = "staging-copied"
	runtimeStoreMigrationStageStagingValidated    = "staging-validated"
	runtimeStoreMigrationStageReplayFencesMerged  = "replay-fences-merged"
	runtimeStoreMigrationStageCanonicalPublished  = "canonical-published"
	runtimeStoreMigrationStageRegistryQuarantined = "registry-quarantined"
)

// runtimeStoreMigrationTestHook is nil in production. Tests use it to inject
// failures only at durable cold-path migration boundaries.
var runtimeStoreMigrationTestHook func(stage string) error

type RuntimeStorePlan struct {
	Action        RuntimeStoreAction
	Scope         teamstore.ScopeIdentity
	CanonicalPath string
	LegacyPath    string
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

func discoverRuntimeScopeMigrationSource(ctx context.Context, scope teamstore.ScopeIdentity, currentPath string) (teamstore.ScopeIdentity, string, bool, error) {
	matches, err := discoverRuntimeScopeMigrationMatches(ctx, scope, currentPath)
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

func discoverRuntimeScopeMigrationMatches(ctx context.Context, scope teamstore.ScopeIdentity, currentPath string) ([]resolvedScopeStoreCandidate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeStoreDiscoveryTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	paths, err := runtimeStoreMigrationCandidatePaths(currentPath, runtimeStoreDiscoveryCandidateLimit)
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
		if !scopeStateMatches(scope, state) && !defaultGlobalStoreCanSeedScope(scope, path, state) {
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
// path without changing the maintenance resolver. It reads at most limit+1
// directory entries across the state and legacy scope roots, then fails
// closed instead of truncating the candidate set and guessing.
func runtimeStoreMigrationCandidatePaths(currentPath string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Teams runtime migration candidate limit must be positive")
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

	remaining := limit
	for _, rootPath := range runtimeStoreMigrationScopeRoots() {
		names, err := readRuntimeStoreMigrationScopeNames(rootPath, remaining+1)
		if err != nil {
			return nil, err
		}
		if len(names) > remaining {
			return nil, fmt.Errorf(
				"Teams runtime migration discovery exceeded %d scope candidates; refusing unbounded scan",
				limit,
			)
		}
		remaining -= len(names)
		sort.Strings(names)
		for _, name := range names {
			add(filepath.Join(rootPath, name, "state.json"))
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

func readRuntimeStoreMigrationScopeNames(rootPath string, limit int) ([]string, error) {
	dir, err := os.Open(rootPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open Teams runtime migration scope root %s: %w", rootPath, err)
	}
	defer dir.Close()
	names, err := dir.Readdirnames(limit)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read Teams runtime migration scope root %s: %w", rootPath, err)
	}
	return names, nil
}

func InspectRuntimeStoreForScope(ctx context.Context, scope teamstore.ScopeIdentity) (RuntimeStorePlan, error) {
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
		if legacyExists {
			plan.Action = RuntimeStoreActionQuarantineLegacy
		} else {
			plan.Action = RuntimeStoreActionReady
		}
		return plan, nil
	}
	if legacyExists {
		plan.Action = RuntimeStoreActionMigrateLegacy
		return plan, nil
	}
	resolved, sourcePath, ok, err := discoverRuntimeScopeMigrationSource(ctx, scope, canonicalPath)
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
	if ctx == nil {
		ctx = context.Background()
	}
	scope = normalizeScopeForResolution(scope)
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return "", err
	}
	if samePath(sourcePath, canonicalPath) {
		return canonicalPath, nil
	}
	canonicalDir := filepath.Dir(canonicalPath)
	canonicalParent := filepath.Dir(canonicalDir)
	if err := preflightLegacyQuarantineTargets(scope.ID, sourcePath); err != nil {
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
	defer releaseScopeTakeoverLocks(locks)

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
	if err := validateOfflineRuntimeStoreWriterStopped(ctx, scope, sourcePath); err != nil {
		return "", err
	}
	if err := preflightLegacyQuarantineTargets(scope.ID, sourcePath); err != nil {
		return "", err
	}
	if err := preflightLegacyGlobalLedgersReadOnly(ctx, scope, sourcePath); err != nil {
		return "", err
	}
	if err := UnionLegacyGlobalLedgers(ctx, scope, sourcePath); err != nil {
		return "", fmt.Errorf("merge legacy Teams replay fences: %w", err)
	}
	if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageReplayFencesMerged); err != nil {
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
	if err := copyLegacyStoreToTarget(scope.ID, stagingPath, sourcePath); err != nil {
		return "", fmt.Errorf("copy legacy Teams store to canonical path: %w", err)
	}
	if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageStagingCopied); err != nil {
		return "", err
	}
	if err := validateRuntimeStoreMigrationStaging(ctx, scope, stagingPath, sourcePath); err != nil {
		return "", fmt.Errorf("validate Teams migration staging directory: %w", err)
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

	expectedLegacyPath, expectedErr := legacyDefaultStorePathForScope(scope.ID)
	if expectedErr == nil && samePath(sourcePath, expectedLegacyPath) {
		if registryPath, ok := registryPathForStoreMigrationSource(sourcePath); ok {
			if err := quarantineRelatedFileFamily(scope.ID, registryPath); err != nil {
				return "", fmt.Errorf("quarantine migrated legacy Teams registry: %w", err)
			}
			if err := runRuntimeStoreMigrationTestHook(runtimeStoreMigrationStageRegistryQuarantined); err != nil {
				return "", err
			}
		}
		if handled, err := quarantineScopedStoreDirectory(scope.ID, sourcePath); err != nil {
			return "", fmt.Errorf("quarantine migrated legacy Teams scope: %w", err)
		} else if !handled {
			return "", fmt.Errorf("legacy Teams scope is not a scoped store: %s", sourcePath)
		}
	}
	return canonicalPath, nil
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
	if oldRegistryPath, ok := registryPathForStoreMigrationSource(sourcePath); ok {
		if err := copyRelatedFileFamilyIfPresent(filepath.Join(targetDir, "registry.json"), oldRegistryPath); err != nil {
			return err
		}
	}
	return appdirs.CopyFileReplacing(targetPath, sourcePath)
}

func validateRuntimeStoreMigrationStaging(ctx context.Context, scope teamstore.ScopeIdentity, stagingPath string, sourcePath string) error {
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
	if !scopeStateMatches(scope, state) && !defaultGlobalStoreCanSeedScope(scope, sourcePath, state) {
		return fmt.Errorf("staging store identity does not match scope %q", scope.ID)
	}
	if oldRegistryPath, ok := registryPathForStoreMigrationSource(sourcePath); ok {
		if exists, err := pathExists(oldRegistryPath); err != nil {
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
	registryPath, ok := registryPathForStoreMigrationSource(sourcePath)
	if !ok {
		return paths
	}
	if relatedMigrationFamilyExists(registryPath) {
		paths = append(paths, registryPath+".lock")
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
	expectedLegacyPath, err := legacyDefaultStorePathForScope(scopeID)
	if err != nil || !samePath(sourcePath, expectedLegacyPath) {
		return err
	}
	sourceDir := filepath.Dir(filepath.Clean(sourcePath))
	backupDir := filepath.Dir(migrationBackupPath(sourcePath, scopeID))
	if err := preflightMigrationPathPair(sourceDir, backupDir, true); err != nil {
		return err
	}
	if registryPath, ok := registryPathForStoreMigrationSource(sourcePath); ok {
		if err := preflightMigrationPathPair(registryPath, migrationBackupPath(registryPath, scopeID), false); err != nil {
			return err
		}
	}
	return nil
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

func validateOfflineRuntimeStoreWriterStopped(ctx context.Context, scope teamstore.ScopeIdentity, sourcePath string) error {
	metadata, err := ProbeScopeMetadataReadOnly(ctx, sourcePath)
	if err != nil {
		return fmt.Errorf("probe legacy Teams writer metadata: %w", err)
	}
	state := teamstore.State{
		Scope:          metadata.Scope,
		ControlChat:    metadata.ControlChat,
		ServiceOwner:   metadata.ServiceOwner,
		LockOwner:      metadata.LockOwner,
		ControlLease:   metadata.ControlLease,
		ServiceControl: metadata.ServiceControl,
	}
	if !scopeStateMatches(scope, state) && !defaultGlobalStoreCanSeedScope(scope, sourcePath, state) {
		return fmt.Errorf("legacy Teams store identity does not match scope %q", scope.ID)
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
			"legacy %s is not confirmed stopped (pid=%d host=%q)",
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
			"legacy control lease remains active until %s",
			lease.LeaseUntil.UTC().Format(time.RFC3339Nano),
		))
	}
	return nil
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
		_, err = executeLegacyOnlyMigrationContext(ctx, plan.Scope, plan.LegacyPath)
	case RuntimeStoreActionQuarantineLegacy:
		err = CompleteOfflineRuntimeStoreTakeover(ctx, plan.Scope, plan.LegacyPath)
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
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	scope = normalizeScopeForResolution(scope)
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
	expectedLegacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		return err
	}
	if !samePath(legacyPath, expectedLegacyPath) {
		return fmt.Errorf("offline Teams store takeover expected legacy path %s, got %s", expectedLegacyPath, legacyPath)
	}
	if err := preflightLegacyQuarantineTargets(scope.ID, legacyPath); err != nil {
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

	lockPaths := migrationLockPathsForStore(legacyPath)
	if storeAlreadyQuarantined && len(lockPaths) > 0 {
		lockPaths = lockPaths[1:]
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
	defer releaseScopeTakeoverLocks(locks)
	if !storeAlreadyQuarantined {
		if err := validateOfflineRuntimeStoreWriterStopped(ctx, scope, legacyPath); err != nil {
			return err
		}
	}
	if err := preflightLegacyQuarantineTargets(scope.ID, legacyPath); err != nil {
		return err
	}
	if err := preflightLegacyGlobalLedgersReadOnly(ctx, scope, legacyPath); err != nil {
		return err
	}
	if err := UnionLegacyGlobalLedgers(ctx, scope, legacyPath); err != nil {
		return err
	}

	// Move the cross-root registry first. If the process stops between these two
	// operations, state.json remains discoverable and the next service start
	// retries the idempotent registry move before committing the scope handoff.
	if registryPath, ok := registryPathForStoreMigrationSource(legacyPath); ok {
		if err := quarantineRelatedFileFamily(scope.ID, registryPath); err != nil {
			return fmt.Errorf("quarantine legacy Teams registry: %w", err)
		}
	}
	if !storeAlreadyQuarantined {
		if handled, err := quarantineScopedStoreDirectory(scope.ID, legacyPath); err != nil {
			return fmt.Errorf("quarantine legacy Teams scope: %w", err)
		} else if !handled {
			return fmt.Errorf("legacy Teams scope is not a scoped store: %s", legacyPath)
		}
	}
	return nil
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

func preflightLegacyGlobalLedgersReadOnly(ctx context.Context, scope teamstore.ScopeIdentity, legacyStorePath string) error {
	legacyRegistryPath, ok := registryPathForStoreMigrationSource(legacyStorePath)
	if !ok {
		return nil
	}
	canonicalRegistryPath, err := DefaultRegistryPathForScope(scope.ID)
	if err != nil {
		return err
	}
	if legacyPath, ok := globalInboundLedgerPathForRegistry(legacyRegistryPath); ok {
		if _, err := readCompletedGlobalInboundForUnion(ctx, legacyPath); err != nil {
			return fmt.Errorf("preflight legacy global inbound ledger: %w", err)
		}
	}
	if canonicalPath, ok := globalInboundLedgerPathForRegistry(canonicalRegistryPath); ok {
		if _, err := readGlobalInboundForUnion(ctx, canonicalPath); err != nil {
			return fmt.Errorf("preflight canonical global inbound ledger: %w", err)
		}
	}
	if legacyPath, ok := globalOutboundLedgerPathForRegistry(legacyRegistryPath); ok {
		if _, err := readGlobalOutboundForUnion(ctx, legacyPath); err != nil {
			return fmt.Errorf("preflight legacy global outbound ledger: %w", err)
		}
	}
	if canonicalPath, ok := globalOutboundLedgerPathForRegistry(canonicalRegistryPath); ok {
		if _, err := readGlobalOutboundForUnion(ctx, canonicalPath); err != nil {
			return fmt.Errorf("preflight canonical global outbound ledger: %w", err)
		}
	}
	return nil
}

// UnionLegacyGlobalLedgers copies only monotonic replay fences into the
// canonical ledgers. The legacy ledgers are shared by every legacy scope, so
// this function never moves, truncates, or deletes them. Holding their normal
// writer locks gives this migration a consistent snapshot while allowing a
// later scope migration to union any subsequently added rows.
func UnionLegacyGlobalLedgers(ctx context.Context, scope teamstore.ScopeIdentity, legacyStorePath string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	legacyRegistryPath, ok := registryPathForStoreMigrationSource(legacyStorePath)
	if !ok {
		return nil
	}
	canonicalRegistryPath, err := DefaultRegistryPathForScope(scope.ID)
	if err != nil {
		return err
	}

	legacyInboundPath, legacyInboundOK := globalInboundLedgerPathForRegistry(legacyRegistryPath)
	canonicalInboundPath, canonicalInboundOK := globalInboundLedgerPathForRegistry(canonicalRegistryPath)
	if legacyInboundOK && canonicalInboundOK && !samePath(legacyInboundPath, canonicalInboundPath) {
		var locks []heldScopeTakeoverLock
		if relatedMigrationFamilyExists(legacyInboundPath, teamsLedgerSQLitePath(legacyInboundPath)) {
			locks, err = acquireScopeTakeoverLocks(ctx, []string{legacyInboundPath + ".lock"})
			if err != nil {
				return fmt.Errorf("lock legacy global Teams inbound ledger: %w", err)
			}
		}
		legacyItems, err := readCompletedGlobalInboundForUnion(ctx, legacyInboundPath)
		releaseScopeTakeoverLocks(locks)
		if err != nil {
			return fmt.Errorf("read legacy global inbound ledger: %w", err)
		}
		canonicalItems, err := readGlobalInboundForUnion(ctx, canonicalInboundPath)
		if err != nil {
			return fmt.Errorf("read canonical global inbound ledger: %w", err)
		}
		var delta []globalInboundItem
		for key, item := range legacyItems {
			if canonical, ok := canonicalItems[key]; ok && canonical.Status == "done" {
				continue
			}
			delta = append(delta, item)
		}
		if len(delta) > 0 {
			if err := updateGlobalInboundSQLite(ctx, canonicalInboundPath, func(tx *sql.Tx, _ time.Time) error {
				for _, item := range delta {
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
	}

	legacyOutboundPath, legacyOutboundOK := globalOutboundLedgerPathForRegistry(legacyRegistryPath)
	canonicalOutboundPath, canonicalOutboundOK := globalOutboundLedgerPathForRegistry(canonicalRegistryPath)
	if legacyOutboundOK && canonicalOutboundOK && !samePath(legacyOutboundPath, canonicalOutboundPath) {
		var locks []heldScopeTakeoverLock
		if relatedMigrationFamilyExists(legacyOutboundPath, teamsLedgerSQLitePath(legacyOutboundPath)) {
			locks, err = acquireScopeTakeoverLocks(ctx, []string{legacyOutboundPath + ".lock"})
			if err != nil {
				return fmt.Errorf("lock legacy global Teams outbound ledger: %w", err)
			}
		}
		legacyItems, err := readGlobalOutboundForUnion(ctx, legacyOutboundPath)
		releaseScopeTakeoverLocks(locks)
		if err != nil {
			return fmt.Errorf("read legacy global outbound ledger: %w", err)
		}
		canonicalItems, err := readGlobalOutboundForUnion(ctx, canonicalOutboundPath)
		if err != nil {
			return fmt.Errorf("read canonical global outbound ledger: %w", err)
		}
		if err := applyLegacyOutboxReplayFences(ctx, scope, legacyItems); err != nil {
			return err
		}
		delta := make([]globalOutboundItem, 0)
		for key, item := range legacyItems {
			if _, ok := canonicalItems[key]; ok {
				continue
			}
			delta = append(delta, item)
		}
		if len(delta) > 0 {
			if err := recordMissingGlobalOutboundBatch(ctx, canonicalOutboundPath, delta, time.Now()); err != nil {
				return fmt.Errorf("merge legacy global outbound ledger: %w", err)
			}
		}
	}
	return nil
}

func applyLegacyOutboxReplayFences(ctx context.Context, scope teamstore.ScopeIdentity, items map[string]globalOutboundItem) error {
	if len(items) == 0 {
		return nil
	}
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return err
	}
	exists, err := inspectRuntimeStorePath(canonicalPath)
	if err != nil || !exists {
		return err
	}
	fences := make([]teamstore.OutboxReplayFence, 0, len(items))
	for _, item := range items {
		if item.ScopeID != "" && item.ScopeID != scope.ID {
			continue
		}
		fences = append(fences, teamstore.OutboxReplayFence{
			OutboxID:       item.OutboxID,
			TeamsChatID:    item.ChatID,
			TeamsMessageID: item.MessageID,
			SessionID:      item.SessionID,
			TurnID:         item.TurnID,
			Kind:           item.Kind,
		})
	}
	store, err := teamstore.Open(canonicalPath)
	if err != nil {
		return fmt.Errorf("open canonical Teams store for outbound replay fence: %w", err)
	}
	_, applyErr := store.ApplyOutboxReplayFences(ctx, fences)
	closeErr := store.Close()
	if applyErr != nil {
		return fmt.Errorf("apply legacy outbound replay fence: %w", applyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close canonical Teams store after outbound replay fence: %w", closeErr)
	}
	return nil
}
