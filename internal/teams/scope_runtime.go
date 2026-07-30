package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// ProbeScopeMetadataReadOnly reads only the JSON pointer/metadata file. It does
// not open SQLite, acquire a flock, create a sidecar, migrate, chmod, or
// checkpoint WAL.
func ProbeScopeMetadataReadOnly(ctx context.Context, path string) (ScopeStoreMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ScopeStoreMetadata{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return ScopeStoreMetadata{}, err
	}
	defer func() { _ = f.Close() }()
	var metadata ScopeStoreMetadata
	if err := json.NewDecoder(&contextReader{ctx: ctx, r: f}).Decode(&metadata); err != nil {
		return ScopeStoreMetadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return ScopeStoreMetadata{}, err
	}
	return metadata, nil
}

type contextReader struct {
	ctx context.Context
	r   *os.File
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func discoverRuntimeScopeMigrationSource(scope teamstore.ScopeIdentity, currentPath string) (teamstore.ScopeIdentity, string, bool, error) {
	matches, err := discoverRuntimeScopeMigrationMatches(scope, currentPath)
	if err != nil {
		return teamstore.ScopeIdentity{}, "", false, err
	}
	// A concurrent migrator briefly exposes both the newly copied canonical
	// store and its not-yet-quarantined legacy source. Wait on that canonical
	// store's migration coordinator and probe again. A pre-existing divergent
	// dual store remains ambiguous after the lock is acquired and still fails
	// closed; this only removes the copy/quarantine race between resolvers.
	if len(matches) > 1 {
		if canonical, ok := singleCanonicalMigrationCandidate(matches); ok {
			coordinator, lockErr := acquireScopeMigrationCoordinatorLock(filepath.Join(filepath.Dir(canonical.path), ".migration.lock"))
			if lockErr != nil {
				return teamstore.ScopeIdentity{}, "", false, lockErr
			}
			matches, err = discoverRuntimeScopeMigrationMatches(scope, currentPath)
			releaseScopeTakeoverLocks([]heldScopeTakeoverLock{coordinator})
			if err != nil {
				return teamstore.ScopeIdentity{}, "", false, err
			}
		}
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

func discoverRuntimeScopeMigrationMatches(scope teamstore.ScopeIdentity, currentPath string) ([]resolvedScopeStoreCandidate, error) {
	paths, err := candidateScopeStorePaths(currentPath)
	if err != nil {
		return nil, err
	}
	var matches []resolvedScopeStoreCandidate
	for _, path := range paths {
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
		metadata, err := ProbeScopeMetadataReadOnly(context.Background(), path)
		if err != nil {
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

func singleCanonicalMigrationCandidate(matches []resolvedScopeStoreCandidate) (resolvedScopeStoreCandidate, bool) {
	var canonical resolvedScopeStoreCandidate
	canonicalCount := 0
	scopeID := ""
	for _, match := range matches {
		matchScopeID := strings.TrimSpace(match.scope.ID)
		if matchScopeID == "" {
			return resolvedScopeStoreCandidate{}, false
		}
		if scopeID == "" {
			scopeID = matchScopeID
		} else if scopeID != matchScopeID {
			return resolvedScopeStoreCandidate{}, false
		}
		path, err := DefaultStorePathForScope(matchScopeID)
		if err != nil {
			return resolvedScopeStoreCandidate{}, false
		}
		if samePath(match.path, path) {
			canonical = match
			canonicalCount++
		}
	}
	return canonical, canonicalCount == 1
}

func executeLegacyOnlyMigration(scope teamstore.ScopeIdentity, sourcePath string) (string, error) {
	scope = normalizeScopeForResolution(scope)
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return "", err
	}
	if samePath(sourcePath, canonicalPath) {
		return canonicalPath, nil
	}
	if err := os.MkdirAll(filepath.Dir(canonicalPath), 0o700); err != nil {
		return "", fmt.Errorf("prepare canonical Teams migration directory: %w", err)
	}
	coordinator, err := acquireScopeMigrationCoordinatorLock(filepath.Join(filepath.Dir(canonicalPath), ".migration.lock"))
	if err != nil {
		return "", err
	}
	defer releaseScopeTakeoverLocks([]heldScopeTakeoverLock{coordinator})

	canonicalExists, err := pathExists(canonicalPath)
	if err != nil {
		return "", fmt.Errorf("inspect canonical Teams migration target: %w", err)
	}
	sourceExists, err := pathExists(sourcePath)
	if err != nil {
		return "", fmt.Errorf("inspect legacy Teams migration source: %w", err)
	}
	if canonicalExists && !sourceExists {
		return canonicalPath, nil
	}
	if !sourceExists {
		return "", fmt.Errorf("legacy Teams migration source disappeared before migration: %s", sourcePath)
	}
	locks, err := acquireScopeTakeoverLocks(context.Background(), migrationLockPathsForStore(sourcePath))
	if err != nil {
		return "", fmt.Errorf("lock legacy Teams migration source: %w", err)
	}
	defer releaseScopeTakeoverLocks(locks)

	// Another process can finish the migration between discovery and the
	// coordinator handoff. Recheck after all source-family locks are held so a
	// resumed copy never races a writer or replaces an already-complete target.
	canonicalExists, err = pathExists(canonicalPath)
	if err != nil {
		return "", fmt.Errorf("reinspect canonical Teams migration target: %w", err)
	}
	sourceExists, err = pathExists(sourcePath)
	if err != nil {
		return "", fmt.Errorf("reinspect legacy Teams migration source: %w", err)
	}
	if canonicalExists && !sourceExists {
		return canonicalPath, nil
	}
	if !sourceExists {
		return "", fmt.Errorf("legacy Teams migration source disappeared while acquiring migration locks: %s", sourcePath)
	}
	if err := copyLegacyStoreToCanonical(scope.ID, canonicalPath, sourcePath); err != nil {
		return "", fmt.Errorf("copy legacy Teams store to canonical path: %w", err)
	}
	if !scopeMigrationComplete(scope.ID, canonicalPath, sourcePath) {
		return "", fmt.Errorf("legacy Teams migration validation did not complete for %s", sourcePath)
	}
	if err := quarantineStoreMigrationSource(scope.ID, sourcePath); err != nil {
		return "", fmt.Errorf("quarantine migrated legacy Teams store: %w", err)
	}
	return canonicalPath, nil
}

func acquireScopeMigrationCoordinatorLock(path string) (heldScopeTakeoverLock, error) {
	lock := flock.New(path)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func copyLegacyStoreToCanonical(scopeID string, canonicalPath string, sourcePath string) error {
	if err := migrateStoreDirRelatedFiles(filepath.Dir(canonicalPath), filepath.Dir(sourcePath)); err != nil {
		return err
	}
	oldRegistryPath, hasRegistry := registryPathForStoreMigrationSource(sourcePath)
	newRegistryPath, err := appdirs.StatePath("teams", "scopes", safeScopePathPart(scopeID), "registry.json")
	if err != nil {
		return err
	}
	if hasRegistry {
		if err := copyRelatedFileFamilyIfPresent(newRegistryPath, oldRegistryPath); err != nil {
			return err
		}
		if err := copyScopeLedgerFilesUnlocked(newRegistryPath, oldRegistryPath); err != nil {
			return err
		}
	}
	return appdirs.CopyFileReplacing(canonicalPath, sourcePath)
}

func copyScopeLedgerFilesUnlocked(newRegistryPath string, oldRegistryPath string) error {
	oldInboundPath, oldInboundOK := globalInboundLedgerPathForRegistry(oldRegistryPath)
	newInboundPath, newInboundOK := globalInboundLedgerPathForRegistry(newRegistryPath)
	if oldInboundOK && newInboundOK {
		if err := copyRelatedFileFamilyIfPresent(newInboundPath, oldInboundPath); err != nil {
			return err
		}
		if err := copyRelatedFileFamilyIfPresent(teamsLedgerSQLitePath(newInboundPath), teamsLedgerSQLitePath(oldInboundPath), "-wal", "-shm"); err != nil {
			return err
		}
	}
	oldOutboundPath, oldOutboundOK := globalOutboundLedgerPathForRegistry(oldRegistryPath)
	newOutboundPath, newOutboundOK := globalOutboundLedgerPathForRegistry(newRegistryPath)
	if oldOutboundOK && newOutboundOK {
		if err := copyRelatedFileFamilyIfPresent(newOutboundPath, oldOutboundPath); err != nil {
			return err
		}
		if err := copyRelatedFileFamilyIfPresent(teamsLedgerSQLitePath(newOutboundPath), teamsLedgerSQLitePath(oldOutboundPath), "-wal", "-shm"); err != nil {
			return err
		}
	}
	return nil
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
	if path, ok := globalInboundLedgerPathForRegistry(registryPath); ok {
		if relatedMigrationFamilyExists(path, teamsLedgerSQLitePath(path)) {
			paths = append(paths, path+".lock")
		}
	}
	if path, ok := globalOutboundLedgerPathForRegistry(registryPath); ok {
		if relatedMigrationFamilyExists(path, teamsLedgerSQLitePath(path)) {
			paths = append(paths, path+".lock")
		}
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

func quarantineStoreMigrationSource(scopeID string, sourcePath string) error {
	if err := quarantineStoreFileFamily(scopeID, sourcePath); err != nil {
		return err
	}
	registryPath, hasRegistry := registryPathForStoreMigrationSource(sourcePath)
	if hasRegistry {
		if inboundPath, ok := globalInboundLedgerPathForRegistry(registryPath); ok {
			if err := quarantineLedgerSource(scopeID, inboundPath); err != nil {
				return err
			}
		}
		if outboundPath, ok := globalOutboundLedgerPathForRegistry(registryPath); ok {
			if err := quarantineLedgerSource(scopeID, outboundPath); err != nil {
				return err
			}
		}
		if err := quarantineRelatedFileFamily(scopeID, registryPath); err != nil {
			return err
		}
	}
	return nil
}

func quarantineStoreFileFamily(scopeID string, sourcePath string) error {
	sourceDir := filepath.Dir(sourcePath)
	if err := quarantineRelatedFileFamily(scopeID, filepath.Join(sourceDir, teamstore.SQLiteFileName), "-wal", "-shm"); err != nil {
		return err
	}
	for _, name := range storeSidecarNames() {
		if err := quarantineRelatedFileFamily(scopeID, filepath.Join(sourceDir, name)); err != nil {
			return err
		}
	}
	return quarantineRelatedFileFamily(scopeID, sourcePath)
}

func quarantineLedgerSource(scopeID string, jsonPath string) error {
	if err := quarantineRelatedFileFamily(scopeID, teamsLedgerSQLitePath(jsonPath), "-wal", "-shm"); err != nil {
		return err
	}
	return quarantineRelatedFileFamily(scopeID, jsonPath)
}

func quarantineRelatedFileFamily(scopeID string, sourceBase string, suffixes ...string) error {
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
	}
	return nil
}

// AutomaticScopeTakeoverOptions contains orchestration hooks owned by the CLI
// service layer. File and replay mechanics remain in this package.
type AutomaticScopeTakeoverOptions struct {
	SafetyCheck         func(context.Context) error
	ValidateLegacyState func(context.Context, teamstore.State) error
	FenceWriter         func(context.Context) error
	StartCanonical      func(context.Context, teamstore.ScopeIdentity, string) error
	OnStage             func(string)
	FailAfterStage      string
}

type AutomaticScopeTakeoverResult struct {
	CanonicalPath     string
	Resolved          bool
	RecoveryInventory []string
	PreflightCount    int
	StartedCanonical  bool
}

func ExecuteAutomaticScopeTakeover(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	legacySources []string,
	opts AutomaticScopeTakeoverOptions,
) (AutomaticScopeTakeoverResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	scope = normalizeScopeForResolution(scope)
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return AutomaticScopeTakeoverResult{}, err
	}
	result := AutomaticScopeTakeoverResult{CanonicalPath: canonicalPath}
	if opts.SafetyCheck != nil {
		if err := opts.SafetyCheck(ctx); err != nil {
			return result, fmt.Errorf("automatic takeover deferred: %w", err)
		}
	}
	pending, err := validateTakeoverBackupStates(scope.ID, legacySources)
	if err != nil {
		return result, err
	}
	legacyState, identity, found, err := preflightLegacyTakeoverState(ctx, scope, pending)
	result.PreflightCount++
	if err != nil {
		return result, err
	}
	if found {
		if reason := unsafeLegacyTakeoverReason(scope, legacyState, opts.FenceWriter != nil); reason != "" {
			return result, fmt.Errorf("automatic takeover deferred: %s", reason)
		}
		if opts.ValidateLegacyState != nil {
			if err := opts.ValidateLegacyState(ctx, legacyState); err != nil {
				return result, fmt.Errorf("automatic takeover deferred: %w", err)
			}
		}
	}
	stage(opts, "preflight-complete")
	if found {
		nextIdentity, err := fileIdentity(filepath.Clean(identity.path))
		if err != nil {
			return result, fmt.Errorf("automatic takeover re-preflight: %w", err)
		}
		if nextIdentity != identity {
			legacyState, identity, found, err = preflightLegacyTakeoverState(ctx, scope, pending)
			result.PreflightCount++
			if err != nil {
				return result, err
			}
			if found {
				if reason := unsafeLegacyTakeoverReason(scope, legacyState, opts.FenceWriter != nil); reason != "" {
					return result, fmt.Errorf("automatic takeover deferred after re-preflight: %s", reason)
				}
				if opts.ValidateLegacyState != nil {
					if err := opts.ValidateLegacyState(ctx, legacyState); err != nil {
						return result, fmt.Errorf("automatic takeover deferred after re-preflight: %w", err)
					}
				}
			}
		}
	}

	if err := failTakeoverStage(opts, "after-drain"); err != nil {
		return result, err
	}
	if opts.FenceWriter != nil {
		if err := opts.FenceWriter(ctx); err != nil {
			return result, fmt.Errorf("automatic takeover deferred while fencing writer: %w", err)
		}
	}
	if err := failTakeoverStage(opts, "after-writer-exit"); err != nil {
		return result, err
	}

	lockPaths := make([]string, 0, len(pending))
	for _, source := range pending {
		lockPaths = append(lockPaths, source+".lock")
	}
	locks, err := acquireScopeTakeoverLocks(ctx, lockPaths)
	if err != nil {
		return result, fmt.Errorf("automatic takeover deferred: %w", err)
	}
	defer releaseScopeTakeoverLocks(locks)

	if found {
		currentIdentity, err := fileIdentity(identity.path)
		if err != nil {
			return result, fmt.Errorf("automatic takeover revalidate legacy identity: %w", err)
		}
		if currentIdentity != identity {
			return result, fmt.Errorf("automatic takeover deferred: legacy identity changed after fencing; complete re-preflight required")
		}
	}

	if err := failTakeoverStage(opts, "before-replay-fence"); err != nil {
		return result, err
	}
	if found {
		inventory, err := mergeLegacyReplaySuppression(ctx, canonicalPath, legacyState)
		if err != nil {
			return result, err
		}
		result.RecoveryInventory = inventory
	}
	if err := failTakeoverStage(opts, "after-replay-fence"); err != nil {
		return result, err
	}

	for _, root := range []string{"config", "cache", "data", "other"} {
		var sources []string
		for _, source := range pending {
			if takeoverSourceRoot(source) == root {
				sources = append(sources, source)
			}
		}
		if len(sources) == 0 {
			continue
		}
		sort.Strings(sources)
		if root != "other" {
			if err := failTakeoverStage(opts, "before-rename-"+root); err != nil {
				return result, err
			}
		}
		for _, source := range sources {
			if err := quarantineTakeoverSource(scope.ID, source); err != nil {
				return result, err
			}
		}
		if root != "other" {
			if err := failTakeoverStage(opts, "after-rename-"+root); err != nil {
				return result, err
			}
		}
	}
	if err := failTakeoverStage(opts, "before-canonical-listener-start"); err != nil {
		return result, err
	}
	if opts.StartCanonical != nil {
		if err := opts.StartCanonical(ctx, scope, canonicalPath); err != nil {
			return result, err
		}
		result.StartedCanonical = true
	}
	result.Resolved = true
	return result, nil
}

func validateTakeoverBackupStates(scopeID string, sources []string) ([]string, error) {
	var pending []string
	for _, source := range sources {
		source = strings.TrimSpace(source)
		if source == "" {
			continue
		}
		backup := migrationBackupPath(source, scopeID)
		sourceExists, sourceErr := regularMigrationPathState(source)
		backupExists, backupErr := regularMigrationPathState(backup)
		if sourceErr != nil || backupErr != nil {
			return nil, errors.Join(sourceErr, backupErr)
		}
		if sourceExists && backupExists {
			return nil, fmt.Errorf("legacy migration backup conflict or reappearance: source %s and backup %s both exist", source, backup)
		}
		if sourceExists {
			pending = append(pending, source)
		}
	}
	return pending, nil
}

func regularMigrationPathState(path string) (bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("migration path is not a regular file: %s", path)
	}
	return true, nil
}

type takeoverFileIdentity struct {
	path    string
	size    int64
	modTime int64
	mode    os.FileMode
	family  string
}

func fileIdentity(path string) (takeoverFileIdentity, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return takeoverFileIdentity{}, err
	}
	var family strings.Builder
	familyPaths := []string{path}
	if filepath.Base(path) == "state.json" {
		sqlitePath := filepath.Join(filepath.Dir(path), teamstore.SQLiteFileName)
		familyPaths = append(familyPaths, sqlitePath, sqlitePath+"-wal", sqlitePath+"-shm")
	}
	for _, familyPath := range familyPaths {
		familyInfo, familyErr := os.Lstat(familyPath)
		if os.IsNotExist(familyErr) {
			fmt.Fprintf(&family, "%s:missing;", filepath.Base(familyPath))
			continue
		}
		if familyErr != nil {
			return takeoverFileIdentity{}, familyErr
		}
		fmt.Fprintf(
			&family,
			"%s:%d:%d:%d;",
			filepath.Base(familyPath),
			familyInfo.Size(),
			familyInfo.ModTime().UnixNano(),
			familyInfo.Mode(),
		)
	}
	return takeoverFileIdentity{
		path:    filepath.Clean(path),
		size:    info.Size(),
		modTime: info.ModTime().UnixNano(),
		mode:    info.Mode(),
		family:  family.String(),
	}, nil
}

func preflightLegacyTakeoverState(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	sources []string,
) (teamstore.State, takeoverFileIdentity, bool, error) {
	for _, source := range sources {
		if filepath.Base(source) != "state.json" {
			continue
		}
		identity, err := fileIdentity(source)
		if err != nil {
			return teamstore.State{}, takeoverFileIdentity{}, false, err
		}
		state, err := teamstore.LoadPathReadOnly(ctx, source)
		if err != nil {
			return teamstore.State{}, takeoverFileIdentity{}, false, fmt.Errorf("load legacy takeover store %s: %w", source, err)
		}
		if !scopeStateMatches(scope, state) {
			return teamstore.State{}, takeoverFileIdentity{}, false, fmt.Errorf("legacy takeover store %s does not match scope %q", source, scope.ID)
		}
		return state, identity, true, nil
	}
	return teamstore.State{}, takeoverFileIdentity{}, false, nil
}

func unsafeLegacyTakeoverReason(scope teamstore.ScopeIdentity, state teamstore.State, canFenceLocalWriter bool) string {
	for _, turn := range state.Turns {
		if turn.Status == teamstore.TurnStatusQueued || turn.Status == teamstore.TurnStatusRunning {
			return "active turn must drain before legacy takeover"
		}
	}
	now := time.Now()
	for _, owner := range []*teamstore.OwnerMetadata{state.ServiceOwner, state.LockOwner} {
		if owner == nil || teamstore.IsStale(*owner, scopeStoreOwnerFreshAfter, now) {
			continue
		}
		if host := strings.TrimSpace(owner.Hostname); host != "" && !strings.EqualFold(host, machineLabel()) {
			return fmt.Sprintf("remote owner %s is still live", host)
		}
		if machineID := strings.TrimSpace(owner.MachineID); machineID != "" {
			localMachine := MachineRecordForUser(User{
				ID:                scope.AccountID,
				UserPrincipalName: scope.UserPrincipal,
			}, scope)
			if machineID != localMachine.ID {
				return fmt.Sprintf("owner %s is not a locally manageable writer", machineID)
			}
		}
		if !canFenceLocalWriter {
			return fmt.Sprintf("live local owner pid %d requires service fencing", owner.PID)
		}
	}
	return ""
}

// PrepareRuntimeStoreForListener performs the dual-store cold path immediately
// before a listener opens the canonical store. The canonical-only path checks
// one exact legacy path and performs no writes.
func PrepareRuntimeStoreForListener(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
	opts AutomaticScopeTakeoverOptions,
) (AutomaticScopeTakeoverResult, error) {
	scope = normalizeScopeForResolution(scope)
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return AutomaticScopeTakeoverResult{}, err
	}
	result := AutomaticScopeTakeoverResult{CanonicalPath: canonicalPath}
	if exists, err := pathExists(canonicalPath); err != nil {
		return result, err
	} else if !exists {
		return result, nil
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		return result, err
	}
	if exists, err := pathExists(legacyPath); err != nil {
		return result, err
	} else if !exists {
		result.Resolved = true
		return result, nil
	}
	sources := []string{legacyPath}
	if registryPath, ok := registryPathForStoreMigrationSource(legacyPath); ok {
		if relatedMigrationFamilyExists(registryPath) {
			sources = append(sources, registryPath)
		}
		if path, ok := globalInboundLedgerPathForRegistry(registryPath); ok &&
			relatedMigrationFamilyExists(path, teamsLedgerSQLitePath(path)) {
			sources = append(sources, path)
		}
		if path, ok := globalOutboundLedgerPathForRegistry(registryPath); ok &&
			relatedMigrationFamilyExists(path, teamsLedgerSQLitePath(path)) {
			sources = append(sources, path)
		}
	}
	return ExecuteAutomaticScopeTakeover(ctx, scope, sources, opts)
}

func mergeLegacyReplaySuppression(ctx context.Context, canonicalPath string, legacy teamstore.State) ([]string, error) {
	st, err := teamstore.Open(canonicalPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()
	var inventory []string
	err = st.Update(ctx, func(state *teamstore.State) error {
		if state.InboundEvents == nil {
			state.InboundEvents = make(map[string]teamstore.InboundEvent)
		}
		if state.OutboxMessages == nil {
			state.OutboxMessages = make(map[string]teamstore.OutboxMessage)
		}
		for id, event := range legacy.InboundEvents {
			if event.Status != teamstore.InboundStatusIgnored {
				continue
			}
			if _, exists := state.InboundEvents[id]; exists || inboundMessageAlreadyRecorded(state.InboundEvents, event) {
				continue
			}
			state.InboundEvents[id] = event
		}
		for id, message := range legacy.OutboxMessages {
			if message.Status == teamstore.OutboxStatusSent {
				if _, exists := state.OutboxMessages[id]; !exists {
					state.OutboxMessages[id] = message
				}
				continue
			}
			inventory = append(inventory, id)
		}
		return nil
	})
	sort.Strings(inventory)
	return inventory, err
}

func inboundMessageAlreadyRecorded(events map[string]teamstore.InboundEvent, candidate teamstore.InboundEvent) bool {
	for _, event := range events {
		if strings.TrimSpace(candidate.TeamsChatID) != "" &&
			strings.TrimSpace(candidate.TeamsMessageID) != "" &&
			strings.TrimSpace(event.TeamsChatID) == strings.TrimSpace(candidate.TeamsChatID) &&
			strings.TrimSpace(event.TeamsMessageID) == strings.TrimSpace(candidate.TeamsMessageID) {
			return true
		}
	}
	return false
}

func quarantineTakeoverSource(scopeID string, source string) error {
	switch filepath.Base(source) {
	case "state.json":
		return quarantineStoreFileFamily(scopeID, source)
	case "registry.json":
		return quarantineRelatedFileFamily(scopeID, source)
	case "inbound-ledger.jsonl", "outbound-ledger.jsonl":
		return quarantineLedgerSource(scopeID, source)
	default:
		return quarantineRelatedFileFamily(scopeID, source)
	}
}

func takeoverSourceRoot(path string) string {
	clean := filepath.Clean(path)
	for _, item := range []struct {
		env  string
		name string
	}{
		{env: "XDG_CONFIG_HOME", name: "config"},
		{env: "XDG_CACHE_HOME", name: "cache"},
		{env: "XDG_DATA_HOME", name: "data"},
	} {
		root := filepath.Clean(strings.TrimSpace(os.Getenv(item.env)))
		if root != "" && root != "." && (clean == root || strings.HasPrefix(clean, root+string(filepath.Separator))) {
			return item.name
		}
	}
	return "other"
}

func stage(opts AutomaticScopeTakeoverOptions, name string) {
	if opts.OnStage != nil {
		opts.OnStage(name)
	}
}

func failTakeoverStage(opts AutomaticScopeTakeoverOptions, name string) error {
	stage(opts, name)
	if strings.TrimSpace(opts.FailAfterStage) == name {
		return fmt.Errorf("injected takeover crash at %s", name)
	}
	return nil
}
