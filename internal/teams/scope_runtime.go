package teams

import (
	"context"
	"database/sql"
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
	if !sourceExists {
		return "", fmt.Errorf("legacy Teams migration source disappeared while acquiring migration locks: %s", sourcePath)
	}
	if err := copyLegacyStoreToCanonical(scope.ID, canonicalPath, sourcePath); err != nil {
		return "", fmt.Errorf("copy legacy Teams store to canonical path: %w", err)
	}
	if err := UnionLegacyGlobalLedgers(context.Background(), scope, sourcePath); err != nil {
		return "", fmt.Errorf("merge legacy Teams replay fences: %w", err)
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
	}
	return appdirs.CopyFileReplacing(canonicalPath, sourcePath)
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

func quarantineStoreMigrationSource(scopeID string, sourcePath string) error {
	if err := quarantineStoreFileFamily(scopeID, sourcePath); err != nil {
		return err
	}
	registryPath, hasRegistry := registryPathForStoreMigrationSource(sourcePath)
	if hasRegistry {
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
	_ = syncParentDir(sourceDir)
	_ = syncParentDir(backupDir)
	return true, nil
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

// PrepareRuntimeStoreForListener is a pure path resolver. It never opens a
// store, writes migration state, drains a listener, fences a process, or
// moves files. A managed service may complete TakeoverRequired before entering
// Listen; foreground listeners fail closed.
func PrepareRuntimeStoreForListener(
	ctx context.Context,
	scope teamstore.ScopeIdentity,
) (RuntimeStorePreparation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RuntimeStorePreparation{}, err
	}
	scope = normalizeScopeForResolution(scope)
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return RuntimeStorePreparation{}, err
	}
	result := RuntimeStorePreparation{CanonicalPath: canonicalPath}
	canonicalExists, err := inspectRuntimeStorePath(canonicalPath)
	if err != nil {
		return result, err
	}

	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		return result, err
	}
	result.LegacyPath = legacyPath
	legacyExists, err := inspectRuntimeStorePath(legacyPath)
	if err != nil {
		return result, err
	}
	if canonicalExists && !legacyExists {
		result.Resolved = true
		return result, nil
	}
	if canonicalExists && legacyExists {
		result.TakeoverRequired = true
		return result, nil
	}
	// Canonical-missing resolution remains the existing migration cold path,
	// which retains compatibility with historical scope IDs and global stores.
	return result, nil
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
	coordinator, err := acquireScopeMigrationCoordinatorLock(canonicalPath + ".takeover.lock")
	if err != nil {
		releaseScopeTakeoverLocks(locks)
		return deferRuntimeStoreTakeover(err.Error())
	}
	defer releaseScopeTakeoverLocks([]heldScopeTakeoverLock{coordinator})
	if err := UnionLegacyGlobalLedgers(ctx, scope, legacyPath); err != nil {
		releaseScopeTakeoverLocks(locks)
		return err
	}
	releaseScopeTakeoverLocks(locks)

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
	var legacyLockPaths []string
	if path, ok := globalInboundLedgerPathForRegistry(legacyRegistryPath); ok &&
		relatedMigrationFamilyExists(path, teamsLedgerSQLitePath(path)) {
		legacyLockPaths = append(legacyLockPaths, path+".lock")
	}
	if path, ok := globalOutboundLedgerPathForRegistry(legacyRegistryPath); ok &&
		relatedMigrationFamilyExists(path, teamsLedgerSQLitePath(path)) {
		legacyLockPaths = append(legacyLockPaths, path+".lock")
	}
	locks, err := acquireScopeTakeoverLocks(ctx, legacyLockPaths)
	if err != nil {
		return fmt.Errorf("lock legacy global Teams ledgers: %w", err)
	}
	defer releaseScopeTakeoverLocks(locks)

	canonicalRegistryPath, err := DefaultRegistryPathForScope(scope.ID)
	if err != nil {
		return err
	}

	legacyInboundPath, legacyInboundOK := globalInboundLedgerPathForRegistry(legacyRegistryPath)
	canonicalInboundPath, canonicalInboundOK := globalInboundLedgerPathForRegistry(canonicalRegistryPath)
	if legacyInboundOK && canonicalInboundOK && !samePath(legacyInboundPath, canonicalInboundPath) {
		legacyItems, err := readCompletedGlobalInboundForUnion(ctx, legacyInboundPath)
		if err != nil {
			return fmt.Errorf("read legacy global inbound ledger: %w", err)
		}
		canonicalItems, err := readCompletedGlobalInboundForUnion(ctx, canonicalInboundPath)
		if err != nil {
			return fmt.Errorf("read canonical global inbound ledger: %w", err)
		}
		var delta []globalInboundItem
		for key, item := range legacyItems {
			if _, ok := canonicalItems[key]; ok {
				continue
			}
			delta = append(delta, item)
		}
		if len(delta) > 0 {
			if err := updateGlobalInboundSQLite(ctx, canonicalInboundPath, func(tx *sql.Tx, _ time.Time) error {
				for _, item := range delta {
					key := globalInboundKey(item.ChatID, item.MessageID)
					if err := insertGlobalInboundSQLiteTxIfMissing(ctx, tx, key, item); err != nil {
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
		legacyItems, err := readGlobalOutboundForUnion(ctx, legacyOutboundPath)
		if err != nil {
			return fmt.Errorf("read legacy global outbound ledger: %w", err)
		}
		canonicalItems, err := readGlobalOutboundForUnion(ctx, canonicalOutboundPath)
		if err != nil {
			return fmt.Errorf("read canonical global outbound ledger: %w", err)
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
