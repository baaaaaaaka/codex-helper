package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/gofrs/flock"
)

const (
	envTeamsMachineID       = "CODEX_HELPER_TEAMS_MACHINE_ID"
	envTeamsMachineLabel    = "CODEX_HELPER_TEAMS_MACHINE_LABEL"
	envTeamsMachineKind     = "CODEX_HELPER_TEAMS_MACHINE_KIND"
	envTeamsMachinePriority = "CODEX_HELPER_TEAMS_MACHINE_PRIORITY"
	envTeamsProfile         = "CODEX_HELPER_TEAMS_PROFILE"

	// Scope resolution runs outside the listener and therefore cannot reuse the
	// listener's configured stale-after value. Five minutes matches the CLI's
	// conservative owner-recovery default while still rejecting retained owner
	// records from an old store.
	scopeStoreOwnerFreshAfter = 5 * time.Minute
	scopeStoreFreshOwnerScore = 1000000
	scopeStoreFreshLeaseScore = 500000
)

var (
	scopeMigrationLegacyCleanupGrace = 7 * 24 * time.Hour
	scopeMigrationCleanupNow         = time.Now
	scopeMigrationCleanupRemove      = os.Remove
)

func ScopeIdentityForUser(user User) teamstore.ScopeIdentity {
	scope := teamstore.ScopeIdentity{
		AccountID:     strings.TrimSpace(user.ID),
		UserPrincipal: strings.TrimSpace(user.UserPrincipalName),
		OSUser:        localOSUser(),
		Profile:       strings.TrimSpace(os.Getenv(envTeamsProfile)),
		ConfigPath:    strings.TrimSpace(os.Getenv("CODEX_HELPER_CONFIG")),
		CodexHome:     strings.TrimSpace(firstNonEmptyString(os.Getenv("CODEX_HOME"), os.Getenv("CODEX_DIR"))),
	}
	if scope.Profile == "" {
		scope.Profile = "default"
	}
	scope.ID = stableID("scope", []string{
		"v2",
		scope.AccountID,
		scope.UserPrincipal,
		scope.Profile,
	})
	return scope
}

func ResolveStorePathForScope(scope teamstore.ScopeIdentity) (teamstore.ScopeIdentity, string, error) {
	scope = normalizeScopeForResolution(scope)
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return scope, "", err
	}
	resolved, path, ok, err := resolveExistingScopeStore(scope, currentPath)
	if err != nil {
		return scope, "", err
	}
	if ok {
		scope.ID = resolved.ID
		if !resolved.CreatedAt.IsZero() {
			scope.CreatedAt = resolved.CreatedAt
		}
		if migratedPath, err := migrateResolvedScopeStore(resolved, path); err == nil && strings.TrimSpace(migratedPath) != "" {
			path = migratedPath
		}
		return scope, path, nil
	}
	return scope, currentPath, nil
}

// ResolveStorePathForMaintenance selects the authoritative existing store
// without migrating it. Chat recreation uses this while a listener is still
// alive so owner metadata continues to identify the exact store that must be
// mutated after the listener drains.
func ResolveStorePathForMaintenance(scope teamstore.ScopeIdentity) (teamstore.ScopeIdentity, string, error) {
	scope = normalizeScopeForResolution(scope)
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return scope, "", err
	}
	resolved, path, ok, err := resolveExistingScopeStoreForMaintenance(scope, currentPath)
	if err != nil {
		return scope, "", err
	}
	if !ok {
		return scope, currentPath, nil
	}
	if strings.TrimSpace(resolved.ID) != "" {
		scope.ID = resolved.ID
	}
	if !resolved.CreatedAt.IsZero() {
		scope.CreatedAt = resolved.CreatedAt
	}
	return scope, path, nil
}

func ResolveRegistryPathForScope(scope teamstore.ScopeIdentity) (teamstore.ScopeIdentity, string, error) {
	resolved, storePath, err := ResolveStorePathForScope(scope)
	if err != nil {
		return scope, "", err
	}
	path, err := registryPathForResolvedScopeStore(resolved.ID, storePath)
	if err != nil {
		return resolved, "", err
	}
	return resolved, path, nil
}

func DefaultStorePathForScope(scopeID string) (string, error) {
	return appdirs.StatePath("teams", "scopes", safeScopePathPart(scopeID), "state.json")
}

func DefaultRegistryPathForScope(scopeID string) (string, error) {
	path, err := appdirs.StatePath("teams", "scopes", safeScopePathPart(scopeID), "registry.json")
	if err != nil {
		return "", err
	}
	legacyPath, legacyErr := legacyDefaultRegistryPathForScope(scopeID)
	if legacyErr != nil {
		return path, nil
	}
	resolved, err := appdirs.ResolveMigratedFile(path, legacyPath)
	if err != nil {
		return "", err
	}
	if sameRegistryPath(resolved, path) && !sameRegistryPath(path, legacyPath) && !registryFileValid(path) && registryFileValid(legacyPath) {
		if err := appdirs.CopyFileReplacing(path, legacyPath); err != nil {
			return legacyPath, nil
		}
	}
	return resolved, nil
}

type resolvedScopeStoreCandidate struct {
	scope                teamstore.ScopeIdentity
	path                 string
	score                int
	updated              time.Time
	freshAuthority       bool
	authorityUpdated     time.Time
	maintenanceAuthority bool
}

func resolveExistingScopeStore(scope teamstore.ScopeIdentity, currentPath string) (teamstore.ScopeIdentity, string, bool, error) {
	return resolveExistingScopeStoreWithOptions(scope, currentPath, false)
}

func resolveExistingScopeStoreForMaintenance(scope teamstore.ScopeIdentity, currentPath string) (teamstore.ScopeIdentity, string, bool, error) {
	return resolveExistingScopeStoreWithOptions(scope, currentPath, true)
}

func resolveExistingScopeStoreWithOptions(scope teamstore.ScopeIdentity, currentPath string, preferFreshAuthority bool) (teamstore.ScopeIdentity, string, bool, error) {
	paths, err := candidateScopeStorePaths(currentPath)
	if err != nil {
		return teamstore.ScopeIdentity{}, "", false, err
	}
	now := time.Now()
	var candidates []resolvedScopeStoreCandidate
	var firstLoadErr error
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return teamstore.ScopeIdentity{}, "", false, err
			}
		}
		st, err := teamstore.Open(path)
		if err != nil {
			if firstLoadErr == nil {
				firstLoadErr = err
			}
			continue
		}
		loadCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		state, err := st.Load(loadCtx)
		cancel()
		closeErr := st.Close()
		if err != nil {
			if firstLoadErr == nil {
				firstLoadErr = err
			}
			continue
		}
		if closeErr != nil {
			if firstLoadErr == nil {
				firstLoadErr = closeErr
			}
			continue
		}
		matched := scopeStateMatches(scope, state)
		if !matched && defaultGlobalStoreCanSeedScope(scope, path, state) {
			matched = true
		}
		if !matched {
			continue
		}
		candidateScope := state.Scope
		if strings.TrimSpace(candidateScope.ID) == "" {
			candidateScope = scope
		}
		score := scopeStoreResolutionScore(state, path, currentPath, now)
		freshAuthority := scopeStoreHasFreshOwner(state, now) || scopeStoreHasFreshControlLease(state, now)
		if preferFreshAuthority {
			score += scopeStoreFreshAuthorityScore(state, now)
		}
		if isDefaultGlobalStorePath(path) {
			score -= 10000
		}
		candidate := resolvedScopeStoreCandidate{
			scope:            candidateScope,
			path:             path,
			score:            score,
			updated:          candidateScope.UpdatedAt,
			freshAuthority:   freshAuthority,
			authorityUpdated: state.ServiceControl.LastDrainOperationAt,
		}
		if state.ServiceControl.LastDrainOperationAt.After(candidate.updated) {
			candidate.updated = state.ServiceControl.LastDrainOperationAt
		}
		if info, err := os.Stat(path); err == nil && info.ModTime().After(candidate.updated) {
			candidate.updated = info.ModTime()
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		if firstLoadErr != nil {
			return teamstore.ScopeIdentity{}, "", false, firstLoadErr
		}
		return teamstore.ScopeIdentity{}, "", false, nil
	}
	if preferFreshAuthority {
		var freshPaths []string
		for _, candidate := range candidates {
			if candidate.freshAuthority {
				freshPaths = append(freshPaths, candidate.path)
			}
		}
		if len(freshPaths) > 1 {
			sort.Strings(freshPaths)
			return teamstore.ScopeIdentity{}, "", false, fmt.Errorf("multiple live Teams stores claim scope %q: %s", scope.ID, strings.Join(freshPaths, ", "))
		}
	}
	for i := range candidates {
		marker := candidates[i].authorityUpdated
		if marker.IsZero() {
			continue
		}
		newerCandidateExists := false
		for j := range candidates {
			if i != j && candidates[j].updated.After(marker) {
				newerCandidateExists = true
				break
			}
		}
		candidates[i].maintenanceAuthority = !newerCandidateExists
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].freshAuthority != candidates[j].freshAuthority {
			return candidates[i].freshAuthority
		}
		if candidates[i].maintenanceAuthority != candidates[j].maintenanceAuthority {
			return candidates[i].maintenanceAuthority
		}
		if candidates[i].maintenanceAuthority && !candidates[i].authorityUpdated.Equal(candidates[j].authorityUpdated) {
			return candidates[i].authorityUpdated.After(candidates[j].authorityUpdated)
		}
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		if !candidates[i].updated.Equal(candidates[j].updated) {
			return candidates[i].updated.After(candidates[j].updated)
		}
		return candidates[i].path < candidates[j].path
	})
	best := candidates[0]
	return best.scope, best.path, true, nil
}

func candidateScopeStorePaths(currentPath string) ([]string, error) {
	stateGlob, err := appdirs.StatePath("teams", "scopes", "*", "state.json")
	if err != nil {
		return nil, err
	}
	stateMatches, err := filepath.Glob(stateGlob)
	if err != nil {
		return nil, err
	}
	var legacyMatches []string
	if legacyGlob, legacyErr := appdirs.LegacyConfigPath("teams", "scopes", "*", "state.json"); legacyErr == nil {
		legacyMatches, err = filepath.Glob(legacyGlob)
		if err != nil {
			return nil, err
		}
	}
	seen := map[string]bool{}
	var paths []string
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" || seen[path] {
			return
		}
		seen[path] = true
		paths = append(paths, path)
	}
	add(currentPath)
	sort.Strings(stateMatches)
	for _, path := range stateMatches {
		add(path)
	}
	sort.Strings(legacyMatches)
	for _, path := range legacyMatches {
		add(path)
	}
	if stateGlobalPath, stateGlobalErr := appdirs.StatePath("teams", "state.json"); stateGlobalErr == nil {
		add(stateGlobalPath)
	}
	if legacyGlobalPath, legacyGlobalErr := appdirs.LegacyConfigPath("teams", "state.json"); legacyGlobalErr == nil {
		add(legacyGlobalPath)
	}
	return paths, nil
}

func legacyDefaultStorePathForScope(scopeID string) (string, error) {
	return appdirs.LegacyConfigPath("teams", "scopes", safeScopePathPart(scopeID), "state.json")
}

func legacyDefaultRegistryPathForScope(scopeID string) (string, error) {
	return appdirs.LegacyCachePath("teams", "scopes", safeScopePathPart(scopeID), "registry.json")
}

func registryPathForResolvedScopeStore(scopeID string, storePath string) (string, error) {
	defaultStorePath, defaultStoreErr := DefaultStorePathForScope(scopeID)
	if defaultStoreErr != nil {
		return "", defaultStoreErr
	}
	if samePath(storePath, defaultStorePath) {
		return DefaultRegistryPathForScope(scopeID)
	}
	legacyStorePath, legacyStoreErr := legacyDefaultStorePathForScope(scopeID)
	if legacyStoreErr == nil && samePath(storePath, legacyStorePath) {
		return legacyDefaultRegistryPathForScope(scopeID)
	}
	if isDefaultGlobalStorePath(storePath) {
		if registryPath, ok := registryPathForStoreMigrationSource(storePath); ok {
			return registryPath, nil
		}
	}
	if legacyRegistryPath, ok := legacyRegistryPathForScopeStore(storePath); ok {
		return legacyRegistryPath, nil
	}
	return DefaultRegistryPathForScope(scopeID)
}

func legacyRegistryPathForScopeStore(storePath string) (string, bool) {
	storePath = strings.TrimSpace(storePath)
	if storePath == "" {
		return "", false
	}
	clean := filepath.Clean(storePath)
	if filepath.Base(clean) != "state.json" {
		return "", false
	}
	scopeDir := filepath.Dir(clean)
	if filepath.Base(filepath.Dir(scopeDir)) != "scopes" {
		return "", false
	}
	legacyBase, err := appdirs.LegacyCachePath("teams", "scopes")
	if err != nil {
		return "", false
	}
	return filepath.Join(legacyBase, filepath.Base(scopeDir), "registry.json"), true
}

func migrateResolvedScopeStore(scope teamstore.ScopeIdentity, storePath string) (string, error) {
	scope = normalizeScopeForResolution(scope)
	newStorePath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return "", err
	}
	if samePath(storePath, newStorePath) {
		if source, ok := incompleteMigrationSourceForScope(scope, newStorePath); ok {
			return migrateResolvedScopeStore(scope, source)
		}
		return newStorePath, nil
	}
	if isDefaultGlobalStorePath(storePath) {
		return migrateResolvedGlobalStore(scope.ID, newStorePath, storePath)
	}
	if scopeMigrationComplete(scope.ID, newStorePath, storePath) {
		cleanupMigratedScopeLegacyFiles(scope.ID, newStorePath, storePath, false)
		return newStorePath, nil
	}
	if ok, err := pathExists(newStorePath); err != nil {
		return "", err
	} else if ok {
		// A partial new store exists. Keep trying to complete the rest of the
		// unit from the locked legacy store instead of accepting the fragment.
	}
	lock := flock.New(storePath + ".lock")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil {
		return storePath, nil
	}
	if !locked {
		return storePath, nil
	}
	defer func() { _ = lock.Unlock() }()

	newScopeDir := filepath.Dir(newStorePath)
	oldScopeDir := filepath.Dir(storePath)
	if err := migrateStoreDirRelatedFiles(newScopeDir, oldScopeDir); err != nil {
		return storePath, nil
	}

	oldRegistryPath, hasOldRegistryPath := registryPathForStoreMigrationSource(storePath)
	newRegistryPath, err := appdirs.StatePath("teams", "scopes", safeScopePathPart(scope.ID), "registry.json")
	if err != nil {
		return storePath, nil
	}
	if hasOldRegistryPath {
		if err := copyLockedFileFamilyIfPresent(newRegistryPath, oldRegistryPath, oldRegistryPath+".lock"); err != nil {
			return storePath, nil
		}
		if err := migrateScopeLedgerFiles(newRegistryPath, oldRegistryPath); err != nil {
			return storePath, nil
		}
	}
	if err := appdirs.CopyFileReplacing(newStorePath, storePath); err != nil {
		return storePath, nil
	}
	if !scopeMigrationComplete(scope.ID, newStorePath, storePath) {
		return storePath, nil
	}
	cleanupMigratedScopeLegacyFiles(scope.ID, newStorePath, storePath, true)
	return newStorePath, nil
}

func migrateResolvedGlobalStore(scopeID string, newStorePath string, storePath string) (string, error) {
	if scopeMigrationComplete(scopeID, newStorePath, storePath) {
		cleanupMigratedScopeLegacyFiles(scopeID, newStorePath, storePath, false)
		return newStorePath, nil
	}
	lock := flock.New(storePath + ".lock")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil {
		return storePath, nil
	}
	if !locked {
		return storePath, nil
	}
	defer func() { _ = lock.Unlock() }()

	newScopeDir := filepath.Dir(newStorePath)
	oldStoreDir := filepath.Dir(storePath)
	if err := migrateStoreDirRelatedFiles(newScopeDir, oldStoreDir); err != nil {
		return storePath, nil
	}

	oldRegistryPath, hasOldRegistryPath := registryPathForStoreMigrationSource(storePath)
	newRegistryPath, err := appdirs.StatePath("teams", "scopes", safeScopePathPart(scopeID), "registry.json")
	if err != nil {
		return storePath, nil
	}
	if hasOldRegistryPath {
		if err := copyLockedFileFamilyIfPresent(newRegistryPath, oldRegistryPath, oldRegistryPath+".lock"); err != nil {
			return storePath, nil
		}
		if err := migrateScopeLedgerFiles(newRegistryPath, oldRegistryPath); err != nil {
			return storePath, nil
		}
	}
	if err := appdirs.CopyFileReplacing(newStorePath, storePath); err != nil {
		return storePath, nil
	}
	if !scopeMigrationComplete(scopeID, newStorePath, storePath) {
		return storePath, nil
	}
	cleanupMigratedScopeLegacyFiles(scopeID, newStorePath, storePath, true)
	return newStorePath, nil
}

func incompleteMigrationSourceForScope(scope teamstore.ScopeIdentity, newStorePath string) (string, bool) {
	scope = normalizeScopeForResolution(scope)
	var sources []string
	if legacyPath, err := legacyDefaultStorePathForScope(scope.ID); err == nil {
		sources = append(sources, legacyPath)
	}
	if stateGlobalPath, err := appdirs.StatePath("teams", "state.json"); err == nil {
		sources = append(sources, stateGlobalPath)
	}
	if legacyGlobalPath, err := appdirs.LegacyConfigPath("teams", "state.json"); err == nil {
		sources = append(sources, legacyGlobalPath)
	}
	for _, source := range sources {
		if samePath(source, newStorePath) {
			continue
		}
		if ok, err := pathExists(source); err != nil || !ok {
			continue
		}
		if isDefaultGlobalStorePath(source) {
			st, err := teamstore.Open(source)
			if err != nil {
				continue
			}
			loadCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			state, loadErr := st.Load(loadCtx)
			cancel()
			closeErr := st.Close()
			if loadErr != nil || closeErr != nil || !defaultGlobalStoreCanSeedScope(scope, source, state) {
				continue
			}
		} else if !storeFileLoadable(source) {
			continue
		}
		if !scopeMigrationComplete(scope.ID, newStorePath, source) {
			return source, true
		}
	}
	return "", false
}

func migrateStoreDirRelatedFiles(newStoreDir string, oldStoreDir string) error {
	newSQLitePath := filepath.Join(newStoreDir, teamstore.SQLiteFileName)
	if resolved, err := appdirs.ResolveMigratedRelatedFiles(
		newSQLitePath,
		filepath.Join(oldStoreDir, teamstore.SQLiteFileName),
		"-wal",
		"-shm",
	); err != nil || resolved != newSQLitePath {
		return fmt.Errorf("migrate store sqlite family from %s", oldStoreDir)
	}
	for _, name := range storeSidecarNames() {
		oldSidecar := filepath.Join(oldStoreDir, name)
		if ok, err := pathExists(oldSidecar); err != nil || !ok {
			continue
		}
		if err := appdirs.CopyFileIfMissing(filepath.Join(newStoreDir, name), oldSidecar); err != nil {
			return err
		}
	}
	return nil
}

type migratedCleanupFamily struct {
	newBase  string
	oldBase  string
	suffixes []string
}

type migratedCleanupPair struct {
	newPath string
	oldPath string
}

func cleanupMigratedScopeLegacyFiles(scopeID string, newStorePath string, oldStorePath string, storeLockHeld bool) {
	if scopeMigrationLegacyCleanupGrace < 0 {
		return
	}
	if !scopeMigrationComplete(scopeID, newStorePath, oldStorePath) {
		return
	}

	oldRegistryPath, hasOldRegistryPath := registryPathForStoreMigrationSource(oldStorePath)
	newRegistryPath := ""
	if hasOldRegistryPath {
		if path, err := appdirs.StatePath("teams", "scopes", safeScopePathPart(scopeID), "registry.json"); err == nil {
			newRegistryPath = path
		}
	}

	newStoreDir := filepath.Dir(newStorePath)
	oldStoreDir := filepath.Dir(oldStorePath)
	storeFamilies := []migratedCleanupFamily{
		{
			newBase:  filepath.Join(newStoreDir, teamstore.SQLiteFileName),
			oldBase:  filepath.Join(oldStoreDir, teamstore.SQLiteFileName),
			suffixes: []string{"-wal", "-shm"},
		},
	}
	for _, name := range storeSidecarNames() {
		storeFamilies = append(storeFamilies, migratedCleanupFamily{
			newBase: filepath.Join(newStoreDir, name),
			oldBase: filepath.Join(oldStoreDir, name),
		})
	}
	storeFamilies = append(storeFamilies, migratedCleanupFamily{
		newBase: newStorePath,
		oldBase: oldStorePath,
	})
	registryCleanupOK := true
	if hasOldRegistryPath {
		if newRegistryPath == "" || !registryMigrationComplete(newRegistryPath, oldRegistryPath) {
			return
		}
		if oldInboundPath, oldOK := globalInboundLedgerPathForRegistry(oldRegistryPath); oldOK {
			if newInboundPath, newOK := globalInboundLedgerPathForRegistry(newRegistryPath); newOK {
				registryCleanupOK = cleanupMigratedLedgerLegacyFilesIfSafe(newInboundPath, oldInboundPath) && registryCleanupOK
			}
		}
		if oldOutboundPath, oldOK := globalOutboundLedgerPathForRegistry(oldRegistryPath); oldOK {
			if newOutboundPath, newOK := globalOutboundLedgerPathForRegistry(newRegistryPath); newOK {
				registryCleanupOK = cleanupMigratedLedgerLegacyFilesIfSafe(newOutboundPath, oldOutboundPath) && registryCleanupOK
			}
		}
		if legacyRelatedFileFamilyExists(oldRegistryPath) {
			if !registryCleanupOK {
				return
			}
			registryCleanupOK = cleanupMigratedFileFamiliesIfSafe(oldRegistryPath+".lock", migratedCleanupFamily{
				newBase: newRegistryPath,
				oldBase: oldRegistryPath,
			})
		}
	}
	if !registryCleanupOK {
		return
	}
	if storeLockHeld {
		cleanupMigratedFileFamiliesIfSafe("", storeFamilies...)
	} else {
		cleanupMigratedFileFamiliesIfSafe(oldStorePath+".lock", storeFamilies...)
	}
}

func cleanupMigratedLedgerLegacyFilesIfSafe(newJSONPath string, oldJSONPath string) bool {
	if !legacyRelatedFileFamilyExists(oldJSONPath) && !legacyRelatedFileFamilyExists(teamsLedgerSQLitePath(oldJSONPath), "-wal", "-shm") {
		return true
	}
	return cleanupMigratedFileFamiliesIfSafe(oldJSONPath+".lock",
		migratedCleanupFamily{
			newBase:  teamsLedgerSQLitePath(newJSONPath),
			oldBase:  teamsLedgerSQLitePath(oldJSONPath),
			suffixes: []string{"-wal", "-shm"},
		},
		migratedCleanupFamily{
			newBase: newJSONPath,
			oldBase: oldJSONPath,
		},
	)
}

func cleanupMigratedFileFamiliesIfSafe(lockPath string, families ...migratedCleanupFamily) bool {
	lockPath = strings.TrimSpace(lockPath)
	if lockPath != "" {
		lock := flock.New(lockPath)
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
		if err != nil || !locked {
			return false
		}
		defer func() { _ = lock.Unlock() }()
	}
	pairs, ok := migratedCleanupPairsIfSafe(families...)
	if !ok {
		return false
	}
	return removeMigratedLegacyPairs(pairs)
}

func migratedCleanupPairsIfSafe(families ...migratedCleanupFamily) ([]migratedCleanupPair, bool) {
	var pairs []migratedCleanupPair
	now := scopeMigrationCleanupNow()
	for _, family := range families {
		if strings.TrimSpace(family.newBase) == "" || strings.TrimSpace(family.oldBase) == "" || samePath(family.newBase, family.oldBase) {
			return nil, false
		}
		for _, suffix := range cleanupSuffixesWithBaseLast(family.suffixes...) {
			oldPath := family.oldBase + suffix
			oldInfo, oldExists, oldSafe, oldErr := cleanupRegularFileInfo(oldPath)
			if oldErr != nil || !oldSafe {
				return nil, false
			}
			if !oldExists {
				continue
			}
			newPath := family.newBase + suffix
			newInfo, newExists, newSafe, newErr := cleanupRegularFileInfo(newPath)
			if newErr != nil || !newSafe || !newExists {
				return nil, false
			}
			if os.SameFile(newInfo, oldInfo) {
				return nil, false
			}
			if oldInfo.ModTime().After(newInfo.ModTime()) {
				return nil, false
			}
			if scopeMigrationLegacyCleanupGrace > 0 && now.Sub(oldInfo.ModTime()) < scopeMigrationLegacyCleanupGrace {
				return nil, false
			}
			sameContent, err := regularFileContentsEqual(newPath, oldPath, newInfo, oldInfo)
			if err != nil || !sameContent {
				return nil, false
			}
			pairs = append(pairs, migratedCleanupPair{newPath: newPath, oldPath: oldPath})
		}
	}
	return pairs, true
}

func cleanupRegularFileInfo(path string) (os.FileInfo, bool, bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, true, nil
		}
		return nil, false, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return info, true, false, nil
	}
	return info, true, true, nil
}

func regularFileContentsEqual(a string, b string, aInfo os.FileInfo, bInfo os.FileInfo) (bool, error) {
	if aInfo.Size() != bInfo.Size() {
		return false, nil
	}
	aHash, err := fileSHA256(a)
	if err != nil {
		return false, err
	}
	bHash, err := fileSHA256(b)
	if err != nil {
		return false, err
	}
	return aHash == bHash, nil
}

func fileSHA256(path string) ([32]byte, error) {
	var out [32]byte
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return out, err
	}
	copy(out[:], h.Sum(nil))
	return out, nil
}

func cleanupSuffixesWithBaseLast(suffixes ...string) []string {
	out := make([]string, 0, len(suffixes)+1)
	for _, suffix := range suffixes {
		if suffix == "" {
			continue
		}
		out = append(out, suffix)
	}
	out = append(out, "")
	return out
}

func removeMigratedLegacyPairs(pairs []migratedCleanupPair) bool {
	dirs := map[string]struct{}{}
	ok := true
	for _, pair := range pairs {
		if err := scopeMigrationCleanupRemove(pair.oldPath); err != nil && !os.IsNotExist(err) {
			ok = false
			continue
		}
		dirs[filepath.Dir(pair.oldPath)] = struct{}{}
	}
	removeEmptyLegacyDirs(dirs)
	return ok
}

func removeEmptyLegacyDirs(dirs map[string]struct{}) {
	ordered := make([]string, 0, len(dirs))
	for dir := range dirs {
		if strings.TrimSpace(dir) != "" {
			ordered = append(ordered, dir)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if len(ordered[i]) != len(ordered[j]) {
			return len(ordered[i]) > len(ordered[j])
		}
		return ordered[i] > ordered[j]
	})
	for _, dir := range ordered {
		_ = scopeMigrationCleanupRemove(dir)
	}
}

func scopeMigrationComplete(scopeID string, newStorePath string, oldStorePath string) bool {
	if ok, err := pathExists(newStorePath); err != nil || !ok {
		return false
	}
	if !storeFileLoadable(newStorePath) {
		return false
	}
	if storeFileNeedsRefresh(newStorePath, oldStorePath) {
		return false
	}
	newStoreDir := filepath.Dir(newStorePath)
	oldStoreDir := filepath.Dir(oldStorePath)
	if !relatedFileFamilyCompleteForExistingLegacy(filepath.Join(newStoreDir, teamstore.SQLiteFileName), filepath.Join(oldStoreDir, teamstore.SQLiteFileName), "-wal", "-shm") {
		return false
	}
	for _, name := range storeSidecarNames() {
		oldSidecar := filepath.Join(oldStoreDir, name)
		if ok, err := pathExists(oldSidecar); err != nil {
			return false
		} else if !ok {
			continue
		}
		if ok, err := pathExists(filepath.Join(newStoreDir, name)); err != nil || !ok {
			return false
		}
	}
	oldRegistryPath, hasOldRegistryPath := registryPathForStoreMigrationSource(oldStorePath)
	if !hasOldRegistryPath {
		return true
	}
	newRegistryPath, err := appdirs.StatePath("teams", "scopes", safeScopePathPart(scopeID), "registry.json")
	if err != nil {
		return false
	}
	if !registryMigrationComplete(newRegistryPath, oldRegistryPath) {
		return false
	}
	return true
}

func storeFileNeedsRefresh(newStorePath string, oldStorePath string) bool {
	oldInfo, oldExists, oldSafe, oldErr := cleanupRegularFileInfo(oldStorePath)
	if oldErr != nil || !oldExists || !oldSafe {
		return false
	}
	newInfo, newExists, newSafe, newErr := cleanupRegularFileInfo(newStorePath)
	if newErr != nil || !newSafe || !newExists {
		return true
	}
	if os.SameFile(newInfo, oldInfo) {
		return false
	}
	if oldInfo.ModTime().After(newInfo.ModTime()) {
		return true
	}
	if oldInfo.ModTime().Equal(newInfo.ModTime()) {
		sameContent, err := regularFileContentsEqual(newStorePath, oldStorePath, newInfo, oldInfo)
		return err != nil || !sameContent
	}
	return false
}

func storeFileLoadable(path string) bool {
	st, err := teamstore.Open(path)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	_, loadErr := st.Load(ctx)
	cancel()
	closeErr := st.Close()
	return loadErr == nil && closeErr == nil
}

func StoreMigrationCompleteForPath(scopeID string, newStorePath string, oldStorePath string) bool {
	return scopeMigrationComplete(scopeID, newStorePath, oldStorePath)
}

func RegistryMigrationCompleteForPath(newRegistryPath string, oldRegistryPath string) bool {
	return registryMigrationComplete(newRegistryPath, oldRegistryPath)
}

func registryMigrationComplete(newRegistryPath string, oldRegistryPath string) bool {
	if !relatedFileFamilyCompleteForExistingLegacy(newRegistryPath, oldRegistryPath) {
		return false
	}
	oldInboundPath, oldInboundOK := globalInboundLedgerPathForRegistry(oldRegistryPath)
	newInboundPath, newInboundOK := globalInboundLedgerPathForRegistry(newRegistryPath)
	if oldInboundOK && newInboundOK && !ledgerFileFamilyCompleteForExistingLegacy(newInboundPath, oldInboundPath) {
		return false
	}
	oldOutboundPath, oldOutboundOK := globalOutboundLedgerPathForRegistry(oldRegistryPath)
	newOutboundPath, newOutboundOK := globalOutboundLedgerPathForRegistry(newRegistryPath)
	if oldOutboundOK && newOutboundOK && !ledgerFileFamilyCompleteForExistingLegacy(newOutboundPath, oldOutboundPath) {
		return false
	}
	return true
}

func storeSidecarNames() []string {
	return []string{
		"helper-restart-pending.json",
		"workflow-notifications.json",
		"workflow-webhook-url",
	}
}

func registryPathForStoreMigrationSource(storePath string) (string, bool) {
	if registryPath, ok := legacyRegistryPathForScopeStore(storePath); ok {
		return registryPath, true
	}
	if !isDefaultGlobalStorePath(storePath) {
		return "", false
	}
	if path, ok := legacyGlobalRegistryPathForMigrationSource(); ok {
		return path, true
	}
	path, err := appdirs.StatePath("teams", "registry.json")
	if err != nil {
		return "", false
	}
	return path, true
}

func legacyGlobalRegistryPathForMigrationSource() (string, bool) {
	legacyPath, err := appdirs.LegacyCachePath("teams-registry.json")
	if err != nil {
		return "", false
	}
	if legacyRelatedFileFamilyExists(legacyPath) || registryLedgerFamilyExists(legacyPath) {
		return legacyPath, true
	}
	return "", false
}

func registryLedgerFamilyExists(registryPath string) bool {
	if inboundPath, ok := globalInboundLedgerPathForRegistry(registryPath); ok {
		if legacyRelatedFileFamilyExists(inboundPath) || legacyRelatedFileFamilyExists(teamsLedgerSQLitePath(inboundPath), "-wal", "-shm") {
			return true
		}
	}
	if outboundPath, ok := globalOutboundLedgerPathForRegistry(registryPath); ok {
		if legacyRelatedFileFamilyExists(outboundPath) || legacyRelatedFileFamilyExists(teamsLedgerSQLitePath(outboundPath), "-wal", "-shm") {
			return true
		}
	}
	return false
}

func migrateScopeLedgerFiles(newRegistryPath string, oldRegistryPath string) error {
	oldInboundPath, oldInboundOK := globalInboundLedgerPathForRegistry(oldRegistryPath)
	newInboundPath, newInboundOK := globalInboundLedgerPathForRegistry(newRegistryPath)
	if oldInboundOK && newInboundOK {
		if err := copyLockedLedgerFileFamilyIfPresent(newInboundPath, oldInboundPath); err != nil {
			return err
		}
	}
	oldOutboundPath, oldOutboundOK := globalOutboundLedgerPathForRegistry(oldRegistryPath)
	newOutboundPath, newOutboundOK := globalOutboundLedgerPathForRegistry(newRegistryPath)
	if oldOutboundOK && newOutboundOK {
		if err := copyLockedLedgerFileFamilyIfPresent(newOutboundPath, oldOutboundPath); err != nil {
			return err
		}
	}
	return nil
}

func copyLockedLedgerFileFamilyIfPresent(newJSONPath string, oldJSONPath string) error {
	if !legacyRelatedFileFamilyExists(oldJSONPath) && !legacyRelatedFileFamilyExists(teamsLedgerSQLitePath(oldJSONPath), "-wal", "-shm") {
		return nil
	}
	lock := flock.New(oldJSONPath + ".lock")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("migration lock was not acquired for %s", oldJSONPath)
	}
	defer func() { _ = lock.Unlock() }()
	if err := copyRelatedFileFamilyIfPresent(newJSONPath, oldJSONPath); err != nil {
		return err
	}
	return copyRelatedFileFamilyIfPresent(teamsLedgerSQLitePath(newJSONPath), teamsLedgerSQLitePath(oldJSONPath), "-wal", "-shm")
}

func copyLockedFileFamilyIfPresent(newBase string, oldBase string, lockPath string, suffixes ...string) error {
	if !legacyRelatedFileFamilyExists(oldBase, suffixes...) {
		return nil
	}
	lock := flock.New(lockPath)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("migration lock was not acquired for %s", oldBase)
	}
	defer func() { _ = lock.Unlock() }()
	return copyRelatedFileFamilyIfPresent(newBase, oldBase, suffixes...)
}

func copyFileFamilyIfPresent(newPath string, oldPath string) error {
	return copyRelatedFileFamilyIfPresent(newPath, oldPath)
}

func copyRelatedFileFamilyIfPresent(newBase string, oldBase string, suffixes ...string) error {
	resolved, err := appdirs.ResolveMigratedRelatedFiles(newBase, oldBase, suffixes...)
	if err != nil {
		return err
	}
	if resolved != newBase {
		return fmt.Errorf("migration fell back to legacy file family %s", oldBase)
	}
	return nil
}

func relatedFileFamilyCompleteForExistingLegacy(newBase string, oldBase string, suffixes ...string) bool {
	for _, suffix := range append([]string{""}, suffixes...) {
		oldExists, oldErr := pathExists(oldBase + suffix)
		if oldErr != nil {
			return false
		}
		if !oldExists {
			continue
		}
		newExists, newErr := regularFileExists(newBase + suffix)
		if newErr != nil || !newExists {
			return false
		}
	}
	return true
}

func ledgerFileFamilyCompleteForExistingLegacy(newJSONPath string, oldJSONPath string) bool {
	if !relatedFileFamilyCompleteForExistingLegacy(newJSONPath, oldJSONPath) {
		return false
	}
	return relatedFileFamilyCompleteForExistingLegacy(teamsLedgerSQLitePath(newJSONPath), teamsLedgerSQLitePath(oldJSONPath), "-wal", "-shm")
}

func legacyRelatedFileFamilyExists(base string, suffixes ...string) bool {
	for _, suffix := range append([]string{""}, suffixes...) {
		ok, err := pathExists(base + suffix)
		if err == nil && ok {
			return true
		}
	}
	return false
}

func isDefaultGlobalStorePath(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	if statePath, err := appdirs.StatePath("teams", "state.json"); err == nil && samePath(path, statePath) {
		return true
	}
	if legacyPath, err := appdirs.LegacyConfigPath("teams", "state.json"); err == nil && samePath(path, legacyPath) {
		return true
	}
	return false
}

func defaultGlobalStoreCanSeedScope(scope teamstore.ScopeIdentity, path string, state teamstore.State) bool {
	if !isDefaultGlobalStorePath(path) {
		return false
	}
	if strings.TrimSpace(state.Scope.ID) != "" {
		return false
	}
	if strings.TrimSpace(state.ControlChat.ScopeID) != "" || strings.TrimSpace(state.ControlChat.AccountID) != "" || strings.TrimSpace(state.ControlChat.Profile) != "" {
		return false
	}
	registryPath, ok := legacyGlobalRegistryPathForMigrationSource()
	if !ok {
		if path, err := appdirs.StatePath("teams", "registry.json"); err == nil {
			registryPath = path
		}
	}
	if strings.TrimSpace(registryPath) == "" {
		return true
	}
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		return false
	}
	if strings.TrimSpace(reg.UserID) != "" && strings.TrimSpace(scope.AccountID) != "" && strings.TrimSpace(reg.UserID) != strings.TrimSpace(scope.AccountID) {
		return false
	}
	if strings.TrimSpace(reg.UserPrincipal) != "" && strings.TrimSpace(scope.UserPrincipal) != "" && !strings.EqualFold(strings.TrimSpace(reg.UserPrincipal), strings.TrimSpace(scope.UserPrincipal)) {
		return false
	}
	return true
}

func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func regularFileExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, nil
	}
	return true, nil
}

func samePath(a string, b string) bool {
	if strings.TrimSpace(a) == "" || strings.TrimSpace(b) == "" {
		return false
	}
	return filepath.Clean(a) == filepath.Clean(b)
}

func scopeStoreResolutionScore(state teamstore.State, path string, currentPath string, now time.Time) int {
	score := 0
	if path == currentPath {
		score += 10
	}
	if teamstore.HasUpgradeBlockingWork(state, now) {
		score += 1000
	}
	if scopeStoreHasFreshOwner(state, now) {
		score += scopeStoreFreshOwnerScore
	} else if state.ServiceOwner != nil || state.LockOwner != nil {
		score += 900
	}
	if scopeStoreHasFreshControlLease(state, now) {
		score += scopeStoreFreshLeaseScore
	} else if state.ControlLease.HolderMachineID != "" {
		score += 800
	}
	if strings.TrimSpace(state.ControlChat.TeamsChatID) != "" {
		score += 500
	}
	score += len(state.Sessions) * 10
	score += len(state.OutboxMessages)
	return score
}

func scopeStoreFreshAuthorityScore(state teamstore.State, now time.Time) int {
	// Maintenance resolution gives live evidence extra weight in addition to
	// the normal resolver score. The explicit fresh-authority sort remains the
	// final guard against a retained store with unusually many stale rows.
	score := 0
	if scopeStoreHasFreshOwner(state, now) {
		score += scopeStoreFreshOwnerScore
	}
	if scopeStoreHasFreshControlLease(state, now) {
		score += scopeStoreFreshLeaseScore
	}
	return score
}

func scopeStoreHasFreshOwner(state teamstore.State, now time.Time) bool {
	for _, owner := range []*teamstore.OwnerMetadata{state.ServiceOwner, state.LockOwner} {
		if owner == nil || owner.LastHeartbeat.IsZero() {
			continue
		}
		if !teamstore.IsStale(*owner, scopeStoreOwnerFreshAfter, now) && !teamstore.OwnerAppearsLocallyDead(*owner) {
			return true
		}
	}
	return false
}

func scopeStoreHasFreshControlLease(state teamstore.State, now time.Time) bool {
	lease := state.ControlLease
	if strings.TrimSpace(lease.HolderMachineID) == "" || lease.Status != teamstore.ControlLeaseStatusActive || lease.LeaseUntil.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return lease.LeaseUntil.After(now)
}

func scopeStateMatches(current teamstore.ScopeIdentity, state teamstore.State) bool {
	if scopeIdentityMatches(current, state.Scope) {
		return true
	}
	control := state.ControlChat
	if strings.TrimSpace(control.ScopeID) == "" && strings.TrimSpace(control.AccountID) == "" && strings.TrimSpace(control.Profile) == "" {
		return false
	}
	return scopeIdentityMatches(current, teamstore.ScopeIdentity{
		ID:        control.ScopeID,
		AccountID: control.AccountID,
		Profile:   control.Profile,
	})
}

func scopeIdentityMatches(current teamstore.ScopeIdentity, existing teamstore.ScopeIdentity) bool {
	current = normalizeScopeForResolution(current)
	existing = normalizeScopeForResolution(existing)
	if current.ID != "" && current.ID == existing.ID {
		return true
	}
	if current.Profile != existing.Profile {
		return false
	}
	accountMatches := current.AccountID != "" && existing.AccountID != "" && current.AccountID == existing.AccountID
	principalMatches := current.UserPrincipal != "" && existing.UserPrincipal != "" && strings.EqualFold(current.UserPrincipal, existing.UserPrincipal)
	return accountMatches || principalMatches
}

func normalizeScopeForResolution(scope teamstore.ScopeIdentity) teamstore.ScopeIdentity {
	scope.ID = strings.TrimSpace(scope.ID)
	scope.AccountID = strings.TrimSpace(scope.AccountID)
	scope.UserPrincipal = strings.TrimSpace(scope.UserPrincipal)
	scope.OSUser = strings.TrimSpace(scope.OSUser)
	scope.Profile = strings.TrimSpace(scope.Profile)
	if scope.Profile == "" {
		scope.Profile = "default"
	}
	scope.ConfigPath = strings.TrimSpace(scope.ConfigPath)
	scope.CodexHome = strings.TrimSpace(scope.CodexHome)
	return scope
}

func MachineRecordForUser(user User, scope teamstore.ScopeIdentity) teamstore.MachineRecord {
	hostname := machineLabel()
	label := strings.TrimSpace(os.Getenv(envTeamsMachineLabel))
	if label == "" {
		label = hostname
	}
	kind := machineKindFromEnv()
	priority := machinePriorityFromEnv(kind)
	machineID := strings.TrimSpace(os.Getenv(envTeamsMachineID))
	if machineID == "" {
		machineID = stableID("machine", []string{
			"v1",
			scope.ID,
			hostname,
			localOSUser(),
			string(kind),
		})
	}
	return teamstore.MachineRecord{
		ID:            machineID,
		ScopeID:       scope.ID,
		Label:         label,
		Hostname:      hostname,
		OSUser:        localOSUser(),
		AccountID:     user.ID,
		UserPrincipal: user.UserPrincipalName,
		Profile:       scope.Profile,
		Kind:          kind,
		Priority:      priority,
	}
}

func machineKindFromEnv() teamstore.MachineKind {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envTeamsMachineKind))) {
	case "primary", "long", "long-lived", "persistent":
		return teamstore.MachineKindPrimary
	case "ephemeral", "temporary", "temp":
		return teamstore.MachineKindEphemeral
	default:
		if looksLikeBatchJob() {
			return teamstore.MachineKindEphemeral
		}
		return teamstore.MachineKindPrimary
	}
}

func machinePriorityFromEnv(kind teamstore.MachineKind) int {
	raw := strings.TrimSpace(os.Getenv(envTeamsMachinePriority))
	if raw == "" {
		return teamstore.DefaultMachinePriority(kind)
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return teamstore.DefaultMachinePriority(kind)
	}
	return value
}

func looksLikeBatchJob() bool {
	for _, name := range []string{"SLURM_JOB_ID", "PBS_JOBID", "LSB_JOBID", "JOB_ID", "KUBERNETES_SERVICE_HOST"} {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			return true
		}
	}
	return false
}

func localOSUser() string {
	for _, name := range []string{"USER", "LOGNAME", "USERNAME"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return home
	}
	return "unknown"
}

func stableID(prefix string, parts []string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + ":" + hex.EncodeToString(sum[:])[:16]
}

func safeScopePathPart(scopeID string) string {
	scopeID = strings.TrimSpace(scopeID)
	if scopeID == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range scopeID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" {
		return "default"
	}
	return out
}
