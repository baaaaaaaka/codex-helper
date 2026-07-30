package teams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/gofrs/flock"
)

func openAndSeedRuntimeSafetyScopeStore(t *testing.T, path string, scope teamstore.ScopeIdentity, chatID string) {
	t.Helper()
	st, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("open store %s: %v", path, err)
	}
	if _, err := st.RecordScope(context.Background(), scope); err != nil {
		_ = st.Close()
		t.Fatalf("record scope %s: %v", path, err)
	}
	if err := st.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{
			ScopeID:     scope.ID,
			AccountID:   scope.AccountID,
			Profile:     scope.Profile,
			TeamsChatID: chatID,
		}
		return nil
	}); err != nil {
		_ = st.Close()
		t.Fatalf("seed store %s: %v", path, err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store %s: %v", path, err)
	}
}

func TestTeamsRuntimeSafetyRuntimeResolverNeverReturnsLegacyPathCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

	_, runtimePath, err := ResolveStorePathForScope(scope)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope: %v", err)
	}
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if runtimePath != currentPath {
		t.Fatalf("runtime resolver returned %q, want canonical state-layer path %q", runtimePath, currentPath)
	}
}

func TestTeamsRuntimeSafetyRuntimeResolverCanonicalFastPathIgnoresLegacyAndStoreLockCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

	lock := flock.New(currentPath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock canonical store: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	opened := 0
	prevOpen := resolveScopeStoreOpen
	resolveScopeStoreOpen = func(path string) (*teamstore.Store, error) {
		opened++
		return prevOpen(path)
	}
	t.Cleanup(func() { resolveScopeStoreOpen = prevOpen })

	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if err != nil {
		t.Fatalf("canonical fast path must not wait on the store lock: %v", err)
	}
	if resolvedPath != currentPath {
		t.Fatalf("runtime resolver returned %q, want canonical %q", resolvedPath, currentPath)
	}
	if opened != 0 {
		t.Fatalf("canonical fast path opened %d store candidates; want zero", opened)
	}
}

func TestTeamsRuntimeSafetyRuntimeResolverCanonicalFastPathDoesNotOpenLoadOrCloseCandidatesCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

	prevOpen := resolveScopeStoreOpen
	prevLoad := resolveScopeStoreLoad
	prevClose := resolveScopeStoreClose
	var opens, loads, closes int
	resolveScopeStoreOpen = func(string) (*teamstore.Store, error) {
		opens++
		return nil, fmt.Errorf("canonical hot path must not open a store")
	}
	resolveScopeStoreLoad = func(*teamstore.Store, context.Context) (teamstore.State, error) {
		loads++
		return teamstore.State{}, fmt.Errorf("canonical hot path must not load a store")
	}
	resolveScopeStoreClose = func(*teamstore.Store) error {
		closes++
		return fmt.Errorf("canonical hot path must not close a store")
	}
	t.Cleanup(func() {
		resolveScopeStoreOpen = prevOpen
		resolveScopeStoreLoad = prevLoad
		resolveScopeStoreClose = prevClose
	})

	if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
		t.Fatalf("canonical hot path unexpectedly probed a candidate: %v", err)
	} else if resolvedPath != currentPath {
		t.Fatalf("resolved path = %q, want canonical %q", resolvedPath, currentPath)
	}
	if opens != 0 || loads != 0 || closes != 0 {
		t.Fatalf("canonical hot path operations: open=%d load=%d close=%d, want all zero", opens, loads, closes)
	}
}

func TestTeamsRuntimeSafetyRuntimeResolverFailsClosedButMaintenanceCanLocateLegacyCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(currentPath), 0o755); err != nil {
		t.Fatalf("mkdir canonical scope: %v", err)
	}
	if err := os.WriteFile(currentPath, []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("write corrupt canonical store: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

	before, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("read corrupt canonical before resolution: %v", err)
	}
	if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
		t.Fatalf("runtime path selection must not open or repair the canonical store: %v", err)
	} else if resolvedPath != currentPath {
		t.Fatalf("runtime resolver returned %q, want canonical %q", resolvedPath, currentPath)
	}
	after, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("read corrupt canonical after resolution: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("runtime path selection repaired or replaced a corrupt canonical store from legacy")
	}
	canonical, err := teamstore.Open(currentPath)
	if err == nil {
		_, err = canonical.Load(context.Background())
		_ = canonical.Close()
	}
	if err == nil {
		t.Fatal("corrupt canonical store unexpectedly loaded; listener open/load must surface the canonical error")
	}
	if _, maintenancePath, err := ResolveStorePathForMaintenance(scope); err != nil {
		t.Fatalf("maintenance resolver must remain able to locate a retained legacy helper: %v", err)
	} else if maintenancePath != legacyPath {
		t.Fatalf("maintenance path = %q, want retained legacy path %q", maintenancePath, legacyPath)
	}
}

func TestTeamsRuntimeSafetyLegacyFallbackCannotCreateSelfReinforcingAuthorityCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	user := User{ID: "teams-user-1", UserPrincipalName: "same@example.test"}
	scope := ScopeIdentityForUser(user)
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

	lock := flock.New(currentPath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock canonical store: %v", err)
	}
	_, firstPath, firstErr := ResolveStorePathForScope(scope)
	if err := lock.Unlock(); err != nil {
		t.Fatalf("unlock canonical store: %v", err)
	}
	if firstErr != nil {
		t.Fatalf("canonical hot path was affected by a busy store lock: %v", firstErr)
	}
	if firstPath != currentPath {
		t.Fatalf("canonical hot path selected %q while locked, want %q", firstPath, currentPath)
	}

	// Reproduce the historical self-reinforcing step explicitly: a listener that
	// was allowed onto legacy immediately wrote a fresh owner and control lease.
	legacy, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	now := time.Now()
	owner, err := teamstore.CurrentOwner("v-legacy", "", "", now)
	if err != nil {
		_ = legacy.Close()
		t.Fatalf("CurrentOwner: %v", err)
	}
	machine := MachineRecordForUser(user, scope)
	owner.ScopeID = scope.ID
	owner.MachineID = machine.ID
	if _, err := legacy.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, now); err != nil {
		_ = legacy.Close()
		t.Fatalf("RecordOwnerHeartbeat: %v", err)
	}
	if _, err := legacy.ClaimControlLease(context.Background(), teamstore.ControlLeaseClaim{
		Scope:    scope,
		Machine:  machine,
		Owner:    owner,
		Duration: time.Minute,
		Now:      now,
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("ClaimControlLease: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}

	if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
		t.Fatalf("runtime resolver let legacy authority affect canonical path selection: %v", err)
	} else if resolvedPath != currentPath {
		t.Fatalf("fresh legacy authority selected %q, want canonical %q", resolvedPath, currentPath)
	}
}

func TestTeamsRuntimeSafetyRuntimeResolverCanonicalPathCannotLoseToLiveDivergentLegacyStoreCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

	legacy, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	owner, err := teamstore.CurrentOwner("v-legacy", "", "", time.Now())
	if err != nil {
		_ = legacy.Close()
		t.Fatalf("CurrentOwner: %v", err)
	}
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		serviceOwner := owner
		lockOwner := owner
		state.ServiceOwner = &serviceOwner
		state.LockOwner = &lockOwner
		state.Sessions["conflict"] = teamstore.SessionContext{
			ID:          "conflict",
			Status:      teamstore.SessionStatusActive,
			TeamsChatID: "legacy-work",
		}
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed live legacy authority: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}

	current, err := teamstore.Open(currentPath)
	if err != nil {
		t.Fatalf("open canonical store: %v", err)
	}
	if err := current.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions["conflict"] = teamstore.SessionContext{
			ID:          "conflict",
			Status:      teamstore.SessionStatusActive,
			TeamsChatID: "canonical-work",
		}
		return nil
	}); err != nil {
		_ = current.Close()
		t.Fatalf("seed canonical conflict: %v", err)
	}
	if err := current.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}

	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if err != nil {
		t.Fatalf("live legacy authority affected canonical path selection: %v", err)
	}
	if resolvedPath != currentPath {
		t.Fatalf("runtime resolver selected legacy %q instead of canonical %q", resolvedPath, currentPath)
	}
}

func TestTeamsRuntimeSafetyCanonicalFastPathDoesNotLoadSessionOrOutboxTablesCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

	current, err := teamstore.Open(currentPath)
	if err != nil {
		t.Fatalf("open canonical store: %v", err)
	}
	if _, _, err := current.CreateSession(context.Background(), teamstore.SessionContext{
		ID:          "corrupt-hot-row",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "canonical-work",
	}); err != nil {
		_ = current.Close()
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := current.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		_ = current.Close()
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	if err := current.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}
	dbPath := filepath.Join(filepath.Dir(currentPath), teamstore.SQLiteFileName)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open canonical sqlite: %v", err)
	}
	if _, err := db.Exec(`UPDATE sessions SET json = '{broken-json' WHERE id = 'corrupt-hot-row'`); err != nil {
		_ = db.Close()
		t.Fatalf("corrupt unrelated session row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close canonical sqlite: %v", err)
	}
	pointerBefore, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("read canonical pointer before resolution: %v", err)
	}

	if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
		t.Fatalf("canonical fast path must not load unrelated session/outbox rows: %v", err)
	} else if resolvedPath != currentPath {
		t.Fatalf("resolved path = %q, want canonical %q", resolvedPath, currentPath)
	}
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("reopen canonical sqlite: %v", err)
	}
	defer db.Close()
	var raw string
	if err := db.QueryRow(`SELECT json FROM sessions WHERE id = 'corrupt-hot-row'`).Scan(&raw); err != nil {
		t.Fatalf("read corrupt sentinel row: %v", err)
	}
	if raw != "{broken-json" {
		t.Fatalf("resolver repaired or replaced the canonical database while probing it; sentinel row = %q", raw)
	}
	pointerAfter, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("read canonical pointer after resolution: %v", err)
	}
	if !bytes.Equal(pointerBefore, pointerAfter) {
		t.Fatal("resolver replaced the canonical store pointer after an unrelated hot-table decode failure; metadata probing must be read-only")
	}
}

func TestTeamsRuntimeSafetyCanonicalFastPathIsStrictlyReadOnlyCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	if err := os.Remove(currentPath + ".lock"); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove fixture lock: %v", err)
	}
	before, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("read canonical store before probe: %v", err)
	}
	beforeInfo, err := os.Stat(currentPath)
	if err != nil {
		t.Fatalf("stat canonical store before probe: %v", err)
	}

	if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
		t.Fatalf("ResolveStorePathForScope: %v", err)
	} else if resolvedPath != currentPath {
		t.Fatalf("resolved path = %q, want canonical %q", resolvedPath, currentPath)
	}
	if _, err := os.Stat(currentPath + ".lock"); !os.IsNotExist(err) {
		t.Fatalf("canonical fast path created a state lock file: %v", err)
	}
	after, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("read canonical store after probe: %v", err)
	}
	afterInfo, err := os.Stat(currentPath)
	if err != nil {
		t.Fatalf("stat canonical store after probe: %v", err)
	}
	if !bytes.Equal(before, after) || beforeInfo.Mode() != afterInfo.Mode() || !beforeInfo.ModTime().Equal(afterInfo.ModTime()) {
		t.Fatalf(
			"canonical fast path modified canonical store: bytes_equal=%t mode=%v->%v mtime=%v->%v",
			bytes.Equal(before, after),
			beforeInfo.Mode(),
			afterInfo.Mode(),
			beforeInfo.ModTime(),
			afterInfo.ModTime(),
		)
	}
}

type runtimeSafetyMigrationFixture struct {
	Scope        teamstore.ScopeIdentity
	LegacyPath   string
	CurrentPath  string
	BackupPath   string
	ResolvedPath string
}

func runtimeSafetyMigrationBackupPath(sourcePath string, scopeID string) string {
	teamsRoot := filepath.Dir(filepath.Dir(filepath.Dir(sourcePath)))
	return filepath.Join(teamsRoot, "migration-backups", safeScopePathPart(scopeID), filepath.Base(sourcePath))
}

func seedRuntimeSafetyMigrationFixture(t *testing.T) runtimeSafetyMigrationFixture {
	t.Helper()
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")
	legacy, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if _, _, err := legacy.CreateSession(context.Background(), teamstore.SessionContext{
		ID:          "legacy-only-session",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "legacy-work",
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("CreateSession: %v", err)
	}
	if _, _, err := legacy.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "legacy-only-outbox",
		SessionID:   "legacy-only-session",
		TeamsChatID: "legacy-work",
		Body:        "preserve me",
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("QueueOutbox: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}

	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope: %v", err)
	}
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if resolvedPath != currentPath {
		t.Fatalf("resolved path = %q, want canonical %q", resolvedPath, currentPath)
	}
	return runtimeSafetyMigrationFixture{
		Scope:        scope,
		LegacyPath:   legacyPath,
		CurrentPath:  currentPath,
		BackupPath:   runtimeSafetyMigrationBackupPath(legacyPath, scope.ID),
		ResolvedPath: resolvedPath,
	}
}

func TestTeamsRuntimeSafetySuccessfulMigrationQuarantinesLegacyOutOfCandidateScanCI(t *testing.T) {
	fixture := seedRuntimeSafetyMigrationFixture(t)
	legacyPath := fixture.LegacyPath
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("successful migration left legacy store in the normal candidate scan at %q (stat err=%v)", legacyPath, err)
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(legacyPath), "store.sqlite*")); err != nil {
		t.Fatalf("glob legacy SQLite family: %v", err)
	} else if len(matches) != 0 {
		t.Fatalf("successful migration left legacy SQLite files in the candidate directory: %v", matches)
	}
	if _, err := os.Stat(fixture.BackupPath); err != nil {
		t.Fatalf("successful migration did not preserve legacy store at fixed backup %q: %v", fixture.BackupPath, err)
	}
}

func TestTeamsRuntimeSafetySuccessfulMigrationPreservesQuarantinedLogicalDataCI(t *testing.T) {
	fixture := seedRuntimeSafetyMigrationFixture(t)
	if _, err := os.Stat(fixture.BackupPath); err != nil {
		t.Fatalf("quarantined backup %q is missing: %v", fixture.BackupPath, err)
	}
	backup, err := teamstore.Open(fixture.BackupPath)
	if err != nil {
		t.Fatalf("open quarantined backup: %v", err)
	}
	backupState, err := backup.Load(context.Background())
	if err != nil {
		_ = backup.Close()
		t.Fatalf("load quarantined backup: %v", err)
	}
	if err := backup.Close(); err != nil {
		t.Fatalf("close quarantined backup: %v", err)
	}
	if backupState.Sessions["legacy-only-session"].TeamsChatID != "legacy-work" ||
		backupState.OutboxMessages["legacy-only-outbox"].Body != "preserve me" {
		t.Fatalf("quarantined backup lost logical data: sessions=%#v outbox=%#v", backupState.Sessions, backupState.OutboxMessages)
	}
}

func TestTeamsRuntimeSafetyLegacyReappearanceAfterFixedBackupIsConflictCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	backupPath := runtimeSafetyMigrationBackupPath(legacyPath, scope.ID)
	openAndSeedRuntimeSafetyScopeStore(t, backupPath, scope, "legacy-snapshot")

	// A legacy store appearing after the fixed backup can only have been recreated
	// and written by an old helper; it is not the already-quarantined snapshot.
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "recreated-legacy-control")
	if resolvedPath, err := exerciseRuntimeSafetyTakeoverCoordinator(scope, legacyPath); err == nil {
		t.Fatalf("runtime resolver selected %q after legacy reappeared post-migration; want explicit conflict", resolvedPath)
	} else if !stringsContainAnyFold(err.Error(), "reappear", "conflict", "split-brain") {
		t.Fatalf("legacy reappearance error = %v, want an explicit conflict classification", err)
	}
}

func TestTeamsRuntimeSafetyMigrationBackupStateNeverOverwritesExistingBackupCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	backupPath := runtimeSafetyMigrationBackupPath(legacyPath, scope.ID)
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "recreated-legacy-control")
	openAndSeedRuntimeSafetyScopeStore(t, backupPath, scope, "preserved-legacy-control")
	backupBefore, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("read backup before resolution: %v", err)
	}

	if resolvedPath, err := exerciseRuntimeSafetyTakeoverCoordinator(scope, legacyPath); err == nil {
		t.Fatalf("runtime resolver selected %q while legacy source and fixed backup both exist; want conflict", resolvedPath)
	} else if !stringsContainAnyFold(err.Error(), "backup", "conflict") {
		t.Fatalf("source+backup error = %v, want a non-overwrite conflict", err)
	}
	backupAfter, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("read backup after conflict: %v", err)
	}
	if !bytes.Equal(backupBefore, backupAfter) {
		t.Fatal("conflicting legacy source overwrote the existing fixed backup")
	}
}

func TestTeamsRuntimeSafetyMigrationResumesPartialPerRootQuarantineCI(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")
	storeBackupPath := runtimeSafetyMigrationBackupPath(legacyPath, scope.ID)
	openAndSeedRuntimeSafetyScopeStore(t, storeBackupPath, scope, "legacy-snapshot")

	scopePart := safeScopePathPart(scope.ID)
	legacyRegistryPath := filepath.Join(cacheBase, "codex-helper", "teams", "scopes", scopePart, "registry.json")
	if err := os.MkdirAll(filepath.Dir(legacyRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	legacyRegistry := []byte(`{"version":1,"control_chat_id":"legacy-control"}`)
	if err := os.WriteFile(legacyRegistryPath, legacyRegistry, 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}
	registryBackupPath := runtimeSafetyMigrationBackupPath(legacyRegistryPath, scope.ID)

	if resolvedPath, err := exerciseRuntimeSafetyTakeoverCoordinator(scope, legacyRegistryPath); err != nil {
		t.Fatalf("resume partial per-root quarantine: %v", err)
	} else if resolvedPath != currentPath {
		t.Fatalf("resolved path = %q, want canonical %q", resolvedPath, currentPath)
	}
	if _, err := os.Stat(legacyRegistryPath); !os.IsNotExist(err) {
		t.Fatalf("partial retry left legacy registry source in place: %v", err)
	}
	got, err := os.ReadFile(registryBackupPath)
	if err != nil {
		t.Fatalf("partial retry did not finish cache-root quarantine: %v", err)
	}
	if !bytes.Equal(got, legacyRegistry) {
		t.Fatalf("quarantined registry bytes changed: got=%q want=%q", got, legacyRegistry)
	}
}

type runtimeSafetyTakeoverFixture struct {
	Root          string
	Scope         teamstore.ScopeIdentity
	CanonicalPath string
	LegacyPath    string
	BackupPath    string
}

func seedRuntimeSafetyTakeoverFixture(t *testing.T) runtimeSafetyTakeoverFixture {
	t.Helper()
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, canonicalPath, scope, "canonical-control")
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

	canonical, err := teamstore.Open(canonicalPath)
	if err != nil {
		t.Fatalf("open canonical store: %v", err)
	}
	if err := canonical.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions["shared-session"] = teamstore.SessionContext{
			ID:          "shared-session",
			Status:      teamstore.SessionStatusActive,
			TeamsChatID: "canonical-chat",
		}
		return nil
	}); err != nil {
		_ = canonical.Close()
		t.Fatalf("seed canonical conflict: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}

	legacy, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions["shared-session"] = teamstore.SessionContext{
			ID:          "shared-session",
			Status:      teamstore.SessionStatusActive,
			TeamsChatID: "legacy-conflicting-chat",
		}
		state.Sessions["legacy-only-session"] = teamstore.SessionContext{
			ID:          "legacy-only-session",
			Status:      teamstore.SessionStatusActive,
			TeamsChatID: "legacy-only-chat",
		}
		state.InboundEvents["inbound-terminal"] = teamstore.InboundEvent{
			ID:             "inbound-terminal",
			TeamsChatID:    "legacy-only-chat",
			TeamsMessageID: "teams-terminal",
			Status:         teamstore.InboundStatusIgnored,
			Text:           "must not be replayed",
		}
		state.InboundEvents["inbound-nonterminal"] = teamstore.InboundEvent{
			ID:             "inbound-nonterminal",
			TeamsChatID:    "legacy-only-chat",
			TeamsMessageID: "teams-nonterminal",
			Status:         teamstore.InboundStatusQueued,
			Text:           "must remain recoverable",
		}
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed legacy divergence: %v", err)
	}
	if _, _, err := legacy.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "legacy-sent",
		SessionID:   "legacy-only-session",
		TeamsChatID: "legacy-only-chat",
		Body:        "already sent",
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("queue legacy sent outbox: %v", err)
	}
	if _, err := legacy.MarkOutboxSent(context.Background(), "legacy-sent", "teams-sent-message"); err != nil {
		_ = legacy.Close()
		t.Fatalf("mark legacy outbox sent: %v", err)
	}
	if _, _, err := legacy.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "legacy-queued",
		SessionID:   "legacy-only-session",
		TeamsChatID: "legacy-only-chat",
		Body:        "must not be sent automatically",
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("queue legacy pending outbox: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	return runtimeSafetyTakeoverFixture{
		Root:          tmp,
		Scope:         scope,
		CanonicalPath: canonicalPath,
		LegacyPath:    legacyPath,
		BackupPath:    runtimeSafetyMigrationBackupPath(legacyPath, scope.ID),
	}
}

// exerciseRuntimeSafetyTakeoverCoordinator is the narrow test adapter for the
// production takeover coordinator. Process and Windows fencing have no work in
// these in-process fixtures; the contract tests below inject those boundaries
// explicitly.
func exerciseRuntimeSafetyTakeoverCoordinator(scope teamstore.ScopeIdentity, legacySources ...string) (string, error) {
	result, err := ExecuteAutomaticScopeTakeover(
		context.Background(),
		scope,
		legacySources,
		AutomaticScopeTakeoverOptions{},
	)
	return result.CanonicalPath, err
}

func TestTeamsRuntimeSafetyAutomaticTakeoverCanonicalWinsAndPreservesLegacyBackupCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)

	if resolvedPath, err := exerciseRuntimeSafetyTakeoverCoordinator(fixture.Scope, fixture.LegacyPath); err != nil {
		t.Fatalf("idle, locally manageable dual store should be taken over automatically: %v", err)
	} else if resolvedPath != fixture.CanonicalPath {
		t.Fatalf("resolved path = %q, want canonical %q", resolvedPath, fixture.CanonicalPath)
	}
	if _, err := os.Stat(fixture.LegacyPath); !os.IsNotExist(err) {
		t.Errorf("automatic takeover left legacy source in the normal scan directory: %v", err)
	}

	canonical, err := teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("open canonical after takeover: %v", err)
	}
	canonicalState, err := canonical.Load(context.Background())
	if err != nil {
		_ = canonical.Close()
		t.Fatalf("load canonical after takeover: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical after takeover: %v", err)
	}
	if got := canonicalState.Sessions["shared-session"].TeamsChatID; got != "canonical-chat" {
		t.Errorf("legacy binding overwrote canonical conflict: got %q", got)
	}
	if _, ok := canonicalState.Sessions["legacy-only-session"]; ok {
		t.Error("automatic takeover performed an unsafe full session merge")
	}

	backup, err := teamstore.Open(fixture.BackupPath)
	if err != nil {
		t.Fatalf("open preserved legacy backup: %v", err)
	}
	backupState, err := backup.Load(context.Background())
	if err != nil {
		_ = backup.Close()
		t.Fatalf("load preserved legacy backup: %v", err)
	}
	if err := backup.Close(); err != nil {
		t.Fatalf("close preserved legacy backup: %v", err)
	}
	if backupState.Sessions["shared-session"].TeamsChatID != "legacy-conflicting-chat" ||
		backupState.OutboxMessages["legacy-queued"].Body != "must not be sent automatically" {
		t.Errorf("legacy backup did not preserve divergent business data: sessions=%#v outbox=%#v", backupState.Sessions, backupState.OutboxMessages)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverImportsOnlyReplaySuppressionCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	if _, err := exerciseRuntimeSafetyTakeoverCoordinator(fixture.Scope, fixture.LegacyPath); err != nil {
		t.Fatalf("automatic takeover: %v", err)
	}
	canonical, err := teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("open canonical after takeover: %v", err)
	}
	defer canonical.Close()

	hasTerminal, err := canonical.HasInboundMessage(context.Background(), "legacy-only-chat", "teams-terminal")
	if err != nil {
		t.Fatalf("lookup terminal replay fence: %v", err)
	}
	if !hasTerminal {
		t.Error("terminal legacy inbound message was not added to duplicate-processing suppression")
	}
	hasNonterminal, err := canonical.HasInboundMessage(context.Background(), "legacy-only-chat", "teams-nonterminal")
	if err != nil {
		t.Fatalf("lookup nonterminal replay fence: %v", err)
	}
	if hasNonterminal {
		t.Error("nonterminal legacy inbound message was incorrectly marked processed")
	}

	state, err := canonical.Load(context.Background())
	if err != nil {
		t.Fatalf("load canonical replay state: %v", err)
	}
	if _, ok := state.OutboxMessages["legacy-queued"]; ok {
		t.Error("queued legacy outbox was imported into the canonical send queue")
	}
	if _, ok := state.OutboxMessages["legacy-sent"]; !ok {
		t.Error("sent legacy outbox idempotency record was not imported")
	} else if state.OutboxMessages["legacy-sent"].Status != teamstore.OutboxStatusSent {
		t.Errorf("sent legacy outbox suppression status = %q, want sent", state.OutboxMessages["legacy-sent"].Status)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverDefersActiveTurnWithoutMutationCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacy, err := teamstore.Open(fixture.LegacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		state.Turns["turn-running"] = teamstore.Turn{
			ID:        "turn-running",
			SessionID: "legacy-only-session",
			Status:    teamstore.TurnStatusRunning,
			StartedAt: time.Now(),
		}
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed active legacy turn: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	before := snapshotRuntimeSafetyFiles(t, fixture.Root)

	resolvedPath, err := exerciseRuntimeSafetyTakeoverCoordinator(fixture.Scope, fixture.LegacyPath)
	if err == nil {
		t.Errorf("runtime entrypoint selected %q instead of deferring an active legacy turn", resolvedPath)
	} else if !stringsContainAnyFold(err.Error(), "active turn", "drain", "deferred") {
		t.Errorf("active-turn takeover error = %v, want drain/deferred classification", err)
	}
	after := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("deferred active-turn takeover modified files: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverDefersRemoteOwnerWithoutMutationCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacy, err := teamstore.Open(fixture.LegacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	owner, err := teamstore.CurrentOwner("v-legacy", "", "", time.Now())
	if err != nil {
		_ = legacy.Close()
		t.Fatalf("CurrentOwner: %v", err)
	}
	owner.ScopeID = fixture.Scope.ID
	owner.MachineID = "remote-machine"
	owner.Hostname = "remote-host.example"
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		state.ServiceOwner = &owner
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed remote owner: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	before := snapshotRuntimeSafetyFiles(t, fixture.Root)

	resolvedPath, err := exerciseRuntimeSafetyTakeoverCoordinator(fixture.Scope, fixture.LegacyPath)
	if err == nil {
		t.Errorf("runtime entrypoint selected %q instead of deferring an unmanageable remote owner", resolvedPath)
	} else if !stringsContainAnyFold(err.Error(), "remote", "owner", "deferred") {
		t.Errorf("remote-owner takeover error = %v, want owner/deferred classification", err)
	}
	after := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("deferred remote-owner takeover modified files: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyDiscoveryUsesBoundedMetadataReadsAcrossUnrelatedScopesCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	target := ScopeIdentityForUser(User{ID: "target-user", UserPrincipalName: "target@example.test"})
	for i := 0; i < 100; i++ {
		scope := teamstore.ScopeIdentity{
			ID:            fmt.Sprintf("scope-unrelated-%03d", i),
			AccountID:     fmt.Sprintf("unrelated-%03d", i),
			UserPrincipal: fmt.Sprintf("unrelated-%03d@example.test", i),
			Profile:       "default",
		}
		path, err := legacyDefaultStorePathForScope(scope.ID)
		if err != nil {
			t.Fatalf("legacyDefaultStorePathForScope(%d): %v", i, err)
		}
		openAndSeedRuntimeSafetyScopeStore(t, path, scope, "unrelated-control")
	}
	targetLegacy, err := legacyDefaultStorePathForScope(target.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope(target): %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, targetLegacy, target, "target-control")

	prevOpen := resolveScopeStoreOpen
	prevLoad := resolveScopeStoreLoad
	var opens, fullLoads int
	resolveScopeStoreOpen = func(path string) (*teamstore.Store, error) {
		opens++
		return prevOpen(path)
	}
	resolveScopeStoreLoad = func(st *teamstore.Store, ctx context.Context) (teamstore.State, error) {
		fullLoads++
		return prevLoad(st, ctx)
	}
	t.Cleanup(func() {
		resolveScopeStoreOpen = prevOpen
		resolveScopeStoreLoad = prevLoad
	})

	if _, _, err := ResolveStorePathForScope(target); err != nil {
		t.Fatalf("cold migration discovery: %v", err)
	}
	if opens > 105 {
		t.Errorf("cold discovery opened %d stores for 100 unrelated scopes; want bounded linear discovery", opens)
	}
	if fullLoads != 0 {
		t.Errorf("cold discovery performed %d full Store.Load calls; want metadata-only probes", fullLoads)
	}
}

type runtimeSafetyFileSnapshot struct {
	Mode    fs.FileMode
	ModTime time.Time
	Size    int64
	SHA256  [sha256.Size]byte
}

func snapshotRuntimeSafetyFiles(t *testing.T, root string) map[string]runtimeSafetyFileSnapshot {
	t.Helper()
	out := map[string]runtimeSafetyFileSnapshot{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[rel] = runtimeSafetyFileSnapshot{
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
			Size:    info.Size(),
			SHA256:  sha256.Sum256(data),
		}
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot %s: %v", root, err)
	}
	return out
}

func runtimeSafetySnapshotChanges(before, after map[string]runtimeSafetyFileSnapshot) []string {
	changed := make([]string, 0)
	for path, beforeFile := range before {
		afterFile, ok := after[path]
		if !ok {
			changed = append(changed, "removed:"+path)
		} else if beforeFile != afterFile {
			changed = append(changed, "modified:"+path)
		}
	}
	for path := range after {
		if _, ok := before[path]; !ok {
			changed = append(changed, "added:"+path)
		}
	}
	sort.Strings(changed)
	return changed
}

func stringsContainAnyFold(value string, candidates ...string) bool {
	value = strings.ToLower(value)
	for _, candidate := range candidates {
		if strings.Contains(value, strings.ToLower(candidate)) {
			return true
		}
	}
	return false
}
