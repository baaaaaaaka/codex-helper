package teams

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/gofrs/flock"
)

type runtimeSafetyMigrationReceipt struct {
	Version     int       `json:"version"`
	ScopeID     string    `json:"scope_id"`
	LegacyPath  string    `json:"legacy_path"`
	BackupPath  string    `json:"backup_path"`
	CompletedAt time.Time `json:"completed_at"`
}

func runtimeSafetyMigrationReceiptPath(currentPath string) string {
	return filepath.Join(filepath.Dir(currentPath), "migration-receipt.json")
}

func readRuntimeSafetyMigrationReceipt(t *testing.T, currentPath string) runtimeSafetyMigrationReceipt {
	t.Helper()
	path := runtimeSafetyMigrationReceiptPath(currentPath)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration receipt %s: %v", path, err)
	}
	var receipt runtimeSafetyMigrationReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatalf("decode migration receipt %s: %v", path, err)
	}
	return receipt
}

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

func TestTeamsRuntimeSafetyRuntimeResolverFailsClosedWhenCanonicalStoreLockTimesOutCI(t *testing.T) {
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

	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if err == nil {
		t.Fatalf("runtime resolver silently downgraded to %q while canonical store %q could not be loaded", resolvedPath, currentPath)
	}
}

func TestTeamsRuntimeSafetyRuntimeResolverFailsClosedForCanonicalProbeFailuresCI(t *testing.T) {
	for _, failure := range []string{"open", "load", "close"} {
		t.Run(failure, func(t *testing.T) {
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
			t.Cleanup(func() {
				resolveScopeStoreOpen = prevOpen
				resolveScopeStoreLoad = prevLoad
				resolveScopeStoreClose = prevClose
			})
			switch failure {
			case "open":
				resolveScopeStoreOpen = func(path string) (*teamstore.Store, error) {
					if path == currentPath {
						return nil, fmt.Errorf("injected canonical open failure")
					}
					return prevOpen(path)
				}
			case "load":
				resolveScopeStoreLoad = func(st *teamstore.Store, ctx context.Context) (teamstore.State, error) {
					if st.Path() == currentPath {
						return teamstore.State{}, fmt.Errorf("injected canonical load failure")
					}
					return prevLoad(st, ctx)
				}
			case "close":
				resolveScopeStoreClose = func(st *teamstore.Store) error {
					err := prevClose(st)
					if st.Path() == currentPath {
						return fmt.Errorf("injected canonical close failure")
					}
					return err
				}
			}

			if _, resolvedPath, err := ResolveStorePathForScope(scope); err == nil {
				t.Fatalf("runtime resolver selected %q after canonical %s failure; want fail-closed error", resolvedPath, failure)
			}
		})
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

	if _, resolvedPath, err := ResolveStorePathForScope(scope); err == nil {
		t.Fatalf("runtime resolver selected %q after canonical load failure; runtime must fail closed", resolvedPath)
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
	if firstErr == nil && firstPath != legacyPath {
		t.Fatalf("unexpected first resolution path %q; want fail-closed error or historical legacy fallback %q", firstPath, legacyPath)
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

	if _, resolvedPath, err := ResolveStorePathForScope(scope); err == nil {
		t.Fatalf("runtime resolver selected %q after legacy acquired fresh authority; want explicit split-brain failure", resolvedPath)
	}
}

func TestTeamsRuntimeSafetyRuntimeResolverRejectsLiveDivergentLegacyStoreCI(t *testing.T) {
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
	if err == nil {
		t.Fatalf("runtime resolver selected %q instead of reporting split-brain between %q and %q", resolvedPath, currentPath, legacyPath)
	}
}

func TestTeamsRuntimeSafetyResolverUsesMetadataProbeInsteadOfLoadingSessionAndOutboxTablesCI(t *testing.T) {
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
		t.Fatalf("metadata resolution must not load unrelated session/outbox rows: %v", err)
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

func TestTeamsRuntimeSafetyResolverMetadataProbeIsStrictlyReadOnlyCI(t *testing.T) {
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
		t.Fatalf("metadata probe created a state lock file: %v", err)
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
			"metadata probe modified canonical store: bytes_equal=%t mode=%v->%v mtime=%v->%v",
			bytes.Equal(before, after),
			beforeInfo.Mode(),
			afterInfo.Mode(),
			beforeInfo.ModTime(),
			afterInfo.ModTime(),
		)
	}
}

func TestTeamsRuntimeSafetySuccessfulMigrationWritesReceiptAndQuarantinesLegacyCI(t *testing.T) {
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
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("successful migration left legacy store in the normal candidate scan at %q (stat err=%v)", legacyPath, err)
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(legacyPath), "store.sqlite*")); err != nil {
		t.Fatalf("glob legacy SQLite family: %v", err)
	} else if len(matches) != 0 {
		t.Fatalf("successful migration left legacy SQLite files in the candidate directory: %v", matches)
	}
	receipt := readRuntimeSafetyMigrationReceipt(t, currentPath)
	if receipt.Version < 1 || receipt.ScopeID != scope.ID || receipt.LegacyPath != legacyPath ||
		receipt.BackupPath == "" || receipt.CompletedAt.IsZero() {
		t.Fatalf("migration receipt is incomplete: %#v", receipt)
	}
	legacyDir := filepath.Clean(filepath.Dir(legacyPath))
	backupDir := filepath.Clean(filepath.Dir(receipt.BackupPath))
	if backupDir == legacyDir || strings.HasPrefix(backupDir+string(filepath.Separator), legacyDir+string(filepath.Separator)) {
		t.Fatalf("migration backup %q remains inside the normal legacy candidate directory %q", receipt.BackupPath, legacyDir)
	}
	backup, err := teamstore.Open(receipt.BackupPath)
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

func TestTeamsRuntimeSafetyLegacyReappearanceAfterMigrationReceiptIsConflictCI(t *testing.T) {
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
	backupPath := filepath.Join(tmp, "migration-backups", scope.ID, "state.json")
	openAndSeedRuntimeSafetyScopeStore(t, backupPath, scope, "legacy-snapshot")
	receipt := runtimeSafetyMigrationReceipt{
		Version:     1,
		ScopeID:     scope.ID,
		LegacyPath:  legacyPath,
		BackupPath:  backupPath,
		CompletedAt: time.Now(),
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("marshal migration receipt: %v", err)
	}
	if err := os.WriteFile(runtimeSafetyMigrationReceiptPath(currentPath), raw, 0o600); err != nil {
		t.Fatalf("write migration receipt: %v", err)
	}

	// A legacy store appearing after the receipt can only have been recreated
	// and written by an old helper; it is not the already-quarantined snapshot.
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "recreated-legacy-control")
	if _, resolvedPath, err := ResolveStorePathForScope(scope); err == nil {
		t.Fatalf("runtime resolver selected %q after legacy reappeared post-migration; want explicit conflict", resolvedPath)
	}
}
