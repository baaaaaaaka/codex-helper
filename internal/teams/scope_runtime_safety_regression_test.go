package teams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
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

func requireRuntimeStoreActionPlan(
	t *testing.T,
	err error,
	action RuntimeStoreAction,
	canonicalPath string,
	legacyPath string,
) RuntimeStorePlan {
	t.Helper()
	plan, ok := RuntimeStorePlanFromError(err)
	if !ok {
		t.Fatalf("error = %v, want structured runtime store action", err)
	}
	if plan.Action != action || plan.CanonicalPath != canonicalPath || plan.LegacyPath != legacyPath {
		t.Fatalf("runtime store plan = %#v, want action=%q canonical=%q legacy=%q", plan, action, canonicalPath, legacyPath)
	}
	return plan
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

	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	_, runtimePath, err := ResolveStorePathForScope(scope)
	if runtimePath != "" {
		t.Fatalf("runtime resolver returned path %q while migration is required", runtimePath)
	}
	requireRuntimeStoreActionPlan(t, err, RuntimeStoreActionMigrateLegacy, currentPath, legacyPath)
}

func TestTeamsRuntimeSafetyBridgeConstructionReturnsPlanBeforeRegistryMutationCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/me" {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"user-1","displayName":"User One","userPrincipalName":"user@example.test"}`)
	}))
	t.Cleanup(server.Close)
	graph := newTestGraphClient(&fakeGraphAuth{token: "access"}, server, nil)
	scope := ScopeIdentityForUser(User{ID: "user-1", UserPrincipalName: "user@example.test"})
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

	before := snapshotRuntimeSafetyFiles(t, tmp)
	var out bytes.Buffer
	bridge, err := newBridgeWithGraphClients(context.Background(), graph, graph, "", &out)
	if bridge != nil {
		t.Fatal("Bridge construction returned a bridge while offline quarantine is required")
	}
	requireRuntimeStoreActionPlan(t, err, RuntimeStoreActionQuarantineLegacy, canonicalPath, legacyPath)
	after := snapshotRuntimeSafetyFiles(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("Bridge construction modified files before returning its plan: %v", runtimeSafetySnapshotChanges(before, after))
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
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")

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
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")

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
	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if resolvedPath != "" {
		t.Fatalf("runtime path selection returned %q while dual-store quarantine is required", resolvedPath)
	}
	requireRuntimeStoreActionPlan(t, err, RuntimeStoreActionQuarantineLegacy, currentPath, legacyPath)
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

func TestTeamsRuntimeSafetyOfflineTakeoverValidatesCanonicalBeforeWritesCI(t *testing.T) {
	for _, tc := range []struct {
		name          string
		seedCanonical func(t *testing.T, path string, scope teamstore.ScopeIdentity)
	}{
		{
			name: "corrupt",
			seedCanonical: func(t *testing.T, path string, _ teamstore.ScopeIdentity) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatalf("mkdir canonical: %v", err)
				}
				if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
					t.Fatalf("write corrupt canonical: %v", err)
				}
			},
		},
		{
			name: "wrong-scope",
			seedCanonical: func(t *testing.T, path string, scope teamstore.ScopeIdentity) {
				t.Helper()
				wrong := scope
				wrong.ID = "scope-other"
				wrong.AccountID = "other-account"
				wrong.UserPrincipal = "other@example.test"
				openAndSeedRuntimeSafetyScopeStore(t, path, wrong, "wrong-control")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsScopeUserDirsForTest(t, tmp)
			t.Setenv("USER", "alice")
			t.Setenv(envTeamsProfile, "default")
			scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
			canonicalPath, err := DefaultStorePathForScope(scope.ID)
			if err != nil {
				t.Fatalf("canonical path: %v", err)
			}
			legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
			if err != nil {
				t.Fatalf("legacy path: %v", err)
			}
			tc.seedCanonical(t, canonicalPath, scope)
			openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")

			before := snapshotRuntimeSafetyFiles(t, tmp)
			err = CompleteOfflineRuntimeStoreTakeover(context.Background(), scope, legacyPath)
			if err == nil || !strings.Contains(err.Error(), "canonical Teams store") {
				t.Fatalf("takeover error = %v, want canonical validation failure", err)
			}
			after := snapshotRuntimeSafetyFiles(t, tmp)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("failed canonical validation wrote files: %v", runtimeSafetySnapshotChanges(before, after))
			}
			if _, err := os.Stat(legacyPath); err != nil {
				t.Fatalf("legacy source was not preserved: %v", err)
			}
		})
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
	if firstPath != "" {
		t.Fatalf("dual-store resolver returned %q while locked", firstPath)
	}
	requireRuntimeStoreActionPlan(t, firstErr, RuntimeStoreActionQuarantineLegacy, currentPath, legacyPath)

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

	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if resolvedPath != "" {
		t.Fatalf("dual-store resolver returned %q after legacy authority changed", resolvedPath)
	}
	requireRuntimeStoreActionPlan(t, err, RuntimeStoreActionQuarantineLegacy, currentPath, legacyPath)
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
	if resolvedPath != "" {
		t.Fatalf("dual-store resolver selected %q instead of returning a quarantine plan", resolvedPath)
	}
	requireRuntimeStoreActionPlan(t, err, RuntimeStoreActionQuarantineLegacy, currentPath, legacyPath)
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
	openAndSeedRuntimeSafetyScopeStore(t, currentPath, scope, "canonical-control")

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

func TestTeamsRuntimeSafetyProbeDiscoversRealSQLiteLegacyMetadataCI(t *testing.T) {
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
		ID:          "corrupt-business-row",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "legacy-work",
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("create legacy session: %v", err)
	}
	if _, err := legacy.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		_ = legacy.Close()
		t.Fatalf("migrate legacy store to SQLite: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	dbPath := filepath.Join(filepath.Dir(legacyPath), teamstore.SQLiteFileName)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open legacy SQLite: %v", err)
	}
	if _, err := db.Exec(`UPDATE sessions SET json = '{broken-business-row' WHERE id = 'corrupt-business-row'`); err != nil {
		_ = db.Close()
		t.Fatalf("corrupt unrelated session row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy SQLite: %v", err)
	}

	before := snapshotRuntimeSafetyFiles(t, tmp)
	metadata, err := ProbeScopeMetadataReadOnly(context.Background(), legacyPath)
	if err != nil {
		t.Fatalf("probe real SQLite legacy metadata: %v", err)
	}
	after := snapshotRuntimeSafetyFiles(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("real SQLite metadata probe modified files: %v", runtimeSafetySnapshotChanges(before, after))
	}
	if metadata.Scope.ID != scope.ID || metadata.ControlChat.TeamsChatID != "legacy-control" {
		t.Fatalf("real SQLite metadata = %#v, want scope %q/control legacy-control", metadata, scope.ID)
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

func TestTeamsRuntimeSafetyListenerRejectsNonRegularCanonicalPathsCI(t *testing.T) {
	tests := []struct {
		name string
		make func(*testing.T, string)
	}{
		{
			name: "directory",
			make: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatalf("mkdir canonical directory: %v", err)
				}
			},
		},
		{
			name: "symlink",
			make: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatalf("mkdir canonical parent: %v", err)
				}
				target := filepath.Join(t.TempDir(), "target-state.json")
				if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
					t.Fatalf("write symlink target: %v", err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Skipf("symlink fixture unavailable: %v", err)
				}
			},
		},
		{
			name: "special-file",
			make: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatalf("mkdir canonical parent: %v", err)
				}
				listener, err := net.Listen("unix", path)
				if err != nil {
					t.Skipf("Unix socket fixture unavailable: %v", err)
				}
				if unixListener, ok := listener.(*net.UnixListener); ok {
					unixListener.SetUnlinkOnClose(false)
				}
				if err := listener.Close(); err != nil {
					t.Fatalf("close Unix socket fixture: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Keep the fixture root short enough for Unix-domain socket path
			// limits while still isolating every user directory.
			tmp, err := os.MkdirTemp("", "cxp-rs-")
			if err != nil {
				t.Fatalf("create short fixture root: %v", err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(tmp) })
			isolateTeamsScopeUserDirsForTest(t, tmp)
			t.Setenv("USER", "alice")
			t.Setenv(envTeamsProfile, "default")
			user := User{ID: "teams-user-1", UserPrincipalName: "same@example.test"}
			scope := ScopeIdentityForUser(user)
			path, err := DefaultStorePathForScope(scope.ID)
			if err != nil {
				t.Fatalf("DefaultStorePathForScope: %v", err)
			}
			test.make(t, path)

			bridge := &Bridge{user: user, scope: scope}
			err = bridge.Listen(context.Background(), BridgeOptions{
				Once: true, Interval: time.Millisecond, Executor: EchoExecutor{},
			})
			if err == nil || !strings.Contains(err.Error(), "not a regular file") {
				t.Fatalf("Bridge.Listen error = %v, want non-regular canonical rejection", err)
			}
		})
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

	plan, err := InspectRuntimeStoreForScope(context.Background(), scope)
	if err != nil {
		t.Fatalf("InspectRuntimeStoreForScope: %v", err)
	}
	if plan.Action != RuntimeStoreActionMigrateLegacy || plan.LegacyPath != legacyPath {
		t.Fatalf("migration plan = %#v, want exact legacy migration", plan)
	}
	if err := CompleteOfflineRuntimeStorePlan(context.Background(), plan); err != nil {
		t.Fatalf("CompleteOfflineRuntimeStorePlan: %v", err)
	}
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope after migration: %v", err)
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

func TestTeamsRuntimeSafetyLegacyOnlyMigrationAppliesDeliveredOutboxFenceToStagingCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-replay-fence", UserPrincipalName: "replay@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacy store path: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")
	legacy, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if _, _, err := legacy.CreateSession(context.Background(), teamstore.SessionContext{
		ID:          "legacy-session",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "legacy-chat",
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("create legacy session: %v", err)
	}
	if _, _, err := legacy.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "legacy-outbox",
		SessionID:   "legacy-session",
		TeamsChatID: "legacy-chat",
		Kind:        "final",
		Body:        "Graph accepted this before the old helper crashed",
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("queue legacy outbox: %v", err)
	}
	if _, err := legacy.MarkOutboxSendAttempt(context.Background(), "legacy-outbox"); err != nil {
		_ = legacy.Close()
		t.Fatalf("claim legacy outbox send: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	legacyRegistry, ok := registryPathForStoreMigrationSource(legacyPath)
	if !ok {
		t.Fatal("legacy registry path unavailable")
	}
	legacyOutbound, ok := globalOutboundLedgerPathForRegistry(legacyRegistry)
	if !ok {
		t.Fatal("legacy outbound ledger path unavailable")
	}
	if err := recordGlobalOutbound(context.Background(), legacyOutbound, globalOutboundItem{
		ChatID:    "legacy-chat",
		MessageID: "teams-delivered-before-crash",
		ScopeID:   scope.ID,
		OutboxID:  "legacy-outbox",
		SessionID: "legacy-session",
		Kind:      "final",
	}, time.Now()); err != nil {
		t.Fatalf("record delivered legacy outbox: %v", err)
	}

	plan, err := InspectRuntimeStoreForScope(context.Background(), scope)
	if err != nil || plan.Action != RuntimeStoreActionMigrateLegacy {
		t.Fatalf("migration plan = %#v err=%v, want legacy migration", plan, err)
	}
	if err := CompleteOfflineRuntimeStorePlan(context.Background(), plan); err != nil {
		t.Fatalf("complete legacy migration: %v", err)
	}
	state, err := teamstore.LoadPathReadOnly(context.Background(), plan.CanonicalPath)
	if err != nil {
		t.Fatalf("load migrated canonical store: %v", err)
	}
	outbox := state.OutboxMessages["legacy-outbox"]
	if outbox.Status != teamstore.OutboxStatusSent || outbox.TeamsMessageID != "teams-delivered-before-crash" {
		t.Fatalf("migrated outbox replay fence = %#v, want sent delivery", outbox)
	}
	var provenance teamstore.MessageProvenanceRecord
	for _, candidate := range state.MessageProvenance {
		if candidate.TeamsChatID == "legacy-chat" && candidate.TeamsMessageID == "teams-delivered-before-crash" {
			provenance = candidate
			break
		}
	}
	if provenance.OutboxID != outbox.ID {
		t.Fatalf("migrated outbox provenance = %#v, want outbox %q", provenance, outbox.ID)
	}
}

func TestTeamsRuntimeSafetyStagedLegacyMigrationRetriesEveryDurableBoundaryCI(t *testing.T) {
	stages := []string{
		runtimeStoreMigrationStageStagingReady,
		runtimeStoreMigrationStageStagingCopied,
		runtimeStoreMigrationStageStagingValidated,
		runtimeStoreMigrationStageReplayFencesMerged,
		runtimeStoreMigrationStageCanonicalPublished,
		runtimeStoreMigrationStageRegistryQuarantined,
	}
	for _, failStage := range stages {
		t.Run(failStage, func(t *testing.T) {
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
			legacyRegistry, err := legacyDefaultRegistryPathForScope(scope.ID)
			if err != nil {
				t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
			}
			if err := os.MkdirAll(filepath.Dir(legacyRegistry), 0o700); err != nil {
				t.Fatalf("mkdir legacy registry: %v", err)
			}
			if err := os.WriteFile(legacyRegistry, []byte(`{"version":1,"control_chat_id":"legacy-control"}`), 0o600); err != nil {
				t.Fatalf("write legacy registry: %v", err)
			}

			plan, err := InspectRuntimeStoreForScope(context.Background(), scope)
			if err != nil || plan.Action != RuntimeStoreActionMigrateLegacy {
				t.Fatalf("initial migration plan = %#v err=%v", plan, err)
			}
			failed := false
			runtimeStoreMigrationTestHook = func(stage string) error {
				if stage == failStage && !failed {
					failed = true
					return fmt.Errorf("injected migration crash at %s", stage)
				}
				return nil
			}
			t.Cleanup(func() { runtimeStoreMigrationTestHook = nil })
			if err := CompleteOfflineRuntimeStorePlan(context.Background(), plan); err == nil || !strings.Contains(err.Error(), failStage) {
				t.Fatalf("migration error = %v, want injected %s failure", err, failStage)
			}
			runtimeStoreMigrationTestHook = nil

			retryPlan, err := InspectRuntimeStoreForScope(context.Background(), scope)
			if err != nil {
				t.Fatalf("inspect retry plan: %v", err)
			}
			wantRetryAction := RuntimeStoreActionMigrateLegacy
			if failStage == runtimeStoreMigrationStageCanonicalPublished || failStage == runtimeStoreMigrationStageRegistryQuarantined {
				wantRetryAction = RuntimeStoreActionQuarantineLegacy
			}
			if retryPlan.Action != wantRetryAction {
				t.Fatalf("retry plan action = %q, want %q after %s", retryPlan.Action, wantRetryAction, failStage)
			}
			if err := CompleteOfflineRuntimeStorePlan(context.Background(), retryPlan); err != nil {
				t.Fatalf("retry migration after %s: %v", failStage, err)
			}
			finalPlan, err := InspectRuntimeStoreForScope(context.Background(), scope)
			if err != nil || finalPlan.Action != RuntimeStoreActionReady {
				t.Fatalf("final plan = %#v err=%v, want ready", finalPlan, err)
			}
			state, err := teamstore.LoadPathReadOnly(context.Background(), finalPlan.CanonicalPath)
			if err != nil {
				t.Fatalf("load canonical store after retry: %v", err)
			}
			if state.ControlChat.TeamsChatID != "legacy-control" {
				t.Fatalf("canonical control binding after retry = %#v", state.ControlChat)
			}
			if _, err := os.Stat(legacyPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("legacy source remains after retry: %v", err)
			}
		})
	}
}

func TestTeamsRuntimeSafetyLegacySourceLockRemainsHeldThroughFinalQuarantineCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-lock-lifetime", UserPrincipalName: "lock@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacy path: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")
	legacyRegistry, err := legacyDefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacy registry: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(legacyRegistry), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	if err := os.WriteFile(legacyRegistry, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}
	plan, err := InspectRuntimeStoreForScope(context.Background(), scope)
	if err != nil || plan.Action != RuntimeStoreActionMigrateLegacy {
		t.Fatalf("migration plan = %#v err=%v", plan, err)
	}

	checked := false
	previousHook := runtimeStoreMigrationTestHook
	runtimeStoreMigrationTestHook = func(stage string) error {
		if stage != runtimeStoreMigrationStageRegistryQuarantined {
			return nil
		}
		checked = true
		probe := flock.New(legacyPath + ".lock")
		locked, err := probe.TryLock()
		if err != nil {
			return err
		}
		if locked {
			_ = probe.Unlock()
			return errors.New("legacy source lock was released before scope quarantine")
		}
		return nil
	}
	t.Cleanup(func() { runtimeStoreMigrationTestHook = previousHook })
	if err := CompleteOfflineRuntimeStorePlan(context.Background(), plan); err != nil {
		t.Fatalf("complete migration: %v", err)
	}
	if !checked {
		t.Fatal("migration did not reach the registry quarantine boundary")
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

func TestTeamsRuntimeSafetyScopedQuarantineSourceBackupStateMatrixCI(t *testing.T) {
	tests := []struct {
		name       string
		source     bool
		backup     bool
		wantErr    bool
		wantSource bool
		wantBackup bool
	}{
		{name: "neither", wantSource: false, wantBackup: false},
		{name: "source_only", source: true, wantSource: false, wantBackup: true},
		{name: "backup_only", backup: true, wantSource: false, wantBackup: true},
		{name: "both", source: true, backup: true, wantErr: true, wantSource: true, wantBackup: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsScopeUserDirsForTest(t, tmp)
			scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
			sourcePath, err := legacyDefaultStorePathForScope(scope.ID)
			if err != nil {
				t.Fatalf("legacyDefaultStorePathForScope: %v", err)
			}
			backupPath := runtimeSafetyMigrationBackupPath(sourcePath, scope.ID)
			if tc.source {
				openAndSeedRuntimeSafetyScopeStore(t, sourcePath, scope, "source")
			}
			if tc.backup {
				openAndSeedRuntimeSafetyScopeStore(t, backupPath, scope, "backup")
			}
			handled, err := quarantineScopedStoreDirectory(scope.ID, sourcePath)
			if !handled {
				t.Fatal("scoped quarantine did not recognize exact legacy path")
			}
			if tc.wantErr {
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), "conflict") {
					t.Fatalf("quarantine error = %v, want conflict", err)
				}
			} else if err != nil {
				t.Fatalf("quarantine error: %v", err)
			}
			sourceExists, sourceErr := pathExists(sourcePath)
			if sourceErr != nil || sourceExists != tc.wantSource {
				t.Fatalf("source exists=%v err=%v, want %v", sourceExists, sourceErr, tc.wantSource)
			}
			backupExists, backupErr := pathExists(backupPath)
			if backupErr != nil || backupExists != tc.wantBackup {
				t.Fatalf("backup exists=%v err=%v, want %v", backupExists, backupErr, tc.wantBackup)
			}
		})
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

	if resolvedPath, err := exerciseRuntimeSafetyTakeoverCoordinator(scope, legacyPath); err != nil {
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

// exerciseRuntimeSafetyTakeoverCoordinator models the service lifecycle after
// its backend has stopped the old child and before it starts the replacement
// listener.
func exerciseRuntimeSafetyTakeoverCoordinator(scope teamstore.ScopeIdentity, legacySources ...string) (string, error) {
	if len(legacySources) != 1 {
		return "", fmt.Errorf("offline takeover requires one exact legacy scope path")
	}
	err := CompleteOfflineRuntimeStoreTakeover(context.Background(), scope, legacySources[0])
	path, pathErr := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return path, err
	}
	return path, pathErr
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

	beforeRetry := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if err := CompleteOfflineRuntimeStoreTakeover(context.Background(), fixture.Scope, fixture.LegacyPath); err != nil {
		t.Fatalf("idempotent offline takeover retry: %v", err)
	}
	afterRetry := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if !reflect.DeepEqual(beforeRetry, afterRetry) {
		t.Fatalf("completed offline takeover retry wrote files: %v", runtimeSafetySnapshotChanges(beforeRetry, afterRetry))
	}
}

func TestTeamsRuntimeSafetyOfflineTakeoverDefersUnfencedRemoteWriterBeforeStagingCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacy, err := teamstore.Open(fixture.LegacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		state.ServiceOwner = &teamstore.OwnerMetadata{
			PID:           4242,
			Hostname:      "remote-host.example",
			ScopeID:       fixture.Scope.ID,
			MachineID:     "remote-machine",
			StartedAt:     time.Now().Add(-time.Minute),
			LastHeartbeat: time.Now(),
		}
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed remote writer: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}

	hookCalled := false
	previousHook := runtimeStoreMigrationTestHook
	runtimeStoreMigrationTestHook = func(string) error {
		hookCalled = true
		return nil
	}
	t.Cleanup(func() { runtimeStoreMigrationTestHook = previousHook })
	err = CompleteOfflineRuntimeStoreTakeover(context.Background(), fixture.Scope, fixture.LegacyPath)
	var deferred *RuntimeStoreTakeoverDeferredError
	if !errors.As(err, &deferred) {
		t.Fatalf("takeover error = %v, want deferred remote-writer fence", err)
	}
	if hookCalled {
		t.Fatal("takeover reached a migration write boundary before fencing the remote writer")
	}
	for _, path := range []string{fixture.CanonicalPath, fixture.LegacyPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("writer-fence failure removed %s: %v", path, err)
		}
	}
	if _, err := os.Stat(fixture.BackupPath); !os.IsNotExist(err) {
		t.Fatalf("writer-fence failure created a legacy backup: %v", err)
	}
}

func TestTeamsRuntimeSafetyOfflineTakeoverDefersReusedLivePIDCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname: %v", err)
	}
	legacy, err := teamstore.Open(fixture.LegacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		state.ServiceOwner = &teamstore.OwnerMetadata{
			PID:           os.Getpid(),
			Hostname:      hostname,
			ScopeID:       fixture.Scope.ID,
			MachineID:     "old-owner-generation",
			StartedAt:     time.Now().Add(-24 * time.Hour),
			LastHeartbeat: time.Now().Add(-24 * time.Hour),
		}
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed reused PID owner: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	err = CompleteOfflineRuntimeStoreTakeover(context.Background(), fixture.Scope, fixture.LegacyPath)
	var deferred *RuntimeStoreTakeoverDeferredError
	if !errors.As(err, &deferred) {
		t.Fatalf("takeover error = %v, want reused-PID deferral", err)
	}
	if _, err := os.Stat(fixture.LegacyPath); err != nil {
		t.Fatalf("reused-PID deferral quarantined legacy store: %v", err)
	}
}

func TestTeamsRuntimeSafetyBackupConflictExitsBeforeAnyWriteCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	if err := os.MkdirAll(filepath.Dir(fixture.BackupPath), 0o700); err != nil {
		t.Fatalf("seed conflicting backup: %v", err)
	}
	before := snapshotRuntimeSafetyFiles(t, fixture.Root)
	err := CompleteOfflineRuntimeStoreTakeover(context.Background(), fixture.Scope, fixture.LegacyPath)
	if err == nil || !strings.Contains(err.Error(), "backup conflict") {
		t.Fatalf("takeover error = %v, want backup conflict", err)
	}
	after := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("backup conflict caused writes before failing: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyOutboundReplayFenceMarksMatchingCanonicalOutboxSentCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	canonical, err := teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("open canonical store: %v", err)
	}
	if _, _, err := canonical.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "shared-outbox",
		SessionID:   "shared-session",
		TeamsChatID: "canonical-chat",
		Kind:        "final",
		Body:        "already delivered by legacy",
	}); err != nil {
		_ = canonical.Close()
		t.Fatalf("queue canonical outbox: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}
	legacyRegistry, ok := registryPathForStoreMigrationSource(fixture.LegacyPath)
	if !ok {
		t.Fatal("legacy registry path unavailable")
	}
	legacyOutbound, ok := globalOutboundLedgerPathForRegistry(legacyRegistry)
	if !ok {
		t.Fatal("legacy outbound ledger path unavailable")
	}
	if err := recordGlobalOutbound(context.Background(), legacyOutbound, globalOutboundItem{
		ChatID:    "canonical-chat",
		MessageID: "teams-already-sent",
		ScopeID:   fixture.Scope.ID,
		OutboxID:  "shared-outbox",
		SessionID: "shared-session",
		Kind:      "final",
	}, time.Now()); err != nil {
		t.Fatalf("record legacy outbound fence: %v", err)
	}

	if err := CompleteOfflineRuntimeStoreTakeover(context.Background(), fixture.Scope, fixture.LegacyPath); err != nil {
		t.Fatalf("offline takeover: %v", err)
	}
	canonical, err = teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("reopen canonical store: %v", err)
	}
	state, err := canonical.Load(context.Background())
	if err != nil {
		_ = canonical.Close()
		t.Fatalf("load canonical store: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}
	outbox := state.OutboxMessages["shared-outbox"]
	if outbox.Status != teamstore.OutboxStatusSent || outbox.TeamsMessageID != "teams-already-sent" {
		t.Fatalf("canonical outbox was not replay-fenced as sent: %#v", outbox)
	}
}

func TestTeamsRuntimeSafetyOutboundReplayFenceRejectsIdentityConflictCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	canonical, err := teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("open canonical store: %v", err)
	}
	if _, _, err := canonical.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "conflicting-outbox",
		SessionID:   "shared-session",
		TeamsChatID: "canonical-chat",
		Kind:        "final",
		Body:        "canonical payload",
	}); err != nil {
		_ = canonical.Close()
		t.Fatalf("queue canonical outbox: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}
	legacyRegistry, _ := registryPathForStoreMigrationSource(fixture.LegacyPath)
	legacyOutbound, _ := globalOutboundLedgerPathForRegistry(legacyRegistry)
	if err := recordGlobalOutbound(context.Background(), legacyOutbound, globalOutboundItem{
		ChatID:    "different-chat",
		MessageID: "teams-conflicting-send",
		ScopeID:   fixture.Scope.ID,
		OutboxID:  "conflicting-outbox",
		SessionID: "shared-session",
		Kind:      "final",
	}, time.Now()); err != nil {
		t.Fatalf("record conflicting legacy fence: %v", err)
	}

	err = CompleteOfflineRuntimeStoreTakeover(context.Background(), fixture.Scope, fixture.LegacyPath)
	if err == nil || !strings.Contains(err.Error(), "identity conflicts") {
		t.Fatalf("takeover error = %v, want outbox identity conflict", err)
	}
	if _, err := os.Stat(fixture.LegacyPath); err != nil {
		t.Fatalf("identity conflict quarantined legacy store: %v", err)
	}
	canonical, err = teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("reopen canonical store: %v", err)
	}
	state, err := canonical.Load(context.Background())
	if err != nil {
		_ = canonical.Close()
		t.Fatalf("load canonical store: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}
	if got := state.OutboxMessages["conflicting-outbox"].Status; got != teamstore.OutboxStatusQueued {
		t.Fatalf("identity conflict changed canonical outbox status to %q", got)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverDoesNotInferReplaySuppressionFromStoreRowsCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	if _, err := exerciseRuntimeSafetyTakeoverCoordinator(fixture.Scope, fixture.LegacyPath); err != nil {
		t.Fatalf("automatic takeover: %v", err)
	}
	registryPath, err := DefaultRegistryPathForScope(fixture.Scope.ID)
	if err != nil {
		t.Fatalf("canonical registry path: %v", err)
	}
	inboundPath, ok := globalInboundLedgerPathForRegistry(registryPath)
	if !ok {
		t.Fatal("canonical inbound ledger path unavailable")
	}
	_, claimed, err := claimGlobalInbound(
		context.Background(),
		inboundPath,
		"legacy-only-chat",
		"teams-terminal",
		"canonical-listener",
		time.Now(),
	)
	if err != nil {
		t.Fatalf("claim terminal replay: %v", err)
	}
	if !claimed {
		t.Error("scope takeover incorrectly inferred a completed global inbound claim from an ignored store row")
	}
	_, claimed, err = claimGlobalInbound(
		context.Background(),
		inboundPath,
		"legacy-only-chat",
		"teams-nonterminal",
		"canonical-listener",
		time.Now(),
	)
	if err != nil {
		t.Fatalf("claim nonterminal replay: %v", err)
	}
	if !claimed {
		t.Error("nonterminal legacy inbound message was incorrectly marked processed")
	}

	outboundPath, ok := globalOutboundLedgerPathForRegistry(registryPath)
	if !ok {
		t.Fatal("canonical outbound ledger path unavailable")
	}
	hasSent, err := hasGlobalOutboundLedgerItem(context.Background(), outboundPath, "legacy-only-chat", "teams-sent-message")
	if err != nil {
		t.Fatalf("lookup sent replay fence: %v", err)
	}
	if hasSent {
		t.Error("scope takeover incorrectly synthesized a shared global outbound record from a legacy store row")
	}

	canonical, err := teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("open canonical after takeover: %v", err)
	}
	defer canonical.Close()
	state, err := canonical.Load(context.Background())
	if err != nil {
		t.Fatalf("load canonical replay state: %v", err)
	}
	if _, ok := state.OutboxMessages["legacy-queued"]; ok {
		t.Error("queued legacy outbox was imported into the canonical send queue")
	}
	if _, ok := state.OutboxMessages["legacy-sent"]; ok {
		t.Error("sent legacy outbox body was copied into the canonical store")
	}
}

func TestTeamsRuntimeSafetyScopeTakeoverDoesNotMoveSharedGlobalLedgersCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacyRegistry, ok := registryPathForStoreMigrationSource(fixture.LegacyPath)
	if !ok {
		t.Fatal("legacy registry path unavailable")
	}
	inboundPath, ok := globalInboundLedgerPathForRegistry(legacyRegistry)
	if !ok {
		t.Fatal("legacy global inbound ledger path unavailable")
	}
	outboundPath, ok := globalOutboundLedgerPathForRegistry(legacyRegistry)
	if !ok {
		t.Fatal("legacy global outbound ledger path unavailable")
	}
	if _, claimed, err := claimGlobalInbound(
		context.Background(),
		inboundPath,
		"shared-chat",
		"shared-message",
		"scope-a-owner",
		time.Now(),
	); err != nil {
		t.Fatalf("seed shared inbound ledger: %v", err)
	} else if !claimed {
		t.Fatal("shared inbound seed was not claimed")
	}
	if err := recordGlobalOutbound(
		context.Background(),
		outboundPath,
		globalOutboundItem{
			ChatID:     "shared-chat",
			MessageID:  "shared-sent-message",
			ScopeID:    "scope-b",
			MachineID:  "machine-b",
			OutboxID:   "scope-b-outbox",
			SessionID:  "scope-b-session",
			RecordedAt: time.Now(),
		},
		time.Now(),
	); err != nil {
		t.Fatalf("seed shared outbound ledger: %v", err)
	}
	if samePath(fixture.LegacyPath, inboundPath) || samePath(fixture.LegacyPath, outboundPath) {
		t.Fatalf("scope takeover source unexpectedly aliases a shared global ledger: %s", fixture.LegacyPath)
	}
	beforeInbound := snapshotRuntimeSafetyFiles(t, filepath.Dir(inboundPath))

	if _, err := exerciseRuntimeSafetyTakeoverCoordinator(fixture.Scope, fixture.LegacyPath); err != nil {
		t.Fatalf("automatic scope takeover: %v", err)
	}

	afterInbound := snapshotRuntimeSafetyFiles(t, filepath.Dir(inboundPath))
	if !reflect.DeepEqual(beforeInbound, afterInbound) {
		t.Fatalf("scope takeover modified or moved shared global ledgers: %v", runtimeSafetySnapshotChanges(beforeInbound, afterInbound))
	}
	if _, claimed, err := claimGlobalInbound(
		context.Background(),
		inboundPath,
		"shared-chat",
		"shared-message",
		"scope-b-owner",
		time.Now(),
	); err != nil {
		t.Fatalf("read shared inbound ledger after takeover: %v", err)
	} else if claimed {
		t.Fatal("scope takeover lost the shared inbound claim")
	}
	if found, err := hasGlobalOutboundLedgerItem(context.Background(), outboundPath, "shared-chat", "shared-sent-message"); err != nil {
		t.Fatalf("read shared outbound ledger after takeover: %v", err)
	} else if !found {
		t.Fatal("scope takeover lost the shared outbound record")
	}
}

func TestTeamsRuntimeSafetyGlobalLedgerUnionIsIdempotentWithoutWritesCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacyRegistry, ok := registryPathForStoreMigrationSource(fixture.LegacyPath)
	if !ok {
		t.Fatal("legacy registry path unavailable")
	}
	inboundPath, ok := globalInboundLedgerPathForRegistry(legacyRegistry)
	if !ok {
		t.Fatal("legacy inbound ledger path unavailable")
	}
	claim, claimed, err := claimGlobalInbound(
		context.Background(),
		inboundPath,
		"shared-chat",
		"completed-message",
		"legacy-listener",
		time.Now(),
	)
	if err != nil || !claimed {
		t.Fatalf("seed legacy inbound: claimed=%v err=%v", claimed, err)
	}
	if err := completeGlobalInbound(context.Background(), claim); err != nil {
		t.Fatalf("complete legacy inbound: %v", err)
	}
	if err := UnionLegacyGlobalLedgers(context.Background(), fixture.Scope, fixture.LegacyPath, fixture.CanonicalPath); err != nil {
		t.Fatalf("first global ledger union: %v", err)
	}
	before := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if err := UnionLegacyGlobalLedgers(context.Background(), fixture.Scope, fixture.LegacyPath, fixture.CanonicalPath); err != nil {
		t.Fatalf("idempotent global ledger union: %v", err)
	}
	after := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("idempotent global ledger union wrote files: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyGlobalLedgerUnionPromotesCanonicalInboundClaimMonotonicallyCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacyRegistry, ok := registryPathForStoreMigrationSource(fixture.LegacyPath)
	if !ok {
		t.Fatal("legacy registry path unavailable")
	}
	canonicalRegistry, err := DefaultRegistryPathForScope(fixture.Scope.ID)
	if err != nil {
		t.Fatalf("canonical registry path: %v", err)
	}
	legacyInbound, _ := globalInboundLedgerPathForRegistry(legacyRegistry)
	canonicalInbound, _ := globalInboundLedgerPathForRegistry(canonicalRegistry)
	legacyClaim, claimed, err := claimGlobalInbound(
		context.Background(), legacyInbound, "shared-chat", "same-inbound", "legacy-owner", time.Now(),
	)
	if err != nil || !claimed {
		t.Fatalf("seed legacy inbound: claimed=%v err=%v", claimed, err)
	}
	if err := completeGlobalInbound(context.Background(), legacyClaim); err != nil {
		t.Fatalf("complete legacy inbound: %v", err)
	}
	if _, claimed, err := claimGlobalInbound(
		context.Background(), canonicalInbound, "shared-chat", "same-inbound", "canonical-owner", time.Now(),
	); err != nil || !claimed {
		t.Fatalf("seed canonical inbound: claimed=%v err=%v", claimed, err)
	}

	legacyOutbound, _ := globalOutboundLedgerPathForRegistry(legacyRegistry)
	canonicalOutbound, _ := globalOutboundLedgerPathForRegistry(canonicalRegistry)
	if err := recordGlobalOutbound(context.Background(), legacyOutbound, globalOutboundItem{
		ChatID: "shared-chat", MessageID: "same-outbound", ScopeID: "legacy-scope", OutboxID: "legacy-outbox",
	}, time.Now()); err != nil {
		t.Fatalf("seed legacy outbound: %v", err)
	}
	if err := recordGlobalOutbound(context.Background(), canonicalOutbound, globalOutboundItem{
		ChatID: "shared-chat", MessageID: "same-outbound", ScopeID: fixture.Scope.ID, OutboxID: "canonical-outbox",
	}, time.Now()); err != nil {
		t.Fatalf("seed canonical outbound: %v", err)
	}

	if err := UnionLegacyGlobalLedgers(context.Background(), fixture.Scope, fixture.LegacyPath, fixture.CanonicalPath); err != nil {
		t.Fatalf("global ledger union: %v", err)
	}
	inboundLedger, err := readGlobalInboundLedger(canonicalInbound)
	if err != nil {
		t.Fatalf("read canonical inbound: %v", err)
	}
	inboundItem := inboundLedger.Items[globalInboundKey("shared-chat", "same-inbound")]
	if inboundItem.Status != "done" {
		t.Fatalf("canonical inbound claim was not monotonically promoted to done: %#v", inboundItem)
	}
	if _, claimed, err := claimGlobalInbound(
		context.Background(),
		canonicalInbound,
		"shared-chat",
		"same-inbound",
		"future-owner",
		time.Now().Add(globalInboundClaimTTL+time.Minute),
	); err != nil {
		t.Fatalf("claim promoted inbound after TTL: %v", err)
	} else if claimed {
		t.Fatal("promoted legacy done inbound became claimable after the old canonical claim TTL")
	}
	outboundItem, found, err := readGlobalOutboundLedgerItem(
		context.Background(), canonicalOutbound, "shared-chat", "same-outbound",
	)
	if err != nil {
		t.Fatalf("read canonical outbound: %v", err)
	}
	if !found || outboundItem.ScopeID != fixture.Scope.ID || outboundItem.OutboxID != "canonical-outbox" {
		t.Fatalf("canonical outbound was overwritten: found=%v item=%#v", found, outboundItem)
	}

	before := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if err := UnionLegacyGlobalLedgers(context.Background(), fixture.Scope, fixture.LegacyPath, fixture.CanonicalPath); err != nil {
		t.Fatalf("idempotent global ledger union: %v", err)
	}
	after := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("already-promoted replay fence caused a second write: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyGlobalLedgerReadOnlyProbeRefusesToCreateMissingSHMCI(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "ledger.sqlite")
	if err := os.WriteFile(path, []byte("sqlite-placeholder"), 0o600); err != nil {
		t.Fatalf("write SQLite placeholder: %v", err)
	}
	if err := os.WriteFile(path+"-wal", []byte("non-empty-wal"), 0o600); err != nil {
		t.Fatalf("write WAL placeholder: %v", err)
	}
	before := snapshotRuntimeSafetyFiles(t, tmp)
	db, err := openTeamsLedgerSQLiteReadOnly(context.Background(), path)
	if db != nil {
		_ = db.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "without creating SHM") {
		t.Fatalf("read-only ledger open error = %v, want missing-SHM refusal", err)
	}
	after := snapshotRuntimeSafetyFiles(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("read-only ledger open modified the SQLite family: %v", runtimeSafetySnapshotChanges(before, after))
	}
	if _, err := os.Lstat(path + "-shm"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only ledger open created SHM: %v", err)
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
	historicalTarget := target
	historicalTarget.ID = "scope-historical-target"
	targetLegacy, err := legacyDefaultStorePathForScope(historicalTarget.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope(target): %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, targetLegacy, historicalTarget, "target-control")

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

	plan, err := InspectRuntimeStoreForScope(context.Background(), target)
	if err != nil {
		t.Fatalf("cold migration discovery: %v", err)
	}
	if plan.Action != RuntimeStoreActionMigrateLegacy || plan.LegacyPath != targetLegacy {
		t.Fatalf("cold migration plan = %#v, want target legacy source", plan)
	}
	if opens > 105 {
		t.Errorf("cold discovery opened %d stores for 100 unrelated scopes; want bounded linear discovery", opens)
	}
	if fullLoads != 0 {
		t.Errorf("cold discovery performed %d full Store.Load calls; want metadata-only probes", fullLoads)
	}
}

func TestTeamsRuntimeSafetyDiscoveryFailsClosedAtCandidateLimitCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)

	root, err := appdirs.LegacyConfigPath("teams", "scopes")
	if err != nil {
		t.Fatalf("legacy scope root: %v", err)
	}
	for _, name := range []string{"scope-a", "scope-b", "scope-c"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			t.Fatalf("create candidate %s: %v", name, err)
		}
	}
	paths, err := runtimeStoreMigrationCandidatePaths(filepath.Join(tmp, "canonical", "state.json"), 2)
	if err == nil || !strings.Contains(err.Error(), "exceeded 2 scope candidates") {
		t.Fatalf("candidate limit result paths=%v err=%v, want fail-closed limit error", paths, err)
	}
}

func TestTeamsRuntimeSafetyHistoricalScopeMigrationSecondStartupUsesCanonicalOnlyCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	historical := current
	historical.ID = "scope-historical-id"
	historicalPath, err := legacyDefaultStorePathForScope(historical.ID)
	if err != nil {
		t.Fatalf("historical legacy path: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, historicalPath, historical, "legacy-control")

	plan, err := InspectRuntimeStoreForScope(context.Background(), current)
	if err != nil {
		t.Fatalf("inspect historical migration: %v", err)
	}
	if plan.Action != RuntimeStoreActionMigrateLegacy || plan.LegacyPath != historicalPath {
		t.Fatalf("historical migration plan = %#v, want source %q", plan, historicalPath)
	}
	if err := CompleteOfflineRuntimeStorePlan(context.Background(), plan); err != nil {
		t.Fatalf("complete historical migration: %v", err)
	}
	if _, err := os.Stat(historicalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("historical scoped source remained writable after migration: %v", err)
	}
	historicalBackup := runtimeSafetyMigrationBackupPath(historicalPath, current.ID)
	if _, err := os.Stat(historicalBackup); err != nil {
		t.Fatalf("historical scoped backup missing at %q: %v", historicalBackup, err)
	}
	resolved, canonicalPath, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("first canonical resolution after historical migration: %v", err)
	}
	wantCanonical, err := DefaultStorePathForScope(current.ID)
	if err != nil {
		t.Fatalf("current canonical path: %v", err)
	}
	if resolved.ID != current.ID || canonicalPath != wantCanonical {
		t.Fatalf("first migration resolved %#v at %q, want current canonical %q", resolved, canonicalPath, wantCanonical)
	}
	canonical, err := teamstore.Open(wantCanonical)
	if err != nil {
		t.Fatalf("open migrated canonical store: %v", err)
	}
	if recorded, err := canonical.RecordScope(context.Background(), current); err != nil {
		_ = canonical.Close()
		t.Fatalf("listener RecordScope after historical migration: %v", err)
	} else if recorded.ID != current.ID {
		_ = canonical.Close()
		t.Fatalf("listener RecordScope returned %#v, want current scope", recorded)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close migrated canonical store: %v", err)
	}

	before := snapshotRuntimeSafetyFiles(t, tmp)
	resolved, secondPath, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("second canonical startup: %v", err)
	}
	if resolved.ID != current.ID || secondPath != wantCanonical {
		t.Fatalf("second startup resolved %#v at %q, want current canonical %q", resolved, secondPath, wantCanonical)
	}
	prepared, err := PrepareRuntimeStoreForListener(context.Background(), current)
	if err != nil {
		t.Fatalf("second listener preparation: %v", err)
	}
	if !prepared.Resolved || prepared.CanonicalPath != wantCanonical {
		t.Fatalf("second listener preparation = %#v, want canonical", prepared)
	}
	after := snapshotRuntimeSafetyFiles(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("second canonical startup performed migration I/O: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyHistoricalScopeMigrationRecoversAfterCanonicalPublishCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	historical := current
	historical.ID = "scope-historical-crash"
	historicalPath, err := legacyDefaultStorePathForScope(historical.ID)
	if err != nil {
		t.Fatalf("historical legacy path: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, historicalPath, historical, "legacy-control")
	plan, err := InspectRuntimeStoreForScope(context.Background(), current)
	if err != nil || plan.Action != RuntimeStoreActionMigrateLegacy {
		t.Fatalf("historical plan = %#v err=%v", plan, err)
	}
	runtimeStoreMigrationTestHook = func(stage string) error {
		if stage == runtimeStoreMigrationStageCanonicalPublished {
			return errors.New("injected post-publish crash")
		}
		return nil
	}
	t.Cleanup(func() { runtimeStoreMigrationTestHook = nil })
	if err := CompleteOfflineRuntimeStorePlan(context.Background(), plan); err == nil {
		t.Fatal("historical migration unexpectedly survived injected crash")
	}
	runtimeStoreMigrationTestHook = nil

	exactLegacyPath, err := legacyDefaultStorePathForScope(current.ID)
	if err != nil {
		t.Fatalf("exact legacy path: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, exactLegacyPath, current, "exact-legacy-control")
	beforeConflict := snapshotRuntimeSafetyFiles(t, tmp)
	if _, err := InspectRuntimeStoreForScope(context.Background(), current); err == nil ||
		!strings.Contains(err.Error(), "multiple legacy Teams stores") {
		t.Fatalf("cleanup conflict error = %v, want both legacy sources reported", err)
	}
	afterConflict := snapshotRuntimeSafetyFiles(t, tmp)
	if !reflect.DeepEqual(beforeConflict, afterConflict) {
		t.Fatalf("cleanup conflict inspection wrote files: %v", runtimeSafetySnapshotChanges(beforeConflict, afterConflict))
	}
	if err := os.RemoveAll(filepath.Dir(exactLegacyPath)); err != nil {
		t.Fatalf("remove exact legacy conflict fixture: %v", err)
	}

	retry, err := InspectRuntimeStoreForScope(context.Background(), current)
	if err != nil {
		t.Fatalf("inspect historical cleanup retry: %v", err)
	}
	if retry.Action != RuntimeStoreActionQuarantineLegacy || retry.LegacyPath != historicalPath {
		t.Fatalf("historical cleanup retry = %#v, want exact historical source", retry)
	}
	if err := CompleteOfflineRuntimeStorePlan(context.Background(), retry); err != nil {
		t.Fatalf("complete historical cleanup retry: %v", err)
	}
	final, err := InspectRuntimeStoreForScope(context.Background(), current)
	if err != nil || final.Action != RuntimeStoreActionReady {
		t.Fatalf("final plan = %#v err=%v, want ready", final, err)
	}
	if _, err := os.Stat(historicalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("historical source remains after retry: %v", err)
	}
	if _, err := os.Stat(runtimeStoreMigrationCleanupPath(final.CanonicalPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("migration cleanup marker remains after retry: %v", err)
	}
}

func TestTeamsRuntimeSafetyMigrationStagingRecoversCopiedWALWithoutSHMCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	sourcePath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacy path: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, sourcePath, scope, "legacy-control")
	source, err := teamstore.Open(sourcePath)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer source.Close()
	if _, err := source.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("migrate source to SQLite: %v", err)
	}
	if err := source.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions["wal-session"] = teamstore.SessionContext{
			ID:          "wal-session",
			TeamsChatID: "chat-only-in-wal",
			Status:      teamstore.SessionStatusActive,
		}
		return nil
	}); err != nil {
		t.Fatalf("write source WAL: %v", err)
	}
	sourceDB := filepath.Join(filepath.Dir(sourcePath), teamstore.SQLiteFileName)
	if info, err := os.Stat(sourceDB + "-wal"); err != nil || info.Size() == 0 {
		t.Fatalf("source WAL fixture is empty: info=%v err=%v", info, err)
	}

	targetPath := filepath.Join(tmp, "staging", "state.json")
	if err := copyLegacyStoreToTarget(scope.ID, targetPath, sourcePath); err != nil {
		t.Fatalf("copy migration target: %v", err)
	}
	targetDB := filepath.Join(filepath.Dir(targetPath), teamstore.SQLiteFileName)
	if _, err := os.Stat(targetDB + "-wal"); err != nil {
		t.Fatalf("copied WAL missing: %v", err)
	}
	if _, err := os.Stat(targetDB + "-shm"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging copy unexpectedly included SHM: %v", err)
	}
	if err := rebindRuntimeStoreMigrationScope(context.Background(), targetPath, scope); err != nil {
		t.Fatalf("recover copied WAL during staging rebind: %v", err)
	}
	state, err := teamstore.LoadPathReadOnly(context.Background(), targetPath)
	if err != nil {
		t.Fatalf("load recovered staging store: %v", err)
	}
	if got := state.Sessions["wal-session"].TeamsChatID; got != "chat-only-in-wal" {
		t.Fatalf("recovered WAL session chat = %q", got)
	}
}

func TestTeamsRuntimeSafetyCanonicalResolverSyscallProbeCI(t *testing.T) {
	if os.Getenv("CXP_TEAMS_RESOLVER_IO_PROBE") != "1" {
		t.Skip("set CXP_TEAMS_RESOLVER_IO_PROBE=1 and run under strace")
	}
	rootFile := strings.TrimSpace(os.Getenv("CXP_TEAMS_RESOLVER_IO_ROOT_FILE"))
	if rootFile == "" {
		t.Fatal("CXP_TEAMS_RESOLVER_IO_ROOT_FILE is required")
	}
	tmp := strings.TrimSpace(os.Getenv("CXP_TEAMS_RESOLVER_IO_ROOT"))
	if tmp == "" {
		t.Fatal("CXP_TEAMS_RESOLVER_IO_ROOT is required")
	}
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatalf("create persistent resolver probe root: %v", err)
	}
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	path, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	// The probe runs twice against one persistent root. Reset only this exact
	// test-owned legacy scope before the canonical phases so the first
	// process's Bridge fixture cannot affect the second process's hot path.
	if err := os.RemoveAll(filepath.Dir(legacyPath)); err != nil {
		t.Fatalf("reset test-owned legacy scope: %v", err)
	}
	if os.Getenv("CXP_TEAMS_RESOLVER_IO_SEED") == "1" {
		openAndSeedRuntimeSafetyScopeStore(t, path, scope, "canonical-control")
	} else if _, err := os.Stat(path); err != nil {
		t.Fatalf("second-process canonical fixture is unavailable: %v", err)
	}
	if err := os.WriteFile(rootFile, []byte(tmp), 0o600); err != nil {
		t.Fatalf("write probe root: %v", err)
	}

	fmt.Fprintln(os.Stderr, "CXP_TEAMS_RESOLVER_IO_BEGIN")
	if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
		t.Fatalf("probe resolution: %v", err)
	} else if resolvedPath != path {
		t.Fatalf("probe resolution path = %q, want %q", resolvedPath, path)
	}
	fmt.Fprintln(os.Stderr, "CXP_TEAMS_RESOLVER_IO_END")
	fmt.Fprintln(os.Stderr, "CXP_TEAMS_LISTENER_PREP_IO_BEGIN")
	prepared, err := PrepareRuntimeStoreForListener(context.Background(), scope)
	if err != nil {
		t.Fatalf("probe listener preparation: %v", err)
	}
	if !prepared.Resolved || prepared.CanonicalPath != path {
		t.Fatalf("probe listener preparation = %#v, want resolved canonical %q", prepared, path)
	}
	fmt.Fprintln(os.Stderr, "CXP_TEAMS_LISTENER_PREP_IO_END")

	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/me" {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"teams-user-1","displayName":"User One","userPrincipalName":"same@example.test"}`)
	}))
	t.Cleanup(server.Close)
	graph := newTestGraphClient(&fakeGraphAuth{token: "access"}, server, nil)
	fmt.Fprintln(os.Stderr, "CXP_TEAMS_BRIDGE_PRESTORE_IO_BEGIN")
	bridge, err := newBridgeWithGraphClients(context.Background(), graph, graph, "", io.Discard)
	if bridge != nil {
		t.Fatal("Bridge construction returned a bridge while dual-store quarantine is required")
	}
	requireRuntimeStoreActionPlan(t, err, RuntimeStoreActionQuarantineLegacy, path, legacyPath)
	fmt.Fprintln(os.Stderr, "CXP_TEAMS_BRIDGE_PRESTORE_IO_END")
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
		if strings.HasSuffix(entry.Name(), ".lock") {
			// Coordination locks are intentionally allowed during a deferred
			// takeover and cannot be read while held on Windows. The invariant
			// here is that durable store/config/ledger data is unchanged.
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
