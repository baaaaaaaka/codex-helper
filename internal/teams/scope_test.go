package teams

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/flock"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestScopeIdentitySeparatesTeamsUsersAndProfiles(t *testing.T) {
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	first := ScopeIdentityForUser(User{ID: "user-1", UserPrincipalName: "alice@example.test"})
	second := ScopeIdentityForUser(User{ID: "user-2", UserPrincipalName: "alice2@example.test"})
	if first.ID == "" || second.ID == "" || first.ID == second.ID {
		t.Fatalf("scope ids should be non-empty and account-specific: first=%#v second=%#v", first, second)
	}

	t.Setenv(envTeamsProfile, "work")
	third := ScopeIdentityForUser(User{ID: "user-1", UserPrincipalName: "alice@example.test"})
	if third.ID == first.ID {
		t.Fatalf("scope id should include helper profile: default=%s work=%s", first.ID, third.ID)
	}
}

func TestTeamsBackgroundKeepaliveScopeIdentityStableAcrossLocalEnvCI(t *testing.T) {
	user := User{ID: "teams-user-1", UserPrincipalName: "same@example.test"}
	t.Setenv(envTeamsProfile, "default")

	t.Setenv("USER", "alice")
	t.Setenv("LOGNAME", "")
	t.Setenv("USERNAME", "")
	t.Setenv("CODEX_HOME", "/home/alice/.codex")
	t.Setenv("CODEX_HELPER_CONFIG", "/home/alice/.config/codex-helper/config.json")
	alice := ScopeIdentityForUser(user)
	aliceStore, err := DefaultStorePathForScope(alice.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope alice error: %v", err)
	}

	t.Setenv("USER", "bob")
	t.Setenv("CODEX_HOME", "/mnt/shared/codex")
	t.Setenv("CODEX_HELPER_CONFIG", "/mnt/shared/codex-helper/config.json")
	bob := ScopeIdentityForUser(user)
	bobStore, err := DefaultStorePathForScope(bob.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope bob error: %v", err)
	}

	if alice.ID == "" || bob.ID == "" || alice.ID != bob.ID {
		t.Fatalf("same Teams account/profile under one config root should share scope IDs: alice=%#v bob=%#v", alice, bob)
	}
	if aliceStore != bobStore {
		t.Fatalf("same Teams account/profile under one config root should share state path: alice=%q bob=%q", aliceStore, bobStore)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathInheritsLegacyScopeCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	t.Setenv("CODEX_HOME", "/home/alice/.codex")
	t.Setenv("CODEX_HELPER_CONFIG", "/home/alice/.config/codex-helper/config.json")

	oldScope := teamstore.ScopeIdentity{
		ID:            "scope:legacy",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		OSUser:        "alice",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_legacy", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), oldScope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{
			ScopeID:     oldScope.ID,
			AccountID:   oldScope.AccountID,
			Profile:     oldScope.Profile,
			TeamsChatID: "legacy-control-chat",
		}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:blocking": {
				ID:          "outbox:blocking",
				TeamsChatID: "legacy-control-chat",
				Kind:        "final",
				Status:      teamstore.OutboxStatusQueued,
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	preexistingRegistryPath, err := DefaultRegistryPathForScope(oldScope.ID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope preexisting: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(preexistingRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir preexisting state scope dir: %v", err)
	}
	if err := os.WriteFile(preexistingRegistryPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write preexisting registry: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	if current.ID == oldScope.ID {
		t.Fatalf("test requires a legacy scope id distinct from current id")
	}
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	wantPath, err := DefaultStorePathForScope(oldScope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope legacy: %v", err)
	}
	if resolved.ID != oldScope.ID || path != wantPath {
		t.Fatalf("resolved scope/path = %#v %q, want migrated legacy scope id %q path %q", resolved, path, oldScope.ID, wantPath)
	}
	registryScope, registryPath, err := ResolveRegistryPathForScope(current)
	if err != nil {
		t.Fatalf("ResolveRegistryPathForScope error: %v", err)
	}
	wantRegistryPath, err := DefaultRegistryPathForScope(oldScope.ID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope legacy: %v", err)
	}
	if registryScope.ID != oldScope.ID || registryPath != wantRegistryPath {
		t.Fatalf("resolved registry = %#v %q, want %q", registryScope, registryPath, wantRegistryPath)
	}

	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("legacy state should remain in place after migration: %v", err)
	}
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("migrated state should exist: %v", err)
	}
}

func TestResolveStorePathForMaintenancePinsLiveLegacyStoreWithoutMigrating(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyStore, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if _, err := legacyStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("record legacy scope: %v", err)
	}
	owner, err := teamstore.CurrentOwner("v-test", "", "", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner: %v", err)
	}
	if _, err := legacyStore.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("record live legacy owner: %v", err)
	}
	if _, _, err := legacyStore.CreateSession(context.Background(), teamstore.SessionContext{ID: "s014", Status: teamstore.SessionStatusActive, TeamsChatID: "original-chat"}); err != nil {
		t.Fatalf("seed live legacy session: %v", err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	currentStore, err := teamstore.Open(currentPath)
	if err != nil {
		t.Fatalf("open current store: %v", err)
	}
	if _, err := currentStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("record current scope: %v", err)
	}
	if _, _, err := currentStore.CreateSession(context.Background(), teamstore.SessionContext{ID: "s014", Status: teamstore.SessionStatusActive, TeamsChatID: "replacement-chat"}); err != nil {
		t.Fatalf("seed retained current session: %v", err)
	}
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("retained-%02d", i)
		if _, _, err := currentStore.CreateSession(context.Background(), teamstore.SessionContext{ID: id, Status: teamstore.SessionStatusActive, TeamsChatID: "retained-chat-" + id}); err != nil {
			t.Fatalf("seed richer retained current session %s: %v", id, err)
		}
	}
	if err := currentStore.Close(); err != nil {
		t.Fatalf("close current store: %v", err)
	}

	resolved, path, err := ResolveStorePathForMaintenance(scope)
	if err != nil {
		t.Fatalf("ResolveStorePathForMaintenance: %v", err)
	}
	if resolved.ID != scope.ID || !samePath(path, legacyPath) {
		t.Fatalf("maintenance resolution = %#v %q, want live legacy %q", resolved, path, legacyPath)
	}
	retained, err := teamstore.Open(currentPath)
	if err != nil {
		t.Fatalf("reopen retained current store: %v", err)
	}
	state, err := retained.Load(context.Background())
	closeErr := retained.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("load retained current store: load=%v close=%v", err, closeErr)
	}
	if got := state.Sessions["s014"].TeamsChatID; got != "replacement-chat" {
		t.Fatalf("maintenance resolution migrated or rewrote retained store: got %q", got)
	}
}

func TestResolveStorePathForScopeKeepsMaintenanceAuthorityAfterOwnerClears(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyStore, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if _, err := legacyStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("record legacy scope: %v", err)
	}
	owner, err := teamstore.CurrentOwner("v-test", "", "", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner: %v", err)
	}
	if _, err := legacyStore.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("record live legacy owner: %v", err)
	}
	if _, _, err := legacyStore.CreateSession(context.Background(), teamstore.SessionContext{ID: "s014", Status: teamstore.SessionStatusActive, TeamsChatID: "authoritative-chat"}); err != nil {
		t.Fatalf("seed live legacy session: %v", err)
	}
	currentStore, err := teamstore.Open(currentPath)
	if err != nil {
		t.Fatalf("open current store: %v", err)
	}
	if _, err := currentStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("record current scope: %v", err)
	}
	if _, _, err := currentStore.CreateSession(context.Background(), teamstore.SessionContext{ID: "s014", Status: teamstore.SessionStatusActive, TeamsChatID: "stale-chat"}); err != nil {
		t.Fatalf("seed stale current session: %v", err)
	}
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("retained-%02d", i)
		if _, _, err := currentStore.CreateSession(context.Background(), teamstore.SessionContext{ID: id, Status: teamstore.SessionStatusActive, TeamsChatID: "retained-chat-" + id}); err != nil {
			t.Fatalf("seed richer current session %s: %v", id, err)
		}
	}
	if err := currentStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ServiceControl.LastDrainOperationID = "older-operation"
		state.ServiceControl.LastDrainOperationAt = time.Now().Add(-time.Hour)
		return nil
	}); err != nil {
		t.Fatalf("seed older maintenance authority: %v", err)
	}
	if err := currentStore.Close(); err != nil {
		t.Fatalf("close current store: %v", err)
	}
	if _, err := legacyStore.SetDrainingOperation(context.Background(), "chat recreate", "operation-authority"); err != nil {
		t.Fatalf("mark maintenance authority: %v", err)
	}
	if err := legacyStore.ClearOwner(context.Background()); err != nil {
		t.Fatalf("clear owner after drain: %v", err)
	}
	if _, err := legacyStore.ClearDrainOperation(context.Background(), "operation-authority"); err != nil {
		t.Fatalf("release maintenance fence: %v", err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}

	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope: %v", err)
	}
	resolvedStore, err := teamstore.Open(resolvedPath)
	if err != nil {
		t.Fatalf("open resolved store: %v", err)
	}
	state, loadErr := resolvedStore.Load(context.Background())
	closeErr := resolvedStore.Close()
	if loadErr != nil || closeErr != nil {
		t.Fatalf("load resolved store: load=%v close=%v", loadErr, closeErr)
	}
	if got := state.Sessions["s014"].TeamsChatID; got != "authoritative-chat" {
		t.Fatalf("normal restart selected stale store %q at %s", got, resolvedPath)
	}
	if state.ServiceControl.LastDrainOperationID != "operation-authority" || state.ServiceControl.LastDrainOperationAt.IsZero() {
		t.Fatalf("maintenance authority marker was not preserved: %#v", state.ServiceControl)
	}
}

func TestResolveStorePathForScopeDoesNotLetOldMaintenanceMarkerOverrideNewerStore(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyStore, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	if _, err := legacyStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("record legacy scope: %v", err)
	}
	if _, _, err := legacyStore.CreateSession(context.Background(), teamstore.SessionContext{ID: "s014", Status: teamstore.SessionStatusActive, TeamsChatID: "old-chat"}); err != nil {
		t.Fatalf("seed legacy session: %v", err)
	}
	if err := legacyStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ServiceControl.LastDrainOperationID = "old-maintenance"
		state.ServiceControl.LastDrainOperationAt = time.Now().Add(-2 * time.Hour)
		return nil
	}); err != nil {
		t.Fatalf("seed old maintenance marker: %v", err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}

	currentStore, err := teamstore.Open(currentPath)
	if err != nil {
		t.Fatalf("open current store: %v", err)
	}
	if _, err := currentStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("record current scope: %v", err)
	}
	if _, _, err := currentStore.CreateSession(context.Background(), teamstore.SessionContext{ID: "s014", Status: teamstore.SessionStatusActive, TeamsChatID: "new-chat"}); err != nil {
		t.Fatalf("seed current session: %v", err)
	}
	if err := currentStore.Close(); err != nil {
		t.Fatalf("close current store: %v", err)
	}

	_, resolvedPath, err := ResolveStorePathForScope(scope)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope: %v", err)
	}
	resolvedStore, err := teamstore.Open(resolvedPath)
	if err != nil {
		t.Fatalf("open resolved store: %v", err)
	}
	state, loadErr := resolvedStore.Load(context.Background())
	closeErr := resolvedStore.Close()
	if loadErr != nil || closeErr != nil {
		t.Fatalf("load resolved store: load=%v close=%v", loadErr, closeErr)
	}
	if got := state.Sessions["s014"].TeamsChatID; got != "new-chat" {
		t.Fatalf("old maintenance marker overrode newer store: got %q from %s", got, resolvedPath)
	}
}

func TestResolveStorePathForMaintenanceFreshOwnerBeatsStaleRicherStore(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	now := time.Now()
	staleOwner, err := teamstore.CurrentOwner("v-stale", "", "", now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CurrentOwner stale: %v", err)
	}
	staleOwner.LastHeartbeat = now.Add(-scopeStoreOwnerFreshAfter - time.Minute)
	legacyStore, err := teamstore.Open(legacyPath)
	if err != nil {
		t.Fatalf("open stale legacy store: %v", err)
	}
	if _, err := legacyStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("record legacy scope: %v", err)
	}
	if err := legacyStore.Update(context.Background(), func(state *teamstore.State) error {
		serviceOwner := staleOwner
		lockOwner := staleOwner
		state.ServiceOwner = &serviceOwner
		state.LockOwner = &lockOwner
		for i := 0; i < 40; i++ {
			id := fmt.Sprintf("legacy-%02d", i)
			state.Sessions[id] = teamstore.SessionContext{ID: id, Status: teamstore.SessionStatusActive, TeamsChatID: "legacy-chat-" + id}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale richer legacy store: %v", err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatalf("close stale legacy store: %v", err)
	}

	currentStore, err := teamstore.Open(currentPath)
	if err != nil {
		t.Fatalf("open fresh current store: %v", err)
	}
	if _, err := currentStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("record current scope: %v", err)
	}
	freshOwner, err := teamstore.CurrentOwner("v-fresh", "", "", now)
	if err != nil {
		t.Fatalf("CurrentOwner fresh: %v", err)
	}
	if _, err := currentStore.RecordOwnerHeartbeat(context.Background(), freshOwner, scopeStoreOwnerFreshAfter, now); err != nil {
		t.Fatalf("record fresh current owner: %v", err)
	}
	if _, _, err := currentStore.CreateSession(context.Background(), teamstore.SessionContext{ID: "s014", Status: teamstore.SessionStatusActive, TeamsChatID: "live-current-chat"}); err != nil {
		t.Fatalf("seed live current session: %v", err)
	}
	if err := currentStore.Close(); err != nil {
		t.Fatalf("close fresh current store: %v", err)
	}

	resolved, path, err := ResolveStorePathForMaintenance(scope)
	if err != nil {
		t.Fatalf("ResolveStorePathForMaintenance: %v", err)
	}
	if resolved.ID != scope.ID || !samePath(path, currentPath) {
		t.Fatalf("maintenance resolution = %#v %q, want fresh current %q", resolved, path, currentPath)
	}
}

func TestResolveStorePathForMaintenanceRejectsMultipleFreshAuthorities(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	for _, path := range []string{legacyPath, currentPath} {
		st, err := teamstore.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		if _, err := st.RecordScope(context.Background(), scope); err != nil {
			t.Fatalf("record scope %s: %v", path, err)
		}
		owner, err := teamstore.CurrentOwner("v-test", "", "", time.Now())
		if err != nil {
			t.Fatalf("CurrentOwner %s: %v", path, err)
		}
		if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, scopeStoreOwnerFreshAfter, time.Now()); err != nil {
			t.Fatalf("record owner %s: %v", path, err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close %s: %v", path, err)
		}
	}

	_, _, err = ResolveStorePathForMaintenance(scope)
	if err == nil || !strings.Contains(err.Error(), "multiple live Teams stores claim scope") || !strings.Contains(err.Error(), legacyPath) || !strings.Contains(err.Error(), currentPath) {
		t.Fatalf("multiple authority error = %v", err)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathUsesControlBindingWhenScopeMissingCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_without_recorded_scope", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.Scope = teamstore.ScopeIdentity{}
		state.ControlChat = teamstore.ControlChatBinding{
			ScopeID:     "scope:without-recorded-scope",
			AccountID:   "teams-user-1",
			Profile:     "default",
			TeamsChatID: "legacy-control-chat",
		}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:blocking": {
				ID:          "outbox:blocking",
				TeamsChatID: "legacy-control-chat",
				Kind:        "final",
				Status:      teamstore.OutboxStatusQueued,
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	wantPath, err := DefaultStorePathForScope(current.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope current: %v", err)
	}
	if path != wantPath {
		t.Fatalf("resolved path = %q, want migrated path %q", path, wantPath)
	}
	if resolved.ID != current.ID {
		t.Fatalf("resolved scope = %#v, want current canonical scope id %q for legacy state without recorded scope", resolved, current.ID)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("legacy state should remain in place after migration: %v", err)
	}
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("migrated state should exist: %v", err)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathCompletesPartialNewScopeCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := teamstore.ScopeIdentity{
		ID:            "scope:partial",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_partial", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:newer": {ID: "outbox:newer", TeamsChatID: "legacy-control", Status: teamstore.OutboxStatusQueued},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	oldRegistryPath, err := legacyDefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	if err := os.WriteFile(oldRegistryPath, []byte(`{"version":1,"control_chat_id":"legacy-control"}`), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}
	newPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir partial new scope: %v", err)
	}
	stale := `{"schema_version":5,"scope":{"id":"scope:partial","account_id":"teams-user-1","user_principal":"same@example.test","profile":"default"},"outbox_messages":{"outbox:stale":{"id":"outbox:stale","teams_chat_id":"legacy-control","status":"queued"}}}`
	if err := os.WriteFile(newPath, []byte(stale), 0o600); err != nil {
		t.Fatalf("write partial new state: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	if resolved.ID != scope.ID || path != newPath {
		t.Fatalf("resolved scope/path = %#v %q, want %q at %q", resolved, path, scope.ID, newPath)
	}
	newStore, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("Open resolved store: %v", err)
	}
	state, err := newStore.Load(context.Background())
	if closeErr := newStore.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("Load resolved store: %v", err)
	}
	if _, ok := state.OutboxMessages["outbox:newer"]; !ok {
		t.Fatalf("resolved store did not refresh stale partial state: %#v", state.OutboxMessages)
	}
	if _, ok := state.OutboxMessages["outbox:stale"]; ok {
		t.Fatalf("resolved store kept stale partial state: %#v", state.OutboxMessages)
	}
	wantRegistryPath, err := DefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope: %v", err)
	}
	assertTeamsFileContent(t, wantRegistryPath, `{"version":1,"control_chat_id":"legacy-control"}`)
}

func TestTeamsBackgroundKeepaliveResolveStorePathRefreshesLoadableNewScopeStateCI(t *testing.T) {
	for _, tc := range []struct {
		name      string
		legacyMod time.Time
		newMod    time.Time
	}{
		{name: "legacy-newer", legacyMod: time.Unix(200, 0), newMod: time.Unix(100, 0)},
		{name: "equal-mtime-divergent", legacyMod: time.Unix(200, 0), newMod: time.Unix(200, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
			t.Setenv("USER", "alice")
			t.Setenv(envTeamsProfile, "default")

			scope := teamstore.ScopeIdentity{
				ID:            "scope:freshness-" + tc.name,
				AccountID:     "teams-user-1",
				UserPrincipal: "same@example.test",
				Profile:       "default",
			}
			oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", safeScopePathPart(scope.ID), "state.json")
			oldStore, err := teamstore.Open(oldPath)
			if err != nil {
				t.Fatalf("Open legacy store: %v", err)
			}
			if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
				t.Fatalf("RecordScope legacy: %v", err)
			}
			if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
				state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
				state.OutboxMessages = map[string]teamstore.OutboxMessage{
					"outbox:fresh": {ID: "outbox:fresh", TeamsChatID: "legacy-control", Status: teamstore.OutboxStatusQueued},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed legacy store: %v", err)
			}
			if err := oldStore.Close(); err != nil {
				t.Fatalf("close legacy store: %v", err)
			}

			newPath, err := DefaultStorePathForScope(scope.ID)
			if err != nil {
				t.Fatalf("DefaultStorePathForScope: %v", err)
			}
			newStore, err := teamstore.Open(newPath)
			if err != nil {
				t.Fatalf("Open new store: %v", err)
			}
			if _, err := newStore.RecordScope(context.Background(), scope); err != nil {
				t.Fatalf("RecordScope new: %v", err)
			}
			if err := newStore.Update(context.Background(), func(state *teamstore.State) error {
				state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
				state.OutboxMessages = map[string]teamstore.OutboxMessage{
					"outbox:stale": {ID: "outbox:stale", TeamsChatID: "legacy-control", Status: teamstore.OutboxStatusQueued},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed new store: %v", err)
			}
			if err := newStore.Close(); err != nil {
				t.Fatalf("close new store: %v", err)
			}
			if err := os.Chtimes(oldPath, tc.legacyMod, tc.legacyMod); err != nil {
				t.Fatalf("chtimes legacy: %v", err)
			}
			if err := os.Chtimes(newPath, tc.newMod, tc.newMod); err != nil {
				t.Fatalf("chtimes new: %v", err)
			}

			current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
			resolved, path, err := ResolveStorePathForScope(current)
			if err != nil {
				t.Fatalf("ResolveStorePathForScope error: %v", err)
			}
			if resolved.ID != scope.ID || path != newPath {
				t.Fatalf("resolved scope/path = %#v %q, want %q at %q", resolved, path, scope.ID, newPath)
			}
			gotStore, err := teamstore.Open(path)
			if err != nil {
				t.Fatalf("Open resolved store: %v", err)
			}
			state, err := gotStore.Load(context.Background())
			if closeErr := gotStore.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
			if err != nil {
				t.Fatalf("Load resolved store: %v", err)
			}
			if _, ok := state.OutboxMessages["outbox:fresh"]; !ok {
				t.Fatalf("resolved store did not refresh from legacy: %#v", state.OutboxMessages)
			}
			if _, ok := state.OutboxMessages["outbox:stale"]; ok {
				t.Fatalf("resolved store kept stale new state: %#v", state.OutboxMessages)
			}
		})
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathFallsBackWhenPartialNewScopeCannotCompleteCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := teamstore.ScopeIdentity{
		ID:            "scope:locked",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_locked", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	oldRegistryPath, err := legacyDefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	if err := os.WriteFile(oldRegistryPath, []byte(`{"version":1,"control_chat_id":"legacy-control"}`), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}
	lock := flock.New(oldRegistryPath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock legacy registry: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	newPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir partial new scope: %v", err)
	}
	partial := `{"schema_version":5,"scope":{"id":"scope:locked","account_id":"teams-user-1","user_principal":"same@example.test","profile":"default"}}`
	if err := os.WriteFile(newPath, []byte(partial), 0o600); err != nil {
		t.Fatalf("write partial new state: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	if resolved.ID != scope.ID || path != oldPath {
		t.Fatalf("resolved scope/path = %#v %q, want legacy %q at %q", resolved, path, scope.ID, oldPath)
	}
	registryScope, registryPath, err := ResolveRegistryPathForScope(current)
	if err != nil {
		t.Fatalf("ResolveRegistryPathForScope error: %v", err)
	}
	if registryScope.ID != scope.ID || registryPath != oldRegistryPath {
		t.Fatalf("resolved registry = %#v %q, want legacy registry %q", registryScope, registryPath, oldRegistryPath)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathRejectsNonRegularSQLiteSidecarCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := teamstore.ScopeIdentity{
		ID:            "scope:sidecar-dir",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_sidecar-dir", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	oldSQLite := filepath.Join(filepath.Dir(oldPath), teamstore.SQLiteFileName)
	if err := os.WriteFile(oldSQLite, []byte("db"), 0o600); err != nil {
		t.Fatalf("write legacy sqlite: %v", err)
	}
	if err := os.WriteFile(oldSQLite+"-wal", []byte("wal"), 0o600); err != nil {
		t.Fatalf("write legacy wal: %v", err)
	}
	oldRegistryPath, err := legacyDefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	if err := os.WriteFile(oldRegistryPath, []byte(`{"version":1,"control_chat_id":"legacy-control"}`), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}

	newPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new scope: %v", err)
	}
	partial := `{"schema_version":5,"scope":{"id":"scope:sidecar-dir","account_id":"teams-user-1","user_principal":"same@example.test","profile":"default"}}`
	if err := os.WriteFile(newPath, []byte(partial), 0o600); err != nil {
		t.Fatalf("write partial new state: %v", err)
	}
	newSQLite := filepath.Join(filepath.Dir(newPath), teamstore.SQLiteFileName)
	if err := os.WriteFile(newSQLite, []byte("db"), 0o600); err != nil {
		t.Fatalf("write new sqlite: %v", err)
	}
	if err := os.MkdirAll(newSQLite+"-wal", 0o700); err != nil {
		t.Fatalf("mkdir bogus new wal directory: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	if resolved.ID != scope.ID || path != oldPath {
		t.Fatalf("resolved scope/path = %#v %q, want legacy fallback %q at %q", resolved, path, scope.ID, oldPath)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathRepairsCorruptNewScopeCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := teamstore.ScopeIdentity{
		ID:            "scope:corrupt-new",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_corrupt-new", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:corrupt-new": {ID: "outbox:corrupt-new", TeamsChatID: "legacy-control", Status: teamstore.OutboxStatusQueued},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	oldRegistryPath, err := legacyDefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	if err := os.WriteFile(oldRegistryPath, []byte(`{"version":1,"control_chat_id":"legacy-control"}`), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}
	newPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new scope: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"schema_version":`), 0o600); err != nil {
		t.Fatalf("write corrupt new state: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	if resolved.ID != scope.ID || path != newPath {
		t.Fatalf("resolved scope/path = %#v %q, want repaired new path %q at %q", resolved, path, scope.ID, newPath)
	}
	st, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("Open repaired store: %v", err)
	}
	state, err := st.Load(context.Background())
	if closeErr := st.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("Load repaired store: %v", err)
	}
	if _, ok := state.OutboxMessages["outbox:corrupt-new"]; !ok {
		t.Fatalf("repaired store lost legacy outbox: %#v", state.OutboxMessages)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathRepairsCorruptNewScopeWithoutRegistryCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := teamstore.ScopeIdentity{
		ID:            "scope:corrupt-no-registry",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_corrupt-no-registry", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:corrupt-no-registry": {ID: "outbox:corrupt-no-registry", TeamsChatID: "legacy-control", Status: teamstore.OutboxStatusQueued},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	newPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new scope: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"schema_version":`), 0o600); err != nil {
		t.Fatalf("write corrupt new state: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	if resolved.ID != scope.ID || path != newPath {
		t.Fatalf("resolved scope/path = %#v %q, want repaired new path %q at %q", resolved, path, scope.ID, newPath)
	}
	st, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("Open repaired store: %v", err)
	}
	state, err := st.Load(context.Background())
	if closeErr := st.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("Load repaired store: %v", err)
	}
	if _, ok := state.OutboxMessages["outbox:corrupt-no-registry"]; !ok {
		t.Fatalf("repaired store lost legacy outbox: %#v", state.OutboxMessages)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathConcurrentMigrationCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := teamstore.ScopeIdentity{
		ID:            "scope:stress",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_stress", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:stress": {ID: "outbox:stress", TeamsChatID: "legacy-control", Status: teamstore.OutboxStatusQueued},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	oldRegistryPath, err := legacyDefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	if err := os.WriteFile(oldRegistryPath, []byte(`{"version":1,"control_chat_id":"legacy-control"}`), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	const workers = 12
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := ResolveStorePathForScope(current)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ResolveStorePathForScope error: %v", err)
		}
	}

	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("final ResolveStorePathForScope error: %v", err)
	}
	wantPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if resolved.ID != scope.ID || path != wantPath {
		t.Fatalf("final resolved scope/path = %#v %q, want %q at %q", resolved, path, scope.ID, wantPath)
	}
	st, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("Open migrated store: %v", err)
	}
	state, err := st.Load(context.Background())
	if closeErr := st.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("Load migrated store: %v", err)
	}
	if _, ok := state.OutboxMessages["outbox:stress"]; !ok {
		t.Fatalf("migrated store lost stress outbox: %#v", state.OutboxMessages)
	}
	wantRegistryPath, err := DefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope: %v", err)
	}
	assertTeamsFileContent(t, wantRegistryPath, `{"version":1,"control_chat_id":"legacy-control"}`)
}

func TestTeamsBackgroundKeepaliveResolveStorePathSubprocessMigrationStressCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := teamstore.ScopeIdentity{
		ID:            "scope:process-stress",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_process-stress", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:process-stress": {ID: "outbox:process-stress", TeamsChatID: "legacy-control", Status: teamstore.OutboxStatusQueued},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	oldRegistryPath, err := legacyDefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	if err := os.WriteFile(oldRegistryPath, []byte(`{"version":1,"control_chat_id":"legacy-control"}`), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}

	const workers = 5
	cmds := make([]*exec.Cmd, 0, workers)
	for i := 0; i < workers; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestTeamsBackgroundKeepaliveResolveStorePathSubprocessMigrationWorkerCI$")
		cmd.Env = append(os.Environ(), "CODEX_HELPER_SCOPE_MIGRATION_WORKER=1")
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Start(); err != nil {
			t.Fatalf("start worker %d: %v", i, err)
		}
		cmds = append(cmds, cmd)
	}
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			var out string
			if buf, ok := cmd.Stdout.(*bytes.Buffer); ok {
				out = buf.String()
			}
			if buf, ok := cmd.Stderr.(*bytes.Buffer); ok && out == "" {
				out = buf.String()
			}
			t.Fatalf("worker %d failed: %v\n%s", i, err, out)
		}
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("final ResolveStorePathForScope error: %v", err)
	}
	wantPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if resolved.ID != scope.ID || path != wantPath {
		t.Fatalf("final resolved scope/path = %#v %q, want %q at %q", resolved, path, scope.ID, wantPath)
	}
	st, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("Open migrated store: %v", err)
	}
	state, err := st.Load(context.Background())
	if closeErr := st.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("Load migrated store: %v", err)
	}
	if _, ok := state.OutboxMessages["outbox:process-stress"]; !ok {
		t.Fatalf("migrated store lost process stress outbox: %#v", state.OutboxMessages)
	}
	wantRegistryPath, err := DefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope: %v", err)
	}
	assertTeamsFileContent(t, wantRegistryPath, `{"version":1,"control_chat_id":"legacy-control"}`)
}

func TestTeamsBackgroundKeepaliveResolveStorePathSubprocessMigrationWorkerCI(t *testing.T) {
	if os.Getenv("CODEX_HELPER_SCOPE_MIGRATION_WORKER") != "1" {
		t.Skip("subprocess worker only")
	}
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	if resolved.ID != "scope:process-stress" {
		t.Fatalf("resolved scope = %#v, want scope:process-stress", resolved)
	}
	if !strings.Contains(filepath.ToSlash(path), "scope_process-stress/state.json") {
		t.Fatalf("resolved path = %q, want migrated process-stress scope path", path)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathMigratesLegacyGlobalStoreCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, cacheBase := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	oldPath := filepath.Join(configBase, "codex-helper", "teams", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy global store: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{
			TeamsChatID: "legacy-control-chat",
		}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:blocking": {
				ID:          "outbox:blocking",
				TeamsChatID: "legacy-control-chat",
				Kind:        "final",
				Status:      teamstore.OutboxStatusQueued,
			},
		}
		state.ServiceControl = teamstore.ServiceControl{Paused: true, Reason: "legacy maintenance"}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy global store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy global store: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "db",
		"-wal": "wal",
		"-shm": "shm",
	} {
		if err := os.WriteFile(filepath.Join(filepath.Dir(oldPath), teamstore.SQLiteFileName)+suffix, []byte(body), 0o600); err != nil {
			t.Fatalf("write legacy store sqlite%s: %v", suffix, err)
		}
	}
	oldRegistryPath := filepath.Join(cacheBase, "codex-helper", "teams-registry.json")
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy global registry: %v", err)
	}
	oldRegistry := `{"version":1,"user_id":"teams-user-1","user_principal":"same@example.test","control_chat_id":"legacy-control-chat"}`
	if err := os.WriteFile(oldRegistryPath, []byte(oldRegistry), 0o600); err != nil {
		t.Fatalf("write legacy global registry: %v", err)
	}
	oldInboundPath, oldInboundOK := globalInboundLedgerPathForRegistry(oldRegistryPath)
	oldOutboundPath, oldOutboundOK := globalOutboundLedgerPathForRegistry(oldRegistryPath)
	if !oldInboundOK || !oldOutboundOK {
		t.Fatal("legacy global ledger paths should resolve")
	}
	if err := os.WriteFile(oldInboundPath, []byte(`{"version":1,"items":{"inbound-1":{"chat_id":"legacy-control-chat","message_id":"m1","status":"done"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy inbound ledger: %v", err)
	}
	if err := os.WriteFile(oldOutboundPath, []byte(`{"version":1,"items":{"outbound-1":{"chat_id":"legacy-control-chat","message_id":"sent-1"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy outbound ledger: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "ledger-db",
		"-wal": "ledger-wal",
		"-shm": "ledger-shm",
	} {
		if err := os.WriteFile(teamsLedgerSQLitePath(oldInboundPath)+suffix, []byte("inbound-"+body), 0o600); err != nil {
			t.Fatalf("write legacy inbound sqlite%s: %v", suffix, err)
		}
		if err := os.WriteFile(teamsLedgerSQLitePath(oldOutboundPath)+suffix, []byte("outbound-"+body), 0o600); err != nil {
			t.Fatalf("write legacy outbound sqlite%s: %v", suffix, err)
		}
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	wantPath, err := DefaultStorePathForScope(current.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope current: %v", err)
	}
	if resolved.ID != current.ID || path != wantPath {
		t.Fatalf("resolved scope/path = %#v %q, want current scope path %q", resolved, path, wantPath)
	}
	newStore, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("Open migrated store: %v", err)
	}
	state, err := newStore.Load(context.Background())
	if closeErr := newStore.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("Load migrated store: %v", err)
	}
	if state.ControlChat.TeamsChatID != "legacy-control-chat" || state.OutboxMessages["outbox:blocking"].Status != teamstore.OutboxStatusQueued {
		t.Fatalf("migrated state missing legacy work: %#v", state)
	}
	if !state.ServiceControl.Paused || state.ServiceControl.Reason != "legacy maintenance" {
		t.Fatalf("migrated service control = %#v, want legacy paused state", state.ServiceControl)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("legacy global state should remain: %v", err)
	}
	for suffix, body := range map[string]string{"": "db", "-wal": "wal", "-shm": "shm"} {
		assertTeamsFileContent(t, filepath.Join(filepath.Dir(path), teamstore.SQLiteFileName)+suffix, body)
	}

	registryScope, registryPath, err := ResolveRegistryPathForScope(current)
	if err != nil {
		t.Fatalf("ResolveRegistryPathForScope error: %v", err)
	}
	wantRegistryPath, err := DefaultRegistryPathForScope(current.ID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope current: %v", err)
	}
	if registryScope.ID != current.ID || registryPath != wantRegistryPath {
		t.Fatalf("resolved registry = %#v %q, want current scope registry %q", registryScope, registryPath, wantRegistryPath)
	}
	assertTeamsFileContent(t, registryPath, oldRegistry)
	newInboundPath, _ := globalInboundLedgerPathForRegistry(registryPath)
	newOutboundPath, _ := globalOutboundLedgerPathForRegistry(registryPath)
	assertTeamsFileContent(t, newInboundPath, `{"version":1,"items":{"inbound-1":{"chat_id":"legacy-control-chat","message_id":"m1","status":"done"}}}`)
	assertTeamsFileContent(t, newOutboundPath, `{"version":1,"items":{"outbound-1":{"chat_id":"legacy-control-chat","message_id":"sent-1"}}}`)
	for suffix, body := range map[string]string{
		"":     "ledger-db",
		"-wal": "ledger-wal",
		"-shm": "ledger-shm",
	} {
		assertTeamsFileContent(t, teamsLedgerSQLitePath(newInboundPath)+suffix, "inbound-"+body)
		assertTeamsFileContent(t, teamsLedgerSQLitePath(newOutboundPath)+suffix, "outbound-"+body)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathCleansEligibleLegacyScopeFilesCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	withScopeMigrationCleanupForTest(t, time.Unix(10_000, 0), 0, nil)

	scope := teamstore.ScopeIdentity{
		ID:            "scope:cleanup",
		AccountID:     "teams-user-1",
		UserPrincipal: "same@example.test",
		Profile:       "default",
	}
	oldPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_cleanup", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := oldStore.RecordScope(context.Background(), scope); err != nil {
		t.Fatalf("RecordScope legacy: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "legacy-control"}
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:cleanup": {ID: "outbox:cleanup", TeamsChatID: "legacy-control", Status: teamstore.OutboxStatusQueued},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}
	oldSQLite := filepath.Join(filepath.Dir(oldPath), teamstore.SQLiteFileName)
	for suffix, body := range map[string]string{
		"":     "db",
		"-wal": "wal",
		"-shm": "shm",
	} {
		if err := os.WriteFile(oldSQLite+suffix, []byte(body), 0o600); err != nil {
			t.Fatalf("write legacy sqlite%s: %v", suffix, err)
		}
	}
	for name, body := range map[string]string{
		"helper-restart-pending.json": `{"pending":true}`,
		"workflow-notifications.json": `{"version":1}`,
		"workflow-webhook-url":        "https://example.test/hook",
	} {
		if err := os.WriteFile(filepath.Join(filepath.Dir(oldPath), name), []byte(body), 0o600); err != nil {
			t.Fatalf("write legacy sidecar %s: %v", name, err)
		}
	}
	oldRegistryPath, err := legacyDefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	oldRegistry := `{"version":1,"control_chat_id":"legacy-control"}`
	if err := os.WriteFile(oldRegistryPath, []byte(oldRegistry), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}
	oldInboundPath, oldInboundOK := globalInboundLedgerPathForRegistry(oldRegistryPath)
	oldOutboundPath, oldOutboundOK := globalOutboundLedgerPathForRegistry(oldRegistryPath)
	if !oldInboundOK || !oldOutboundOK {
		t.Fatal("legacy ledger paths should resolve")
	}
	for _, path := range []string{oldInboundPath, oldOutboundPath} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir legacy ledger parent: %v", err)
		}
	}
	if err := os.WriteFile(oldInboundPath, []byte(`{"version":1,"items":{"inbound-1":{"chat_id":"legacy-control","message_id":"m1"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy inbound ledger: %v", err)
	}
	if err := os.WriteFile(oldOutboundPath, []byte(`{"version":1,"items":{"outbound-1":{"chat_id":"legacy-control","message_id":"sent-1"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy outbound ledger: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "ledger-db",
		"-wal": "ledger-wal",
		"-shm": "ledger-shm",
	} {
		if err := os.WriteFile(teamsLedgerSQLitePath(oldInboundPath)+suffix, []byte("inbound-"+body), 0o600); err != nil {
			t.Fatalf("write legacy inbound sqlite%s: %v", suffix, err)
		}
		if err := os.WriteFile(teamsLedgerSQLitePath(oldOutboundPath)+suffix, []byte("outbound-"+body), 0o600); err != nil {
			t.Fatalf("write legacy outbound sqlite%s: %v", suffix, err)
		}
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	wantPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	if resolved.ID != scope.ID || path != wantPath {
		t.Fatalf("resolved scope/path = %#v %q, want %q at %q", resolved, path, scope.ID, wantPath)
	}
	newStore, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("Open migrated store: %v", err)
	}
	state, err := newStore.Load(context.Background())
	if closeErr := newStore.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("Load migrated store: %v", err)
	}
	if _, ok := state.OutboxMessages["outbox:cleanup"]; !ok {
		t.Fatalf("migrated store lost cleanup outbox: %#v", state.OutboxMessages)
	}
	assertTeamsPathMissing(t, oldPath)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		assertTeamsPathMissing(t, oldSQLite+suffix)
		assertTeamsFileContent(t, filepath.Join(filepath.Dir(path), teamstore.SQLiteFileName)+suffix, map[string]string{"": "db", "-wal": "wal", "-shm": "shm"}[suffix])
	}
	for _, name := range storeSidecarNames() {
		assertTeamsPathMissing(t, filepath.Join(filepath.Dir(oldPath), name))
	}
	assertTeamsPathMissing(t, oldRegistryPath)
	assertTeamsPathMissing(t, oldInboundPath)
	assertTeamsPathMissing(t, oldOutboundPath)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		assertTeamsPathMissing(t, teamsLedgerSQLitePath(oldInboundPath)+suffix)
		assertTeamsPathMissing(t, teamsLedgerSQLitePath(oldOutboundPath)+suffix)
	}
	newRegistryPath, err := DefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope: %v", err)
	}
	assertTeamsFileContent(t, newRegistryPath, oldRegistry)
	newInboundPath, _ := globalInboundLedgerPathForRegistry(newRegistryPath)
	newOutboundPath, _ := globalOutboundLedgerPathForRegistry(newRegistryPath)
	assertTeamsFileContent(t, newInboundPath, `{"version":1,"items":{"inbound-1":{"chat_id":"legacy-control","message_id":"m1"}}}`)
	assertTeamsFileContent(t, newOutboundPath, `{"version":1,"items":{"outbound-1":{"chat_id":"legacy-control","message_id":"sent-1"}}}`)
	for suffix, body := range map[string]string{
		"":     "ledger-db",
		"-wal": "ledger-wal",
		"-shm": "ledger-shm",
	} {
		assertTeamsFileContent(t, teamsLedgerSQLitePath(newInboundPath)+suffix, "inbound-"+body)
		assertTeamsFileContent(t, teamsLedgerSQLitePath(newOutboundPath)+suffix, "outbound-"+body)
	}
}

func TestCleanupMigratedFileFamilySkipsFreshLegacyCI(t *testing.T) {
	tmp := t.TempDir()
	newPath := filepath.Join(tmp, "state", "registry.json")
	oldPath := filepath.Join(tmp, "cache", "registry.json")
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o700); err != nil {
		t.Fatalf("mkdir old: %v", err)
	}
	for _, path := range []string{newPath, oldPath} {
		if err := os.WriteFile(path, []byte(`{"version":1}`), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	now := time.Unix(10_000, 0)
	withScopeMigrationCleanupForTest(t, now, 24*time.Hour, nil)
	for _, path := range []string{newPath, oldPath} {
		if err := os.Chtimes(path, now, now); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	cleanupMigratedFileFamiliesIfSafe("", migratedCleanupFamily{newBase: newPath, oldBase: oldPath})
	assertTeamsFileContent(t, oldPath, `{"version":1}`)
}

func TestCleanupMigratedFileFamilySkipsNewerLegacyCI(t *testing.T) {
	tmp := t.TempDir()
	newPath := filepath.Join(tmp, "state", "registry.json")
	oldPath := filepath.Join(tmp, "cache", "registry.json")
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o700); err != nil {
		t.Fatalf("mkdir old: %v", err)
	}
	for _, path := range []string{newPath, oldPath} {
		if err := os.WriteFile(path, []byte(`{"version":1}`), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	now := time.Unix(10_000, 0)
	withScopeMigrationCleanupForTest(t, now, 0, nil)
	if err := os.Chtimes(newPath, now, now); err != nil {
		t.Fatalf("chtimes new: %v", err)
	}
	if err := os.Chtimes(oldPath, now.Add(time.Second), now.Add(time.Second)); err != nil {
		t.Fatalf("chtimes old: %v", err)
	}

	cleanupMigratedFileFamiliesIfSafe("", migratedCleanupFamily{newBase: newPath, oldBase: oldPath})
	assertTeamsFileContent(t, oldPath, `{"version":1}`)
}

func TestCleanupMigratedFileFamilySkipsLockedLegacyCI(t *testing.T) {
	tmp := t.TempDir()
	newPath := filepath.Join(tmp, "state", "registry.json")
	oldPath := filepath.Join(tmp, "cache", "registry.json")
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o700); err != nil {
		t.Fatalf("mkdir old: %v", err)
	}
	for _, path := range []string{newPath, oldPath} {
		if err := os.WriteFile(path, []byte(`{"version":1}`), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	now := time.Unix(10_000, 0)
	withScopeMigrationCleanupForTest(t, now, 0, nil)
	for _, path := range []string{newPath, oldPath} {
		if err := os.Chtimes(path, now, now); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}
	lock := flock.New(oldPath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock legacy file: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	cleanupMigratedFileFamiliesIfSafe(oldPath+".lock", migratedCleanupFamily{newBase: newPath, oldBase: oldPath})
	assertTeamsFileContent(t, oldPath, `{"version":1}`)
}

func TestCleanupMigratedFileFamilySkipsSameFileAliasCI(t *testing.T) {
	tmp := t.TempDir()
	oldDir := filepath.Join(tmp, "legacy")
	aliasDir := filepath.Join(tmp, "state-link")
	if err := os.MkdirAll(oldDir, 0o700); err != nil {
		t.Fatalf("mkdir old dir: %v", err)
	}
	if err := os.Symlink(oldDir, aliasDir); err != nil {
		t.Skipf("symlink aliases are not available: %v", err)
	}
	oldPath := filepath.Join(oldDir, "registry.json")
	newPath := filepath.Join(aliasDir, "registry.json")
	if err := os.WriteFile(oldPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write aliased file: %v", err)
	}
	now := time.Unix(10_000, 0)
	withScopeMigrationCleanupForTest(t, now, 0, nil)
	if err := os.Chtimes(oldPath, now, now); err != nil {
		t.Fatalf("chtimes aliased file: %v", err)
	}

	cleanupMigratedFileFamiliesIfSafe("", migratedCleanupFamily{newBase: newPath, oldBase: oldPath})
	assertTeamsFileContent(t, oldPath, `{"version":1}`)
	assertTeamsFileContent(t, newPath, `{"version":1}`)
}

func TestCleanupMigratedScopeLegacyFilesKeepsStoreWhenRegistryCleanupBlockedCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsScopeUserDirsForTest(t, tmp)
	scopeID := "scope:cleanup-blocked"
	oldStorePath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope_cleanup-blocked", "state.json")
	newStorePath, err := DefaultStorePathForScope(scopeID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	for _, path := range []string{oldStorePath, newStorePath} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir store parent %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(`{"schema_version":5}`), 0o600); err != nil {
			t.Fatalf("write store %s: %v", path, err)
		}
	}
	oldRegistryPath, err := legacyDefaultRegistryPathForScope(scopeID)
	if err != nil {
		t.Fatalf("legacyDefaultRegistryPathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy registry: %v", err)
	}
	if err := os.WriteFile(oldRegistryPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write legacy registry: %v", err)
	}
	newRegistryPath, err := DefaultRegistryPathForScope(scopeID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope: %v", err)
	}
	assertTeamsFileContent(t, newRegistryPath, `{"version":1}`)
	now := time.Unix(10_000, 0)
	withScopeMigrationCleanupForTest(t, now, 0, func(path string) error {
		if path == oldRegistryPath {
			return errors.New("simulated registry cleanup failure")
		}
		return os.Remove(path)
	})
	for _, path := range []string{oldStorePath, newStorePath, oldRegistryPath, newRegistryPath} {
		if err := os.Chtimes(path, now, now); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	cleanupMigratedScopeLegacyFiles(scopeID, newStorePath, oldStorePath, false)
	assertTeamsFileContent(t, oldStorePath, `{"schema_version":5}`)
	assertTeamsFileContent(t, oldRegistryPath, `{"version":1}`)
}

func TestCleanupMigratedFileFamilyRemoveFailureIsRetryableCI(t *testing.T) {
	tmp := t.TempDir()
	newBase := filepath.Join(tmp, "state", "store.sqlite")
	oldBase := filepath.Join(tmp, "cache", "store.sqlite")
	if err := os.MkdirAll(filepath.Dir(newBase), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldBase), 0o700); err != nil {
		t.Fatalf("mkdir old: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "db",
		"-wal": "wal",
		"-shm": "shm",
	} {
		for _, base := range []string{newBase, oldBase} {
			if err := os.WriteFile(base+suffix, []byte(body), 0o600); err != nil {
				t.Fatalf("write %s%s: %v", base, suffix, err)
			}
		}
	}
	now := time.Unix(10_000, 0)
	oldTime := now.Add(-48 * time.Hour)
	for _, base := range []string{newBase, oldBase} {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Chtimes(base+suffix, oldTime, oldTime); err != nil {
				t.Fatalf("chtimes %s%s: %v", base, suffix, err)
			}
		}
	}
	failPath := oldBase + "-wal"
	withScopeMigrationCleanupForTest(t, now, 24*time.Hour, func(path string) error {
		if path == failPath {
			return errors.New("simulated unlink failure")
		}
		return os.Remove(path)
	})

	if ok := cleanupMigratedFileFamiliesIfSafe("", migratedCleanupFamily{newBase: newBase, oldBase: oldBase, suffixes: []string{"-wal", "-shm"}}); ok {
		t.Fatal("cleanup should report failure when one file cannot be removed")
	}
	assertTeamsFileContent(t, failPath, "wal")
	if _, err := os.Stat(filepath.Dir(oldBase)); err != nil {
		t.Fatalf("old parent should remain while failed file remains: %v", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		assertTeamsFileContent(t, newBase+suffix, map[string]string{"": "db", "-wal": "wal", "-shm": "shm"}[suffix])
	}

	withScopeMigrationCleanupForTest(t, now, 24*time.Hour, nil)
	if ok := cleanupMigratedFileFamiliesIfSafe("", migratedCleanupFamily{newBase: newBase, oldBase: oldBase, suffixes: []string{"-wal", "-shm"}}); !ok {
		t.Fatal("retry cleanup should succeed")
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		assertTeamsPathMissing(t, oldBase+suffix)
		assertTeamsFileContent(t, newBase+suffix, map[string]string{"": "db", "-wal": "wal", "-shm": "shm"}[suffix])
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathSkipsOtherUserLegacyGlobalStoreCI(t *testing.T) {
	tmp := t.TempDir()
	configBase, cacheBase := isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	oldPath := filepath.Join(configBase, "codex-helper", "teams", "state.json")
	oldStore, err := teamstore.Open(oldPath)
	if err != nil {
		t.Fatalf("Open legacy global store: %v", err)
	}
	if err := oldStore.Update(context.Background(), func(state *teamstore.State) error {
		state.OutboxMessages = map[string]teamstore.OutboxMessage{
			"outbox:other": {ID: "outbox:other", TeamsChatID: "other-control", Status: teamstore.OutboxStatusQueued},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy global store: %v", err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("close legacy global store: %v", err)
	}
	oldRegistryPath := filepath.Join(cacheBase, "codex-helper", "teams-registry.json")
	if err := os.MkdirAll(filepath.Dir(oldRegistryPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy global registry: %v", err)
	}
	if err := os.WriteFile(oldRegistryPath, []byte(`{"version":1,"user_id":"other-user","user_principal":"other@example.test"}`), 0o600); err != nil {
		t.Fatalf("write legacy global registry: %v", err)
	}

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	wantPath, err := DefaultStorePathForScope(current.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope current: %v", err)
	}
	if resolved.ID != current.ID || path != wantPath {
		t.Fatalf("resolved scope/path = %#v %q, want empty current scope path %q", resolved, path, wantPath)
	}
	if _, err := os.Stat(wantPath); !os.IsNotExist(err) {
		t.Fatalf("other-user legacy global store should not be migrated, stat err = %v", err)
	}
}

func withScopeMigrationCleanupForTest(t *testing.T, now time.Time, grace time.Duration, remove func(string) error) {
	t.Helper()
	prevGrace := scopeMigrationLegacyCleanupGrace
	prevNow := scopeMigrationCleanupNow
	prevRemove := scopeMigrationCleanupRemove
	scopeMigrationLegacyCleanupGrace = grace
	scopeMigrationCleanupNow = func() time.Time { return now }
	if remove == nil {
		scopeMigrationCleanupRemove = os.Remove
	} else {
		scopeMigrationCleanupRemove = remove
	}
	t.Cleanup(func() {
		scopeMigrationLegacyCleanupGrace = prevGrace
		scopeMigrationCleanupNow = prevNow
		scopeMigrationCleanupRemove = prevRemove
	})
}

func assertTeamsPathMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("%s should be missing, stat err = %v", path, err)
	}
}

func isolateTeamsScopeUserDirsForTest(t *testing.T, tmp string) (string, string) {
	t.Helper()
	home := filepath.Join(tmp, "home")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("APPDATA", filepath.Join(tmp, "AppData", "Roaming"))
	t.Setenv("LOCALAPPDATA", filepath.Join(tmp, "AppData", "Local"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "state"))
	t.Setenv("CODEX_HELPER_STATE_DIR", "")
	configBase, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("UserConfigDir error: %v", err)
	}
	cacheBase, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("UserCacheDir error: %v", err)
	}
	return configBase, cacheBase
}

func TestMachineRecordHonorsPrimaryEphemeralEnv(t *testing.T) {
	scope := teamstore.ScopeIdentity{ID: "scope-1", Profile: "default"}
	t.Setenv(envTeamsMachineKind, "ephemeral")
	t.Setenv(envTeamsMachineLabel, "temp-node")
	t.Setenv(envTeamsMachineID, "machine-explicit")
	machine := MachineRecordForUser(User{ID: "user-1", UserPrincipalName: "user@example.test"}, scope)
	if machine.ID != "machine-explicit" || machine.Kind != teamstore.MachineKindEphemeral || machine.Priority != teamstore.DefaultMachinePriority(teamstore.MachineKindEphemeral) {
		t.Fatalf("unexpected ephemeral machine record: %#v", machine)
	}
	if machine.Label != "temp-node" {
		t.Fatalf("machine label = %q, want temp-node", machine.Label)
	}

	t.Setenv(envTeamsMachineKind, "primary")
	t.Setenv(envTeamsMachinePriority, "250")
	t.Setenv(envTeamsMachineID, "")
	primary := MachineRecordForUser(User{ID: "user-1", UserPrincipalName: "user@example.test"}, scope)
	if primary.Kind != teamstore.MachineKindPrimary || primary.Priority != 250 {
		t.Fatalf("unexpected primary machine record: %#v", primary)
	}
	if !strings.HasPrefix(primary.ID, "machine:") {
		t.Fatalf("generated machine id = %q, want machine prefix", primary.ID)
	}
}
