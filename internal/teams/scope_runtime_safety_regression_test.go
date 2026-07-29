package teams

import (
	"context"
	"os"
	"path/filepath"
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

func TestTeamsRuntimeSafetySuccessfulMigrationRemovesLegacyFromCandidateScanCI(t *testing.T) {
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
}
