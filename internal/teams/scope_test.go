package teams

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
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
	oldPath := filepath.Join(tmp, "config", "codex-helper", "teams", "scopes", "scope_legacy", "state.json")
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

	current := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	if current.ID == oldScope.ID {
		t.Fatalf("test requires a legacy scope id distinct from current id")
	}
	resolved, path, err := ResolveStorePathForScope(current)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope error: %v", err)
	}
	if resolved.ID != oldScope.ID || path != oldPath {
		t.Fatalf("resolved scope/path = %#v %q, want legacy %#v %q", resolved, path, oldScope, oldPath)
	}
	registryScope, registryPath, err := ResolveRegistryPathForScope(current)
	if err != nil {
		t.Fatalf("ResolveRegistryPathForScope error: %v", err)
	}
	wantRegistryPath := filepath.Join(tmp, "cache", "codex-helper", "teams", "scopes", "scope_legacy", "registry.json")
	if registryScope.ID != oldScope.ID || registryPath != wantRegistryPath {
		t.Fatalf("resolved registry = %#v %q, want %q", registryScope, registryPath, wantRegistryPath)
	}

	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("legacy state should remain in place for safe in-place inheritance: %v", err)
	}
}

func TestTeamsBackgroundKeepaliveResolveStorePathUsesControlBindingWhenScopeMissingCI(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	oldPath := filepath.Join(tmp, "config", "codex-helper", "teams", "scopes", "scope_without_recorded_scope", "state.json")
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
	if path != oldPath {
		t.Fatalf("resolved path = %q, want legacy path %q", path, oldPath)
	}
	if resolved.ID != current.ID {
		t.Fatalf("resolved scope = %#v, want current canonical scope id %q for legacy state without recorded scope", resolved, current.ID)
	}
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
