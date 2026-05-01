package teams

import (
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

func TestTeamsBackgroundKeepaliveScopeIdentitySeparatesOSUsersCI(t *testing.T) {
	user := User{ID: "teams-user-1", UserPrincipalName: "same@example.test"}
	t.Setenv(envTeamsProfile, "default")
	t.Setenv("CODEX_HOME", "/shared/codex-home")
	t.Setenv("CODEX_HELPER_CONFIG", "/shared/codex-helper/config.json")

	t.Setenv("USER", "alice")
	t.Setenv("LOGNAME", "")
	t.Setenv("USERNAME", "")
	alice := ScopeIdentityForUser(user)
	aliceStore, err := DefaultStorePathForScope(alice.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope alice error: %v", err)
	}

	t.Setenv("USER", "bob")
	bob := ScopeIdentityForUser(user)
	bobStore, err := DefaultStorePathForScope(bob.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope bob error: %v", err)
	}

	if alice.ID == "" || bob.ID == "" || alice.ID == bob.ID {
		t.Fatalf("same Teams account/profile on different OS users should not share scope IDs: alice=%#v bob=%#v", alice, bob)
	}
	if aliceStore == bobStore {
		t.Fatalf("same Teams account/profile on different OS users should not share state path: alice=%q bob=%q", aliceStore, bobStore)
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
