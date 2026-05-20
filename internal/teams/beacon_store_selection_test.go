package teams

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
)

func TestLoadBeaconStateWithSharedProfilesIncludesHistoricalSharedStores(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", "")
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	baseStore, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("NewStore base: %v", err)
	}
	oldShared := filepath.Join(t.TempDir(), "old-shared")
	newShared := filepath.Join(t.TempDir(), "new-shared")
	if err := baseStore.Save(beacon.State{
		Version: beacon.StateVersion,
		Profiles: map[string]beacon.Profile{
			"gpu": {Name: "gpu", Revision: 2, Provider: beacon.ProviderSlurm, SharedPath: newShared},
		},
		ProfileHistory: map[string]beacon.Profile{
			"gpu@1": {Name: "gpu", Revision: 1, Provider: beacon.ProviderSlurm, SharedPath: oldShared},
		},
	}); err != nil {
		t.Fatalf("save base: %v", err)
	}
	oldStore, err := beacon.NewStore(beacon.SharedStorePath(oldShared))
	if err != nil {
		t.Fatalf("NewStore old shared: %v", err)
	}
	if err := oldStore.Save(beacon.State{
		Version: beacon.StateVersion,
		Allocations: map[string]beacon.AllocationRequest{
			"req-old": {
				ID:        "req-old",
				Profile:   "gpu",
				Provider:  beacon.ProviderSlurm,
				State:     beacon.AllocationRunning,
				UpdatedAt: time.Unix(1, 0),
			},
		},
	}); err != nil {
		t.Fatalf("save old shared: %v", err)
	}

	merged, err := loadBeaconStateWithSharedProfiles()
	if err != nil {
		t.Fatalf("load merged state: %v", err)
	}
	if _, ok := merged.Allocations["req-old"]; !ok {
		t.Fatalf("merged state did not include allocation from historical shared_path; allocations=%v", sortedAllocationIDsForTest(merged.Allocations))
	}
}

func sortedAllocationIDsForTest(allocations map[string]beacon.AllocationRequest) string {
	var ids []string
	for id := range allocations {
		ids = append(ids, id)
	}
	return strings.Join(ids, ",")
}
