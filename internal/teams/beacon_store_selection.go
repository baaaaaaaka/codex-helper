package teams

import (
	"os"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
)

func (b *Bridge) beaconStoreForTurn(sessionID string, turnID string) (*beacon.Store, error) {
	baseStore, err := beacon.NewStore("")
	if err != nil {
		return nil, err
	}
	baseState, err := baseStore.Load()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_STORE")) != "" {
		return baseStore, nil
	}
	profile, ok := beaconProfileForTurnStore(baseState, sessionID, turnID)
	if !ok || strings.TrimSpace(profile.SharedPath) == "" {
		return baseStore, nil
	}
	sharedStore, err := beacon.NewStore(beacon.SharedStorePath(profile.SharedPath))
	if err != nil {
		return nil, err
	}
	if err := seedSharedBeaconStore(sharedStore, baseState, sessionID, turnID, profile); err != nil {
		return nil, err
	}
	return sharedStore, nil
}

func beaconProfileForTurnStore(st beacon.State, sessionID string, turnID string) (beacon.Profile, bool) {
	if snap, ok := beacon.TargetSnapshotForTurn(st, turnID); ok {
		if profile, ok := st.Profiles[strings.TrimSpace(snap.Profile)]; ok && strings.TrimSpace(snap.Target) == beacon.TargetBeacon {
			return profile, true
		}
	}
	if conv, ok := st.Conversations[strings.TrimSpace(sessionID)]; ok {
		snap := conv.Current
		if conv.Pending != nil {
			snap = *conv.Pending
		}
		if strings.TrimSpace(snap.Target) == beacon.TargetBeacon {
			if profile, ok := st.Profiles[strings.TrimSpace(snap.Profile)]; ok {
				return profile, true
			}
		}
	}
	return beacon.Profile{}, false
}

func seedSharedBeaconStore(store *beacon.Store, base beacon.State, sessionID string, turnID string, profile beacon.Profile) error {
	return store.Update(func(st *beacon.State) error {
		st.Profiles[profile.Name] = profile
		for key, revision := range base.ProfileHistory {
			if strings.TrimSpace(revision.Name) == strings.TrimSpace(profile.Name) || strings.HasPrefix(key, strings.TrimSpace(profile.Name)+"@") {
				st.ProfileHistory[key] = revision
			}
		}
		if conv, ok := base.Conversations[strings.TrimSpace(sessionID)]; ok {
			existing := st.Conversations[strings.TrimSpace(sessionID)]
			conv.Queued = mergeBeaconQueuedTurns(existing.Queued, conv.Queued)
			st.Conversations[strings.TrimSpace(sessionID)] = conv
		}
		if snap, ok := base.TurnTargets[strings.TrimSpace(turnID)]; ok {
			st.TurnTargets[strings.TrimSpace(turnID)] = snap
		}
		return nil
	})
}

func mergeBeaconQueuedTurns(existing []beacon.QueuedTurn, incoming []beacon.QueuedTurn) []beacon.QueuedTurn {
	seen := map[string]bool{}
	out := make([]beacon.QueuedTurn, 0, len(existing)+len(incoming))
	for _, turn := range append(append([]beacon.QueuedTurn(nil), existing...), incoming...) {
		id := strings.TrimSpace(turn.ID)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, turn)
	}
	return out
}

func beaconSharedStoresForProfiles(base beacon.State, basePath string) ([]*beacon.Store, error) {
	seen := map[string]bool{strings.TrimSpace(basePath): true}
	var stores []*beacon.Store
	addProfile := func(profile beacon.Profile) error {
		path := beacon.SharedStorePath(profile.SharedPath)
		if strings.TrimSpace(path) == "" || seen[strings.TrimSpace(path)] {
			return nil
		}
		seen[strings.TrimSpace(path)] = true
		store, err := beacon.NewStore(path)
		if err != nil {
			return err
		}
		stores = append(stores, store)
		return nil
	}
	for _, profile := range base.Profiles {
		if err := addProfile(profile); err != nil {
			return nil, err
		}
	}
	for _, profile := range base.ProfileHistory {
		if err := addProfile(profile); err != nil {
			return nil, err
		}
	}
	return stores, nil
}

func loadBeaconStateWithSharedProfiles() (beacon.State, error) {
	baseStore, err := beacon.NewStore("")
	if err != nil {
		return beacon.State{}, err
	}
	merged, err := baseStore.Load()
	if err != nil {
		return beacon.State{}, err
	}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_STORE")) != "" {
		return merged, nil
	}
	sharedStores, err := beaconSharedStoresForProfiles(merged, baseStore.Path())
	if err != nil {
		return beacon.State{}, err
	}
	for _, store := range sharedStores {
		shared, err := store.Load()
		if err != nil {
			return beacon.State{}, err
		}
		for id, req := range shared.Allocations {
			merged.Allocations[id] = req
		}
		for id, machine := range shared.Machines {
			merged.Machines[id] = machine
		}
		for id, job := range shared.JobAttempts {
			merged.JobAttempts[id] = job
		}
		for id, terminal := range shared.Terminals {
			merged.Terminals[id] = terminal
		}
		for id, notification := range shared.Notifications {
			merged.Notifications[id] = notification
		}
	}
	return merged, nil
}
