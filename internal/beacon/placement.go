package beacon

import (
	"fmt"
	"sort"
	"strings"
)

type PlacementInput struct {
	ConversationID        string
	ExplicitBeaconProfile string
	LegacyProxyRoute      string
	Isolation             Isolation
	Signature             string
}

type PlacementDecision struct {
	Action    string
	Target    TargetSnapshot
	MachineID string
	Reason    string
}

func DecidePlacement(st State, in PlacementInput, proxyExists func(string) bool) (PlacementDecision, error) {
	st.normalize()
	resolved := ResolveNewTarget(st, NewTargetInput{
		ExplicitBeaconProfile: strings.TrimSpace(in.ExplicitBeaconProfile),
		LegacyProxyRoute:      strings.TrimSpace(in.LegacyProxyRoute),
	}, proxyExists)
	target := TargetSnapshot{
		Target:     resolved.Target,
		Profile:    resolved.Profile,
		Signature:  strings.TrimSpace(in.Signature),
		ProxyRoute: resolved.ProxyRoute,
	}
	if resolved.Error != "" {
		return PlacementDecision{Action: "reject", Target: target, Reason: resolved.Error}, fmt.Errorf("%s", resolved.Error)
	}
	if target.Target == TargetLocal {
		return PlacementDecision{Action: "run_local", Target: target, Reason: "default local execution"}, nil
	}

	profile := normalizeProfileRevision(st.Profiles[target.Profile])
	target.ProfileRevision = profile.Revision
	isolation := in.Isolation
	if isolation == "" {
		isolation = profile.IsolationDefault
	}
	if isolation == "" {
		isolation = IsolationShared
	}
	target.Isolation = isolation

	if m, ok := reusableMachine(st, target.Profile, isolation, strings.TrimSpace(in.ConversationID)); ok {
		target.MachineID = m.ID
		target.LeaseID = m.LeaseID
		target.ProviderJobID = m.ProviderJobID
		return PlacementDecision{
			Action:    "reuse_machine",
			Target:    target,
			MachineID: m.ID,
			Reason:    "compatible accepting machine is available",
		}, nil
	}
	return PlacementDecision{
		Action: "allocate_machine",
		Target: target,
		Reason: "no compatible accepting machine is reusable",
	}, nil
}

func reusableMachine(st State, profile string, isolation Isolation, conversationID string) (Machine, bool) {
	var machines []Machine
	for _, m := range st.Machines {
		machines = append(machines, m)
	}
	sort.Slice(machines, func(i, j int) bool { return machines[i].ID < machines[j].ID })
	for _, m := range machines {
		if strings.TrimSpace(m.Profile) != strings.TrimSpace(profile) {
			continue
		}
		if strings.ToLower(strings.TrimSpace(m.State)) != "accepting" {
			continue
		}
		if m.ExternalOwned {
			continue
		}
		if m.Isolation == IsolationExclusive || isolation == IsolationExclusive {
			if m.Isolation != IsolationExclusive || isolation != IsolationExclusive {
				continue
			}
			if len(m.Jobs) > 0 {
				continue
			}
			if !machineChatsAreEmptyOrOnly(m.Chats, conversationID) {
				continue
			}
			return m, true
		}
		if m.Isolation == "" || m.Isolation == IsolationShared {
			return m, true
		}
	}
	return Machine{}, false
}

func machineChatsAreEmptyOrOnly(chats []string, conversationID string) bool {
	if len(chats) == 0 {
		return true
	}
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return false
	}
	for _, chat := range chats {
		if strings.TrimSpace(chat) != conversationID {
			return false
		}
	}
	return true
}
