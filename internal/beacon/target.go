package beacon

import (
	"fmt"
	"strings"
	"time"
)

const (
	TargetLocal  = "local"
	TargetBeacon = "beacon"
)

type NewTargetInput struct {
	ExplicitBeaconProfile string
	LegacyProxyRoute      string
	ExplicitBeaconFailed  bool
}

type TargetResolution struct {
	Target     string
	Profile    string
	ProxyRoute string
	Error      string
}

func ResolveNewTarget(st State, in NewTargetInput, proxyExists func(string) bool) TargetResolution {
	if strings.TrimSpace(in.ExplicitBeaconProfile) == "" {
		return TargetResolution{Target: TargetLocal, ProxyRoute: strings.TrimSpace(in.LegacyProxyRoute)}
	}
	name := strings.TrimSpace(in.ExplicitBeaconProfile)
	p, ok := st.Profiles[name]
	if !ok {
		return TargetResolution{Target: TargetBeacon, Profile: name, ProxyRoute: strings.TrimSpace(in.LegacyProxyRoute), Error: "beacon profile not found"}
	}
	if reasons := p.DraftReasons(proxyExists); len(reasons) > 0 {
		return TargetResolution{Target: TargetBeacon, Profile: name, ProxyRoute: strings.TrimSpace(in.LegacyProxyRoute), Error: strings.Join(reasons, "; ")}
	}
	if in.ExplicitBeaconFailed {
		return TargetResolution{Target: TargetBeacon, Profile: name, ProxyRoute: strings.TrimSpace(in.LegacyProxyRoute), Error: "explicit beacon request failed"}
	}
	return TargetResolution{Target: TargetBeacon, Profile: name, ProxyRoute: strings.TrimSpace(in.LegacyProxyRoute)}
}

type SwitchInput struct {
	ConversationID      string
	ProfileName         string
	Signature           string
	Fork                bool
	HasQueuedOrRunning  bool
	SignatureCompatible bool
	Now                 time.Time
}

type SwitchResult struct {
	Action  string
	Message string
}

func SwitchProfile(st *State, in SwitchInput, proxyExists func(string) bool) (SwitchResult, error) {
	if st == nil {
		return SwitchResult{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	convID := strings.TrimSpace(in.ConversationID)
	if convID == "" {
		return SwitchResult{}, fmt.Errorf("conversation id is required")
	}
	p, ok := st.Profiles[strings.TrimSpace(in.ProfileName)]
	if !ok {
		return SwitchResult{}, fmt.Errorf("beacon profile %q not found", in.ProfileName)
	}
	if reasons := p.DraftReasons(proxyExists); len(reasons) > 0 {
		return SwitchResult{}, fmt.Errorf("beacon profile %q is not ready: %s", p.Name, strings.Join(reasons, "; "))
	}
	if !in.SignatureCompatible && !in.Fork {
		return SwitchResult{
			Action:  "require_fork",
			Message: "incompatible execution signature; run beacon switch-profile " + p.Name + " --fork",
		}, nil
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	conv := st.Conversations[convID]
	conv.ID = convID
	next := TargetSnapshot{
		Target:          TargetBeacon,
		Profile:         p.Name,
		ProfileRevision: normalizeProfileRevision(p).Revision,
		Signature:       strings.TrimSpace(in.Signature),
		ProxyRoute:      p.ProxyProfile,
		Isolation:       p.IsolationDefault,
	}
	if next.Isolation == "" {
		next.Isolation = IsolationShared
	}
	if in.HasQueuedOrRunning || len(conv.Queued) > 0 {
		conv.Pending = &next
		conv.UpdatedAt = now
		st.Conversations[convID] = conv
		message := "current turn stays " + targetSnapshotLabel(conv.Current) + "; future turns use " + p.Name
		if !in.SignatureCompatible && in.Fork {
			message += "; fork accepted for incompatible execution signature"
		}
		return SwitchResult{
			Action:  "pending",
			Message: message,
		}, nil
	}
	conv.Current = next
	conv.Pending = nil
	conv.UpdatedAt = now
	st.Conversations[convID] = conv
	return SwitchResult{Action: "applied", Message: "current target is " + p.Name}, nil
}

func targetSnapshotLabel(snapshot TargetSnapshot) string {
	switch strings.TrimSpace(snapshot.Target) {
	case TargetBeacon:
		if strings.TrimSpace(snapshot.Profile) != "" {
			return TargetBeacon + ":" + strings.TrimSpace(snapshot.Profile)
		}
		return TargetBeacon
	case TargetLocal, "":
		return TargetLocal
	default:
		label := strings.TrimSpace(snapshot.Target)
		if strings.TrimSpace(snapshot.Profile) != "" {
			label += ":" + strings.TrimSpace(snapshot.Profile)
		}
		return label
	}
}

func QueueTurn(st *State, conversationID string, turnID string, now time.Time) (QueuedTurn, error) {
	if st == nil {
		return QueuedTurn{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	convID := strings.TrimSpace(conversationID)
	if convID == "" {
		return QueuedTurn{}, fmt.Errorf("conversation id is required")
	}
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return QueuedTurn{}, fmt.Errorf("turn id is required")
	}
	conv := st.Conversations[convID]
	conv.ID = convID
	snapshot := conv.Current
	if conv.Pending != nil {
		snapshot = *conv.Pending
	}
	if snapshot.Target == "" {
		snapshot.Target = TargetLocal
	}
	turn := QueuedTurn{ID: turnID, Snapshot: snapshot}
	conv.Queued = append(conv.Queued, turn)
	if now.IsZero() {
		now = time.Now()
	}
	conv.UpdatedAt = now
	st.Conversations[convID] = conv
	st.TurnTargets[turnID] = snapshot
	return turn, nil
}

func RenderStatus(st State, conversationID string) string {
	st.normalize()
	conv := st.Conversations[strings.TrimSpace(conversationID)]
	current := conv.Current
	if current.Target == "" {
		current.Target = TargetLocal
	}
	pending := ""
	if conv.Pending != nil {
		pending = conv.Pending.Target
		if conv.Pending.Profile != "" {
			pending += ":" + conv.Pending.Profile
		}
	}
	turnSnapshot := ""
	if len(conv.Queued) > 0 {
		turnSnapshot = conv.Queued[0].Snapshot.Target
		if conv.Queued[0].Snapshot.Profile != "" {
			turnSnapshot += ":" + conv.Queued[0].Snapshot.Profile
		}
	}
	allocationID := ""
	allocationState := ""
	providerState := ""
	providerReason := ""
	var statusAllocation AllocationRequest
	hasStatusAllocation := false
	if len(conv.Queued) > 0 {
		reqID := ManagedRequestID(conversationID, conv.Queued[0].ID)
		if req, ok := st.Allocations[reqID]; ok {
			statusAllocation = req
			hasStatusAllocation = true
		}
	}
	if !hasStatusAllocation {
		statusAllocation, hasStatusAllocation = latestAllocationForConversation(st, conversationID)
	}
	if hasStatusAllocation {
		allocationID = statusAllocation.ID
		allocationState = string(statusAllocation.State)
		providerState = statusAllocation.RawProviderState
		providerReason = statusAllocation.ProviderReason
		if current.ProviderJobID == "" {
			current.ProviderJobID = statusAllocation.ProviderIdentity.ProviderJobID
		}
	}
	return strings.Join([]string{
		"current_target=" + current.Target,
		"profile=" + current.Profile,
		"pending_target=" + pending,
		"turn_snapshot=" + turnSnapshot,
		"proxy=" + current.ProxyRoute,
		"isolation=" + string(current.Isolation),
		"lease=" + current.LeaseID,
		"machine=" + current.MachineID,
		"provider_job=" + current.ProviderJobID,
		"allocation=" + allocationID,
		"allocation_state=" + allocationState,
		"provider_state=" + providerState,
		"provider_reason=" + providerReason,
	}, " ")
}

func latestAllocationForConversation(st State, conversationID string) (AllocationRequest, bool) {
	conversationID = strings.TrimSpace(conversationID)
	var best AllocationRequest
	for _, req := range st.Allocations {
		if strings.TrimSpace(req.ConversationID) != conversationID {
			continue
		}
		if best.ID == "" || req.UpdatedAt.After(best.UpdatedAt) || (req.UpdatedAt.Equal(best.UpdatedAt) && req.ID > best.ID) {
			best = req
		}
	}
	return best, best.ID != ""
}
