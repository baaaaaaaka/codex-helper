package beacon

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
)

type TurnExecutionAction string

const (
	TurnRunLocal       TurnExecutionAction = "run_local"
	TurnRunBeacon      TurnExecutionAction = "run_beacon"
	TurnWaitAllocation TurnExecutionAction = "wait_allocation"
	TurnReject         TurnExecutionAction = "reject"
)

type TurnExecutionPlan struct {
	Action              TurnExecutionAction    `json:"action"`
	ConversationID      string                 `json:"conversation_id,omitempty"`
	TurnID              string                 `json:"turn_id,omitempty"`
	Snapshot            TargetSnapshot         `json:"snapshot"`
	AllocationRequestID string                 `json:"allocation_request_id,omitempty"`
	AllocationState     AllocationState        `json:"allocation_state,omitempty"`
	MachineID           string                 `json:"machine_id,omitempty"`
	LeaseID             string                 `json:"lease_id,omitempty"`
	ProviderJobID       string                 `json:"provider_job_id,omitempty"`
	ProviderState       string                 `json:"provider_state,omitempty"`
	ProviderReason      string                 `json:"provider_reason,omitempty"`
	SubmitAction        AllocationSubmitAction `json:"submit_action,omitempty"`
	Reason              string                 `json:"reason,omitempty"`
}

const (
	DefaultWorkerHeartbeatStaleAfter = 2 * time.Minute
	DefaultMinimumLeaseTTL           = 30 * time.Second
	DefaultRenewBeforeDeadline       = 5 * time.Minute
	DefaultRenewInFlightTimeout      = 5 * time.Minute
	DefaultRenewFailureRetryAfter    = 5 * time.Minute
	DefaultMaxPreStartReplacements   = 3
)

func ManagedRequestID(conversationID, turnID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(conversationID) + "\x00" + strings.TrimSpace(turnID)))
	return "req-" + fmt.Sprintf("%x", sum[:16])
}

func DeterministicJobName(requestID string) string {
	requestID = sanitizeJobName(strings.TrimSpace(requestID))
	if requestID == "" {
		requestID = "request"
	}
	name := "cxp-" + requestID
	if len(name) > 63 {
		name = strings.TrimRight(name[:63], "-_.")
	}
	if name == "" || name == "cxp-" {
		return "cxp-request"
	}
	return name
}

func sanitizeJobName(in string) string {
	var b strings.Builder
	for _, r := range in {
		if r > unicode.MaxASCII {
			b.WriteByte('-')
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	return strings.Trim(b.String(), "-_.")
}

func SnapshotTurnTarget(st *State, conversationID string, turnID string, now time.Time) (QueuedTurn, error) {
	if st == nil {
		return QueuedTurn{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return QueuedTurn{}, fmt.Errorf("turn id is required")
	}
	if snap, ok := TargetSnapshotForTurn(*st, turnID); ok {
		return QueuedTurn{ID: turnID, Snapshot: snap}, nil
	}
	turn, err := QueueTurn(st, conversationID, turnID, now)
	if err != nil {
		return QueuedTurn{}, err
	}
	st.TurnTargets[turn.ID] = turn.Snapshot
	return turn, nil
}

func TargetSnapshotForTurn(st State, turnID string) (TargetSnapshot, bool) {
	st.normalize()
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return TargetSnapshot{}, false
	}
	if snap, ok := st.TurnTargets[turnID]; ok {
		if snap.Target == "" {
			snap.Target = TargetLocal
		}
		return snap, true
	}
	for _, conv := range st.Conversations {
		for _, queued := range conv.Queued {
			if strings.TrimSpace(queued.ID) == turnID {
				snap := queued.Snapshot
				if snap.Target == "" {
					snap.Target = TargetLocal
				}
				return snap, true
			}
		}
	}
	return TargetSnapshot{}, false
}

func RemoveTurnSnapshot(st *State, conversationID string, turnID string, now time.Time) {
	if st == nil {
		return
	}
	st.normalize()
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return
	}
	delete(st.TurnTargets, turnID)
	convID := strings.TrimSpace(conversationID)
	if convID == "" {
		for id, conv := range st.Conversations {
			conv.Queued = queuedWithoutTurn(conv.Queued, turnID)
			if now.IsZero() {
				now = time.Now()
			}
			conv.UpdatedAt = now
			st.Conversations[id] = conv
		}
		return
	}
	conv := st.Conversations[convID]
	conv.Queued = queuedWithoutTurn(conv.Queued, turnID)
	if now.IsZero() {
		now = time.Now()
	}
	conv.UpdatedAt = now
	st.Conversations[convID] = conv
}

func queuedWithoutTurn(in []QueuedTurn, turnID string) []QueuedTurn {
	out := in[:0]
	for _, queued := range in {
		if strings.TrimSpace(queued.ID) != turnID {
			out = append(out, queued)
		}
	}
	return out
}

func PlanTurnExecution(st *State, conversationID string, turnID string, now time.Time) (TurnExecutionPlan, error) {
	if st == nil {
		return TurnExecutionPlan{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	convID := strings.TrimSpace(conversationID)
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return TurnExecutionPlan{}, fmt.Errorf("turn id is required")
	}
	queued, err := SnapshotTurnTarget(st, convID, turnID, now)
	if err != nil {
		return TurnExecutionPlan{}, err
	}
	snap := queued.Snapshot
	if snap.Target == "" {
		snap.Target = TargetLocal
	}
	plan := TurnExecutionPlan{
		ConversationID: convID,
		TurnID:         turnID,
		Snapshot:       snap,
	}
	if snap.Target != TargetBeacon {
		plan.Action = TurnRunLocal
		plan.Reason = "local target"
		return plan, nil
	}
	if strings.TrimSpace(snap.Profile) == "" {
		plan.Action = TurnReject
		plan.Reason = "beacon target has no profile"
		return plan, nil
	}
	req, _, err := EnsureAllocationRequest(st, convID, turnID, snap, now)
	if err != nil {
		return TurnExecutionPlan{}, err
	}
	plan.AllocationRequestID = req.ID
	plan.AllocationState = req.State
	plan.ProviderJobID = req.ProviderIdentity.ProviderJobID
	plan.ProviderState = req.RawProviderState
	plan.ProviderReason = req.ProviderReason
	if readyMachineSnapshot(st, snap, req, now) {
		plan.Action = TurnRunBeacon
		plan.AllocationRequestID = req.ID
		plan.AllocationState = req.State
		plan.MachineID = snap.MachineID
		plan.LeaseID = snap.LeaseID
		plan.ProviderJobID = snap.ProviderJobID
		plan.Reason = "ready beacon lease"
		return plan, nil
	}
	if machine, ok := readyMachineForAllocation(*st, req, now); ok {
		bound := snap
		bound.MachineID = machine.ID
		bound.LeaseID = machine.LeaseID
		bound.ProviderJobID = firstNonEmpty(machine.ProviderJobID, req.ProviderIdentity.ProviderJobID)
		bindTurnSnapshot(st, convID, turnID, bound, now)
		req.Target = bound
		req.ProviderIdentity.ProviderJobID = bound.ProviderJobID
		req.State = AllocationRunning
		if now.IsZero() {
			now = time.Now()
		}
		req.UpdatedAt = now
		st.Allocations[req.ID] = req
		plan.Action = TurnRunBeacon
		plan.AllocationRequestID = req.ID
		plan.AllocationState = req.State
		plan.MachineID = bound.MachineID
		plan.LeaseID = bound.LeaseID
		plan.ProviderJobID = bound.ProviderJobID
		plan.ProviderState = req.RawProviderState
		plan.ProviderReason = req.ProviderReason
		plan.Snapshot = bound
		plan.Reason = "ready beacon lease"
		return plan, nil
	}
	plan.Action = TurnWaitAllocation
	plan.Reason = "explicit beacon target requires managed allocation; local fallback is disabled"
	return plan, nil
}

func readyMachineSnapshot(st *State, snap TargetSnapshot, req AllocationRequest, now time.Time) bool {
	if strings.TrimSpace(snap.MachineID) == "" ||
		strings.TrimSpace(snap.LeaseID) == "" {
		return false
	}
	if providerRequiresJobID(req.Provider) && strings.TrimSpace(snap.ProviderJobID) == "" {
		return false
	}
	if st == nil {
		return false
	}
	for _, machine := range st.Machines {
		if machineMatchesSnapshot(machine, snap) && MachineCanAcceptAllocation(machine, req, now) {
			return true
		}
	}
	return false
}

func machineMatchesSnapshot(machine Machine, snap TargetSnapshot) bool {
	if strings.TrimSpace(machine.ID) != strings.TrimSpace(snap.MachineID) {
		return false
	}
	if strings.TrimSpace(machine.LeaseID) != strings.TrimSpace(snap.LeaseID) {
		return false
	}
	if strings.TrimSpace(machine.ProviderJobID) != strings.TrimSpace(snap.ProviderJobID) {
		return false
	}
	if strings.TrimSpace(snap.Profile) != "" && strings.TrimSpace(machine.Profile) != "" && strings.TrimSpace(machine.Profile) != strings.TrimSpace(snap.Profile) {
		return false
	}
	return true
}

func readyMachineForAllocation(st State, req AllocationRequest, now time.Time) (Machine, bool) {
	profile := strings.TrimSpace(req.Profile)
	isolation := req.Isolation
	if isolation == "" {
		isolation = req.ProfileSnapshot.IsolationDefault
	}
	if isolation == "" {
		isolation = IsolationShared
	}
	providerJobID := firstNonEmpty(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID)
	var machines []Machine
	for _, machine := range st.Machines {
		machines = append(machines, machine)
	}
	sort.Slice(machines, func(i, j int) bool { return machines[i].ID < machines[j].ID })
	for _, machine := range machines {
		if strings.TrimSpace(machine.Profile) != profile {
			continue
		}
		if strings.TrimSpace(machine.ID) == "" || strings.TrimSpace(machine.LeaseID) == "" {
			continue
		}
		if providerRequiresJobID(req.Provider) && strings.TrimSpace(machine.ProviderJobID) == "" {
			continue
		}
		if providerJobID != "" && strings.TrimSpace(machine.ProviderJobID) != providerJobID {
			continue
		}
		if !MachineCanAcceptAllocation(machine, req, now) {
			continue
		}
		if machine.Isolation == IsolationExclusive || isolation == IsolationExclusive {
			if machine.Isolation != IsolationExclusive || isolation != IsolationExclusive {
				continue
			}
			if len(machine.Jobs) > 0 {
				continue
			}
			if !machineChatsAreEmptyOrOnly(machine.Chats, req.ConversationID) {
				continue
			}
			return machine, true
		}
		if machine.Isolation == "" || machine.Isolation == IsolationShared {
			return machine, true
		}
	}
	return Machine{}, false
}

func machineLooksReadyForSnapshot(machine Machine, snap TargetSnapshot, now time.Time) bool {
	if !strings.EqualFold(strings.TrimSpace(machine.State), string(LeaseAccepting)) {
		return false
	}
	if machine.ExternalOwned {
		return false
	}
	if !workerHeartbeatFresh(machine, now) {
		return false
	}
	if !machineDoctorAccepts(machine) {
		return false
	}
	if strings.TrimSpace(snap.Signature) != "" && strings.TrimSpace(machine.Execution.Hash) != strings.TrimSpace(snap.Signature) {
		return false
	}
	if !leaseTTLAccepts(machine, now) {
		return false
	}
	return true
}

func MachineCanAcceptAllocation(machine Machine, req AllocationRequest, now time.Time) bool {
	if !strings.EqualFold(strings.TrimSpace(machine.State), string(LeaseAccepting)) {
		return false
	}
	if machine.ExternalOwned {
		return false
	}
	if strings.TrimSpace(req.Profile) != "" && strings.TrimSpace(machine.Profile) != strings.TrimSpace(req.Profile) {
		return false
	}
	if providerRequiresJobID(req.Provider) && strings.TrimSpace(machine.ProviderJobID) == "" {
		return false
	}
	providerJobID := firstNonEmpty(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID)
	if providerJobID != "" && strings.TrimSpace(machine.ProviderJobID) != providerJobID {
		return false
	}
	if !workerHeartbeatFresh(machine, now) {
		return false
	}
	if !machineDoctorAccepts(machine) {
		return false
	}
	if !workerMembershipProofOK(req, machine.ProviderJobID, machine.MembershipProof) {
		return false
	}
	if strings.TrimSpace(req.Execution.Hash) != "" && strings.TrimSpace(machine.Execution.Hash) != strings.TrimSpace(req.Execution.Hash) {
		return false
	}
	if machine.ProviderState != "" && machine.ProviderState != ProviderJobRunning {
		return false
	}
	if !leaseTTLAccepts(machine, now) {
		return false
	}
	return true
}

func providerRequiresJobID(provider Provider) bool {
	switch provider {
	case ProviderSlurm, ProviderLSF:
		return true
	default:
		return false
	}
}

func workerHeartbeatFresh(machine Machine, now time.Time) bool {
	if machine.LastHeartbeat.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return now.Sub(machine.LastHeartbeat) <= DefaultWorkerHeartbeatStaleAfter
}

func machineDoctorAccepts(machine Machine) bool {
	if len(machine.DoctorBlockers) > 0 {
		return false
	}
	if workerDoctorIsZero(machine.Doctor) {
		return false
	}
	return WorkerDoctorPassed(machine.Doctor)
}

func leaseTTLAccepts(machine Machine, now time.Time) bool {
	if machine.LeaseExpiresAt.IsZero() {
		return true
	}
	if now.IsZero() {
		now = time.Now()
	}
	return machine.LeaseExpiresAt.Sub(now) >= DefaultMinimumLeaseTTL
}

func bindTurnSnapshot(st *State, conversationID string, turnID string, snapshot TargetSnapshot, now time.Time) {
	if st == nil {
		return
	}
	st.normalize()
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return
	}
	st.TurnTargets[turnID] = snapshot
	convID := strings.TrimSpace(conversationID)
	if convID == "" {
		return
	}
	conv := st.Conversations[convID]
	conv.ID = convID
	for i := range conv.Queued {
		if strings.TrimSpace(conv.Queued[i].ID) == turnID {
			conv.Queued[i].Snapshot = snapshot
		}
	}
	if now.IsZero() {
		now = time.Now()
	}
	conv.UpdatedAt = now
	st.Conversations[convID] = conv
}

func EnsureAllocationRequest(st *State, conversationID string, turnID string, snap TargetSnapshot, now time.Time) (AllocationRequest, bool, error) {
	if st == nil {
		return AllocationRequest{}, false, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	conversationID = strings.TrimSpace(conversationID)
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return AllocationRequest{}, false, fmt.Errorf("turn id is required")
	}
	if snap.Target != TargetBeacon {
		return AllocationRequest{}, false, fmt.Errorf("allocation requires a beacon target")
	}
	profileName := strings.TrimSpace(snap.Profile)
	if profileName == "" {
		return AllocationRequest{}, false, fmt.Errorf("beacon profile is required")
	}
	profile, ok := st.Profiles[profileName]
	if !ok {
		return AllocationRequest{}, false, fmt.Errorf("beacon profile %q not found", profileName)
	}
	if reasons := profile.DraftReasons(nil); len(reasons) > 0 {
		return AllocationRequest{}, false, fmt.Errorf("beacon profile %q is not ready: %s", profileName, strings.Join(reasons, "; "))
	}
	reqID := ManagedRequestID(conversationID, turnID)
	if existing, ok := st.Allocations[reqID]; ok {
		return existing, false, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	isolation := snap.Isolation
	if isolation == "" {
		isolation = profile.IsolationDefault
	}
	if isolation == "" {
		isolation = IsolationShared
	}
	req := AllocationRequest{
		ID:                reqID,
		ConversationID:    conversationID,
		TurnID:            turnID,
		Profile:           profile.Name,
		ProfileSnapshot:   profile,
		Provider:          profile.Provider,
		Isolation:         isolation,
		Target:            snap,
		Execution:         snapshotExecution(snap),
		DeterministicName: DeterministicJobName(reqID),
		State:             AllocationRequestPersisted,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	st.Allocations[req.ID] = req
	return req, true, nil
}

func snapshotExecution(snap TargetSnapshot) ExecutionSignature {
	if snap.SignatureDetails != nil && snap.SignatureDetails.Hash != "" {
		return *snap.SignatureDetails
	}
	if strings.TrimSpace(snap.Signature) == "" {
		return ExecutionSignature{}
	}
	return ExecutionSignature{Hash: strings.TrimSpace(snap.Signature)}
}

type SchedulerQueryResult struct {
	ProviderJobID    string
	RawState         string
	Reason           string
	ProviderDeadline time.Time
	DurableNegative  bool
	QueryError       bool
	MultipleMatches  bool
}

type AllocationSubmitAction string

const (
	AllocationSubmitAlreadyKnown AllocationSubmitAction = "already_known"
	AllocationSubmitAdopt        AllocationSubmitAction = "adopt_existing"
	AllocationSubmitNow          AllocationSubmitAction = "submit"
	AllocationSubmitWait         AllocationSubmitAction = "wait"
	AllocationSubmitAttention    AllocationSubmitAction = "needs_attention"
)

type AllocationRenewAction string

const (
	AllocationRenewSkip           AllocationRenewAction = "skip"
	AllocationRenewWait           AllocationRenewAction = "wait"
	AllocationRenewNow            AllocationRenewAction = "renew"
	AllocationRenewNeedsAttention AllocationRenewAction = "needs_attention"
)

type AllocationRenewOptions struct {
	RenewBefore       time.Duration
	InFlightTimeout   time.Duration
	FailureRetryAfter time.Duration
	StartedOnly       bool
}

func (opts AllocationRenewOptions) withDefaults() AllocationRenewOptions {
	if opts.RenewBefore <= 0 {
		opts.RenewBefore = DefaultRenewBeforeDeadline
	}
	if opts.InFlightTimeout <= 0 {
		opts.InFlightTimeout = DefaultRenewInFlightTimeout
	}
	if opts.FailureRetryAfter <= 0 {
		opts.FailureRetryAfter = DefaultRenewFailureRetryAfter
	}
	return opts
}

func DecideAllocationSubmit(req AllocationRequest, query SchedulerQueryResult) AllocationSubmitAction {
	if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" {
		return AllocationSubmitAlreadyKnown
	}
	if query.QueryError {
		return AllocationSubmitWait
	}
	if query.MultipleMatches {
		return AllocationSubmitAttention
	}
	if strings.TrimSpace(query.ProviderJobID) != "" {
		return AllocationSubmitAdopt
	}
	if req.SubmitAttempts > 0 && !query.DurableNegative {
		return AllocationSubmitWait
	}
	return AllocationSubmitNow
}

type AllocationAdapter interface {
	QueryAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error)
	SubmitAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error)
}

type AllocationCancelAdapter interface {
	CancelAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error)
}

type AllocationRenewAdapter interface {
	RenewAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error)
}

func ReconcileAllocationSubmit(ctx context.Context, st *State, requestID string, adapter AllocationAdapter, now time.Time) (AllocationRequest, AllocationSubmitAction, error) {
	if st == nil {
		return AllocationRequest{}, "", fmt.Errorf("nil beacon state")
	}
	if adapter == nil {
		return AllocationRequest{}, "", fmt.Errorf("allocation adapter is required")
	}
	st.normalize()
	requestID = strings.TrimSpace(requestID)
	req, ok := st.Allocations[requestID]
	if !ok {
		return AllocationRequest{}, "", fmt.Errorf("allocation request %q not found", requestID)
	}
	if now.IsZero() {
		now = time.Now()
	}
	query, err := adapter.QueryAllocation(ctx, req)
	if err != nil {
		query.QueryError = true
		query.Reason = err.Error()
	}
	action := DecideAllocationSubmit(req, query)
	switch action {
	case AllocationSubmitAlreadyKnown:
		return req, action, nil
	case AllocationSubmitWait:
		req.ProviderReason = strings.TrimSpace(query.Reason)
		req.RawProviderState = strings.TrimSpace(query.RawState)
		if !query.ProviderDeadline.IsZero() {
			req.ProviderDeadline = query.ProviderDeadline
		}
		req.UpdatedAt = now
		st.Allocations[requestID] = req
		return req, action, nil
	case AllocationSubmitAttention:
		req.State = AllocationNeedsAttention
		req.ProviderReason = firstNonEmpty(strings.TrimSpace(query.Reason), "multiple provider jobs matched deterministic allocation name")
		req.RawProviderState = strings.TrimSpace(query.RawState)
		if !query.ProviderDeadline.IsZero() {
			req.ProviderDeadline = query.ProviderDeadline
		}
		req.UpdatedAt = now
		st.Allocations[requestID] = req
		return req, action, nil
	case AllocationSubmitAdopt:
		req.ProviderIdentity.ProviderJobID = strings.TrimSpace(query.ProviderJobID)
		req.RawProviderState = strings.TrimSpace(query.RawState)
		req.ProviderReason = strings.TrimSpace(query.Reason)
		if !query.ProviderDeadline.IsZero() {
			req.ProviderDeadline = query.ProviderDeadline
		}
		req.State = AllocationSubmitted
		req.UpdatedAt = now
		st.Allocations[requestID] = req
		return req, action, nil
	case AllocationSubmitNow:
		submitted, err := adapter.SubmitAllocation(ctx, req)
		req.SubmitAttempts++
		if err != nil {
			req.State = AllocationNeedsAttention
			req.ProviderReason = err.Error()
			req.UpdatedAt = now
			st.Allocations[requestID] = req
			return req, action, err
		}
		if strings.TrimSpace(submitted.ProviderJobID) != "" {
			req.ProviderIdentity.ProviderJobID = strings.TrimSpace(submitted.ProviderJobID)
		}
		req.RawProviderState = strings.TrimSpace(submitted.RawState)
		req.ProviderReason = strings.TrimSpace(submitted.Reason)
		if !submitted.ProviderDeadline.IsZero() {
			req.ProviderDeadline = submitted.ProviderDeadline
		}
		req.State = AllocationSubmitted
		req.UpdatedAt = now
		st.Allocations[requestID] = req
		return req, action, nil
	default:
		return req, action, fmt.Errorf("unknown allocation submit action %q", action)
	}
}

func ReconcileAllocationSubmitOutsideLock(ctx context.Context, store *Store, requestID string, adapter AllocationAdapter, now time.Time) (AllocationRequest, AllocationSubmitAction, error) {
	if store == nil {
		return AllocationRequest{}, "", fmt.Errorf("nil beacon store")
	}
	if adapter == nil {
		return AllocationRequest{}, "", fmt.Errorf("allocation adapter is required")
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return AllocationRequest{}, "", fmt.Errorf("allocation request id is required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	st, err := store.Load()
	if err != nil {
		return AllocationRequest{}, "", err
	}
	req, ok := st.Allocations[requestID]
	if !ok {
		return AllocationRequest{}, "", fmt.Errorf("allocation request %q not found", requestID)
	}
	if allocationStateTerminal(req.State) {
		return req, AllocationSubmitAlreadyKnown, nil
	}
	query, queryErr := adapter.QueryAllocation(ctx, req)
	if queryErr != nil {
		query.QueryError = true
		query.Reason = queryErr.Error()
	}
	action := DecideAllocationSubmit(req, query)
	var submitted SchedulerQueryResult
	var submitErr error
	if action == AllocationSubmitNow {
		latest, loadErr := store.Load()
		if loadErr != nil {
			return AllocationRequest{}, action, loadErr
		}
		current, ok := latest.Allocations[requestID]
		if !ok {
			return AllocationRequest{}, action, fmt.Errorf("allocation request %q not found", requestID)
		}
		if allocationStateTerminal(current.State) {
			return current, AllocationSubmitAlreadyKnown, nil
		}
		if strings.TrimSpace(current.ProviderIdentity.ProviderJobID) != "" {
			return current, AllocationSubmitAlreadyKnown, nil
		}
		if current.State != req.State ||
			current.SubmitAttempts != req.SubmitAttempts ||
			current.ReplacementEpoch != req.ReplacementEpoch ||
			strings.TrimSpace(current.ReplacementID) != strings.TrimSpace(req.ReplacementID) {
			return current, AllocationSubmitWait, nil
		}
		req = current
		submitted, submitErr = adapter.SubmitAllocation(ctx, req)
	}
	var updated AllocationRequest
	updateErr := store.Update(func(st *State) error {
		current, ok := st.Allocations[requestID]
		if !ok {
			return fmt.Errorf("allocation request %q not found", requestID)
		}
		if allocationStateTerminal(current.State) {
			updated = current
			return nil
		}
		if strings.TrimSpace(current.ProviderIdentity.ProviderJobID) != "" && action == AllocationSubmitNow {
			action = AllocationSubmitAlreadyKnown
		}
		updated = applyAllocationSubmitResult(current, query, submitted, action, submitErr, now)
		st.Allocations[requestID] = updated
		return nil
	})
	if updateErr != nil {
		return AllocationRequest{}, action, updateErr
	}
	if queryErr != nil && action != AllocationSubmitNow {
		return updated, action, queryErr
	}
	return updated, action, submitErr
}

func ReconcileAllocationRenewOutsideLock(ctx context.Context, store *Store, requestID string, adapter AllocationRenewAdapter, opts AllocationRenewOptions, now time.Time) (AllocationRequest, AllocationRenewAction, error) {
	if store == nil {
		return AllocationRequest{}, "", fmt.Errorf("nil beacon store")
	}
	if adapter == nil {
		return AllocationRequest{}, "", fmt.Errorf("renew adapter is required")
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return AllocationRequest{}, "", fmt.Errorf("allocation request id is required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	opts = opts.withDefaults()
	var claimed AllocationRequest
	action := AllocationRenewSkip
	if err := store.Update(func(st *State) error {
		current, ok := st.Allocations[requestID]
		if !ok {
			return fmt.Errorf("allocation request %q not found", requestID)
		}
		claimed = current
		nextAction := decideAllocationRenew(*st, current, opts, now)
		action = nextAction
		switch nextAction {
		case AllocationRenewNow:
			current.RenewEpoch++
			current.RenewStartedAt = now
			current.RenewError = ""
			current.UpdatedAt = now
			st.Allocations[requestID] = current
			claimed = current
		case AllocationRenewNeedsAttention:
			current.State = AllocationNeedsAttention
			current.ProviderReason = firstNonEmpty(current.ProviderReason, "beacon allocation lease is expiring and cannot be renewed automatically")
			current.UpdatedAt = now
			st.Allocations[requestID] = current
			claimed = current
		}
		return nil
	}); err != nil {
		return AllocationRequest{}, action, err
	}
	if action != AllocationRenewNow {
		return claimed, action, nil
	}
	renewed, renewErr := adapter.RenewAllocation(ctx, claimed)
	var updated AllocationRequest
	updateErr := store.Update(func(st *State) error {
		current, ok := st.Allocations[requestID]
		if !ok {
			return fmt.Errorf("allocation request %q not found", requestID)
		}
		updated = current
		if allocationStateTerminal(current.State) ||
			current.RenewEpoch != claimed.RenewEpoch ||
			strings.TrimSpace(current.ProviderIdentity.ProviderJobID) != strings.TrimSpace(claimed.ProviderIdentity.ProviderJobID) {
			return nil
		}
		updated = applyAllocationRenewResult(*st, current, renewed, renewErr, now)
		st.Allocations[requestID] = updated
		applyAllocationDeadlineToMachines(st, updated, now)
		if strings.TrimSpace(updated.RawProviderState) != "" {
			projection := ProjectRawProviderState(updated.Provider, updated.RawProviderState, updated.ProviderReason, AllocationHasStartedJob(*st, updated.ID), allocationHasEverRun(updated))
			_, _ = UpdateAllocationProjection(st, updated.ID, projection, now)
			updated = st.Allocations[requestID]
		}
		return nil
	})
	if updateErr != nil {
		return AllocationRequest{}, action, updateErr
	}
	if renewErr != nil {
		return updated, action, renewErr
	}
	return updated, action, nil
}

func decideAllocationRenew(st State, req AllocationRequest, opts AllocationRenewOptions, now time.Time) AllocationRenewAction {
	if allocationStateTerminal(req.State) || req.State == AllocationNeedsAttention {
		return AllocationRenewSkip
	}
	if !req.CancelRequestedAt.IsZero() {
		return AllocationRenewSkip
	}
	if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) == "" {
		return AllocationRenewSkip
	}
	if opts.StartedOnly && !AllocationHasStartedJob(st, req.ID) {
		return AllocationRenewSkip
	}
	deadline := AllocationLeaseDeadline(st, req)
	if deadline.IsZero() {
		return AllocationRenewSkip
	}
	if deadline.After(now.Add(opts.RenewBefore)) {
		return AllocationRenewSkip
	}
	if renewalInFlight(req, opts.InFlightTimeout, now) {
		return AllocationRenewWait
	}
	if !req.RenewCompletedAt.IsZero() && !req.RenewCompletedAt.Before(req.RenewStartedAt) && now.Sub(req.RenewCompletedAt) < opts.FailureRetryAfter && strings.TrimSpace(req.RenewError) == "" {
		if deadline.Sub(now) <= DefaultMinimumLeaseTTL && !deadline.After(req.RenewCompletedAt) {
			return AllocationRenewNeedsAttention
		}
		return AllocationRenewWait
	}
	if strings.TrimSpace(req.RenewError) != "" && !req.RenewCompletedAt.IsZero() && now.Sub(req.RenewCompletedAt) < opts.FailureRetryAfter {
		if deadline.Sub(now) <= DefaultMinimumLeaseTTL {
			return AllocationRenewNeedsAttention
		}
		return AllocationRenewWait
	}
	return AllocationRenewNow
}

func renewalInFlight(req AllocationRequest, timeout time.Duration, now time.Time) bool {
	if req.RenewStartedAt.IsZero() {
		return false
	}
	if !req.RenewCompletedAt.IsZero() && !req.RenewCompletedAt.Before(req.RenewStartedAt) {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return now.Sub(req.RenewStartedAt) < timeout
}

func applyAllocationRenewResult(st State, req AllocationRequest, renewed SchedulerQueryResult, renewErr error, now time.Time) AllocationRequest {
	if now.IsZero() {
		now = time.Now()
	}
	req.RenewCompletedAt = now
	req.UpdatedAt = now
	if renewErr != nil {
		req.RenewError = renewErr.Error()
		req.ProviderReason = renewErr.Error()
		if deadline := AllocationLeaseDeadline(st, req); deadline.IsZero() || deadline.Sub(now) <= DefaultMinimumLeaseTTL {
			req.State = AllocationNeedsAttention
		}
		return req
	}
	req.RenewError = ""
	if providerJobID := strings.TrimSpace(renewed.ProviderJobID); providerJobID != "" && providerJobID != strings.TrimSpace(req.ProviderIdentity.ProviderJobID) {
		req.State = AllocationNeedsAttention
		req.ProviderReason = "renew returned a different provider job id"
		return req
	}
	if strings.TrimSpace(renewed.RawState) != "" {
		req.RawProviderState = strings.TrimSpace(renewed.RawState)
	}
	if strings.TrimSpace(renewed.Reason) != "" {
		req.ProviderReason = strings.TrimSpace(renewed.Reason)
	}
	if !renewed.ProviderDeadline.IsZero() {
		req.ProviderDeadline = renewed.ProviderDeadline
	}
	return req
}

func applyAllocationSubmitResult(req AllocationRequest, query SchedulerQueryResult, submitted SchedulerQueryResult, action AllocationSubmitAction, submitErr error, now time.Time) AllocationRequest {
	if now.IsZero() {
		now = time.Now()
	}
	switch action {
	case AllocationSubmitAlreadyKnown:
		if strings.TrimSpace(query.RawState) != "" {
			req.RawProviderState = strings.TrimSpace(query.RawState)
		}
		if strings.TrimSpace(query.Reason) != "" {
			req.ProviderReason = strings.TrimSpace(query.Reason)
		}
		if !query.ProviderDeadline.IsZero() {
			req.ProviderDeadline = query.ProviderDeadline
		}
	case AllocationSubmitWait:
		req.RawProviderState = strings.TrimSpace(query.RawState)
		req.ProviderReason = strings.TrimSpace(query.Reason)
		if !query.ProviderDeadline.IsZero() {
			req.ProviderDeadline = query.ProviderDeadline
		}
	case AllocationSubmitAttention:
		req.State = AllocationNeedsAttention
		req.ProviderReason = firstNonEmpty(strings.TrimSpace(query.Reason), "multiple provider jobs matched deterministic allocation name")
		req.RawProviderState = strings.TrimSpace(query.RawState)
		if !query.ProviderDeadline.IsZero() {
			req.ProviderDeadline = query.ProviderDeadline
		}
	case AllocationSubmitAdopt:
		req.ProviderIdentity.ProviderJobID = strings.TrimSpace(query.ProviderJobID)
		req.RawProviderState = strings.TrimSpace(query.RawState)
		req.ProviderReason = strings.TrimSpace(query.Reason)
		if !query.ProviderDeadline.IsZero() {
			req.ProviderDeadline = query.ProviderDeadline
		}
		req.State = AllocationSubmitted
	case AllocationSubmitNow:
		req.SubmitAttempts++
		if submitErr != nil {
			req.State = AllocationNeedsAttention
			req.ProviderReason = submitErr.Error()
			break
		}
		if strings.TrimSpace(submitted.ProviderJobID) != "" {
			req.ProviderIdentity.ProviderJobID = strings.TrimSpace(submitted.ProviderJobID)
		}
		req.RawProviderState = strings.TrimSpace(submitted.RawState)
		req.ProviderReason = strings.TrimSpace(submitted.Reason)
		if !submitted.ProviderDeadline.IsZero() {
			req.ProviderDeadline = submitted.ProviderDeadline
		}
		req.State = AllocationSubmitted
	}
	req.UpdatedAt = now
	return req
}

func allocationStateTerminal(state AllocationState) bool {
	switch state {
	case AllocationCanceled, AllocationExpired, AllocationFailed:
		return true
	default:
		return false
	}
}

func AllocationLeaseDeadline(st State, req AllocationRequest) time.Time {
	st.normalize()
	deadline := req.ProviderDeadline
	providerJobID := strings.TrimSpace(req.ProviderIdentity.ProviderJobID)
	if providerJobID == "" {
		providerJobID = strings.TrimSpace(req.Target.ProviderJobID)
	}
	if providerJobID == "" {
		return deadline
	}
	for _, machine := range st.Machines {
		if strings.TrimSpace(machine.ProviderJobID) != providerJobID || machine.LeaseExpiresAt.IsZero() {
			continue
		}
		if deadline.IsZero() || machine.LeaseExpiresAt.Before(deadline) {
			deadline = machine.LeaseExpiresAt
		}
	}
	return deadline
}

func AllocationHasStartedJob(st State, allocationID string) bool {
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return false
	}
	for _, job := range st.JobAttempts {
		if strings.TrimSpace(job.RequestID) != allocationID {
			continue
		}
		switch job.Phase {
		case JobStartIntent, JobStarted, JobTerminal, JobAmbiguous, JobQuarantined:
			return true
		}
	}
	return false
}

func allocationCanAutoReplaceBeforeStart(st State, allocationID string) bool {
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return false
	}
	for _, job := range st.JobAttempts {
		if strings.TrimSpace(job.RequestID) != allocationID {
			continue
		}
		if job.Phase != JobQueued && job.Phase != JobTombstoned {
			return false
		}
	}
	return true
}

func allocationHasEverRun(req AllocationRequest) bool {
	switch req.State {
	case AllocationRunning, AllocationExpired, AllocationFailed:
		return true
	default:
		return false
	}
}

func applyAllocationDeadlineToMachines(st *State, req AllocationRequest, now time.Time) {
	if st == nil || req.ProviderDeadline.IsZero() {
		return
	}
	providerJobID := strings.TrimSpace(req.ProviderIdentity.ProviderJobID)
	if providerJobID == "" {
		providerJobID = strings.TrimSpace(req.Target.ProviderJobID)
	}
	if providerJobID == "" {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	for key, machine := range st.Machines {
		if strings.TrimSpace(machine.ProviderJobID) != providerJobID {
			continue
		}
		machine.LeaseExpiresAt = req.ProviderDeadline
		machine.UpdatedAt = now
		st.Machines[key] = machine
	}
}

func ApplyAllocationDeadlineToMachines(st *State, req AllocationRequest, now time.Time) {
	applyAllocationDeadlineToMachines(st, req, now)
}

func RecordAllocationSubmit(st *State, requestID string, providerJobID string, rawState string, reason string, now time.Time) (AllocationRequest, error) {
	if st == nil {
		return AllocationRequest{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	requestID = strings.TrimSpace(requestID)
	req, ok := st.Allocations[requestID]
	if !ok {
		return AllocationRequest{}, fmt.Errorf("allocation request %q not found", requestID)
	}
	if now.IsZero() {
		now = time.Now()
	}
	providerJobID = strings.TrimSpace(providerJobID)
	if providerJobID != "" {
		req.ProviderIdentity.ProviderJobID = providerJobID
	}
	req.RawProviderState = strings.TrimSpace(rawState)
	req.ProviderReason = strings.TrimSpace(reason)
	req.SubmitAttempts++
	req.State = AllocationSubmitted
	req.UpdatedAt = now
	st.Allocations[requestID] = req
	return req, nil
}

func UpdateAllocationProjection(st *State, requestID string, projection ProviderProjection, now time.Time) (AllocationRequest, error) {
	if st == nil {
		return AllocationRequest{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	requestID = strings.TrimSpace(requestID)
	req, ok := st.Allocations[requestID]
	if !ok {
		return AllocationRequest{}, fmt.Errorf("allocation request %q not found", requestID)
	}
	if now.IsZero() {
		now = time.Now()
	}
	req.RawProviderState = strings.TrimSpace(projection.RawState)
	req.ProviderReason = strings.TrimSpace(projection.Reason)
	if allocationStateTerminal(req.State) {
		req.UpdatedAt = now
		st.Allocations[requestID] = req
		return req, nil
	}
	switch projection.Action {
	case ReconcilePending:
		req.State = AllocationPending
	case ReconcileRunning:
		req.State = AllocationRunning
	case ReconcileFinalizing:
		req.State = AllocationRunning
	case ReconcileCompleted:
		req.State = AllocationExpired
	case ReconcileLost:
		req.State = AllocationFailed
	case ReconcileQuarantine, ReconcileSuspended, ReconcileDrain:
		req.State = AllocationNeedsAttention
	default:
		req.State = AllocationNeedsAttention
	}
	req.UpdatedAt = now
	st.Allocations[requestID] = req
	applyAllocationDeadlineToMachines(st, req, now)
	applyProviderProjectionToMachinesAndJobs(st, req, projection, now)
	if current, ok := st.Allocations[requestID]; ok {
		return current, nil
	}
	return req, nil
}

func applyProviderProjectionToMachinesAndJobs(st *State, req AllocationRequest, projection ProviderProjection, now time.Time) {
	if st == nil {
		return
	}
	providerJobID := strings.TrimSpace(req.ProviderIdentity.ProviderJobID)
	if providerJobID == "" {
		providerJobID = strings.TrimSpace(req.Target.ProviderJobID)
	}
	autoReplaced := false
	if projection.Action == ReconcileLost && allocationCanAutoReplaceBeforeStart(*st, req.ID) && req.ReplacementEpoch < DefaultMaxPreStartReplacements {
		autoReplaced = true
		req.ReplacementID = providerJobID
		req.ReplacementEpoch++
		req.ProviderIdentity.ProviderJobID = ""
		req.Target.ProviderJobID = ""
		req.Target.MachineID = ""
		req.Target.LeaseID = ""
		req.RawProviderState = strings.TrimSpace(projection.RawState)
		req.ProviderReason = firstNonEmpty(projection.Reason, "provider job was lost before start; replacement allocation pending")
		req.ProviderDeadline = time.Time{}
		req.State = AllocationRequestPersisted
		req.SubmitAttempts = 0
		req.UpdatedAt = now
		st.Allocations[req.ID] = req
		for id, attempt := range st.JobAttempts {
			if strings.TrimSpace(attempt.RequestID) != strings.TrimSpace(req.ID) || jobAttemptTerminal(attempt.Phase) {
				continue
			}
			attempt.Phase = JobQueued
			attempt.WorkerID = ""
			attempt.LeaseID = ""
			attempt.ProviderIdentity = ProviderIdentity{}
			attempt.Target.ProviderJobID = ""
			attempt.Target.MachineID = ""
			attempt.Target.LeaseID = ""
			attempt.Reason = firstNonEmpty(projection.Reason, "provider job was lost before start; queued for replacement allocation")
			attempt.UpdatedAt = now
			st.JobAttempts[id] = attempt
			removeJobFromMachines(st, attempt.ID)
		}
	}
	for key, machine := range st.Machines {
		if providerJobID == "" || strings.TrimSpace(machine.ProviderJobID) != providerJobID {
			continue
		}
		machine.ProviderState = projection.Projected
		if !req.ProviderDeadline.IsZero() {
			machine.LeaseExpiresAt = req.ProviderDeadline
		}
		switch projection.Action {
		case ReconcileRunning:
			if strings.TrimSpace(machine.State) == "" || strings.EqualFold(strings.TrimSpace(machine.State), string(LeaseStarting)) {
				machine.State = string(LeaseAccepting)
			}
		case ReconcilePending:
			if strings.TrimSpace(machine.State) == "" {
				machine.State = string(LeaseStarting)
			}
		case ReconcileFinalizing:
			machine.State = string(LeaseFinalizing)
		case ReconcileCompleted:
			machine.State = string(LeaseExpired)
		case ReconcileLost:
			machine.State = string(LeaseLost)
		case ReconcileQuarantine:
			machine.State = string(LeaseNeedsAttention)
		case ReconcileSuspended, ReconcileDrain:
			machine.State = string(LeaseDraining)
		}
		machine.UpdatedAt = now
		st.Machines[key] = machine
	}
	switch projection.Action {
	case ReconcileLost:
		if autoReplaced {
			return
		}
		for id, attempt := range st.JobAttempts {
			if strings.TrimSpace(attempt.RequestID) != strings.TrimSpace(req.ID) || jobAttemptTerminal(attempt.Phase) {
				continue
			}
			if attempt.Phase == JobStartIntent || attempt.Phase == JobStarted {
				attempt.Phase = JobAmbiguous
				attempt.Reason = firstNonEmpty(projection.Reason, "provider job was lost after possible start")
			} else {
				attempt.Phase = JobTombstoned
				attempt.Reason = firstNonEmpty(projection.Reason, "provider job was lost before start")
				removeJobFromMachines(st, attempt.ID)
			}
			attempt.UpdatedAt = now
			st.JobAttempts[id] = attempt
		}
	case ReconcileQuarantine:
		for id, attempt := range st.JobAttempts {
			if strings.TrimSpace(attempt.RequestID) != strings.TrimSpace(req.ID) || jobAttemptTerminal(attempt.Phase) {
				continue
			}
			attempt.Phase = JobQuarantined
			attempt.Reason = firstNonEmpty(projection.Reason, "provider state requires quarantine")
			attempt.UpdatedAt = now
			st.JobAttempts[id] = attempt
		}
	}
}

func jobAttemptTerminal(phase JobPhase) bool {
	switch phase {
	case JobTerminal, JobTombstoned, JobQuarantined:
		return true
	default:
		return false
	}
}

type WorkerDoctor struct {
	SharedRootMounted  bool `json:"shared_root_mounted"`
	AtomicCreateOK     bool `json:"atomic_create_ok"`
	FreeBytesOK        bool `json:"free_bytes_ok"`
	FreeInodesOK       bool `json:"free_inodes_ok"`
	CodexAvailable     bool `json:"codex_available"`
	CXPAvailable       bool `json:"cxp_available"`
	HomeOK             bool `json:"home_ok"`
	TmpWritable        bool `json:"tmp_writable"`
	ProxyOK            bool `json:"proxy_ok"`
	AuthPathOK         bool `json:"auth_path_ok"`
	ImageDigestMatch   bool `json:"image_digest_match"`
	ProtocolOK         bool `json:"protocol_ok"`
	MembershipProofOK  bool `json:"membership_proof_ok"`
	ContainerRuntimeOK bool `json:"container_runtime_ok"`
	ModulesOK          bool `json:"modules_ok"`
	BindMountsOK       bool `json:"bind_mounts_ok"`
	ProxyEnvInsideOK   bool `json:"proxy_env_inside_ok"`
}

func WorkerDoctorPassed(in WorkerDoctor) bool {
	return len(WorkerDoctorBlockers(in)) == 0
}

func WorkerDoctorBlockers(in WorkerDoctor) []string {
	var blockers []string
	if !in.SharedRootMounted {
		blockers = append(blockers, "missing shared root")
	}
	if !in.AtomicCreateOK {
		blockers = append(blockers, "atomic create/rename failed")
	}
	if !in.FreeBytesOK {
		blockers = append(blockers, "insufficient free bytes")
	}
	if !in.FreeInodesOK {
		blockers = append(blockers, "insufficient free inodes")
	}
	if !in.CodexAvailable {
		blockers = append(blockers, "missing codex")
	}
	if !in.CXPAvailable {
		blockers = append(blockers, "missing cxp")
	}
	if !in.HomeOK {
		blockers = append(blockers, "invalid HOME")
	}
	if !in.TmpWritable {
		blockers = append(blockers, "tmp is not writable")
	}
	if !in.ProxyOK {
		blockers = append(blockers, "proxy route failed")
	}
	if !in.AuthPathOK {
		blockers = append(blockers, "auth path unavailable")
	}
	if !in.ImageDigestMatch {
		blockers = append(blockers, "image digest mismatch")
	}
	if !in.ProtocolOK {
		blockers = append(blockers, "worker protocol incompatible")
	}
	if !in.MembershipProofOK {
		blockers = append(blockers, "provider membership proof failed")
	}
	if !in.ContainerRuntimeOK {
		blockers = append(blockers, "container runtime unavailable")
	}
	if !in.ModulesOK {
		blockers = append(blockers, "required modules unavailable")
	}
	if !in.BindMountsOK {
		blockers = append(blockers, "required bind mounts unavailable")
	}
	if !in.ProxyEnvInsideOK {
		blockers = append(blockers, "proxy environment missing inside worker")
	}
	return blockers
}

type ProviderJobState string

const (
	ProviderJobPending   ProviderJobState = "pending"
	ProviderJobRunning   ProviderJobState = "running"
	ProviderJobSuspended ProviderJobState = "suspended"
	ProviderJobPreempted ProviderJobState = "preempted"
	ProviderJobRequeued  ProviderJobState = "requeued"
	ProviderJobDone      ProviderJobState = "done"
	ProviderJobFailed    ProviderJobState = "failed"
	ProviderJobNodeFail  ProviderJobState = "node_fail"
	ProviderJobUnknown   ProviderJobState = "unknown"
)

type ReconcileAction string

const (
	ReconcileDrain      ReconcileAction = "drain"
	ReconcileLost       ReconcileAction = "lost"
	ReconcileQuarantine ReconcileAction = "quarantine"
	ReconcilePending    ReconcileAction = "pending"
	ReconcileRunning    ReconcileAction = "running"
	ReconcileSuspended  ReconcileAction = "suspended"
	ReconcileCompleted  ReconcileAction = "completed"
	ReconcileFinalizing ReconcileAction = "finalizing"
)

type ProviderProjection struct {
	Provider      Provider         `json:"provider,omitempty"`
	RawState      string           `json:"raw_state,omitempty"`
	Reason        string           `json:"reason,omitempty"`
	Projected     ProviderJobState `json:"projected,omitempty"`
	Action        ReconcileAction  `json:"action,omitempty"`
	PreviouslyRan bool             `json:"previously_ran,omitempty"`
}

func ProjectRawProviderState(provider Provider, rawState string, reason string, started bool, previouslyRan bool) ProviderProjection {
	normalized := strings.ToUpper(strings.TrimSpace(rawState))
	projection := ProviderProjection{Provider: provider, RawState: rawState, Reason: reason, PreviouslyRan: previouslyRan}
	if provider == ProviderLSF && normalized == "PEND" && previouslyRan {
		projection.Projected = ProviderJobSuspended
		if started {
			projection.Action = ReconcileQuarantine
		} else {
			projection.Action = ReconcileSuspended
		}
		return projection
	}
	switch string(provider) + ":" + normalized {
	case "slurm:PD", "lsf:PEND":
		projection.Projected = ProviderJobPending
		projection.Action = ReconcilePending
	case "slurm:R", "lsf:RUN":
		projection.Projected = ProviderJobRunning
		projection.Action = ReconcileRunning
	case "slurm:CG", "slurm:CD", "lsf:DONE", "lsf:EXIT":
		projection.Projected = ProviderJobDone
		projection.Action = ReconcileFinalizing
	case "slurm:S", "slurm:STOPPED", "lsf:SSUSP", "lsf:USUSP", "lsf:PSUSP":
		projection.Projected = ProviderJobSuspended
		projection.Action = ReconcileSuspended
	case "slurm:CA", "slurm:F", "slurm:TO", "slurm:PR", "slurm:REQUEUE", "slurm:NF", "slurm:OOM", "lsf:UNKWN", "lsf:ZOMBI":
		projection.Projected = ProviderJobFailed
		if started {
			projection.Action = ReconcileQuarantine
		} else {
			projection.Action = ReconcileLost
		}
	default:
		projection.Projected = ProviderJobUnknown
		projection.Action = ReconcileDrain
	}
	return projection
}

type LeaseReadiness struct {
	ProviderState       ProviderJobState
	WorkerHeartbeat     bool
	Doctor              WorkerDoctor
	MembershipProofOK   bool
	SignatureMatch      bool
	ProtocolCompatible  bool
	ResourceAvailable   bool
	RemainingTTLSeconds int
	RequiredTTLSeconds  int
}

func LeaseCanAccept(in LeaseReadiness) bool {
	return in.ProviderState == ProviderJobRunning &&
		in.WorkerHeartbeat &&
		WorkerDoctorPassed(in.Doctor) &&
		in.MembershipProofOK &&
		in.SignatureMatch &&
		in.ProtocolCompatible &&
		in.ResourceAvailable &&
		(in.RequiredTTLSeconds <= 0 || in.RemainingTTLSeconds >= in.RequiredTTLSeconds)
}

type TerminalIntegrity string

const (
	TerminalNone              TerminalIntegrity = "none"
	TerminalValid             TerminalIntegrity = "valid"
	TerminalEventGap          TerminalIntegrity = "event_gap"
	TerminalHMACBad           TerminalIntegrity = "hmac_bad"
	TerminalSeqBad            TerminalIntegrity = "seq_bad"
	TerminalDuplicate         TerminalIntegrity = "duplicate"
	TerminalDuplicateSame     TerminalIntegrity = "duplicate_same"
	TerminalDuplicateConflict TerminalIntegrity = "duplicate_conflict"
	TerminalLateWrite         TerminalIntegrity = "late_write"
)

type RecoveryAction string

const (
	RecoveryRequeue    RecoveryAction = "requeue"
	RecoveryMonitor    RecoveryAction = "monitor"
	RecoveryComplete   RecoveryAction = "complete"
	RecoveryAmbiguous  RecoveryAction = "ambiguous"
	RecoveryQuarantine RecoveryAction = "quarantine"
)

func RecoverJob(phase JobPhase, workerAlive bool, terminal TerminalIntegrity) RecoveryAction {
	switch terminal {
	case TerminalValid, TerminalDuplicateSame:
		return RecoveryComplete
	case TerminalEventGap, TerminalHMACBad, TerminalSeqBad, TerminalDuplicate, TerminalDuplicateConflict, TerminalLateWrite:
		return RecoveryQuarantine
	}
	if workerAlive {
		return RecoveryMonitor
	}
	switch phase {
	case JobQueued, JobClaimed:
		return RecoveryRequeue
	case JobStartIntent, JobStarted:
		return RecoveryAmbiguous
	default:
		return RecoveryQuarantine
	}
}

type LaunchWindow struct {
	ClaimSynced           bool
	StartIntentSynced     bool
	ProcessStartAckSynced bool
	WorkerProvesNoExec    bool
}

func RecoverLaunchWindow(in LaunchWindow) RecoveryAction {
	if !in.ClaimSynced {
		return RecoveryQuarantine
	}
	if !in.StartIntentSynced {
		return RecoveryRequeue
	}
	if in.WorkerProvesNoExec {
		return RecoveryRequeue
	}
	return RecoveryAmbiguous
}

type RuntimeFailureKind string

const (
	RuntimeFailureAllocationDenied   RuntimeFailureKind = "allocation_denied"
	RuntimeFailureTemporaryScheduler RuntimeFailureKind = "temporary_scheduler"
	RuntimeFailureOOM                RuntimeFailureKind = "oom"
	RuntimeFailureWalltime           RuntimeFailureKind = "walltime"
	RuntimeFailureAdminCancel        RuntimeFailureKind = "admin_cancel"
	RuntimeFailureNodeReboot         RuntimeFailureKind = "node_reboot"
	RuntimeFailureContainerKill      RuntimeFailureKind = "container_kill"
	RuntimeFailureDiskFull           RuntimeFailureKind = "disk_full"
	RuntimeFailureTerminalValid      RuntimeFailureKind = "terminal_valid"
)

type RuntimeFailureInput struct {
	Kind              RuntimeFailureKind
	AfterProcessStart bool
	ValidTerminal     bool
}

type RuntimeFailureDecision struct {
	Action string `json:"action"`
	Retry  string `json:"retry"`
}

func ClassifyRuntimeFailure(in RuntimeFailureInput) RuntimeFailureDecision {
	if in.ValidTerminal || in.Kind == RuntimeFailureTerminalValid {
		return RuntimeFailureDecision{Action: "deliver_terminal", Retry: "no"}
	}
	switch in.Kind {
	case RuntimeFailureAllocationDenied:
		return RuntimeFailureDecision{Action: "fail_request", Retry: "after-edit"}
	case RuntimeFailureTemporaryScheduler:
		return RuntimeFailureDecision{Action: "retry_allocation", Retry: "auto"}
	case RuntimeFailureOOM:
		if !in.AfterProcessStart {
			return RuntimeFailureDecision{Action: "requeue", Retry: "auto"}
		}
		return RuntimeFailureDecision{Action: "ambiguous", Retry: "fork-required"}
	case RuntimeFailureWalltime, RuntimeFailureAdminCancel, RuntimeFailureNodeReboot, RuntimeFailureContainerKill:
		if in.AfterProcessStart {
			return RuntimeFailureDecision{Action: "ambiguous", Retry: "fork-required"}
		}
		return RuntimeFailureDecision{Action: "requeue", Retry: "auto"}
	case RuntimeFailureDiskFull:
		if in.AfterProcessStart {
			return RuntimeFailureDecision{Action: "needs_attention", Retry: "unsafe"}
		}
		return RuntimeFailureDecision{Action: "block_claim", Retry: "after-cleanup"}
	default:
		if in.AfterProcessStart {
			return RuntimeFailureDecision{Action: "ambiguous", Retry: "manual"}
		}
		return RuntimeFailureDecision{Action: "needs_attention", Retry: "manual"}
	}
}

type BeaconErrorContext struct {
	Phase          string
	Target         string
	ProviderJobID  string
	ProviderState  string
	ProviderReason string
	ConversationID string
	JobID          string
	Retry          string
	Next           string
}

func RenderBeaconError(in BeaconErrorContext) string {
	return "phase=" + strings.TrimSpace(in.Phase) +
		" target=" + strings.TrimSpace(in.Target) +
		" provider_job=" + strings.TrimSpace(in.ProviderJobID) +
		" provider_state=" + strings.TrimSpace(in.ProviderState) +
		" provider_reason=" + strings.TrimSpace(in.ProviderReason) +
		" conversation=" + strings.TrimSpace(in.ConversationID) +
		" job=" + strings.TrimSpace(in.JobID) +
		" retry=" + strings.TrimSpace(in.Retry) +
		" next=" + strings.TrimSpace(in.Next)
}
